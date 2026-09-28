//! Raft consensus. Every write is a log entry; an entry is applied to the
//! state machine once a majority of nodes has persisted it, so an
//! acknowledged write survives the loss of any minority of nodes (e.g. one
//! availability zone out of three).
//!
//! The protocol follows the Raft paper, plus:
//! * pre-vote and leader stickiness, so a node that was partitioned away
//!   cannot disrupt a healthy leader when it comes back;
//! * check-quorum, so a leader that loses contact with a majority steps down
//!   instead of accepting writes it cannot commit;
//! * log compaction with snapshot transfer for lagging or replaced nodes.
//!
//! All state transitions happen on one dedicated thread (the "node"), which
//! owns every write to the redb file. Networking runs on tokio.

pub mod log;
pub mod rpc;
pub mod snapshot;

use crate::error::{Error, Result};
use crate::statemachine::{self, Command, Proposal};
use crate::storage::Store;
use bson::Document;
use log::{Entry, RaftLog, put_u64};
use redb::Durability;
use rpc::{PeerClient, Request, Response, Security};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub id: u64,
    /// Address other nodes use for Raft traffic.
    pub raft_addr: String,
    /// Address clients use (reported to drivers in `hello`).
    pub client_addr: String,
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub id: u64,
    pub members: Vec<Member>,
    pub cluster: String,
    pub key: Option<Vec<u8>>,
    pub heartbeat: Duration,
    pub election_min: Duration,
    pub election_max: Duration,
    pub rpc_timeout: Duration,
    pub data_dir: PathBuf,
    /// Compact the log once it holds this many applied entries...
    pub compact_threshold: u64,
    /// ...keeping this many recent entries for followers that are slightly behind.
    pub keep_entries: u64,
    pub max_append_bytes: usize,
}

impl RaftConfig {
    pub fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Follower => "follower",
            Role::PreCandidate => "pre-candidate",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerStatus {
    pub id: u64,
    pub match_index: u64,
    pub last_ack_millis_ago: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Status {
    pub id: u64,
    pub role: Role,
    pub term: u64,
    pub leader: Option<u64>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_index: u64,
    pub peers: Vec<PeerStatus>,
}

/// Result of proposing an entry on this node.
#[derive(Debug)]
pub enum Outcome {
    /// Committed and applied at `index`; the command's reply or error.
    Applied(u64, std::result::Result<Document, Error>),
    /// This node is not the leader; nothing was logged.
    NotLeader,
    /// The entry was logged but leadership changed before it was applied; it
    /// may or may not take effect.
    Lost,
}

enum Sent {
    Vote { pre: bool },
    Append,
}

enum Event {
    Rpc(Request, oneshot::Sender<Response>),
    Response { peer: u64, term_sent: u64, sent: Sent, resp: std::result::Result<Response, Error> },
    Propose(Vec<u8>, oneshot::Sender<Outcome>),
    SnapshotDone { peer: u64, term_sent: u64, last_index: u64, resp_term: u64, ok: bool },
}

struct PeerState {
    next: u64,
    matched: u64,
    inflight: bool,
    snapshotting: bool,
    last_sent: Option<Instant>,
    last_ack: Option<Instant>,
}

struct IncomingSnapshot {
    path: PathBuf,
    last_index: u64,
    last_term: u64,
    offset: u64,
    file: std::fs::File,
}

struct Node {
    cfg: RaftConfig,
    store: Arc<Store>,
    log: RaftLog,
    role: Role,
    leader: Option<u64>,
    commit_index: u64,
    election_deadline: Instant,
    last_leader_contact: Option<Instant>,
    next_quorum_check: Instant,
    votes: HashSet<u64>,
    peers: BTreeMap<u64, PeerState>,
    pending: BTreeMap<u64, (u64, oneshot::Sender<Outcome>)>,
    proposals: Vec<(Vec<u8>, Option<oneshot::Sender<Outcome>>)>,
    incoming: Option<IncomingSnapshot>,
    events: std_mpsc::Sender<Event>,
    rt: tokio::runtime::Handle,
    clients: Arc<HashMap<u64, PeerClient>>,
    status_tx: watch::Sender<Status>,
    applied_tx: watch::Sender<u64>,
}

/// The async-facing handle to the Raft node.
#[derive(Clone)]
pub struct RaftHandle {
    events: std_mpsc::Sender<Event>,
    pub status: watch::Receiver<Status>,
    pub applied: watch::Receiver<u64>,
    clients: Arc<HashMap<u64, PeerClient>>,
    pub cfg: Arc<RaftConfig>,
}

pub fn random_timeout(min: Duration, max: Duration) -> Duration {
    if max <= min {
        return min;
    }
    let span = (max - min).as_millis() as u64;
    min + Duration::from_millis(rand::random::<u64>() % span.max(1))
}

/// Starts the Raft node thread and the peer listener. Returns the handle.
pub async fn start(cfg: RaftConfig, store: Arc<Store>) -> Result<RaftHandle> {
    let sec = Security { cluster: cfg.cluster.clone(), node: cfg.id, key: cfg.key.clone().map(Arc::new) };
    let mut clients = HashMap::new();
    for m in &cfg.members {
        if m.id != cfg.id {
            clients.insert(m.id, PeerClient::new(m.raft_addr.clone(), m.id, sec.clone()));
        }
    }
    let clients = Arc::new(clients);
    let log = RaftLog::load(&store)?;
    let (tx, rx) = std_mpsc::channel();
    let status = Status {
        id: cfg.id,
        role: Role::Follower,
        term: log.term,
        leader: None,
        commit_index: log.applied_index,
        applied_index: log.applied_index,
        last_index: log.last_index,
        peers: vec![],
    };
    let (status_tx, status_rx) = watch::channel(status);
    let (applied_tx, applied_rx) = watch::channel(log.applied_index);
    let now = Instant::now();
    let peers = cfg
        .members
        .iter()
        .filter(|m| m.id != cfg.id)
        .map(|m| (m.id, PeerState { next: log.last_index + 1, matched: 0, inflight: false, snapshotting: false, last_sent: None, last_ack: None }))
        .collect();
    std::fs::create_dir_all(cfg.data_dir.join("snapshots"))?;
    let node = Node {
        commit_index: log.applied_index,
        election_deadline: now + random_timeout(cfg.election_min, cfg.election_max),
        next_quorum_check: now,
        last_leader_contact: None,
        cfg: cfg.clone(),
        store,
        log,
        role: Role::Follower,
        leader: None,
        votes: HashSet::new(),
        peers,
        pending: BTreeMap::new(),
        proposals: Vec::new(),
        incoming: None,
        events: tx.clone(),
        rt: tokio::runtime::Handle::current(),
        clients: clients.clone(),
        status_tx,
        applied_tx,
    };
    std::thread::Builder::new()
        .name("raft".into())
        .spawn(move || {
            if let Err(e) = node.run(rx) {
                tracing::error!(error = %e, "raft node failed; exiting so the process can be restarted");
                std::process::exit(70);
            }
        })
        .map_err(|e| Error::internal(format!("cannot start raft thread: {e}")))?;
    let handle = RaftHandle { events: tx, status: status_rx, applied: applied_rx, clients, cfg: Arc::new(cfg) };
    let me = handle.cfg.members.iter().find(|m| m.id == handle.cfg.id).cloned();
    if let Some(me) = me
        && handle.cfg.members.len() > 1
    {
        let listener = tokio::net::TcpListener::bind(bind_addr(&me.raft_addr)).await?;
        tracing::info!(addr = %me.raft_addr, "raft listener started");
        let h = handle.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { continue };
                let h = h.clone();
                let sec = sec.clone();
                tokio::spawn(rpc::serve_peer(stream, sec, move |_peer, req| {
                    let h = h.clone();
                    async move { h.handle_rpc(req).await }
                }));
            }
        });
    }
    Ok(handle)
}

/// Listen on all interfaces at the port of an advertised `host:port`.
pub fn bind_addr(advertised: &str) -> String {
    match advertised.rsplit_once(':') {
        Some((_, port)) => format!("0.0.0.0:{port}"),
        None => advertised.to_string(),
    }
}

impl RaftHandle {
    async fn handle_rpc(&self, req: Request) -> Response {
        match req {
            Request::Forward { proposal } => match self.propose_local(proposal).await {
                Outcome::Applied(index, Ok(reply)) => Response::Forward { index, reply: Some(reply), error: None },
                Outcome::Applied(index, Err(e)) => Response::Forward { index, reply: None, error: Some((e.code, e.msg)) },
                Outcome::NotLeader => Response::Forward { index: 0, reply: None, error: Some((10107, "not leader".into())) },
                Outcome::Lost => Response::Forward {
                    index: 0,
                    reply: None,
                    error: Some((11602, "leadership changed while the write was in progress".into())),
                },
            },
            other => {
                let (tx, rx) = oneshot::channel();
                if self.events.send(Event::Rpc(other, tx)).is_err() {
                    return Response::Error { msg: "node stopped".into() };
                }
                rx.await.unwrap_or(Response::Error { msg: "node stopped".into() })
            }
        }
    }

    /// Proposes on this node; only succeeds on the leader.
    pub async fn propose_local(&self, data: Vec<u8>) -> Outcome {
        let (tx, rx) = oneshot::channel();
        if self.events.send(Event::Propose(data, tx)).is_err() {
            return Outcome::NotLeader;
        }
        rx.await.unwrap_or(Outcome::Lost)
    }

    pub fn is_leader(&self) -> bool {
        self.status.borrow().role == Role::Leader
    }

    /// Replicates a write through the leader (forwarding from followers when
    /// `forward` is set) and returns its reply once applied on this node.
    pub async fn propose(&self, p: &Proposal, forward: bool) -> Result<Document> {
        let data = p.encode();
        let deadline = tokio::time::Instant::now() + self.cfg.election_max * 4 + Duration::from_secs(2);
        let mut status = self.status.clone();
        loop {
            let st = status.borrow_and_update().clone();
            match st.leader {
                Some(l) if l == self.cfg.id && st.role == Role::Leader => match self.propose_local(data.clone()).await {
                    Outcome::Applied(_, r) => return r,
                    Outcome::NotLeader => {}
                    Outcome::Lost => {
                        return Err(Error::interrupted_repl_change("leadership changed while the write was in progress"));
                    }
                },
                Some(l) if forward => {
                    let Some(client) = self.clients.get(&l) else {
                        return Err(Error::internal("unknown leader id"));
                    };
                    let timeout = deadline.saturating_duration_since(tokio::time::Instant::now()).max(Duration::from_secs(1));
                    match client.call(Request::Forward { proposal: data.clone() }, timeout).await {
                        Ok(Response::Forward { index, reply, error }) => {
                            if let Some((10107, _)) = error {
                                // The leader moved; look again.
                            } else {
                                // Read-your-writes on this node: wait until it applied the entry.
                                let mut applied = self.applied.clone();
                                let _ = tokio::time::timeout(Duration::from_secs(5), applied.wait_for(|a| *a >= index)).await;
                                return match (reply, error) {
                                    (Some(r), _) => Ok(r),
                                    (None, Some((code, msg))) => Err(Error::from_code(code, msg)),
                                    (None, None) => Err(Error::internal("empty forward response")),
                                };
                            }
                        }
                        Ok(other) => return Err(Error::internal(format!("unexpected forward response: {other:?}"))),
                        Err(e) => {
                            return Err(Error::host_unreachable(format!("could not reach the leader (node {l}): {}", e.msg)));
                        }
                    }
                }
                Some(_) => return Err(Error::not_writable_primary("not primary")),
                None if !forward && st.role != Role::Leader => return Err(Error::not_writable_primary("not primary")),
                None => {}
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(Error::not_writable_primary("no leader elected; the cluster may have lost its majority"));
            }
            let _ = tokio::time::timeout(Duration::from_millis(100).min(deadline - now), status.changed()).await;
        }
    }

    /// Waits until this node has applied every write committed before the
    /// call (linearizable read barrier).
    pub async fn barrier(&self, forward: bool) -> Result<()> {
        let p = Proposal { cmd: Command::Noop, now_millis: 0, session: None };
        self.propose(&p, forward).await.map(|_| ())
    }
}

impl Node {
    fn run(mut self, rx: std_mpsc::Receiver<Event>) -> Result<()> {
        tracing::info!(id = self.cfg.id, term = self.log.term, last_index = self.log.last_index, applied = self.log.applied_index, "raft node starting");
        loop {
            let wait = self.next_wakeup().saturating_duration_since(Instant::now());
            match rx.recv_timeout(wait) {
                Ok(ev) => self.handle(ev)?,
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Err(std_mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            }
            let mut drained = 0;
            while let Ok(ev) = rx.try_recv() {
                self.handle(ev)?;
                drained += 1;
                if drained > 10_000 {
                    break;
                }
            }
            self.tick()?;
            self.flush_proposals()?;
            self.send_appends()?;
            self.apply_committed()?;
            self.maybe_compact()?;
            self.publish_status();
        }
    }

    fn next_wakeup(&self) -> Instant {
        if self.role == Role::Leader {
            let hb = Instant::now() + self.cfg.heartbeat;
            hb.min(self.next_quorum_check)
        } else {
            self.election_deadline
        }
    }

    fn reset_election_timer(&mut self) {
        self.election_deadline = Instant::now() + random_timeout(self.cfg.election_min, self.cfg.election_max);
    }

    fn quorum(&self) -> usize {
        self.cfg.quorum()
    }

    fn publish_status(&self) {
        let now = Instant::now();
        let st = Status {
            id: self.cfg.id,
            role: self.role,
            term: self.log.term,
            leader: self.leader,
            commit_index: self.commit_index,
            applied_index: self.log.applied_index,
            last_index: self.log.last_index,
            peers: self
                .peers
                .iter()
                .map(|(id, p)| PeerStatus {
                    id: *id,
                    match_index: p.matched,
                    last_ack_millis_ago: p.last_ack.map(|t| now.duration_since(t).as_millis() as u64),
                })
                .collect(),
        };
        self.status_tx.send_if_modified(|old| {
            let changed = old.role != st.role
                || old.term != st.term
                || old.leader != st.leader
                || old.commit_index != st.commit_index
                || old.applied_index != st.applied_index
                || old.last_index != st.last_index;
            *old = st;
            changed
        });
    }

    fn handle(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Propose(data, reply) => {
                if self.role == Role::Leader {
                    self.proposals.push((data, Some(reply)));
                } else {
                    let _ = reply.send(Outcome::NotLeader);
                }
            }
            Event::Rpc(req, reply) => {
                let resp = match req {
                    Request::Vote { term, candidate, last_index, last_term, pre_vote } => {
                        self.on_vote_request(term, candidate, last_index, last_term, pre_vote)?
                    }
                    Request::Append { term, leader, prev_index, prev_term, entries, commit } => {
                        self.on_append(term, leader, prev_index, prev_term, entries, commit)?
                    }
                    Request::Snapshot { term, leader, last_index, last_term, offset, data, done } => {
                        self.on_snapshot_chunk(term, leader, last_index, last_term, offset, data, done)?
                    }
                    Request::Forward { .. } => Response::Error { msg: "forward is handled asynchronously".into() },
                };
                let _ = reply.send(resp);
            }
            Event::Response { peer, term_sent, sent, resp } => self.on_response(peer, term_sent, sent, resp)?,
            Event::SnapshotDone { peer, term_sent, last_index, resp_term, ok } => {
                if resp_term > self.log.term {
                    self.become_follower(resp_term, None)?;
                }
                if let Some(p) = self.peers.get_mut(&peer) {
                    p.snapshotting = false;
                    if ok && self.role == Role::Leader && term_sent == self.log.term {
                        p.matched = p.matched.max(last_index);
                        p.next = p.matched + 1;
                        p.last_ack = Some(Instant::now());
                        tracing::info!(peer, last_index, "snapshot installed on peer");
                    }
                }
                self.advance_commit()?;
            }
        }
        Ok(())
    }

    fn tick(&mut self) -> Result<()> {
        let now = Instant::now();
        if self.role == Role::Leader {
            if now >= self.next_quorum_check {
                // check-quorum: step down if a majority hasn't answered recently.
                let window = self.cfg.election_max;
                let alive = 1 + self.peers.values().filter(|p| p.last_ack.is_some_and(|t| now.duration_since(t) <= window)).count();
                if alive < self.quorum() {
                    tracing::warn!(term = self.log.term, alive, "leader lost contact with a majority; stepping down");
                    self.become_follower(self.log.term, None)?;
                } else {
                    self.next_quorum_check = now + window;
                }
            }
        } else if now >= self.election_deadline {
            self.start_pre_vote()?;
        }
        Ok(())
    }

    fn start_pre_vote(&mut self) -> Result<()> {
        self.role = Role::PreCandidate;
        self.leader = None;
        self.votes = HashSet::from([self.cfg.id]);
        self.reset_election_timer();
        tracing::debug!(term = self.log.term + 1, "starting pre-vote");
        if self.votes.len() >= self.quorum() {
            return self.become_candidate();
        }
        let term = self.log.term + 1;
        let req = |_: u64| Request::Vote { term, candidate: self.cfg.id, last_index: self.log.last_index, last_term: self.log.last_term, pre_vote: true };
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            self.send(id, term, Sent::Vote { pre: true }, req(id));
        }
        Ok(())
    }

    fn become_candidate(&mut self) -> Result<()> {
        let term = self.log.term + 1;
        self.log.save_hard_state(&self.store, term, Some(self.cfg.id))?;
        self.role = Role::Candidate;
        self.leader = None;
        self.votes = HashSet::from([self.cfg.id]);
        self.reset_election_timer();
        tracing::info!(term, "starting election");
        if self.votes.len() >= self.quorum() {
            return self.become_leader();
        }
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            let req = Request::Vote { term, candidate: self.cfg.id, last_index: self.log.last_index, last_term: self.log.last_term, pre_vote: false };
            self.send(id, term, Sent::Vote { pre: false }, req);
        }
        Ok(())
    }

    fn become_leader(&mut self) -> Result<()> {
        tracing::info!(term = self.log.term, "became leader");
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        let now = Instant::now();
        let next = self.log.last_index + 1;
        for p in self.peers.values_mut() {
            p.next = next;
            p.matched = 0;
            p.inflight = false;
            p.last_sent = None;
            p.last_ack = Some(now); // grace period before check-quorum
        }
        self.next_quorum_check = now + self.cfg.election_max;
        // A no-op entry from the new term lets earlier entries commit.
        let noop = Proposal { cmd: Command::Noop, now_millis: 0, session: None };
        self.proposals.insert(0, (noop.encode(), None));
        Ok(())
    }

    fn become_follower(&mut self, term: u64, leader: Option<u64>) -> Result<()> {
        if term > self.log.term {
            self.log.save_hard_state(&self.store, term, None)?;
        }
        if self.role == Role::Leader {
            tracing::info!(term, "stepping down");
            for (_, (_, reply)) in std::mem::take(&mut self.pending) {
                let _ = reply.send(Outcome::Lost);
            }
        }
        for (_, reply) in self.proposals.drain(..) {
            if let Some(r) = reply {
                let _ = r.send(Outcome::NotLeader);
            }
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.reset_election_timer();
        Ok(())
    }

    fn log_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        last_term > self.log.last_term || (last_term == self.log.last_term && last_index >= self.log.last_index)
    }

    fn heard_from_leader_recently(&self) -> bool {
        self.role == Role::Leader || self.last_leader_contact.is_some_and(|t| t.elapsed() < self.cfg.election_min)
    }

    fn on_vote_request(&mut self, term: u64, candidate: u64, last_index: u64, last_term: u64, pre_vote: bool) -> Result<Response> {
        // Leader stickiness: a node that still hears from a leader ignores
        // attempts to replace it (protects against disruptive rejoiners).
        if term > self.log.term && self.heard_from_leader_recently() {
            return Ok(Response::Vote { term: self.log.term, granted: false });
        }
        if pre_vote {
            let granted = term > self.log.term && self.log_up_to_date(last_index, last_term);
            return Ok(Response::Vote { term: self.log.term, granted });
        }
        if term > self.log.term {
            self.become_follower(term, None)?;
        }
        let granted = term == self.log.term
            && self.vote_available(candidate)
            && self.log_up_to_date(last_index, last_term);
        if granted {
            if self.log.vote != Some(candidate) {
                self.log.save_hard_state(&self.store, term, Some(candidate))?;
            }
            self.reset_election_timer();
        }
        Ok(Response::Vote { term: self.log.term, granted })
    }

    fn vote_available(&self, candidate: u64) -> bool {
        self.log.vote.is_none() || self.log.vote == Some(candidate)
    }

    fn accept_leader(&mut self, term: u64, leader: u64) -> Result<()> {
        if term > self.log.term || self.role != Role::Follower {
            self.become_follower(term, Some(leader))?;
        }
        self.leader = Some(leader);
        self.last_leader_contact = Some(Instant::now());
        self.reset_election_timer();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append(&mut self, term: u64, leader: u64, prev_index: u64, prev_term: u64, entries: Vec<Entry>, commit: u64) -> Result<Response> {
        if term < self.log.term {
            return Ok(Response::Append { term: self.log.term, success: false, match_index: 0, hint: 0 });
        }
        self.accept_leader(term, leader)?;
        let fail = |hint: u64, t: u64| Response::Append { term: t, success: false, match_index: 0, hint };
        if prev_index > self.log.last_index {
            return Ok(fail(self.log.last_index + 1, self.log.term));
        }
        // Entries at or below the compaction point are committed and applied here already.
        let (prev_index, entries) = if prev_index < self.log.start_index {
            let skip = (self.log.start_index - prev_index) as usize;
            if skip >= entries.len() {
                let m = prev_index + entries.len() as u64;
                return Ok(Response::Append { term: self.log.term, success: true, match_index: m, hint: 0 });
            }
            (self.log.start_index, entries[skip..].to_vec())
        } else {
            match self.log.term_at(&self.store, prev_index)? {
                Some(t) if t == prev_term => (prev_index, entries),
                Some(t) => {
                    // Skip back over the whole conflicting term.
                    let mut i = prev_index;
                    while i > self.log.start_index + 1 && self.log.term_at(&self.store, i - 1)? == Some(t) {
                        i -= 1;
                    }
                    return Ok(fail(i.max(self.commit_index + 1), self.log.term));
                }
                None => return Ok(fail(self.log.last_index + 1, self.log.term)),
            }
        };
        let last_new = prev_index + entries.len() as u64;
        // Find the first entry we don't already have.
        let mut first_new = entries.len();
        let mut truncate = None;
        for (i, e) in entries.iter().enumerate() {
            if e.index > self.log.last_index {
                first_new = i;
                break;
            }
            if self.log.term_at(&self.store, e.index)? != Some(e.term) {
                if e.index <= self.commit_index {
                    return Err(Error::internal(format!(
                        "raft safety violation: leader {leader} tried to overwrite committed entry {}",
                        e.index
                    )));
                }
                truncate = Some(e.index);
                first_new = i;
                break;
            }
        }
        if first_new < entries.len() {
            self.log.append(&self.store, truncate, &entries[first_new..])?;
        }
        let new_commit = commit.min(last_new);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
        }
        Ok(Response::Append { term: self.log.term, success: true, match_index: last_new, hint: 0 })
    }

    #[allow(clippy::too_many_arguments)]
    fn on_snapshot_chunk(&mut self, term: u64, leader: u64, last_index: u64, last_term: u64, offset: u64, data: Vec<u8>, done: bool) -> Result<Response> {
        use std::io::Write;
        if term < self.log.term {
            return Ok(Response::Snapshot { term: self.log.term, ok: false });
        }
        self.accept_leader(term, leader)?;
        let t = self.log.term;
        if offset == 0 {
            let path = self.cfg.data_dir.join("snapshots").join(format!("recv-{last_index}"));
            let file = std::fs::File::create(&path)?;
            if let Some(old) = self.incoming.take() {
                let _ = std::fs::remove_file(old.path);
            }
            self.incoming = Some(IncomingSnapshot { path, last_index, last_term, offset: 0, file });
        }
        let Some(inc) = self.incoming.as_mut() else {
            return Ok(Response::Snapshot { term: t, ok: false });
        };
        if inc.last_index != last_index || inc.last_term != last_term || inc.offset != offset {
            return Ok(Response::Snapshot { term: t, ok: false });
        }
        inc.file.write_all(&data)?;
        inc.offset += data.len() as u64;
        if !done {
            return Ok(Response::Snapshot { term: t, ok: true });
        }
        let inc = self.incoming.take().unwrap();
        inc.file.sync_all()?;
        drop(inc.file);
        if last_index <= self.log.applied_index {
            let _ = std::fs::remove_file(&inc.path);
            return Ok(Response::Snapshot { term: t, ok: true });
        }
        let keep_suffix = self.log.term_at(&self.store, last_index)? == Some(last_term);
        tracing::info!(last_index, last_term, keep_suffix, "installing snapshot from leader");
        snapshot::install(&self.store, &inc.path, last_index, last_term, keep_suffix)?;
        let _ = std::fs::remove_file(&inc.path);
        self.log.start_index = last_index;
        self.log.start_term = last_term;
        self.log.applied_index = last_index;
        if !keep_suffix {
            self.log.last_index = last_index;
            self.log.last_term = last_term;
        }
        self.commit_index = self.commit_index.max(last_index);
        let _ = self.applied_tx.send(last_index);
        Ok(Response::Snapshot { term: t, ok: true })
    }

    fn send(&self, peer: u64, term_sent: u64, sent: Sent, req: Request) {
        if !self.clients.contains_key(&peer) {
            return;
        }
        let clients = self.clients.clone();
        let tx = self.events.clone();
        let timeout = self.cfg.rpc_timeout;
        self.rt.spawn(async move {
            let resp = clients[&peer].call(req, timeout).await;
            let _ = tx.send(Event::Response { peer, term_sent, sent, resp });
        });
    }

    fn on_response(&mut self, peer: u64, term_sent: u64, sent: Sent, resp: std::result::Result<Response, Error>) -> Result<()> {
        match sent {
            Sent::Append => {
                if let Some(p) = self.peers.get_mut(&peer) {
                    p.inflight = false;
                }
            }
            Sent::Vote { .. } => {}
        }
        let resp = match resp {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };
        match (sent, resp) {
            (Sent::Vote { pre }, Response::Vote { term, granted }) => {
                if term > self.log.term {
                    self.become_follower(term, None)?;
                    return Ok(());
                }
                let expected = if pre { Role::PreCandidate } else { Role::Candidate };
                let expected_term = if pre { self.log.term + 1 } else { self.log.term };
                if self.role != expected || term_sent != expected_term || !granted {
                    return Ok(());
                }
                self.votes.insert(peer);
                if self.votes.len() >= self.quorum() {
                    if pre {
                        self.become_candidate()?;
                    } else {
                        self.become_leader()?;
                    }
                }
            }
            (Sent::Append, Response::Append { term, success, match_index, hint }) => {
                if term > self.log.term {
                    self.become_follower(term, None)?;
                    return Ok(());
                }
                if self.role != Role::Leader || term_sent != self.log.term {
                    return Ok(());
                }
                let Some(p) = self.peers.get_mut(&peer) else { return Ok(()) };
                p.last_ack = Some(Instant::now());
                if success {
                    if match_index > p.matched {
                        p.matched = match_index;
                    }
                    p.next = p.next.max(p.matched + 1);
                    self.advance_commit()?;
                } else {
                    p.next = (p.matched + 1).max(hint.min(p.next.saturating_sub(1))).max(1);
                    p.last_sent = None; // retry immediately
                }
            }
            (_, Response::Error { msg }) => {
                tracing::debug!(peer, %msg, "peer returned an error");
            }
            _ => {}
        }
        Ok(())
    }

    fn flush_proposals(&mut self) -> Result<()> {
        if self.proposals.is_empty() {
            return Ok(());
        }
        if self.role != Role::Leader {
            for (_, reply) in self.proposals.drain(..) {
                if let Some(r) = reply {
                    let _ = r.send(Outcome::NotLeader);
                }
            }
            return Ok(());
        }
        let term = self.log.term;
        let mut entries = Vec::with_capacity(self.proposals.len());
        let mut replies = Vec::new();
        let mut index = self.log.last_index;
        for (data, reply) in self.proposals.drain(..) {
            index += 1;
            entries.push(Entry { index, term, data });
            if let Some(r) = reply {
                replies.push((index, r));
            }
        }
        self.log.append(&self.store, None, &entries)?;
        for (index, r) in replies {
            self.pending.insert(index, (term, r));
        }
        self.advance_commit()
    }

    fn advance_commit(&mut self) -> Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let mut matches: Vec<u64> = self.peers.values().map(|p| p.matched).collect();
        matches.push(self.log.last_index);
        matches.sort_unstable_by(|a, b| b.cmp(a));
        let n = matches[self.quorum() - 1];
        // Only entries from the current term are committed by counting replicas.
        if n > self.commit_index && self.log.term_at(&self.store, n)? == Some(self.log.term) {
            self.commit_index = n;
        }
        Ok(())
    }

    fn send_appends(&mut self) -> Result<()> {
        if self.role != Role::Leader {
            return Ok(());
        }
        let now = Instant::now();
        let ids: Vec<u64> = self.peers.keys().copied().collect();
        for id in ids {
            let (next, due) = {
                let p = &self.peers[&id];
                if p.inflight || p.snapshotting {
                    continue;
                }
                let heartbeat_due = p.last_sent.is_none_or(|t| now.duration_since(t) >= self.cfg.heartbeat);
                (p.next, heartbeat_due || p.next <= self.log.last_index)
            };
            if !due {
                continue;
            }
            if next <= self.log.start_index {
                self.start_snapshot_transfer(id);
                continue;
            }
            let prev_index = next - 1;
            let Some(prev_term) = self.log.term_at(&self.store, prev_index)? else {
                self.start_snapshot_transfer(id);
                continue;
            };
            let entries = self.log.entries(&self.store, next, self.log.last_index, self.cfg.max_append_bytes)?;
            let req = Request::Append { term: self.log.term, leader: self.cfg.id, prev_index, prev_term, entries, commit: self.commit_index };
            let p = self.peers.get_mut(&id).unwrap();
            p.inflight = true;
            p.last_sent = Some(now);
            self.send(id, self.log.term, Sent::Append, req);
        }
        Ok(())
    }

    fn start_snapshot_transfer(&mut self, peer: u64) {
        let Some(p) = self.peers.get_mut(&peer) else { return };
        p.snapshotting = true;
        let store = self.store.clone();
        let clients = self.clients.clone();
        let tx = self.events.clone();
        let term = self.log.term;
        let leader = self.cfg.id;
        let dir = self.cfg.data_dir.join("snapshots");
        let timeout = self.cfg.rpc_timeout.max(Duration::from_secs(10));
        tracing::info!(peer, "peer is behind the compacted log; sending a snapshot");
        self.rt.spawn(async move {
            let path = dir.join(format!("send-{peer}-{}", rand::random::<u32>()));
            let p2 = path.clone();
            let made = tokio::task::spawn_blocking(move || snapshot::create(&store, &p2)).await;
            let (last_index, last_term) = match made {
                Ok(Ok(v)) => v,
                other => {
                    tracing::error!(?other, "failed to create snapshot");
                    let _ = tx.send(Event::SnapshotDone { peer, term_sent: term, last_index: 0, resp_term: 0, ok: false });
                    return;
                }
            };
            let result: std::result::Result<u64, u64> = async {
                let data = tokio::fs::read(&path).await.map_err(|_| 0u64)?;
                const CHUNK: usize = 1 << 20;
                let mut offset = 0usize;
                loop {
                    let end = (offset + CHUNK).min(data.len());
                    let done = end == data.len();
                    let req = Request::Snapshot { term, leader, last_index, last_term, offset: offset as u64, data: data[offset..end].to_vec(), done };
                    match clients[&peer].call(req, timeout).await {
                        Ok(Response::Snapshot { term: t, ok: true }) if t == term => {}
                        Ok(Response::Snapshot { term: t, .. }) => return Err(t),
                        _ => return Err(0),
                    }
                    if done {
                        return Ok(last_index);
                    }
                    offset = end;
                }
            }
            .await;
            let _ = tokio::fs::remove_file(&path).await;
            let ev = match result {
                Ok(li) => Event::SnapshotDone { peer, term_sent: term, last_index: li, resp_term: term, ok: true },
                Err(t) => Event::SnapshotDone { peer, term_sent: term, last_index: 0, resp_term: t, ok: false },
            };
            let _ = tx.send(ev);
        });
    }

    fn apply_committed(&mut self) -> Result<()> {
        while self.log.applied_index < self.commit_index {
            let from = self.log.applied_index + 1;
            let batch = self.log.entries(&self.store, from, self.commit_index, 8 << 20)?;
            if batch.is_empty() {
                return Err(Error::internal(format!("committed entries {from}..={} missing from the log", self.commit_index)));
            }
            let mut txn = self.store.begin_write()?;
            // The log is already durable; replaying it rebuilds anything lost
            // in a crash, so state-machine commits needn't fsync.
            txn.set_durability(Durability::None).map_err(|e| Error::internal(format!("durability: {e}")))?;
            let mut results = Vec::with_capacity(batch.len());
            for e in &batch {
                let p = Proposal::decode(&e.data)?;
                let r = statemachine::apply(&txn, &p, e.index)?;
                results.push((e.index, e.term, r));
            }
            let last = batch.last().unwrap();
            put_u64(&txn, "applied_index", last.index)?;
            put_u64(&txn, "applied_term", last.term)?;
            txn.commit()?;
            self.log.applied_index = last.index;
            for (index, term, r) in results {
                if let Some((pterm, reply)) = self.pending.remove(&index) {
                    let _ = reply.send(if pterm == term { Outcome::Applied(index, r) } else { Outcome::Lost });
                }
            }
            let _ = self.applied_tx.send(self.log.applied_index);
        }
        Ok(())
    }

    fn maybe_compact(&mut self) -> Result<()> {
        let applied = self.log.applied_index;
        if applied.saturating_sub(self.log.start_index) > self.cfg.compact_threshold {
            let upto = applied.saturating_sub(self.cfg.keep_entries);
            if upto > self.log.start_index {
                self.log.compact(&self.store, upto)?;
                tracing::debug!(upto, "compacted raft log");
            }
        }
        Ok(())
    }
}

/// Parses `id=raft_host:port/client_host:port,...` member lists.
pub fn parse_members(s: &str) -> Result<Vec<Member>> {
    let mut out = Vec::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (id, addrs) = part.split_once('=').ok_or_else(|| Error::bad_value(format!("bad member '{part}', expected id=raft_addr/client_addr")))?;
        let id: u64 = id.trim().parse().map_err(|_| Error::bad_value(format!("bad member id in '{part}'")))?;
        if id == 0 {
            return Err(Error::bad_value("member ids must be positive"));
        }
        let (raft, client) = addrs.split_once('/').ok_or_else(|| Error::bad_value(format!("bad member '{part}', expected id=raft_addr/client_addr")))?;
        out.push(Member { id, raft_addr: raft.trim().to_string(), client_addr: client.trim().to_string() });
    }
    let mut ids: Vec<u64> = out.iter().map(|m| m.id).collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != out.len() {
        return Err(Error::bad_value("duplicate member ids"));
    }
    Ok(out)
}
