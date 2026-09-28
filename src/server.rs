//! The client-facing server: accepts MongoDB wire-protocol connections and
//! dispatches commands. Reads are served from a local MVCC snapshot; writes
//! go through Raft.

use crate::auth::{self, Conversation};
use crate::bsonutil::{get_int, now_millis};
use crate::error::{Error, Result};
use crate::raft::{RaftHandle, Role};
use crate::statemachine::{Command, Proposal, SessionTxn};
use crate::storage::Store;
use crate::wire::{self, Request};
use bson::{Binary, Bson, DateTime, Document, doc, oid::ObjectId, spec::BinarySubtype};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

pub const MAX_WIRE_VERSION: i32 = 17; // MongoDB 6.0
pub const COMPAT_VERSION: &str = "6.0.0";
pub const MAX_BSON_SIZE: i32 = 16 * 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const CURSOR_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: String,
    /// This node's address as clients should reach it.
    pub advertise: String,
    /// Replica set name reported to drivers (None: report as standalone).
    pub repl_set_name: Option<String>,
    pub auth: bool,
    pub forward_writes: bool,
    pub root_user: Option<(String, String)>,
}

pub struct Cursor {
    pub ns: String,
    pub docs: VecDeque<Document>,
    pub last_used: Instant,
    pub owner: Option<String>,
}

pub struct Server {
    pub store: Arc<Store>,
    pub raft: RaftHandle,
    pub cfg: ServerConfig,
    pub cursors: Mutex<HashMap<i64, Cursor>>,
    conn_counter: AtomicI64,
    pub active_conns: AtomicI64,
    started: Instant,
    pub process_id: ObjectId,
}

#[derive(Clone, Debug)]
pub struct AuthedUser {
    pub db: String,
    pub user: String,
    pub read_only: bool,
}

pub struct Conn {
    pub id: i64,
    pub peer: SocketAddr,
    pub user: Option<AuthedUser>,
    scram: Option<Conversation>,
}

impl Conn {
    pub fn owner(&self) -> Option<String> {
        self.user.as_ref().map(|u| format!("{}.{}", u.db, u.user))
    }
}

/// Commands allowed before authentication.
const UNAUTHENTICATED: &[&str] = &[
    "hello",
    "isMaster",
    "ismaster",
    "ping",
    "buildInfo",
    "buildinfo",
    "saslStart",
    "saslContinue",
    "logout",
    "endSessions",
    "connectionStatus",
    "whatsmyuri",
    "getnonce",
];

/// Commands that change data (need write privileges and a writable node).
pub const WRITE_COMMANDS: &[&str] = &[
    "insert",
    "update",
    "delete",
    "findAndModify",
    "findandmodify",
    "create",
    "drop",
    "dropDatabase",
    "createIndexes",
    "dropIndexes",
    "deleteIndexes",
    "renameCollection",
    "createUser",
    "updateUser",
    "dropUser",
];

impl Server {
    pub fn new(store: Arc<Store>, raft: RaftHandle, cfg: ServerConfig) -> Arc<Server> {
        Arc::new(Server {
            store,
            raft,
            cfg,
            cursors: Mutex::new(HashMap::new()),
            conn_counter: AtomicI64::new(0),
            active_conns: AtomicI64::new(0),
            started: Instant::now(),
            process_id: ObjectId::new(),
        })
    }

    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        self.clone().spawn_background_tasks();
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let s = self.clone();
            tokio::spawn(async move {
                s.active_conns.fetch_add(1, Ordering::Relaxed);
                if let Err(e) = s.clone().handle_connection(stream, peer).await {
                    tracing::debug!(%peer, error = %e, "connection closed with error");
                }
                s.active_conns.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }

    async fn handle_connection(self: Arc<Self>, stream: TcpStream, peer: SocketAddr) -> Result<()> {
        let _ = stream.set_nodelay(true);
        let (r, mut w) = stream.into_split();
        let mut r = BufReader::with_capacity(64 * 1024, r);
        let id = self.conn_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let mut conn = Conn { id, peer, user: None, scram: None };
        while let Some((h, body)) = wire::read_message(&mut r).await? {
            let req = match wire::parse_request(&h, &body) {
                Ok(r) => r,
                Err(e) => {
                    // Reply in the protocol the client used, then carry on.
                    let reply = e.to_doc();
                    let bytes = if h.op_code == wire::OP_QUERY {
                        wire::encode_op_reply(h.request_id, &reply)?
                    } else {
                        wire::encode_msg_reply(h.request_id, &reply)?
                    };
                    wire::write_all(&mut w, &bytes).await?;
                    continue;
                }
            };
            match req {
                Request::Msg { request_id, body, more_to_come } => {
                    let db = match body.get("$db") {
                        Some(Bson::String(d)) => d.clone(),
                        _ => {
                            let reply = Error::failed_to_parse("OP_MSG requests require a $db argument").to_doc();
                            wire::write_all(&mut w, &wire::encode_msg_reply(request_id, &reply)?).await?;
                            continue;
                        }
                    };
                    let reply = self.clone().run_command(&mut conn, &db, body).await;
                    if !more_to_come {
                        let bytes = match wire::encode_msg_reply(request_id, &reply) {
                            Ok(b) if b.len() <= wire::MAX_MESSAGE_SIZE => b,
                            _ => wire::encode_msg_reply(
                                request_id,
                                &Error::bson_too_large("reply exceeds the maximum message size").to_doc(),
                            )?,
                        };
                        wire::write_all(&mut w, &bytes).await?;
                    }
                }
                Request::Query { request_id, db, body } => {
                    let reply = self.clone().run_command(&mut conn, &db, body).await;
                    wire::write_all(&mut w, &wire::encode_op_reply(request_id, &reply)?).await?;
                }
            }
        }
        Ok(())
    }

    /// Runs one command and returns the reply document (success or error).
    pub async fn run_command(self: Arc<Self>, conn: &mut Conn, db: &str, body: Document) -> Document {
        let Some(name) = body.keys().next().cloned() else {
            return Error::command_not_found("no command specified").to_doc();
        };
        let is_write = WRITE_COMMANDS.contains(&name.as_str());
        let result = self.clone().dispatch(conn, db, &name, &body).await;
        match result {
            Ok(reply) => reply,
            Err(e) => {
                let mut d = e.to_doc();
                if is_write && e.is_retryable_write() {
                    d.insert("errorLabels", vec!["RetryableWriteError"]);
                }
                if e.code != 59 && e.code != 13 && e.code != 18 {
                    tracing::debug!(command = %name, error = %e, "command failed");
                }
                d
            }
        }
    }

    async fn check_auth(&self, conn: &Conn, name: &str, db: &str, body: &Document) -> Result<()> {
        if !self.cfg.auth || UNAUTHENTICATED.contains(&name) {
            return Ok(());
        }
        match &conn.user {
            None => {
                // Localhost exception: with no users yet, a local client may create the first one.
                if name == "createUser" && conn.peer.ip().is_loopback() && self.store.snapshot()?.users()?.is_empty() {
                    return Ok(());
                }
                Err(Error::unauthorized(format!("Command {name} requires authentication")))
            }
            Some(u) => {
                let writes = WRITE_COMMANDS.contains(&name) || (name == "aggregate" && has_out_stage(body));
                if u.read_only && writes {
                    return Err(Error::unauthorized(format!("not authorized on {db} to execute command {name}")));
                }
                Ok(())
            }
        }
    }

    async fn dispatch(self: Arc<Self>, conn: &mut Conn, db: &str, name: &str, body: &Document) -> Result<Document> {
        self.check_auth(conn, name, db, body).await?;
        if body.contains_key("startTransaction") || matches!(body.get("autocommit"), Some(Bson::Boolean(false))) {
            return Err(Error::illegal_operation("Mango does not support multi-document transactions"));
        }
        if let Some(Bson::Document(rc)) = body.get("readConcern") {
            match rc.get_str("level") {
                Ok("linearizable") => self.raft.barrier(self.cfg.forward_writes).await?,
                Ok("snapshot") => {
                    return Err(Error::not_implemented("readConcern 'snapshot' requires transactions, which Mango does not support"));
                }
                _ => {}
            }
        }
        match name {
            "hello" | "isMaster" | "ismaster" => Ok(self.hello(conn, body)),
            "ping" => Ok(doc! {"ok": 1.0}),
            "buildInfo" | "buildinfo" => Ok(build_info()),
            "saslStart" => self.sasl_start(conn, db, body).await,
            "saslContinue" => self.sasl_continue(conn, body),
            "logout" => {
                conn.user = None;
                Ok(doc! {"ok": 1.0})
            }
            "endSessions" | "refreshSessions" | "killSessions" | "killAllSessions" | "killAllSessionsByPattern" => Ok(doc! {"ok": 1.0}),
            "startSession" => {
                let id = Binary { subtype: BinarySubtype::Uuid, bytes: (0..16).map(|_| rand::random::<u8>()).collect() };
                Ok(doc! {"id": {"id": id}, "timeoutMinutes": 30, "ok": 1.0})
            }
            "connectionStatus" => {
                let users: Vec<Document> = conn.user.iter().map(|u| doc! {"user": &u.user, "db": &u.db}).collect();
                Ok(doc! {"authInfo": {"authenticatedUsers": users, "authenticatedUserRoles": []}, "ok": 1.0})
            }
            "whatsmyuri" => Ok(doc! {"you": conn.peer.to_string(), "ok": 1.0}),
            "getParameter" => Ok(get_parameter(body)),
            "getCmdLineOpts" => Ok(doc! {"argv": [], "parsed": {}, "ok": 1.0}),
            "getLog" => Ok(doc! {"totalLinesWritten": 0, "log": [], "ok": 1.0}),
            "hostInfo" => Ok(doc! {
                "system": {"currentTime": DateTime::now(), "hostname": &self.cfg.advertise, "numCores": std::thread::available_parallelism().map(|n| n.get() as i32).unwrap_or(1)},
                "os": {"type": std::env::consts::OS},
                "extra": {},
                "ok": 1.0
            }),
            "serverStatus" => Ok(self.server_status()),
            "currentOp" => Ok(doc! {"inprog": [], "ok": 1.0}),
            "killOp" | "fsync" | "fsyncUnlock" | "setFeatureCompatibilityVersion" => Ok(doc! {"ok": 1.0}),
            "listCommands" => Ok(list_commands()),
            "replSetGetStatus" => self.repl_status(),
            "replSetGetConfig" => self.repl_config(),
            "mangoStatus" => Ok(self.mango_status()),
            "createUser" => self.create_user(db, body).await,
            "updateUser" => self.update_user(db, body).await,
            "dropUser" => self.drop_user(db, body).await,
            "usersInfo" => self.users_info(db, body),
            "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "create" | "drop" | "dropDatabase" | "createIndexes"
            | "dropIndexes" | "deleteIndexes" | "renameCollection" => self.write_command(db, name, body).await,
            "find" | "getMore" | "killCursors" | "count" | "distinct" | "aggregate" | "listCollections" | "listIndexes"
            | "listDatabases" | "dbStats" | "collStats" | "explain" => self.read_command(conn, db, name, body).await,
            "commitTransaction" | "abortTransaction" => {
                Err(Error::no_such_transaction("Mango does not support multi-document transactions"))
            }
            other => Err(Error::command_not_found(format!("no such command: '{other}'"))),
        }
    }

    // ---------- replication topology ----------

    fn member_addr(&self, id: u64) -> Option<String> {
        self.raft.cfg.members.iter().find(|m| m.id == id).map(|m| m.client_addr.clone())
    }

    pub fn hello(&self, conn: &Conn, body: &Document) -> Document {
        let st = self.raft.status.borrow().clone();
        let is_leader = st.role == Role::Leader;
        let mut d = Document::new();
        if body.contains_key("helloOk") {
            d.insert("helloOk", true);
        }
        match &self.cfg.repl_set_name {
            Some(set) => {
                d.insert("isWritablePrimary", is_leader);
                d.insert("ismaster", is_leader);
                d.insert("secondary", !is_leader);
                d.insert("setName", set.clone());
                d.insert("setVersion", 1);
                let hosts: Vec<String> = self.raft.cfg.members.iter().map(|m| m.client_addr.clone()).collect();
                d.insert("hosts", hosts);
                if let Some(l) = st.leader.and_then(|l| self.member_addr(l)) {
                    d.insert("primary", l);
                }
                d.insert("me", self.cfg.advertise.clone());
                if is_leader {
                    let mut oid = [0u8; 12];
                    oid[..4].copy_from_slice(&[0x7f, 0xff, 0xff, 0xff]);
                    oid[4..].copy_from_slice(&st.term.to_be_bytes());
                    d.insert("electionId", ObjectId::from_bytes(oid));
                }
                d.insert("lastWrite", doc! {"lastWriteDate": DateTime::now(), "majorityWriteDate": DateTime::now()});
            }
            None => {
                d.insert("isWritablePrimary", is_leader || self.cfg.forward_writes);
                d.insert("ismaster", is_leader || self.cfg.forward_writes);
            }
        }
        // Mango only implements SCRAM-SHA-256, so it is advertised for any
        // user name (some drivers ask about "admin.<user>" whatever the
        // authSource, and answering the same way for every name avoids
        // revealing which users exist).
        if body.contains_key("saslSupportedMechs") {
            d.insert("saslSupportedMechs", vec![auth::MECHANISM]);
        }
        d.insert("maxBsonObjectSize", MAX_BSON_SIZE);
        d.insert("maxMessageSizeBytes", wire::MAX_MESSAGE_SIZE as i32);
        d.insert("maxWriteBatchSize", 100_000);
        d.insert("localTime", DateTime::now());
        d.insert("logicalSessionTimeoutMinutes", 30);
        d.insert("connectionId", conn.id);
        d.insert("minWireVersion", 0);
        d.insert("maxWireVersion", MAX_WIRE_VERSION);
        d.insert("readOnly", false);
        d.insert("ok", 1.0);
        d
    }

    fn repl_status(&self) -> Result<Document> {
        let Some(set) = &self.cfg.repl_set_name else {
            return Err(Error { code: 76, code_name: "NoReplicationEnabled", msg: "not running with --replSet".into() });
        };
        let st = self.raft.status.borrow().clone();
        let mut members = Vec::new();
        for m in &self.raft.cfg.members {
            let is_self = m.id == st.id;
            let is_leader = st.leader == Some(m.id);
            let (state, state_str) = if is_leader { (1, "PRIMARY") } else { (2, "SECONDARY") };
            let mut md = doc! {"_id": m.id as i64, "name": &m.client_addr, "state": state, "stateStr": state_str};
            if is_self {
                md.insert("self", true);
                md.insert("health", 1.0);
                md.insert("appliedIndex", st.applied_index as i64);
            } else if let Some(p) = st.peers.iter().find(|p| p.id == m.id) {
                let healthy =
                    st.role != Role::Leader || p.last_ack_millis_ago.is_some_and(|ms| ms < self.raft.cfg.election_min.as_millis() as u64);
                md.insert("health", if healthy { 1.0 } else { 0.0 });
                if st.role == Role::Leader {
                    md.insert("matchIndex", p.match_index as i64);
                    if let Some(ms) = p.last_ack_millis_ago {
                        md.insert("lastHeartbeatMillisAgo", ms as i64);
                    }
                }
            }
            members.push(md);
        }
        Ok(doc! {
            "set": set.clone(),
            "date": DateTime::now(),
            "myState": if st.role == Role::Leader { 1 } else { 2 },
            "term": st.term as i64,
            "members": members,
            "ok": 1.0
        })
    }

    fn repl_config(&self) -> Result<Document> {
        let Some(set) = &self.cfg.repl_set_name else {
            return Err(Error { code: 76, code_name: "NoReplicationEnabled", msg: "not running with --replSet".into() });
        };
        let members: Vec<Document> =
            self.raft.cfg.members.iter().map(|m| doc! {"_id": m.id as i64, "host": &m.client_addr, "votes": 1, "priority": 1.0}).collect();
        Ok(doc! {"config": {"_id": set.clone(), "version": 1, "members": members, "protocolVersion": 1i64}, "ok": 1.0})
    }

    fn mango_status(&self) -> Document {
        let st = self.raft.status.borrow().clone();
        let peers: Vec<Document> = st
            .peers
            .iter()
            .map(|p| {
                let mut d = doc! {"id": p.id as i64, "matchIndex": p.match_index as i64};
                if let Some(ms) = p.last_ack_millis_ago {
                    d.insert("lastAckMillisAgo", ms as i64);
                }
                d
            })
            .collect();
        doc! {
            "id": st.id as i64,
            "role": st.role.as_str(),
            "term": st.term as i64,
            "leader": st.leader.map(|l| Bson::Int64(l as i64)).unwrap_or(Bson::Null),
            "commitIndex": st.commit_index as i64,
            "appliedIndex": st.applied_index as i64,
            "lastIndex": st.last_index as i64,
            "peers": peers,
            "version": env!("CARGO_PKG_VERSION"),
            "ok": 1.0
        }
    }

    fn server_status(&self) -> Document {
        let st = self.raft.status.borrow().clone();
        let active = self.active_conns.load(Ordering::Relaxed);
        doc! {
            "host": &self.cfg.advertise,
            "version": COMPAT_VERSION,
            "process": "mango",
            "pid": std::process::id() as i64,
            "uptime": self.started.elapsed().as_secs() as i64,
            "uptimeMillis": self.started.elapsed().as_millis() as i64,
            "localTime": DateTime::now(),
            "connections": {"current": active as i32, "available": 1_000_000 - active as i32, "totalCreated": self.conn_counter.load(Ordering::Relaxed)},
            "mango": {"role": st.role.as_str(), "term": st.term as i64, "appliedIndex": st.applied_index as i64},
            "ok": 1.0
        }
    }

    // ---------- authentication ----------

    async fn sasl_start(&self, conn: &mut Conn, db: &str, body: &Document) -> Result<Document> {
        let mech = body.get_str("mechanism").unwrap_or("");
        if mech != auth::MECHANISM {
            return Err(Error {
                code: 334,
                code_name: "MechanismUnavailable",
                msg: format!("Received authentication for mechanism {mech} which is not enabled"),
            });
        }
        let payload = sasl_payload(body)?;
        let snap = self.store.snapshot()?;
        let (conv, reply) = Conversation::start(db, &payload, |user| snap.user(db, user))?;
        conn.scram = Some(conv);
        Ok(doc! {"conversationId": 1, "done": false, "payload": Binary { subtype: BinarySubtype::Generic, bytes: reply }, "ok": 1.0})
    }

    fn sasl_continue(&self, conn: &mut Conn, body: &Document) -> Result<Document> {
        let payload = sasl_payload(body)?;
        let Some(conv) = conn.scram.as_mut() else {
            return Err(Error::auth_failed("No SASL session state found"));
        };
        let reply = match conv.finish(&payload) {
            Ok(r) => r,
            Err(e) => {
                conn.scram = None;
                return Err(e);
            }
        };
        // After a verified proof the conversation is complete; drivers may
        // skip the final empty round trip.
        let done = conv.done || conv.verified;
        if conv.verified {
            let snap = self.store.snapshot()?;
            let read_only = snap.user(&conv.db, &conv.user)?.map(|u| auth::is_read_only(&u)).unwrap_or(false);
            conn.user = Some(AuthedUser { db: conv.db.clone(), user: conv.user.clone(), read_only });
        }
        if conv.done {
            conn.scram = None;
        }
        Ok(doc! {"conversationId": 1, "done": done, "payload": Binary { subtype: BinarySubtype::Generic, bytes: reply }, "ok": 1.0})
    }

    async fn create_user(&self, db: &str, body: &Document) -> Result<Document> {
        let user = body.get_str("createUser").map_err(|_| Error::bad_value("createUser must be a string"))?;
        let pwd = body.get_str("pwd").map_err(|_| Error::bad_value("createUser requires a 'pwd' string (SCRAM-SHA-256)"))?;
        if user.is_empty() || user.contains('\0') {
            return Err(Error::bad_value("user names must be non-empty"));
        }
        if pwd.is_empty() {
            return Err(Error::bad_value("password cannot be empty"));
        }
        if let Some(Bson::Array(m)) = body.get("mechanisms")
            && !m.iter().any(|x| x.as_str() == Some(auth::MECHANISM))
        {
            return Err(Error::bad_value("Mango only supports SCRAM-SHA-256"));
        }
        let roles = body.get("roles").cloned().unwrap_or(Bson::Array(vec![]));
        let doc = auth::user_doc(db, user, pwd, roles);
        self.propose(Command::PutUser { key: format!("{db}.{user}"), user: doc, create: true }, None).await
    }

    async fn update_user(&self, db: &str, body: &Document) -> Result<Document> {
        let user = body.get_str("updateUser").map_err(|_| Error::bad_value("updateUser must be a string"))?;
        let snap = self.store.snapshot()?;
        let existing = snap.user(db, user)?.ok_or_else(|| Error {
            code: 11,
            code_name: "UserNotFound",
            msg: format!("User {user}@{db} not found"),
        })?;
        let roles = body.get("roles").cloned().unwrap_or_else(|| existing.get("roles").cloned().unwrap_or(Bson::Array(vec![])));
        let doc = match body.get_str("pwd") {
            Ok(pwd) => auth::user_doc(db, user, pwd, roles),
            Err(_) => {
                let mut d = existing.clone();
                d.insert("roles", roles);
                d
            }
        };
        self.propose(Command::PutUser { key: format!("{db}.{user}"), user: doc, create: false }, None).await
    }

    async fn drop_user(&self, db: &str, body: &Document) -> Result<Document> {
        let user = body.get_str("dropUser").map_err(|_| Error::bad_value("dropUser must be a string"))?;
        self.propose(Command::DropUser { key: format!("{db}.{user}") }, None).await
    }

    fn users_info(&self, db: &str, body: &Document) -> Result<Document> {
        let snap = self.store.snapshot()?;
        let all = snap.users()?;
        let want: Box<dyn Fn(&Document) -> bool> = match body.get("usersInfo") {
            Some(Bson::String(u)) => {
                let u = u.clone();
                let db = db.to_string();
                Box::new(move |d| d.get_str("user").ok() == Some(u.as_str()) && d.get_str("db").ok() == Some(db.as_str()))
            }
            Some(Bson::Document(q)) => {
                let (u, d2) = (q.get_str("user").unwrap_or("").to_string(), q.get_str("db").unwrap_or(db).to_string());
                Box::new(move |d| d.get_str("user").ok() == Some(u.as_str()) && d.get_str("db").ok() == Some(d2.as_str()))
            }
            _ => {
                let db = db.to_string();
                Box::new(move |d| d.get_str("db").ok() == Some(db.as_str()))
            }
        };
        let users: Vec<Document> = all
            .iter()
            .filter(|d| want(d))
            .map(|d| {
                doc! {
                    "_id": d.get("_id").cloned().unwrap_or(Bson::Null),
                    "user": d.get("user").cloned().unwrap_or(Bson::Null),
                    "db": d.get("db").cloned().unwrap_or(Bson::Null),
                    "roles": d.get("roles").cloned().unwrap_or(Bson::Array(vec![])),
                    "mechanisms": [auth::MECHANISM],
                }
            })
            .collect();
        Ok(doc! {"users": users, "ok": 1.0})
    }

    // ---------- writes ----------

    pub async fn propose(&self, cmd: Command, session: Option<SessionTxn>) -> Result<Document> {
        let p = Proposal { cmd, now_millis: now_millis(), session };
        self.raft.propose(&p, self.cfg.forward_writes).await
    }

    // ---------- background tasks ----------

    fn spawn_background_tasks(self: Arc<Self>) {
        // Idle cursor reaper.
        let s = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let mut c = s.cursors.lock().await;
                c.retain(|_, cur| cur.last_used.elapsed() < CURSOR_TIMEOUT);
            }
        });
        // TTL expiry, driven by the leader.
        let s = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(20)).await;
                if !s.raft.is_leader() {
                    continue;
                }
                if let Err(e) = s.expire_ttl().await {
                    tracing::warn!(error = %e, "TTL pass failed");
                }
            }
        });
        // Bootstrap the root user.
        if let Some((user, pwd)) = self.cfg.root_user.clone() {
            let s = self.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let has_users = match s.store.snapshot().and_then(|snap| snap.users()) {
                        Ok(u) => !u.is_empty(),
                        Err(_) => continue,
                    };
                    if has_users {
                        return;
                    }
                    if !s.raft.is_leader() {
                        continue;
                    }
                    let doc = auth::user_doc("admin", &user, &pwd, Bson::Array(vec![Bson::Document(doc! {"role": "root", "db": "admin"})]));
                    match s.propose(Command::PutUser { key: format!("admin.{user}"), user: doc, create: true }, None).await {
                        Ok(_) => {
                            tracing::info!(%user, "created root user");
                            return;
                        }
                        Err(e) => tracing::debug!(error = %e, "root user bootstrap will retry"),
                    }
                }
            });
        }
    }

    async fn expire_ttl(&self) -> Result<()> {
        let snap = self.store.snapshot()?;
        let now = now_millis();
        for (ns, meta) in snap.namespaces()? {
            for ix in &meta.indexes {
                let Some(secs) = ix.expire_after_seconds else { continue };
                let Some(field) = ix.key.keys().next() else { continue };
                let cutoff = now - secs.saturating_mul(1000);
                let probe = crate::query::Matcher::parse(&doc! {field.clone(): {"$lt": DateTime::from_millis(cutoff)}})?;
                if snap.find(&ns, &probe, &crate::storage::ScanOpts { limit: Some(1) })?.is_empty() {
                    continue;
                }
                let r = self.propose(Command::Expire { ns: ns.clone(), field: field.clone(), cutoff_millis: cutoff }, None).await?;
                tracing::debug!(%ns, removed = ?r.get("n"), "TTL expiry");
            }
        }
        Ok(())
    }
}

fn sasl_payload(body: &Document) -> Result<Vec<u8>> {
    match body.get("payload") {
        Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
        Some(Bson::String(s)) => Ok(s.as_bytes().to_vec()),
        _ => Err(Error::bad_value("SASL payload must be binary")),
    }
}

fn has_out_stage(body: &Document) -> bool {
    matches!(body.get("pipeline"), Some(Bson::Array(p)) if p.iter().any(|s| matches!(s, Bson::Document(d) if d.contains_key("$out") || d.contains_key("$merge"))))
}

pub fn build_info() -> Document {
    doc! {
        "version": COMPAT_VERSION,
        "gitVersion": "mango",
        "versionArray": [6, 0, 0, 0],
        "modules": [],
        "allocator": "system",
        "javascriptEngine": "none",
        "sysInfo": "deprecated",
        "bits": 64,
        "debug": false,
        "maxBsonObjectSize": MAX_BSON_SIZE,
        "storageEngines": ["mango"],
        "mango": {"version": env!("CARGO_PKG_VERSION")},
        "ok": 1.0
    }
}

fn get_parameter(body: &Document) -> Document {
    let mut d = Document::new();
    let all = matches!(body.get("getParameter"), Some(Bson::String(s)) if s == "*");
    if all || body.contains_key("featureCompatibilityVersion") {
        d.insert("featureCompatibilityVersion", doc! {"version": "6.0"});
    }
    if all || body.contains_key("authenticationMechanisms") {
        d.insert("authenticationMechanisms", vec![auth::MECHANISM]);
    }
    d.insert("ok", 1.0);
    d
}

fn list_commands() -> Document {
    let mut cmds = Document::new();
    for c in [
        "aggregate",
        "buildInfo",
        "collStats",
        "connectionStatus",
        "count",
        "create",
        "createIndexes",
        "createUser",
        "dbStats",
        "delete",
        "distinct",
        "drop",
        "dropDatabase",
        "dropIndexes",
        "dropUser",
        "endSessions",
        "explain",
        "find",
        "findAndModify",
        "getMore",
        "hello",
        "insert",
        "killCursors",
        "listCollections",
        "listDatabases",
        "listIndexes",
        "mangoStatus",
        "ping",
        "renameCollection",
        "replSetGetStatus",
        "saslContinue",
        "saslStart",
        "serverStatus",
        "update",
        "updateUser",
        "usersInfo",
    ] {
        cmds.insert(c, doc! {"help": "", "adminOnly": false});
    }
    doc! {"commands": cmds, "ok": 1.0}
}

/// Extracts retryable-write session info (`lsid` + `txnNumber`).
pub fn session_of(body: &Document) -> Option<SessionTxn> {
    let lsid = body.get("lsid")?.clone();
    let txn_number = match body.get("txnNumber")? {
        Bson::Int64(n) => *n,
        Bson::Int32(n) => *n as i64,
        _ => return None,
    };
    Some(SessionTxn { lsid, txn_number })
}

pub fn int_field(body: &Document, k: &str) -> Result<Option<i64>> {
    get_int(body, k)
}
