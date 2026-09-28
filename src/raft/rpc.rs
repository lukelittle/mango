//! Raft peer-to-peer messages and their transport: length-prefixed BSON
//! frames over TCP, multiplexed by request id, with a mutual HMAC
//! challenge-response handshake when a cluster key is configured.

use super::log::Entry;
use crate::error::Error;
use bson::Document;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, mpsc, oneshot};

pub const MAX_FRAME: usize = 64 * 1024 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Vote {
        term: u64,
        candidate: u64,
        last_index: u64,
        last_term: u64,
        pre_vote: bool,
    },
    Append {
        term: u64,
        leader: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
    },
    Snapshot {
        term: u64,
        leader: u64,
        last_index: u64,
        last_term: u64,
        offset: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        done: bool,
    },
    /// A client write forwarded by a follower to the leader.
    Forward {
        #[serde(with = "serde_bytes")]
        proposal: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Vote { term: u64, granted: bool },
    Append { term: u64, success: bool, match_index: u64, hint: u64 },
    Snapshot { term: u64, ok: bool },
    Forward { index: u64, reply: Option<Document>, error: Option<(i32, String)> },
    Error { msg: String },
}

#[derive(Debug, Serialize, Deserialize)]
struct Frame<T> {
    id: u64,
    msg: T,
}

#[derive(Debug, Serialize, Deserialize)]
enum Handshake {
    Hello {
        cluster: String,
        node: u64,
        #[serde(with = "serde_bytes")]
        nonce: Vec<u8>,
    },
    Auth {
        cluster: String,
        node: u64,
        #[serde(with = "serde_bytes")]
        mac: Vec<u8>,
        #[serde(with = "serde_bytes")]
        nonce: Vec<u8>,
    },
    Accept {
        #[serde(with = "serde_bytes")]
        mac: Vec<u8>,
    },
    Reject {
        reason: String,
    },
}

pub async fn write_msg<T: Serialize>(w: &mut (impl AsyncWriteExt + Unpin), msg: &T) -> std::io::Result<()> {
    let bytes = bson::serialize_to_vec(msg).map_err(|e| std::io::Error::other(e.to_string()))?;
    if bytes.len() > MAX_FRAME {
        return Err(std::io::Error::other("frame too large"));
    }
    let mut buf = Vec::with_capacity(bytes.len() + 4);
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bytes);
    w.write_all(&buf).await?;
    w.flush().await
}

pub async fn read_msg<T: DeserializeOwned>(r: &mut (impl AsyncReadExt + Unpin)) -> std::io::Result<T> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::other("frame too large"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    bson::deserialize_from_slice(&buf).map_err(|e| std::io::Error::other(format!("bad frame: {e}")))
}

#[derive(Clone)]
pub struct Security {
    pub cluster: String,
    pub node: u64,
    pub key: Option<Arc<Vec<u8>>>,
}

impl Security {
    fn mac(&self, label: &str, nonce: &[u8], node: u64) -> Vec<u8> {
        let Some(key) = &self.key else { return Vec::new() };
        let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac accepts any key length");
        m.update(label.as_bytes());
        m.update(self.cluster.as_bytes());
        m.update(nonce);
        m.update(&node.to_be_bytes());
        m.finalize().into_bytes().to_vec()
    }

    fn verify(&self, label: &str, nonce: &[u8], node: u64, mac: &[u8]) -> bool {
        let Some(key) = &self.key else { return true };
        let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac accepts any key length");
        m.update(label.as_bytes());
        m.update(self.cluster.as_bytes());
        m.update(nonce);
        m.update(&node.to_be_bytes());
        m.verify_slice(mac).is_ok()
    }
}

fn nonce() -> Vec<u8> {
    (0..32).map(|_| rand::random::<u8>()).collect()
}

/// Server side of the handshake. Returns the authenticated peer's node id.
pub async fn accept_handshake(stream: &mut TcpStream, sec: &Security) -> std::io::Result<u64> {
    let server_nonce = nonce();
    write_msg(stream, &Handshake::Hello { cluster: sec.cluster.clone(), node: sec.node, nonce: server_nonce.clone() }).await?;
    let msg: Handshake =
        tokio::time::timeout(Duration::from_secs(5), read_msg(stream)).await.map_err(|_| std::io::Error::other("handshake timeout"))??;
    let Handshake::Auth { cluster, node, mac, nonce: client_nonce } = msg else {
        return Err(std::io::Error::other("expected Auth"));
    };
    if cluster != sec.cluster {
        write_msg(stream, &Handshake::Reject { reason: format!("cluster name mismatch: {cluster}") }).await?;
        return Err(std::io::Error::other("cluster name mismatch"));
    }
    if sec.key.is_some() && !sec.verify("client", &server_nonce, node, &mac) {
        write_msg(stream, &Handshake::Reject { reason: "authentication failed".into() }).await?;
        return Err(std::io::Error::other("peer failed authentication"));
    }
    write_msg(stream, &Handshake::Accept { mac: sec.mac("server", &client_nonce, sec.node) }).await?;
    Ok(node)
}

/// Client side of the handshake: verifies the server knows the key too.
pub async fn connect_handshake(stream: &mut TcpStream, sec: &Security, expect_node: u64) -> std::io::Result<()> {
    let msg: Handshake = read_msg(stream).await?;
    let Handshake::Hello { cluster, node, nonce: server_nonce } = msg else {
        return Err(std::io::Error::other("expected Hello"));
    };
    if cluster != sec.cluster {
        return Err(std::io::Error::other(format!("peer belongs to cluster '{cluster}'")));
    }
    if node != expect_node {
        return Err(std::io::Error::other(format!("expected node {expect_node}, reached node {node}")));
    }
    let client_nonce = nonce();
    write_msg(
        stream,
        &Handshake::Auth {
            cluster: sec.cluster.clone(),
            node: sec.node,
            mac: sec.mac("client", &server_nonce, sec.node),
            nonce: client_nonce.clone(),
        },
    )
    .await?;
    match read_msg::<Handshake>(stream).await? {
        Handshake::Accept { mac } => {
            if sec.key.is_some() && !sec.verify("server", &client_nonce, node, &mac) {
                return Err(std::io::Error::other("peer failed to prove the cluster key"));
            }
            Ok(())
        }
        Handshake::Reject { reason } => Err(std::io::Error::other(format!("rejected by peer: {reason}"))),
        _ => Err(std::io::Error::other("unexpected handshake message")),
    }
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Response>>>>;

/// A multiplexed connection to one peer, reconnecting on demand.
pub struct PeerClient {
    tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Response>)>,
}

impl PeerClient {
    pub fn new(addr: String, peer_id: u64, sec: Security) -> PeerClient {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(peer_task(addr, peer_id, sec, rx));
        PeerClient { tx }
    }

    /// Sends a request and waits for the response, failing after `timeout`.
    pub async fn call(&self, req: Request, timeout: Duration) -> Result<Response, Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((req, tx)).map_err(|_| Error::host_unreachable("peer client stopped"))?;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(_)) => Err(Error::host_unreachable("connection to peer lost")),
            Err(_) => Err(Error::network_timeout("peer did not respond in time")),
        }
    }
}

async fn peer_task(addr: String, peer_id: u64, sec: Security, mut rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Response>)>) {
    let mut writer: Option<OwnedWriteHalf> = None;
    let mut alive = Arc::new(AtomicBool::new(false));
    let mut pending: Pending = Arc::new(Mutex::new(HashMap::new()));
    let mut next_id = 1u64;
    let mut backoff_until = tokio::time::Instant::now();
    while let Some((req, reply)) = rx.recv().await {
        if reply.is_closed() {
            continue; // the caller already timed out
        }
        if !alive.load(Ordering::SeqCst) {
            writer = None;
        }
        if writer.is_none() {
            if tokio::time::Instant::now() < backoff_until {
                drop(reply); // fail fast while the peer is known to be down
                continue;
            }
            match connect(&addr, peer_id, &sec).await {
                Ok(stream) => {
                    let (r, w) = stream.into_split();
                    pending = Arc::new(Mutex::new(HashMap::new()));
                    alive = Arc::new(AtomicBool::new(true));
                    tokio::spawn(reader_task(r, pending.clone(), alive.clone()));
                    writer = Some(w);
                }
                Err(e) => {
                    tracing::debug!(peer = peer_id, %addr, error = %e, "cannot connect to peer");
                    backoff_until = tokio::time::Instant::now() + Duration::from_millis(250);
                    drop(reply);
                    continue;
                }
            }
        }
        let id = next_id;
        next_id += 1;
        pending.lock().await.insert(id, reply);
        let w = writer.as_mut().unwrap();
        // A peer behind a blackholed link can stop draining its socket; don't
        // let one write block this connection forever.
        let written = tokio::time::timeout(WRITE_TIMEOUT, write_msg(w, &Frame { id, msg: req }))
            .await
            .unwrap_or_else(|_| Err(std::io::Error::other("write timed out")));
        if let Err(e) = written {
            tracing::debug!(peer = peer_id, error = %e, "peer write failed");
            writer = None;
            alive.store(false, Ordering::SeqCst);
            pending.lock().await.clear();
        }
    }
}

async fn connect(addr: &str, peer_id: u64, sec: &Security) -> std::io::Result<TcpStream> {
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::other("connect timeout"))??;
    stream.set_nodelay(true)?;
    tokio::time::timeout(Duration::from_secs(5), connect_handshake(&mut stream, sec, peer_id))
        .await
        .map_err(|_| std::io::Error::other("handshake timeout"))??;
    Ok(stream)
}

async fn reader_task(mut r: OwnedReadHalf, pending: Pending, alive: Arc<AtomicBool>) {
    loop {
        match read_msg::<Frame<Response>>(&mut r).await {
            Ok(f) => {
                if let Some(tx) = pending.lock().await.remove(&f.id) {
                    let _ = tx.send(f.msg);
                }
            }
            Err(_) => {
                // Dropping the senders fails every in-flight call.
                alive.store(false, Ordering::SeqCst);
                pending.lock().await.clear();
                return;
            }
        }
    }
}

/// Serves one inbound peer connection: each request is handled concurrently
/// and responses are written back tagged with the request id.
pub async fn serve_peer<F, Fut>(stream: TcpStream, sec: Security, handler: F)
where
    F: Fn(u64, Request) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = Response> + Send + 'static,
{
    let mut stream = stream;
    let _ = stream.set_nodelay(true);
    let peer = match accept_handshake(&mut stream, &sec).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "rejected peer connection");
            return;
        }
    };
    let (mut r, w) = stream.into_split();
    let w = Arc::new(Mutex::new(w));
    loop {
        let frame: Frame<Request> = match read_msg(&mut r).await {
            Ok(f) => f,
            Err(_) => return,
        };
        let h = handler.clone();
        let w = w.clone();
        tokio::spawn(async move {
            let resp = h(peer, frame.msg).await;
            let mut w = w.lock().await;
            let _ = write_msg(&mut *w, &Frame { id: frame.id, msg: resp }).await;
        });
    }
}
