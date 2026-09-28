//! Wiring: opens storage, starts Raft and the client listener.

use crate::error::{Error, Result};
use crate::raft::{self, Member, RaftConfig, RaftHandle};
use crate::server::{Server, ServerConfig};
use crate::storage::Store;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: u64,
    pub data_dir: PathBuf,
    pub bind: String,
    pub advertise: String,
    /// All members, including this node. Empty means a single-node cluster.
    pub members: Vec<Member>,
    pub cluster_name: String,
    pub cluster_key: Option<Vec<u8>>,
    pub repl_set_name: Option<String>,
    pub report_standalone: bool,
    pub forward_writes: bool,
    pub auth: bool,
    pub root_user: Option<(String, String)>,
    pub heartbeat: Duration,
    pub election_timeout: Duration,
    pub compact_threshold: u64,
    pub keep_entries: u64,
}

impl Config {
    pub fn single_node(data_dir: PathBuf, bind: &str) -> Config {
        Config {
            node_id: 1,
            data_dir,
            bind: bind.to_string(),
            advertise: bind.to_string(),
            members: vec![],
            cluster_name: "mango".into(),
            cluster_key: None,
            repl_set_name: None,
            report_standalone: true,
            forward_writes: true,
            auth: false,
            root_user: None,
            heartbeat: Duration::from_millis(100),
            election_timeout: Duration::from_millis(1000),
            compact_threshold: 20_000,
            keep_entries: 5_000,
        }
    }
}

pub struct Running {
    pub addr: SocketAddr,
    pub server: Arc<Server>,
    pub raft: RaftHandle,
}

pub async fn start(cfg: Config) -> Result<Running> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let store = Arc::new(Store::open(&cfg.data_dir.join("mango.redb"))?);
    let listener =
        tokio::net::TcpListener::bind(&cfg.bind).await.map_err(|e| Error::internal(format!("cannot listen on {}: {e}", cfg.bind)))?;
    let addr = listener.local_addr()?;
    let advertise = if cfg.advertise.ends_with(":0") { addr.to_string() } else { cfg.advertise.clone() };
    let members = if cfg.members.is_empty() {
        vec![Member { id: cfg.node_id, raft_addr: String::new(), client_addr: advertise.clone() }]
    } else {
        cfg.members.clone()
    };
    if !members.iter().any(|m| m.id == cfg.node_id) {
        return Err(Error::bad_value(format!("node id {} is not in the member list", cfg.node_id)));
    }
    let multi = members.len() > 1;
    let rcfg = RaftConfig {
        id: cfg.node_id,
        members,
        cluster: cfg.cluster_name.clone(),
        key: cfg.cluster_key.clone(),
        heartbeat: cfg.heartbeat,
        election_min: cfg.election_timeout,
        election_max: cfg.election_timeout * 2,
        rpc_timeout: (cfg.election_timeout).max(Duration::from_millis(500)),
        data_dir: cfg.data_dir.clone(),
        compact_threshold: cfg.compact_threshold,
        keep_entries: cfg.keep_entries,
        max_append_bytes: 4 << 20,
    };
    let raft = raft::start(rcfg, store.clone()).await?;
    let repl_set_name =
        if cfg.report_standalone || !multi { None } else { Some(cfg.repl_set_name.clone().unwrap_or_else(|| cfg.cluster_name.clone())) };
    let scfg = ServerConfig {
        bind: cfg.bind.clone(),
        advertise,
        repl_set_name,
        auth: cfg.auth,
        forward_writes: cfg.forward_writes,
        root_user: cfg.root_user.clone(),
    };
    let server = Server::new(store, raft.clone(), scfg);
    tokio::spawn(server.clone().serve(listener));
    tracing::info!(%addr, node = cfg.node_id, "mango is accepting connections");
    Ok(Running { addr, server, raft })
}
