use clap::Parser;
use mango::app::{self, Config};
use mango::raft::parse_members;
use std::path::PathBuf;
use std::time::Duration;

/// 🥭 Mango: a MongoDB-compatible document database with built-in Raft replication.
///
/// Plant one node for a quick taste, or an orchard of three or five across
/// availability zones: the harvest survives losing any minority of trees.
#[derive(Parser, Debug)]
#[command(version, about, long_about)]
struct Args {
    /// Directory for the database file and snapshots.
    #[arg(long, env = "MANGO_DATA_DIR", default_value = "./mango-data")]
    data_dir: PathBuf,

    /// Address to accept MongoDB client connections on.
    #[arg(long, env = "MANGO_BIND", default_value = "0.0.0.0:27017")]
    bind: String,

    /// Address clients should use to reach this node (reported to drivers).
    /// Defaults to <hostname>:<bind port>.
    #[arg(long, env = "MANGO_ADVERTISE")]
    advertise: Option<String>,

    /// This node's id (a positive integer that appears in --members).
    #[arg(long, env = "MANGO_NODE_ID")]
    node_id: Option<u64>,

    /// Derive the node id from a StatefulSet-style hostname ("mango-2" => 3).
    #[arg(long, env = "MANGO_NODE_ID_FROM_HOSTNAME", default_value_t = false)]
    node_id_from_hostname: bool,

    /// Cluster members: "1=raft_host:port/client_host:port,2=...". Omit for a single node.
    #[arg(long, env = "MANGO_MEMBERS", default_value = "")]
    members: String,

    /// Cluster name; nodes refuse to talk to other clusters.
    #[arg(long, env = "MANGO_CLUSTER_NAME", default_value = "mango")]
    cluster_name: String,

    /// File holding the shared secret nodes use to authenticate each other.
    #[arg(long, env = "MANGO_CLUSTER_KEY_FILE", conflicts_with = "cluster_key")]
    cluster_key_file: Option<PathBuf>,

    /// The shared cluster secret itself (prefer --cluster-key-file).
    #[arg(long, env = "MANGO_CLUSTER_KEY", hide_env_values = true)]
    cluster_key: Option<String>,

    /// Replica set name reported to drivers (defaults to the cluster name).
    #[arg(long, env = "MANGO_REPL_SET_NAME")]
    repl_set_name: Option<String>,

    /// Report every node as a standalone writable server (for deployments
    /// behind a load balancer); followers forward writes to the leader.
    #[arg(long, env = "MANGO_STANDALONE", default_value_t = false)]
    standalone: bool,

    /// Reject writes on followers instead of forwarding them to the leader.
    #[arg(long, env = "MANGO_NO_FORWARD_WRITES", default_value_t = false)]
    no_forward_writes: bool,

    /// Require SCRAM-SHA-256 authentication.
    #[arg(long, env = "MANGO_AUTH", default_value_t = false)]
    auth: bool,

    /// Root user created on first start (with --auth).
    #[arg(long, env = "MANGO_ROOT_USER")]
    root_user: Option<String>,

    /// Password for the root user.
    #[arg(long, env = "MANGO_ROOT_PASSWORD", hide_env_values = true)]
    root_password: Option<String>,

    /// Leader heartbeat interval.
    #[arg(long, env = "MANGO_HEARTBEAT_MS", default_value_t = 150)]
    heartbeat_ms: u64,

    /// Minimum election timeout (the maximum is twice this).
    #[arg(long, env = "MANGO_ELECTION_TIMEOUT_MS", default_value_t = 1500)]
    election_timeout_ms: u64,

    /// Compact the Raft log once it holds this many applied entries.
    #[arg(long, env = "MANGO_COMPACT_THRESHOLD", default_value_t = 20_000, hide = true)]
    compact_threshold: u64,

    /// Entries kept after compaction for followers that are slightly behind.
    #[arg(long, env = "MANGO_KEEP_ENTRIES", default_value_t = 5_000, hide = true)]
    keep_entries: u64,

    /// Skip the startup banner.
    #[arg(long, env = "MANGO_NO_BANNER", default_value_t = false)]
    no_banner: bool,

    /// Log level filter (e.g. "info", "mango=debug").
    #[arg(long, env = "MANGO_LOG", default_value = "info")]
    log: String,
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok().map(|s| s.trim().to_string()))
        .unwrap_or_else(|| "localhost".into())
}

const MANGO_ART: &str = r#"
          \ //
           \/
        .-'``'-.
      .'   ___  '.
     /   .'   '.  \
    |   /  ~~   \  |
    |   \  ~~   /  |
     \   '.___.'  /
      '.        .'
        '-.__.-'"#;

fn banner(cfg: &Config) -> String {
    let trees = cfg.members.len().max(1);
    let orchard = if trees == 1 {
        "a single tree (no replication)".to_string()
    } else {
        format!("tree {} of an orchard of {trees} (survives losing {})", cfg.node_id, (trees - 1) / 2)
    };
    format!(
        "{MANGO_ART}\n   mango {} · ripe, juicy, MongoDB-compatible\n   planting {orchard}\n   clients: {} · data: {}\n",
        env!("CARGO_PKG_VERSION"),
        cfg.advertise,
        cfg.data_dir.display()
    )
}

fn main() {
    let args = Args::parse();
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_env_filter(tracing_subscriber::EnvFilter::new(&args.log)).init();
    if let Err(e) = real_main(args) {
        eprintln!("mango: {e}");
        std::process::exit(1);
    }
}

fn real_main(args: Args) -> Result<(), String> {
    let members = parse_members(&args.members).map_err(|e| e.msg)?;
    let node_id = match (args.node_id, args.node_id_from_hostname) {
        (Some(id), _) => id,
        (None, true) => {
            let h = hostname();
            let ordinal: u64 = h
                .split('.')
                .next()
                .and_then(|s| s.rsplit('-').next())
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| format!("cannot derive a node id from hostname '{h}'"))?;
            ordinal + 1
        }
        (None, false) => {
            if members.len() > 1 {
                return Err("--node-id (or --node-id-from-hostname) is required with --members".into());
            }
            members.first().map(|m| m.id).unwrap_or(1)
        }
    };
    let port = args.bind.rsplit_once(':').map(|(_, p)| p.to_string()).unwrap_or_else(|| "27017".into());
    let advertise = args
        .advertise
        .clone()
        .or_else(|| members.iter().find(|m| m.id == node_id).map(|m| m.client_addr.clone()))
        .unwrap_or_else(|| format!("{}:{port}", hostname()));
    let cluster_key = match (&args.cluster_key_file, &args.cluster_key) {
        (Some(p), _) => {
            let k = std::fs::read(p).map_err(|e| format!("cannot read cluster key file {}: {e}", p.display()))?;
            Some(String::from_utf8_lossy(&k).trim().as_bytes().to_vec())
        }
        (None, Some(k)) => Some(k.trim().as_bytes().to_vec()),
        (None, None) => None,
    };
    if cluster_key.as_ref().is_some_and(|k| k.len() < 16) {
        return Err("the cluster key must be at least 16 bytes".into());
    }
    if members.len() > 1 && cluster_key.is_none() {
        tracing::warn!("no --cluster-key-file: peers are not authenticated; only use this on a trusted network");
    }
    let root_user = match (args.root_user.clone(), args.root_password.clone()) {
        (Some(u), Some(p)) => Some((u, p)),
        (None, None) => None,
        _ => return Err("--root-user and --root-password must be given together".into()),
    };
    if args.auth && root_user.is_none() {
        tracing::warn!("--auth without --root-user: create the first user from localhost");
    }
    let cfg = Config {
        node_id,
        data_dir: args.data_dir,
        bind: args.bind,
        advertise,
        members,
        cluster_name: args.cluster_name,
        cluster_key,
        repl_set_name: args.repl_set_name,
        report_standalone: args.standalone,
        forward_writes: !args.no_forward_writes,
        auth: args.auth,
        root_user,
        heartbeat: Duration::from_millis(args.heartbeat_ms),
        election_timeout: Duration::from_millis(args.election_timeout_ms),
        compact_threshold: args.compact_threshold,
        keep_entries: args.keep_entries,
    };
    if cfg.heartbeat * 3 > cfg.election_timeout {
        return Err("the election timeout must be at least 3x the heartbeat interval".into());
    }
    if !args.no_banner {
        eprintln!("{}", banner(&cfg));
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    rt.block_on(async move {
        app::start(cfg).await.map_err(|e| e.to_string())?;
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|e| e.to_string())?;
        tokio::select! {
            r = tokio::signal::ctrl_c() => r.map_err(|e| e.to_string())?,
            _ = term.recv() => {}
        }
        tracing::info!("🧺 shutting down; the harvest is safely stored");
        Ok(())
    })
}
