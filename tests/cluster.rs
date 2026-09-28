//! Multi-node tests: three `mango` processes on localhost, driven with the
//! official MongoDB driver. Nodes are killed with SIGKILL to simulate losing
//! an availability zone.

use futures_util::TryStreamExt;
use mongodb::Client;
use mongodb::bson::{Document, doc};
use mongodb::options::ClientOptions;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Node {
    id: u64,
    client_port: u16,
    data_dir: PathBuf,
    child: Option<Child>,
}

struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<Node>,
    members: String,
    key_file: PathBuf,
    extra: Vec<String>,
}

impl Cluster {
    fn new(n: u64, extra: &[&str]) -> Cluster {
        let dir = tempfile::tempdir().unwrap();
        let key_file = dir.path().join("cluster.key");
        std::fs::write(&key_file, "test-cluster-key-0123456789abcdef").unwrap();
        let mut nodes = Vec::new();
        let mut members = Vec::new();
        for id in 1..=n {
            let (raft, client) = (free_port(), free_port());
            members.push(format!("{id}=127.0.0.1:{raft}/127.0.0.1:{client}"));
            nodes.push(Node { id, client_port: client, data_dir: dir.path().join(format!("n{id}")), child: None });
        }
        let mut c =
            Cluster { _dir: dir, nodes, members: members.join(","), key_file, extra: extra.iter().map(|s| s.to_string()).collect() };
        for i in 0..c.nodes.len() {
            c.start(i);
        }
        c
    }

    fn start(&mut self, i: usize) {
        let node = &mut self.nodes[i];
        let log = std::fs::File::create(node.data_dir.with_extension("log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_mango"))
            .args(["--node-id", &node.id.to_string()])
            .args(["--data-dir", node.data_dir.to_str().unwrap()])
            .args(["--bind", &format!("127.0.0.1:{}", node.client_port)])
            .args(["--members", &self.members])
            .args(["--cluster-key-file", self.key_file.to_str().unwrap()])
            .args(["--heartbeat-ms", "50", "--election-timeout-ms", "400", "--log", "mango=debug"])
            .args(&self.extra)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        node.child = Some(child);
    }

    fn kill(&mut self, i: usize) {
        if let Some(mut c) = self.nodes[i].child.take() {
            c.kill().unwrap();
            c.wait().unwrap();
        }
    }

    fn uri(&self) -> String {
        let hosts: Vec<String> = self.nodes.iter().map(|n| format!("127.0.0.1:{}", n.client_port)).collect();
        format!("mongodb://{}/?replicaSet=mango&serverSelectionTimeoutMS=20000", hosts.join(","))
    }

    async fn direct(&self, i: usize) -> Client {
        let uri = format!("mongodb://127.0.0.1:{}/?directConnection=true&serverSelectionTimeoutMS=5000", self.nodes[i].client_port);
        Client::with_uri_str(uri).await.unwrap()
    }

    /// Index of the current leader, waiting for one to be elected.
    async fn leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            for (i, n) in self.nodes.iter().enumerate() {
                if n.child.is_none() {
                    continue;
                }
                let c = self.direct(i).await;
                if let Ok(st) = c.database("admin").run_command(doc! {"mangoStatus": 1}).await
                    && st.get_str("role") == Ok("leader")
                {
                    return i;
                }
            }
            assert!(Instant::now() < deadline, "no leader elected");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn applied(&self, i: usize) -> i64 {
        let st = self.direct(i).await.database("admin").run_command(doc! {"mangoStatus": 1}).await.unwrap();
        st.get_i64("appliedIndex").unwrap()
    }

    /// Waits until every running node has applied the same index.
    async fn converge(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut vals = Vec::new();
            for (i, n) in self.nodes.iter().enumerate() {
                if n.child.is_some() {
                    vals.push(self.applied(i).await);
                }
            }
            if vals.windows(2).all(|w| w[0] == w[1]) {
                return;
            }
            assert!(Instant::now() < deadline, "nodes did not converge: {vals:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn contents(&self, i: usize, coll: &str) -> Vec<Document> {
        let c = self.direct(i).await;
        c.database("test").collection::<Document>(coll).find(doc! {}).sort(doc! {"_id": 1}).await.unwrap().try_collect().await.unwrap()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..self.nodes.len() {
            self.kill(i);
        }
    }
}

async fn replset_client(c: &Cluster) -> Client {
    let mut opts = ClientOptions::parse(c.uri()).await.unwrap();
    opts.retry_writes = Some(true);
    Client::with_options(opts).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn replicates_and_survives_leader_loss() {
    let mut cluster = Cluster::new(3, &[]);
    let leader = cluster.leader().await;
    let client = replset_client(&cluster).await;
    let coll = client.database("test").collection::<Document>("kv");
    for i in 0..50 {
        coll.insert_one(doc! {"_id": i, "v": i}).await.unwrap();
    }
    cluster.converge().await;
    let expected = cluster.contents(leader, "kv").await;
    assert_eq!(expected.len(), 50);
    for i in 0..3 {
        assert_eq!(cluster.contents(i, "kv").await, expected, "node {i} differs");
    }

    // Lose the leader (e.g. its availability zone goes down).
    cluster.kill(leader);
    let start = Instant::now();
    for i in 50..100 {
        coll.insert_one(doc! {"_id": i, "v": i}).await.unwrap();
    }
    eprintln!("50 writes across failover took {:?}", start.elapsed());
    let new_leader = cluster.leader().await;
    assert_ne!(new_leader, leader);
    assert_eq!(coll.count_documents(doc! {}).await.unwrap(), 100);

    // The old leader rejoins and catches up.
    cluster.start(leader);
    cluster.converge().await;
    let expected = cluster.contents(new_leader, "kv").await;
    assert_eq!(expected.len(), 100);
    for i in 0..3 {
        assert_eq!(cluster.contents(i, "kv").await, expected, "node {i} differs after rejoin");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn followers_forward_writes_and_read_their_writes() {
    let cluster = Cluster::new(3, &[]);
    let leader = cluster.leader().await;
    let follower = (leader + 1) % 3;
    let c = cluster.direct(follower).await;
    let coll = c.database("test").collection::<Document>("fwd");
    coll.insert_one(doc! {"_id": 1, "n": 0}).await.unwrap();
    coll.update_one(doc! {"_id": 1}, doc! {"$inc": {"n": 5}}).await.unwrap();
    // Read-your-writes on the follower that forwarded them.
    let d = coll.find_one(doc! {"_id": 1}).await.unwrap().unwrap();
    assert_eq!(d.get_i32("n").unwrap(), 5);
    let hello = c.database("admin").run_command(doc! {"hello": 1}).await.unwrap();
    assert!(!hello.get_bool("isWritablePrimary").unwrap());
    assert_eq!(hello.get_str("setName").unwrap(), "mango");
    assert_eq!(hello.get_array("hosts").unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn minority_cannot_write() {
    let mut cluster = Cluster::new(3, &[]);
    let leader = cluster.leader().await;
    let c = cluster.direct(leader).await;
    let coll = c.database("test").collection::<Document>("q");
    coll.insert_one(doc! {"_id": 1}).await.unwrap();
    // Take down both followers: the leader must step down rather than accept
    // writes it cannot replicate.
    for i in 0..3 {
        if i != leader {
            cluster.kill(i);
        }
    }
    let r = tokio::time::timeout(Duration::from_secs(20), coll.insert_one(doc! {"_id": 2})).await;
    if let Ok(Ok(_)) = r {
        panic!("a minority accepted a write")
    }
    // Committed data is still readable on the survivor.
    assert_eq!(coll.count_documents(doc! {"_id": 1}).await.unwrap(), 1);
    let st = c.database("admin").run_command(doc! {"mangoStatus": 1}).await.unwrap();
    assert_ne!(st.get_str("role").unwrap(), "leader");
}

#[tokio::test(flavor = "multi_thread")]
async fn lagging_node_catches_up_by_snapshot() {
    let mut cluster = Cluster::new(3, &["--compact-threshold", "50", "--keep-entries", "10"]);
    let leader = cluster.leader().await;
    let lagger = (leader + 1) % 3;
    cluster.kill(lagger);
    let client = replset_client(&cluster).await;
    let coll = client.database("test").collection::<Document>("snap");
    coll.create_index(mongodb::IndexModel::builder().keys(doc! {"k": 1}).build()).await.unwrap();
    for i in 0..300 {
        coll.insert_one(doc! {"_id": i, "k": i % 10}).await.unwrap();
    }
    cluster.start(lagger);
    cluster.converge().await;
    let expected = cluster.contents(leader, "snap").await;
    assert_eq!(expected.len(), 300);
    assert_eq!(cluster.contents(lagger, "snap").await, expected);
    // Indexes came across too.
    let lc = cluster.direct(lagger).await;
    let names = lc.database("test").collection::<Document>("snap").list_index_names().await.unwrap();
    assert!(names.contains(&"k_1".to_string()));
    let n = lc.database("test").collection::<Document>("snap").count_documents(doc! {"k": 3}).await.unwrap();
    assert_eq!(n, 30);
    let log = std::fs::read_to_string(cluster.nodes[lagger].data_dir.with_extension("log")).unwrap();
    assert!(log.contains("installing snapshot"), "expected the lagging node to install a snapshot");
}

/// Randomly kills and restarts nodes (never more than one down at a time,
/// like losing one AZ) while a client writes. Every acknowledged write must
/// survive, retried increments must apply at most once, and all replicas
/// must end up identical.
#[tokio::test(flavor = "multi_thread")]
async fn chaos_kill_restart_keeps_acknowledged_writes() {
    let rounds: usize = std::env::var("MANGO_CHAOS_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(6);
    let mut cluster = Cluster::new(3, &["--compact-threshold", "200", "--keep-entries", "50"]);
    cluster.leader().await;
    let client = replset_client(&cluster).await;
    let coll = client.database("test").collection::<Document>("chaos");
    coll.insert_one(doc! {"_id": "counter", "n": 0i64}).await.unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let coll = coll.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let (mut acked_inserts, mut acked_incs, mut attempted_incs) = (Vec::new(), 0i64, 0i64);
            let mut i = 0i64;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                if coll.insert_one(doc! {"_id": i, "v": i}).await.is_ok() {
                    acked_inserts.push(i);
                }
                attempted_incs += 1;
                if coll.update_one(doc! {"_id": "counter"}, doc! {"$inc": {"n": 1i64}}).await.is_ok() {
                    acked_incs += 1;
                }
                i += 1;
            }
            (acked_inserts, acked_incs, attempted_incs)
        })
    };
    for round in 0..rounds {
        tokio::time::sleep(Duration::from_millis(700)).await;
        let victim = rand::random::<u64>() as usize % 3;
        eprintln!("chaos round {round}: killing node {}", victim + 1);
        cluster.kill(victim);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        cluster.start(victim);
    }
    tokio::time::sleep(Duration::from_millis(1000)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let (acked_inserts, acked_incs, attempted_incs) = writer.await.unwrap();
    eprintln!("acked {} inserts, {acked_incs}/{attempted_incs} increments", acked_inserts.len());
    assert!(acked_inserts.len() > 20, "the cluster made too little progress");

    cluster.converge().await;
    let leader = cluster.leader().await;
    let data = cluster.contents(leader, "chaos").await;
    let present: std::collections::HashSet<i64> =
        data.iter().filter_map(|d| d.get("_id").and_then(|v| v.as_i64().or(v.as_i32().map(i64::from)))).collect();
    for id in &acked_inserts {
        assert!(present.contains(id), "acknowledged insert {id} was lost");
    }
    let counter = data.iter().find(|d| d.get_str("_id") == Ok("counter")).unwrap().get_i64("n").unwrap();
    assert!(counter >= acked_incs && counter <= attempted_incs, "counter {counter} outside [{acked_incs}, {attempted_incs}]");
    for i in 0..3 {
        assert_eq!(cluster.contents(i, "chaos").await, data, "replica {} diverged", i + 1);
    }
}
