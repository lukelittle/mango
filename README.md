# Mango 🥭

Mango is a MongoDB-compatible document database written in Rust, with replication built in. It speaks the MongoDB wire protocol, so existing drivers and tools (the official Rust and Python drivers are tested in CI) connect to it unchanged. Every write goes through [Raft](https://raft.github.io/) consensus, so a cluster spread across three availability zones keeps serving reads and writes when a whole zone goes down, and it never loses a write it has acknowledged.

```sh
cargo run --release -- --data-dir ./data            # single node on :27017
mongosh "mongodb://localhost:27017"                  # or any MongoDB driver
```

## Where Mango improves on MongoDB

These are design choices. The tests in `tests/` exercise each one.

| | MongoDB | Mango |
|---|---|---|
| **Acknowledged writes** | Durable across failover only with `w: "majority"`. `w: 1` writes can be rolled back. | Every write is acknowledged only after a majority of nodes has made it durable, whatever `writeConcern` the client sends. |
| **Reads** | The default `local` read concern can return data that is later rolled back. | Nodes apply only committed log entries, so no read on any node returns data that could be rolled back. `readConcern: "linearizable"` is also supported. |
| **Writing to any node** | Secondaries reject writes (`NotWritablePrimary`). | Followers forward writes to the leader. They wait (up to 5 s) to apply the write locally before replying, so reads on the same node see it. Mango works behind a plain TCP load balancer or with `directConnection=true` to any node. |
| **Retried writes** | Supported, with dedup state kept per replica set member. | The dedup record for a retried write is part of the replicated state, so a write retried across a failover is applied once. The chaos test checks this with counters. |
| **Operations** | `mongod` plus a replica-set config document (`rs.initiate`) plus a keyfile. | A single binary (a ~50 MB container image). Members are configured with a flag or env var, and a cluster elects its first leader by itself. |
| **Auth** | SCRAM-SHA-1 (which first hashes the password with MD5) and SCRAM-SHA-256. | SCRAM-SHA-256 only. Nodes authenticate each other with mutual HMAC-SHA256 challenge-response over a shared cluster key. |

Consensus includes **pre-vote** and **leader stickiness**, so a node rejoining after a partition cannot disrupt a healthy leader. It also includes **check-quorum**: a leader that loses contact with the majority steps down within two election timeouts and stops accepting writes it could not commit. **Snapshots** bring nodes that fell far behind, or that lost their disk, back up to date.

## Measured behaviour

From the test suite, running three `mango` processes on localhost with a 400 ms election timeout and the official MongoDB Rust driver:

* **Leader loss:** after `SIGKILL` of the leader, 50 consecutive writes complete in about 1–1.5 s in total, including the election. The driver sees no errors.
* **Chaos:** 50 rounds of killing and restarting a random node during a continuous insert-and-`$inc` workload, with about 5,700 writes. No acknowledged insert was lost, no increment was applied twice, and all three replicas ended with identical contents.
* **Minority:** with two of three nodes down, the survivor steps down and refuses writes. Committed data stays readable.

With default settings in Docker Compose, PyMongo wrote 100 documents in 2.2 s straight after the primary container was killed. That time includes detecting the failure and the election.

Also tested: the official Rust driver, PyMongo, and `mongosh`.

## Deploying across availability zones

A three-node cluster survives the loss of any one zone. A five-node cluster survives two.

### Kubernetes (EKS, GKE, AKS, …)

[`deploy/kubernetes/mango.yaml`](deploy/kubernetes/mango.yaml) contains a StatefulSet that places **one pod per zone** (`topologySpreadConstraints` on `topology.kubernetes.io/zone` with `DoNotSchedule`). It also contains a PodDisruptionBudget that allows only one pod down during drains and upgrades, zonal volumes, a headless service giving each pod a stable name, and a load-balanced client service.

```sh
kubectl create namespace mango
kubectl -n mango create secret generic mango-secrets \
  --from-literal=cluster-key="$(openssl rand -hex 32)" \
  --from-literal=root-password="$(openssl rand -base64 24)"
kubectl -n mango apply -f deploy/kubernetes/mango.yaml
```

Connect with the replica-set URI, so drivers route to the leader:

```
mongodb://root:<pw>@mango-0.mango.mango.svc.cluster.local:27017,mango-1.mango.mango.svc.cluster.local:27017,mango-2.mango.mango.svc.cluster.local:27017/?replicaSet=mango&authSource=admin
```

You can also point a driver at the `mango-client` service with `directConnection=true`, because any node accepts writes.

### Docker Compose (three simulated zones on one machine)

```sh
cd deploy/compose
export MANGO_CLUSTER_KEY=$(openssl rand -hex 32)
docker compose up -d
mongosh "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=mango"
docker compose kill mango-a     # lose a "zone": writes keep working
```

### VMs or bare metal

Run one node per zone. Every node gets the same `--members` list and cluster key:

```sh
mango --node-id 1 \
  --members "1=10.0.1.10:7017/db-a.example.com:27017,2=10.0.2.10:7017/db-b.example.com:27017,3=10.0.3.10:7017/db-c.example.com:27017" \
  --cluster-key-file /etc/mango/cluster.key \
  --data-dir /var/lib/mango --auth --root-user root --root-password "$ROOT_PW"
```

Each member is `id=raft_address/client_address`. Nodes use the raft address to reach each other (port 7017 here). Drivers are given the client address. Keep port 7017 private to the cluster.

## Configuration

Every flag has an environment variable (`--auth` → `MANGO_AUTH`, and so on). See `mango --help`.

| Flag | Default | |
|---|---|---|
| `--data-dir` | `./mango-data` | Database file and snapshot staging area |
| `--bind` | `0.0.0.0:27017` | Client listener |
| `--advertise` | from `--members`, or `<hostname>:<port>` | Address reported to drivers |
| `--node-id` / `--node-id-from-hostname` | | `mango-2` → node 3 (StatefulSet ordinals) |
| `--members` | single node | `id=raft/client,...` |
| `--cluster-key-file` / `--cluster-key` | none | Shared secret for peer authentication (≥16 bytes). Without it, peers are not authenticated. |
| `--cluster-name` | `mango` | Nodes refuse peers from other clusters. It is also the replica-set name. |
| `--auth`, `--root-user`, `--root-password` | off | SCRAM-SHA-256. The root user is created on first start. |
| `--standalone` | off | Report nodes as standalone servers, for use behind a load balancer |
| `--no-forward-writes` | off | Followers reject writes like MongoDB secondaries do |
| `--heartbeat-ms`, `--election-timeout-ms` | 150, 1500 | Suited to latency within a region. Raise them for clusters that span regions. |

## Compatibility

Mango reports itself as MongoDB 6.0 (wire version 17).

**Supported:**

* **CRUD and cursors:** `insert`, `update`, `delete`, `findAndModify`, `find`, `getMore`, `killCursors`, `count`, `distinct`, bulk writes (ordered and unordered), upserts, and retryable writes.
* **Query operators:** `$eq $ne $gt $gte $lt $lte $in $nin $and $or $nor $not $exists $type $regex $size $all $elemMatch $mod $expr`, with MongoDB's array-traversal, null-matches-missing and type-bracketing semantics.
* **Update operators:** `$set $unset $inc $mul $min $max $rename $setOnInsert $currentDate $push` (with `$each $position $slice $sort`), `$addToSet $pop $pull $pullAll $bit`. Positional `$`, `$[]` and `$[id]` with `arrayFilters` work, as do replacement and pipeline updates.
* **Projection:** inclusion and exclusion, `$slice`, `$elemMatch`, positional `.$`, and computed fields.
* **Aggregation stages:** `$match $project $addFields/$set $unset $sort $limit $skip $group $count $unwind $lookup` (both the equality form and the `let`/`pipeline` form), `$replaceRoot/$replaceWith $sortByCount $facet $sample $unionWith`.
* **Aggregation expressions:** about 100, covering arithmetic, string, array, set, date, type conversion, `$cond/$switch/$let/$map/$filter/$reduce`, and more.
* **Indexes:** single-field and compound, multikey, `unique`, `sparse`, `partialFilterExpression`, and TTL (`expireAfterSeconds`). `explain` shows the chosen plan.
* **Catalog and admin:** `create`, `drop`, `dropDatabase`, `renameCollection`, `listCollections`, `listIndexes`, `listDatabases`, `dbStats`, `collStats`, `hello`, `buildInfo`, `serverStatus`, `replSetGetStatus`, `replSetGetConfig`, `createUser`/`updateUser`/`dropUser`/`usersInfo`, and `mangoStatus` (Raft state).

**Not supported (yet).** Commands that need these features return a clear error:

* Multi-document transactions
* Change streams
* Text, geospatial and hashed indexes
* Collations
* Capped, time-series and clustered collections, and schema validators
* `$out` and `$merge`
* Server-side JavaScript
* SCRAM-SHA-1, x.509 and LDAP auth, and fine-grained roles: users are either read-only (`read` or `readAnyDatabase`) or full access

**Known limitations:**

* **No TLS.** Run Mango on a private network or behind a service mesh or TLS-terminating proxy.
* **Fixed membership.** Members are set when the cluster is created. Adding or removing nodes means creating a new cluster and copying the data (with `mongodump`/`mongorestore`).
* **Simple query planner.** It uses an index when the query has an equality, `$in` or range condition on the index's first field, and otherwise scans the collection. Sorts and aggregations run in memory.
* **Decimal128.** Values are stored and compared, but arithmetic on them is done as double.
* **No awaitable `hello`.** Drivers find out about a new leader when a write fails with `NotWritablePrimary` (they retry immediately) or at their next heartbeat.

## Architecture

```
client ──► wire.rs (OP_MSG/OP_QUERY) ──► server.rs / commands.rs
                                            │ reads: MVCC snapshot (storage.rs)
                                            │ writes: Proposal ──► raft/ (log, election, replication)
                                            ▼                          │ committed
                                   storage.rs (redb) ◄── statemachine.rs (deterministic apply)
```

* **`keystring.rs`**: an order-preserving binary encoding of BSON values. Byte order matches MongoDB's sort order, and numerically equal values (`1`, `1.0`, `NumberLong(1)`) get the same key. Sorting, equality, `_id` uniqueness and index order all use it, so they cannot disagree.
* **`storage.rs`**: documents and secondary indexes live in [redb](https://github.com/cberner/redb), an ACID, MVCC, copy-on-write B-tree store, along with the Raft log. Applying a log entry and advancing `applied_index` happen in one transaction.
* **`statemachine.rs`**: `apply` is deterministic. Generated `_id`s and timestamps are fixed when a write is proposed, so every replica computes the same state and the same reply.
* **`raft/`**: leader election with pre-vote, log replication, commit, compaction, snapshot transfer, and write forwarding. The Raft state machine runs on one thread. Networking is async (tokio), using length-prefixed BSON frames multiplexed over one connection per peer.

## Development

```sh
cargo test                                   # unit + driver + 3-process cluster tests
MANGO_CHAOS_ROUNDS=25 cargo test --test cluster chaos -- --nocapture
python tests/compat/pymongo_smoke.py "mongodb://127.0.0.1:27017/?directConnection=true"
```

Licensed under Apache-2.0.
