//! CRUD, query and catalog commands.

use crate::aggregate::{self, Env, SortSpec, Source, Stage};
use crate::bsonutil::{get_bool, get_doc, get_int, get_str};
use crate::error::{Error, Result};
use crate::keystring;
use crate::projection::Projection;
use crate::query::{Matcher, lookup};
use crate::server::{Conn, Cursor, MAX_BATCH_BYTES, Server, session_of};
use crate::statemachine::Command;
use crate::storage::{ScanOpts, Snapshot, split_ns, validate_coll_name, validate_db_name};
use bson::{Bson, Document, doc, oid::ObjectId};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

fn coll_name<'a>(body: &'a Document, cmd: &str) -> Result<&'a str> {
    match body.get(cmd) {
        Some(Bson::String(s)) => {
            validate_coll_name(s).or_else(|e| if s.starts_with("system.") { Ok(()) } else { Err(e) })?;
            Ok(s)
        }
        Some(other) => Err(Error::invalid_namespace(format!("collection name has invalid type {}", crate::bsonutil::type_name(other)))),
        None => Err(Error::failed_to_parse(format!("missing collection name for {cmd}"))),
    }
}

fn docs_array(body: &Document, key: &str) -> Result<Vec<Document>> {
    match body.get(key) {
        Some(Bson::Array(a)) => a
            .iter()
            .map(|v| match v {
                Bson::Document(d) => Ok(d.clone()),
                _ => Err(Error::type_mismatch(format!("{key} entries must be documents"))),
            })
            .collect(),
        None => Err(Error::failed_to_parse(format!("BSON field '{key}' is missing but a required field"))),
        Some(_) => Err(Error::type_mismatch(format!("{key} must be an array"))),
    }
}

impl Source for Snapshot {
    fn scan(&self, db: &str, coll: &str, filter: &Matcher) -> Result<Vec<Document>> {
        self.find(&format!("{db}.{coll}"), filter, &ScanOpts { limit: None })
    }
}

impl Server {
    pub async fn write_command(self: Arc<Self>, db: &str, name: &str, body: &Document) -> Result<Document> {
        validate_db_name(db)?;
        let session = session_of(body);
        let ordered = get_bool(body, "ordered")?.unwrap_or(true);
        let cmd = match name {
            "insert" => {
                let coll = coll_name(body, name)?;
                let mut docs = docs_array(body, "documents")?;
                if docs.is_empty() {
                    return Err(Error::invalid_options("Write batch sizes must be between 1 and 100000. Got 0 operations."));
                }
                if docs.len() > 100_000 {
                    return Err(Error::invalid_options(format!(
                        "Write batch sizes must be between 1 and 100000. Got {} operations.",
                        docs.len()
                    )));
                }
                for d in docs.iter_mut() {
                    if !d.contains_key("_id") {
                        let mut with_id = doc! {"_id": ObjectId::new()};
                        with_id.extend(std::mem::take(d));
                        *d = with_id;
                    }
                }
                Command::Insert { ns: format!("{db}.{coll}"), docs, ordered }
            }
            "update" => {
                let coll = coll_name(body, name)?;
                let updates = docs_array(body, "updates")?;
                if updates.is_empty() || updates.len() > 100_000 {
                    return Err(Error::invalid_options(format!(
                        "Write batch sizes must be between 1 and 100000. Got {} operations.",
                        updates.len()
                    )));
                }
                let upsert_ids = updates.iter().map(|_| ObjectId::new()).collect();
                Command::Update { ns: format!("{db}.{coll}"), updates, ordered, upsert_ids }
            }
            "delete" => {
                let coll = coll_name(body, name)?;
                let deletes = docs_array(body, "deletes")?;
                if deletes.is_empty() || deletes.len() > 100_000 {
                    return Err(Error::invalid_options(format!(
                        "Write batch sizes must be between 1 and 100000. Got {} operations.",
                        deletes.len()
                    )));
                }
                Command::Delete { ns: format!("{db}.{coll}"), deletes, ordered }
            }
            "findAndModify" | "findandmodify" => {
                let coll = coll_name(body, name)?;
                let mut spec = body.clone();
                for k in ["findAndModify", "findandmodify", "$db", "lsid", "txnNumber", "$clusterTime", "writeConcern", "$readPreference"] {
                    spec.remove(k);
                }
                Command::FindAndModify { ns: format!("{db}.{coll}"), spec, upsert_id: ObjectId::new() }
            }
            "create" => {
                let coll = coll_name(body, name)?;
                let mut options = body.clone();
                for k in ["create", "$db", "lsid", "txnNumber", "$clusterTime", "writeConcern", "$readPreference", "comment"] {
                    options.remove(k);
                }
                Command::Create { ns: format!("{db}.{coll}"), options }
            }
            "drop" => Command::Drop { ns: format!("{db}.{}", coll_name(body, name)?) },
            "dropDatabase" => Command::DropDatabase { db: db.to_string() },
            "createIndexes" => {
                let coll = coll_name(body, name)?;
                let indexes = docs_array(body, "indexes")?;
                if indexes.is_empty() {
                    return Err(Error::bad_value("Must specify at least one index to create"));
                }
                Command::CreateIndexes { ns: format!("{db}.{coll}"), indexes }
            }
            "dropIndexes" | "deleteIndexes" => {
                let coll = coll_name(body, name)?;
                let index =
                    body.get("index").cloned().ok_or_else(|| Error::failed_to_parse("BSON field 'dropIndexes.index' is missing"))?;
                Command::DropIndexes { ns: format!("{db}.{coll}"), index }
            }
            "renameCollection" => {
                if db != "admin" {
                    return Err(Error::unauthorized("renameCollection may only be run against the admin database."));
                }
                let from =
                    get_str(body, "renameCollection")?.ok_or_else(|| Error::bad_value("renameCollection requires a source namespace"))?;
                let to = get_str(body, "to")?.ok_or_else(|| Error::bad_value("renameCollection requires a 'to' namespace"))?;
                let (fdb, fcoll) = split_ns(from);
                validate_db_name(fdb)?;
                if fcoll.is_empty() {
                    return Err(Error::invalid_namespace(format!("Invalid source namespace: {from}")));
                }
                Command::Rename { from: from.to_string(), to: to.to_string(), drop_target: get_bool(body, "dropTarget")?.unwrap_or(false) }
            }
            _ => return Err(Error::command_not_found(format!("no such command: '{name}'"))),
        };
        self.propose(cmd, session).await
    }

    pub async fn read_command(self: Arc<Self>, conn: &mut Conn, db: &str, name: &str, body: &Document) -> Result<Document> {
        match name {
            "getMore" => return self.get_more(conn, db, body).await,
            "killCursors" => return self.kill_cursors(body).await,
            _ => {}
        }
        if name != "listDatabases" {
            validate_db_name(db)?;
        }
        let snap = self.store.snapshot()?;
        let db = db.to_string();
        let name = name.to_string();
        let body = body.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<ReadResult> {
            match name.as_str() {
                "find" => find(&snap, &db, &body),
                "count" => count(&snap, &db, &body).map(ReadResult::Reply),
                "distinct" => distinct(&snap, &db, &body).map(ReadResult::Reply),
                "aggregate" => aggregate_cmd(&snap, &db, &body),
                "listCollections" => list_collections(&snap, &db, &body),
                "listIndexes" => list_indexes(&snap, &db, &body),
                "listDatabases" => list_databases(&snap, &body).map(ReadResult::Reply),
                "dbStats" => db_stats(&snap, &db).map(ReadResult::Reply),
                "collStats" => coll_stats(&snap, &db, &body).map(ReadResult::Reply),
                "explain" => explain(&snap, &db, &body).map(ReadResult::Reply),
                _ => Err(Error::command_not_found(format!("no such command: '{name}'"))),
            }
        })
        .await
        .map_err(|e| Error::internal(format!("query task failed: {e}")))??;
        match result {
            ReadResult::Reply(d) => Ok(d),
            ReadResult::Cursor { ns, docs, batch_size, single_batch } => {
                Ok(self.cursor_reply(conn, ns, docs, batch_size, single_batch, "firstBatch").await)
            }
        }
    }

    async fn cursor_reply(
        &self,
        conn: &Conn,
        ns: String,
        docs: Vec<Document>,
        batch_size: Option<usize>,
        single_batch: bool,
        field: &str,
    ) -> Document {
        let mut docs: VecDeque<Document> = docs.into();
        let batch = take_batch(&mut docs, batch_size);
        let id = if docs.is_empty() || single_batch {
            0i64
        } else {
            let mut cursors = self.cursors.lock().await;
            let mut id = (rand::random::<u64>() >> 2) as i64;
            while id == 0 || cursors.contains_key(&id) {
                id = (rand::random::<u64>() >> 2) as i64;
            }
            cursors.insert(id, Cursor { ns: ns.clone(), docs, last_used: Instant::now(), owner: conn.owner() });
            id
        };
        let mut c = Document::new();
        c.insert(field, batch);
        c.insert("id", id);
        c.insert("ns", ns);
        doc! {"cursor": c, "ok": 1.0}
    }

    async fn get_more(&self, conn: &Conn, db: &str, body: &Document) -> Result<Document> {
        let id = match body.get("getMore") {
            Some(Bson::Int64(i)) => *i,
            Some(Bson::Int32(i)) => *i as i64,
            _ => return Err(Error::type_mismatch("getMore requires a long cursor id")),
        };
        let coll = get_str(body, "collection")?.ok_or_else(|| Error::failed_to_parse("getMore requires 'collection'"))?;
        let batch_size = get_int(body, "batchSize")?.filter(|b| *b > 0).map(|b| b as usize);
        let mut cursors = self.cursors.lock().await;
        let Some(cur) = cursors.get_mut(&id) else {
            return Err(Error::cursor_not_found(format!("cursor id {id} not found")));
        };
        if cur.ns != format!("{db}.{coll}") {
            return Err(Error::unauthorized(format!(
                "Requested getMore on namespace '{db}.{coll}', but cursor belongs to a different namespace {}",
                cur.ns
            )));
        }
        if cur.owner != conn.owner() {
            return Err(Error::unauthorized("cursor belongs to a different user"));
        }
        cur.last_used = Instant::now();
        let batch = take_batch(&mut cur.docs, batch_size.or(Some(usize::MAX)));
        let ns = cur.ns.clone();
        let done = cur.docs.is_empty();
        if done {
            cursors.remove(&id);
        }
        Ok(doc! {"cursor": {"nextBatch": batch, "id": if done { 0i64 } else { id }, "ns": ns}, "ok": 1.0})
    }

    async fn kill_cursors(&self, body: &Document) -> Result<Document> {
        let ids = match body.get("cursors") {
            Some(Bson::Array(a)) => a.clone(),
            _ => return Err(Error::failed_to_parse("killCursors requires a 'cursors' array")),
        };
        let mut cursors = self.cursors.lock().await;
        let (mut killed, mut not_found) = (Vec::new(), Vec::new());
        for v in ids {
            let id = match v {
                Bson::Int64(i) => i,
                Bson::Int32(i) => i as i64,
                _ => continue,
            };
            if cursors.remove(&id).is_some() {
                killed.push(Bson::Int64(id));
            } else {
                not_found.push(Bson::Int64(id));
            }
        }
        Ok(doc! {"cursorsKilled": killed, "cursorsNotFound": not_found, "cursorsAlive": [], "cursorsUnknown": [], "ok": 1.0})
    }
}

pub enum ReadResult {
    Reply(Document),
    Cursor { ns: String, docs: Vec<Document>, batch_size: Option<usize>, single_batch: bool },
}

/// Takes up to `batch_size` documents (default 101) without exceeding 16MB,
/// always at least one if available.
fn take_batch(docs: &mut VecDeque<Document>, batch_size: Option<usize>) -> Vec<Bson> {
    let limit = batch_size.unwrap_or(101);
    let mut out = Vec::new();
    let mut bytes = 0usize;
    while out.len() < limit {
        let Some(d) = docs.front() else { break };
        let size = crate::bsonutil::doc_size(d);
        if !out.is_empty() && bytes + size > MAX_BATCH_BYTES {
            break;
        }
        bytes += size;
        out.push(Bson::Document(docs.pop_front().unwrap()));
    }
    out
}

fn reject_unsupported(body: &Document) -> Result<()> {
    if body.contains_key("collation") {
        return Err(Error::not_implemented("collations are not supported by Mango"));
    }
    if matches!(body.get("tailable"), Some(Bson::Boolean(true))) {
        return Err(Error::not_implemented("tailable cursors are not supported by Mango"));
    }
    Ok(())
}

fn batch_size_of(body: &Document, cursor_field: bool) -> Result<Option<usize>> {
    let v = if cursor_field {
        match get_doc(body, "cursor")? {
            Some(c) => get_int(c, "batchSize")?,
            None => None,
        }
    } else {
        get_int(body, "batchSize")?
    };
    match v {
        Some(b) if b < 0 => Err(Error::bad_value("batchSize must be non-negative")),
        Some(b) => Ok(Some(b as usize)),
        None => Ok(None),
    }
}

struct FindSpec {
    ns: String,
    matcher: Matcher,
    sort: Option<SortSpec>,
    projection: Option<Projection>,
    skip: usize,
    limit: Option<usize>,
    batch_size: Option<usize>,
    single_batch: bool,
}

fn parse_find(db: &str, body: &Document) -> Result<FindSpec> {
    reject_unsupported(body)?;
    let coll = coll_name(body, "find")?;
    let empty = Document::new();
    let filter = get_doc(body, "filter")?.unwrap_or(&empty);
    let sort = get_doc(body, "sort")?.filter(|s| !s.is_empty()).map(SortSpec::parse).transpose()?;
    let projection = get_doc(body, "projection")?.filter(|p| !p.is_empty()).map(|p| Projection::parse(p, true)).transpose()?;
    let skip = match get_int(body, "skip")? {
        Some(s) if s < 0 => return Err(Error::bad_value("skip value must be non-negative")),
        Some(s) => s as usize,
        None => 0,
    };
    let mut single_batch = get_bool(body, "singleBatch")?.unwrap_or(false);
    let limit = match get_int(body, "limit")? {
        Some(l) if l < 0 => {
            single_batch = true;
            Some(l.unsigned_abs() as usize)
        }
        Some(0) | None => None,
        Some(l) => Some(l as usize),
    };
    if body.contains_key("let") {
        return Err(Error::not_implemented("'let' variables in find are not supported by Mango; use aggregate"));
    }
    Ok(FindSpec {
        ns: format!("{db}.{coll}"),
        matcher: Matcher::parse(filter)?,
        sort,
        projection,
        skip,
        limit,
        batch_size: batch_size_of(body, false)?,
        single_batch,
    })
}

fn run_find(snap: &Snapshot, f: &FindSpec) -> Result<Vec<Document>> {
    let scan_limit = if f.sort.is_none() { f.limit.map(|l| l + f.skip) } else { None };
    let mut docs = snap.find(&f.ns, &f.matcher, &ScanOpts { limit: scan_limit })?;
    if let Some(s) = &f.sort {
        docs = s.sort(docs);
    }
    let docs: Vec<Document> = docs.into_iter().skip(f.skip).take(f.limit.unwrap_or(usize::MAX)).collect();
    match &f.projection {
        Some(p) => docs.iter().map(|d| p.apply(d, Some(&f.matcher))).collect(),
        None => Ok(docs),
    }
}

fn find(snap: &Snapshot, db: &str, body: &Document) -> Result<ReadResult> {
    let f = parse_find(db, body)?;
    let docs = run_find(snap, &f)?;
    let batch_size = match (f.batch_size, f.limit, f.single_batch) {
        (_, Some(l), true) => Some(l),
        (b, _, _) => b,
    };
    Ok(ReadResult::Cursor { ns: f.ns, docs, batch_size, single_batch: f.single_batch })
}

fn count(snap: &Snapshot, db: &str, body: &Document) -> Result<Document> {
    reject_unsupported(body)?;
    let coll = coll_name(body, "count")?;
    let ns = format!("{db}.{coll}");
    let empty = Document::new();
    let query = get_doc(body, "query")?.unwrap_or(&empty);
    let skip = get_int(body, "skip")?.unwrap_or(0).max(0) as usize;
    let limit = get_int(body, "limit")?.map(|l| l.unsigned_abs() as usize).filter(|l| *l > 0);
    let n = if query.is_empty() {
        snap.count_all(&ns)? as usize
    } else {
        let m = Matcher::parse(query)?;
        snap.find(&ns, &m, &ScanOpts { limit: limit.map(|l| l + skip) })?.len()
    };
    let n = n.saturating_sub(skip).min(limit.unwrap_or(usize::MAX));
    Ok(doc! {"n": n as i64, "ok": 1.0})
}

fn distinct(snap: &Snapshot, db: &str, body: &Document) -> Result<Document> {
    reject_unsupported(body)?;
    let coll = coll_name(body, "distinct")?;
    let key = get_str(body, "key")?.ok_or_else(|| Error::failed_to_parse("distinct requires a 'key' string"))?;
    if key.is_empty() {
        return Err(Error::failed_to_parse("FieldPath cannot be constructed with empty string"));
    }
    let empty = Document::new();
    let query = get_doc(body, "query")?.unwrap_or(&empty);
    let m = Matcher::parse(query)?;
    let docs = snap.find(&format!("{db}.{coll}"), &m, &ScanOpts { limit: None })?;
    let parts: Vec<String> = key.split('.').map(String::from).collect();
    let mut seen = std::collections::BTreeMap::new();
    for d in &docs {
        let mut cands = Vec::new();
        lookup(d, &parts, &mut cands);
        for c in cands.into_iter().flatten() {
            match c {
                Bson::Array(a) => {
                    for e in a {
                        seen.entry(keystring::encode(e)).or_insert_with(|| e.clone());
                    }
                }
                v => {
                    seen.entry(keystring::encode(v)).or_insert_with(|| v.clone());
                }
            }
        }
    }
    let values: Vec<Bson> = seen.into_values().collect();
    Ok(doc! {"values": values, "ok": 1.0})
}

fn aggregate_cmd(snap: &Snapshot, db: &str, body: &Document) -> Result<ReadResult> {
    reject_unsupported(body)?;
    let coll = match body.get("aggregate") {
        Some(Bson::String(_)) => coll_name(body, "aggregate")?,
        _ => return Err(Error::not_implemented("collection-less aggregate is not supported by Mango")),
    };
    let ns = format!("{db}.{coll}");
    let pipeline = match body.get("pipeline") {
        Some(Bson::Array(p)) => aggregate::parse_pipeline(p)?,
        _ => return Err(Error::failed_to_parse("'pipeline' option must be specified as an array")),
    };
    let explain = get_bool(body, "explain")?.unwrap_or(false);
    if !explain && !body.contains_key("cursor") {
        return Err(Error::failed_to_parse("The 'cursor' option is required, except for aggregate with the explain argument"));
    }
    let vars: Vec<(String, Bson)> = match get_doc(body, "let")? {
        Some(l) => l
            .iter()
            .map(|(k, v)| Ok((k.clone(), crate::expr::Expr::parse(v)?.eval(&crate::expr::Vars::root(&Document::new()))?)))
            .collect::<Result<_>>()?,
        None => vec![],
    };
    // A leading $match (without $$variables) selects the scan and can use an index.
    let (scan_filter, rest) = match pipeline.first() {
        Some(Stage::Match(m)) if vars.is_empty() => (m.clone(), &pipeline[1..]),
        _ => (Matcher::Always, &pipeline[..]),
    };
    if explain {
        let plan = snap.plan(&ns, &scan_filter)?;
        return Ok(ReadResult::Reply(doc! {"queryPlanner": {"namespace": &ns, "winningPlan": plan.describe()}, "ok": 1.0}));
    }
    let input = snap.find(&ns, &scan_filter, &ScanOpts { limit: None })?;
    let env = Env { db: db.to_string(), source: snap, vars };
    let out = aggregate::run_pipeline(rest, input, &env)?;
    Ok(ReadResult::Cursor { ns, docs: out, batch_size: batch_size_of(body, true)?, single_batch: false })
}

fn list_collections(snap: &Snapshot, db: &str, body: &Document) -> Result<ReadResult> {
    let empty = Document::new();
    let filter = Matcher::parse(get_doc(body, "filter")?.unwrap_or(&empty))?;
    let name_only = get_bool(body, "nameOnly")?.unwrap_or(false);
    let prefix = format!("{db}.");
    let mut out = Vec::new();
    for (ns, meta) in snap.namespaces()? {
        let Some(name) = ns.strip_prefix(&prefix) else { continue };
        let mut d = doc! {"name": name, "type": "collection"};
        if !name_only {
            d.insert("options", meta.options.clone());
            let mut info = doc! {"readOnly": false};
            if let Some(u) = &meta.uuid {
                info.insert("uuid", Bson::Binary(u.clone()));
            }
            d.insert("info", info);
            d.insert("idIndex", doc! {"v": 2, "key": {"_id": 1}, "name": "_id_"});
        }
        if filter.matches(&d)? {
            out.push(d);
        }
    }
    Ok(ReadResult::Cursor {
        ns: format!("{db}.$cmd.listCollections"),
        docs: out,
        batch_size: batch_size_of(body, true)?,
        single_batch: false,
    })
}

fn list_indexes(snap: &Snapshot, db: &str, body: &Document) -> Result<ReadResult> {
    let coll = coll_name(body, "listIndexes")?;
    let ns = format!("{db}.{coll}");
    let meta = snap.meta(&ns)?.ok_or_else(|| Error::namespace_not_found(format!("ns does not exist: {ns}")))?;
    let mut docs = vec![doc! {"v": 2, "key": {"_id": 1}, "name": "_id_"}];
    docs.extend(meta.indexes.iter().map(|i| i.to_spec()));
    Ok(ReadResult::Cursor { ns, docs, batch_size: batch_size_of(body, true)?, single_batch: false })
}

fn list_databases(snap: &Snapshot, body: &Document) -> Result<Document> {
    let name_only = get_bool(body, "nameOnly")?.unwrap_or(false);
    let empty = Document::new();
    let filter = Matcher::parse(get_doc(body, "filter")?.unwrap_or(&empty))?;
    let mut sizes: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for (ns, _) in snap.namespaces()? {
        let (db, _) = split_ns(&ns);
        let bytes = if name_only { 0 } else { snap.coll_stats(&ns)?.1 };
        *sizes.entry(db.to_string()).or_insert(0) += bytes;
    }
    let mut dbs = Vec::new();
    let mut total = 0i64;
    for (name, size) in sizes {
        let d = if name_only {
            doc! {"name": &name}
        } else {
            doc! {"name": &name, "sizeOnDisk": size as i64, "empty": false}
        };
        if filter.matches(&d)? {
            total += size as i64;
            dbs.push(d);
        }
    }
    let mut reply = doc! {"databases": dbs};
    if !name_only {
        reply.insert("totalSize", total);
    }
    reply.insert("ok", 1.0);
    Ok(reply)
}

fn db_stats(snap: &Snapshot, db: &str) -> Result<Document> {
    let prefix = format!("{db}.");
    let (mut colls, mut objects, mut size, mut indexes) = (0i64, 0i64, 0i64, 0i64);
    for (ns, meta) in snap.namespaces()? {
        if !ns.starts_with(&prefix) {
            continue;
        }
        let (n, bytes) = snap.coll_stats(&ns)?;
        colls += 1;
        objects += n as i64;
        size += bytes as i64;
        indexes += meta.indexes.len() as i64 + 1;
    }
    Ok(doc! {
        "db": db, "collections": colls, "views": 0, "objects": objects,
        "avgObjSize": if objects > 0 { size as f64 / objects as f64 } else { 0.0 },
        "dataSize": size as f64, "storageSize": size as f64, "indexes": indexes, "indexSize": 0.0,
        "totalSize": size as f64, "scaleFactor": 1, "ok": 1.0
    })
}

fn coll_stats(snap: &Snapshot, db: &str, body: &Document) -> Result<Document> {
    let coll = coll_name(body, "collStats")?;
    let ns = format!("{db}.{coll}");
    let meta = snap.meta(&ns)?.ok_or_else(|| Error::namespace_not_found(format!("Collection [{ns}] not found.")))?;
    let (n, bytes) = snap.coll_stats(&ns)?;
    let mut index_sizes = doc! {"_id_": 0};
    for i in &meta.indexes {
        index_sizes.insert(i.name.clone(), 0);
    }
    Ok(doc! {
        "ns": &ns, "count": n as i64, "size": bytes as i64,
        "avgObjSize": if n > 0 { (bytes / n) as i64 } else { 0 },
        "storageSize": bytes as i64, "nindexes": meta.indexes.len() as i32 + 1,
        "totalIndexSize": 0, "indexSizes": index_sizes, "ok": 1.0
    })
}

fn explain(snap: &Snapshot, db: &str, body: &Document) -> Result<Document> {
    let Some(Bson::Document(inner)) = body.get("explain") else {
        return Err(Error::failed_to_parse("explain requires a command document"));
    };
    let name = inner.keys().next().cloned().unwrap_or_default();
    let (ns, filter) = match name.as_str() {
        "find" => {
            let f = parse_find(db, inner)?;
            (f.ns, f.matcher)
        }
        "count" | "distinct" => {
            let coll = coll_name(inner, &name)?;
            let empty = Document::new();
            (format!("{db}.{coll}"), Matcher::parse(get_doc(inner, "query")?.unwrap_or(&empty))?)
        }
        "aggregate" => {
            let coll = coll_name(inner, "aggregate")?;
            let m = match inner.get("pipeline") {
                Some(Bson::Array(p)) => match aggregate::parse_pipeline(p)?.into_iter().next() {
                    Some(Stage::Match(m)) => m,
                    _ => Matcher::Always,
                },
                _ => Matcher::Always,
            };
            (format!("{db}.{coll}"), m)
        }
        "update" | "delete" | "findAndModify" => {
            let coll = coll_name(inner, &name)?;
            let q = match name.as_str() {
                "update" => docs_array(inner, "updates")?.first().and_then(|u| u.get_document("q").ok().cloned()),
                "delete" => docs_array(inner, "deletes")?.first().and_then(|u| u.get_document("q").ok().cloned()),
                _ => inner.get_document("query").ok().cloned(),
            };
            (format!("{db}.{coll}"), Matcher::parse(&q.unwrap_or_default())?)
        }
        other => return Err(Error::illegal_operation(format!("explain is not supported for '{other}'"))),
    };
    let plan = snap.plan(&ns, &filter)?;
    Ok(doc! {
        "queryPlanner": {"namespace": &ns, "winningPlan": plan.describe()},
        "serverInfo": {"version": crate::server::COMPAT_VERSION, "mango": env!("CARGO_PKG_VERSION")},
        "ok": 1.0
    })
}
