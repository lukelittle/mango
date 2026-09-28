//! The replicated state machine: every write is a [`Proposal`] in the Raft
//! log, and [`apply`] turns it into storage changes plus the reply the client
//! sees. `apply` must be deterministic: every input it needs (generated ids,
//! the current time) is fixed in the proposal before it is logged.

use crate::aggregate::SortSpec;
use crate::bsonutil::{doc_from_bytes, doc_to_bytes, get_bool, get_int};
use crate::error::{Error, Result};
use crate::keystring;
use crate::projection::Projection;
use crate::query::Matcher;
use crate::storage::{self, CATALOG, SESSIONS, USERS, WriteColl, parse_index_spec, split_ns};
use crate::update::{Update, UpdateCtx, check_array_filters_used, parse_array_filters, upsert_document};
use bson::{Bson, Document, doc, oid::ObjectId};
use redb::{ReadableTable, WriteTransaction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Noop,
    Insert { ns: String, docs: Vec<Document>, ordered: bool },
    Update { ns: String, updates: Vec<Document>, ordered: bool, upsert_ids: Vec<ObjectId> },
    Delete { ns: String, deletes: Vec<Document>, ordered: bool },
    FindAndModify { ns: String, spec: Document, upsert_id: ObjectId },
    Create { ns: String, options: Document },
    Drop { ns: String },
    DropDatabase { db: String },
    CreateIndexes { ns: String, indexes: Vec<Document> },
    DropIndexes { ns: String, index: Bson },
    Rename { from: String, to: String, drop_target: bool },
    PutUser { key: String, user: Document, create: bool },
    DropUser { key: String },
    /// Deletes documents whose TTL index field is older than `cutoff_millis`.
    Expire { ns: String, field: String, cutoff_millis: i64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionTxn {
    pub lsid: Bson,
    pub txn_number: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Proposal {
    pub cmd: Command,
    pub now_millis: i64,
    pub session: Option<SessionTxn>,
}

impl Proposal {
    pub fn encode(&self) -> Vec<u8> {
        bson::serialize_to_vec(self).expect("proposal serializes")
    }

    pub fn decode(b: &[u8]) -> Result<Proposal> {
        bson::deserialize_from_slice(b).map_err(|e| Error::internal(format!("corrupt log entry: {e}")))
    }

    /// Writes can be retried by drivers; only these commands are recorded
    /// per session.
    fn retryable(&self) -> bool {
        matches!(self.cmd, Command::Insert { .. } | Command::Update { .. } | Command::Delete { .. } | Command::FindAndModify { .. })
    }
}

const SESSION_TTL_MILLIS: i64 = 30 * 60 * 1000;

/// Applies one committed log entry. Returns the command reply (or command
/// error), which is also recorded for retryable writes.
pub fn apply(txn: &WriteTransaction, p: &Proposal, index: u64) -> Result<std::result::Result<Document, Error>> {
    let session_key = match (&p.session, p.retryable()) {
        (Some(s), true) => Some((keystring::encode(&s.lsid), s.txn_number)),
        _ => None,
    };
    if let Some((key, txn_number)) = &session_key {
        let t = txn.open_table(SESSIONS)?;
        if let Some(rec) = t.get(key.as_slice())? {
            let rec = doc_from_bytes(rec.value())?;
            let prev = rec.get_i64("txnNumber").unwrap_or(-1);
            if prev == *txn_number {
                return Ok(match rec.get_document("reply") {
                    Ok(r) => Ok(r.clone()),
                    Err(_) => Err(Error::internal("corrupt retryable write record")),
                });
            }
            if prev > *txn_number {
                return Ok(Err(Error {
                    code: 225,
                    code_name: "TransactionTooOld",
                    msg: format!("Cannot start transaction {txn_number} on session because a newer transaction {prev} has already started"),
                }));
            }
        }
    }
    if index % 1024 == 0 {
        prune_sessions(txn, p.now_millis)?;
    }
    let result = apply_cmd(txn, p)?;
    if let (Some((key, txn_number)), Ok(reply)) = (&session_key, &result) {
        let rec = doc! {"txnNumber": *txn_number, "reply": reply.clone(), "lastUse": p.now_millis};
        txn.open_table(SESSIONS)?.insert(key.as_slice(), doc_to_bytes(&rec)?.as_slice())?;
    }
    Ok(result)
}

fn prune_sessions(txn: &WriteTransaction, now: i64) -> Result<()> {
    let mut t = txn.open_table(SESSIONS)?;
    t.retain(|_, v| match Document::from_reader(v) {
        Ok(d) => d.get_i64("lastUse").map(|l| l >= now - SESSION_TTL_MILLIS).unwrap_or(false),
        Err(_) => false,
    })?;
    Ok(())
}

/// Storage failures (`Err` of the outer result) are fatal to the node;
/// command failures are returned to the client in the inner result.
fn apply_cmd(txn: &WriteTransaction, p: &Proposal) -> Result<std::result::Result<Document, Error>> {
    let r = match &p.cmd {
        Command::Noop => Ok(doc! {"ok": 1.0}),
        Command::Insert { ns, docs, ordered } => insert(txn, ns, docs, *ordered),
        Command::Update { ns, updates, ordered, upsert_ids } => update(txn, ns, updates, *ordered, upsert_ids, p.now_millis),
        Command::Delete { ns, deletes, ordered } => delete(txn, ns, deletes, *ordered),
        Command::FindAndModify { ns, spec, upsert_id } => find_and_modify(txn, ns, spec, *upsert_id, p.now_millis),
        Command::Create { ns, options } => create(txn, ns, options),
        Command::Drop { ns } => drop_coll(txn, ns),
        Command::DropDatabase { db } => drop_database(txn, db),
        Command::CreateIndexes { ns, indexes } => create_indexes(txn, ns, indexes),
        Command::DropIndexes { ns, index } => drop_indexes(txn, ns, index),
        Command::Rename { from, to, drop_target } => rename(txn, from, to, *drop_target),
        Command::PutUser { key, user, create } => put_user(txn, key, user, *create),
        Command::DropUser { key } => drop_user(txn, key),
        Command::Expire { ns, field, cutoff_millis } => expire(txn, ns, field, *cutoff_millis),
    };
    // Storage-level errors surface as InternalError from redb conversions;
    // they must abort the whole batch rather than be reported to one client.
    match r {
        Err(e) if e.code == 1 && e.msg.starts_with("storage error") => Err(e),
        other => Ok(other),
    }
}

fn with_write_errors(mut reply: Document, errors: Vec<Document>) -> Document {
    if !errors.is_empty() {
        reply.insert("writeErrors", errors);
    }
    reply
}

fn is_storage_error(e: &Error) -> bool {
    e.code == 1 && e.msg.starts_with("storage error")
}

fn insert(txn: &WriteTransaction, ns: &str, docs: &[Document], ordered: bool) -> Result<Document> {
    let mut coll = WriteColl::open_or_create(txn, ns, 0)?;
    let mut n = 0i32;
    let mut errors = Vec::new();
    for (i, d) in docs.iter().enumerate() {
        match coll.insert(txn, d.clone()) {
            Ok(()) => n += 1,
            Err(e) if is_storage_error(&e) => return Err(e),
            Err(e) => {
                errors.push(e.to_write_error(i));
                if ordered {
                    break;
                }
            }
        }
    }
    coll.finish(txn)?;
    Ok(with_write_errors(doc! {"n": n, "ok": 1.0}, errors))
}

struct UpdateStmt {
    query: Matcher,
    update: Update,
    multi: bool,
    upsert: bool,
    filters: Vec<(String, Matcher)>,
}

fn parse_update_stmt(s: &Document) -> Result<UpdateStmt> {
    for k in s.keys() {
        if !matches!(k.as_str(), "q" | "u" | "multi" | "upsert" | "arrayFilters" | "hint" | "collation" | "c" | "sort") {
            return Err(Error::failed_to_parse(format!("BSON field 'update.updates.{k}' is an unknown field.")));
        }
    }
    if s.contains_key("collation") {
        return Err(Error::not_implemented("collations are not supported by Mango"));
    }
    let q = match s.get("q") {
        Some(Bson::Document(q)) => q,
        _ => return Err(Error::failed_to_parse("BSON field 'update.updates.q' is missing but a required field")),
    };
    let u = s.get("u").ok_or_else(|| Error::failed_to_parse("BSON field 'update.updates.u' is missing but a required field"))?;
    let update = Update::parse(u)?;
    let multi = get_bool(s, "multi")?.unwrap_or(false);
    if multi && update.is_replacement() {
        return Err(Error::failed_to_parse("multi update is not supported for replacement-style update"));
    }
    let filters = parse_array_filters(s.get("arrayFilters"))?;
    check_array_filters_used(&update, &filters)?;
    Ok(UpdateStmt { query: Matcher::parse(q)?, update, multi, upsert: get_bool(s, "upsert")?.unwrap_or(false), filters })
}

fn doc_bytes_equal(a: &Document, b: &Document) -> bool {
    match (a.to_vec(), b.to_vec()) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

fn update(txn: &WriteTransaction, ns: &str, updates: &[Document], ordered: bool, upsert_ids: &[ObjectId], now: i64) -> Result<Document> {
    let mut coll: Option<WriteColl> = WriteColl::open(txn, ns)?;
    let (mut n, mut modified) = (0i64, 0i64);
    let mut upserted = Vec::new();
    let mut errors = Vec::new();
    for (i, raw) in updates.iter().enumerate() {
        let r: Result<()> = (|| {
            let stmt = parse_update_stmt(raw)?;
            let found = match &coll {
                Some(c) => c.find(txn, &stmt.query, if stmt.multi { None } else { Some(1) })?,
                None => vec![],
            };
            if found.is_empty() {
                if stmt.upsert {
                    let ctx = UpdateCtx { is_insert: true, now_millis: now, query: Some(&stmt.query), array_filters: &stmt.filters };
                    let id = upsert_ids.get(i).copied().ok_or_else(|| Error::internal("missing upsert id"))?;
                    let new = upsert_document(&stmt.query, &stmt.update, &ctx, id)?;
                    let new_id = new.get("_id").cloned().unwrap_or(Bson::Null);
                    if coll.is_none() {
                        coll = Some(WriteColl::open_or_create(txn, ns, 0)?);
                    }
                    coll.as_mut().unwrap().insert(txn, new)?;
                    n += 1;
                    upserted.push(doc! {"index": i as i32, "_id": new_id});
                }
                return Ok(());
            }
            let c = coll.as_mut().unwrap();
            let ctx = UpdateCtx { is_insert: false, now_millis: now, query: Some(&stmt.query), array_filters: &stmt.filters };
            for (idk, old) in found {
                let new = stmt.update.apply(&old, &ctx)?;
                n += 1;
                if !doc_bytes_equal(&old, &new) {
                    c.replace(txn, &idk, &old, new)?;
                    modified += 1;
                }
            }
            Ok(())
        })();
        if let Err(e) = r {
            if is_storage_error(&e) {
                return Err(e);
            }
            errors.push(e.to_write_error(i));
            if ordered {
                break;
            }
        }
    }
    if let Some(c) = coll {
        c.finish(txn)?;
    }
    let mut reply = doc! {"n": n as i32, "nModified": modified as i32};
    if !upserted.is_empty() {
        reply.insert("upserted", upserted);
    }
    reply.insert("ok", 1.0);
    Ok(with_write_errors(reply, errors))
}

fn delete(txn: &WriteTransaction, ns: &str, deletes: &[Document], ordered: bool) -> Result<Document> {
    let Some(mut coll) = WriteColl::open(txn, ns)? else {
        // Still validate the statements so errors are reported consistently.
        let mut errors = Vec::new();
        for (i, d) in deletes.iter().enumerate() {
            if let Err(e) = parse_delete_stmt(d) {
                errors.push(e.to_write_error(i));
                if ordered {
                    break;
                }
            }
        }
        return Ok(with_write_errors(doc! {"n": 0, "ok": 1.0}, errors));
    };
    let mut n = 0i64;
    let mut errors = Vec::new();
    for (i, raw) in deletes.iter().enumerate() {
        let r: Result<()> = (|| {
            let (m, limit) = parse_delete_stmt(raw)?;
            for (idk, old) in coll.find(txn, &m, limit)? {
                coll.delete(txn, &idk, &old)?;
                n += 1;
            }
            Ok(())
        })();
        if let Err(e) = r {
            if is_storage_error(&e) {
                return Err(e);
            }
            errors.push(e.to_write_error(i));
            if ordered {
                break;
            }
        }
    }
    coll.finish(txn)?;
    Ok(with_write_errors(doc! {"n": n as i32, "ok": 1.0}, errors))
}

fn parse_delete_stmt(d: &Document) -> Result<(Matcher, Option<usize>)> {
    for k in d.keys() {
        if !matches!(k.as_str(), "q" | "limit" | "hint" | "collation") {
            return Err(Error::failed_to_parse(format!("BSON field 'delete.deletes.{k}' is an unknown field.")));
        }
    }
    if d.contains_key("collation") {
        return Err(Error::not_implemented("collations are not supported by Mango"));
    }
    let q = match d.get("q") {
        Some(Bson::Document(q)) => q,
        _ => return Err(Error::failed_to_parse("BSON field 'delete.deletes.q' is missing but a required field")),
    };
    let limit = match get_int(d, "limit")? {
        None => return Err(Error::failed_to_parse("BSON field 'delete.deletes.limit' is missing but a required field")),
        Some(0) => None,
        Some(1) => Some(1),
        Some(other) => return Err(Error::failed_to_parse(format!("The limit field in delete objects must be 0 or 1. Got {other}"))),
    };
    Ok((Matcher::parse(q)?, limit))
}

fn find_and_modify(txn: &WriteTransaction, ns: &str, spec: &Document, upsert_id: ObjectId, now: i64) -> Result<Document> {
    let empty = Document::new();
    let query = crate::bsonutil::get_doc(spec, "query")?.unwrap_or(&empty);
    let m = Matcher::parse(query)?;
    let remove = get_bool(spec, "remove")?.unwrap_or(false);
    let return_new = get_bool(spec, "new")?.unwrap_or(false);
    let upsert = get_bool(spec, "upsert")?.unwrap_or(false);
    let sort = crate::bsonutil::get_doc(spec, "sort")?.filter(|s| !s.is_empty()).map(SortSpec::parse).transpose()?;
    let fields = crate::bsonutil::get_doc(spec, "fields")?.map(|f| Projection::parse(f, true)).transpose()?;
    if spec.contains_key("collation") {
        return Err(Error::not_implemented("collations are not supported by Mango"));
    }
    let update = match spec.get("update") {
        Some(u) => Some(Update::parse(u)?),
        None => None,
    };
    if remove && (update.is_some() || return_new || upsert) {
        return Err(Error::failed_to_parse("Cannot specify both an update and remove=true (or upsert/new with remove)"));
    }
    if !remove && update.is_none() {
        return Err(Error::failed_to_parse("Either an update or remove=true must be specified"));
    }
    let filters = parse_array_filters(spec.get("arrayFilters"))?;
    if let Some(u) = &update {
        check_array_filters_used(u, &filters)?;
    }
    let project = |d: Document| -> Result<Bson> {
        Ok(Bson::Document(match &fields {
            Some(p) => p.apply(&d, Some(&m))?,
            None => d,
        }))
    };

    let mut coll = WriteColl::open(txn, ns)?;
    let mut found = match &coll {
        Some(c) => c.find(txn, &m, if sort.is_some() { None } else { Some(1) })?,
        None => vec![],
    };
    if let Some(s) = &sort {
        let mut keyed: Vec<_> = found.into_iter().map(|(k, d)| (s.key(&d), k, d)).collect();
        keyed.sort_by(|a, b| s.compare_keys(&a.0, &b.0));
        found = keyed.into_iter().take(1).map(|(_, k, d)| (k, d)).collect();
    }
    let target = found.into_iter().next();
    let reply = match (target, remove) {
        (None, _) if !(upsert && !remove) => doc! {"lastErrorObject": {"n": 0, "updatedExisting": false}, "value": Bson::Null, "ok": 1.0},
        (None, _) => {
            let u = update.unwrap();
            let ctx = UpdateCtx { is_insert: true, now_millis: now, query: Some(&m), array_filters: &filters };
            let new = upsert_document(&m, &u, &ctx, upsert_id)?;
            let id = new.get("_id").cloned().unwrap_or(Bson::Null);
            if coll.is_none() {
                coll = Some(WriteColl::open_or_create(txn, ns, 0)?);
            }
            coll.as_mut().unwrap().insert(txn, new.clone())?;
            let value = if return_new { project(storage::prepare_for_storage(new)?)? } else { Bson::Null };
            doc! {"lastErrorObject": {"n": 1, "updatedExisting": false, "upserted": id}, "value": value, "ok": 1.0}
        }
        (Some((idk, old)), true) => {
            coll.as_mut().unwrap().delete(txn, &idk, &old)?;
            doc! {"lastErrorObject": {"n": 1}, "value": project(old)?, "ok": 1.0}
        }
        (Some((idk, old)), false) => {
            let u = update.unwrap();
            let ctx = UpdateCtx { is_insert: false, now_millis: now, query: Some(&m), array_filters: &filters };
            let new = u.apply(&old, &ctx)?;
            let stored = if doc_bytes_equal(&old, &new) { old.clone() } else { coll.as_mut().unwrap().replace(txn, &idk, &old, new)? };
            let value = if return_new { project(stored)? } else { project(old)? };
            doc! {"lastErrorObject": {"n": 1, "updatedExisting": true}, "value": value, "ok": 1.0}
        }
    };
    if let Some(c) = coll {
        c.finish(txn)?;
    }
    Ok(reply)
}

fn create(txn: &WriteTransaction, ns: &str, options: &Document) -> Result<Document> {
    if WriteColl::open(txn, ns)?.is_some() {
        return Err(Error::namespace_exists(format!("Collection {ns} already exists.")));
    }
    for (k, v) in options {
        match k.as_str() {
            "capped" if crate::bsonutil::truthy(v) => return Err(Error::not_implemented("capped collections are not supported by Mango")),
            "timeseries" | "clusteredIndex" | "viewOn" | "pipeline" | "validator" | "collation" | "encryptedFields" | "changeStreamPreAndPostImages" => {
                return Err(Error::not_implemented(format!("collection option '{k}' is not supported by Mango")));
            }
            _ => {}
        }
    }
    let mut c = WriteColl::open_or_create(txn, ns, 0)?;
    c.meta.options = options.clone();
    c.dirty = true;
    c.finish(txn)?;
    Ok(doc! {"ok": 1.0})
}

fn drop_coll(txn: &WriteTransaction, ns: &str) -> Result<Document> {
    match WriteColl::open(txn, ns)? {
        Some(c) => {
            let n_indexes = c.meta.indexes.len() as i32 + 1;
            c.drop(txn)?;
            Ok(doc! {"nIndexesWas": n_indexes, "ns": ns, "ok": 1.0})
        }
        None => Ok(doc! {"ok": 1.0}),
    }
}

fn drop_database(txn: &WriteTransaction, db: &str) -> Result<Document> {
    let prefix = format!("{db}.");
    let names: Vec<String> = {
        let cat = txn.open_table(CATALOG)?;
        let end = crate::storage::prefix_successor(prefix.as_bytes());
        let end = String::from_utf8(end).unwrap_or_else(|_| format!("{db}/"));
        let mut v = Vec::new();
        for item in cat.range(prefix.as_str()..end.as_str())? {
            let (k, _) = item?;
            v.push(k.value().to_string());
        }
        v
    };
    for ns in names {
        if let Some(c) = WriteColl::open(txn, &ns)? {
            c.drop(txn)?;
        }
    }
    Ok(doc! {"dropped": db, "ok": 1.0})
}

fn create_indexes(txn: &WriteTransaction, ns: &str, specs: &[Document]) -> Result<Document> {
    let existed = WriteColl::open(txn, ns)?.is_some();
    // Validate everything before creating the collection.
    let mut parsed = Vec::new();
    for s in specs {
        let key = s.get_document("key").map_err(|_| Error::cannot_create_index("index specification must have a 'key' document"))?;
        if key.len() == 1 && key.contains_key("_id") && crate::bsonutil::as_f64(key.get("_id").unwrap()) == Some(1.0) {
            if s.get_bool("unique").ok() == Some(false) {
                return Err(Error::invalid_options("The _id index must be unique"));
            }
            continue; // the implicit _id index
        }
        parsed.push(parse_index_spec(s, 0)?);
    }
    let mut coll = WriteColl::open_or_create(txn, ns, 0)?;
    let before = coll.meta.indexes.len() as i32 + 1;
    let mut note = None;
    for mut ix in parsed {
        if let Some(existing) = coll.meta.indexes.iter().find(|e| e.name == ix.name) {
            let same = existing.key == ix.key
                && existing.unique == ix.unique
                && existing.sparse == ix.sparse
                && existing.partial == ix.partial
                && existing.expire_after_seconds == ix.expire_after_seconds;
            if same {
                note = Some("all indexes already exist");
                continue;
            }
            if existing.key == ix.key {
                return Err(Error::index_options_conflict(format!(
                    "An existing index has the same name as the requested index but different options. Requested index: {}, existing index: {}",
                    ix.to_spec(),
                    existing.to_spec()
                )));
            }
            return Err(Error::index_key_specs_conflict(format!(
                "An existing index has the same name as the requested index but a different key. Requested index: {}, existing index: {}",
                ix.to_spec(),
                existing.to_spec()
            )));
        }
        if let Some(existing) = coll.meta.indexes.iter().find(|e| e.key == ix.key && e.partial == ix.partial) {
            return Err(Error::index_options_conflict(format!(
                "Index already exists with a different name: {}",
                existing.name
            )));
        }
        ix.id = coll.meta.next_index_id;
        coll.meta.next_index_id += 1;
        coll.dirty = true;
        coll.add_index(txn, ix)?;
    }
    let after = coll.meta.indexes.len() as i32 + 1;
    coll.dirty = true;
    coll.finish(txn)?;
    let mut reply = doc! {"numIndexesBefore": before, "numIndexesAfter": after, "createdCollectionAutomatically": !existed};
    if let Some(n) = note {
        reply.insert("note", n);
    }
    reply.insert("ok", 1.0);
    Ok(reply)
}

fn drop_indexes(txn: &WriteTransaction, ns: &str, index: &Bson) -> Result<Document> {
    let Some(mut coll) = WriteColl::open(txn, ns)? else {
        return Err(Error::namespace_not_found(format!("ns not found {ns}")));
    };
    let was = coll.meta.indexes.len() as i32 + 1;
    let names: Vec<String> = match index {
        Bson::String(s) if s == "*" => coll.meta.indexes.iter().map(|i| i.name.clone()).collect(),
        Bson::String(s) => vec![s.clone()],
        Bson::Array(a) => a
            .iter()
            .map(|v| match v {
                Bson::String(s) => Ok(s.clone()),
                _ => Err(Error::type_mismatch("dropIndexes 'index' array must contain strings")),
            })
            .collect::<Result<_>>()?,
        Bson::Document(key) => {
            if key == &doc! {"_id": 1} {
                return Err(Error::invalid_options("cannot drop _id index"));
            }
            match coll.meta.indexes.iter().find(|i| &i.key == key) {
                Some(i) => vec![i.name.clone()],
                None => return Err(Error::index_not_found(format!("can't find index with key: {key}"))),
            }
        }
        _ => return Err(Error::type_mismatch("dropIndexes 'index' must be a string, array or document")),
    };
    for n in &names {
        if n == "_id_" {
            return Err(Error::invalid_options("cannot drop _id index"));
        }
        if !coll.meta.indexes.iter().any(|i| &i.name == n) {
            return Err(Error::index_not_found(format!("index not found with name [{n}]")));
        }
    }
    for n in names {
        coll.drop_index(txn, &n)?;
    }
    coll.finish(txn)?;
    Ok(doc! {"nIndexesWas": was, "ok": 1.0})
}

fn rename(txn: &WriteTransaction, from: &str, to: &str, drop_target: bool) -> Result<Document> {
    let (tdb, tcoll) = split_ns(to);
    storage::validate_db_name(tdb)?;
    storage::validate_coll_name(tcoll)?;
    if from == to {
        return Err(Error::illegal_operation("Can't rename a collection to itself"));
    }
    let Some(src) = WriteColl::open(txn, from)? else {
        return Err(Error::namespace_not_found(format!("Source collection {from} does not exist")));
    };
    if let Some(target) = WriteColl::open(txn, to)? {
        if !drop_target {
            return Err(Error::namespace_exists(format!("target namespace exists")));
        }
        target.drop(txn)?;
    }
    let mut cat = txn.open_table(CATALOG)?;
    cat.remove(from)?;
    let bytes = bson::serialize_to_vec(&src.meta).map_err(|e| Error::internal(format!("encode catalog: {e}")))?;
    cat.insert(to, bytes.as_slice())?;
    Ok(doc! {"ok": 1.0})
}

fn put_user(txn: &WriteTransaction, key: &str, user: &Document, create: bool) -> Result<Document> {
    let mut t = txn.open_table(USERS)?;
    let exists = t.get(key)?.is_some();
    if create && exists {
        return Err(Error { code: 51003, code_name: "Location51003", msg: format!("User \"{key}\" already exists") });
    }
    if !create && !exists {
        return Err(Error { code: 11, code_name: "UserNotFound", msg: format!("User {key} not found") });
    }
    t.insert(key, doc_to_bytes(user)?.as_slice())?;
    Ok(doc! {"ok": 1.0})
}

fn drop_user(txn: &WriteTransaction, key: &str) -> Result<Document> {
    let mut t = txn.open_table(USERS)?;
    if t.remove(key)?.is_none() {
        return Err(Error { code: 11, code_name: "UserNotFound", msg: format!("User {key} not found") });
    }
    Ok(doc! {"ok": 1.0})
}

fn expire(txn: &WriteTransaction, ns: &str, field: &str, cutoff: i64) -> Result<Document> {
    let Some(mut coll) = WriteColl::open(txn, ns)? else { return Ok(doc! {"n": 0, "ok": 1.0}) };
    let m = Matcher::parse(&doc! {field: {"$lt": bson::DateTime::from_millis(cutoff)}})?;
    let mut n = 0;
    for (idk, old) in coll.find(txn, &m, None)? {
        coll.delete(txn, &idk, &old)?;
        n += 1;
    }
    coll.finish(txn)?;
    Ok(doc! {"n": n, "ok": 1.0})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Store;

    fn run(s: &Store, cmd: Command) -> std::result::Result<Document, Error> {
        let txn = s.begin_write().unwrap();
        let r = apply(&txn, &Proposal { cmd, now_millis: 1_700_000_000_000, session: None }, 1).unwrap();
        txn.commit().unwrap();
        r
    }

    #[test]
    fn crud_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.redb")).unwrap();
        let r = run(&s, Command::Insert { ns: "d.c".into(), docs: vec![doc! {"_id": 1, "a": 1}, doc! {"_id": 1}, doc! {"_id": 2, "a": 2}], ordered: false }).unwrap();
        assert_eq!(r.get_i32("n").unwrap(), 2);
        assert_eq!(r.get_array("writeErrors").unwrap().len(), 1);
        let r = run(
            &s,
            Command::Update {
                ns: "d.c".into(),
                updates: vec![doc! {"q": {}, "u": {"$inc": {"a": 10}}, "multi": true}, doc! {"q": {"_id": 9}, "u": {"$set": {"x": 1}}, "upsert": true}],
                ordered: true,
                upsert_ids: vec![ObjectId::new(), ObjectId::new()],
            },
        )
        .unwrap();
        assert_eq!(r.get_i32("n").unwrap(), 3);
        assert_eq!(r.get_i32("nModified").unwrap(), 2);
        assert_eq!(r.get_array("upserted").unwrap()[0].as_document().unwrap().get_i32("_id").unwrap(), 9);
        let r = run(&s, Command::Delete { ns: "d.c".into(), deletes: vec![doc! {"q": {"a": {"$gt": 11}}, "limit": 0}], ordered: true }).unwrap();
        assert_eq!(r.get_i32("n").unwrap(), 1);
        let r = run(
            &s,
            Command::FindAndModify { ns: "d.c".into(), spec: doc! {"query": {"_id": 1}, "update": {"$set": {"z": 1}}, "new": true}, upsert_id: ObjectId::new() },
        )
        .unwrap();
        assert_eq!(r.get_document("value").unwrap(), &doc! {"_id": 1, "a": 11, "z": 1});
        let r = run(&s, Command::CreateIndexes { ns: "d.c".into(), indexes: vec![doc! {"key": {"z": 1}, "name": "z_1", "unique": true}] }).unwrap();
        assert_eq!(r.get_i32("numIndexesAfter").unwrap(), 2);
        let e = run(&s, Command::Insert { ns: "d.c".into(), docs: vec![doc! {"_id": 5, "z": 1}], ordered: true }).unwrap();
        assert_eq!(e.get_array("writeErrors").unwrap()[0].as_document().unwrap().get_i32("code").unwrap(), 11000);
        run(&s, Command::Rename { from: "d.c".into(), to: "d.c2".into(), drop_target: false }).unwrap();
        let snap = s.snapshot().unwrap();
        assert!(snap.meta("d.c").unwrap().is_none());
        assert_eq!(snap.count_all("d.c2").unwrap(), 2);
    }

    #[test]
    fn retryable_writes_apply_once() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.redb")).unwrap();
        let session = Some(SessionTxn { lsid: Bson::Document(doc! {"id": "abc"}), txn_number: 1 });
        let p = Proposal {
            cmd: Command::Update { ns: "d.c".into(), updates: vec![doc! {"q": {"_id": 1}, "u": {"$inc": {"n": 1}}, "upsert": true}], ordered: true, upsert_ids: vec![ObjectId::new()] },
            now_millis: 0,
            session,
        };
        for _ in 0..3 {
            let txn = s.begin_write().unwrap();
            apply(&txn, &p, 2).unwrap().unwrap();
            txn.commit().unwrap();
        }
        let snap = s.snapshot().unwrap();
        let docs = snap.find("d.c", &Matcher::Always, &storage::ScanOpts { limit: None }).unwrap();
        assert_eq!(docs, vec![doc! {"_id": 1, "n": 1}]);
    }
}
