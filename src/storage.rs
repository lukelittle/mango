//! Durable storage on top of redb: the collection catalog, documents,
//! secondary indexes, the query planner, and the Raft log/metadata tables.
//!
//! Layout (all in one redb file, so a state-machine step and its
//! `applied_index` commit atomically):
//!
//! * `catalog`:  "db.coll" -> CollMeta (BSON)
//! * `docs`:     coll_id(8) ++ keystring(_id) -> document (BSON)
//! * `indexes`:  coll_id(8) ++ index_id(4) ++ keystring(k1)..keystring(kn) ++ keystring(_id) -> keystring(_id)
//! * `sessions`: keystring(lsid) -> retryable-write record
//! * `users`:    "db.user" -> credentials
//! * `meta`, `raft_log`: consensus state (see `raft::log`)

use crate::bsonutil::{doc_from_bytes, doc_size, doc_to_bytes, type_name};
use crate::error::{Error, Result};
use crate::keystring::{self, TAG_NULL, bracket_end, bracket_start, type_bracket};
use crate::query::{CmpOp, InItem, Matcher, Pred};
use bson::{Bson, Document, doc};
use redb::{Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::path::Path;

pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
pub const CATALOG: TableDefinition<&str, &[u8]> = TableDefinition::new("catalog");
pub const DOCS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("docs");
pub const INDEXES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("indexes");
pub const SESSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("sessions");
pub const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");

pub const MAX_DOC_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexMeta {
    pub id: i64,
    pub name: String,
    pub key: Document,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub sparse: bool,
    #[serde(default)]
    pub partial: Option<Document>,
    #[serde(default)]
    pub expire_after_seconds: Option<i64>,
    #[serde(default)]
    pub multikey: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollMeta {
    pub id: i64,
    pub indexes: Vec<IndexMeta>,
    pub next_index_id: i64,
    #[serde(default)]
    pub options: Document,
    /// Stable identifier reported as the collection UUID.
    #[serde(default)]
    pub uuid: Option<bson::Binary>,
}

impl IndexMeta {
    pub fn key_paths(&self) -> Vec<Vec<String>> {
        self.key.keys().map(|k| k.split('.').map(String::from).collect()).collect()
    }

    /// The index spec as reported by `listIndexes`.
    pub fn to_spec(&self) -> Document {
        let mut d = doc! {"v": 2, "key": self.key.clone(), "name": self.name.clone()};
        if self.unique {
            d.insert("unique", true);
        }
        if self.sparse {
            d.insert("sparse", true);
        }
        if let Some(p) = &self.partial {
            d.insert("partialFilterExpression", p.clone());
        }
        if let Some(e) = self.expire_after_seconds {
            d.insert("expireAfterSeconds", e);
        }
        d
    }

    /// Whether the planner may use this index to find candidates. Sparse and
    /// partial indexes don't contain every document, so they are only used
    /// for their constraints.
    fn plannable(&self) -> bool {
        !self.sparse && self.partial.is_none()
    }
}

pub fn split_ns(ns: &str) -> (&str, &str) {
    match ns.split_once('.') {
        Some((d, c)) => (d, c),
        None => (ns, ""),
    }
}

pub fn validate_db_name(db: &str) -> Result<()> {
    if db.is_empty() || db.len() > 63 || db.contains(['/', '\\', '.', ' ', '"', '$', '\0']) {
        return Err(Error::invalid_namespace(format!("Invalid database name: '{db}'")));
    }
    Ok(())
}

pub fn validate_coll_name(coll: &str) -> Result<()> {
    if coll.is_empty() || coll.contains('$') || coll.contains('\0') || coll.starts_with('.') {
        return Err(Error::invalid_namespace(format!("Invalid collection name: '{coll}'")));
    }
    if coll.starts_with("system.") {
        return Err(Error::invalid_namespace(format!("Invalid system namespace: {coll}")));
    }
    Ok(())
}

fn coll_prefix(id: i64) -> [u8; 8] {
    (id as u64).to_be_bytes()
}

fn index_prefix(coll: i64, idx: i64) -> Vec<u8> {
    let mut v = coll_prefix(coll).to_vec();
    v.extend_from_slice(&(idx as u32).to_be_bytes());
    v
}

/// Smallest byte string greater than every string with prefix `p`.
pub fn prefix_successor(p: &[u8]) -> Vec<u8> {
    let mut v = p.to_vec();
    while let Some(last) = v.pop() {
        if last != 0xFF {
            v.push(last + 1);
            return v;
        }
    }
    vec![0xFF; p.len() + 1] // unreachable for our key formats
}

pub fn id_key(doc: &Document) -> Result<Vec<u8>> {
    let id = doc.get("_id").ok_or_else(|| Error::internal("document has no _id"))?;
    Ok(keystring::encode(id))
}

fn doc_key(coll: i64, id_key: &[u8]) -> Vec<u8> {
    let mut k = coll_prefix(coll).to_vec();
    k.extend_from_slice(id_key);
    k
}

/// Validates a document before it is stored and moves `_id` to the front.
pub fn prepare_for_storage(mut doc: Document) -> Result<Document> {
    match doc.get("_id") {
        None => return Err(Error::internal("document to store has no _id")),
        Some(Bson::Array(_)) => return Err(Error::invalid_id_field("The '_id' value cannot be of type array")),
        Some(Bson::RegularExpression(_)) => return Err(Error::invalid_id_field("The '_id' value cannot be of type regex")),
        Some(Bson::Undefined) => return Err(Error::invalid_id_field("The '_id' value cannot be of type undefined")),
        Some(Bson::Document(d)) => {
            if d.keys().any(|k| k.starts_with('$')) {
                return Err(Error::dollar_prefixed_field("_id fields may not contain '$'-prefixed fields"));
            }
        }
        _ => {}
    }
    for k in doc.keys() {
        if k.starts_with('$') {
            return Err(Error::dollar_prefixed_field(format!(
                "Document can't have $ prefixed field names: {k}"
            )));
        }
    }
    if doc.keys().next().map(String::as_str) != Some("_id") {
        let id = doc.remove("_id").unwrap();
        let mut d = doc! {"_id": id};
        d.extend(doc);
        doc = d;
    }
    let size = doc_size(&doc);
    if size > MAX_DOC_SIZE {
        return Err(Error::bson_too_large(format!("object to insert too large. size in bytes: {size}, max size: {MAX_DOC_SIZE}")));
    }
    Ok(doc)
}

// ---------- index keys ----------

struct FieldValues {
    values: Vec<Bson>,
    is_array: bool,
    found: bool,
}

fn field_values(doc: &Document, parts: &[String]) -> FieldValues {
    let mut fv = FieldValues { values: Vec::new(), is_array: false, found: false };
    match doc.get(&parts[0]) {
        None => fv.values.push(Bson::Null),
        Some(v) => walk(v, &parts[1..], &mut fv),
    }
    fv
}

fn walk(v: &Bson, rest: &[String], fv: &mut FieldValues) {
    if rest.is_empty() {
        fv.found = true;
        match v {
            Bson::Array(a) => {
                fv.is_array = true;
                if a.is_empty() {
                    fv.values.push(Bson::Null);
                }
                fv.values.extend(a.iter().cloned());
            }
            v => fv.values.push(v.clone()),
        }
        return;
    }
    match v {
        Bson::Document(d) => match d.get(&rest[0]) {
            None => fv.values.push(Bson::Null),
            Some(x) => walk(x, &rest[1..], fv),
        },
        Bson::Array(a) => {
            fv.is_array = true;
            if a.is_empty() {
                fv.values.push(Bson::Null);
            }
            for e in a {
                match e {
                    Bson::Document(d) => match d.get(&rest[0]) {
                        None => fv.values.push(Bson::Null),
                        Some(x) => walk(x, &rest[1..], fv),
                    },
                    _ => fv.values.push(Bson::Null),
                }
            }
        }
        _ => fv.values.push(Bson::Null),
    }
}

/// Encoded keys (without prefix or _id suffix) for `doc` in `index`, and
/// whether the document makes the index multikey. `None` means the document
/// is not indexed (sparse/partial).
pub fn index_keys(index: &IndexMeta, doc: &Document) -> Result<Option<(BTreeSet<Vec<u8>>, bool)>> {
    if let Some(pf) = &index.partial {
        if !Matcher::parse(pf)?.matches(doc)? {
            return Ok(None);
        }
    }
    let fields: Vec<FieldValues> = index.key_paths().iter().map(|p| field_values(doc, p)).collect();
    if index.sparse && !fields.iter().any(|f| f.found) {
        return Ok(None);
    }
    let arrays = fields.iter().filter(|f| f.is_array).count();
    if arrays > 1 {
        return Err(Error::cannot_index_parallel_arrays(format!(
            "cannot index parallel arrays [{}]",
            index.key.keys().cloned().collect::<Vec<_>>().join("] [")
        )));
    }
    let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
    keys.insert(Vec::new());
    for f in &fields {
        let mut next = BTreeSet::new();
        let encoded: BTreeSet<Vec<u8>> = f.values.iter().map(keystring::encode).collect();
        for prefix in &keys {
            for e in &encoded {
                let mut k = prefix.clone();
                k.extend_from_slice(e);
                next.insert(k);
            }
        }
        keys = next;
    }
    Ok(Some((keys, arrays > 0)))
}

pub fn parse_index_spec(spec: &Document, id: i64) -> Result<IndexMeta> {
    let Some(Bson::Document(key)) = spec.get("key") else {
        return Err(Error::cannot_create_index("index specification must have a 'key' document"));
    };
    if key.is_empty() {
        return Err(Error::cannot_create_index("Index keys cannot be empty."));
    }
    for (k, v) in key {
        if k.is_empty() || k.split('.').any(|p| p.is_empty() || p.starts_with('$')) {
            return Err(Error::cannot_create_index(format!("Index key contains an illegal field name: '{k}'")));
        }
        match v {
            Bson::String(s) => {
                return Err(Error::not_implemented(format!("'{s}' indexes are not supported by Mango")));
            }
            v => match crate::bsonutil::as_f64(v) {
                Some(f) if f != 0.0 && f.is_finite() => {}
                _ => return Err(Error::cannot_create_index(format!("Values in the index key pattern can't be 0 or non-numeric; found {k}: {v}"))),
            },
        }
    }
    let name = match spec.get("name") {
        Some(Bson::String(n)) if !n.is_empty() => n.clone(),
        Some(_) => return Err(Error::cannot_create_index("The index name must be a non-empty string")),
        None => key.iter().map(|(k, v)| format!("{k}_{v}")).collect::<Vec<_>>().join("_"),
    };
    let flag = |k: &str| -> Result<bool> { Ok(crate::bsonutil::get_bool(spec, k)?.unwrap_or(false)) };
    let partial = match spec.get("partialFilterExpression") {
        None => None,
        Some(Bson::Document(p)) => {
            Matcher::parse(p)?;
            Some(p.clone())
        }
        Some(_) => return Err(Error::type_mismatch("partialFilterExpression must be an object")),
    };
    let expire = match spec.get("expireAfterSeconds") {
        None => None,
        Some(v) => match crate::bsonutil::as_i64(v) {
            Some(s) if s >= 0 => Some(s),
            _ => return Err(Error::cannot_create_index("expireAfterSeconds must be a non-negative integer")),
        },
    };
    if expire.is_some() && key.len() != 1 {
        return Err(Error::cannot_create_index("TTL indexes are single-field indexes, compound indexes do not support TTL"));
    }
    for k in spec.keys() {
        if !matches!(
            k.as_str(),
            "key" | "name" | "unique" | "sparse" | "partialFilterExpression" | "expireAfterSeconds" | "v" | "background" | "ns" | "hidden"
                | "collation"
        ) {
            return Err(Error::invalid_options(format!("The field '{k}' is not valid for an index specification")));
        }
    }
    if spec.contains_key("collation") {
        return Err(Error::not_implemented("collations are not supported by Mango"));
    }
    let unique = flag("unique")?;
    if name == "_id_" && (key.len() != 1 || !key.contains_key("_id")) {
        return Err(Error::bad_value("The index name '_id_' is reserved for the _id index"));
    }
    Ok(IndexMeta { id, name, key: key.clone(), unique, sparse: flag("sparse")?, partial, expire_after_seconds: expire, multikey: false })
}

// ---------- query planning ----------

#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    CollScan,
    IdPoints(Vec<Vec<u8>>),
    IdRange(Vec<u8>, Vec<u8>),
    IndexPoints { index: String, keys: Vec<Vec<u8>> },
    IndexRange { index: String, lo: Vec<u8>, hi: Vec<u8> },
}

impl Plan {
    pub fn describe(&self) -> Document {
        match self {
            Plan::CollScan => doc! {"stage": "COLLSCAN"},
            Plan::IdPoints(k) => doc! {"stage": "IDHACK", "keys": k.len() as i64},
            Plan::IdRange(..) => doc! {"stage": "IXSCAN", "indexName": "_id_"},
            Plan::IndexPoints { index, keys } => doc! {"stage": "IXSCAN", "indexName": index, "points": keys.len() as i64},
            Plan::IndexRange { index, .. } => doc! {"stage": "IXSCAN", "indexName": index},
        }
    }
}

/// Values usable as exact index lookup points.
fn point_value(v: &Bson) -> bool {
    !matches!(v, Bson::Array(_) | Bson::MinKey | Bson::MaxKey | Bson::Undefined)
}

/// Encoded [lo, hi) bounds for the conjunct predicates on one field, if any
/// is a range predicate usable with an index.
fn range_bounds(preds: &[&Pred], intersect: bool) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut lo: Option<Vec<u8>> = None;
    let mut hi: Option<Vec<u8>> = None;
    let mut bracket: Option<u8> = None;
    for p in preds {
        let Pred::Cmp(op, v) = p else { continue };
        if matches!(v, Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey | Bson::Array(_)) {
            continue;
        }
        let b = type_bracket(v);
        if bracket.is_some_and(|x| x != b) {
            continue;
        }
        let enc = keystring::encode(v);
        match op {
            CmpOp::Gt | CmpOp::Gte => {
                if lo.is_some() {
                    continue;
                }
                lo = Some(if *op == CmpOp::Gt { prefix_successor(&enc) } else { enc });
            }
            CmpOp::Lt | CmpOp::Lte => {
                if hi.is_some() {
                    continue;
                }
                hi = Some(if *op == CmpOp::Lte { prefix_successor(&enc) } else { enc });
            }
        }
        bracket = Some(b);
        if !intersect {
            break;
        }
    }
    let b = bracket?;
    Some((lo.unwrap_or_else(|| bracket_start(b)), hi.unwrap_or_else(|| bracket_end(b))))
}

pub fn choose_plan(meta: &CollMeta, m: &Matcher) -> Plan {
    let conj = m.conjuncts();
    let preds_for = |path: &str| -> Vec<&Pred> { conj.iter().filter(|(p, _)| *p == path).map(|(_, pr)| *pr).collect() };

    // _id equality / $in
    let id_preds = preds_for("_id");
    for p in &id_preds {
        match p {
            Pred::Eq(v) if point_value(v) => return Plan::IdPoints(vec![keystring::encode(v)]),
            Pred::In(items) if items.iter().all(|i| matches!(i, InItem::Value(v) if point_value(v))) => {
                let mut keys: Vec<Vec<u8>> = items
                    .iter()
                    .map(|i| match i {
                        InItem::Value(v) => keystring::encode(v),
                        _ => unreachable!(),
                    })
                    .collect();
                keys.sort();
                keys.dedup();
                return Plan::IdPoints(keys);
            }
            _ => {}
        }
    }
    // secondary index equality / $in
    for idx in meta.indexes.iter().filter(|i| i.plannable()) {
        let first = idx.key.keys().next().unwrap();
        for p in preds_for(first) {
            match p {
                Pred::Eq(v) if point_value(v) => {
                    return Plan::IndexPoints { index: idx.name.clone(), keys: vec![keystring::encode(v)] };
                }
                Pred::In(items) if !items.is_empty() && items.iter().all(|i| matches!(i, InItem::Value(v) if point_value(v))) => {
                    let mut keys: Vec<Vec<u8>> = items
                        .iter()
                        .map(|i| match i {
                            InItem::Value(v) => keystring::encode(v),
                            _ => unreachable!(),
                        })
                        .collect();
                    keys.sort();
                    keys.dedup();
                    return Plan::IndexPoints { index: idx.name.clone(), keys };
                }
                _ => {}
            }
        }
    }
    // ranges
    if let Some((lo, hi)) = range_bounds(&id_preds, true) {
        return Plan::IdRange(lo, hi);
    }
    for idx in meta.indexes.iter().filter(|i| i.plannable()) {
        let first = idx.key.keys().next().unwrap();
        if let Some((lo, hi)) = range_bounds(&preds_for(first), !idx.multikey) {
            return Plan::IndexRange { index: idx.name.clone(), lo, hi };
        }
    }
    Plan::CollScan
}

// ---------- reading ----------

/// Scan options: stop after `limit` matches (only when no sort is needed).
pub struct ScanOpts {
    pub limit: Option<usize>,
}

fn index_by_name<'m>(meta: &'m CollMeta, name: &str) -> Result<&'m IndexMeta> {
    meta.indexes.iter().find(|i| i.name == name).ok_or_else(|| Error::internal(format!("index {name} vanished")))
}

/// Finds matching documents using any readable pair of tables (read or
/// write transaction). Returns (id_key, document) pairs.
pub fn scan_matching<D, I>(docs: &D, idx: &I, meta: &CollMeta, m: &Matcher, plan: &Plan, opts: &ScanOpts) -> Result<Vec<(Vec<u8>, Document)>>
where
    D: ReadableTable<&'static [u8], &'static [u8]>,
    I: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut out = Vec::new();
    let full = |out: &Vec<(Vec<u8>, Document)>| opts.limit.is_some_and(|l| out.len() >= l);
    let cp = coll_prefix(meta.id);
    let consider = |idk: &[u8], bytes: &[u8], out: &mut Vec<(Vec<u8>, Document)>| -> Result<()> {
        let d = doc_from_bytes(bytes)?;
        if m.matches(&d)? {
            out.push((idk.to_vec(), d));
        }
        Ok(())
    };
    match plan {
        Plan::CollScan | Plan::IdRange(..) => {
            let (lo, hi) = match plan {
                Plan::IdRange(lo, hi) => (doc_key(meta.id, lo), doc_key(meta.id, hi)),
                _ => (cp.to_vec(), prefix_successor(&cp)),
            };
            for item in docs.range(lo.as_slice()..hi.as_slice())? {
                let (k, v) = item?;
                consider(&k.value()[8..], v.value(), &mut out)?;
                if full(&out) {
                    break;
                }
            }
        }
        Plan::IdPoints(keys) => {
            for idk in keys {
                if let Some(v) = docs.get(doc_key(meta.id, idk).as_slice())? {
                    consider(idk, v.value(), &mut out)?;
                    if full(&out) {
                        break;
                    }
                }
            }
        }
        Plan::IndexPoints { index, keys } => {
            let ix = index_by_name(meta, index)?;
            let base = index_prefix(meta.id, ix.id);
            let mut seen = HashSet::new();
            'outer: for k in keys {
                let mut p = base.clone();
                p.extend_from_slice(k);
                let end = prefix_successor(&p);
                for item in idx.range(p.as_slice()..end.as_slice())? {
                    let (_, v) = item?;
                    let idk = v.value().to_vec();
                    if !seen.insert(idk.clone()) {
                        continue;
                    }
                    if let Some(dv) = docs.get(doc_key(meta.id, &idk).as_slice())? {
                        consider(&idk, dv.value(), &mut out)?;
                        if full(&out) {
                            break 'outer;
                        }
                    }
                }
            }
        }
        Plan::IndexRange { index, lo, hi } => {
            let ix = index_by_name(meta, index)?;
            let base = index_prefix(meta.id, ix.id);
            let mut a = base.clone();
            a.extend_from_slice(lo);
            let mut b = base.clone();
            b.extend_from_slice(hi);
            let mut seen = HashSet::new();
            for item in idx.range(a.as_slice()..b.as_slice())? {
                let (_, v) = item?;
                let idk = v.value().to_vec();
                if !seen.insert(idk.clone()) {
                    continue;
                }
                if let Some(dv) = docs.get(doc_key(meta.id, &idk).as_slice())? {
                    consider(&idk, dv.value(), &mut out)?;
                    if full(&out) {
                        break;
                    }
                }
            }
        }
    }
    Ok(out)
}

pub fn read_meta<T: ReadableTable<&'static str, &'static [u8]>>(catalog: &T, ns: &str) -> Result<Option<CollMeta>> {
    match catalog.get(ns)? {
        None => Ok(None),
        Some(v) => Ok(Some(bson::deserialize_from_slice(v.value()).map_err(|e| Error::internal(format!("corrupt catalog: {e}")))?)),
    }
}

/// A consistent read-only view of the database (an MVCC snapshot).
pub struct Snapshot {
    txn: ReadTransaction,
}

impl Snapshot {
    pub fn meta(&self, ns: &str) -> Result<Option<CollMeta>> {
        let cat = self.txn.open_table(CATALOG)?;
        read_meta(&cat, ns)
    }

    /// All namespaces, sorted.
    pub fn namespaces(&self) -> Result<Vec<(String, CollMeta)>> {
        let cat = self.txn.open_table(CATALOG)?;
        let mut out = Vec::new();
        for item in cat.iter()? {
            let (k, v) = item?;
            let meta: CollMeta = bson::deserialize_from_slice(v.value()).map_err(|e| Error::internal(format!("corrupt catalog: {e}")))?;
            out.push((k.value().to_string(), meta));
        }
        Ok(out)
    }

    pub fn find(&self, ns: &str, m: &Matcher, opts: &ScanOpts) -> Result<Vec<Document>> {
        let Some(meta) = self.meta(ns)? else { return Ok(vec![]) };
        let plan = choose_plan(&meta, m);
        let docs = self.txn.open_table(DOCS)?;
        let idx = self.txn.open_table(INDEXES)?;
        Ok(scan_matching(&docs, &idx, &meta, m, &plan, opts)?.into_iter().map(|(_, d)| d).collect())
    }

    pub fn plan(&self, ns: &str, m: &Matcher) -> Result<Plan> {
        Ok(match self.meta(ns)? {
            Some(meta) => choose_plan(&meta, m),
            None => Plan::CollScan,
        })
    }

    pub fn count_all(&self, ns: &str) -> Result<u64> {
        let Some(meta) = self.meta(ns)? else { return Ok(0) };
        let docs = self.txn.open_table(DOCS)?;
        let cp = coll_prefix(meta.id);
        let end = prefix_successor(&cp);
        let mut n = 0;
        for item in docs.range(cp.as_slice()..end.as_slice())? {
            item?;
            n += 1;
        }
        Ok(n)
    }

    /// (document count, total BSON bytes) of a collection.
    pub fn coll_stats(&self, ns: &str) -> Result<(u64, u64)> {
        let Some(meta) = self.meta(ns)? else { return Ok((0, 0)) };
        let docs = self.txn.open_table(DOCS)?;
        let cp = coll_prefix(meta.id);
        let end = prefix_successor(&cp);
        let (mut n, mut bytes) = (0u64, 0u64);
        for item in docs.range(cp.as_slice()..end.as_slice())? {
            let (_, v) = item?;
            n += 1;
            bytes += v.value().len() as u64;
        }
        Ok((n, bytes))
    }

    pub fn user(&self, db: &str, user: &str) -> Result<Option<Document>> {
        let t = self.txn.open_table(USERS)?;
        match t.get(format!("{db}.{user}").as_str())? {
            None => Ok(None),
            Some(v) => Ok(Some(doc_from_bytes(v.value())?)),
        }
    }

    pub fn users(&self) -> Result<Vec<Document>> {
        let t = self.txn.open_table(USERS)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, v) = item?;
            out.push(doc_from_bytes(v.value())?);
        }
        Ok(out)
    }

    pub fn txn(&self) -> &ReadTransaction {
        &self.txn
    }
}

pub struct Store {
    pub db: Database,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        let db = Database::create(path)?;
        let txn = db.begin_write()?;
        {
            txn.open_table(META)?;
            txn.open_table(LOG)?;
            txn.open_table(CATALOG)?;
            txn.open_table(DOCS)?;
            txn.open_table(INDEXES)?;
            txn.open_table(SESSIONS)?;
            txn.open_table(USERS)?;
        }
        txn.commit()?;
        Ok(Store { db })
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot { txn: self.db.begin_read()? })
    }

    pub fn begin_write(&self) -> Result<WriteTransaction> {
        Ok(self.db.begin_write()?)
    }
}

// ---------- writing ----------

/// A collection opened for writing inside a write transaction.
pub struct WriteColl {
    pub ns: String,
    pub meta: CollMeta,
    pub dirty: bool,
}

fn dup_key_error(ns: &str, index: &IndexMeta, doc: &Document) -> Error {
    let mut kv = Document::new();
    for k in index.key.keys() {
        let v = crate::update::get_at(doc, &k.split('.').map(String::from).collect::<Vec<_>>()).cloned().unwrap_or(Bson::Null);
        kv.insert(k.clone(), v);
    }
    let shown: Vec<String> = kv.iter().map(|(k, v)| format!("{k}: {v}")).collect();
    Error::duplicate_key(format!(
        "E11000 duplicate key error collection: {ns} index: {} dup key: {{ {} }}",
        index.name,
        shown.join(", ")
    ))
}

impl WriteColl {
    pub fn open(txn: &WriteTransaction, ns: &str) -> Result<Option<WriteColl>> {
        let cat = txn.open_table(CATALOG)?;
        Ok(read_meta(&cat, ns)?.map(|meta| WriteColl { ns: ns.to_string(), meta, dirty: false }))
    }

    /// Opens the collection, creating it (implicitly, as MongoDB does on first write).
    pub fn open_or_create(txn: &WriteTransaction, ns: &str, uuid_seed: u64) -> Result<WriteColl> {
        if let Some(c) = WriteColl::open(txn, ns)? {
            return Ok(c);
        }
        let (db, coll) = split_ns(ns);
        validate_db_name(db)?;
        validate_coll_name(coll)?;
        let id = next_coll_id(txn)?;
        let mut uuid = [0u8; 16];
        uuid[..8].copy_from_slice(&(id as u64).to_be_bytes());
        uuid[8..].copy_from_slice(&uuid_seed.to_be_bytes());
        let meta = CollMeta {
            id,
            indexes: vec![],
            next_index_id: 1,
            options: Document::new(),
            uuid: Some(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: uuid.to_vec() }),
        };
        let wc = WriteColl { ns: ns.to_string(), meta, dirty: true };
        wc.save(txn)?;
        Ok(WriteColl { dirty: false, ..wc })
    }

    pub fn save(&self, txn: &WriteTransaction) -> Result<()> {
        let mut cat = txn.open_table(CATALOG)?;
        let bytes = bson::serialize_to_vec(&self.meta).map_err(|e| Error::internal(format!("encode catalog: {e}")))?;
        cat.insert(self.ns.as_str(), bytes.as_slice())?;
        Ok(())
    }

    pub fn finish(self, txn: &WriteTransaction) -> Result<()> {
        if self.dirty { self.save(txn) } else { Ok(()) }
    }

    pub fn find(&self, txn: &WriteTransaction, m: &Matcher, limit: Option<usize>) -> Result<Vec<(Vec<u8>, Document)>> {
        let docs = txn.open_table(DOCS)?;
        let idx = txn.open_table(INDEXES)?;
        let plan = choose_plan(&self.meta, m);
        scan_matching(&docs, &idx, &self.meta, m, &plan, &ScanOpts { limit })
    }

    /// Computes the index entries for `doc`, failing on unique violations
    /// against documents other than `self_id`.
    fn entries_for(&mut self, txn: &WriteTransaction, doc: &Document, self_id: &[u8]) -> Result<Vec<(i64, BTreeSet<Vec<u8>>)>> {
        let idx_table = txn.open_table(INDEXES)?;
        let mut out = Vec::new();
        for i in 0..self.meta.indexes.len() {
            let index = &self.meta.indexes[i];
            let Some((keys, multikey)) = index_keys(index, doc)? else {
                out.push((index.id, BTreeSet::new()));
                continue;
            };
            if index.unique {
                let base = index_prefix(self.meta.id, index.id);
                for k in &keys {
                    let mut p = base.clone();
                    p.extend_from_slice(k);
                    let end = prefix_successor(&p);
                    for item in idx_table.range(p.as_slice()..end.as_slice())? {
                        let (_, v) = item?;
                        if v.value() != self_id {
                            return Err(dup_key_error(&self.ns, index, doc));
                        }
                    }
                }
            }
            if multikey && !index.multikey {
                self.meta.indexes[i].multikey = true;
                self.dirty = true;
            }
            out.push((self.meta.indexes[i].id, keys));
        }
        Ok(out)
    }

    pub fn insert(&mut self, txn: &WriteTransaction, doc: Document) -> Result<()> {
        let doc = prepare_for_storage(doc)?;
        let idk = id_key(&doc)?;
        let dk = doc_key(self.meta.id, &idk);
        {
            let docs = txn.open_table(DOCS)?;
            if docs.get(dk.as_slice())?.is_some() {
                let id_index = IndexMeta {
                    id: 0,
                    name: "_id_".into(),
                    key: doc! {"_id": 1},
                    unique: true,
                    sparse: false,
                    partial: None,
                    expire_after_seconds: None,
                    multikey: false,
                };
                return Err(dup_key_error(&self.ns, &id_index, &doc));
            }
        }
        let entries = self.entries_for(txn, &doc, &idk)?;
        let bytes = doc_to_bytes(&doc)?;
        txn.open_table(DOCS)?.insert(dk.as_slice(), bytes.as_slice())?;
        let mut idx = txn.open_table(INDEXES)?;
        for (index_id, keys) in entries {
            let base = index_prefix(self.meta.id, index_id);
            for k in keys {
                let mut full = base.clone();
                full.extend_from_slice(&k);
                full.extend_from_slice(&idk);
                idx.insert(full.as_slice(), idk.as_slice())?;
            }
        }
        Ok(())
    }

    /// Replaces a stored document (same _id) and maintains its index entries.
    pub fn replace(&mut self, txn: &WriteTransaction, idk: &[u8], old: &Document, new: Document) -> Result<Document> {
        let new = prepare_for_storage(new)?;
        let new_entries = self.entries_for(txn, &new, idk)?;
        let old_entries: Vec<(i64, BTreeSet<Vec<u8>>)> = self
            .meta
            .indexes
            .iter()
            .map(|i| Ok((i.id, index_keys(i, old)?.map(|(k, _)| k).unwrap_or_default())))
            .collect::<Result<_>>()?;
        let bytes = doc_to_bytes(&new)?;
        txn.open_table(DOCS)?.insert(doc_key(self.meta.id, idk).as_slice(), bytes.as_slice())?;
        let mut idx = txn.open_table(INDEXES)?;
        for ((index_id, old_keys), (_, new_keys)) in old_entries.iter().zip(new_entries.iter()) {
            let base = index_prefix(self.meta.id, *index_id);
            for k in old_keys.difference(new_keys) {
                let mut full = base.clone();
                full.extend_from_slice(k);
                full.extend_from_slice(idk);
                idx.remove(full.as_slice())?;
            }
            for k in new_keys.difference(old_keys) {
                let mut full = base.clone();
                full.extend_from_slice(k);
                full.extend_from_slice(idk);
                idx.insert(full.as_slice(), idk)?;
            }
        }
        Ok(new)
    }

    pub fn delete(&mut self, txn: &WriteTransaction, idk: &[u8], old: &Document) -> Result<()> {
        let mut idx = txn.open_table(INDEXES)?;
        for index in &self.meta.indexes {
            if let Some((keys, _)) = index_keys(index, old)? {
                let base = index_prefix(self.meta.id, index.id);
                for k in keys {
                    let mut full = base.clone();
                    full.extend_from_slice(&k);
                    full.extend_from_slice(idk);
                    idx.remove(full.as_slice())?;
                }
            }
        }
        txn.open_table(DOCS)?.remove(doc_key(self.meta.id, idk).as_slice())?;
        Ok(())
    }

    /// Builds a new index over existing documents. Fails (without changes)
    /// if existing documents violate it.
    pub fn add_index(&mut self, txn: &WriteTransaction, mut index: IndexMeta) -> Result<()> {
        let all = self.find(txn, &Matcher::Always, None)?;
        let base = index_prefix(self.meta.id, index.id);
        let mut pending: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut seen_unique: std::collections::HashMap<Vec<u8>, Vec<u8>> = std::collections::HashMap::new();
        for (idk, d) in &all {
            if let Some((keys, multikey)) = index_keys(&index, d)? {
                index.multikey |= multikey;
                for k in keys {
                    if index.unique
                        && let Some(other) = seen_unique.insert(k.clone(), idk.clone())
                        && other != *idk
                    {
                        return Err(dup_key_error(&self.ns, &index, d));
                    }
                    let mut full = base.clone();
                    full.extend_from_slice(&k);
                    full.extend_from_slice(idk);
                    pending.push((full, idk.clone()));
                }
            }
        }
        let mut idx = txn.open_table(INDEXES)?;
        for (k, v) in pending {
            idx.insert(k.as_slice(), v.as_slice())?;
        }
        self.meta.indexes.push(index);
        self.dirty = true;
        Ok(())
    }

    pub fn drop_index(&mut self, txn: &WriteTransaction, name: &str) -> Result<()> {
        let pos = self.meta.indexes.iter().position(|i| i.name == name).ok_or_else(|| Error::index_not_found(format!("index not found with name [{name}]")))?;
        let index = self.meta.indexes.remove(pos);
        let base = index_prefix(self.meta.id, index.id);
        let end = prefix_successor(&base);
        txn.open_table(INDEXES)?.retain_in(base.as_slice()..end.as_slice(), |_, _| false)?;
        self.dirty = true;
        Ok(())
    }

    /// Removes the collection, its documents and its index entries.
    pub fn drop(self, txn: &WriteTransaction) -> Result<()> {
        let cp = coll_prefix(self.meta.id);
        let end = prefix_successor(&cp);
        txn.open_table(DOCS)?.retain_in(cp.as_slice()..end.as_slice(), |_, _| false)?;
        txn.open_table(INDEXES)?.retain_in(cp.as_slice()..end.as_slice(), |_, _| false)?;
        txn.open_table(CATALOG)?.remove(self.ns.as_str())?;
        Ok(())
    }
}

fn next_coll_id(txn: &WriteTransaction) -> Result<i64> {
    let mut meta = txn.open_table(META)?;
    let cur = match meta.get("next_coll_id")? {
        Some(v) => i64::from_be_bytes(v.value().try_into().map_err(|_| Error::internal("corrupt next_coll_id"))?),
        None => 1,
    };
    meta.insert("next_coll_id", (cur + 1).to_be_bytes().as_slice())?;
    Ok(cur)
}

pub fn describe_type(v: &Bson) -> &'static str {
    type_name(v)
}

/// Encoded key for null, used for tests of the planner.
pub fn null_key() -> Vec<u8> {
    vec![TAG_NULL]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.redb")).unwrap();
        (dir, s)
    }

    #[test]
    fn insert_find_index() {
        let (_d, s) = store();
        let txn = s.begin_write().unwrap();
        let mut c = WriteColl::open_or_create(&txn, "db.c", 1).unwrap();
        for i in 0..50 {
            c.insert(&txn, doc! {"_id": i, "n": i % 5, "tags": [format!("t{}", i % 3), "all"]}).unwrap();
        }
        let ix = parse_index_spec(&doc! {"key": {"n": 1}, "name": "n_1"}, c.meta.next_index_id).unwrap();
        c.meta.next_index_id += 1;
        c.add_index(&txn, ix).unwrap();
        let ix = parse_index_spec(&doc! {"key": {"tags": 1}}, c.meta.next_index_id).unwrap();
        c.meta.next_index_id += 1;
        c.add_index(&txn, ix).unwrap();
        assert!(c.meta.indexes[1].multikey);
        assert!(c.insert(&txn, doc! {"_id": 3}).unwrap_err().code == 11000);
        c.finish(&txn).unwrap();
        txn.commit().unwrap();

        let snap = s.snapshot().unwrap();
        let q = Matcher::parse(&doc! {"n": 2}).unwrap();
        assert!(matches!(snap.plan("db.c", &q).unwrap(), Plan::IndexPoints { .. }));
        assert_eq!(snap.find("db.c", &q, &ScanOpts { limit: None }).unwrap().len(), 10);
        let q = Matcher::parse(&doc! {"n": {"$gte": 3}}).unwrap();
        assert!(matches!(snap.plan("db.c", &q).unwrap(), Plan::IndexRange { .. }));
        assert_eq!(snap.find("db.c", &q, &ScanOpts { limit: None }).unwrap().len(), 20);
        let q = Matcher::parse(&doc! {"tags": "all"}).unwrap();
        assert_eq!(snap.find("db.c", &q, &ScanOpts { limit: None }).unwrap().len(), 50);
        let q = Matcher::parse(&doc! {"_id": {"$in": [1, 2, 99]}}).unwrap();
        assert_eq!(snap.find("db.c", &q, &ScanOpts { limit: None }).unwrap().len(), 2);
        let q = Matcher::parse(&doc! {"_id": {"$gt": 45}}).unwrap();
        assert_eq!(snap.find("db.c", &q, &ScanOpts { limit: None }).unwrap().len(), 4);
        assert_eq!(snap.count_all("db.c").unwrap(), 50);
    }

    #[test]
    fn unique_and_update_maintenance() {
        let (_d, s) = store();
        let txn = s.begin_write().unwrap();
        let mut c = WriteColl::open_or_create(&txn, "db.u", 1).unwrap();
        let ix = parse_index_spec(&doc! {"key": {"email": 1}, "unique": true}, 1).unwrap();
        c.add_index(&txn, ix).unwrap();
        c.insert(&txn, doc! {"_id": 1, "email": "a"}).unwrap();
        c.insert(&txn, doc! {"_id": 2, "email": "b"}).unwrap();
        assert_eq!(c.insert(&txn, doc! {"_id": 3, "email": "a"}).unwrap_err().code, 11000);
        let old = doc! {"_id": 2, "email": "b"};
        let idk = keystring::encode(&Bson::Int32(2));
        assert_eq!(c.replace(&txn, &idk, &old, doc! {"_id": 2, "email": "a"}).unwrap_err().code, 11000);
        c.replace(&txn, &idk, &old, doc! {"_id": 2, "email": "c"}).unwrap();
        c.insert(&txn, doc! {"_id": 4, "email": "b"}).unwrap();
        let found = c.find(&txn, &Matcher::parse(&doc! {"email": "c"}).unwrap(), None).unwrap();
        assert_eq!(found.len(), 1);
        c.delete(&txn, &idk, &doc! {"_id": 2, "email": "c"}).unwrap();
        assert!(c.find(&txn, &Matcher::parse(&doc! {"email": "c"}).unwrap(), None).unwrap().is_empty());
        c.insert(&txn, doc! {"_id": 5, "email": "c"}).unwrap();
        // parallel arrays
        let ix = parse_index_spec(&doc! {"key": {"x": 1, "y": 1}}, 2).unwrap();
        c.add_index(&txn, ix).unwrap();
        assert_eq!(c.insert(&txn, doc! {"_id": 6, "email": "z", "x": [1], "y": [2]}).unwrap_err().code, 171);
    }
}
