//! State-machine snapshots, used to bring a follower up to date when the
//! leader has already compacted the log entries it needs.
//!
//! A snapshot is a raw copy of the state-machine tables taken from one redb
//! read transaction (an MVCC snapshot), together with the applied index/term
//! of that same transaction. Format: repeated
//! `table(u8) key_len(u32) key val_len(u32) val`, all little-endian.

use crate::error::{Error, Result};
use crate::storage::{CATALOG, DOCS, INDEXES, LOG, META, SESSIONS, Store, USERS};
use redb::{Durability, ReadableDatabase, ReadableTable};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

const T_CATALOG: u8 = 1;
const T_DOCS: u8 = 2;
const T_INDEXES: u8 = 3;
const T_SESSIONS: u8 = 4;
const T_USERS: u8 = 5;
const T_META: u8 = 6;

/// Meta keys that belong to the state machine (copied by snapshots).
const SM_META_KEYS: &[&str] = &["next_coll_id"];

fn put(w: &mut impl Write, table: u8, k: &[u8], v: &[u8]) -> std::io::Result<()> {
    w.write_all(&[table])?;
    w.write_all(&(k.len() as u32).to_le_bytes())?;
    w.write_all(k)?;
    w.write_all(&(v.len() as u32).to_le_bytes())?;
    w.write_all(v)
}

/// Writes a snapshot to `path`; returns its (last_index, last_term).
pub fn create(store: &Store, path: &Path) -> Result<(u64, u64)> {
    let txn = store.db.begin_read()?;
    let meta = txn.open_table(META)?;
    let read_u64 = |k: &str| -> Result<u64> {
        Ok(match meta.get(k)? {
            Some(v) => u64::from_be_bytes(v.value().try_into().map_err(|_| Error::internal("corrupt meta"))?),
            None => 0,
        })
    };
    let last_index = read_u64("applied_index")?;
    let last_term = read_u64("applied_term")?;
    let mut w = BufWriter::new(std::fs::File::create(path)?);
    for (tag, def) in [(T_DOCS, DOCS), (T_INDEXES, INDEXES), (T_SESSIONS, SESSIONS)] {
        let t = txn.open_table(def)?;
        for item in t.iter()? {
            let (k, v) = item?;
            put(&mut w, tag, k.value(), v.value())?;
        }
    }
    for (tag, def) in [(T_CATALOG, CATALOG), (T_USERS, USERS)] {
        let t = txn.open_table(def)?;
        for item in t.iter()? {
            let (k, v) = item?;
            put(&mut w, tag, k.value().as_bytes(), v.value())?;
        }
    }
    for k in SM_META_KEYS {
        if let Some(v) = meta.get(*k)? {
            put(&mut w, T_META, k.as_bytes(), v.value())?;
        }
    }
    w.flush()?;
    w.into_inner().map_err(|e| Error::internal(format!("snapshot write: {e}")))?.sync_all()?;
    Ok((last_index, last_term))
}

fn read_chunk(r: &mut impl Read) -> std::io::Result<Option<(u8, Vec<u8>, Vec<u8>)>> {
    let mut tag = [0u8; 1];
    match r.read_exact(&mut tag) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let mut k = vec![0u8; u32::from_le_bytes(len) as usize];
    r.read_exact(&mut k)?;
    r.read_exact(&mut len)?;
    let mut v = vec![0u8; u32::from_le_bytes(len) as usize];
    r.read_exact(&mut v)?;
    Ok(Some((tag[0], k, v)))
}

/// Replaces the state machine with the snapshot at `path`, in one durable
/// transaction. Log entries after the snapshot are kept only if the log
/// contains the snapshot's last entry (same index and term); otherwise the
/// whole log is discarded.
pub fn install(store: &Store, path: &Path, last_index: u64, last_term: u64, keep_log_suffix: bool) -> Result<()> {
    let mut txn = store.db.begin_write()?;
    txn.set_durability(Durability::Immediate).map_err(|e| Error::internal(format!("durability: {e}")))?;
    {
        txn.open_table(DOCS)?.retain(|_, _| false)?;
        txn.open_table(INDEXES)?.retain(|_, _| false)?;
        txn.open_table(SESSIONS)?.retain(|_, _| false)?;
        txn.open_table(CATALOG)?.retain(|_, _| false)?;
        txn.open_table(USERS)?.retain(|_, _| false)?;
        let mut docs = txn.open_table(DOCS)?;
        let mut idx = txn.open_table(INDEXES)?;
        let mut sessions = txn.open_table(SESSIONS)?;
        let mut catalog = txn.open_table(CATALOG)?;
        let mut users = txn.open_table(USERS)?;
        let mut meta = txn.open_table(META)?;
        let mut r = BufReader::new(std::fs::File::open(path)?);
        while let Some((tag, k, v)) = read_chunk(&mut r)? {
            let as_str = |k: &[u8]| String::from_utf8(k.to_vec()).map_err(|_| Error::internal("corrupt snapshot key"));
            match tag {
                T_DOCS => {
                    docs.insert(k.as_slice(), v.as_slice())?;
                }
                T_INDEXES => {
                    idx.insert(k.as_slice(), v.as_slice())?;
                }
                T_SESSIONS => {
                    sessions.insert(k.as_slice(), v.as_slice())?;
                }
                T_CATALOG => {
                    catalog.insert(as_str(&k)?.as_str(), v.as_slice())?;
                }
                T_USERS => {
                    users.insert(as_str(&k)?.as_str(), v.as_slice())?;
                }
                T_META => {
                    let key = as_str(&k)?;
                    if SM_META_KEYS.contains(&key.as_str()) {
                        meta.insert(key.as_str(), v.as_slice())?;
                    }
                }
                other => return Err(Error::internal(format!("corrupt snapshot: table tag {other}"))),
            }
        }
        meta.insert("applied_index", last_index.to_be_bytes().as_slice())?;
        meta.insert("applied_term", last_term.to_be_bytes().as_slice())?;
        meta.insert("log_start_index", last_index.to_be_bytes().as_slice())?;
        meta.insert("log_start_term", last_term.to_be_bytes().as_slice())?;
        let mut log = txn.open_table(LOG)?;
        if keep_log_suffix {
            log.retain_in(..=last_index, |_, _| false)?;
        } else {
            log.retain(|_, _| false)?;
        }
    }
    txn.commit()?;
    Ok(())
}
