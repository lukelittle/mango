//! Persistent Raft state: hard state (term, vote), the log, and the
//! compaction point. Stored in the same redb file as the state machine.

use crate::error::{Error, Result};
use crate::storage::{LOG, META, Store};
use redb::{Durability, ReadableDatabase, ReadableTable, WriteTransaction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

fn get_u64(t: &impl ReadableTable<&'static str, &'static [u8]>, k: &str) -> Result<u64> {
    Ok(match t.get(k)? {
        Some(v) => u64::from_be_bytes(v.value().try_into().map_err(|_| Error::internal(format!("corrupt meta {k}")))?),
        None => 0,
    })
}

pub fn put_u64(txn: &WriteTransaction, k: &str, v: u64) -> Result<()> {
    txn.open_table(META)?.insert(k, v.to_be_bytes().as_slice())?;
    Ok(())
}

fn encode_entry(term: u64, data: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + data.len());
    v.extend_from_slice(&term.to_be_bytes());
    v.extend_from_slice(data);
    v
}

fn decode_entry(index: u64, raw: &[u8]) -> Result<Entry> {
    if raw.len() < 8 {
        return Err(Error::internal(format!("corrupt log entry {index}")));
    }
    Ok(Entry { index, term: u64::from_be_bytes(raw[..8].try_into().unwrap()), data: raw[8..].to_vec() })
}

/// In-memory view of the persistent log bounds plus accessors. All writes
/// are made by the Raft thread.
pub struct RaftLog {
    pub term: u64,
    pub vote: Option<u64>,
    /// Index/term of the last entry removed by compaction (or installed by
    /// snapshot). Entries start at `start_index + 1`.
    pub start_index: u64,
    pub start_term: u64,
    pub last_index: u64,
    pub last_term: u64,
    pub applied_index: u64,
}

impl RaftLog {
    pub fn load(store: &Store) -> Result<RaftLog> {
        let txn = store.db.begin_read()?;
        let meta = txn.open_table(META)?;
        let term = get_u64(&meta, "raft_term")?;
        let vote = match get_u64(&meta, "raft_vote")? {
            0 => None,
            v => Some(v),
        };
        let start_index = get_u64(&meta, "log_start_index")?;
        let start_term = get_u64(&meta, "log_start_term")?;
        let applied_index = get_u64(&meta, "applied_index")?;
        let log = txn.open_table(LOG)?;
        let (last_index, last_term) = match log.last()? {
            Some((k, v)) => {
                let e = decode_entry(k.value(), v.value())?;
                (e.index, e.term)
            }
            None => (start_index, start_term),
        };
        Ok(RaftLog { term, vote, start_index, start_term, last_index, last_term, applied_index })
    }

    pub fn save_hard_state(&mut self, store: &Store, term: u64, vote: Option<u64>) -> Result<()> {
        let txn = store.db.begin_write()?;
        put_u64(&txn, "raft_term", term)?;
        put_u64(&txn, "raft_vote", vote.unwrap_or(0))?;
        txn.commit()?;
        self.term = term;
        self.vote = vote;
        Ok(())
    }

    /// Term of the entry at `index`; `None` if compacted away or beyond the end.
    pub fn term_at(&self, store: &Store, index: u64) -> Result<Option<u64>> {
        if index == self.start_index {
            return Ok(Some(self.start_term));
        }
        if index < self.start_index || index > self.last_index {
            return Ok(None);
        }
        let txn = store.db.begin_read()?;
        let log = txn.open_table(LOG)?;
        match log.get(index)? {
            Some(v) => Ok(Some(decode_entry(index, v.value())?.term)),
            None => Err(Error::internal(format!("log entry {index} missing"))),
        }
    }

    /// Entries in `[from, to]`, stopping early once `max_bytes` is exceeded
    /// (at least one entry is returned if any exist).
    pub fn entries(&self, store: &Store, from: u64, to: u64, max_bytes: usize) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        if from > to || from <= self.start_index {
            return Ok(out);
        }
        let txn = store.db.begin_read()?;
        let log = txn.open_table(LOG)?;
        let mut bytes = 0;
        for item in log.range(from..=to)? {
            let (k, v) = item?;
            bytes += v.value().len();
            out.push(decode_entry(k.value(), v.value())?);
            if bytes >= max_bytes {
                break;
            }
        }
        Ok(out)
    }

    /// Appends entries after removing any conflicting suffix starting at
    /// `truncate_from` (if given). Durable when this returns.
    pub fn append(&mut self, store: &Store, truncate_from: Option<u64>, entries: &[Entry]) -> Result<()> {
        if truncate_from.is_none() && entries.is_empty() {
            return Ok(());
        }
        let txn = store.db.begin_write()?;
        {
            let mut log = txn.open_table(LOG)?;
            if let Some(from) = truncate_from {
                log.retain_in(from.., |_, _| false)?;
            }
            for e in entries {
                log.insert(e.index, encode_entry(e.term, &e.data).as_slice())?;
            }
        }
        txn.commit()?;
        if let Some(from) = truncate_from {
            self.last_index = from - 1;
            self.last_term = if self.last_index == self.start_index {
                self.start_term
            } else {
                self.term_at(store, self.last_index)?.ok_or_else(|| Error::internal("truncated below compaction point"))?
            };
        }
        if let Some(last) = entries.last() {
            self.last_index = last.index;
            self.last_term = last.term;
        }
        Ok(())
    }

    /// Discards entries up to and including `upto` (which must be applied).
    pub fn compact(&mut self, store: &Store, upto: u64) -> Result<()> {
        if upto <= self.start_index || upto > self.applied_index {
            return Ok(());
        }
        let term = self.term_at(store, upto)?.ok_or_else(|| Error::internal("compaction point missing"))?;
        let mut txn = store.db.begin_write()?;
        txn.set_durability(Durability::Immediate).map_err(|e| Error::internal(format!("durability: {e}")))?;
        {
            let mut log = txn.open_table(LOG)?;
            log.retain_in(..=upto, |_, _| false)?;
        }
        put_u64(&txn, "log_start_index", upto)?;
        put_u64(&txn, "log_start_term", term)?;
        txn.commit()?;
        self.start_index = upto;
        self.start_term = term;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_truncate_compact() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("r.redb")).unwrap();
        let mut log = RaftLog::load(&store).unwrap();
        assert_eq!((log.last_index, log.last_term), (0, 0));
        let es: Vec<Entry> = (1..=5).map(|i| Entry { index: i, term: 1, data: vec![i as u8] }).collect();
        log.append(&store, None, &es).unwrap();
        assert_eq!((log.last_index, log.last_term), (5, 1));
        log.append(&store, Some(4), &[Entry { index: 4, term: 2, data: vec![] }]).unwrap();
        assert_eq!((log.last_index, log.last_term), (4, 2));
        assert_eq!(log.term_at(&store, 3).unwrap(), Some(1));
        assert_eq!(log.entries(&store, 2, 10, usize::MAX).unwrap().len(), 3);
        log.save_hard_state(&store, 2, Some(1)).unwrap();
        log.applied_index = 3;
        log.compact(&store, 3).unwrap();
        let reloaded = RaftLog::load(&store).unwrap();
        assert_eq!((reloaded.term, reloaded.vote), (2, Some(1)));
        assert_eq!((reloaded.start_index, reloaded.start_term, reloaded.last_index, reloaded.last_term), (3, 1, 4, 2));
        assert_eq!(reloaded.term_at(&store, 3).unwrap(), Some(1));
        assert_eq!(reloaded.term_at(&store, 2).unwrap(), None);
    }
}
