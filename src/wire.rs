//! MongoDB wire protocol framing: OP_MSG (modern drivers) and OP_QUERY /
//! OP_REPLY (the legacy handshake some drivers still use for the first
//! `hello`).

use crate::error::{Error, Result};
use bson::{Bson, Document};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const OP_REPLY: i32 = 1;
pub const OP_QUERY: i32 = 2004;
pub const OP_COMPRESSED: i32 = 2012;
pub const OP_MSG: i32 = 2013;

pub const MAX_MESSAGE_SIZE: usize = 48_000_000;

const FLAG_CHECKSUM: u32 = 1;
const FLAG_MORE_TO_COME: u32 = 1 << 1;

#[derive(Debug)]
pub struct Header {
    pub length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: i32,
}

#[derive(Debug)]
pub enum Request {
    /// A command sent with OP_MSG; `more_to_come` means no reply is wanted.
    Msg { request_id: i32, body: Document, more_to_come: bool },
    /// A command sent with OP_QUERY against `<db>.$cmd`.
    Query { request_id: i32, db: String, body: Document },
}

fn read_i32(b: &[u8], at: usize) -> Result<i32> {
    b.get(at..at + 4).map(|s| i32::from_le_bytes(s.try_into().unwrap())).ok_or_else(|| Error::failed_to_parse("truncated message"))
}

fn read_cstring(b: &[u8], at: usize) -> Result<(String, usize)> {
    let end = b[at..].iter().position(|&c| c == 0).ok_or_else(|| Error::failed_to_parse("unterminated cstring"))?;
    let s = std::str::from_utf8(&b[at..at + end]).map_err(|_| Error::failed_to_parse("invalid UTF-8 in cstring"))?;
    Ok((s.to_string(), at + end + 1))
}

fn read_doc(b: &[u8], at: usize) -> Result<(Document, usize)> {
    let len = read_i32(b, at)? as usize;
    if len < 5 || at + len > b.len() {
        return Err(Error::failed_to_parse("invalid BSON document length"));
    }
    let d = Document::from_reader(&b[at..at + len]).map_err(|e| Error::failed_to_parse(format!("invalid BSON: {e}")))?;
    Ok((d, at + len))
}

/// Reads one message. `Ok(None)` on clean EOF.
pub async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<(Header, Vec<u8>)>> {
    let mut hdr = [0u8; 16];
    match r.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let h = Header {
        length: i32::from_le_bytes(hdr[0..4].try_into().unwrap()),
        request_id: i32::from_le_bytes(hdr[4..8].try_into().unwrap()),
        response_to: i32::from_le_bytes(hdr[8..12].try_into().unwrap()),
        op_code: i32::from_le_bytes(hdr[12..16].try_into().unwrap()),
    };
    if h.length < 16 || h.length as usize > MAX_MESSAGE_SIZE {
        return Err(Error::failed_to_parse(format!("invalid message length {}", h.length)));
    }
    let mut body = vec![0u8; h.length as usize - 16];
    r.read_exact(&mut body).await?;
    Ok(Some((h, body)))
}

pub fn parse_request(h: &Header, b: &[u8]) -> Result<Request> {
    match h.op_code {
        OP_MSG => {
            let flags = read_i32(b, 0)? as u32;
            if flags & !(FLAG_CHECKSUM | FLAG_MORE_TO_COME | (1 << 16)) & 0xFFFF != 0 {
                return Err(Error::failed_to_parse(format!("unsupported OP_MSG flag bits {flags:#x}")));
            }
            let end = if flags & FLAG_CHECKSUM != 0 { b.len().saturating_sub(4) } else { b.len() };
            let mut at = 4;
            let mut body: Option<Document> = None;
            let mut sequences: Vec<(String, Vec<Bson>)> = Vec::new();
            while at < end {
                let kind = b[at];
                at += 1;
                match kind {
                    0 => {
                        let (d, next) = read_doc(b, at)?;
                        if body.is_some() {
                            return Err(Error::failed_to_parse("OP_MSG has more than one body section"));
                        }
                        body = Some(d);
                        at = next;
                    }
                    1 => {
                        let size = read_i32(b, at)? as usize;
                        let sec_end = at + size;
                        if size < 5 || sec_end > end {
                            return Err(Error::failed_to_parse("invalid document sequence size"));
                        }
                        let (id, mut p) = read_cstring(b, at + 4)?;
                        let mut docs = Vec::new();
                        while p < sec_end {
                            let (d, next) = read_doc(b, p)?;
                            docs.push(Bson::Document(d));
                            p = next;
                        }
                        sequences.push((id, docs));
                        at = sec_end;
                    }
                    k => return Err(Error::failed_to_parse(format!("unknown OP_MSG section kind {k}"))),
                }
            }
            let mut body = body.ok_or_else(|| Error::failed_to_parse("OP_MSG has no body section"))?;
            for (id, docs) in sequences {
                if body.contains_key(&id) {
                    return Err(Error::failed_to_parse(format!("duplicate field {id} in OP_MSG body and document sequence")));
                }
                body.insert(id, Bson::Array(docs));
            }
            Ok(Request::Msg { request_id: h.request_id, body, more_to_come: flags & FLAG_MORE_TO_COME != 0 })
        }
        OP_QUERY => {
            let (ns, at) = read_cstring(b, 4)?;
            let at = at + 8; // numberToSkip, numberToReturn
            let (mut q, _) = read_doc(b, at)?;
            let Some(db) = ns.strip_suffix(".$cmd") else {
                return Err(Error::failed_to_parse("OP_QUERY is only supported for commands"));
            };
            // Legacy wrapped form: {$query: {...}, $readPreference: ...}
            if let Some(Bson::Document(inner)) = q.get("$query").or_else(|| q.get("query")).cloned()
                && q.keys().next().is_some_and(|k| k == "$query" || k == "query")
            {
                q = inner;
            }
            Ok(Request::Query { request_id: h.request_id, db: db.to_string(), body: q })
        }
        OP_COMPRESSED => Err(Error::failed_to_parse("compression was not negotiated")),
        op => Err(Error::failed_to_parse(format!("unsupported opcode {op}"))),
    }
}

static NEXT_ID: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);

fn header(buf: &mut Vec<u8>, response_to: i32, op: i32) {
    buf.extend_from_slice(&0i32.to_le_bytes());
    buf.extend_from_slice(&NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed).to_le_bytes());
    buf.extend_from_slice(&response_to.to_le_bytes());
    buf.extend_from_slice(&op.to_le_bytes());
}

pub fn encode_msg_reply(response_to: i32, doc: &Document) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    header(&mut buf, response_to, OP_MSG);
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.push(0);
    doc.to_writer(&mut buf).map_err(|e| Error::internal(format!("encode reply: {e}")))?;
    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    Ok(buf)
}

pub fn encode_op_reply(response_to: i32, doc: &Document) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    header(&mut buf, response_to, OP_REPLY);
    buf.extend_from_slice(&0i32.to_le_bytes()); // responseFlags
    buf.extend_from_slice(&0i64.to_le_bytes()); // cursorID
    buf.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
    buf.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
    doc.to_writer(&mut buf).map_err(|e| Error::internal(format!("encode reply: {e}")))?;
    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    Ok(buf)
}

pub async fn write_all<W: AsyncWrite + Unpin>(w: &mut W, buf: &[u8]) -> Result<()> {
    w.write_all(buf).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn op_msg_with_sequence() {
        let body = doc! {"insert": "c", "$db": "d"};
        let d1 = doc! {"_id": 1};
        let d2 = doc! {"_id": 2};
        let mut b = Vec::new();
        b.extend_from_slice(&0u32.to_le_bytes());
        b.push(0);
        body.to_writer(&mut b).unwrap();
        b.push(1);
        let mut seq = Vec::new();
        seq.extend_from_slice(b"documents\0");
        d1.to_writer(&mut seq).unwrap();
        d2.to_writer(&mut seq).unwrap();
        b.extend_from_slice(&((seq.len() + 4) as i32).to_le_bytes());
        b.extend_from_slice(&seq);
        let h = Header { length: (b.len() + 16) as i32, request_id: 7, response_to: 0, op_code: OP_MSG };
        match parse_request(&h, &b).unwrap() {
            Request::Msg { body, .. } => {
                assert_eq!(body.get_array("documents").unwrap().len(), 2);
                assert_eq!(body.get_str("$db").unwrap(), "d");
            }
            _ => panic!(),
        }
    }
}
