//! Small helpers for working with BSON values.

use crate::error::{Error, Result};
use bson::{Bson, Document};

pub fn as_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(d) => Some(*d),
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// Integer value of a number that is exactly integral.
pub fn as_i64(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(d) if d.fract() == 0.0 && d.abs() < 9.2e18 => Some(*d as i64),
        Bson::Decimal128(_) => as_f64(v).filter(|d| d.fract() == 0.0 && d.abs() < 9.2e18).map(|d| d as i64),
        _ => None,
    }
}

pub fn is_number(v: &Bson) -> bool {
    matches!(v, Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_))
}

/// Truthiness as used by `$cond`, `$and`, `$expr`, ...
pub fn truthy(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(d) => *d != 0.0 && !d.is_nan(),
        Bson::Decimal128(_) => as_f64(v).is_some_and(|d| d != 0.0),
        _ => true,
    }
}

/// Numeric BSON type code, as used by `$type`.
pub fn type_code(v: &Bson) -> i32 {
    match v {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::RegularExpression(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    }
}

pub fn type_name(v: &Bson) -> &'static str {
    match v {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::DbPointer(_) => "dbPointer",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

/// Maps a `$type` alias to a type code (or 0 for the "number" alias).
pub fn type_code_from_alias(alias: &str) -> Option<i32> {
    Some(match alias {
        "double" => 1,
        "string" => 2,
        "object" => 3,
        "array" => 4,
        "binData" => 5,
        "undefined" => 6,
        "objectId" => 7,
        "bool" => 8,
        "date" => 9,
        "null" => 10,
        "regex" => 11,
        "dbPointer" => 12,
        "javascript" => 13,
        "symbol" => 14,
        "javascriptWithScope" => 15,
        "int" => 16,
        "timestamp" => 17,
        "long" => 18,
        "decimal" => 19,
        "minKey" => -1,
        "maxKey" => 127,
        "number" => 0,
        _ => return None,
    })
}

/// Encoded size of a document in bytes.
pub fn doc_size(d: &Document) -> usize {
    d.to_vec().map(|v| v.len()).unwrap_or(usize::MAX)
}

pub fn doc_to_bytes(d: &Document) -> Result<Vec<u8>> {
    d.to_vec().map_err(|e| Error::bad_value(format!("cannot encode document: {e}")))
}

pub fn doc_from_bytes(b: &[u8]) -> Result<Document> {
    Document::from_reader(b).map_err(|e| Error::internal(format!("corrupt document: {e}")))
}

/// Parses a non-negative array index path component ("0", "12"; not "01" or "-1").
pub fn parse_index(part: &str) -> Option<usize> {
    if part.is_empty() || part.len() > 1 && part.starts_with('0') {
        return None;
    }
    if !part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    part.parse().ok()
}

/// Gets a value by dotted path without array expansion (numeric parts index
/// into arrays). Used where MongoDB also uses plain navigation.
pub fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut cur = doc.get(first)?;
    for part in parts {
        cur = match cur {
            Bson::Document(d) => d.get(part)?,
            Bson::Array(a) => a.get(parse_index(part)?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Integer-valued BSON number from a command field (e.g. `limit`, `batchSize`).
pub fn get_int(doc: &Document, key: &str) -> Result<Option<i64>> {
    match doc.get(key) {
        None | Some(Bson::Null) => Ok(None),
        Some(v) => match as_f64(v) {
            Some(f) if f.fract() == 0.0 => Ok(Some(f as i64)),
            _ => Err(Error::type_mismatch(format!(
                "Field '{key}' should be an integer, but found {}",
                type_name(v)
            ))),
        },
    }
}

pub fn get_bool(doc: &Document, key: &str) -> Result<Option<bool>> {
    match doc.get(key) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Boolean(b)) => Ok(Some(*b)),
        Some(v) if is_number(v) => Ok(Some(truthy(v))),
        Some(v) => Err(Error::type_mismatch(format!(
            "Field '{key}' should be a boolean, but found {}",
            type_name(v)
        ))),
    }
}

pub fn get_doc<'a>(doc: &'a Document, key: &str) -> Result<Option<&'a Document>> {
    match doc.get(key) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Document(d)) => Ok(Some(d)),
        Some(v) => Err(Error::type_mismatch(format!(
            "Field '{key}' should be an object, but found {}",
            type_name(v)
        ))),
    }
}

pub fn get_str<'a>(doc: &'a Document, key: &str) -> Result<Option<&'a str>> {
    match doc.get(key) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::String(s)) => Ok(Some(s)),
        Some(v) => Err(Error::type_mismatch(format!(
            "Field '{key}' should be a string, but found {}",
            type_name(v)
        ))),
    }
}

/// Current wall-clock time in milliseconds since the epoch.
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
