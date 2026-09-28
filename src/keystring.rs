//! Order-preserving binary encoding of BSON values ("key strings").
//!
//! `encode(a) < encode(b)` (byte-wise) exactly when `a` sorts before `b` in
//! MongoDB's BSON comparison order, and the encodings are equal exactly when
//! the values compare equal (so `1`, `1.0` and `NumberLong(1)` share a key).
//! Encodings are prefix-free, so several can be concatenated into compound
//! index keys and scanned by prefix.
//!
//! All comparisons in Mango go through this module, which keeps sort order,
//! equality, `_id` uniqueness and index order consistent with each other.

use bson::{Bson, Document};
use std::cmp::Ordering;

// Canonical type brackets, in MongoDB's cross-type sort order.
pub const TAG_MINKEY: u8 = 0x0A;
pub const TAG_NULL: u8 = 0x14;
pub const TAG_NUMBER: u8 = 0x1E;
pub const TAG_STRING: u8 = 0x28;
pub const TAG_OBJECT: u8 = 0x32;
pub const TAG_ARRAY: u8 = 0x3C;
pub const TAG_BINARY: u8 = 0x46;
pub const TAG_OID: u8 = 0x50;
pub const TAG_BOOL: u8 = 0x5A;
pub const TAG_DATE: u8 = 0x64;
pub const TAG_TIMESTAMP: u8 = 0x6E;
pub const TAG_REGEX: u8 = 0x78;
pub const TAG_DBPOINTER: u8 = 0x7D;
pub const TAG_CODE: u8 = 0x82;
pub const TAG_CODE_W_SCOPE: u8 = 0x8C;
pub const TAG_MAXKEY: u8 = 0xF0;

const END: u8 = 0x00;

/// The canonical type bracket of a value. Values in different brackets never
/// match range operators such as `$gt`.
pub fn type_bracket(v: &Bson) -> u8 {
    match v {
        Bson::MinKey => TAG_MINKEY,
        Bson::Null | Bson::Undefined => TAG_NULL,
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => TAG_NUMBER,
        Bson::String(_) | Bson::Symbol(_) => TAG_STRING,
        Bson::Document(_) => TAG_OBJECT,
        Bson::Array(_) => TAG_ARRAY,
        Bson::Binary(_) => TAG_BINARY,
        Bson::ObjectId(_) => TAG_OID,
        Bson::Boolean(_) => TAG_BOOL,
        Bson::DateTime(_) => TAG_DATE,
        Bson::Timestamp(_) => TAG_TIMESTAMP,
        Bson::RegularExpression(_) => TAG_REGEX,
        Bson::DbPointer(_) => TAG_DBPOINTER,
        Bson::JavaScriptCode(_) => TAG_CODE,
        Bson::JavaScriptCodeWithScope(_) => TAG_CODE_W_SCOPE,
        Bson::MaxKey => TAG_MAXKEY,
    }
}

pub fn encode(v: &Bson) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    write_value(&mut out, v);
    out
}

/// Appends the encoding of `v` to `out`.
pub fn write_value(out: &mut Vec<u8>, v: &Bson) {
    out.push(type_bracket(v));
    write_payload(out, v);
}

/// The encoding of a whole document, as if it were an embedded object.
pub fn encode_doc(d: &Document) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(TAG_OBJECT);
    write_object_payload(&mut out, d);
    out
}

fn write_payload(out: &mut Vec<u8>, v: &Bson) {
    match v {
        Bson::MinKey | Bson::MaxKey | Bson::Null | Bson::Undefined => {}
        Bson::Double(d) => write_number_f64(out, *d),
        Bson::Int32(i) => write_number_int(out, *i as i64),
        Bson::Int64(i) => write_number_int(out, *i),
        Bson::Decimal128(d) => {
            let f = d.to_string().parse::<f64>().unwrap_or(f64::NAN);
            write_number_f64(out, f)
        }
        Bson::String(s) | Bson::Symbol(s) => write_str(out, s),
        Bson::Document(d) => write_object_payload(out, d),
        Bson::Array(a) => {
            for e in a {
                write_value(out, e);
            }
            out.push(END);
        }
        Bson::Binary(b) => {
            out.extend_from_slice(&(b.bytes.len() as u32).to_be_bytes());
            out.push(u8::from(b.subtype));
            out.extend_from_slice(&b.bytes);
        }
        Bson::ObjectId(o) => out.extend_from_slice(&o.bytes()),
        Bson::Boolean(b) => out.push(*b as u8),
        Bson::DateTime(dt) => out.extend_from_slice(&i64_key(dt.timestamp_millis())),
        Bson::Timestamp(ts) => {
            out.extend_from_slice(&ts.time.to_be_bytes());
            out.extend_from_slice(&ts.increment.to_be_bytes());
        }
        Bson::RegularExpression(r) => {
            write_str(out, r.pattern.as_str());
            write_str(out, r.options.as_str());
        }
        Bson::DbPointer(p) => {
            // The fields of DbPointer are private; its Debug output is stable
            // within a process, which is all ordering of this deprecated type needs.
            write_str(out, &format!("{p:?}"));
        }
        Bson::JavaScriptCode(c) => write_str(out, c),
        Bson::JavaScriptCodeWithScope(c) => {
            write_str(out, &c.code);
            write_object_payload(out, &c.scope);
        }
    }
}

fn write_object_payload(out: &mut Vec<u8>, d: &Document) {
    // MongoDB compares embedded documents element by element: first the
    // value's type bracket, then the field name, then the value.
    for (k, v) in d {
        out.push(type_bracket(v));
        write_str(out, k);
        write_payload(out, v);
    }
    out.push(END);
}

/// Strings are escaped so that 0x00 terminates them: 0x00 -> 0x00 0xFF.
fn write_str(out: &mut Vec<u8>, s: &str) {
    for &b in s.as_bytes() {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.push(END);
}

fn i64_key(i: i64) -> [u8; 8] {
    ((i as u64) ^ (1u64 << 63)).to_be_bytes()
}

fn f64_key(d: f64) -> [u8; 8] {
    let d = if d == 0.0 { 0.0 } else { d }; // fold -0.0 into 0.0
    let bits = d.to_bits();
    let bits = if bits >> 63 == 1 { !bits } else { bits ^ (1u64 << 63) };
    bits.to_be_bytes()
}

fn i128_key(i: i128) -> [u8; 16] {
    ((i as u128) ^ (1u128 << 127)).to_be_bytes()
}

const TWO_POW_127: f64 = 170141183460469231731687303715884105728.0;

// Numbers: one byte (0 = NaN, which sorts below every other number, 1 = other),
// then the value rounded to f64, then the exact integer value as a tiebreak so
// that large int64s that round to the same double still order correctly.
fn write_number_f64(out: &mut Vec<u8>, d: f64) {
    if d.is_nan() {
        out.push(0);
        out.extend_from_slice(&[0u8; 24]);
        return;
    }
    out.push(1);
    out.extend_from_slice(&f64_key(d));
    let tiebreak = if d.fract() == 0.0 && d.abs() < TWO_POW_127 { d as i128 } else { 0 };
    out.extend_from_slice(&i128_key(tiebreak));
}

fn write_number_int(out: &mut Vec<u8>, i: i64) {
    out.push(1);
    out.extend_from_slice(&f64_key(i as f64));
    out.extend_from_slice(&i128_key(i as i128));
}

/// Total order over BSON values, matching MongoDB's sort order.
pub fn compare(a: &Bson, b: &Bson) -> Ordering {
    // Fast paths for common same-type comparisons.
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => return x.cmp(y),
        (Bson::Int64(x), Bson::Int64(y)) => return x.cmp(y),
        (Bson::String(x), Bson::String(y)) => return x.as_bytes().cmp(y.as_bytes()),
        (Bson::ObjectId(x), Bson::ObjectId(y)) => return x.bytes().cmp(&y.bytes()),
        _ => {}
    }
    encode(a).cmp(&encode(b))
}

pub fn values_equal(a: &Bson, b: &Bson) -> bool {
    compare(a, b) == Ordering::Equal
}

/// Smallest possible key in the bracket `tag`.
pub fn bracket_start(tag: u8) -> Vec<u8> {
    vec![tag]
}

/// A key greater than every key in bracket `tag`.
pub fn bracket_end(tag: u8) -> Vec<u8> {
    vec![tag + 1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Binary, DateTime, Timestamp, doc, oid::ObjectId, spec::BinarySubtype};

    fn lt(a: Bson, b: Bson) {
        assert_eq!(compare(&a, &b), Ordering::Less, "{a:?} < {b:?}");
        assert_eq!(compare(&b, &a), Ordering::Greater, "{b:?} > {a:?}");
    }
    fn eq(a: Bson, b: Bson) {
        assert_eq!(compare(&a, &b), Ordering::Equal, "{a:?} == {b:?}");
    }

    #[test]
    fn cross_type_order() {
        let ordered = vec![
            Bson::MinKey,
            Bson::Null,
            Bson::Double(f64::NAN),
            Bson::Double(f64::NEG_INFINITY),
            Bson::Int32(-5),
            Bson::Double(0.5),
            Bson::Int64(7),
            Bson::Double(f64::INFINITY),
            Bson::String("".into()),
            Bson::String("a".into()),
            Bson::String("a\0".into()),
            Bson::String("ab".into()),
            Bson::String("b".into()),
            Bson::Document(doc! {}),
            Bson::Document(doc! {"a": 1}),
            Bson::Array(vec![]),
            Bson::Array(vec![Bson::Int32(1)]),
            Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![9] }),
            Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![0, 0] }),
            Bson::ObjectId(ObjectId::from_bytes([0; 12])),
            Bson::ObjectId(ObjectId::from_bytes([1; 12])),
            Bson::Boolean(false),
            Bson::Boolean(true),
            Bson::DateTime(DateTime::from_millis(-1)),
            Bson::DateTime(DateTime::from_millis(1)),
            Bson::Timestamp(Timestamp { time: 1, increment: 2 }),
            Bson::MaxKey,
        ];
        for w in ordered.windows(2) {
            lt(w[0].clone(), w[1].clone());
        }
    }

    #[test]
    fn numeric_equality_across_types() {
        eq(Bson::Int32(1), Bson::Double(1.0));
        eq(Bson::Int64(1), Bson::Int32(1));
        eq(Bson::Double(0.0), Bson::Double(-0.0));
        lt(Bson::Int32(1), Bson::Double(1.5));
        lt(Bson::Double(1.5), Bson::Int32(2));
    }

    #[test]
    fn large_int64_precision() {
        let big = 1i64 << 60;
        lt(Bson::Int64(big), Bson::Int64(big + 1));
        eq(Bson::Int64(big), Bson::Double(big as f64));
        lt(Bson::Double(big as f64), Bson::Int64(big + 1));
        lt(Bson::Int64(i64::MAX), Bson::Double(9223372036854775808.0));
        lt(Bson::Double(-9223372036854775808.0 * 2.0), Bson::Int64(i64::MIN));
        eq(Bson::Double(-9223372036854775808.0), Bson::Int64(i64::MIN));
    }

    #[test]
    fn documents_compare_by_fields() {
        lt(Bson::Document(doc! {"a": 1}), Bson::Document(doc! {"a": 2}));
        lt(Bson::Document(doc! {"a": 1}), Bson::Document(doc! {"a": 1, "b": 1}));
        lt(Bson::Document(doc! {"a": 5}), Bson::Document(doc! {"b": 1}));
        // type bracket of the value is compared before the field name
        lt(Bson::Document(doc! {"b": 1}), Bson::Document(doc! {"a": "x"}));
        eq(Bson::Document(doc! {"a": 1}), Bson::Document(doc! {"a": 1.0}));
    }

    #[test]
    fn prefix_free_concatenation() {
        // ("a", 2) vs ("a\0", 1) must order by the first component
        let mut k1 = encode(&Bson::String("a".into()));
        k1.extend(encode(&Bson::Int32(2)));
        let mut k2 = encode(&Bson::String("a\0".into()));
        k2.extend(encode(&Bson::Int32(1)));
        assert!(k1 < k2);
    }
}
