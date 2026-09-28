//! Query filters: parsing a filter document into a [`Matcher`] and evaluating
//! it against documents with MongoDB's semantics (dotted paths, implicit array
//! traversal, type bracketing, null-matches-missing, ...).

use crate::bsonutil::{as_f64, get_path, is_number, parse_index, type_code, type_code_from_alias, type_name};
use crate::error::{Error, Result};
use crate::expr::{Expr, Vars};
use crate::keystring::{compare, type_bracket, values_equal};
use bson::{Bson, Document};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Matcher {
    All(Vec<Matcher>),
    Any(Vec<Matcher>),
    Nor(Vec<Matcher>),
    Field { path: String, parts: Vec<String>, pred: Pred },
    Expr(Box<Expr>),
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone)]
pub enum Pred {
    Eq(Bson),
    Cmp(CmpOp, Bson),
    In(Vec<InItem>),
    Exists(bool),
    Type(Vec<i32>),
    Regex(Box<CompiledRegex>),
    Size(usize),
    ElemMatchDoc(Box<Matcher>),
    ElemMatchValue(Vec<Pred>),
    Mod(i64, i64),
    /// Every sub-predicate must match (each with its own array traversal).
    AllOf(Vec<Pred>),
    Not(Box<Pred>),
    Never,
}

#[derive(Debug, Clone)]
pub enum InItem {
    Value(Bson),
    Regex(CompiledRegex),
}

#[derive(Debug, Clone)]
pub struct CompiledRegex {
    pub pattern: String,
    pub options: String,
    pub re: regex::Regex,
}

impl CompiledRegex {
    pub fn new(pattern: &str, options: &str) -> Result<CompiledRegex> {
        let mut flags = String::new();
        for c in options.chars() {
            match c {
                'i' | 'm' | 's' | 'x' => {
                    if !flags.contains(c) {
                        flags.push(c)
                    }
                }
                'u' | 'l' => {}
                other => return Err(Error::bad_value(format!("invalid flag in regex options: {other}"))),
            }
        }
        let full = if flags.is_empty() { pattern.to_string() } else { format!("(?{flags}){pattern}") };
        let re = regex::Regex::new(&full)
            .map_err(|e| Error::bad_value(format!("Regular expression is invalid: {e}")))?;
        Ok(CompiledRegex { pattern: pattern.to_string(), options: options.to_string(), re })
    }

    pub fn is_match(&self, v: &Bson) -> bool {
        match v {
            Bson::String(s) | Bson::Symbol(s) => self.re.is_match(s),
            Bson::RegularExpression(r) => r.pattern.as_str() == self.pattern && r.options.as_str() == self.options,
            _ => false,
        }
    }
}

impl Matcher {
    pub fn parse(filter: &Document) -> Result<Matcher> {
        let mut clauses = Vec::new();
        for (k, v) in filter {
            if let Some(m) = parse_top(k, v)? {
                clauses.push(m);
            }
        }
        Ok(match clauses.len() {
            0 => Matcher::Always,
            1 => clauses.pop().unwrap(),
            _ => Matcher::All(clauses),
        })
    }

    pub fn matches(&self, doc: &Document) -> Result<bool> {
        self.matches_env(doc, &[])
    }

    /// Like `matches`, with extra variables visible to `$expr` (used by
    /// `$lookup` sub-pipelines with `let`).
    pub fn matches_env(&self, doc: &Document, env: &[(String, Bson)]) -> Result<bool> {
        Ok(match self {
            Matcher::Always => true,
            Matcher::All(ms) => {
                for m in ms {
                    if !m.matches_env(doc, env)? {
                        return Ok(false);
                    }
                }
                true
            }
            Matcher::Any(ms) => {
                for m in ms {
                    if m.matches_env(doc, env)? {
                        return Ok(true);
                    }
                }
                false
            }
            Matcher::Nor(ms) => {
                for m in ms {
                    if m.matches_env(doc, env)? {
                        return Ok(false);
                    }
                }
                true
            }
            Matcher::Field { parts, pred, .. } => {
                let mut cands = Vec::new();
                lookup(doc, parts, &mut cands);
                pred.matches(&cands)?
            }
            Matcher::Expr(e) => {
                let v = e.eval(&Vars::with_bindings(doc, env))?;
                crate::bsonutil::truthy(&v)
            }
        })
    }

    /// Top-level conjuncts of the form `path <pred>`; used for index
    /// selection and upsert seeding. Disjunctions are not descended into.
    pub fn conjuncts(&self) -> Vec<(&str, &Pred)> {
        let mut out = Vec::new();
        self.collect_conjuncts(&mut out);
        out
    }

    fn collect_conjuncts<'a>(&'a self, out: &mut Vec<(&'a str, &'a Pred)>) {
        match self {
            Matcher::All(ms) => ms.iter().for_each(|m| m.collect_conjuncts(out)),
            Matcher::Field { path, pred, .. } => match pred {
                Pred::AllOf(ps) => ps.iter().for_each(|p| out.push((path, p))),
                p => out.push((path, p)),
            },
            _ => {}
        }
    }

    /// Equality conditions that seed the document created by an upsert.
    pub fn upsert_seed(&self) -> Vec<(&str, &Bson)> {
        self.conjuncts()
            .into_iter()
            .filter_map(|(path, pred)| match pred {
                Pred::Eq(v) => Some((path, v)),
                Pred::In(items) if items.len() == 1 => match &items[0] {
                    InItem::Value(v) => Some((path, v)),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    /// Index into the array at `array_path` of the first element that
    /// satisfies the query's conditions on that array (for the positional
    /// `$` update operator and projection).
    pub fn positional_index(&self, doc: &Document, array_path: &str) -> Result<Option<usize>> {
        let prefix = format!("{array_path}.");
        let relevant: Vec<&Matcher> = match self {
            Matcher::All(ms) => ms.iter().collect(),
            m => vec![m],
        }
        .into_iter()
        .filter(|m| matches!(m, Matcher::Field { path, .. } if path == array_path || path.starts_with(&prefix)))
        .collect();
        if relevant.is_empty() {
            return Ok(None);
        }
        let Some(Bson::Array(arr)) = get_path(doc, array_path) else { return Ok(None) };
        let parts: Vec<&str> = array_path.split('.').collect();
        for (i, elem) in arr.iter().enumerate() {
            // A synthetic document holding just this element in the array.
            let mut v = Bson::Array(vec![elem.clone()]);
            for part in parts.iter().rev() {
                let mut d = Document::new();
                d.insert(*part, v);
                v = Bson::Document(d);
            }
            let Bson::Document(synthetic) = v else { unreachable!() };
            let mut all = true;
            for m in &relevant {
                if !m.matches(&synthetic)? {
                    all = false;
                    break;
                }
            }
            if all {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }
}

fn parse_top(key: &str, value: &Bson) -> Result<Option<Matcher>> {
    match key {
        "$and" | "$or" | "$nor" => {
            let Bson::Array(arr) = value else {
                return Err(Error::bad_value(format!("{key} must be an array")));
            };
            if arr.is_empty() {
                return Err(Error::bad_value("$and/$or/$nor must be a nonempty array"));
            }
            let mut subs = Vec::with_capacity(arr.len());
            for e in arr {
                let Bson::Document(d) = e else {
                    return Err(Error::bad_value("$or/$and/$nor entries need to be full objects"));
                };
                subs.push(Matcher::parse(d)?);
            }
            Ok(Some(match key {
                "$and" => Matcher::All(subs),
                "$or" => Matcher::Any(subs),
                _ => Matcher::Nor(subs),
            }))
        }
        "$expr" => Ok(Some(Matcher::Expr(Box::new(Expr::parse(value)?)))),
        "$comment" => Ok(None),
        "$where" => Err(Error::not_implemented("$where (server-side JavaScript) is not supported by Mango")),
        "$text" => Err(Error::not_implemented("$text search is not supported by Mango")),
        "$jsonSchema" => Err(Error::not_implemented("$jsonSchema is not supported by Mango")),
        k if k.starts_with('$') => Err(Error::bad_value(format!("unknown top level operator: {k}"))),
        path => {
            if path.is_empty() || path.split('.').any(|p| p.is_empty()) {
                return Err(Error::bad_value(format!("invalid field path in query: '{path}'")));
            }
            let pred = parse_field_value(value)?;
            Ok(Some(Matcher::Field {
                path: path.to_string(),
                parts: path.split('.').map(String::from).collect(),
                pred,
            }))
        }
    }
}

fn is_operator_doc(d: &Document) -> bool {
    d.keys().next().is_some_and(|k| k.starts_with('$'))
}

/// Parses the value side of `{path: value}`.
pub fn parse_field_value(value: &Bson) -> Result<Pred> {
    match value {
        Bson::Document(d) if is_operator_doc(d) => parse_operators(d),
        Bson::RegularExpression(r) => {
            Ok(Pred::Regex(Box::new(CompiledRegex::new(r.pattern.as_str(), r.options.as_str())?)))
        }
        v => Ok(Pred::Eq(v.clone())),
    }
}

fn parse_operators(d: &Document) -> Result<Pred> {
    let mut preds = Vec::new();
    // $regex and $options are one operator split over two keys.
    let regex_opts = match d.get("$options") {
        None => None,
        Some(Bson::String(s)) => Some(s.as_str()),
        Some(_) => return Err(Error::bad_value("$options has to be a string")),
    };
    if regex_opts.is_some() && !d.contains_key("$regex") {
        return Err(Error::bad_value("$options needs a $regex"));
    }
    for (op, arg) in d {
        let pred = match op.as_str() {
            "$eq" => Pred::Eq(arg.clone()),
            "$ne" => Pred::Not(Box::new(Pred::Eq(arg.clone()))),
            "$gt" => cmp_pred(CmpOp::Gt, arg)?,
            "$gte" => cmp_pred(CmpOp::Gte, arg)?,
            "$lt" => cmp_pred(CmpOp::Lt, arg)?,
            "$lte" => cmp_pred(CmpOp::Lte, arg)?,
            "$in" => Pred::In(parse_in(arg)?),
            "$nin" => Pred::Not(Box::new(Pred::In(parse_in(arg)?))),
            "$exists" => Pred::Exists(crate::bsonutil::truthy(arg)),
            "$type" => Pred::Type(parse_type(arg)?),
            "$size" => match arg {
                v if is_number(v) => {
                    let f = as_f64(v).unwrap();
                    if f.fract() != 0.0 {
                        return Err(Error::bad_value("$size must be a whole number"));
                    }
                    if f < 0.0 {
                        Pred::Never
                    } else {
                        Pred::Size(f as usize)
                    }
                }
                _ => return Err(Error::bad_value("$size needs a number")),
            },
            "$all" => {
                let Bson::Array(items) = arg else {
                    return Err(Error::bad_value("$all needs an array"));
                };
                if items.is_empty() {
                    Pred::Never
                } else {
                    let mut ps = Vec::new();
                    for it in items {
                        ps.push(match it {
                            Bson::Document(sub) if sub.keys().next().is_some_and(|k| k == "$elemMatch") => {
                                parse_operators(sub)?
                            }
                            Bson::RegularExpression(r) => {
                                Pred::Regex(Box::new(CompiledRegex::new(r.pattern.as_str(), r.options.as_str())?))
                            }
                            v => Pred::Eq(v.clone()),
                        });
                    }
                    Pred::AllOf(ps)
                }
            }
            "$elemMatch" => {
                let Bson::Document(sub) = arg else {
                    return Err(Error::bad_value("$elemMatch needs an Object"));
                };
                let value_form = !sub.is_empty()
                    && sub.keys().all(|k| k.starts_with('$') && !matches!(k.as_str(), "$and" | "$or" | "$nor" | "$expr"));
                if value_form {
                    match parse_operators(sub)? {
                        Pred::AllOf(ps) => Pred::ElemMatchValue(ps),
                        p => Pred::ElemMatchValue(vec![p]),
                    }
                } else {
                    Pred::ElemMatchDoc(Box::new(Matcher::parse(sub)?))
                }
            }
            "$mod" => {
                let Bson::Array(a) = arg else {
                    return Err(Error::bad_value("malformed mod, needs to be an array"));
                };
                if a.len() != 2 {
                    return Err(Error::bad_value("malformed mod, not enough elements"));
                }
                let (Some(div), Some(rem)) = (as_f64(&a[0]), as_f64(&a[1])) else {
                    return Err(Error::bad_value("malformed mod, divisor and remainder must be numbers"));
                };
                if !div.is_finite() || !rem.is_finite() {
                    return Err(Error::bad_value("malformed mod, divisor and remainder must be finite"));
                }
                let div = div.trunc() as i64;
                if div == 0 {
                    return Err(Error::bad_value("divisor cannot be 0"));
                }
                Pred::Mod(div, rem.trunc() as i64)
            }
            "$regex" => {
                let (pattern, own_opts) = match arg {
                    Bson::String(s) => (s.clone(), String::new()),
                    Bson::RegularExpression(r) => (r.pattern.to_string(), r.options.to_string()),
                    _ => return Err(Error::bad_value("$regex has to be a string")),
                };
                let opts = match regex_opts {
                    Some(o) => {
                        if !own_opts.is_empty() {
                            return Err(Error::bad_value("options set in both $regex and $options"));
                        }
                        o.to_string()
                    }
                    None => own_opts,
                };
                Pred::Regex(Box::new(CompiledRegex::new(&pattern, &opts)?))
            }
            "$options" => continue,
            "$not" => match arg {
                Bson::Document(sub) if is_operator_doc(sub) => Pred::Not(Box::new(parse_operators(sub)?)),
                Bson::RegularExpression(r) => Pred::Not(Box::new(Pred::Regex(Box::new(CompiledRegex::new(
                    r.pattern.as_str(),
                    r.options.as_str(),
                )?)))),
                _ => return Err(Error::bad_value("$not needs a regex or a document")),
            },
            "$comment" => continue,
            other => return Err(Error::bad_value(format!("unknown operator: {other}"))),
        };
        preds.push(pred);
    }
    Ok(match preds.len() {
        1 => preds.pop().unwrap(),
        _ => Pred::AllOf(preds),
    })
}

fn cmp_pred(op: CmpOp, arg: &Bson) -> Result<Pred> {
    if let Bson::RegularExpression(_) = arg {
        // Range comparisons against a regex only match equal regexes.
        return Ok(match op {
            CmpOp::Gte | CmpOp::Lte => Pred::Eq(arg.clone()),
            _ => Pred::Never,
        });
    }
    Ok(Pred::Cmp(op, arg.clone()))
}

fn parse_in(arg: &Bson) -> Result<Vec<InItem>> {
    let Bson::Array(items) = arg else {
        return Err(Error::bad_value("$in needs an array"));
    };
    items
        .iter()
        .map(|it| match it {
            Bson::RegularExpression(r) => Ok(InItem::Regex(CompiledRegex::new(r.pattern.as_str(), r.options.as_str())?)),
            Bson::Document(d) if is_operator_doc(d) => Err(Error::bad_value("cannot nest $ under $in")),
            v => Ok(InItem::Value(v.clone())),
        })
        .collect()
}

fn parse_type(arg: &Bson) -> Result<Vec<i32>> {
    let one = |v: &Bson| -> Result<i32> {
        match v {
            Bson::String(s) => type_code_from_alias(s).ok_or_else(|| Error::bad_value(format!("Unknown type name alias: {s}"))),
            v if is_number(v) => {
                let f = as_f64(v).unwrap();
                let code = f as i32;
                if f.fract() != 0.0 || type_code_name_valid(code).is_none() {
                    return Err(Error::bad_value(format!("Invalid numerical type code: {f}")));
                }
                Ok(code)
            }
            _ => Err(Error::type_mismatch("type must be represented as a number or a string")),
        }
    };
    match arg {
        Bson::Array(a) => a.iter().map(one).collect(),
        v => Ok(vec![one(v)?]),
    }
}

fn type_code_name_valid(code: i32) -> Option<()> {
    matches!(code, -1 | 1..=19 | 127).then_some(())
}

/// Collects the values reachable at `parts`, expanding arrays the way
/// MongoDB does. `None` marks a branch where the path is missing.
pub fn lookup<'a>(doc: &'a Document, parts: &[String], out: &mut Vec<Option<&'a Bson>>) {
    match doc.get(&parts[0]) {
        None => out.push(None),
        Some(v) => descend(v, &parts[1..], out),
    }
}

fn descend<'a>(v: &'a Bson, rest: &[String], out: &mut Vec<Option<&'a Bson>>) {
    if rest.is_empty() {
        out.push(Some(v));
        return;
    }
    match v {
        Bson::Document(d) => lookup(d, rest, out),
        Bson::Array(arr) => {
            if let Some(i) = parse_index(&rest[0]) {
                match arr.get(i) {
                    Some(e) => descend(e, &rest[1..], out),
                    None => {
                        // No such position; array elements might still have a field of that name.
                        let before = out.len();
                        for e in arr {
                            if let Bson::Document(d) = e
                                && d.contains_key(&rest[0])
                            {
                                lookup(d, rest, out);
                            }
                        }
                        if out.len() == before {
                            out.push(None);
                        }
                    }
                }
            } else {
                for e in arr {
                    match e {
                        Bson::Document(d) => lookup(d, rest, out),
                        _ => out.push(None),
                    }
                }
            }
        }
        _ => out.push(None),
    }
}

impl Pred {
    pub fn matches(&self, cands: &[Option<&Bson>]) -> Result<bool> {
        Ok(match self {
            Pred::Never => false,
            Pred::Not(p) => !p.matches(cands)?,
            Pred::AllOf(ps) => {
                for p in ps {
                    if !p.matches(cands)? {
                        return Ok(false);
                    }
                }
                true
            }
            Pred::Exists(want) => cands.iter().any(|c| c.is_some()) == *want,
            Pred::Size(n) => cands.iter().any(|c| matches!(c, Some(Bson::Array(a)) if a.len() == *n)),
            Pred::ElemMatchDoc(m) => {
                for c in cands {
                    if let Some(Bson::Array(a)) = c {
                        for e in a {
                            if let Bson::Document(d) = e
                                && m.matches(d)?
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
                false
            }
            Pred::ElemMatchValue(ps) => {
                for c in cands {
                    if let Some(Bson::Array(a)) = c {
                        'elems: for e in a {
                            for p in ps {
                                if !p.matches(&[Some(e)])? {
                                    continue 'elems;
                                }
                            }
                            return Ok(true);
                        }
                    }
                }
                false
            }
            _ => cands.iter().any(|c| match c {
                None => self.matches_missing(),
                Some(v) => {
                    self.matches_value(v)
                        || matches!(v, Bson::Array(a) if a.iter().any(|e| self.matches_value(e)))
                }
            }),
        })
    }

    fn matches_missing(&self) -> bool {
        match self {
            Pred::Eq(Bson::Null) => true,
            Pred::Cmp(CmpOp::Gte | CmpOp::Lte, Bson::Null) => true,
            Pred::In(items) => items.iter().any(|i| matches!(i, InItem::Value(Bson::Null))),
            _ => false,
        }
    }

    /// Element-wise predicates against a single value (no array expansion).
    fn matches_value(&self, v: &Bson) -> bool {
        match self {
            Pred::Eq(want) => values_equal(v, want),
            Pred::Cmp(op, want) => {
                if matches!(want, Bson::MinKey | Bson::MaxKey) {
                    // MinKey/MaxKey compare against every type.
                } else if type_bracket(v) != type_bracket(want) {
                    return false;
                }
                let ord = compare(v, want);
                match op {
                    CmpOp::Gt => ord == Ordering::Greater,
                    CmpOp::Gte => ord != Ordering::Less,
                    CmpOp::Lt => ord == Ordering::Less,
                    CmpOp::Lte => ord != Ordering::Greater,
                }
            }
            Pred::In(items) => items.iter().any(|it| match it {
                InItem::Value(want) => values_equal(v, want),
                InItem::Regex(re) => re.is_match(v),
            }),
            Pred::Type(codes) => {
                let t = type_code(v);
                codes.iter().any(|&c| c == t || (c == 0 && is_number(v)))
            }
            Pred::Regex(re) => re.is_match(v),
            Pred::Mod(div, rem) => match as_f64(v) {
                Some(f) if f.is_finite() => {
                    let i = f.trunc() as i64;
                    i.wrapping_rem(*div) == *rem
                }
                _ => false,
            },
            _ => false,
        }
    }
}

/// Human readable description of a value's type, for error messages.
pub fn describe(v: &Bson) -> String {
    format!("{} {}", type_name(v), v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn m(filter: Document, d: Document) -> bool {
        Matcher::parse(&filter).unwrap().matches(&d).unwrap()
    }

    #[test]
    fn equality_and_arrays() {
        assert!(m(doc! {"a": 1}, doc! {"a": 1.0}));
        assert!(m(doc! {"a": 1}, doc! {"a": [3, 1]}));
        assert!(m(doc! {"a": [3, 1]}, doc! {"a": [3, 1]}));
        assert!(!m(doc! {"a": [1, 3]}, doc! {"a": [3, 1]}));
        assert!(m(doc! {"a.b": 2}, doc! {"a": [{"b": 1}, {"b": 2}]}));
        assert!(m(doc! {"a.1.b": 2}, doc! {"a": [{"b": 1}, {"b": 2}]}));
        assert!(!m(doc! {"a.0.b": 2}, doc! {"a": [{"b": 1}, {"b": 2}]}));
        assert!(m(doc! {"a": {"b": 1}}, doc! {"a": {"b": 1}}));
        assert!(!m(doc! {"a": {"b": 1}}, doc! {"a": {"b": 1, "c": 1}}));
    }

    #[test]
    fn null_and_exists() {
        assert!(m(doc! {"a": null}, doc! {"b": 1}));
        assert!(m(doc! {"a": null}, doc! {"a": null}));
        assert!(!m(doc! {"a": null}, doc! {"a": 0}));
        assert!(m(doc! {"a": {"$ne": null}}, doc! {"a": 0}));
        assert!(!m(doc! {"a": {"$ne": null}}, doc! {}));
        assert!(m(doc! {"a": {"$exists": false}}, doc! {}));
        assert!(m(doc! {"a.b": {"$exists": true}}, doc! {"a": [{"c": 1}, {"b": 1}]}));
        assert!(m(doc! {"a.b": null}, doc! {"a": [{"c": 1}, {"b": 1}]}));
    }

    #[test]
    fn comparisons_bracket_types() {
        assert!(m(doc! {"a": {"$gt": 5}}, doc! {"a": 6i64}));
        assert!(!m(doc! {"a": {"$gt": 5}}, doc! {"a": "6"}));
        assert!(m(doc! {"a": {"$gt": 1, "$lt": 5}}, doc! {"a": [0, 10]}));
        assert!(!m(doc! {"a": {"$elemMatch": {"$gt": 1, "$lt": 5}}}, doc! {"a": [0, 10]}));
        assert!(m(doc! {"a": {"$elemMatch": {"$gt": 1, "$lt": 5}}}, doc! {"a": [0, 3]}));
        assert!(m(doc! {"a": {"$lte": null}}, doc! {}));
    }

    #[test]
    fn logical_and_misc() {
        assert!(m(doc! {"$or": [{"a": 1}, {"b": 2}]}, doc! {"b": 2}));
        assert!(!m(doc! {"$nor": [{"a": 1}, {"b": 2}]}, doc! {"b": 2}));
        assert!(m(doc! {"a": {"$in": [1, 2]}}, doc! {"a": 2}));
        assert!(m(doc! {"a": {"$in": [null]}}, doc! {}));
        assert!(m(doc! {"a": {"$nin": [1, 2]}}, doc! {"a": 3}));
        assert!(m(doc! {"a": {"$all": [1, 2]}}, doc! {"a": [2, 3, 1]}));
        assert!(!m(doc! {"a": {"$all": [1, 2]}}, doc! {"a": [2, 3]}));
        assert!(m(doc! {"a": {"$size": 2}}, doc! {"a": [2, 3]}));
        assert!(m(doc! {"a": {"$type": "number"}}, doc! {"a": 2.5}));
        assert!(m(doc! {"a": {"$type": "array"}}, doc! {"a": []}));
        assert!(m(doc! {"a": {"$mod": [4, 1]}}, doc! {"a": 9}));
        assert!(m(doc! {"a": {"$regex": "^ab", "$options": "i"}}, doc! {"a": "ABc"}));
        assert!(m(doc! {"a": {"$not": {"$gt": 5}}}, doc! {"a": 3}));
        assert!(m(doc! {"a": {"$not": {"$gt": 5}}}, doc! {}));
        assert!(m(doc! {"a": {"$elemMatch": {"x": 1, "y": {"$gt": 1}}}}, doc! {"a": [{"x": 1, "y": 0}, {"x": 1, "y": 2}]}));
        assert!(m(doc! {"$expr": {"$gt": ["$a", "$b"]}}, doc! {"a": 3, "b": 2}));
    }

    #[test]
    fn rejects_bad_operators() {
        assert!(Matcher::parse(&doc! {"a": {"$foo": 1}}).is_err());
        assert!(Matcher::parse(&doc! {"$foo": 1}).is_err());
        assert!(Matcher::parse(&doc! {"$or": []}).is_err());
    }

    #[test]
    fn positional() {
        let q = Matcher::parse(&doc! {"arr.id": 3}).unwrap();
        let d = doc! {"arr": [{"id": 1}, {"id": 3}]};
        assert_eq!(q.positional_index(&d, "arr").unwrap(), Some(1));
    }
}
