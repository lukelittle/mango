//! Aggregation expressions (`{$add: ["$a", 1]}`, `"$field"`, `"$$var"`, ...).
//!
//! Used by `$expr` in queries, by aggregation stages and by pipeline updates.
//! Expressions are parsed once and evaluated against many documents.

use crate::bsonutil::{as_f64, as_i64, is_number, truthy, type_name};
use crate::error::{Error, Result};
use crate::keystring::compare;
use bson::{Bson, DateTime, Document, oid::ObjectId};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Bson),
    /// `$a.b` relative to `$$CURRENT`.
    Field(Vec<String>),
    /// `$$name.a.b`
    Var(String, Vec<String>),
    Object(Vec<(String, Expr)>),
    Array(Vec<Expr>),
    Op(String, Vec<Expr>),
    Cond(Box<Expr>, Box<Expr>, Box<Expr>),
    Switch(Vec<(Expr, Expr)>, Option<Box<Expr>>),
    Let(Vec<(String, Expr)>, Box<Expr>),
    Map {
        input: Box<Expr>,
        var: String,
        body: Box<Expr>,
    },
    Filter {
        input: Box<Expr>,
        var: String,
        cond: Box<Expr>,
        limit: Option<Box<Expr>>,
    },
    Reduce {
        input: Box<Expr>,
        init: Box<Expr>,
        body: Box<Expr>,
    },
    /// Operators taking named arguments, e.g. `$trim: {input, chars}`.
    Named(String, Vec<(String, Expr)>),
}

/// Variable bindings for evaluation. `CURRENT` defaults to `ROOT`.
#[derive(Clone)]
pub struct Vars<'a> {
    pub root: &'a Document,
    bindings: Vec<(String, Bson)>,
}

impl<'a> Vars<'a> {
    pub fn root(doc: &'a Document) -> Vars<'a> {
        Vars { root: doc, bindings: Vec::new() }
    }

    pub fn with_bindings(doc: &'a Document, env: &[(String, Bson)]) -> Vars<'a> {
        Vars { root: doc, bindings: env.to_vec() }
    }

    pub fn with(&self, name: &str, v: Bson) -> Vars<'a> {
        let mut b = self.bindings.clone();
        b.push((name.to_string(), v));
        Vars { root: self.root, bindings: b }
    }

    fn get(&self, name: &str) -> Result<Option<Bson>> {
        if let Some((_, v)) = self.bindings.iter().rev().find(|(n, _)| n == name) {
            return Ok(Some(v.clone()));
        }
        match name {
            "ROOT" | "CURRENT" => Ok(Some(Bson::Document(self.root.clone()))),
            "REMOVE" => Ok(None),
            "NOW" => Ok(Some(Bson::DateTime(DateTime::now()))),
            _ => Err(Error::failed_to_parse(format!("Use of undefined variable: {name}"))),
        }
    }

    fn current_field(&self, parts: &[String]) -> Option<Bson> {
        if let Some((_, v)) = self.bindings.iter().rev().find(|(n, _)| n == "CURRENT") {
            return path_value(v, parts);
        }
        let first = self.root.get(&parts[0])?;
        path_value(first, &parts[1..])
    }
}

/// Aggregation-style path navigation: arrays map over their elements.
pub fn path_value(v: &Bson, parts: &[String]) -> Option<Bson> {
    if parts.is_empty() {
        return Some(v.clone());
    }
    match v {
        Bson::Document(d) => d.get(&parts[0]).and_then(|x| path_value(x, &parts[1..])),
        Bson::Array(arr) => {
            let mut out = Vec::new();
            for e in arr {
                if matches!(e, Bson::Document(_) | Bson::Array(_))
                    && let Some(x) = path_value(e, parts)
                {
                    out.push(x);
                }
            }
            Some(Bson::Array(out))
        }
        _ => None,
    }
}

fn parse_field_path(s: &str) -> Result<Vec<String>> {
    if s.is_empty() || s.split('.').any(|p| p.is_empty() || p.starts_with('$')) {
        return Err(Error::failed_to_parse(format!("Invalid field path: '${s}'")));
    }
    Ok(s.split('.').map(String::from).collect())
}

fn args_of(v: &Bson) -> Vec<&Bson> {
    match v {
        Bson::Array(a) => a.iter().collect(),
        other => vec![other],
    }
}

const NAMED_OPS: &[&str] = &[
    "$trim",
    "$ltrim",
    "$rtrim",
    "$regexMatch",
    "$regexFind",
    "$replaceOne",
    "$replaceAll",
    "$dateToString",
    "$convert",
    "$getField",
    "$dateFromParts",
    "$sortArray",
];

impl Expr {
    pub fn parse(v: &Bson) -> Result<Expr> {
        Ok(match v {
            Bson::String(s) if s.starts_with("$$") => {
                let rest = &s[2..];
                let mut it = rest.splitn(2, '.');
                let name = it.next().unwrap_or("").to_string();
                if name.is_empty() {
                    return Err(Error::failed_to_parse("empty variable name"));
                }
                let path = match it.next() {
                    Some(p) => parse_field_path(p)?,
                    None => vec![],
                };
                Expr::Var(name, path)
            }
            Bson::String(s) if s.starts_with('$') => Expr::Field(parse_field_path(&s[1..])?),
            Bson::Array(a) => Expr::Array(a.iter().map(Expr::parse).collect::<Result<_>>()?),
            Bson::Document(d) => {
                let first = d.keys().next();
                match first {
                    Some(k) if k.starts_with('$') => {
                        if d.len() != 1 {
                            return Err(Error::failed_to_parse(format!(
                                "an expression specification must contain exactly one field, the name of the expression. Found {} fields",
                                d.len()
                            )));
                        }
                        let (op, arg) = d.iter().next().unwrap();
                        Expr::parse_op(op, arg)?
                    }
                    _ => {
                        let mut fields = Vec::new();
                        for (k, v) in d {
                            if k.starts_with('$') {
                                return Err(Error::failed_to_parse(format!("field names may not start with '$': {k}")));
                            }
                            fields.push((k.clone(), Expr::parse(v)?));
                        }
                        Expr::Object(fields)
                    }
                }
            }
            other => Expr::Literal(other.clone()),
        })
    }

    fn parse_op(op: &str, arg: &Bson) -> Result<Expr> {
        let named = |arg: &Bson| -> Result<Vec<(String, Expr)>> {
            let Bson::Document(d) = arg else {
                return Err(Error::failed_to_parse(format!("{op} requires an object as an argument")));
            };
            d.iter().map(|(k, v)| Ok((k.clone(), Expr::parse(v)?))).collect()
        };
        let get = |d: &Document, k: &str| -> Result<Box<Expr>> {
            d.get(k)
                .map(|v| Expr::parse(v).map(Box::new))
                .unwrap_or_else(|| Err(Error::failed_to_parse(format!("Missing '{k}' parameter to {op}"))))
        };
        Ok(match op {
            "$literal" => Expr::Literal(arg.clone()),
            "$cond" => match arg {
                Bson::Array(a) if a.len() == 3 => {
                    Expr::Cond(Box::new(Expr::parse(&a[0])?), Box::new(Expr::parse(&a[1])?), Box::new(Expr::parse(&a[2])?))
                }
                Bson::Document(d) => Expr::Cond(get(d, "if")?, get(d, "then")?, get(d, "else")?),
                _ => return Err(Error::failed_to_parse("$cond requires 3 arguments")),
            },
            "$switch" => {
                let Bson::Document(d) = arg else { return Err(Error::failed_to_parse("$switch requires an object")) };
                let Some(Bson::Array(branches)) = d.get("branches") else {
                    return Err(Error::failed_to_parse("$switch requires at least one branch"));
                };
                let mut bs = Vec::new();
                for b in branches {
                    let Bson::Document(bd) = b else { return Err(Error::failed_to_parse("$switch branch must be an object")) };
                    bs.push((*get(bd, "case")?, *get(bd, "then")?));
                }
                let default = d.get("default").map(Expr::parse).transpose()?.map(Box::new);
                Expr::Switch(bs, default)
            }
            "$let" => {
                let Bson::Document(d) = arg else { return Err(Error::failed_to_parse("$let only supports an object")) };
                let Some(Bson::Document(vars)) = d.get("vars") else {
                    return Err(Error::failed_to_parse("Missing 'vars' parameter to $let"));
                };
                let vs = vars.iter().map(|(k, v)| Ok((k.clone(), Expr::parse(v)?))).collect::<Result<_>>()?;
                Expr::Let(vs, get(d, "in")?)
            }
            "$map" => {
                let Bson::Document(d) = arg else { return Err(Error::failed_to_parse("$map only supports an object")) };
                let var = d.get_str("as").unwrap_or("this").to_string();
                Expr::Map { input: get(d, "input")?, var, body: get(d, "in")? }
            }
            "$filter" => {
                let Bson::Document(d) = arg else { return Err(Error::failed_to_parse("$filter only supports an object")) };
                let var = d.get_str("as").unwrap_or("this").to_string();
                let limit = d.get("limit").map(Expr::parse).transpose()?.map(Box::new);
                Expr::Filter { input: get(d, "input")?, var, cond: get(d, "cond")?, limit }
            }
            "$reduce" => {
                let Bson::Document(d) = arg else { return Err(Error::failed_to_parse("$reduce only supports an object")) };
                Expr::Reduce { input: get(d, "input")?, init: get(d, "initialValue")?, body: get(d, "in")? }
            }
            op if NAMED_OPS.contains(&op) => Expr::Named(op.to_string(), named(arg)?),
            op if KNOWN_OPS.contains(&op) => Expr::Op(op.to_string(), args_of(arg).into_iter().map(Expr::parse).collect::<Result<_>>()?),
            op => return Err(Error::invalid_options(format!("Unrecognized expression '{op}'"))),
        })
    }

    pub fn eval(&self, vars: &Vars) -> Result<Bson> {
        Ok(self.eval_opt(vars)?.unwrap_or(Bson::Null))
    }

    /// Evaluates the expression; `None` means "missing" (e.g. a missing field
    /// or `$$REMOVE`), which callers building documents drop.
    pub fn eval_opt(&self, vars: &Vars) -> Result<Option<Bson>> {
        Ok(match self {
            Expr::Literal(v) => Some(v.clone()),
            Expr::Field(parts) => vars.current_field(parts),
            Expr::Var(name, parts) => match vars.get(name)? {
                None => None,
                Some(v) => path_value(&v, parts),
            },
            Expr::Object(fields) => {
                let mut d = Document::new();
                for (k, e) in fields {
                    if let Some(v) = e.eval_opt(vars)? {
                        d.insert(k.clone(), v);
                    }
                }
                Some(Bson::Document(d))
            }
            Expr::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for e in items {
                    out.push(e.eval_opt(vars)?.unwrap_or(Bson::Null));
                }
                Some(Bson::Array(out))
            }
            Expr::Cond(c, t, f) => {
                if truthy(&c.eval(vars)?) {
                    t.eval_opt(vars)?
                } else {
                    f.eval_opt(vars)?
                }
            }
            Expr::Switch(branches, default) => {
                for (case, then) in branches {
                    if truthy(&case.eval(vars)?) {
                        return then.eval_opt(vars);
                    }
                }
                match default {
                    Some(d) => d.eval_opt(vars)?,
                    None => {
                        return Err(Error::bad_value(
                            "$switch could not find a matching branch for an input, and no default was specified.",
                        ));
                    }
                }
            }
            Expr::Let(defs, body) => {
                let mut v2 = vars.clone();
                for (name, e) in defs {
                    let val = e.eval(vars)?;
                    v2 = v2.with(name, val);
                }
                body.eval_opt(&v2)?
            }
            Expr::Map { input, var, body } => match input.eval(vars)? {
                Bson::Null => Some(Bson::Null),
                Bson::Array(a) => {
                    let mut out = Vec::with_capacity(a.len());
                    for e in a {
                        out.push(body.eval(&vars.with(var, e))?);
                    }
                    Some(Bson::Array(out))
                }
                other => return Err(Error::bad_value(format!("input to $map must be an array not {}", type_name(&other)))),
            },
            Expr::Filter { input, var, cond, limit } => {
                let limit = match limit {
                    Some(l) => match l.eval(vars)? {
                        Bson::Null => None,
                        v => Some(
                            as_i64(&v).filter(|n| *n > 0).ok_or_else(|| Error::bad_value("$filter: limit must be a positive integer"))?
                                as usize,
                        ),
                    },
                    None => None,
                };
                match input.eval(vars)? {
                    Bson::Null => Some(Bson::Null),
                    Bson::Array(a) => {
                        let mut out = Vec::new();
                        for e in a {
                            if limit.is_some_and(|l| out.len() >= l) {
                                break;
                            }
                            if truthy(&cond.eval(&vars.with(var, e.clone()))?) {
                                out.push(e);
                            }
                        }
                        Some(Bson::Array(out))
                    }
                    other => {
                        return Err(Error::bad_value(format!("input to $filter must be an array not {}", type_name(&other))));
                    }
                }
            }
            Expr::Reduce { input, init, body } => match input.eval(vars)? {
                Bson::Null => Some(Bson::Null),
                Bson::Array(a) => {
                    let mut acc = init.eval(vars)?;
                    for e in a {
                        let v2 = vars.with("value", acc).with("this", e);
                        acc = body.eval(&v2)?;
                    }
                    Some(acc)
                }
                other => return Err(Error::bad_value(format!("input to $reduce must be an array not {}", type_name(&other)))),
            },
            Expr::Named(op, args) => eval_named(op, args, vars)?,
            Expr::Op(op, args) => {
                if op == "$ifNull" {
                    for a in args {
                        match a.eval_opt(vars)? {
                            None | Some(Bson::Null) | Some(Bson::Undefined) => continue,
                            Some(v) => return Ok(Some(v)),
                        }
                    }
                    return Ok(Some(Bson::Null));
                }
                if op == "$and" {
                    for a in args {
                        if !truthy(&a.eval(vars)?) {
                            return Ok(Some(Bson::Boolean(false)));
                        }
                    }
                    return Ok(Some(Bson::Boolean(true)));
                }
                if op == "$or" {
                    for a in args {
                        if truthy(&a.eval(vars)?) {
                            return Ok(Some(Bson::Boolean(true)));
                        }
                    }
                    return Ok(Some(Bson::Boolean(false)));
                }
                let mut vals = Vec::with_capacity(args.len());
                let mut missing = Vec::with_capacity(args.len());
                for a in args {
                    let v = a.eval_opt(vars)?;
                    missing.push(v.is_none());
                    vals.push(v.unwrap_or(Bson::Null));
                }
                if op == "$type" {
                    arity(op, &vals, 1)?;
                    return Ok(Some(Bson::String(if missing[0] { "missing".into() } else { type_name(&vals[0]).into() })));
                }
                Some(eval_op(op, vals)?)
            }
        })
    }
}

const KNOWN_OPS: &[&str] = &[
    "$add",
    "$subtract",
    "$multiply",
    "$divide",
    "$mod",
    "$abs",
    "$ceil",
    "$floor",
    "$round",
    "$trunc",
    "$pow",
    "$sqrt",
    "$exp",
    "$ln",
    "$log",
    "$log10",
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$cmp",
    "$and",
    "$or",
    "$not",
    "$concat",
    "$toLower",
    "$toUpper",
    "$substr",
    "$substrBytes",
    "$substrCP",
    "$strLenBytes",
    "$strLenCP",
    "$split",
    "$indexOfBytes",
    "$indexOfCP",
    "$strcasecmp",
    "$size",
    "$arrayElemAt",
    "$first",
    "$last",
    "$concatArrays",
    "$in",
    "$indexOfArray",
    "$isArray",
    "$reverseArray",
    "$slice",
    "$range",
    "$setUnion",
    "$setIntersection",
    "$setDifference",
    "$setEquals",
    "$setIsSubset",
    "$allElementsTrue",
    "$anyElementTrue",
    "$mergeObjects",
    "$objectToArray",
    "$arrayToObject",
    "$type",
    "$toString",
    "$toInt",
    "$toLong",
    "$toDouble",
    "$toBool",
    "$toObjectId",
    "$toDate",
    "$isNumber",
    "$year",
    "$month",
    "$dayOfMonth",
    "$hour",
    "$minute",
    "$second",
    "$millisecond",
    "$dayOfWeek",
    "$dayOfYear",
    "$isoDayOfWeek",
    "$sum",
    "$avg",
    "$min",
    "$max",
    "$ifNull",
    "$rand",
    "$toHashedIndexKey",
    "$zip",
    "$sortArray",
];

fn arity(op: &str, vals: &[Bson], n: usize) -> Result<()> {
    if vals.len() != n {
        return Err(Error::failed_to_parse(format!("Expression {op} takes exactly {n} arguments. {} were passed in.", vals.len())));
    }
    Ok(())
}

fn is_nullish(v: &Bson) -> bool {
    matches!(v, Bson::Null | Bson::Undefined)
}

// ---------- numeric helpers ----------

/// Adds two numbers with MongoDB's type promotion (int -> long -> double).
pub fn num_add(a: &Bson, b: &Bson) -> Option<Bson> {
    Some(match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => match x.checked_add(*y) {
            Some(r) => Bson::Int32(r),
            None => Bson::Int64(*x as i64 + *y as i64),
        },
        (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => {
            let (x, y) = (as_i64(a)?, as_i64(b)?);
            match x.checked_add(y) {
                Some(r) => Bson::Int64(r),
                None => Bson::Double(x as f64 + y as f64),
            }
        }
        _ => Bson::Double(as_f64(a)? + as_f64(b)?),
    })
}

pub fn num_mul(a: &Bson, b: &Bson) -> Option<Bson> {
    Some(match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => match x.checked_mul(*y) {
            Some(r) => Bson::Int32(r),
            None => Bson::Int64(*x as i64 * *y as i64),
        },
        (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => {
            let (x, y) = (as_i64(a)?, as_i64(b)?);
            match x.checked_mul(y) {
                Some(r) => Bson::Int64(r),
                None => Bson::Double(x as f64 * y as f64),
            }
        }
        _ => Bson::Double(as_f64(a)? * as_f64(b)?),
    })
}

fn num_sub(a: &Bson, b: &Bson) -> Option<Bson> {
    let neg = match b {
        Bson::Int32(y) => match y.checked_neg() {
            Some(n) => Bson::Int32(n),
            None => Bson::Int64(-(*y as i64)),
        },
        Bson::Int64(y) => match y.checked_neg() {
            Some(n) => Bson::Int64(n),
            None => Bson::Double(-(*y as f64)),
        },
        other => Bson::Double(-as_f64(other)?),
    };
    num_add(a, &neg)
}

fn need_num(op: &str, v: &Bson) -> Result<f64> {
    as_f64(v).ok_or_else(|| Error::type_mismatch(format!("{op} only supports numeric types, not {}", type_name(v))))
}

/// Rounds half to even at `place` decimal places (MongoDB's `$round`).
fn round_half_even(x: f64, place: i32) -> f64 {
    let m = 10f64.powi(place);
    let y = x * m;
    let r = y.round();
    let r = if (y - y.trunc()).abs() == 0.5 { 2.0 * (y / 2.0).round() } else { r };
    r / m
}

fn keep_int_type(orig: &Bson, f: f64) -> Bson {
    match orig {
        Bson::Int32(_) if f >= i32::MIN as f64 && f <= i32::MAX as f64 => Bson::Int32(f as i32),
        Bson::Int64(_) | Bson::Int32(_) if f.abs() < 9.2e18 => Bson::Int64(f as i64),
        _ => Bson::Double(f),
    }
}

// ---------- date helpers ----------

/// (year, month 1-12, day 1-31) from days since 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

struct DateParts {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millis: u32,
    day_of_week: u32, // 1 = Sunday
    day_of_year: u32,
}

fn date_parts(ms: i64) -> DateParts {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let day_of_year = (days - days_from_civil(year, 1, 1) + 1) as u32;
    // 1970-01-01 was a Thursday (5 when Sunday = 1).
    let day_of_week = ((days + 4).rem_euclid(7) + 1) as u32;
    DateParts {
        year,
        month,
        day,
        hour: (rem / 3_600_000) as u32,
        minute: (rem / 60_000 % 60) as u32,
        second: (rem / 1000 % 60) as u32,
        millis: (rem % 1000) as u32,
        day_of_week,
        day_of_year,
    }
}

fn to_date_millis(op: &str, v: &Bson) -> Result<i64> {
    match v {
        Bson::DateTime(d) => Ok(d.timestamp_millis()),
        Bson::Timestamp(t) => Ok(t.time as i64 * 1000),
        Bson::ObjectId(o) => Ok(o.timestamp().timestamp_millis()),
        other => Err(Error::type_mismatch(format!("{op}: can't convert from BSON type {} to Date", type_name(other)))),
    }
}

fn format_date(fmt: &str, ms: i64) -> Result<String> {
    let p = date_parts(ms);
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('Y') => out.push_str(&format!("{:04}", p.year)),
            Some('m') => out.push_str(&format!("{:02}", p.month)),
            Some('d') => out.push_str(&format!("{:02}", p.day)),
            Some('H') => out.push_str(&format!("{:02}", p.hour)),
            Some('M') => out.push_str(&format!("{:02}", p.minute)),
            Some('S') => out.push_str(&format!("{:02}", p.second)),
            Some('L') => out.push_str(&format!("{:03}", p.millis)),
            Some('j') => out.push_str(&format!("{:03}", p.day_of_year)),
            Some('w') => out.push_str(&format!("{}", p.day_of_week)),
            Some('u') => out.push_str(&format!("{}", if p.day_of_week == 1 { 7 } else { p.day_of_week - 1 })),
            Some('z') => out.push_str("+0000"),
            Some('Z') => out.push('0'),
            Some('%') => out.push('%'),
            other => {
                return Err(Error::bad_value(format!(
                    "Invalid format character '%{}' in format string",
                    other.map(String::from).unwrap_or_default()
                )));
            }
        }
    }
    Ok(out)
}

fn format_date_iso(ms: i64) -> String {
    format_date("%Y-%m-%dT%H:%M:%S.%LZ", ms).unwrap_or_default()
}

/// Parses the ISO-8601 subset MongoDB's `$toDate` commonly sees.
fn parse_date_string(s: &str) -> Option<i64> {
    if let Ok(d) = DateTime::parse_rfc3339_str(s) {
        return Some(d.timestamp_millis());
    }
    let b = s.as_bytes();
    if b.len() == 10 && b[4] == b'-' && b[7] == b'-' {
        let y: i64 = s[0..4].parse().ok()?;
        let m: u32 = s[5..7].parse().ok()?;
        let d: u32 = s[8..10].parse().ok()?;
        if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        return Some(days_from_civil(y, m, d) * 86_400_000);
    }
    None
}

// ---------- conversion ----------

pub fn to_string_value(v: &Bson) -> Result<Bson> {
    Ok(match v {
        Bson::Null | Bson::Undefined => Bson::Null,
        Bson::String(s) | Bson::Symbol(s) => Bson::String(s.clone()),
        Bson::Int32(i) => Bson::String(i.to_string()),
        Bson::Int64(i) => Bson::String(i.to_string()),
        Bson::Double(d) => Bson::String(format_double(*d)),
        Bson::Decimal128(d) => Bson::String(d.to_string()),
        Bson::Boolean(b) => Bson::String(b.to_string()),
        Bson::ObjectId(o) => Bson::String(o.to_hex()),
        Bson::DateTime(d) => Bson::String(format_date_iso(d.timestamp_millis())),
        other => {
            return Err(Error::type_mismatch(format!("Unsupported conversion from {} to string", type_name(other))));
        }
    })
}

fn format_double(d: f64) -> String {
    if d.is_nan() {
        "NaN".into()
    } else if d.is_infinite() {
        if d > 0.0 { "Infinity".into() } else { "-Infinity".into() }
    } else {
        format!("{d}")
    }
}

fn convert(v: &Bson, to: &str) -> Result<Bson> {
    let fail = || Error::failed_to_parse(format!("Unsupported conversion from {} to {to}", type_name(v)));
    if is_nullish(v) {
        return Ok(Bson::Null);
    }
    Ok(match to {
        "string" => to_string_value(v)?,
        "bool" => Bson::Boolean(truthy(v)),
        "double" => match v {
            Bson::String(s) => Bson::Double(s.trim().parse().map_err(|_| fail())?),
            Bson::Boolean(b) => Bson::Double(if *b { 1.0 } else { 0.0 }),
            Bson::DateTime(d) => Bson::Double(d.timestamp_millis() as f64),
            v => Bson::Double(as_f64(v).ok_or_else(fail)?),
        },
        "int" | "long" => {
            let i: i64 = match v {
                Bson::String(s) => s.trim().parse().map_err(|_| fail())?,
                Bson::Boolean(b) => *b as i64,
                Bson::DateTime(d) if to == "long" => d.timestamp_millis(),
                v => {
                    let f = as_f64(v).ok_or_else(fail)?;
                    if !f.is_finite() || f.abs() >= 9.2e18 {
                        return Err(Error::failed_to_parse(format!("Conversion would overflow target type in $convert with {f}")));
                    }
                    f.trunc() as i64
                }
            };
            if to == "int" {
                Bson::Int32(
                    i32::try_from(i)
                        .map_err(|_| Error::failed_to_parse(format!("Conversion would overflow target type in $convert with {i}")))?,
                )
            } else {
                Bson::Int64(i)
            }
        }
        "objectId" => match v {
            Bson::ObjectId(o) => Bson::ObjectId(*o),
            Bson::String(s) => Bson::ObjectId(ObjectId::parse_str(s).map_err(|_| fail())?),
            _ => return Err(fail()),
        },
        "date" => match v {
            Bson::String(s) => Bson::DateTime(DateTime::from_millis(parse_date_string(s).ok_or_else(fail)?)),
            Bson::Int64(i) => Bson::DateTime(DateTime::from_millis(*i)),
            Bson::Double(d) => Bson::DateTime(DateTime::from_millis(*d as i64)),
            other => Bson::DateTime(DateTime::from_millis(to_date_millis("$convert", other)?)),
        },
        _ => return Err(Error::bad_value(format!("Unknown type name: {to}"))),
    })
}

fn named_arg<'e>(args: &'e [(String, Expr)], k: &str) -> Option<&'e Expr> {
    args.iter().find(|(n, _)| n == k).map(|(_, e)| e)
}

fn eval_named(op: &str, args: &[(String, Expr)], vars: &Vars) -> Result<Option<Bson>> {
    let req =
        |k: &str| -> Result<Bson> { named_arg(args, k).ok_or_else(|| Error::failed_to_parse(format!("{op} requires '{k}'")))?.eval(vars) };
    let opt = |k: &str| -> Result<Option<Bson>> { named_arg(args, k).map(|e| e.eval(vars)).transpose() };
    Ok(Some(match op {
        "$trim" | "$ltrim" | "$rtrim" => {
            let input = req("input")?;
            let chars = opt("chars")?;
            let s = match input {
                Bson::String(s) => s,
                v if is_nullish(&v) => return Ok(Some(Bson::Null)),
                v => return Err(Error::type_mismatch(format!("{op} requires its input to be a string, got {}", type_name(&v)))),
            };
            let set: Vec<char> = match chars {
                Some(Bson::String(c)) => c.chars().collect(),
                Some(v) if !is_nullish(&v) => return Err(Error::type_mismatch(format!("{op} requires 'chars' to be a string"))),
                _ => vec![' ', '\t', '\n', '\r', '\x0B', '\x0C', '\0', '\u{00A0}'],
            };
            let f = |c: char| set.contains(&c);
            Bson::String(match op {
                "$trim" => s.trim_matches(f).to_string(),
                "$ltrim" => s.trim_start_matches(f).to_string(),
                _ => s.trim_end_matches(f).to_string(),
            })
        }
        "$regexMatch" | "$regexFind" => {
            let input = req("input")?;
            let (pattern, mut opts) = match req("regex")? {
                Bson::String(s) => (s, String::new()),
                Bson::RegularExpression(r) => (r.pattern.to_string(), r.options.to_string()),
                v => return Err(Error::type_mismatch(format!("{op} needs 'regex' to be of type string or regex, got {}", type_name(&v)))),
            };
            if let Some(Bson::String(o)) = opt("options")? {
                opts.push_str(&o);
            }
            let re = crate::query::CompiledRegex::new(&pattern, &opts)?;
            match input {
                Bson::String(s) => {
                    if op == "$regexMatch" {
                        Bson::Boolean(re.re.is_match(&s))
                    } else {
                        match re.re.captures(&s) {
                            None => Bson::Null,
                            Some(c) => {
                                let m = c.get(0).unwrap();
                                let idx = s[..m.start()].chars().count() as i32;
                                let caps: Vec<Bson> =
                                    c.iter().skip(1).map(|g| g.map(|g| Bson::String(g.as_str().into())).unwrap_or(Bson::Null)).collect();
                                Bson::Document(bson::doc! {"match": m.as_str(), "idx": idx, "captures": caps})
                            }
                        }
                    }
                }
                v if is_nullish(&v) => {
                    if op == "$regexMatch" {
                        Bson::Boolean(false)
                    } else {
                        Bson::Null
                    }
                }
                v => return Err(Error::type_mismatch(format!("{op} needs 'input' to be of type string, got {}", type_name(&v)))),
            }
        }
        "$replaceOne" | "$replaceAll" => {
            let (input, find, repl) = (req("input")?, req("find")?, req("replacement")?);
            if is_nullish(&input) || is_nullish(&find) || is_nullish(&repl) {
                return Ok(Some(Bson::Null));
            }
            match (input, find, repl) {
                (Bson::String(i), Bson::String(f), Bson::String(r)) => Bson::String(if op == "$replaceOne" {
                    i.replacen(&f, &r, 1)
                } else if f.is_empty() {
                    i
                } else {
                    i.replace(&f, &r)
                }),
                _ => return Err(Error::type_mismatch(format!("{op} requires string arguments"))),
            }
        }
        "$dateToString" => {
            let date = req("date")?;
            if is_nullish(&date) {
                return Ok(Some(opt("onNull")?.unwrap_or(Bson::Null)));
            }
            let ms = to_date_millis(op, &date)?;
            match opt("format")? {
                Some(Bson::String(f)) => Bson::String(format_date(&f, ms)?),
                None => Bson::String(format_date_iso(ms)),
                Some(v) => {
                    return Err(Error::type_mismatch(format!(
                        "$dateToString requires that 'format' be a string, found: {}",
                        type_name(&v)
                    )));
                }
            }
        }
        "$dateFromParts" => {
            let num = |k: &str, d: i64| -> Result<i64> {
                match opt(k)? {
                    None | Some(Bson::Null) => Ok(d),
                    Some(v) => as_i64(&v).ok_or_else(|| Error::type_mismatch(format!("'{k}' must evaluate to an integer"))),
                }
            };
            let year = num("year", 1970)?;
            let month = num("month", 1)?;
            let day = num("day", 1)?;
            // Months overflow into years, days into months, like MongoDB.
            let total_months = year * 12 + (month - 1);
            let (y, m) = (total_months.div_euclid(12), (total_months.rem_euclid(12) + 1) as u32);
            let days = days_from_civil(y, m, 1) + day - 1;
            let ms = days * 86_400_000
                + num("hour", 0)? * 3_600_000
                + num("minute", 0)? * 60_000
                + num("second", 0)? * 1000
                + num("millisecond", 0)?;
            Bson::DateTime(DateTime::from_millis(ms))
        }
        "$convert" => {
            let input =
                named_arg(args, "input").ok_or_else(|| Error::failed_to_parse("Missing 'input' parameter to $convert"))?.eval_opt(vars)?;
            let to = match req("to")? {
                Bson::String(s) => s,
                v => match as_i64(&v) {
                    Some(1) => "double".into(),
                    Some(2) => "string".into(),
                    Some(7) => "objectId".into(),
                    Some(8) => "bool".into(),
                    Some(9) => "date".into(),
                    Some(16) => "int".into(),
                    Some(18) => "long".into(),
                    _ => return Err(Error::bad_value("Unknown type in $convert 'to'")),
                },
            };
            match input {
                None | Some(Bson::Null) => opt("onNull")?.unwrap_or(Bson::Null),
                Some(v) => match convert(&v, &to) {
                    Ok(r) => r,
                    Err(e) => match opt("onError")? {
                        Some(fallback) => fallback,
                        None => return Err(e),
                    },
                },
            }
        }
        "$getField" => {
            let field = match req("field")? {
                Bson::String(s) => s,
                _ => return Err(Error::bad_value("$getField requires 'field' to evaluate to type String")),
            };
            let input = match named_arg(args, "input") {
                Some(e) => e.eval(vars)?,
                None => Bson::Document(vars.root.clone()),
            };
            match input {
                Bson::Document(d) => return Ok(d.get(&field).cloned()),
                _ => return Ok(None),
            }
        }
        "$sortArray" => {
            let input = req("input")?;
            let sort_by = req("sortBy")?;
            let Bson::Array(mut a) = input else {
                if is_nullish(&input) {
                    return Ok(Some(Bson::Null));
                }
                return Err(Error::type_mismatch("$sortArray requires an array input"));
            };
            match sort_by {
                Bson::Document(spec) => {
                    let spec = crate::aggregate::SortSpec::parse(&spec)?;
                    a.sort_by(|x, y| match (x, y) {
                        (Bson::Document(dx), Bson::Document(dy)) => spec.compare(dx, dy),
                        _ => compare(x, y),
                    });
                }
                v => {
                    let dir = as_i64(&v).unwrap_or(1);
                    a.sort_by(|x, y| if dir < 0 { compare(y, x) } else { compare(x, y) });
                }
            }
            Bson::Array(a)
        }
        _ => return Err(Error::invalid_options(format!("Unrecognized expression '{op}'"))),
    }))
}

fn eval_op(op: &str, mut vals: Vec<Bson>) -> Result<Bson> {
    Ok(match op {
        "$add" => {
            let mut acc = Bson::Int32(0);
            let mut date: Option<i64> = None;
            for v in &vals {
                match v {
                    Bson::Null | Bson::Undefined => return Ok(Bson::Null),
                    Bson::DateTime(d) => {
                        if date.is_some() {
                            return Err(Error::type_mismatch("only one date allowed in an $add expression"));
                        }
                        date = Some(d.timestamp_millis());
                    }
                    v if is_number(v) => acc = num_add(&acc, v).unwrap(),
                    v => return Err(Error::type_mismatch(format!("$add only supports numeric or date types, not {}", type_name(v)))),
                }
            }
            match date {
                Some(ms) => Bson::DateTime(DateTime::from_millis(ms + as_f64(&acc).unwrap().round() as i64)),
                None => acc,
            }
        }
        "$subtract" => {
            arity(op, &vals, 2)?;
            match (&vals[0], &vals[1]) {
                (a, b) if is_nullish(a) || is_nullish(b) => Bson::Null,
                (Bson::DateTime(a), Bson::DateTime(b)) => Bson::Int64(a.timestamp_millis() - b.timestamp_millis()),
                (Bson::DateTime(a), b) if is_number(b) => {
                    Bson::DateTime(DateTime::from_millis(a.timestamp_millis() - as_f64(b).unwrap().round() as i64))
                }
                (a, b) if is_number(a) && is_number(b) => num_sub(a, b).unwrap(),
                (a, b) => {
                    return Err(Error::type_mismatch(format!("can't $subtract {} from {}", type_name(b), type_name(a))));
                }
            }
        }
        "$multiply" => {
            let mut acc = Bson::Int32(1);
            for v in &vals {
                if is_nullish(v) {
                    return Ok(Bson::Null);
                }
                if !is_number(v) {
                    return Err(Error::type_mismatch(format!("$multiply only supports numeric types, not {}", type_name(v))));
                }
                acc = num_mul(&acc, v).unwrap();
            }
            acc
        }
        "$divide" => {
            arity(op, &vals, 2)?;
            if is_nullish(&vals[0]) || is_nullish(&vals[1]) {
                return Ok(Bson::Null);
            }
            let (a, b) = (need_num(op, &vals[0])?, need_num(op, &vals[1])?);
            if b == 0.0 {
                return Err(Error::bad_value("can't $divide by zero"));
            }
            Bson::Double(a / b)
        }
        "$mod" => {
            arity(op, &vals, 2)?;
            if is_nullish(&vals[0]) || is_nullish(&vals[1]) {
                return Ok(Bson::Null);
            }
            match (&vals[0], &vals[1]) {
                (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => {
                    let (a, b) = (as_i64(&vals[0]).unwrap(), as_i64(&vals[1]).unwrap());
                    if b == 0 {
                        return Err(Error::bad_value("can't $mod by zero"));
                    }
                    let r = a.wrapping_rem(b);
                    if matches!((&vals[0], &vals[1]), (Bson::Int32(_), Bson::Int32(_))) { Bson::Int32(r as i32) } else { Bson::Int64(r) }
                }
                _ => {
                    let (a, b) = (need_num(op, &vals[0])?, need_num(op, &vals[1])?);
                    if b == 0.0 {
                        return Err(Error::bad_value("can't $mod by zero"));
                    }
                    Bson::Double(a % b)
                }
            }
        }
        "$abs" | "$ceil" | "$floor" | "$sqrt" | "$exp" | "$ln" | "$log10" => {
            arity(op, &vals, 1)?;
            let v = &vals[0];
            if is_nullish(v) {
                return Ok(Bson::Null);
            }
            let f = need_num(op, v)?;
            match op {
                "$abs" => match v {
                    Bson::Int32(i) => i.checked_abs().map(Bson::Int32).unwrap_or(Bson::Int64((*i as i64).abs())),
                    Bson::Int64(i) => i.checked_abs().map(Bson::Int64).unwrap_or(Bson::Double(f.abs())),
                    _ => Bson::Double(f.abs()),
                },
                "$ceil" => keep_int_type(v, f.ceil()),
                "$floor" => keep_int_type(v, f.floor()),
                "$sqrt" => {
                    if f < 0.0 {
                        return Err(Error::bad_value("$sqrt's argument must be greater than or equal to 0"));
                    }
                    Bson::Double(f.sqrt())
                }
                "$exp" => Bson::Double(f.exp()),
                "$ln" => {
                    if f <= 0.0 {
                        return Err(Error::bad_value("$ln's argument must be a positive number"));
                    }
                    Bson::Double(f.ln())
                }
                _ => {
                    if f <= 0.0 {
                        return Err(Error::bad_value("$log10's argument must be a positive number"));
                    }
                    Bson::Double(f.log10())
                }
            }
        }
        "$round" | "$trunc" => {
            if vals.is_empty() || vals.len() > 2 {
                return Err(Error::failed_to_parse(format!("{op} takes 1 or 2 arguments")));
            }
            let place = match vals.get(1) {
                None => 0,
                Some(p) => as_i64(p)
                    .filter(|p| (-20..100).contains(p))
                    .ok_or_else(|| Error::bad_value(format!("{op} place must be an integer between -20 and 100")))?
                    as i32,
            };
            let v = &vals[0];
            if is_nullish(v) {
                return Ok(Bson::Null);
            }
            let f = need_num(op, v)?;
            let r = if op == "$round" {
                round_half_even(f, place)
            } else {
                let m = 10f64.powi(place);
                (f * m).trunc() / m
            };
            match v {
                Bson::Double(_) | Bson::Decimal128(_) => Bson::Double(r),
                _ => keep_int_type(v, r),
            }
        }
        "$pow" => {
            arity(op, &vals, 2)?;
            if is_nullish(&vals[0]) || is_nullish(&vals[1]) {
                return Ok(Bson::Null);
            }
            let (b, e) = (need_num(op, &vals[0])?, need_num(op, &vals[1])?);
            if b == 0.0 && e < 0.0 {
                return Err(Error::bad_value("$pow cannot take a base of 0 and a negative exponent"));
            }
            match (&vals[0], &vals[1]) {
                (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) if e >= 0.0 => {
                    let r = as_i64(&vals[0]).unwrap().checked_pow(e as u32).filter(|_| e <= u32::MAX as f64);
                    match r {
                        Some(r) if matches!(vals[0], Bson::Int32(_)) && matches!(vals[1], Bson::Int32(_)) => {
                            i32::try_from(r).map(Bson::Int32).unwrap_or(Bson::Int64(r))
                        }
                        Some(r) => Bson::Int64(r),
                        None => Bson::Double(b.powf(e)),
                    }
                }
                _ => Bson::Double(b.powf(e)),
            }
        }
        "$log" => {
            arity(op, &vals, 2)?;
            if is_nullish(&vals[0]) || is_nullish(&vals[1]) {
                return Ok(Bson::Null);
            }
            let (x, base) = (need_num(op, &vals[0])?, need_num(op, &vals[1])?);
            if x <= 0.0 || base <= 0.0 || base == 1.0 {
                return Err(Error::bad_value("$log's arguments must be positive and the base must not be 1"));
            }
            Bson::Double(x.log(base))
        }
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" | "$cmp" => {
            arity(op, &vals, 2)?;
            let ord = compare(&vals[0], &vals[1]);
            match op {
                "$eq" => Bson::Boolean(ord == Ordering::Equal),
                "$ne" => Bson::Boolean(ord != Ordering::Equal),
                "$gt" => Bson::Boolean(ord == Ordering::Greater),
                "$gte" => Bson::Boolean(ord != Ordering::Less),
                "$lt" => Bson::Boolean(ord == Ordering::Less),
                "$lte" => Bson::Boolean(ord != Ordering::Greater),
                _ => Bson::Int32(match ord {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                }),
            }
        }
        "$not" => {
            arity(op, &vals, 1)?;
            Bson::Boolean(!truthy(&vals[0]))
        }
        "$concat" => {
            let mut s = String::new();
            for v in &vals {
                match v {
                    Bson::String(x) => s.push_str(x),
                    v if is_nullish(v) => return Ok(Bson::Null),
                    v => return Err(Error::type_mismatch(format!("$concat only supports strings, not {}", type_name(v)))),
                }
            }
            Bson::String(s)
        }
        "$toLower" | "$toUpper" => {
            arity(op, &vals, 1)?;
            let s = match &vals[0] {
                v if is_nullish(v) => String::new(),
                Bson::String(s) => s.clone(),
                v => match to_string_value(v)? {
                    Bson::String(s) => s,
                    _ => String::new(),
                },
            };
            Bson::String(if op == "$toLower" { s.to_lowercase() } else { s.to_uppercase() })
        }
        "$substr" | "$substrBytes" | "$substrCP" => {
            arity(op, &vals, 3)?;
            let s = match &vals[0] {
                v if is_nullish(v) => String::new(),
                Bson::String(s) => s.clone(),
                v => match to_string_value(v)? {
                    Bson::String(s) => s,
                    _ => String::new(),
                },
            };
            let start = as_i64(&vals[1]).ok_or_else(|| Error::type_mismatch(format!("{op}: starting index must be a numeric type")))?;
            let len = as_i64(&vals[2]).ok_or_else(|| Error::type_mismatch(format!("{op}: length must be a numeric type")))?;
            if op == "$substrCP" {
                if start < 0 || len < 0 {
                    return Err(Error::bad_value("$substrCP: indexes must be non-negative"));
                }
                Bson::String(s.chars().skip(start as usize).take(len as usize).collect())
            } else {
                let bytes = s.as_bytes();
                if start < 0 {
                    return Err(Error::bad_value(format!("{op}: starting index must be non-negative")));
                }
                let st = (start as usize).min(bytes.len());
                let en = if len < 0 { bytes.len() } else { (st + len as usize).min(bytes.len()) };
                if !s.is_char_boundary(st) || !s.is_char_boundary(en) {
                    return Err(Error::bad_value(format!("{op}: Invalid range, index is in the middle of a UTF-8 character")));
                }
                Bson::String(s[st..en].to_string())
            }
        }
        "$strLenBytes" | "$strLenCP" => {
            arity(op, &vals, 1)?;
            match &vals[0] {
                Bson::String(s) => Bson::Int32(if op == "$strLenCP" { s.chars().count() } else { s.len() } as i32),
                v => return Err(Error::type_mismatch(format!("{op} requires a string argument, found: {}", type_name(v)))),
            }
        }
        "$split" => {
            arity(op, &vals, 2)?;
            match (&vals[0], &vals[1]) {
                (a, _) if is_nullish(a) => Bson::Null,
                (Bson::String(s), Bson::String(d)) => {
                    if d.is_empty() {
                        return Err(Error::bad_value("$split requires a non-empty separator"));
                    }
                    Bson::Array(s.split(d.as_str()).map(|p| Bson::String(p.into())).collect())
                }
                _ => return Err(Error::type_mismatch("$split requires string arguments")),
            }
        }
        "$indexOfBytes" | "$indexOfCP" => {
            if vals.len() < 2 || vals.len() > 4 {
                return Err(Error::failed_to_parse(format!("{op} requires 2 to 4 arguments")));
            }
            let (s, sub) = match (&vals[0], &vals[1]) {
                (a, _) if is_nullish(a) => return Ok(Bson::Null),
                (Bson::String(s), Bson::String(sub)) => (s.clone(), sub.clone()),
                _ => return Err(Error::type_mismatch(format!("{op} requires string arguments"))),
            };
            let units: Vec<String> = if op == "$indexOfCP" {
                s.chars().map(String::from).collect()
            } else {
                s.bytes().map(|b| (b as char).to_string()).collect()
            };
            let start = vals.get(2).map(|v| as_i64(v).unwrap_or(0).max(0) as usize).unwrap_or(0);
            let end = vals.get(3).map(|v| as_i64(v).unwrap_or(0).max(0) as usize).unwrap_or(units.len()).min(units.len());
            let found = if op == "$indexOfCP" {
                let hay: Vec<char> = s.chars().collect();
                let needle: Vec<char> = sub.chars().collect();
                (start..=end.saturating_sub(needle.len()))
                    .take_while(|_| start <= end)
                    .find(|&i| i + needle.len() <= end && hay[i..i + needle.len()] == needle[..])
            } else {
                let hay = s.as_bytes();
                let needle = sub.as_bytes();
                (start..=end.saturating_sub(needle.len()))
                    .take_while(|_| start <= end)
                    .find(|&i| i + needle.len() <= end && &hay[i..i + needle.len()] == needle)
            };
            Bson::Int32(found.map(|i| i as i32).unwrap_or(-1))
        }
        "$strcasecmp" => {
            arity(op, &vals, 2)?;
            let a = to_string_value(&vals[0])?;
            let b = to_string_value(&vals[1])?;
            let (a, b) = (
                if let Bson::String(s) = a { s.to_lowercase() } else { String::new() },
                if let Bson::String(s) = b { s.to_lowercase() } else { String::new() },
            );
            Bson::Int32(match a.cmp(&b) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            })
        }
        "$size" => {
            arity(op, &vals, 1)?;
            match &vals[0] {
                Bson::Array(a) => Bson::Int32(a.len() as i32),
                v => {
                    return Err(Error::type_mismatch(format!(
                        "The argument to $size must be an array. Type of argument: {}",
                        type_name(v)
                    )));
                }
            }
        }
        "$arrayElemAt" => {
            arity(op, &vals, 2)?;
            match (&vals[0], &vals[1]) {
                (a, i) if is_nullish(a) || is_nullish(i) => Bson::Null,
                (Bson::Array(a), i) => {
                    let i = as_i64(i).ok_or_else(|| Error::bad_value("$arrayElemAt's second argument must be an integer"))?;
                    let idx = if i < 0 { a.len() as i64 + i } else { i };
                    if idx < 0 || idx as usize >= a.len() {
                        return Ok(Bson::Null); // missing
                    }
                    a[idx as usize].clone()
                }
                _ => return Err(Error::type_mismatch("$arrayElemAt's first argument must be an array")),
            }
        }
        "$first" | "$last" => {
            arity(op, &vals, 1)?;
            match &vals[0] {
                v if is_nullish(v) => Bson::Null,
                Bson::Array(a) => (if op == "$first" { a.first() } else { a.last() }).cloned().unwrap_or(Bson::Null),
                v => return Err(Error::type_mismatch(format!("{op}'s argument must be an array, but is {}", type_name(v)))),
            }
        }
        "$concatArrays" => {
            let mut out = Vec::new();
            for v in vals {
                match v {
                    Bson::Array(a) => out.extend(a),
                    v if is_nullish(&v) => return Ok(Bson::Null),
                    v => return Err(Error::type_mismatch(format!("$concatArrays only supports arrays, not {}", type_name(&v)))),
                }
            }
            Bson::Array(out)
        }
        "$in" => {
            arity(op, &vals, 2)?;
            match &vals[1] {
                Bson::Array(a) => Bson::Boolean(a.iter().any(|e| compare(e, &vals[0]) == Ordering::Equal)),
                v => return Err(Error::type_mismatch(format!("$in requires an array as a second argument, found: {}", type_name(v)))),
            }
        }
        "$indexOfArray" => {
            if vals.len() < 2 || vals.len() > 4 {
                return Err(Error::failed_to_parse("$indexOfArray requires 2 to 4 arguments"));
            }
            match &vals[0] {
                v if is_nullish(v) => Bson::Null,
                Bson::Array(a) => {
                    let start = vals.get(2).and_then(as_i64).unwrap_or(0).max(0) as usize;
                    let end = vals.get(3).and_then(as_i64).map(|e| e.max(0) as usize).unwrap_or(a.len()).min(a.len());
                    let found = (start..end).find(|&i| compare(&a[i], &vals[1]) == Ordering::Equal);
                    Bson::Int32(found.map(|i| i as i32).unwrap_or(-1))
                }
                _ => return Err(Error::type_mismatch("$indexOfArray requires an array")),
            }
        }
        "$isArray" => {
            arity(op, &vals, 1)?;
            Bson::Boolean(matches!(vals[0], Bson::Array(_)))
        }
        "$isNumber" => {
            arity(op, &vals, 1)?;
            Bson::Boolean(is_number(&vals[0]))
        }
        "$reverseArray" => {
            arity(op, &vals, 1)?;
            match vals.pop().unwrap() {
                Bson::Array(mut a) => {
                    a.reverse();
                    Bson::Array(a)
                }
                v if is_nullish(&v) => Bson::Null,
                v => return Err(Error::type_mismatch(format!("$reverseArray requires an array, not {}", type_name(&v)))),
            }
        }
        "$slice" => {
            if vals.len() != 2 && vals.len() != 3 {
                return Err(Error::failed_to_parse("$slice takes 2 or 3 arguments"));
            }
            let a = match &vals[0] {
                v if is_nullish(v) => return Ok(Bson::Null),
                Bson::Array(a) => a.clone(),
                v => return Err(Error::type_mismatch(format!("First argument to $slice must be an array, not {}", type_name(v)))),
            };
            let n1 = as_i64(&vals[1]).ok_or_else(|| Error::type_mismatch("$slice arguments must be integers"))?;
            Bson::Array(if vals.len() == 2 {
                if n1 >= 0 {
                    a.into_iter().take(n1 as usize).collect()
                } else {
                    let skip = a.len().saturating_sub(n1.unsigned_abs() as usize);
                    a.into_iter().skip(skip).collect()
                }
            } else {
                let n = as_i64(&vals[2]).ok_or_else(|| Error::type_mismatch("$slice arguments must be integers"))?;
                if n <= 0 {
                    return Err(Error::bad_value("Third argument to $slice must be positive"));
                }
                let start = if n1 < 0 { a.len().saturating_sub(n1.unsigned_abs() as usize) } else { (n1 as usize).min(a.len()) };
                a.into_iter().skip(start).take(n as usize).collect()
            })
        }
        "$range" => {
            if vals.len() != 2 && vals.len() != 3 {
                return Err(Error::failed_to_parse("$range requires 2 or 3 arguments"));
            }
            let s = as_i64(&vals[0]).ok_or_else(|| Error::bad_value("$range requires a numeric starting value"))?;
            let e = as_i64(&vals[1]).ok_or_else(|| Error::bad_value("$range requires a numeric ending value"))?;
            let step = match vals.get(2) {
                Some(v) => as_i64(v).ok_or_else(|| Error::bad_value("$range requires a numeric step value"))?,
                None => 1,
            };
            if step == 0 {
                return Err(Error::bad_value("$range requires a non-zero step value"));
            }
            let mut out = Vec::new();
            let mut i = s;
            while (step > 0 && i < e) || (step < 0 && i > e) {
                out.push(Bson::Int32(i as i32));
                if out.len() > 10_000_000 {
                    return Err(Error::bad_value("$range would produce too many elements"));
                }
                i += step;
            }
            Bson::Array(out)
        }
        "$setUnion" | "$setIntersection" | "$setDifference" | "$setEquals" | "$setIsSubset" => {
            let mut sets = Vec::new();
            for v in &vals {
                match v {
                    Bson::Array(a) => sets.push(dedup(a)),
                    v if is_nullish(v) => return Ok(Bson::Null),
                    v => return Err(Error::type_mismatch(format!("All operands of {op} must be arrays, found {}", type_name(v)))),
                }
            }
            let contains = |s: &Vec<Bson>, x: &Bson| s.iter().any(|y| compare(x, y) == Ordering::Equal);
            match op {
                "$setUnion" => Bson::Array(dedup(&sets.concat())),
                "$setIntersection" => {
                    let mut it = sets.into_iter();
                    let first = it.next().unwrap_or_default();
                    let rest: Vec<_> = it.collect();
                    Bson::Array(first.into_iter().filter(|x| rest.iter().all(|s| contains(s, x))).collect())
                }
                "$setDifference" => {
                    if sets.len() != 2 {
                        return Err(Error::failed_to_parse("$setDifference takes exactly 2 arguments"));
                    }
                    Bson::Array(sets[0].iter().filter(|x| !contains(&sets[1], x)).cloned().collect())
                }
                "$setEquals" => {
                    if sets.len() < 2 {
                        return Err(Error::failed_to_parse("$setEquals needs at least two arguments"));
                    }
                    let first = &sets[0];
                    Bson::Boolean(sets.iter().all(|s| s.len() == first.len() && s.iter().all(|x| contains(first, x))))
                }
                _ => {
                    if sets.len() != 2 {
                        return Err(Error::failed_to_parse("$setIsSubset takes exactly 2 arguments"));
                    }
                    Bson::Boolean(sets[0].iter().all(|x| contains(&sets[1], x)))
                }
            }
        }
        "$allElementsTrue" | "$anyElementTrue" => {
            arity(op, &vals, 1)?;
            match &vals[0] {
                Bson::Array(a) => Bson::Boolean(if op == "$allElementsTrue" { a.iter().all(truthy) } else { a.iter().any(truthy) }),
                v => return Err(Error::type_mismatch(format!("{op}'s argument must be an array, but is {}", type_name(v)))),
            }
        }
        "$mergeObjects" => {
            let mut out = Document::new();
            for v in vals {
                match v {
                    Bson::Document(d) => {
                        for (k, v) in d {
                            out.insert(k, v);
                        }
                    }
                    v if is_nullish(&v) => {}
                    v => {
                        return Err(Error::type_mismatch(format!(
                            "$mergeObjects requires object inputs, but input is of type {}",
                            type_name(&v)
                        )));
                    }
                }
            }
            Bson::Document(out)
        }
        "$objectToArray" => {
            arity(op, &vals, 1)?;
            match &vals[0] {
                Bson::Document(d) => Bson::Array(d.iter().map(|(k, v)| Bson::Document(bson::doc! {"k": k, "v": v.clone()})).collect()),
                v if is_nullish(v) => Bson::Null,
                v => return Err(Error::type_mismatch(format!("$objectToArray requires a document input, found: {}", type_name(v)))),
            }
        }
        "$arrayToObject" => {
            arity(op, &vals, 1)?;
            match &vals[0] {
                v if is_nullish(v) => Bson::Null,
                Bson::Array(a) => {
                    let mut out = Document::new();
                    for e in a {
                        match e {
                            Bson::Document(d) if d.len() == 2 && d.contains_key("k") && d.contains_key("v") => {
                                let Ok(k) = d.get_str("k") else {
                                    return Err(Error::type_mismatch("$arrayToObject requires 'k' to be a string"));
                                };
                                out.insert(k, d.get("v").unwrap().clone());
                            }
                            Bson::Array(pair) if pair.len() == 2 => {
                                let Bson::String(k) = &pair[0] else {
                                    return Err(Error::type_mismatch("$arrayToObject requires keys to be strings"));
                                };
                                out.insert(k.clone(), pair[1].clone());
                            }
                            _ => return Err(Error::bad_value("$arrayToObject requires an array of {k, v} documents or [k, v] pairs")),
                        }
                    }
                    Bson::Document(out)
                }
                v => return Err(Error::type_mismatch(format!("$arrayToObject requires an array input, found: {}", type_name(v)))),
            }
        }
        "$zip" => return Err(Error::not_implemented("$zip is not supported by Mango")),
        "$toString" => {
            arity(op, &vals, 1)?;
            convert(&vals[0], "string")?
        }
        "$toInt" | "$toLong" | "$toDouble" | "$toBool" | "$toObjectId" | "$toDate" => {
            arity(op, &vals, 1)?;
            let to = match op {
                "$toInt" => "int",
                "$toLong" => "long",
                "$toDouble" => "double",
                "$toBool" => "bool",
                "$toObjectId" => "objectId",
                _ => "date",
            };
            convert(&vals[0], to)?
        }
        "$year" | "$month" | "$dayOfMonth" | "$hour" | "$minute" | "$second" | "$millisecond" | "$dayOfWeek" | "$dayOfYear"
        | "$isoDayOfWeek" => {
            arity(op, &vals, 1)?;
            if is_nullish(&vals[0]) {
                return Ok(Bson::Null);
            }
            let p = date_parts(to_date_millis(op, &vals[0])?);
            Bson::Int32(match op {
                "$year" => p.year as i32,
                "$month" => p.month as i32,
                "$dayOfMonth" => p.day as i32,
                "$hour" => p.hour as i32,
                "$minute" => p.minute as i32,
                "$second" => p.second as i32,
                "$millisecond" => p.millis as i32,
                "$dayOfWeek" => p.day_of_week as i32,
                "$isoDayOfWeek" => {
                    if p.day_of_week == 1 {
                        7
                    } else {
                        p.day_of_week as i32 - 1
                    }
                }
                _ => p.day_of_year as i32,
            })
        }
        "$sum" | "$avg" | "$min" | "$max" => {
            let items: Vec<Bson> = if vals.len() == 1 {
                match vals.pop().unwrap() {
                    Bson::Array(a) => a,
                    v => vec![v],
                }
            } else {
                vals
            };
            match op {
                "$sum" => {
                    let mut acc = Bson::Int32(0);
                    for v in &items {
                        if is_number(v) {
                            acc = num_add(&acc, v).unwrap();
                        }
                    }
                    acc
                }
                "$avg" => {
                    let nums: Vec<f64> = items.iter().filter_map(|v| if is_number(v) { as_f64(v) } else { None }).collect();
                    if nums.is_empty() { Bson::Null } else { Bson::Double(nums.iter().sum::<f64>() / nums.len() as f64) }
                }
                _ => {
                    let mut best: Option<Bson> = None;
                    for v in items.into_iter().filter(|v| !is_nullish(v)) {
                        best = Some(match best {
                            None => v,
                            Some(b) => {
                                let ord = compare(&v, &b);
                                if (op == "$min" && ord == Ordering::Less) || (op == "$max" && ord == Ordering::Greater) { v } else { b }
                            }
                        });
                    }
                    best.unwrap_or(Bson::Null)
                }
            }
        }
        "$rand" => Bson::Double(rand::random::<f64>()),
        "$toHashedIndexKey" => return Err(Error::not_implemented("$toHashedIndexKey is not supported by Mango")),
        _ => return Err(Error::invalid_options(format!("Unrecognized expression '{op}'"))),
    })
}

fn dedup(a: &[Bson]) -> Vec<Bson> {
    let mut out: Vec<Bson> = Vec::new();
    for x in a {
        if !out.iter().any(|y| compare(x, y) == Ordering::Equal) {
            out.push(x.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn ev(e: Bson, d: Document) -> Bson {
        Expr::parse(&e).unwrap().eval(&Vars::root(&d)).unwrap()
    }

    #[test]
    fn arithmetic_promotion() {
        assert_eq!(ev(Bson::Document(doc! {"$add": [1, 2]}), doc! {}), Bson::Int32(3));
        assert_eq!(ev(Bson::Document(doc! {"$add": [i32::MAX, 1]}), doc! {}), Bson::Int64(i32::MAX as i64 + 1));
        assert_eq!(ev(Bson::Document(doc! {"$add": [1, 2.5]}), doc! {}), Bson::Double(3.5));
        assert_eq!(ev(Bson::Document(doc! {"$multiply": ["$a", 3]}), doc! {"a": 2i64}), Bson::Int64(6));
        assert_eq!(ev(Bson::Document(doc! {"$subtract": [5, "$missing"]}), doc! {}), Bson::Null);
        assert_eq!(ev(Bson::Document(doc! {"$round": [2.5]}), doc! {}), Bson::Double(2.0));
        assert_eq!(ev(Bson::Document(doc! {"$round": [3.5]}), doc! {}), Bson::Double(4.0));
    }

    #[test]
    fn field_paths_over_arrays() {
        assert_eq!(
            ev(Bson::String("$a.b".into()), doc! {"a": [{"b": 1}, {"c": 2}, {"b": 3}]}),
            Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)])
        );
    }

    #[test]
    fn higher_order() {
        let e = Bson::Document(doc! {"$map": {"input": "$xs", "as": "x", "in": {"$multiply": ["$$x", 2]}}});
        assert_eq!(ev(e, doc! {"xs": [1, 2]}), Bson::Array(vec![Bson::Int32(2), Bson::Int32(4)]));
        let e = Bson::Document(doc! {"$reduce": {"input": [1, 2, 3], "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}});
        assert_eq!(ev(e, doc! {}), Bson::Int32(6));
        let e = Bson::Document(doc! {"$filter": {"input": [1, 2, 3], "cond": {"$gte": ["$$this", 2]}}});
        assert_eq!(ev(e, doc! {}), Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)]));
    }

    #[test]
    fn dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(2000, 3, 1), 11017);
        for d in [-800_000i64, -1, 0, 59, 11016, 20000, 1_000_000] {
            let (y, m, dd) = civil_from_days(d);
            assert_eq!(days_from_civil(y, m, dd), d);
        }
        let ms = DateTime::parse_rfc3339_str("2024-02-29T13:45:06.789Z").unwrap();
        let d = doc! {"d": ms};
        assert_eq!(ev(Bson::Document(doc! {"$year": "$d"}), d.clone()), Bson::Int32(2024));
        assert_eq!(ev(Bson::Document(doc! {"$dayOfWeek": "$d"}), d.clone()), Bson::Int32(5)); // Thursday
        assert_eq!(ev(Bson::Document(doc! {"$dayOfYear": "$d"}), d.clone()), Bson::Int32(60));
        assert_eq!(
            ev(Bson::Document(doc! {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S.%L", "date": "$d"}}), d),
            Bson::String("2024-02-29 13:45:06.789".into())
        );
    }

    #[test]
    fn cond_and_remove() {
        let e = Expr::parse(&Bson::Document(doc! {"$cond": [{"$gt": ["$a", 1]}, "$$REMOVE", "$a"]})).unwrap();
        let d = doc! {"a": 5};
        assert_eq!(e.eval_opt(&Vars::root(&d)).unwrap(), None);
    }
}
