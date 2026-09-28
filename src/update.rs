//! Update documents: operator updates (`$set`, `$inc`, `$push`, ...),
//! replacements and pipeline updates, including positional paths
//! (`a.$.b`, `a.$[].b`, `a.$[x].b` with `arrayFilters`) and upserts.

use crate::aggregate::Stage;
use crate::bsonutil::{as_i64, is_number, parse_index, type_name};
use crate::error::{Error, Result};
use crate::expr::num_add;
use crate::keystring::{compare, values_equal};
use crate::query::{Matcher, parse_field_value};
use bson::{Bson, DateTime, Document, Timestamp, doc, oid::ObjectId};
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Update {
    Replacement(Document),
    Operators(Vec<Op>),
    Pipeline(Vec<Stage>),
}

#[derive(Debug, Clone)]
pub struct Op {
    kind: OpKind,
    path: String,
    arg: Bson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Set,
    SetOnInsert,
    Unset,
    Inc,
    Mul,
    Min,
    Max,
    CurrentDate,
    Rename,
    Push,
    AddToSet,
    Pop,
    Pull,
    PullAll,
    Bit,
}

/// Everything an update needs besides the document itself. All values are
/// fixed when the write is proposed so every replica computes the same result.
pub struct UpdateCtx<'a> {
    pub is_insert: bool,
    pub now_millis: i64,
    pub query: Option<&'a Matcher>,
    pub array_filters: &'a [(String, Matcher)],
}

pub fn parse_array_filters(filters: Option<&Bson>) -> Result<Vec<(String, Matcher)>> {
    let Some(filters) = filters else { return Ok(vec![]) };
    let Bson::Array(arr) = filters else {
        return Err(Error::type_mismatch("arrayFilters must be an array"));
    };
    let mut out: Vec<(String, Matcher)> = Vec::new();
    for f in arr {
        let Bson::Document(d) = f else {
            return Err(Error::type_mismatch("arrayFilters entries must be objects"));
        };
        let mut ident: Option<String> = None;
        for k in d.keys() {
            let id = k.split('.').next().unwrap_or("").to_string();
            if id.starts_with('$') {
                // e.g. {$or: [...]}; identifiers come from the sub-clauses
                continue;
            }
            if !id.chars().next().is_some_and(|c| c.is_ascii_lowercase()) || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
                return Err(Error::bad_value(format!(
                    "The top-level field name must be an alphanumeric string beginning with a lowercase letter, found '{id}'"
                )));
            }
            match &ident {
                None => ident = Some(id),
                Some(prev) if *prev != id => {
                    return Err(Error::failed_to_parse(format!(
                        "Error parsing array filter: Expected a single top-level field name, found '{prev}' and '{id}'"
                    )));
                }
                _ => {}
            }
        }
        let Some(ident) = ident else {
            return Err(Error::failed_to_parse("Cannot use an expression without a top-level field name in arrayFilters"));
        };
        if out.iter().any(|(i, _)| *i == ident) {
            return Err(Error::failed_to_parse(format!("Found multiple array filters with the same top-level field name {ident}")));
        }
        out.push((ident, Matcher::parse(d)?));
    }
    Ok(out)
}

impl Update {
    pub fn parse(u: &Bson) -> Result<Update> {
        match u {
            Bson::Array(stages) => {
                let mut out = Vec::new();
                for s in stages {
                    let Bson::Document(sd) = s else {
                        return Err(Error::failed_to_parse("each element of the update pipeline must be an object"));
                    };
                    let stage = Stage::parse(sd)?;
                    if !stage.allowed_in_update() {
                        return Err(Error::invalid_options(format!(
                            "{} is not allowed to be used within an update",
                            sd.keys().next().map(String::as_str).unwrap_or("")
                        )));
                    }
                    out.push(stage);
                }
                Ok(Update::Pipeline(out))
            }
            Bson::Document(d) => {
                if d.keys().next().is_some_and(|k| k.starts_with('$')) {
                    Update::parse_operators(d)
                } else {
                    for k in d.keys() {
                        if k.starts_with('$') {
                            return Err(Error::dollar_prefixed_field(format!(
                                "The dollar ($) prefixed field '{k}' in '{k}' is not allowed in the context of an update's replacement document."
                            )));
                        }
                    }
                    Ok(Update::Replacement(d.clone()))
                }
            }
            _ => Err(Error::failed_to_parse("Update argument must be either an object or an array")),
        }
    }

    fn parse_operators(d: &Document) -> Result<Update> {
        let mut ops = Vec::new();
        for (name, arg) in d {
            let kind = match name.as_str() {
                "$set" => OpKind::Set,
                "$setOnInsert" => OpKind::SetOnInsert,
                "$unset" => OpKind::Unset,
                "$inc" => OpKind::Inc,
                "$mul" => OpKind::Mul,
                "$min" => OpKind::Min,
                "$max" => OpKind::Max,
                "$currentDate" => OpKind::CurrentDate,
                "$rename" => OpKind::Rename,
                "$push" => OpKind::Push,
                "$addToSet" => OpKind::AddToSet,
                "$pop" => OpKind::Pop,
                "$pull" => OpKind::Pull,
                "$pullAll" => OpKind::PullAll,
                "$bit" => OpKind::Bit,
                other if other.starts_with('$') => {
                    return Err(Error::failed_to_parse(format!("Unknown modifier: {other}. Expected a valid update modifier or pipeline-style update specified as an array")));
                }
                other => {
                    return Err(Error::failed_to_parse(format!(
                        "Unknown modifier: {other}. Expected a valid update modifier or pipeline-style update specified as an array"
                    )));
                }
            };
            let Bson::Document(fields) = arg else {
                return Err(Error::failed_to_parse(format!(
                    "Modifiers operate on fields but we found type {} instead. For example: {{$mod: {{<field>: ...}}}} not {{{name}: {arg}}}",
                    type_name(arg)
                )));
            };
            for (path, v) in fields {
                validate_update_path(path)?;
                match kind {
                    OpKind::Inc | OpKind::Mul if !is_number(v) => {
                        return Err(Error::type_mismatch(format!(
                            "Cannot {} with non-numeric argument: {{{path}: {v}}}",
                            if kind == OpKind::Inc { "increment" } else { "multiply" }
                        )));
                    }
                    OpKind::Rename => {
                        let Bson::String(to) = v else {
                            return Err(Error::bad_value(format!("The 'to' field for $rename must be a string: {path}: {v}")));
                        };
                        validate_update_path(to)?;
                        if path.contains('$') || to.contains('$') {
                            return Err(Error::bad_value("The source and target field for $rename must not contain positional operators"));
                        }
                        if to == path {
                            return Err(Error::bad_value(format!("The source and target field for $rename must differ: {path}: {v}")));
                        }
                        if is_prefix_path(path, to) || is_prefix_path(to, path) {
                            return Err(Error::bad_value(format!(
                                "The source and target field for $rename must not be on the same path: {path}: {v}"
                            )));
                        }
                    }
                    OpKind::Pop => {
                        if !is_number(v) {
                            return Err(Error::failed_to_parse(format!("Expected a number in: {path}: {v}")));
                        }
                        let n = crate::bsonutil::as_f64(v).unwrap();
                        if n != 1.0 && n != -1.0 {
                            return Err(Error::failed_to_parse(format!("$pop expects 1 or -1, found: {v}")));
                        }
                    }
                    OpKind::PullAll if !matches!(v, Bson::Array(_)) => {
                        return Err(Error::bad_value(format!("$pullAll requires an array argument but was given a {}", type_name(v))));
                    }
                    OpKind::CurrentDate => match v {
                        Bson::Boolean(_) => {}
                        Bson::Document(t) if matches!(t.get_str("$type"), Ok("date") | Ok("timestamp")) && t.len() == 1 => {}
                        _ => {
                            return Err(Error::bad_value(
                                "The '$type' string field is required to be 'date' or 'timestamp': {$currentDate: {field : {$type: 'date'}}}",
                            ));
                        }
                    },
                    OpKind::Bit => {
                        let Bson::Document(b) = v else {
                            return Err(Error::bad_value(format!("The $bit modifier is not compatible with a {}. You must pass in an embedded document", type_name(v))));
                        };
                        for (bop, bv) in b {
                            if !matches!(bop.as_str(), "and" | "or" | "xor") {
                                return Err(Error::bad_value(format!("The $bit modifier only supports 'and', 'or', and 'xor', not '{bop}'")));
                            }
                            if !matches!(bv, Bson::Int32(_) | Bson::Int64(_)) {
                                return Err(Error::bad_value("The $bit modifier field must be an Integer(32/64 bit)"));
                            }
                        }
                    }
                    OpKind::Push | OpKind::AddToSet => validate_each(kind, v)?,
                    _ => {}
                }
                ops.push(Op { kind, path: path.clone(), arg: v.clone() });
            }
        }
        if ops.is_empty() {
            return Err(Error::failed_to_parse("'update' is empty. You must specify a field like so: {$set: {<field>: ...}}"));
        }
        // No two updated paths may overlap.
        let mut all_paths: Vec<&str> = Vec::new();
        for op in &ops {
            all_paths.push(&op.path);
            if op.kind == OpKind::Rename
                && let Bson::String(to) = &op.arg
            {
                all_paths.push(to);
            }
        }
        for (i, a) in all_paths.iter().enumerate() {
            for b in &all_paths[i + 1..] {
                if a == b || is_prefix_path(a, b) || is_prefix_path(b, a) {
                    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
                    return Err(Error::conflicting_update(format!("Updating the path '{long}' would create a conflict at '{short}'")));
                }
            }
        }
        Ok(Update::Operators(ops))
    }

    pub fn is_replacement(&self) -> bool {
        matches!(self, Update::Replacement(_))
    }

    /// Applies the update to `doc`, returning the new document.
    pub fn apply(&self, doc: &Document, ctx: &UpdateCtx) -> Result<Document> {
        let mut out = match self {
            Update::Replacement(r) => {
                let mut new = Document::new();
                match (doc.get("_id"), r.get("_id")) {
                    (Some(old), Some(new_id)) if old != new_id => {
                        return Err(immutable_id_error(doc, "_id"));
                    }
                    (Some(old), _) => {
                        new.insert("_id", old.clone());
                    }
                    (None, Some(new_id)) => {
                        new.insert("_id", new_id.clone());
                    }
                    (None, None) => {}
                }
                for (k, v) in r {
                    if k != "_id" {
                        new.insert(k.clone(), v.clone());
                    }
                }
                new
            }
            Update::Operators(ops) => {
                let mut new = doc.clone();
                for op in ops {
                    op.apply(&mut new, doc, ctx)?;
                }
                new
            }
            Update::Pipeline(stages) => {
                let mut cur = doc.clone();
                for st in stages {
                    cur = st.apply_single(cur)?;
                }
                cur
            }
        };
        match (doc.get("_id"), out.get("_id")) {
            (Some(a), Some(b)) if a != b => return Err(immutable_id_error(doc, "_id")),
            (Some(a), None) => {
                // Pipeline $unset/$project may drop _id; MongoDB keeps it.
                let mut with_id = doc! {"_id": a.clone()};
                with_id.extend(out);
                out = with_id;
            }
            _ => {}
        }
        Ok(out)
    }
}

fn immutable_id_error(doc: &Document, field: &str) -> Error {
    let id = doc.get("_id").map(|v| v.to_string()).unwrap_or_default();
    Error::immutable_field(format!(
        "Performing an update on the path '{field}' would modify the immutable field '{field}' (document _id: {id})"
    ))
}

fn validate_each(kind: OpKind, v: &Bson) -> Result<()> {
    let Bson::Document(d) = v else { return Ok(()) };
    if !d.contains_key("$each") {
        return Ok(());
    }
    if !matches!(d.get("$each"), Some(Bson::Array(_))) {
        return Err(Error::bad_value("The argument to $each must be an array"));
    }
    for (k, val) in d {
        match k.as_str() {
            "$each" => {}
            "$position" | "$slice" if kind == OpKind::Push => {
                if as_i64(val).is_none() {
                    return Err(Error::bad_value(format!("The value for {k} must be an integer value")));
                }
            }
            "$sort" if kind == OpKind::Push => match val {
                Bson::Document(spec) => {
                    if spec.is_empty() {
                        return Err(Error::bad_value("The $sort pattern is empty when it should be a set of fields."));
                    }
                    for (_, dir) in spec {
                        if !matches!(as_i64(dir), Some(1) | Some(-1)) {
                            return Err(Error::bad_value("The $sort element value must be either 1 or -1"));
                        }
                    }
                }
                v if matches!(as_i64(v), Some(1) | Some(-1)) => {}
                _ => return Err(Error::bad_value("The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields")),
            },
            other => {
                return Err(Error::bad_value(format!(
                    "Unrecognized clause in {}: {other}",
                    if kind == OpKind::Push { "$push" } else { "$addToSet" }
                )));
            }
        }
    }
    Ok(())
}

fn validate_update_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(Error::empty_field_name("An empty update path is not valid."));
    }
    for part in path.split('.') {
        if part.is_empty() {
            return Err(Error::empty_field_name(format!("The update path '{path}' contains an empty field name, which is not allowed.")));
        }
        if part.starts_with('$') && !(part == "$" || part == "$[]" || (part.starts_with("$[") && part.ends_with(']'))) {
            return Err(Error::dollar_prefixed_field(format!(
                "The dollar ($) prefixed field '{part}' in '{path}' is not valid for storage."
            )));
        }
    }
    Ok(())
}

/// Whether `a` is a strict path prefix of `b` ("a.b" of "a.b.c").
pub fn is_prefix_path(a: &str, b: &str) -> bool {
    b.len() > a.len() && b.starts_with(a) && b.as_bytes()[a.len()] == b'.'
}

// ---------- concrete path manipulation ----------

pub fn get_at<'a>(doc: &'a Document, parts: &[String]) -> Option<&'a Bson> {
    let mut cur = doc.get(&parts[0])?;
    for p in &parts[1..] {
        cur = match cur {
            Bson::Document(d) => d.get(p)?,
            Bson::Array(a) => a.get(parse_index(p)?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn describe_container(field: &str, v: &Bson) -> String {
    format!("{{{field}: {v}}}")
}

/// Sets the value at a concrete path, creating intermediate documents.
pub fn set_at(doc: &mut Document, parts: &[String], value: Bson) -> Result<()> {
    if parts.len() == 1 {
        doc.insert(parts[0].clone(), value);
        return Ok(());
    }
    let head = &parts[0];
    if !doc.contains_key(head) {
        doc.insert(head.clone(), Bson::Document(Document::new()));
    }
    let child = doc.get_mut(head).unwrap();
    set_in_value(child, head, &parts[1..], value)
}

fn set_in_value(container: &mut Bson, container_name: &str, parts: &[String], value: Bson) -> Result<()> {
    match container {
        Bson::Document(d) => set_at(d, parts, value),
        Bson::Array(a) => {
            let Some(idx) = parse_index(&parts[0]) else {
                return Err(Error::path_not_viable(format!(
                    "Cannot create field '{}' in element {}",
                    parts[0],
                    describe_container(container_name, &Bson::Array(a.clone()))
                )));
            };
            if idx > 1_500_000 {
                return Err(Error::bad_value("can't set an array element past 1500000"));
            }
            while a.len() <= idx {
                a.push(Bson::Null);
            }
            if parts.len() == 1 {
                a[idx] = value;
                Ok(())
            } else {
                if a[idx] == Bson::Null {
                    a[idx] = Bson::Document(Document::new());
                }
                let name = parts[0].clone();
                set_in_value(&mut a[idx], &name, &parts[1..], value)
            }
        }
        other => Err(Error::path_not_viable(format!(
            "Cannot create field '{}' in element {}",
            parts[0],
            describe_container(container_name, other)
        ))),
    }
}

/// Removes the value at a concrete path. Array elements are set to null,
/// as in MongoDB, so other positions don't shift.
pub fn unset_at(doc: &mut Document, parts: &[String]) {
    if parts.len() == 1 {
        doc.remove(&parts[0]);
        return;
    }
    let Some(child) = doc.get_mut(&parts[0]) else { return };
    unset_in_value(child, &parts[1..]);
}

fn unset_in_value(v: &mut Bson, parts: &[String]) {
    match v {
        Bson::Document(d) => unset_at(d, parts),
        Bson::Array(a) => {
            let Some(idx) = parse_index(&parts[0]) else { return };
            if idx >= a.len() {
                return;
            }
            if parts.len() == 1 {
                a[idx] = Bson::Null;
            } else {
                unset_in_value(&mut a[idx], &parts[1..]);
            }
        }
        _ => {}
    }
}

impl Op {
    fn apply(&self, doc: &mut Document, original: &Document, ctx: &UpdateCtx) -> Result<()> {
        if self.kind == OpKind::SetOnInsert && !ctx.is_insert {
            return Ok(());
        }
        // Changes to _id are rejected by `Update::apply` once all ops ran.
        for parts in expand_path(&self.path, doc, original, ctx)? {
            self.apply_one(doc, &parts, ctx)?;
        }
        Ok(())
    }

    fn apply_one(&self, doc: &mut Document, parts: &[String], ctx: &UpdateCtx) -> Result<()> {
        let path = parts.join(".");
        let cur = get_at(doc, parts).cloned();
        match self.kind {
            OpKind::Set | OpKind::SetOnInsert => set_at(doc, parts, self.arg.clone()),
            OpKind::Unset => {
                unset_at(doc, parts);
                Ok(())
            }
            OpKind::Inc | OpKind::Mul => {
                let new = match &cur {
                    None => {
                        if self.kind == OpKind::Inc {
                            self.arg.clone()
                        } else {
                            match &self.arg {
                                Bson::Int32(_) => Bson::Int32(0),
                                Bson::Int64(_) => Bson::Int64(0),
                                _ => Bson::Double(0.0),
                            }
                        }
                    }
                    Some(v) if is_number(v) => {
                        let r = if self.kind == OpKind::Inc {
                            checked_arith(v, &self.arg, true)
                        } else {
                            checked_arith(v, &self.arg, false)
                        };
                        r.ok_or_else(|| {
                            Error::bad_value(format!(
                                "Failed to apply {} operations to current value ({v}) for document {{_id: {}}}: overflow",
                                if self.kind == OpKind::Inc { "$inc" } else { "$mul" },
                                doc.get("_id").map(|v| v.to_string()).unwrap_or_default()
                            ))
                        })?
                    }
                    Some(v) => {
                        return Err(Error::type_mismatch(format!(
                            "Cannot apply {} to a value of non-numeric type. {{_id: {}}} has the field '{}' of non-numeric type {}",
                            if self.kind == OpKind::Inc { "$inc" } else { "$mul" },
                            doc.get("_id").map(|v| v.to_string()).unwrap_or_default(),
                            parts.last().unwrap(),
                            type_name(v)
                        )));
                    }
                };
                set_at(doc, parts, new)
            }
            OpKind::Min | OpKind::Max => {
                let replace = match &cur {
                    None => true,
                    Some(v) => {
                        let ord = compare(&self.arg, v);
                        (self.kind == OpKind::Min && ord == Ordering::Less) || (self.kind == OpKind::Max && ord == Ordering::Greater)
                    }
                };
                if replace { set_at(doc, parts, self.arg.clone()) } else { Ok(()) }
            }
            OpKind::CurrentDate => {
                let ts = matches!(&self.arg, Bson::Document(t) if t.get_str("$type").ok() == Some("timestamp"));
                let v = if ts {
                    Bson::Timestamp(Timestamp { time: (ctx.now_millis / 1000) as u32, increment: 1 })
                } else {
                    Bson::DateTime(DateTime::from_millis(ctx.now_millis))
                };
                set_at(doc, parts, v)
            }
            OpKind::Rename => {
                let Some(v) = cur else { return Ok(()) };
                if parts.len() > 1 && matches!(get_at(doc, &parts[..parts.len() - 1]), Some(Bson::Array(_))) {
                    return Err(Error::bad_value(format!("The source field cannot be an array element, '{path}' in doc has an array")));
                }
                let Bson::String(to) = &self.arg else { unreachable!() };
                let to_parts: Vec<String> = to.split('.').map(String::from).collect();
                for i in 1..to_parts.len() {
                    if matches!(get_at(doc, &to_parts[..i]), Some(Bson::Array(_))) {
                        return Err(Error::bad_value(format!("The destination field cannot be an array element, '{to}' in doc has an array")));
                    }
                }
                unset_at(doc, parts);
                set_at(doc, &to_parts, v)
            }
            OpKind::Push => {
                let mut arr = self.target_array(&cur, doc, &path)?;
                let (items, position, slice, sort) = match &self.arg {
                    Bson::Document(d) if d.contains_key("$each") => {
                        let Some(Bson::Array(items)) = d.get("$each") else { unreachable!() };
                        (items.clone(), d.get("$position").and_then(as_i64), d.get("$slice").and_then(as_i64), d.get("$sort").cloned())
                    }
                    v => (vec![v.clone()], None, None, None),
                };
                match position {
                    None => arr.extend(items),
                    Some(p) => {
                        let len = arr.len() as i64;
                        let at = if p < 0 { (len + p).max(0) } else { p.min(len) } as usize;
                        let tail = arr.split_off(at);
                        arr.extend(items);
                        arr.extend(tail);
                    }
                }
                if let Some(spec) = sort {
                    match spec {
                        Bson::Document(sd) => {
                            let spec = crate::aggregate::SortSpec::parse(&sd)?;
                            let empty = Document::new();
                            arr.sort_by(|a, b| {
                                let da = if let Bson::Document(d) = a { d } else { &empty };
                                let db = if let Bson::Document(d) = b { d } else { &empty };
                                spec.compare(da, db)
                            });
                        }
                        v => {
                            let desc = as_i64(&v) == Some(-1);
                            arr.sort_by(|a, b| if desc { compare(b, a) } else { compare(a, b) });
                        }
                    }
                }
                if let Some(s) = slice {
                    if s >= 0 {
                        arr.truncate(s as usize);
                    } else {
                        let keep = s.unsigned_abs() as usize;
                        if arr.len() > keep {
                            arr.drain(..arr.len() - keep);
                        }
                    }
                }
                set_at(doc, parts, Bson::Array(arr))
            }
            OpKind::AddToSet => {
                let mut arr = self.target_array(&cur, doc, &path)?;
                let items = match &self.arg {
                    Bson::Document(d) if d.contains_key("$each") => match d.get("$each") {
                        Some(Bson::Array(items)) => items.clone(),
                        _ => unreachable!(),
                    },
                    v => vec![v.clone()],
                };
                for it in items {
                    if !arr.iter().any(|e| values_equal(e, &it)) {
                        arr.push(it);
                    }
                }
                set_at(doc, parts, Bson::Array(arr))
            }
            OpKind::Pop => {
                let Some(v) = cur else { return Ok(()) };
                let Bson::Array(mut a) = v else {
                    return Err(Error::type_mismatch(format!("Path '{path}' contains an element of non-array type '{}'", type_name(&v))));
                };
                if !a.is_empty() {
                    if crate::bsonutil::as_f64(&self.arg) == Some(-1.0) {
                        a.remove(0);
                    } else {
                        a.pop();
                    }
                }
                set_at(doc, parts, Bson::Array(a))
            }
            OpKind::Pull | OpKind::PullAll => {
                let Some(v) = cur else { return Ok(()) };
                let Bson::Array(a) = v else {
                    return Err(Error::bad_value(format!("Cannot apply $pull to a non-array value")));
                };
                let kept: Vec<Bson> = if self.kind == OpKind::PullAll {
                    let Bson::Array(remove) = &self.arg else { unreachable!() };
                    a.into_iter().filter(|e| !remove.iter().any(|r| values_equal(e, r))).collect()
                } else {
                    let cond = PullCond::parse(&self.arg)?;
                    let mut kept = Vec::with_capacity(a.len());
                    for e in a {
                        if !cond.matches(&e)? {
                            kept.push(e);
                        }
                    }
                    kept
                };
                set_at(doc, parts, Bson::Array(kept))
            }
            OpKind::Bit => {
                let base = match &cur {
                    None => Bson::Int32(0),
                    Some(v @ (Bson::Int32(_) | Bson::Int64(_))) => v.clone(),
                    Some(v) => {
                        return Err(Error::bad_value(format!(
                            "Cannot apply $bit to a value of non-integral type. {{_id: {}}} has the field {} of non-integer type {}",
                            doc.get("_id").map(|v| v.to_string()).unwrap_or_default(),
                            parts.last().unwrap(),
                            type_name(v)
                        )));
                    }
                };
                let Bson::Document(ops) = &self.arg else { unreachable!() };
                let mut acc = base;
                for (bop, bv) in ops {
                    acc = match (&acc, bv) {
                        (Bson::Int32(a), Bson::Int32(b)) => Bson::Int32(match bop.as_str() {
                            "and" => a & b,
                            "or" => a | b,
                            _ => a ^ b,
                        }),
                        (a, b) => {
                            let (a, b) = (as_i64(a).unwrap(), as_i64(b).unwrap());
                            Bson::Int64(match bop.as_str() {
                                "and" => a & b,
                                "or" => a | b,
                                _ => a ^ b,
                            })
                        }
                    };
                }
                set_at(doc, parts, acc)
            }
        }
    }

    fn target_array(&self, cur: &Option<Bson>, doc: &Document, path: &str) -> Result<Vec<Bson>> {
        match cur {
            None => Ok(Vec::new()),
            Some(Bson::Array(a)) => Ok(a.clone()),
            Some(v) => Err(Error::bad_value(format!(
                "The field '{path}' must be an array but is of type {} in document {{_id: {}}}",
                type_name(v),
                doc.get("_id").map(|v| v.to_string()).unwrap_or_default()
            ))),
        }
    }
}

fn checked_arith(a: &Bson, b: &Bson, add: bool) -> Option<Bson> {
    let int = |v: &Bson| matches!(v, Bson::Int32(_) | Bson::Int64(_));
    if int(a) && int(b) {
        let (x, y) = (as_i64(a)?, as_i64(b)?);
        let r = if add { x.checked_add(y)? } else { x.checked_mul(y)? };
        if matches!((a, b), (Bson::Int32(_), Bson::Int32(_))) {
            return Some(i32::try_from(r).map(Bson::Int32).unwrap_or(Bson::Int64(r)));
        }
        return Some(Bson::Int64(r));
    }
    if add { num_add(a, b) } else { crate::expr::num_mul(a, b) }
}

enum PullCond {
    Value(Bson),
    Pred(crate::query::Pred),
    Doc(Matcher),
}

impl PullCond {
    fn parse(arg: &Bson) -> Result<PullCond> {
        Ok(match arg {
            Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => {
                let only_logical = d.keys().all(|k| matches!(k.as_str(), "$and" | "$or" | "$nor" | "$expr"));
                if only_logical { PullCond::Doc(Matcher::parse(d)?) } else { PullCond::Pred(parse_field_value(arg)?) }
            }
            Bson::Document(d) => PullCond::Doc(Matcher::parse(d)?),
            Bson::RegularExpression(_) => PullCond::Pred(parse_field_value(arg)?),
            v => PullCond::Value(v.clone()),
        })
    }

    fn matches(&self, e: &Bson) -> Result<bool> {
        match self {
            PullCond::Value(v) => Ok(values_equal(e, v)),
            PullCond::Pred(p) => p.matches(&[Some(e)]),
            PullCond::Doc(m) => match e {
                Bson::Document(d) => m.matches(d),
                _ => Ok(false),
            },
        }
    }
}

/// Expands positional segments (`$`, `$[]`, `$[id]`) into concrete paths.
fn expand_path(path: &str, doc: &Document, original: &Document, ctx: &UpdateCtx) -> Result<Vec<Vec<String>>> {
    let parts: Vec<String> = path.split('.').map(String::from).collect();
    if !parts.iter().any(|p| p.starts_with('$')) {
        return Ok(vec![parts]);
    }
    let mut resolved = parts.clone();
    // The positional `$` is resolved against the query.
    if let Some(pos) = parts.iter().position(|p| p == "$") {
        if parts.iter().filter(|p| *p == "$").count() > 1 {
            return Err(Error::bad_value(format!("Too many positional (i.e. '$') elements found in path '{path}'")));
        }
        if pos == 0 {
            return Err(Error::bad_value(format!("Cannot have positional (i.e. '$') element in the first position in path '{path}'")));
        }
        let array_path = parts[..pos].join(".");
        let idx = match ctx.query {
            Some(q) if !ctx.is_insert => q.positional_index(original, &array_path)?,
            _ => None,
        };
        let Some(idx) = idx else {
            return Err(Error::bad_value("The positional operator did not find the match needed from the query."));
        };
        resolved[pos] = idx.to_string();
    }
    let mut out = Vec::new();
    expand_rec(doc, &resolved, 0, Vec::new(), ctx, &mut out)?;
    Ok(out)
}

fn expand_rec(doc: &Document, parts: &[String], i: usize, prefix: Vec<String>, ctx: &UpdateCtx, out: &mut Vec<Vec<String>>) -> Result<()> {
    let Some(i_all) = parts[i..].iter().position(|p| p.starts_with("$[")) else {
        let mut full = prefix;
        full.extend_from_slice(&parts[i..]);
        out.push(full);
        return Ok(());
    };
    let seg = i + i_all;
    let mut base = prefix;
    base.extend_from_slice(&parts[i..seg]);
    let target = if base.is_empty() { None } else { get_at(doc, &base) };
    let Some(Bson::Array(arr)) = target else {
        return Err(Error::bad_value(format!(
            "The path '{}' must exist in the document in order to apply array updates.",
            base.join(".")
        )));
    };
    let token = &parts[seg];
    let ident = &token[2..token.len() - 1];
    let filter = if ident.is_empty() {
        None
    } else {
        Some(ctx.array_filters.iter().find(|(id, _)| id == ident).map(|(_, m)| m).ok_or_else(|| {
            Error::bad_value(format!("No array filter found for identifier '{ident}' in path '{}'", parts.join(".")))
        })?)
    };
    for (idx, elem) in arr.iter().enumerate() {
        if let Some(m) = filter {
            let mut probe = Document::new();
            probe.insert(ident, elem.clone());
            if !m.matches(&probe)? {
                continue;
            }
        }
        let mut next = base.clone();
        next.push(idx.to_string());
        expand_rec(doc, parts, seg + 1, next, ctx, out)?;
    }
    Ok(())
}

/// Checks that all array filter identifiers are used by some update path.
pub fn check_array_filters_used(update: &Update, filters: &[(String, Matcher)]) -> Result<()> {
    let Update::Operators(ops) = update else {
        if !filters.is_empty() {
            return Err(Error::failed_to_parse("arrayFilters may only be specified with update operators"));
        }
        return Ok(());
    };
    for (ident, _) in filters {
        let tok = format!("$[{ident}]");
        if !ops.iter().any(|o| o.path.split('.').any(|p| p == tok)) {
            return Err(Error::failed_to_parse(format!(
                "The array filter for identifier '{ident}' was not used in the update"
            )));
        }
    }
    Ok(())
}

/// Builds the document inserted by an upsert that matched nothing.
pub fn upsert_document(query: &Matcher, update: &Update, ctx: &UpdateCtx, generated_id: ObjectId) -> Result<Document> {
    let mut seed = Document::new();
    for (path, value) in query.upsert_seed() {
        let parts: Vec<String> = path.split('.').map(String::from).collect();
        if get_at(&seed, &parts).is_some() {
            continue;
        }
        set_at(&mut seed, &parts, value.clone())?;
    }
    let mut doc = match update {
        Update::Replacement(r) => {
            let mut d = Document::new();
            if let Some(id) = r.get("_id").or_else(|| seed.get("_id")) {
                d.insert("_id", id.clone());
            }
            for (k, v) in r {
                if k != "_id" {
                    d.insert(k.clone(), v.clone());
                }
            }
            d
        }
        _ => {
            let seed_id = seed.get("_id").cloned();
            let d = update.apply(&seed, ctx)?;
            if let (Some(a), Some(b)) = (&seed_id, d.get("_id"))
                && a != b
            {
                return Err(immutable_id_error(&seed, "_id"));
            }
            d
        }
    };
    if !doc.contains_key("_id") {
        let mut with_id = doc! {"_id": generated_id};
        with_id.extend(doc);
        doc = with_id;
    } else if doc.keys().next().map(String::as_str) != Some("_id") {
        let id = doc.remove("_id").unwrap();
        let mut with_id = doc! {"_id": id};
        with_id.extend(doc);
        doc = with_id;
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(d: Document, u: Document) -> Result<Document> {
        let q = Matcher::parse(&doc! {}).unwrap();
        let ctx = UpdateCtx { is_insert: false, now_millis: 1000, query: Some(&q), array_filters: &[] };
        Update::parse(&Bson::Document(u))?.apply(&d, &ctx)
    }

    #[test]
    fn basic_operators() {
        let d = doc! {"_id": 1, "a": 1, "arr": [1, 2]};
        assert_eq!(run(d.clone(), doc! {"$set": {"b.c": 2}}).unwrap(), doc! {"_id": 1, "a": 1, "arr": [1, 2], "b": {"c": 2}});
        assert_eq!(run(d.clone(), doc! {"$inc": {"a": 2}}).unwrap().get("a"), Some(&Bson::Int32(3)));
        assert_eq!(run(d.clone(), doc! {"$inc": {"a": 1.5}}).unwrap().get("a"), Some(&Bson::Double(2.5)));
        assert_eq!(run(d.clone(), doc! {"$unset": {"a": ""}}).unwrap(), doc! {"_id": 1, "arr": [1, 2]});
        assert_eq!(run(d.clone(), doc! {"$push": {"arr": 3}}).unwrap().get("arr"), Some(&Bson::from(vec![1, 2, 3])));
        assert_eq!(run(d.clone(), doc! {"$addToSet": {"arr": 2}}).unwrap().get("arr"), Some(&Bson::from(vec![1, 2])));
        assert_eq!(run(d.clone(), doc! {"$pull": {"arr": {"$gt": 1}}}).unwrap().get("arr"), Some(&Bson::from(vec![1])));
        assert_eq!(run(d.clone(), doc! {"$pop": {"arr": -1}}).unwrap().get("arr"), Some(&Bson::from(vec![2])));
        assert_eq!(run(d.clone(), doc! {"$set": {"arr.3": 9}}).unwrap().get("arr"), Some(&Bson::Array(vec![1.into(), 2.into(), Bson::Null, 9.into()])));
        assert_eq!(run(d.clone(), doc! {"$rename": {"a": "z"}}).unwrap(), doc! {"_id": 1, "arr": [1, 2], "z": 1});
        assert_eq!(run(d.clone(), doc! {"$max": {"a": 5}}).unwrap().get("a"), Some(&Bson::Int32(5)));
        assert_eq!(run(d.clone(), doc! {"$min": {"a": 5}}).unwrap().get("a"), Some(&Bson::Int32(1)));
        assert_eq!(run(d.clone(), doc! {"$mul": {"q": 5}}).unwrap().get("q"), Some(&Bson::Int32(0)));
        assert_eq!(
            run(d.clone(), doc! {"$push": {"arr": {"$each": [5, 0], "$sort": 1, "$slice": -3}}}).unwrap().get("arr"),
            Some(&Bson::from(vec![1, 2, 5]))
        );
    }

    #[test]
    fn errors() {
        let d = doc! {"_id": 1, "a": 1, "s": "x"};
        assert_eq!(run(d.clone(), doc! {"$set": {"_id": 2}}).unwrap_err().code, 66);
        assert!(run(d.clone(), doc! {"$set": {"_id": 1}}).is_ok());
        assert_eq!(run(d.clone(), doc! {"$inc": {"s": 1}}).unwrap_err().code, 14);
        assert_eq!(run(d.clone(), doc! {"$set": {"a.b": 1}}).unwrap_err().code, 28);
        assert_eq!(run(d.clone(), doc! {"$set": {"a": 1}, "$inc": {"a": 1}}).unwrap_err().code, 40);
        assert_eq!(run(d.clone(), doc! {"$set": {"a": 1, "a.b": 1}}).unwrap_err().code, 40);
        assert_eq!(run(d.clone(), doc! {"$inc": {"a": i64::MAX}}).unwrap_err().code, 2);
        assert_eq!(run(d.clone(), doc! {"$inc": {"a": i32::MAX}}).unwrap().get("a"), Some(&Bson::Int64(i32::MAX as i64 + 1)));
    }

    #[test]
    fn positional_and_array_filters() {
        let d = doc! {"_id": 1, "arr": [{"id": 1, "v": 0}, {"id": 3, "v": 0}]};
        let q = Matcher::parse(&doc! {"arr.id": 3}).unwrap();
        let ctx = UpdateCtx { is_insert: false, now_millis: 0, query: Some(&q), array_filters: &[] };
        let out = Update::parse(&Bson::Document(doc! {"$set": {"arr.$.v": 7}})).unwrap().apply(&d, &ctx).unwrap();
        assert_eq!(out, doc! {"_id": 1, "arr": [{"id": 1, "v": 0}, {"id": 3, "v": 7}]});

        let out = Update::parse(&Bson::Document(doc! {"$inc": {"arr.$[].v": 1}})).unwrap().apply(&d, &ctx).unwrap();
        assert_eq!(out, doc! {"_id": 1, "arr": [{"id": 1, "v": 1}, {"id": 3, "v": 1}]});

        let filters = parse_array_filters(Some(&Bson::Array(vec![Bson::Document(doc! {"e.id": {"$lt": 2}})]))).unwrap();
        let ctx = UpdateCtx { is_insert: false, now_millis: 0, query: Some(&q), array_filters: &filters };
        let out = Update::parse(&Bson::Document(doc! {"$set": {"arr.$[e].v": 5}})).unwrap().apply(&d, &ctx).unwrap();
        assert_eq!(out, doc! {"_id": 1, "arr": [{"id": 1, "v": 5}, {"id": 3, "v": 0}]});
    }

    #[test]
    fn upserts() {
        let q = Matcher::parse(&doc! {"name": "x", "n": {"$gt": 1}}).unwrap();
        let ctx = UpdateCtx { is_insert: true, now_millis: 0, query: Some(&q), array_filters: &[] };
        let u = Update::parse(&Bson::Document(doc! {"$set": {"a": 1}, "$setOnInsert": {"created": true}})).unwrap();
        let oid = ObjectId::from_bytes([7; 12]);
        let d = upsert_document(&q, &u, &ctx, oid).unwrap();
        assert_eq!(d, doc! {"_id": oid, "name": "x", "a": 1, "created": true});
    }
}
