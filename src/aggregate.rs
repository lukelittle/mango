//! The aggregation pipeline.

use crate::bsonutil::{as_f64, as_i64, is_number, type_name};
use crate::error::{Error, Result};
use crate::expr::{Expr, Vars, num_add};
use crate::keystring::{self, compare};
use crate::projection::{Projection, exclude_path, set_path_agg};
use crate::query::{InItem, Matcher, Pred, lookup};
use bson::{Bson, Document, doc};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Read access to other collections, for `$lookup` and `$unionWith`.
pub trait Source {
    fn scan(&self, db: &str, coll: &str, filter: &Matcher) -> Result<Vec<Document>>;
}

pub struct Env<'a> {
    pub db: String,
    pub source: &'a dyn Source,
    pub vars: Vec<(String, Bson)>,
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    keys: Vec<(Vec<String>, bool)>, // (path, ascending)
}

impl SortSpec {
    pub fn parse(spec: &Document) -> Result<SortSpec> {
        if spec.is_empty() {
            return Err(Error::bad_value("$sort stage must have at least one sort key"));
        }
        let mut keys = Vec::new();
        for (k, v) in spec {
            let asc = match v {
                v if is_number(v) => match as_f64(v) {
                    Some(1.0) => true,
                    Some(-1.0) => false,
                    _ => {
                        return Err(Error::bad_value(format!(
                            "$sort key ordering must be 1 (for ascending) or -1 (for descending), found {v}"
                        )));
                    }
                },
                Bson::Document(d) if d.contains_key("$meta") => {
                    return Err(Error::not_implemented("$meta sort keys are not supported by Mango"));
                }
                _ => {
                    return Err(Error::bad_value(format!(
                        "$sort key ordering must be 1 (for ascending) or -1 (for descending), found {v}"
                    )));
                }
            };
            if k.is_empty() || k.split('.').any(|p| p.is_empty() || p.starts_with('$')) {
                return Err(Error::bad_value(format!("invalid sort key '{k}'")));
            }
            keys.push((k.split('.').map(String::from).collect(), asc));
        }
        Ok(SortSpec { keys })
    }

    /// The comparable sort key for a document: for each key, the minimum
    /// (ascending) or maximum (descending) value reachable at the path, with
    /// arrays contributing their elements, and missing sorting as null.
    pub fn key(&self, doc: &Document) -> Vec<Vec<u8>> {
        self.keys
            .iter()
            .map(|(parts, asc)| {
                let mut cands = Vec::new();
                lookup(doc, parts, &mut cands);
                let mut best: Option<Vec<u8>> = None;
                let mut consider = |v: &Bson| {
                    let k = keystring::encode(v);
                    best = Some(match best.take() {
                        None => k,
                        Some(b) => {
                            if (*asc && k < b) || (!*asc && k > b) {
                                k
                            } else {
                                b
                            }
                        }
                    });
                };
                for c in cands {
                    match c {
                        None => consider(&Bson::Null),
                        Some(Bson::Array(a)) if a.is_empty() => consider(&Bson::Undefined),
                        Some(Bson::Array(a)) => a.iter().for_each(&mut consider),
                        Some(v) => consider(v),
                    }
                }
                best.unwrap_or_else(|| keystring::encode(&Bson::Null))
            })
            .collect()
    }

    pub fn compare_keys(&self, a: &[Vec<u8>], b: &[Vec<u8>]) -> Ordering {
        for (i, (_, asc)) in self.keys.iter().enumerate() {
            let o = a[i].cmp(&b[i]);
            if o != Ordering::Equal {
                return if *asc { o } else { o.reverse() };
            }
        }
        Ordering::Equal
    }

    pub fn compare(&self, a: &Document, b: &Document) -> Ordering {
        self.compare_keys(&self.key(a), &self.key(b))
    }

    pub fn sort(&self, docs: Vec<Document>) -> Vec<Document> {
        let mut keyed: Vec<(Vec<Vec<u8>>, Document)> = docs.into_iter().map(|d| (self.key(&d), d)).collect();
        keyed.sort_by(|a, b| self.compare_keys(&a.0, &b.0));
        keyed.into_iter().map(|(_, d)| d).collect()
    }

    /// Paths and directions, for index selection.
    pub fn fields(&self) -> impl Iterator<Item = (String, bool)> + '_ {
        self.keys.iter().map(|(p, asc)| (p.join("."), *asc))
    }
}

#[derive(Debug, Clone)]
pub enum Acc {
    Sum(Expr),
    Avg(Expr),
    Min(Expr),
    Max(Expr),
    First(Expr),
    Last(Expr),
    Push(Expr),
    AddToSet(Expr),
    MergeObjects(Expr),
    StdDev(Expr, bool),
    Count,
}

#[derive(Debug, Clone)]
pub enum Stage {
    Match(Matcher),
    Project(Projection),
    AddFields(Vec<(Vec<String>, Expr)>),
    Unset(Vec<Vec<String>>),
    Sort(SortSpec),
    Limit(usize),
    Skip(usize),
    Group(Expr, Vec<(String, Acc)>),
    Count(String),
    Unwind {
        path: Vec<String>,
        index_field: Option<String>,
        preserve: bool,
    },
    Lookup {
        from: String,
        local: Option<String>,
        foreign: Option<String>,
        lets: Vec<(String, Expr)>,
        pipeline: Vec<Stage>,
        as_field: Vec<String>,
    },
    ReplaceRoot(Expr),
    SortByCount(Expr),
    Facet(Vec<(String, Vec<Stage>)>),
    Sample(usize),
    UnionWith {
        coll: String,
        pipeline: Vec<Stage>,
    },
}

pub fn parse_pipeline(stages: &[Bson]) -> Result<Vec<Stage>> {
    stages
        .iter()
        .map(|s| match s {
            Bson::Document(d) => Stage::parse(d),
            _ => Err(Error::type_mismatch("Each element of the 'pipeline' array must be an object")),
        })
        .collect()
}

fn field_name(s: &str, what: &str) -> Result<String> {
    if s.is_empty() || s.starts_with('$') || s.contains('\0') {
        return Err(Error::bad_value(format!("{what} must be a non-empty string that does not start with '$'")));
    }
    Ok(s.to_string())
}

fn non_negative(v: &Bson, stage: &str) -> Result<usize> {
    match as_f64(v) {
        Some(f) if f.fract() == 0.0 && f >= 0.0 => Ok(f as usize),
        _ => Err(Error::bad_value(format!("invalid argument to {stage} stage: expected a non-negative integer, found {v}"))),
    }
}

impl Stage {
    pub fn parse(d: &Document) -> Result<Stage> {
        if d.len() != 1 {
            return Err(
                Error::location("A pipeline stage specification object must contain exactly one field.").with_code(40323, "Location40323")
            );
        }
        let (name, arg) = d.iter().next().unwrap();
        let as_doc = |what: &str| -> Result<&Document> {
            match arg {
                Bson::Document(x) => Ok(x),
                _ => Err(Error::type_mismatch(format!("{what} specification must be an object"))),
            }
        };
        Ok(match name.as_str() {
            "$match" => Stage::Match(Matcher::parse(as_doc("$match")?)?),
            "$project" => Stage::Project(Projection::parse(as_doc("$project")?, false)?),
            "$addFields" | "$set" => {
                let spec = as_doc(name)?;
                let mut fields = Vec::new();
                flatten_add_fields(spec, "", &mut fields)?;
                Stage::AddFields(fields)
            }
            "$unset" => {
                let paths: Vec<Vec<String>> = match arg {
                    Bson::String(s) => vec![s.split('.').map(String::from).collect()],
                    Bson::Array(a) => a
                        .iter()
                        .map(|v| match v {
                            Bson::String(s) => Ok(s.split('.').map(String::from).collect()),
                            _ => Err(Error::type_mismatch("$unset specification must be a string or an array of strings")),
                        })
                        .collect::<Result<_>>()?,
                    _ => return Err(Error::type_mismatch("$unset specification must be a string or an array of strings")),
                };
                if paths.is_empty() {
                    return Err(Error::bad_value("$unset specification must be a string or an array with at least one field"));
                }
                Stage::Unset(paths)
            }
            "$sort" => Stage::Sort(SortSpec::parse(as_doc("$sort")?)?),
            "$limit" => {
                let n = non_negative(arg, "$limit")?;
                if n == 0 {
                    return Err(Error::bad_value("the limit must be positive"));
                }
                Stage::Limit(n)
            }
            "$skip" => Stage::Skip(non_negative(arg, "$skip")?),
            "$group" => {
                let spec = as_doc("$group")?;
                let Some(id) = spec.get("_id") else {
                    return Err(Error::bad_value("a group specification must include an _id"));
                };
                let id = Expr::parse(id)?;
                let mut accs = Vec::new();
                for (k, v) in spec {
                    if k == "_id" {
                        continue;
                    }
                    let name = field_name(k, "$group field name")?;
                    if k.contains('.') {
                        return Err(Error::bad_value(format!("The field name '{k}' cannot contain '.'")));
                    }
                    let Bson::Document(ad) = v else {
                        return Err(Error::type_mismatch(format!("The field '{k}' must be an accumulator object")));
                    };
                    if ad.len() != 1 {
                        return Err(Error::type_mismatch(format!("The field '{k}' must specify one accumulator")));
                    }
                    let (op, e) = ad.iter().next().unwrap();
                    let ex = || Expr::parse(e);
                    let acc = match op.as_str() {
                        "$sum" => Acc::Sum(ex()?),
                        "$avg" => Acc::Avg(ex()?),
                        "$min" => Acc::Min(ex()?),
                        "$max" => Acc::Max(ex()?),
                        "$first" => Acc::First(ex()?),
                        "$last" => Acc::Last(ex()?),
                        "$push" => Acc::Push(ex()?),
                        "$addToSet" => Acc::AddToSet(ex()?),
                        "$mergeObjects" => Acc::MergeObjects(ex()?),
                        "$stdDevPop" => Acc::StdDev(ex()?, true),
                        "$stdDevSamp" => Acc::StdDev(ex()?, false),
                        "$count" => Acc::Count,
                        other => return Err(Error::bad_value(format!("unknown group operator '{other}'"))),
                    };
                    accs.push((name, acc));
                }
                Stage::Group(id, accs)
            }
            "$count" => {
                let Bson::String(s) = arg else {
                    return Err(Error::type_mismatch("the count field must be a non-empty string"));
                };
                if s.contains('.') {
                    return Err(Error::bad_value("the count field cannot contain '.'"));
                }
                Stage::Count(field_name(s, "the count field")?)
            }
            "$unwind" => {
                let (path, index_field, preserve) = match arg {
                    Bson::String(s) => (s.clone(), None, false),
                    Bson::Document(u) => {
                        let path = u.get_str("path").map_err(|_| Error::bad_value("no path specified to $unwind stage"))?.to_string();
                        let idx = match u.get("includeArrayIndex") {
                            None => None,
                            Some(Bson::String(s)) => Some(field_name(s, "includeArrayIndex")?),
                            Some(_) => return Err(Error::type_mismatch("expected a non-empty string for the includeArrayIndex option")),
                        };
                        let preserve = match u.get("preserveNullAndEmptyArrays") {
                            None => false,
                            Some(Bson::Boolean(b)) => *b,
                            Some(_) => return Err(Error::type_mismatch("expected a boolean for the preserveNullAndEmptyArrays option")),
                        };
                        (path, idx, preserve)
                    }
                    _ => return Err(Error::type_mismatch("expected either a string or an object as specification for $unwind stage")),
                };
                let Some(p) = path.strip_prefix('$') else {
                    return Err(Error::bad_value("path option to $unwind stage should be prefixed with a '$'"));
                };
                Stage::Unwind { path: p.split('.').map(String::from).collect(), index_field, preserve }
            }
            "$lookup" => {
                let spec = as_doc("$lookup")?;
                let from =
                    spec.get_str("from").map_err(|_| Error::failed_to_parse("$lookup requires a 'from' collection name"))?.to_string();
                let as_field = spec.get_str("as").map_err(|_| Error::failed_to_parse("must specify 'as' field for a $lookup"))?;
                let local = spec.get_str("localField").ok().map(String::from);
                let foreign = spec.get_str("foreignField").ok().map(String::from);
                if local.is_some() != foreign.is_some() {
                    return Err(Error::failed_to_parse("$lookup requires both or neither of 'localField' and 'foreignField'"));
                }
                let pipeline = match spec.get("pipeline") {
                    Some(Bson::Array(p)) => parse_pipeline(p)?,
                    None => vec![],
                    Some(_) => return Err(Error::failed_to_parse("$lookup 'pipeline' must be an array")),
                };
                if local.is_none() && !spec.contains_key("pipeline") {
                    return Err(Error::failed_to_parse("$lookup requires either 'pipeline' or both 'localField' and 'foreignField'"));
                }
                let lets = match spec.get("let") {
                    Some(Bson::Document(l)) => l.iter().map(|(k, v)| Ok((k.clone(), Expr::parse(v)?))).collect::<Result<_>>()?,
                    None => vec![],
                    Some(_) => return Err(Error::failed_to_parse("$lookup 'let' must be an object")),
                };
                Stage::Lookup { from, local, foreign, lets, pipeline, as_field: as_field.split('.').map(String::from).collect() }
            }
            "$replaceRoot" => {
                let spec = as_doc("$replaceRoot")?;
                let nr = spec.get("newRoot").ok_or_else(|| Error::bad_value("no newRoot specified for the $replaceRoot stage"))?;
                Stage::ReplaceRoot(Expr::parse(nr)?)
            }
            "$replaceWith" => Stage::ReplaceRoot(Expr::parse(arg)?),
            "$sortByCount" => Stage::SortByCount(Expr::parse(arg)?),
            "$facet" => {
                let spec = as_doc("$facet")?;
                let mut facets = Vec::new();
                for (k, v) in spec {
                    let Bson::Array(p) = v else {
                        return Err(Error::type_mismatch(format!("arguments to $facet must be arrays, {k} is type {}", type_name(v))));
                    };
                    let stages = parse_pipeline(p)?;
                    if stages.iter().any(|s| matches!(s, Stage::Facet(_))) {
                        return Err(Error::bad_value("$facet is not allowed to be used within a $facet stage"));
                    }
                    facets.push((field_name(k, "$facet field name")?, stages));
                }
                Stage::Facet(facets)
            }
            "$sample" => {
                let spec = as_doc("$sample")?;
                let size = spec.get("size").ok_or_else(|| Error::failed_to_parse("$sample stage must specify a size"))?;
                Stage::Sample(non_negative(size, "$sample")?)
            }
            "$unionWith" => match arg {
                Bson::String(c) => Stage::UnionWith { coll: c.clone(), pipeline: vec![] },
                Bson::Document(u) => {
                    let coll = u.get_str("coll").map_err(|_| Error::failed_to_parse("$unionWith requires 'coll'"))?.to_string();
                    let pipeline = match u.get("pipeline") {
                        Some(Bson::Array(p)) => parse_pipeline(p)?,
                        None => vec![],
                        Some(_) => return Err(Error::failed_to_parse("$unionWith 'pipeline' must be an array")),
                    };
                    Stage::UnionWith { coll, pipeline }
                }
                _ => return Err(Error::failed_to_parse("$unionWith requires a string or object argument")),
            },
            "$out" | "$merge" => {
                return Err(Error::not_implemented(format!(
                    "{name} is not supported by Mango; write results with insert commands instead"
                )));
            }
            other => return Err(Error::location(format!("Unrecognized pipeline stage name: '{other}'")).with_code(40324, "Location40324")),
        })
    }

    pub fn allowed_in_update(&self) -> bool {
        matches!(self, Stage::AddFields(_) | Stage::Project(_) | Stage::Unset(_) | Stage::ReplaceRoot(_))
    }

    /// Applies a document-to-document stage (used by pipeline updates).
    pub fn apply_single(&self, doc: Document) -> Result<Document> {
        match self {
            Stage::AddFields(fields) => add_fields(doc, fields, &[]),
            Stage::Project(p) => p.apply(&doc, None),
            Stage::Unset(paths) => {
                let mut d = doc;
                for p in paths {
                    exclude_path(&mut d, p);
                }
                Ok(d)
            }
            Stage::ReplaceRoot(e) => replace_root(&doc, e, &[]),
            _ => Err(Error::invalid_options("stage not allowed in an update")),
        }
    }
}

fn flatten_add_fields(spec: &Document, prefix: &str, out: &mut Vec<(Vec<String>, Expr)>) -> Result<()> {
    for (k, v) in spec {
        if k.starts_with('$') {
            return Err(Error::bad_value(format!("FieldPath field names may not start with '$'. Given: {k}")));
        }
        let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        match v {
            Bson::Document(d) if !d.is_empty() && !d.keys().next().unwrap().starts_with('$') => flatten_add_fields(d, &path, out)?,
            v => out.push((path.split('.').map(String::from).collect(), Expr::parse(v)?)),
        }
    }
    Ok(())
}

fn add_fields(mut doc: Document, fields: &[(Vec<String>, Expr)], env: &[(String, Bson)]) -> Result<Document> {
    let src = doc.clone();
    let vars = Vars::with_bindings(&src, env);
    for (path, e) in fields {
        match e.eval_opt(&vars)? {
            Some(v) => set_path_agg(&mut doc, path, v),
            None => exclude_path(&mut doc, path),
        }
    }
    Ok(doc)
}

fn replace_root(doc: &Document, e: &Expr, env: &[(String, Bson)]) -> Result<Document> {
    match e.eval(&Vars::with_bindings(doc, env))? {
        Bson::Document(d) => Ok(d),
        other => Err(Error::location(format!(
            "'newRoot' expression must evaluate to an object, but resulting value was: {other}. Type of resulting value: '{}'.",
            type_name(&other)
        ))
        .with_code(40228, "Location40228")),
    }
}

#[derive(Default)]
struct AccState {
    sum: Option<Bson>,
    count: i64,
    fsum: f64,
    fsum_sq: f64,
    best: Option<Bson>,
    first_set: bool,
    list: Vec<Bson>,
    merged: Document,
}

pub fn run_pipeline(stages: &[Stage], mut docs: Vec<Document>, env: &Env) -> Result<Vec<Document>> {
    for stage in stages {
        docs = run_stage(stage, docs, env)?;
    }
    Ok(docs)
}

fn run_stage(stage: &Stage, docs: Vec<Document>, env: &Env) -> Result<Vec<Document>> {
    Ok(match stage {
        Stage::Match(m) => {
            let mut out = Vec::with_capacity(docs.len());
            for d in docs {
                if m.matches_env(&d, &env.vars)? {
                    out.push(d);
                }
            }
            out
        }
        Stage::Project(p) => docs.iter().map(|d| p.apply(d, None)).collect::<Result<_>>()?,
        Stage::AddFields(fields) => docs.into_iter().map(|d| add_fields(d, fields, &env.vars)).collect::<Result<_>>()?,
        Stage::Unset(paths) => docs
            .into_iter()
            .map(|mut d| {
                for p in paths {
                    exclude_path(&mut d, p);
                }
                d
            })
            .collect(),
        Stage::Sort(spec) => spec.sort(docs),
        Stage::Limit(n) => docs.into_iter().take(*n).collect(),
        Stage::Skip(n) => docs.into_iter().skip(*n).collect(),
        Stage::Count(name) => {
            if docs.is_empty() {
                vec![]
            } else {
                let n = docs.len();
                let v = i32::try_from(n).map(Bson::Int32).unwrap_or(Bson::Int64(n as i64));
                let mut d = Document::new();
                d.insert(name.clone(), v);
                vec![d]
            }
        }
        Stage::Group(id, accs) => group(docs, id, accs, env)?,
        Stage::SortByCount(e) => {
            let grouped = group(docs, e, &[("count".to_string(), Acc::Sum(Expr::Literal(Bson::Int32(1))))], env)?;
            let spec = SortSpec::parse(&doc! {"count": -1})?;
            spec.sort(grouped)
        }
        Stage::Unwind { path, index_field, preserve } => {
            let mut out = Vec::new();
            for d in docs {
                let val = crate::update::get_at(&d, path).cloned();
                match val {
                    Some(Bson::Array(a)) if !a.is_empty() => {
                        for (i, e) in a.into_iter().enumerate() {
                            let mut nd = d.clone();
                            crate::update::set_at(&mut nd, path, e)?;
                            if let Some(f) = index_field {
                                nd.insert(f.clone(), Bson::Int64(i as i64));
                            }
                            out.push(nd);
                        }
                    }
                    Some(Bson::Array(_)) => {
                        if *preserve {
                            let mut nd = d;
                            exclude_path(&mut nd, path);
                            if let Some(f) = index_field {
                                nd.insert(f.clone(), Bson::Null);
                            }
                            out.push(nd);
                        }
                    }
                    None | Some(Bson::Null) | Some(Bson::Undefined) => {
                        if *preserve {
                            let mut nd = d;
                            if let Some(f) = index_field {
                                nd.insert(f.clone(), Bson::Null);
                            }
                            out.push(nd);
                        }
                    }
                    Some(_) => {
                        let mut nd = d;
                        if let Some(f) = index_field {
                            nd.insert(f.clone(), Bson::Null);
                        }
                        out.push(nd);
                    }
                }
            }
            out
        }
        Stage::Lookup { from, local, foreign, lets, pipeline, as_field } => {
            let mut out = Vec::with_capacity(docs.len());
            let all_foreign = env.source.scan(&env.db, from, &Matcher::Always)?;
            for mut d in docs {
                let mut matched: Vec<Document> = match (local, foreign) {
                    (Some(lf), Some(ff)) => {
                        let lparts: Vec<String> = lf.split('.').map(String::from).collect();
                        let fparts: Vec<String> = ff.split('.').map(String::from).collect();
                        let mut cands = Vec::new();
                        lookup(&d, &lparts, &mut cands);
                        let mut items = Vec::new();
                        for c in cands {
                            match c {
                                None => items.push(InItem::Value(Bson::Null)),
                                Some(Bson::Array(a)) => {
                                    if a.is_empty() {
                                        items.push(InItem::Value(Bson::Null));
                                    }
                                    a.iter().for_each(|e| items.push(InItem::Value(e.clone())))
                                }
                                Some(v) => items.push(InItem::Value(v.clone())),
                            }
                        }
                        let pred = Pred::In(items);
                        let mut res = Vec::new();
                        for f in &all_foreign {
                            let mut fc = Vec::new();
                            lookup(f, &fparts, &mut fc);
                            if pred.matches(&fc)? {
                                res.push(f.clone());
                            }
                        }
                        res
                    }
                    _ => all_foreign.clone(),
                };
                if !pipeline.is_empty() {
                    let mut sub_vars = env.vars.clone();
                    let vars = Vars::with_bindings(&d, &env.vars);
                    for (name, e) in lets {
                        sub_vars.push((name.clone(), e.eval(&vars)?));
                    }
                    let sub_env = Env { db: env.db.clone(), source: env.source, vars: sub_vars };
                    matched = run_pipeline(pipeline, matched, &sub_env)?;
                }
                set_path_agg(&mut d, as_field, Bson::Array(matched.into_iter().map(Bson::Document).collect()));
                out.push(d);
            }
            out
        }
        Stage::ReplaceRoot(e) => docs.iter().map(|d| replace_root(d, e, &env.vars)).collect::<Result<_>>()?,
        Stage::Facet(facets) => {
            let mut out = Document::new();
            for (name, stages) in facets {
                let res = run_pipeline(stages, docs.clone(), env)?;
                out.insert(name.clone(), Bson::Array(res.into_iter().map(Bson::Document).collect()));
            }
            vec![out]
        }
        Stage::Sample(n) => {
            use rand::seq::SliceRandom;
            let mut docs = docs;
            docs.shuffle(&mut rand::rng());
            docs.truncate(*n);
            docs
        }
        Stage::UnionWith { coll, pipeline } => {
            let mut docs = docs;
            let other = env.source.scan(&env.db, coll, &Matcher::Always)?;
            docs.extend(run_pipeline(pipeline, other, env)?);
            docs
        }
    })
}

fn group(docs: Vec<Document>, id: &Expr, accs: &[(String, Acc)], env: &Env) -> Result<Vec<Document>> {
    let mut order: Vec<(Bson, Vec<AccState>)> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    for d in &docs {
        let vars = Vars::with_bindings(d, &env.vars);
        let key = id.eval(&vars)?;
        let k = keystring::encode(&key);
        let slot = match index.get(&k) {
            Some(i) => *i,
            None => {
                order.push((key, accs.iter().map(|_| AccState::default()).collect()));
                index.insert(k, order.len() - 1);
                order.len() - 1
            }
        };
        let states = &mut order[slot].1;
        for (i, (_, acc)) in accs.iter().enumerate() {
            let st = &mut states[i];
            match acc {
                Acc::Count => st.count += 1,
                Acc::Sum(e) => {
                    let v = e.eval(&vars)?;
                    let v = match v {
                        Bson::Array(_) => continue,
                        v => v,
                    };
                    if is_number(&v) {
                        st.sum = Some(match st.sum.take() {
                            None => v,
                            Some(s) => num_add(&s, &v).unwrap(),
                        });
                    }
                }
                Acc::Avg(e) | Acc::StdDev(e, _) => {
                    let v = e.eval(&vars)?;
                    if is_number(&v) {
                        let f = as_f64(&v).unwrap();
                        st.count += 1;
                        st.fsum += f;
                        st.fsum_sq += f * f;
                        st.list.push(Bson::Double(f));
                    }
                }
                Acc::Min(e) | Acc::Max(e) => {
                    let v = e.eval(&vars)?;
                    if matches!(v, Bson::Null | Bson::Undefined) {
                        continue;
                    }
                    let is_min = matches!(acc, Acc::Min(_));
                    let replace = match &st.best {
                        None => true,
                        Some(b) => {
                            let o = compare(&v, b);
                            (is_min && o == Ordering::Less) || (!is_min && o == Ordering::Greater)
                        }
                    };
                    if replace {
                        st.best = Some(v);
                    }
                }
                Acc::First(e) => {
                    if !st.first_set {
                        st.best = Some(e.eval(&vars)?);
                        st.first_set = true;
                    }
                }
                Acc::Last(e) => st.best = Some(e.eval(&vars)?),
                Acc::Push(e) => {
                    if let Some(v) = e.eval_opt(&vars)? {
                        st.list.push(v);
                    }
                }
                Acc::AddToSet(e) => {
                    if let Some(v) = e.eval_opt(&vars)?
                        && !st.list.iter().any(|x| compare(x, &v) == Ordering::Equal)
                    {
                        st.list.push(v);
                    }
                }
                Acc::MergeObjects(e) => match e.eval(&vars)? {
                    Bson::Document(m) => {
                        for (k, v) in m {
                            st.merged.insert(k, v);
                        }
                    }
                    Bson::Null | Bson::Undefined => {}
                    other => {
                        return Err(Error::type_mismatch(format!(
                            "$mergeObjects requires object inputs, but input is of type {}",
                            type_name(&other)
                        )));
                    }
                },
            }
        }
    }
    let mut out = Vec::with_capacity(order.len());
    for (key, states) in order {
        let mut d = doc! {"_id": key};
        for ((name, acc), st) in accs.iter().zip(states) {
            let v = match acc {
                Acc::Count => i32::try_from(st.count).map(Bson::Int32).unwrap_or(Bson::Int64(st.count)),
                Acc::Sum(_) => st.sum.unwrap_or(Bson::Int32(0)),
                Acc::Avg(_) => {
                    if st.count == 0 {
                        Bson::Null
                    } else {
                        Bson::Double(st.fsum / st.count as f64)
                    }
                }
                Acc::StdDev(_, pop) => {
                    let n = st.count as f64;
                    if st.count == 0 || (!pop && st.count < 2) {
                        Bson::Null
                    } else {
                        let mean = st.fsum / n;
                        let ss: f64 = st.list.iter().map(|x| (as_f64(x).unwrap() - mean).powi(2)).sum();
                        Bson::Double((ss / if *pop { n } else { n - 1.0 }).sqrt())
                    }
                }
                Acc::Min(_) | Acc::Max(_) | Acc::First(_) | Acc::Last(_) => st.best.unwrap_or(Bson::Null),
                Acc::Push(_) | Acc::AddToSet(_) => Bson::Array(st.list),
                Acc::MergeObjects(_) => Bson::Document(st.merged),
            };
            d.insert(name.clone(), v);
        }
        out.push(d);
    }
    Ok(out)
}

/// Integer argument helper shared by commands.
pub fn int_arg(v: &Bson) -> Option<i64> {
    as_i64(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoSource;
    impl Source for NoSource {
        fn scan(&self, _: &str, coll: &str, _: &Matcher) -> Result<Vec<Document>> {
            if coll == "other" {
                return Ok(vec![
                    doc! {"_id": 10, "k": 1, "v": "a"},
                    doc! {"_id": 11, "k": 2, "v": "b"},
                    doc! {"_id": 12, "k": 1, "v": "c"},
                ]);
            }
            Ok(vec![])
        }
    }

    fn run(p: Vec<Document>, docs: Vec<Document>) -> Vec<Document> {
        let stages = parse_pipeline(&p.into_iter().map(Bson::Document).collect::<Vec<_>>()).unwrap();
        let env = Env { db: "test".into(), source: &NoSource, vars: vec![] };
        run_pipeline(&stages, docs, &env).unwrap()
    }

    #[test]
    fn group_sort_project() {
        let docs = vec![doc! {"_id": 1, "cat": "a", "n": 1}, doc! {"_id": 2, "cat": "b", "n": 5}, doc! {"_id": 3, "cat": "a", "n": 2.5}];
        let out = run(
            vec![
                doc! {"$group": {"_id": "$cat", "total": {"$sum": "$n"}, "c": {"$count": {}}, "ns": {"$push": "$n"}}},
                doc! {"$sort": {"_id": 1}},
            ],
            docs.clone(),
        );
        assert_eq!(out, vec![doc! {"_id": "a", "total": 3.5, "c": 2, "ns": [1, 2.5]}, doc! {"_id": "b", "total": 5, "c": 1, "ns": [5]}]);
        let out = run(vec![doc! {"$match": {"n": {"$gt": 1}}}, doc! {"$count": "k"}], docs.clone());
        assert_eq!(out, vec![doc! {"k": 2}]);
        let out = run(
            vec![doc! {"$sort": {"n": -1}}, doc! {"$limit": 1}, doc! {"$project": {"_id": 0, "double": {"$multiply": ["$n", 2]}}}],
            docs,
        );
        assert_eq!(out, vec![doc! {"double": 10}]);
    }

    #[test]
    fn unwind_lookup() {
        let docs = vec![doc! {"_id": 1, "ks": [1, 2]}, doc! {"_id": 2, "ks": []}];
        let out = run(vec![doc! {"$unwind": "$ks"}], docs.clone());
        assert_eq!(out, vec![doc! {"_id": 1, "ks": 1}, doc! {"_id": 1, "ks": 2}]);
        let out = run(vec![doc! {"$unwind": {"path": "$ks", "preserveNullAndEmptyArrays": true, "includeArrayIndex": "i"}}], docs.clone());
        assert_eq!(out.len(), 3);
        assert_eq!(out[2], doc! {"_id": 2, "i": Bson::Null});
        let out = run(
            vec![
                doc! {"$lookup": {"from": "other", "localField": "ks", "foreignField": "k", "as": "m"}},
                doc! {"$project": {"n": {"$size": "$m"}}},
            ],
            docs,
        );
        assert_eq!(out, vec![doc! {"_id": 1, "n": 3}, doc! {"_id": 2, "n": 0}]);
    }

    #[test]
    fn lookup_pipeline_with_let() {
        let docs = vec![doc! {"_id": 1, "want": 2}];
        let out = run(
            vec![
                doc! {"$lookup": {"from": "other", "let": {"w": "$want"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$k", "$$w"]}}}], "as": "m"}},
            ],
            docs,
        );
        assert_eq!(out[0].get_array("m").unwrap().len(), 1);
    }

    #[test]
    fn sort_arrays_and_missing() {
        let docs = vec![doc! {"_id": 1, "a": [5, 1]}, doc! {"_id": 2, "a": 3}, doc! {"_id": 3}];
        let ids = |v: Vec<Document>| v.iter().map(|d| d.get_i32("_id").unwrap()).collect::<Vec<_>>();
        assert_eq!(ids(run(vec![doc! {"$sort": {"a": 1}}], docs.clone())), vec![3, 1, 2]);
        assert_eq!(ids(run(vec![doc! {"$sort": {"a": -1}}], docs)), vec![1, 2, 3]);
    }
}
