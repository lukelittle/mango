//! Projections for `find` (and `$project` in aggregation).

use crate::bsonutil::{as_i64, is_number, truthy};
use crate::error::{Error, Result};
use crate::expr::{Expr, Vars};
use crate::query::Matcher;
use bson::{Bson, Document};

#[derive(Debug, Clone)]
enum Kind {
    Include,
    Exclude,
    Slice(i64, Option<i64>),
    ElemMatch(Matcher),
    Positional,
    Computed(Expr),
}

#[derive(Debug, Clone)]
pub struct Projection {
    inclusion: bool,
    include_id: bool,
    entries: Vec<(Vec<String>, Kind)>,
}

impl Projection {
    /// `find_mode` enables `$slice`, `$elemMatch` and positional `.$`
    /// projection operators; otherwise (aggregation `$project`) operator
    /// documents are plain expressions.
    pub fn parse(spec: &Document, find_mode: bool) -> Result<Projection> {
        let mut flat = Vec::new();
        flatten(spec, "", find_mode, &mut flat)?;
        let mut include_id = true;
        let mut inclusion: Option<bool> = None;
        let mut entries = Vec::new();
        let mut set_mode = |incl: bool, path: &str| -> Result<()> {
            match inclusion {
                Some(m) if m != incl => Err(Error::location(format!(
                    "Cannot do {} on field {path} in {} projection",
                    if incl { "inclusion" } else { "exclusion" },
                    if m { "inclusion" } else { "exclusion" }
                ))
                .with_code(31254, "Location31254")),
                _ => {
                    inclusion = Some(incl);
                    Ok(())
                }
            }
        };
        for (path, v) in flat {
            let kind = match &v {
                Bson::Boolean(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                    if truthy(&v) { Kind::Include } else { Kind::Exclude }
                }
                Bson::Document(d) if find_mode && d.len() == 1 && d.contains_key("$slice") => {
                    let arg = d.get("$slice").unwrap();
                    match arg {
                        v if is_number(v) => {
                            let n = as_i64(v).ok_or_else(|| Error::bad_value("$slice limit must be an integer"))?;
                            if n >= 0 { Kind::Slice(0, Some(n)) } else { Kind::Slice(n, None) }
                        }
                        Bson::Array(a) if a.len() == 2 => {
                            let skip = as_i64(&a[0]).ok_or_else(|| Error::bad_value("$slice skip must be an integer"))?;
                            let limit = as_i64(&a[1]).ok_or_else(|| Error::bad_value("$slice limit must be an integer"))?;
                            if limit <= 0 {
                                return Err(Error::bad_value("$slice limit must be positive"));
                            }
                            Kind::Slice(skip, Some(limit))
                        }
                        _ => return Err(Error::bad_value("$slice only supports numbers and [skip, limit] arrays")),
                    }
                }
                Bson::Document(d) if find_mode && d.len() == 1 && d.contains_key("$elemMatch") => {
                    let Some(Bson::Document(cond)) = d.get("$elemMatch") else {
                        return Err(Error::bad_value("elemMatch: Invalid argument, object required."));
                    };
                    if path.contains('.') {
                        return Err(Error::bad_value("Cannot use $elemMatch projection on a nested field."));
                    }
                    Kind::ElemMatch(Matcher::parse(cond)?)
                }
                other => Kind::Computed(Expr::parse(other)?),
            };
            let path = if find_mode && path.ends_with(".$") {
                let base = path[..path.len() - 2].to_string();
                if base.contains(".$") {
                    return Err(Error::bad_value("Positional projection may only contain a single '$'"));
                }
                entries.push((base.split('.').map(String::from).collect(), Kind::Positional));
                set_mode(true, &base)?;
                continue;
            } else {
                path
            };
            if path == "_id" {
                match kind {
                    Kind::Include => {
                        include_id = true;
                        continue;
                    }
                    Kind::Exclude => {
                        include_id = false;
                        continue;
                    }
                    _ => {}
                }
            }
            match &kind {
                Kind::Include | Kind::Computed(_) | Kind::ElemMatch(_) => set_mode(true, &path)?,
                Kind::Exclude => set_mode(false, &path)?,
                _ => {}
            }
            entries.push((path.split('.').map(String::from).collect(), kind));
        }
        let parsed = Projection { inclusion: inclusion.unwrap_or(false), include_id, entries };
        if !find_mode && parsed.entries.is_empty() && parsed.include_id {
            return Err(Error::invalid_options("Invalid $project :: caused by :: projection specification must have at least one field"));
        }
        Ok(parsed)
    }

    pub fn apply(&self, doc: &Document, query: Option<&Matcher>) -> Result<Document> {
        let mut out = if self.inclusion {
            let tree = self.include_tree();
            let mut out = Document::new();
            if self.include_id
                && let Some(id) = doc.get("_id")
            {
                out.insert("_id", id.clone());
            }
            project_include(doc, &tree, &mut out, true);
            out
        } else {
            let mut out = doc.clone();
            if !self.include_id {
                out.remove("_id");
            }
            for (parts, kind) in &self.entries {
                if matches!(kind, Kind::Exclude) {
                    exclude_path(&mut out, parts);
                }
            }
            out
        };
        // Operators that transform what is already selected.
        let vars = Vars::root(doc);
        for (parts, kind) in &self.entries {
            match kind {
                Kind::Computed(e) => {
                    let v = e.eval_opt(&vars)?;
                    match v {
                        Some(v) => set_path_agg(&mut out, parts, v),
                        None => exclude_path(&mut out, parts),
                    }
                }
                Kind::Slice(skip, limit) => {
                    if let Some(Bson::Array(a)) = crate::update::get_at(&out, parts).cloned() {
                        let len = a.len() as i64;
                        let start = if *skip < 0 { (len + skip).max(0) } else { (*skip).min(len) } as usize;
                        let end = match limit {
                            Some(l) => (start + *l as usize).min(a.len()),
                            None => a.len(),
                        };
                        let sliced = a[start..end].to_vec();
                        let _ = crate::update::set_at(&mut out, parts, Bson::Array(sliced));
                    }
                }
                Kind::ElemMatch(m) => {
                    let field = &parts[0];
                    out.remove(field);
                    if let Some(Bson::Array(a)) = doc.get(field) {
                        for e in a {
                            if let Bson::Document(ed) = e
                                && m.matches(ed)?
                            {
                                out.insert(field.clone(), Bson::Array(vec![e.clone()]));
                                break;
                            }
                        }
                    }
                }
                Kind::Positional => {
                    let path = parts.join(".");
                    let idx = match query {
                        Some(q) => q.positional_index(doc, &path)?,
                        None => None,
                    };
                    let Some(idx) = idx else {
                        return Err(Error::bad_value(
                            "Executor error during find command :: caused by :: positional operator '.$' couldn't find a matching element in the array",
                        )
                        .with_code(51246, "Location51246"));
                    };
                    if let Some(Bson::Array(a)) = crate::update::get_at(doc, parts) {
                        let _ = crate::update::set_at(&mut out, parts, Bson::Array(vec![a[idx].clone()]));
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn include_tree(&self) -> Tree {
        let mut root = Tree::default();
        for (parts, kind) in &self.entries {
            if matches!(kind, Kind::Include | Kind::Slice(..) | Kind::Positional) {
                root.insert(parts);
            }
        }
        root
    }
}

impl Error {
    pub fn with_code(mut self, code: i32, name: &'static str) -> Error {
        self.code = code;
        self.code_name = name;
        self
    }
}

fn flatten(spec: &Document, prefix: &str, find_mode: bool, out: &mut Vec<(String, Bson)>) -> Result<()> {
    for (k, v) in spec {
        if k.starts_with('$') && !(find_mode && k == "$") {
            return Err(Error::bad_value(format!("FieldPath field names may not start with '$'. Given: {k}")));
        }
        let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        match v {
            Bson::Document(d) if !d.is_empty() && !d.keys().next().unwrap().starts_with('$') => {
                flatten(d, &path, find_mode, out)?
            }
            Bson::Document(d) if d.is_empty() => {
                return Err(Error::bad_value(format!("An empty sub-projection is not a valid value. Found empty object at path {path}")));
            }
            _ => out.push((path, v.clone())),
        }
    }
    Ok(())
}

#[derive(Default, Debug)]
struct Tree {
    leaf: bool,
    children: Vec<(String, Tree)>,
}

impl Tree {
    fn insert(&mut self, parts: &[String]) {
        if parts.is_empty() {
            self.leaf = true;
            return;
        }
        let child = match self.children.iter_mut().position(|(k, _)| *k == parts[0]) {
            Some(i) => &mut self.children[i].1,
            None => {
                self.children.push((parts[0].clone(), Tree::default()));
                &mut self.children.last_mut().unwrap().1
            }
        };
        child.insert(&parts[1..]);
    }

    fn child(&self, k: &str) -> Option<&Tree> {
        self.children.iter().find(|(n, _)| n == k).map(|(_, t)| t)
    }
}

fn project_include(doc: &Document, tree: &Tree, out: &mut Document, top: bool) {
    for (k, v) in doc {
        if top && k == "_id" {
            continue;
        }
        let Some(sub) = tree.child(k) else { continue };
        if sub.leaf {
            out.insert(k.clone(), v.clone());
            continue;
        }
        match v {
            Bson::Document(d) => {
                let mut o = Document::new();
                project_include(d, sub, &mut o, false);
                out.insert(k.clone(), Bson::Document(o));
            }
            Bson::Array(a) => {
                out.insert(k.clone(), Bson::Array(project_include_array(a, sub)));
            }
            _ => {}
        }
    }
}

fn project_include_array(a: &[Bson], tree: &Tree) -> Vec<Bson> {
    let mut out = Vec::new();
    for e in a {
        match e {
            Bson::Document(d) => {
                let mut o = Document::new();
                project_include(d, tree, &mut o, false);
                out.push(Bson::Document(o));
            }
            Bson::Array(inner) => out.push(Bson::Array(project_include_array(inner, tree))),
            _ => {}
        }
    }
    out
}

pub fn exclude_path(doc: &mut Document, parts: &[String]) {
    if parts.len() == 1 {
        doc.remove(&parts[0]);
        return;
    }
    match doc.get_mut(&parts[0]) {
        Some(Bson::Document(d)) => exclude_path(d, &parts[1..]),
        Some(Bson::Array(a)) => exclude_in_array(a, &parts[1..]),
        _ => {}
    }
}

fn exclude_in_array(a: &mut [Bson], parts: &[String]) {
    for e in a.iter_mut() {
        match e {
            Bson::Document(d) => exclude_path(d, parts),
            Bson::Array(inner) => exclude_in_array(inner, parts),
            _ => {}
        }
    }
}

/// Sets a (possibly dotted) field the way `$addFields`/`$project` do: creates
/// sub-documents and, when an intermediate value is an array, sets the field
/// in each element document.
pub fn set_path_agg(doc: &mut Document, parts: &[String], value: Bson) {
    if parts.len() == 1 {
        doc.insert(parts[0].clone(), value);
        return;
    }
    match doc.get_mut(&parts[0]) {
        Some(Bson::Document(d)) => set_path_agg(d, &parts[1..], value),
        Some(Bson::Array(a)) => {
            for e in a.iter_mut() {
                if let Bson::Document(d) = e {
                    set_path_agg(d, &parts[1..], value.clone());
                }
            }
        }
        _ => {
            let mut d = Document::new();
            set_path_agg(&mut d, &parts[1..], value);
            doc.insert(parts[0].clone(), Bson::Document(d));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn p(spec: Document, d: Document) -> Document {
        Projection::parse(&spec, true).unwrap().apply(&d, None).unwrap()
    }

    #[test]
    fn inclusion_exclusion() {
        let d = doc! {"_id": 1, "a": 1, "b": {"c": 1, "d": 2}, "arr": [{"x": 1, "y": 2}, 5]};
        assert_eq!(p(doc! {"a": 1}, d.clone()), doc! {"_id": 1, "a": 1});
        assert_eq!(p(doc! {"a": 1, "_id": 0}, d.clone()), doc! {"a": 1});
        assert_eq!(p(doc! {"b.c": 1}, d.clone()), doc! {"_id": 1, "b": {"c": 1}});
        assert_eq!(p(doc! {"arr.x": 1}, d.clone()), doc! {"_id": 1, "arr": [{"x": 1}]});
        assert_eq!(p(doc! {"b": 0, "arr": 0}, d.clone()), doc! {"_id": 1, "a": 1});
        assert_eq!(p(doc! {"b.c": 0, "arr": 0, "_id": 0}, d.clone()), doc! {"a": 1, "b": {"d": 2}});
        assert!(Projection::parse(&doc! {"a": 1, "b": 0}, true).is_err());
    }

    #[test]
    fn operators() {
        let d = doc! {"_id": 1, "arr": [1, 2, 3, 4], "o": [{"k": 1}, {"k": 2}]};
        assert_eq!(p(doc! {"arr": {"$slice": 2}}, d.clone()), doc! {"_id": 1, "arr": [1, 2], "o": [{"k": 1}, {"k": 2}]});
        assert_eq!(p(doc! {"arr": {"$slice": -1}, "o": 0}, d.clone()), doc! {"_id": 1, "arr": [4]});
        assert_eq!(p(doc! {"arr": {"$slice": [1, 2]}, "_id": 0, "o": 0}, d.clone()), doc! {"arr": [2, 3]});
        assert_eq!(p(doc! {"o": {"$elemMatch": {"k": 2}}}, d.clone()), doc! {"_id": 1, "o": [{"k": 2}]});
        assert_eq!(p(doc! {"n": {"$size": "$arr"}}, d.clone()), doc! {"_id": 1, "n": 4});
        let q = Matcher::parse(&doc! {"o.k": 2}).unwrap();
        let out = Projection::parse(&doc! {"o.$": 1}, true).unwrap().apply(&d, Some(&q)).unwrap();
        assert_eq!(out, doc! {"_id": 1, "o": [{"k": 2}]});
    }
}
