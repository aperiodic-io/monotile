//! Expressions over columns (the historical executor, ADR-0016): compiled from the same SQL as
//! `Compiler` compiles, with the same names and aliases, into a `Program` of nodes evaluated a
//! batch at a time. Each node's result is the column of what its closure gives row by row: a
//! typed loop where the operands are of one common kind, else the row semantics value by value
//! (`arith`, `compare`, the scalar functions themselves). A node it has no columnar form for is
//! the `Compiler`'s closure, run over each row. Equal subexpressions (an alias used twice) are one
//! node, evaluated once per batch.
use super::*;
use crate::column::{any_true, Batch, Buf, Col, Data};
use std::collections::HashMap;

/// A node of a program: what it computes from the nodes before it.
#[derive(Clone)]
enum Node {
    Col(usize),
    Const(Value),
    Neg(usize),
    /// SQL NOT, AND, OR: three-valued, on truths.
    Not(usize),
    And(usize, usize),
    Or(usize, usize),
    Arith(B, usize, usize),
    Cmp(B, usize, usize),
    /// `IS NULL` (`false`: `IS NOT NULL`).
    IsNull(usize, bool),
    /// `x [NOT] IN (constants)`.
    In(usize, Vec<Value>, bool),
    /// `if(cond, then, else)`, CASE WHEN ... (the first arm whose condition is true).
    Case(Vec<(usize, usize)>, Option<usize>),
    NullIf(usize, usize),
    Coalesce(Vec<usize>),
    /// A one-argument scalar function: its name and its closure over a row of that one value.
    Fn1(String, usize, Ex),
    Tuple(Vec<usize>),
    /// Anything else: its closure over the whole row.
    Row(Ex),
}

/// Compiled expressions over a batch of one scope.
#[derive(Clone, Default)]
pub(crate) struct Program {
    nodes: Vec<Node>,
    /// Each node's text, for sharing equal ones.
    keys: HashMap<String, usize>,
    /// The columns of the scope its nodes read, by position (the whole row: a `Row` node).
    pub(crate) reads: Vec<bool>,
}

/// One batch's evaluation: each node's column once computed, and the rows `Row` nodes read.
pub(crate) struct Eval<'b> {
    batch: &'b Batch,
    memo: Vec<Option<Arc<Col>>>,
    rows: Option<Vec<Vec<Value>>>,
}

impl Program {
    pub(crate) fn new(width: usize) -> Program {
        Program { reads: vec![false; width], ..Program::default() }
    }

    fn add(&mut self, key: String, node: Node) -> usize {
        if let Some(&i) = self.keys.get(&key) {
            return i;
        }
        match &node {
            Node::Col(i) => {
                if *i >= self.reads.len() {
                    self.reads.resize(*i + 1, false);
                }
                self.reads[*i] = true
            }
            Node::Row(_) => self.reads.iter_mut().for_each(|r| *r = true),
            _ => {}
        }
        self.nodes.push(node);
        self.keys.insert(key, self.nodes.len() - 1);
        self.nodes.len() - 1
    }

    /// The column of the scope node `i` is, if it is one as it is.
    pub(crate) fn column(&self, i: usize) -> Option<usize> {
        match self.nodes[i] {
            Node::Col(c) => Some(c),
            _ => None,
        }
    }

    /// Each node's text, in node order: programs of the same texts compute the same columns.
    pub(crate) fn signature(&self) -> Vec<&str> {
        let mut by: Vec<(usize, &str)> = self.keys.iter().map(|(k, i)| (*i, k.as_str())).collect();
        by.sort_unstable();
        by.into_iter().map(|(_, k)| k).collect()
    }

    /// The columns of the scope the nodes `roots` read, through the nodes they are made of.
    pub(crate) fn reads_of(&self, roots: impl IntoIterator<Item = usize>) -> Vec<bool> {
        let mut reads = vec![false; self.reads.len()];
        let (mut todo, mut seen) = (roots.into_iter().collect::<Vec<_>>(), vec![false; self.nodes.len()]);
        while let Some(i) = todo.pop() {
            if std::mem::replace(&mut seen[i], true) {
                continue;
            }
            match &self.nodes[i] {
                Node::Col(c) => reads[*c] = true,
                Node::Row(_) => reads.iter_mut().for_each(|r| *r = true),
                Node::Const(_) => {}
                Node::Neg(a) | Node::Not(a) | Node::IsNull(a, _) | Node::In(a, ..) | Node::Fn1(_, a, _) => {
                    todo.push(*a)
                }
                Node::And(a, b) | Node::Or(a, b) | Node::Arith(_, a, b) | Node::Cmp(_, a, b) | Node::NullIf(a, b) => {
                    todo.extend([*a, *b])
                }
                Node::Case(arms, other) => {
                    todo.extend(arms.iter().flat_map(|(c, v)| [*c, *v]));
                    todo.extend(*other);
                }
                Node::Coalesce(args) | Node::Tuple(args) => todo.extend(args),
            }
        }
        reads
    }

    pub(crate) fn eval<'b>(&self, batch: &'b Batch) -> Eval<'b> {
        Eval { batch, memo: vec![None; self.nodes.len()], rows: None }
    }

    /// Node `i`'s column over the batch of `ev`.
    pub(crate) fn col(&self, ev: &mut Eval<'_>, i: usize) -> Arc<Col> {
        if let Some(c) = &ev.memo[i] {
            return c.clone();
        }
        let n = ev.batch.len;
        let c = match &self.nodes[i] {
            Node::Col(j) => ev.batch.cols[*j].clone(),
            Node::Const(v) => Arc::new(Col::new(Data::Const(v.clone(), n))),
            Node::Neg(a) => Arc::new(neg_col(&self.col(ev, *a))),
            Node::Not(a) => {
                let a = self.col(ev, *a);
                match plain_bools(&a) {
                    Some(v) => Arc::new(Col::new(Data::Bool(v.iter().map(|b| !b).collect()))),
                    None => Arc::new(bools(truths(&a).into_iter().map(|t| t.map(|b| !b)))),
                }
            }
            Node::And(a, b) | Node::Or(a, b) => {
                let and = matches!(self.nodes[i], Node::And(..));
                let (x, y) = (self.col(ev, *a), self.col(ev, *b));
                if let (Some(x), Some(y)) = (plain_bools(&x), plain_bools(&y)) {
                    // no NULL: two-valued
                    let v = x.iter().zip(y).map(|(a, b)| if and { *a & *b } else { *a | *b }).collect();
                    ev.memo[i] = Some(Arc::new(Col::new(Data::Bool(v))));
                    return ev.memo[i].clone().expect("set");
                }
                let (x, y) = (truths(&x), truths(&y));
                Arc::new(bools(x.into_iter().zip(y).map(|(l, r)| match (and, l, r) {
                    (true, Some(false), _) | (true, _, Some(false)) => Some(false),
                    (true, Some(true), Some(true)) => Some(true),
                    (false, Some(true), _) | (false, _, Some(true)) => Some(true),
                    (false, Some(false), Some(false)) => Some(false),
                    _ => None,
                })))
            }
            Node::Arith(op, a, b) => Arc::new(arith_col(op, &self.col(ev, *a), &self.col(ev, *b))),
            Node::Cmp(op, a, b) => Arc::new(cmp_col(op, &self.col(ev, *a), &self.col(ev, *b))),
            Node::IsNull(a, null) => {
                let a = self.col(ev, *a);
                let v: Buf<bool> = match mask(&a) {
                    Mask::None => vec![!*null; n].into(),
                    Mask::Rows(m) => m.iter().map(|x| *x == *null).collect(),
                    Mask::Other => (0..n).map(|r| a.is_null(r) == *null).collect(),
                };
                Arc::new(Col::new(Data::Bool(v)))
            }
            Node::In(a, items, negated) => {
                let a = self.col(ev, *a);
                Arc::new(in_col(&a, items, *negated))
            }
            Node::Case(arms, other) => {
                // `if(x IS NULL, NULL, y)`, `if(x IS NOT NULL, y, NULL)`: y, NULL where x is too
                if let ([(_, v)], Some(o), Node::IsNull(x, null)) = (&arms[..], other, &self.nodes[arms[0].0]) {
                    let (gone, kept) = if *null { (*v, *o) } else { (*o, *v) };
                    if matches!(self.nodes[gone], Node::Const(Value::Null)) {
                        let (x, y) = (self.col(ev, *x), self.col(ev, kept));
                        if !matches!(mask(&y), Mask::Other) {
                            let c = Arc::new(Col { data: y.data.clone(), nulls: either(&x, &y) });
                            ev.memo[i] = Some(c.clone());
                            return c;
                        }
                    }
                }
                let conds: Vec<Vec<Option<bool>>> = arms.iter().map(|(c, _)| truths(&self.col(ev, *c))).collect();
                let vals: Vec<Arc<Col>> = arms.iter().map(|(_, v)| self.col(ev, *v)).collect();
                let other = other.map(|o| self.col(ev, o));
                Arc::new(case_col(n, &conds, &vals, other.as_ref()))
            }
            Node::NullIf(a, b) => Arc::new(null_if_col(&self.col(ev, *a), &self.col(ev, *b))),
            Node::Coalesce(args) => {
                let cols: Vec<Arc<Col>> = args.iter().map(|a| self.col(ev, *a)).collect();
                Arc::new(Col::from_values(
                    (0..n)
                        .map(|r| cols.iter().map(|c| c.get(r)).find(|v| !v.is_null()).unwrap_or(Value::Null))
                        .collect(),
                ))
            }
            Node::Fn1(name, a, f) => Arc::new(fn1_col(name, f, &self.col(ev, *a))),
            Node::Tuple(items) => {
                let cols: Vec<Arc<Col>> = items.iter().map(|a| self.col(ev, *a)).collect();
                Arc::new(Col::new(Data::Vals(
                    (0..n).map(|r| Value::Array(cols.iter().map(|c| c.get(r)).collect())).collect(),
                )))
            }
            Node::Row(f) => {
                let rows = ev.rows.get_or_insert_with(|| ev.batch.rows());
                Arc::new(Col::from_values(rows.iter().map(|r| f(r)).collect()))
            }
        };
        ev.memo[i] = Some(c.clone());
        c
    }
}

/// Compiles expressions of one scope into a `Program`, resolving names as `Compiler` does
/// (SELECT aliases ClickHouse-style, an item's own name inside it being the column).
pub(crate) struct VCompiler<'a> {
    pub(crate) scope: &'a Scope,
    pub(crate) aliases: Vec<(String, Expr)>,
    /// A SELECT over a stream with window functions: each call's result is a column after the
    /// scope's (`Lags`).
    pub(crate) windows: Option<&'a mut Lags>,
    expanding: Vec<String>,
}

/// The window functions of a SELECT over a stream, as the historical executor computes them:
/// `lag` calls, each once, its result at column `base + i`. Any other window function is
/// refused (the engine's operators run such a view).
#[derive(Debug, Default)]
pub(crate) struct Lags {
    pub(crate) base: usize,
    /// (call text, argument, offset, default, PARTITION BY)
    pub(crate) calls: Vec<(String, Expr, usize, Value, Vec<Expr>)>,
}

impl Lags {
    fn slot(&mut self, name: &str, params: &[Expr], args: &[Expr], over: &ast::WindowType) -> R<usize> {
        let ast::WindowType::WindowSpec(spec) = over else { return Err("a named window".into()) };
        let plain = name == "lag"
            && params.is_empty()
            && spec.window_name.is_none()
            && spec.order_by.is_empty()
            && spec.window_frame.is_none();
        if !plain {
            return Err(format!("{name} OVER ({spec}) has no columnar form"));
        }
        let literal_of = |e: &Expr| match e {
            Expr::Value(v) => literal(&v.value),
            Expr::UnaryOp { op: U::Minus, expr } => match expr.as_ref() {
                Expr::Value(v) => number(&format!("-{}", v.value)),
                e => Err(format!("lag expects a literal, got -{e}")),
            },
            e => Err(format!("lag expects a literal, got {e}")),
        };
        let (x, offset, default) = match args {
            [x] => (x, 1, Value::Null),
            [x, n] => (x, literal_of(n)?.i64().unwrap_or(0), Value::Null),
            [x, n, d] => (x, literal_of(n)?.i64().unwrap_or(0), literal_of(d)?),
            _ => return Err(format!("lag of {} arguments", args.len())),
        };
        if !(1..=crate::over::MAX_ROWS as i64).contains(&offset) {
            return Err(format!("lag offset {offset}"));
        }
        let key = format!("{name}({}) OVER ({spec})", args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(","));
        if let Some(i) = self.calls.iter().position(|c| c.0 == key) {
            return Ok(self.base + i);
        }
        self.calls.push((key, x.clone(), offset as usize, default, spec.partition_by.clone()));
        Ok(self.base + self.calls.len() - 1)
    }
}

impl<'a> VCompiler<'a> {
    pub(crate) fn new(scope: &'a Scope) -> VCompiler<'a> {
        VCompiler { scope, aliases: vec![], windows: None, expanding: vec![] }
    }

    /// The row compiler at this point: same scope, aliases and names being expanded.
    fn rows(&self) -> Compiler<'_> {
        let mut c = Compiler::new(self.scope);
        c.aliases = self.aliases.clone();
        c.expanding = self.expanding.clone();
        c
    }

    fn key(&self, what: &str, e: &Expr) -> String {
        format!("{what}{e}|{}", self.expanding.join(","))
    }

    /// Compiles a SELECT item named `alias` (`Compiler::compile_item`).
    pub(crate) fn item(&mut self, p: &mut Program, e: &Expr, alias: &str) -> R<usize> {
        self.expanding.push(alias.to_string());
        let r = self.expr(p, e);
        self.expanding.pop();
        r
    }

    /// Compiles `e` as a value (`Compiler::compile`). Errors are the row compiler's, so a view
    /// the engine plans is planned here too.
    pub(crate) fn expr(&mut self, p: &mut Program, e: &Expr) -> R<usize> {
        if self.windows.is_none() {
            self.rows().compile(e)?;
        }
        self.node(p, e)
    }

    /// Compiles `e` as a condition (`Compiler::condition`): its truth is the column's.
    pub(crate) fn condition(&mut self, p: &mut Program, e: &Expr) -> R<usize> {
        if self.windows.is_none() {
            self.rows().condition(e)?;
        }
        self.node(p, e)
    }

    /// The node of `e`, which the row compiler compiles.
    fn node(&mut self, p: &mut Program, e: &Expr) -> R<usize> {
        let row = |me: &mut Self, p: &mut Program| -> R<usize> {
            if me.windows.is_some() {
                return Err(format!("{e}: no columnar form beside window functions"));
            }
            let f = me.rows().compile(e)?;
            Ok(p.add(me.key("row:", e), Node::Row(f)))
        };
        Ok(match e {
            Expr::Identifier(id) => return self.column(p, None, &id.value, e),
            Expr::CompoundIdentifier(ids) if ids.len() == 2 => {
                return self.column(p, Some(&ids[0].value), &ids[1].value, e)
            }
            Expr::Value(v) => p.add(format!("const:{e}"), Node::Const(literal(&v.value)?)),
            Expr::Nested(x) => self.node(p, x)?,
            Expr::UnaryOp { op: U::Minus, expr } => match expr.as_ref() {
                Expr::Value(v) if matches!(v.value, ast::Value::Number(..)) => {
                    p.add(format!("const:{e}"), Node::Const(number(&format!("-{}", v.value))?))
                }
                x => {
                    let a = self.node(p, x)?;
                    p.add(self.key("neg:", e), Node::Neg(a))
                }
            },
            Expr::UnaryOp { op: U::Plus, expr } => self.node(p, expr)?,
            Expr::UnaryOp { op: U::Not, expr } => {
                let a = self.node(p, expr)?;
                p.add(self.key("not:", e), Node::Not(a))
            }
            Expr::BinaryOp { left, op, right } => {
                let (l, r) = (self.rows().class(left), self.rows().class(right));
                // a string literal read as a number or datetime: the row compiler's
                if (l == Some(Class::Str)) != (r == Some(Class::Str)) && l.is_some() && r.is_some() {
                    return row(self, p);
                }
                let (a, b) = (self.node(p, left)?, self.node(p, right)?);
                let node = match op {
                    B::And => Node::And(a, b),
                    B::Or => Node::Or(a, b),
                    B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq => Node::Cmp(op.clone(), a, b),
                    B::Plus | B::Minus | B::Multiply | B::Divide | B::Modulo => Node::Arith(op.clone(), a, b),
                    _ => return row(self, p),
                };
                p.add(self.key("bin:", e), node)
            }
            Expr::IsNull(x) | Expr::IsNotNull(x) => {
                let a = self.node(p, x)?;
                p.add(self.key("isnull:", e), Node::IsNull(a, matches!(e, Expr::IsNull(_))))
            }
            Expr::InList { expr, list, negated } => {
                let items: Option<Vec<Value>> = list
                    .iter()
                    .map(|i| match i {
                        Expr::Value(v) => literal(&v.value).ok(),
                        _ => None,
                    })
                    .collect();
                let Some(items) = items else { return row(self, p) };
                let a = self.node(p, expr)?;
                p.add(self.key("in:", e), Node::In(a, items, *negated))
            }
            Expr::Case { operand: None, conditions, else_result, .. } => {
                let mut arms = vec![];
                for w in conditions {
                    arms.push((self.node(p, &w.condition)?, self.node(p, &w.result)?));
                }
                let other = else_result.as_ref().map(|x| self.node(p, x)).transpose()?;
                p.add(self.key("case:", e), Node::Case(arms, other))
            }
            Expr::Tuple(items) => {
                let items = items.iter().map(|x| self.node(p, x)).collect::<R<Vec<_>>>()?;
                p.add(self.key("tuple:", e), Node::Tuple(items))
            }
            Expr::Function(f) => match self.function(p, f, e)? {
                Some(i) => i,
                None => row(self, p)?,
            },
            _ => row(self, p)?,
        })
    }

    fn column(&mut self, p: &mut Program, qual: Option<&str>, name: &str, e: &Expr) -> R<usize> {
        if qual.is_none() && !self.expanding.iter().any(|n| n == name) {
            if let Some((_, x)) = self.aliases.iter().find(|(a, _)| a == name).cloned() {
                self.expanding.push(name.to_string());
                let r = self.node(p, &x);
                self.expanding.pop();
                return r;
            }
        }
        let i = self.scope.find(qual, name).ok_or_else(|| format!("unknown column {e}"))?;
        Ok(p.add(format!("col:{i}"), Node::Col(i)))
    }

    /// A function with a columnar form; `None` for the others (and aggregates, window calls).
    fn function(&mut self, p: &mut Program, f: &ast::Function, e: &Expr) -> R<Option<usize>> {
        let name = f.name.to_string().to_ascii_lowercase();
        if let (Some(over), Some(w)) = (&f.over, self.windows.as_deref_mut()) {
            let slot = w.slot(&name, &plain_args(&f.parameters)?, &plain_args(&f.args)?, over)?;
            return Ok(Some(p.add(format!("col:{slot}"), Node::Col(slot))));
        }
        let plain = matches!(f.parameters, FunctionArguments::None)
            && f.over.is_none()
            && f.filter.is_none()
            && f.null_treatment.is_none()
            && f.within_group.is_empty();
        let Ok(args) = plain_args(&f.args) else { return Ok(None) };
        if !plain || crate::agg::is_aggregate(&name) {
            return Ok(None);
        }
        Ok(Some(match (name.as_str(), args.as_slice()) {
            ("if", [c, a, b]) => {
                let (c, a, b) = (self.node(p, c)?, self.node(p, a)?, self.node(p, b)?);
                p.add(self.key("if:", e), Node::Case(vec![(c, a)], Some(b)))
            }
            ("null_if", [a, b]) => {
                let (a, b) = (self.node(p, a)?, self.node(p, b)?);
                p.add(self.key("nullif:", e), Node::NullIf(a, b))
            }
            ("coalesce", args) => {
                let args = args.iter().map(|a| self.node(p, a)).collect::<R<Vec<_>>>()?;
                p.add(self.key("coalesce:", e), Node::Coalesce(args))
            }
            (
                "abs"
                | "ln"
                | "sqrt"
                | "to_float32"
                | "to_float64"
                | "to_int32"
                | "to_int64_or_zero"
                | "to_unix_timestamp64_micro"
                | "from_unix_timestamp64_micro"
                | "to_start_of_day"
                | "to_start_of_hour"
                | "to_start_of_minute",
                [x],
            ) => {
                let a = self.node(p, x)?;
                let f = scalar(&name, vec![Arg::Col(0)], |_| None)?;
                p.add(self.key("fn:", e), Node::Fn1(name, a, f))
            }
            _ => return Ok(None),
        }))
    }
}

/// A column of truths from optional booleans.
fn bools(t: impl Iterator<Item = Option<bool>>) -> Col {
    let (mut v, mut nulls, mut any) = (vec![], vec![], false);
    for b in t {
        v.push(b.unwrap_or(false));
        nulls.push(b.is_none());
        any |= b.is_none();
    }
    Col { data: Data::Bool(v.into()), nulls: any.then(|| nulls.into()) }
}

/// The booleans of a column of them without NULLs.
fn plain_bools(c: &Col) -> Option<&[bool]> {
    match (&c.data, &c.nulls) {
        (Data::Bool(v), None) => Some(v),
        _ => None,
    }
}

/// Each row's SQL truth (`truth`).
pub(crate) fn truths(c: &Col) -> Vec<Option<bool>> {
    let n = c.len();
    match (&c.data, &c.nulls) {
        (Data::Bool(v), None) => v.iter().map(|b| Some(*b)).collect(),
        (Data::Bool(v), Some(m)) => v.iter().zip(m).map(|(b, null)| (!null).then_some(*b)).collect(),
        (Data::Const(v, _), _) => vec![truth(v); n],
        _ => {
            rowwise();
            (0..n).map(|r| truth(&c.get(r))).collect()
        }
    }
}

/// A NULL mask: where either operand is NULL.
fn either(a: &Col, b: &Col) -> Option<Buf<bool>> {
    match (mask(a), mask(b)) {
        (Mask::None, Mask::None) => None,
        (Mask::Rows(m), Mask::None) | (Mask::None, Mask::Rows(m)) => Some(m.clone()),
        (Mask::Rows(x), Mask::Rows(y)) => Some(x.iter().zip(y.iter()).map(|(x, y)| *x | *y).collect()),
        _ => {
            rowwise();
            Some((0..a.len()).map(|r| a.is_null(r) || b.is_null(r)).collect())
        }
    }
}

/// Which rows of a column are NULL, where that is known without reading its values.
enum Mask<'a> {
    None,
    Rows(&'a Buf<bool>),
    /// Every row (a NULL constant), or not known (values, rows of two columns).
    Other,
}

fn mask(c: &Col) -> Mask<'_> {
    match (&c.nulls, &c.data) {
        (_, Data::Vals(_) | Data::Choose(..)) => Mask::Other,
        (None, Data::Const(Value::Null, _)) => Mask::Other,
        (None, _) => Mask::None,
        (Some(m), _) => Mask::Rows(m),
    }
}

fn is_null_const(c: &Col) -> bool {
    matches!(&c.data, Data::Const(Value::Null, _))
}

/// Float64 values of a column, NULLs included as placeholders, if it holds only Float64s.
fn f64_of(c: &Col) -> Option<Cow<'_, [f64]>> {
    match &c.data {
        Data::F64(v) => Some(Cow::Borrowed(&v[..])),
        Data::Const(Value::F64(f), n) => Some(Cow::Owned(vec![*f; *n])),
        _ => None,
    }
}

/// The values of a column row by row, the row engine's way, into a column.
fn per_row(n: usize, f: impl FnMut(usize) -> Value) -> Col {
    rowwise();
    Col::from_values((0..n).map(f).collect())
}

#[cfg(test)]
thread_local! {
    static ROWWISE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A row-by-row way taken where a column has no typed loop: the tests hold the common
/// shapes to none.
fn rowwise() {
    #[cfg(test)]
    ROWWISE.with(|c| c.set(c.get() + 1));
}

fn neg_col(a: &Col) -> Col {
    match &a.data {
        Data::F64(v) => Col { data: Data::F64(v.iter().map(|x| -x).collect()), nulls: a.nulls.clone() },
        _ => per_row(a.len(), |r| neg(&a.get(r))),
    }
}

fn is_time(c: &Col) -> bool {
    matches!(c.data, Data::Time(_) | Data::Const(Value::Time(_), _))
}

/// Whether `a op b` is a time: a time plus or minus a number of µs, or a number plus a time.
pub(crate) fn time_result(op: &B, a: bool, b: bool) -> bool {
    matches!((op, a, b), (B::Plus, true, false) | (B::Plus, false, true) | (B::Minus, true, false))
}

/// `arith`, column by column: two Float64 operands (or one and a Float64 constant) in a loop.
fn arith_col(op: &B, a: &Col, b: &Col) -> Col {
    let n = a.len();
    if let (Some(x), Some(y)) = (f64_of(a), f64_of(b)) {
        // a loop per operator: through a function pointer, a call per value and no vector loop
        let xy = x.iter().zip(y.iter());
        let v = match op {
            B::Plus => xy.map(|(x, y)| x + y).collect(),
            B::Minus => xy.map(|(x, y)| x - y).collect(),
            B::Multiply => xy.map(|(x, y)| x * y).collect(),
            B::Divide => xy.map(|(x, y)| x / y).collect(),
            _ => xy.map(|(x, y)| x % y).collect(),
        };
        return Col { data: Data::F64(v), nulls: either(a, b) };
    }
    // a Float64 column with a number constant: arith makes both Float64
    if let (Some(x), Data::Const(c, _)) = (f64_of(a), &b.data) {
        if let (Some(y), true) = (c.f64(), matches!(c, Value::Int(_) | Value::UInt(_))) {
            let c = Col::new(Data::Const(Value::F64(y), n));
            let _ = x;
            return arith_col(op, a, &c);
        }
    }
    if let (Data::Const(c, _), Some(_)) = (&a.data, f64_of(b)) {
        if let (Some(x), true) = (c.f64(), matches!(c, Value::Int(_) | Value::UInt(_))) {
            return arith_col(op, &Col::new(Data::Const(Value::F64(x), n)), b);
        }
    }
    // integers (and times, which arithmetic reads as integers): Int64 results, wrapping
    if let (Some(x), Some(y)) = (int_of(a), int_of(b)) {
        if !(matches!(op, B::Divide) || is_uint(a) && is_uint(b)) {
            let mut nulls = either(a, b).map(|n| n.to_vec());
            let v: Vec<i64> = x
                .iter()
                .zip(y.iter())
                .enumerate()
                .map(|(r, (x, y))| match op {
                    B::Plus => x.wrapping_add(*y),
                    B::Minus => x.wrapping_sub(*y),
                    B::Multiply => x.wrapping_mul(*y),
                    _ if *y == 0 => {
                        // an integer % by zero is NULL
                        nulls.get_or_insert_with(|| vec![false; n])[r] = true;
                        0
                    }
                    _ => x.wrapping_rem(*y),
                })
                .collect();
            let nulls = nulls.filter(|m| any_true(m)).map(Into::into);
            // a time plus or minus µs is a time (`arith`)
            if time_result(op, is_time(a), is_time(b)) {
                return Col { data: Data::Time(v.into()), nulls };
            }
            return Col { data: Data::Int(v.into()), nulls };
        }
    }
    per_row(n, |r| arith(op, &a.get(r), &b.get(r)))
}

/// Integer values of a column of integers or times, or of an integer constant (NULLs as
/// placeholders).
fn int_of(c: &Col) -> Option<Cow<'_, [i64]>> {
    match &c.data {
        Data::Int(v) | Data::Time(v) => Some(Cow::Borrowed(&v[..])),
        // as `Value::i64` reads them
        Data::UInt(v) => Some(Cow::Owned(v.iter().map(|u| *u as i64).collect())),
        Data::Const(Value::Int(i) | Value::Time(i), n) => Some(Cow::Owned(vec![*i; *n])),
        Data::Const(Value::UInt(u), n) => Some(Cow::Owned(vec![*u as i64; *n])),
        _ => None,
    }
}

fn is_uint(c: &Col) -> bool {
    matches!(c.data, Data::UInt(_) | Data::Const(Value::UInt(_), _))
}

/// A comparison, column by column (`cmp`): its truth, NULL where either side is.
fn cmp_col(op: &B, a: &Col, b: &Col) -> Col {
    use std::cmp::Ordering::{self, *};
    let n = a.len();
    let ok: fn(Ordering) -> bool = match op {
        B::Lt => |o| o == Less,
        B::LtEq => |o| o != Greater,
        B::Gt => |o| o == Greater,
        B::GtEq => |o| o != Less,
        B::Eq => |o| o == Equal,
        _ => |o| o != Equal,
    };
    let ne = matches!(op, B::NotEq);
    let nulls = either(a, b);
    let typed = |v: Vec<bool>| Col { data: Data::Bool(v.into()), nulls: nulls.clone() };
    // Float64 with Float64: a NaN compares as nothing (only != holds)
    if let (Some(x), Some(y)) = (f64_of(a), f64_of(b)) {
        return typed(x.iter().zip(y.iter()).map(|(x, y)| x.partial_cmp(y).map_or(ne, ok)).collect());
    }
    // Float64 with a number: `compare` makes it a float comparison
    if let (Some(x), Data::Const(c, _)) = (f64_of(a), &b.data) {
        if let (Some(y), false) = (c.f64(), matches!(c, Value::Bool(_))) {
            return typed(x.iter().map(|x| x.partial_cmp(&y).map_or(ne, ok)).collect());
        }
    }
    match (&a.data, &b.data) {
        (Data::Int(x) | Data::Time(x), Data::Int(y) | Data::Time(y))
            if matches!((&a.data, &b.data), (Data::Int(_), Data::Int(_)) | (Data::Time(_), Data::Time(_))) =>
        {
            typed(x.iter().zip(y).map(|(x, y)| ok(x.cmp(y))).collect())
        }
        (Data::Int(x), Data::Const(Value::Int(c), _)) | (Data::Time(x), Data::Const(Value::Time(c), _)) => {
            typed(x.iter().map(|x| ok(x.cmp(c))).collect())
        }
        // an integer or time with a number of another kind: `compare` compares their floats
        (Data::Int(x) | Data::Time(x), Data::Const(c @ (Value::UInt(_) | Value::F64(_) | Value::F32(_)), _))
        | (Data::Int(x), Data::Const(c @ Value::Time(_), _))
        | (Data::Time(x), Data::Const(c @ Value::Int(_), _)) => {
            let y = c.f64().expect("a number");
            typed(x.iter().map(|x| (*x as f64).partial_cmp(&y).map_or(ne, ok)).collect())
        }
        (Data::Str(s), Data::Const(Value::Str(c), _)) if matches!(op, B::Eq | B::NotEq) => {
            let c = c.as_bytes();
            typed(s.iter_bytes().map(|x| (x == c) != ne).collect())
        }
        // UTF-8 bytes order as the strings do
        (Data::Str(s), Data::Const(Value::Str(c), _)) => {
            typed(s.iter_bytes().map(|x| ok(x.cmp(c.as_bytes()))).collect())
        }
        (Data::Str(x), Data::Str(y)) => typed(x.iter_bytes().zip(y.iter_bytes()).map(|(x, y)| ok(x.cmp(y))).collect()),
        _ => {
            rowwise();
            let eq = matches!(op, B::Eq | B::NotEq);
            bools((0..n).map(|r| {
                let (x, y) = (a.get(r), b.get(r));
                if eq {
                    equal(&x, &y).map(|e| e != ne)
                } else if x.is_null() || y.is_null() {
                    None
                } else {
                    Some(compare(&x, &y).is_some_and(ok))
                }
            }))
        }
    }
}

fn in_col(a: &Col, items: &[Value], negated: bool) -> Col {
    let n = a.len();
    if let (Data::Str(s), true) = (&a.data, items.iter().all(|i| matches!(i, Value::Str(_)))) {
        let texts: Vec<&[u8]> = items.iter().filter_map(Value::str).map(str::as_bytes).collect();
        let v = s.iter_bytes().map(|x| texts.contains(&x) != negated).collect();
        return Col { data: Data::Bool(v), nulls: a.nulls.clone() };
    }
    bools((0..n).map(|r| {
        let v = a.get(r);
        (!v.is_null()).then(|| items.iter().any(|i| equal(&v, i) == Some(true)) != negated)
    }))
}

/// The first arm whose condition is true, else `other` (NULL without one).
fn case_col(n: usize, conds: &[Vec<Option<bool>>], vals: &[Arc<Col>], other: Option<&Arc<Col>>) -> Col {
    let ([v], Some(o)) = (vals, other) else {
        let pick = |r: usize| conds.iter().position(|c| c[r] == Some(true));
        return per_row(n, |r| match pick(r) {
            Some(i) => vals[i].get(r),
            None => other.map_or(Value::Null, |o| o.get(r)),
        });
    };
    let picks: Vec<bool> = conds[0].iter().map(|c| *c == Some(true)).collect();
    // both Float64: a select
    if let (Some(x), Some(y)) = (f64_of(v), f64_of(o)) {
        let mut nulls = vec![false; n];
        let data = (0..n)
            .map(|r| {
                let (src, val) = if picks[r] { (&**v, x[r]) } else { (&**o, y[r]) };
                nulls[r] = src.is_null(r);
                val
            })
            .collect::<Vec<f64>>();
        let any = any_true(&nulls);
        return Col { data: Data::F64(data.into()), nulls: any.then(|| nulls.into()) };
    }
    // one branch NULL: the other's values, NULL where it is not taken
    let with_nulls = |c: &Col, taken: bool| -> Option<Col> {
        let nulls: Buf<bool> = match mask(c) {
            Mask::None => picks.iter().map(|p| *p != taken).collect(),
            Mask::Rows(m) => picks.iter().zip(m.iter()).map(|(p, m)| *p != taken || *m).collect(),
            Mask::Other => return None,
        };
        Some(Col { data: c.data.clone(), nulls: Some(nulls) })
    };
    if is_null_const(o) {
        if let Some(c) = with_nulls(v, true) {
            return c;
        }
    }
    if is_null_const(v) {
        if let Some(c) = with_nulls(o, false) {
            return c;
        }
    }
    // otherwise each row from its branch, as it is
    Col::new(Data::Choose(picks.into(), v.clone(), o.clone()))
}

/// `null_if(a, b)`: NULL where `a` equals `b` (`compare`), else `a`.
fn null_if_col(a: &Col, b: &Col) -> Col {
    let n = a.len();
    if let (Data::F64(x), Data::Const(c, _)) = (&a.data, &b.data) {
        if let (Some(y), false) = (c.f64(), matches!(c, Value::Bool(_))) {
            let nulls: Buf<bool> = match &a.nulls {
                None => x.iter().map(|x| *x == y).collect(),
                Some(m) => x.iter().zip(m.iter()).map(|(x, m)| *m || *x == y).collect(),
            };
            let any = any_true(&nulls);
            return Col { data: Data::F64(x.clone()), nulls: any.then_some(nulls) };
        }
    }
    per_row(n, |r| {
        let v = a.get(r);
        if compare(&v, &b.get(r)) == Some(std::cmp::Ordering::Equal) {
            Value::Null
        } else {
            v
        }
    })
}

/// A one-argument scalar function (`scalar`), with typed loops for the common ones.
fn fn1_col(name: &str, f: &Ex, a: &Col) -> Col {
    let nulls = a.nulls.clone();
    let typed = |data: Data| Col { data, nulls: nulls.clone() };
    match (name, &a.data) {
        ("from_unix_timestamp64_micro", Data::Int(v) | Data::Time(v)) => typed(Data::Time(v.clone())),
        ("to_unix_timestamp64_micro", Data::Int(v) | Data::Time(v)) => typed(Data::Int(v.clone())),
        ("to_start_of_day" | "to_start_of_hour" | "to_start_of_minute", Data::Int(v) | Data::Time(v)) => {
            let w = match name {
                "to_start_of_day" => 86_400_000_000,
                "to_start_of_hour" => 3_600_000_000,
                _ => 60_000_000,
            };
            typed(Data::Time(v.iter().map(|t| super::start_of(*t, w)).collect()))
        }
        ("abs", Data::F64(v)) => typed(Data::F64(v.iter().map(|x| x.abs()).collect())),
        ("ln", Data::F64(v)) => typed(Data::F64(v.iter().map(|x| x.ln()).collect())),
        ("sqrt", Data::F64(v)) => typed(Data::F64(v.iter().map(|x| x.sqrt()).collect())),
        ("to_float64", Data::F64(v)) => typed(Data::F64(v.clone())),
        // text, as the archive keeps prices: each parsed as `cast` parses it, without a copy
        ("to_float64", Data::Str(s)) if a.nulls.is_none() => {
            Col::from_values(s.iter().map(|x| crate::value::parse_str(x, &Type::F64)).collect())
        }
        ("to_float32", Data::F64(v)) => typed(Data::F32(v.iter().map(|x| *x as f32).collect())),
        ("to_int64_or_zero", Data::Int(v)) => typed(Data::Int(v.clone())),
        ("to_int64_or_zero", Data::Str(s)) if a.nulls.is_none() => {
            Col::new(Data::Int(s.iter().map(|x| x.trim().parse().unwrap_or(0)).collect()))
        }
        _ => per_row(a.len(), |r| f(&[a.get(r)])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn scope() -> Scope {
        let mut s = Scope::new(["i", "f", "s", "t", "u", "g", "n"].map(String::from));
        s.types = vec![
            Some(Type::Int(64)),
            Some(Type::F64),
            Some(Type::Str),
            Some(Type::Time(6)),
            Some(Type::UInt(64)),
            Some(Type::Nullable(Box::new(Type::F64))),
            None,
        ];
        s
    }

    /// Every expression the row compiler gives, column by column: the same values.
    fn check(exprs: &[&str], rows: &[Vec<Value>]) {
        let scope = scope();
        let batch = Batch::from_rows(rows, 7);
        for text in exprs {
            let e = crate::sql::parse_expr(text).unwrap();
            let row = Compiler::new(&scope).compile(&e).unwrap();
            let mut p = Program::new(7);
            let i = VCompiler::new(&scope).expr(&mut p, &e).unwrap();
            let col = p.col(&mut p.eval(&batch), i);
            for (r, row_values) in rows.iter().enumerate() {
                let (want, got) = (row(row_values), col.get(r));
                assert!(same(&want, &got), "{text} on row {r} {row_values:?}: row {want:?}, column {got:?}");
            }
        }
    }

    /// The same value: floats bit for bit, NaNs alike.
    fn same(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()),
            (Value::F32(a), Value::F32(b)) => a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()),
            (Value::Array(a), Value::Array(b)) => a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| same(a, b)),
            (a, b) => a == b,
        }
    }

    const EXPRS: &[&str] = &[
        "f + 1",
        "f - g",
        "f * 2.5",
        "f / g",
        "f % 3",
        "i + 1",
        "i % 60000000",
        "i < 100",
        "i >= 2.5",
        "t = 7",
        "u - 3",
        "t - t % 60000000 = t - t % 60000000",
        "i - i % 7 = t - t % 7",
        "if(t - t % 5 = i - i % 5, f * f, 0)",
        "if(s = 'buy', NULL, f)",
        "if(f > 0, f, NULL)",
        "i - u",
        "i * i",
        "i / 2",
        "i % 0",
        "u + u",
        "-f",
        "-i",
        "f > 0",
        "f <= g",
        "g = 0",
        "f != f",
        "i >= 2",
        "t < t",
        "s = 'buy'",
        "s != 'sell'",
        "s < 'b'",
        "s IN ('buy', 'sell')",
        "s NOT IN ('buy')",
        "i IN (1, 2)",
        "g IS NULL",
        "f IS NOT NULL",
        "f > 0 AND g > 0",
        "f > 0 OR g < 0",
        "NOT (g > 0)",
        "if(s = 'buy', f, 0)",
        "if(g IS NULL, NULL, t)",
        "if(g IS NOT NULL, f, NULL)",
        "if(null_if(f, 0) IS NULL, NULL, g)",
        "if(g IS NULL, NULL, if(f > 0, f, 0))",
        "if(g IS NULL, f, NULL)",
        "if(f > 0, f, g)",
        "if(f > 0, i, f)",
        "CASE WHEN f > 1 THEN 'a' WHEN g > 1 THEN 'b' END",
        "null_if(f, 0)",
        "null_if(g, 0)",
        "null_if(i, 0)",
        "null_if(s, 'buy')",
        "coalesce(g, f, 0)",
        "abs(f - g)",
        "abs(i)",
        "abs(g)",
        "ln(f)",
        "sqrt(g)",
        "to_float32(f)",
        "to_float32(i)",
        "to_float64(g)",
        "to_int32(f)",
        "to_int64_or_zero(s)",
        "to_int64_or_zero(i)",
        "from_unix_timestamp64_micro(i)",
        "to_unix_timestamp64_micro(t)",
        "to_start_of_day(t)",
        "to_start_of_hour(from_unix_timestamp64_micro(i))",
        "(f, s, i)",
        "f / null_if(g + f, 0) * 10000",
        "format_datetime(t, '%Y-%m-%d')",
        "concat(s, '|', to_string(i))",
        "n + 1",
        "n = 'x'",
        "t >= '2026-01-01 00:00:00'",
    ];

    #[test]
    fn columns_are_what_rows_give() {
        let row = |i: i64, f: f64, s: &str, t: i64, u: u64, g: Option<f64>, n: Value| {
            vec![
                Value::Int(i),
                Value::F64(f),
                Value::Str(s.into()),
                Value::Time(t),
                Value::UInt(u),
                g.map_or(Value::Null, Value::F64),
                n,
            ]
        };
        let rows = vec![
            row(1, 1.5, "buy", 1_789_946_100_000_000, 3, Some(2.0), Value::Int(1)),
            row(-2, 0.0, "sell", 0, 0, None, Value::Str("x".into())),
            row(i64::MAX, f64::NAN, "", -1, u64::MAX, Some(0.0), Value::Null),
            row(0, -0.0, "10", 7, 1, Some(f64::INFINITY), Value::F64(2.5)),
        ];
        check(EXPRS, &rows);
    }

    /// The pipelines' shapes of expression compile to typed nodes, none to the row compiler's
    /// closure (`Node::Row`), whose values would be the same.
    #[test]
    fn common_shapes_compile_to_typed_nodes() {
        let scope = scope();
        for text in [
            "-f",
            "+f",
            "NOT (f > 1)",
            "(f + 1) * 2",
            "f > 0 AND (g IS NULL OR f < g)",
            "if(s = 'buy', f, 0)",
            "CASE WHEN f > 0 THEN f WHEN f < 0 THEN -f ELSE 0 END",
            "if(g IS NULL, NULL, t)",
            "null_if(f, 0)",
            "coalesce(g, f)",
            "s IN ('buy', 'sell')",
            "abs(f) / null_if(g + f, 0) * 10000",
            "to_unix_timestamp64_micro(t)",
            "(t, s, f)",
            "1.5",
            // a join side's column, by its qualifier
            "x.f + 1",
        ] {
            let mut p = Program::new(7);
            let mut scope = scope.clone();
            scope.cols[1].0 = Some("x".into());
            VCompiler::new(&scope).expr(&mut p, &crate::sql::parse_expr(text).unwrap()).unwrap();
            assert!(!p.nodes.iter().any(|n| matches!(n, Node::Row(_))), "{text}");
        }
    }

    /// The common shapes over typed columns run as typed loops: the row compiler's values,
    /// none of them row by row.
    #[test]
    fn common_shapes_run_as_typed_loops() {
        let shapes = [
            "from_unix_timestamp64_micro(i)",
            "to_unix_timestamp64_micro(t)",
            "to_start_of_day(t)",
            "to_start_of_hour(t)",
            "to_start_of_minute(i)",
            "abs(f)",
            "ln(f)",
            "sqrt(f)",
            "to_float64(f)",
            "to_float64(s)",
            "to_float32(f)",
            "to_int64_or_zero(i)",
            "to_int64_or_zero(s)",
            "-f",
            "f > g",
            "f > 1",
            "i = i",
            "t >= t",
            "i = 5",
            "i = -5",
            "i > 2.5",
            "t > 5",
            "s = '12'",
            "s < '5'",
            "s < s",
            "i + 1",
            "i + -1",
            "t - 1",
            "i + u",
            "f * g",
            "f * 2.5",
            "2 * f",
            "NOT (f > 1)",
            "(f > 1) AND (g > 1)",
            "(g > 1) OR (g < 0)",
            "NOT n",
            // a branch of NULLs: the other branch's values, NULL where it is not taken
            "if(f > 1, f, NULL) * 2",
            "if(f > 1, NULL, f) * 2",
            "g > g",
        ];
        let rows: Vec<Vec<Value>> = (0..40i64)
            .map(|k| {
                vec![
                    Value::Int(k * 37 % 11),
                    Value::F64(k as f64 * 0.75 + 0.5),
                    Value::Str(format!("{}", k % 9).into()),
                    Value::Time(1_789_000_000_000_000 + k * 7_919_000),
                    Value::UInt(k as u64 * 3),
                    if k % 4 == 0 { Value::Null } else { Value::F64(k as f64 - 20.0) },
                    Value::Null,
                ]
            })
            .collect();
        check(&shapes, &rows);
        let (scope, batch) = (scope(), Batch::from_rows(&rows, 7));
        for text in shapes {
            let mut p = Program::new(7);
            let i = VCompiler::new(&scope).expr(&mut p, &crate::sql::parse_expr(text).unwrap()).unwrap();
            let before = ROWWISE.with(std::cell::Cell::get);
            p.col(&mut p.eval(&batch), i);
            assert_eq!(ROWWISE.with(std::cell::Cell::get), before, "{text} ran row by row");
        }
        // and what has no typed loop is counted: UInt arithmetic, row by row
        let mut p = Program::new(7);
        let i = VCompiler::new(&scope).expr(&mut p, &crate::sql::parse_expr("u * 3").unwrap()).unwrap();
        let before = ROWWISE.with(std::cell::Cell::get);
        p.col(&mut p.eval(&batch), i);
        assert_eq!(ROWWISE.with(std::cell::Cell::get), before + 1);
        // an integer with a time compares as their floats do, past 2^53 too
        let mut big = rows.clone();
        (big[0][0], big[0][3]) = (Value::Int((1 << 53) + 1), Value::Time(1 << 53));
        check(&["i < t", "i > t", "i = t"], &big);
        // a comparison of no typed loop: NULL where either side is
        check(&["i < g", "g >= i", "i = g"], &rows);
        // text parsed with its NULLs kept
        let mut nulled = rows.clone();
        nulled[3][2] = Value::Null;
        check(&["to_float64(s)", "to_int64_or_zero(s)"], &nulled);
    }

    /// A node of the row compiler's (no columnar form) reads every column of the scope.
    #[test]
    fn a_row_compiled_node_reads_every_column() {
        let scope = scope();
        let mut p = Program::new(7);
        VCompiler::new(&scope).expr(&mut p, &crate::sql::parse_expr("format_datetime(t, '%Y')").unwrap()).unwrap();
        assert!(p.nodes.iter().any(|n| matches!(n, Node::Row(_))));
        assert_eq!(p.reads, [true; 7]);
    }

    /// An alias is its expression, in which its own name is the column (`f * 2 AS f`).
    #[test]
    fn an_alias_of_a_columns_name_reads_the_column_in_itself() {
        let scope = scope();
        let mut c = VCompiler::new(&scope);
        c.aliases = vec![("f".into(), crate::sql::parse_expr("f * 2").unwrap())];
        let mut p = Program::new(7);
        let i = c.expr(&mut p, &crate::sql::parse_expr("f + 1").unwrap()).unwrap();
        let rows = vec![vec![
            Value::Int(0),
            Value::F64(1.5),
            Value::Str("".into()),
            Value::Time(0),
            Value::UInt(0),
            Value::Null,
            Value::Null,
        ]];
        assert_eq!(p.col(&mut p.eval(&Batch::from_rows(&rows, 7)), i).get(0), Value::F64(4.0));
    }

    /// `lag` alone, of a literal offset and default, has a columnar form; other window calls
    /// and `lag`s ordered, framed or named are the row engine's.
    #[test]
    fn only_a_plain_lag_has_a_columnar_form() {
        let scope = scope();
        let compile = |text: &str| -> R<usize> {
            let mut lags = Lags { base: 7, calls: vec![] };
            let mut c = VCompiler::new(&scope);
            c.windows = Some(&mut lags);
            c.expr(&mut Program::new(7), &crate::sql::parse_expr(text).unwrap())
        };
        compile("lag(f, 2, 0.5) OVER (PARTITION BY s)").unwrap();
        compile("lag(f) OVER (PARTITION BY s)").unwrap();
        for text in [
            "lead(f) OVER (PARTITION BY s)",
            "lag(f) OVER (PARTITION BY s ORDER BY t)",
            "lag(f) OVER (w PARTITION BY s)",
            "lag(f) OVER (PARTITION BY s ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        ] {
            let e = compile(text).unwrap_err();
            assert!(e.contains("has no columnar form"), "{text}: {e}");
        }
    }

    fn value() -> impl Strategy<Value = Option<f64>> {
        prop_oneof![
            Just(None),
            Just(Some(0.0)),
            Just(Some(-0.0)),
            Just(Some(f64::NAN)),
            Just(Some(1.0)),
            (-1e6..1e6f64).prop_map(Some),
        ]
    }

    proptest! {
        #[test]
        fn random_rows_give_the_same_columns(
            rows in prop::collection::vec((any::<i32>(), value(), prop_oneof![Just("buy"), Just("sell"), Just("x")], any::<i32>(), value()), 1..20)
        ) {
            let rows: Vec<Vec<Value>> = rows
                .into_iter()
                .map(|(i, f, s, t, g)| {
                    vec![
                        Value::Int(i as i64),
                        f.map_or(Value::Null, Value::F64),
                        Value::Str(s.into()),
                        Value::Time(t as i64),
                        Value::UInt(i.unsigned_abs() as u64),
                        g.map_or(Value::Null, Value::F64),
                        Value::Null,
                    ]
                })
                .collect();
            check(EXPRS, &rows);
        }
    }
}
