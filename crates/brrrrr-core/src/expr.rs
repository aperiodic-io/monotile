//! SQL expressions compiled once into closures, with ClickHouse/Proton semantics: arithmetic
//! with a float operand, Float32 ones included, and every `/` is Float64, NULL propagates, WHERE
//! treats NULL as false.
use crate::format::{padded, scratch, text_value, write_text};
use crate::value::{civil, Type, Value};
use sqlparser::ast::{
    self, BinaryOperator as B, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator as U,
};
use std::borrow::Cow;
use std::sync::Arc;

pub(crate) mod functions;
pub(crate) mod vector;

/// A compiled expression over a row.
pub type Ex = Compiled<Value>;
/// A compiled condition: its SQL truth, `None` for NULL.
pub type Pred = Compiled<Option<bool>>;
/// A sink's Kafka headers, compiled from its `_tp_message_headers` (`Compiler::headers`).
pub type Headers = Compiled<Vec<(String, String)>>;
/// A compiled expression over a row, yielding `T`.
pub type Compiled<T> = Arc<dyn Fn(&[Value]) -> T + Send + Sync>;

/// An operand: column and constant reads are borrowed rather than cloned, which matters for
/// the many `side = 'buy'`-style tests per row (a string clone is two atomic operations).
#[derive(Clone)]
pub(crate) enum Arg {
    Col(usize),
    Const(Value),
    Ex(Ex),
}

impl Arg {
    #[inline]
    pub(crate) fn eval<'a>(&'a self, r: &'a [Value]) -> Cow<'a, Value> {
        match self {
            Arg::Col(i) => Cow::Borrowed(&r[*i]),
            Arg::Const(v) => Cow::Borrowed(v),
            Arg::Ex(e) => Cow::Owned(e(r)),
        }
    }

    /// The operand's value, owned: a column or literal cloned, an expression called directly
    /// (no `Cow` around its result, which costs where most operands are expressions).
    #[inline]
    pub(crate) fn value(&self, r: &[Value]) -> Value {
        match self {
            Arg::Col(i) => r[*i].clone(),
            Arg::Const(v) => v.clone(),
            Arg::Ex(e) => e(r),
        }
    }

    fn into_ex(self) -> Ex {
        match self {
            Arg::Col(i) => Arc::new(move |r: &[Value]| r[i].clone()),
            Arg::Const(v) => constant(v),
            Arg::Ex(e) => e,
        }
    }
}

/// Names visible to an expression: `(qualifier, column)` per row position, with the column's
/// type where the planner knows it.
#[derive(Clone, Debug, Default)]
pub struct Scope {
    pub cols: Vec<(Option<String>, String)>,
    pub types: Vec<Option<Type>>,
}

impl Scope {
    pub fn new(names: impl IntoIterator<Item = String>) -> Scope {
        Scope { cols: names.into_iter().map(|n| (None, n)).collect(), types: vec![] }
    }
    fn ty(&self, i: usize) -> Option<&Type> {
        self.types.get(i).and_then(Option::as_ref).map(Type::base)
    }
    fn find(&self, qual: Option<&str>, name: &str) -> Option<usize> {
        self.cols.iter().position(|(q, n)| n == name && (qual.is_none() || q.as_deref() == qual))
    }
}

/// What an expression statically is, where certain (see `Compiler::class`). Ordered so that
/// the larger of a string and another class is the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Class {
    Str,
    Num,
    Time,
}

/// A string literal read as a value of `ty` to compare with, as ClickHouse converts a constant
/// string compared with a number or datetime; `None` if it does not read as one.
fn read_as(text: &str, ty: &Type) -> Option<Value> {
    match ty {
        Type::Time(_) => crate::value::parse_datetime(text).map(Value::Time),
        _ => {
            text.trim().parse::<f64>().ok()?;
            Some(Value::Str(text.into()).cast(ty))
        }
    }
}

/// Receives aggregate calls met while compiling (window queries): returns the output position
/// holding that aggregate's result.
pub trait Aggregates {
    fn aggregate(&mut self, name: &str, params: &[Value], args: &[Expr]) -> Result<Option<usize>, String>;
}

/// Receives window function calls (`f(...) OVER (...)`) met while compiling a SELECT list:
/// returns the output position holding that call's result for each row.
pub trait Windows {
    fn window(&mut self, name: &str, params: &[Value], args: &[Expr], over: &ast::WindowType) -> Result<usize, String>;
}

pub struct Compiler<'a> {
    pub scope: &'a Scope,
    /// SELECT aliases, substituted ClickHouse-style wherever their name is used.
    pub aliases: Vec<(String, Expr)>,
    pub aggregates: Option<&'a mut dyn Aggregates>,
    pub windows: Option<&'a mut dyn Windows>,
    pub(crate) expanding: Vec<String>,
}

type R<T> = Result<T, String>;

impl<'a> Compiler<'a> {
    pub fn new(scope: &'a Scope) -> Compiler<'a> {
        Compiler { scope, aliases: vec![], aggregates: None, windows: None, expanding: vec![] }
    }

    /// Compiles a SELECT item named `alias`: inside its own expression the name means the
    /// column, not the alias (`x * 2 AS x`), while other items see the alias (ClickHouse).
    pub fn compile_item(&mut self, e: &Expr, alias: &str) -> R<Ex> {
        self.expanding.push(alias.to_string());
        let r = self.compile(e);
        self.expanding.pop();
        r
    }

    pub fn compile(&mut self, e: &Expr) -> R<Ex> {
        Ok(self.arg(e)?.into_ex())
    }

    /// Compiles `e` as an operand the engine reads per row: columns and literals by reference.
    pub(crate) fn operand(&mut self, e: &Expr) -> R<Arg> {
        self.arg(e)
    }

    /// Compiles `e` as an operand: columns and literals stay readable by reference.
    fn arg(&mut self, e: &Expr) -> R<Arg> {
        Ok(match e {
            Expr::Identifier(id) => self.column(None, &id.value)?,
            Expr::CompoundIdentifier(ids) if ids.len() == 2 => self.column(Some(&ids[0].value), &ids[1].value)?,
            Expr::Value(v) => Arg::Const(literal(&v.value)?),
            // a constant, as `time_bucket`'s width must be
            Expr::Interval(i) => Arg::Const(Value::Int(interval_us(i)?)),
            Expr::Nested(e) => self.arg(e)?,
            Expr::UnaryOp { op: U::Minus, expr } => match expr.as_ref() {
                Expr::Value(v) if matches!(v.value, ast::Value::Number(..)) => {
                    Arg::Const(number(&format!("-{}", v.value))?)
                }
                _ => Arg::Ex(self.compile_ex(e)?),
            },
            e => Arg::Ex(self.compile_ex(e)?),
        })
    }

    /// The type of a column reference, if the scope knows it. Aliases are not expanded (that
    /// would register the aggregates in them early): an alias is of unknown type.
    fn column_type(&self, e: &Expr) -> Option<Type> {
        let (qual, name) = match e {
            Expr::Identifier(id) => (None, &id.value),
            Expr::CompoundIdentifier(ids) if ids.len() == 2 => (Some(ids[0].value.as_str()), &ids[1].value),
            _ => return None,
        };
        if qual.is_none() && !self.expanding.contains(name) && self.aliases.iter().any(|(a, _)| a == name) {
            return None;
        }
        self.scope.ty(self.scope.find(qual, name)?).cloned()
    }

    /// Whether `e` is statically a string, so it cannot be a condition.
    pub(crate) fn is_string(&mut self, e: &Expr) -> bool {
        self.class(e) == Some(Class::Str)
    }

    /// What `e` statically is, where that is certain from a literal or a typed column; `None`
    /// when it depends on anything else (functions, CASE, NULL). Only certain classes are
    /// checked, so no valid expression is refused for lack of type inference.
    fn class(&self, e: &Expr) -> Option<Class> {
        match e {
            Expr::Value(v) => match &v.value {
                ast::Value::SingleQuotedString(_) | ast::Value::DoubleQuotedString(_) => Some(Class::Str),
                ast::Value::Number(..) => Some(Class::Num),
                _ => None,
            },
            Expr::Nested(e) => self.class(e),
            Expr::UnaryOp { op: U::Minus | U::Plus, expr } => self.class(expr).filter(|c| *c == Class::Num),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => match self.column_type(e)? {
                Type::Str => Some(Class::Str),
                Type::Int(_) | Type::UInt(_) | Type::F32 | Type::F64 => Some(Class::Num),
                Type::Time(_) => Some(Class::Time),
                _ => None,
            },
            _ => None,
        }
    }

    /// Compiles `e` where only its SQL truth matters (WHERE, CASE WHEN, the operands of AND,
    /// OR and NOT): no `Value::Bool` is built on the way, and a column compared with a constant
    /// is read in place.
    pub fn condition(&mut self, e: &Expr) -> R<Pred> {
        Ok(match e {
            Expr::Nested(x) => self.condition(x)?,
            Expr::UnaryOp { op: U::Not, expr } => {
                if self.is_string(expr) {
                    return Err(format!("a string is not a condition: {e}"));
                }
                let p = self.condition(expr)?;
                Arc::new(move |r| p(r).map(|b| !b))
            }
            Expr::BinaryOp { left, op: op @ (B::And | B::Or), right } => {
                if self.is_string(left) || self.is_string(right) {
                    return Err(format!("a string is not a condition: {e}"));
                }
                let (a, b) = (self.condition(left)?, self.condition(right)?);
                // three-valued logic; the right side is skipped when the left decides
                if matches!(op, B::And) {
                    Arc::new(move |r| match a(r) {
                        Some(false) => Some(false),
                        l => match (l, b(r)) {
                            (_, Some(false)) => Some(false),
                            (Some(true), Some(true)) => Some(true),
                            _ => None,
                        },
                    })
                } else {
                    Arc::new(move |r| match a(r) {
                        Some(true) => Some(true),
                        l => match (l, b(r)) {
                            (_, Some(true)) => Some(true),
                            (Some(false), Some(false)) => Some(false),
                            _ => None,
                        },
                    })
                }
            }
            Expr::BinaryOp { left, op: op @ (B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq), right } => {
                let (a, b) = self.operands(e, left, op, right)?;
                cmp(op, a, b, |t| t)
            }
            e => {
                let a = self.arg(e)?;
                Arc::new(move |r| truth(&a.eval(r)))
            }
        })
    }

    /// The operands of `left op right`, checked where their types are certain: arithmetic on
    /// a string is an error, and a string literal compared with a number or datetime is read
    /// as one (ClickHouse), while a string column compared with one is an error.
    fn operands(&mut self, e: &Expr, left: &Expr, op: &B, right: &Expr) -> R<(Arg, Arg)> {
        let (l, r) = (self.class(left), self.class(right));
        let string = l == Some(Class::Str) || r == Some(Class::Str);
        match op {
            B::Plus | B::Minus | B::Multiply | B::Divide | B::Modulo if string => {
                return Err(format!("arithmetic on a string: {e}"));
            }
            B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq if string && l != r && l.is_some() && r.is_some() => {
                // a string literal compared with a number or datetime is read as one
                // (ClickHouse); a string column compared with one is an error
                let (text, other) = if l == Some(Class::Str) { (left, right) } else { (right, left) };
                let Expr::Value(ast::ValueWithSpan { value: ast::Value::SingleQuotedString(text), .. }) = text else {
                    let other = if l.max(r) == Some(Class::Time) { "datetime" } else { "number" };
                    return Err(format!("compares a string with a {other}: {e}"));
                };
                let ty = self.column_type(other).unwrap_or(match l.max(r) {
                    Some(Class::Time) => Type::Time(6),
                    _ => Type::F64,
                });
                let v = read_as(text, &ty).ok_or(format!("'{text}' is not a {ty:?} to compare with: {e}"))?;
                return Ok(if l == Some(Class::Str) {
                    (Arg::Const(v), self.arg(right)?)
                } else {
                    (self.arg(left)?, Arg::Const(v))
                });
            }
            _ => {}
        }
        Ok((self.arg(left)?, self.arg(right)?))
    }

    fn compile_ex(&mut self, e: &Expr) -> R<Ex> {
        Ok(match e {
            Expr::UnaryOp { op: U::Minus, expr } => {
                let a = self.arg(expr)?;
                Arc::new(move |r| neg(&a.eval(r)))
            }
            Expr::UnaryOp { op: U::Plus, expr } => self.compile(expr)?,
            Expr::UnaryOp { op: U::Not, .. } | Expr::BinaryOp { op: B::And | B::Or, .. } => {
                let p = self.condition(e)?;
                Arc::new(move |r| p(r).map_or(Value::Null, Value::Bool))
            }
            // `a || b`: text joined (NULL if either is)
            Expr::BinaryOp { left, op: B::StringConcat, right } => {
                self.compile_ex(&call_of("concat", vec![(**left).clone(), (**right).clone()]))?
            }
            Expr::BinaryOp { left, op, right } => {
                let (a, b) = self.operands(e, left, op, right)?;
                match op {
                    B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq => {
                        cmp(op, a, b, |t: Option<bool>| t.map_or(Value::Null, Value::Bool))
                    }
                    _ => binary(op, a, b)?,
                }
            }
            Expr::IsNull(e) => {
                let a = self.arg(e)?;
                Arc::new(move |r| Value::Bool(a.eval(r).is_null()))
            }
            Expr::IsNotNull(e) => {
                let a = self.arg(e)?;
                Arc::new(move |r| Value::Bool(!a.eval(r).is_null()))
            }
            Expr::InList { expr, list, negated } => {
                let a = self.arg(expr)?;
                let items = list.iter().map(|e| self.arg(e)).collect::<R<Vec<_>>>()?;
                let neg = *negated;
                Arc::new(move |r| {
                    let v = a.eval(r);
                    if v.is_null() {
                        return Value::Null;
                    }
                    Value::Bool(items.iter().any(|i| equal(&v, &i.eval(r)) == Some(true)) != neg)
                })
            }
            Expr::Case { operand: None, conditions, else_result, .. } => {
                let arms = conditions
                    .iter()
                    .map(|w| Ok((self.condition(&w.condition)?, self.arg(&w.result)?)))
                    .collect::<R<Vec<_>>>()?;
                let other = else_result.as_ref().map(|e| self.arg(e)).transpose()?;
                Arc::new(move |r| {
                    for (c, v) in &arms {
                        if c(r) == Some(true) {
                            return v.eval(r).into_owned();
                        }
                    }
                    other.as_ref().map_or(Value::Null, |o| o.eval(r).into_owned())
                })
            }
            Expr::Tuple(items) | Expr::Array(ast::Array { elem: items, .. }) => {
                let items = items.iter().map(|e| self.compile(e)).collect::<R<Vec<_>>>()?;
                Arc::new(move |r| Value::Array(items.iter().map(|i| i(r)).collect()))
            }
            Expr::Function(f) => self.function(f)?,
            // arr[i], 1-based; out of range: NULL
            Expr::CompoundFieldAccess { root, access_chain } => match &access_chain[..] {
                [ast::AccessExpr::Subscript(ast::Subscript::Index { index })] => {
                    let (arr, i) = (self.compile(root)?, self.compile(index)?);
                    Arc::new(move |r| match (arr(r), i(r).i64().and_then(|i| usize::try_from(i - 1).ok())) {
                        (Value::Array(a), Some(at)) => a.get(at).cloned().unwrap_or(Value::Null),
                        _ => Value::Null,
                    })
                }
                _ => return Err(format!("unsupported expression: {e}")),
            },
            e => match desugar(e)? {
                Some(d) => self.compile(&d)?,
                None => return Err(format!("unsupported expression: {e}")),
            },
        })
    }

    /// A sink's `_tp_message_headers` of the usual shape,
    /// `cast(([key, ...], [value, ...]), 'map(...)')`, compiled to the headers `engine::headers`
    /// reads from the map it evaluates to (the text of each item, paired up to the shorter
    /// array), each key and value written once into a string of its length rather than through
    /// string values, arrays and the map. `None` for any other expression.
    pub fn headers(&mut self, e: &Expr) -> R<Option<Headers>> {
        let Some([Expr::Tuple(kv), Expr::Value(t)]) = call(e, "cast").as_deref() else {
            return Ok(None);
        };
        let (ast::Value::SingleQuotedString(t), [Expr::Array(k), Expr::Array(v)]) = (&t.value, kv.as_slice()) else {
            return Ok(None);
        };
        if !matches!(Type::parse(t), Ok(Type::Map(..))) {
            return Ok(None);
        }
        let mut texts = |items: &[Expr]| items.iter().map(|e| self.header_text(e)).collect::<R<Vec<_>>>();
        let (keys, values) = (texts(&k.elem)?, texts(&v.elem)?);
        Ok(Some(Arc::new(move |r| {
            let text = |t: &HeaderText| {
                scratch(|s| {
                    t(r, s);
                    s.as_str().to_owned()
                })
            };
            keys.iter().zip(&values).map(|(k, v)| (text(k), text(v))).collect()
        })))
    }

    /// A header's key or value: `concat(...)` written in place (`"NULL"` when it is NULL),
    /// anything else as its value's text, that of `to_string(x)` being that of `x`.
    fn header_text(&mut self, e: &Expr) -> R<HeaderText> {
        if let Some(args) = call(e, "concat") {
            let args = args.into_iter().map(|a| self.arg(call1(a, "to_string").unwrap_or(a))).collect::<R<Vec<_>>>()?;
            return Ok(Box::new(move |r, s| {
                let start = s.len();
                if !concat(&args, r, s) {
                    s.truncate(start);
                    s.push_str("NULL"); // the text of a NULL
                }
            }));
        }
        let a = self.arg(call1(e, "to_string").unwrap_or(e))?;
        Ok(Box::new(move |r, s| write_text(s, &a.eval(r))))
    }

    fn column(&mut self, qual: Option<&str>, name: &str) -> R<Arg> {
        if qual.is_none() && !self.expanding.iter().any(|n| n == name) {
            if let Some((_, e)) = self.aliases.iter().find(|(a, _)| a == name).cloned() {
                self.expanding.push(name.to_string());
                let r = self.arg(&e);
                self.expanding.pop();
                return r;
            }
        }
        let i = self
            .scope
            .find(qual, name)
            .ok_or_else(|| format!("unknown column {}{name}", qual.map_or(String::new(), |q| format!("{q}."))))?;
        Ok(Arg::Col(i))
    }

    fn function(&mut self, f: &ast::Function) -> R<Ex> {
        let name = f.name.to_string().to_ascii_lowercase();
        let (params, args) = (plain_args(&f.parameters)?, plain_args(&f.args)?);
        // clauses of an aggregate or window call nothing here reads: refused, never ignored
        if f.filter.is_some() || f.null_treatment.is_some() || !f.within_group.is_empty() {
            return Err(format!("FILTER, IGNORE/RESPECT NULLS and WITHIN GROUP are not supported: {f}"));
        }
        if let Some(over) = &f.over {
            let Some(windows) = self.windows.as_deref_mut() else {
                return Err(format!("a window function outside the SELECT list of a query over a stream: {f}"));
            };
            let params = params.iter().map(|p| match p {
                Expr::Value(v) => literal(&v.value),
                p => Err(format!("window function parameter must be a literal: {p}")),
            });
            let slot = windows.window(&name, &params.collect::<R<Vec<_>>>()?, &args, over)?;
            return Ok(Arc::new(move |r: &[Value]| r[slot].clone()));
        }
        if let Some(aggs) = self.aggregates.as_deref_mut() {
            let params = params.iter().map(|p| match p {
                Expr::Value(v) => literal(&v.value),
                p => Err(format!("aggregate parameter must be a literal: {p}")),
            });
            let params = params.collect::<R<Vec<_>>>()?;
            if let Some(slot) = aggs.aggregate(&name, &params, &args)? {
                return Ok(Arc::new(move |r: &[Value]| r[slot].clone()));
            }
        } else if crate::agg::is_aggregate(&name) {
            return Err(format!("aggregate {name} outside of GROUP BY"));
        }
        if !params.is_empty() {
            return Err(format!("{name} takes no parameters"));
        }
        if let ("array_sum", [Expr::Function(g)]) = (name.as_str(), args.as_slice()) {
            if g.name.to_string().eq_ignore_ascii_case("array_slice") {
                // L2 sums the top N levels: sum the range in place instead of building the slice
                if let [arr, off, len] = plain_args(&g.args)?.as_slice() {
                    let (arr, off, len) = (self.arg(arr)?, self.arg(off)?, self.arg(len)?);
                    return Ok(Arc::new(move |r| match (&*arr.eval(r), off.eval(r).i64(), len.eval(r).i64()) {
                        (Value::Array(a), Some(o), Some(l)) => Value::F64(sum(&a[slice(a.len(), o, l)])),
                        _ => Value::Null,
                    }));
                }
            }
        }
        if let ("if", [cond, then, other]) = (name.as_str(), args.as_slice()) {
            // L2 guards every level sum with `if(length(x) > 0, ..)`
            let (cond, then, other) = (self.condition(cond)?, self.arg(then)?, self.arg(other)?);
            return Ok(Arc::new(move |r| {
                let pick = if cond(r) == Some(true) { &then } else { &other };
                pick.eval(r).into_owned()
            }));
        }
        // `concat(to_string(x), ..)` is `concat(x, ..)` (NULL for a NULL `x`, else with its text)
        // without a string value made in between
        let unwrap = |e| if name == "concat" { call1(e, "to_string").unwrap_or(e) } else { e };
        let a = args.iter().map(|e| self.arg(unwrap(e))).collect::<R<Vec<_>>>()?;
        let lit = |i: usize| -> Option<String> {
            match args.get(i) {
                Some(Expr::Value(v)) => match &v.value {
                    ast::Value::SingleQuotedString(s) => Some(s.clone()),
                    _ => None,
                },
                _ => None,
            }
        };
        scalar(&name, a, lit)
    }
}

/// A call of `name` with plain arguments.
pub(crate) fn call_of(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(ast::Function {
        name: ast::ObjectName::from(vec![ast::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args.into_iter().map(|a| FunctionArg::Unnamed(FunctionArgExpr::Expr(a))).collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn string_lit(s: &str) -> Expr {
    Expr::value(ast::Value::SingleQuotedString(s.into()))
}

/// An SQL type as the engine's (for `CAST`).
pub(crate) fn sql_type(t: &ast::DataType) -> R<&'static str> {
    use ast::DataType as T;
    Ok(match t {
        T::Double(_)
        | T::DoublePrecision
        | T::Float(_)
        | T::Float64
        | T::Real
        | T::Float4
        | T::Float8
        | T::Float32
        | T::Decimal(_)
        | T::Numeric(_)
        | T::Dec(_) => "nullable(float64)",
        T::Int(_)
        | T::Integer(_)
        | T::BigInt(_)
        | T::SmallInt(_)
        | T::TinyInt(_)
        | T::Int64
        | T::Int32
        | T::Int8(_)
        | T::Int16
        | T::Int4(_)
        | T::Int2(_)
        | T::HugeInt => "nullable(int64)",
        T::UBigInt | T::UInt64 | T::UInt32 | T::UInt8 | T::UInt16 | T::BigIntUnsigned(_) | T::IntUnsigned(_) => {
            "nullable(uint64)"
        }
        T::Varchar(_) | T::Text | T::String(_) | T::Char(_) | T::CharVarying(_) | T::Character(_) | T::Nvarchar(_) => {
            "nullable(string)"
        }
        T::Bool | T::Boolean => "nullable(bool)",
        T::Timestamp(..) | T::Datetime(_) | T::Datetime64(..) | T::TimestampNtz(_) => "nullable(datetime64(6))",
        T::Date | T::Date32 => "date",
        t => {
            return Err(format!(
                "CAST to {t} is not supported: cast to DOUBLE, BIGINT, VARCHAR, BOOLEAN, TIMESTAMP or DATE"
            ))
        }
    })
}

/// An interval in µs: `INTERVAL '5 minutes'`, `INTERVAL '5' MINUTE`, `INTERVAL 5 SECOND`.
fn interval_us(i: &ast::Interval) -> R<i64> {
    let v = match i.value.as_ref() {
        Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::Number(s, _) => s.clone(),
            v => return Err(format!("unsupported interval {v}")),
        },
        v => return Err(format!("unsupported interval {v}")),
    };
    let text = match &i.leading_field {
        Some(f) => format!("{v} {f}"),
        None => v,
    };
    crate::value::duration_us(&text).ok_or(format!("INTERVAL '{text}': a whole number of microseconds to weeks"))
}

/// The special forms of SQL as calls of the functions that compute them (`EXTRACT(hour FROM t)`
/// is `date_part('hour', t)`); `None` for anything else.
fn desugar(e: &Expr) -> R<Option<Expr>> {
    Ok(Some(match e {
        Expr::Cast { expr, data_type, .. } => match sql_type(data_type)? {
            "date" => call_of(
                "date_trunc",
                vec![string_lit("day"), call_of("cast", vec![(**expr).clone(), string_lit("nullable(datetime64(6))")])],
            ),
            t => call_of("cast", vec![(**expr).clone(), string_lit(t)]),
        },
        Expr::TypedString(ast::TypedString { data_type, value, .. }) => {
            let text = match &value.value {
                ast::Value::SingleQuotedString(s) => s.clone(),
                v => return Err(format!("unsupported literal {v}")),
            };
            desugar(&Expr::Cast {
                kind: ast::CastKind::Cast,
                expr: Box::new(string_lit(&text)),
                data_type: data_type.clone(),
                format: None,
            })?
            .expect("a cast")
        }
        Expr::Interval(i) => Expr::value(ast::Value::Number(interval_us(i)?.to_string(), false)),
        Expr::AtTimeZone { timestamp, time_zone } => {
            call_of("timezone", vec![(**time_zone).clone(), (**timestamp).clone()])
        }
        Expr::Extract { field, expr, .. } => {
            call_of("date_part", vec![string_lit(&field.to_string().to_lowercase()), (**expr).clone()])
        }
        Expr::Floor { expr, field: ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime) } => {
            call_of("floor", vec![(**expr).clone()])
        }
        Expr::Ceil { expr, field: ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime) } => {
            call_of("ceil", vec![(**expr).clone()])
        }
        Expr::Substring { expr, substring_from, substring_for, .. } => {
            let mut a = vec![
                (**expr).clone(),
                substring_from.as_deref().cloned().unwrap_or(Expr::value(ast::Value::Number("1".into(), false))),
            ];
            a.extend(substring_for.as_deref().cloned());
            call_of("substr", a)
        }
        Expr::Trim { expr, trim_where, trim_what: None, trim_characters: None } => {
            let name = match trim_where {
                Some(ast::TrimWhereField::Leading) => "ltrim",
                Some(ast::TrimWhereField::Trailing) => "rtrim",
                _ => "trim",
            };
            call_of(name, vec![(**expr).clone()])
        }
        Expr::Like { negated, expr, pattern, escape_char: None, any: false }
        | Expr::ILike { negated, expr, pattern, escape_char: None, any: false } => {
            let f = if matches!(e, Expr::ILike { .. }) { "ilike" } else { "like" };
            let c = call_of(f, vec![(**expr).clone(), (**pattern).clone()]);
            if *negated {
                Expr::UnaryOp { op: U::Not, expr: Box::new(c) }
            } else {
                c
            }
        }
        Expr::Between { expr, negated, low, high } => {
            let (ge, le) = (
                Expr::BinaryOp { left: expr.clone(), op: B::GtEq, right: low.clone() },
                Expr::BinaryOp { left: expr.clone(), op: B::LtEq, right: high.clone() },
            );
            let both = Expr::BinaryOp { left: Box::new(ge), op: B::And, right: Box::new(le) };
            if *negated {
                Expr::UnaryOp { op: U::Not, expr: Box::new(Expr::Nested(Box::new(both))) }
            } else {
                both
            }
        }
        Expr::Case { operand: Some(x), conditions, else_result, case_token, end_token } => Expr::Case {
            operand: None,
            conditions: conditions
                .iter()
                .map(|w| ast::CaseWhen {
                    condition: Expr::BinaryOp { left: x.clone(), op: B::Eq, right: Box::new(w.condition.clone()) },
                    result: w.result.clone(),
                })
                .collect(),
            else_result: else_result.clone(),
            case_token: case_token.clone(),
            end_token: end_token.clone(),
        },
        Expr::IsTrue(x) => {
            Expr::BinaryOp { left: x.clone(), op: B::Eq, right: Box::new(Expr::value(ast::Value::Boolean(true))) }
        }
        Expr::IsFalse(x) => {
            Expr::BinaryOp { left: x.clone(), op: B::Eq, right: Box::new(Expr::value(ast::Value::Boolean(false))) }
        }
        Expr::Position { expr, r#in } => call_of("strpos", vec![(**r#in).clone(), (**expr).clone()]),
        _ => return Ok(None),
    }))
}

fn constant(v: Value) -> Ex {
    Arc::new(move |_| v.clone())
}

fn number(n: &str) -> R<Value> {
    if let Ok(u) = n.parse::<u64>() {
        return Ok(Value::UInt(u));
    }
    if let Ok(i) = n.parse::<i64>() {
        return Ok(Value::Int(i));
    }
    n.parse::<f64>().map(Value::F64).map_err(|_| format!("{n:?} is not a number"))
}

fn literal(v: &ast::Value) -> R<Value> {
    Ok(match v {
        ast::Value::Number(n, _) => number(n)?,
        ast::Value::SingleQuotedString(s) => Value::Str(s.as_str().into()),
        ast::Value::Boolean(b) => Value::Bool(*b),
        ast::Value::Null => Value::Null,
        v => return Err(format!("unsupported literal {v}")),
    })
}

/// SQL truth: NULL is unknown.
pub fn truth(v: &Value) -> Option<bool> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(*b),
        v => v.f64().map(|f| f != 0.0),
    }
}

fn is_float(v: &Value) -> bool {
    matches!(v, Value::F32(_) | Value::F64(_))
}

fn neg(v: &Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::F32(f) => Value::F32(-f),
        Value::F64(f) => Value::F64(-f),
        Value::Int(i) => Value::Int(i.wrapping_neg()),
        Value::UInt(u) => Value::Int((*u as i64).wrapping_neg()),
        v => Value::F64(-v.f64().unwrap_or(0.0)),
    }
}

/// ClickHouse arithmetic: any float operand makes it Float64 (even Float32 op Float32),
/// integers wrap in 64 bits, `/` is always Float64. `%` takes the sign of the dividend; an
/// integer `%` by zero, which ClickHouse raises, is NULL (a stream cannot fail).
fn arith(op: &B, a: &Value, b: &Value) -> Value {
    if a.is_null() || b.is_null() {
        return Value::Null;
    }
    if matches!(op, B::Divide) {
        return Value::F64(a.f64().unwrap_or(0.0) / b.f64().unwrap_or(0.0));
    }
    if is_float(a) || is_float(b) {
        let (x, y) = (a.f64().unwrap_or(0.0), b.f64().unwrap_or(0.0));
        return Value::F64(match op {
            B::Plus => x + y,
            B::Minus => x - y,
            B::Modulo => x % y,
            _ => x * y,
        });
    }
    let (x, y) = (a.i64().unwrap_or(0), b.i64().unwrap_or(0));
    if vector::time_result(op, matches!(a, Value::Time(_)), matches!(b, Value::Time(_))) {
        return Value::Time(if matches!(op, B::Minus) { x.wrapping_sub(y) } else { x.wrapping_add(y) });
    }
    let r = match op {
        B::Plus => x.wrapping_add(y),
        B::Minus => x.wrapping_sub(y),
        B::Modulo if y == 0 => return Value::Null,
        B::Modulo => x.wrapping_rem(y),
        _ => x.wrapping_mul(y),
    };
    match (a, b, op) {
        (Value::UInt(_), Value::UInt(_), B::Plus | B::Multiply) => Value::UInt(r as u64),
        _ => Value::Int(r),
    }
}

pub fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Str(x), Value::Str(y)) => Some(x.cmp(y)),
        (Value::Int(x), Value::Int(y)) | (Value::Time(x), Value::Time(y)) => Some(x.cmp(y)),
        (Value::UInt(x), Value::UInt(y)) => Some(x.cmp(y)),
        // tuples in order of their elements, a prefix first: `arg_max(x, (time, id))` is
        // LAST(x ORDER BY time, id)
        (Value::Array(x), Value::Array(y)) => {
            for (a, b) in x.iter().zip(y.iter()) {
                match compare(a, b)? {
                    std::cmp::Ordering::Equal => {}
                    o => return Some(o),
                }
            }
            Some(x.len().cmp(&y.len()))
        }
        (a, b) => a.f64()?.partial_cmp(&b.f64()?),
    }
}

/// `a` against `b` as a view's `ORDER BY` sorts them: ascending, NULLS LAST, the default and
/// the only order the planner takes. ClickHouse's ORDER BY reference ("Sorting of Special
/// Values"), which Proton keeps: "first the values, then NaN, then NULL".
///
/// A total order, which `compare` is not, and Rust's sorts may panic on less: `compare` has no
/// answer for NULL and NaN, and compares an integer with a number of another type as the
/// doubles they round to, so that `Int(2^53 + 1)` equals `F64(2^53)`, which equals `Int(2^53)`,
/// which is less than `Int(2^53 + 1)`. Here numbers of different types compare exactly; wherever
/// `compare` has an answer, this is the same answer, unless such a rounded tie decided it (an
/// array's next element may then decide). NaNs are equal whatever their sign, and -0 equals 0,
/// as `compare` has them. Values `compare` cannot order (a string and a number, an array and
/// either: never in one column of a typed stream, at most in an expression like `if(c, 1, 'a')`)
/// go by kind: numbers, strings, arrays, then NaN and NULL. Arrays (tuples) in order of their
/// elements, by this order, a prefix first.
pub fn order_by(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (Sorted::of(a), Sorted::of(b)) {
        (Sorted::Int(x), Sorted::Int(y)) => x.cmp(&y),
        (Sorted::Float(x), Sorted::Float(y)) if x == y => Equal,
        (Sorted::Float(x), Sorted::Float(y)) => x.total_cmp(&y),
        (Sorted::Float(x), Sorted::Int(y)) => float_against(x, y),
        (Sorted::Int(x), Sorted::Float(y)) => float_against(y, x).reverse(),
        (Sorted::Str(x), Sorted::Str(y)) => x.cmp(y),
        (Sorted::Array(x), Sorted::Array(y)) => {
            let first = x.iter().zip(y.iter()).map(|(a, b)| order_by(a, b)).find(|o| o.is_ne());
            first.unwrap_or_else(|| x.len().cmp(&y.len()))
        }
        (x, y) => x.kind().cmp(&y.kind()),
    }
}

/// A value as `order_by` reads it: integers of every type (booleans and times too) as one
/// integer, which holds any of them exactly, floats apart from NaN.
enum Sorted<'a> {
    Int(i128),
    Float(f64),
    Str(&'a str),
    Array(&'a [Value]),
    NaN,
    Null,
}

impl Sorted<'_> {
    fn of(v: &Value) -> Sorted<'_> {
        let float = |f: f64| if f.is_nan() { Sorted::NaN } else { Sorted::Float(f) };
        match v {
            Value::Bool(b) => Sorted::Int(*b as i128),
            Value::Int(i) | Value::Time(i) => Sorted::Int(*i as i128),
            Value::UInt(u) => Sorted::Int(*u as i128),
            Value::F32(f) => float(*f as f64),
            Value::F64(f) => float(*f),
            Value::Str(s) => Sorted::Str(s),
            Value::Array(a) => Sorted::Array(a),
            Value::Null => Sorted::Null,
        }
    }

    /// The order of values of different kinds.
    fn kind(&self) -> u8 {
        match self {
            Sorted::Int(_) | Sorted::Float(_) => 0,
            Sorted::Str(_) => 1,
            Sorted::Array(_) => 2,
            Sorted::NaN => 3,
            Sorted::Null => 4,
        }
    }
}

/// A float (not NaN) against an integer, exactly: its integer part first (the cast saturates at
/// ±2^127, beyond every integer a `Value` holds, so the infinities and floats of 2^64 or more
/// fall on the right side), then its fraction, which has its sign.
fn float_against(f: f64, i: i128) -> std::cmp::Ordering {
    let whole = f.trunc();
    (whole as i128).cmp(&i).then(f.total_cmp(&whole))
}

/// `a = b` as SQL: `None` when either side is NULL. Same result as `compare(..) == Equal`, but
/// strings of different lengths are unequal without comparing bytes.
pub fn equal(a: &Value, b: &Value) -> Option<bool> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Str(x), Value::Str(y)) => Some(x.len() == y.len() && x == y),
        (a, b) => Some(compare(a, b) == Some(std::cmp::Ordering::Equal)),
    }
}

/// Arithmetic: one closure per operator, and two Float64 operands (most of them, in every
/// pipeline) computed directly, as `arith` would.
fn binary(op: &B, a: Arg, b: Arg) -> R<Ex> {
    fn with<F: Fn(f64, f64) -> f64 + Copy + Send + Sync + 'static>(op: B, a: Arg, b: Arg, f: F) -> Ex {
        match (a, b) {
            (Arg::Col(i), Arg::Col(j)) => Arc::new(move |r| match (&r[i], &r[j]) {
                (Value::F64(x), Value::F64(y)) => Value::F64(f(*x, *y)),
                (x, y) => arith(&op, x, y),
            }),
            (a, b) => Arc::new(move |r| match (&*a.eval(r), &*b.eval(r)) {
                (Value::F64(x), Value::F64(y)) => Value::F64(f(*x, *y)),
                (x, y) => arith(&op, x, y),
            }),
        }
    }
    Ok(match op {
        B::Plus => with(B::Plus, a, b, |x, y| x + y),
        B::Minus => with(B::Minus, a, b, |x, y| x - y),
        B::Multiply => with(B::Multiply, a, b, |x, y| x * y),
        B::Divide => with(B::Divide, a, b, |x, y| x / y),
        B::Modulo => with(B::Modulo, a, b, |x, y| x % y),
        op => return Err(format!("unsupported operator {op}")),
    })
}

/// `a op b` for a comparison, as SQL truth mapped by `out`. A column compared with a constant
/// (on either side) reads the row in place, and a Float64 column against a number compares
/// the floats directly: the same results as `compare`/`equal`, without their general path.
fn cmp<T: 'static>(
    op: &B,
    a: Arg,
    b: Arg,
    out: impl Fn(Option<bool>) -> T + Copy + Send + Sync + 'static,
) -> Compiled<T> {
    use std::cmp::Ordering::{self, *};
    // the constant on the right; `x op c` is `c op' x` with the order reversed
    let (a, b, op) = match (a, b) {
        (Arg::Const(c), Arg::Col(i)) => {
            let op = match op {
                B::Lt => B::Gt,
                B::LtEq => B::GtEq,
                B::Gt => B::Lt,
                B::GtEq => B::LtEq,
                op => op.clone(),
            };
            (Arg::Col(i), Arg::Const(c), op)
        }
        (a, b) => (a, b, op.clone()),
    };
    if let B::Eq | B::NotEq = op {
        let ne = matches!(op, B::NotEq); // NaN: only `!=` holds
        return match (a, b) {
            (Arg::Col(i), Arg::Const(c)) => Arc::new(move |r| out(equal(&r[i], &c).map(|eq| eq != ne))),
            (a, b) => Arc::new(move |r| out(equal(&a.eval(r), &b.eval(r)).map(|eq| eq != ne))),
        };
    }
    let ok: fn(Ordering) -> bool = match op {
        B::Lt => |o| o == Less,
        B::LtEq => |o| o != Greater,
        B::Gt => |o| o == Greater,
        _ => |o| o != Less,
    };
    match (a, b) {
        (Arg::Col(_), Arg::Const(Value::Null)) => Arc::new(move |_| out(None)),
        (Arg::Col(i), Arg::Const(c)) => {
            // `compare` of a Float64 with any value is this float comparison
            let num = c.f64();
            Arc::new(move |r| {
                out(match (&r[i], num) {
                    (Value::Null, _) => None,
                    (Value::F64(x), Some(y)) => Some(x.partial_cmp(&y).is_some_and(ok)),
                    (x, _) => Some(compare(x, &c).is_some_and(ok)),
                })
            })
        }
        (a, b) => Arc::new(move |r| {
            let (x, y) = (a.eval(r), b.eval(r));
            out(if x.is_null() || y.is_null() { None } else { Some(compare(&x, &y).is_some_and(ok)) })
        }),
    }
}

fn f64_or_null(v: &Value, f: impl Fn(f64) -> f64) -> Value {
    v.f64().map_or(Value::Null, |x| Value::F64(f(x)))
}

/// Scalar functions. `lit(i)` is argument `i` when it is a string literal (patterns, formats).
/// Plain call arguments: DISTINCT, ORDER BY ... inside the call would silently change the result.
pub(crate) fn plain_args(args: &FunctionArguments) -> R<Vec<Expr>> {
    match args {
        FunctionArguments::None => Ok(vec![]),
        FunctionArguments::List(l) if l.duplicate_treatment.is_none() && l.clauses.is_empty() => l
            .args
            .iter()
            .filter(|a| !matches!(a, FunctionArg::Unnamed(FunctionArgExpr::Wildcard))) // count(*)
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
                a => Err(format!("unsupported argument {a}")),
            })
            .collect(),
        FunctionArguments::List(l) if l.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct) => Err(format!(
            "DISTINCT in the arguments {args} is not supported: count(DISTINCT x) is uniq_exact(x); \
                 otherwise aggregate a SELECT DISTINCT subquery"
        )),
        a => Err(format!("unsupported arguments {a}")),
    }
}

/// `array_slice`'s range: 1-based offset, negative from the end; a negative length stops that
/// far from the end.
fn slice(n: usize, off: i64, len: i64) -> std::ops::Range<usize> {
    let n = n as i64;
    let start = if off > 0 { off - 1 } else { n.saturating_add(off) }.clamp(0, n);
    let end = if len >= 0 { start.saturating_add(len).min(n) } else { n.saturating_add(len).max(start) };
    start as usize..end as usize
}

/// `array_sum`: sequential over Float64, non-numbers skipped.
/// The start of the `w`-µs period `t` is in, from the epoch; `i64::MIN` where that is before it.
pub(crate) fn start_of(t: i64, w: i64) -> i64 {
    t.checked_sub(t.rem_euclid(w)).unwrap_or(i64::MIN)
}

fn sum(a: &[Value]) -> f64 {
    a.iter().filter_map(Value::f64).fold(0.0, |s, x| s + x)
}

/// Scalar functions. `lit(i)` is argument `i` when it is a string literal (patterns, formats).
fn scalar(name: &str, a: Vec<Arg>, lit: impl Fn(usize) -> Option<String>) -> R<Ex> {
    let arity =
        |n: usize| if a.len() == n { Ok(()) } else { Err(format!("{name} expects {n} arguments, got {}", a.len())) };
    let one = |f: fn(&Value) -> Value| -> R<Ex> {
        arity(1)?;
        let x = a[0].clone();
        Ok(Arc::new(move |r| f(&x.eval(r))))
    };
    Ok(match name {
        "coalesce" => {
            Arc::new(move |r| a.iter().map(|e| e.eval(r)).find(|v| !v.is_null()).map_or(Value::Null, Cow::into_owned))
        }
        "null_if" => {
            arity(2)?;
            let [x, y]: [Arg; 2] = a.try_into().unwrap_or_else(|_| unreachable!("two arguments"));
            match y {
                // L2's ratios: `x / null_if(y, 0)`; a Float64 against a number compares as floats
                Arg::Const(c) => {
                    // `compare` of a Float64 with any value is this float comparison
                    let num = c.f64();
                    Arc::new(move |r| {
                        let v = x.eval(r);
                        let eq = match (&*v, num) {
                            (Value::F64(f), Some(n)) => *f == n,
                            (v, _) => compare(v, &c) == Some(std::cmp::Ordering::Equal),
                        };
                        if eq {
                            Value::Null
                        } else {
                            v.into_owned()
                        }
                    })
                }
                y => Arc::new(move |r| {
                    let v = x.eval(r);
                    if compare(&v, &y.eval(r)) == Some(std::cmp::Ordering::Equal) {
                        Value::Null
                    } else {
                        v.into_owned()
                    }
                }),
            }
        }
        "if" => return Err(format!("if expects 3 arguments, got {}", a.len())),
        "abs" => one(|v| match v {
            Value::F32(f) => Value::F32(f.abs()),
            Value::Int(i) => Value::UInt(i.unsigned_abs()),
            Value::UInt(u) => Value::UInt(*u),
            v => f64_or_null(v, f64::abs),
        })?,
        "ln" => one(|v| f64_or_null(v, f64::ln))?,
        // NaN below 0, as in ClickHouse
        "sqrt" => one(|v| f64_or_null(v, f64::sqrt))?,
        // exactly what `cast` gives, without its general dispatch for the common inputs
        "to_float32" => one(|v| match v {
            Value::F64(x) => Value::F32(*x as f32),
            Value::F32(x) => Value::F32(*x),
            Value::Null => Value::Null,
            v => v.cast(&Type::F32),
        })?,
        "to_float64" => one(|v| match v {
            Value::F64(x) => Value::F64(*x),
            Value::Null => Value::Null,
            v => v.cast(&Type::F64),
        })?,
        "to_int32" => one(|v| if v.is_null() { Value::Null } else { v.cast(&Type::Int(32)) })?,
        "to_string" => one(|v| if v.is_null() { Value::Null } else { Value::Str(text_value(v)) })?,
        // DuckDB's COALESCE(TRY_CAST(x AS BIGINT), 0): a trade id as a number, 0 where it is none
        "to_int64_or_zero" => one(|v| match v {
            Value::Str(s) => Value::Int(s.trim().parse().unwrap_or(0)),
            v => Value::Int(v.i64().unwrap_or(0)),
        })?,
        "to_unix_timestamp64_micro" => one(|v| v.i64().map_or(Value::Null, Value::Int))?,
        "from_unix_timestamp64_micro" => one(|v| v.i64().map_or(Value::Null, Value::Time))?,
        // the start of the UTC day, hour or minute of a time (anchors for running totals); the
        // earliest time where the start is before it
        "to_start_of_day" => one(|v| v.i64().map_or(Value::Null, |t| Value::Time(start_of(t, 86_400_000_000))))?,
        "to_start_of_hour" => one(|v| v.i64().map_or(Value::Null, |t| Value::Time(start_of(t, 3_600_000_000))))?,
        "to_start_of_minute" => one(|v| v.i64().map_or(Value::Null, |t| Value::Time(start_of(t, 60_000_000))))?,
        // arrays, such as the top N levels of a book snapshot's
        "length" => one(|v| match v {
            Value::Array(a) => Value::UInt(a.len() as u64),
            Value::Str(s) => Value::UInt(s.chars().count() as u64),
            _ => Value::Null,
        })?,
        "array_sum" => one(|v| if let Value::Array(a) = v { Value::F64(sum(a)) } else { Value::Null })?,
        "array_slice" => {
            arity(3)?;
            Arc::new(move |r| match (&*a[0].eval(r), a[1].eval(r).i64(), a[2].eval(r).i64()) {
                (Value::Array(arr), Some(off), Some(len)) => Value::Array(arr[slice(arr.len(), off, len)].into()),
                _ => Value::Null,
            })
        }
        // its `to_string(x)` arguments are compiled as `x` (`Compiler::function`)
        "concat" => {
            Arc::new(move |r| scratch(|s| if concat(&a, r, s) { Value::Str(s.as_str().into()) } else { Value::Null }))
        }
        "cast" => {
            arity(2)?;
            let t = Type::parse(&lit(1).ok_or("cast type must be a string literal")?)?;
            let x = a[0].clone();
            Arc::new(move |r| x.eval(r).cast(&t))
        }
        "format_datetime" => {
            arity(2)?;
            let fmt = lit(1).ok_or("format_datetime format must be a string literal")?;
            // %Y, %m and %d only; anything else is refused, not guessed
            if fmt.split('%').skip(1).any(|p| !p.starts_with(['Y', 'm', 'd'])) {
                return Err(format!("unsupported format_datetime format {fmt}"));
            }
            // the format depends on the day only, and a window's rows share one: the last day's text
            // is shared rather than formatted again
            let (x, last) = (a[0].clone(), std::sync::Mutex::new((i64::MIN, Arc::<str>::from(""))));
            Arc::new(move |r| {
                x.eval(r).i64().map_or(Value::Null, |us| {
                    let day = us.div_euclid(86_400_000_000);
                    let mut last = last.lock().unwrap_or_else(|p| p.into_inner());
                    if last.0 != day {
                        *last = (day, format_datetime(us, &fmt).into());
                    }
                    Value::Str(last.1.clone())
                })
            })
        }
        "match" => {
            arity(2)?;
            let re = regex::Regex::new(&lit(1).ok_or("match pattern must be a string literal")?)
                .map_err(|e| e.to_string())?;
            let x = a[0].clone();
            Arc::new(move |r| x.eval(r).str().map_or(Value::Null, |s| Value::Bool(re.is_match(s))))
        }
        "replace_regexp_all" => {
            arity(3)?;
            let re = regex::Regex::new(&lit(1).ok_or("replace_regexp_all pattern must be a string literal")?)
                .map_err(|e| e.to_string())?;
            // RE2 replacement `\N` back-references -> regex's `${N}`; a literal `$` must be escaped
            let repl = lit(2).ok_or("replace_regexp_all replacement must be a string literal")?;
            let repl =
                regex::Regex::new(r"\\(\d)").unwrap().replace_all(&repl.replace('$', "$$"), "$${$1}").to_string();
            let x = a[0].clone();
            Arc::new(move |r| {
                x.eval(r)
                    .str()
                    .map_or(Value::Null, |s| Value::Str(re.replace_all(s, repl.as_str()).into_owned().into()))
            })
        }
        other => match functions::function(other, &a, &lit) {
            Some(f) => f?,
            None => return Err(format!("unknown function {other}")),
        },
    })
}

/// `format_datetime` with the `%Y`, `%m` and `%d` specifiers (UTC), validated at compile time.
/// The digits are written in place, not through `format!`: every sink row has a `day`.
pub fn format_datetime(us: i64, fmt: &str) -> String {
    let (y, m, d) = civil(us.div_euclid(86_400_000_000));
    let mut parts = fmt.split('%');
    // `%Y` is the one specifier longer than itself, by 2 digits from year 0 to 9999
    let mut out = String::with_capacity(2 * fmt.len());
    out.push_str(parts.next().unwrap_or_default());
    for p in parts {
        match p.as_bytes()[0] {
            b'Y' if (0..=9999).contains(&y) => padded(&mut out, y, 4),
            b'Y' => out.push_str(&format!("{y:04}")), // `-001`, `10000`: never in practice
            b'm' => padded(&mut out, m.into(), 2),
            _ => padded(&mut out, d.into(), 2),
        }
        out.push_str(&p[1..]);
    }
    out
}

/// Appends the text `concat` makes of `args` to `s`: `false` (NULL) when one of them is NULL.
fn concat(args: &[Arg], r: &[Value], s: &mut String) -> bool {
    for e in args {
        match &*e.eval(r) {
            Value::Null => return false,
            Value::Str(x) => s.push_str(x),
            v => write_text(s, v),
        }
    }
    true
}

/// Appends the text of a sink header's key or value for a row (`Compiler::headers`).
type HeaderText = Box<dyn Fn(&[Value], &mut String) + Send + Sync>;

/// The arguments of `e` if it is a plain call of `f` (any case): no parameters, OVER, FILTER,
/// DISTINCT, named or wildcard arguments, nothing that would change what `f` computes.
fn call<'e>(e: &'e Expr, f: &str) -> Option<Vec<&'e Expr>> {
    let Expr::Function(g) = e else { return None };
    let FunctionArguments::List(l) = &g.args else { return None };
    let plain = matches!(g.parameters, FunctionArguments::None)
        && g.over.is_none()
        && g.filter.is_none()
        && g.null_treatment.is_none()
        && g.within_group.is_empty()
        && l.duplicate_treatment.is_none()
        && l.clauses.is_empty();
    if !plain || !g.name.to_string().eq_ignore_ascii_case(f) {
        return None;
    }
    let arg = |a: &'e FunctionArg| match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => Some(x),
        _ => None,
    };
    l.args.iter().map(arg).collect()
}

/// The argument of `e` if it is a plain call of `f` with one (`call`).
fn call1<'e>(e: &'e Expr, f: &str) -> Option<&'e Expr> {
    match call(e, f)?.as_slice() {
        [x] => Some(*x),
        _ => None,
    }
}
