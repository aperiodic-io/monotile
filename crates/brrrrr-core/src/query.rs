//! Ad-hoc SQL as a pipeline of the engine's own views (ADR-0018).
//!
//! `SELECT ... FROM 'trades.parquet' WHERE ... GROUP BY time_bucket('1m', ts), symbol` is
//! compiled into what the engine runs live: streams for its tables (each a source), views for
//! its steps (a filter or join first, then a tumbling window, then HAVING), and a sink for its
//! result. The historical executor runs that over the files, column by column; the same views
//! would run on a live feed. So a query's answer is the engine's, live and over history alike.
//!
//! The mapping:
//! - a table: a path or URL in quotes (`'data/*.parquet'`, `read_csv('x.csv.gz')`) or a name the
//!   caller knows (`trades`): a source stream of its columns, each nullable;
//! - `GROUP BY time_bucket(width, ts), keys`: a tumbling window on `ts` (its rows in time
//!   order: `ts` becomes its table's clock); `GROUP BY keys` alone, or aggregates without GROUP
//!   BY: one window over every row; `DISTINCT`: a GROUP BY of its items;
//! - `WHERE`, joins, and GROUP BY keys that are not plain columns: a view before the window;
//!   `HAVING`: a view after it;
//! - `GROUP BY time_bucket(width, ts, zone)`: a tumbling window over the zone's wall clock time,
//!   grouped by the bucket's start too; `time_bucket_gapfill(width, ts)`: the window's output
//!   through `gap_fill`, which adds each group's empty buckets (`locf`, `interpolate`);
//! - `ASOF [LEFT] JOIN r ON keys AND l.ts >= r.ts`: the engine's exact as-of join, the times
//!   offset by constant intervals or not (`l.ts + INTERVAL '5 seconds' >= r.ts`, a markout: `r`
//!   read in the order of its time shifted), strict (`>`) or not, with a tolerance or not
//!   (`l.ts - r.ts <= INTERVAL '1 second'`); `[LEFT] JOIN r ON keys`: a lookup of the right row
//!   of the same keys (the last one, if several);
//! - `lead(...) OVER (...)`: the engine's `Lead`, its rows out of time order (`unordered`);
//! - `LEFT JOIN LATERAL (SELECT aggregates FROM r WHERE keys AND r.ts BETWEEN l.ts - a AND
//!   l.ts + b) ON true`, or `FROM l, LATERAL (...)`: a window join (the engine's `Within`);
//! - a subquery or CTE: its own views, into a stream the outer query reads;
//! - `UNION ALL`: views writing one stream, read in its tables' time order; `UNION`: a
//!   `SELECT DISTINCT` of that;
//! - DuckDB's percentiles (`median`, `quantile_cont`, `percentile_cont`): the engine's
//!   `quantile_exact`, which keeps every value; `count(DISTINCT x)`: `uniq_exact`;
//! - the outermost `ORDER BY`, `LIMIT` and `OFFSET`: applied to the result (`Compiled::finish`).
//!
//! Anything else is refused with what to write instead, never ignored.
use crate::column::{Batch, Col, Data};
use crate::engine::{Emit, Historical, Output, Pool, Row};
use crate::expr::order_by;
use crate::sql::{token_sql, Catalog, Column, Kind, Stream, View};
use crate::value::{Type, Value};
use sqlparser::ast::{
    self, visit_expressions_mut, BinaryOperator, Distinct, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr, SetOperator,
    SetQuantifier, Statement, TableAlias, TableFactor, Value as AstValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Word};
use std::collections::{BTreeMap, HashMap};
use std::ops::ControlFlow;

type R<T> = Result<T, String>;

/// A table a query reads: a file, a glob or a URL (with the reader its function named, if
/// any: `read_csv`, and the options after its path: `delim = '|', header = false`, as SQL), or
/// a name the caller resolves.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Table {
    Path { path: String, format: Option<String>, options: Option<String> },
    Named(String),
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Table::Path { path, format: Some(r), .. } if r == "delta" || r == "iceberg" => {
                write!(f, "{r}_scan('{}')", path.replace('\'', "''"))
            }
            Table::Path { path, format: Some(r), options: Some(o) } => {
                write!(f, "read_{r}('{}', {o})", path.replace('\'', "''"))
            }
            Table::Path { path, .. } => write!(f, "'{}'", path.replace('\'', "''")),
            Table::Named(n) => f.write_str(n),
        }
    }
}

/// How a source's rows are ordered for the run: by a time column, as they come, or all at
/// once ahead of every other source's (a table only looked up, by a join on keys).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Clock {
    /// Its rows in the order of this column (µs).
    Column(String),
    /// Its rows in the order they are read: the runtime fills the source's last column (`__row`)
    /// with 0, 1, 2, ...
    Row,
    /// Every row before any other source's: the runtime fills `__row` with `i64::MIN / 4`.
    First,
    /// Its rows in the order of this column plus a constant (µs), which the runtime puts in the
    /// source's last column (`__clock`): the right side of an as-of join at a time offset.
    Shifted(String, i64),
}

/// A source stream of a compiled query: the table it reads, its columns, and its clock.
#[derive(Clone, Debug)]
pub struct Source {
    pub stream: String,
    pub table: Table,
    /// The table's columns, as read (the stream may add `__row` after them).
    pub columns: Vec<(String, Type)>,
    pub clock: Clock,
}

/// An ad-hoc query as a pipeline.
#[derive(Clone, Debug)]
pub struct Compiled {
    pub catalog: Catalog,
    pub sources: Vec<Source>,
    /// The sink the result's rows go to.
    pub result: String,
    pub columns: Vec<String>,
    /// Columns after `columns` the result's rows hold only to be ordered by (dropped by
    /// `finish`).
    pub hidden: usize,
    /// The outermost ORDER BY: (result column, descending, NULLs first).
    pub order: Vec<(usize, bool, bool)>,
    pub limit: Option<usize>,
    pub offset: usize,
    /// Every tumbling window's width (µs).
    pub widths: Vec<i64>,
    /// The row an aggregate without GROUP BY gives over no rows (SQL's: `count` 0, the others
    /// NULL), when it is the result.
    pub empty: Option<Row>,
    /// The column of each source its rows can be split by, so that parts of the query run side
    /// by side, each on the rows of some keys: every stateful step is keyed by it, but the
    /// windows over every row of `merge`.
    pub partition: Option<BTreeMap<String, String>>,
    /// The streams of the windows without a time bucket that the parts of a partitioned run each
    /// aggregate their rows in, merged at the end (`Historical::hold`): a global aggregate, or
    /// one by keys other than the partition's, over a join or window keyed by it.
    pub merge: Vec<String>,
}

/// The table a SELECT without FROM reads: one row, of no column anyone reads.
pub const ONE_ROW: &str = "__one_row";

/// The clock value of every row of a `Clock::First` source.
pub const FIRST: i64 = i64::MIN / 4;

impl Compiled {
    /// The clock range a run covers: every time a row may have, on every window's boundary.
    pub fn range(&self) -> std::ops::Range<i64> {
        let mut l: i64 = 1;
        for w in &self.widths {
            l = lcm(l, *w).unwrap_or(l);
        }
        // wide enough for every time and for `FIRST`
        let n = (i64::MAX / 2) / l;
        -n * l..n * l
    }

    /// The result's ORDER BY, OFFSET and LIMIT, applied to its batches; its hidden columns dropped.
    pub fn finish_batches(&self, batches: Vec<Batch>, pool: &dyn Pool) -> Vec<Batch> {
        let keep = self.limit.map(|l| self.offset.saturating_add(l));
        let mut batches = if self.order.is_empty() { batches } else { sorted(batches, &self.order, keep, pool) };
        let keep = self.columns.len();
        if self.hidden > 0 {
            for b in &mut batches {
                b.cols.truncate(keep);
            }
        }
        let total: usize = batches.iter().map(|b| b.len).sum();
        let end = self.limit.map_or(total, |l| self.offset.saturating_add(l).min(total));
        let start = self.offset.min(end);
        if (start, end) == (0, total) {
            return batches;
        }
        let mut at = 0;
        let mut out = vec![];
        for b in batches {
            let (len, from, to) = (b.len, start.max(at), end.min(at + b.len));
            if from < to {
                out.push(if (from, to) == (at, at + len) { b } else { b.slice(from - at..to - at) });
            }
            at += len;
        }
        out
    }

    /// An error of the engine in the query's terms: its tables by their names, not its streams'.
    pub fn humane(&self, e: &str) -> String {
        let mut out = e.to_string();
        // "__v3: ..." names the view: the query has one name for all of them
        if let Some(rest) = out.strip_prefix("__") {
            if let Some((name, msg)) = rest.split_once(": ") {
                if name.chars().skip(1).all(|c| c.is_ascii_digit()) {
                    out = msg.to_string();
                }
            }
        }
        for s in &self.sources {
            out = out.replace(&s.stream, &s.table.to_string());
        }
        if out.contains("unknown column") {
            let cols: Vec<String> = self
                .sources
                .iter()
                .map(|s| {
                    format!("{} has {}", s.table, s.columns.iter().map(|c| c.0.as_str()).collect::<Vec<_>>().join(", "))
                })
                .collect();
            out = format!("{out} ({})", cols.join("; "));
        }
        out
    }

    /// Why the query cannot be a live view, if it cannot: the live engine matches an as-of join's
    /// left row as it arrives, so the right row a markout wants (at a time offset) is not there
    /// yet and an older one may be gone (keep_versions); and it keeps no window join's range.
    pub fn live(&self) -> R<()> {
        if self.sources.iter().any(|s| matches!(s.clock, Clock::Shifted(_, by) if by != 1)) {
            return Err("a live view's ASOF JOIN takes the latest row at a row's own time; one at a time offset \
                        (t.ts + INTERVAL ...) is a query over the tables"
                .into());
        }
        let lateral = |v: &View| match v.query.body.as_ref() {
            SetExpr::Select(s) => s
                .from
                .iter()
                .flat_map(|f| &f.joins)
                .any(|j| matches!(j.relation, TableFactor::Derived { lateral: true, .. })),
            _ => false,
        };
        if self.catalog.views.iter().any(lateral) {
            return Err("a window join (JOIN LATERAL) is a query over the tables, not a live view".into());
        }
        Ok(())
    }

    /// The pipeline as SQL: its streams and views, as `brrrrr run` would take them.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        for s in self.catalog.streams.values() {
            let cols: Vec<String> = s.columns.iter().map(|c| format!("{} {}", c.name, type_name(&c.ty))).collect();
            let what = match s.kind {
                Kind::Stream => "STREAM",
                _ => "EXTERNAL STREAM",
            };
            let from = self.sources.iter().find(|x| x.stream == s.name).map(|x| {
                let clock = match &x.clock {
                    Clock::Column(c) => format!("in {c} order"),
                    Clock::Row => "in file order".into(),
                    Clock::First => "read first".into(),
                    Clock::Shifted(c, by) => {
                        format!("in {c} {} {} order", if *by < 0 { '-' } else { '+' }, tumble_width(by.abs()))
                    }
                };
                format!(" -- {}, {clock}", x.table)
            });
            let sink = if s.name == self.result { " -- the result" } else { "" };
            out.push_str(&format!(
                "CREATE {what} {} ({});{}{sink}\n",
                s.name,
                cols.join(", "),
                from.unwrap_or_default()
            ));
        }
        for v in &self.catalog.views {
            let emit = match v.emit_delay_us {
                None => String::new(),
                Some(0) => " EMIT AFTER WINDOW CLOSE".into(),
                Some(d) => format!(" EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '{d}' MICROSECOND"),
            };
            let body = asof_text(&v.query.to_string(), &v.asof);
            out.push_str(&format!("CREATE MATERIALIZED VIEW {} INTO {} AS {body}{emit};\n", v.name, v.target));
        }
        out
    }
}

/// The column of each source the run can be split by (`partition`), and the windows over every
/// row the parts then merge: first every stateful step keyed by it; else every one but the
/// windows without a time bucket (`global`, by step), when only views without state read
/// their output, directly or not.
fn partition_merged(
    keyed: &[Option<Vec<(String, String)>>],
    global: &[(usize, String)],
    sources: &[Source],
    cat: &Catalog,
) -> (Option<BTreeMap<String, String>>, Vec<String>) {
    if let Some(p) = partition(keyed, sources) {
        return (Some(p), vec![]);
    }
    let rest: Vec<_> =
        keyed.iter().enumerate().filter(|(i, _)| !global.iter().any(|g| g.0 == *i)).map(|(_, k)| k.clone()).collect();
    let merge: Vec<String> = global.iter().map(|g| g.1.clone()).collect();
    // the views downstream of a merged window: none with state
    let mut reached = merge.clone();
    let mut i = 0;
    while i < reached.len() {
        for v in &cat.views {
            // a relation, or a table function's stream (`tumble(s, ...)`)
            let mut reads = false;
            let _ = ast::visit_relations(&v.query, |r| {
                reads |= r.to_string() == reached[i];
                ControlFlow::<()>::Continue(())
            });
            if let SetExpr::Select(s) = v.query.body.as_ref() {
                for f in &s.from {
                    if let TableFactor::Table { args: Some(a), .. } = &f.relation {
                        reads |= matches!(a.args.first(), Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id)))) if id.value == reached[i]);
                    }
                }
            }
            if !reads {
                continue;
            }
            let joined =
                matches!(v.query.body.as_ref(), SetExpr::Select(s) if s.from.iter().any(|f| !f.joins.is_empty()));
            let over = v.query.to_string().contains(" OVER ");
            if v.emit_delay_us.is_some() || joined || over {
                return (None, vec![]);
            }
            if !reached.contains(&v.target) {
                reached.push(v.target.clone());
            }
        }
        i += 1;
    }
    match partition(&rest, sources) {
        Some(p) if !merge.is_empty() => (Some(p), merge),
        _ => (None, vec![]),
    }
}

/// The column of each source every stateful step is keyed by, if there is one for each and the
/// query has a stateful step at all.
fn partition(keyed: &[Option<Vec<(String, String)>>], sources: &[Source]) -> Option<BTreeMap<String, String>> {
    if keyed.is_empty() {
        return None;
    }
    let mut out = BTreeMap::new();
    for s in sources {
        // the first key of the first step reading it that every step reading it has
        let steps: Vec<&Vec<(String, String)>> = keyed
            .iter()
            .map(Option::as_ref)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .filter(|k| k.iter().any(|(x, _)| *x == s.stream))
            .collect();
        let first = steps.first()?;
        let col = first
            .iter()
            .filter(|(x, _)| *x == s.stream)
            .map(|(_, c)| c)
            .find(|c| steps.iter().all(|k| k.iter().any(|(x, y)| *x == s.stream && y == *c)))?;
        out.insert(s.stream.clone(), col.clone());
    }
    Some(out)
}

fn lcm(a: i64, b: i64) -> Option<i64> {
    fn gcd(a: i64, b: i64) -> i64 {
        if b == 0 {
            a
        } else {
            gcd(b, a % b)
        }
    }
    (a / gcd(a, b)).checked_mul(b)
}

fn type_name(t: &Type) -> String {
    match t {
        Type::Bool => "bool".into(),
        Type::Int(b) => format!("int{b}"),
        Type::UInt(b) => format!("uint{b}"),
        Type::F32 => "float32".into(),
        Type::F64 => "float64".into(),
        Type::Str => "string".into(),
        Type::Time(p) => format!("datetime64({p})"),
        Type::Array(t) => format!("array({})", type_name(t)),
        Type::Map(k, v) => format!("map({}, {})", type_name(k), type_name(v)),
        Type::Nullable(t) => format!("nullable({})", type_name(t)),
        Type::Any => "any".into(),
    }
}

/// `LEFT JOIN` back to `ASOF LEFT JOIN` where the view's flags say so (display only).
fn asof_text(body: &str, flags: &[bool]) -> String {
    let mut out = String::new();
    let mut rest = body;
    let mut k = 0;
    while let Some(i) = rest.find("LEFT JOIN") {
        out.push_str(&rest[..i]);
        if flags.get(k).copied().unwrap_or(false) && !rest[i..].starts_with("LEFT JOIN LATERAL") {
            out.push_str("ASOF ");
        }
        out.push_str("LEFT JOIN");
        rest = &rest[i + "LEFT JOIN".len()..];
        k += 1;
    }
    out.push_str(rest);
    out
}

/// The text with each table given as a string (`FROM 'x.parquet'`) made an identifier
/// (`__file0`), and `ASOF [LEFT|INNER] JOIN` made `GLOBAL LEFT|INNER JOIN`, a join sqlparser
/// parses and nothing else here writes; returns the files in order.
fn prepare(sql: &str) -> R<(String, Vec<String>)> {
    let toks = Tokenizer::new(&GenericDialect {}, sql).tokenize().map_err(|e| e.to_string())?;
    let sig: Vec<usize> = (0..toks.len()).filter(|&i| !matches!(toks[i], Token::Whitespace(_))).collect();
    let word = |i: usize, w: &str| matches!(&toks[i], Token::Word(x) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w));
    let ident = |s: &str| {
        Token::Word(Word { value: s.into(), quote_style: None, keyword: sqlparser::keywords::Keyword::NoKeyword })
    };
    let (mut out, mut files) = (toks.clone(), vec![]);
    for (n, &i) in sig.iter().enumerate() {
        if word(i, "GLOBAL") {
            return Err("GLOBAL joins are not supported".into());
        }
        if let Token::SingleQuotedString(path) = &toks[i] {
            if n > 0 && (word(sig[n - 1], "FROM") || word(sig[n - 1], "JOIN")) {
                out[i] = ident(&format!("__file{}", files.len()));
                files.push(path.clone());
            }
        }
        if word(i, "ASOF") {
            let raw = |v: &str| {
                Token::Word(Word {
                    value: v.into(),
                    quote_style: None,
                    keyword: sqlparser::keywords::Keyword::NoKeyword,
                })
            };
            // Snowflake's `ASOF JOIN r MATCH_CONDITION (l.t >= r.t) [ON keys]`: the condition
            // goes into ON, and the join keeps every left row
            let mut depth = 0i32;
            let mut matched = None;
            for (k, &j) in sig.iter().enumerate().skip(n + 1) {
                match &toks[j] {
                    Token::LParen => depth += 1,
                    Token::RParen if depth == 0 => break,
                    Token::RParen => depth -= 1,
                    _ if depth == 0 && word(j, "MATCH_CONDITION") => {
                        matched = Some(k);
                        break;
                    }
                    _ if depth == 0
                        && ["WHERE", "GROUP", "ORDER", "LIMIT", "UNION", "HAVING", "QUALIFY"]
                            .iter()
                            .any(|w| word(j, w)) =>
                    {
                        break
                    }
                    _ if depth == 0 && k > n + 2 && word(j, "JOIN") => break,
                    _ => {}
                }
            }
            if let Some(k) = matched {
                if !matches!(sig.get(k + 1).map(|&j| &toks[j]), Some(Token::LParen)) {
                    return Err("MATCH_CONDITION needs a condition in parentheses".into());
                }
                let (mut d, mut close) = (0, None);
                for (m, &j) in sig.iter().enumerate().skip(k + 1) {
                    match toks[j] {
                        Token::LParen => d += 1,
                        Token::RParen => d -= 1,
                        _ => {}
                    }
                    if d == 0 {
                        close = Some(m);
                        break;
                    }
                }
                let close = close.ok_or("MATCH_CONDITION's parenthesis does not close")?;
                let cond: String = toks[sig[k + 1] + 1..sig[close]].iter().map(token_sql).collect();
                for t in &mut out[sig[k]..=sig[close]] {
                    *t = Token::Whitespace(sqlparser::tokenizer::Whitespace::Space);
                }
                match sig.get(close + 1).copied().filter(|&j| word(j, "ON")) {
                    Some(on) => out[on] = raw(&format!("ON ({cond}) AND")),
                    None => out[sig[close]] = raw(&format!(" ON ({cond})")),
                }
                // ASOF [LEFT|INNER] JOIN: a left join either way
                out[i] = raw("GLOBAL");
                if let Some(&j) = sig.get(n + 1) {
                    if word(j, "INNER") {
                        out[j] = raw("LEFT");
                    } else if word(j, "JOIN") {
                        out[i] = raw("GLOBAL LEFT");
                    }
                }
                continue;
            }
            let next = sig.get(n + 1).copied();
            match next {
                Some(j) if word(j, "LEFT") || word(j, "INNER") => out[i] = ident("GLOBAL"),
                Some(j) if word(j, "JOIN") => out[i] = raw("GLOBAL INNER"),
                _ => {}
            }
        }
    }
    Ok((out.iter().map(token_sql).collect(), files))
}

/// A relation as a stream: its name and columns.
#[derive(Clone, Debug)]
struct Rel {
    stream: String,
    cols: Vec<String>,
}

/// A relation of a SELECT: the stream it reads and the qualifier its columns go by.
struct Scoped {
    rel: Rel,
    qualifier: Option<String>,
}

struct Ctx<'a> {
    cat: Catalog,
    sources: Vec<Source>,
    files: Vec<String>,
    ctes: HashMap<String, Rel>,
    /// Named and file tables read already: their source stream.
    tables: HashMap<Table, String>,
    resolve: &'a mut Resolve<'a>,
    /// The values of a `PIVOT` without `IN` (`compile_with`).
    values: &'a mut Values<'a>,
    /// Per stream column, the source columns it is a copy of (one per side of a UNION ALL).
    lineage: HashMap<(String, String), Vec<(String, String)>>,
    widths: Vec<i64>,
    /// Stream columns whose values come in order (a window's bucket, and copies of it), beside
    /// the sources' time columns: a window function may order by them.
    ordered: std::collections::HashSet<(String, String)>,
    /// Streams whose rows come out of time order: `lead`'s, each row when its next ones come,
    /// and what reads them. No time operation reads them.
    unordered: std::collections::HashSet<String>,
    /// The windows without a time bucket: (their step in `keyed`, their stream).
    global_windows: Vec<(usize, String)>,
    /// The streams of windows over every row, with the row each gives over no rows.
    global: HashMap<String, Row>,
    /// Each stateful step's keys (a window's, a join's, a window function's partition), as
    /// the source columns they copy; `None` for a step keyed by none (a window over every row).
    keyed: Vec<Option<Vec<(String, String)>>>,
    n: usize,
}

/// Gives a table's columns (from its files' schema).
pub type Resolve<'a> = dyn FnMut(&Table) -> R<Vec<(String, Type)>> + 'a;

/// Runs a query (`SELECT DISTINCT col AS v FROM ... ORDER BY v`) and gives its column's values:
/// the columns of a `PIVOT` that does not name them (`IN (...)`), which must be known first.
pub type Values<'a> = dyn FnMut(&str) -> R<Vec<Value>> + 'a;

/// Compiles one ad-hoc query, as `compile_with`, its `PIVOT`s naming their values.
pub fn compile(sql: &str, resolve: &mut Resolve<'_>) -> R<Compiled> {
    compile_with(sql, resolve, &mut |_| Err("PIVOT without IN (...): name its values, IN ('A', 'B')".into()))
}

/// Compiles one ad-hoc query; `resolve` gives a table's columns (from its files' schema), and
/// `values` the values of a `PIVOT` that does not name them.
pub fn compile_with(sql: &str, resolve: &mut Resolve<'_>, values: &mut Values<'_>) -> R<Compiled> {
    let pivot = simple_pivot(sql, values)?;
    let (text, files) = prepare(pivot.as_deref().unwrap_or(sql))?;
    too_deep(&text)?;
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &text).map_err(|e| match e {
        sqlparser::parser::ParserError::RecursionLimitExceeded => {
            "the query is too large to run: parentheses or subqueries nested more than 50 deep".to_string()
        }
        e => e.to_string(),
    })?;
    let q = match (stmts.pop(), stmts.is_empty()) {
        (Some(Statement::Query(q)), true) => q,
        (Some(_), true) => return Err("only SELECT queries run here".into()),
        (None, _) => return Err("no query".into()),
        _ => return Err("one query at a time".into()),
    };
    let mut q = *q;
    let mut order = q.order_by.take();
    let (limit, offset) = limit_offset(q.limit_clause.take())?;
    // ORDER BY an expression the SELECT does not show: a column of its own, dropped at the end
    let mut hidden = 0;
    if let (Some(ob), SetExpr::Select(sel)) = (order.as_mut(), q.body.as_mut()) {
        if let ast::OrderByKind::Expressions(es) = &mut ob.kind {
            let shown: Vec<String> = sel
                .projection
                .iter()
                .flat_map(|i| match i {
                    SelectItem::UnnamedExpr(x) => vec![item_name(x), x.to_string()],
                    SelectItem::ExprWithAlias { expr, alias } => vec![alias.value.clone(), expr.to_string()],
                    _ => vec![],
                })
                .collect();
            let star =
                sel.projection.iter().any(|i| matches!(i, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)));
            let all = matches!(es.as_slice(), [e] if matches!(&e.expr, Expr::Identifier(i) if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("all")));
            for e in es.iter_mut() {
                let name = item_name(&e.expr);
                let positional = matches!(&e.expr, Expr::Value(v) if matches!(v.value, AstValue::Number(..)));
                if all || positional || star || shown.contains(&name) || shown.contains(&e.expr.to_string()) {
                    continue;
                }
                let alias = format!("__order{hidden}");
                sel.projection.push(SelectItem::ExprWithAlias { expr: e.expr.clone(), alias: Ident::new(&alias) });
                e.expr = ident(&alias);
                hidden += 1;
            }
        }
    }
    let mut c = Ctx {
        cat: Catalog::default(),
        sources: vec![],
        files,
        ctes: HashMap::new(),
        tables: HashMap::new(),
        resolve,
        values,
        lineage: HashMap::new(),
        widths: vec![],
        ordered: Default::default(),
        unordered: Default::default(),
        global: HashMap::new(),
        keyed: vec![],
        global_windows: vec![],
        n: 0,
    };
    // the items' texts, which an ORDER BY may name (`column_index`)
    let texts: Vec<String> = match q.body.as_ref() {
        SetExpr::Select(s) => s
            .projection
            .iter()
            .flat_map(|i| match i {
                SelectItem::UnnamedExpr(x) | SelectItem::ExprWithAlias { expr: x, .. } => Some(x.to_string()),
                _ => None,
            })
            .collect(),
        _ => vec![],
    };
    // the query taken apart as it is compiled: no copy of it (a copy recurses down it, as deep
    // as it nests: thousands of UNION ALLs)
    let rel = c.query(q)?;
    // the last views write the result: an external stream, as a sink is
    let result = rel.stream.clone();
    if c.sources.iter().any(|s| s.stream == result) {
        return Err("internal: a query must have a view".into());
    }
    let sink = c.cat.streams.get_mut(&result).expect("the query's stream");
    sink.kind = Kind::External;
    sink.settings = [("type", "kafka"), ("topic", "__result"), ("data_format", "JSONEachRow")]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let order = match order {
        None => vec![],
        Some(ob) => {
            let es = match ob.kind {
                ast::OrderByKind::Expressions(es) => es,
                ast::OrderByKind::All(_) => vec![],
            };
            // ORDER BY ALL: by every column, left to right
            let all = es.is_empty()
                || matches!(es.as_slice(), [e] if matches!(&e.expr, Expr::Identifier(i) if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("all")));
            if all {
                let desc = es.first().is_some_and(|e| matches!(e.options.sort, Some(ast::OrderBySort::Desc)));
                (0..rel.cols.len() - hidden).map(|i| (i, desc, desc)).collect::<Vec<_>>()
            } else {
                es.iter()
                    .map(|e| {
                        let i = column_index(&e.expr, &rel.cols, &texts)?;
                        let desc = matches!(e.options.sort, Some(ast::OrderBySort::Desc));
                        // NULLs sort as the largest value, as in PostgreSQL
                        Ok((i, desc, e.options.nulls_first.unwrap_or(desc)))
                    })
                    .collect::<R<_>>()?
            }
        }
    };
    // every source a clock: what no time operation chose
    for s in &mut c.sources {
        let (name, ty) = match s.clock {
            Clock::Column(_) => continue,
            Clock::Shifted(..) => ("__clock", Type::Time(6)),
            _ => ("__row", Type::Int(64)),
        };
        let st = c.cat.streams.get_mut(&s.stream).expect("a source's stream");
        st.columns.push(Column { name: name.into(), ty, materialized: None, default: None });
    }
    let (partition, merge) = partition_merged(&c.keyed, &c.global_windows, &c.sources, &c.cat);
    let empty = c.global.get(&result).cloned();
    let compiled = Compiled {
        catalog: c.cat,
        sources: c.sources,
        result,
        hidden,
        columns: rel.cols[..rel.cols.len() - hidden].to_vec(),
        order,
        limit,
        offset,
        widths: c.widths,
        empty,
        partition,
        merge,
    };
    // planned now: what the engine refuses is refused before anything is read
    crate::engine::Engine::new(&compiled.catalog).map_err(|e| compiled.humane(&e))?;
    Ok(compiled)
}

/// How deep a query's expressions may nest (`1 + 1 + ...`, parentheses, subqueries), as
/// DuckDB's `max_expression_depth`, and how many branches a UNION ALL may have: compiling a
/// query, and dropping it, recurses down them, and a deeper one would overflow a thread's stack
/// (a server's request) rather than be refused.
pub const MAX_DEPTH: usize = 1000;
pub const MAX_UNION: usize = 10_000;
/// The most buckets a `time_bucket_gapfill` range may hold, each key's.
pub const MAX_GAP_BUCKETS: i64 = 10_000_000;

/// An error if `text` nests past `MAX_DEPTH` or `MAX_UNION`, told from its tokens before it is
/// parsed (a parse would build the very tree too deep to walk): an expression's depth is at most
/// its operators', those of each parenthesis it is in added, and a parenthesis (a call, a
/// subquery) nests at most sqlparser's recursion limit deep.
fn too_deep(text: &str) -> R<()> {
    let Ok(toks) = Tokenizer::new(&GenericDialect {}, text).tokenize() else { return Ok(()) };
    // per open parenthesis: the operators of its item so far, its set operations
    let mut open: Vec<(usize, usize)> = vec![(0, 0)];
    for t in &toks {
        let word = |w: &str| matches!(t, Token::Word(x) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w));
        let depth = open.len();
        let top = open.last_mut().expect("the query's own");
        match t {
            Token::LParen => open.push((0, 0)),
            Token::RParen if depth > 1 => {
                open.pop();
            }
            Token::Comma => top.0 = 0,
            _ if ["UNION", "INTERSECT", "EXCEPT"].iter().any(|w| word(w)) => {
                top.1 += 1;
                if top.1 >= MAX_UNION {
                    return Err(format!("the query is too large to run: a UNION of more than {MAX_UNION} queries"));
                }
            }
            Token::Plus
            | Token::Minus
            | Token::Mul
            | Token::Div
            | Token::Mod
            | Token::StringConcat
            | Token::Eq
            | Token::Neq
            | Token::Lt
            | Token::Gt
            | Token::LtEq
            | Token::GtEq => top.0 += 1,
            _ if ["AND", "OR", "NOT", "IS", "LIKE", "ILIKE", "IN", "BETWEEN"].iter().any(|w| word(w)) => top.0 += 1,
            _ => {}
        }
        if open.iter().map(|o| o.0).sum::<usize>() > MAX_DEPTH {
            return Err(format!("the query is too large to run: expressions nested more than {MAX_DEPTH} deep"));
        }
    }
    Ok(())
}

fn limit_offset(l: Option<ast::LimitClause>) -> R<(Option<usize>, usize)> {
    let num = |e: &Expr| -> R<usize> {
        match e {
            Expr::Value(v) => match &v.value {
                AstValue::Number(n, _) => n.parse().map_err(|_| format!("LIMIT/OFFSET {n} is not a whole number")),
                v => Err(format!("LIMIT/OFFSET {v} is not a whole number")),
            },
            e => Err(format!("LIMIT/OFFSET {e} is not a whole number")),
        }
    };
    Ok(match l {
        None => (None, 0),
        Some(ast::LimitClause::LimitOffset { limit, offset, limit_by }) => {
            if !limit_by.is_empty() {
                return Err("LIMIT BY is not supported".into());
            }
            (limit.as_ref().map(num).transpose()?, offset.as_ref().map(|o| num(&o.value)).transpose()?.unwrap_or(0))
        }
        Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => (Some(num(&limit)?), num(&offset)?),
    })
}

/// An ORDER BY item of the outermost query as a result column: its name, alias, position
/// (1-based) or the very expression of an item (`texts`).
fn column_index(e: &Expr, cols: &[String], texts: &[String]) -> R<usize> {
    if let Expr::Value(v) = e {
        if let AstValue::Number(n, _) = &v.value {
            return n
                .parse::<usize>()
                .ok()
                .filter(|i| (1..=cols.len()).contains(i))
                .map(|i| i - 1)
                .ok_or(format!("ORDER BY {n}: there are {} columns", cols.len()));
        }
    }
    let name = match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        Expr::CompoundIdentifier(ids) => ids.last().map(|i| i.value.clone()),
        _ => None,
    };
    if let Some(i) = name.and_then(|n| cols.iter().position(|c| *c == n)) {
        return Ok(i);
    }
    if let Some(i) = texts.iter().position(|t| *t == e.to_string()) {
        return Ok(i);
    }
    Err(format!("ORDER BY {e}: order by a column of the result (its name, alias or position)"))
}

fn ident(s: &str) -> Expr {
    Expr::Identifier(Ident::new(s))
}

fn table(name: &str, alias: Option<&str>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![Ident::new(name)]),
        alias: alias.map(|a| TableAlias { explicit: true, name: Ident::new(a), columns: vec![], at: None }),
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

/// The name an item's column goes by: its alias, a column's name, or the expression's text.
fn item_name(e: &Expr) -> String {
    match e {
        Expr::Identifier(i) => i.value.clone(),
        Expr::CompoundIdentifier(ids) => ids.last().map_or(String::new(), |i| i.value.clone()),
        e => e.to_string(),
    }
}

/// One query parsed.
fn parse_query(text: &str) -> R<Query> {
    match Parser::parse_sql(&GenericDialect {}, text).map_err(|e| e.to_string())?.pop() {
        Some(Statement::Query(q)) => Ok(*q),
        _ => Err(format!("internal: {text} is not a query")),
    }
}

/// A PIVOT's value's column name: a string's text, a number's, else the expression's.
fn value_name(e: &Expr) -> String {
    match e {
        Expr::Value(v) => match &v.value {
            AstValue::SingleQuotedString(s) => s.clone(),
            AstValue::Number(n, _) => n.clone(),
            AstValue::Null => "NULL".into(),
            v => v.to_string(),
        },
        e => e.to_string(),
    }
}

/// A PIVOT's values, the column of the query `sql` runs aside (`Values`), as (name, literal).
fn distinct(values: &mut Values<'_>, sql: &str) -> R<Vec<(String, Expr)>> {
    let lit = |v: &Value| match v {
        Value::Null => Expr::value(AstValue::Null),
        Value::Bool(b) => Expr::value(AstValue::Boolean(*b)),
        Value::Int(_) | Value::UInt(_) | Value::F64(_) | Value::F32(_) => number_text(&text(v)),
        v => Expr::value(AstValue::SingleQuotedString(crate::format::text_value(v).to_string())),
    };
    Ok(values(sql)?.iter().map(|v| (text(v), lit(v))).collect())
}

/// A PIVOT's columns, each value's aggregates (name, SQL): `agg FILTER (WHERE col = value)`,
/// named the value's name with a single aggregate without alias, else `value_alias`.
fn pivot_items(aggs: &[(Expr, Option<String>)], col: &Expr, values: &[(String, Expr)]) -> R<Vec<(String, String)>> {
    if values.is_empty() {
        return Err("PIVOT on no values".into());
    }
    let mut out = vec![];
    for (name, v) in values {
        let cond = match v {
            Expr::Value(x) if x.value == AstValue::Null => Expr::IsNull(Box::new(col.clone())),
            v => Expr::BinaryOp { left: Box::new(col.clone()), op: BinaryOperator::Eq, right: Box::new(v.clone()) },
        };
        for (agg, alias) in aggs {
            let mut agg = agg.clone();
            let Expr::Function(f) = &mut agg else { return Err(format!("PIVOT ... USING an aggregate, not {agg}")) };
            f.filter = Some(Box::new(match f.filter.take() {
                Some(c) => and(Expr::Nested(c), cond.clone()),
                None => cond.clone(),
            }));
            let column = match (alias, aggs.len()) {
                (Some(a), _) => format!("{name}_{a}"),
                (None, 1) => name.clone(),
                (None, _) => format!("{name}_{}", item_name(&aggs[out.len() % aggs.len()].0)),
            };
            out.push((column, agg.to_string()));
        }
    }
    Ok(out)
}

/// DuckDB's simplified PIVOT statement, `PIVOT t ON col [IN (values)] USING agg [AS name], ...
/// [GROUP BY keys] [ORDER BY ...] [LIMIT ...]`, as the SELECT it is: with GROUP BY, the keys and
/// each value's aggregates (`pivot_items`) grouped by them; without, SQL's `PIVOT (agg FOR col
/// IN (values))`, which groups by every other column. Values not named are run for (`values`).
fn simple_pivot(sql: &str, values: &mut Values<'_>) -> R<Option<String>> {
    let Ok(toks) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else { return Ok(None) };
    let sig: Vec<usize> = (0..toks.len()).filter(|&i| !matches!(toks[i], Token::Whitespace(_))).collect();
    let word = |n: usize, w: &str| matches!(sig.get(n).map(|&i| &toks[i]), Some(Token::Word(x)) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w));
    if !word(0, "PIVOT") {
        return Ok(None);
    }
    // the clauses at the top level
    let (mut depth, mut at) = (0i32, HashMap::new());
    for n in 0..sig.len() {
        match toks[sig[n]] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth == 0 => {
                for (k, w) in [("on", "ON"), ("using", "USING"), ("limit", "LIMIT")] {
                    if word(n, w) {
                        at.entry(k).or_insert(n);
                    }
                }
                for (k, w) in [("group", "GROUP"), ("order", "ORDER")] {
                    if word(n, w) && word(n + 1, "BY") {
                        at.entry(k).or_insert(n);
                    }
                }
            }
            _ => {}
        }
    }
    let usage = "PIVOT table ON column [IN (values)] USING aggregate [GROUP BY keys]";
    let (Some(&on), Some(&using)) = (at.get("on"), at.get("using")) else { return Err(usage.into()) };
    let text = |a: usize, b: usize| -> String {
        if a >= b || a >= sig.len() {
            return String::new();
        }
        toks[sig[a]..sig.get(b).copied().unwrap_or(toks.len())]
            .iter()
            .map(token_sql)
            .collect::<String>()
            .trim()
            .to_string()
    };
    let end = |from: usize| {
        ["group", "order", "limit"]
            .iter()
            .filter_map(|k| at.get(k).copied())
            .filter(|&n| n > from)
            .min()
            .unwrap_or(sig.len())
    };
    let (group, order) = (at.get("group").copied(), ["order", "limit"].iter().filter_map(|k| at.get(k).copied()).min());
    if on > using || group.is_some_and(|g| g < using) {
        return Err(usage.into());
    }
    let source = text(1, on);
    // ON col [IN (values)]
    let parser = |t: &str| Parser::new(&GenericDialect {}).try_with_sql(t).map_err(|e| e.to_string());
    let into = (on + 1..using).find(|&n| word(n, "IN"));
    let on_text = text(on + 1, into.unwrap_or(using));
    let mut p = parser(&on_text)?;
    let col = p.parse_expr().map_err(|e| e.to_string())?;
    if p.peek_token().token != Token::EOF {
        return Err(format!("PIVOT ... ON one column [IN (values)], not {on_text}"));
    }
    let named = match into {
        Some(i) => {
            let mut p = parser(&text(i + 1, using))?;
            p.expect_token(&Token::LParen).map_err(|e| e.to_string())?;
            let l = p
                .parse_comma_separated(|p| {
                    let e = p.parse_expr()?;
                    let a = if p.parse_keyword(sqlparser::keywords::Keyword::AS) {
                        Some(p.parse_identifier()?.value)
                    } else {
                        None
                    };
                    Ok((a.unwrap_or_else(|| value_name(&e)), e))
                })
                .map_err(|e| e.to_string())?;
            p.expect_token(&Token::RParen).map_err(|e| e.to_string())?;
            Some(l)
        }
        None => None,
    };
    let values = match named {
        Some(l) => l,
        None => distinct(values, &format!("SELECT DISTINCT {col} AS v FROM {source} ORDER BY v"))?,
    };
    let aggs: Vec<(Expr, Option<String>)> = parser(&text(using + 1, end(using)))?
        .parse_comma_separated(|p| {
            let e = p.parse_expr()?;
            let a = if p.parse_keyword(sqlparser::keywords::Keyword::AS) {
                Some(p.parse_identifier()?.value)
            } else {
                None
            };
            Ok((e, a))
        })
        .map_err(|e| e.to_string())?;
    let tail = order.map_or(String::new(), |o| text(o, sig.len()));
    let items = pivot_items(&aggs, &col, &values)?;
    let columns: Vec<String> = items.iter().map(|(n, i)| format!("{i} AS {}", Ident::with_quote('"', n))).collect();
    Ok(Some(match group {
        Some(g) => {
            let keys = text(g + 2, end(g + 1));
            format!("SELECT {keys}, {} FROM {source} GROUP BY {keys} {tail}", columns.join(", "))
        }
        None => {
            let aggs: Vec<String> =
                aggs.iter().map(|(e, a)| a.as_ref().map_or(e.to_string(), |a| format!("{e} AS {a}"))).collect();
            let vals: Vec<String> =
                values.iter().map(|(n, e)| format!("{e} AS {}", Ident::with_quote('"', n))).collect();
            format!("SELECT * FROM {source} PIVOT ({} FOR {col} IN ({})) {tail}", aggs.join(", "), vals.join(", "))
        }
    }))
}

/// `name`, or `name_1`, `name_2`, ... if it is taken.
fn unique(name: &str, taken: &[String]) -> String {
    if !taken.iter().any(|t| t == name) {
        return name.to_string();
    }
    (1..).map(|i| format!("{name}_{i}")).find(|n| !taken.contains(n)).expect("a free name")
}

fn function_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Function(f) => Some(f.name.to_string().to_ascii_lowercase()),
        _ => None,
    }
}

fn args(e: &Expr) -> Vec<Expr> {
    let Expr::Function(f) = e else { return vec![] };
    let FunctionArguments::List(l) = &f.args else { return vec![] };
    l.args
        .iter()
        .filter_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => Some(x.clone()),
            _ => None,
        })
        .collect()
}

/// Whether `e` calls an aggregate (outside window functions).
fn has_aggregate(e: &Expr) -> bool {
    let mut found = false;
    let mut e = e.clone();
    let _ = visit_expressions_mut(&mut e, |x| {
        found |= matches!(x, Expr::Function(f) if is_aggregate_call(f));
        ControlFlow::<()>::Continue(())
    });
    found
}

/// Whether `f` is an aggregate (not a window function).
fn is_aggregate_call(f: &ast::Function) -> bool {
    let name = f.name.to_string().to_ascii_lowercase();
    f.over.is_none()
        && (crate::agg::is_aggregate(&name)
            || crate::agg::if_base(&name).is_some()
            || AGG_ALIASES.contains(&name.as_str()))
}

/// Aggregates under other names (`expr::Compiler` maps them).
const AGG_ALIASES: &[&str] = &[
    "first",
    "last",
    "max_by",
    "min_by",
    "mean",
    "approx_quantile",
    "percentile_cont",
    "var",
    "approx_count_distinct",
];

/// The engine's name for a function people know by another (DuckDB's, TimescaleDB's, kdb+'s).
fn engine_name(name: &str) -> Option<&'static str> {
    Some(match name {
        "first" | "min_by" => "arg_min",
        "last" | "max_by" => "arg_max",
        "mean" => "avg",
        "stddev_samp" => "stddev",
        "var_samp" | "var" => "variance",
        // exact: as many distinct values as fit in memory
        "approx_count_distinct" => "uniq_exact",
        _ => return None,
    })
}

fn parametric(name: &str, level: Expr, arg: Expr) -> Expr {
    let mut call = crate::expr::call_of(name, vec![arg]);
    if let Expr::Function(f) = &mut call {
        f.parameters = FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(level))],
            clauses: vec![],
        });
    }
    call
}

/// Renames the functions of `e` the engine knows by another name, and writes the ones it
/// takes in another form as that form: `median(x)` and `quantile_cont(x, p)` as
/// `quantile_exact(p)(x)` (DuckDB's percentile, exact however many values), `count(DISTINCT x)`
/// as `uniq_exact(x)`, `ema(x, n) OVER (...)` as the engine's `avg(x, 'period', n) OVER (...)`.
fn rename_functions(e: &mut Expr) {
    let _ = visit_expressions_mut(e, |x| {
        let Expr::Function(f) = x else { return ControlFlow::<()>::Continue(()) };
        // `count(DISTINCT x)` is `uniq_exact(x)`, `min`/`max(DISTINCT x)` are `min`/`max(x)`; any
        // other DISTINCT stays, for the compiler to refuse
        if let FunctionArguments::List(l) = &mut f.args {
            if l.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct) {
                match f.name.to_string().to_ascii_lowercase().as_str() {
                    "count" => f.name = ObjectName::from(vec![Ident::new("uniq_exact")]),
                    "min" | "max" => {}
                    _ => return ControlFlow::Continue(()),
                }
                l.duplicate_treatment = None;
            }
        }
        if let Some(p) = percentile(f) {
            *x = p;
        }
        let Expr::Function(f) = x else { unreachable!("a call") };
        // `agg(x) FILTER (WHERE c)` is the engine's `agg_if(x, c)`
        if let Some(c) = f.filter.take() {
            let name = f.name.to_string().to_ascii_lowercase();
            let name = engine_name(&name).map(str::to_string).unwrap_or(name);
            let mut a = args(&Expr::Function(f.clone()));
            let params = std::mem::replace(&mut f.parameters, FunctionArguments::None);
            // of two arguments (`last(x, ts)`, `vwap`): a row whose second is NULL is none
            let (name, a) = match a.as_mut_slice() {
                [_, b] if crate::agg::arity(&name) == 2 => {
                    *b = Expr::Case {
                        case_token: ast::helpers::attached_token::AttachedToken::empty(),
                        end_token: ast::helpers::attached_token::AttachedToken::empty(),
                        operand: None,
                        conditions: vec![ast::CaseWhen { condition: (*c).clone(), result: b.clone() }],
                        else_result: None,
                    };
                    (name, a)
                }
                _ => {
                    a.push(*c);
                    (format!("{name}_if"), a)
                }
            };
            let mut call = crate::expr::call_of(&name, a);
            if let Expr::Function(g) = &mut call {
                g.parameters = params;
            }
            *x = call;
            return ControlFlow::Continue(());
        }
        let name = f.name.to_string().to_ascii_lowercase();
        match (name.as_str(), args(&Expr::Function(f.clone())).as_slice(), &f.parameters) {
            ("ema", [v, n], FunctionArguments::None) if f.over.is_some() => {
                let kind = match n {
                    Expr::Value(val) => match &val.value {
                        AstValue::Number(t, _) if t.parse::<f64>().is_ok_and(|k| k < 1.0) => "alpha",
                        _ => "period",
                    },
                    _ => "period",
                };
                let mut call = crate::expr::call_of("avg", vec![v.clone(), string(kind), n.clone()]);
                if let Expr::Function(g) = &mut call {
                    g.over = f.over.clone();
                }
                *x = call;
            }
            _ => {
                if let Some(n) = engine_name(&name) {
                    f.name = ObjectName::from(vec![Ident::new(n)]);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// A percentile in DuckDB's spellings as the engine's call, with its OVER and FILTER:
/// `median(x)`, `quantile_cont(x, p)` and `percentile_cont(p) WITHIN GROUP (ORDER BY x)` as
/// `quantile_exact(p)(x)`, `approx_quantile(x, p)` as `quantile(p)(x)` (a sample).
fn percentile(f: &ast::Function) -> Option<Expr> {
    if !matches!(f.parameters, FunctionArguments::None) {
        return None;
    }
    let name = f.name.to_string().to_ascii_lowercase();
    let (to, level, arg) = match (name.as_str(), args(&Expr::Function(f.clone())).as_slice()) {
        ("median", [v]) => ("quantile_exact", number_text("0.5"), v.clone()),
        ("quantile_cont" | "quantile" | "percentile_cont", [v, p]) => ("quantile_exact", p.clone(), v.clone()),
        ("approx_quantile", [v, p]) => ("quantile", p.clone(), v.clone()),
        ("percentile_cont", [p]) if f.within_group.len() == 1 => {
            ("quantile_exact", p.clone(), f.within_group[0].expr.clone())
        }
        _ => return None,
    };
    let mut call = parametric(to, level, arg);
    if let Expr::Function(g) = &mut call {
        (g.over, g.filter) = (f.over.clone(), f.filter.clone());
    }
    Some(call)
}

fn number_text(n: &str) -> Expr {
    Expr::value(AstValue::Number(n.into(), false))
}

fn has_window_function(e: &Expr) -> bool {
    let mut found = false;
    let mut e = e.clone();
    let _ = visit_expressions_mut(&mut e, |x| {
        if matches!(x, Expr::Function(f) if f.over.is_some()) {
            found = true;
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

/// A width argument (`'1m'`, `'15s'`, `INTERVAL '5' MINUTE`, `INTERVAL '1 hour'`) in µs.
pub fn width_us(e: &Expr) -> R<i64> {
    let text = match e {
        Expr::Value(v) => match &v.value {
            AstValue::SingleQuotedString(s) => s.clone(),
            v => return Err(format!("a bucket width is a duration ('1m', '15s', INTERVAL '1 hour'), not {v}")),
        },
        Expr::Interval(i) => {
            let v = match i.value.as_ref() {
                Expr::Value(v) => match &v.value {
                    AstValue::SingleQuotedString(s) | AstValue::Number(s, _) => s.clone(),
                    v => return Err(format!("unsupported interval {v}")),
                },
                v => return Err(format!("unsupported interval {v}")),
            };
            match &i.leading_field {
                Some(f) => format!("{v} {f}"),
                None => v,
            }
        }
        e => return Err(format!("a bucket width is a duration ('1m', '15s', INTERVAL '1 hour'), not {e}")),
    };
    duration(&text).ok_or(format!("{text:?} is not a duration: write '100ms', '15s', '1m', '4h', '1d' or '1 minute'"))
}

pub use crate::value::duration_us as duration;

/// A width as the engine's `tumble` takes it.
fn tumble_width(us: i64) -> String {
    for (unit, n) in [("d", 86_400_000_000), ("h", 3_600_000_000), ("m", 60_000_000), ("s", 1_000_000), ("ms", 1_000)] {
        if us % n == 0 {
            return format!("{}{unit}", us / n);
        }
    }
    format!("{us}us")
}

impl Ctx<'_> {
    fn fresh(&mut self, what: &str) -> String {
        self.n += 1;
        format!("__{what}{}", self.n)
    }

    /// A new internal stream of `cols` (whatever their values).
    fn stream(&mut self, name: &str, cols: &[String]) {
        let columns =
            cols.iter().map(|c| Column { name: c.clone(), ty: Type::Any, materialized: None, default: None }).collect();
        self.cat
            .streams
            .insert(name.into(), Stream { name: name.into(), kind: Kind::Stream, columns, settings: BTreeMap::new() });
    }

    fn view(&mut self, target: &str, mut query: Query, windowed: bool) {
        // `x AS x` as `x`: an alias hides its column's type from the engine (which reads a text
        // literal compared with a time column as a time)
        if let SetExpr::Select(sel) = query.body.as_mut() {
            for item in &mut sel.projection {
                if let SelectItem::ExprWithAlias { expr, alias } = item {
                    let same = match expr {
                        Expr::Identifier(i) => i.value == alias.value,
                        Expr::CompoundIdentifier(ids) => ids.last().is_some_and(|i| i.value == alias.value),
                        _ => false,
                    };
                    if same {
                        *item = SelectItem::UnnamedExpr(expr.clone());
                    }
                }
            }
        }
        let name = self.fresh("v");
        let asof = count_left_joins(&query);
        self.cat.views.push(View {
            name,
            target: target.into(),
            query,
            asof: vec![true; asof],
            emit_delay_us: windowed.then_some(0),
            settings: BTreeMap::new(),
        });
    }

    /// A source stream for `t`, read once however often the query names it.
    fn source(&mut self, t: Table) -> R<Rel> {
        if t == Table::Named(ONE_ROW.into()) && !self.tables.contains_key(&t) {
            let stream = self.fresh("one");
            let columns = vec![Column { name: "__one".into(), ty: Type::Int(64), materialized: None, default: None }];
            let settings = [
                ("type", "kafka"),
                ("topic", stream.as_str()),
                ("data_format", "ProtobufSingle"),
                ("format_schema", "-"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            self.cat
                .streams
                .insert(stream.clone(), Stream { name: stream.clone(), kind: Kind::External, columns, settings });
            self.sources.push(Source {
                stream: stream.clone(),
                table: t.clone(),
                columns: vec![("__one".into(), Type::Int(64))],
                clock: Clock::Row,
            });
            self.tables.insert(t, stream.clone());
            return Ok(Rel { stream, cols: vec!["__one".into()] });
        }
        if let Some(s) = self.tables.get(&t) {
            let cols = self.sources.iter().find(|x| x.stream == *s).expect("a source").columns.clone();
            return Ok(Rel { stream: s.clone(), cols: cols.into_iter().map(|c| c.0).collect() });
        }
        let cols = (self.resolve)(&t)?;
        if cols.is_empty() {
            return Err(format!("{t} has no columns"));
        }
        Ok(self.new_source(t, cols, true))
    }

    /// A source stream for `t` that no other step reads (an as-of join's right side at an
    /// offset, read in the order of its shifted time).
    fn own_source(&mut self, t: Table) -> R<Rel> {
        let cols = match self.sources.iter().find(|s| s.table == t) {
            Some(s) => s.columns.clone(),
            None => (self.resolve)(&t)?,
        };
        Ok(self.new_source(t, cols, false))
    }

    /// A new source stream for `t` of `cols`; `shared`: the one every other mention of `t` reads.
    fn new_source(&mut self, t: Table, cols: Vec<(String, Type)>, shared: bool) -> Rel {
        let stream = match &t {
            Table::Named(n) if shared && !self.cat.streams.contains_key(n) && !n.starts_with("__") => n.clone(),
            _ => self.fresh("src"),
        };
        let columns = cols
            .iter()
            .map(|(n, ty)| Column {
                name: n.clone(),
                ty: match ty {
                    Type::Nullable(_) | Type::Any => ty.clone(),
                    t => Type::Nullable(Box::new(t.clone())),
                },
                materialized: None,
                default: None,
            })
            .collect();
        let settings =
            [("type", "kafka"), ("topic", stream.as_str()), ("data_format", "ProtobufSingle"), ("format_schema", "-")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        self.cat
            .streams
            .insert(stream.clone(), Stream { name: stream.clone(), kind: Kind::External, columns, settings });
        for (c, _) in &cols {
            self.lineage.insert((stream.clone(), c.clone()), vec![(stream.clone(), c.clone())]);
        }
        self.sources.push(Source {
            stream: stream.clone(),
            table: t.clone(),
            columns: cols.clone(),
            clock: Clock::Row,
        });
        if shared {
            self.tables.insert(t, stream.clone());
        }
        Rel { stream, cols: cols.into_iter().map(|c| c.0).collect() }
    }

    /// Notes `out` out of time order if it is `lead`'s or reads a stream that is.
    fn taint(&mut self, out: &str, scopes: &[Scoped], lead: bool) {
        if lead || scopes.iter().any(|s| self.unordered.contains(&s.rel.stream)) {
            self.unordered.insert(out.to_string());
        }
    }

    /// Sets the clock of each source `stream.col` is a copy of; an error where one has another.
    fn clock(&mut self, stream: &str, col: &str, why: &str) -> R<()> {
        if self.unordered.contains(stream) {
            return Err(format!(
                "{why}: the rows of lead(...) come when their next rows do, out of time order: \
                 compute lead in the outermost query, or lag the other way"
            ));
        }
        let Some(copied) = self.lineage.get(&(stream.to_string(), col.to_string())).cloned() else {
            return Err(format!("{why}: {col} must be a column of a table, not computed"));
        };
        for (src, c) in copied {
            let s = self.sources.iter_mut().find(|s| s.stream == src).expect("a source");
            match &s.clock {
                Clock::Column(x) | Clock::Shifted(x, _) if *x != c => {
                    return Err(format!(
                        "{why}: {} would be read in {x} order and in {c} order; use one time column per table",
                        s.table
                    ))
                }
                Clock::Shifted(..) => return Err(format!("{why}: {} is read at a time offset", s.table)),
                _ => s.clock = Clock::Column(c),
            }
        }
        Ok(())
    }

    fn query(&mut self, mut q: Query) -> R<Rel> {
        let unsupported = [
            (q.order_by.is_some(), "ORDER BY inside a subquery (order the outermost query)"),
            (q.limit_clause.is_some(), "LIMIT inside a subquery (limit the outermost query)"),
            (q.fetch.is_some(), "FETCH"),
            (!q.locks.is_empty(), "FOR UPDATE/SHARE"),
            (q.settings.is_some(), "SETTINGS"),
            (q.format_clause.is_some(), "FORMAT"),
            (q.with.as_ref().is_some_and(|w| w.recursive), "WITH RECURSIVE"),
        ];
        if let Some((_, what)) = unsupported.iter().find(|u| u.0) {
            return Err(format!("{what} is not supported"));
        }
        let saved = self.ctes.clone();
        for c in q.with.take().into_iter().flat_map(|w| w.cte_tables) {
            let rel = self.query(*c.query)?;
            self.ctes.insert(c.alias.name.value.clone(), rel);
        }
        let rel = self.set_expr(*q.body);
        self.ctes = saved;
        rel
    }

    fn set_expr(&mut self, body: SetExpr) -> R<Rel> {
        match body {
            SetExpr::Select(s) => self.select(*s),
            SetExpr::Query(q) => self.query(*q),
            SetExpr::SetOperation { op: SetOperator::Union, set_quantifier, left, right }
                if !matches!(set_quantifier, SetQuantifier::All) =>
            {
                // `a UNION b` is `SELECT DISTINCT * FROM (a UNION ALL b)`
                let Ok(mut s) = Parser::parse_sql(&GenericDialect {}, "SELECT DISTINCT * FROM (SELECT 1) AS __union")
                else {
                    unreachable!("valid SQL")
                };
                let Some(Statement::Query(mut q)) = s.pop() else { unreachable!("a query") };
                let SetExpr::Select(sel) = q.body.as_mut() else { unreachable!("a select") };
                let TableFactor::Derived { subquery, .. } = &mut sel.from[0].relation else {
                    unreachable!("a subquery")
                };
                *subquery.body =
                    SetExpr::SetOperation { op: SetOperator::Union, set_quantifier: SetQuantifier::All, left, right };
                self.set_expr(*q.body)
            }
            SetExpr::SetOperation { op: SetOperator::Union, .. } => {
                // a chain of UNION ALLs, its branches left to right: one stream, however many,
                // and no recursion down the chain (thousands of branches)
                let mut branches = vec![];
                let mut at = body;
                loop {
                    match at {
                        SetExpr::SetOperation {
                            op: SetOperator::Union,
                            set_quantifier: SetQuantifier::All,
                            left,
                            right,
                        } => {
                            branches.push(*right);
                            at = *left;
                        }
                        other => {
                            branches.push(other);
                            break;
                        }
                    }
                }
                branches.reverse();
                let sides = branches.into_iter().map(|b| self.set_expr(b)).collect::<R<Vec<Rel>>>()?;
                let first = sides[0].clone();
                if let Some(other) = sides.iter().find(|s| s.cols.len() != first.cols.len()) {
                    return Err(format!("UNION ALL of {} and {} columns", first.cols.len(), other.cols.len()));
                }
                let out = self.fresh("u");
                self.stream(&out, &first.cols);
                if sides.iter().any(|s| self.unordered.contains(&s.stream)) {
                    self.unordered.insert(out.clone());
                }
                for side in &sides {
                    let items = side
                        .cols
                        .iter()
                        .zip(&first.cols)
                        .map(|(c, to)| SelectItem::ExprWithAlias { expr: ident(c), alias: Ident::new(to) })
                        .collect();
                    let q = simple_query(items, table(&side.stream, None), None);
                    self.view(&out, q, false);
                }
                // a column copies the source columns every side's copies, if each copies one
                for (k, to) in first.cols.iter().enumerate() {
                    let of = |side: &Rel| self.lineage.get(&(side.stream.clone(), side.cols[k].clone()));
                    let all: Option<Vec<(String, String)>> =
                        sides.iter().map(|s| of(s).cloned()).collect::<Option<Vec<_>>>().map(|v| v.concat());
                    if let Some(all) = all {
                        self.lineage.insert((out.clone(), to.clone()), all);
                    }
                }
                Ok(Rel { stream: out, cols: first.cols })
            }
            b => Err(format!("unsupported query: {b} (SELECT, subqueries, WITH, UNION and UNION ALL are)")),
        }
    }

    /// SQL text with its files as written (`'path'`, not `__file0`): a query run aside.
    fn original(&self, text: &str) -> String {
        let mut out = text.to_string();
        for (i, f) in self.files.iter().enumerate().rev() {
            out = out.replace(&format!("__file{i}"), &format!("'{}'", f.replace('\'', "''")));
        }
        out
    }

    /// A FROM or JOIN relation as a stream, and the qualifier its columns go by.
    fn relation(&mut self, t: TableFactor) -> R<Scoped> {
        let t = match t {
            TableFactor::Derived { lateral: false, subquery, alias, .. } => {
                let rel = self.query(*subquery)?;
                return Ok(Scoped { rel, qualifier: alias.as_ref().map(|a| a.name.value.clone()) });
            }
            TableFactor::Pivot { table, aggregate_functions, value_column, value_source, default_on_null, alias } => {
                let [col] = value_column.as_slice() else {
                    return Err("PIVOT (agg FOR column IN (...)): one column".into());
                };
                let values = match value_source {
                    ast::PivotValueSource::List(l) => l
                        .into_iter()
                        .map(|v| (v.alias.map(|a| a.value).unwrap_or_else(|| value_name(&v.expr)), v.expr))
                        .collect(),
                    ast::PivotValueSource::Any(_) => {
                        let text =
                            format!("SELECT DISTINCT {col} AS v FROM {} ORDER BY v", self.original(&table.to_string()));
                        distinct(self.values, &text)?
                    }
                    ast::PivotValueSource::Subquery(q) => {
                        let text = self.original(&q.to_string());
                        distinct(self.values, &text)?
                    }
                };
                let aggs: Vec<(Expr, Option<String>)> =
                    aggregate_functions.into_iter().map(|a| (a.expr, a.alias.map(|a| a.value))).collect();
                let inner = self.relation(*table)?;
                // every other column groups, as DuckDB's and SQL Server's PIVOT do
                let mut used: Vec<String> = vec![item_name(col)];
                for (a, _) in &aggs {
                    let mut a = a.clone();
                    let _ = visit_expressions_mut(&mut a, |x| {
                        if matches!(x, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                            used.push(item_name(x));
                        }
                        ControlFlow::<()>::Continue(())
                    });
                }
                let group: Vec<String> = inner
                    .rel
                    .cols
                    .iter()
                    .filter(|c| !used.contains(c))
                    .map(|c| Ident::with_quote('"', c).to_string())
                    .collect();
                let mut items = group.clone();
                for (name, item) in pivot_items(&aggs, col, &values)? {
                    let item = match &default_on_null {
                        Some(d) => format!("coalesce({item}, {d})"),
                        None => item,
                    };
                    items.push(format!("{item} AS {}", Ident::with_quote('"', name)));
                }
                let name = self.fresh("pivot");
                self.ctes.insert(name.clone(), inner.rel);
                let by = if group.is_empty() { String::new() } else { format!(" GROUP BY {}", group.join(", ")) };
                let rel = self.query(parse_query(&format!("SELECT {} FROM {name}{by}", items.join(", ")))?)?;
                return Ok(Scoped { rel, qualifier: alias.map(|a| a.name.value) });
            }
            TableFactor::Unpivot { table, value, name, columns, null_inclusion, alias } => {
                let inner = self.relation(*table)?;
                let unpivoted: Vec<(String, String)> = columns
                    .iter()
                    .map(|c| (item_name(&c.expr), c.alias.as_ref().map_or(item_name(&c.expr), |a| a.value.clone())))
                    .collect();
                let others: Vec<String> = (inner.rel.cols.iter())
                    .filter(|c| !unpivoted.iter().any(|u| u.0 == **c))
                    .map(|c| Ident::with_quote('"', c).to_string())
                    .collect();
                let cte = self.fresh("unpivot");
                self.ctes.insert(cte.clone(), inner.rel);
                let nulls = matches!(null_inclusion, Some(ast::NullInclusion::IncludeNulls));
                let quoted = |s: &str| Ident::with_quote('"', s).to_string();
                let branches: Vec<String> = unpivoted
                    .iter()
                    .map(|(c, label)| {
                        let mut items = others.clone();
                        items.push(format!("'{}' AS {}", label.replace('\'', "''"), quoted(&name.value)));
                        items.push(format!("{} AS {}", quoted(c), quoted(&item_name(&value))));
                        let keep = if nulls { String::new() } else { format!(" WHERE {} IS NOT NULL", quoted(c)) };
                        format!("SELECT {} FROM {cte}{keep}", items.join(", "))
                    })
                    .collect();
                if branches.is_empty() {
                    return Err("UNPIVOT (value FOR name IN (columns)): name its columns".into());
                }
                let rel = self.query(parse_query(&branches.join(" UNION ALL "))?)?;
                return Ok(Scoped { rel, qualifier: alias.map(|a| a.name.value) });
            }
            t => t,
        };
        match &t {
            TableFactor::Table { name, alias, args: None, .. } => {
                let n = name.to_string();
                let rel = if let Some(i) = n.strip_prefix("__file").and_then(|i| i.parse::<usize>().ok()) {
                    let path = self.files.get(i).cloned().ok_or("internal: a file placeholder")?;
                    self.source(Table::Path { path, format: None, options: None })?
                } else if let Some(r) = self.ctes.get(&n) {
                    r.clone()
                } else {
                    self.source(Table::Named(n.clone()))?
                };
                let qualifier =
                    alias.as_ref().map(|a| a.name.value.clone()).or_else(|| (!n.starts_with("__")).then_some(n));
                Ok(Scoped { rel, qualifier })
            }
            TableFactor::Table { name, alias, args: Some(a), .. } => {
                let f = name.to_string().to_ascii_lowercase();
                let format = match f.as_str() {
                    "read_parquet" | "parquet_scan" => "parquet",
                    "read_csv" | "read_csv_auto" => "csv",
                    "read_json" | "read_ndjson" | "read_json_auto" => "json",
                    // Databento's binary encoding
                    "read_dbn" => "dbn",
                    // a table format: the files its log or metadata names
                    "delta_scan" => "delta",
                    "iceberg_scan" => "iceberg",
                    _ => {
                        return Err(format!(
                            "unknown table function {name}: read a file with FROM 'path', read_parquet/read_csv/read_json/read_dbn('path') \
                             or delta_scan/iceberg_scan('table')"
                        ))
                    }
                };
                let path = match a.args.first() {
                    Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))) => match &v.value {
                        AstValue::SingleQuotedString(s) => s.clone(),
                        _ => return Err(format!("{name}: the path is a string")),
                    },
                    _ => return Err(format!("{name}: the path is a string")),
                };
                // the reader's options, as SQL: `delim = '|', types = {'ts': 'TIMESTAMP'}`
                let options: Vec<String> = a.args[1..]
                    .iter()
                    .map(|o| match o {
                        FunctionArg::Named { name, arg, .. }
                        | FunctionArg::ExprNamed { name: Expr::Identifier(name), arg, .. } => {
                            Ok(format!("{name} = {arg}"))
                        }
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            e @ Expr::BinaryOp { op: BinaryOperator::Eq, .. },
                        )) => Ok(e.to_string()),
                        o => Err(format!("{name}: {o} is not an option: write name = value")),
                    })
                    .collect::<R<_>>()?;
                let options = (!options.is_empty()).then(|| options.join(", "));
                let rel = self.source(Table::Path { path, format: Some(format.into()), options })?;
                Ok(Scoped { rel, qualifier: alias.as_ref().map(|a| a.name.value.clone()) })
            }
            t => Err(format!("unsupported FROM {t}")),
        }
    }

    fn select(&mut self, mut s: Select) -> R<Rel> {
        let unsupported = [
            (s.top.is_some(), "TOP (write LIMIT)"),
            (s.into.is_some(), "SELECT INTO"),
            (!s.lateral_views.is_empty(), "LATERAL VIEW"),
            (s.prewhere.is_some(), "PREWHERE"),
            (s.qualify.is_some(), "QUALIFY (filter a subquery instead)"),
            (!s.connect_by.is_empty(), "CONNECT BY"),
            (matches!(s.distinct, Some(Distinct::On(_))), "DISTINCT ON"),
            (s.exclude.is_some(), "EXCLUDE"),
        ];
        if let Some((_, what)) = unsupported.iter().find(|u| u.0) {
            return Err(format!("{what} is not supported"));
        }
        inline_windows(&mut s)?;
        if let Some(h) = &mut s.having {
            rename_functions(h);
        }
        // the relations: streams, with the qualifiers their columns go by
        let mut scopes = vec![];
        // `FROM t, LATERAL (...)`: a window join
        if let [_, ast::TableWithJoins { relation: lateral @ TableFactor::Derived { lateral: true, .. }, joins }] =
            s.from.as_slice()
        {
            if joins.is_empty() {
                let join = ast::Join {
                    relation: lateral.clone(),
                    global: false,
                    join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
                };
                s.from.pop();
                s.from[0].joins.push(join);
            }
        }
        if s.from.len() > 1 {
            return Err("FROM a, b is not supported: write a JOIN".into());
        }
        if s.from.is_empty() {
            // SELECT 1 + 1: over one row
            s.from.push(ast::TableWithJoins { relation: table(ONE_ROW, None), joins: vec![] });
        }
        let from = s.from.first_mut().expect("a FROM");
        let first = self.relation(std::mem::replace(&mut from.relation, table(ONE_ROW, None)))?;
        from.relation = table(&first.rel.stream, first.qualifier.as_deref());
        scopes.push(first);
        let (mut inner_checks, mut within) = (vec![], vec![]);
        for j in from.joins.iter_mut() {
            if let TableFactor::Derived { lateral: true, subquery, alias, .. } = &mut j.relation {
                let sc = self.lateral(subquery, alias.as_ref().map(|a| a.name.value.clone()), &scopes)?;
                let on_true = |c: &JoinConstraint| match c {
                    JoinConstraint::On(Expr::Value(v)) => v.value == AstValue::Boolean(true),
                    c => matches!(c, JoinConstraint::None),
                };
                match &j.join_operator {
                    JoinOperator::Left(c)
                    | JoinOperator::LeftOuter(c)
                    | JoinOperator::Inner(c)
                    | JoinOperator::Join(c)
                    | JoinOperator::CrossJoin(c)
                        if on_true(c) => {}
                    op => return Err(format!("a window join is [LEFT] JOIN LATERAL (...) ON true, not {op:?}")),
                }
                j.join_operator = JoinOperator::Left(JoinConstraint::On(Expr::value(AstValue::Boolean(true))));
                scopes.push(sc);
                continue;
            }
            let before = self.sources.len();
            let mut sc = self.relation(std::mem::replace(&mut j.relation, table(ONE_ROW, None)))?;
            let made = self.sources.len() > before;
            let asof = j.global;
            j.global = false;
            let (constraint, inner) = match std::mem::replace(&mut j.join_operator, JoinOperator::CrossApply) {
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (c, false),
                JoinOperator::Inner(c) | JoinOperator::Join(c) => (c, true),
                op => {
                    let kind = match op {
                        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => "RIGHT JOIN",
                        JoinOperator::FullOuter(_) => "FULL JOIN",
                        JoinOperator::CrossJoin(_) => "CROSS JOIN",
                        JoinOperator::Semi(_) | JoinOperator::LeftSemi(_) | JoinOperator::RightSemi(_) => "SEMI JOIN",
                        JoinOperator::Anti(_) | JoinOperator::LeftAnti(_) | JoinOperator::RightAnti(_) => "ANTI JOIN",
                        _ => "this join",
                    };
                    return Err(format!(
                        "{kind} is not supported: brrrrr joins look up the right table for each left row, \
                         as ASOF [LEFT] JOIN or [LEFT] JOIN ... ON keys (for a RIGHT JOIN, swap the tables \
                         and write LEFT JOIN)"
                    ));
                }
            };
            let JoinConstraint::On(on) = constraint else {
                return Err("a join needs ON: the keys, and the time for ASOF".into());
            };
            let (on, check, tolerance) = self.join_condition(on, &scopes, &mut sc, asof, made)?;
            j.relation = table(&sc.rel.stream, sc.qualifier.as_deref());
            if inner {
                inner_checks.push(check);
                inner_checks.extend(tolerance);
            } else if let Some(t) = tolerance {
                within.push((scopes.len(), t));
            }
            j.join_operator = JoinOperator::Left(JoinConstraint::On(on));
            scopes.push(sc);
        }
        if let Some(w) = inner_checks.into_iter().reduce(and) {
            s.selection = Some(match s.selection.take() {
                Some(x) => and(Expr::Nested(Box::new(x)), w),
                None => w,
            });
        }
        // `*` and `t.*` as their columns, each named once
        let mut items = vec![];
        for item in std::mem::take(&mut s.projection) {
            match item {
                SelectItem::Wildcard(_) => {
                    for sc in &scopes {
                        for c in &sc.rel.cols {
                            items.push(SelectItem::UnnamedExpr(qualified(sc, c)));
                        }
                    }
                }
                SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(n), _) => {
                    let q = n.to_string();
                    let sc = scopes
                        .iter()
                        .find(|s| s.qualifier.as_deref() == Some(q.as_str()))
                        .ok_or(format!("{q}.*: no table {q}"))?;
                    for c in &sc.rel.cols {
                        items.push(SelectItem::UnnamedExpr(qualified(sc, c)));
                    }
                }
                i => items.push(i),
            }
        }
        let mut names: Vec<String> = vec![];
        for item in &mut items {
            let (e, name) = match item {
                SelectItem::UnnamedExpr(e) => (e.clone(), unique(&item_name(e), &names)),
                SelectItem::ExprWithAlias { expr, alias } => (expr.clone(), unique(&alias.value, &names)),
                i => return Err(format!("unsupported select item {i}")),
            };
            *item = SelectItem::ExprWithAlias { expr: e, alias: Ident::new(&name) };
            names.push(name);
        }
        for item in &mut items {
            if let SelectItem::ExprWithAlias { expr, .. } = item {
                rename_functions(expr);
            }
        }
        s.projection = items;
        // ASOF LEFT JOIN's tolerance: a match older than it is no match, its columns NULL
        for (k, t) in within {
            let all: Vec<&Scoped> = scopes.iter().collect();
            let from = std::mem::take(&mut s.from);
            let _ = visit_expressions_mut(&mut s, |x| {
                if resolve_col(x, &all).is_some_and(|(sc, _)| std::ptr::eq(sc, &scopes[k])) {
                    *x = Expr::Case {
                        case_token: ast::helpers::attached_token::AttachedToken::empty(),
                        end_token: ast::helpers::attached_token::AttachedToken::empty(),
                        operand: None,
                        conditions: vec![ast::CaseWhen { condition: t.clone(), result: x.clone() }],
                        else_result: None,
                    };
                }
                ControlFlow::<()>::Continue(())
            });
            s.from = from;
        }
        let grouped = !matches!(&s.group_by, GroupByExpr::Expressions(k, _) if k.is_empty())
            || s.having.is_some()
            || matches!(s.distinct, Some(Distinct::Distinct))
            || s.projection.iter().any(|i| matches!(i, SelectItem::ExprWithAlias { expr, .. } if has_aggregate(expr)));
        // time columns of what this select reads: its clock
        self.clocks(&s, &scopes)?;
        self.window_order(&mut s, &scopes)?;
        if !grouped {
            let out = self.fresh("s");
            self.stream(&out, &names);
            let lead = s.projection.iter().any(|i| {
                let mut found = false;
                let mut e = match i {
                    SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
                    _ => return false,
                };
                let _ = visit_expressions_mut(&mut e, |x| {
                    found |= matches!(x, Expr::Function(f) if f.over.is_some() && function_name(x).as_deref() == Some("lead"));
                    ControlFlow::<()>::Continue(())
                });
                found
            });
            self.taint(&out, &scopes, lead);
            for (item, n) in s.projection.iter().zip(&names) {
                if let SelectItem::ExprWithAlias { expr, .. } = item {
                    self.copy_lineage(expr, &scopes, &out, n);
                }
            }
            let q = Query { body: Box::new(SetExpr::Select(Box::new(s))), ..empty_query() };
            self.view(&out, q, false);
            return Ok(Rel { stream: out, cols: names });
        }
        self.grouped(s, scopes, names)
    }

    /// ON of a join, as the engine's as-of join takes it: equalities left = right, then one
    /// `left >= right`. A lookup (no ASOF) takes its right rows at the time `0 >= 0`. Returns
    /// it, the condition an inner join keeps its rows by, and an as-of join's tolerance (`l.ts -
    /// r.ts <= INTERVAL '1 second'`: a match older than that is none).
    ///
    /// The times may be offset by constant intervals (`l.ts + INTERVAL '5 seconds' >= r.ts`, a
    /// markout): the right table is then read on its own, in the order of its time shifted the
    /// other way (`Clock::Shifted`), which stays in time order beside the left table's. `l.ts >
    /// r.ts` is `l.ts >= r.ts + 1µs`, the times being whole µs.
    fn join_condition(
        &mut self,
        on: Expr,
        left: &[Scoped],
        right: &mut Scoped,
        asof: bool,
        made: bool,
    ) -> R<(Expr, Expr, Option<Expr>)> {
        let mut conds = vec![];
        let mut stack = vec![on];
        while let Some(c) = stack.pop() {
            match c {
                Expr::BinaryOp { left, op: BinaryOperator::And, right } => stack.extend([*right, *left]),
                Expr::Nested(e) => stack.push(*e),
                c => conds.push(c),
            }
        }
        let side_of = |e: &Expr| -> Option<bool> {
            // true: the right relation's; None: no qualifier names it
            let mut q = None;
            let mut e = e.clone();
            let _ = visit_expressions_mut(&mut e, |x| {
                match x {
                    Expr::CompoundIdentifier(ids) if ids.len() == 2 => {
                        let name = &ids[0].value;
                        if right.qualifier.as_deref() == Some(name.as_str()) {
                            q = Some(true);
                        } else if left.iter().any(|s| s.qualifier.as_deref() == Some(name.as_str())) {
                            q = Some(false);
                        }
                    }
                    Expr::Identifier(i) if q.is_none() => {
                        let (inr, inl) =
                            (right.rel.cols.contains(&i.value), left.iter().any(|s| s.rel.cols.contains(&i.value)));
                        if inr && !inl {
                            q = Some(true);
                        } else if inl && !inr {
                            q = Some(false);
                        }
                    }
                    _ => {}
                }
                ControlFlow::<()>::Continue(())
            });
            q
        };
        let (mut keys, mut time, mut tolerance) = (vec![], None, None);
        for c in conds {
            let Expr::BinaryOp { left: a, op, right: b } = c else {
                return Err(format!(
                    "unsupported join condition {c}: equalities of keys, and l.time >= r.time for ASOF"
                ));
            };
            if asof
                && matches!(op, BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq)
            {
                // `l.ts op r.ts + d`
                let cond = Expr::BinaryOp { left: a.clone(), op: op.clone(), right: b.clone() };
                let bad = || {
                    format!(
                        "unsupported ASOF JOIN condition {cond}: compare the times, as l.ts >= r.ts, \
                         l.ts + INTERVAL '5 seconds' >= r.ts, or l.ts - r.ts <= INTERVAL '1 second'"
                    )
                };
                let ((mut terms, x), (other, y)) = (linear(&a).ok_or_else(bad)?, linear(&b).ok_or_else(bad)?);
                terms.extend(other.into_iter().map(|(e, k)| (e, -k)));
                let [(p, kp), (q, kq)] = terms.as_slice() else { return Err(bad()) };
                let (l, r, k) = match (side_of(p), side_of(q)) {
                    (Some(false), Some(true)) if *kq == -kp => (p, q, *kp),
                    (Some(true), Some(false)) if *kq == -kp => (q, p, *kq),
                    _ => return Err(bad()),
                };
                let c = x.checked_sub(y).ok_or_else(bad)?;
                // k (l - r) + c op 0
                let (op, d) = if k == 1 { (op, -c) } else { (flip(&op).expect("an inequality"), c) };
                match op {
                    BinaryOperator::GtEq | BinaryOperator::Gt if time.is_none() => {
                        let strict = i64::from(op == BinaryOperator::Gt);
                        time = Some((l.clone(), r.clone(), d.checked_add(strict).ok_or_else(bad)?));
                    }
                    BinaryOperator::LtEq | BinaryOperator::Lt if tolerance.is_none() => tolerance = Some(cond),
                    _ => return Err(format!("ASOF JOIN takes one time condition and one tolerance at most: {cond}")),
                }
                continue;
            }
            // the left side's operand first
            let (a, op, b) = match (side_of(&a), side_of(&b)) {
                (Some(true), _) | (_, Some(false)) => {
                    let flipped = flip(&op).ok_or(format!("unsupported join condition {a} {op} {b}"))?;
                    (*b, flipped, *a)
                }
                _ => (*a, op, *b),
            };
            match op {
                BinaryOperator::Eq if side_of(&a) == Some(false) && side_of(&b) == Some(true) => keys.push((a, b)),
                BinaryOperator::Eq => {
                    return Err(format!("join keys are a column of each table, not {a} = {b} (filter in WHERE)"))
                }
                op => return Err(format!("unsupported join condition {a} {op} {b}")),
            }
        }
        let shifted = matches!(time, Some((_, _, by)) if by != 0);
        let (lt, rt) = match time {
            Some((l, r, 0)) => (l, r),
            Some((l, r, by)) => {
                // the right table read on its own, in the order of its time shifted by `by`
                let (sc, col) = resolve_col(&r, &[&*right]).ok_or(format!("ASOF JOIN: {r} must be a time column"))?;
                let stream = sc.rel.stream.clone();
                let Some(at) = self.sources.iter().position(|s| s.stream == stream) else {
                    return Err(format!(
                        "ASOF JOIN at a time offset: {} must be a table, not a subquery",
                        right.qualifier.as_deref().unwrap_or("its right side")
                    ));
                };
                if !self.sources[at].columns.iter().any(|(c, t)| *c == col && matches!(t, Type::Time(_))) {
                    return Err(format!("ASOF JOIN at a time offset (or >): {r} must be a time column"));
                }
                if made {
                    self.tables.retain(|_, s| *s != stream);
                } else {
                    let t = self.sources[at].table.clone();
                    right.rel.stream = self.own_source(t)?.stream;
                }
                let s = self.sources.iter_mut().find(|s| s.stream == right.rel.stream).expect("a source");
                s.clock = Clock::Shifted(col, by);
                let lcol = resolve_col(&l, &left.iter().collect::<Vec<_>>())
                    .ok_or(format!("ASOF JOIN: {l} must be a time column of its table"))?;
                let lstream = lcol.0.rel.stream.clone();
                self.clock(&lstream, &lcol.1, "ASOF JOIN")?;
                (l, qualified(right, "__clock"))
            }
            None if asof => return Err("ASOF JOIN needs the time: ON l.key = r.key AND l.ts >= r.ts".into()),
            None => (number(0), number(0)),
        };
        if asof && !shifted {
            // each side's rows in the order of its time column
            for (e, scopes) in [(&lt, left.iter().collect::<Vec<_>>()), (&rt, vec![&*right])] {
                let (sc, col) =
                    resolve_col(e, &scopes).ok_or(format!("ASOF JOIN: {e} must be a time column of its table"))?;
                let stream = sc.rel.stream.clone();
                self.clock(&stream, &col, "ASOF JOIN")?;
            }
        } else if !asof {
            // a lookup's table is read before every other
            let s = self.sources.iter_mut().find(|s| s.stream == right.rel.stream);
            match s {
                Some(s) if s.clock == Clock::Row => s.clock = Clock::First,
                Some(_) => {}
                None => return Err("a JOIN on keys looks up a table: join a file or table, not a subquery".into()),
            }
        }
        // inner: the right side matched; the time is never NULL in a match, and a key never is
        let check = match (&rt, keys.first()) {
            (e, _) if asof => Expr::IsNotNull(Box::new(e.clone())),
            (_, Some((_, k))) => Expr::IsNotNull(Box::new(k.clone())),
            _ => return Err("a JOIN needs ON keys".into()),
        };
        let (lefts, rights): (Vec<Expr>, Vec<Expr>) = keys.iter().cloned().unzip();
        let mut all: Vec<&Scoped> = left.iter().collect();
        all.push(right);
        let both: Vec<Expr> = lefts.into_iter().zip(rights).flat_map(|(a, b)| [a, b]).collect();
        self.note_keys(&both, &all);
        let mut on = Expr::BinaryOp { left: Box::new(lt), op: BinaryOperator::GtEq, right: Box::new(rt) };
        for (a, b) in keys.into_iter().rev() {
            on = and(Expr::BinaryOp { left: Box::new(a), op: BinaryOperator::Eq, right: Box::new(b) }, on);
        }
        Ok((on, check, tolerance))
    }

    /// A window join's subquery (`LATERAL (SELECT aggregates FROM r WHERE r.k = l.k AND r.ts
    /// BETWEEN l.ts - INTERVAL '1 second' AND l.ts)`), its table made its stream, both tables in
    /// the order of their times; the engine's `Within` takes the rest. Returns its scope.
    fn lateral(&mut self, sub: &mut Query, alias: Option<String>, left: &[Scoped]) -> R<Scoped> {
        let usage = "a window join is LEFT JOIN LATERAL (SELECT aggregates FROM r WHERE r.key = l.key \
                     AND r.ts BETWEEN l.ts - INTERVAL '1 second' AND l.ts) ON true";
        let SetExpr::Select(sel) = sub.body.as_mut() else { return Err(usage.into()) };
        let [from] = sel.from.as_mut_slice() else { return Err(usage.into()) };
        if !from.joins.is_empty() {
            return Err(usage.into());
        }
        let inner = self.relation(std::mem::replace(&mut from.relation, table(ONE_ROW, None)))?;
        from.relation = table(&inner.rel.stream, inner.qualifier.as_deref());
        let mut cols = vec![];
        for item in &mut sel.projection {
            let (mut e, name) = match item {
                SelectItem::UnnamedExpr(e) => (e.clone(), unique(&item_name(e), &cols)),
                SelectItem::ExprWithAlias { expr, alias } => (expr.clone(), unique(&alias.value, &cols)),
                i => return Err(format!("{i}: {usage}")),
            };
            rename_functions(&mut e);
            *item = SelectItem::ExprWithAlias { expr: e, alias: Ident::new(&name) };
            cols.push(name);
        }
        // the times: a column of the subquery's table, and the left row's ± constants
        let mut conds: Vec<Expr> = sel.selection.iter().cloned().collect();
        let (mut times, mut keys) = (None, vec![]);
        let all: Vec<&Scoped> = left.iter().collect();
        // a column as SQL scopes it: the subquery's table first
        let col = |e: &Expr| resolve_col(e, &[&inner]).map(|c| (true, c)).or(resolve_col(e, &all).map(|c| (false, c)));
        while let Some(c) = conds.pop() {
            let (r, others): (&Expr, Vec<&Expr>) = match &c {
                Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                    conds.extend([(**right).clone(), (**left).clone()]);
                    continue;
                }
                Expr::Nested(x) => {
                    conds.push((**x).clone());
                    continue;
                }
                Expr::Between { expr, negated: false, low, high } => (expr, vec![low, high]),
                Expr::BinaryOp { left: a, op: BinaryOperator::Eq, right: b } => {
                    if let (Some((x, _)), Some((y, _))) = (col(a), col(b)) {
                        if x != y {
                            keys.extend([(**a).clone(), (**b).clone()]);
                        }
                    }
                    continue;
                }
                Expr::BinaryOp {
                    left: a,
                    op: BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq,
                    right: b,
                } => match col(a) {
                    Some((true, _)) => (&**a, vec![&**b]),
                    _ => (&**b, vec![&**a]),
                },
                _ => continue,
            };
            let Some((true, (_, rcol))) = col(r) else { continue };
            // a time: a column of a time type, or of a type unknown here (a subquery's)
            let typed = self.sources.iter().find(|s| s.stream == inner.rel.stream).and_then(|s| {
                s.columns.iter().find(|c| c.0 == rcol).map(|c| match &c.1 {
                    Type::Nullable(t) => matches!(**t, Type::Time(_)),
                    t => matches!(t, Type::Time(_)),
                })
            });
            if typed == Some(false) {
                continue;
            }
            for o in others {
                let base = linear(o).and_then(|(terms, _)| match terms.as_slice() {
                    [(e, 1)] => col(e),
                    _ => None,
                });
                if let Some((false, (sc, lcol))) = base {
                    times = Some((rcol.clone(), sc.rel.stream.clone(), lcol));
                }
            }
        }
        let Some((rcol, lstream, lcol)) = times else { return Err(format!("a window join needs its range: {usage}")) };
        let rstream = inner.rel.stream.clone();
        self.clock(&rstream, &rcol, "a window join")?;
        self.clock(&lstream, &lcol, "a window join")?;
        let mut scopes = all;
        scopes.push(&inner);
        self.note_keys(&keys, &scopes);
        let stream = self.fresh("lateral");
        Ok(Scoped { rel: Rel { stream, cols }, qualifier: alias })
    }

    /// Notes the clock each time operation of `s` asks of its sources: OVER (ORDER BY t).
    fn clocks(&mut self, s: &Select, scopes: &[Scoped]) -> R<()> {
        let (mut wanted, mut partitions) = (vec![], vec![]);
        for item in &s.projection {
            let mut e = match item {
                SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
                _ => continue,
            };
            let _ = visit_expressions_mut(&mut e, |x| {
                if let Expr::Function(f) = x {
                    if let Some(ast::WindowType::WindowSpec(w)) = &f.over {
                        if let Some(o) = w.order_by.first() {
                            wanted.push(o.expr.clone());
                        }
                        partitions.push(w.partition_by.clone());
                    }
                }
                ControlFlow::<()>::Continue(())
            });
        }
        let all: Vec<&Scoped> = scopes.iter().collect();
        for p in partitions {
            self.note_keys(&p, &all);
        }
        for e in wanted {
            let all: Vec<&Scoped> = scopes.iter().collect();
            if let Some((sc, col)) = resolve_col(&e, &all) {
                let stream = sc.rel.stream.clone();
                // a window's bucket comes in order already
                if !self.ordered.contains(&(stream.clone(), col.clone())) {
                    self.clock(&stream, &col, "OVER (ORDER BY ...)")?;
                }
            }
        }
        Ok(())
    }

    /// Window functions as the engine computes them: over each partition's rows in the order
    /// they are read, which is its table's time order. `ORDER BY` the time column says so and
    /// is dropped; another order cannot be had. An aggregate without `ORDER BY` is SQL's total
    /// of the whole partition, which needs its last row before its first: refused.
    fn window_order(&mut self, s: &mut Select, scopes: &[Scoped]) -> R<()> {
        let all: Vec<&Scoped> = scopes.iter().collect();
        let lineage = &self.lineage;
        let sources = &self.sources;
        let ordered = &self.ordered;
        let mut err = None;
        for item in &mut s.projection {
            let SelectItem::ExprWithAlias { expr, .. } = item else { continue };
            let _ = visit_expressions_mut(expr, |x| {
                let Expr::Function(f) = x else { return ControlFlow::<()>::Continue(()) };
                let name = f.name.to_string().to_ascii_lowercase();
                let ema = name == "avg" && args(&Expr::Function(f.clone())).len() == 3;
                let aggregate = crate::agg::is_aggregate(&name) && !ema;
                let Some(ast::WindowType::WindowSpec(w)) = &mut f.over else { return ControlFlow::Continue(()) };
                if w.order_by.is_empty() {
                    if aggregate && w.window_frame.is_none() {
                        err = Some(format!(
                            "{name}(...) OVER (PARTITION BY ...) without ORDER BY is the whole partition's total, which a stream cannot know until its end: write ORDER BY the time for a running {name}, or {name} in a subquery GROUP BY the partition and join it"
                        ));
                    }
                    return ControlFlow::Continue(());
                }
                let [o] = w.order_by.as_slice() else {
                    err = Some(format!(
                        "{name} OVER (... ORDER BY one time column): rows go in their table's time order"
                    ));
                    return ControlFlow::Continue(());
                };
                let desc = matches!(o.options.sort, Some(ast::OrderBySort::Desc));
                let at = resolve_col(&o.expr, &all).map(|(sc, c)| (sc.rel.stream.clone(), c));
                let copied = at.as_ref().and_then(|k| lineage.get(k));
                let is_clock = at.as_ref().is_some_and(|k| ordered.contains(k))
                    || copied.is_some_and(|cs| {
                        cs.iter().all(|(src, c)| {
                            sources.iter().any(|x| x.stream == *src && x.clock == Clock::Column(c.clone()))
                        })
                    });
                if desc || !is_clock {
                    err = Some(format!(
                        "{name} OVER (... ORDER BY {}): window functions run in the time order of their table's time column, ascending",
                        o.expr
                    ));
                } else if w.window_frame.is_none() {
                    w.order_by.clear();
                }
                ControlFlow::Continue(())
            });
        }
        err.map_or(Ok(()), Err)
    }

    /// Notes a stateful step keyed by `keys` (columns of `scopes`): the source columns they copy.
    fn note_keys(&mut self, keys: &[Expr], scopes: &[&Scoped]) {
        let cols: Vec<(String, String)> = keys
            .iter()
            .filter_map(|k| resolve_col(k, scopes))
            .filter_map(|(sc, c)| self.lineage.get(&(sc.rel.stream.clone(), c)).cloned())
            .flatten()
            .collect();
        self.keyed.push((!cols.is_empty()).then_some(cols));
    }

    fn copy_lineage(&mut self, e: &Expr, scopes: &[Scoped], out: &str, name: &str) {
        let all: Vec<&Scoped> = scopes.iter().collect();
        if let Some((sc, col)) = resolve_col(e, &all) {
            if self.ordered.contains(&(sc.rel.stream.clone(), col.clone())) {
                self.ordered.insert((out.to_string(), name.to_string()));
            }
            if let Some(l) = self.lineage.get(&(sc.rel.stream.clone(), col)).cloned() {
                self.lineage.insert((out.to_string(), name.to_string()), l);
            }
        }
    }

    /// A SELECT with GROUP BY, aggregates, HAVING or DISTINCT: a view to a single relation of
    /// plain columns when it needs one, the window, and HAVING's filter.
    fn grouped(&mut self, mut s: Select, scopes: Vec<Scoped>, names: Vec<String>) -> R<Rel> {
        if s.projection.iter().any(|i| matches!(i, SelectItem::ExprWithAlias { expr, .. } if has_window_function(expr)))
        {
            return Err("window functions over GROUP BY: aggregate in a subquery, and OVER (...) outside it".into());
        }
        // the group keys, with aliases and positions read as their items
        let items: Vec<(Expr, String)> = s
            .projection
            .iter()
            .map(|i| match i {
                SelectItem::ExprWithAlias { expr, alias } => (expr.clone(), alias.value.clone()),
                _ => unreachable!("named above"),
            })
            .collect();
        let mut keys: Vec<Expr> = match std::mem::replace(&mut s.group_by, GroupByExpr::Expressions(vec![], vec![])) {
            GroupByExpr::Expressions(k, m) if m.is_empty() => k,
            GroupByExpr::All(_) => items.iter().filter(|(e, _)| !has_aggregate(e)).map(|(e, _)| e.clone()).collect(),
            _ => return Err("GROUP BY modifiers (ROLLUP, CUBE, TOTALS) are not supported".into()),
        };
        if matches!(s.distinct.take(), Some(Distinct::Distinct)) {
            if items.iter().any(|(e, _)| has_aggregate(e)) {
                return Err("DISTINCT with aggregates is not supported".into());
            }
            keys.extend(items.iter().map(|(e, _)| e.clone()));
        }
        for k in &mut keys {
            if let Expr::Value(v) = k {
                if let AstValue::Number(n, _) = &v.value {
                    let i: usize = n.parse().map_err(|_| format!("GROUP BY {n}"))?;
                    *k = items
                        .get(i.wrapping_sub(1))
                        .ok_or(format!("GROUP BY {n}: there are {} items", items.len()))?
                        .0
                        .clone();
                    continue;
                }
            }
            if let Expr::Identifier(id) = k {
                let shadowed = scopes.iter().any(|sc| sc.rel.cols.contains(&id.value));
                if let (false, Some((e, _))) = (shadowed, items.iter().find(|(_, a)| *a == id.value)) {
                    *k = e.clone();
                }
            }
        }
        // one time bucket at most: the window
        let is_bucket = |e: &Expr| {
            matches!(
                function_name(e).as_deref(),
                Some("time_bucket" | "date_bin" | "tumble_start" | "time_bucket_gapfill")
            )
        };
        let buckets: Vec<Expr> = keys.iter().filter(|k| is_bucket(k)).cloned().collect();
        if buckets.len() > 1 {
            return Err("GROUP BY one time_bucket(...) at most".into());
        }
        keys.retain(|k| !is_bucket(k));
        let bucket = buckets.into_iter().next();
        // TimescaleDB's gap filling: the window's output through `gap_fill`, over [start, finish)
        let mut gapfill = None;
        let mut bucket_args = bucket.as_ref().map(args).unwrap_or_default();
        if let Some(b) = bucket.as_ref().filter(|b| function_name(b).as_deref() == Some("time_bucket_gapfill")) {
            match bucket_args.as_slice() {
                [_, _] => gapfill = Some((None, None)),
                [_, _, start, finish] => gapfill = Some((time_literal(start)?, time_literal(finish)?)),
                _ => return Err(format!("{b}: time_bucket_gapfill(width, time[, start, finish]) expected")),
            }
            bucket_args.truncate(2);
            // a range of more buckets than rows could ever fill is a mistake, not a query
            if let Some((Some(start), Some(finish))) = gapfill {
                if finish.saturating_sub(start) / width_us(&bucket_args[0])? > MAX_GAP_BUCKETS {
                    return Err(format!("{b}: more than {MAX_GAP_BUCKETS} buckets from start to finish"));
                }
            }
        }
        let (width, ts, zone) = match &bucket {
            Some(b) => match bucket_args.as_slice() {
                [w, t] => (width_us(w)?, Some(t.clone()), None),
                [w, t, z] => {
                    let name = match z {
                        Expr::Value(v) => match &v.value {
                            AstValue::SingleQuotedString(z) => Some(z.clone()),
                            _ => None,
                        },
                        _ => None,
                    };
                    if name.as_deref().and_then(crate::expr::functions::zone).is_none() {
                        return Err(format!("{b}: {}", crate::expr::functions::unknown_zone(name)));
                    }
                    (width_us(w)?, Some(t.clone()), Some(z.clone()))
                }
                _ => return Err(format!("{b}: time_bucket(width, time[, zone]) expected")),
            },
            None => (1_000, None, None),
        };
        // a zone's buckets: a window over its wall clock's time (`__time`), grouped by the
        // bucket's start as well, as a time: it tells the hour a DST end reads twice from the first
        if let (Some(_), Some(b)) = (&zone, &bucket) {
            keys.push(b.clone());
        }
        // a relation of plain columns before the window, unless it reads one already
        let plain = |e: &Expr| matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
        let direct = scopes.len() == 1
            && s.from[0].joins.is_empty()
            && s.selection.is_none()
            && keys.iter().all(plain)
            && ts.as_ref().is_none_or(plain);
        let (input, cols, mut rename): (String, Vec<String>, HashMap<String, String>) = if direct {
            let sc = &scopes[0];
            (sc.rel.stream.clone(), sc.rel.cols.clone(), HashMap::new())
        } else {
            self.pre_view(&mut s, &scopes, &mut keys, ts.as_ref(), zone.as_ref())?
        };
        let zoned = zone.as_ref().map(|_| item_name(keys.last().expect("the zone's bucket")));
        // the window's own scope: one stream, its columns by name
        let unqualify = |e: &mut Expr, rename: &HashMap<String, String>| {
            let _ = visit_expressions_mut(e, |x| {
                let r = match x {
                    Expr::CompoundIdentifier(ids) if ids.len() == 2 => Some(
                        rename
                            .get(&ids.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."))
                            .cloned()
                            .unwrap_or_else(|| ids[1].value.clone()),
                    ),
                    Expr::Identifier(i) => rename.get(&i.value).cloned(),
                    _ => None,
                };
                if let Some(r) = r {
                    *x = ident(&r);
                }
                ControlFlow::<()>::Continue(())
            });
        };
        // the items as the pre-view left them (a computed key's item names its column)
        let items: Vec<(Expr, String)> = s
            .projection
            .iter()
            .map(|i| match i {
                SelectItem::ExprWithAlias { expr, alias } => (expr.clone(), alias.value.clone()),
                _ => unreachable!("named above"),
            })
            .collect();
        let ts = ts.map(|mut t| {
            unqualify(&mut t, &rename);
            t
        });
        if let Some(t) = &ts {
            let col = item_name(t);
            self.clock(&input, &col, "time_bucket")?;
            rename.insert("__bucket".into(), col);
        }
        let ts = if zone.is_some() { Some(ident("__time")) } else { ts };
        let bucket_text = bucket.as_ref().map(|b| b.to_string());
        let mut window_items = vec![];
        let mut bucket_alias = None;
        for (e, alias) in &items {
            let mut e = e.clone();
            if bucket_text.as_deref() == Some(e.to_string().as_str()) {
                e = ident("window_start");
                bucket_alias = Some(alias.clone());
            } else {
                if zoned.as_ref().is_some_and(|z| matches!(&e, Expr::Identifier(i) if i.value == *z)) {
                    bucket_alias = Some(alias.clone());
                }
                unqualify(&mut e, &rename);
            }
            window_items.push(SelectItem::ExprWithAlias { expr: e, alias: Ident::new(alias) });
        }
        let mut group = vec![ident("window_start")];
        for k in &keys {
            let mut k = k.clone();
            unqualify(&mut k, &rename);
            if !plain(&k) || !cols.contains(&item_name(&k)) {
                return Err(format!("GROUP BY {k}: group by columns of the table, or by items of the SELECT"));
            }
            // a key's items name it as the window's output does
            group.push(k);
        }
        // HAVING: computed by the window, kept by a view after it
        let having = s.having.take().map(|mut h| {
            unqualify(&mut h, &rename);
            h
        });
        if having.is_some() {
            window_items.push(SelectItem::ExprWithAlias {
                expr: having.clone().expect("having"),
                alias: Ident::new("__having"),
            });
        }
        let time_arg = ts.unwrap_or_else(|| number(0));
        let tumble = table_function("tumble", vec![ident(&input), time_arg, string(&tumble_width(width))]);
        // gap filling: the window gives the keys and each aggregate (`__a<i>`, or `__f<i>` filled by
        // locf or interpolate), `gap_fill` the buckets between, and a view the items of them
        let mut fill = None;
        if let Some((start, finish)) = gapfill {
            if having.is_some() {
                return Err("HAVING with time_bucket_gapfill: filter the filled rows in an outer query".into());
            }
            let mut out =
                vec![SelectItem::ExprWithAlias { expr: ident("window_start"), alias: Ident::new("__bucket") }];
            out.extend(
                group[1..]
                    .iter()
                    .map(|k| SelectItem::ExprWithAlias { expr: k.clone(), alias: Ident::new(item_name(k)) }),
            );
            let mut gap_args = vec![ident("__bucket"), string(&tumble_width(width))];
            if start.is_some() || finish.is_some() {
                let at = |t: Option<i64>| t.map_or(Expr::value(AstValue::Null), number);
                gap_args.extend([at(start), at(finish)]);
            }
            gap_args.extend(group[1..].iter().cloned());
            let mut filled = vec![];
            for item in std::mem::take(&mut window_items) {
                let SelectItem::ExprWithAlias { mut expr, alias } = item else { unreachable!("named above") };
                let mut err = None;
                let mut add = |e: Expr, mode: Option<&str>, out: &mut Vec<SelectItem>| {
                    let name = format!("__a{}", out.len());
                    out.push(SelectItem::ExprWithAlias { expr: e, alias: Ident::new(&name) });
                    if let Some(m) = mode {
                        gap_args.push(crate::expr::call_of(m, vec![ident(&name)]));
                    }
                    ident(&name)
                };
                // locf(x) and interpolate(x) first: x whole, its aggregates in it
                let _ = visit_expressions_mut(&mut expr, |x| {
                    if let Some(m @ ("locf" | "interpolate")) = function_name(x).as_deref() {
                        match args(x).as_slice() {
                            [a] => *x = add(a.clone(), Some(m), &mut out),
                            _ => err = Some(format!("{x}: {m}(aggregate) expected")),
                        }
                    }
                    ControlFlow::<()>::Continue(())
                });
                let _ = visit_expressions_mut(&mut expr, |x| {
                    if matches!(x, Expr::Function(f) if is_aggregate_call(f)) {
                        *x = add(x.clone(), None, &mut out);
                    } else if matches!(x, Expr::Identifier(i) if i.value == "window_start") {
                        *x = ident("__bucket");
                    }
                    ControlFlow::<()>::Continue(())
                });
                err.map_or(Ok(()), Err)?;
                filled.push(SelectItem::ExprWithAlias { expr, alias });
            }
            window_items = out;
            fill = Some((filled, gap_args));
        }
        let mut wq = simple_query(window_items, tumble, None);
        if let SetExpr::Select(sel) = wq.body.as_mut() {
            sel.group_by = GroupByExpr::Expressions(group.clone(), vec![]);
        }
        self.widths.push(width);
        // the window is keyed by its GROUP BY columns; its output's key columns copy them
        let in_scope = Scoped { rel: Rel { stream: input.clone(), cols: cols.clone() }, qualifier: None };
        let step = self.keyed.len();
        self.note_keys(&group[1..], &[&in_scope]);
        let mut out_cols = names.clone();
        if having.is_some() {
            out_cols.push("__having".into());
        }
        if let (SetExpr::Select(sel), Some(_)) = (wq.body.as_ref(), &fill) {
            out_cols = sel.projection.iter().map(select_name).collect();
        }
        let out = self.fresh("g");
        if bucket.is_none() {
            self.global_windows.push((step, out.clone()));
        }
        self.stream(&out, &out_cols);
        if keys.is_empty() && bucket.is_none() && having.is_none() {
            let row = items
                .iter()
                .map(|(e, _)| match function_name(e).as_deref() {
                    Some("count" | "count_if" | "uniq_exact" | "uniq_exact_if") => Value::UInt(0),
                    _ => Value::Null,
                })
                .collect();
            self.global.insert(out.clone(), row);
        }
        if let Some(a) = &bucket_alias {
            self.ordered.insert((out.clone(), a.clone()));
        }
        if let SetExpr::Select(sel) = wq.body.as_ref() {
            for item in &sel.projection {
                if let SelectItem::ExprWithAlias { expr: e @ Expr::Identifier(_), alias } = item {
                    self.copy_lineage(e, std::slice::from_ref(&in_scope), &out, &alias.value);
                }
            }
        }
        self.view(&out, wq, true);
        if zone.is_some() && width % 3_600_000_000 != 0 {
            // the wall clock goes back when DST ends, at most an hour and on the hour: windows of
            // whole hours are still open then, narrower ones are held an hour longer
            self.cat.views.last_mut().expect("the window").emit_delay_us = Some(3_600_000_000);
        }
        if let Some((items, mut gap_args)) = fill {
            gap_args.insert(0, ident(&out));
            let filled = self.fresh("f");
            self.stream(&filled, &names);
            let g = Scoped { rel: Rel { stream: out.clone(), cols: out_cols }, qualifier: None };
            for item in &items {
                if let SelectItem::ExprWithAlias { expr: e @ Expr::Identifier(_), alias } = item {
                    self.copy_lineage(e, std::slice::from_ref(&g), &filled, &alias.value);
                }
            }
            // keyed as the window is
            self.note_keys(&group[1..], &[&in_scope]);
            self.view(&filled, simple_query(items, table_function("gap_fill", gap_args), None), false);
            return Ok(Rel { stream: filled, cols: names });
        }
        if having.is_none() {
            return Ok(Rel { stream: out, cols: names });
        }
        let kept = self.fresh("h");
        self.stream(&kept, &names);
        let q = simple_query(
            names.iter().map(|n| SelectItem::UnnamedExpr(ident(n))).collect(),
            table(&out, None),
            Some(ident("__having")),
        );
        self.view(&kept, q, false);
        let g = Scoped { rel: Rel { stream: out.clone(), cols: out_cols }, qualifier: None };
        for n in &names {
            self.copy_lineage(&ident(n), std::slice::from_ref(&g), &kept, n);
            if self.ordered.contains(&(out.clone(), n.clone())) {
                self.ordered.insert((kept.clone(), n.clone()));
            }
        }
        Ok(Rel { stream: kept, cols: names })
    }

    /// The view before a window: the select's relations, joins and WHERE, as one stream of
    /// the plain columns its items, keys and time read (and each computed key as `__k<i>`).
    /// Returns the stream, its columns, and each column reference's name in it.
    fn pre_view(
        &mut self,
        s: &mut Select,
        scopes: &[Scoped],
        keys: &mut [Expr],
        ts: Option<&Expr>,
        zone: Option<&Expr>,
    ) -> R<(String, Vec<String>, HashMap<String, String>)> {
        let mut refs: Vec<(Option<String>, String)> = vec![];
        let mut note = |e: &Expr| {
            let mut e = e.clone();
            let _ = visit_expressions_mut(&mut e, |x| {
                match x {
                    Expr::Identifier(i) => refs.push((None, i.value.clone())),
                    Expr::CompoundIdentifier(ids) if ids.len() == 2 => {
                        refs.push((Some(ids[0].value.clone()), ids[1].value.clone()))
                    }
                    _ => {}
                }
                ControlFlow::<()>::Continue(())
            });
        };
        for i in &s.projection {
            if let SelectItem::ExprWithAlias { expr, .. } = i {
                note(expr);
            }
        }
        if let Some(h) = &s.having {
            note(h);
        }
        for k in keys.iter() {
            note(k);
        }
        if let Some(t) = ts {
            note(t);
        }
        let aliases: Vec<String> = s
            .projection
            .iter()
            .filter_map(|i| match i {
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                _ => None,
            })
            .collect();
        let (mut items, mut cols, mut rename) = (vec![], vec![], HashMap::new());
        for (q, c) in refs {
            let key = match &q {
                Some(q) => format!("{q}.{c}"),
                None => c.clone(),
            };
            if rename.contains_key(&key) {
                continue;
            }
            // a column of one of the relations (an item's alias is not a column)
            let all: Vec<&Scoped> = scopes.iter().collect();
            let e = match &q {
                Some(q) => Expr::CompoundIdentifier(vec![Ident::new(q), Ident::new(&c)]),
                None => ident(&c),
            };
            if resolve_col(&e, &all).is_none() {
                if q.is_none() && (aliases.contains(&c) || c == "window_start" || c == "window_end") {
                    continue;
                }
                return Err(format!("unknown column {e}"));
            }
            let name = unique(&c, &cols);
            items.push(SelectItem::ExprWithAlias { expr: e.clone(), alias: Ident::new(&name) });
            cols.push(name.clone());
            rename.insert(key, name);
        }
        // computed keys: columns of their own
        for (i, k) in keys.iter_mut().enumerate() {
            if matches!(k, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                continue;
            }
            let name = format!("__k{i}");
            let text = k.to_string();
            items.push(SelectItem::ExprWithAlias { expr: k.clone(), alias: Ident::new(&name) });
            cols.push(name.clone());
            // its items name it
            for item in &mut s.projection {
                if let SelectItem::ExprWithAlias { expr, .. } = item {
                    if expr.to_string() == text {
                        *expr = ident(&name);
                    }
                }
            }
            *k = ident(&name);
        }
        if let Some(t) = ts {
            if !matches!(t, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                return Err(format!("time_bucket(width, {t}): the time is a column of the table"));
            }
            if let Some(z) = zone {
                let local = crate::expr::call_of("timezone", vec![z.clone(), t.clone()]);
                items.push(SelectItem::ExprWithAlias { expr: local, alias: Ident::new("__time") });
                cols.push("__time".into());
            }
        }
        if items.is_empty() {
            // count(*) alone reads no column: keep one
            let sc = &scopes[0];
            let c = sc.rel.cols[0].clone();
            items.push(SelectItem::ExprWithAlias { expr: qualified(sc, &c), alias: Ident::new(&c) });
            cols.push(c);
        }
        let out = self.fresh("p");
        self.stream(&out, &cols);
        self.taint(&out, scopes, false);
        for item in &items {
            if let SelectItem::ExprWithAlias { expr, alias } = item {
                self.copy_lineage(expr, scopes, &out, &alias.value);
            }
        }
        let mut pre = s.clone();
        pre.projection = items;
        pre.group_by = GroupByExpr::Expressions(vec![], vec![]);
        pre.having = None;
        pre.distinct = None;
        let q = Query { body: Box::new(SetExpr::Select(Box::new(pre))), ..empty_query() };
        self.view(&out, q, false);
        // the window reads only the new stream
        s.from = vec![];
        s.selection = None;
        Ok((out, cols, rename))
    }
}

/// `OVER w ... WINDOW w AS (...)`: each named window written out where it is used.
fn inline_windows(s: &mut Select) -> R<()> {
    let defs: HashMap<String, ast::WindowSpec> = std::mem::take(&mut s.named_window)
        .into_iter()
        .map(|d| match d.1 {
            ast::NamedWindowExpr::WindowSpec(w) => Ok((d.0.value, w)),
            ast::NamedWindowExpr::NamedWindow(n) => Err(format!("WINDOW {} AS {n}: write the window out", d.0)),
        })
        .collect::<R<_>>()?;
    let mut missing = None;
    for item in &mut s.projection {
        let (SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. }) = item else { continue };
        let _ = visit_expressions_mut(e, |x| {
            if let Expr::Function(f) = x {
                if let Some(ast::WindowType::NamedWindow(n)) = &f.over {
                    match defs.get(&n.value) {
                        Some(w) => f.over = Some(ast::WindowType::WindowSpec(w.clone())),
                        None => missing = Some(n.value.clone()),
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    missing.map_or(Ok(()), |n| Err(format!("OVER {n}: no WINDOW {n} AS (...)")))
}

fn number(n: i64) -> Expr {
    Expr::value(AstValue::Number(n.to_string(), false))
}

fn string(s: &str) -> Expr {
    Expr::value(AstValue::SingleQuotedString(s.into()))
}

fn and(a: Expr, b: Expr) -> Expr {
    Expr::BinaryOp { left: Box::new(a), op: BinaryOperator::And, right: Box::new(b) }
}

fn flip(op: &BinaryOperator) -> Option<BinaryOperator> {
    Some(match op {
        BinaryOperator::Eq => BinaryOperator::Eq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        _ => return None,
    })
}

/// `e` as columns, each added (1) or subtracted (-1), plus a constant interval (µs):
/// `t.ts + INTERVAL '5 seconds'` is `([(t.ts, 1)], 5_000_000)`.
fn linear(e: &Expr) -> Option<(Vec<(Expr, i64)>, i64)> {
    match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Some((vec![(e.clone(), 1)], 0)),
        Expr::Nested(x) => linear(x),
        Expr::Interval(_) => Some((vec![], width_us(e).ok()?)),
        Expr::BinaryOp { left, op: op @ (BinaryOperator::Plus | BinaryOperator::Minus), right } => {
            let ((mut terms, x), (other, y)) = (linear(left)?, linear(right)?);
            let k = if *op == BinaryOperator::Plus { 1 } else { -1 };
            terms.extend(other.into_iter().map(|(e, c)| (e, c * k)));
            Some((terms, x.checked_add(y * k)?))
        }
        _ => None,
    }
}

/// A column reference of `scopes` as (its scope, its column).
fn resolve_col<'s>(e: &Expr, scopes: &[&'s Scoped]) -> Option<(&'s Scoped, String)> {
    match e {
        Expr::Identifier(i) => scopes.iter().find(|s| s.rel.cols.contains(&i.value)).map(|s| (*s, i.value.clone())),
        Expr::CompoundIdentifier(ids) if ids.len() == 2 => scopes
            .iter()
            .find(|s| s.qualifier.as_deref() == Some(ids[0].value.as_str()) && s.rel.cols.contains(&ids[1].value))
            .map(|s| (*s, ids[1].value.clone())),
        Expr::Nested(e) => resolve_col(e, scopes),
        _ => None,
    }
}

fn qualified(sc: &Scoped, c: &str) -> Expr {
    match &sc.qualifier {
        Some(q) => Expr::CompoundIdentifier(vec![Ident::new(q), Ident::new(c)]),
        None => ident(c),
    }
}

fn count_left_joins(q: &Query) -> usize {
    match q.body.as_ref() {
        SetExpr::Select(s) => s.from.iter().map(|f| f.joins.len()).sum(),
        _ => 0,
    }
}

fn empty_query() -> Query {
    let Ok(mut s) = Parser::parse_sql(&GenericDialect {}, "SELECT 1") else { unreachable!("valid SQL") };
    let Some(Statement::Query(q)) = s.pop() else { unreachable!("a query") };
    *q
}

/// `name(args)` in FROM: `tumble(stream, ts, '1m')`.
fn table_function(name: &str, args: Vec<Expr>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![Ident::new(name)]),
        alias: None,
        args: Some(ast::TableFunctionArgs {
            args: args.into_iter().map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e))).collect(),
            settings: None,
        }),
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

/// A time in SQL text as µs: `'2024-01-02 09:30'`, `TIMESTAMP '...'`; NULL as `None`.
fn time_literal(e: &Expr) -> R<Option<i64>> {
    let text = match e {
        Expr::Value(v) => match &v.value {
            AstValue::Null => return Ok(None),
            AstValue::SingleQuotedString(s) => s.clone(),
            _ => String::new(),
        },
        Expr::TypedString(t) => match &t.value.value {
            AstValue::SingleQuotedString(s) => s.clone(),
            _ => String::new(),
        },
        _ => String::new(),
    };
    crate::value::parse_datetime(text.trim().trim_end_matches('Z'))
        .map(Some)
        .ok_or(format!("{e} is not a time: write '2024-01-02 09:30:00'"))
}

/// The column a SELECT item gives.
fn select_name(i: &SelectItem) -> String {
    match i {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::UnnamedExpr(e) => item_name(e),
        i => i.to_string(),
    }
}

fn simple_query(items: Vec<SelectItem>, from: TableFactor, filter: Option<Expr>) -> Query {
    let mut q = empty_query();
    let SetExpr::Select(s) = q.body.as_mut() else { unreachable!("a select") };
    s.projection = items;
    s.from = vec![ast::TableWithJoins { relation: from, joins: vec![] }];
    s.selection = filter;
    q
}

/// Where a run's result rows go: the result sink's, kept; nothing else is made a message.
struct Collect<'a, 'e> {
    sink: &'a str,
    /// What came, in order: batches as views made them, rows made batches.
    batches: Vec<Batch>,
    rows: Vec<Row>,
    /// Rows past this many are not kept (a LIMIT without ORDER BY).
    keep: usize,
    kept: usize,
    /// Where the batches go as they come, not kept (`execute_each`), and its first error.
    each: Option<&'e mut Each<'e>>,
    failed: Option<String>,
    /// An ORDER BY with a LIMIT: the rows kept past many are cut to its first (`sorted`).
    top: Option<(&'a [Order], usize)>,
    held: usize,
    pool: &'a dyn Pool,
}

/// An ORDER BY key: (result column, descending, NULLs first).
type Order = (usize, bool, bool);

/// What takes a result's batches as they come (`execute_each`).
pub type Each<'a> = dyn FnMut(Batch) -> R<()> + 'a;

impl<'a, 'e> Collect<'a, 'e> {
    fn new(c: &'a Compiled, keep: usize, pool: &'a dyn Pool) -> Collect<'a, 'e> {
        let top = c.limit.map(|l| c.offset.saturating_add(l)).filter(|&n| n > 0 && !c.order.is_empty());
        Collect {
            sink: &c.result,
            batches: vec![],
            rows: vec![],
            keep,
            kept: 0,
            each: None,
            failed: None,
            top: top.map(|n| (&c.order[..], n)),
            held: 0,
            pool,
        }
    }

    fn rows_to_batch(&mut self) {
        if let Some(width) = self.rows.first().map(Vec::len) {
            let b = Batch::from_rows(&std::mem::take(&mut self.rows), width);
            self.take(b);
        }
    }

    /// A batch kept, or given on.
    fn take(&mut self, b: Batch) {
        match &mut self.each {
            Some(each) if self.failed.is_none() => {
                if let Err(e) = each(b) {
                    self.failed = Some(e);
                }
            }
            Some(_) => {}
            None => {
                self.held += b.len;
                self.batches.push(b);
                if let Some((order, n)) = self.top.filter(|&(_, n)| self.held > (2 * n).max(1 << 20)) {
                    self.batches = sorted(std::mem::take(&mut self.batches), order, Some(n), self.pool);
                    self.held = self.batches.iter().map(|b| b.len).sum();
                }
            }
        }
    }

    fn done(mut self) -> R<Vec<Batch>> {
        self.rows_to_batch();
        match self.failed {
            Some(e) => Err(e),
            None => Ok(self.batches),
        }
    }
}

impl Output for Collect<'_, '_> {
    fn push(&mut self, _: Emit) {}
    fn row(&mut self, sink: &str, row: &[Value]) -> bool {
        if sink == self.sink && self.kept < self.keep {
            self.rows.push(row.to_vec());
            self.kept += 1;
            // given on a batch at a time
            if (self.each.is_some() || self.top.is_some()) && self.rows.len() >= 65_536 {
                self.rows_to_batch();
            }
        }
        true
    }
    fn batch(&mut self, sink: &str, b: &Batch) -> bool {
        if sink != self.sink {
            return false;
        }
        if self.kept < self.keep {
            self.rows_to_batch();
            let n = b.len.min(self.keep - self.kept);
            self.take(if n == b.len { b.clone() } else { b.slice(0..n) });
            self.kept += n;
        }
        true
    }
}

/// A source's batches with its clock column `__row` added: the row's number, or `FIRST`.
struct Clocked<'a> {
    inner: Box<dyn crate::engine::Source + Send + 'a>,
    first: bool,
    next: i64,
}

impl crate::engine::Source for Clocked<'_> {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        let b = match self.inner.next()? {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let col = if self.first {
            Col::new(Data::Const(Value::Int(FIRST), b.len))
        } else {
            let c = Col::new(Data::Int((self.next..self.next + b.len as i64).collect()));
            self.next += b.len as i64;
            c
        };
        let mut cols = b.cols;
        cols.push(std::sync::Arc::new(col));
        Some(Ok(Batch::new(b.len, cols)))
    }
}

/// A source's batches with their time shifted by `by` µs added as its clock column `__clock`.
struct Shifted<'a> {
    inner: Box<dyn crate::engine::Source + Send + 'a>,
    clock: usize,
    by: i64,
}

impl crate::engine::Source for Shifted<'_> {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        let b = match self.inner.next()? {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let c = &b.cols[self.clock];
        let t = match c.i64s() {
            Some(v) => v.iter().map(|t| t.saturating_add(self.by)).collect(),
            None => (0..b.len).map(|i| c.get(i).i64().map_or(i64::MIN, |t| t.saturating_add(self.by))).collect(),
        };
        let mut cols = b.cols;
        cols.push(std::sync::Arc::new(Col::new(Data::Time(t))));
        Some(Ok(Batch::new(b.len, cols)))
    }
}

/// A source's rows with a time held whole and in time order (a table whose files are not),
/// rows without a time left out.
struct Sorted(std::vec::IntoIter<Batch>);

impl Sorted {
    fn new(src: &mut dyn crate::engine::Source, width: usize, clock: usize) -> R<Sorted> {
        let mut rows = vec![];
        while let Some(b) = src.next() {
            rows.extend(b?.rows().into_iter().filter(|r| r[clock].i64().is_some()));
        }
        // a stable sort: rows of one time keep their order
        rows.sort_by_key(|r: &Row| r[clock].i64());
        let batches: Vec<Batch> = rows.chunks(65_536).map(|c| Batch::from_rows(c, width)).collect();
        Ok(Sorted(batches.into_iter()))
    }
}

impl crate::engine::Source for Sorted {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        self.0.next().map(Ok)
    }
}

/// A source's batches without the rows whose time is NULL (a time operation cannot place them).
struct Timed<'a> {
    inner: Box<dyn crate::engine::Source + Send + 'a>,
    clock: usize,
}

impl crate::engine::Source for Timed<'_> {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        let b = match self.inner.next()? {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let c = &b.cols[self.clock];
        if (0..b.len).all(|i| !c.is_null(i)) {
            return Some(Ok(b));
        }
        let keep: Vec<usize> = (0..b.len).filter(|&i| !c.is_null(i)).collect();
        Some(Ok(b.take(&keep)))
    }
}

/// A source as the engine reads it.
pub type Input<'a> = Box<dyn crate::engine::Source + Send + 'a>;

/// Opens a source of a compiled query: its batches in its table's columns, those `reads` says
/// false left NULL if the reader likes (they are not read). With `parts` (a key column and a
/// count), the source's rows split into that many sources by `parts_of` that column, each in
/// the source's order (`split` does it for a reader that does not).
pub type Open<'o, 'a> = dyn FnMut(&Source, &[bool], Option<(usize, usize)>) -> R<Vec<Input<'a>>> + 'o;

/// The rows of each of `n` parts in a batch, by the hash of their `key` column's value: equal
/// keys of every source to the same part.
pub fn parts_of(col: &Col, n: usize) -> Vec<Vec<usize>> {
    let len = col.len();
    let part: Vec<usize> = match &col.data {
        Data::Const(v, _) => {
            let mut rows: Vec<Vec<usize>> = vec![vec![]; n];
            rows[part_of(v, n)] = (0..len).collect();
            return rows;
        }
        // a dictionary's: each entry's string hashed once
        Data::Str(strs) if col.nulls.is_none() && strs.codes().is_some() => {
            let codes = strs.codes().expect("checked");
            let mut of: Vec<usize> = vec![];
            codes
                .iter()
                .enumerate()
                .map(|(r, &c)| {
                    let c = c as usize;
                    if of.len() <= c {
                        of.resize(c + 1, usize::MAX);
                    }
                    if of[c] == usize::MAX {
                        of[c] = hash_bytes(strs.bytes(r)) % n;
                    }
                    of[c]
                })
                .collect()
        }
        Data::Str(strs) if col.nulls.is_none() => strs.iter_bytes().map(|s| hash_bytes(s) % n).collect(),
        _ => (0..len).map(|i| part_of(&col.get(i), n)).collect(),
    };
    // each part's rows in a vector of its size (the rows are held until their part reads them)
    let mut sizes = vec![0; n];
    part.iter().for_each(|&p| sizes[p] += 1);
    let mut rows: Vec<Vec<usize>> = sizes.into_iter().map(Vec::with_capacity).collect();
    part.iter().enumerate().for_each(|(i, &p)| rows[p].push(i));
    rows
}

/// A batch's rows by part (`parts_of`): each part's batch, empty ones left out as `None`.
pub fn split_batch(b: &Batch, key: usize, n: usize) -> Vec<Option<Batch>> {
    parts_of(&b.cols[key], n)
        .into_iter()
        .map(|r| match r.len() {
            0 => None,
            k if k == b.len => Some(b.clone()),
            _ => Some(b.take(&r)),
        })
        .collect()
}

/// A source split into `n` parts by its `key` column, for a reader that does not split its
/// own: whichever part needs rows reads the next batch for all.
pub fn split<'a>(src: Input<'a>, key: usize, n: usize) -> Vec<Input<'a>> {
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Splitter {
        src,
        key,
        queues: vec![Default::default(); n],
        error: None,
        done: false,
    }));
    (0..n).map(|me| Box::new(Part { shared: shared.clone(), me }) as Input<'a>).collect()
}

/// A source's rows split by the hash of a key column among the parts of a run: whichever part
/// needs rows reads the source's next batch and hands each part its rows of it.
///
/// ponytail: unbounded queues; a part far behind the others (one hot key) holds the rows they
/// read past it. Bound them with a condition variable if a skewed table ever needs it.
struct Splitter<'a> {
    src: Box<dyn crate::engine::Source + Send + 'a>,
    key: usize,
    queues: Vec<std::collections::VecDeque<Batch>>,
    error: Option<String>,
    done: bool,
}

impl Splitter<'_> {
    fn fill(&mut self) {
        let Some(b) = self.src.next() else {
            self.done = true;
            return;
        };
        let b = match b {
            Ok(b) => b,
            Err(e) => {
                self.error = Some(e);
                self.done = true;
                return;
            }
        };
        let n = self.queues.len();
        for (p, part) in split_batch(&b, self.key, n).into_iter().enumerate() {
            if let Some(part) = part {
                self.queues[p].push_back(part);
            }
        }
    }
}

fn hash_str(s: &str) -> usize {
    hash_bytes(s.as_bytes())
}

/// A string's hash by its bytes (`hash_str`'s): read without making a `str` of each.
fn hash_bytes(s: &[u8]) -> usize {
    use std::hash::Hasher;
    let mut h = rustc_hash::FxHasher::default();
    h.write(s);
    h.write_u8(0xff);
    (h.finish() >> 7) as usize
}

/// The part a key's rows go to: equal keys of every source to the same part.
fn part_of(v: &Value, n: usize) -> usize {
    match v {
        Value::Str(s) => hash_str(s) % n,
        Value::Null => 0,
        v => hash_str(&crate::format::text_value(v)) % n,
    }
}

/// One part's rows of a split source.
struct Part<'a> {
    shared: std::sync::Arc<std::sync::Mutex<Splitter<'a>>>,
    me: usize,
}

impl crate::engine::Source for Part<'_> {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        let mut s = self.shared.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if let Some(b) = s.queues[self.me].pop_front() {
                return Some(Ok(b));
            }
            if let Some(e) = &s.error {
                return Some(Err(e.clone()));
            }
            if s.done {
                return None;
            }
            s.fill();
        }
    }
}

/// Runs a compiled query over its sources, opened by `open`, on `pool`. A source with a time
/// column is read in its order; one that turns out not to be is read again, whole, and sorted.
/// A query whose stateful steps are all keyed by one column of each source runs in parts side by
/// side, each on the rows of some keys (`Compiled::partition`). Returns the result's rows,
/// ordered and limited as the query says.
pub fn execute<'a>(c: &Compiled, open: &mut Open<'_, 'a>, pool: &dyn Pool) -> R<Vec<Row>> {
    Ok(execute_batches(c, open, pool)?.iter().flat_map(Batch::rows).collect())
}

/// `execute`, its result's rows in batches of its columns (as the views made them: nothing is
/// made rows that was not).
pub fn execute_batches<'a>(c: &Compiled, open: &mut Open<'_, 'a>, pool: &dyn Pool) -> R<Vec<Batch>> {
    match run(c, open, pool, false, None) {
        Err(e) if e.contains("not in clock order") => run(c, open, pool, true, None),
        r => r,
    }
    .map_err(|e| c.humane(&e))
}

/// `execute_batches`, each batch given to `each` as the query makes it where the result is in
/// the order its rows come (no ORDER BY, OFFSET or keyed parts to order, no table read in time
/// order that a second run might read again): none is held. Otherwise when it is done.
pub fn execute_each<'a>(c: &Compiled, open: &mut Open<'_, 'a>, pool: &dyn Pool, each: &mut Each<'_>) -> R<()> {
    let streams = c.order.is_empty()
        && c.offset == 0
        && c.partition.is_none()
        && c.sources.iter().all(|s| matches!(s.clock, Clock::Row | Clock::First));
    if !streams {
        return execute_batches(c, open, pool)?.into_iter().try_for_each(each);
    }
    let rest = run(c, open, pool, false, Some(&mut *each)).map_err(|e| c.humane(&e))?;
    rest.into_iter().try_for_each(each)
}

/// A source as one run reads it: rows without a time left out, put in time order if `sort`,
/// numbered where it has no time column.
fn clocked<'a>(
    s: &Source,
    src: Box<dyn crate::engine::Source + Send + 'a>,
    sort: bool,
) -> R<Box<dyn crate::engine::Source + Send + 'a>> {
    Ok(match &s.clock {
        Clock::Column(col) | Clock::Shifted(col, _) => {
            let clock = s.columns.iter().position(|c| c.0 == *col).ok_or(format!("no column {col}"))?;
            let mut src: Box<dyn crate::engine::Source + Send + 'a> = Box::new(Timed { inner: src, clock });
            if sort {
                src = Box::new(Sorted::new(src.as_mut(), s.columns.len(), clock)?);
            }
            if let Clock::Shifted(_, by) = s.clock {
                src = Box::new(Shifted { inner: src, clock, by });
            }
            src
        }
        Clock::Row | Clock::First => Box::new(Clocked { inner: src, first: s.clock == Clock::First, next: 0 }),
    })
}

fn run<'a, 'e>(
    c: &Compiled,
    open: &mut Open<'_, 'a>,
    pool: &dyn Pool,
    sort: bool,
    each: Option<&'e mut Each<'e>>,
) -> R<Vec<Batch>> {
    let mut h = Historical::new(&c.catalog)?;
    for s in &c.sources {
        match &s.clock {
            Clock::Column(col) => h.set_clock(&s.stream, col)?,
            Clock::Shifted(..) => h.set_clock(&s.stream, "__clock")?,
            _ => h.set_clock(&s.stream, "__row")?,
        }
    }
    let mut parts = match (&c.partition, pool.threads()) {
        (Some(_), n) if n > 1 => n,
        _ => 1,
    };
    // a global aggregate over keyed steps: each part's groups, merged (`merge`)
    if parts > 1 && !c.merge.is_empty() {
        let mut held = h.clone();
        if c.merge.iter().all(|t| held.hold(t)) {
            h = held;
        } else {
            parts = 1;
        }
    }
    let mut raw: Vec<Vec<Input<'a>>> = vec![];
    for s in &c.sources {
        let reads = h.reads(&s.stream);
        let mut reads = reads[..s.columns.len().min(reads.len())].to_vec();
        let key = c
            .partition
            .as_ref()
            .and_then(|p| p.get(&s.stream))
            .and_then(|col| s.columns.iter().position(|x| x.0 == *col));
        if let Some(i) = key {
            reads[i] = true;
        }
        if let Clock::Shifted(col, _) = &s.clock {
            reads[s.columns.iter().position(|x| x.0 == *col).expect("its time column")] = true;
        }
        let split = key.filter(|_| parts > 1).map(|k| (k, parts));
        if s.table == Table::Named(ONE_ROW.into()) {
            let one = Batch::new(1, vec![std::sync::Arc::new(Col::new(Data::Const(Value::Int(1), 1)))]);
            let mut got: Vec<Input<'a>> = vec![Box::new(Sorted(vec![one].into_iter()))];
            got.extend((1..split.map_or(1, |s| s.1)).map(|_| Box::new(Sorted(vec![].into_iter())) as Input<'a>));
            raw.push(got);
            continue;
        }
        let got = open(s, &reads, split)?;
        if got.len() != if split.is_some() { parts } else { 1 } {
            return Err(format!("internal: {} parts of {}", got.len(), s.table));
        }
        raw.push(got);
    }
    // a LIMIT without ORDER BY: the rows past it are not kept
    let keep = match (c.limit, c.order.is_empty() && c.partition.is_none()) {
        (Some(l), true) => c.offset.saturating_add(l),
        _ => usize::MAX,
    };
    let mut rows = if parts > 1 {
        // parts[i] holds part i of every source
        let mut by_part: Vec<Vec<Input<'a>>> = (0..parts).map(|_| vec![]).collect();
        for src in raw {
            for (i, p) in src.into_iter().enumerate() {
                by_part[i].push(p);
            }
        }
        // each part's rows, and its run, whose held windows the first one's absorbs
        type Ran = Option<R<(Vec<Batch>, Historical)>>;
        let results: Vec<std::sync::Mutex<Ran>> = (0..parts).map(|_| std::sync::Mutex::new(None)).collect();
        let jobs: Vec<crate::engine::Task> = by_part
            .into_iter()
            .enumerate()
            .map(|(me, srcs)| {
                let (mut h, results) = (h.clone(), &results);
                Box::new(move || {
                    let r = (|| {
                        let mut inputs: Vec<(String, Box<dyn crate::engine::Source + '_>)> = vec![];
                        for (s, src) in c.sources.iter().zip(srcs) {
                            inputs.push((s.stream.clone(), clocked(s, src, sort)?));
                        }
                        let mut out = Collect::new(c, keep, &crate::engine::Serial);
                        h.run(inputs, c.range(), &crate::engine::Serial, &mut out)?;
                        out.done()
                    })()
                    .map(|rows| (rows, h));
                    *results[me].lock().unwrap_or_else(|p| p.into_inner()) = Some(r);
                }) as crate::engine::Task
            })
            .collect();
        pool.run(jobs);
        let (mut all, mut first) = (vec![], None::<Historical>);
        for r in results {
            let (rows, mut h) = r.into_inner().unwrap_or_else(|p| p.into_inner()).ok_or("a part did not run")??;
            all.extend(rows);
            match &mut first {
                Some(f) => f.absorb(&mut h),
                None => first = Some(h),
            }
        }
        if let Some(mut f) = first.filter(|_| !c.merge.is_empty()) {
            let mut out = Collect::new(c, keep, pool);
            f.close_held(&crate::engine::Serial, &mut out)?;
            all.extend(out.done()?);
        }
        all
    } else {
        let mut inputs: Vec<(String, Box<dyn crate::engine::Source + 'a>)> = vec![];
        for (s, mut src) in c.sources.iter().zip(raw) {
            inputs.push((s.stream.clone(), clocked(s, src.remove(0), sort)?));
        }
        // given on as they come, when the caller takes them so (one part: in the order they come)
        let streamed = each.is_some();
        let mut out = Collect::new(c, keep, pool);
        out.each = each;
        h.run(inputs, c.range(), pool, &mut out)?;
        let kept = out.kept;
        let batches = out.done()?;
        if streamed {
            return Ok(match (&c.empty, kept) {
                (Some(e), 0) => vec![Batch::from_rows(std::slice::from_ref(e), e.len())],
                _ => batches,
            });
        }
        batches
    };
    if rows.iter().all(|b| b.len == 0) {
        if let Some(e) = &c.empty {
            rows = vec![Batch::from_rows(std::slice::from_ref(e), e.len())];
        }
    }
    // a keyed query's rows in one order however many parts ran it: by every column
    if c.partition.is_some() && c.order.is_empty() {
        let width = rows.iter().map(|b| b.cols.len()).max().unwrap_or(0);
        let keys: Vec<(usize, bool, bool)> = (0..width).map(|i| (i, false, false)).collect();
        rows = sorted(rows, &keys, None, pool);
    }
    Ok(c.finish_batches(rows, pool))
}

/// Batches in one, its rows in the order of `keys` (column, descending, NULLs first), ties in
/// the order they came; the first `keep` only. The keys are read as numbers where they can be
/// (strings of a dictionary: their ranks), the rows sorted in parts on `pool`'s threads (a
/// part's first `keep` only), then merged.
fn sorted(batches: Vec<Batch>, keys: &[(usize, bool, bool)], keep: Option<usize>, pool: &dyn Pool) -> Vec<Batch> {
    if batches.iter().all(|b| b.len == 0) {
        return batches;
    }
    let batches: Vec<Batch> = batches.into_iter().filter(|b| b.len > 0).collect();
    let total: usize = batches.iter().map(|b| b.len).sum();
    let keep = keep.unwrap_or(total).min(total);
    let keys: Vec<SortKey> = keys
        .iter()
        .map(|&(k, desc, nulls_first)| {
            SortKey::of(&batches.iter().map(|b| &*b.cols[k]).collect::<Vec<_>>(), desc, nulls_first, pool)
        })
        .collect();
    let packed = packed(&keys, total);
    let order: Vec<usize> = match keys.as_slice() {
        // keys of numbers in 64 bits together: (key, row) pairs
        _ if packed.is_some() => {
            let pairs = packed.expect("packed").into_iter().zip(0..).collect();
            sort_parallel(pairs, keep, pool, &|a, b| a.cmp(b)).into_iter().map(|(_, r)| r).collect()
        }
        // one key of numbers: (key, row) pairs, NULLs apart
        [SortKey { key: Key::U(v), nulls, nulls_first }] => {
            let null = |r: usize| nulls.as_ref().is_some_and(|n| n[r]);
            let pairs: Vec<(u64, usize)> = (0..total).filter(|&r| !null(r)).map(|r| (v[r], r)).collect();
            let rows = sort_parallel(pairs, keep, pool, &|a, b| a.cmp(b)).into_iter().map(|(_, r)| r);
            let nulls = (0..total).filter(|&r| null(r));
            if *nulls_first {
                nulls.chain(rows).take(keep).collect()
            } else {
                rows.chain(nulls).take(keep).collect()
            }
        }
        _ => sort_parallel((0..total).collect(), keep, pool, &|&a, &b| {
            keys.iter().map(|k| k.cmp(a, b)).find(|o| o.is_ne()).unwrap_or_else(|| a.cmp(&b))
        }),
    };
    gather(&batches, &order, pool)
}

/// An ORDER BY key's values: numbers as `u64`s in their order (descending: reversed), strings,
/// or values of several kinds; and its NULLs.
struct SortKey {
    key: Key,
    nulls: Option<crate::column::Buf<bool>>,
    nulls_first: bool,
}

enum Key {
    U(Vec<u64>),
    /// Strings, descending or not.
    S(crate::column::Strs, bool),
    V(Vec<Value>, bool),
}

impl SortKey {
    /// The key of the column of these parts (a batch's each).
    fn of(cols: &[&Col], desc: bool, nulls_first: bool, pool: &dyn Pool) -> SortKey {
        let u = |v: Vec<u64>| Key::U(if desc { v.into_iter().map(|x| !x).collect() } else { v });
        let nulls = cols.iter().any(|c| c.nulls.is_some()).then(|| {
            cols.iter().flat_map(|c| (0..c.len()).map(move |i| c.nulls.as_ref().is_some_and(|n| n[i]))).collect()
        });
        let same = cols.windows(2).all(|w| std::mem::discriminant(&w[0].data) == std::mem::discriminant(&w[1].data));
        let key = match cols.first().map(|c| &c.data) {
            Some(Data::Str(_)) if same => {
                let strs: Vec<&crate::column::Strs> = cols
                    .iter()
                    .map(|c| match &c.data {
                        Data::Str(s) => s,
                        _ => unreachable!("checked"),
                    })
                    .collect();
                match ranks(&strs, pool) {
                    Some(r) => u(r),
                    None => match Col::concat(cols).data {
                        Data::Str(s) => Key::S(s, desc),
                        _ => unreachable!("strings"),
                    },
                }
            }
            Some(Data::Int(_) | Data::Time(_) | Data::UInt(_) | Data::Bool(_) | Data::F64(_) | Data::F32(_))
                if same =>
            {
                let mut v = Vec::with_capacity(cols.iter().map(|c| c.len()).sum());
                for c in cols {
                    match &c.data {
                        Data::Int(x) | Data::Time(x) => v.extend(x.iter().map(|&x| x as u64 ^ 1 << 63)),
                        Data::UInt(x) => v.extend_from_slice(x),
                        Data::Bool(x) => v.extend(x.iter().map(|&x| x as u64)),
                        Data::F64(x) => v.extend(x.iter().map(|&x| float_key(x))),
                        Data::F32(x) => v.extend(x.iter().map(|&x| float_key(x as f64))),
                        _ => unreachable!("checked"),
                    }
                }
                u(v)
            }
            _ => {
                let c = Col::concat(cols);
                let v = (0..c.len()).map(|i| c.get(i)).collect();
                return SortKey { key: Key::V(v, desc), nulls: None, nulls_first };
            }
        };
        SortKey { key, nulls, nulls_first }
    }

    /// Rows `a` and `b` in its order (`compare_ordered`'s).
    fn cmp(&self, a: usize, b: usize) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        if let Some(n) = &self.nulls {
            match (n[a], n[b]) {
                (true, true) => return Equal,
                (true, false) => return if self.nulls_first { Less } else { Greater },
                (false, true) => return if self.nulls_first { Greater } else { Less },
                _ => {}
            }
        }
        match &self.key {
            Key::U(v) => v[a].cmp(&v[b]),
            Key::S(s, desc) => {
                let o = s.get(a).cmp(s.get(b));
                if *desc {
                    o.reverse()
                } else {
                    o
                }
            }
            Key::V(v, desc) => compare_ordered(&v[a], &v[b], *desc, self.nulls_first),
        }
    }
}

/// Keys of numbers as one `u64` a row, if their ranges fit it: each its value less its least,
/// in as many bits as its range needs (and one for NULL, if it has some), the first key highest.
fn packed(keys: &[SortKey], total: usize) -> Option<Vec<u64>> {
    let mut out = vec![0u64; total];
    let mut width = 0;
    for k in keys {
        let Key::U(v) = &k.key else { return None };
        let null = |r: usize| k.nulls.as_ref().is_some_and(|n| n[r]);
        let (lo, hi) = (0..total).filter(|&r| !null(r)).fold((u64::MAX, 0), |(l, h), r| (l.min(v[r]), h.max(v[r])));
        let span = 64 - hi.saturating_sub(lo).leading_zeros();
        let bits = span + k.nulls.is_some() as u32;
        width += bits;
        if width > 64 {
            return None;
        }
        // NULLs first: 0, the others a 1 above their value; last: past every value
        let (when_null, flag) = match (&k.nulls, k.nulls_first) {
            (None, _) => (0, 0),
            (Some(_), true) => (0, 1 << span),
            (Some(_), false) => (1 << span, 0),
        };
        for (r, o) in out.iter_mut().enumerate() {
            let part = if null(r) { when_null } else { flag | (v[r] - lo) };
            *o = o.checked_shl(bits).unwrap_or(0) | part;
        }
    }
    Some(out)
}

/// A float as a `u64` in `order_by`'s order: -0 as 0, NaN after every other.
fn float_key(f: f64) -> u64 {
    if f.is_nan() {
        return u64::MAX;
    }
    let b = (if f == 0.0 { 0.0 } else { f }).to_bits();
    if b >> 63 == 1 {
        !b
    } else {
        b | 1 << 63
    }
}

/// Strings of these parts as their ranks (one string, one rank), if they are few (a tenth of
/// the rows at most); `None` else, to compare as strings. A part a job: a dictionary's codes
/// read, other strings hashed.
fn ranks(parts: &[&crate::column::Strs], pool: &dyn Pool) -> Option<Vec<u64>> {
    let total: usize = parts.iter().map(|s| s.len()).sum();
    // each part's rows' ids of their strings, and each id's string
    type Found<'s> = std::sync::Mutex<Option<(Vec<u32>, Vec<&'s str>)>>;
    let slots: Vec<Found> = parts.iter().map(|_| Default::default()).collect();
    let jobs = parts.iter().zip(&slots).map(|(&s, slot)| {
        Box::new(move || {
            let found = match s.codes() {
                Some(codes) => {
                    let mut first: Vec<Option<usize>> = vec![None; codes.iter().max().map_or(0, |m| *m as usize + 1)];
                    for (r, &c) in codes.iter().enumerate() {
                        first[c as usize].get_or_insert(r);
                    }
                    (codes.to_vec(), first.iter().map(|r| r.map_or("", |r| s.get(r))).collect())
                }
                None => {
                    let most = s.len() / 2 + 64;
                    let mut seen: HashMap<&str, u32> = HashMap::new();
                    let mut ids = Vec::with_capacity(s.len());
                    for r in 0..s.len() {
                        let n = seen.len() as u32;
                        ids.push(*seen.entry(s.get(r)).or_insert(n));
                        if seen.len() > most {
                            return;
                        }
                    }
                    let mut texts = vec![""; seen.len()];
                    seen.into_iter().for_each(|(t, i)| texts[i as usize] = t);
                    (ids, texts)
                }
            };
            *slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(found);
        }) as crate::engine::Task<'_>
    });
    pool.run(jobs.collect());
    let found: Vec<(Vec<u32>, Vec<&str>)> =
        slots.into_iter().map(|m| m.into_inner().unwrap_or_else(|p| p.into_inner())).collect::<Option<_>>()?;
    let mut all: Vec<&str> = found.iter().flat_map(|(_, t)| t.iter().copied()).collect();
    all.sort_unstable();
    all.dedup();
    if all.len() > total / 10 + 1024 {
        return None;
    }
    let mut out = Vec::with_capacity(total);
    for (ids, texts) in found {
        let rank: Vec<u64> = texts.iter().map(|t| all.binary_search(t).expect("ranked") as u64).collect();
        out.extend(ids.iter().map(|&i| rank[i as usize]));
    }
    Some(out)
}

/// `items` in `cmp`'s order (a total one), the first `keep` only: parts sorted a job each
/// (their first `keep` chosen first), then cut at the same values into as many ranges, each
/// range of every part merged a job each.
fn sort_parallel<T: Copy + Send + Sync>(
    mut items: Vec<T>,
    keep: usize,
    pool: &dyn Pool,
    cmp: &(dyn Fn(&T, &T) -> std::cmp::Ordering + Sync),
) -> Vec<T> {
    fn lock<T: Default>(m: std::sync::Mutex<T>) -> T {
        m.into_inner().unwrap_or_else(|p| p.into_inner())
    }
    let parts = pool.threads().clamp(1, items.len() / pool.part_rows() + 1);
    let size = items.len().div_ceil(parts).max(1);
    let lens: Vec<std::sync::Mutex<usize>> = items.chunks(size).map(|_| Default::default()).collect();
    let jobs = items.chunks_mut(size).zip(&lens).map(|(part, len)| {
        Box::new(move || {
            if keep < part.len() {
                part.select_nth_unstable_by(keep, cmp);
            }
            let n = keep.min(part.len());
            part[..n].sort_unstable_by(cmp);
            *len.lock().unwrap_or_else(|p| p.into_inner()) = n;
        }) as crate::engine::Task<'_>
    });
    pool.run(jobs.collect());
    let lens: Vec<usize> = lens.into_iter().map(lock).collect();
    if lens.len() <= 1 {
        items.truncate(lens.first().copied().unwrap_or(0));
        return items;
    }
    let runs: Vec<&[T]> = items.chunks(size).zip(&lens).map(|(p, &n)| &p[..n]).collect();
    // the values the ranges are cut at: of a sample of every part
    let mut sample: Vec<T> =
        runs.iter().flat_map(|r| (1..=parts).filter_map(move |i| r.get(i * r.len() / (parts + 1)))).copied().collect();
    sample.sort_unstable_by(cmp);
    let cuts: Vec<T> = (1..parts).filter_map(|i| sample.get(i * sample.len() / parts)).copied().collect();
    // each part's ranges: where each cut falls in it
    let bounds: Vec<Vec<usize>> = runs
        .iter()
        .map(|r| {
            let mut b = vec![0];
            b.extend(cuts.iter().map(|c| r.partition_point(|x| cmp(x, c).is_lt())));
            b.push(r.len());
            b
        })
        .collect();
    let ranges = cuts.len() + 1;
    // the ranges needed for the first `keep`
    let mut before = 0;
    let needed: Vec<(usize, usize)> = (0..ranges)
        .map_while(|g| {
            let n: usize = bounds.iter().map(|b| b[g + 1] - b[g]).sum();
            let at = before;
            before += n;
            (at < keep).then(|| (g, keep - at))
        })
        .collect();
    let outs: Vec<std::sync::Mutex<Vec<T>>> = needed.iter().map(|_| Default::default()).collect();
    let (runs, bounds) = (&runs, &bounds);
    let jobs = needed.iter().zip(&outs).map(|(&(g, want), out)| {
        Box::new(move || {
            let mut m: Vec<T> = runs.iter().zip(bounds).flat_map(|(r, b)| r[b[g]..b[g + 1]].iter().copied()).collect();
            // sorted runs one after another: the stable sort merges them
            m.sort_by(cmp);
            m.truncate(want);
            *out.lock().unwrap_or_else(|p| p.into_inner()) = m;
        }) as crate::engine::Task<'_>
    });
    pool.run(jobs.collect());
    let mut sorted = Vec::with_capacity(keep.min(before));
    outs.into_iter().for_each(|o| sorted.extend_from_slice(&lock(o)));
    sorted
}

/// The rows at `order` (of `batches` one after another), in a batch for each part of it, a
/// column of a part a job: each row read from its batch, in one pass.
fn gather(batches: &[Batch], order: &[usize], pool: &dyn Pool) -> Vec<Batch> {
    if order.is_empty() {
        return batches.iter().take(1).map(|b| b.slice(0..0)).collect();
    }
    let width = batches.first().map_or(0, |b| b.cols.len());
    // each batch's first row
    let starts: Vec<usize> = batches.iter().scan(0, |at, b| Some(std::mem::replace(at, *at + b.len))).collect();
    let size = order.len().div_ceil(pool.threads().max(1)).max(pool.part_rows());
    let parts: Vec<&[usize]> = order.chunks(size).collect();
    // each part's rows as (batch, row of it), a part a job
    let ats: Vec<std::sync::Mutex<Vec<(u32, u32)>>> = parts.iter().map(|_| Default::default()).collect();
    let starts = &starts;
    let jobs = parts.iter().zip(&ats).map(|(&part, at)| {
        Box::new(move || {
            *at.lock().unwrap_or_else(|p| p.into_inner()) = part
                .iter()
                .map(|&r| {
                    let b = starts.partition_point(|&s| s <= r) - 1;
                    (b as u32, (r - starts[b]) as u32)
                })
                .collect();
        }) as crate::engine::Task<'_>
    });
    pool.run(jobs.collect());
    let ats: Vec<Vec<(u32, u32)>> =
        ats.into_iter().map(|a| a.into_inner().unwrap_or_else(|p| p.into_inner())).collect();
    let slots: Vec<std::sync::Mutex<Option<Col>>> = (0..parts.len() * width).map(|_| Default::default()).collect();
    let slots_ref = &slots;
    let mut jobs: Vec<crate::engine::Task<'_>> = vec![];
    for (p, at) in ats.iter().enumerate() {
        for j in 0..width {
            jobs.push(Box::new(move || {
                let cols: Vec<&Col> = batches.iter().map(|b| &*b.cols[j]).collect();
                let at = at.iter().map(|&(b, r)| (b as usize, r as usize));
                *slots_ref[p * width + j].lock().unwrap_or_else(|p| p.into_inner()) = Some(gather_col(&cols, at));
            }));
        }
    }
    pool.run(jobs);
    let mut slots =
        slots.into_iter().map(|s| s.into_inner().unwrap_or_else(|p| p.into_inner()).expect("a column taken"));
    parts
        .iter()
        .map(|part| {
            Batch::new(part.len(), (0..width).map(|_| std::sync::Arc::new(slots.next().expect("a column"))).collect())
        })
        .collect()
}

/// The rows at `at` (part, row of it) of a column in parts.
fn gather_col(cols: &[&Col], at: impl Iterator<Item = (usize, usize)> + Clone) -> Col {
    let same = cols.windows(2).all(|w| std::mem::discriminant(&w[0].data) == std::mem::discriminant(&w[1].data));
    let nulls = cols
        .iter()
        .any(|c| c.nulls.is_some())
        .then(|| at.clone().map(|(b, r)| cols[b].nulls.as_ref().is_some_and(|n| n[r])).collect());
    macro_rules! read {
        ($v:ident) => {
            Data::$v(
                at.map(|(b, r)| match &cols[b].data {
                    Data::$v(x) => x[r],
                    _ => unreachable!("checked"),
                })
                .collect(),
            )
        };
    }
    let data = match &cols[0].data {
        Data::Int(_) if same => read!(Int),
        Data::Time(_) if same => read!(Time),
        Data::UInt(_) if same => read!(UInt),
        Data::F64(_) if same => read!(F64),
        Data::F32(_) if same => read!(F32),
        Data::Bool(_) if same => read!(Bool),
        Data::Str(_) if same => Data::Str(
            at.map(|(b, r)| match &cols[b].data {
                Data::Str(s) => s.get(r),
                _ => unreachable!("checked"),
            })
            .collect(),
        ),
        // values of several kinds
        _ => return Col::from_values(at.map(|(b, r)| cols[b].get(r)).collect()),
    };
    Col { data, nulls }
}

/// Two values in an ORDER BY's order: NULLs where `nulls_first` says, the others ascending or
/// descending.
fn compare_ordered(x: &Value, y: &Value, desc: bool, nulls_first: bool) -> std::cmp::Ordering {
    match (x.is_null(), y.is_null()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) if nulls_first => std::cmp::Ordering::Less,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) if nulls_first => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        _ if desc => order_by(y, x),
        _ => order_by(x, y),
    }
}

/// A value as a result shows it: `NULL`, a time to the second unless it has a fraction.
pub fn text(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Time(us) => {
            let t = crate::format::text_value(v).to_string();
            if us.rem_euclid(1_000_000) == 0 {
                t.trim_end_matches(".000000").to_string()
            } else {
                t.trim_end_matches('0').to_string()
            }
        }
        v => crate::format::text_value(v).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trades(_: &Table) -> R<Vec<(String, Type)>> {
        Ok(vec![
            ("ts".into(), Type::Time(6)),
            ("symbol".into(), Type::Str),
            ("price".into(), Type::F64),
            ("size".into(), Type::F64),
        ])
    }

    #[test]
    fn durations() {
        assert_eq!(duration("1m"), Some(60_000_000));
        assert_eq!(duration("15s"), Some(15_000_000));
        assert_eq!(duration("100ms"), Some(100_000));
        assert_eq!(duration("250us"), Some(250));
        assert_eq!(duration("5 minutes"), Some(300_000_000));
        assert_eq!(duration("1 hour"), Some(3_600_000_000));
        assert_eq!(duration("1d"), Some(86_400_000_000));
        assert_eq!(duration("0s"), None);
        assert_eq!(duration("1 fortnight"), None);
        assert_eq!(tumble_width(60_000_000), "1m");
        assert_eq!(tumble_width(1_500_000), "1500ms");
    }

    #[test]
    fn a_bar_query_is_a_window_on_the_time_column() {
        let c = compile(
            "SELECT time_bucket('1m', ts) AS minute, symbol, first(price, ts) AS open, max(price) AS high \
             FROM 'trades.parquet' GROUP BY minute, symbol ORDER BY minute, symbol LIMIT 10",
            &mut trades,
        )
        .unwrap();
        assert_eq!(c.columns, ["minute", "symbol", "open", "high"]);
        assert_eq!(c.sources[0].clock, Clock::Column("ts".into()));
        assert_eq!(c.widths, [60_000_000]);
        assert_eq!(c.limit, Some(10));
        assert_eq!(c.order, [(0, false, false), (1, false, false)]);
        let e = c.explain();
        assert!(e.contains("tumble(__src1, ts, '1m')"), "{e}");
        if let Err(e) = crate::engine::Engine::new(&c.catalog) {
            panic!("{e}\n{}", c.explain());
        }
    }

    #[test]
    fn where_and_computed_keys_go_through_a_view_first() {
        let c = compile(
            "SELECT upper(symbol) AS s, count(*) AS n FROM 'x.parquet' WHERE price > 0 GROUP BY 1 HAVING count(*) > 1",
            &mut trades,
        )
        .unwrap();
        assert_eq!(c.catalog.views.len(), 3, "{}", c.explain());
        assert_eq!(c.sources[0].clock, Clock::Row);
    }

    #[test]
    fn refusals_say_what_to_write() {
        let err = |q: &str| compile(q, &mut trades).unwrap_err();
        assert!(err("SELECT * FROM 'a' t ASOF JOIN 'b' q ON t.symbol = q.symbol AND t.ts > q.price * 2").contains(">="));
        assert!(err("SELECT * FROM 'a' INTERSECT SELECT * FROM 'b'").contains("UNION"));
        assert!(err("SELECT * FROM 'a', 'b'").contains("JOIN"));
        assert!(err("SELECT * FROM (SELECT * FROM 'a' LIMIT 1)").contains("outermost"));
    }
}
