//! Proton's pipeline SQL: a token-level DDL envelope parser,
//! with query bodies and expressions parsed by `sqlparser` (docs/adr/0003).
use crate::value::Type;
use sqlparser::ast::{visit_expressions_mut, visit_relations_mut, Expr, Ident, ObjectNamePart, Query, VisitMut};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer, Whitespace, Word};
use std::collections::{BTreeMap, HashMap};
use std::ops::ControlFlow;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    /// `MATERIALIZED` expression (such as a sink's `_tp_message_headers`): computed on every
    /// insert, whatever the query supplies.
    pub materialized: Option<Expr>,
    /// `DEFAULT` expression: computed only when the inserting query has no
    /// column of this name.
    pub default: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Kind {
    /// `CREATE STREAM`: an internal edge between views.
    Stream,
    /// `CREATE EXTERNAL STREAM ... SETTINGS type='kafka'`.
    External,
    /// `CREATE EXTERNAL TABLE ... SETTINGS type='s3'` with its `PARTITION BY`.
    Table(Option<Box<Expr>>),
}

#[derive(Clone, Debug)]
pub struct Stream {
    pub name: String,
    pub kind: Kind,
    pub columns: Vec<Column>,
    pub settings: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct View {
    pub name: String,
    pub target: String,
    pub query: Query,
    /// `ASOF` was written before each `LEFT JOIN`, in order (rewritten to plain joins for parsing).
    pub asof: Vec<bool>,
    /// `EMIT AFTER WINDOW CLOSE WITH DELAY ...` in microseconds.
    pub emit_delay_us: Option<i64>,
    /// The query's `SETTINGS`: spill thresholds (no effect here) and `keep_versions`.
    pub settings: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    pub streams: BTreeMap<String, Stream>,
    /// In creation order: the order in which views receive rows.
    pub views: Vec<View>,
}

/// A located parse error (1-based line and column in the script).
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub line: u64,
    pub column: u64,
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}: {}", self.line, self.column, self.message)
    }
}

type R<T> = Result<T, Error>;

struct Cursor {
    toks: Vec<TokenWithSpan>,
    pos: usize,
}

impl Cursor {
    fn peek(&self) -> &Token {
        self.toks.get(self.pos).map_or(&Token::EOF, |t| &t.token)
    }
    fn err<T>(&self, message: impl Into<String>) -> R<T> {
        let span = self.toks.get(self.pos.min(self.toks.len().saturating_sub(1))).map(|t| t.span.start);
        let (line, column) = span.map_or((0, 0), |s| (s.line, s.column));
        Err(Error { line, column, message: message.into() })
    }
    fn next(&mut self) -> Token {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }
    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Token::Word(w) if w.value.eq_ignore_ascii_case(kw) && w.quote_style.is_none())
    }
    fn kw(&mut self, kw: &str) -> bool {
        let ok = self.is_kw(kw);
        self.pos += ok as usize;
        ok
    }
    fn expect_kw(&mut self, kw: &str) -> R<()> {
        if self.kw(kw) {
            Ok(())
        } else {
            self.err(format!("expected {kw}, found {}", self.peek()))
        }
    }
    fn expect(&mut self, t: Token) -> R<()> {
        if *self.peek() == t {
            self.pos += 1;
            Ok(())
        } else {
            self.err(format!("expected {t}, found {}", self.peek()))
        }
    }
    fn ident(&mut self) -> R<String> {
        match self.next() {
            Token::Word(w) => Ok(w.value),
            t => {
                self.pos -= 1;
                self.err(format!("expected an identifier, found {t}"))
            }
        }
    }
    /// Tokens up to (not including) the first top-level token matching `stop`.
    fn until(&mut self, stop: impl Fn(&Token, &Cursor) -> bool) -> Vec<TokenWithSpan> {
        let (start, mut depth) = (self.pos, 0i32);
        while *self.peek() != Token::EOF && !(depth == 0 && stop(self.peek(), self)) {
            match self.peek() {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            self.pos += 1;
        }
        self.toks[start..self.pos].to_vec()
    }
    fn at_top_kw(&self, kws: &[&str]) -> bool {
        kws.iter().any(|k| self.is_kw(k))
    }
}

/// Proton's SQL is ClickHouse's, bar what `rewrite` and `fuse_durations` fix on the tokens.
const DIALECT: ClickHouseDialect = ClickHouseDialect {};

fn parser_err(toks: &[TokenWithSpan], e: impl std::fmt::Display) -> Error {
    let s = toks.first().map(|t| t.span.start);
    Error { line: s.map_or(0, |s| s.line), column: s.map_or(0, |s| s.column), message: e.to_string() }
}

fn expr(c: &Cursor, toks: Vec<TokenWithSpan>) -> R<Expr> {
    // an empty expression is located at the token that ended it
    let at = if toks.is_empty() { c.toks[c.pos.min(c.toks.len().saturating_sub(1))..].to_vec() } else { toks.clone() };
    whole(rewrite(toks).0, &at, Parser::parse_expr)
}

/// `toks` parsed by `parse` to their end: what it leaves is an error located at it, never
/// ignored (`SELECT x FROM s WHERE x > 0 junk` is not a query).
fn whole<T>(
    toks: Vec<TokenWithSpan>,
    at: &[TokenWithSpan],
    parse: impl FnOnce(&mut Parser<'static>) -> Result<T, ParserError>,
) -> R<T> {
    let mut p = Parser::new(&DIALECT).with_tokens_with_locations(toks);
    let parsed = parse(&mut p).map_err(|e| parser_err(at, e))?;
    match p.peek_token() {
        t if t.token == Token::EOF => Ok(parsed),
        t => Err(parser_err(std::slice::from_ref(&t), format!("unexpected {} after the end", t.token))),
    }
}

/// Proton's bare durations (`tumble(s, t, 15m)`) as string literals (`'15m'`): a number and a
/// unit word written together. The tokenizer splits `15m` into both; `15 m` (a number and an
/// alias `m`), or the two apart by a comment, are not a duration. So this runs on the tokens as
/// they come, whitespace and comments included, before anything drops them.
fn fuse_durations(toks: Vec<TokenWithSpan>) -> Vec<TokenWithSpan> {
    let mut out: Vec<TokenWithSpan> = Vec::with_capacity(toks.len());
    let mut toks = toks.into_iter().peekable();
    while let Some(mut t) = toks.next() {
        if let (Token::Number(n, _), Some(Token::Word(w))) = (&t.token, toks.peek().map(|u| &u.token)) {
            if w.quote_style.is_none() && ["ms", "s", "m", "h", "d"].contains(&w.value.as_str()) {
                t.token = Token::SingleQuotedString(format!("{n}{}", w.value));
                toks.next();
            }
        }
        out.push(t);
    }
    out
}

/// Proton lexical differences, fixed on tokens: `interval` used as an identifier and
/// `ASOF LEFT JOIN` (bare durations are `fuse_durations`'). Returns the rewritten tokens and the
/// ASOF flags of each `LEFT JOIN` in order.
fn rewrite(toks: Vec<TokenWithSpan>) -> (Vec<TokenWithSpan>, Vec<bool>) {
    let sig: Vec<usize> = (0..toks.len()).filter(|&i| !matches!(toks[i].token, Token::Whitespace(_))).collect();
    let next = |i: usize| sig.iter().find(|&&j| j > i).map(|&j| &toks[j].token);
    let (mut out, mut asof, mut pending_asof) = (Vec::with_capacity(toks.len()), vec![], false);
    for (i, t) in toks.iter().enumerate() {
        let mut t = t.clone();
        match &t.token {
            Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("ASOF") => {
                pending_asof = true;
                continue;
            }
            Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("LEFT") => {
                asof.push(std::mem::take(&mut pending_asof));
            }
            // lowercase `interval` not followed by a literal is Proton's column name
            Token::Word(w)
                if w.quote_style.is_none()
                    && w.value == "interval"
                    && !matches!(next(i), Some(Token::SingleQuotedString(_) | Token::Number(..))) =>
            {
                t.token = Token::Word(Word {
                    value: "interval".into(),
                    quote_style: Some('`'),
                    keyword: sqlparser::keywords::Keyword::NoKeyword,
                });
            }
            _ => {}
        }
        out.push(t);
    }
    (out, asof)
}

fn literal(c: &mut Cursor) -> R<String> {
    match c.next() {
        Token::SingleQuotedString(s) | Token::Number(s, _) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Minus => Ok(format!("-{}", literal(c)?)),
        t => {
            c.pos -= 1;
            c.err(format!("expected a setting value, found {t}"))
        }
    }
}

fn settings(c: &mut Cursor) -> R<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    if c.kw("SETTINGS") {
        loop {
            let k = c.ident()?;
            c.expect(Token::Eq)?;
            out.insert(k, literal(c)?);
            if *c.peek() != Token::Comma {
                break;
            }
            c.next();
        }
    }
    Ok(out)
}

fn columns(c: &mut Cursor) -> R<Vec<Column>> {
    c.expect(Token::LParen)?;
    let mut cols = vec![];
    loop {
        let name = c.ident()?;
        let ty_toks =
            c.until(|t, c| matches!(t, Token::Comma | Token::RParen) || c.at_top_kw(&["MATERIALIZED", "DEFAULT"]));
        let ty_text: String = ty_toks.iter().map(|t| token_sql(&t.token)).collect();
        let ty = Type::parse(&ty_text).or_else(|e| c.err(e))?;
        let always = c.kw("MATERIALIZED");
        let computed = if always || c.kw("DEFAULT") {
            let toks = c.until(|t, _| matches!(t, Token::Comma | Token::RParen));
            Some(expr(c, toks)?)
        } else {
            None
        };
        let (materialized, default) = if always { (computed, None) } else { (None, computed) };
        cols.push(Column { name, ty, materialized, default });
        // `until` stopped at one of these or at the statement's end (located at its last token)
        match c.peek() {
            Token::Comma => {
                c.next();
            }
            Token::RParen => {
                c.next();
                break;
            }
            t => return c.err(format!("expected , or ) in column list, found {t}")),
        }
    }
    Ok(cols)
}

fn interval_us(c: &mut Cursor) -> R<i64> {
    c.expect_kw("INTERVAL")?;
    let raw = literal(c)?;
    let n: i64 = raw.trim().parse().or_else(|_| c.err(format!("interval {raw:?} is not a whole number")))?;
    let at = c.pos;
    let unit = c.ident()?.to_ascii_uppercase();
    let mul = match unit.trim_end_matches('S') {
        "MICROSECOND" => 1,
        "MILLISECOND" => 1_000,
        "SECOND" => 1_000_000,
        "MINUTE" => 60_000_000,
        "HOUR" => 3_600_000_000,
        _ => return c.err(format!("unsupported interval unit {unit}")),
    };
    match n.checked_mul(mul) {
        Some(us) if us >= 0 => Ok(us),
        _ => {
            c.pos = at - 1;
            c.err(format!("interval {n} {unit} is negative or too long"))
        }
    }
}

fn statement(c: &mut Cursor, cat: &mut Catalog) -> R<()> {
    c.expect_kw("CREATE")?;
    let external = c.kw("EXTERNAL");
    let materialized = c.kw("MATERIALIZED");
    let table = !materialized && c.kw("TABLE");
    if table && !external {
        c.pos -= 1;
        return c.err("expected STREAM (only EXTERNAL tables are supported)");
    }
    if materialized {
        c.expect_kw("VIEW")?;
    } else if !table {
        c.expect_kw("STREAM")?;
    }
    if c.kw("IF") {
        c.expect_kw("NOT")?;
        c.expect_kw("EXISTS")?;
    }
    let at = c.pos;
    let name = c.ident()?;
    if cat.streams.contains_key(&name) || cat.views.iter().any(|v| v.name == name) {
        c.pos = at;
        return c.err(format!("{name} is already defined"));
    }
    if materialized {
        c.expect_kw("INTO")?;
        let target = c.ident()?;
        c.expect_kw("AS")?;
        let body = c.until(|_, c| c.at_top_kw(&["EMIT", "SETTINGS"]));
        let emit_delay_us = if c.kw("EMIT") {
            for kw in ["AFTER", "WINDOW", "CLOSE"] {
                c.expect_kw(kw)?;
            }
            Some(if c.kw("WITH") {
                c.expect_kw("DELAY")?;
                interval_us(c)?
            } else {
                0
            })
        } else {
            None
        };
        let settings = settings(c)?; // spill thresholds do not affect results; keep_versions does
        if adhoc(&body) {
            if !settings.is_empty() {
                return c.err("SETTINGS on a view in ad-hoc SQL: they are Proton's, for views FROM tumble(...)");
            }
            splice(cat, &name, &target, &body, emit_delay_us).map_err(|e| parser_err(&body, e))?;
            return match c.peek() {
                Token::EOF => Ok(()),
                t => c.err(format!("unexpected {t} after statement")),
            };
        }
        let (toks, asof) = rewrite(body.clone());
        let query = whole(toks, &body, Parser::parse_query)?;
        cat.views.push(View { name, target, query: *query, asof, emit_delay_us, settings });
    } else {
        let columns = columns(c)?;
        let partition = if c.kw("PARTITION") {
            c.expect_kw("BY")?;
            let toks = c.until(|_, c| c.at_top_kw(&["SETTINGS"]));
            Some(expr(c, toks)?)
        } else {
            None
        };
        if c.kw("TTL") {
            c.until(|_, c| c.at_top_kw(&["SETTINGS"])); // retention only
        }
        let settings = settings(c)?;
        let kind = match (external, table) {
            (true, true) => Kind::Table(partition.map(Box::new)),
            (true, false) => Kind::External,
            _ => Kind::Stream,
        };
        cat.streams.insert(name.clone(), Stream { name, kind, columns, settings });
    }
    if *c.peek() != Token::EOF {
        return c.err(format!("unexpected {} after statement", c.peek()));
    }
    Ok(())
}

/// Whether a view's query is in the ad-hoc dialect (`brrrrr sql`'s, ADR-0018) rather than
/// Proton's: it groups (`GROUP BY`, at any depth) without a window table function, which every
/// Proton view that groups reads (`tumble(...)`; `hop` and `session` are refused either way).
/// So every view Proton's dialect plans stays Proton's, and the ad-hoc ones are those it refused.
fn adhoc(body: &[TokenWithSpan]) -> bool {
    let word =
        |t: &Token, k: &str| matches!(t, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(k));
    let grouped = body.windows(2).any(|p| word(&p[0].token, "GROUP") && word(&p[1].token, "BY"));
    let windowed = body
        .windows(2)
        .any(|p| ["tumble", "hop", "session"].iter().any(|f| word(&p[0].token, f)) && p[1].token == Token::LParen);
    grouped && !windowed
}

/// An ad-hoc view, `name`, compiled as `brrrrr sql` compiles a query (`query::compile`): its
/// tables are the catalog's streams, its result rows go into `target` (columns by name), and its
/// views and streams join the catalog as `<name>__v1`, `<name>__g2`, ... Its windows close
/// `delay` (EMIT AFTER WINDOW CLOSE WITH DELAY) after their end, as a Proton view's.
fn splice(
    cat: &mut Catalog,
    name: &str,
    target: &str,
    body: &[TokenWithSpan],
    delay: Option<i64>,
) -> Result<(), String> {
    use crate::query::{compile, Clock, Table};
    let text = body.iter().map(|t| token_sql(&t.token)).collect::<Vec<_>>().join(" ");
    let streams = &cat.streams;
    let mut resolve = |t: &Table| match t {
        Table::Named(n) => streams
            .get(n)
            .filter(|s| !matches!(s.kind, Kind::Table(_)))
            .map(|s| s.columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect())
            .ok_or(format!("no stream {n}")),
        Table::Path { .. } => Err(format!("a pipeline's view reads its streams, not {t}")),
    };
    let q = compile(&text, &mut resolve)?;
    if !q.order.is_empty() || q.limit.is_some() || q.offset > 0 {
        return Err("a view's rows go out as its windows close: ORDER BY and LIMIT are for queries".into());
    }
    for s in &q.sources {
        match (&s.table, &s.clock) {
            (Table::Named(n), Clock::Column(_)) if *n == s.stream => {}
            _ => {
                return Err(format!(
                    "{} is read here as a whole table, which a stream never is (GROUP BY without \
                     time_bucket, a JOIN on keys alone, an ASOF JOIN at a time offset): a pipeline's view \
                     groups by time_bucket(width, a time column) and joins with ASOF JOIN",
                    s.table
                ))
            }
        }
    }
    // the query's own streams and views, named after this view; its result is the target
    let own = q.catalog.streams.keys().chain(q.catalog.views.iter().map(|v| &v.name)).filter(|n| n.starts_with("__"));
    let mut names: HashMap<String, String> = own.map(|n| (n.clone(), format!("{name}{n}"))).collect();
    if let Some(taken) =
        names.values().find(|n| cat.streams.contains_key(*n) || cat.views.iter().any(|v| v.name == **n))
    {
        return Err(format!("{taken} is already defined"));
    }
    names.insert(q.result.clone(), target.to_string());
    let rename = |i: &mut Ident| {
        if let Some(n) = names.get(&i.value) {
            i.value = n.clone();
        }
    };
    for mut s in q.catalog.streams.into_values().filter(|s| s.kind == Kind::Stream) {
        s.name = names[&s.name].clone();
        cat.streams.insert(s.name.clone(), s);
    }
    for mut v in q.catalog.views {
        v.name = names[&v.name].clone();
        v.target = names.get(&v.target).cloned().unwrap_or(v.target);
        let _ = visit_relations_mut(&mut v.query, |o| {
            o.0.iter_mut().for_each(|p| {
                if let ObjectNamePart::Identifier(i) = p {
                    rename(i)
                }
            });
            ControlFlow::<()>::Continue(())
        });
        visit_exprs_mut(&mut v.query, |e| {
            match e {
                Expr::Identifier(i) => rename(i),
                Expr::CompoundIdentifier(ids) => ids.iter_mut().for_each(rename),
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
        if v.emit_delay_us.is_some() {
            v.emit_delay_us = Some(delay.unwrap_or(0));
        }
        cat.views.push(v);
    }
    Ok(())
}

/// sqlparser's `visit_expressions_mut` with the closure behind `dyn`: its walk over the whole
/// syntax tree is compiled once per node type, not once per closure (megabytes of the binary).
pub fn visit_exprs_mut<V: VisitMut>(v: &mut V, mut f: impl FnMut(&mut Expr) -> ControlFlow<()>) {
    let f: &mut dyn FnMut(&mut Expr) -> ControlFlow<()> = &mut f;
    let _ = visit_expressions_mut(v, f);
}

/// Parses a whole bootstrap script. Statements are separated by `;`; comments are ignored.
pub fn parse(script: &str) -> R<Catalog> {
    let toks = Tokenizer::new(&DIALECT, script).tokenize_with_location().map_err(|e| Error {
        line: e.location.line,
        column: e.location.column,
        message: e.message,
    })?;
    let toks: Vec<TokenWithSpan> = fuse_durations(toks)
        .into_iter()
        .filter(|t| {
            !matches!(
                t.token,
                Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_))
            )
        })
        .collect();
    let mut cat = Catalog::default();
    for stmt in toks.split(|t| t.token == Token::SemiColon) {
        let stmt: Vec<TokenWithSpan> = stmt.to_vec();
        if stmt.iter().all(|t| matches!(t.token, Token::Whitespace(_))) {
            continue;
        }
        let mut c = Cursor { toks: stmt, pos: 0 };
        // the envelope parser, and the query and expression slices it takes, see significant
        // tokens only (whitespace mattered only to fuse_durations, which has run)
        c.toks.retain(|t| !matches!(t.token, Token::Whitespace(_)));
        statement(&mut c, &mut cat)?;
    }
    Ok(cat)
}

/// A token as SQL text that reads back as the same token: the quotes inside a quoted string or
/// identifier doubled (`Token`'s own `Display` writes `'it's'` for the literal `'it''s'`).
pub fn token_sql(t: &Token) -> String {
    let quoted = |q: char, s: &str| format!("{q}{}{q}", s.replace(q, &format!("{q}{q}")));
    match t {
        Token::SingleQuotedString(s) => quoted('\'', s),
        Token::NationalStringLiteral(s) => format!("N{}", quoted('\'', s)),
        Token::DoubleQuotedString(s) => quoted('"', s),
        Token::Word(w) => match w.quote_style {
            Some(q @ ('"' | '`')) => quoted(q, &w.value),
            Some('[') => format!("[{}]", w.value),
            _ => w.value.clone(),
        },
        t => t.to_string(),
    }
}

/// Parses one Proton expression (`explain`, tests).
pub fn parse_expr(s: &str) -> R<Expr> {
    let toks = Tokenizer::new(&DIALECT, s).tokenize_with_location().map_err(|e| Error {
        line: e.location.line,
        column: e.location.column,
        message: e.message,
    })?;
    let toks = fuse_durations(toks);
    expr(&Cursor { toks: toks.clone(), pos: toks.len() }, toks)
}
