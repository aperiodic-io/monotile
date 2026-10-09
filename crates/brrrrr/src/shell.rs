//! `brrrrr sql`: statements over files and object stores (`brrrrr_lake`), from the command
//! line, a file, standard input, or an interactive shell.
use anyhow::{bail, Context, Result};
use brrrrr_lake::write::{self, Out};
use brrrrr_lake::{Answer, Lake};
use std::cell::RefCell;
use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// The statements to run (`;`-separated); without them, those of --file or standard input,
    /// or an interactive shell.
    pub sql: Option<String>,
    /// Statements from a file.
    #[arg(short, long, value_name = "FILE")]
    pub file: Option<PathBuf>,
    /// Names a location as a table: `trades=data/trades/*.parquet`, `quotes=s3://bucket/quotes/`.
    #[arg(short, long = "table", value_name = "NAME=LOCATION")]
    pub tables: Vec<String>,
    /// How results are written: table (on a terminal), csv, json or parquet (to a file or a
    /// pipe). By default a table on a terminal and CSV otherwise.
    #[arg(long, value_name = "FORMAT")]
    pub format: Option<String>,
    /// Writes the last statement's result to this file (its format from its name), local or an
    /// object store URL.
    #[arg(short, long, value_name = "PATH")]
    pub output: Option<String>,
    /// Threads per query; by default as many as there are CPUs.
    #[arg(long, value_name = "N")]
    pub threads: Option<usize>,
    /// Rows a table shows at most (the first and last halves).
    #[arg(long, value_name = "N", default_value_t = 40)]
    pub max_rows: usize,
}

/// `brrrrr` alone: the shell, with the options' defaults (40 rows a table shows).
impl Default for Args {
    fn default() -> Args {
        Args { sql: None, file: None, tables: vec![], format: None, output: None, threads: None, max_rows: 40 }
    }
}

/// Splits a script into statements at `;` outside quotes and comments.
pub fn statements(script: &str) -> Vec<String> {
    let (mut out, mut cur) = (vec![], String::new());
    let mut chars = script.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => {
                quote = None;
                cur.push(c);
            }
            (Some(_), c) => cur.push(c),
            (None, '\'' | '"' | '`') => {
                quote = Some(c);
                cur.push(c);
            }
            (None, '-') if chars.peek() == Some(&'-') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        cur.push('\n');
                        break;
                    }
                }
            }
            (None, ';') => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            (None, c) => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

pub fn run(a: Args) -> Result<()> {
    let mut lake = Lake::new();
    if let Some(t) = a.threads {
        lake.threads = t.max(1);
    }
    for t in &a.tables {
        let (n, l) = t.split_once('=').with_context(|| format!("--table {t}: NAME=LOCATION"))?;
        lake.register(n.trim(), l.trim());
    }
    let script = match (&a.sql, &a.file) {
        (Some(s), _) => Some(s.clone()),
        (None, Some(f)) => Some(std::fs::read_to_string(f).with_context(|| f.display().to_string())?),
        (None, None) if !std::io::stdin().is_terminal() => {
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s)?;
            Some(s)
        }
        _ => None,
    };
    let Some(script) = script else { return interactive(lake, &a) };
    let format = match &a.format {
        Some(f) => Out::parse(f).with_context(|| format!("--format {f}: table, csv, json or parquet"))?,
        None if std::io::stdout().is_terminal() => Out::Table,
        None => Out::Csv,
    };
    let stmts = statements(&script);
    for (i, s) in stmts.iter().enumerate() {
        let answer = lake.execute(s).map_err(|e| anyhow::anyhow!(brrrrr_lake::message(&e)))?;
        let last = i + 1 == stmts.len();
        match (&a.output, last) {
            (Some(path), true) if answer.message.is_none() => {
                let out = match &a.format {
                    Some(_) => format,
                    None => Out::of_path(path).unwrap_or(Out::Parquet),
                };
                let tmp = std::env::temp_dir().join(format!(".brrrrr-out-{}", std::process::id()));
                let mut f = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
                write::write(&mut f, out, &answer.columns, &answer.rows, usize::MAX)?;
                f.flush()?;
                drop(f);
                let r = lake.files.upload(&tmp, path).map_err(|e| anyhow::anyhow!(brrrrr_lake::message(&e)));
                let _ = std::fs::remove_file(&tmp);
                r?;
                eprintln!("{} rows written to {path}", answer.rows.len());
            }
            _ => show(&answer, format, a.max_rows)?,
        }
    }
    Ok(())
}

fn show(a: &Answer, format: Out, max_rows: usize) -> Result<()> {
    if let Some(m) = &a.message {
        eprintln!("{m}");
        return Ok(());
    }
    if a.columns.is_empty() {
        return Ok(());
    }
    if format == Out::Parquet && std::io::stdout().is_terminal() {
        bail!("Parquet is not for a terminal: write it to a file (-o result.parquet) or a pipe");
    }
    let mut out = std::io::stdout().lock();
    write::write(&mut out, format, &a.columns, &a.rows, max_rows)?;
    Ok(())
}

const HELP: &str = "\
A statement ends with ;  and may span lines. Tab completes keywords, functions, tables and
columns, and file names inside quotes. Ctrl-C stops a running query; Ctrl-D leaves.

Files and object stores are tables, in quotes:
  FROM 'trades.parquet' LIMIT 5;                       -- SELECT * FROM ...
  SELECT symbol, count(*) AS n FROM 'data/*.csv' GROUP BY symbol;
  DESCRIBE 's3://bucket/trades/';                      -- its columns and types
Bars, VWAP and as-of joins:
  SELECT time_bucket('1m', ts) AS minute, symbol, first(price, ts) AS open, max(price) AS high,
         min(price) AS low, last(price, ts) AS close, vwap(price, size) AS vwap
  FROM 'trades.parquet' GROUP BY minute, symbol ORDER BY minute;
  SELECT t.ts, t.symbol, t.price, q.bid, q.ask
  FROM 'trades.parquet' t ASOF JOIN 'quotes.parquet' q ON t.symbol = q.symbol AND t.ts >= q.ts;
Names and results:
  CREATE TABLE trades AS 'data/trades/';               -- name a location
  CREATE VIEW bars AS SELECT ...;                      -- name a query
  COPY (SELECT ...) TO 'bars.parquet';                 -- or .csv, .json, s3://...
  SHOW TABLES;   EXPLAIN SELECT ...;
Commands
  .tables              the tables and views          .mode table|csv|json   how answers print
  .timer on|off        each statement's time (on)    .rows N                rows a table shows
  .read FILE           runs a file's statements      .help  .quit
More: https://aperiodic-io.github.io/monotile/docs/
";

/// Keywords the shell completes, in the case the word is typed in.
const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "ORDER BY",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "AS",
    "ON",
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "NULL",
    "LIKE",
    "ILIKE",
    "BETWEEN",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "JOIN",
    "LEFT JOIN",
    "ASOF JOIN",
    "ASOF LEFT JOIN",
    "UNION ALL",
    "DISTINCT",
    "WITH",
    "OVER",
    "PARTITION BY",
    "INTERVAL",
    "DESC",
    "ASC",
    "CREATE",
    "VIEW",
    "TABLE",
    "REPLACE",
    "DROP",
    "COPY",
    "TO",
    "DESCRIBE",
    "SHOW TABLES",
    "EXPLAIN",
    "TRUE",
    "FALSE",
    "CAST",
    "FILTER",
    "QUALIFY",
];

/// Functions the shell completes (an opening parenthesis follows).
const FUNCTIONS: &[&str] = &[
    "abs",
    "approx_count_distinct",
    "approx_quantile",
    "arg_max",
    "arg_min",
    "avg",
    "ceil",
    "coalesce",
    "concat",
    "concat_ws",
    "contains",
    "corr",
    "count",
    "count_if",
    "cume_dist",
    "date_bin",
    "date_diff",
    "date_part",
    "date_trunc",
    "dayofweek",
    "dayofyear",
    "dense_rank",
    "ema",
    "ends_with",
    "epoch",
    "epoch_ms",
    "epoch_ns",
    "epoch_us",
    "exp",
    "extract",
    "first",
    "first_value",
    "floor",
    "greatest",
    "ifnull",
    "interpolate",
    "isodow",
    "kurtosis",
    "lag",
    "last",
    "last_value",
    "lead",
    "least",
    "left",
    "length",
    "ln",
    "locf",
    "log",
    "lower",
    "lpad",
    "ltrim",
    "max",
    "max_by",
    "mean",
    "median",
    "min",
    "min_by",
    "now",
    "ntile",
    "nullif",
    "percent_rank",
    "percentile_cont",
    "power",
    "quantile_cont",
    "rank",
    "read_csv",
    "read_json",
    "read_parquet",
    "regexp_extract",
    "regexp_matches",
    "regexp_replace",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "row_number",
    "rpad",
    "rtrim",
    "sign",
    "skewness",
    "split_part",
    "sqrt",
    "starts_with",
    "stddev",
    "stddev_samp",
    "strftime",
    "strpos",
    "substr",
    "sum",
    "sum_if",
    "time_bucket",
    "time_bucket_gapfill",
    "timezone",
    "to_timestamp",
    "trim",
    "trunc",
    "twap",
    "upper",
    "var_samp",
    "variance",
    "vwap",
    "weighted_avg",
];

/// Files a quoted path completes to: data, compressed data, and directories.
const DATA: &[&str] = &[".parquet", ".csv", ".tsv", ".json", ".jsonl", ".ndjson", ".gz", ".zst", ".dbn"];

/// The word at `pos` of `line` and what it completes to: `(where the word starts, candidates)`.
/// Inside a quote, paths (`paths` of what is typed, as `'file'`, or `'dir/` to go on); otherwise
/// keywords, functions, `tables`, and the `columns` of the tables the statement names after FROM
/// or JOIN (names or quoted paths).
fn candidates(
    line: &str,
    pos: usize,
    tables: &[String],
    columns: &dyn Fn(&str) -> Vec<String>,
    paths: &dyn Fn(&str) -> Vec<String>,
) -> (usize, Vec<String>) {
    let before = &line[..pos];
    if before.matches('\'').count() % 2 == 1 {
        let at = before.rfind('\'').unwrap_or(0);
        let typed = &before[at + 1..];
        let out = paths(typed).into_iter().map(|p| if p.ends_with('/') { format!("'{p}") } else { format!("'{p}'") });
        return (at, out.collect());
    }
    let start = before.rfind(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.')).map_or(0, |i| i + 1);
    let word = &before[start..];
    // a qualified column (`t.price`): only the part after the dot
    let (start, word) = match word.rfind('.') {
        Some(d) => (start + d + 1, &word[d + 1..]),
        None => (start, word),
    };
    let after_from = before[..start].split_whitespace().last().is_some_and(|w| {
        w.eq_ignore_ascii_case("FROM") || w.eq_ignore_ascii_case("JOIN") || w.eq_ignore_ascii_case("DESCRIBE")
    });
    if word.is_empty() && !after_from {
        return (pos, vec![]);
    }
    let lower = word.chars().all(|c| !c.is_ascii_uppercase()) && !word.is_empty();
    let fits = |c: &str| c.len() >= word.len() && c[..word.len()].eq_ignore_ascii_case(word);
    let mut out: Vec<String> = tables.iter().filter(|t| fits(t)).cloned().collect();
    if !after_from {
        // the columns of the statement's tables: names, and quoted paths, after FROM or JOIN
        let words: Vec<&str> = line.split_whitespace().collect();
        for w in words.windows(2) {
            if w[0].eq_ignore_ascii_case("FROM") || w[0].eq_ignore_ascii_case("JOIN") {
                let t = w[1].trim_end_matches([';', ',', ')']);
                out.extend(columns(t).into_iter().filter(|c| fits(c)));
            }
        }
        out.extend(FUNCTIONS.iter().filter(|f| fits(f)).map(|f| format!("{f}(")));
        out.extend(
            KEYWORDS.iter().filter(|k| fits(k)).map(|k| if lower { k.to_ascii_lowercase() } else { k.to_string() }),
        );
    }
    let mut seen = std::collections::HashSet::new();
    out.retain(|c| seen.insert(c.clone()));
    (start, out)
}

/// The paths `typed` (a path's start) goes on to: directories (ending `/`) and data files.
fn paths(typed: &str) -> Vec<String> {
    let (dir, base) = match typed.rfind('/') {
        Some(i) => (&typed[..=i], &typed[i + 1..]),
        None => ("", typed),
    };
    let Ok(entries) = std::fs::read_dir(if dir.is_empty() { "." } else { dir }) else { return vec![] };
    let mut out: Vec<String> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let is_dir = e.file_type().is_ok_and(|t| t.is_dir());
            let data = DATA.iter().any(|x| name.ends_with(x));
            (name.starts_with(base) && !name.starts_with('.') && (is_dir || data))
                .then(|| format!("{dir}{name}{}", if is_dir { "/" } else { "" }))
        })
        .collect();
    out.sort();
    out
}

/// The shell's line editing: completion (`candidates`), and a statement that goes on over lines
/// until its `;` (a command, `.help`, is one line).
struct Helper {
    lake: Rc<RefCell<Lake>>,
}

impl rustyline::completion::Completer for Helper {
    type Candidate = String;

    fn complete(&self, line: &str, pos: usize, _: &rustyline::Context<'_>) -> rustyline::Result<(usize, Vec<String>)> {
        let mut lake = self.lake.borrow_mut();
        let tables: Vec<String> = lake
            .execute("SHOW TABLES")
            .map_or(vec![], |a| a.rows.iter().map(|r| r[0].str().unwrap_or_default().to_string()).collect());
        let columns = |t: &str| -> Vec<String> {
            // a table's columns, from DESCRIBE: a name, or a local path (no download to complete)
            let local = t.starts_with('\'') && !t.contains("://");
            if !local && !tables.iter().any(|n| n == t) {
                return vec![];
            }
            self.lake
                .try_borrow_mut()
                .ok()
                .and_then(|mut l| l.execute(&format!("DESCRIBE {t}")).ok())
                .map_or(vec![], |a| a.rows.iter().map(|r| r[0].str().unwrap_or_default().to_string()).collect())
        };
        drop(lake);
        Ok(candidates(line, pos, &tables, &columns, &paths))
    }
}

impl rustyline::validate::Validator for Helper {
    fn validate(
        &self,
        ctx: &mut rustyline::validate::ValidationContext<'_>,
    ) -> rustyline::Result<rustyline::validate::ValidationResult> {
        let t = ctx.input().trim();
        Ok(if t.is_empty() || t.starts_with('.') || t.ends_with(';') {
            rustyline::validate::ValidationResult::Valid(None)
        } else {
            rustyline::validate::ValidationResult::Incomplete
        })
    }
}

impl rustyline::hint::Hinter for Helper {
    type Hint = String;
}
impl rustyline::highlight::Highlighter for Helper {}
impl rustyline::Helper for Helper {}

fn interactive(lake: Lake, a: &Args) -> Result<()> {
    use rustyline::error::ReadlineError;
    let lake = Rc::new(RefCell::new(lake));
    let config =
        rustyline::Config::builder().completion_type(rustyline::CompletionType::List).auto_add_history(false).build();
    let mut ed = rustyline::Editor::<Helper, rustyline::history::DefaultHistory>::with_config(config)?;
    ed.set_helper(Some(Helper { lake: lake.clone() }));
    let history = std::env::var("HOME").map(|h| PathBuf::from(h).join(".brrrrr_history")).ok();
    if let Some(h) = &history {
        let _ = ed.load_history(h);
    }
    // Ctrl-C while a query runs stops it (at its next rows); while typing, it clears the line
    let stop = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, stop.clone())?;
    println!(
        "brrrrr {} · SQL over files and object stores · .help for help, Ctrl-D to leave",
        env!("CARGO_PKG_VERSION")
    );
    let (mut mode, mut timer, mut max_rows) = (Out::Table, true, a.max_rows);
    loop {
        let input = match ed.readline("brrrrr> ") {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(e.into()),
        };
        let t = input.trim();
        if t.is_empty() {
            continue;
        }
        let _ = ed.add_history_entry(t);
        let script = if t.starts_with('.') {
            let mut w = t.split_whitespace();
            match (w.next(), w.next()) {
                (Some(".quit" | ".exit" | ".q"), _) => break,
                (Some(".help"), _) => print!("{HELP}"),
                (Some(".tables"), _) => report(lake.borrow_mut().execute("SHOW TABLES"), mode, max_rows, None),
                (Some(".mode"), Some(m)) => match Out::parse(m).filter(|m| *m != Out::Parquet) {
                    Some(m) => mode = m,
                    None => eprintln!(".mode table, csv or json"),
                },
                (Some(".timer"), Some(v)) => timer = v == "on",
                (Some(".rows"), Some(n)) => max_rows = n.parse().unwrap_or(max_rows),
                (Some(".read"), Some(f)) => match std::fs::read_to_string(f) {
                    Ok(text) => {
                        run_all(&lake, &text, &stop, mode, max_rows, timer);
                    }
                    Err(e) => eprintln!("error: {f}: {e}"),
                },
                _ => eprintln!("unknown command {t}: .help lists them"),
            }
            continue;
        } else {
            input
        };
        run_all(&lake, &script, &stop, mode, max_rows, timer);
    }
    if let Some(h) = &history {
        let _ = ed.save_history(h);
    }
    Ok(())
}

/// Runs a script's statements, each answer printed; Ctrl-C (`stop`) stops a query and the
/// statements after it.
fn run_all(lake: &Rc<RefCell<Lake>>, script: &str, stop: &AtomicBool, mode: Out, max_rows: usize, timer: bool) {
    for s in statements(script) {
        stop.store(false, Ordering::Relaxed);
        let start = Instant::now();
        let query = matches!(
            s.split_whitespace().next().map(str::to_ascii_uppercase).as_deref(),
            Some("SELECT" | "WITH" | "FROM" | "PIVOT")
        ) || s.starts_with('(');
        let r = if query { lake.borrow().query_until(&s, stop) } else { lake.borrow_mut().execute(&s) };
        if r.is_err() && stop.load(Ordering::Relaxed) {
            eprintln!("canceled ({:.3} s)", start.elapsed().as_secs_f64());
            return;
        }
        report(r, mode, max_rows, timer.then(|| start.elapsed()));
    }
}

fn report(r: Result<Answer>, mode: Out, max_rows: usize, took: Option<std::time::Duration>) {
    match r {
        Ok(a) => {
            if let Err(e) = show(&a, mode, max_rows) {
                eprintln!("error: {e:#}");
            }
            if let Some(t) = took {
                eprintln!("({:.3} s)", t.as_secs_f64());
            }
        }
        Err(e) => eprintln!("error: {}", brrrrr_lake::message(&e)),
    }
}

#[cfg(test)]
mod tests {
    use super::{candidates, statements};

    fn complete(line: &str) -> Vec<String> {
        let tables = vec!["trades".to_string(), "quotes".to_string()];
        let columns = |t: &str| match t {
            "trades" => vec!["ts".into(), "symbol".into(), "price".into(), "size".into()],
            "'data/q.csv'" => vec!["bid".into(), "ask".into()],
            _ => vec![],
        };
        let paths = |typed: &str| {
            ["data/", "data/q.csv", "trades.parquet"]
                .iter()
                .filter(|p| p.starts_with(typed))
                .map(|p| p.to_string())
                .collect()
        };
        candidates(line, line.len(), &tables, &columns, &paths).1
    }

    #[test]
    fn tab_completes_keywords_in_the_case_typed_functions_tables_and_paths() {
        assert_eq!(complete("SEL"), ["SELECT"]);
        assert_eq!(complete("sel"), ["select"]);
        assert_eq!(complete("SELECT time_b"), ["time_bucket(", "time_bucket_gapfill("]);
        assert_eq!(complete("SELECT * FROM "), ["trades", "quotes"]);
        assert_eq!(complete("SELECT * FROM tr"), ["trades"]);
        assert_eq!(complete("SELECT * FROM 'tr"), ["'trades.parquet'"]);
        assert_eq!(complete("FROM 'da"), ["'data/", "'data/q.csv'"]);
        assert!(complete("SELECT ").is_empty(), "nothing typed, nothing offered");
    }

    #[test]
    fn tab_completes_the_columns_of_the_tables_a_statement_names() {
        assert_eq!(complete("SELECT pr"), Vec::<String>::new(), "no table named yet");
        let line = "SELECT symbol, pr FROM trades";
        let at = "SELECT symbol, pr".len();
        let tables = vec!["trades".to_string()];
        let columns = |t: &str| if t == "trades" { vec!["price".to_string(), "size".into()] } else { vec![] };
        let (start, got) = candidates(line, at, &tables, &columns, &|_| vec![]);
        assert_eq!((start, got), ("SELECT symbol, ".len(), vec!["price".to_string()]));
        let line = "SELECT t.si FROM trades t";
        let (start, got) = candidates(line, "SELECT t.si".len(), &tables, &columns, &|_| vec![]);
        assert_eq!((start, got), ("SELECT t.".len(), vec!["size".to_string(), "sign(".into()]), "a qualified column");
        assert_eq!(complete("SELECT b FROM 'data/q.csv' WHERE b"), ["bid", "between"]);
    }

    #[test]
    fn a_script_splits_at_semicolons_outside_quotes_and_comments() {
        assert_eq!(statements("SELECT 1; SELECT ';' -- a; comment\n; ;"), ["SELECT 1", "SELECT ';'"]);
        assert_eq!(statements("FROM 'a;b.csv'"), ["FROM 'a;b.csv'"]);
    }
}
