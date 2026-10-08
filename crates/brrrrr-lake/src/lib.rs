//! brrrrr's tables in files and object stores, and the statements run over them: queries
//! (`brrrrr_core::query`, ADR-0018), `EXPLAIN`, `DESCRIBE`, `SHOW TABLES`, `CREATE VIEW` and
//! `COPY (...) TO`. The shell, the server and the Python package all run statements here.
pub mod dbn;
pub mod files;
pub mod live;
pub mod ranged;
pub mod read;
pub mod tables;
pub mod write;

use anyhow::{anyhow, bail, Result};
use brrrrr_core::column::Batch;
use brrrrr_core::engine::{Pool, Row, Serial, Task};
use brrrrr_core::query::{self, Table};
use brrrrr_core::value::{Type, Value};
use files::{File, Files, Format};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

/// What a statement returned: rows (in the batches of columns the query made them in: rows
/// are made only for a caller that asks for them), or a message (`COPY`'s count, a view's
/// creation).
#[derive(Clone, Debug, Default)]
pub struct Answer {
    pub columns: Vec<String>,
    pub rows: write::Rows,
    pub message: Option<String>,
}

impl Answer {
    fn message(m: String) -> Answer {
        Answer { message: Some(m), ..Answer::default() }
    }

    fn lines(column: &str, text: &str) -> Answer {
        Answer {
            columns: vec![column.into()],
            rows: text.lines().map(|l| vec![Value::Str(l.into())]).collect::<Vec<Row>>().into(),
            message: None,
        }
    }
}

/// Jobs on as many threads as there are CPUs (the batches' parts; rows stay in order).
pub struct Threads(pub usize);

impl Pool for Threads {
    fn run<'a>(&self, jobs: Vec<Task<'a>>) {
        if self.0 <= 1 || jobs.len() <= 1 {
            jobs.into_iter().for_each(|j| j());
            return;
        }
        let queue = Mutex::new(jobs.into_iter());
        std::thread::scope(|s| {
            for _ in 0..self.0 {
                s.spawn(|| loop {
                    let job = queue.lock().expect("jobs").next();
                    match job {
                        Some(j) => j(),
                        None => break,
                    }
                });
            }
        });
    }
    fn threads(&self) -> usize {
        self.0
    }
}

/// A session: its named tables, its views and the files it has read.
pub struct Lake {
    pub files: Files,
    tables: BTreeMap<String, String>,
    views: BTreeMap<String, String>,
    /// Tables held in memory (DataFrames registered from Python): their batches.
    memory: BTreeMap<String, Vec<arrow_array::RecordBatch>>,
    /// Tables that grow (`brrrrr serve`): history in Parquet, the newest rows in memory.
    live: BTreeMap<String, std::sync::Arc<live::Live>>,
    pub threads: usize,
}

impl Default for Lake {
    fn default() -> Self {
        Lake::new()
    }
}

/// The statement's first word, upper case, after comments and blanks.
fn first_word(sql: &str) -> String {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.split_once('\n').map_or("", |r| r.1).trim_start();
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.split_once("*/").map_or("", |r| r.1).trim_start();
        } else {
            break;
        }
    }
    s.split(|c: char| !c.is_ascii_alphanumeric() && c != '_').next().unwrap_or("").to_ascii_uppercase()
}

/// `sql` after its first word and the blanks after it.
fn after_first_word(sql: &str) -> &str {
    let w = first_word(sql);
    let i = sql.to_ascii_uppercase().find(&w).map_or(0, |i| i + w.len());
    sql[i..].trim_start()
}

/// The table files a name finds in the working directory: `name.parquet`, `name.csv`, ...,
/// or a directory `name/`.
fn local_table(name: &str) -> Option<String> {
    let exts = ["parquet", "csv", "csv.gz", "tsv", "json", "jsonl", "ndjson", "jsonl.gz"];
    exts.iter().map(|e| format!("{name}.{e}")).find(|p| std::path::Path::new(p).is_file()).or_else(|| {
        let d = std::path::Path::new(name);
        d.is_dir().then(|| name.to_string())
    })
}

impl Lake {
    pub fn new() -> Lake {
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        Lake {
            files: Files::default(),
            tables: BTreeMap::new(),
            views: BTreeMap::new(),
            memory: BTreeMap::new(),
            live: BTreeMap::new(),
            threads,
        }
    }

    /// Names a location (a path, glob, directory or URL) as a table.
    pub fn register(&mut self, name: &str, location: &str) {
        self.tables.insert(name.to_string(), location.to_string());
    }

    /// Names Arrow batches as a table (a DataFrame's): queried as files are, held in memory.
    pub fn register_batches(&mut self, name: &str, batches: Vec<arrow_array::RecordBatch>) -> Result<()> {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or(anyhow!("{name}: no batches (an empty table has a schema: register one batch of no rows)"))?;
        if read::columns_of(&schema).is_empty() {
            bail!("{name}: no columns brrrrr reads (numbers, text, booleans, times)");
        }
        self.memory.insert(name.to_string(), batches);
        Ok(())
    }

    /// Names a live table.
    pub fn register_live(&mut self, t: std::sync::Arc<live::Live>) {
        self.live.insert(t.name.clone(), t);
    }

    /// The live tables, by name.
    pub fn live_tables(&self) -> &BTreeMap<String, std::sync::Arc<live::Live>> {
        &self.live
    }

    /// Forgets a table held in memory.
    pub fn unregister(&mut self, name: &str) {
        self.memory.remove(name);
    }

    /// Defines a view: a query its name stands for in later ones.
    pub fn view(&mut self, name: &str, sql: &str) -> Result<()> {
        self.views.insert(name.to_string(), sql.trim().trim_end_matches(';').to_string());
        // checked now, not when first used
        if let Err(e) = self.compile_listed(&format!("SELECT * FROM {name}")) {
            self.views.remove(name);
            return Err(e);
        }
        Ok(())
    }

    /// Runs one statement.
    pub fn execute(&mut self, sql: &str) -> Result<Answer> {
        let sql = sql.trim().trim_end_matches(';').trim();
        match first_word(sql).as_str() {
            "" => Ok(Answer::default()),
            "SELECT" | "WITH" | "FROM" | "(" | "PIVOT" => self.query(sql),
            "EXPLAIN" => {
                let c = self.compile_listed(after_first_word(sql))?.0;
                Ok(Answer::lines("pipeline", &c.explain()))
            }
            "DESCRIBE" | "DESC" => self.describe(after_first_word(sql)),
            "SHOW" => {
                let rest = after_first_word(sql).to_ascii_uppercase();
                if !rest.starts_with("TABLES") {
                    bail!("SHOW TABLES is the only SHOW");
                }
                let mut rows: Vec<Row> = self
                    .tables
                    .iter()
                    .map(|(n, l)| vec![Value::Str(n.as_str().into()), Value::Str("table".into()), Value::Str(l.as_str().into())])
                    .collect();
                rows.extend(self.live.iter().map(|(n, l)| {
                    vec![Value::Str(n.as_str().into()), Value::Str("live".into()), Value::Str(l.dir.display().to_string().into())]
                }));
                rows.extend(self.memory.keys().map(|n| {
                    vec![Value::Str(n.as_str().into()), Value::Str("dataframe".into()), Value::Str("(in memory)".into())]
                }));
                rows.extend(self.views.iter().map(|(n, q)| {
                    vec![Value::Str(n.as_str().into()), Value::Str("view".into()), Value::Str(q.as_str().into())]
                }));
                Ok(Answer { columns: vec!["name".into(), "kind".into(), "definition".into()], rows: rows.into(), message: None })
            }
            "CREATE" => self.create(after_first_word(sql)),
            "DROP" => {
                let rest = after_first_word(sql);
                let name = rest.split_whitespace().last().unwrap_or("");
                let had = self.views.remove(name).is_some() | self.tables.remove(name).is_some();
                if !had && !rest.to_ascii_uppercase().contains("IF EXISTS") {
                    bail!("no table or view {name}");
                }
                Ok(Answer::message(format!("dropped {name}")))
            }
            "COPY" => self.copy(after_first_word(sql)),
            w => bail!(
                "{w}: brrrrr runs SELECT, EXPLAIN, DESCRIBE, SHOW TABLES, CREATE VIEW, CREATE TABLE ... AS 'location', DROP and COPY"
            ),
        }
    }

    /// `CREATE [OR REPLACE] VIEW name AS query`, or `CREATE TABLE name AS 'location'`.
    fn create(&mut self, rest: &str) -> Result<Answer> {
        let up = rest.to_ascii_uppercase();
        let rest = if up.starts_with("OR REPLACE") { rest[10..].trim_start() } else { rest };
        let kind = first_word(rest);
        let rest = after_first_word(rest);
        let (name, body) = rest.split_once(|c: char| c.is_whitespace()).ok_or(anyhow!("CREATE {kind} name AS ..."))?;
        let body = body.trim_start();
        if first_word(body) != "AS" {
            bail!("CREATE {kind} {name} AS ...");
        }
        let body = after_first_word(body);
        match kind.as_str() {
            "VIEW" => {
                self.view(name, body)?;
                Ok(Answer::message(format!("view {name} created")))
            }
            "TABLE" => {
                let loc = body.trim().trim_matches(['\'', '"']);
                if body.trim().starts_with('\'') {
                    self.files.first(loc, None)?;
                    self.register(name, loc);
                    Ok(Answer::message(format!("table {name} is {loc}")))
                } else {
                    // a query's result as a table: a view, run when read
                    self.view(name, body)?;
                    Ok(Answer::message(format!("table {name} created (a view: its query runs when read)")))
                }
            }
            k => bail!("CREATE {k}: brrrrr creates VIEWs and TABLEs"),
        }
    }

    /// `COPY (query) TO 'destination' [(FORMAT csv|json|parquet)]`.
    fn copy(&mut self, rest: &str) -> Result<Answer> {
        let rest = rest.trim();
        let (q, after) = if rest.starts_with('(') {
            let mut depth = 0;
            let mut end = None;
            for (i, c) in rest.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(i);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let end = end.ok_or(anyhow!("COPY (query) TO 'file': unbalanced parentheses"))?;
            (rest[1..end].to_string(), rest[end + 1..].trim())
        } else {
            // COPY table TO ...
            let (t, a) = rest.split_once(|c: char| c.is_whitespace()).ok_or(anyhow!("COPY (query) TO 'file'"))?;
            (format!("SELECT * FROM {t}"), a.trim())
        };
        if first_word(after) != "TO" {
            bail!("COPY (query) TO 'file'");
        }
        let after = after_first_word(after);
        let (dest, opts) = match after.strip_prefix('\'').and_then(|a| a.split_once('\'')) {
            Some((d, o)) => (d.to_string(), o.trim()),
            None => bail!("COPY ... TO 'file': the destination in quotes"),
        };
        // (FORMAT parquet, PARTITION_BY (date, symbol), OVERWRITE_OR_IGNORE): DuckDB's options
        let (mut format, mut by, mut overwrite) = (None, vec![], false);
        let opts = opts.strip_prefix('(').and_then(|o| o.strip_suffix(')')).unwrap_or(opts);
        for o in split_top(opts, ',') {
            let (name, value) = o.trim().split_once(char::is_whitespace).unwrap_or((o.trim(), ""));
            let value = value.trim().trim_matches(['\'', '"']);
            match name.to_ascii_uppercase().as_str() {
                "FORMAT" => {
                    format = Some(write::Out::parse(value).ok_or(anyhow!("FORMAT {value}: parquet, csv or json"))?)
                }
                "PARTITION_BY" => {
                    let cols = value.strip_prefix('(').and_then(|v| v.strip_suffix(')')).unwrap_or(value);
                    by = cols.split(',').map(|c| c.trim().trim_matches('"').to_string()).collect();
                }
                "OVERWRITE_OR_IGNORE" => overwrite = !matches!(value.to_ascii_lowercase().as_str(), "false" | "0"),
                "OVERWRITE" | "APPEND" => {
                    bail!("COPY ... ({name}): brrrrr writes into a directory of files with OVERWRITE_OR_IGNORE")
                }
                _ => {}
            }
        }
        let format = format.or_else(|| write::Out::of_path(&dest)).unwrap_or(write::Out::Parquet);
        if !by.is_empty() && !overwrite && self.files.has_files(&dest)? {
            bail!("{dest} is not empty: write into it with OVERWRITE_OR_IGNORE (files of the same names are replaced)");
        }
        // written as the query makes its batches: none held but a few row groups being encoded
        let (rows, partitions) = std::thread::scope(|scope| {
            let mut out = Copy {
                lake: self,
                scope,
                closing: Default::default(),
                dest: dest.clone(),
                format,
                by,
                parts: None,
                rows: 0,
                partitions: 0,
            };
            let a = self.query_each(&q, &mut |columns, b| out.push(columns, &b).map_err(|e| message(&e)))?;
            out.finish(&a.columns)
        })?;
        Ok(Answer::message(match partitions {
            0 => format!("{rows} rows written to {dest}"),
            n => format!("{rows} rows written to {dest}, in {n} partitions"),
        }))
    }

    /// `DESCRIBE table`, `DESCRIBE 'file'` or `DESCRIBE query`: the columns and their types.
    fn describe(&mut self, what: &str) -> Result<Answer> {
        let sql = match first_word(what).as_str() {
            "SELECT" | "WITH" | "FROM" | "PIVOT" => what.to_string(),
            _ => format!("SELECT * FROM {what}"),
        };
        let c = self.compile_listed(&sql)?.0;
        let mut rows = vec![];
        if c.catalog.views.len() == 1 && c.sources.len() == 1 && first_word(what) != "SELECT" {
            for (n, t) in &c.sources[0].columns {
                rows.push(vec![Value::Str(n.as_str().into()), Value::Str(type_name(t).into())]);
            }
        } else {
            for n in &c.columns {
                rows.push(vec![Value::Str(n.as_str().into()), Value::Str("any".into())]);
            }
        }
        Ok(Answer { columns: vec!["column".into(), "type".into()], rows: rows.into(), message: None })
    }

    /// The query with its views made CTEs, `FROM x` alone made `SELECT * FROM x`, and `now()`
    /// the time it starts (the engine reads no clock).
    fn expand(&self, sql: &str) -> String {
        let sql = if first_word(sql) == "FROM" { format!("SELECT * {sql}") } else { sql.to_string() };
        let sql = with_now(&sql);
        // the views it reads, and theirs, each before its readers
        let mut order: Vec<String> = vec![];
        fn visit(lake: &Lake, sql: &str, order: &mut Vec<String>, depth: usize) {
            if depth > 32 {
                return;
            }
            for w in sql.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
                if let Some(body) = lake.views.get(w) {
                    if !order.iter().any(|o| o == w) {
                        visit(lake, body, order, depth + 1);
                        if !order.iter().any(|o| o == w) {
                            order.push(w.to_string());
                        }
                    }
                }
            }
        }
        visit(self, &sql, &mut order, 0);
        if order.is_empty() {
            return sql;
        }
        let ctes: Vec<String> = order.iter().map(|v| format!("{v} AS ({})", self.views[v])).collect();
        let ctes = ctes.join(", ");
        if first_word(&sql) == "WITH" {
            format!("WITH {ctes}, {}", after_first_word(&sql))
        } else {
            format!("WITH {ctes} {sql}")
        }
    }

    /// The values a PIVOT without IN pivots on: its query's (`query::Values`), run first.
    fn pivot_values(&self, sql: &str) -> std::result::Result<Vec<Value>, String> {
        let a = self.query(sql).map_err(|e| message(&e))?;
        Ok(a.rows.iter_rows().map(|r| r[0].clone()).collect())
    }

    /// A query as a pipeline that runs as rows arrive (a live view's): its joins look up
    /// tables, never a subquery's rows, which would be those of when it was compiled.
    pub fn compile(&self, sql: &str) -> Result<query::Compiled> {
        let sql = self.expand(sql);
        let found = Mutex::new(Found::default());
        query::compile_with(&sql, &mut |t| self.columns(t, &[], &found).map_err(|e| message(&e)), &mut |q| {
            self.pivot_values(q)
        })
        .map_err(|e| match lookups(&sql, true) {
            Ok((_, subs)) if !subs.is_empty() => anyhow!(
                "{e}: a live view joins tables, not subqueries, CTEs or views (their rows would be those of when \
                     the view was made)"
            ),
            _ => anyhow!(e),
        })
    }

    /// A query compiled to run once, and what it found of its tables: the subqueries its joins
    /// look up are run first, as tables of their own (`lookups`).
    ///
    /// It is compiled twice: first over each table's first file alone (its columns), then over
    /// the files of the partitions that query reads (`narrowed`), listed alone.
    fn compile_listed(&self, sql: &str) -> Result<(query::Compiled, Found)> {
        let sql = self.expand(sql);
        let Ok((c, mut found)) = self.compile_found(&sql, Found { probe: true, ..Found::default() }) else {
            // what the first files' columns cannot compile, every file's may
            return self.compile_found(&sql, Found::default());
        };
        found.probe = false;
        for (t, probe) in std::mem::take(&mut found.files) {
            let files = match c.sources.iter().find(|s| s.table == t) {
                Some(s) => self.narrowed(&c, s, &probe[0])?,
                None => self.locate(&t)?,
            };
            found.first.insert(t.clone(), probe[0].clone());
            found.files.insert(t, files);
        }
        self.compile_found(&sql, found)
    }

    fn compile_found(&self, sql: &str, found: Found) -> Result<(query::Compiled, Found)> {
        let run = |(sql, subs): (String, Vec<(String, String)>), found: Found| -> Result<(query::Compiled, Found)> {
            let found = Mutex::new(found);
            let c =
                query::compile_with(&sql, &mut |t| self.columns(t, &subs, &found).map_err(|e| message(&e)), &mut |q| {
                    self.pivot_values(q)
                })
                .map_err(|e| anyhow!(e))?;
            Ok((c, found.into_inner().expect("found")))
        };
        let lookups_only = lookups(sql, false)?;
        let n = lookups_only.1.len();
        run(lookups_only, found.clone()).or_else(|e| {
            // an ASOF JOIN of a subquery whose rows the engine cannot order by time (it computes
            // the time): its rows too, ordered as a table's are
            match lookups(sql, true)? {
                all if all.1.len() > n => run(all, found).map_err(|_| e),
                _ => Err(e),
            }
        })
    }

    /// The files of `s`'s table the query may read rows of, listed: when its conditions fix
    /// the table's leading partitions to values (`date = '2024-01-02'`, `date IN (...)`, the
    /// keys of the directories right under the table's, in order), only the directories of
    /// those values are listed (`date=2024-01-02/`). `first` is the table's first file.
    fn narrowed(&self, c: &query::Compiled, s: &query::Source, first: &File) -> Result<Vec<File>> {
        let (location, format, options) = self.location(&s.table)?;
        let root = location.trim_end_matches('/');
        let rel = first.name.strip_prefix(root).filter(|r| r.starts_with('/') && !files::has_glob(root));
        let mut dirs = vec![String::new()];
        // the leading partitions: text (a whole number may be written 09 or 9), values that
        // are directory names as they are
        let plain = |v: &String| !v.is_empty() && v.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c));
        let dirs_of_first: Vec<&str> = rel.map_or(vec![], |r| r.trim_start_matches('/').split('/').collect());
        let leading = dirs_of_first[..dirs_of_first.len().saturating_sub(1)].iter().map_while(|d| d.split_once('='));
        for (key, _) in leading {
            let text = s.columns.iter().any(|(n, t)| n == key && *t == Type::Str);
            let Some(vals) = values(c, &s.stream, key, 0).filter(|v| text && v.iter().all(plain)) else { break };
            if vals.iter().any(|v| v.parse::<i64>().is_ok()) || dirs.len() * vals.len() > 256 {
                break;
            }
            dirs = dirs.iter().flat_map(|d| vals.iter().map(move |v| format!("{d}{key}={v}/"))).collect();
        }
        let files = if dirs == [""] {
            self.files.list(&location, format.as_deref())?
        } else {
            self.files.list_under(&location, format.as_deref(), &dirs)?
        };
        Ok(with_options(files, &options))
    }

    /// The names a query may give a table: those named, viewed, held and live, and those of the
    /// table files in the working directory (`local_table`).
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> =
            self.tables.keys().chain(self.views.keys()).chain(self.memory.keys()).cloned().collect();
        names.extend(self.live.keys().cloned());
        for e in std::fs::read_dir(".").into_iter().flatten().flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            match n.split_once('.') {
                Some((stem, _)) if files::format_of(&n, None).is_some() => names.push(stem.to_string()),
                None if e.path().is_dir() => names.push(n),
                _ => {}
            }
        }
        names
    }

    /// A table's files, listed: `ready` them before reading.
    fn locate(&self, t: &Table) -> Result<Vec<File>> {
        let (location, format, options) = self.location(t)?;
        Ok(with_options(self.files.list(&location, format.as_deref())?, &options))
    }

    /// Where a table's files are, and the format its reader names.
    fn location(&self, t: &Table) -> Result<(String, Option<String>, Arc<read::Options>)> {
        Ok(match t {
            Table::Path { path, format, options } => {
                let options = options.as_deref().map(read::Options::parse).transpose()?.unwrap_or_default();
                // `delta_scan`, `iceberg_scan`: a table of that format, its files Parquet
                let format = match format.as_deref() {
                    Some(kind @ ("delta" | "iceberg")) => {
                        if !tables::is(&self.files, path, kind)? {
                            bail!("{t}: no {} table at {path}", if kind == "delta" { "Delta" } else { "Iceberg" });
                        }
                        Some("parquet".to_string())
                    }
                    f => f.map(str::to_string),
                };
                (path.clone(), format, Arc::new(options))
            }
            Table::Named(n) => match self.tables.get(n) {
                Some(l) => (l.clone(), None, Arc::default()),
                None => match local_table(n) {
                    Some(l) => (l, None, Arc::default()),
                    None => match files::closest(n, self.table_names().iter().map(String::as_str)) {
                        Some(close) => bail!("no table {n}: did you mean {close}?"),
                        None => bail!(
                            "no table {n}: query a file (FROM 'data/{n}.parquet', FROM 's3://bucket/{n}/*.parquet'), \
                             or name one (CREATE TABLE {n} AS 'path', or brrrrr sql --table {n}=path)"
                        ),
                    },
                },
            },
        })
    }

    /// Files fetched (remote ones into the cache) and their CSV delimiters sniffed.
    fn ready(&self, files: &mut [File]) -> Result<()> {
        self.files.fetch(files)?;
        for f in files {
            if let Format::Csv { delimiter: b',' } = f.format {
                if !f.name.to_ascii_lowercase().contains(".tsv") && f.options.delim.is_none() {
                    let head = read::peek(f, 64 << 10)?;
                    let mut lines = head.split(|b| *b == b'\n').skip(f.options.skip);
                    f.format = Format::Csv { delimiter: read::sniff_delimiter(lines.next().unwrap_or(&[])) };
                }
            }
        }
        Ok(())
    }

    /// A table's columns; `subs` are the subqueries that stand as tables (`lookups`), run here.
    fn columns(&self, t: &Table, subs: &[(String, String)], found: &Mutex<Found>) -> Result<Vec<(String, Type)>> {
        if let Table::Named(n) = t {
            if let Some(b) = found.lock().expect("found").rows.get(n) {
                return Ok(read::columns_of(&b[0].schema()));
            }
            if let Some((_, sql)) = subs.iter().find(|s| s.0 == *n) {
                let a = self.query(sql)?;
                let b = write::record_batch(&a.columns, &a.rows)?;
                let cols = read::columns_of(&b.schema());
                found.lock().expect("found").rows.insert(n.clone(), vec![b]);
                return Ok(cols);
            }
            if let Some(l) = self.live.get(n) {
                let st = l.state.read().unwrap_or_else(|p| p.into_inner());
                let s = st.schema.clone().ok_or(anyhow!("{n} has no rows yet, so no columns: write some first"))?;
                return Ok(read::columns_of(&s));
            }
            if let Some(b) = self.memory.get(n) {
                return Ok(read::columns_of(&b[0].schema()));
            }
        }
        let (listed, probe, first) = {
            let f = found.lock().expect("found");
            (f.files.get(t).cloned(), f.probe, f.first.get(t).cloned())
        };
        let mut files = match listed {
            Some(f) => f,
            None if probe => {
                let (location, format, options) = self.location(t)?;
                with_options(vec![self.files.first(&location, format.as_deref())?], &options)
            }
            None => self.locate(t)?,
        };
        // no file of the partitions the query reads: the columns of the table's first
        let mut head = match (files.is_empty(), first) {
            (true, Some(f)) => vec![f],
            _ => vec![],
        };
        let of = if head.is_empty() { &mut files } else { &mut head };
        // union_by_name: every file's columns
        let n = if of[0].options.union_by_name { of.len() } else { 1 };
        self.ready(&mut of[..n])?;
        let cols = read::schema(of)?;
        found.lock().expect("found").files.insert(t.clone(), files);
        Ok(cols)
    }

    /// Runs a query (`SELECT`, `WITH`, `FROM x`).
    pub fn query(&self, sql: &str) -> Result<Answer> {
        self.query_until(sql, &AtomicBool::new(false))
    }

    /// `query`, which ends with an error once `stop` is set (a cancel, a time limit): its
    /// sources' next batches see it.
    pub fn query_until(&self, sql: &str, stop: &AtomicBool) -> Result<Answer> {
        self.run_query(sql, stop, None)
    }

    /// `query`, its result's batches given to `each` (with its columns) as they are made where
    /// they can be (`query::execute_each`): none held. The answer has its columns, no rows.
    pub fn query_each(&self, sql: &str, each: &mut Each<'_>) -> Result<Answer> {
        self.run_query(sql, &AtomicBool::new(false), Some(each))
    }

    fn run_query(&self, sql: &str, stop: &AtomicBool, each: Option<&mut Each<'_>>) -> Result<Answer> {
        let (c, found) = self.compile_listed(sql)?;
        let open_sources = |s: &query::Source,
                            reads: &[bool],
                            parts: Option<(usize, usize)>|
         -> Result<Vec<query::Input>, String> {
            let e = |e: anyhow::Error| message(&e);
            if let Table::Named(n) = &s.table {
                if let Some(l) = self.live.get(n) {
                    // the files and the rows in memory as of one moment: a flush is all or nothing
                    let (files, buffer) = {
                        let st = l.state.read().unwrap_or_else(|p| p.into_inner());
                        (l.files(), st.buffer.clone())
                    };
                    let files: Vec<File> = files
                        .into_iter()
                        .map(|p| File {
                            name: p.display().to_string(),
                            partitions: vec![],
                            object: None,
                            options: Default::default(),
                            lease: None,
                            ranged: None,
                            local: p,
                            format: Format::Parquet,
                            gzip: false,
                        })
                        .collect();
                    let n_parts = parts.map_or(1, |p| p.1);
                    let mut history: Vec<query::Input> = if files.is_empty() {
                        (0..n_parts)
                            .map(|_| {
                                Box::new(read::Memory::new(n, vec![], s.columns.clone(), reads.to_vec()))
                                    as query::Input
                            })
                            .collect()
                    } else {
                        read::Parallel::open(
                            files,
                            s.columns.clone(),
                            reads.to_vec(),
                            self.threads,
                            parts,
                            c.sources.len() == 1,
                        )
                        .map_err(e)?
                        .into_iter()
                        .map(|p| Box::new(p) as query::Input)
                        .collect()
                    };
                    let recent: query::Input =
                        Box::new(read::Memory::new(n, buffer, s.columns.clone(), reads.to_vec()));
                    let recent: Vec<query::Input> = match parts {
                        Some(p) => read::Parallel::fan_out(recent, p)
                            .map_err(e)?
                            .into_iter()
                            .map(|p| Box::new(p) as query::Input)
                            .collect(),
                        None => vec![recent],
                    };
                    return Ok(history
                        .drain(..)
                        .zip(recent)
                        .map(|(a, b)| Box::new(read::Chain(Some(a), b)) as query::Input)
                        .collect());
                }
                if let Some(b) = self.memory.get(n).or(found.rows.get(n)) {
                    let m: query::Input = Box::new(read::Memory::new(n, b.clone(), s.columns.clone(), reads.to_vec()));
                    return Ok(match parts {
                        Some(p) => read::Parallel::fan_out(m, p)
                            .map_err(e)?
                            .into_iter()
                            .map(|p| Box::new(p) as query::Input)
                            .collect(),
                        None => vec![m],
                    });
                }
            }
            let mut files = match found.files.get(&s.table) {
                Some(f) => f.clone(),
                None => self.locate(&s.table).map_err(e)?,
            };
            // the files whose partitions the query keeps rows of: the others are never fetched
            files.retain(|f| {
                let known = s
                    .columns
                    .iter()
                    .filter_map(|(k, ty)| read::partition(f, k).map(|v| (k.clone(), read::partition_value(v, ty))))
                    .collect();
                reaches(&c, &s.stream, &known, 0)
            });
            self.ready(&mut files).map_err(e)?;
            if files.iter().all(|f| f.format == Format::Parquet) {
                let p = read::Parallel::open(
                    files,
                    s.columns.clone(),
                    reads.to_vec(),
                    self.threads,
                    parts,
                    c.sources.len() == 1,
                )
                .map_err(e)?;
                return Ok(p.into_iter().map(|p| Box::new(p) as query::Input).collect());
            }
            let r: query::Input = Box::new(read::Reader::new(files, s.columns.clone(), reads.to_vec()));
            Ok(match parts {
                Some(p) => {
                    read::Parallel::fan_out(r, p).map_err(e)?.into_iter().map(|p| Box::new(p) as query::Input).collect()
                }
                None => vec![r],
            })
        };
        let mut open = |s: &query::Source, reads: &[bool], parts: Option<(usize, usize)>| {
            open_sources(s, reads, parts)
                .map(|v| v.into_iter().map(|i| Box::new(Stoppable(i, stop)) as query::Input).collect())
        };
        let pool: &dyn Pool = if self.threads > 1 { &Threads(self.threads) } else { &Serial };
        if let Some(each) = each {
            let columns = c.columns.clone();
            query::execute_each(&c, &mut open, pool, &mut |b| each(&columns, b)).map_err(|e| anyhow!(e))?;
            return Ok(Answer { columns: c.columns, ..Answer::default() });
        }
        let rows = query::execute_batches(&c, &mut open, pool).map_err(|e| anyhow!(e))?;
        Ok(Answer { columns: c.columns, rows: write::Rows::of_batches(rows), message: None })
    }
}

/// The files open of a partitioned COPY at most: the least recently written one is closed
/// past them (DuckDB's `partitioned_write_max_open_files`); its partition's later rows go to a
/// file of its own, the next number.
const OPEN_PARTITIONS: usize = 100;

/// Rows held at most by a partitioned COPY's open files for their next row groups.
const HELD: usize = 1 << 21;

/// A file of a COPY being written, to its place as it is (`files::Writer`).
struct Output {
    writer: Option<write::ResultWriter<std::io::BufWriter<files::Writer>>>,
}

impl Output {
    fn create(files: &Files, dest: &str, format: write::Out, columns: &[String]) -> Result<Output> {
        let out = std::io::BufWriter::with_capacity(1 << 20, files.create(dest)?);
        Ok(Output { writer: Some(write::ResultWriter::new(out, format, columns, None)?) })
    }

    /// The file written whole, and in its place.
    fn close(mut self) -> Result<()> {
        let f = self.writer.take().expect("writer").finish()?;
        f.into_inner().map_err(|e| e.into_error())?.close()
    }
}

/// Files closed at most at a time, each on a thread of its own: their last row groups encoded,
/// synced, moved or uploaded, while the next rows are written.
const CLOSING: usize = 16;

/// `COPY ... TO`: a result written as its batches come, to one file, or, with `by`, to a file
/// in a directory of each partition (`key=value/data_0.parquet`, as DuckDB lays them out).
struct Copy<'a, 's> {
    lake: &'a Lake,
    scope: &'s std::thread::Scope<'s, 'a>,
    closing: Closing<'s>,
    dest: String,
    format: write::Out,
    by: Vec<String>,
    /// One file's, or the open partitions' (least recently written first), and each partition's
    /// files so far.
    parts: Option<Parts>,
    rows: usize,
    partitions: usize,
}

struct Parts {
    keys: Vec<usize>,
    kept: Vec<usize>,
    columns: Vec<String>,
    open: Vec<(String, Output)>,
    files: HashMap<String, usize>,
}

impl<'a, 's> Copy<'a, 's> {
    fn push(&mut self, columns: &[String], b: &Batch) -> Result<()> {
        self.rows += b.len;
        if self.parts.is_none() {
            let keys: Vec<usize> = self
                .by
                .iter()
                .map(|c| {
                    columns.iter().position(|x| x == c).ok_or(anyhow!("PARTITION_BY {c}: the result has no column {c}"))
                })
                .collect::<Result<_>>()?;
            if !keys.is_empty() && keys.len() == columns.len() {
                bail!("PARTITION_BY every column: no column is left to write in the files");
            }
            let kept: Vec<usize> = (0..columns.len()).filter(|i| !keys.contains(i)).collect();
            let columns: Vec<String> = kept.iter().map(|&i| columns[i].clone()).collect();
            let mut open = vec![];
            if keys.is_empty() {
                open.push((String::new(), Output::create(&self.lake.files, &self.dest, self.format, &columns)?));
            }
            self.parts = Some(Parts { keys, kept, columns, open, files: HashMap::new() });
        }
        let p = self.parts.as_mut().expect("parts");
        if p.keys.is_empty() {
            return p.open[0].1.writer.as_mut().expect("writer").push(b);
        }
        // each row's partition: its keys' ids, folded into one (dense) id a row
        let mut group = vec![0u32; b.len];
        for &k in &p.keys {
            let mut dense = HashMap::new();
            for (g, id) in group.iter_mut().zip(ids(&b.cols[k])) {
                let n = dense.len() as u32;
                *g = *dense.entry((*g, id)).or_insert(n);
            }
        }
        // each partition's directory (of its first row), and its rows in their order
        let mut dirs: Vec<Option<usize>> = vec![];
        let mut rows: Vec<(String, Vec<usize>)> = vec![];
        let mut at: HashMap<String, usize> = HashMap::new();
        for (r, &g) in group.iter().enumerate() {
            let g = g as usize;
            if g >= dirs.len() {
                dirs.resize(g + 1, None);
            }
            let d = *dirs[g].get_or_insert_with(|| {
                let dir: Vec<String> = p
                    .keys
                    .iter()
                    .map(|&k| {
                        let v = b.cols[k].get(r);
                        files::partition_dir(&columns[k], (!v.is_null()).then(|| query::text(&v)).as_deref())
                    })
                    .collect();
                let dir = dir.join("/");
                // two ids of one text (a dictionary may hold a string twice): one directory
                *at.entry(dir.clone()).or_insert_with(|| {
                    rows.push((dir, vec![]));
                    rows.len() - 1
                })
            });
            rows[d].1.push(r);
        }
        for (dir, at) in rows {
            let part = Batch::new(at.len(), p.kept.iter().map(|&i| std::sync::Arc::new(b.cols[i].take(&at))).collect());
            let i = match p.open.iter().position(|(d, _)| *d == dir) {
                Some(i) => i,
                None => {
                    if p.open.len() >= OPEN_PARTITIONS {
                        let (_, oldest) = p.open.remove(0);
                        close(&mut self.closing, self.scope, oldest)?;
                    }
                    let n = p.files.entry(dir.clone()).or_insert(0);
                    if *n == 0 {
                        self.partitions += 1;
                    }
                    let ext = match self.format {
                        write::Out::Csv => "csv",
                        write::Out::Json => "json",
                        _ => "parquet",
                    };
                    let root = self.dest.trim_end_matches('/');
                    // a store's URL is unescaped once (`%2F` would be `/`): the escapes stay in its key
                    let d = if files::is_url(root) && !root.starts_with("file://") {
                        dir.replace('%', "%25")
                    } else {
                        dir.clone()
                    };
                    let out = Output::create(
                        &self.lake.files,
                        &format!("{root}/{d}/data_{n}.{ext}"),
                        self.format,
                        &p.columns,
                    )?;
                    *n += 1;
                    p.open.push((dir, out));
                    p.open.len() - 1
                }
            };
            // the most recently written last
            let mut entry = p.open.remove(i);
            entry.1.writer.as_mut().expect("writer").push(&part)?;
            p.open.push(entry);
        }
        // past `HELD` rows held by the open files, the most held written as a row group
        let held = |o: &Output| o.writer.as_ref().map_or(0, |w| w.pending());
        while p.open.iter().map(|(_, o)| held(o)).sum::<usize>() > HELD {
            let (_, o) = p.open.iter_mut().max_by_key(|(_, o)| held(o)).expect("open");
            o.writer.as_mut().expect("writer").row_group()?;
        }
        Ok(())
    }

    /// Every file closed: the rows written, and the partitions.
    fn finish(mut self, columns: &[String]) -> Result<(usize, usize)> {
        if self.parts.is_none() {
            // no row: a file of the result's columns still, without a partition
            self.push(columns, &Batch::new(0, vec![]))?;
        }
        for (_, out) in self.parts.take().expect("parts").open {
            close(&mut self.closing, self.scope, out)?;
        }
        self.closing.drain(..).try_for_each(join)?;
        Ok((self.rows, self.partitions))
    }
}

/// Each row's id of its value: rows of one value share one (strings a dictionary's: rows of two
/// codes may share a string, and differ).
fn ids(c: &brrrrr_core::column::Col) -> Vec<u64> {
    use brrrrr_core::column::Data;
    if c.nulls.is_none() {
        match &c.data {
            Data::Int(v) | Data::Time(v) => return v.iter().map(|&x| x as u64).collect(),
            Data::UInt(v) => return v.to_vec(),
            Data::Str(s) => {
                if let Some(codes) = s.codes() {
                    return codes.iter().map(|&x| x.into()).collect();
                }
                let mut seen = HashMap::new();
                return (0..s.len())
                    .map(|r| {
                        let n = seen.len() as u64;
                        *seen.entry(s.get(r)).or_insert(n)
                    })
                    .collect();
            }
            _ => {}
        }
    }
    let mut seen = HashMap::new();
    (0..c.len())
        .map(|r| {
            let v = c.get(r);
            let n = seen.len() as u64;
            *seen.entry((!v.is_null()).then(|| query::text(&v))).or_insert(n)
        })
        .collect()
}

type Closing<'s> = std::collections::VecDeque<std::thread::ScopedJoinHandle<'s, Result<()>>>;

/// `out` closed on a thread of its own, once fewer than `CLOSING` are.
fn close<'a, 's>(closing: &mut Closing<'s>, scope: &'s std::thread::Scope<'s, 'a>, out: Output) -> Result<()> {
    if closing.len() >= CLOSING {
        closing.pop_front().map_or(Ok(()), join)?;
    }
    closing.push_back(scope.spawn(move || out.close()));
    Ok(())
}

fn join(h: std::thread::ScopedJoinHandle<'_, Result<()>>) -> Result<()> {
    h.join().unwrap_or_else(|p| std::panic::resume_unwind(p))
}

/// What takes a result's batches as they come (`query_each`), with its columns.
pub type Each<'a> = dyn FnMut(&[String], Batch) -> Result<(), String> + 'a;

/// A query's source that ends it once `.1` is set.
struct Stoppable<'a>(query::Input<'a>, &'a AtomicBool);

impl brrrrr_core::engine::Source for Stoppable<'_> {
    fn next(&mut self) -> Option<Result<brrrrr_core::column::Batch, String>> {
        if self.1.load(std::sync::atomic::Ordering::Relaxed) {
            return Some(Err("the query was stopped".into()));
        }
        self.0.next()
    }
}

/// Files read with a reader's options.
fn with_options(mut files: Vec<File>, options: &Arc<read::Options>) -> Vec<File> {
    files.iter_mut().for_each(|f| f.options = options.clone());
    files
}

/// What compiling a query found of its tables: their files (listed), and the rows of the
/// subqueries that stand as tables.
#[derive(Clone, Default)]
struct Found {
    files: HashMap<Table, Vec<File>>,
    rows: HashMap<String, Vec<arrow_array::RecordBatch>>,
    /// Each table's first file alone is listed (`compile_listed`'s first compile).
    probe: bool,
    /// Each table's first file, of all of its partitions.
    first: HashMap<Table, File>,
}

/// The query with the subqueries and CTEs its joins look up as tables of their own (`__sub0`,
/// `__sub1`, ...), and each one's query: run first, its rows are looked up as a table's are
/// (the engine looks up a table's rows, all read before the other tables', never a subquery's,
/// which it computes as it goes). With `asof`, the right sides of ASOF JOINs too: their rows are
/// then ordered by time as a table's are. Each query has the CTEs it reads (a view is a CTE),
/// and the CTEs nothing reads any more are left out, so that none is computed for nothing.
fn lookups(sql: &str, asof: bool) -> Result<(String, Vec<(String, String)>)> {
    use sqlparser::keywords::Keyword;
    use sqlparser::tokenizer::{Token, Tokenizer};
    let toks = Tokenizer::new(&sqlparser::dialect::GenericDialect {}, sql).tokenize().map_err(|e| anyhow!("{e}"))?;
    let sig: Vec<usize> = (0..toks.len()).filter(|&i| !matches!(toks[i], Token::Whitespace(_))).collect();
    let tok = |n: usize| sig.get(n).map(|&i| &toks[i]);
    let word = |n: usize, w: &str| matches!(tok(n), Some(Token::Word(x)) if x.quote_style.is_none() && x.value.eq_ignore_ascii_case(w));
    // the closing parenthesis of the one at `n`
    let close = |n: usize| -> Option<usize> {
        let mut depth = 0;
        for m in n..sig.len() {
            match tok(m)? {
                Token::LParen => depth += 1,
                Token::RParen if depth == 1 => return Some(m),
                Token::RParen => depth -= 1,
                _ if depth == 0 => return None,
                _ => {}
            }
        }
        None
    };
    let text = |out: &[String], a: usize, b: usize| out[sig[a]..=sig[b]].concat();
    // the outermost WITH's CTEs: name, where it starts, and its query's parentheses
    let ctes: Vec<(String, usize, usize, usize)> = (|| {
        let mut ctes = vec![];
        if !word(0, "WITH") || word(1, "RECURSIVE") {
            return Some(ctes);
        }
        let mut n = 1;
        loop {
            let Some(Token::Word(name)) = tok(n) else { return None };
            let mut m = n + 1;
            if let Some(Token::LParen) = tok(m) {
                m = close(m)? + 1;
            }
            let end = word(m, "AS").then(|| close(m + 1))??;
            ctes.push((name.value.clone(), n, m + 1, end));
            if !matches!(tok(end + 1), Some(Token::Comma)) {
                return Some(ctes);
            }
            n = end + 2;
        }
    })()
    .unwrap_or_default();
    let body = ctes.last().map_or(0, |c| c.3 + 1);
    // the CTEs the tokens `a..=b` read (FROM or JOIN them), and those these read
    let reads = |out: &[String], direct: &[(usize, usize)]| -> Vec<bool> {
        let mut need = vec![false; ctes.len()];
        let mark = |need: &mut Vec<bool>, a: usize, b: usize| {
            for n in a.max(1)..=b.min(sig.len() - 1) {
                if (word(n - 1, "FROM") || word(n - 1, "JOIN"))
                    && out[sig[n]] == brrrrr_core::sql::token_sql(&toks[sig[n]])
                {
                    if let Some(k) = ctes.iter().position(|c| matches!(tok(n), Some(Token::Word(w)) if w.value == c.0))
                    {
                        need[k] = true;
                    }
                }
            }
        };
        for &(a, b) in direct {
            mark(&mut need, a, b);
        }
        for k in (0..ctes.len()).rev() {
            if need[k] {
                mark(&mut need, ctes[k].2, ctes[k].3);
            }
        }
        need
    };
    let original: Vec<String> = toks.iter().map(brrrrr_core::sql::token_sql).collect();
    // a query of tokens `a..=b`, with the CTEs it reads
    let standalone = |a: usize, b: usize| -> String {
        let defs: Vec<String> = reads(&original, &[(a, b)])
            .iter()
            .zip(&ctes)
            .filter(|(n, _)| **n)
            .map(|(_, c)| text(&original, c.1, c.3))
            .collect();
        match (defs.is_empty(), word(a, "WITH")) {
            (true, _) => text(&original, a, b),
            (false, true) => format!("WITH {}, {}", defs.join(", "), text(&original, a + 1, b)),
            (false, false) => format!("WITH {} {}", defs.join(", "), text(&original, a, b)),
        }
    };
    let mut out = original.clone();
    let mut subs: Vec<(String, String)> = vec![];
    let mut made: HashMap<usize, String> = HashMap::new();
    let mut inside = 0;
    for n in 1..sig.len() {
        if n < inside || !word(n, "JOIN") {
            continue;
        }
        let before = |w: &str| word(n - 1, w) || n >= 2 && word(n - 2, w);
        if before("RIGHT") || before("FULL") || before("CROSS") || before("ASOF") && !asof {
            continue;
        }
        let name = format!("__sub{}", subs.len());
        match tok(n + 1) {
            // JOIN (SELECT ...): its query; the table in its place
            Some(Token::LParen) if word(n + 2, "SELECT") || word(n + 2, "WITH") || word(n + 2, "FROM") => {
                let Some(end) = close(n + 1) else { continue };
                subs.push((name.clone(), standalone(n + 2, end - 1)));
                out[sig[n + 1]..=sig[end]].iter_mut().for_each(String::clear);
                out[sig[n + 1]] = name;
                inside = end + 1;
            }
            // JOIN cte: the CTE's query; the table in its place, and the CTE a reading of it
            Some(Token::Word(w)) if w.quote_style.is_none() => {
                let Some(k) = ctes.iter().position(|c| c.0 == w.value) else { continue };
                let table = match made.get(&k) {
                    Some(t) => t.clone(),
                    None => {
                        let (_, _, open, end) = ctes[k];
                        let mut need = reads(&original, &[(open, end)]);
                        need[k] = true;
                        let defs: Vec<String> =
                            (0..=k).filter(|&j| need[j]).map(|j| text(&original, ctes[j].1, ctes[j].3)).collect();
                        subs.push((name.clone(), format!("WITH {} SELECT * FROM {}", defs.join(", "), w.value)));
                        out[sig[open] + 1..sig[end]].iter_mut().for_each(String::clear);
                        out[sig[open] + 1] = format!("SELECT * FROM {name}");
                        made.insert(k, name.clone());
                        name
                    }
                };
                let aliased =
                    word(n + 2, "AS") || matches!(tok(n + 2), Some(Token::Word(a)) if a.keyword == Keyword::NoKeyword);
                out[sig[n + 1]] = if aliased { table } else { format!("{table} AS {}", w.value) };
            }
            _ => {}
        }
    }
    if ctes.is_empty() || subs.is_empty() {
        return Ok((out.concat(), subs));
    }
    // the CTEs the query still reads
    let need = reads(&out, &[(body, sig.len() - 1)]);
    let defs: Vec<String> = need.iter().zip(&ctes).filter(|(n, _)| **n).map(|(_, c)| text(&out, c.1, c.3)).collect();
    let rest = out[sig[body]..].concat();
    let sql = if defs.is_empty() { rest } else { format!("WITH {} {rest}", defs.join(", ")) };
    Ok((sql, subs))
}

/// How a view reads a stream as its first relation, once: its WHERE, the stream's column each
/// name in it is, and, when it passes the rows on as they are (no window, no window function),
/// the stream it writes and its columns that copy the stream's (theirs, the stream's).
struct Reading<'a> {
    filter: Option<&'a sqlparser::ast::Expr>,
    column: Names<'a>,
    passes: Option<(&'a str, Vec<(String, String)>)>,
}

/// The column of a stream an expression of a view reading it names, if it names one.
type Names<'a> = Box<dyn Fn(&sqlparser::ast::Expr) -> Option<String> + 'a>;

/// The views reading `stream`: each one's `Reading`, or `None` for one reading it otherwise
/// (as a join's right side, twice, ...).
fn readings<'a>(c: &'a query::Compiled, stream: &'a str) -> Vec<Option<Reading<'a>>> {
    use sqlparser::ast::{visit_expressions, visit_relations, Expr, GroupByExpr, SelectItem, SetExpr, TableFactor};
    use std::ops::ControlFlow;
    let reads = |v: &brrrrr_core::sql::View| {
        let mut n = 0;
        let _ = visit_relations(&v.query, |r| {
            n += usize::from(r.to_string() == stream);
            ControlFlow::<()>::Continue(())
        });
        n
    };
    let reading = |v: &'a brrrrr_core::sql::View| -> Option<Reading<'a>> {
        let SetExpr::Select(s) = v.query.body.as_ref() else { return None };
        let [from] = s.from.as_slice() else { return None };
        let TableFactor::Table { name, alias, .. } = &from.relation else { return None };
        if name.to_string() != stream || reads(v) != 1 {
            return None;
        }
        // a column of the stream: `t.col`, or `col` when no joined relation has one so named
        let first = alias.as_ref().map_or(stream, |a| a.name.value.as_str());
        let joined: Option<Vec<&str>> = from
            .joins
            .iter()
            .map(|j| match &j.relation {
                TableFactor::Table { name, .. } => c.catalog.streams.get(&name.to_string()),
                _ => None,
            })
            .try_fold(vec![], |mut all, st| {
                all.extend(st?.columns.iter().map(|c| c.name.as_str()));
                Some(all)
            });
        let column = move |e: &Expr| -> Option<String> {
            match e {
                Expr::Identifier(i) if joined.as_ref().is_some_and(|j| !j.contains(&i.value.as_str())) => {
                    Some(i.value.clone())
                }
                Expr::CompoundIdentifier(ids) if ids.len() == 2 && ids[0].value == first => Some(ids[1].value.clone()),
                _ => None,
            }
        };
        let mut over = false;
        let _ = visit_expressions(&v.query, |x| {
            over |= matches!(x, Expr::Function(f) if f.over.is_some());
            ControlFlow::<()>::Continue(())
        });
        let grouped = !matches!(&s.group_by, GroupByExpr::Expressions(k, _) if k.is_empty());
        let rows = !(v.emit_delay_us.is_some() || over || grouped || s.having.is_some() || s.distinct.is_some());
        let passes = rows.then(|| {
            let copies = s
                .projection
                .iter()
                .filter_map(|item| {
                    let (e, name) = match item {
                        SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
                        SelectItem::UnnamedExpr(e @ Expr::Identifier(i)) => (e, i.value.clone()),
                        SelectItem::UnnamedExpr(e @ Expr::CompoundIdentifier(ids)) => (e, ids.last()?.value.clone()),
                        _ => return None,
                    };
                    Some((name, column(e)?))
                })
                .collect();
            (v.target.as_str(), copies)
        });
        Some(Reading { filter: s.selection.as_ref(), column: Box::new(column), passes })
    };
    c.catalog.views.iter().filter(|v| reads(v) > 0).map(reading).collect()
}

/// Whether rows of `stream` whose columns hold the `known` values (a file's partitions) may
/// reach the result: false only when every view reading the stream drops them by its WHERE, or
/// passes them on, row for row, to a stream where they are dropped. The files of partitions no
/// row of reaches it are not read. What it cannot tell keeps the rows.
fn reaches(c: &query::Compiled, stream: &str, known: &HashMap<String, Value>, depth: usize) -> bool {
    if known.is_empty() || stream == c.result || depth > 64 {
        return true;
    }
    let readings = readings(c, stream);
    readings.is_empty()
        || readings.into_iter().any(|r| {
            let Some(r) = r else { return true };
            let value = |e: &sqlparser::ast::Expr| (r.column)(e).and_then(|c| known.get(&c));
            if r.filter.and_then(|w| holds(w, &value)) == Some(false) {
                return false;
            }
            let Some((target, copies)) = r.passes else { return true };
            let next = copies.iter().filter_map(|(t, s)| Some((t.clone(), known.get(s)?.clone()))).collect();
            reaches(c, target, &next, depth + 1)
        })
}

/// The values column `key` of `stream` has in the rows that may reach the result, when the
/// query's conditions list them (`key = 'a'`, `key IN ('a', 'b')`): `None` when any may.
fn values(c: &query::Compiled, stream: &str, key: &str, depth: usize) -> Option<BTreeSet<String>> {
    if stream == c.result || depth > 64 {
        return None;
    }
    let readings = readings(c, stream);
    if readings.is_empty() {
        return None;
    }
    let mut all = BTreeSet::new();
    for r in readings {
        let r = r?;
        let own = r.filter.and_then(|w| listed(w, &|e| (r.column)(e).as_deref() == Some(key)));
        let after = r.passes.and_then(|(target, copies)| {
            let (theirs, _) = copies.iter().find(|(_, s)| s == key)?;
            values(c, target, theirs, depth + 1)
        });
        all.extend(match (own, after) {
            (Some(a), Some(b)) => a.intersection(&b).cloned().collect(),
            (a, b) => a.or(b)?,
        });
    }
    Some(all)
}

/// The text values `e` holds for alone, of the column `is` tells: `col = 'a'`, `col IN ('a',
/// 'b')`, and AND and OR of them (with anything else under an AND).
fn listed(e: &sqlparser::ast::Expr, is: &dyn Fn(&sqlparser::ast::Expr) -> bool) -> Option<BTreeSet<String>> {
    use sqlparser::ast::{BinaryOperator as B, Expr, Value as Lit};
    let text = |e: &Expr| match e {
        Expr::Value(v) => match &v.value {
            Lit::SingleQuotedString(t) => Some(t.clone()),
            _ => None,
        },
        _ => None,
    };
    match e {
        Expr::Nested(x) => listed(x, is),
        Expr::BinaryOp { left, op: B::And, right } => match (listed(left, is), listed(right, is)) {
            (Some(a), Some(b)) => Some(a.intersection(&b).cloned().collect()),
            (a, b) => a.or(b),
        },
        Expr::BinaryOp { left, op: B::Or, right } => {
            Some(listed(left, is)?.into_iter().chain(listed(right, is)?).collect())
        }
        Expr::BinaryOp { left, op: B::Eq, right } if is(left) => Some([text(right)?].into()),
        Expr::BinaryOp { left, op: B::Eq, right } if is(right) => Some([text(left)?].into()),
        Expr::InList { expr, list, negated: false } if is(expr) => list.iter().map(text).collect(),
        _ => None,
    }
}

/// Whether `e` holds for rows whose columns `value` knows: `None` when it may or may not.
/// It knows comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`) of a known column
/// with a literal, compared as the engine compares them, and AND, OR and NOT of them.
fn holds<'a>(e: &sqlparser::ast::Expr, value: &dyn Fn(&sqlparser::ast::Expr) -> Option<&'a Value>) -> Option<bool> {
    use sqlparser::ast::{BinaryOperator as B, Expr, UnaryOperator, Value as Lit};
    use std::cmp::Ordering::{self, *};
    // a known column against a literal
    let order = |col: &Expr, lit: &Expr| -> Option<Ordering> {
        let Expr::Value(l) = lit else { return None };
        let lit = match &l.value {
            Lit::SingleQuotedString(t) => Value::Str(t.as_str().into()),
            Lit::Number(n, _) => n.parse().map(Value::Int).or_else(|_| n.parse().map(Value::F64)).ok()?,
            _ => return None,
        };
        brrrrr_core::expr::compare(value(col)?, &lit)
    };
    match e {
        Expr::Nested(x) => holds(x, value),
        Expr::UnaryOp { op: UnaryOperator::Not, expr } => holds(expr, value).map(|b| !b),
        Expr::BinaryOp { left, op: B::And, right } => match (holds(left, value), holds(right, value)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        Expr::BinaryOp { left, op: B::Or, right } => match (holds(left, value), holds(right, value)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        Expr::BinaryOp { left, op, right } => {
            let (o, flipped) = match order(left, right) {
                Some(o) => (o, false),
                None => (order(right, left)?, true),
            };
            let o = if flipped { o.reverse() } else { o };
            Some(match op {
                B::Eq => o == Equal,
                B::NotEq => o != Equal,
                B::Lt => o == Less,
                B::LtEq => o != Greater,
                B::Gt => o == Greater,
                B::GtEq => o != Less,
                _ => return None,
            })
        }
        Expr::InList { expr, list, negated } => {
            let mut any = false;
            for x in list {
                any |= order(expr, x)? == Equal;
            }
            Some(any != *negated)
        }
        Expr::Between { expr, negated, low, high } => {
            Some((order(expr, low)? != Less && order(expr, high)? != Greater) != *negated)
        }
        _ => None,
    }
}

/// `s` split at `sep` outside parentheses.
fn split_top(s: &str, sep: char) -> Vec<&str> {
    let (mut out, mut depth, mut from) = (vec![], 0, 0);
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c == sep && depth == 0 => {
                out.push(&s[from..i]);
                from = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[from..]);
    out.retain(|p| !p.trim().is_empty());
    out
}

/// `now()` and `current_timestamp` as the time now, a literal.
fn with_now(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("now()") && !lower.contains("current_timestamp") {
        return sql.to_string();
    }
    let us = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as i64);
    let lit = format!("TIMESTAMP '{}'", brrrrr_core::query::text(&Value::Time(us)));
    let mut out = String::new();
    let mut rest = sql;
    loop {
        let l = rest.to_ascii_lowercase();
        let hit =
            [("now()", 5), ("current_timestamp", 17)].into_iter().filter_map(|(p, n)| l.find(p).map(|i| (i, n))).min();
        match hit {
            Some((i, n)) => {
                out.push_str(&rest[..i]);
                out.push_str(&lit);
                rest = &rest[i + n..];
            }
            None => break,
        }
    }
    out.push_str(rest);
    out
}

/// An error as one line a person reads: each cause once (object stores repeat their causes in
/// their messages), and what to check for the common ones.
pub fn message(e: &anyhow::Error) -> String {
    let mut out = String::new();
    for cause in e.chain() {
        let t = cause.to_string();
        let t = t.trim().trim_end_matches(':').trim();
        if t.is_empty() || out.contains(t) {
            continue;
        }
        if !out.is_empty() {
            out.push_str(": ");
        }
        out.push_str(t);
    }
    // a store's error says it once, then again from its source: keep the first time
    if let Some(i) = out.find(": Error performing") {
        if let Some(j) = out[i + 2..].find(": Error performing") {
            out.truncate(i + 2 + j);
        }
    }
    let hint = if out.contains("hint:") {
        None
    } else if out.contains("403") || out.contains("lacked the necessary privileges") || out.contains("Unauthorized") {
        Some("check the store's credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, GOOGLE_SERVICE_ACCOUNT, AZURE_STORAGE_ACCOUNT_KEY)")
    } else if out.contains("Connection refused") || out.contains("dns error") || out.contains("connect error") {
        Some("the store is not reachable: check its endpoint (AWS_ENDPOINT_URL for S3-compatible stores) and the network")
    } else if out.contains("NoSuchBucket") || out.contains("ContainerNotFound") || out.contains("bucket does not exist")
    {
        Some("no such bucket or container")
    } else {
        None
    };
    match hint {
        Some(h) => format!("{out}\n  hint: {h}"),
        None => out,
    }
}

/// A type as `DESCRIBE` shows it.
pub fn type_name(t: &Type) -> &'static str {
    match t.base() {
        Type::Bool => "boolean",
        Type::Int(_) => "bigint",
        Type::UInt(_) => "ubigint",
        Type::F32 | Type::F64 => "double",
        Type::Str => "varchar",
        Type::Time(_) => "timestamp",
        Type::Array(_) => "array",
        Type::Map(..) => "map",
        _ => "any",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_by_their_first_word() {
        assert_eq!(first_word("  -- hi\n /* x */ select 1"), "SELECT");
        assert_eq!(after_first_word("EXPLAIN SELECT 1"), "SELECT 1");
        let mut l = Lake::new();
        l.views.insert("v".into(), "SELECT * FROM 'a.csv'".into());
        assert_eq!(l.expand("FROM v"), "WITH v AS (SELECT * FROM 'a.csv') SELECT * FROM v");
        assert_eq!(
            l.expand("WITH w AS (SELECT 1 FROM v) SELECT * FROM w"),
            "WITH v AS (SELECT * FROM 'a.csv'), w AS (SELECT 1 FROM v) SELECT * FROM w"
        );
        assert!(l.execute("VACUUM").is_err());
    }

    #[test]
    fn the_values_a_query_fixes_a_partition_to() {
        let cols: Vec<(String, Type)> = [("ts", Type::Time(6)), ("price", Type::Int(64)), ("date", Type::Str)]
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect();
        let dates = |sql: &str| -> Option<Vec<String>> {
            let c = query::compile(sql, &mut |_| Ok(cols.clone())).unwrap_or_else(|e| panic!("{sql}: {e}"));
            values(&c, &c.sources[0].stream, "date", 0).map(|v| v.into_iter().collect())
        };
        let some = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert_eq!(dates("SELECT * FROM t WHERE date = 'a' AND price > 1"), some(&["a"]));
        assert_eq!(dates("SELECT count(*) FROM t WHERE 'b' = date OR date IN ('a', 'c')"), some(&["a", "b", "c"]));
        assert_eq!(dates("SELECT * FROM t WHERE date IN ('a', 'b') AND date = 'b'"), some(&["b"]));
        assert_eq!(dates("SELECT * FROM (SELECT date AS d, price FROM t) WHERE d = 'a'"), some(&["a"]));
        assert_eq!(
            dates("SELECT * FROM t WHERE date = 'a' UNION ALL SELECT * FROM t WHERE date = 'b'"),
            some(&["a", "b"])
        );
        for sql in [
            "SELECT * FROM t",
            "SELECT * FROM t WHERE date >= 'a'",
            "SELECT * FROM t WHERE date = 'a' OR price > 1",
            "SELECT * FROM t WHERE date NOT IN ('a')",
            "SELECT * FROM t WHERE date = 'a' UNION ALL SELECT * FROM t",
            "SELECT * FROM (SELECT date, count(*) AS n FROM t GROUP BY date) WHERE date = 'a'",
        ] {
            assert_eq!(dates(sql), None, "{sql}");
        }
    }

    #[test]
    fn the_subqueries_joins_look_up_are_tables() {
        let l = |sql: &str, asof: bool| lookups(sql, asof).unwrap();
        let sub = |n: &str, q: &str| (n.to_string(), q.to_string());
        assert_eq!(
            l("SELECT * FROM 't.csv' t JOIN (SELECT k, avg(x) AS a FROM 't.csv' GROUP BY k) s ON t.k = s.k", false),
            (
                "SELECT * FROM 't.csv' t JOIN __sub0 s ON t.k = s.k".into(),
                vec![sub("__sub0", "SELECT k, avg(x) AS a FROM 't.csv' GROUP BY k")]
            )
        );
        // a CTE, with the CTEs it reads; those nothing reads any more left out
        assert_eq!(
            l("WITH a AS (FROM 'x'), b AS (FROM a), c AS (FROM 'y') SELECT * FROM t JOIN b ON t.k = b.k JOIN c ON t.k = c.j", false),
            (
                "SELECT * FROM t JOIN __sub0 AS b ON t.k = b.k JOIN __sub1 AS c ON t.k = c.j".into(),
                vec![sub("__sub0", "WITH a AS (FROM 'x'), b AS (FROM a) SELECT * FROM b"), sub("__sub1", "WITH c AS (FROM 'y') SELECT * FROM c")]
            )
        );
        assert_eq!(
            l("WITH a AS (FROM 'x'), b AS (FROM 'y') SELECT * FROM a JOIN (FROM b) s ON a.k = s.k", false),
            (
                "WITH a AS (FROM 'x') SELECT * FROM a JOIN __sub0 s ON a.k = s.k".into(),
                vec![sub("__sub0", "WITH b AS (FROM 'y') FROM b")]
            )
        );
        // a CTE read by FROM too is a reading of the table, made once
        assert_eq!(
            l("WITH a AS (FROM 'x') SELECT * FROM a JOIN a AS b ON a.k = b.k", false).0,
            "WITH a AS (SELECT * FROM __sub0) SELECT * FROM a JOIN __sub0 AS b ON a.k = b.k"
        );
        // a subquery's own joins are its run's
        let nested = "SELECT * FROM t JOIN (SELECT * FROM u JOIN (FROM v) w ON u.k = w.k) x ON t.k = x.k";
        assert_eq!(l(nested, false).1, [sub("__sub0", "SELECT * FROM u JOIN (FROM v) w ON u.k = w.k")]);
        // ASOF JOINs only when asked; a table is no subquery
        let asof = "SELECT * FROM t ASOF LEFT JOIN (FROM q WHERE b > 0) q ON t.k = q.k AND t.ts >= q.ts";
        assert_eq!(l(asof, false), (asof.to_string(), vec![]));
        assert_eq!(l(asof, true).1, [sub("__sub0", "FROM q WHERE b > 0")]);
        let plain = "WITH a AS (FROM 'x') SELECT * FROM a JOIN 'y.csv' y ON a.k = y.k JOIN u ON a.k = u.k";
        assert_eq!(l(plain, true), (plain.to_string(), vec![]));
    }

    #[test]
    fn the_partitions_a_query_drops() {
        let cols: Vec<(String, Type)> =
            [("ts", Type::Time(6)), ("price", Type::Int(64)), ("date", Type::Str), ("hour", Type::Int(64))]
                .into_iter()
                .map(|(n, t)| (n.to_string(), t))
                .collect();
        // per source, whether rows of date=2024-01-02/hour=9 may reach the result
        let keeps = |sql: &str| -> Vec<bool> {
            let c = query::compile(sql, &mut |_| Ok(cols.clone())).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let known = HashMap::from([
                ("date".to_string(), Value::Str("2024-01-02".into())),
                ("hour".to_string(), Value::Int(9)),
            ]);
            c.sources.iter().map(|s| reaches(&c, &s.stream, &known, 0)).collect()
        };
        let kept = |w: &str| keeps(&format!("SELECT price FROM t WHERE {w}")) == [true];
        for w in [
            "date = '2024-01-02'",
            "date IN ('2024-01-01', '2024-01-02')",
            "date BETWEEN '2024-01-01' AND '2024-01-02'",
            "date >= '2024-01-02' AND date < '2024-01-03'",
            "'2024-01-01' < date",
            "hour < 10", // as numbers: 9 < 10 (as text '9' > '10')
            "hour BETWEEN 8.5 AND 9",
            "hour IN (9, 10)",
            "NOT hour <> 9",
            "date = '2024-01-01' OR price > 100", // price may be
            "hour = '9'",                         // not compared so: not used
            "upper(date) = 'X'",
            "price > 100",
        ] {
            assert!(kept(w), "{w}");
        }
        for w in [
            "date = '2024-01-01'",
            "date IN ('2024-01-01', '2024-01-03')",
            "date NOT IN ('2024-01-02')",
            "date BETWEEN '2024-01-03' AND '2024-01-31'",
            "date > '2024-01-02'",
            "'2024-01-02' > date",
            "hour >= 10",
            "hour = 9.5",
            "hour NOT BETWEEN 0 AND 23",
            "price > 100 AND (date < '2024-01-02' OR hour > 9)",
            "NOT (hour = 9)",
        ] {
            assert!(!kept(w), "{w}");
        }
        // the condition of a query over the table: aggregated, through subqueries, CTEs and views
        assert_eq!(keeps("SELECT count(*) FROM t WHERE date = '2024-01-03'"), [false]);
        assert_eq!(keeps("SELECT * FROM (SELECT price, date AS d FROM t) WHERE d = '2024-01-03'"), [false]);
        assert_eq!(keeps("WITH x AS (SELECT * FROM t WHERE price > 1) SELECT count(*) FROM x WHERE hour > 9"), [false]);
        let mut l = Lake::new();
        l.views.insert("v".into(), "SELECT ts, price, date FROM t".into());
        assert_eq!(keeps(&l.expand("SELECT * FROM v WHERE date = '2024-01-03'")), [false]);
        // ... but not through what reads other rows than its own: a window, a window function
        assert_eq!(
            keeps("SELECT * FROM (SELECT date, count(*) AS n FROM t GROUP BY date) WHERE date = '2024-01-03'"),
            [true]
        );
        assert_eq!(
            keeps("SELECT * FROM (SELECT date, sum(price) OVER (ORDER BY ts) AS s FROM t) WHERE date = '2024-01-03'"),
            [true]
        );
        // a window function after the WHERE, as SQL has it, is the WHERE's rows'
        assert_eq!(keeps("SELECT sum(price) OVER (ORDER BY ts) FROM t WHERE date = '2024-01-03'"), [false]);
        // a join: each table by its own conditions; the looked-up (right) one by none, as which
        // of its rows a lookup finds depends on its other rows
        assert_eq!(keeps("SELECT * FROM t JOIN u ON t.price = u.price WHERE t.date = '2024-01-03'"), [false, true]);
        assert_eq!(keeps("SELECT * FROM t JOIN u ON t.price = u.price WHERE u.date = '2024-01-03'"), [true, true]);
        assert_eq!(
            keeps(
                "SELECT * FROM t ASOF JOIN (SELECT * FROM u WHERE hour = 10) AS w ON t.price = w.price AND t.ts >= w.ts \
                 WHERE t.hour = 9"
            ),
            [true, false]
        );
        // unqualified, a column both tables have is neither's
        assert_eq!(keeps("SELECT * FROM t JOIN u ON t.price = u.price WHERE date = '2024-01-03'"), [true, true]);
        // a table read twice: its rows kept for either
        assert_eq!(keeps("SELECT * FROM t AS a JOIN t AS b ON a.price = b.price WHERE a.date = '2024-01-03'"), [true]);
        assert_eq!(
            keeps("SELECT price FROM t WHERE date = '2024-01-03' UNION ALL SELECT price FROM t WHERE hour = 9"),
            [true]
        );
        assert_eq!(
            keeps("SELECT price FROM t WHERE date = '2024-01-03' UNION ALL SELECT price FROM t WHERE hour = 8"),
            [false]
        );
    }
}
