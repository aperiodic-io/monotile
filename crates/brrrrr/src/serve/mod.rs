//! `brrrrr serve`: one process holding today's data in memory and its history in Parquet, queried
//! as one table (kdb+'s real-time and historical databases, and the gateway that joins them).
//!
//! - Live tables live in the data directory (`brrrrr_lake::live`): written over HTTP (JSON lines
//!   or CSV) or from Kafka topics, flushed to Parquet by day every `--flush` interval.
//! - Live views (`CREATE LIVE VIEW bars AS SELECT ...`) are the query's pipeline (ADR-0018) run
//!   by the streaming engine as rows arrive; what they emit is a live table of its own.
//! - Clients: the HTTP API and console (`http`), the PostgreSQL wire protocol (`pg`), and
//!   subscriptions to a table's new rows or to a query's answer, as server-sent events.
use anyhow::{anyhow, bail, Context, Result};
use brrrrr_core::engine::{Emit, Engine, Output, Row};
use brrrrr_core::query::{Clock, Compiled, FIRST};
use brrrrr_core::value::Value;
use brrrrr_lake::live::Live;
use brrrrr_lake::{read, write, Answer, Lake};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

pub mod http;
pub mod pg;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Where live tables keep their Parquet history and write-ahead logs.
    #[arg(long, default_value = "brrrrr-data", env = "BRRRRR_DATA")]
    pub data: PathBuf,
    /// The HTTP API and console's address.
    #[arg(long, default_value = "127.0.0.1:4242", env = "BRRRRR_HTTP")]
    pub http: String,
    /// The PostgreSQL wire protocol's address (psql, Grafana, drivers); `off` for none.
    #[arg(long, default_value = "127.0.0.1:5433", env = "BRRRRR_PG")]
    pub pg: String,
    /// A token clients must give: `Authorization: Bearer <token>` over HTTP, the password over
    /// PostgreSQL. Without one, anyone who reaches the ports may read and write.
    #[arg(long, env = "BRRRRR_TOKEN")]
    pub token: Option<String>,
    /// How often rows held in memory are flushed to Parquet.
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    pub flush: Duration,
    /// fsync each write to the log before answering it (slower; nothing written is lost even if
    /// the machine fails).
    #[arg(long)]
    pub sync: bool,
    /// Names a location as a table: `trades=s3://bucket/trades/` (history kept elsewhere).
    #[arg(short, long = "table", value_name = "NAME=LOCATION")]
    pub tables: Vec<String>,
    /// Kafka brokers to ingest from (`--ingest`).
    #[arg(long, env = "BRRRRR_KAFKA")]
    pub kafka: Option<String>,
    /// A Kafka topic written into a live table, one JSON object per message: `trades=market.trades`.
    #[arg(long = "ingest", value_name = "TABLE=TOPIC")]
    pub ingest: Vec<String>,
    /// Threads per query; by default as many as there are CPUs.
    #[arg(long, value_name = "N")]
    pub threads: Option<usize>,
    /// How long a client's statement may take, waiting for its turn included; `0s` for no limit.
    /// A PostgreSQL session's `SET statement_timeout` lowers it.
    #[arg(long, default_value = "5m", value_parser = parse_duration)]
    pub query_timeout: Duration,
    /// How many clients' statements run at once: the others wait their turn (up to 16 for each
    /// that runs; more are refused), so that heavy queries leave CPUs to writes and live views.
    #[arg(long, value_name = "N", default_value_t = 4, value_parser = clap::value_parser!(u32).range(1..))]
    pub max_queries: u32,
}

/// A client's statement's stop: a cancel (a client gone, PostgreSQL's CancelRequest) or its time
/// limit. A query's sources see it (`Lake::query_until`) and end it.
#[derive(Default)]
pub struct Stop {
    flag: std::sync::atomic::AtomicBool,
    why: Mutex<Option<String>>,
    /// A shorter time limit than the server's (`SET statement_timeout`).
    pub limit: Option<Duration>,
}

impl Stop {
    pub fn with_limit(limit: Option<Duration>) -> Arc<Stop> {
        Arc::new(Stop { limit, ..Stop::default() })
    }

    /// Stops the statement, saying why (the first reason given stands).
    pub fn stop(&self, why: &str) {
        self.why.lock().unwrap().get_or_insert_with(|| why.to_string());
        self.flag.store(true, Ordering::Relaxed);
    }

    fn stopped(&self) -> Option<String> {
        self.flag.load(Ordering::Relaxed).then(|| self.why.lock().unwrap().clone().unwrap_or_default())
    }
}

/// How many clients' statements run (`.0`) and wait (`.1`).
#[derive(Default)]
struct Slots {
    counts: Mutex<(u32, u32)>,
    freed: std::sync::Condvar,
}

/// A statement's turn, given back when dropped.
struct Turn<'a>(&'a Slots);

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.0.counts.lock().unwrap().0 -= 1;
        self.0.freed.notify_one();
    }
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    brrrrr_core::value::duration_us(s)
        .map(|us| Duration::from_micros(us as u64))
        .ok_or(format!("{s:?}: a duration such as 10s, 1m or 500ms"))
}

/// A live view: its pipeline, the engine running it, and where its rows go.
struct View {
    name: String,
    compiled: Compiled,
    engine: Mutex<Engine>,
    /// Per source: (table, stream, the next row number for a `__row` clock).
    sources: Vec<(String, String, AtomicU64)>,
}

/// The rows a view's sink emits.
struct Collect<'a> {
    sink: &'a str,
    rows: Vec<Row>,
}

impl Output for Collect<'_> {
    fn push(&mut self, _: Emit) {}
    fn row(&mut self, sink: &str, row: &[Value]) -> bool {
        if sink == self.sink {
            self.rows.push(row.to_vec());
        }
        true
    }
}

/// Server counters, for `/metrics`.
#[derive(Default)]
pub struct Counters {
    pub queries: AtomicU64,
    pub query_errors: AtomicU64,
    pub rows_written: AtomicU64,
    pub rows_flushed: AtomicU64,
    pub view_rows: AtomicU64,
    /// Client statements stopped: past their time limit, canceled (PostgreSQL), their client gone.
    pub timed_out: AtomicU64,
    pub canceled: AtomicU64,
    pub abandoned: AtomicU64,
    /// Client statements refused, too many already waiting their turn (`--max-queries`).
    pub busy: AtomicU64,
}

/// Why a client's statement was stopped, besides its time limit.
pub const CANCELED: &str = "canceling statement due to user request";
pub const HUNG_UP: &str = "the client hung up";

pub struct Server {
    pub lake: RwLock<Lake>,
    pub data: PathBuf,
    pub token: Option<String>,
    pub sync: bool,
    views: Mutex<Vec<Arc<View>>>,
    /// Per table, the subscribers to its new rows (each a JSON object per row).
    subscribers: Mutex<BTreeMap<String, Vec<SyncSender<Arc<String>>>>>,
    pub counters: Counters,
    pub started: Instant,
    query_timeout: Option<Duration>,
    max_queries: u32,
    slots: Slots,
    /// Writes hold it shared; creating a live view holds it alone, so that the view takes the
    /// rows before it and every row after, none twice.
    ingest: RwLock<()>,
}

impl Server {
    pub fn new(a: &Args) -> Result<Arc<Server>> {
        std::fs::create_dir_all(&a.data).with_context(|| a.data.display().to_string())?;
        let mut lake = Lake::new();
        if let Some(t) = a.threads {
            lake.threads = t.max(1);
        }
        for t in &a.tables {
            let (n, l) = t.split_once('=').ok_or(anyhow!("--table {t}: NAME=LOCATION"))?;
            lake.register(n.trim(), l.trim());
        }
        // the live tables of the data directory, their logs replayed
        for e in std::fs::read_dir(&a.data)? {
            let e = e?;
            let name = e.file_name().to_string_lossy().to_string();
            if e.path().is_dir() && !name.starts_with(['_', '.']) {
                let time = std::fs::read_to_string(e.path().join("_time")).ok().map(|t| t.trim().to_string());
                lake.register_live(Arc::new(Live::open(&name, &e.path(), time.filter(|t| !t.is_empty()), a.sync)?));
            }
        }
        let s = Arc::new(Server {
            lake: RwLock::new(lake),
            data: a.data.clone(),
            token: a.token.clone(),
            sync: a.sync,
            views: Mutex::new(vec![]),
            subscribers: Mutex::new(BTreeMap::new()),
            counters: Counters::default(),
            started: Instant::now(),
            query_timeout: Some(a.query_timeout).filter(|t| !t.is_zero()),
            max_queries: a.max_queries,
            slots: Slots::default(),
            ingest: RwLock::new(()),
        });
        // the catalog: views and named locations, as they were created
        if let Ok(text) = std::fs::read_to_string(a.data.join("_catalog.sql")) {
            for stmt in crate::shell::statements(&text) {
                if let Err(e) = s.execute_inner(&stmt, false, &Default::default()) {
                    eprintln!("warning: _catalog.sql: {stmt}: {}", brrrrr_lake::message(&e));
                }
            }
        }
        Ok(s)
    }

    /// Runs a client's statement: a query, or one that changes the catalog (kept in
    /// `_catalog.sql`), in its turn (`--max-queries`) and within its time limit. A query ends
    /// early with the reason when `stop` is set.
    pub fn execute(&self, sql: &str, stop: &Arc<Stop>) -> Result<Answer> {
        self.counters.queries.fetch_add(1, Ordering::Relaxed);
        // the time limit, its turn's wait included
        let limit = match (self.query_timeout, stop.limit) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let start = std::time::Instant::now();
        let (done, ended) = std::sync::mpsc::channel::<()>();
        let (r, counted) = std::thread::scope(|scope| {
            // the statement's watch, every 50 ms until it ends: its time limit, and a stop
            // counted when it is made (a hang-up, a cancel), not when the query next looks at
            // it, a batch later
            let watch = scope.spawn(move || loop {
                if ended.recv_timeout(Duration::from_millis(50)) != Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
                    return false;
                }
                if let Some(limit) = limit.filter(|l| start.elapsed() >= *l) {
                    stop.stop(&format!(
                        "the statement ran past its time limit of {limit:?} (--query-timeout, or SET statement_timeout)"
                    ));
                }
                if let Some(why) = stop.stopped() {
                    self.count_stop(&why);
                    return true;
                }
            });
            // a query a stop ended says why, not where it was
            let r = self.turn(stop).and_then(|_turn| {
                self.execute_inner(sql, true, &stop.flag).map_err(|e| stop.stopped().map_or(e, |why| anyhow!(why)))
            });
            drop(done);
            (r, watch.join().unwrap())
        });
        if let (true, false, Some(why)) = (r.is_err(), counted, stop.stopped()) {
            self.count_stop(&why);
        }
        if r.is_err() {
            self.counters.query_errors.fetch_add(1, Ordering::Relaxed);
        }
        r
    }

    /// A client's statement stopped, counted by why.
    fn count_stop(&self, why: &str) {
        let c = &self.counters;
        match why {
            CANCELED => &c.canceled,
            HUNG_UP => &c.abandoned,
            _ => &c.timed_out,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// A statement's turn to run: at once if fewer than `--max-queries` run, else when one ends,
    /// unless 16 for each are waiting already or it is stopped while waiting.
    fn turn(&self, stop: &Stop) -> Result<Turn<'_>> {
        let mut c = self.slots.counts.lock().unwrap();
        if c.0 >= self.max_queries {
            if c.1 >= 16 * self.max_queries {
                self.counters.busy.fetch_add(1, Ordering::Relaxed);
                bail!(
                    "the server is busy: {} statements run and {} wait their turn (--max-queries); try again later",
                    c.0,
                    c.1
                );
            }
            c.1 += 1;
            while c.0 >= self.max_queries {
                if let Some(why) = stop.stopped() {
                    c.1 -= 1;
                    bail!("{why}, waiting for its turn ({} statements run, --max-queries)", c.0);
                }
                c = self.slots.freed.wait_timeout(c, Duration::from_millis(50)).unwrap().0;
            }
            c.1 -= 1;
        }
        c.0 += 1;
        Ok(Turn(&self.slots))
    }

    /// How many client statements run and wait their turn.
    pub fn running(&self) -> (u32, u32) {
        *self.slots.counts.lock().unwrap()
    }

    fn execute_inner(&self, sql: &str, persist: bool, stop: &std::sync::atomic::AtomicBool) -> Result<Answer> {
        let sql = sql.trim().trim_end_matches(';').trim();
        let up = sql.to_ascii_uppercase();
        let words: Vec<&str> = up.split_whitespace().take(5).collect();
        let live_view = matches!(words.as_slice(), ["CREATE", "LIVE", "VIEW", ..])
            || matches!(words.as_slice(), ["CREATE", "MATERIALIZED", "VIEW", ..])
            || matches!(words.as_slice(), ["CREATE", "OR", "REPLACE", "LIVE", "VIEW", ..]);
        if live_view {
            let a = self.create_view(sql)?;
            if persist {
                self.remember(sql)?;
            }
            return Ok(a);
        }
        let changes = matches!(words.first(), Some(&("CREATE" | "DROP")));
        if !changes {
            // queries read the catalog: side by side
            let lake = self.lake.read().unwrap_or_else(|p| p.into_inner());
            if matches!(words.first(), Some(&("SELECT" | "WITH" | "FROM" | "(" | "PIVOT"))) {
                return lake.query_until(sql, stop);
            }
            drop(lake);
        }
        let a = self.lake.write().unwrap_or_else(|p| p.into_inner()).execute(sql)?;
        if persist && changes {
            self.remember(sql)?;
        }
        Ok(a)
    }

    fn remember(&self, sql: &str) -> Result<()> {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(self.data.join("_catalog.sql"))?;
        writeln!(f, "{sql};")?;
        Ok(())
    }

    /// `CREATE [OR REPLACE] LIVE VIEW name AS query`: the query's pipeline, run as rows arrive.
    fn create_view(&self, sql: &str) -> Result<Answer> {
        let up = sql.to_ascii_uppercase();
        let at = up.find(" VIEW ").ok_or(anyhow!("CREATE LIVE VIEW name AS SELECT ..."))? + 6;
        let rest = sql[at..].trim_start();
        let (name, body) =
            rest.split_once(char::is_whitespace).ok_or(anyhow!("CREATE LIVE VIEW name AS SELECT ..."))?;
        let body = body.trim_start();
        if !body.to_ascii_uppercase().starts_with("AS") {
            bail!("CREATE LIVE VIEW {name} AS SELECT ...");
        }
        let body = body[2..].trim();
        let compiled = {
            let lake = self.lake.read().unwrap_or_else(|p| p.into_inner());
            // a view's own table (a restart makes the view again over it) is no other table
            let own = self.data.join(name).join("_view").exists();
            if lake.live_tables().contains_key(name)
                && !own
                && !self.views.lock().unwrap().iter().any(|v| v.name == name)
            {
                bail!("{name} is a table already");
            }
            lake.compile(body)?
        };
        if compiled.order.is_empty() && compiled.limit.is_none() {
        } else {
            bail!("a live view emits rows as they come: ORDER BY and LIMIT belong to the queries that read it");
        }
        let lake = self.lake.read().unwrap_or_else(|p| p.into_inner());
        compiled.live().map_err(|e| anyhow!(e))?;
        for s in &compiled.sources {
            match &s.table {
                brrrrr_core::query::Table::Named(t) if lake.live_tables().contains_key(t) => {}
                t => bail!("a live view reads live tables (written to this server); {t} is not one"),
            }
        }
        drop(lake);
        let engine = Engine::new(&compiled.catalog).map_err(|e| anyhow!(compiled.humane(&e)))?;
        let sources = compiled
            .sources
            .iter()
            .map(|s| match &s.table {
                brrrrr_core::query::Table::Named(t) => (t.clone(), s.stream.clone(), AtomicU64::new(0)),
                _ => unreachable!("checked above"),
            })
            .collect();
        let v = Arc::new(View { name: name.to_string(), compiled, engine: Mutex::new(engine), sources });
        // the rows written so far first, through the same engine: a view is the same answer
        // over history and live
        let _pause = self.ingest.write().unwrap_or_else(|p| p.into_inner());
        let n = self.backfill(&v)?;
        let mut views = self.views.lock().unwrap();
        views.retain(|x| x.name != name);
        views.push(v);
        Ok(Answer {
            message: Some(format!(
                "live view {name} created over {n} rows so far: its rows go to table {name} as they come"
            )),
            ..Default::default()
        })
    }

    /// A new view's rows from its tables' rows so far, its sources merged in time order.
    fn backfill(&self, v: &View) -> Result<usize> {
        let lake = self.lake.read().unwrap_or_else(|p| p.into_inner());
        // (clock value, source, row), every source's rows in their order
        let mut all: Vec<(i64, usize, Row)> = vec![];
        for (k, (table, stream, next)) in v.sources.iter().enumerate() {
            let src = v.compiled.sources.iter().find(|s| s.stream == *stream).expect("a source");
            let cols: Vec<String> = src.columns.iter().map(|c| format!("\"{}\"", c.0)).collect();
            let a = lake.query(&format!("SELECT {} FROM {table}", cols.join(", ")))?;
            let clock = match &src.clock {
                Clock::Column(c) | Clock::Shifted(c, _) => src.columns.iter().position(|x| x.0 == *c),
                _ => None,
            };
            for (i, mut r) in a.rows.into_iter().enumerate() {
                let t = match clock {
                    Some(c) => r[c].i64().unwrap_or(i64::MIN),
                    None if src.clock == Clock::First => FIRST,
                    None => i as i64,
                };
                match src.clock {
                    Clock::Row => r.push(Value::Int(i as i64)),
                    Clock::First => r.push(Value::Int(FIRST)),
                    Clock::Shifted(_, by) => r.push(Value::Time(t.saturating_add(by))),
                    Clock::Column(_) => {}
                }
                let t = match src.clock {
                    Clock::Shifted(_, by) => t.saturating_add(by),
                    _ => t,
                };
                all.push((t, k, r));
            }
            next.store(all.iter().filter(|x| x.1 == k).count() as u64, Ordering::Relaxed);
        }
        drop(lake);
        // ponytail: every row in memory at once, and through the row engine; a table of
        // billions of rows wants the historical executor and the engine's state carried over
        all.sort_by_key(|x| x.0);
        let n = all.len();
        let mut engine = v.engine.lock().unwrap_or_else(|p| p.into_inner());
        let mut c = Collect { sink: &v.compiled.result, rows: vec![] };
        for (_, k, r) in all {
            engine.insert(&v.sources[k].1, vec![r], &mut c);
        }
        drop(engine);
        // a view made again (a restart) has emitted these rows already, up to the newest time
        // its table holds: only what comes after
        let mut rows = c.rows;
        let existing = self.lake.read().unwrap_or_else(|p| p.into_inner()).live_tables().get(&v.name).cloned();
        if let Some(t) = existing {
            let time = t.time.clone();
            let newest = match &time {
                Some(col) => {
                    let a = self
                        .lake
                        .read()
                        .unwrap_or_else(|p| p.into_inner())
                        .query(&format!("SELECT max(\"{col}\") AS t FROM {}", v.name))?;
                    a.rows.first().and_then(|r| r[0].i64())
                }
                None => None,
            };
            let at = time.as_ref().and_then(|c| v.compiled.columns.iter().position(|x| x == c));
            match (newest, at) {
                (Some(t), Some(i)) => rows.retain(|r| r[i].i64().is_some_and(|x| x > t)),
                _ => rows.clear(),
            }
        }
        if !rows.is_empty() {
            self.emit(v, rows)?;
        }
        Ok(n)
    }

    /// Rows a view emitted: to its table, and on to the views and subscribers reading it.
    fn emit(&self, v: &View, out: Vec<Row>) -> Result<()> {
        self.counters.view_rows.fetch_add(out.len() as u64, Ordering::Relaxed);
        let cols = &v.compiled.columns;
        let rows: Vec<Row> = out
            .into_iter()
            .map(|mut r| {
                r.truncate(cols.len());
                r
            })
            .collect();
        let rb = write::record_batch(cols, &rows)?;
        // its columns of the view's types: read back from JSON, a float column whose first values
        // are whole would be a column of integers, and its later fractions cut off
        let t = self.table(&v.name, || Ok(rb.clone()), None)?;
        let mark = t.dir.join("_view");
        if !mark.exists() {
            std::fs::write(mark, b"")?;
        }
        t.append(rb.clone())?;
        self.publish(&v.name, &rb)
    }

    /// Rows written to a table (created on the first write, its time column `time`, else its
    /// first column of times): to the table, to the live views reading it, to its subscribers.
    pub fn write(&self, table: &str, body: &[u8], csv: bool, time: Option<&str>) -> Result<usize> {
        valid_name(table)?;
        let _writing = self.ingest.read().unwrap_or_else(|p| p.into_inner());
        let t = self.table(table, || read::parse_rows(body, csv, None), time)?;
        let schema = t.state.read().unwrap_or_else(|p| p.into_inner()).schema.clone();
        let batch = read::parse_rows(body, csv, schema.as_ref())?;
        let n = t.append(batch.clone())?;
        self.counters.rows_written.fetch_add(n as u64, Ordering::Relaxed);
        self.publish(table, &batch)?;
        Ok(n)
    }

    /// The live table `name`, made if new, of the columns of its `first` rows.
    fn table(
        &self,
        name: &str,
        first: impl FnOnce() -> Result<arrow_array::RecordBatch>,
        time: Option<&str>,
    ) -> Result<Arc<Live>> {
        if let Some(t) = self.lake.read().unwrap_or_else(|p| p.into_inner()).live_tables().get(name) {
            return Ok(t.clone());
        }
        let mut lake = self.lake.write().unwrap_or_else(|p| p.into_inner());
        if let Some(t) = lake.live_tables().get(name) {
            return Ok(t.clone());
        }
        let first = first()?;
        let time = match time {
            Some(t) => {
                first.schema().index_of(t).map_err(|_| anyhow!("{name}: no column {t} for time"))?;
                Some(t.to_string())
            }
            None => first
                .schema()
                .fields()
                .iter()
                .find(|f| matches!(f.data_type(), arrow_schema::DataType::Timestamp(..)))
                .map(|f| f.name().clone()),
        };
        let dir = self.data.join(name);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("_time"), time.clone().unwrap_or_default())?;
        let t = Arc::new(Live::open(name, &dir, time, self.sync)?);
        // its columns are its first rows': a query before those are in answers no rows
        t.state.write().unwrap_or_else(|p| p.into_inner()).schema.get_or_insert(first.schema());
        lake.register_live(t.clone());
        Ok(t)
    }

    /// New rows of `table`: to the views reading it (and on, to theirs), then its subscribers.
    fn publish(&self, table: &str, batch: &arrow_array::RecordBatch) -> Result<()> {
        let views: Vec<Arc<View>> =
            self.views.lock().unwrap().iter().filter(|v| v.sources.iter().any(|s| s.0 == table)).cloned().collect();
        for v in views {
            let out = {
                let mut engine = v.engine.lock().unwrap_or_else(|p| p.into_inner());
                let mut c = Collect { sink: &v.compiled.result, rows: vec![] };
                for (t, stream, next) in v.sources.iter().filter(|s| s.0 == table) {
                    let src = v.compiled.sources.iter().find(|s| s.stream == *stream).expect("a source");
                    let mut rows = read::rows_of(batch, &src.columns)?;
                    match &src.clock {
                        Clock::Column(_) => {}
                        Clock::Shifted(c, by) => {
                            let c = src.columns.iter().position(|x| x.0 == *c).expect("its time column");
                            rows.iter_mut().for_each(|r| {
                                let t = r[c].i64().map_or(Value::Null, |t| Value::Time(t.saturating_add(*by)));
                                r.push(t)
                            });
                        }
                        Clock::Row => {
                            let k = next.fetch_add(rows.len() as u64, Ordering::Relaxed) as i64;
                            rows.iter_mut().enumerate().for_each(|(i, r)| r.push(Value::Int(k + i as i64)));
                        }
                        Clock::First => rows.iter_mut().for_each(|r| r.push(Value::Int(FIRST))),
                    }
                    let _ = t;
                    engine.insert(stream, rows, &mut c);
                }
                c.rows
            };
            if !out.is_empty() {
                self.emit(&v, out)?;
            }
        }
        // subscribers: a JSON object per row
        let mut subs = self.subscribers.lock().unwrap();
        if let Some(list) = subs.get_mut(table) {
            let schema = batch.schema();
            let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
            let types = read::columns_of(&schema);
            let rows = read::rows_of(batch, &types)?;
            let mut text = String::new();
            for r in &rows {
                write::json_row(&mut text, &cols, r);
                text.push('\n');
            }
            let text = Arc::new(text);
            // a subscriber that cannot keep up (a full channel) or left is dropped
            list.retain(|s| s.try_send(text.clone()).is_ok());
        }
        Ok(())
    }

    /// A subscription to a table's new rows: JSON lines as they are written.
    pub fn subscribe(&self, table: &str) -> Receiver<Arc<String>> {
        let (tx, rx) = sync_channel(1024);
        self.subscribers.lock().unwrap().entry(table.to_string()).or_default().push(tx);
        rx
    }

    /// Flushes every live table's rows held in memory to Parquet.
    pub fn flush(&self) -> Result<usize> {
        let tables: Vec<Arc<Live>> =
            self.lake.read().unwrap_or_else(|p| p.into_inner()).live_tables().values().cloned().collect();
        let mut n = 0;
        for t in tables {
            n += t.flush().with_context(|| format!("flushing {}", t.name))?;
        }
        self.counters.rows_flushed.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    /// The tables and views, for clients: name, kind, columns, rows in memory, files.
    pub fn tables(&self) -> Vec<TableInfo> {
        let lake = self.lake.read().unwrap_or_else(|p| p.into_inner());
        let views: Vec<String> = self.views.lock().unwrap().iter().map(|v| v.name.clone()).collect();
        let mut out: Vec<_> = lake
            .live_tables()
            .iter()
            .map(|(n, t)| {
                let st = t.state.read().unwrap_or_else(|p| p.into_inner());
                let cols = st
                    .schema
                    .as_ref()
                    .map(|s| {
                        read::columns_of(s)
                            .into_iter()
                            .map(|(c, ty)| (c, brrrrr_lake::type_name(&ty).to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let kind = if views.contains(n) { "live view" } else { "live" };
                TableInfo { name: n.clone(), kind, columns: cols, rows: st.rows, files: t.files().len() }
            })
            .collect();
        for v in views {
            if !out.iter().any(|t| t.name == v) {
                out.push(TableInfo { name: v, kind: "live view", columns: vec![], rows: 0, files: 0 });
            }
        }
        out
    }
}

/// A table as `/tables` lists it.
pub struct TableInfo {
    pub name: String,
    pub kind: &'static str,
    /// (name, type)
    pub columns: Vec<(String, String)>,
    /// Rows in memory (since the last flush).
    pub rows: usize,
    pub files: usize,
}

fn valid_name(t: &str) -> Result<()> {
    if t.is_empty()
        || !t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || t.starts_with('_')
        || t.starts_with(|c: char| c.is_ascii_digit())
    {
        bail!("{t:?} is not a table name: letters, digits and _, not first");
    }
    Ok(())
}

pub fn run(a: Args) -> Result<()> {
    let server = Server::new(&a)?;
    // flush on a timer
    {
        let s = server.clone();
        let every = a.flush;
        std::thread::Builder::new().name("flush".into()).spawn(move || loop {
            std::thread::sleep(every);
            if let Err(e) = s.flush() {
                eprintln!("error: {}", brrrrr_lake::message(&e));
            }
        })?;
    }
    if !a.ingest.is_empty() {
        let brokers = a.kafka.clone().ok_or(anyhow!("--ingest needs --kafka BROKERS"))?;
        kafka(server.clone(), &brokers, &a.ingest)?;
    }
    if a.pg != "off" {
        let (s, addr) = (server.clone(), a.pg.clone());
        std::thread::Builder::new().name("pg".into()).spawn(move || {
            if let Err(e) = pg::serve(&addr, s) {
                eprintln!("error: PostgreSQL protocol on {addr}: {e:#}");
            }
        })?;
    }
    eprintln!(
        "brrrrr {} serving {}: http://{} (the console, the API){}{}",
        env!("CARGO_PKG_VERSION"),
        a.data.display(),
        a.http,
        if a.pg == "off" { String::new() } else { format!(", postgresql://{}", a.pg) },
        if a.token.is_some() { ", with a token" } else { "" }
    );
    // a graceful stop: what is in memory flushed first
    let s = server.clone();
    let mut signals = signal_hook::iterator::Signals::new([signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT])?;
    std::thread::spawn(move || {
        if signals.forever().next().is_some() {
            eprintln!("stopping: flushing the rows in memory");
            let _ = s.flush();
            std::process::exit(0);
        }
    });
    http::serve(&a.http, server)
}

/// Kafka topics into live tables: one JSON object per message, written in batches.
fn kafka(server: Arc<Server>, brokers: &str, ingest: &[String]) -> Result<()> {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::Message;
    let mut topics: BTreeMap<String, String> = BTreeMap::new();
    for i in ingest {
        let (t, topic) = i.split_once('=').ok_or(anyhow!("--ingest {i}: TABLE=TOPIC"))?;
        valid_name(t)?;
        topics.insert(topic.to_string(), t.to_string());
    }
    let consumer: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "brrrrr-serve")
        // a restarted server's last session holds the topics' partitions until it times out:
        // the brokers' least (librdkafka's 45 s would pause ingest as long)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    let names: Vec<&str> = topics.keys().map(String::as_str).collect();
    consumer.subscribe(&names)?;
    std::thread::Builder::new().name("kafka".into()).spawn(move || {
        let mut pending: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut last = Instant::now();
        loop {
            if let Some(Ok(m)) = consumer.poll(Duration::from_millis(100)) {
                if let (Some(p), Some(t)) = (m.payload(), topics.get(m.topic())) {
                    let b = pending.entry(t.clone()).or_default();
                    b.extend_from_slice(p);
                    b.push(b'\n');
                }
            }
            if last.elapsed() > Duration::from_millis(200) || pending.values().any(|b| b.len() > 4 << 20) {
                let mut ok = true;
                for (t, body) in std::mem::take(&mut pending) {
                    if let Err(e) = server.write(&t, &body, false, None) {
                        eprintln!("error: ingesting into {t}: {}", brrrrr_lake::message(&e));
                        ok = false;
                    }
                }
                // at least once: offsets move past what the log holds
                if ok {
                    let _ = consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Async);
                }
                last = Instant::now();
            }
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        a: Args,
    }

    #[test]
    fn a_statement_waits_its_turn_until_stopped_and_too_many_waiting_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().display().to_string();
        let cli = Cli::try_parse_from(["serve", "--data", &data, "--max-queries", "1"]).unwrap();
        let s = Server::new(&cli.a).unwrap();
        let held = s.turn(&Stop::default()).unwrap();
        let waiting: Vec<_> = (0..16)
            .map(|_| {
                let (s, stop) = (s.clone(), Stop::with_limit(None));
                let st = stop.clone();
                (stop, std::thread::spawn(move || s.turn(&st).map(|_| ()).map_err(|e| e.to_string())))
            })
            .collect();
        while s.running() != (1, 16) {
            std::thread::sleep(Duration::from_millis(5));
        }
        let busy = s.turn(&Stop::default()).err().unwrap().to_string();
        assert!(busy.starts_with("the server is busy: 1 statements run and 16 wait"), "{busy}");
        waiting[0].0.stop(HUNG_UP);
        let mut waiting = waiting.into_iter();
        let gone = waiting.next().unwrap().1.join().unwrap().unwrap_err();
        assert_eq!(gone, "the client hung up, waiting for its turn (1 statements run, --max-queries)");
        drop(held);
        for (_, w) in waiting {
            assert!(w.join().unwrap().is_ok());
        }
        assert_eq!(s.running(), (0, 0));
    }
}
