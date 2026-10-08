//! `brrrrr historical`: a pipeline's SQL over days of Parquet files instead of a live feed
//! (ADR-0016). Each symbol is a partition: its sources' files of every day, in order, through
//! its own `Historical`; partitions run side by side, the largest first. Each column a source's
//! pipelines read is decoded on a thread of its own, ahead of the pipeline. Each
//! symbol's messages go to `<out>/<symbol>.jsonl` once it is complete, and `_SUCCESS` lists
//! them all once every symbol is.
use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, LargeStringArray, StringArray};
use brrrrr_core::column::{Batch, Col, Data, Strs};
use brrrrr_core::engine::{Emit, Historical, Mapping, Output, Pool, Source, Task};
use brrrrr_core::sql::{Catalog, Kind};
use brrrrr_core::value::{Type, Value};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DAY: i64 = 86_400_000_000;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// The pipeline's SQL file.
    #[arg(long)]
    pub sql: PathBuf,
    /// A source stream's files, `<source>=<path>` with `{symbol}` and `{day}` (YYYY-MM-DD) in the
    /// path; one per source the SQL reads. A day without a file is a day without rows.
    #[arg(long = "source", value_name = "SOURCE=PATH", required = true)]
    pub sources: Vec<String>,
    /// The first day (YYYY-MM-DD, UTC).
    #[arg(long)]
    pub from: String,
    /// The day after the last.
    #[arg(long)]
    pub to: String,
    /// The symbols (comma-separated); by default every symbol the first source has a file of
    /// on the first day.
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<String>,
    /// Where each symbol's messages go; must not exist.
    #[arg(long)]
    pub out: PathBuf,
    /// Symbols run at once; by default as many as there are CPUs.
    #[arg(long, value_name = "N")]
    pub threads: Option<usize>,
    /// Threads decoding each column a source's pipelines read, each a row group ahead; by
    /// default as many as there are threads per symbol still to run (at most 4), so that the
    /// last, largest symbols decode on the threads the others leave.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..=64))]
    pub decode_threads: Option<u64>,
    /// Take each source's `symbol` column from the file's path (`{symbol}`) rather than decode
    /// it: each file holds the rows of the symbol it is named after, as many archives are laid
    /// out (`__source_symbol`).
    #[arg(long)]
    pub symbol_from_path: bool,
    /// Take each source's rows of one capture time to be in its views' ORDER BY order already
    /// (sorted by the clock, then the other keys, as the archive's are): columns read only to
    /// check that order (a trade's id) are not read. Unchecked.
    #[arg(long)]
    pub trust_order: bool,
    /// Symbols run at once (each on the threads the others leave it), by default as many as
    /// there are threads. Fewer give each more threads to decode on, each a row group ahead,
    /// which takes more memory, not less.
    #[arg(long, value_name = "N")]
    pub symbols_at_once: Option<usize>,
    /// Rows per batch read from a file.
    #[arg(long, value_name = "N", default_value_t = 65_536)]
    pub batch_rows: usize,
    /// A source column computed from its files' columns, not read under its name: an SQL
    /// expression over the file's (Int64, Float64 and string) columns, for files not laid out as
    /// the stream is (`trades.price=to_float64(price)`, `trades.quantity=amount`).
    #[arg(long = "map", value_name = "SOURCE.COLUMN=EXPR")]
    pub maps: Vec<String>,
    /// The rows of a source's files that are its stream's: a condition over the file's columns
    /// (a file of several streams' rows, each where its value is: `markprice_source=mark_price IS
    /// NOT NULL`).
    #[arg(long = "filter", value_name = "SOURCE=CONDITION")]
    pub filters: Vec<String>,
    /// JSON lines per symbol (`<symbol>.jsonl`: topic, headers, payload), or a Parquet file per
    /// sink and symbol (`<sink>/<symbol>.parquet`) in the sink's columns and types.
    #[arg(long, value_enum, default_value_t = Format::Jsonl)]
    pub format: Format,
    /// A key and value written into every Parquet file's footer (provenance: the image, the
    /// code's version).
    #[arg(long = "parquet-metadata", value_name = "KEY=VALUE")]
    pub parquet_metadata: Vec<String>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Jsonl,
    Parquet,
}

/// Each source's columns computed from its files' (`--map`): (column, expression).
type Maps = BTreeMap<String, Vec<(String, String)>>;

/// A sink's columns (position in a row, name, type), and their values so far.
type Table = (Vec<(usize, String, Type)>, Vec<Builder>);

/// What one symbol's run read and wrote.
#[derive(Debug, Default)]
struct Done {
    symbol: String,
    rows: u64,
    messages: u64,
    seconds: f64,
    /// Waiting for decoded rows, and running the pipelines.
    read_seconds: f64,
    run_seconds: f64,
    files: usize,
    /// Each file written, under its temporary name and its own.
    outputs: Vec<(PathBuf, PathBuf)>,
}

pub fn run(a: Args) -> Result<()> {
    let text = std::fs::read_to_string(&a.sql).with_context(|| a.sql.display().to_string())?;
    let cat = brrrrr_core::sql::parse(&text).map_err(|e| anyhow!("{}:{e}", a.sql.display()))?;
    // planned once: each symbol runs a copy
    let mut plan = Historical::new(&cat).map_err(|e| anyhow!(e))?;
    plan.set_trust_order(a.trust_order);
    let mut templates = BTreeMap::new();
    for s in &a.sources {
        let (name, path) = s.split_once('=').ok_or_else(|| anyhow!("--source {s}: expected SOURCE=PATH"))?;
        if !path.contains("{symbol}") {
            bail!("--source {s}: the path has no {{symbol}}");
        }
        templates.insert(name.to_string(), path.to_string());
    }
    let needed: Vec<String> = plan.sources().iter().map(|s| s.name.clone()).collect();
    for n in &needed {
        if !templates.contains_key(n) {
            bail!("no --source for {n}, which the SQL reads");
        }
    }
    // the others are ignored, so that one set of --source fits every pipeline (a misspelt one
    // is missing above)
    templates.retain(|n, _| needed.contains(n));
    // each source's computed columns: (column, expression)
    let mut maps: Maps = BTreeMap::new();
    for m in &a.maps {
        let (col, expr) = m.split_once('=').ok_or_else(|| anyhow!("--map {m}: expected SOURCE.COLUMN=EXPR"))?;
        let (src, col) = col.split_once('.').ok_or_else(|| anyhow!("--map {m}: expected SOURCE.COLUMN=EXPR"))?;
        let st =
            plan.sources().into_iter().find(|s| s.name == src).ok_or_else(|| anyhow!("--map {m}: no source {src}"))?;
        if !st.columns.iter().any(|c| c.name == col) {
            bail!("--map {m}: {src} has no column {col}");
        }
        maps.entry(src.to_string()).or_default().push((col.to_string(), expr.to_string()));
    }
    let mut filters: BTreeMap<String, String> = BTreeMap::new();
    for f in &a.filters {
        let (src, cond) = f.split_once('=').ok_or_else(|| anyhow!("--filter {f}: expected SOURCE=CONDITION"))?;
        if !plan.sources().iter().any(|s| s.name == src) {
            bail!("--filter {f}: no source {src}");
        }
        maps.entry(src.to_string()).or_default();
        filters.insert(src.to_string(), cond.to_string());
    }
    let day = |d: &str| {
        brrrrr_core::value::parse_datetime(d)
            .filter(|t| t % DAY == 0)
            .ok_or_else(|| anyhow!("{d}: not a day (YYYY-MM-DD)"))
    };
    let (from, to) = (day(&a.from)?, day(&a.to)?);
    if to <= from {
        bail!("--to {} is not after --from {}", a.to, a.from);
    }
    let days: Vec<String> =
        (0..(to - from) / DAY).map(|i| brrrrr_core::expr::format_datetime(from + i * DAY, "%Y-%m-%d")).collect();
    let symbols = if a.symbols.is_empty() { discover(&templates[&needed[0]], &days[0])? } else { a.symbols.clone() };
    if symbols.is_empty() {
        bail!("no symbols: none given, and no file of {} on {}", needed[0], days[0]);
    }
    std::fs::create_dir(&a.out).with_context(|| format!("{}: the output directory must not exist", a.out.display()))?;
    // each symbol's files, the largest symbol first
    let mut parts: Vec<(String, BTreeMap<String, Vec<PathBuf>>, u64)> = vec![];
    for sym in &symbols {
        let mut files = BTreeMap::new();
        let mut bytes = 0;
        for (src, t) in &templates {
            let paths: Vec<PathBuf> = days
                .iter()
                .map(|d| PathBuf::from(t.replace("{symbol}", sym).replace("{day}", d)))
                .filter(|p| p.exists())
                .collect();
            // a path without {day}: one file for every day, read once
            let mut paths = paths;
            paths.dedup();
            bytes += paths.iter().map(|p| p.metadata().map_or(0, |m| m.len())).sum::<u64>();
            files.insert(src.clone(), paths);
        }
        if files.values().all(Vec::is_empty) {
            bail!("{sym}: no file of any source");
        }
        parts.push((sym.clone(), files, bytes));
    }
    parts.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));
    let threads = a.threads.unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get())).max(1);
    let started = Instant::now();
    let done = Mutex::new(vec![]);
    let failed = Mutex::new(None::<anyhow::Error>);
    // one pool for the symbols and for the parts of their batches: a thread done with the small
    // symbols takes parts of the large ones. Symbols are taken largest first.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("historical-{i}"))
        .build()
        .context("a thread pool")?;
    let jobs = Rayon(&pool);
    let left = std::sync::atomic::AtomicUsize::new(parts.len());
    let lanes = a.symbols_at_once.unwrap_or(threads).clamp(1, parts.len().max(1));
    let queue = Mutex::new(parts.iter());
    pool.scope_fifo(|s| {
        // `lanes` symbols at once, each lane taking the next (the largest first)
        for _ in 0..lanes {
            let (plan, a, done, failed, jobs, left, queue, maps, cat, filters) =
                (&plan, &a, &done, &failed, &jobs, &left, &queue, &maps, &cat, &filters);
            s.spawn_fifo(move |_| loop {
                if failed.lock().unwrap_or_else(|p| p.into_inner()).is_some() {
                    return;
                }
                let Some((sym, files, _)) = queue.lock().unwrap_or_else(|p| p.into_inner()).next() else { return };
                // the threads a symbol may decode on: its share of those of the symbols in flight
                let share = threads / left.load(std::sync::atomic::Ordering::Relaxed).min(lanes).max(1);
                let decode = a.decode_threads.map_or(share.clamp(1, 4), |d| d as usize);
                let r = partition(plan, cat, a, (maps, filters), sym, files, from..to, jobs, decode);
                left.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                if let Err(e) = r.map(|d| done.lock().unwrap_or_else(|p| p.into_inner()).push(d)) {
                    failed.lock().unwrap_or_else(|p| p.into_inner()).get_or_insert(e.context(sym.clone()));
                }
            });
        }
    });
    if let Some(e) = failed.into_inner().unwrap_or_else(|p| p.into_inner()) {
        return Err(e);
    }
    let mut done = done.into_inner().unwrap_or_else(|p| p.into_inner());
    done.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    let seconds = started.elapsed().as_secs_f64();
    let mut manifest = format!(
        "{{\"sql\":{},\"from\":\"{}\",\"to\":\"{}\",\"seconds\":{seconds:.3},\"peak_rss_bytes\":{},\"symbols\":[",
        json_str(&a.sql.display().to_string()),
        a.from,
        a.to,
        peak_rss()
    );
    for (i, d) in done.iter().enumerate() {
        manifest.push_str(&format!(
            "{}{{\"symbol\":{},\"rows\":{},\"messages\":{},\"files\":{},\"seconds\":{:.3},\"read_seconds\":{:.3},\"run_seconds\":{:.3}}}",
            if i > 0 { "," } else { "" },
            json_str(&d.symbol),
            d.rows,
            d.messages,
            d.files,
            d.seconds,
            d.read_seconds,
            d.run_seconds
        ));
    }
    manifest.push_str("]}\n");
    publish(&a.out, &done, &manifest)?;
    print!("{manifest}");
    Ok(())
}

/// Threads syncing files at once (`sync_all`).
const SYNC_THREADS: usize = 16;

/// `paths` on disk, on `SYNC_THREADS` threads at most, each a share of them: a thread a file is
/// tens of thousands of threads for a day of an exchange's symbols (a file each sink's), whose
/// stacks' mappings pass `vm.max_map_count` (65530 by default) and the next thread cannot start.
/// The threads it started.
fn sync_all(paths: &[&Path]) -> Result<usize> {
    std::thread::scope(|s| {
        let synced: Vec<_> = (paths.chunks(paths.len().div_ceil(SYNC_THREADS).max(1)))
            .map(|share| {
                s.spawn(move || {
                    share.iter().try_for_each(|p| {
                        File::open(p).and_then(|f| f.sync_all()).with_context(|| p.display().to_string())
                    })
                })
            })
            .collect();
        let n = synced.len();
        for h in synced {
            h.join().map_err(|_| anyhow!("a sync thread panicked"))??;
        }
        Ok(n)
    })
}

/// Every symbol's file on disk (synced side by side: past the first, a sync finds little to
/// write), then under its name, then `_SUCCESS`: nothing is there under its name before all of
/// it is, and on disk.
fn publish(out: &Path, done: &[Done], manifest: &str) -> Result<()> {
    let files: Vec<&(PathBuf, PathBuf)> = done.iter().flat_map(|d| &d.outputs).collect();
    let dirs: std::collections::BTreeSet<PathBuf> =
        files.iter().filter_map(|(_, f)| f.parent().map(Path::to_path_buf)).chain([out.to_path_buf()]).collect();
    sync_all(&files.iter().map(|(p, _)| p.as_path()).collect::<Vec<_>>())?;
    for (p, f) in &files {
        std::fs::rename(p, f)?;
    }
    let mut f = File::create(out.join("_SUCCESS"))?;
    f.write_all(manifest.as_bytes())?;
    f.sync_all()?;
    // the renames and `_SUCCESS` in their directories
    sync_all(&dirs.iter().map(PathBuf::as_path).collect::<Vec<_>>()).map(drop)
}

/// The symbols `template` has a file of on `day`: its file name with `{symbol}` matched.
fn discover(template: &str, day: &str) -> Result<Vec<String>> {
    let path = PathBuf::from(template.replace("{day}", day));
    let (dir, name) =
        (path.parent().unwrap_or(Path::new(".")), path.file_name().and_then(|n| n.to_str()).unwrap_or(""));
    let (pre, post) =
        name.split_once("{symbol}").ok_or_else(|| anyhow!("{template}: {{symbol}} is not in the file name"))?;
    let mut out = vec![];
    for e in std::fs::read_dir(dir).with_context(|| dir.display().to_string())? {
        let n = e?.file_name().to_string_lossy().into_owned();
        if let Some(sym) = n.strip_prefix(pre).and_then(|r| r.strip_suffix(post)) {
            if !sym.is_empty() {
                out.push(sym.to_string());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// The runtime's pool for a `Historical`'s parts of batches: rayon's.
struct Rayon<'p>(&'p rayon::ThreadPool);

impl Pool for Rayon<'_> {
    fn run<'a>(&self, jobs: Vec<Task<'a>>) {
        self.0.scope(|s| {
            for j in jobs {
                s.spawn(move |_| j());
            }
        });
    }

    fn threads(&self) -> usize {
        self.0.current_num_threads()
    }
}

/// One symbol: its sources' files through a `Historical`, into `<out>/<symbol>.jsonl` (or a
/// Parquet file per sink).
#[allow(clippy::too_many_arguments)]
fn partition(
    plan: &Historical,
    cat: &Catalog,
    a: &Args,
    (maps, filters): (&Maps, &BTreeMap<String, String>),
    sym: &str,
    files: &BTreeMap<String, Vec<PathBuf>>,
    range: std::ops::Range<i64>,
    pool: &dyn Pool,
    decode: usize,
) -> Result<Done> {
    let started = Instant::now();
    let mut h = plan.clone();
    let mut out = match a.format {
        Format::Jsonl => {
            let tmp = a.out.join(format!(".{sym}.jsonl.part"));
            let lines = Lines::new(File::create(&tmp).with_context(|| tmp.display().to_string())?);
            Out::Lines(lines, tmp, a.out.join(format!("{sym}.jsonl")))
        }
        Format::Parquet => Out::Parquet(Parquets::new(cat, &a.out, sym, &a.parquet_metadata)?),
    };
    let waited = Mutex::new(std::time::Duration::ZERO);
    let stats = std::thread::scope(|s| -> Result<_> {
        let mut inputs: Vec<(String, Box<dyn Source>)> = vec![];
        for st in h.sources() {
            let mut reads = h.reads(&st.name);
            // a symbol named by the path: its column is a constant, not read
            let symbol = st.columns.iter().position(|c| c.name == "symbol" && matches!(c.ty.base(), Type::Str));
            let path = symbol.filter(|k| a.symbol_from_path && reads.get(*k).copied().unwrap_or(false));
            if let Some(k) = path {
                reads[k] = false;
            }
            let paths = &files[&st.name];
            let width = st.columns.len();
            // the columns computed from the file's: those the pipelines read
            let mut mapping = None;
            let mut items: Vec<(usize, String, Type)> = vec![];
            let mut aliases = vec![];
            if let (Some(m), Some(first)) = (maps.get(&st.name), paths.first()) {
                let mut schema = file_columns(first)?;
                // a file column of a stream column's name the stream reads as it is: of the
                // stream column's type (text as a price) in the expressions too, decoded once
                let direct: Vec<Option<usize>> = (schema.iter())
                    .map(|(name, _)| {
                        st.columns
                            .iter()
                            .position(|c| c.name == *name)
                            .filter(|k| !m.iter().any(|(c, _)| *c == st.columns[*k].name))
                    })
                    .collect();
                for ((_, ty), k) in schema.iter_mut().zip(&direct) {
                    if let Some(k) = k {
                        *ty = st.columns[*k].ty.clone();
                    }
                }
                let mut exprs = vec![];
                for (col, expr) in m {
                    let k = st.columns.iter().position(|c| c.name == *col).expect("checked");
                    if reads[k] {
                        reads[k] = false;
                        exprs.push((k, st.columns[k].ty.clone(), expr.clone()));
                    }
                }
                let filter = filters.get(&st.name).map(String::as_str);
                let mapped = Mapping::new(&schema, &exprs, filter).map_err(|e| anyhow!("{}: {e}", st.name))?;
                for (j, (name, ty)) in schema.iter().enumerate() {
                    if mapped.reads().get(j).copied().unwrap_or(false) {
                        match direct[j].filter(|k| reads[*k]) {
                            Some(k) => aliases.push((width + j, k)),
                            None => items.push((width + j, name.clone(), ty.clone())),
                        }
                    }
                }
                mapping = Some((mapped, schema.len()));
            }
            items.splice(
                0..0,
                st.columns
                    .iter()
                    .enumerate()
                    .filter(|(k, _)| reads[*k])
                    .map(|(k, c)| (k, c.name.clone(), c.ty.clone())),
            );
            let mut src = Parquet::open(s, &items, width, paths, decode, a.batch_rows)?;
            src.constants = path.map(|k| (k, Value::Str(sym.into()))).into_iter().collect();
            src.mapping = mapping;
            src.aliases = aliases;
            inputs.push((st.name.clone(), Box::new(Timed { src, waited: &waited })));
        }
        h.run(inputs, range, pool, &mut out).map_err(|e| anyhow!(e))
    })?;
    let (messages, outputs) = out.finish()?;
    let waited = *waited.lock().unwrap_or_else(|p| p.into_inner());
    Ok(Done {
        symbol: sym.to_string(),
        rows: stats.rows.values().map(|r| r.1).sum(),
        messages,
        seconds: started.elapsed().as_secs_f64(),
        read_seconds: waited.as_secs_f64(),
        run_seconds: started.elapsed().saturating_sub(waited).as_secs_f64(),
        files: files.values().map(Vec::len).sum(),
        outputs,
    })
}

/// The columns of a Parquet file it can decode: Int64, Float64 and strings.
fn file_columns(path: &Path) -> Result<Vec<(String, Type)>> {
    let f = File::open(path).with_context(|| path.display().to_string())?;
    let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::default())
        .with_context(|| format!("{}: not a Parquet file", path.display()))?;
    use arrow_schema::DataType as D;
    Ok(meta
        .schema()
        .fields()
        .iter()
        .filter_map(|f| {
            let ty = match f.data_type() {
                D::Int64 => Type::Int(64),
                D::Float64 => Type::F64,
                D::Utf8 | D::LargeUtf8 => Type::Str,
                _ => return None,
            };
            Some((f.name().clone(), ty))
        })
        .collect())
}

/// A source, and the time its reader waited for its rows.
struct Timed<'w, S> {
    src: S,
    waited: &'w Mutex<std::time::Duration>,
}

impl<S: Source> Source for Timed<'_, S> {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        let at = Instant::now();
        let b = self.src.next();
        *self.waited.lock().unwrap_or_else(|p| p.into_inner()) += at.elapsed();
        b
    }
}

/// Messages as JSON lines: `{"topic":..,"headers":[[k,v],..],"payload":<the message>}`. The
/// first write error is kept and returned by `finish`: `Output::push` cannot fail.
struct Lines {
    w: std::io::BufWriter<File>,
    err: Option<std::io::Error>,
    messages: u64,
    line: String,
}

impl Lines {
    fn new(f: File) -> Lines {
        Lines { w: std::io::BufWriter::with_capacity(1 << 20, f), err: None, messages: 0, line: String::new() }
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(e) = self.err.take() {
            return Err(e.into());
        }
        self.w.flush()?;
        Ok(())
    }
}

impl Output for Lines {
    fn push(&mut self, e: Emit) {
        if self.err.is_some() {
            return;
        }
        let l = &mut self.line;
        l.clear();
        l.push_str("{\"topic\":");
        l.push_str(&json_str(&e.topic));
        l.push_str(",\"headers\":[");
        for (i, (k, v)) in e.headers.iter().enumerate() {
            l.push_str(if i > 0 { ",[" } else { "[" });
            l.push_str(&json_str(k));
            l.push(',');
            l.push_str(&json_str(v));
            l.push(']');
        }
        l.push_str("],\"payload\":");
        l.push_str(e.payload.trim_end());
        l.push_str("}\n");
        self.messages += 1;
        if let Err(err) = self.w.write_all(l.as_bytes()) {
            self.err = Some(err);
        }
    }
}

/// Where a symbol's messages go: JSON lines (written as they come, and their file's temporary and
/// own names), or a Parquet file per sink.
enum Out {
    Lines(Lines, PathBuf, PathBuf),
    Parquet(Parquets),
}

impl Out {
    /// Writes what is left: the messages, and each file under its temporary name and its own.
    fn finish(self) -> Result<(u64, Vec<(PathBuf, PathBuf)>)> {
        match self {
            Out::Lines(mut l, tmp, path) => {
                l.finish()?;
                Ok((l.messages, vec![(tmp, path)]))
            }
            Out::Parquet(p) => p.finish(),
        }
    }
}

impl Output for Out {
    fn push(&mut self, e: Emit) {
        if let Out::Lines(l, ..) = self {
            l.push(e);
        }
    }

    fn row(&mut self, sink: &str, row: &[Value]) -> bool {
        match self {
            Out::Lines(..) => false,
            Out::Parquet(p) => p.row(sink, row),
        }
    }
}

/// Each sink's rows (every one the SQL's views write), a Parquet file per sink:
/// `<out>/<sink>/<symbol>.parquet`, in the sink's columns (`_tp_*` aside) and types.
struct Parquets {
    out: PathBuf,
    symbol: String,
    /// Per sink: its columns (position in a row, name, type), and their values so far.
    sinks: BTreeMap<String, Table>,
    rows: u64,
    metadata: Vec<(String, String)>,
}

impl Parquets {
    fn new(cat: &Catalog, out: &Path, symbol: &str, metadata: &[String]) -> Result<Parquets> {
        let metadata = (metadata.iter())
            .map(|kv| {
                kv.split_once('=')
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .ok_or_else(|| anyhow!("--parquet-metadata {kv}: expected KEY=VALUE"))
            })
            .collect::<Result<_>>()?;
        let written: std::collections::BTreeSet<&str> = cat.views.iter().map(|v| v.target.as_str()).collect();
        let sinks = (cat.streams.values())
            .filter(|s| matches!(s.kind, Kind::External) && written.contains(s.name.as_str()))
            .map(|s| {
                let cols: Vec<(usize, String, Type)> = (s.columns.iter().enumerate())
                    .filter(|(_, c)| !c.name.starts_with("_tp_"))
                    .map(|(k, c)| (k, c.name.clone(), c.ty.clone()))
                    .collect();
                let builders = cols.iter().map(|(_, _, ty)| Builder::of(ty)).collect();
                (s.name.clone(), (cols, builders))
            })
            .collect();
        Ok(Parquets { out: out.to_path_buf(), symbol: symbol.to_string(), sinks, rows: 0, metadata })
    }

    fn row(&mut self, sink: &str, row: &[Value]) -> bool {
        let Some((cols, builders)) = self.sinks.get_mut(sink) else { return false };
        for ((k, ..), b) in cols.iter().zip(builders.iter_mut()) {
            b.push(&row[*k]);
        }
        self.rows += 1;
        true
    }

    fn finish(mut self) -> Result<(u64, Vec<(PathBuf, PathBuf)>)> {
        let mut files = vec![];
        for (sink, (cols, builders)) in &mut self.sinks {
            let dir = self.out.join(sink);
            std::fs::create_dir_all(&dir).with_context(|| dir.display().to_string())?;
            let (tmp, path) =
                (dir.join(format!(".{}.parquet.part", self.symbol)), dir.join(format!("{}.parquet", self.symbol)));
            write_parquet(&tmp, cols, builders, &self.metadata).with_context(|| tmp.display().to_string())?;
            files.push((tmp, path));
        }
        Ok((self.rows, files))
    }
}

/// A sink column's values as its rows come, in the Arrow type of its Parquet column: 8 bytes a
/// Float64 rather than a `Value`'s 32 (a sink's rows are held until the symbol is done).
enum Builder {
    F32(arrow_array::builder::Float32Builder),
    F64(arrow_array::builder::Float64Builder),
    I32(arrow_array::builder::Int32Builder),
    I64(arrow_array::builder::Int64Builder),
    U64(arrow_array::builder::UInt64Builder),
    Time(arrow_array::builder::TimestampMicrosecondBuilder),
    Bool(arrow_array::builder::BooleanBuilder),
    Str(arrow_array::builder::StringBuilder),
}

impl Builder {
    fn of(ty: &Type) -> Builder {
        use arrow_array::builder::*;
        match ty.base() {
            Type::F32 => Builder::F32(Float32Builder::new()),
            Type::F64 => Builder::F64(Float64Builder::new()),
            Type::Int(b) if *b <= 32 => Builder::I32(Int32Builder::new()),
            Type::Int(_) => Builder::I64(Int64Builder::new()),
            Type::UInt(_) => Builder::U64(UInt64Builder::new()),
            Type::Time(_) => Builder::Time(TimestampMicrosecondBuilder::new()),
            Type::Bool => Builder::Bool(BooleanBuilder::new()),
            _ => Builder::Str(StringBuilder::new()),
        }
    }

    fn push(&mut self, x: &Value) {
        match self {
            Builder::F32(b) => b.append_option(x.f64().map(|f| f as f32)),
            Builder::F64(b) => b.append_option(x.f64()),
            Builder::I32(b) => b.append_option(x.i64().map(|i| i as i32)),
            Builder::I64(b) => b.append_option(x.i64()),
            Builder::U64(b) => b.append_option(match x {
                Value::UInt(u) => Some(*u),
                x => x.i64().map(|i| i as u64),
            }),
            Builder::Time(b) => b.append_option(x.i64()),
            Builder::Bool(b) => b.append_option(match x {
                Value::Bool(v) => Some(*v),
                _ => None,
            }),
            Builder::Str(b) => b.append_option(x.str()),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::F32(b) => Arc::new(b.finish()),
            Builder::F64(b) => Arc::new(b.finish()),
            Builder::I32(b) => Arc::new(b.finish()),
            Builder::I64(b) => Arc::new(b.finish()),
            Builder::U64(b) => Arc::new(b.finish()),
            Builder::Time(b) => Arc::new(b.finish()),
            Builder::Bool(b) => Arc::new(b.finish()),
            Builder::Str(b) => Arc::new(b.finish()),
        }
    }
}

/// A column's encoding: one of Parquet's, or `None`, a dictionary of its values.
type Choice = Option<parquet::basic::Encoding>;

/// Columns of `values` (each its type) as a Parquet file, small: ZSTD, and each column in the
/// encoding that makes it smallest, of its kind's (`candidates`): each is tried on the column
/// alone. Floats that repeat (a price at 15 s) are smallest plain, floats that do not (a volume)
/// split by byte; times and counts delta-packed, or in a dictionary when few.
fn write_parquet(
    path: &Path,
    cols: &[(usize, String, Type)],
    builders: &mut [Builder],
    metadata: &[(String, String)],
) -> Result<()> {
    use parquet::basic::Encoding as E;
    let floats = [Some(E::PLAIN), Some(E::BYTE_STREAM_SPLIT), None];
    let integers = [Some(E::DELTA_BINARY_PACKED), Some(E::PLAIN), None];
    let mut props =
        parquet_properties(9).set_key_value_metadata((!metadata.is_empty()).then(|| {
            metadata.iter().map(|(k, v)| parquet::file::metadata::KeyValue::new(k.clone(), v.clone())).collect()
        }));
    let mut fields = vec![];
    for ((_, name, ty), b) in cols.iter().zip(builders) {
        let a = b.finish();
        let candidates: &[Choice] = match b {
            Builder::F32(_) | Builder::F64(_) => &floats,
            Builder::I32(_) | Builder::I64(_) | Builder::U64(_) | Builder::Time(_) => &integers,
            Builder::Bool(_) => &[],
            Builder::Str(_) => &[None],
        };
        let nullable = matches!(ty, Type::Nullable(_));
        match candidates {
            [] => {}
            [only] => props = choose(props, name, *only),
            _ => props = choose(props, name, smallest(name, &a, nullable, candidates)?),
        }
        fields.push((name.clone(), a, nullable));
    }
    let batch = arrow_array::RecordBatch::try_from_iter_with_nullable(fields)?;
    let mut w = arrow_writer(File::create(path)?, &batch, props.build())?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

/// The writer's properties every file and every trial has: Parquet 2, ZSTD at `level` (9 for a
/// file), no dictionary but where a column is given one.
fn parquet_properties(level: i32) -> parquet::file::properties::WriterPropertiesBuilder {
    use parquet::basic::{Compression, ZstdLevel};
    parquet::file::properties::WriterProperties::builder()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(level).expect("a ZSTD level")))
        .set_dictionary_enabled(false)
        // pages as large as their bytes allow, not of 20k rows: each is compressed alone, and
        // a long column's values repeat across them (a 15s window's month: 3.6x the bytes)
        .set_data_page_row_count_limit(usize::MAX)
        // a column's min and max, not every page's: an output's pages are few
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
}

/// A writer of `batch`'s schema without the Arrow schema in the footer (Parquet's says it all,
/// and it is most of a small file's footer).
fn arrow_writer<W: std::io::Write + Send>(
    w: W,
    batch: &arrow_array::RecordBatch,
    props: parquet::file::properties::WriterProperties,
) -> Result<parquet::arrow::ArrowWriter<W>> {
    let options =
        parquet::arrow::arrow_writer::ArrowWriterOptions::new().with_properties(props).with_skip_arrow_metadata(true);
    Ok(parquet::arrow::ArrowWriter::try_new_with_options(w, batch.schema(), options)?)
}

fn choose(
    props: parquet::file::properties::WriterPropertiesBuilder,
    name: &str,
    choice: Choice,
) -> parquet::file::properties::WriterPropertiesBuilder {
    let path = parquet::schema::types::ColumnPath::from(name);
    match choice {
        Some(e) => props.set_column_encoding(path, e),
        None => props.set_column_dictionary_enabled(path, true),
    }
}

/// Of `candidates`, the encoding in which column `a` alone is smallest (the first of equals).
fn smallest(name: &str, a: &ArrayRef, nullable: bool, candidates: &[Choice]) -> Result<Choice> {
    let mut best = (usize::MAX, None);
    for c in candidates {
        let size = column_bytes(name, a, nullable, *c)?;
        if size < best.0 {
            best = (size, *c);
        }
    }
    Ok(best.1)
}

/// The bytes of a file of column `a` alone, in `choice`.
fn column_bytes(name: &str, a: &ArrayRef, nullable: bool, choice: Choice) -> Result<usize> {
    let batch = arrow_array::RecordBatch::try_from_iter_with_nullable([(name, a.clone(), nullable)])?;
    let mut buf = vec![];
    // ranked at a quicker level than the file is written at: the order is the same
    let mut w = arrow_writer(&mut buf, &batch, choose(parquet_properties(3), name, choice).build())?;
    w.write(&batch)?;
    w.close()?;
    Ok(buf.len())
}

/// A JSON string literal.
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The process's peak resident memory, from /proc (0 where there is none).
fn peak_rss() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kb = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok());
    kb.unwrap_or(0) * 1024
}

/// A source stream's files, row group after row group, each column it reads decoded by a
/// thread of its own, a few batches ahead of the reader, which puts each batch together from
/// its columns' (every column of a row group comes in batches of the same rows).
struct Parquet {
    /// Per column read: its position in the stream and, per decoding thread, its batches
    /// (`None` at the end of a row group). Thread `t` of `k` decodes row groups `t`, `t + k`, ...
    cols: Vec<(usize, Vec<Decoded>)>,
    width: usize,
    /// The row group being read, of `groups`.
    group: usize,
    groups: usize,
    /// A source none of whose columns the pipelines read: the row count of each row group.
    counts: std::collections::VecDeque<usize>,
    /// Columns of one value in every row, not read (the symbol of a file named after it).
    constants: Vec<(usize, Value)>,
    /// Columns computed from the file's, which come after the stream's (`width` on), and how
    /// many the file has.
    mapping: Option<(Mapping, usize)>,
    /// Columns of the file the expressions read that are a stream column the stream reads:
    /// (position among the file's, the stream column's).
    aliases: Vec<(usize, usize)>,
    /// A source of few rows, decoded on the reader's thread.
    inline: Option<Inline>,
}

/// Sources of at most this many rows are decoded on the reader's thread.
const INLINE_ROWS: usize = 1 << 17;

/// Row groups decoded as they are read: each column of the next, then its batches.
struct Inline {
    groups: Vec<(PathBuf, ArrowReaderMetadata, usize)>,
    /// (position, name, type) of each column read
    cols: Vec<(usize, String, Type)>,
    batch_rows: usize,
    ready: std::collections::VecDeque<Vec<(usize, Col)>>,
}

/// A decoding thread's batches of a column: `None` at the end of a row group.
type Decoded = Receiver<Result<Option<Col>, String>>;

impl Parquet {
    /// Decodes `items` (each column's position, name in the files and type) of `files`, each on
    /// `threads` threads, each thread a whole row group ahead of the reader, into batches of a
    /// stream of `width` columns.
    fn open<'s>(
        s: &'s std::thread::Scope<'s, '_>,
        items: &[(usize, String, Type)],
        width: usize,
        files: &[PathBuf],
        threads: usize,
        batch_rows: usize,
    ) -> Result<Parquet> {
        // every row group of every file, in order
        let mut groups = vec![];
        for path in files {
            let f = File::open(path).with_context(|| path.display().to_string())?;
            let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::default())
                .with_context(|| format!("{}: not a Parquet file", path.display()))?;
            for g in 0..meta.metadata().num_row_groups() {
                groups.push((path.clone(), meta.clone(), g));
            }
        }
        let total: usize = groups.iter().map(|(_, m, i)| m.metadata().row_group(*i).num_rows() as usize).sum();
        if total <= INLINE_ROWS {
            // few rows: decoded here, as they are read, without threads
            let cols = items.to_vec();
            let counts = groups.iter().map(|(_, m, i)| m.metadata().row_group(*i).num_rows() as usize).collect();
            let inline = Some(Inline { groups, cols, batch_rows, ready: Default::default() });
            return Ok(Parquet {
                cols: vec![],
                width,
                group: 0,
                groups: 0,
                counts,
                constants: vec![],
                mapping: None,
                aliases: vec![],
                inline,
            });
        }
        let rows = groups.iter().map(|(_, m, i)| m.metadata().row_group(*i).num_rows() as usize).max().unwrap_or(0);
        let threads = threads.clamp(1, groups.len().max(1));
        // with several threads, a row group's batches and its end, so that each decodes the next
        // while the reader takes the one before; with one, a few batches
        let ahead = if threads > 1 { rows.div_ceil(batch_rows.max(1)) + 1 } else { 2 };
        let groups = Arc::new(groups);
        let mut cols = vec![];
        for (k, name, ty) in items {
            let mut rx = vec![];
            for t in 0..threads {
                let (tx, r) = sync_channel(ahead);
                rx.push(r);
                let (groups, name, ty) = (groups.clone(), name.clone(), ty.clone());
                std::thread::Builder::new().name(format!("decode-{name}")).spawn_scoped(s, move || {
                    for (path, meta, i) in groups.iter().skip(t).step_by(threads) {
                        let sent =
                            decode(path, meta, *i, &name, &ty, batch_rows, &mut |c| tx.send(Ok(Some(c))).is_ok());
                        let end = match sent {
                            Ok(true) => Ok(None),
                            Ok(false) => return, // the reader is gone
                            Err(e) => Err(format!("{}: {e:#}", path.display())),
                        };
                        let failed = end.is_err();
                        if tx.send(end).is_err() || failed {
                            return;
                        }
                    }
                })?;
            }
            cols.push((*k, rx));
        }
        let counts = groups.iter().map(|(_, m, i)| m.metadata().row_group(*i).num_rows() as usize).collect();
        Ok(Parquet {
            cols,
            width,
            group: 0,
            groups: groups.len(),
            counts,
            constants: vec![],
            mapping: None,
            aliases: vec![],
            inline: None,
        })
    }
}

impl Parquet {
    /// The batch of `cols` (each column's position and values), the others NULL or constant.
    fn batch(&self, cols: Vec<(usize, Col)>, n: usize) -> Batch {
        let file = self.mapping.as_ref().map_or(0, |m| m.1);
        let mut out: Vec<Arc<Col>> =
            (0..self.width + file).map(|_| Arc::new(Col::new(Data::Const(Value::Null, n)))).collect();
        for (k, c) in cols {
            out[k] = Arc::new(c);
        }
        for (k, v) in &self.constants {
            out[*k] = Arc::new(Col::new(Data::Const(v.clone(), n)));
        }
        for (j, k) in &self.aliases {
            out[*j] = out[*k].clone();
        }
        if let Some((m, _)) = &self.mapping {
            let (cols, keep) = m.apply(&Batch::new(n, out.split_off(self.width)));
            for (k, c) in cols {
                out[k] = c;
            }
            if let Some(rows) = keep {
                return Batch::new(n, out).take(&rows);
            }
        }
        Batch::new(n, out)
    }
}

impl Source for Parquet {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        if let Some(inline) = self.inline.as_mut() {
            while inline.ready.is_empty() && !inline.groups.is_empty() && !inline.cols.is_empty() {
                let (path, meta, i) = inline.groups.remove(0);
                let mut per_col: Vec<Vec<Col>> = vec![];
                for (_, name, ty) in &inline.cols {
                    let mut got = vec![];
                    if let Err(e) = decode(&path, &meta, i, name, ty, inline.batch_rows, &mut |c| {
                        got.push(c);
                        true
                    }) {
                        return Some(Err(format!("{}: {e:#}", path.display())));
                    }
                    per_col.push(got);
                }
                let n = per_col.first().map_or(0, Vec::len);
                for b in 0..n {
                    inline.ready.push_back(
                        inline
                            .cols
                            .iter()
                            .zip(&mut per_col)
                            .map(|((k, ..), c)| {
                                (*k, std::mem::replace(&mut c[b], Col::new(Data::Const(Value::Null, 0))))
                            })
                            .collect(),
                    );
                }
            }
            if inline.cols.is_empty() {
                let n = self.counts.pop_front()?;
                return Some(Ok(self.batch(vec![], n)));
            }
            let cols = self.inline.as_mut().and_then(|i| i.ready.pop_front())?;
            let n = cols.first().map_or(0, |c| c.1.len());
            return Some(Ok(self.batch(cols, n)));
        }
        if self.cols.is_empty() {
            // no column read: rows without values (never in practice, the clock is read)
            let n = self.counts.pop_front()?;
            let cols = (0..self.width).map(|_| Arc::new(Col::new(Data::Const(Value::Null, n)))).collect();
            return Some(Ok(Batch::new(n, cols)));
        }
        while self.group < self.groups {
            let mut got: Vec<(usize, Col)> = Vec::with_capacity(self.cols.len());
            let mut ended = 0;
            for (k, rx) in &self.cols {
                match rx[self.group % rx.len()].recv() {
                    Ok(Ok(Some(c))) => got.push((*k, c)),
                    Ok(Ok(None)) => ended += 1,
                    Ok(Err(e)) => {
                        self.group = self.groups;
                        return Some(Err(e));
                    }
                    Err(_) => {
                        self.group = self.groups;
                        return Some(Err("a decoding thread stopped".into()));
                    }
                }
            }
            if ended == self.cols.len() {
                self.group += 1;
                continue;
            }
            let n = got[0].1.len();
            if ended > 0 || got.iter().any(|(_, c)| c.len() != n) {
                self.group = self.groups;
                return Some(Err("columns of a row group in batches of different rows".into()));
            }
            return Some(Ok(self.batch(got, n)));
        }
        None
    }
}

/// Column `name` of row group `i` of a file, as columns of `ty` of `batch_rows` rows, each to
/// `send` as it is decoded; `false` if `send` stopped.
fn decode(
    path: &Path,
    meta: &ArrowReaderMetadata,
    i: usize,
    name: &str,
    ty: &Type,
    batch_rows: usize,
    send: &mut dyn FnMut(Col) -> bool,
) -> Result<bool> {
    let schema = meta.parquet_schema();
    let leaf = (0..schema.num_columns())
        .find(|&j| schema.column(j).name() == name)
        .ok_or_else(|| anyhow!("no column {name}"))?;
    // text read as Float64: as a dictionary, so that each distinct value is parsed once
    let field = meta.schema().field_with_name(name).ok();
    let text = field
        .is_some_and(|f| matches!(f.data_type(), arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8));
    let meta = if matches!(ty.base(), Type::F64) && text {
        let fields: Vec<arrow_schema::FieldRef> = (meta.schema().fields().iter())
            .map(|f| {
                if f.name() == name {
                    let dict = arrow_schema::DataType::Dictionary(
                        Box::new(arrow_schema::DataType::Int32),
                        Box::new(f.data_type().clone()),
                    );
                    Arc::new(f.as_ref().clone().with_data_type(dict))
                } else {
                    f.clone()
                }
            })
            .collect();
        let hint = Arc::new(arrow_schema::Schema::new(fields));
        ArrowReaderMetadata::try_new(meta.metadata().clone(), ArrowReaderOptions::new().with_schema(hint))?
    } else {
        meta.clone()
    };
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(File::open(path)?, meta)
        .with_row_groups(vec![i])
        .with_projection(ProjectionMask::leaves(schema, [leaf]))
        .with_batch_size(batch_rows)
        .build()?;
    for rb in reader {
        let c = column(rb?.column(0), ty).with_context(|| format!("column {name}"))?;
        if !send(c) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Text as Float64s, each parsed as `to_float64` parses it (`Value::cast`), a dictionary's values
/// once each; `None` if not text.
fn text_floats(a: &ArrayRef) -> Result<Option<Vec<Option<f64>>>> {
    use arrow_array::types::Int32Type;
    use arrow_array::DictionaryArray;
    let parse = |s: &str| Value::Str(s.into()).cast(&Type::F64).f64();
    let texts = |a: &dyn Array| -> Option<Vec<Option<f64>>> {
        if let Some(s) = a.as_any().downcast_ref::<StringArray>() {
            return Some(s.iter().map(|x| x.and_then(parse)).collect());
        }
        a.as_any().downcast_ref::<LargeStringArray>().map(|s| s.iter().map(|x| x.and_then(parse)).collect())
    };
    if let Some(d) = a.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let Some(values) = texts(d.values().as_ref()) else { return Ok(None) };
        return Ok(Some(d.keys().iter().map(|k| k.and_then(|k| values[k as usize])).collect()));
    }
    Ok(texts(a.as_ref()))
}

/// An Arrow array as a column of `ty`: Int64 for an integer, Float64 for a float, UTF-8 for a
/// string. A column of one value is a constant (a partition's symbol, its exchange).
fn column(a: &ArrayRef, ty: &Type) -> Result<Col> {
    let nulls = a.nulls().filter(|n| n.null_count() != 0).map(|n| (0..a.len()).map(|i| n.is_null(i)).collect());
    let data = match ty.base() {
        Type::Int(64) | Type::Time(_) => {
            let v = a.as_any().downcast_ref::<Int64Array>().ok_or_else(|| anyhow!("{} is not Int64", a.data_type()))?;
            let v = v.values();
            let time = matches!(ty.base(), Type::Time(_));
            if nulls.is_none() && !v.is_empty() && v.iter().all(|x| *x == v[0]) {
                Data::Const(if time { Value::Time(v[0]) } else { Value::Int(v[0]) }, v.len())
            } else if time {
                Data::Time(v.to_vec().into())
            } else {
                Data::Int(v.to_vec().into())
            }
        }
        Type::F64 => match a.as_any().downcast_ref::<Float64Array>() {
            Some(v) => Data::F64(v.values().to_vec().into()),
            None => {
                // text, as the archive keeps prices
                let v = text_floats(a)?.ok_or_else(|| anyhow!("{} is not Float64 or text", a.data_type()))?;
                let nulls: Vec<bool> = v.iter().map(Option::is_none).collect();
                let nulls = nulls.contains(&true).then(|| nulls.into());
                return Ok(Col { data: Data::F64(v.iter().map(|x| x.unwrap_or(0.0)).collect()), nulls });
            }
        },
        Type::Str => {
            let s = if let Some(s) = a.as_any().downcast_ref::<StringArray>() {
                let o = s.value_offsets();
                let (lo, hi) = (o[0] as usize, o[o.len() - 1] as usize);
                let offsets = o.iter().map(|x| (*x - o[0]) as u32).collect();
                Strs::from_parts(offsets, s.values()[lo..hi].to_vec()).map_err(|e| anyhow!(e))?
            } else if let Some(s) = a.as_any().downcast_ref::<LargeStringArray>() {
                (0..s.len()).map(|i| s.value(i)).collect()
            } else {
                bail!("{} is not a string", a.data_type());
            };
            if nulls.is_none() && !s.is_empty() && s.iter().all(|x| x == s.get(0)) {
                Data::Const(Value::Str(s.get(0).into()), s.len())
            } else {
                Data::Str(s)
            }
        }
        t => bail!("a {} column for {t:?}: brrrrr reads int64, float64 and string columns", a.data_type()),
    };
    Ok(Col { data, nulls })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use brrrrr_core::engine::{Asof, Engine};
    use brrrrr_core::sql::Stream;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use serde_json::Value as J;

    fn root(p: &str) -> PathBuf {
        PathBuf::from(format!("{}/../../{p}", env!("CARGO_MANIFEST_DIR")))
    }

    /// Tens of thousands of files are synced on `SYNC_THREADS` threads, not a thread each, every
    /// one of them; a file that cannot be opened is an error, named.
    #[test]
    fn files_are_synced_on_a_bounded_number_of_threads() {
        let d = scratch("sync");
        let paths: Vec<PathBuf> = (0..1000).map(|i| d.join(format!("{i}"))).collect();
        paths.iter().for_each(|p| std::fs::write(p, b"x").unwrap());
        let all: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
        assert_eq!(sync_all(&all).unwrap(), SYNC_THREADS);
        assert_eq!(sync_all(&all[..3]).unwrap(), 3);
        assert_eq!(sync_all(&[]).unwrap(), 0);
        let missing = d.join("missing");
        let err = sync_all(&[all[0], &missing, all[1]]).unwrap_err();
        assert!(format!("{err:#}").contains("missing"), "{err:#}");
    }

    /// A fresh directory under the target dir.
    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("brrrrr-historical-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Synthetic rows of `stream` (a market.proto message's columns), in clock order: three
    /// symbols' rows about every 90 s over 2026-09-20 and 21, each stream and symbol its own
    /// random walk, ids in clock order.
    fn rows(cat: &Catalog, stream: &str) -> Vec<Vec<Value>> {
        let st = &cat.streams[stream];
        let mut x = stream.bytes().fold(0x9e37_79b9_7f4a_7c15u64, |h, b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3));
        let mut next = move || {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (x >> 11) as f64 / (1u64 << 53) as f64
        };
        let start = 1_789_862_400_000_000i64; // 2026-09-20
        let mut out = vec![];
        for (symbol, base) in [("BTCUSDT", 60_000.0), ("ETHUSDT", 2_500.0), ("SOLUSDT", 150.0)] {
            let (mut t, mut walk, mut rate) = (start + (next() * 90e6) as i64, 1.0, 0.0001);
            while t < start + 2 * DAY {
                walk *= 1.0 + (next() - 0.5) * 0.002;
                if next() < 0.1 {
                    rate = ((next() - 0.5) * 0.002 * 1e8).round() / 1e8;
                }
                let price = (base * walk * 100.0).round() / 100.0;
                let quantity = ((0.01 + next() * 3.0) * 1000.0).round() / 1000.0;
                let row = st.columns.iter().map(|c| match c.name.as_str() {
                    "time" => Value::Int(t - (next() * 2_000.0) as i64),
                    "local_timestamp" => Value::Int(t),
                    "exchange" => Value::Int(1),
                    "symbol" => Value::Str(symbol.into()),
                    "id" => Value::Str(format!("{t:017}").into()),
                    "side" => Value::Str(if next() < 0.5 { "buy" } else { "sell" }.into()),
                    "quantity" | "bid_amount" | "ask_amount" => Value::F64(quantity),
                    "amount" => Value::F64(price * quantity),
                    "ask_price" => Value::F64(price + 0.01),
                    "mark_price" => Value::F64(price * (1.0 + (next() - 0.5) * 1e-4)),
                    "index_price" => Value::F64(price * (1.0 - (next() - 0.5) * 1e-4)),
                    "funding_rate" => Value::F64(rate),
                    "open_interest" => Value::F64((1e6 * walk).round()),
                    "long_short_ratio" => Value::F64(0.5 + next()),
                    "long_share" | "short_share" => Value::F64(next()),
                    _ => Value::F64(price), // price, bid_price
                });
                out.push(row.collect::<Vec<_>>());
                t += 30_000_000 + (next() * 120e6) as i64;
            }
        }
        let clock = st.columns.iter().position(|c| c.name == "local_timestamp").unwrap();
        out.sort_by_key(|r| r[clock].i64()); // stable: a symbol's rows keep their order
        out
    }

    /// `rows` (of `st`) as a Parquet file of row groups of at most `group` rows.
    fn write(path: &Path, st: &Stream, rows: &[Vec<Value>], group: usize) {
        let cols: Vec<(String, ArrayRef)> = st
            .columns
            .iter()
            .enumerate()
            .map(|(k, c)| {
                let a: ArrayRef = match c.ty.base() {
                    Type::F64 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| r[k].f64()))),
                    Type::Int(_) => Arc::new(Int64Array::from_iter(rows.iter().map(|r| r[k].i64()))),
                    _ => Arc::new(StringArray::from_iter(rows.iter().map(|r| r[k].str().map(str::to_string)))),
                };
                (c.name.clone(), a)
            })
            .collect();
        let batch = RecordBatch::try_from_iter(cols).unwrap();
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(group)).build();
        let mut w = ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    const DAY: i64 = 86_400_000_000;

    /// Each source's rows, by name.
    type Sources = Vec<(String, Vec<Vec<Value>>)>;

    /// Each source of `sql`'s fixture rows in files of `<dir>/<day>/<source>_<symbol>.parquet`
    /// (the symbols' days apart), and the --source arguments for them.
    fn files(sql: &str, dir: &Path, group: usize) -> (Catalog, Vec<String>, Sources) {
        let cat = brrrrr_core::sql::parse(&std::fs::read_to_string(root(sql)).unwrap()).unwrap();
        let h = Historical::new(&cat).unwrap();
        let mut args = vec![];
        let mut all = vec![];
        for st in h.sources() {
            let rows = rows(&cat, &st.name);
            let clock = st.columns.iter().position(|c| c.name.starts_with("local_timestamp")).unwrap();
            let sym = st.columns.iter().position(|c| c.name == "symbol").unwrap();
            let mut by: BTreeMap<(String, String), Vec<Vec<Value>>> = BTreeMap::new();
            for r in &rows {
                let day = brrrrr_core::expr::format_datetime(r[clock].i64().unwrap(), "%Y-%m-%d");
                by.entry((day, r[sym].str().unwrap().to_string())).or_default().push(r.clone());
            }
            for ((day, s), rows) in &by {
                std::fs::create_dir_all(dir.join(day)).unwrap();
                write(&dir.join(day).join(format!("{}_{s}.parquet", st.name)), st, rows, group);
            }
            args.push(format!("{}={}/{{day}}/{}_{{symbol}}.parquet", st.name, dir.display(), st.name));
            all.push((st.name.clone(), rows));
        }
        (cat, args, all)
    }

    /// The engine's messages for `sources` (merged by clock, one symbol at a time), sorted.
    fn engine_run(cat: &Catalog, sources: &[(String, Vec<Vec<Value>>)]) -> Vec<String> {
        let mut out = vec![];
        let symbols: std::collections::BTreeSet<String> = sources
            .iter()
            .flat_map(|(_, rows)| {
                rows.iter().map(|r| {
                    r.iter().find_map(|v| v.str().filter(|s| s.ends_with("USDT")).map(str::to_string)).unwrap()
                })
            })
            .collect();
        for sym in symbols {
            let mut e = Engine::new(cat).unwrap();
            e.set_asof(Asof::Exact);
            let mut all = vec![];
            for (name, rows) in sources {
                let st = &cat.streams[name];
                let clock = st.columns.iter().position(|c| c.name.starts_with("local_timestamp")).unwrap();
                let s = st.columns.iter().position(|c| c.name == "symbol").unwrap();
                all.extend(
                    rows.iter()
                        .filter(|r| r[s].str() == Some(&sym))
                        .map(|r| (r[clock].i64().unwrap(), name.clone(), r.clone())),
                );
            }
            all.sort_by_key(|a| a.0);
            let mut emits = vec![];
            for (_, name, r) in all {
                e.insert(&name, vec![r], &mut emits);
            }
            e.close_until(i64::MAX, &mut emits);
            out.extend(emits.into_iter().map(|m| format!("{} {}", m.topic, m.payload.trim())));
        }
        out.sort();
        out
    }

    /// The messages a run wrote, sorted, and its manifest.
    fn written(out: &Path) -> (Vec<String>, J) {
        let mut msgs = vec![];
        for f in std::fs::read_dir(out).unwrap() {
            let p = f.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            assert!(!name.starts_with('.'), "a part file left: {name}");
            if name.ends_with(".jsonl") {
                for l in std::fs::read_to_string(&p).unwrap().lines() {
                    let m: J = serde_json::from_str(l).unwrap();
                    msgs.push(format!("{} {}", m["topic"].as_str().unwrap(), m["payload"]));
                }
            }
        }
        msgs.sort();
        (msgs, serde_json::from_str(&std::fs::read_to_string(out.join("_SUCCESS")).unwrap()).unwrap())
    }

    fn args(sql: &str, sources: &[String], out: &Path) -> Args {
        Args {
            sql: root(sql),
            sources: sources.to_vec(),
            from: "2026-09-20".into(),
            to: "2026-09-22".into(),
            symbols: vec![],
            out: out.to_path_buf(),
            threads: Some(4),
            decode_threads: None,
            symbol_from_path: false,
            trust_order: false,
            symbols_at_once: None,
            batch_rows: 1000,
            maps: vec![],
            filters: vec![],
            format: Format::Jsonl,
            parquet_metadata: vec![],
        }
    }

    /// Re-serialised as serde_json writes a payload, for comparing with what `written` reads.
    /// Whether `got` are `want`'s messages (`normal`), a float's sum of a window built from its
    /// narrower windows' within rounding (ADR-0017): 1e-12 of it, one Float32 step where it is
    /// one. Every other value, and every topic, is the same.
    #[track_caller]
    fn assert_agree(got: &[String], want: &[String], what: &str) {
        fn close(x: &J, y: &J) -> bool {
            match (x, y) {
                (J::Number(a), J::Number(b)) if a != b => {
                    let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) else { return false };
                    let (d, m) = ((a - b).abs(), a.abs().max(b.abs()));
                    d <= 1e-12 * m || (a as f32 as f64 == a && b as f32 as f64 == b && d <= m * f32::EPSILON as f64)
                }
                (J::Object(a), J::Object(b)) => {
                    a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|w| close(v, w)))
                }
                _ => x == y,
            }
        }
        let split = |m: &String| -> (String, J) {
            let (topic, payload) = m.split_once(' ').unwrap();
            (topic.to_string(), serde_json::from_str(payload).unwrap())
        };
        let ok = got.len() == want.len()
            && got.iter().zip(want).all(|(g, w)| {
                let ((t, x), (u, y)) = (split(g), split(w));
                t == u && close(&x, &y)
            });
        assert!(
            ok,
            "{what}: {} messages, {} wanted; first differing: {:?}",
            got.len(),
            want.len(),
            got.iter().zip(want).find(|(g, w)| g != w)
        );
    }

    fn normal(msgs: Vec<String>) -> Vec<String> {
        let mut v: Vec<String> = msgs
            .into_iter()
            .map(|m| {
                let (topic, payload) = m.split_once(' ').unwrap();
                format!("{topic} {}", serde_json::from_str::<J>(payload).unwrap())
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn every_symbol_of_the_parquet_files_writes_what_the_engine_writes() {
        for (sql, group) in [("fixtures/pipelines/derivatives.sql", 1000), ("fixtures/pipelines/flow.sql", 777)] {
            let dir = scratch(&format!("engine-{group}"));
            let (cat, sources, rows) = files(sql, &dir, group);
            let want = normal(engine_run(&cat, &rows));
            for (k, (symbol_from_path, trust_order, decode)) in
                [(false, false, None), (true, true, Some(3))].into_iter().enumerate()
            {
                let out = dir.join(format!("out-{k}"));
                let mut a = args(sql, &sources, &out);
                (a.symbol_from_path, a.trust_order, a.decode_threads) = (symbol_from_path, trust_order, decode);
                run(a).unwrap();
                let (got, manifest) = written(&out);
                assert_agree(&normal(got), &want, &format!("{sql}, run {k}"));
                let symbols = manifest["symbols"].as_array().unwrap();
                assert_eq!(symbols.len(), 3, "{manifest}");
                assert!(symbols.iter().all(|s| s["rows"].as_u64().unwrap() > 0 && s["files"].as_u64().unwrap() > 0));
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// More rows than are decoded on the reader's thread: decoded on threads of their own, each
    /// a row group ahead, in row groups of every size.
    #[test]
    fn many_rows_are_decoded_on_threads_and_give_the_same_messages() {
        let sql = "fixtures/pipelines/bars.sql";
        let dir = scratch("threads");
        let cat = brrrrr_core::sql::parse(&std::fs::read_to_string(root(sql)).unwrap()).unwrap();
        let st = &cat.streams["trades"];
        let base = 1_789_948_800_000_000i64;
        let rows: Vec<Vec<Value>> = (0..INLINE_ROWS as i64 + 5_000)
            .map(|i| {
                let t = base + i * 300_000 + (i % 7);
                vec![
                    Value::Int(t - 1_000),
                    Value::Str(format!("{:09}", i).into()),
                    Value::Int(1),
                    Value::Str("BTCUSDT".into()),
                    Value::F64(60_000.0 + (i % 97) as f64),
                    Value::Int(t),
                    Value::Str(if i % 3 == 0 { "sell" } else { "buy" }.into()),
                    Value::F64(0.01 * (1 + i % 13) as f64),
                    Value::F64(600.0 * (1 + i % 13) as f64),
                ]
            })
            .collect();
        // days apart, as files of each day
        let mut by: BTreeMap<String, Vec<Vec<Value>>> = BTreeMap::new();
        for r in &rows {
            by.entry(brrrrr_core::expr::format_datetime(r[5].i64().unwrap(), "%Y-%m-%d")).or_default().push(r.clone());
        }
        for (day, rows) in &by {
            std::fs::create_dir_all(dir.join(day)).unwrap();
            write(&dir.join(day).join("trades_BTCUSDT.parquet"), st, rows, 9_999);
        }
        let want = normal(engine_run(&cat, &[("trades".to_string(), rows)]));
        let out = dir.join("out");
        let mut a = args(sql, &[format!("trades={}/{{day}}/trades_{{symbol}}.parquet", dir.display())], &out);
        let (first, last) = (by.keys().next().unwrap().clone(), by.keys().last().unwrap().clone());
        a.from = first;
        a.to = brrrrr_core::expr::format_datetime(brrrrr_core::value::parse_datetime(&last).unwrap() + DAY, "%Y-%m-%d");
        a.decode_threads = Some(2);
        run(a).unwrap();
        assert_agree(&normal(written(&out).0), &want, sql);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn what_cannot_run_is_refused_with_its_reason() {
        let sql = "fixtures/pipelines/bars.sql";
        let dir = scratch("refused");
        let (cat, sources, _) = files(sql, &dir, 500);
        let err = |a: Args| format!("{:#}", run(a).unwrap_err());
        // no --source for a stream the SQL reads
        let e = err(args(sql, &[], &dir.join("o1")));
        assert!(e.contains("no --source for trades"), "{e}");
        // a path without {symbol}
        let e = err(args(sql, &["trades=/x/{day}.parquet".into()], &dir.join("o2")));
        assert!(e.contains("{symbol}"), "{e}");
        // the output there already
        std::fs::create_dir_all(dir.join("o3")).unwrap();
        let e = err(args(sql, &sources, &dir.join("o3")));
        assert!(e.contains("must not exist"), "{e}");
        // not a day, an empty range
        let mut a = args(sql, &sources, &dir.join("o4"));
        a.from = "2026-09-20T01:00".into();
        assert!(err(a).contains("not a day"));
        let mut a = args(sql, &sources, &dir.join("o5"));
        a.to = a.from.clone();
        assert!(err(a).contains("is not after"));
        // rows out of their clock's order: refused, and nothing published
        let st = &cat.streams["trades"];
        let mut rows = rows(&cat, "trades");
        rows.retain(|r| r[3].str() == Some("BTCUSDT"));
        rows.swap(3, 40);
        let day = brrrrr_core::expr::format_datetime(rows[0][5].i64().unwrap(), "%Y-%m-%d");
        let bad = dir.join("bad");
        std::fs::create_dir_all(bad.join(&day)).unwrap();
        write(&bad.join(&day).join("trades_BTCUSDT.parquet"), st, &rows, 500);
        let mut a =
            args(sql, &[format!("trades={}/{{day}}/trades_{{symbol}}.parquet", bad.display())], &dir.join("o6"));
        a.symbols = vec!["BTCUSDT".into()];
        let e = err(a);
        assert!(e.contains("not in clock order"), "{e}");
        assert!(!dir.join("o6").join("_SUCCESS").exists());
        // a column of another type than the stream's
        let wrong = dir.join("wrong");
        std::fs::create_dir_all(wrong.join(&day)).unwrap();
        let batch =
            RecordBatch::try_from_iter([("local_timestamp", Arc::new(StringArray::from(vec!["1"])) as ArrayRef)])
                .unwrap();
        let mut w = ArrowWriter::try_new(
            File::create(wrong.join(&day).join("trades_BTCUSDT.parquet")).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let mut a =
            args(sql, &[format!("trades={}/{{day}}/trades_{{symbol}}.parquet", wrong.display())], &dir.join("o7"));
        a.symbols = vec!["BTCUSDT".into()];
        let e = err(a);
        assert!(e.contains("no column") || e.contains("is not Int64"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Files laid out otherwise than their stream (the archive's: a month in one file, prices as
    /// text, the quantity as `amount`, rows of another stream beside), mapped and filtered: the
    /// messages of the stream's own files. And as Parquet: a file per sink, its messages typed.
    #[test]
    fn files_of_another_layout_are_mapped_and_parquet_holds_the_messages() {
        let sql = "fixtures/pipelines/bars.sql";
        let dir = scratch("mapped");
        let (cat, sources, all) = files(sql, &dir.join("plain"), 1000);
        let want_out = dir.join("want");
        let mut a = args(sql, &sources, &want_out);
        a.symbols = vec!["BTCUSDT".into()];
        run(a).unwrap();
        let (want, _) = written(&want_out);
        assert!(!want.is_empty());
        // the archive's layout: one file of both days, a row without a price to leave out
        let mut rows: Vec<Vec<Value>> = all[0].1.iter().filter(|r| r[3].str() == Some("BTCUSDT")).cloned().collect();
        let mut stray = rows[10].clone();
        stray[4] = Value::Null;
        rows.insert(11, stray);
        let text = |k: usize| -> ArrayRef {
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r[k].str().map(str::to_string))))
        };
        let ints = |k: usize| -> ArrayRef { Arc::new(Int64Array::from_iter(rows.iter().map(|r| r[k].i64()))) };
        let floats = |k: usize| -> ArrayRef { Arc::new(Float64Array::from_iter(rows.iter().map(|r| r[k].f64()))) };
        let price: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r[4].f64().map(|p| p.to_string()))));
        let batch = RecordBatch::try_from_iter([
            ("symbol", text(3)),
            ("time", ints(0)),
            ("local_timestamp", ints(5)),
            ("id", text(1)),
            ("side", text(6)),
            ("price", price),
            ("amount", floats(7)),
            ("notional", floats(8)),
        ])
        .unwrap();
        std::fs::create_dir_all(dir.join("archive")).unwrap();
        let mut w = ArrowWriter::try_new(
            File::create(dir.join("archive/trades_BTCUSDT.parquet")).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let got_out = dir.join("got");
        let mut a = args(sql, &[format!("trades={}/archive/trades_{{symbol}}.parquet", dir.display())], &got_out);
        a.symbols = vec!["BTCUSDT".into()];
        a.maps = [
            "price=to_float64(price)",
            "quantity=amount",
            "amount=notional",
            &format!("exchange={}", rows[0][2].i64().unwrap()),
        ]
        .iter()
        .map(|m| format!("trades.{m}"))
        .collect();
        a.filters = vec!["trades=price IS NOT NULL".into()];
        run(a).unwrap();
        assert_agree(&normal(written(&got_out).0), &normal(want.clone()), "the mapped files");
        // as Parquet: a file per sink, each of its topic's messages, the footer's metadata
        let pq_out = dir.join("parquet");
        let mut a = args(sql, &sources, &pq_out);
        a.symbols = vec!["BTCUSDT".into()];
        a.format = Format::Parquet;
        a.parquet_metadata = vec!["origin=test".into()];
        run(a).unwrap();
        let (mut sinks, mut sink_rows) = (0, 0);
        for st in cat.streams.values().filter(|s| matches!(s.kind, Kind::External) && s.name.ends_with("_out")) {
            let f = File::open(pq_out.join(&st.name).join("BTCUSDT.parquet")).unwrap();
            let r = parquet::file::reader::SerializedFileReader::new(f).unwrap();
            use parquet::file::reader::FileReader;
            // compressed with ZSTD (its level is not in the file)
            let chunk = r.metadata().row_group(0).column(0);
            assert!(matches!(chunk.compression(), parquet::basic::Compression::ZSTD(_)), "{}", st.name);
            let meta = r.metadata().file_metadata();
            let kv = meta.key_value_metadata().unwrap();
            assert!(kv.iter().any(|k| k.key == "origin" && k.value.as_deref() == Some("test")));
            let topic = &st.settings["topic"];
            let times: Vec<i64> = want
                .iter()
                .filter(|m| m.split_once(' ').unwrap().0 == topic)
                .map(|m| serde_json::from_str::<J>(m.split_once(' ').unwrap().1).unwrap()["time"].as_i64().unwrap())
                .collect();
            assert_eq!(meta.num_rows() as usize, times.len(), "{}", st.name);
            sink_rows += meta.num_rows() as u64;
            let b = ParquetRecordBatchReaderBuilder::try_new(
                File::open(pq_out.join(&st.name).join("BTCUSDT.parquet")).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            let mut got: Vec<i64> = b
                .flat_map(|b| {
                    let b = b.unwrap();
                    let col = b.column_by_name("time").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
                    col.values().to_vec()
                })
                .collect();
            let mut times = times;
            got.sort();
            times.sort();
            assert_eq!(got, times, "{}", st.name);
            sinks += 1;
        }
        assert_eq!(sinks, 3);
        // the manifest counts each row written as a message
        let (_, manifest) = written(&pq_out);
        assert_eq!(manifest["symbols"][0]["messages"].as_u64(), Some(sink_rows), "{manifest}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An archive's layout: prices as text, read as the stream's Float64s and
    /// used as such in another column's expression; the clock under another name; a quantity
    /// under the name of the stream's notional.
    #[test]
    fn the_archives_layout_reads_text_prices_and_maps_its_columns() {
        let sql = "fixtures/pipelines/bars.sql";
        let dir = scratch("archive-layout");
        let (_, sources, all) = files(sql, &dir.join("plain"), 1000);
        let want_out = dir.join("want");
        let mut a = args(sql, &sources, &want_out);
        a.symbols = vec!["BTCUSDT".into()];
        run(a).unwrap();
        let (want, _) = written(&want_out);
        assert!(!want.is_empty());
        let rows: Vec<Vec<Value>> = all[0].1.iter().filter(|r| r[3].str() == Some("BTCUSDT")).cloned().collect();
        let text = |k: usize| -> ArrayRef {
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r[k].str().map(str::to_string))))
        };
        let ints = |k: usize| -> ArrayRef { Arc::new(Int64Array::from_iter(rows.iter().map(|r| r[k].i64()))) };
        let floats = |k: usize| -> ArrayRef { Arc::new(Float64Array::from_iter(rows.iter().map(|r| r[k].f64()))) };
        let price: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r[4].f64().map(|p| p.to_string()))));
        let batch = RecordBatch::try_from_iter([
            ("symbol", text(3)),
            ("time", ints(0)),
            ("lts", ints(5)),
            ("id", text(1)),
            ("side", text(6)),
            ("price", price),
            ("amount", floats(7)),
            ("notional", floats(8)),
        ])
        .unwrap();
        std::fs::create_dir_all(dir.join("archive")).unwrap();
        let mut w =
            ArrowWriter::try_new(File::create(dir.join("archive/t_BTCUSDT.parquet")).unwrap(), batch.schema(), None)
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let source = format!("trades={}/archive/t_{{symbol}}.parquet", dir.display());
        let maps = |extra: &str| -> Vec<String> {
            [
                "local_timestamp=lts",
                "quantity=amount",
                // the text price, read as a Float64: `price * 0` is 0.0
                "amount=notional + price * 0",
                &format!("exchange={}", rows[0][2].i64().unwrap()),
                extra,
            ]
            .iter()
            .filter(|m| !m.is_empty())
            .map(|m| format!("trades.{m}"))
            .collect()
        };
        let got_out = dir.join("got");
        let mut a = args(sql, std::slice::from_ref(&source), &got_out);
        a.symbols = vec!["BTCUSDT".into()];
        a.maps = maps("");
        run(a).unwrap();
        assert_agree(&normal(written(&got_out).0), &normal(want), "the archive's layout");
        // a column the stream does not have
        let mut a = args(sql, &[source], &dir.join("none"));
        a.symbols = vec!["BTCUSDT".into()];
        a.maps = maps("notional=1");
        let e = run(a).unwrap_err().to_string();
        assert!(e.contains("trades has no column notional"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Each column type as the Parquet type it is written as, its values kept.
    #[test]
    fn every_column_type_is_written_as_its_parquet_type() {
        let dir = scratch("types");
        let path = dir.join("t.parquet");
        let cols = [
            (0, "f32".to_string(), Type::F32),
            (1, "f64".to_string(), Type::Nullable(Box::new(Type::F64))),
            (2, "i32".to_string(), Type::Int(32)),
            (3, "i64".to_string(), Type::Int(64)),
            (4, "u64".to_string(), Type::UInt(64)),
            (5, "t".to_string(), Type::Time(6)),
            (6, "b".to_string(), Type::Bool),
            (7, "s".to_string(), Type::Str),
        ];
        let n = 50i64;
        let values: Vec<Vec<Value>> = vec![
            (0..n).map(|i| Value::F64(i as f64 / 4.0)).collect(),
            (0..n).map(|i| if i % 7 == 0 { Value::Null } else { Value::F64(i as f64 * 1.5) }).collect(),
            (0..n).map(|i| Value::Int(-i)).collect(),
            (0..n).map(|i| Value::Int(i << 40)).collect(),
            (0..n).map(|i| Value::UInt(i as u64 * 3)).collect(),
            (0..n).map(|i| Value::Int(1_789_000_000_000_000 + i)).collect(),
            (0..n).map(|i| Value::Bool(i % 3 == 0)).collect(),
            (0..n).map(|i| Value::Str(format!("s{}", i % 4).into())).collect(),
        ];
        let mut builders: Vec<Builder> = cols.iter().map(|(_, _, ty)| Builder::of(ty)).collect();
        for (b, v) in builders.iter_mut().zip(&values) {
            v.iter().for_each(|x| b.push(x));
        }
        write_parquet(&path, &cols, &mut builders, &[]).unwrap();
        let r = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let b = r.build().unwrap().next().unwrap().unwrap();
        use arrow_schema::{DataType as D, TimeUnit};
        let types: Vec<D> = b.schema().fields().iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(
            types,
            [
                D::Float32,
                D::Float64,
                D::Int32,
                D::Int64,
                D::UInt64,
                D::Timestamp(TimeUnit::Microsecond, None),
                D::Boolean,
                D::Utf8
            ]
        );
        use arrow_array::cast::AsArray;
        use arrow_array::types::{
            Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType, UInt64Type,
        };
        let r = 0..n;
        assert_eq!(
            b.column(0).as_primitive::<Float32Type>().values().to_vec(),
            r.clone().map(|i| i as f32 / 4.0).collect::<Vec<_>>()
        );
        let f64s: Vec<Option<f64>> = b.column(1).as_primitive::<Float64Type>().iter().collect();
        assert_eq!(f64s, r.clone().map(|i| (i % 7 != 0).then_some(i as f64 * 1.5)).collect::<Vec<_>>());
        assert_eq!(
            b.column(2).as_primitive::<Int32Type>().values().to_vec(),
            r.clone().map(|i| -i as i32).collect::<Vec<_>>()
        );
        assert_eq!(
            b.column(3).as_primitive::<Int64Type>().values().to_vec(),
            r.clone().map(|i| i << 40).collect::<Vec<_>>()
        );
        assert_eq!(
            b.column(4).as_primitive::<UInt64Type>().values().to_vec(),
            r.clone().map(|i| i as u64 * 3).collect::<Vec<_>>()
        );
        assert_eq!(
            b.column(5).as_primitive::<TimestampMicrosecondType>().values().to_vec(),
            r.clone().map(|i| 1_789_000_000_000_000 + i).collect::<Vec<_>>()
        );
        let bools: Vec<Option<bool>> = b.column(6).as_boolean().iter().collect();
        assert_eq!(bools, r.clone().map(|i| Some(i % 3 == 0)).collect::<Vec<_>>());
        let strs: Vec<Option<&str>> = b.column(7).as_string::<i32>().iter().collect();
        let want: Vec<String> = r.map(|i| format!("s{}", i % 4)).collect();
        assert_eq!(strs, want.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Of a column's candidate encodings, the smallest is taken: a dictionary for few prices, the
    /// byte split for many distinct volumes, each smaller than the first candidate.
    #[test]
    fn a_column_is_written_in_its_smallest_encoding() {
        use parquet::basic::Encoding as E;
        let floats = [Some(E::PLAIN), Some(E::BYTE_STREAM_SPLIT), None];
        // 16 prices in no order (a linear congruential sequence)
        let mut x = 1u64;
        let few: ArrayRef = Arc::new(Float64Array::from_iter_values((0..20_000).map(|_| {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            100.0 + (x >> 60) as f64 * 0.25
        })));
        let many: ArrayRef =
            Arc::new(Float64Array::from_iter_values((0..20_000).map(|i| 1000.0 + (i as f64 * 0.37).sin() * 3.0)));
        for (a, want) in [(&few, None), (&many, Some(E::BYTE_STREAM_SPLIT))] {
            let sizes: Vec<usize> = floats.iter().map(|c| column_bytes("x", a, false, *c).unwrap()).collect();
            let got = smallest("x", a, false, &floats).unwrap();
            assert_eq!(got, want, "{sizes:?}");
            let k = floats.iter().position(|c| *c == want).unwrap();
            assert!(sizes.iter().enumerate().all(|(j, s)| j == k || sizes[k] < *s), "{sizes:?}");
        }
    }

    /// What a file's symbol is: its name's with --symbol-from-path, else its column's.
    #[test]
    fn symbol_from_path_takes_the_files_name_for_the_symbol_column() {
        let sql = "fixtures/pipelines/bars.sql";
        let dir = scratch("from-path");
        let cat = brrrrr_core::sql::parse(&std::fs::read_to_string(root(sql)).unwrap()).unwrap();
        let mut rows = rows(&cat, "trades");
        rows.retain(|r| r[3].str() == Some("BTCUSDT"));
        let day = brrrrr_core::expr::format_datetime(rows[0][5].i64().unwrap(), "%Y-%m-%d");
        std::fs::create_dir_all(dir.join(&day)).unwrap();
        write(&dir.join(&day).join("trades_XYZUSDT.parquet"), &cat.streams["trades"], &rows, 500);
        for (from_path, want) in [(true, "XYZUSDT"), (false, "BTCUSDT")] {
            let out = dir.join(format!("out-{from_path}"));
            let mut a = args(sql, &[format!("trades={}/{{day}}/trades_{{symbol}}.parquet", dir.display())], &out);
            a.symbol_from_path = from_path;
            run(a).unwrap();
            let (msgs, _) = written(&out);
            assert!(!msgs.is_empty());
            for m in &msgs {
                let (_, payload) = m.split_once(' ').unwrap();
                assert_eq!(serde_json::from_str::<J>(payload).unwrap()["symbol"], want, "{m}");
            }
        }
        // a day past the range is not read: a symbol whose only file is there has none
        let mut a = args(
            sql,
            &[format!("trades={}/{{day}}/trades_{{symbol}}.parquet", dir.display())],
            &dir.join("out-before"),
        );
        let before = brrrrr_core::value::parse_datetime(&day).unwrap() - DAY;
        (a.from, a.to) = (brrrrr_core::expr::format_datetime(before, "%Y-%m-%d"), day.clone());
        a.symbols = vec!["XYZUSDT".into()];
        let e = format!("{:#}", run(a).unwrap_err());
        assert!(e.contains("no file of any source"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A line per message (topic, headers, payload), counted; a write error is returned by
    /// `finish`.
    #[test]
    fn lines_are_json_and_a_write_error_is_returned() {
        let dir = scratch("lines");
        let emit = |payload: &str| Emit {
            topic: "t.1m".into(),
            payload: payload.into(),
            headers: vec![("k".into(), "v".into()), ("dedup".into(), "a|b".into())],
            window_end: 0,
        };
        let mut l = Lines::new(File::create(dir.join("x.jsonl")).unwrap());
        l.push(emit("{\"a\":1}\n"));
        l.push(emit("{\"a\":2}"));
        l.finish().unwrap();
        assert_eq!(l.messages, 2);
        let text = std::fs::read_to_string(dir.join("x.jsonl")).unwrap();
        assert_eq!(
            text.lines().next().unwrap(),
            r#"{"topic":"t.1m","headers":[["k","v"],["dedup","a|b"]],"payload":{"a":1}}"#
        );
        assert_eq!(text.lines().count(), 2);
        // a full disk: more than the buffer holds fails at a push, less at the flush
        for n in [1, 20_000] {
            let mut l = Lines::new(File::options().write(true).open("/dev/full").unwrap());
            for _ in 0..n {
                l.push(emit(&format!("{{\"pad\":\"{}\"}}", "x".repeat(100))));
            }
            assert!(l.finish().is_err(), "{n} messages");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn peak_memory_is_the_processs() {
        // more than a MiB, in bytes: the test binary alone is larger
        let rss = peak_rss();
        assert!(rss > 1 << 20 && rss.is_multiple_of(1024), "{rss}");
    }

    /// Arrow arrays as columns: a column of one value a constant, NULLs kept, a sliced string
    /// array's own strings.
    #[test]
    fn arrow_arrays_become_columns_of_their_values() {
        let int = Type::Int(64);
        let c = column(&(Arc::new(Int64Array::from(vec![5, 5, 5])) as ArrayRef), &int).unwrap();
        assert!(matches!(c.data, Data::Const(Value::Int(5), 3)), "{c:?}");
        let c = column(&(Arc::new(Int64Array::from(vec![5, 5, 5])) as ArrayRef), &Type::Time(6)).unwrap();
        assert!(matches!(c.data, Data::Const(Value::Time(5), 3)), "{c:?}");
        let c = column(&(Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef), &int).unwrap();
        assert!(matches!(&c.data, Data::Int(v) if v.is_empty()), "{c:?}");
        let c = column(&(Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])) as ArrayRef), &int).unwrap();
        assert_eq!((0..3).map(|r| c.get(r)).collect::<Vec<_>>(), [Value::Int(1), Value::Null, Value::Int(1)]);
        let f = column(&(Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef), &Type::F64).unwrap();
        assert_eq!(f.get(1), Value::F64(2.5));
        let s: ArrayRef = Arc::new(StringArray::from(vec!["x", "ab", "ab", "cd"]));
        let c = column(&s.slice(1, 2), &Type::Str).unwrap();
        assert!(matches!(&c.data, Data::Const(Value::Str(v), 2) if &**v == "ab"), "{c:?}");
        let c = column(&s.slice(2, 2), &Type::Str).unwrap();
        assert_eq!((0..2).map(|r| c.get(r)).collect::<Vec<_>>(), [Value::Str("ab".into()), Value::Str("cd".into())]);
        let c =
            column(&(Arc::new(StringArray::from(vec![Some("a"), None, Some("a")])) as ArrayRef), &Type::Str).unwrap();
        assert_eq!(c.get(1), Value::Null);
        assert!(column(&s, &int).is_err());
        // text as Float64, as `to_float64` parses it: plain, or a dictionary's values once each
        let words = column(&s, &Type::F64).unwrap();
        assert_eq!(words.get(0), Value::Str("x".into()).cast(&Type::F64));
        let prices: ArrayRef = Arc::new(StringArray::from(vec![Some("0.5"), None, Some("1.25")]));
        let c = column(&prices, &Type::F64).unwrap();
        assert_eq!((0..3).map(|r| c.get(r)).collect::<Vec<_>>(), [Value::F64(0.5), Value::Null, Value::F64(1.25)]);
        let dict: ArrayRef = Arc::new(
            vec![Some("0.5"), Some("2"), None, Some("0.5")]
                .into_iter()
                .collect::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>(),
        );
        let c = column(&dict, &Type::F64).unwrap();
        assert_eq!(
            (0..4).map(|r| c.get(r)).collect::<Vec<_>>(),
            [Value::F64(0.5), Value::F64(2.0), Value::Null, Value::F64(0.5)]
        );
        assert!(column(&(Arc::new(Int64Array::from(vec![1])) as ArrayRef), &Type::F64).is_err());
    }

    #[test]
    fn json_strings_are_escaped() {
        assert_eq!(json_str(" a"), "\" a\"");
        assert_eq!(json_str("a\"b\\c\nd\te\r\u{1}"), "\"a\\\"b\\\\c\\nd\\te\\r\\u0001\"");
    }
}
