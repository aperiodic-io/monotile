//! `brrrrr run`'s Parquet streams: `CREATE EXTERNAL STREAM s (...) SETTINGS type = 'file',
//! data_format = 'Parquet', path = '<directory, glob or URL>'`, a source if a view reads it, a
//! sink if a view writes it (`engine::file_settings` checks the settings).
//!
//! A source reads the Parquet files under its path in name order, a batch at a time, and goes
//! on reading the files that appear after the last one (named later: a time or a sequence in
//! their names), listed every `LIST_EVERY`. Its position in a checkpoint is its file and the
//! rows read of it, `(<stream>#<file>, 0, rows)`: on restore, the files named before that one
//! are read, and that one from that row. Several sources are merged in time order by their
//! `time_column`: a chunk of one source's rows goes in only once no other source has an
//! earlier row waiting (a source with nothing waiting holds none back).
//!
//! A sink writes the rows of each checkpoint interval to files of their own, one per Hive
//! directory of its `partition_by` columns (whose values the directory names and the files do
//! not hold): `<path>/<key>=<value>/<pipeline>.<epoch>.<token>.parquet`, row groups encoded as
//! the rows come. At a checkpoint's cut the files are closed, and before the checkpoint is
//! written (on its thread) moved or uploaded into place: a checkpoint is only there once the
//! files of every row before its cut are. A file of an epoch whose checkpoint was never
//! written holds rows the restart produces again from the checkpoint before: on restore, every
//! file of this pipeline's of a later epoch than the restored one is deleted (`Sinks::clean`).
//! So each row is in the files exactly once, whatever crashes; between a crash and the restart
//! a reader may see the rows of an epoch that is then deleted and written again. A start
//! without a checkpoint deletes nothing: what its replay produces again is written again.
use anyhow::{bail, Result};
use brrrrr_core::column::Batch;
use brrrrr_core::engine::Source;
use brrrrr_core::sql::{Catalog, Kind, Stream};
use brrrrr_core::value::{Type, Value};
use brrrrr_lake::files::{self, File, Files};
use brrrrr_lake::read::Reader;
use brrrrr_lake::write::{self, ResultWriter};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How often a source read to the end of its files lists them again for new ones.
// ponytail: a listing every second; a store's notifications if a prefix of many objects costs too much
pub const LIST_EVERY: Duration = Duration::from_secs(1);

/// Rows a sink holds for a partition before they go to its writer as a batch.
const BATCH: usize = 8192;

/// Rows held at most by a sink's open files for their next row groups (as `COPY`'s): past it,
/// the file holding the most writes a row group.
const HELD: usize = 1 << 21;

type Row = Vec<Value>;

/// This build reads and writes files (`parquet_stub.rs`'s refuses a pipeline with file streams).
pub fn supported(_: &Catalog) -> Result<()> {
    Ok(())
}

/// Whether `s` is a file stream (`type = 'file'`).
pub fn is_file(s: &Stream) -> bool {
    s.settings.get("type").is_some_and(|t| t == "file")
}

/// The file streams of `cat` a view writes (sinks) or reads (sources).
fn file_streams(cat: &Catalog, sinks: bool) -> Vec<&Stream> {
    let written = |s: &&Stream| cat.views.iter().any(|v| v.target == s.name);
    cat.streams.values().filter(|s| s.kind == Kind::External && is_file(s) && written(s) == sinks).collect()
}

/// A source's position in a checkpoint: its file, as the topic of partition 0.
pub fn topic(stream: &str, file: &str) -> String {
    format!("{stream}#{file}")
}

/// The file and rows read of it where `stream` was, among a checkpoint's source positions: the
/// last file named (positions of earlier ones are dropped as a source moves on, `prune`).
fn resume(stream: &str, positions: &[(String, i32, i64)]) -> Option<(String, i64)> {
    let prefix = format!("{stream}#");
    positions.iter().filter_map(|(t, _, o)| Some((t.strip_prefix(&prefix)?.to_string(), *o))).max()
}

/// Drops the positions of `stream`'s files before `file`: a source's position is its last.
pub fn prune(positions: &mut HashMap<(String, i32), i64>, stream: &str, file: &str) {
    let prefix = format!("{stream}#");
    positions.retain(|(t, _), _| t.strip_prefix(&prefix).is_none_or(|f| f >= file));
}

/// A row's time for merging sources: its time column's µs (or integer); NULL first.
fn time_of(row: &[Value], col: usize) -> i64 {
    match &row[col] {
        Value::Time(t) | Value::Int(t) => *t,
        _ => i64::MIN,
    }
}

/// Of the sources' first waiting rows' times (None: nothing waiting), the one to take a chunk
/// from (the earliest, the first of equals) and how late its rows may be for this chunk: no
/// later than the next earliest source's first row (None: no other waits).
fn pick(heads: &[Option<i64>]) -> Option<(usize, Option<i64>)> {
    let (i, _) = heads.iter().enumerate().filter_map(|(i, h)| Some((i, (*h)?))).min_by_key(|(_, t)| *t)?;
    let bound = heads.iter().enumerate().filter(|(j, _)| *j != i).filter_map(|(_, h)| *h).min();
    Some((i, bound))
}

/// Rows of one source's file for the engine, and its position after them: (`topic`, 0,
/// `offset`).
#[derive(Debug, PartialEq)]
pub struct Chunk {
    pub stream: String,
    pub file: String,
    pub topic: String,
    pub offset: i64,
    pub rows: Vec<Row>,
}

/// The files of every file stream: one cache and the object stores' clients, shared.
fn shared() -> Arc<Files> {
    static FILES: std::sync::OnceLock<Arc<Files>> = std::sync::OnceLock::new();
    FILES.get_or_init(|| Arc::new(Files::default())).clone()
}

/// The file sources of a pipeline.
pub struct Sources {
    files: Arc<Files>,
    locs: Vec<Location>,
    every: Duration,
    /// Files that did not read (`errors`); each is read again `every` later.
    errors: u64,
    log: crate::run::Throttle,
}

struct Location {
    stream: String,
    path: String,
    columns: Vec<(String, Type)>,
    time: Option<usize>,
    /// The file read or last read, and the rows of it given out.
    at: Option<(String, i64)>,
    /// Whether `at`'s file is read whole.
    done: bool,
    /// `at`'s file and its reader while it is read; rows of it read and not given out yet, and
    /// how many of its next ones to skip (given out before a restart, or before it failed).
    reading: Option<(File, Reader)>,
    rows: VecDeque<Row>,
    skip: i64,
    /// Files to read next, in name order, and every name listed.
    queue: VecDeque<File>,
    known: HashSet<String>,
    listed: Option<Instant>,
    /// After a file did not read, when to try it again.
    retry: Option<Instant>,
    /// The last file there at start: once it is read, everything there at start is.
    first: Option<String>,
}

impl Sources {
    /// The file sources of `cat`, resuming at `positions` (a checkpoint's), their files listed:
    /// `None` if there are none.
    pub fn open(cat: &Catalog, positions: &[(String, i32, i64)]) -> Result<Option<Sources>> {
        Sources::listing_every(cat, positions, shared(), LIST_EVERY)
    }

    fn listing_every(
        cat: &Catalog,
        positions: &[(String, i32, i64)],
        files: Arc<Files>,
        every: Duration,
    ) -> Result<Option<Sources>> {
        let streams = file_streams(cat, false);
        if streams.is_empty() {
            return Ok(None);
        }
        let locs = streams
            .into_iter()
            .map(|s| {
                let at = resume(&s.name, positions);
                Location {
                    stream: s.name.clone(),
                    path: s.settings["path"].trim().to_string(),
                    columns: s.columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect(),
                    time: s.settings.get("time_column").and_then(|c| s.columns.iter().position(|x| x.name == *c)),
                    skip: at.as_ref().map_or(0, |a| a.1),
                    at,
                    done: false,
                    reading: None,
                    rows: VecDeque::new(),
                    queue: VecDeque::new(),
                    known: HashSet::new(),
                    listed: None,
                    retry: None,
                    first: None,
                }
            })
            .collect();
        let mut s = Sources { files, locs, every, errors: 0, log: crate::run::Throttle::new() };
        for i in 0..s.locs.len() {
            s.list(i, Instant::now(), true)?;
        }
        Ok(Some(s))
    }

    /// Lists location `i`'s files and queues those named after the last one queued or read. At
    /// `start`, the file of its position is queued again (to read on from its row), and the
    /// last file there is noted. A file that appears named before what was read is not read:
    /// said, at most once a second.
    fn list(&mut self, i: usize, now: Instant, start: bool) -> Result<()> {
        let loc = &mut self.locs[i];
        loc.listed = Some(now);
        let missing = local(&loc.path).is_some_and(|p| !files::has_glob(p) && !std::path::Path::new(p).exists());
        let found = if missing {
            vec![] // no directory yet: no files yet
        } else {
            // by their names: a file of another kind there is not one of the source's
            match self.files.list(&loc.path, None) {
                Ok(found) => found.into_iter().filter(|f| f.format == files::Format::Parquet).collect(),
                // an empty directory or prefix: no files yet
                Err(e) if format!("{e}").starts_with("no file") => vec![],
                Err(e) => return Err(e),
            }
        };
        if start {
            loc.first = found.last().map(|f| f.name.clone());
        }
        let after = loc.queue.back().map(|f| f.name.clone()).or_else(|| loc.at.as_ref().map(|a| a.0.clone()));
        for f in found {
            if !loc.known.insert(f.name.clone()) {
                continue;
            }
            // named after: new (a name listed before is `known`), or at start the file of its
            // position, read on from its row
            if after.as_ref().is_none_or(|a| f.name >= *a) {
                loc.queue.push_back(f);
            } else if !start {
                if let Some(n) = self.log.allow(now) {
                    eprintln!(
                        "{}: {} appeared after {} was read and is named before it: not read (a source's files are \
                         read in name order){}",
                        loc.stream,
                        f.name,
                        after.as_deref().unwrap_or(""),
                        crate::run::left_out(n)
                    );
                }
            }
        }
        Ok(())
    }

    /// Fills location `i`'s rows from its file, or the next: whether it has rows waiting. A file
    /// that does not read (or a listing that fails) is counted, said, and read again from its
    /// row `every` later.
    fn fill(&mut self, i: usize, now: Instant) -> bool {
        loop {
            let loc = &mut self.locs[i];
            if !loc.rows.is_empty() {
                return true;
            }
            if loc.retry.is_some_and(|t| now < t) {
                return false;
            }
            if let Some((_, r)) = loc.reading.as_mut() {
                match r.next() {
                    Some(Ok(b)) => {
                        let skip = loc.skip.min(b.len as i64);
                        loc.skip -= skip;
                        loc.rows.extend(b.rows().into_iter().skip(skip as usize));
                    }
                    Some(Err(e)) => {
                        let (f, _) = loc.reading.take().expect("read");
                        loc.skip = loc.at.as_ref().map_or(0, |a| a.1);
                        loc.queue.push_front(f);
                        self.failed(i, now, &e);
                        return false;
                    }
                    None => (loc.reading, loc.done) = (None, true),
                }
                continue;
            }
            if loc.queue.is_empty() {
                if loc.listed.is_some_and(|t| now.saturating_duration_since(t) < self.every) {
                    return false;
                }
                if let Err(e) = self.list(i, now, false) {
                    let e = format!("listing {}: {e:#}", self.locs[i].path);
                    self.failed(i, now, &e);
                    return false;
                }
            }
            let loc = &mut self.locs[i];
            let Some(mut f) = loc.queue.pop_front() else { return false };
            if loc.at.as_ref().is_none_or(|a| a.0 != f.name) {
                (loc.at, loc.skip) = (Some((f.name.clone(), 0)), 0);
            }
            loc.done = false;
            // ponytail: a store's file is fetched (or its row groups, past 16 MiB, as they are read)
            // on the data thread, between batches; a read-ahead thread if that stalls a pipeline
            match self.files.fetch(std::slice::from_mut(&mut f)) {
                Ok(()) => {
                    let all = vec![true; loc.columns.len()];
                    let r = Reader::new(vec![f.clone()], loc.columns.clone(), all);
                    loc.reading = Some((f, r));
                }
                Err(e) => {
                    loc.queue.push_front(f);
                    self.failed(i, now, &format!("{e:#}"));
                    return false;
                }
            }
        }
    }

    /// Counts and says (at most once a second) why location `i` read nothing, and has it try
    /// again `every` later.
    fn failed(&mut self, i: usize, now: Instant, e: &str) {
        self.errors += 1;
        self.locs[i].retry = Some(now + self.every);
        if let Some(n) = self.log.allow(now) {
            eprintln!("{}: {e}; trying again in {:?}{}", self.locs[i].stream, self.every, crate::run::left_out(n));
        }
    }

    /// The next rows for the engine, at most `most`, of one file of one source: `None` while
    /// no source has rows waiting.
    pub fn next(&mut self, most: usize, now: Instant) -> Option<Chunk> {
        let heads: Vec<Option<i64>> = (0..self.locs.len())
            .map(|i| {
                let waiting = self.fill(i, now);
                let loc = &self.locs[i];
                waiting.then(|| loc.time.map_or(i64::MIN, |c| time_of(&loc.rows[0], c)))
            })
            .collect();
        let (i, bound) = pick(&heads)?;
        let loc = &mut self.locs[i];
        let mut rows = Vec::with_capacity(most.min(loc.rows.len()));
        while rows.len() < most {
            let Some(r) = loc.rows.front() else { break };
            if !rows.is_empty() && loc.time.zip(bound).is_some_and(|(c, b)| time_of(r, c) > b) {
                break;
            }
            rows.push(loc.rows.pop_front().expect("a row"));
        }
        let at = loc.at.as_mut().expect("rows of a file");
        at.1 += rows.len() as i64;
        Some(Chunk {
            stream: loc.stream.clone(),
            file: at.0.clone(),
            topic: topic(&loc.stream, &at.0),
            offset: at.1,
            rows,
        })
    }

    /// The files that did not read since the last call (`brrrrr_decode_errors_total`).
    pub fn errors(&mut self) -> u64 {
        std::mem::take(&mut self.errors)
    }

    /// Whether every source has read every file listed to its end (`--idle-close`).
    pub fn caught_up(&self) -> bool {
        self.locs
            .iter()
            .all(|l| l.rows.is_empty() && l.reading.is_none() && l.queue.is_empty() && (l.done || l.at.is_none()))
    }

    /// Whether every source has read every file there at start: the replay of what an earlier
    /// run wrote is over.
    pub fn past_start(&self) -> bool {
        self.locs.iter().all(|l| match (&l.first, &l.at) {
            (None, _) => true,
            (Some(_), None) => false,
            // read whole: `done` comes once its reader has no more and every row went out
            (Some(first), Some((at, _))) => at > first || at == first && l.done,
        })
    }
}

/// A local destination's path; None for a store's.
fn local(dest: &str) -> Option<&str> {
    (!files::is_url(dest) || dest.starts_with("file://")).then(|| dest.strip_prefix("file://").unwrap_or(dest))
}

/// The Parquet column kind of a stream column's type.
fn kind(t: &Type) -> write::Kind {
    match t.base() {
        Type::Bool => write::Kind::Bool,
        Type::Int(_) => write::Kind::Int,
        Type::UInt(_) => write::Kind::UInt,
        Type::F32 | Type::F64 => write::Kind::Float,
        Type::Time(_) => write::Kind::Time,
        _ => write::Kind::Text,
    }
}

/// A pipeline's name as its sinks' files carry it: letters, digits, `_` and `-`.
fn owner_of(pipeline: &str) -> String {
    pipeline.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' }).collect()
}

/// A sink file's name: `<owner>.<epoch:020>.<token>.parquet`.
fn file_name(owner: &str, epoch: u64, token: &str) -> String {
    format!("{owner}.{epoch:020}.{token}.parquet")
}

/// The owner and epoch of a sink file's name (`file_name`); None for any other name.
fn parse_name(name: &str) -> Option<(&str, u64)> {
    let mut parts = name.strip_suffix(".parquet")?.splitn(3, '.');
    let (owner, epoch, token) = (parts.next()?, parts.next()?, parts.next()?);
    let digits = epoch.len() == 20 && epoch.bytes().all(|b| b.is_ascii_digit());
    (digits && !token.is_empty() && !token.contains('.')).then(|| Some((owner, epoch.parse().ok()?)))?
}

/// The file sinks of a pipeline: each one's files of the rows since the last cut.
pub struct Sinks {
    files: Arc<Files>,
    owner: String,
    /// This process's, in its files' names: no two instances name a file alike.
    token: String,
    made: u64,
    sinks: BTreeMap<String, Sink>,
    failed: Option<anyhow::Error>,
    /// `HELD`.
    held: usize,
}

struct Sink {
    root: String,
    /// The partition columns and their names; the columns written, their names and kinds.
    keys: Vec<(usize, String)>,
    kept: Vec<usize>,
    columns: Vec<String>,
    kinds: Vec<write::Kind>,
    /// Open files by partition directory ("" without partitions).
    open: BTreeMap<String, Open>,
}

/// A sink file being written: under a hidden name in its directory (for a store's, in a
/// temporary directory) until it is committed. Dropped uncommitted, it is removed.
struct Open {
    rows: Vec<Row>,
    writer: Option<ResultWriter<std::io::BufWriter<std::fs::File>>>,
    tmp: PathBuf,
    /// Where it goes: its directory, the sink's root and the partition's; its number in this
    /// process (a retried checkpoint commits files of the same epoch).
    dir: String,
    id: u64,
}

impl Drop for Open {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.tmp);
    }
}

impl Open {
    /// The rows held go to the writer.
    fn flush(&mut self, width: usize) -> Result<()> {
        if !self.rows.is_empty() {
            let b = Batch::from_rows(&std::mem::take(&mut self.rows), width);
            self.writer.as_mut().expect("writer").push(&b)?;
        }
        Ok(())
    }

    fn held(&self) -> usize {
        self.rows.len() + self.writer.as_ref().map_or(0, |w| w.pending())
    }
}

/// The files of the sinks' rows up to a checkpoint's cut, to commit before it is written.
pub struct Cut {
    files: Arc<Files>,
    owner: String,
    token: String,
    open: Vec<Open>,
}

impl Sinks {
    /// The file sinks of `cat`, their files named for `pipeline`: `None` if there are none.
    pub fn open(cat: &Catalog, pipeline: &str) -> Option<Sinks> {
        Sinks::with_files(cat, pipeline, shared())
    }

    fn with_files(cat: &Catalog, pipeline: &str, files: Arc<Files>) -> Option<Sinks> {
        let streams = file_streams(cat, true);
        if streams.is_empty() {
            return None;
        }
        let mut sinks = BTreeMap::new();
        for s in streams {
            let by: Vec<&str> =
                s.settings.get("partition_by").map_or(vec![], |b| b.split(',').map(str::trim).collect());
            let keys: Vec<(usize, String)> = (s.columns.iter().enumerate())
                .filter(|(_, c)| by.contains(&c.name.as_str()))
                .map(|(i, c)| (i, c.name.clone()))
                .collect();
            let kept: Vec<usize> = (0..s.columns.len()).filter(|i| !keys.iter().any(|k| k.0 == *i)).collect();
            let sink = Sink {
                root: s.settings["path"].trim().trim_end_matches('/').to_string(),
                columns: kept.iter().map(|&i| s.columns[i].name.clone()).collect(),
                kinds: kept.iter().map(|&i| kind(&s.columns[i].ty)).collect(),
                keys,
                kept,
                open: BTreeMap::new(),
            };
            sinks.insert(s.name.clone(), sink);
        }
        let token = crate::store::unique();
        Some(Sinks { files, owner: owner_of(pipeline), token, made: 0, sinks, failed: None, held: HELD })
    }

    /// Takes a row of `sink` in its stream's columns: false if it is no file sink. A failed
    /// write is kept for `result`, and nothing more is taken.
    pub fn row(&mut self, sink: &str, row: &[Value]) -> bool {
        if !self.sinks.contains_key(sink) {
            return false;
        }
        if self.failed.is_none() {
            if let Err(e) = self.write(sink, row) {
                self.failed = Some(e.context(format!("writing {sink}")));
            }
        }
        true
    }

    fn write(&mut self, sink: &str, row: &[Value]) -> Result<()> {
        let s = self.sinks.get_mut(sink).expect("a file sink");
        let remote = local(&s.root).is_none();
        let dir: Vec<String> = (s.keys.iter())
            .map(|(k, name)| {
                let v = &row[*k];
                let d = files::partition_dir(name, (!v.is_null()).then(|| brrrrr_core::query::text(v)).as_deref());
                // a store's URL is unescaped once (`%2F` would be `/`): the escapes stay in its key
                if remote {
                    d.replace('%', "%25")
                } else {
                    d
                }
            })
            .collect();
        let dir = dir.join("/");
        if !s.open.contains_key(&dir) {
            let path = match dir.as_str() {
                "" => s.root.clone(),
                d => format!("{}/{d}", s.root),
            };
            self.made += 1;
            let hidden = format!(".{}.{}-{}.tmp", self.owner, self.token, self.made);
            let tmp = match local(&path) {
                Some(p) => {
                    std::fs::create_dir_all(p).map_err(|e| anyhow::anyhow!("{p}: {e}"))?;
                    PathBuf::from(p).join(hidden)
                }
                None => std::env::temp_dir().join(hidden),
            };
            let f = std::fs::File::create(&tmp).map_err(|e| anyhow::anyhow!("{}: {e}", tmp.display()))?;
            let writer =
                ResultWriter::new(std::io::BufWriter::new(f), write::Out::Parquet, &s.columns, Some(s.kinds.clone()))?;
            s.open.insert(dir.clone(), Open { rows: vec![], writer: Some(writer), tmp, dir: path, id: self.made });
        }
        let width = s.kept.len();
        let o = s.open.get_mut(&dir).expect("open");
        o.rows.push(s.kept.iter().map(|&i| row[i].clone()).collect());
        if o.rows.len() >= BATCH {
            o.flush(width)?;
        }
        // past `HELD` rows held by the open files, the most held written as a row group
        while s.open.values().map(Open::held).sum::<usize>() > self.held {
            let o = s.open.values_mut().max_by_key(|o| o.held()).expect("open");
            o.flush(width)?;
            o.writer.as_mut().expect("writer").row_group()?;
        }
        Ok(())
    }

    /// The error a write met, once: the instance stops on it (its restart replays).
    pub fn result(&mut self) -> Result<()> {
        self.failed.take().map_or(Ok(()), Err)
    }

    /// The files of every row taken since the last cut, for the checkpoint being cut to commit
    /// before it is written. The next rows go to new files.
    pub fn cut(&mut self) -> Result<Cut> {
        self.result()?;
        let mut open = vec![];
        for s in self.sinks.values_mut() {
            for (_, mut o) in std::mem::take(&mut s.open) {
                o.flush(s.kept.len())?;
                open.push(o);
            }
        }
        Ok(Cut { files: self.files.clone(), owner: self.owner.clone(), token: self.token.clone(), open })
    }

    /// Deletes this pipeline's files of epochs after `restored` (the checkpoint a restart
    /// restored): they hold rows the restart produces again. Also the hidden files a crash left.
    /// The files deleted.
    pub fn clean(&self, restored: u64) -> Result<Vec<String>> {
        let mut deleted = vec![];
        for s in self.sinks.values() {
            for (name, _) in self.files.names(&s.root, false)? {
                let base = name.rsplit('/').next().unwrap_or(&name);
                let ours = parse_name(base).is_some_and(|(o, e)| o == self.owner && e > restored)
                    || base.starts_with(&format!(".{}.", self.owner)) && base.ends_with(".tmp");
                if ours {
                    self.files.delete(&name)?;
                    deleted.push(name);
                }
            }
        }
        Ok(deleted)
    }
}

impl Cut {
    /// Moves or uploads the files into place as epoch `epoch`'s, each only before `deadline`:
    /// past it another instance may have taken the pipeline over and deleted this epoch's files
    /// as uncommitted, and one put now would stay. The files' names.
    // ponytail: an upload begun before the deadline may land after it (as a Kafka message in
    // flight may); per-request timeouts bound to the deadline if that window matters
    pub fn commit(self, epoch: u64, deadline: Instant) -> Result<Vec<String>> {
        let mut names = vec![];
        for mut o in self.open {
            if Instant::now() >= deadline {
                bail!("the lease ran out before checkpoint {epoch}'s files were written: restarting replays them");
            }
            let f = o.writer.take().expect("writer").finish()?;
            f.into_inner().map_err(|e| e.into_error())?.sync_all()?;
            let name = format!("{}/{}", o.dir, file_name(&self.owner, epoch, &format!("{}-{}", self.token, o.id)));
            match local(&name) {
                Some(to) => std::fs::rename(&o.tmp, to).map_err(|e| anyhow::anyhow!("{to}: {e}"))?,
                None => self.files.upload(&o.tmp, &name)?,
            }
            names.push(name);
        }
        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// A Parquet file of `rows` in columns `cols` (name, type).
    pub(crate) fn parquet_file(path: &Path, cols: &[(&str, &str)], rows: &[Row]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let columns: Vec<String> = cols.iter().map(|c| c.0.to_string()).collect();
        let kinds = cols.iter().map(|c| kind(&Type::parse(c.1).unwrap())).collect();
        let f = std::fs::File::create(path).unwrap();
        let mut w = ResultWriter::new(std::io::BufWriter::new(f), write::Out::Parquet, &columns, Some(kinds)).unwrap();
        w.push(&Batch::from_rows(rows, cols.len())).unwrap();
        w.finish().unwrap();
    }

    /// Every row of the Parquet files under `dir`, in name order, as the columns `cols` (a
    /// partition's column from its directory).
    pub(crate) fn read_back(dir: &str, cols: &[(&str, &str)]) -> Vec<Row> {
        let files = Files::default().list(dir, Some("parquet")).unwrap();
        let cols: Vec<_> = cols.iter().map(|c| (c.0.to_string(), Type::parse(c.1).unwrap())).collect();
        let mut r = Reader::new(files, cols.clone(), vec![true; cols.len()]);
        let mut out = vec![];
        while let Some(b) = r.next() {
            out.extend(b.unwrap().rows());
        }
        out
    }

    const TRADES: [(&str, &str); 3] = [("ts", "datetime64(3)"), ("sym", "string"), ("px", "float64")];

    fn trade(ms: i64, sym: &str, px: f64) -> Row {
        vec![Value::Time(ms * 1000), Value::Str(sym.into()), Value::F64(px)]
    }

    /// A pipeline reading the trades of `sources` (stream, path, time column) into a stream.
    fn reading(sources: &[(&str, &str, Option<&str>)]) -> Catalog {
        let mut sql = String::from("CREATE STREAM o (ts datetime64(3), sym string, px float64);\n");
        for (name, path, time) in sources {
            let time = time.map_or(String::new(), |t| format!(", time_column = '{t}'"));
            sql += &format!(
                "CREATE EXTERNAL STREAM {name} (ts datetime64(3), sym string, px float64) SETTINGS type = 'file', \
                 data_format = 'Parquet', path = '{path}'{time};\nCREATE MATERIALIZED VIEW v_{name} INTO o AS SELECT ts, sym, px FROM {name};\n"
            );
        }
        let cat = brrrrr_core::sql::parse(&sql).unwrap();
        brrrrr_core::engine::Engine::new(&cat).unwrap();
        cat
    }

    fn sources(cat: &Catalog, positions: &[(String, i32, i64)], every: Duration) -> Sources {
        Sources::listing_every(cat, positions, Arc::new(Files::default()), every).unwrap().unwrap()
    }

    /// Every chunk until none is waiting: (stream, file's last name part, offset, rows' ms).
    fn drain(s: &mut Sources, most: usize, now: Instant) -> Vec<(String, String, i64, Vec<i64>)> {
        let mut out = vec![];
        while let Some(c) = s.next(most, now) {
            let ms = c.rows.iter().map(|r| r[0].i64().unwrap() / 1000).collect();
            assert_eq!(c.topic, topic(&c.stream, &c.file));
            out.push((c.stream, c.file.rsplit('/').next().unwrap().to_string(), c.offset, ms));
        }
        out
    }

    fn chunk(stream: &str, file: &str, offset: i64, ms: &[i64]) -> (String, String, i64, Vec<i64>) {
        (stream.into(), file.into(), offset, ms.to_vec())
    }

    #[test]
    fn a_sink_file_name_carries_its_pipeline_and_epoch() {
        let name = file_name("bars", 42, "17-99-0-3");
        assert_eq!(name, "bars.00000000000000000042.17-99-0-3.parquet");
        assert_eq!(parse_name(&name), Some(("bars", 42)));
        assert_eq!(parse_name(&file_name("a-b_c", u64::MAX, "t")), Some(("a-b_c", u64::MAX)));
        for other in [
            "bars.42.t.parquet",                     // not 20 digits
            "bars.0000000000000000004x.t.parquet",   // not digits
            "bars.00000000000000000042..parquet",    // no token
            "bars.00000000000000000042.t.csv",       // not Parquet
            "bars.00000000000000000042.parquet",     // no token at all
            "data_0.parquet",                        // COPY's
            "bars.00000000000000000042.t.u.parquet", // a token of two parts
            "bars.000000000000000000042.t.parquet",  // 21 digits
            "bars.99999999999999999999.t.parquet",   // past u64
        ] {
            assert_eq!(parse_name(other), None, "{other}");
        }
        assert_eq!(owner_of("bars.v1@shadow.{1}/x"), "bars_v1_shadow__1__x");
        assert_eq!(owner_of("trade-size_2"), "trade-size_2");
    }

    #[test]
    fn a_source_resumes_at_its_last_file_and_keeps_only_that_position() {
        let at = |t: &str, o| (t.to_string(), 0, o);
        let positions = vec![at("s#a/1.parquet", 10), at("s#a/2.parquet", 3), at("t#a/9.parquet", 1), at("s2#z", 5)];
        assert_eq!(resume("s", &positions), Some(("a/2.parquet".into(), 3)));
        assert_eq!(resume("t", &positions), Some(("a/9.parquet".into(), 1)));
        assert_eq!(resume("u", &positions), None);
        let mut map: HashMap<(String, i32), i64> = positions.into_iter().map(|(t, p, o)| ((t, p), o)).collect();
        prune(&mut map, "s", "a/2.parquet");
        let mut kept: Vec<_> = map.keys().map(|k| k.0.as_str()).collect();
        kept.sort_unstable();
        assert_eq!(kept, ["s#a/2.parquet", "s2#z", "t#a/9.parquet"]);
        assert_eq!(topic("s", "a/2.parquet"), "s#a/2.parquet");
    }

    #[test]
    fn sources_are_merged_by_their_first_waiting_rows() {
        assert_eq!(pick(&[]), None);
        assert_eq!(pick(&[None, None]), None);
        assert_eq!(pick(&[Some(5)]), Some((0, None)));
        assert_eq!(pick(&[None, Some(5)]), Some((1, None)));
        assert_eq!(pick(&[Some(7), Some(5), Some(6)]), Some((1, Some(6))));
        assert_eq!(pick(&[Some(5), None, Some(9)]), Some((0, Some(9))));
        assert_eq!(pick(&[Some(5), Some(5)]), Some((0, Some(5))), "the first of equals, the other's time its bound");
        assert_eq!(pick(&[Some(i64::MIN), Some(3)]), Some((0, Some(3))));
        let row = |v: Value| vec![Value::Int(0), v];
        assert_eq!(time_of(&row(Value::Time(9)), 1), 9);
        assert_eq!(time_of(&row(Value::Int(-4)), 1), -4);
        assert_eq!(time_of(&row(Value::Null), 1), i64::MIN);
        assert_eq!(time_of(&row(Value::Time(9)), 0), 0);
    }

    #[test]
    fn a_sink_column_is_written_as_its_type() {
        use write::Kind as K;
        let k = |s: &str| kind(&Type::parse(s).unwrap());
        assert_eq!(
            ["bool", "int8", "int64", "uint32", "float32", "float64", "datetime64(3)", "datetime", "string"].map(k),
            [K::Bool, K::Int, K::Int, K::UInt, K::Float, K::Float, K::Time, K::Time, K::Text]
        );
        assert_eq!(k("nullable(float64)"), K::Float);
        assert_eq!(k("low_cardinality(string)"), K::Text);
    }

    #[test]
    fn local_destinations_are_paths_and_file_urls() {
        assert_eq!(local("out/bars"), Some("out/bars"));
        assert_eq!(local("/abs/out"), Some("/abs/out"));
        assert_eq!(local("file:///abs/out"), Some("/abs/out"));
        assert_eq!(local("s3://b/out"), None);
        assert_eq!(local("gs://b/out"), None);
    }

    #[test]
    fn a_source_reads_its_files_in_name_order_a_chunk_at_a_time_and_resumes_where_it_was() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("in");
        parquet_file(&dir.join("2.parquet"), &TRADES, &[trade(20, "A", 1.0), trade(21, "B", 2.0)]);
        parquet_file(&dir.join("1.parquet"), &TRADES, &[trade(10, "A", 1.0), trade(11, "A", 1.5), trade(12, "B", 3.0)]);
        std::fs::write(dir.join("notes.txt"), "not a Parquet file").unwrap();
        let path = dir.display().to_string();
        let cat = reading(&[("t", &path, None)]);
        let now = Instant::now();
        let mut s = sources(&cat, &[], LIST_EVERY);
        assert!(!s.past_start() && !s.caught_up());
        let first = s.next(2, now).unwrap();
        assert_eq!(first.rows, vec![trade(10, "A", 1.0), trade(11, "A", 1.5)]);
        assert_eq!((first.stream.as_str(), first.offset), ("t", 2));
        assert_eq!(first.file, format!("{path}/1.parquet"));
        // a chunk is of one file: its end is a position of its own
        assert_eq!(drain(&mut s, 2, now), [chunk("t", "1.parquet", 3, &[12]), chunk("t", "2.parquet", 2, &[20, 21])]);
        assert!(s.past_start() && s.caught_up());
        assert_eq!(s.errors(), 0);
        // from a checkpoint: the rows of its file after its position, then the files after it
        let at = |file: &str, o| vec![(topic("t", &format!("{path}/{file}")), 0, o)];
        let mut s = sources(&cat, &at("1.parquet", 1), LIST_EVERY);
        assert_eq!(
            drain(&mut s, 10, now),
            [chunk("t", "1.parquet", 3, &[11, 12]), chunk("t", "2.parquet", 2, &[20, 21])]
        );
        let mut s = sources(&cat, &at("1.parquet", 3), LIST_EVERY);
        let first = s.next(1, now).unwrap();
        assert_eq!((first.file.rsplit('/').next(), first.offset), (Some("2.parquet"), 1));
        assert!(!s.past_start(), "the last file there at start, part read");
        assert_eq!(drain(&mut s, 10, now), [chunk("t", "2.parquet", 2, &[21])]);
        assert!(s.past_start());
        let mut s = sources(&cat, &at("2.parquet", 1), LIST_EVERY);
        assert!(!s.past_start(), "resumed in the last file there at start");
        assert_eq!(drain(&mut s, 10, now), [chunk("t", "2.parquet", 2, &[21])]);
        assert!(s.past_start() && s.caught_up());
        let mut s = sources(&cat, &at("2.parquet", 2), LIST_EVERY);
        assert_eq!(drain(&mut s, 10, now), []);
        assert!(s.past_start() && s.caught_up());
        // a position past every file there (they were deleted since): nothing to replay
        let s = sources(&cat, &at("3.parquet", 5), LIST_EVERY);
        assert!(s.past_start());
        // a file of the checkpoint's since deleted: the files named after it
        let mut s = sources(&cat, &at("1a.parquet", 7), LIST_EVERY);
        assert_eq!(drain(&mut s, 10, now), [chunk("t", "2.parquet", 2, &[20, 21])]);
        // another stream's positions are not this one's
        let other = vec![(topic("u", &format!("{path}/2.parquet")), 0, 2)];
        assert_eq!(drain(&mut sources(&cat, &other, LIST_EVERY), 10, now).len(), 2);
    }

    /// A file of more batches than one (`Reader`'s are 65,536 rows), resumed in its first: the
    /// rows given out before are skipped once, the rest all read.
    #[test]
    fn a_source_resumed_in_a_file_of_several_batches_reads_each_row_after_its_position_once() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("in");
        let rows: Vec<Row> = (0..70_000).map(|i| trade(i, "A", 1.0)).collect();
        parquet_file(&dir.join("1.parquet"), &TRADES, &rows);
        let path = dir.display().to_string();
        let cat = reading(&[("t", &path, None)]);
        let mut s = sources(&cat, &[(topic("t", &format!("{path}/1.parquet")), 0, 3)], LIST_EVERY);
        let got: Vec<i64> = drain(&mut s, 100_000, Instant::now()).into_iter().flat_map(|c| c.3).collect();
        assert_eq!(got, (3..70_000).collect::<Vec<_>>());
    }

    #[test]
    fn files_that_appear_are_read_once_listed_and_one_named_before_what_was_read_is_not() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("in");
        let path = dir.display().to_string();
        let cat = reading(&[("t", &path, None)]);
        // no directory yet: no files yet
        let mut s = sources(&cat, &[], Duration::from_secs(1));
        let t0 = Instant::now();
        assert!(s.past_start() && s.caught_up());
        assert_eq!(drain(&mut s, 10, t0), []);
        parquet_file(&dir.join("b.parquet"), &TRADES, &[trade(1, "A", 1.0)]);
        // listed a second after the last listing
        assert_eq!(drain(&mut s, 10, t0 + Duration::from_millis(999)), []);
        assert_eq!(drain(&mut s, 10, t0 + Duration::from_secs(1)), [chunk("t", "b.parquet", 1, &[1])]);
        parquet_file(&dir.join("a.parquet"), &TRADES, &[trade(2, "A", 1.0)]);
        parquet_file(&dir.join("c.parquet"), &TRADES, &[trade(3, "A", 1.0)]);
        assert_eq!(drain(&mut s, 10, t0 + Duration::from_secs(2)), [chunk("t", "c.parquet", 1, &[3])]);
        assert_eq!(drain(&mut s, 10, t0 + Duration::from_secs(9)), []);
        assert!(s.caught_up() && s.past_start());
        // an empty directory at start: none there to read first
        let empty = d.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        let mut s = sources(&reading(&[("t", &empty.display().to_string(), None)]), &[], Duration::ZERO);
        assert!(s.past_start());
        assert_eq!(drain(&mut s, 10, t0), []);
    }

    #[test]
    fn a_file_that_does_not_read_is_counted_and_read_again_a_listing_later() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("in");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("1.parquet"), "being written").unwrap();
        let cat = reading(&[("t", &dir.display().to_string(), None)]);
        let t0 = Instant::now();
        let mut s = sources(&cat, &[], Duration::from_secs(1));
        assert_eq!(drain(&mut s, 10, t0), []);
        assert_eq!(s.errors(), 1);
        assert_eq!(s.errors(), 0, "taken");
        assert!(!s.caught_up(), "the file is still to read");
        assert_eq!(drain(&mut s, 10, t0 + Duration::from_millis(500)), [], "not tried again yet");
        assert_eq!(s.errors(), 0);
        parquet_file(&dir.join("1.parquet"), &TRADES, &[trade(1, "A", 1.0), trade(2, "A", 1.0)]);
        assert_eq!(drain(&mut s, 10, t0 + Duration::from_secs(1)), [chunk("t", "1.parquet", 2, &[1, 2])]);
        assert_eq!(s.errors(), 0);
        assert!(s.caught_up());
    }

    #[test]
    fn sources_are_read_merged_in_time_order() {
        let d = tempfile::tempdir().unwrap();
        let (tdir, qdir) = (d.path().join("t"), d.path().join("q"));
        let rows = |ms: &[i64]| ms.iter().map(|&m| trade(m, "A", 1.0)).collect::<Vec<_>>();
        parquet_file(&tdir.join("1.parquet"), &TRADES, &rows(&[1, 4, 5]));
        parquet_file(&tdir.join("2.parquet"), &TRADES, &rows(&[9, 12]));
        parquet_file(&qdir.join("1.parquet"), &TRADES, &rows(&[2, 3, 5, 6, 7, 8]));
        let (tp, qp) = (tdir.display().to_string(), qdir.display().to_string());
        let cat = reading(&[("t", &tp, Some("ts")), ("q", &qp, Some("ts"))]);
        let mut s = sources(&cat, &[], LIST_EVERY);
        let got = drain(&mut s, 2, Instant::now());
        assert_eq!(
            got,
            [
                chunk("t", "1.parquet", 1, &[1]),
                chunk("q", "1.parquet", 2, &[2, 3]),
                chunk("t", "1.parquet", 3, &[4, 5]),
                chunk("q", "1.parquet", 4, &[5, 6]), // a time both have: the source first named first
                chunk("q", "1.parquet", 6, &[7, 8]),
                chunk("t", "2.parquet", 2, &[9, 12]), // the other has nothing waiting: holds none back
            ]
        );
    }

    /// A sink of trades by day: `partition_by` names its directories.
    fn writing(path: &str, by: Option<&str>) -> Catalog {
        let by = by.map_or(String::new(), |b| format!(", partition_by = '{b}'"));
        let sql = format!(
            "CREATE STREAM i (ts datetime64(3), day nullable(string), sym string, px nullable(float64), n uint64, up bool);
CREATE EXTERNAL STREAM o (ts datetime64(3), day nullable(string), sym string, px nullable(float64), n uint64, up bool)
SETTINGS type = 'file', data_format = 'Parquet', path = '{path}'{by};
CREATE EXTERNAL STREAM k (ts datetime64(3)) SETTINGS type = 'kafka', topic = 'k', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW v INTO o AS SELECT * FROM i;
CREATE MATERIALIZED VIEW w INTO k AS SELECT ts FROM i;"
        );
        let cat = brrrrr_core::sql::parse(&sql).unwrap();
        brrrrr_core::engine::Engine::new(&cat).unwrap();
        cat
    }

    fn row(ms: i64, day: Option<&str>, sym: &str, px: Option<f64>) -> Row {
        let day = day.map_or(Value::Null, |d| Value::Str(d.into()));
        let px = px.map_or(Value::Null, Value::F64);
        vec![Value::Time(ms * 1000), day, Value::Str(sym.into()), px, Value::UInt(ms as u64), Value::Bool(ms % 2 == 0)]
    }

    const WRITTEN: [(&str, &str); 6] = [
        ("ts", "datetime64(3)"),
        ("day", "nullable(string)"),
        ("sym", "string"),
        ("px", "nullable(float64)"),
        ("n", "uint64"),
        ("up", "bool"),
    ];

    fn sinks(cat: &Catalog) -> Sinks {
        Sinks::with_files(cat, "bars.v1", Arc::new(Files::default())).unwrap()
    }

    /// The files under `dir`, relative to it, sorted.
    fn listed(dir: &Path) -> Vec<String> {
        let mut out: Vec<String> = Files::default()
            .names(&dir.display().to_string(), false)
            .unwrap()
            .into_iter()
            .map(|(n, _)| n.strip_prefix(&format!("{}/", dir.display())).unwrap().to_string())
            .collect();
        out.sort();
        out
    }

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    #[test]
    fn a_sink_writes_each_partition_s_rows_of_a_checkpoint_to_a_file_of_its_epoch() {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("out");
        let cat = writing(&out.display().to_string(), Some("day"));
        let mut s = sinks(&cat);
        assert!(!s.row("k", &[Value::Time(1)]), "a Kafka sink's rows are not taken");
        let (a, b, none) = (
            row(1, Some("2024-01-02"), "A", Some(1.5)),
            row(2, Some("a/b c"), "B", None),
            row(3, None, "C", Some(-2.0)),
        );
        for r in [&a, &b, &none, &row(4, Some("2024-01-02"), "A", Some(2.5))] {
            assert!(s.row("o", r));
        }
        s.result().unwrap();
        // nothing in place before its checkpoint's cut commits it
        assert!(
            listed(&out).iter().all(|f| f.rsplit('/').next().unwrap().starts_with(".bars_v1.")),
            "{:?}",
            listed(&out)
        );
        let names = s.cut().unwrap().commit(7, later()).unwrap();
        assert_eq!(names.len(), 3);
        let files = listed(&out);
        assert_eq!(files.len(), 3, "the hidden files moved into place: {files:?}");
        let token = &s.token;
        for (f, dir) in files.iter().zip(["day=2024-01-02", "day=__HIVE_DEFAULT_PARTITION__", "day=a%2Fb%20c"]) {
            let (path, name) = f.rsplit_once('/').unwrap();
            assert_eq!(path, dir);
            assert_eq!(parse_name(name), Some(("bars_v1", 7)), "{name}");
            assert!(name.contains(token.as_str()), "{name}");
        }
        assert_eq!(
            names.iter().map(|n| n.strip_prefix(&format!("{}/", out.display())).unwrap()).collect::<Vec<_>>(),
            {
                let mut f = files.clone();
                f.sort();
                f
            }
        );
        let mut back = read_back(&out.display().to_string(), &WRITTEN);
        back.sort_by_key(|r| r[0].i64());
        assert_eq!(back, [a, b, none, row(4, Some("2024-01-02"), "A", Some(2.5))]);
        // the next checkpoint's rows: files of their own; a cut of nothing commits nothing
        assert!(s.row("o", &row(5, Some("2024-01-02"), "A", Some(3.0))));
        assert_eq!(s.cut().unwrap().commit(8, later()).unwrap().len(), 1);
        assert_eq!(s.cut().unwrap().commit(9, later()).unwrap(), Vec::<String>::new());
        let epochs: Vec<u64> =
            listed(&out).iter().map(|f| parse_name(f.rsplit('/').next().unwrap()).unwrap().1).collect();
        assert_eq!(epochs.iter().filter(|e| **e == 8).count(), 1);
        assert_eq!(read_back(&out.display().to_string(), &WRITTEN).len(), 5);
        // a failed checkpoint is tried again under its epoch: its files are named apart
        assert!(s.row("o", &row(6, Some("2024-01-02"), "A", Some(3.0))));
        s.cut().unwrap().commit(10, later()).unwrap();
        assert!(s.row("o", &row(7, Some("2024-01-02"), "A", Some(3.0))));
        s.cut().unwrap().commit(10, later()).unwrap();
        assert_eq!(read_back(&out.display().to_string(), &WRITTEN).len(), 7);
    }

    #[test]
    fn a_sink_without_partitions_writes_its_rows_in_its_directory() {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("flat");
        let mut s = sinks(&writing(&format!("file://{}/", out.display()), None));
        let rows: Vec<Row> = (0..(BATCH as i64 + 5)).map(|i| row(i, Some("d"), "A", Some(i as f64))).collect();
        for r in &rows {
            assert!(s.row("o", r));
        }
        let names = s.cut().unwrap().commit(1, later()).unwrap();
        assert_eq!(names.len(), 1);
        assert!(names[0].starts_with(&format!("file://{}/bars_v1.00000000000000000001.", out.display())), "{names:?}");
        assert_eq!(listed(&out).len(), 1);
        assert_eq!(read_back(&out.display().to_string(), &WRITTEN), rows);
    }

    #[test]
    fn a_sink_writes_a_row_group_of_its_fullest_file_past_the_rows_it_holds() {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("out");
        let mut s = sinks(&writing(&out.display().to_string(), Some("day")));
        s.held = 10;
        for i in 0..8 {
            assert!(s.row("o", &row(i, Some("big"), "A", None)));
        }
        for i in 0..2 {
            assert!(s.row("o", &row(i, Some("small"), "A", None)));
        }
        let held = |s: &Sinks| s.sinks["o"].open.values().map(Open::held).collect::<Vec<_>>();
        assert_eq!(held(&s), [8, 2], "10 held: not past it");
        // a partition's rows go to its writer `BATCH` at a time
        assert_eq!(s.sinks["o"].open["day=big"].rows.len(), 8);
        assert!(s.row("o", &row(2, Some("small"), "A", None)));
        assert_eq!(held(&s), [0, 3], "past it: the fullest one's rows written as a row group");
        for i in 0..2 {
            assert!(s.row("o", &row(i, Some("small"), "A", None)));
        }
        assert_eq!(held(&s), [0, 5]);
        s.cut().unwrap().commit(1, later()).unwrap();
        assert_eq!(read_back(&out.display().to_string(), &WRITTEN).len(), 13);
    }

    #[test]
    fn files_are_not_put_in_place_past_the_lease() {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("out");
        let mut s = sinks(&writing(&out.display().to_string(), None));
        assert!(s.row("o", &row(1, None, "A", None)));
        let err = s.cut().unwrap().commit(3, Instant::now()).unwrap_err();
        assert!(err.to_string().contains("the lease ran out before checkpoint 3's files were written"), "{err}");
        assert_eq!(listed(&out), Vec::<String>::new(), "nothing put in place, the hidden file removed");
    }

    #[test]
    fn a_write_that_fails_is_the_sink_s_error_and_it_takes_nothing_more() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("a-file");
        std::fs::write(&file, "").unwrap();
        let mut s = sinks(&writing(&file.join("out").display().to_string(), None));
        assert!(s.row("o", &row(1, None, "A", None)), "taken, and failed");
        assert!(s.row("o", &row(2, None, "A", None)));
        let err = s.cut().err().unwrap();
        assert!(format!("{err:#}").starts_with("writing o: "), "{err:#}");
        s.result().unwrap();
        assert!(s.cut().unwrap().commit(1, later()).unwrap().is_empty());
    }

    #[test]
    fn clean_deletes_this_pipeline_s_files_of_later_epochs_and_its_hidden_ones() {
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("out");
        let s = sinks(&writing(&out.display().to_string(), Some("day")));
        assert_eq!(s.clean(0).unwrap(), Vec::<String>::new(), "no directory yet");
        let names = [
            "day=a/bars_v1.00000000000000000005.t-1.parquet",
            "day=a/bars_v1.00000000000000000006.t-2.parquet",
            "day=b/bars_v1.00000000000000000007.t-3.parquet",
            "day=b/bars_v1.00000000000000000008.u-1.parquet",
            "day=b/other.00000000000000000009.t-1.parquet",
            "day=b/data_0.parquet",
            "day=b/.bars_v1.t-4.tmp",
            "day=b/.other.t-4.tmp",
            "notes.txt",
        ];
        for n in names {
            let p = out.join(n);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, "").unwrap();
        }
        let mut deleted: Vec<String> = s
            .clean(6)
            .unwrap()
            .iter()
            .map(|n| n.strip_prefix(&format!("{}/", out.display())).unwrap().to_string())
            .collect();
        deleted.sort();
        assert_eq!(deleted, [names[6], names[2], names[3]]);
        assert_eq!(
            listed(&out),
            [names[0], names[1], names[5], names[4], names[7], names[8]].map(String::from).iter().fold(
                vec![],
                |mut v, n| {
                    v.push(n.clone());
                    v.sort();
                    v
                }
            )
        );
        assert_eq!(s.clean(6).unwrap(), Vec::<String>::new());
    }
}
