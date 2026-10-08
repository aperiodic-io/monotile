//! The historical executor (ADR-0016): the pipelines of a SQL script over sources read from
//! files, each in the order of its clock column, rather than over a live feed. The views that
//! take source rows run on batches holding a chunk of the sources' time; a window's output goes
//! into the engine, whose views after it run as they do live. The output is the engine's for
//! the same rows in time order, message for message.
//!
//! A view's projection, ORDER BY and tumbling window run on columns (`expr::vector`,
//! `agg::kernel`); whatever has no columnar form (a join, a window function, a window holding a
//! t-digest) runs the engine's own operators over the batch's rows.
use super::*;
use crate::agg::kernel::{add_rows, Args, Rows as Picked};
use crate::column::{Batch, Col, Data};
use crate::expr::vector::{truths, Lags, Program, VCompiler};

/// Runs jobs to completion before returning, here or on other threads: the runtime's thread
/// pool (`Serial` runs them one after the other). Each job owns what it changes, so the output
/// is the same however they run.
pub trait Pool: Sync {
    fn run<'a>(&self, jobs: Vec<Task<'a>>);
    /// How many threads run jobs: a batch is cut in about as many parts.
    fn threads(&self) -> usize;
    /// Rows below which a batch is not cut into parts.
    fn part_rows(&self) -> usize {
        PART_ROWS
    }
}

/// A job for a `Pool`.
pub type Task<'a> = Box<dyn FnOnce() + Send + 'a>;

/// Every job on the calling thread, in turn.
#[derive(Clone, Copy, Debug, Default)]
pub struct Serial;

impl Pool for Serial {
    fn run<'a>(&self, jobs: Vec<Task<'a>>) {
        jobs.into_iter().for_each(|j| j());
    }

    fn threads(&self) -> usize {
        1
    }
}

/// Rows below which a batch is not cut into parts for a pool's threads.
const PART_ROWS: usize = 16_384;

/// Rows a window evaluates its aggregates' arguments over at a time.
const BLOCK: usize = 16_384;

/// `n` rows cut into about as many runs as `pool` has threads, each of at least its
/// `part_rows` (one run below).
fn parts(n: usize, pool: &dyn Pool) -> Vec<std::ops::Range<usize>> {
    let k = pool.threads().min(n / pool.part_rows().max(1)).max(1);
    (0..k).map(|i| i * n / k..(i + 1) * n / k).collect()
}

/// One source stream's rows, in batches, in the order of its clock column.
pub trait Source {
    /// The next rows, in the stream's column order; `None` at the end.
    fn next(&mut self) -> Option<Result<Batch, String>>;
}

/// What a run read and did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Stats {
    /// Per source: rows read, rows inside the range.
    pub rows: BTreeMap<String, (u64, u64)>,
    pub chunks: u64,
    /// Rows windows dropped: past their watermark, without a time.
    pub late: u64,
    pub null_time: u64,
    /// Views that run the engine's operators over rows, by name.
    pub row_views: Vec<String>,
}

/// A source stream: its name, width and the column its rows are ordered by.
#[derive(Clone)]
struct Src {
    name: String,
    width: usize,
    clock: usize,
}

/// Where a batch-run view's rows go.
#[derive(Clone)]
enum Out {
    /// A stream only batch-run views read: their input for this chunk.
    Slot(usize),
    /// Into the engine (a window's output, or a sink): its views take them from there.
    Engine,
}

/// What flows from one of a view's operators to the next: a chunk's rows, as batches or rows.
enum Flow {
    Batches(Vec<Batch>),
    Rows(Vec<Row>),
}

impl Flow {
    fn rows(self) -> Vec<Row> {
        match self {
            Flow::Batches(b) => b.iter().flat_map(Batch::rows).collect(),
            Flow::Rows(r) => r,
        }
    }

    fn batches(self, width: usize) -> Vec<Batch> {
        match self {
            Flow::Batches(b) => b,
            Flow::Rows(r) => vec![Batch::from_rows(&r, width)],
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Flow::Batches(b) => b.iter().all(|b| b.len == 0),
            Flow::Rows(r) => r.is_empty(),
        }
    }
}

/// A view's operator over batches.
#[derive(Clone)]
enum BOp {
    /// SELECT over columns: the rows `filter` keeps, then each item (`None`: no reader reads it;
    /// `all` holds every item's node).
    Project {
        prog: Program,
        width: usize,
        filter: Option<usize>,
        items: Vec<Option<usize>>,
        all: Vec<usize>,
    },
    /// ORDER BY (`Op::Sort`, `Op::Hold`): rows in key order pass as they are, others are sorted,
    /// stably. A held one's order holds across chunks: a chunk's first row before the last
    /// one's is an input out of order, refused.
    /// Trusted (`Historical::set_trust_order`): rows pass as they come, unchecked.
    Order {
        prog: Program,
        keys: Vec<usize>,
        held: bool,
        last: Option<Row>,
        trusted: bool,
    },
    Window(Box<BWindow>),
    /// `lag` window functions over columns (`Op::Over`).
    Lag(Box<BLag>),
    /// An exact ASOF LEFT JOIN over columns (`Op::Join`): a view's first operator.
    Join(Box<BJoin>),
    /// The engine's operators, over rows.
    Rows(Vec<Op>),
}

/// `Op::Join` (exact) over columns, its sides' rows in the order of their join times: a left
/// row takes, of each right side, the row of its key with the greatest time at or before its
/// own, the last of equal ones (the newest version), or the side's defaults. Each side's join
/// time must not go back: a chunk's rows of a side then hold every right row a left row of the
/// chunk can take, besides the newest of each key before it.
#[derive(Clone)]
struct BJoin {
    /// Each side's operators over its input (the stream's width), and its output's width.
    sides: Vec<(Vec<BOp>, usize, usize)>,
    rights: Vec<BRight>,
}

#[derive(Clone)]
struct BRight {
    /// Over the rows joined so far (the left side's and the right sides' before): the time
    /// and key of a left row.
    left: Program,
    left_ts: usize,
    left_keys: Vec<usize>,
    /// Over this side's rows.
    right: Program,
    ts: usize,
    keys: Vec<usize>,
    defaults: Row,
    /// Per key text, the newest row of this side.
    newest: FxHashMap<String, Row>,
    /// The latest join time taken on each side: neither may go back.
    left_max: i64,
    right_max: i64,
    /// Left rows dropped without a time (the engine's `Dropped::null_time`).
    null_time: u64,
}

/// `Op::Over` for `lag` calls: the rows WHERE keeps, each call's values in a column after the
/// input's, partition by partition as `over::Group` keeps them (arrival order, the least
/// recently used tenth evicted at `MAX_PARTITIONS`).
#[derive(Clone)]
struct BLag {
    prog: Program,
    width: usize,
    filter: Option<usize>,
    /// (argument, offset, default) per call, in slot order.
    calls: Vec<(usize, usize, Value)>,
    groups: Vec<LagGroup>,
}

/// The calls sharing one PARTITION BY: its keys, its calls, and per partition (by key text)
/// when it was last used and each call's last values.
#[derive(Clone)]
struct LagGroup {
    keys: Vec<usize>,
    calls: Vec<usize>,
    parts: FxHashMap<String, (u64, Vec<VecDeque<Value>>)>,
    used: u64,
}

/// A tumbling window over columns: the engine's window (its groups, watermark and output), its
/// time, keys and aggregate arguments as nodes over the input batch.
#[derive(Clone)]
struct BWindow {
    w: Box<Window>,
    prog: Program,
    ts: usize,
    keys: Vec<usize>,
    args: Vec<BArgs>,
}

#[derive(Clone, Debug)]
enum BArgs {
    Cols(Vec<usize>),
    /// `arg_max(x, (a, b))`: `x`, and the key's items.
    Keyed(usize, Vec<usize>),
    /// A sequence aggregate's tuple, item by item.
    Tuple(Vec<usize>),
}

/// A view run on batches: its engine plan, the slot of each of its inputs (one per join side),
/// and its operators.
#[derive(Clone)]
struct BView {
    name: String,
    plan: usize,
    inputs: Vec<usize>,
    out: Out,
    ops: Vec<BOp>,
    /// 1 + the greatest level of the views writing its inputs (sources: 0). Views of one level
    /// read nothing another writes: they run side by side.
    level: usize,
}

#[derive(Clone)]
pub struct Historical {
    engine: Engine,
    sources: Vec<Src>,
    /// Each stream batch-run views read (sources first), by name: its slot.
    slots: Vec<String>,
    /// Per slot: the columns its readers read (the others are not computed).
    needed: Vec<Vec<bool>>,
    views: Vec<BView>,
    /// Every window's width: a range must start and end on their boundaries.
    widths: Vec<i64>,
    chunk_rows: usize,
    /// Sets of window views that differ in width alone, the narrowest first, each with the first
    /// view of the run of views of their level they are in (`fused_windows`).
    fused: Vec<(usize, Vec<usize>)>,
    /// The slots several views write (a UNION ALL), each with its column that copies every
    /// writer's source clock: a chunk's rows of it go to its readers in that order, as they come
    /// live (`merge_order`).
    merged: Vec<(usize, usize)>,
    /// The window views a run leaves open at its end (`hold`).
    held: Vec<usize>,
}

/// Each stream column a `Mapping` computes (its position, its values), and the rows its filter
/// keeps (`None`: every one).
pub type Mapped = (Vec<(usize, Arc<Col>)>, Option<Vec<usize>>);

/// A stream's columns computed from another table's (a file's), each an SQL expression over its
/// columns: a source whose files are not laid out as its stream (the archive's prices as text,
/// a trade's quantity under another name).
pub struct Mapping {
    prog: Program,
    /// Per expression: its stream column, its node, the column's type.
    outs: Vec<(usize, usize, Type)>,
    /// The condition a row of the input must meet to be one of the stream's.
    filter: Option<usize>,
}

impl Mapping {
    /// `input`: the table's columns; `exprs`: each stream column's position, type and expression;
    /// `filter`: the condition a row must meet (the rows of a table of several streams).
    pub fn new(input: &[(String, Type)], exprs: &[(usize, Type, String)], filter: Option<&str>) -> R<Mapping> {
        let mut scope = Scope::new(input.iter().map(|(n, _)| n.clone()));
        scope.types = input.iter().map(|(_, t)| Some(t.clone())).collect();
        let mut prog = Program::new(input.len());
        let mut outs = vec![];
        for (k, ty, text) in exprs {
            let e = crate::sql::parse_expr(text).map_err(|e| format!("{text}: {e}"))?;
            let node = VCompiler::new(&scope).expr(&mut prog, &e).map_err(|e| format!("{text}: {e}"))?;
            outs.push((*k, node, ty.clone()));
        }
        let filter = match filter {
            Some(text) => {
                let e = crate::sql::parse_expr(text).map_err(|e| format!("{text}: {e}"))?;
                Some(VCompiler::new(&scope).condition(&mut prog, &e).map_err(|e| format!("{text}: {e}"))?)
            }
            None => None,
        };
        Ok(Mapping { prog, outs, filter })
    }

    /// The input's columns the expressions and the filter read.
    pub fn reads(&self) -> Vec<bool> {
        self.prog.reads_of(self.outs.iter().map(|o| o.1).chain(self.filter))
    }

    /// Each stream column the expressions give over a batch of the input, in its type; and the
    /// rows the filter keeps (`None`: every one).
    pub fn apply(&self, input: &Batch) -> Mapped {
        let mut ev = self.prog.eval(input);
        let cols = self.outs.iter().map(|(k, node, ty)| (*k, cast(&self.prog.col(&mut ev, *node), ty))).collect();
        let keep = self.filter.map(|f| {
            let t = truths(&self.prog.col(&mut ev, f));
            (0..t.len()).filter(|r| t[*r] == Some(true)).collect()
        });
        (cols, keep)
    }
}

/// Rows per chunk, by the source that reaches it first.
pub const CHUNK_ROWS: usize = 1 << 17;

impl Historical {
    /// Plans `cat` for a historical run: as `Engine::new` (exact ASOF joins), with each source's
    /// clock its `local_timestamp` or `local_timestamp_us` column (`set_clock` for another).
    pub fn new(cat: &Catalog) -> R<Historical> {
        let mut engine = Engine::new(cat)?;
        engine.set_asof(Asof::Exact);
        let written: HashSet<&String> = engine.plans.iter().map(|p| &p.target).collect();
        let mut sources = vec![];
        for name in engine.plans.iter().flat_map(|p| &p.inputs) {
            if written.contains(name) || sources.iter().any(|s: &Src| s.name == *name) {
                continue;
            }
            let st = &engine.streams[name];
            let clock = st.columns.iter().position(|c| c.name == "local_timestamp" || c.name == "local_timestamp_us");
            sources.push(Src { name: name.clone(), width: st.columns.len(), clock: clock.unwrap_or(usize::MAX) });
        }
        sources.sort_by(|a, b| a.name.cmp(&b.name));
        let planned: Vec<&View> = cat.views.iter().filter(|v| !engine.skipped.contains(&v.name)).collect();
        // streams a view the engine runs writes too (a UNION ALL of a window's output and source
        // rows): each batch-run writer's rows go into the engine, whose views read them
        let mut into_engine: HashSet<String> = HashSet::new();
        let (slots, mut views) = loop {
            let (slots, views) = batch_views(&engine, &planned, &sources, &into_engine)?;
            // written into the engine: by a view it runs, or a window's
            let into = |i: usize| !views.iter().any(|b: &BView| b.plan == i && matches!(b.out, Out::Slot(_)));
            let mixed = (engine.plans.iter().enumerate())
                .filter(|(i, p)| slots.contains(&p.target) && into(*i))
                .map(|(_, p)| p.target.clone())
                .collect::<Vec<_>>();
            if mixed.is_empty() {
                break (slots, views);
            }
            into_engine.extend(mixed);
        };
        // a stream of source rows is read by batch-run views only
        for (i, p) in engine.plans.iter().enumerate() {
            if !views.iter().any(|b| b.plan == i) {
                if let Some(s) = p.inputs.iter().find(|s| slots.contains(s)) {
                    let v = &planned[i].name;
                    return Err(format!("{v}: reads {s} beside a window's output: not supported historically"));
                }
            }
        }
        let mut widths = vec![];
        fn walk(ops: &[Op], widths: &mut Vec<i64>) {
            for op in ops {
                match op {
                    Op::Window(w) => widths.push(w.width),
                    Op::Join(j) => j.sides.iter().for_each(|s| walk(s, widths)),
                    _ => {}
                }
            }
        }
        engine.plans.iter().for_each(|p| walk(&p.ops, &mut widths));
        for b in &mut views {
            let ops = std::mem::take(&mut engine.plans[b.plan].ops);
            b.ops = columnar(cat, planned[b.plan], ops).map_err(|e| format!("{}: {e}", b.name))?;
        }
        let mut h = Historical {
            engine,
            sources,
            slots,
            needed: vec![],
            views,
            widths,
            chunk_rows: CHUNK_ROWS,
            fused: vec![],
            merged: vec![],
            held: vec![],
        };
        h.merge_order();
        h.fused = fused_windows(&h.views);
        Ok(h)
    }

    /// Finds the column of each slot several views write that copies, through projections,
    /// every writer's source clock (`merged`), which its writers then compute; then `prune`.
    /// Without one (a writer that computes its time, or a join), its readers take a chunk's rows
    /// writer by writer.
    fn merge_order(&mut self) {
        let mut clock: Vec<Option<usize>> =
            self.slots.iter().map(|s| self.sources.iter().find(|x| x.name == *s).map(|x| x.clock)).collect();
        let mut writers: Vec<Vec<Option<usize>>> = vec![vec![]; self.slots.len()];
        // upstream first: a slot's writers before its readers
        for b in &self.views {
            let Out::Slot(s) = b.out else { continue };
            let copy = match (b.ops.as_slice(), b.inputs.as_slice()) {
                ([BOp::Project { prog, all, .. }], [i]) => clock[*i].and_then(|c| {
                    let item = all.iter().position(|n| prog.column(*n) == Some(c))?;
                    self.engine.plans[b.plan].by_name.iter().position(|x| *x == Some(item))
                }),
                _ => None,
            };
            writers[s].push(copy);
            clock[s] = copy.filter(|_| writers[s].iter().all(|w| *w == copy));
        }
        self.merged =
            (0..self.slots.len()).filter(|s| writers[*s].len() > 1).filter_map(|s| Some((s, clock[s]?))).collect();
        self.prune();
    }

    /// Marks the columns of each slot its readers read, and stops computing the items of a
    /// projection that feed only columns nobody reads.
    fn prune(&mut self) {
        let width = |s: &String| self.engine.streams[s].columns.len();
        let mut needed: Vec<Vec<bool>> = self.slots.iter().map(|s| vec![false; width(s)]).collect();
        for &(s, clock) in &self.merged {
            needed[s][clock] = true;
        }
        // downstream first: what a view's readers read decides which of its items it computes,
        // and those what it reads
        for b in self.views.iter_mut().rev() {
            if let Out::Slot(s) = b.out {
                let plan = &self.engine.plans[b.plan];
                let used: Vec<usize> =
                    plan.by_name.iter().zip(&needed[s]).filter_map(|(i, n)| i.filter(|_| *n)).collect();
                // ORDER BY keys read the output too, unless the order is trusted
                let keys = b.ops.iter().find_map(|o| match o {
                    BOp::Order { prog, trusted: false, .. } => Some(prog.reads.clone()),
                    _ => None,
                });
                if let Some(BOp::Project { items, all, .. }) =
                    b.ops.iter_mut().find(|o| matches!(o, BOp::Project { .. }))
                {
                    for (k, (item, node)) in items.iter_mut().zip(all.iter()).enumerate() {
                        let read =
                            used.contains(&k) || keys.as_ref().is_some_and(|r| r.get(k).copied().unwrap_or(false));
                        *item = read.then_some(*node);
                    }
                }
            }
            // the columns of its input a projection's items and filter read
            let project = |prog: &Program, filter: &Option<usize>, items: &[Option<usize>]| {
                prog.reads_of(items.iter().flatten().copied().chain(*filter))
            };
            let reads: Option<Vec<bool>> = match (b.ops.as_slice(), b.inputs.as_slice()) {
                ([BOp::Project { prog, filter, items, .. }, ..], [_]) => Some(project(prog, filter, items)),
                ([BOp::Window(w), ..], [_]) => Some(w.prog.reads.clone()),
                // the projection after window functions reads the input's columns as they are
                ([BOp::Lag(l), BOp::Project { prog, filter, items, .. }, ..], [_]) => {
                    let p = project(prog, filter, items);
                    Some(
                        (0..l.width)
                            .map(|k| {
                                l.prog.reads.get(k).copied().unwrap_or(false) || p.get(k).copied().unwrap_or(false)
                            })
                            .collect(),
                    )
                }
                _ => None,
            };
            for &s in &b.inputs {
                for (k, n) in needed[s].iter_mut().enumerate() {
                    *n |= reads.as_ref().is_none_or(|r| r.get(k).copied().unwrap_or(false));
                }
            }
        }
        self.needed = needed;
    }

    /// The source streams a run reads, by name.
    pub fn sources(&self) -> Vec<&Stream> {
        self.sources.iter().map(|s| &self.engine.streams[&s.name]).collect()
    }

    /// The columns of `source` the pipelines read (its clock among them): the others need not be
    /// read at all, and may come as NULLs.
    pub fn reads(&self, source: &str) -> Vec<bool> {
        let i = self.slots.iter().position(|s| s == source);
        let mut r = i.map_or(vec![], |i| self.needed[i].clone());
        if let Some(s) = self.sources.iter().find(|s| s.name == source) {
            if let Some(c) = r.get_mut(s.clock) {
                *c = true;
            }
        }
        r
    }

    /// Leaves the window writing `target` open at the end of each run: its groups' aggregates
    /// are this part's of the run, which `absorb` merges with the other parts' and `close_held`
    /// closes. False, and nothing held, when its aggregates do not merge so (`Acc::
    /// mergeable_across`) or it is not a window into the engine.
    pub fn hold(&mut self, target: &str) -> bool {
        let found = self.views.iter_mut().enumerate().find(|(_, b)| self.engine.plans[b.plan].target == target);
        let Some((v, b)) = found else { return false };
        let Some(w) = window_of(&mut b.ops) else { return false };
        let mergeable = accs(&w.aggs, &w.shares).is_ok_and(|a| a.iter().all(Acc::mergeable_across));
        if !mergeable || !matches!(b.out, Out::Engine) {
            return false;
        }
        // fused windows close together: this one alone
        self.fused.retain(|(_, set)| !set.contains(&v));
        self.held.push(v);
        true
    }

    /// Merges the groups of `other`'s held windows (a run's other part: other keys) into this
    /// one's, group by group.
    pub fn absorb(&mut self, other: &mut Historical) {
        for &v in &self.held {
            let (Some(w), Some(o)) = (window_of(&mut self.views[v].ops), window_of(&mut other.views[v].ops)) else {
                continue;
            };
            for (start, groups) in std::mem::take(&mut o.open) {
                let mine = w.open.entry(start).or_default();
                for (key, (keys, accs)) in groups {
                    match mine.get_mut(&key) {
                        Some((_, a)) => a.iter_mut().zip(&accs).for_each(|(a, b)| a.merge(b)),
                        None => {
                            mine.insert(key, (keys, accs));
                        }
                    }
                }
            }
        }
    }

    /// Closes the held windows, as the end of a run would have: their rows go on through the
    /// views after them.
    pub fn close_held<O: Output + ?Sized>(&mut self, pool: &dyn Pool, out: &mut O) -> R<()> {
        let mut slots: Vec<Vec<Batch>> = vec![vec![]; self.slots.len()];
        for v in std::mem::take(&mut self.held) {
            let sides = self.views[v].inputs.len();
            let flow = run_view(&mut self.views[v].ops, vec![vec![]; sides], true, pool)?;
            self.deliver(v, flow, &mut slots, out);
        }
        self.engine.close_until(i64::MAX, out);
        Ok(())
    }

    /// Orders `source`'s rows by `column` (its time in µs).
    pub fn set_clock(&mut self, source: &str, column: &str) -> R<()> {
        let s = self.sources.iter_mut().find(|s| s.name == source).ok_or(format!("no source {source}"))?;
        let st = &self.engine.streams[source];
        s.clock = st.columns.iter().position(|c| c.name == column).ok_or(format!("{source} has no column {column}"))?;
        self.merge_order();
        Ok(())
    }

    pub fn set_chunk_rows(&mut self, n: usize) {
        self.chunk_rows = n.max(1);
    }

    /// Takes each source's rows of one clock time to be in its views' ORDER BY order already (a
    /// held ORDER BY's input sorted by its keys, as `kdb+`'s `s#`): they pass as they come, and
    /// a column read only to check that order (a trade's id) is not read at all. Unchecked:
    /// rows out of that order are aggregated in the order they come.
    pub fn set_trust_order(&mut self, trust: bool) {
        for b in &mut self.views {
            for op in &mut b.ops {
                if let BOp::Order { trusted, .. } = op {
                    *trusted = trust;
                }
            }
        }
        self.prune();
    }

    /// Runs the pipelines over `inputs` (by source name; a source without one is empty), each
    /// in the order of its clock: the rows whose clock is in `range`, which starts and ends on
    /// every window's boundary, so that every window is whole. Every window closes at the end.
    pub fn run<O: Output + ?Sized>(
        &mut self,
        inputs: Vec<(String, Box<dyn Source + '_>)>,
        range: std::ops::Range<i64>,
        pool: &dyn Pool,
        out: &mut O,
    ) -> R<Stats> {
        for w in &self.widths {
            if range.start.rem_euclid(*w) != 0 || range.end.rem_euclid(*w) != 0 {
                return Err(format!("the range {range:?} does not start and end on {w} µs windows' boundaries"));
            }
        }
        let mut feeds: Vec<Feed> = self.sources.iter().map(|s| Feed::new(&s.name, s.clock, s.width)).collect();
        for (name, src) in inputs {
            let f = feeds.iter_mut().find(|f| f.name == name).ok_or(format!("no source {name} in the SQL"))?;
            if f.src.is_some() {
                return Err(format!("two inputs for {name}"));
            }
            if f.clock == usize::MAX {
                return Err(format!("{name}: no clock column (local_timestamp or local_timestamp_us)"));
            }
            f.src = Some(src);
        }
        let mut stats = Stats::default();
        let mut slots: Vec<Vec<Batch>> = vec![vec![]; self.slots.len()];
        loop {
            // the chunk ends before the first clock past `chunk_rows` rows of any source
            let mut until = i64::MAX;
            for f in &mut feeds {
                f.fill(self.chunk_rows + 1, &range)?;
                if let Some(t) = f.clock_at(self.chunk_rows) {
                    until = until.min(t);
                }
            }
            // more than a chunk of rows of one time: all of them
            let Some(first) = feeds.iter().filter_map(|f| f.clock_at(0)).min() else { break };
            if until <= first {
                until = first.saturating_add(1);
            }
            for (i, f) in feeds.iter_mut().enumerate() {
                slots[i].extend(f.take_until(until));
            }
            stats.chunks += 1;
            self.chunk(&mut slots, pool, out, false)?;
        }
        self.chunk(&mut slots, pool, out, true)?;
        self.engine.close_until(i64::MAX, out);
        for f in feeds {
            stats.rows.insert(f.name, (f.read, f.kept));
        }
        for b in &self.views {
            for op in &b.ops {
                match op {
                    BOp::Window(w) => {
                        stats.late += w.w.late;
                        stats.null_time += w.w.dropped.null_time;
                    }
                    BOp::Join(j) => stats.null_time += j.rights.iter().map(|r| r.null_time).sum::<u64>(),
                    BOp::Rows(ops) => {
                        let (late, null) = super::drops(ops);
                        stats.late += late;
                        stats.null_time += null;
                        if !stats.row_views.contains(&b.name) {
                            stats.row_views.push(b.name.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        stats.late += self.engine.late();
        stats.null_time += self.engine.null_time();
        Ok(stats)
    }

    /// Runs the batch-run views over one chunk, upstream first: `slots` holds each source's
    /// rows, and receives each stream's as its writer runs. At the `end`, each view takes what
    /// the views before it wrote as they closed, then closes as an event at the end of time
    /// would.
    fn chunk<O: Output + ?Sized>(
        &mut self,
        slots: &mut [Vec<Batch>],
        pool: &dyn Pool,
        out: &mut O,
        end: bool,
    ) -> R<()> {
        let mut i = 0;
        while i < self.views.len() {
            // the views of one level side by side, their outputs delivered in creation order
            let level = self.views[i].level;
            let n = self.views[i..].iter().take_while(|b| b.level == level).count();
            for &(s, clock) in &self.merged {
                if self.views[i..i + n].iter().any(|b| b.inputs.contains(&s)) {
                    in_clock_order(&mut slots[s], clock);
                }
            }
            let inputs: Vec<Vec<Vec<Batch>>> =
                self.views[i..i + n].iter().map(|b| b.inputs.iter().map(|&s| slots[s].clone()).collect()).collect();
            // a held window stays open at the end
            let ends: Vec<bool> = (i..i + n).map(|v| end && !self.held.contains(&v)).collect();
            let mut flows: Vec<Option<R<Flow>>> = (0..n).map(|_| None).collect();
            {
                // a job per view, or per set of windows run together
                let mut units: Vec<Vec<usize>> = (self.fused.iter())
                    .filter(|(run, _)| *run == i)
                    .map(|(_, set)| set.iter().map(|v| v - i).collect())
                    .collect();
                let together: HashSet<usize> = units.iter().flatten().copied().collect();
                units.extend((0..n).filter(|k| !together.contains(k)).map(|k| vec![k]));
                let mut views: Vec<Option<&mut BView>> = self.views[i..i + n].iter_mut().map(Some).collect();
                let mut outs: Vec<Option<&mut Option<R<Flow>>>> = flows.iter_mut().map(Some).collect();
                let mut inputs: Vec<Option<Vec<Vec<Batch>>>> = inputs.into_iter().map(Some).collect();
                let jobs: Vec<Task> = (units.iter())
                    .map(|unit| -> Task {
                        let mut vs: Vec<&mut BView> =
                            unit.iter().map(|k| views[*k].take().expect("a unit a view")).collect();
                        let mut fs: Vec<&mut Option<R<Flow>>> =
                            unit.iter().map(|k| outs[*k].take().expect("a unit a view")).collect();
                        let mut ins: Vec<Vec<Vec<Batch>>> =
                            unit.iter().map(|k| inputs[*k].take().expect("a unit a view")).collect();
                        let end = ends[unit[0]];
                        Box::new(move || {
                            if let ([b], [f]) = (&mut vs[..], &mut fs[..]) {
                                let input = ins.pop().expect("its input");
                                **f = Some(
                                    run_view(&mut b.ops, input, end, pool).map_err(|e| format!("{}: {e}", b.name)),
                                );
                                return;
                            }
                            // windows of one input: its batches
                            let input = ins.swap_remove(0).into_iter().next().unwrap_or_default();
                            let mut ws: Vec<&mut BWindow> = (vs.iter_mut())
                                .map(|b| match &mut b.ops[0] {
                                    BOp::Window(w) => &mut **w,
                                    _ => unreachable!("a set of windows"),
                                })
                                .collect();
                            for (f, rows) in fs.into_iter().zip(windows(&mut ws, input, end, pool)) {
                                *f = Some(Ok(Flow::Rows(rows)));
                            }
                        })
                    })
                    .collect();
                if jobs.len() == 1 {
                    jobs.into_iter().for_each(|j| j());
                } else {
                    pool.run(jobs);
                }
            }
            for (k, f) in flows.into_iter().enumerate() {
                self.deliver(i + k, f.expect("a job per view")?, slots, out);
            }
            i += n;
        }
        slots.iter_mut().for_each(Vec::clear);
        Ok(())
    }

    /// View `i`'s output: into its slot, laid out in the stream's columns, or into the engine.
    fn deliver<O: Output + ?Sized>(&mut self, i: usize, flow: Flow, slots: &mut [Vec<Batch>], out: &mut O) {
        if flow.is_empty() {
            return;
        }
        let b = &self.views[i];
        let e = &mut self.engine;
        match b.out {
            Out::Slot(s) => {
                let (plan, st) = (&e.plans[b.plan], &e.streams[&e.plans[b.plan].target]);
                let computed = e.computed.get(&st.name).map_or(&[][..], Vec::as_slice);
                let laid = match flow {
                    Flow::Batches(batches) if computed.is_empty() => batches
                        .iter()
                        .filter(|b| b.len > 0)
                        .map(|b| lay_out_batch(plan, st, b, &self.needed[s]))
                        .collect(),
                    flow => {
                        let rows: Vec<Row> = flow
                            .rows()
                            .into_iter()
                            .map(|r| {
                                let mut full = Vec::with_capacity(st.columns.len());
                                lay_out(plan, st, Cow::Owned(r), &mut full);
                                compute(plan, st, computed, &mut full);
                                full
                            })
                            .collect();
                        vec![Batch::from_rows(&rows, st.columns.len())]
                    }
                };
                slots[s].extend(laid);
            }
            Out::Engine => {
                // a sink's rows as they are, in batches, to an output that takes them so
                if let Flow::Batches(batches) = &flow {
                    let plan = &e.plans[b.plan];
                    let st = &e.streams[&plan.target];
                    if st.kind == Kind::External && e.computed.get(&st.name).is_none_or(Vec::is_empty) {
                        let all = vec![true; st.columns.len()];
                        let mut laid = batches.iter().filter(|b| b.len > 0).map(|b| lay_out_batch(plan, st, b, &all));
                        if laid.all(|l| out.batch(&st.name, &l)) {
                            out.flush();
                            return;
                        }
                    }
                }
                let outer = e.window_end;
                let from = b.ops.iter().map(|o| match o {
                    BOp::Window(w) => w.w.emitted_from,
                    BOp::Rows(ops) => emitted_from(ops),
                    _ => i64::MAX,
                });
                e.window_end = outer.min(from.min().unwrap_or(i64::MAX));
                e.write_target(b.plan, flow.rows(), out);
                e.window_end = outer;
            }
        }
    }
}

/// The views that run on batches, upstream first: those reading only sources and streams such
/// views write, each stream's slot (sources first). A window ends the batch run, and so does a
/// stream of `into_engine`: what they write goes into the engine.
fn batch_views(
    engine: &Engine,
    planned: &[&View],
    sources: &[Src],
    into_engine: &HashSet<String>,
) -> R<(Vec<String>, Vec<BView>)> {
    let mut slots: Vec<String> = sources.iter().map(|s| s.name.clone()).collect();
    let mut views: Vec<BView> = vec![];
    for v in engine.upstream_first() {
        let p = &engine.plans[v];
        let batched = |s: &String| slots.contains(s);
        if !p.inputs.iter().any(batched) {
            continue;
        }
        let name = planned[v].name.clone();
        if let Some(s) = p.inputs.iter().find(|s| !batched(s)) {
            return Err(format!("{name}: reads {s} beside streams of source rows: not supported historically"));
        }
        let window = p.ops.iter().any(|o| matches!(o, Op::Window(_)));
        let out = if window || engine.streams[&p.target].kind != Kind::Stream || into_engine.contains(&p.target) {
            Out::Engine
        } else if let Some(s) = slots.iter().position(|x| *x == p.target) {
            // another writer of the stream (a UNION ALL): into its slot as well
            Out::Slot(s)
        } else {
            slots.push(p.target.clone());
            Out::Slot(slots.len() - 1)
        };
        let inputs: Vec<usize> = p.inputs.iter().map(|s| slots.iter().position(|x| x == s).expect("batched")).collect();
        let level = 1
            + (views.iter())
                .filter(|b| matches!(b.out, Out::Slot(s) if inputs.contains(&s)))
                .map(|b| b.level)
                .max()
                .unwrap_or(0);
        views.push(BView { name, plan: v, inputs, out, ops: vec![], level });
    }
    Ok((slots, views))
}

/// A slot several views wrote, as one batch in the order of its column `clock` (a copy of each
/// writer's source clock), rows of one time in the order written.
fn in_clock_order(slot: &mut Vec<Batch>, clock: usize) {
    slot.retain(|b| b.len > 0);
    if slot.len() < 2 {
        return;
    }
    let b = Batch::concat(slot);
    let c = &b.cols[clock];
    let mut order: Vec<usize> = (0..b.len).collect();
    order.sort_by_key(|r| c.get(*r).i64());
    *slot = vec![b.take(&order)];
}

/// Window views of one level over one input that differ in width alone (each interval's of a
/// pipeline's window: the same arguments, keys and aggregates), each width a multiple of the
/// narrowest's, which comes first: run together (`BWindow::apply_many`).
fn fused_windows(views: &[BView]) -> Vec<(usize, Vec<usize>)> {
    let mut sets: BTreeMap<(usize, Vec<usize>, String), Vec<usize>> = BTreeMap::new();
    // the first view of each run of views of one level, which `chunk` runs side by side: views of
    // one level may come apart (pipelines one after another in one SQL)
    let mut run = 0;
    for (i, v) in views.iter().enumerate() {
        if i > 0 && views[i - 1].level != v.level {
            run = i;
        }
        if let [BOp::Window(w)] = v.ops.as_slice() {
            let what = format!("{:?}|{}|{:?}|{:?}|{:?}", w.prog.signature(), w.ts, w.keys, w.args, w.w.agg_keys);
            sets.entry((run, v.inputs.clone(), what)).or_default().push(i);
        }
    }
    let width = |i: &usize| match &views[*i].ops[0] {
        BOp::Window(w) => w.w.width,
        _ => unreachable!("windows"),
    };
    let mut out = vec![];
    for ((run, ..), mut set) in sets {
        set.sort_by_key(width);
        let narrowest = width(&set[0]);
        set.retain(|i| width(i) % narrowest == 0);
        if set.len() > 1 {
            out.push((run, set));
        }
    }
    out
}

/// `batch`, a view's output, laid out in its target's columns and types (`lay_out`): a column
/// no reader reads is left NULL.
fn lay_out_batch(plan: &Plan, st: &Stream, batch: &Batch, needed: &[bool]) -> Batch {
    let n = batch.len;
    let cols = st.columns.iter().zip(&plan.by_name).zip(needed).map(|((c, i), needed)| match i {
        _ if !needed => Arc::new(Col::new(Data::Const(Value::Null, n))),
        None => Arc::new(Col::new(Data::Const(c.ty.default_value(), n))),
        Some(i) => cast(&batch.cols[*i], &c.ty),
    });
    Batch::new(n, cols.collect())
}

/// A column cast to a column type (`Value::cast`), kept as it is where it already conforms.
fn cast(col: &Arc<Col>, ty: &Type) -> Arc<Col> {
    if *ty == Type::Any {
        return col.clone();
    }
    let nullable = matches!(ty, Type::Nullable(_));
    let conforms = (nullable || col.nulls.is_none())
        && match (&col.data, ty.base()) {
            (Data::F64(_), Type::F64) | (Data::F32(_), Type::F32) | (Data::Bool(_), Type::Bool) => true,
            (Data::Time(_), Type::Time(_)) | (Data::Str(_), Type::Str) => true,
            (Data::Int(_), Type::Int(64)) | (Data::UInt(_), Type::UInt(64)) => true,
            (Data::Const(v, _), _) => v.conforms(ty),
            _ => false,
        };
    if conforms {
        return col.clone();
    }
    if let Data::Const(v, n) = &col.data {
        return Arc::new(Col::new(Data::Const(v.cast(ty), *n)));
    }
    Arc::new(Col::from_values((0..col.len()).map(|r| col.get(r).cast_into(ty)).collect()))
}

/// The window of a view's operators: its first.
fn window_of(ops: &mut [BOp]) -> Option<&mut Window> {
    match ops.first_mut()? {
        BOp::Window(w) => Some(&mut w.w),
        BOp::Rows(ops) => match ops.first_mut()? {
            Op::Window(w) => Some(&mut **w),
            _ => None,
        },
        _ => None,
    }
}

/// Runs a view's operators over its inputs' batches (one list per join side); at the `end`,
/// closes them as an event at the end of time would.
fn run_view(ops: &mut [BOp], inputs: Vec<Vec<Batch>>, end: bool, pool: &dyn Pool) -> R<Flow> {
    let (first, rest) = ops.split_first_mut().expect("a view has an operator");
    let mut flow = match first {
        BOp::Join(j) => Flow::Batches(j.apply(inputs, end, pool)?),
        BOp::Rows(row_ops) => {
            let sides = inputs.len();
            let rows = inputs.into_iter().map(|bs| bs.iter().flat_map(Batch::rows).collect()).collect();
            let mut out = run_rows(row_ops, rows);
            if end {
                out.extend(close(row_ops, &vec![i64::MAX; sides], i64::MAX).0);
            }
            Flow::Rows(out)
        }
        op => apply(op, Flow::Batches(inputs.into_iter().next().unwrap_or_default()), end, pool)?,
    };
    for op in rest {
        flow = apply(op, flow, end, pool)?;
    }
    Ok(flow)
}

/// Windows run together (`BWindow::apply_many`) over their input's batches: each one's rows;
/// at the `end`, closed.
fn windows(ws: &mut [&mut BWindow], input: Vec<Batch>, end: bool, pool: &dyn Pool) -> Vec<Vec<Row>> {
    let batches: Vec<Batch> = input.into_iter().filter(|b| b.len > 0).collect();
    let mut rows = if batches.is_empty() { vec![vec![]; ws.len()] } else { BWindow::apply_many(ws, &batches, pool) };
    if end {
        for (w, rows) in ws.iter_mut().zip(&mut rows) {
            rows.extend(w.w.advance(i64::MAX));
        }
    }
    rows
}

/// One operator over a flow; at the `end`, closed.
fn apply(op: &mut BOp, flow: Flow, end: bool, pool: &dyn Pool) -> R<Flow> {
    match op {
        BOp::Project { .. } | BOp::Order { .. } if flow.is_empty() => Ok(flow),
        BOp::Project { prog, width, filter, items, .. } => {
            // each batch in parts, side by side
            let parts: Vec<Batch> = (flow.batches(*width).iter())
                .flat_map(|b| {
                    parts(b.len, pool).into_iter().map(|r| if r.len() == b.len { b.clone() } else { b.slice(r) })
                })
                .collect();
            let (prog, filter, items) = (&*prog, *filter, &*items);
            let mut out: Vec<Batch> = vec![Batch::default(); parts.len()];
            let project = move |b: &Batch| -> Batch {
                let b = match filter {
                    Some(f) => {
                        let t = truths(&prog.col(&mut prog.eval(b), f));
                        let keep: Vec<usize> = (0..b.len).filter(|&r| t[r] == Some(true)).collect();
                        if keep.len() == b.len {
                            b.clone()
                        } else {
                            b.take(&keep)
                        }
                    }
                    None => b.clone(),
                };
                let mut ev = prog.eval(&b);
                let cols = items.iter().map(|i| match i {
                    Some(i) => prog.col(&mut ev, *i),
                    None => Arc::new(Col::new(Data::Const(Value::Null, b.len))),
                });
                Batch::new(b.len, cols.collect())
            };
            if parts.len() == 1 {
                out[0] = project(&parts[0]);
            } else {
                let project = &project;
                pool.run(
                    parts
                        .iter()
                        .zip(out.iter_mut())
                        .map(|(b, o)| -> Task { Box::new(move || *o = project(b)) })
                        .collect(),
                );
            }
            Ok(Flow::Batches(out.into_iter().filter(|b| b.len > 0).collect()))
        }
        BOp::Order { trusted: true, .. } => Ok(flow),
        BOp::Order { prog, keys, held, last, .. } => {
            let batches: Vec<Batch> = flow.batches(prog.reads.len()).into_iter().filter(|b| b.len > 0).collect();
            let key_cols = |b: &Batch| -> Vec<Arc<Col>> {
                let mut ev = prog.eval(b);
                keys.iter().map(|k| prog.col(&mut ev, *k)).collect()
            };
            let mut cols: Vec<Vec<Arc<Col>>> = batches.iter().map(key_cols).collect();
            // in order within each batch, and from each batch's last row to the next one's first
            let within = cols.iter().all(|c| in_order(c));
            let across = cols.windows(2).all(|w| {
                let n = w[0][0].len();
                order_rows(&w[0], n - 1, &w[1], 0).is_le()
            });
            let batches = if within && across {
                batches
            } else {
                let b = Batch::concat(&batches);
                let c = key_cols(&b);
                let mut order: Vec<usize> = (0..b.len).collect();
                order.sort_by(|x, y| order_rows(&c, *x, &c, *y));
                let b = b.take(&order);
                cols = vec![key_cols(&b)];
                vec![b]
            };
            if *held && !batches.is_empty() {
                let first: Row = cols[0].iter().map(|c| c.get(0)).collect();
                if let Some(l) = last.as_ref() {
                    if l.iter().zip(&first).map(|(x, y)| order_by(x, y)).find(|o| o.is_ne()).is_some_and(|o| o.is_gt())
                    {
                        return Err(format!("rows out of ORDER BY order across chunks: {first:?} after {l:?}"));
                    }
                }
                let (c, n) = (cols.last().expect("a batch"), batches.last().expect("a batch").len);
                *last = Some(c.iter().map(|c| c.get(n - 1)).collect());
            }
            Ok(Flow::Batches(batches))
        }
        BOp::Join(_) => unreachable!("a join is a view's first operator"),
        BOp::Lag(l) if flow.is_empty() => {
            let _ = l;
            Ok(flow)
        }
        BOp::Lag(l) => {
            let batches = flow.batches(l.width);
            Ok(Flow::Batches(batches.iter().filter(|b| b.len > 0).map(|b| l.apply(b)).collect()))
        }
        BOp::Window(w) => {
            let width = w.prog.reads.len();
            let mut rows = vec![];
            if !flow.is_empty() {
                let batches: Vec<Batch> = flow.batches(width).into_iter().filter(|b| b.len > 0).collect();
                rows = w.apply(&batches, pool);
            }
            if end {
                rows.extend(w.w.advance(i64::MAX));
            }
            Ok(Flow::Rows(rows))
        }
        BOp::Rows(ops) => {
            let rows = flow.rows();
            let mut out = if rows.is_empty() { vec![] } else { run(ops, 0, &rows, i64::MAX) };
            if end {
                out.extend(close(ops, &[i64::MAX], i64::MAX).0);
            }
            Ok(Flow::Rows(out))
        }
    }
}

/// Whether a batch's rows are in `ORDER BY` order by its key columns: the first key compared
/// in a loop of its own, the others only where it ties.
fn in_order(cols: &[Arc<Col>]) -> bool {
    // a key of one value orders nothing
    let varied: Vec<Arc<Col>> = cols.iter().filter(|c| !matches!(c.data, Data::Const(..))).cloned().collect();
    let cols = varied.as_slice();
    let Some(first) = cols.first() else { return true };
    let n = first.len();
    match first.i64s() {
        Some(t) => t
            .windows(2)
            .enumerate()
            .all(|(r, w)| w[0] < w[1] || (w[0] == w[1] && order_rows(&cols[1..], r, &cols[1..], r + 1).is_le())),
        None => (1..n).all(|r| order_rows(cols, r - 1, cols, r).is_le()),
    }
}

/// Row `a` of key columns `x` against row `b` of `y`, in `ORDER BY` order (`sorted`): typed
/// for the common keys (a time, then a string or a number).
fn order_rows(x: &[Arc<Col>], a: usize, y: &[Arc<Col>], b: usize) -> Ordering {
    for (c, d) in x.iter().zip(y) {
        if let (Data::Const(u, _), Data::Const(v, _)) = (&c.data, &d.data) {
            match order_by(u, v) {
                Ordering::Equal => continue,
                o => return o,
            }
        }
        let o = match (c.i64s(), d.i64s(), c.strs(), d.strs()) {
            // an integer and a time compare as numbers, as `order_by` compares them
            (Some(u), Some(v), ..) => u[a].cmp(&v[b]),
            (_, _, Some(u), Some(v)) => u.get(a).cmp(v.get(b)),
            _ => order_by(&c.get(a), &d.get(b)),
        };
        if o.is_ne() {
            return o;
        }
    }
    Ordering::Equal
}

/// A group of a window in a chunk: its window start, its key values, and its rows in each batch.
type GroupRows = (i64, Vec<Value>, Vec<(usize, Members)>);

/// Whether an aggregate is a `min` or `max`, alone or of the rows a condition picks.
fn min_or_max(a: &Acc) -> bool {
    match a {
        Acc::Min(_) | Acc::Max(_) => true,
        Acc::If(inner) => min_or_max(inner),
        _ => false,
    }
}

/// Whether an aggregate's argument holds a NaN: a float column's in a loop of its own, any other
/// value by value (a constant, values, rows of two kinds).
fn has_nan(a: &Args<'_>) -> bool {
    let Args::Cols(cols) = a else { return false };
    cols.iter().any(|c| match &c.data {
        Data::F64(v) => v.iter().any(|x| x.is_nan()),
        Data::F32(v) => v.iter().any(|x| x.is_nan()),
        Data::Int(_) | Data::UInt(_) | Data::Time(_) | Data::Bool(_) | Data::Str(_) => false,
        _ => (0..c.len()).any(|r| c.get(r).f64().is_some_and(f64::is_nan)),
    })
}

/// The merges of a block's state into wider windows (tests: that the narrowest's are taken).
#[cfg(test)]
static MERGES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A join side as batch operators: its operators, its stream's width, its columns (qualifier,
/// name).
type Side = (Vec<BOp>, usize, Vec<(Option<String>, String)>);

/// A group's rows in a batch: a run of them, or rows picked out in order.
enum Members {
    Run(usize, usize),
    Pick(Vec<usize>),
}

/// A batch's rows a window takes, and their window starts' runs.
struct Taken {
    /// Each row's time (`i64::MIN` where none).
    times: Vec<i64>,
    /// The rows taken, if not all of them.
    rows: Option<Vec<usize>>,
    /// The keys' columns, and their one value when every row has it.
    keys: Vec<Arc<Col>>,
    constant: Option<Vec<Value>>,
}

impl BWindow {
    /// `Window::apply` over a chunk's batches: the closed groups' rows. With the rows in time
    /// order and one key, each window's rows are a run: groups are added side by side, each by
    /// one job, over its rows of whatever batches they are in.
    fn apply(&mut self, batches: &[Batch], pool: &dyn Pool) -> Vec<Row> {
        let (groups, runs) = self.group(batches);
        let BWindow { w, prog, args, .. } = self;
        let mut accs_of = group_accs(w, &groups, runs);
        add_split(prog, args, batches, &groups, &mut accs_of, runs, pool);
        w.ripe()
    }

    /// Windows that differ in width alone (`fused_windows`), the narrowest first, over the same
    /// batches: each one's closed groups' rows. With every window's rows in runs, each block's
    /// arguments are evaluated once, and added to each window's group of its time: the same rows
    /// in the same order as each window alone, its aggregates' jobs side by side. Else each
    /// window alone.
    fn apply_many(ws: &mut [&mut BWindow], batches: &[Batch], pool: &dyn Pool) -> Vec<Vec<Row>> {
        let grouped: Vec<(Vec<GroupRows>, bool)> = ws.iter_mut().map(|w| w.group(batches)).collect();
        let mut wins: Vec<(&mut Box<Window>, &Program, &[BArgs])> = (ws.iter_mut())
            .map(|bw| {
                let BWindow { w, prog, args, .. } = &mut **bw;
                (w, &*prog, &args[..])
            })
            .collect();
        if wins.len() == 1 || !grouped.iter().all(|(g, runs)| *runs && !g.is_empty()) {
            return (wins.iter_mut())
                .zip(&grouped)
                .map(|((w, prog, args), (groups, runs))| {
                    let mut accs_of = group_accs(w, groups, *runs);
                    add_split(prog, args, batches, groups, &mut accs_of, *runs, pool);
                    w.ripe()
                })
                .collect();
        }
        let (prog, args) = (wins[0].1, wins[0].2);
        let base = &grouped[0].0;
        // the aggregates the wider windows take from the narrowest's (ADR-0017): each block's
        // rows folded once more, into a state of their own, merged into the wider windows' groups
        let fresh: Vec<Option<Acc>> = (super::accs(&wins[0].0.aggs, &wins[0].0.shares).expect("planned"))
            .into_iter()
            .map(|a| a.mergeable().then_some(a))
            .collect();
        let fresh = &fresh;
        // each narrowest window's group, in each window: the group of its start
        let into: Vec<Vec<usize>> = (wins.iter().zip(&grouped))
            .map(|((w, ..), (groups, _))| {
                let mut g = 0;
                (base.iter())
                    .map(|(start, _, _)| {
                        while groups[g].0 != floor(*start, w.width) {
                            g += 1;
                        }
                        g
                    })
                    .collect()
            })
            .collect();
        {
            let mut accs: Vec<Vec<&mut Vec<Acc>>> =
                (wins.iter_mut().zip(&grouped)).map(|((w, ..), (groups, runs))| group_accs(w, groups, *runs)).collect();
            // the aggregates in about as many parts as there are threads, each part's of every
            // window and group: (part, window, group)
            let rows: usize =
                (base.iter().flat_map(|g| &g.2)).map(|(_, m)| if let Members::Run(a, z) = m { z - a } else { 0 }).sum();
            let n = args.len();
            let k = if parts(rows, pool).len() > 1 { pool.threads().clamp(1, n.max(1)) } else { 1 };
            let ranges: Vec<std::ops::Range<usize>> = (0..k).map(|t| t * n / k..(t + 1) * n / k).collect();
            let mut shares: Vec<Vec<Vec<&mut [Acc]>>> = (0..k).map(|_| vec![]).collect();
            for window in accs.iter_mut() {
                let mut per: Vec<Vec<&mut [Acc]>> = (0..k).map(|_| vec![]).collect();
                for group in window.iter_mut() {
                    let mut rest: &mut [Acc] = &mut group[..];
                    for (t, r) in ranges.iter().enumerate() {
                        let (mine, others) = std::mem::take(&mut rest).split_at_mut(r.len());
                        per[t].push(mine);
                        rest = others;
                    }
                }
                for (t, p) in per.into_iter().enumerate() {
                    shares[t].push(p);
                }
            }
            let into = &into;
            let add = move |r: std::ops::Range<usize>, mut accs: Vec<Vec<&mut [Acc]>>| {
                for (bg, (_, _, members)) in base.iter().enumerate() {
                    for (bi, m) in members {
                        let Members::Run(a, z) = m else { unreachable!("runs") };
                        let mut at = *a;
                        while at < *z {
                            let end = (at + BLOCK).min(*z);
                            let block = batches[*bi].slice(at..end);
                            let mut ev = prog.eval(&block);
                            for i in r.clone() {
                                let cols: Vec<Arc<Col>> = match &args[i] {
                                    BArgs::Cols(c) | BArgs::Tuple(c) => {
                                        c.iter().map(|x| prog.col(&mut ev, *x)).collect()
                                    }
                                    BArgs::Keyed(x, key) => {
                                        std::iter::once(x).chain(key).map(|x| prog.col(&mut ev, *x)).collect()
                                    }
                                };
                                let cols: Vec<&Col> = cols.iter().map(|c| &**c).collect();
                                let a = match &args[i] {
                                    BArgs::Cols(_) => Args::Cols(cols),
                                    BArgs::Keyed(..) => Args::Keyed(cols[0], cols[1..].to_vec()),
                                    BArgs::Tuple(_) => Args::Tuple(cols),
                                };
                                let rows = Picked::Range(0, end - at);
                                match &fresh[i] {
                                    // a sequence of trades: the wider windows that hold trades hold the one
                                    // just before this block, whose price its first return is on
                                    Some(f) if f.trades().is_some() => {
                                        let (narrow, wider) = accs.split_at_mut(1);
                                        let narrow = &mut narrow[0][into[0][bg]][i - r.start];
                                        add_rows(narrow, &a, rows);
                                        let first = match &a {
                                            Args::Tuple(c) => c[0].get(0).i64(),
                                            _ => None,
                                        };
                                        let held = (wider.iter().enumerate())
                                            .map(|(j, w)| &w[into[j + 1][bg]][i - r.start])
                                            .find(|w| w.trades().is_some_and(|(held, _)| held));
                                        // trades of one time on both sides of the block: in order together, row by row
                                        let block = match held {
                                            Some(w) if w.trades().map(|(_, newest)| Some(newest)) == Some(first) => {
                                                None
                                            }
                                            Some(w) => {
                                                let mut b = w.seeded(f);
                                                add_rows(&mut b, &a, rows);
                                                Some(Some(b))
                                            }
                                            None => Some(None),
                                        };
                                        for (j, window) in wider.iter_mut().enumerate() {
                                            let w = &mut window[into[j + 1][bg]][i - r.start];
                                            match &block {
                                                None => add_rows(w, &a, rows),
                                                // a window this block starts: the narrowest's, which it also starts
                                                _ if !w.trades().is_some_and(|(held, _)| held) => *w = narrow.clone(),
                                                Some(Some(b)) => w.merge(b),
                                                Some(None) => unreachable!("a window that holds trades was found"),
                                            }
                                        }
                                    }
                                    // a NaN first holds a min or max: what came after it is not in the state
                                    Some(f) if !(min_or_max(f) && has_nan(&a)) => {
                                        let mut block = f.clone();
                                        add_rows(&mut block, &a, Picked::Range(0, end - at));
                                        add_rows(
                                            &mut accs[0][into[0][bg]][i - r.start],
                                            &a,
                                            Picked::Range(0, end - at),
                                        );
                                        for (j, window) in accs.iter_mut().enumerate().skip(1) {
                                            window[into[j][bg]][i - r.start].merge(&block);
                                            #[cfg(test)]
                                            MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                    }
                                    _ => {
                                        for (j, window) in accs.iter_mut().enumerate() {
                                            add_rows(
                                                &mut window[into[j][bg]][i - r.start],
                                                &a,
                                                Picked::Range(0, end - at),
                                            );
                                        }
                                    }
                                }
                            }
                            at = end;
                        }
                    }
                }
            };
            let add = &add;
            let work: Vec<Task> = (ranges.into_iter().zip(shares))
                .map(|(r, share)| -> Task { Box::new(move || add(r, share)) })
                .collect();
            if work.len() == 1 {
                work.into_iter().for_each(|j| j());
            } else {
                pool.run(work);
            }
        }
        wins.into_iter().map(|(w, ..)| w.ripe()).collect()
    }

    /// The rows a chunk's batches give the window, as its groups (each made, with its
    /// accumulators, if new), in order; and whether they are runs (one key, every row taken, in
    /// time order).
    fn group(&mut self, batches: &[Batch]) -> (Vec<GroupRows>, bool) {
        let BWindow { w, prog, ts, keys, .. } = self;
        // the rows each batch gives the window (`Window::apply`: with a time, not behind the
        // watermark it had before the chunk)
        let prev = w.watermark;
        let taken: Vec<Taken> = batches
            .iter()
            .map(|b| {
                let mut ev = prog.eval(b);
                let t = prog.col(&mut ev, *ts);
                let key_cols: Vec<Arc<Col>> = keys.iter().map(|k| prog.col(&mut ev, *k)).collect();
                let constant = key_cols.iter().map(|k| k.constant()).collect();
                // one time in every row (a window over every row: its time is a constant)
                let same: Option<Vec<i64>> = match &t.data {
                    Data::Const(v, n) if t.nulls.is_none() => v.i64().filter(|t0| *t0 >= prev).map(|t0| vec![t0; *n]),
                    _ => None,
                };
                let all = same.as_deref().or_else(|| {
                    t.i64s().filter(|t| t.first().is_some_and(|t0| *t0 >= prev) && t.windows(2).all(|w| w[0] <= w[1]))
                });
                match all {
                    Some(t) => {
                        w.max_ts = w.max_ts.max(t[t.len() - 1]);
                        Taken { times: t.to_vec(), rows: None, keys: key_cols, constant }
                    }
                    None => {
                        let mut rows = Vec::with_capacity(b.len);
                        let times = (0..b.len)
                            .map(|r| match t.get(r).i64() {
                                None => {
                                    w.dropped.null_time += 1;
                                    i64::MIN
                                }
                                Some(x) if x < prev => {
                                    w.late += 1;
                                    x
                                }
                                Some(x) => {
                                    w.max_ts = w.max_ts.max(x);
                                    rows.push(r);
                                    x
                                }
                            })
                            .collect();
                        Taken { times, rows: Some(rows), keys: key_cols, constant }
                    }
                }
            })
            .collect();
        // one key in every batch, every row taken, in time order across them: runs
        let one = taken.first().and_then(|t| t.constant.clone());
        let runs = one.is_some()
            && taken.iter().all(|t| t.rows.is_none() && t.constant == one)
            && taken.windows(2).all(|p| p[0].times.last() <= p[1].times.first());
        // the groups, in order, and their members: (batch, rows)
        let mut groups: Vec<GroupRows> = vec![];
        if runs {
            let key = one.expect("checked");
            for (bi, t) in taken.iter().enumerate() {
                let mut at = 0;
                while at < t.times.len() {
                    let start = floor(t.times[at], w.width);
                    let n = t.times[at..].partition_point(|x| *x < start.saturating_add(w.width));
                    match groups.last_mut() {
                        Some(g) if g.0 == start => g.2.push((bi, Members::Run(at, at + n))),
                        _ => groups.push((start, key.clone(), vec![(bi, Members::Run(at, at + n))])),
                    }
                    at += n;
                }
            }
        } else {
            let mut by: BTreeMap<(i64, String), usize> = BTreeMap::new();
            for (bi, t) in taken.iter().enumerate() {
                let all: Vec<usize>;
                let rows = match &t.rows {
                    Some(r) => r,
                    None => {
                        all = (0..t.times.len()).collect();
                        &all
                    }
                };
                let mut picks: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
                // one key column of text or integers without NULLs: rows by the key's own value,
                // each key's `Value` and text made once per batch, not once per row
                let single = match (&t.constant, t.keys.as_slice()) {
                    (None, [k]) if k.nulls.is_none() => match &k.data {
                        Data::Str(_) | Data::Int(_) => Some(k),
                        _ => None,
                    },
                    _ => None,
                };
                if let Some(k) = single {
                    let key_of = |r: usize| match &k.data {
                        Data::Str(s) => KeyRef::Str(s.bytes(r)),
                        Data::Int(v) => KeyRef::Int(v[r]),
                        _ => unreachable!("checked above"),
                    };
                    // each (window, key)'s rows, in the order they first come
                    let mut order: Vec<((i64, KeyRef), Vec<usize>)> = vec![];
                    let first = rows.first().map(|&r| floor(t.times[r], w.width));
                    if rows.iter().all(|&r| Some(floor(t.times[r], w.width)) == first) {
                        // one window (a window over every row): the key alone, hashed once a row, or
                        // looked up by its dictionary entry
                        let start = first.unwrap_or_default();
                        let codes = match &k.data {
                            Data::Str(s) => s.codes(),
                            _ => None,
                        };
                        let mut at: FxHashMap<KeyRef, usize> = FxHashMap::default();
                        let mut by_code: Vec<usize> = vec![];
                        for &r in rows.iter().filter(|_| codes.is_some()) {
                            let c = codes.expect("filtered")[r] as usize;
                            if by_code.len() <= c {
                                by_code.resize(c + 1, usize::MAX);
                            }
                            if by_code[c] == usize::MAX {
                                order.push(((start, key_of(r)), vec![]));
                                by_code[c] = order.len() - 1;
                            }
                            order[by_code[c]].1.push(r);
                        }
                        for &r in rows.iter().filter(|_| codes.is_none()) {
                            let kr = key_of(r);
                            let i = *at.entry(kr).or_insert_with(|| {
                                order.push(((start, kr), vec![]));
                                order.len() - 1
                            });
                            order[i].1.push(r);
                        }
                    } else {
                        let mut at: FxHashMap<(i64, KeyRef), usize> = FxHashMap::default();
                        for &r in rows {
                            let sk = (floor(t.times[r], w.width), key_of(r));
                            let i = *at.entry(sk).or_insert_with(|| {
                                order.push((sk, vec![]));
                                order.len() - 1
                            });
                            order[i].1.push(r);
                        }
                    }
                    for ((start, kr), rs) in order {
                        let key = vec![match kr {
                            KeyRef::Str(s) => Value::Str(std::str::from_utf8(s).expect("a string's bytes").into()),
                            KeyRef::Int(i) => Value::Int(i),
                        }];
                        key_text(&mut w.text, key.iter());
                        let g = *by.entry((start, w.text.clone())).or_insert_with(|| {
                            groups.push((start, key, vec![]));
                            groups.len() - 1
                        });
                        picks.entry(g).or_default().extend(rs);
                    }
                    for (g, mut rows) in picks {
                        rows.sort_unstable();
                        groups[g].2.push((bi, Members::Pick(rows)));
                    }
                    continue;
                }
                for &r in rows {
                    let key: Vec<Value> = match &t.constant {
                        Some(k) => k.clone(),
                        None => t.keys.iter().map(|k| k.get(r)).collect(),
                    };
                    key_text(&mut w.text, key.iter());
                    let start = floor(t.times[r], w.width);
                    let g = *by.entry((start, w.text.clone())).or_insert_with(|| {
                        groups.push((start, key, vec![]));
                        groups.len() - 1
                    });
                    picks.entry(g).or_default().push(r);
                }
                for (g, rows) in picks {
                    groups[g].2.push((bi, Members::Pick(rows)));
                }
            }
        }
        // every group there, then each group's accumulators, in the groups' order
        for (start, key, _) in &groups {
            key_text(&mut w.text, key.iter());
            let g = w.open.entry(*start).or_default();
            if !g.contains_key(w.text.as_str()) {
                g.insert(w.text.clone(), (key.clone(), accs(&w.aggs, &w.shares).expect("checked at plan time")));
            }
        }
        (groups, runs)
    }
}

/// A row's key of one text or integer column, borrowed from its batch (`BWindow::group`).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum KeyRef<'a> {
    Str(&'a [u8]),
    Int(i64),
}

/// Each of `groups`' accumulators in `w`, in their order.
fn group_accs<'w>(w: &'w mut Window, groups: &[GroupRows], runs: bool) -> Vec<&'w mut Vec<Acc>> {
    let texts: Vec<String> = (groups.iter())
        .map(|(_, key, _)| {
            let mut t = String::new();
            key_text(&mut t, key.iter());
            t
        })
        .collect();
    let mut accs_of: Vec<&mut Vec<Acc>> = Vec::with_capacity(groups.len());
    if runs {
        // one key, one group per window start, in start order
        if let Some(first) = groups.first() {
            let mut open = w.open.range_mut(first.0..);
            for (start, _, _) in groups {
                let (_, g) = open.find(|(s, _)| *s == start).expect("made above");
                accs_of.push(&mut g.get_mut(texts[0].as_str()).expect("made above").1);
            }
        }
    } else {
        let mut by: BTreeMap<(i64, &str), usize> = BTreeMap::new();
        for (i, (start, _, _)) in groups.iter().enumerate() {
            by.insert((*start, texts[i].as_str()), i);
        }
        let mut found: Vec<Option<&mut Vec<Acc>>> = (0..groups.len()).map(|_| None).collect();
        for (start, g) in w.open.iter_mut() {
            for (text, (_, accs)) in g.iter_mut() {
                if let Some(&i) = by.get(&(*start, text.as_str())) {
                    found[i] = Some(accs);
                }
            }
        }
        accs_of.extend(found.into_iter().map(|a| a.expect("made above")));
    }
    accs_of
}

/// `groups`' rows of `batches` into their accumulators (`accs_of`, in their order), in blocks of
/// at most `BLOCK` rows; with runs, groups side by side, each by one job.
fn add_split(
    prog: &Program,
    args: &[BArgs],
    batches: &[Batch],
    groups: &[GroupRows],
    accs_of: &mut [&mut Vec<Acc>],
    runs: bool,
    pool: &dyn Pool,
) {
    // a run of groups' rows into their accumulators, in blocks of at most `BLOCK` rows of a
    // batch: the arguments' columns of a block at a time, which a cache holds
    let add = |groups: &[GroupRows], accs: &mut [&mut Vec<Acc>]| {
        let eval = |b: &Batch| -> Vec<Vec<Arc<Col>>> {
            let mut ev = prog.eval(b);
            (args.iter())
                .map(|x| match x {
                    BArgs::Cols(c) | BArgs::Tuple(c) => c.iter().map(|i| prog.col(&mut ev, *i)).collect(),
                    BArgs::Keyed(x, key) => std::iter::once(x).chain(key).map(|i| prog.col(&mut ev, *i)).collect(),
                })
                .collect()
        };
        let into = |accs: &mut Vec<Acc>, cols: &[Vec<Arc<Col>>], rows: Picked| {
            for (acc, (spec, cols)) in accs.iter_mut().zip(args.iter().zip(cols)) {
                let cols: Vec<&Col> = cols.iter().map(|c| &**c).collect();
                let a = match spec {
                    BArgs::Cols(_) => Args::Cols(cols),
                    BArgs::Keyed(..) => Args::Keyed(cols[0], cols[1..].to_vec()),
                    BArgs::Tuple(_) => Args::Tuple(cols),
                };
                add_rows(acc, &a, rows);
            }
        };
        // a batch's arguments, evaluated once for every group that picks rows of it
        let mut picked: Vec<Option<Vec<Vec<Arc<Col>>>>> = vec![None; batches.len()];
        for ((_, _, members), accs) in groups.iter().zip(accs.iter_mut()) {
            for (bi, m) in members {
                let b = &batches[*bi];
                match m {
                    Members::Run(a, z) => {
                        let mut at = *a;
                        while at < *z {
                            let end = (at + BLOCK).min(*z);
                            let cols = eval(&b.slice(at..end));
                            into(accs, &cols, Picked::Range(0, end - at));
                            at = end;
                        }
                    }
                    // rows picked out of a batch: over the batch
                    Members::Pick(p) => {
                        let cols = picked[*bi].get_or_insert_with(|| eval(b));
                        into(accs, cols, Picked::Pick(p))
                    }
                }
            }
        }
    };
    let rows: usize =
        (groups.iter().flat_map(|g| &g.2)).map(|(_, m)| if let Members::Run(a, z) = m { z - a } else { 0 }).sum();
    let k = if runs { parts(rows, pool).len() } else { 1 };
    if k == 1 || groups.len() == 1 {
        add(groups, accs_of);
    } else {
        // runs of groups of about equal rows, each a job
        let size = |g: &GroupRows| -> usize {
            g.2.iter().map(|(_, m)| if let Members::Run(a, z) = m { z - a } else { 0 }).sum()
        };
        let add = &add;
        let mut rest: &mut [&mut Vec<Acc>] = accs_of;
        let (mut work, mut from, mut filled): (Vec<Task>, usize, usize) = (vec![], 0, 0);
        for (i, g) in groups.iter().enumerate() {
            filled += size(g);
            if i + 1 == groups.len() || filled >= rows / k {
                let (mine, others) = std::mem::take(&mut rest).split_at_mut(i + 1 - from);
                rest = others;
                let run = &groups[from..=i];
                work.push(Box::new(move || add(run, mine)));
                (from, filled) = (i + 1, 0);
            }
        }
        pool.run(work);
    }
}

impl BJoin {
    /// One chunk of each side (left first): the left rows joined.
    fn apply(&mut self, inputs: Vec<Vec<Batch>>, end: bool, pool: &dyn Pool) -> R<Vec<Batch>> {
        let mut sides = vec![];
        for ((ops, width, out), input) in self.sides.iter_mut().zip(inputs) {
            let mut flow = Flow::Batches(input);
            for op in ops.iter_mut() {
                flow = apply(op, flow, end, pool)?;
            }
            let b = Batch::concat(&flow.batches(*width).into_iter().filter(|b| b.len > 0).collect::<Vec<_>>());
            sides.push(if b.cols.is_empty() { empty(*out) } else { b });
        }
        let mut sides = sides.into_iter();
        let mut joined = sides.next().expect("a left side");
        for (r, side) in self.rights.iter_mut().zip(sides) {
            joined = r.join(joined, &side)?;
        }
        Ok(if joined.len == 0 { vec![] } else { vec![joined] })
    }
}

/// A batch of no rows, `width` columns wide.
fn empty(width: usize) -> Batch {
    Batch::new(0, (0..width).map(|_| Arc::new(Col::new(Data::Const(Value::Null, 0)))).collect())
}

impl BRight {
    /// `left` (the rows joined so far) with this side's matching row's columns after its own.
    fn join(&mut self, left: Batch, right: &Batch) -> R<Batch> {
        let mut lev = self.left.eval(&left);
        let lts = self.left.col(&mut lev, self.left_ts);
        let lkeys: Vec<Arc<Col>> = self.left_keys.iter().map(|k| self.left.col(&mut lev, *k)).collect();
        let mut rev = self.right.eval(right);
        let rts = self.right.col(&mut rev, self.ts);
        let rkeys: Vec<Arc<Col>> = self.keys.iter().map(|k| self.right.col(&mut rev, *k)).collect();
        let lt = times(&lts, &mut self.left_max, "left")?;
        let rt = times(&rts, &mut self.right_max, "right")?;
        // a left row without a time is dropped (`Dropped::admit`); a right one is no version
        let (left, lkeys, lt) = match &lt {
            Times::All(_) => (left, lkeys, lt),
            Times::Some(t) => {
                let kept: Vec<usize> = (0..left.len).filter(|&r| t[r].is_some()).collect();
                self.null_time += (left.len - kept.len()) as u64;
                let lkeys = lkeys.iter().map(|k| Arc::new(k.take(&kept))).collect();
                let lt = Times::All(Cow::Owned(kept.iter().map(|&r| t[r].expect("kept")).collect()));
                (left.take(&kept), lkeys, lt)
            }
        };
        let Times::All(lt) = lt else { unreachable!("kept") };
        // each left row's key text, and its match among this chunk's versions of its key
        let mut text = String::new();
        let constant = |cols: &[Arc<Col>]| cols.iter().map(|c| c.constant()).collect::<Option<Vec<Value>>>();
        let mut matches: Vec<Option<usize>> = Vec::with_capacity(left.len);
        let keys: Vec<String>;
        // the last version of each key in this chunk
        let mut last: FxHashMap<String, usize> = FxHashMap::default();
        match (constant(&lkeys), constant(&rkeys), &rt) {
            (Some(lk), Some(rk), Times::All(rt)) => {
                // one key on each side, every right row a version: the last right row at or
                // before each left time, by its rank among the right times
                key_text(&mut text, lk.iter());
                let mine = text.clone();
                key_text(&mut text, rk.iter());
                let same = text == mine;
                matches.extend(lt.iter().map(|t| {
                    let at = if same { rt.partition_point(|x| x <= t) } else { 0 };
                    at.checked_sub(1)
                }));
                keys = vec![mine];
                if right.len > 0 {
                    last.insert(text.clone(), right.len - 1);
                }
            }
            _ => {
                let rt = |r: usize| match &rt {
                    Times::All(t) => Some(t[r]),
                    Times::Some(t) => t[r],
                };
                // keys as dense ids for this chunk: a key's text made once per distinct key
                let mut ids: FxHashMap<String, usize> = FxHashMap::default();
                let mut names: Vec<String> = vec![];
                let mut id_of = |cols: &[Arc<Col>], r: usize, text: &mut String| -> usize {
                    key_text(text, cols.iter().map(|c| c.get(r)));
                    if let Some(&i) = ids.get(text.as_str()) {
                        return i;
                    }
                    names.push(text.clone());
                    ids.insert(text.clone(), names.len() - 1);
                    names.len() - 1
                };
                // one text column without NULLs: the id by the string's bytes, no value made (nor a
                // `str`, whose slicing checks its ends are on characters)
                let fast = |cols: &[Arc<Col>]| match cols {
                    [c] if c.nulls.is_none() => match &c.data {
                        Data::Str(s) => Some(s.clone()),
                        _ => None,
                    },
                    _ => None,
                };
                let mut by: Vec<Vec<usize>> = vec![];
                {
                    let mut seen: FxHashMap<&[u8], usize> = FxHashMap::default();
                    let rs = fast(&rkeys);
                    for r in (0..right.len).filter(|&r| rt(r).is_some()) {
                        let id = match &rs {
                            Some(s) => match seen.get(s.bytes(r)) {
                                Some(&i) => i,
                                None => {
                                    let i = id_of(&rkeys, r, &mut text);
                                    seen.insert(s.bytes(r), i);
                                    i
                                }
                            },
                            None => id_of(&rkeys, r, &mut text),
                        };
                        if by.len() <= id {
                            by.resize(id + 1, vec![]);
                        }
                        by[id].push(r);
                    }
                }
                let mut cursors: Vec<usize> = vec![];
                let mut key_ids = Vec::with_capacity(left.len);
                {
                    let mut seen: FxHashMap<&[u8], usize> = FxHashMap::default();
                    let ls = fast(&lkeys);
                    for (i, &t) in lt.iter().enumerate() {
                        let id = match &ls {
                            Some(s) => match seen.get(s.bytes(i)) {
                                Some(&k) => k,
                                None => {
                                    let k = id_of(&lkeys, i, &mut text);
                                    seen.insert(s.bytes(i), k);
                                    k
                                }
                            },
                            None => id_of(&lkeys, i, &mut text),
                        };
                        if cursors.len() <= id {
                            cursors.resize(id + 1, 0);
                        }
                        let rows = by.get(id).map_or(&[][..], Vec::as_slice);
                        let at = &mut cursors[id];
                        while *at < rows.len() && rt(rows[*at]).is_some_and(|x| x <= t) {
                            *at += 1;
                        }
                        matches.push(at.checked_sub(1).map(|j| rows[j]));
                        key_ids.push(id);
                    }
                }
                // a left row with no version in this chunk takes its key's newest before it,
                // or the defaults: one row each after the chunk's right rows, gathered as they are
                let mut extra: Vec<Row> = vec![];
                let mut fill_at: FxHashMap<usize, usize> = FxHashMap::default();
                let idx: Vec<usize> = matches
                    .iter()
                    .zip(&key_ids)
                    .map(|(m, id)| match m {
                        Some(r) => *r,
                        None => *fill_at.entry(*id).or_insert_with(|| {
                            extra.push(
                                self.newest.get(names[*id].as_str()).cloned().unwrap_or_else(|| self.defaults.clone()),
                            );
                            right.len + extra.len() - 1
                        }),
                    })
                    .collect();
                for (id, rows) in by.iter().enumerate() {
                    if let Some(r) = rows.last() {
                        last.insert(names[id].clone(), *r);
                    }
                }
                let combined = if extra.is_empty() {
                    right.clone()
                } else {
                    Batch::concat(&[right.clone(), Batch::from_rows(&extra, right.cols.len())])
                };
                let mut cols = left.cols.clone();
                cols.extend(combined.cols.iter().map(|c| Arc::new(c.take(&idx))));
                for (k, r) in last {
                    self.newest.insert(k, right.row(r));
                }
                return Ok(Batch::new(left.len, cols));
            }
        }
        // one key: a left row with no version in this chunk takes the newest before it, or the
        // defaults
        let mut cols = left.cols.clone();
        for j in 0..right.cols.len() {
            let fill = self.newest.get(keys[0].as_str()).map_or_else(|| self.defaults[j].clone(), |row| row[j].clone());
            cols.push(Arc::new(right.cols[j].gather(&matches, &fill)));
        }
        // this chunk's newest version of each key, for the left rows to come
        for (k, r) in last {
            self.newest.insert(k, right.row(r));
        }
        Ok(Batch::new(left.len, cols))
    }
}

/// A join time column's values: all of them, or each one if any is NULL.
enum Times<'a> {
    All(Cow<'a, [i64]>),
    Some(Vec<Option<i64>>),
}

/// A join time column's values, checked not to go back from `max`, which moves.
fn times<'a>(c: &'a Col, max: &mut i64, side: &str) -> R<Times<'a>> {
    let back = |t: i64, max: i64| Err(format!("the ASOF join's {side} time goes back: {t} after {max}"));
    if let Some(v) = c.i64s() {
        if let Some(w) = v.windows(2).find(|w| w[1] < w[0]) {
            return back(w[1], w[0]);
        }
        if let Some(&t) = v.first().filter(|t| **t < *max) {
            return back(t, *max);
        }
        *max = v.last().copied().unwrap_or(*max).max(*max);
        return Ok(Times::All(Cow::Borrowed(v)));
    }
    let t: Vec<Option<i64>> = (0..c.len()).map(|r| c.get(r).i64()).collect();
    for t in t.iter().flatten() {
        if *t < *max {
            return back(*t, *max);
        }
        *max = *t;
    }
    Ok(match t.iter().copied().collect::<Option<Vec<i64>>>() {
        Some(all) => Times::All(Cow::Owned(all)),
        None => Times::Some(t),
    })
}

impl BLag {
    /// The rows WHERE keeps, with each call's column after the input's.
    fn apply(&mut self, b: &Batch) -> Batch {
        let b = match self.filter {
            Some(f) => {
                let t = truths(&self.prog.col(&mut self.prog.eval(b), f));
                let keep: Vec<usize> = (0..b.len).filter(|&r| t[r] == Some(true)).collect();
                if keep.len() == b.len {
                    b.clone()
                } else {
                    b.take(&keep)
                }
            }
            None => b.clone(),
        };
        let n = b.len;
        let mut ev = self.prog.eval(&b);
        let args: Vec<Arc<Col>> = self.calls.iter().map(|c| self.prog.col(&mut ev, c.0)).collect();
        let mut out: Vec<Option<Arc<Col>>> = vec![None; self.calls.len()];
        for g in &mut self.groups {
            let keys: Vec<Arc<Col>> = g.keys.iter().map(|k| self.prog.col(&mut ev, *k)).collect();
            // runs of one partition
            let mut runs: Vec<(usize, usize, Vec<Value>)> = vec![];
            match keys.iter().map(|k| k.constant()).collect::<Option<Vec<Value>>>() {
                Some(key) => runs.push((0, n, key)),
                None => {
                    for r in 0..n {
                        let key: Vec<Value> = keys.iter().map(|k| k.get(r)).collect();
                        match runs.last_mut() {
                            Some(run) if run.2 == key => run.1 = r + 1,
                            _ => runs.push((r, r + 1, key)),
                        }
                    }
                }
            }
            let mut parts: Vec<Vec<Col>> = vec![vec![]; g.calls.len()];
            let mut text = String::new();
            for (a, z, key) in runs {
                key_text(&mut text, key.iter());
                if !g.parts.contains_key(text.as_str()) {
                    if g.parts.len() >= crate::over::MAX_PARTITIONS {
                        // `over::Group::evict`
                        let k = (g.parts.len() / 10).max(1);
                        let mut by_use: Vec<(u64, String)> = g.parts.iter().map(|(t, p)| (p.0, t.clone())).collect();
                        by_use.sort_unstable();
                        by_use[..k].iter().for_each(|(_, t)| drop(g.parts.remove(t)));
                    }
                    g.parts.insert(text.clone(), (0, vec![VecDeque::new(); g.calls.len()]));
                }
                g.used += (z - a) as u64;
                let part = g.parts.get_mut(text.as_str()).expect("inserted above");
                part.0 = g.used;
                for (j, &c) in g.calls.iter().enumerate() {
                    let (_, offset, default) = &self.calls[c];
                    parts[j].push(lagged(&args[c].slice(a..z), &mut part.1[j], *offset, default));
                }
            }
            for (j, &c) in g.calls.iter().enumerate() {
                out[c] = Some(Arc::new(Col::concat(&parts[j].iter().collect::<Vec<_>>())));
            }
        }
        let mut cols = b.cols.clone();
        cols.extend(out.into_iter().map(|c| c.expect("every call in a group")));
        Batch::new(n, cols)
    }
}

/// `lag(x, offset, default)` over `x`'s rows of one partition: each row's value `offset` rows
/// back, the partition's last values (`last`) before these rows; `last` then holds this run's.
fn lagged(x: &Col, last: &mut VecDeque<Value>, offset: usize, default: &Value) -> Col {
    let n = x.len();
    // the rows before: from the partition's last values, or the default before there are enough
    let k = n.min(offset);
    let head: Vec<Value> = (0..k)
        .map(|r| match (last.len() + r).checked_sub(offset) {
            Some(i) => last[i].clone(),
            None => default.clone(),
        })
        .collect();
    let col = if n > offset {
        Col::concat(&[&Col::from_values(head), &x.slice(0..n - offset)])
    } else {
        Col::from_values(head)
    };
    // the last `offset` values, oldest first
    for r in n.saturating_sub(offset)..n {
        last.push_back(x.get(r));
    }
    while last.len() > offset {
        last.pop_front();
    }
    col
}

/// The batch operators of a view, from its SQL and its engine operators: columnar where the
/// view is a SELECT over one stream (a projection, its ORDER BY) or a tumbling window over one,
/// the engine's operators otherwise.
fn columnar(cat: &Catalog, view: &View, ops: Vec<Op>) -> R<Vec<BOp>> {
    let q = &view.query;
    let rows = |ops: Vec<Op>| Ok(vec![BOp::Rows(ops)]);
    let SetExpr::Select(s) = q.body.as_ref() else { return rows(ops) };
    let [from] = s.from.as_slice() else { return rows(ops) };
    if q.with.is_some() || q.order_by.is_some() && !from.joins.is_empty() {
        return rows(ops);
    }
    if !from.joins.is_empty() {
        // planned again by the engine for the fallback: the join's own operators are moved
        let fallback = || -> R<Vec<Op>> {
            let mut ops = Planner { cat, view }.query(&view.query, &HashMap::new())?.ops;
            // exact, as `Historical::new` sets the engine's joins
            fn exact(ops: &mut [Op]) {
                for op in ops {
                    if let Op::Join(j) = op {
                        j.exact = true;
                        j.sides.iter_mut().for_each(|s| exact(s));
                    }
                }
            }
            exact(&mut ops);
            Ok(ops)
        };
        return match join_view(cat, s, from, ops) {
            Ok(b) => Ok(b),
            Err(_) => rows(fallback()?),
        };
    }
    match &from.relation {
        TableFactor::Table { name, args: None, alias, .. } if cat.streams.contains_key(&name.to_string()) => {
            let over = matches!(ops.first(), Some(Op::Over(_)));
            let shape = matches!(
                &ops[usize::from(over)..],
                [Op::Project { .. }] | [Op::Project { .. }, Op::Sort(_) | Op::Hold(_)]
            );
            if !shape {
                return rows(ops);
            }
            match projection(cat, q, s, &name.to_string(), alias.as_ref(), &ops) {
                Ok((out, _)) => Ok(out),
                // a window function or an expression with no columnar form beside one
                Err(_) if over => rows(ops),
                Err(e) => Err(e),
            }
        }
        TableFactor::Table { name, args: Some(a), .. } if name.to_string().eq_ignore_ascii_case("tumble") => {
            let mut ops = ops;
            let Some(Op::Window(w)) = ops.first() else { return rows(ops) };
            let [src, ts, _] = a.args.as_slice() else { return rows(ops) };
            let GroupByExpr::Expressions(group, _) = &s.group_by else { return rows(ops) };
            if w.digests {
                return rows(ops);
            }
            let scope = Schema::of(&cat.streams[&name_of(arg(src)?)]).scope();
            let mut prog = Program::new(scope.cols.len());
            let ts = VCompiler::new(&scope).expr(&mut prog, arg(ts)?)?;
            let keys = group
                .iter()
                .filter(|k| !matches!(name_of(k).as_str(), "window_start" | "window_end"))
                .map(|k| VCompiler::new(&scope).expr(&mut prog, k))
                .collect::<R<Vec<_>>>()?;
            // the aggregates' arguments, in the order the window keeps their accumulators
            let calls = aggregate_calls(s)?;
            if calls.iter().map(|c| c.0.as_str()).ne(w.agg_keys.iter().map(String::as_str)) {
                return rows(ops);
            }
            let mut args = vec![];
            for (_, name, exprs) in &calls {
                let mut nodes =
                    |es: &[Expr]| es.iter().map(|e| VCompiler::new(&scope).expr(&mut prog, e)).collect::<R<_>>();
                args.push(match (crate::agg::tuple_items(name), exprs.as_slice()) {
                    (Some(_), [Expr::Tuple(items)]) => BArgs::Tuple(nodes(items)?),
                    (None, [x, Expr::Tuple(key)]) => BArgs::Keyed(nodes(std::slice::from_ref(x))?[0], nodes(key)?),
                    _ => BArgs::Cols(nodes(exprs)?),
                });
            }
            let Op::Window(w) = ops.remove(0) else { unreachable!("checked above") };
            let mut out = vec![BOp::Window(Box::new(BWindow { w, prog, ts, keys, args }))];
            if !ops.is_empty() {
                out.push(BOp::Rows(ops));
            }
            Ok(out)
        }
        _ => rows(ops),
    }
}

/// A SELECT over one stream as batch operators: its window functions (`lag` only), the
/// projection, its ORDER BY.
fn projection(
    cat: &Catalog,
    q: &Query,
    s: &Select,
    stream: &str,
    alias: Option<&ast::TableAlias>,
    ops: &[Op],
) -> R<(Vec<BOp>, Vec<String>)> {
    let schema = Schema::of(&cat.streams[stream]).qualify(alias);
    let scope = schema.scope();
    let width = scope.cols.len();
    let mut lags = Lags { base: width, calls: vec![] };
    let mut prog = Program::new(width);
    let over = matches!(ops.first(), Some(Op::Over(_)));
    let (mut items, mut names) = (vec![], vec![]);
    {
        let mut vc = VCompiler::new(&scope);
        vc.aliases = aliases(s);
        if over {
            vc.windows = Some(&mut lags);
        }
        for item in &s.projection {
            let (i, name) = match item {
                SelectItem::Wildcard(_) => {
                    for (_, n, _) in &schema.0 {
                        items.push(Some(vc.expr(&mut prog, &Expr::Identifier(ast::Ident::new(n)))?));
                        names.push(n.clone());
                    }
                    continue;
                }
                SelectItem::UnnamedExpr(e) => (vc.item(&mut prog, e, &name_of(e))?, name_of(e)),
                SelectItem::ExprWithAlias { expr, alias } => {
                    (vc.item(&mut prog, expr, &alias.value)?, alias.value.clone())
                }
                i => return Err(format!("unsupported select item {i}")),
            };
            items.push(Some(i));
            names.push(name);
        }
    }
    let mut out = vec![];
    let filter = if over {
        // WHERE runs before the window functions, over the input row; no alias in it is a call
        let mut lp = Program::new(width);
        let mut vc = VCompiler::new(&scope);
        vc.aliases = aliases(s);
        let filter = s.selection.as_ref().map(|w| vc.condition(&mut lp, w)).transpose()?;
        let mut calls = vec![];
        let mut groups: Vec<LagGroup> = vec![];
        for (k, (_, x, offset, default, partition)) in lags.calls.iter().enumerate() {
            calls.push((VCompiler::new(&scope).expr(&mut lp, x)?, *offset, default.clone()));
            let keys = partition.iter().map(|e| VCompiler::new(&scope).expr(&mut lp, e)).collect::<R<Vec<_>>>()?;
            match groups.iter_mut().find(|g| g.keys == keys) {
                Some(g) => g.calls.push(k),
                None => groups.push(LagGroup { keys, calls: vec![k], parts: FxHashMap::default(), used: 0 }),
            }
        }
        out.push(BOp::Lag(Box::new(BLag { prog: lp, width, filter, calls, groups })));
        None
    } else {
        let mut vc = VCompiler::new(&scope);
        vc.aliases = aliases(s);
        s.selection.as_ref().map(|w| vc.condition(&mut prog, w)).transpose()?
    };
    let all = items.iter().map(|i| i.expect("every item compiled")).collect();
    out.push(BOp::Project { prog, width: width + lags.calls.len(), filter, items, all });
    if let Some(last @ (Op::Sort(_) | Op::Hold(_))) = ops.last() {
        let Some(OrderByKind::Expressions(es)) = q.order_by.as_ref().map(|o| &o.kind) else {
            return Err("an ORDER BY without keys".into());
        };
        let oscope = Scope::new(names.clone());
        let mut prog = Program::new(oscope.cols.len());
        let keys = es.iter().map(|e| VCompiler::new(&oscope).expr(&mut prog, &e.expr)).collect::<R<Vec<_>>>()?;
        out.push(BOp::Order { prog, keys, held: matches!(last, Op::Hold(_)), last: None, trusted: false });
    }
    Ok((out, names))
}

/// A view `FROM a ASOF LEFT JOIN b ON ... [ASOF LEFT JOIN ...]` as batch operators: the join
/// over its sides' columnar operators, then the SELECT over the joined columns.
fn join_view(cat: &Catalog, s: &Select, from: &ast::TableWithJoins, mut ops: Vec<Op>) -> R<Vec<BOp>> {
    if from.joins.iter().any(|j| matches!(j.relation, TableFactor::Derived { lateral: true, .. })) {
        return Err("a window join runs on rows".into());
    }
    if ops.len() != 2 || !matches!(ops[1], Op::Project { .. }) {
        return Err("a join with more than a SELECT after it".into());
    }
    let Op::Join(j) = ops.remove(0) else { return Err("not a join".into()) };
    // each side: its operators, its stream's width, its columns (qualified by its alias)
    let side = |t: &TableFactor, side_ops: &[Op]| -> R<Side> {
        let (q, alias) = match t {
            TableFactor::Derived { subquery, alias, .. } => (&**subquery, alias.as_ref()),
            TableFactor::Table { name, args: None, alias, .. } if cat.streams.contains_key(&name.to_string()) => {
                let st = &cat.streams[&name.to_string()];
                let q = alias.as_ref().map(|a| a.name.value.clone());
                let cols = st.columns.iter().map(|c| (q.clone(), c.name.clone())).collect();
                return Ok((vec![], st.columns.len(), cols));
            }
            t => return Err(format!("a join side {t}")),
        };
        let SetExpr::Select(sel) = q.body.as_ref() else { return Err("a side's query".into()) };
        let [f] = sel.from.as_slice() else { return Err("a side of more than one relation".into()) };
        let TableFactor::Table { name, args: None, alias: inner, .. } = &f.relation else {
            return Err("a side's relation".into());
        };
        if !f.joins.is_empty()
            || q.with.is_some()
            || !matches!(&sel.group_by, GroupByExpr::Expressions(k, _) if k.is_empty())
        {
            return Err("a side that is not a SELECT over a stream".into());
        }
        let stream = name.to_string();
        let (bops, names) = projection(cat, q, sel, &stream, inner.as_ref(), side_ops)?;
        if bops.iter().any(|o| matches!(o, BOp::Order { held: true, .. } | BOp::Lag(_))) {
            return Err("a held ORDER BY or a window function in a join side".into());
        }
        let qual = alias.map(|a| a.name.value.clone());
        Ok((bops, cat.streams[&stream].columns.len(), names.into_iter().map(|n| (qual.clone(), n)).collect()))
    };
    let mut sides = vec![];
    let mut cols: Vec<(Option<String>, String)> = vec![];
    let relations = std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation));
    let mut rights = vec![];
    for (k, (t, side_ops)) in relations.zip(&j.sides).enumerate() {
        let (bops, width, side_cols) = side(t, side_ops)?;
        let out = side_cols.len();
        if k > 0 {
            let JoinOperator::Left(JoinConstraint::On(on)) = &from.joins[k - 1].join_operator else {
                return Err("not an ASOF LEFT JOIN ... ON".into());
            };
            let ls = Scope { cols: cols.clone(), types: vec![] };
            let rs = Scope { cols: side_cols.clone(), types: vec![] };
            let (mut left, mut right) = (Program::new(cols.len()), Program::new(out));
            let (mut left_keys, mut keys, mut ts) = (vec![], vec![], None);
            let mut conds = vec![on];
            while let Some(c) = conds.pop() {
                match c {
                    Expr::BinaryOp { left: l, op: BinaryOperator::And, right: r } => {
                        conds.extend([l.as_ref(), r.as_ref()])
                    }
                    Expr::BinaryOp { left: l, op: BinaryOperator::Eq, right: r } => {
                        left_keys.insert(0, VCompiler::new(&ls).expr(&mut left, l)?);
                        keys.insert(0, VCompiler::new(&rs).expr(&mut right, r)?);
                    }
                    Expr::BinaryOp { left: l, op: BinaryOperator::GtEq, right: r } if ts.is_none() => {
                        ts = Some((VCompiler::new(&ls).expr(&mut left, l)?, VCompiler::new(&rs).expr(&mut right, r)?));
                    }
                    c => return Err(format!("an ASOF condition {c}")),
                }
            }
            let (left_ts, ts) = ts.ok_or("no ASOF time condition")?;
            let defaults = j.rights[k - 1].defaults.clone();
            if defaults.len() != out {
                return Err("a side's columns are not the engine's".into());
            }
            rights.push(BRight {
                left,
                left_ts,
                left_keys,
                right,
                ts,
                keys,
                defaults,
                newest: FxHashMap::default(),
                left_max: i64::MIN,
                right_max: i64::MIN,
                null_time: 0,
            });
        }
        sides.push((bops, width, out));
        cols.extend(side_cols);
    }
    // the SELECT over the joined row
    let scope = Scope { cols, types: vec![] };
    let mut prog = Program::new(scope.cols.len());
    let mut vc = VCompiler::new(&scope);
    vc.aliases = aliases(s);
    let mut items = vec![];
    for item in &s.projection {
        items.push(Some(match item {
            SelectItem::UnnamedExpr(e) => vc.item(&mut prog, e, &name_of(e))?,
            SelectItem::ExprWithAlias { expr, alias } => vc.item(&mut prog, expr, &alias.value)?,
            i => return Err(format!("a select item {i}")),
        }));
    }
    let filter = s.selection.as_ref().map(|w| vc.condition(&mut prog, w)).transpose()?;
    let width = scope.cols.len();
    let all = items.iter().map(|i| i.expect("every item compiled")).collect();
    Ok(vec![BOp::Join(Box::new(BJoin { sides, rights })), BOp::Project { prog, width, filter, items, all }])
}

/// The aggregate calls of a windowed SELECT in the order `Collect` meets them, each once: (its
/// key, name, argument expressions).
fn aggregate_calls(s: &Select) -> R<Vec<(String, String, Vec<Expr>)>> {
    struct Calls {
        base: usize,
        calls: Vec<(String, String, Vec<Expr>)>,
    }
    impl Aggregates for Calls {
        fn aggregate(&mut self, name: &str, params: &[Value], args: &[Expr]) -> R<Option<usize>> {
            if !crate::agg::is_aggregate(name) {
                return Ok(None);
            }
            let key = format!("{name}{params:?}({})", args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(","));
            if let Some(i) = self.calls.iter().position(|(k, ..)| *k == key) {
                return Ok(Some(self.base + i));
            }
            self.calls.push((key, name.to_string(), args.to_vec()));
            Ok(Some(self.base + self.calls.len() - 1))
        }
    }
    // the group row's scope: window_start, window_end, the keys
    let GroupByExpr::Expressions(group, _) = &s.group_by else { return Ok(vec![]) };
    let mut names = vec!["window_start".to_string(), "window_end".to_string()];
    names.extend(group.iter().filter(|k| !matches!(name_of(k).as_str(), "window_start" | "window_end")).map(name_of));
    let gs = Scope::new(names);
    let mut calls = Calls { base: gs.cols.len(), calls: vec![] };
    let mut c = Compiler::new(&gs);
    c.aliases = aliases(s);
    c.aggregates = Some(&mut calls);
    for item in &s.projection {
        match item {
            SelectItem::UnnamedExpr(e) => drop(c.compile_item(e, &name_of(e))?),
            SelectItem::ExprWithAlias { expr, alias } => drop(c.compile_item(expr, &alias.value)?),
            _ => {}
        }
    }
    drop(c);
    Ok(calls.calls)
}

/// `ops` over each input's rows (one input per join side). A join takes its sides' rows in time
/// order, as they come live: an exact join holds a left row until every right side has passed
/// it, but releases one that waited `ASOF_HOLD_US` of left time, which a side taken whole
/// before the others would pass.
fn run_rows(ops: &mut [Op], inputs: Vec<Vec<Row>>) -> Vec<Row> {
    let [Op::Join(j), rest @ ..] = ops else {
        let rows: Vec<Row> = inputs.into_iter().flatten().collect();
        return if rows.is_empty() { vec![] } else { run(ops, 0, &rows, i64::MAX) };
    };
    let mut all = vec![];
    for (s, rows) in inputs.into_iter().enumerate() {
        let rows = if rows.is_empty() { rows } else { run(&mut j.sides[s], 0, &rows, i64::MAX) };
        for r in rows {
            let t = if s == 0 { j.rights[0].left_ts.eval(&r).i64() } else { j.rights[s - 1].ts.eval(&r).i64() };
            all.push((t.unwrap_or(i64::MIN), s, r));
        }
    }
    // stable: rows of one time keep their side's order
    all.sort_by_key(|(t, s, _)| (*t, *s));
    let mut out = vec![];
    let mut all = all.into_iter().peekable();
    while let Some((_, side, r)) = all.next() {
        let mut run_ = vec![r];
        while let Some((_, _, r)) = all.next_if(|(_, s, _)| *s == side) {
            run_.push(r);
        }
        out.extend(j.take(side, run_, i64::MAX));
    }
    if rest.is_empty() || out.is_empty() {
        out
    } else {
        run(rest, 0, &out, i64::MAX)
    }
}

/// A source's batches as they are read, cut into chunks by clock.
struct Feed<'a> {
    name: String,
    clock: usize,
    width: usize,
    src: Option<Box<dyn Source + 'a>>,
    /// Read and not yet taken, in order; the first from row `at`.
    queue: VecDeque<Batch>,
    at: usize,
    /// Rows queued (from `at`).
    queued: usize,
    /// The clock of the last row read (order is checked across batches).
    last: i64,
    read: u64,
    kept: u64,
    done: bool,
}

impl<'a> Feed<'a> {
    fn new(name: &str, clock: usize, width: usize) -> Feed<'a> {
        Feed {
            name: name.into(),
            clock,
            width,
            src: None,
            queue: VecDeque::new(),
            at: 0,
            queued: 0,
            last: i64::MIN,
            read: 0,
            kept: 0,
            done: false,
        }
    }

    /// Whether to read on: fewer than `n` rows queued, and the source not done.
    fn wants(&self, n: usize) -> bool {
        self.queued < n && !self.done
    }

    /// Reads until `n` rows inside `range` are queued, or the source ends. Rows outside it are
    /// dropped; every row's clock is checked to be in order.
    fn fill(&mut self, n: usize, range: &std::ops::Range<i64>) -> R<()> {
        while self.wants(n) {
            let Some(src) = self.src.as_mut() else {
                self.done = true;
                break;
            };
            let Some(b) = src.next() else {
                self.done = true;
                break;
            };
            let b = b.map_err(|e| format!("{}: {e}", self.name))?;
            if b.cols.len() != self.width {
                return Err(format!(
                    "{}: a batch of {} columns, the stream has {}",
                    self.name,
                    b.cols.len(),
                    self.width
                ));
            }
            let clock = &b.cols[self.clock];
            let times: Cow<[i64]> = match clock.i64s() {
                Some(t) => Cow::Borrowed(t),
                None => {
                    let t: Option<Vec<i64>> = (0..b.len).map(|i| clock.get(i).i64()).collect();
                    Cow::Owned(t.ok_or(format!("{}: a row without a clock", self.name))?)
                }
            };
            for (i, t) in times.iter().enumerate() {
                if *t < self.last {
                    return Err(format!(
                        "{}: not in clock order: {t} after {} (row {} of the source)",
                        self.name,
                        self.last,
                        self.read + i as u64
                    ));
                }
                self.last = *t;
            }
            self.read += b.len as u64;
            let (lo, hi) = (times.partition_point(|t| *t < range.start), times.partition_point(|t| *t < range.end));
            if hi > lo {
                self.kept += (hi - lo) as u64;
                self.queued += hi - lo;
                self.queue.push_back(if lo == 0 && hi == b.len { b } else { b.slice(lo..hi) });
            }
        }
        Ok(())
    }

    /// The clock of the `n`th queued row, if there is one.
    fn clock_at(&self, mut n: usize) -> Option<i64> {
        n += self.at;
        for b in &self.queue {
            if n < b.len {
                return b.cols[self.clock].get(n).i64();
            }
            n -= b.len;
        }
        None
    }

    /// The queued rows before `until`, as the batches (or the parts of them) they came in. A
    /// chunk ends on a clock, so it never splits rows of one time.
    fn take_until(&mut self, until: i64) -> Vec<Batch> {
        let mut parts = vec![];
        while let Some(b) = self.queue.front() {
            let clock = &b.cols[self.clock];
            let end = match clock.i64s() {
                Some(t) => self.at + t[self.at..].partition_point(|t| *t < until),
                None => (self.at..b.len).find(|&i| clock.get(i).i64().is_some_and(|t| t >= until)).unwrap_or(b.len),
            };
            if end > self.at {
                parts.push(if self.at == 0 && end == b.len { b.clone() } else { b.slice(self.at..end) });
                self.queued -= end - self.at;
            }
            if end < b.len {
                self.at = end;
                break;
            }
            self.queue.pop_front();
            self.at = 0;
        }
        parts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Batch;

    struct Batches(VecDeque<R<Batch>>);

    impl Source for Batches {
        fn next(&mut self) -> Option<R<Batch>> {
            self.0.pop_front()
        }
    }

    /// Each job on a thread of its own, batches cut into parts of a few rows.
    struct Threads;

    impl Pool for Threads {
        fn run<'a>(&self, jobs: Vec<Task<'a>>) {
            // a few workers taking the jobs in turn
            let queue = std::sync::Mutex::new(jobs.into_iter());
            std::thread::scope(|s| {
                for _ in 0..3 {
                    s.spawn(|| {
                        while let Some(j) = queue.lock().unwrap().next() {
                            j()
                        }
                    });
                }
            });
        }
        fn threads(&self) -> usize {
            3
        }
        fn part_rows(&self) -> usize {
            2
        }
    }

    const DAY: i64 = 86_400_000_000;
    const T0: i64 = 1_789_948_800_000_000;

    fn cat(sql: &str) -> Catalog {
        crate::sql::parse(sql).unwrap()
    }

    /// Messages as compared: topic, payload and the window end they hold, sorted.
    fn messages(out: Vec<Emit>) -> Vec<String> {
        let mut v: Vec<String> =
            out.into_iter().map(|m| format!("{} {} {}", m.topic, m.payload, m.window_end)).collect();
        v.sort();
        v
    }

    /// `messages` without their window ends, which hold every window a write closed: they are
    /// the engine's only for the same writes (`engine_chunks`).
    fn sans_end(v: Vec<String>) -> Vec<String> {
        let mut v: Vec<String> = v.into_iter().map(|m| m[..m.rfind(' ').unwrap()].to_string()).collect();
        v.sort();
        v
    }

    /// The engine's messages for `src` inserted `n` rows at a time (the executor's chunks of
    /// `n` rows of distinct clocks), and the engine.
    fn engine_chunks(c: &Catalog, src: &[Row], n: usize) -> (Vec<String>, Engine) {
        let mut e = Engine::new(c).unwrap();
        let mut out = vec![];
        for part in src.chunks(n) {
            e.insert("src", part.to_vec(), &mut out);
        }
        e.close_until(i64::MAX, &mut out);
        (messages(out), e)
    }

    /// The engine's messages for `sources` merged by `clock` (one insert of each run of a
    /// source, or all of one source at a time with `whole`), sorted, without window ends.
    fn engine(cat: &Catalog, sources: &[(&str, Vec<Row>)], clock: usize, whole: bool) -> Vec<String> {
        let mut e = Engine::new(cat).unwrap();
        e.set_asof(Asof::Exact);
        let mut out = vec![];
        if whole {
            for (name, rows) in sources {
                e.insert(name, rows.clone(), &mut out);
            }
        } else {
            let mut all: Vec<(i64, usize, &Row)> = sources
                .iter()
                .enumerate()
                .flat_map(|(k, (_, rows))| rows.iter().map(move |r| (r[clock].i64().unwrap_or(i64::MIN), k, r)))
                .collect();
            all.sort_by_key(|(t, k, _)| (*t, *k));
            for (_, k, r) in all {
                e.insert(sources[k].0, vec![r.clone()], &mut out);
            }
        }
        e.close_until(i64::MAX, &mut out);
        sans_end(messages(out))
    }

    /// The historical executor's messages, sorted, and its stats.
    fn historical(
        cat: &Catalog,
        sources: &[(&str, Vec<Row>)],
        batch: usize,
        chunk: usize,
        pool: &dyn Pool,
    ) -> R<(Vec<String>, Stats)> {
        let mut h = Historical::new(cat)?;
        h.set_chunk_rows(chunk);
        let inputs = sources
            .iter()
            .map(|(name, rows)| {
                let width = cat.streams[*name].columns.len();
                let b: VecDeque<R<Batch>> = rows.chunks(batch).map(|c| Ok(Batch::from_rows(c, width))).collect();
                (name.to_string(), Box::new(Batches(b)) as Box<dyn Source>)
            })
            .collect();
        let mut out = vec![];
        let stats = h.run(inputs, T0..T0 + DAY, pool, &mut out)?;
        Ok((messages(out), stats))
    }

    /// The same messages as the engine, in batches and chunks of every size, on both pools.
    fn same(sql: &str, sources: &[(&str, Vec<Row>)], clock: usize) -> Stats {
        let c = cat(sql);
        let want = engine(&c, sources, clock, false);
        assert!(!want.is_empty());
        let mut stats = Stats::default();
        for (batch, chunk) in [(1000, 1 << 16), (3, 5), (7, 2)] {
            for pool in [&Serial as &dyn Pool, &Threads] {
                let (got, s) = historical(&c, sources, batch, chunk, pool).unwrap();
                assert_eq!(sans_end(got), want, "batches of {batch}, chunks of {chunk}");
                stats = s;
            }
        }
        stats
    }

    const SOURCE: &str = "
        CREATE EXTERNAL STREAM src (local_timestamp int64, time int64, k string, x float64, id string)
          SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'src', data_format = 'ProtobufSingle', format_schema = 's:S';
        CREATE EXTERNAL STREAM out (k string, t int64, a nullable(float64), b nullable(float64))
          SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'out', data_format = 'JSONEachRow';";

    fn rows(n: i64) -> Vec<Row> {
        (0..n)
            .map(|i| {
                vec![
                    Value::Int(T0 + i * 7_000_000),
                    Value::Int(T0 + (i * 7_000_000 + (i % 5) * 31_000_000) % DAY),
                    Value::Str(["p", "q"][(i % 2) as usize].into()),
                    Value::F64(((i * 37) % 101) as f64),
                    Value::Str(format!("{}", 1000 - i % 9).into()),
                ]
            })
            .collect()
    }

    #[test]
    fn views_without_a_columnar_form_run_the_engines_operators() {
        // a t-digest's window (its merge schedule is the engine's), a window function other than
        // lag, a stream's MATERIALIZED column, an ORDER BY after a window, a row-wise function
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t datetime64(6), k string, x float64, c float64 MATERIALIZED x * 2, d string);
            CREATE STREAM run (t datetime64(6), k string, x float64, s float64);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT from_unix_timestamp64_micro(local_timestamp) AS t, k, x, format_datetime(from_unix_timestamp64_micro(local_timestamp), '%Y-%m-%d') AS d FROM src;
            CREATE MATERIALIZED VIEW r INTO run AS
              SELECT t, k, x, sum(c) OVER (PARTITION BY k ORDER BY t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM mid;
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, quantile_t_digest(0.5)(x) AS a, max(s) AS b
              FROM tumble(run, t, 1m) GROUP BY window_start, k ORDER BY k
              EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND;"
        );
        let stats = same(&sql, &[("src", rows(400))], 0);
        assert_eq!(stats.row_views, ["r", "w"]);
    }

    #[test]
    fn rows_out_of_key_order_are_sorted_and_late_ones_dropped() {
        // ORDER BY the ids, and windows over another time than the clock: rows out of its order,
        // some behind the watermark, some without one
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t nullable(datetime64(6)), k string, x float64, id string);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT if(x > 95, NULL, from_unix_timestamp64_micro(time)) AS t, k, x, id FROM src ORDER BY id;
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, latest(x) AS b
              FROM tumble(mid, t, 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND;
            CREATE MATERIALIZED VIEW q INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, quantile_t_digest(0.5)(x) AS a, 0.0 AS b
              FROM tumble(mid, t, 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND;"
        );
        let c = cat(&sql);
        let src = rows(300);
        // the engine's inserts as the executor's chunks: the late rows are those of the second
        // behind the first's watermark
        let (want, e) = engine_chunks(&c, &src, 150);
        for pool in [&Serial as &dyn Pool, &Threads] {
            let (got, stats) = historical(&c, &[("src", src.clone())], 1 << 20, 150, pool).unwrap();
            assert_eq!(got, want);
            assert!(stats.late > 0 && stats.null_time > 0 && stats.chunks == 2, "{stats:?}");
            // the columnar window's late rows and the engine's (the digest's window)
            assert_eq!((stats.late, stats.null_time), (e.late(), e.null_time()), "{stats:?}");
            assert_eq!(stats.row_views, ["q"]);
        }
    }

    #[test]
    fn the_engines_own_dropped_rows_are_counted() {
        // a window over a window's rows runs in the engine: those of q ten minutes behind the
        // watermark p's set, and the large sums without a time
        let sql = format!(
            "{SOURCE}
            CREATE STREAM agg (t nullable(datetime64(6)), k string, x float64);
            CREATE MATERIALIZED VIEW w1 INTO agg AS
              SELECT if(sum(x) > 300, NULL, from_unix_timestamp64_micro(to_unix_timestamp64_micro(window_start)
                - if(k = 'q', 600000000, 0))) AS t, k, sum(x) AS x
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;
            CREATE MATERIALIZED VIEW w2 INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, max(x) AS b
              FROM tumble(agg, t, 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;"
        );
        let c = cat(&sql);
        let src = rows(600);
        // the engine's inserts as the executor's chunks: a window's rows reach the next a chunk
        // at a time, and those behind what an earlier chunk's set are late
        let (want, e) = engine_chunks(&c, &src, 150);
        assert!(e.late() > 0 && e.null_time() > 0, "{} {}", e.late(), e.null_time());
        for pool in [&Serial as &dyn Pool, &Threads] {
            let (got, stats) = historical(&c, &[("src", src.clone())], 1 << 20, 150, pool).unwrap();
            assert_eq!(got, want);
            assert_eq!((stats.late, stats.null_time, stats.chunks), (e.late(), e.null_time(), 4), "{stats:?}");
        }
    }

    #[test]
    fn windows_of_other_programs_run_apart() {
        // of dividing widths, the same slots and aggregate calls, but keys of other columns: each
        // its own (only the programs' texts tell them apart)
        let sql = format!(
            "{SOURCE}
            CREATE MATERIALIZED VIEW w1 INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, 0.0 AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m)
              GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;
            CREATE MATERIALIZED VIEW w5 INTO out AS
              SELECT id AS k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, 0.0 AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 5m)
              GROUP BY window_start, id EMIT AFTER WINDOW CLOSE;"
        );
        let h = Historical::new(&cat(&sql)).unwrap();
        assert!(h.fused.is_empty() && h.views.iter().all(|v| matches!(v.ops[..], [BOp::Window(_)])));
        same(&sql, &[("src", rows(300))], 0);
    }

    #[test]
    fn wider_windows_take_the_narrowests_rows_and_a_nan_first_holds_a_min() {
        // 1m and 5m together (ADR-0017): a min, and a min of the rows a condition picks, of
        // Float32 and Float64. Each 5m window's second minute starts with a NaN, which holds
        // its fold, before values below every other minute's: they reach the 5m window, as
        // the engine's every row does, bit for bit
        let sql = format!(
            "{SOURCE}
            CREATE MATERIALIZED VIEW w1 INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, min(to_float32(x)) AS a, min_if(x, k = 'p') AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;
            CREATE MATERIALIZED VIEW w5 INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, min(to_float32(x)) AS a, min_if(x, k = 'p') AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 5m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;"
        );
        assert_eq!(Historical::new(&cat(&sql)).unwrap().fused.len(), 1);
        // one key, as a symbol's file is: the windows' rows are runs, which they take together
        let one = |nan: bool| -> Vec<Row> {
            let mut src = rows(600);
            let mut minute = i64::MIN;
            for r in src.iter_mut() {
                r[2] = Value::Str("p".into());
                let t = r[0].i64().unwrap();
                let first = std::mem::replace(&mut minute, t.div_euclid(60_000_000)) != minute;
                r[3] = Value::F64(match minute.rem_euclid(5) {
                    1 if nan && first => f64::NAN,
                    1 => 0.5 + (t % 3) as f64,
                    _ => 60.0 + (t % 7) as f64,
                });
            }
            src
        };
        let before = MERGES.load(std::sync::atomic::Ordering::Relaxed);
        same(&sql, &[("src", one(true))], 0);
        // without a NaN, each wider window merges the narrowest's states
        same(&sql.replace("min(to_float32(x))", "min(x)"), &[("src", one(false))], 0);
        assert!(MERGES.load(std::sync::atomic::Ordering::Relaxed) > before);
    }

    #[test]
    fn windows_of_one_level_apart_in_the_plan_run_together_where_adjacent() {
        // several pipelines in one SQL: views of one level come apart in the plan (a window over
        // the source, a projection of it and a window over that, then another window over the
        // source). The windows that differ in width alone are run together only within a run of
        // views of their level; the others alone: the engine's messages
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t datetime64(6), k string, x float64);
            CREATE MATERIALIZED VIEW w1 INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, 0.0 AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT from_unix_timestamp64_micro(local_timestamp) AS t, k, x FROM src;
            CREATE MATERIALIZED VIEW wm INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, max(x) AS a, 1.0 AS b
              FROM tumble(mid, t, 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;
            CREATE MATERIALIZED VIEW w5 INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, 0.0 AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 5m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;"
        );
        let mut src = rows(400);
        src.iter_mut().for_each(|r| r[2] = Value::Str("p".into()));
        same(&sql, &[("src", src)], 0);
    }

    #[test]
    fn a_row_run_joins_dropped_rows_are_counted() {
        // a join with a window function other than lag runs the engine's operators, its join
        // exact as the engine's (b's row of a's time is a's match); its left rows without a time
        // are dropped, and counted as the engine counts them
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t nullable(datetime64(6)), k string, x float64);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT if(x > 95, NULL, from_unix_timestamp64_micro(local_timestamp)) AS t, k, x FROM src;
            CREATE MATERIALIZED VIEW j INTO out AS
              SELECT a.k AS k, to_unix_timestamp64_micro(a.t) AS t, b.x AS a,
                sum(a.x) OVER (PARTITION BY a.k ORDER BY a.t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS b
              FROM mid AS a ASOF LEFT JOIN src AS b ON a.k = b.k AND a.t >= from_unix_timestamp64_micro(b.local_timestamp);"
        );
        let c = cat(&sql);
        let src = rows(300);
        let mut e = Engine::new(&c).unwrap();
        e.insert("src", src.clone(), &mut vec![]);
        e.close_until(i64::MAX, &mut vec![]);
        assert!(e.null_time() > 0);
        let stats = same(&sql, &[("src", src)], 0);
        assert_eq!(stats.row_views, ["j"]);
        assert_eq!(stats.null_time, e.null_time(), "{stats:?}");
    }

    #[test]
    fn a_held_order_its_input_breaks_across_chunks_is_refused() {
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t datetime64(6), k string, x float64, id string);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT from_unix_timestamp64_micro(local_timestamp) AS t, k, x, id FROM src ORDER BY id SETTINGS order_hold_ms = 50;
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, max(x) AS b
              FROM tumble(mid, t, 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;"
        );
        let e = historical(&cat(&sql), &[("src", rows(50))], 4, 4, &Serial).unwrap_err();
        assert!(e.contains("out of ORDER BY order across chunks"), "{e}");
    }

    #[test]
    fn a_run_is_refused_what_it_cannot_take() {
        let sql = format!(
            "{SOURCE}
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, max(x) AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 5m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;"
        );
        let c = cat(&sql);
        let run = |inputs: Vec<(String, Box<dyn Source>)>, range: std::ops::Range<i64>| {
            let mut h = Historical::new(&c).unwrap();
            h.run(inputs, range, &Serial, &mut vec![]).unwrap_err()
        };
        let src = |rows: Vec<R<Batch>>| Box::new(Batches(rows.into())) as Box<dyn Source>;
        let ok = || src(vec![Ok(Batch::from_rows(&rows(10), 5))]);
        assert!(run(vec![], T0 + 1..T0 + DAY).contains("windows' boundaries"));
        assert!(run(vec![("nope".into(), ok())], T0..T0 + DAY).contains("no source nope"));
        assert!(run(vec![("src".into(), ok()), ("src".into(), ok())], T0..T0 + DAY).contains("two inputs"));
        assert!(run(vec![("src".into(), src(vec![Err("broken".into())]))], T0..T0 + DAY).contains("src: broken"));
        assert!(run(vec![("src".into(), src(vec![Ok(Batch::from_rows(&rows(3), 4)),]))], T0..T0 + DAY)
            .contains("4 columns"));
        let mut back = rows(10);
        back.swap(2, 6);
        assert!(run(vec![("src".into(), src(vec![Ok(Batch::from_rows(&back, 5))]))], T0..T0 + DAY)
            .contains("not in clock order"));
        let mut none = rows(3);
        none[1][0] = Value::Null;
        assert!(run(vec![("src".into(), src(vec![Ok(Batch::from_rows(&none, 5))]))], T0..T0 + DAY)
            .contains("without a clock"));
        // a clock of another column, then none at all
        let mut h = Historical::new(&c).unwrap();
        h.set_clock("src", "time").unwrap();
        assert!(h.set_clock("src", "nope").unwrap_err().contains("no column nope"));
        assert!(h.set_clock("nope", "time").unwrap_err().contains("no source nope"));
        let unclocked = SOURCE.replace("local_timestamp int64", "lt int64")
            + "
            CREATE MATERIALIZED VIEW w INTO out AS SELECT k, lt AS t, x AS a, x AS b FROM src;";
        let mut h = Historical::new(&cat(&unclocked)).unwrap();
        let e = h.run(vec![("src".into(), ok())], T0..T0 + DAY, &Serial, &mut vec![]).unwrap_err();
        assert!(e.contains("no clock column"), "{e}");
    }

    /// A stream two views of source rows write (a UNION ALL) reaches its readers in clock order,
    /// as live: `lag` over it takes the row before in time, whichever source it came from. And
    /// one a window's output writes too goes into the engine, with its readers.
    #[test]
    fn a_stream_of_several_writers_is_read_in_clock_order() {
        let src2 = SOURCE.replace("src (", "src2 (").replace("'src'", "'src2'");
        let src2 = &src2[..src2.find("CREATE EXTERNAL STREAM out").unwrap()];
        let union = format!(
            "{SOURCE}{src2}
            CREATE STREAM mid (t int64, k string, x float64);
            CREATE MATERIALIZED VIEW a INTO mid AS SELECT local_timestamp AS t, k, x FROM src;
            CREATE MATERIALIZED VIEW b INTO mid AS SELECT local_timestamp AS t, k, x * 10 AS x FROM src2;
            CREATE MATERIALIZED VIEW w INTO out AS SELECT k, t, x AS a, lag(x) OVER (PARTITION BY k) AS b FROM mid;"
        );
        let later: Vec<Row> = rows(40)
            .into_iter()
            .map(|mut r| {
                r[0] = Value::Int(r[0].i64().unwrap() + 3_500_000);
                r
            })
            .collect();
        let sources = [("src", rows(40)), ("src2", later)];
        same(&union, &sources, 0);
        let h = Historical::new(&cat(&union)).unwrap();
        assert_eq!(h.merged, [(2, 0)], "mid, by t");
        // a writer that computes its time: no order to read the stream in
        let computed = union.replace("local_timestamp AS t, k, x * 10", "local_timestamp + 1 AS t, k, x * 10");
        assert!(Historical::new(&cat(&computed)).unwrap().merged.is_empty());
        let mixed = format!(
            "{SOURCE}
            CREATE STREAM mid (t int64, k string, x float64);
            CREATE MATERIALIZED VIEW a INTO mid AS SELECT local_timestamp AS t, k, x FROM src;
            CREATE MATERIALIZED VIEW g INTO mid AS
              SELECT to_unix_timestamp64_micro(window_start) AS t, k, sum(x) AS x
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;
            CREATE MATERIALIZED VIEW w INTO out AS SELECT k, t, x AS a, x AS b FROM mid;"
        );
        same(&mixed, &[("src", rows(40))], 0);
    }

    #[test]
    fn plans_the_executor_cannot_run_are_refused() {
        let beside = format!(
            "{SOURCE}
            CREATE STREAM agg (k string, t int64, s float64);
            CREATE STREAM mid (t int64, k string, x float64);
            CREATE MATERIALIZED VIEW m INTO mid AS SELECT local_timestamp AS t, k, x FROM src;
            CREATE MATERIALIZED VIEW g INTO agg AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS s
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT a.k AS k, a.t AS t, a.x AS a, b.s AS b
              FROM mid AS a ASOF LEFT JOIN agg AS b ON a.k = b.k AND a.t >= b.t;"
        );
        assert!(Historical::new(&cat(&beside)).err().unwrap().contains("beside streams of source rows"));
    }

    #[test]
    fn an_asof_join_time_that_goes_back_is_refused() {
        let sql = format!(
            "{SOURCE}
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT a.k AS k, a.local_timestamp AS t, a.x AS a, b.x AS b
              FROM src AS a ASOF LEFT JOIN (SELECT time, k, x FROM src) AS b ON a.k = b.k AND a.local_timestamp >= b.time;"
        );
        let e = historical(&cat(&sql), &[("src", rows(40))], 1000, 1 << 16, &Serial).unwrap_err();
        assert!(e.contains("time goes back"), "{e}");
    }

    /// A file's columns as a stream's: each an expression over them (text parsed, a column
    /// renamed, a product, an integer cast to the column's float), the file's other columns not
    /// read, and only the rows the filter keeps.
    #[test]
    fn a_mapping_computes_a_streams_columns_from_a_files() {
        let input = [
            ("p".to_string(), Type::Str),
            ("q".to_string(), Type::F64),
            ("t".to_string(), Type::Int(64)),
            ("u".to_string(), Type::Str),
        ];
        let exprs = [
            (0, Type::F64, "to_float64(p)".to_string()),
            (2, Type::F64, "q * to_float64(p)".to_string()),
            (1, Type::F64, "t".to_string()),
        ];
        let m = Mapping::new(&input, &exprs, Some("p IS NOT NULL")).unwrap();
        assert_eq!(m.reads(), [true, true, true, false]);
        let rows = vec![
            vec![Value::Str("0.5".into()), Value::F64(2.0), Value::Int(1), Value::Null],
            vec![Value::Null, Value::F64(3.0), Value::Int(2), Value::Null],
            vec![Value::Str("1.25".into()), Value::F64(4.0), Value::Int(3), Value::Null],
        ];
        let (cols, keep) = m.apply(&Batch::from_rows(&rows, 4));
        assert_eq!(keep, Some(vec![0, 2]));
        assert_eq!(cols.iter().map(|c| c.0).collect::<Vec<_>>(), [0, 2, 1]);
        assert_eq!((cols[0].1.get(0), cols[0].1.get(2)), (Value::F64(0.5), Value::F64(1.25)));
        assert_eq!(cols[1].1.get(2), Value::F64(5.0));
        // an int64 cast to the stream's float64
        assert_eq!(cols[2].1.get(2), Value::F64(3.0));
        // every row, without a filter; an expression that does not compile is refused
        let (_, keep) = Mapping::new(&input, &exprs, None).unwrap().apply(&Batch::from_rows(&rows, 4));
        assert_eq!(keep, None);
        assert!(Mapping::new(&input, &[(0, Type::F64, "nope + 1".to_string())], None).is_err());
        assert!(Mapping::new(&input, &[(0, Type::F64, "1 +".to_string())], None).is_err());
        assert!(Mapping::new(&input, &[], Some("1 +")).is_err());
    }

    /// Rows before and after the range are read, counted, and left out, wherever the batches cut.
    #[test]
    fn rows_outside_the_range_are_read_and_left() {
        let sql = format!(
            "{SOURCE}
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, max(x) AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;"
        );
        let c = cat(&sql);
        let inside = rows(100);
        let at = |t: i64| {
            let mut r = inside[0].clone();
            r[0] = Value::Int(t);
            r
        };
        let mut all: Vec<Row> = (1..=3).rev().map(|i| at(T0 - i * 1000)).collect();
        all.extend(inside.clone());
        all.push(at(T0 + DAY));
        let want = sans_end(historical(&c, &[("src", inside)], 7, 1 << 16, &Serial).unwrap().0);
        for batch in [1, 2, 7, 1000] {
            for chunk in [10, 1 << 16] {
                let (got, stats) = historical(&c, &[("src", all.clone())], batch, chunk, &Serial).unwrap();
                assert_eq!(sans_end(got), want, "batches of {batch}");
                assert_eq!(stats.rows["src"], (104, 100), "batches of {batch}");
                assert_eq!(stats.chunks, 100_u64.div_ceil(chunk as u64), "batches of {batch}, chunks of {chunk}");
            }
        }
    }

    #[test]
    fn reads_lists_the_columns_the_views_read() {
        let sql = format!(
            "{SOURCE}
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, max(x) AS b
              FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;"
        );
        let h = Historical::new(&cat(&sql)).unwrap();
        assert_eq!(h.reads("src"), [true, false, true, true, false]);
        assert_eq!(h.sources().iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), ["src"]);
        // its clock, though no view reads it: the feed orders the chunks by it
        let by_time = sql.replace("from_unix_timestamp64_micro(local_timestamp)", "from_unix_timestamp64_micro(time)");
        assert_eq!(Historical::new(&cat(&by_time)).unwrap().reads("src"), [true, true, true, true, false]);
        // through a projection that orders by the ids, a held ORDER BY, and a projection after
        // `lag`: the columns their items read, the ids only while their order is checked
        let window = "CREATE MATERIALIZED VIEW w INTO out AS
              SELECT k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, 1.0 AS b
              FROM tumble(mid, from_unix_timestamp64_micro(t), 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE;";
        let ordered = format!(
            "{SOURCE}
            CREATE STREAM mid (t int64, k string, x float64, id string);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT local_timestamp AS t, k, x, id FROM src ORDER BY t, id SETTINGS order_hold_ms = 50;
            {window}"
        );
        let mut h = Historical::new(&cat(&ordered)).unwrap();
        assert_eq!(h.reads("src"), [true, false, true, true, true]);
        h.set_trust_order(true);
        assert_eq!(h.reads("src"), [true, false, true, true, false]);
        assert_eq!(h.reads("mid"), [true, true, true, false], "a stream between views");
        let lagged = format!(
            "{SOURCE}
            CREATE STREAM mid (t int64, k string, x float64);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT local_timestamp AS t, k, lag(x) OVER (PARTITION BY k) AS x FROM src;
            {window}"
        );
        assert_eq!(Historical::new(&cat(&lagged)).unwrap().reads("src"), [true, false, true, true, false]);
    }

    /// Windows of one input that differ in width alone run together when each width is a
    /// multiple of the narrowest (1m and 5m, by key or of one key) and alone when not (2m, 3m,
    /// 5m): the engine's rows either way, rows on a window's boundary in the next one, and no
    /// view on rows (an aggregate item without a name among them).
    #[test]
    fn windows_of_widths_that_divide_run_together() {
        let view = |name: &str, key: &str, width: &str, aggs: &str| {
            let (item, group) = if key == "k" { ("k".to_string(), ", k") } else { (format!("'{key}' AS k"), "") };
            format!(
                "CREATE MATERIALIZED VIEW {name} INTO out AS
                  SELECT {item}, to_unix_timestamp64_micro(window_start) AS t, {aggs}
                  FROM tumble(src, from_unix_timestamp64_micro(local_timestamp), {width})
                  GROUP BY window_start{group} EMIT AFTER WINDOW CLOSE;"
            )
        };
        let keyed = "sum(x) AS a, latest(x) AS b, count()";
        let sql = [
            SOURCE.to_string(),
            view("k1", "k", "1m", keyed),
            view("k5", "k", "5m", keyed),
            view("n1", "n", "1m", "max(x) AS a, min(x) AS b"),
            view("n5", "n", "5m", "max(x) AS a, min(x) AS b"),
            view("m2", "m", "2m", "sum(x) AS a, latest(x) AS b"),
            view("m3", "m", "3m", "sum(x) AS a, latest(x) AS b"),
            view("m5", "m", "5m", "sum(x) AS a, latest(x) AS b"),
        ]
        .join("\n");
        let h = Historical::new(&cat(&sql)).unwrap();
        let names = |set: &Vec<usize>| set.iter().map(|&v| h.views[v].name.as_str()).collect::<Vec<_>>();
        assert_eq!(h.fused.iter().map(|(_, set)| names(set)).collect::<Vec<_>>(), [["k1", "k5"], ["n1", "n5"]]);
        let stats = same(&sql, &[("src", rows(400))], 0);
        assert!(stats.row_views.is_empty(), "{stats:?}");
    }

    /// An ORDER BY whose first key falls as the rows' times rise: each chunk's rows sorted
    /// (`latest` is of the earliest), and the window over them takes every one.
    #[test]
    fn rows_in_reverse_key_order_are_sorted() {
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t datetime64(6), x float64, nt int64);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT from_unix_timestamp64_micro(local_timestamp) AS t, x, 0 - local_timestamp AS nt FROM src ORDER BY nt;
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT 'n' AS k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, latest(x) AS b
              FROM tumble(mid, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE;"
        );
        let c = cat(&sql);
        let src = rows(300);
        let (want, _) = engine_chunks(&c, &src, 150);
        for pool in [&Serial as &dyn Pool, &Threads] {
            let (got, stats) = historical(&c, &[("src", src.clone())], 1 << 20, 150, pool).unwrap();
            assert_eq!(got, want);
            assert_eq!(stats.chunks, 2);
        }
    }

    /// A window's rows behind the watermark of the chunks before are dropped and counted
    /// whatever order its batches hold them in (in order from a late one, out of order from one
    /// that is not), one at the watermark is not late, and batches each in time order but going
    /// back across them are added as such.
    #[test]
    fn rows_behind_the_watermark_are_late_however_the_batches_hold_them() {
        let sql = format!(
            "{SOURCE}
            CREATE MATERIALIZED VIEW w INTO out AS
              SELECT 'n' AS k, to_unix_timestamp64_micro(window_start) AS t, sum(x) AS a, max(x) AS b
              FROM tumble(src, from_unix_timestamp64_micro(time), 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE;"
        );
        let c = cat(&sql);
        // chunks of 4 rows, times in seconds: the watermarks before the 2nd, 3rd and 4th are
        // 120 s, 120 s and 180 s; 100 and 90 are late
        let times = [10, 20, 150, 30, 100, 120, 130, 140, 200, 90, 210, 205, 300, 310, 250, 260];
        let mut src = rows(times.len() as i64);
        for (r, s) in src.iter_mut().zip(times) {
            r[1] = Value::Int(T0 + s * 1_000_000);
        }
        let (want, e) = engine_chunks(&c, &src, 4);
        assert_eq!(e.late(), 2);
        for batch in [2, 1 << 20] {
            for pool in [&Serial as &dyn Pool, &Threads] {
                let (got, stats) = historical(&c, &[("src", src.clone())], batch, 4, pool).unwrap();
                assert_eq!(got, want, "batches of {batch}");
                assert_eq!(stats.late, 2, "batches of {batch}");
            }
        }
    }

    /// An ASOF join's left time with NULLs (dropped, counted as the engine counts them) and ties.
    #[test]
    fn a_join_time_with_nulls_and_ties_gives_what_the_engine_gives() {
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t nullable(datetime64(6)), k string, x float64);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT if(x > 95, NULL, from_unix_timestamp64_micro(local_timestamp)) AS t, k, x FROM src;
            CREATE MATERIALIZED VIEW j INTO out AS
              SELECT a.k AS k, to_unix_timestamp64_micro(a.t) AS t, a.x AS a, b.x AS b
              FROM mid AS a ASOF LEFT JOIN src AS b ON a.k = b.k AND a.t >= from_unix_timestamp64_micro(b.local_timestamp);"
        );
        let mut src = rows(300);
        for (i, r) in src.iter_mut().enumerate() {
            r[0] = Value::Int(T0 + (i as i64 / 2) * 7_000_000);
        }
        let mut e = Engine::new(&cat(&sql)).unwrap();
        e.set_asof(Asof::Exact);
        e.insert("src", src.clone(), &mut vec![]);
        e.close_until(i64::MAX, &mut vec![]);
        assert!(e.null_time() > 0);
        let stats = same(&sql, &[("src", src)], 0);
        assert!(stats.row_views.is_empty(), "{stats:?}");
        assert_eq!(stats.null_time, e.null_time());
    }

    /// `lag` after a WHERE, two calls of different partitions.
    #[test]
    fn lags_of_two_partitions_after_a_filter_give_what_the_engine_gives() {
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t int64, k string, p nullable(float64), q float64);
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT local_timestamp AS t, k, lag(x) OVER (PARTITION BY k) AS p, lag(x, 2, 0.5) OVER (PARTITION BY id) AS q
              FROM src WHERE x > 20;
            CREATE MATERIALIZED VIEW w INTO out AS SELECT k, t, p AS a, q AS b FROM mid;"
        );
        let stats = same(&sql, &[("src", rows(200))], 0);
        assert!(stats.row_views.is_empty(), "{stats:?}");
    }

    /// Past `over::MAX_PARTITIONS`, `lag`'s least recently used tenth of the partitions is
    /// evicted, as the engine evicts it: a key used again is kept, an evicted one starts over.
    #[test]
    fn lags_least_recently_used_partitions_are_evicted() {
        let n = crate::over::MAX_PARTITIONS as i64;
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t int64, k string, p nullable(float64));
            CREATE MATERIALIZED VIEW m INTO mid AS
              SELECT local_timestamp AS t, id AS k, lag(x) OVER (PARTITION BY id) AS p FROM src;
            CREATE MATERIALIZED VIEW w INTO out AS SELECT k, t, p AS a, p AS b FROM mid WHERE t >= {};",
            T0 + n
        );
        let row = |i: i64, id: &str| {
            vec![
                Value::Int(T0 + i),
                Value::Int(T0),
                Value::Str("p".into()),
                Value::F64(i as f64),
                Value::Str(id.into()),
            ]
        };
        let mut src: Vec<Row> = (0..n).map(|i| row(i, &i.to_string())).collect();
        // "0" used again, a new key evicts "1" to "10000", which "1" and "10000" then miss
        for (i, id) in ["0", "new", "1", "10000", "10001", "0"].into_iter().enumerate() {
            src.push(row(n + i as i64, id));
        }
        let c = cat(&sql);
        let want = engine(&c, &[("src", src.clone())], 0, true);
        assert_eq!(want.len(), 6);
        let missed =
            |k: &str| want.iter().any(|m| m.contains(&format!(r#""k":"{k}","t""#)) && m.contains(r#""a":null"#));
        assert_eq!(
            [missed("1"), missed("10000"), missed("10001"), missed("0")],
            [true, true, false, false],
            "{want:?}"
        );
        for pool in [&Serial as &dyn Pool, &Threads] {
            let (got, _) = historical(&c, &[("src", src.clone())], 1 << 20, 1 << 16, pool).unwrap();
            assert_eq!(sans_end(got), want);
        }
    }

    /// A join's WITH, its CTE named as a stream, and a join side's WITH run the engine's
    /// operators: they read the CTE, not the stream.
    #[test]
    fn with_queries_run_the_engines_operators() {
        let sql = format!(
            "{SOURCE}
            CREATE STREAM mid (t int64, k string, x float64);
            CREATE MATERIALIZED VIEW w INTO out AS
              WITH mid AS (SELECT local_timestamp AS t, k, x FROM src)
              SELECT a.k AS k, a.local_timestamp AS t, a.x AS a, b.x AS b
              FROM src AS a ASOF LEFT JOIN mid AS b ON a.k = b.k AND a.local_timestamp >= b.t;
            CREATE MATERIALIZED VIEW j INTO out AS
              SELECT a.k AS k, a.local_timestamp AS t, a.x AS a, b.x AS b
              FROM src AS a ASOF LEFT JOIN (WITH c AS (SELECT local_timestamp AS t, k, x FROM src) SELECT t, k, x FROM c) AS b
              ON a.k = b.k AND a.local_timestamp >= b.t;"
        );
        let mut stats = same(&sql, &[("src", rows(50))], 0);
        stats.row_views.sort();
        assert_eq!(stats.row_views, ["j", "w"]);
    }

    /// A source clock of another integer type than int64 (no `i64s`): chunks cut row by row.
    #[test]
    fn a_uint64_clock_cuts_chunks() {
        let sql = SOURCE.replace("local_timestamp int64", "local_timestamp uint64")
            + "CREATE MATERIALIZED VIEW w INTO out AS SELECT k, local_timestamp AS t, x AS a, x AS b FROM src;";
        let mut src = rows(30);
        for r in &mut src {
            r[0] = Value::UInt(r[0].i64().unwrap() as u64);
        }
        same(&sql, &[("src", src)], 0);
    }
}
