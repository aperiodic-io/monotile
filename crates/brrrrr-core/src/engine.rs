//! The dataflow (ADR-0012): every materialized view is compiled into a short chain of operators
//! on rows. Inserting a chunk into a stream runs each view reading it, in creation order, and
//! inserts the output by name (cast to the column types) into the view's target: an internal
//! stream (recursively) or a Kafka sink (one JSONEachRow message per row). Views into S3 tables
//! are not run: exporting a sink's topic to object storage is a separate job's.
//!
//! Proton semantics reproduced here:
//! - windows: one watermark per view, `floor(max_ts - delay, width)` after each chunk; a row
//!   older than the previous watermark is late and dropped; windows with `end <= watermark` are
//!   emitted in start order (WatermarkStamper.cpp);
//! - `ASOF LEFT JOIN` (`Asof::Arrival`; `Asof::Exact` is a batch engine's as-of join
//!   instead, see `Asof`): a one-way lookup. Right rows only update a per-key table of the
//!   `keep_versions` (default 3) latest
//!   versions (a new version goes before equal times; the oldest is evicted); a left row is
//!   emitted at once with the latest version `<=` its time, or type defaults (join_use_nulls=0);
//! - `ORDER BY` in a streaming subquery sorts within the chunk only; a view's own `ORDER BY`
//!   with `SETTINGS order_hold_ms` sorts across chunks, holding rows that long (`Hold`, ADR-0013).
use crate::agg::Acc;
use crate::book::{Book, BookStats, Guard, Kept, TopN};
use crate::expr::{compare, order_by, Aggregates, Arg, Compiler, Ex, Headers, Pred, Scope, Windows};
use crate::format::{to_text, write_text, RowFormat};
use crate::over::{
    Frame, Func, Group as OverGroup, GroupState as OverState, GroupStateRef as OverStateRef, Over, Spec,
};
use crate::sql::{Catalog, Kind, Stream, View};
use crate::value::{Type, Value};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    self, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, JoinConstraint, JoinOperator, OrderByKind,
    Query, Select, SelectItem, SetExpr, TableFactor,
};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{btree_map::Entry, BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

pub type Row = Vec<Value>;

mod batch;
pub use batch::{Historical, Mapping, Pool, Serial, Source, Stats as HistoricalStats, Task, CHUNK_ROWS};

/// One Kafka message: the sink's topic, a JSONEachRow line and the `_tp_message_headers`.
#[derive(Clone, Debug, PartialEq)]
pub struct Emit {
    /// Shared by every message of the sink.
    pub topic: Arc<str>,
    pub payload: String,
    pub headers: Vec<(String, String)>,
    /// The earliest end (µs) of the windows whose close wrote it, through every view on its way
    /// (`i64::MAX`: no window did): no window the message holds ended before it. The runtime
    /// withholds by it what a sink no longer holds (`--unverified-history`).
    pub window_end: i64,
}

/// Where the engine puts the messages its views emit. A `Vec<Emit>` collects them. The
/// runtime's output also sends what it holds whenever a view has written a sink (`flush`), so
/// that one view's messages need not wait for the views after it: a 15s bar for the 1h windows
/// the same row closes.
pub trait Output {
    fn push(&mut self, e: Emit);
    /// A row of sink `sink`, in its stream's columns and types, before it is made a message:
    /// `true` takes it instead (the historical executor's Parquet files), and no message is made.
    fn row(&mut self, _sink: &str, _row: &[Value]) -> bool {
        false
    }
    /// Rows of sink `sink` as a batch of its stream's columns, from a view that makes batches:
    /// `true` takes them so, and `row` is not called for them.
    fn batch(&mut self, _sink: &str, _batch: &crate::column::Batch) -> bool {
        false
    }
    /// A view has written a sink: everything pushed so far may go.
    fn flush(&mut self) {}
}

impl Output for Vec<Emit> {
    fn push(&mut self, e: Emit) {
        Vec::push(self, e);
    }
}

type R<T> = Result<T, String>;

/// Planner schema: (qualifier, name, type when known) per row position.
#[derive(Clone, Default)]
struct Schema(Vec<(Option<String>, String, Option<Type>)>);

impl Schema {
    fn of(s: &Stream) -> Schema {
        Schema(s.columns.iter().map(|c| (None, c.name.clone(), Some(c.ty.clone()))).collect())
    }
    fn scope(&self) -> Scope {
        Scope {
            cols: self.0.iter().map(|(q, n, _)| (q.clone(), n.clone())).collect(),
            types: self.0.iter().map(|c| c.2.clone()).collect(),
        }
    }
    fn qualify(mut self, q: Option<&ast::TableAlias>) -> Schema {
        if let Some(q) = q {
            self.0.iter_mut().for_each(|c| c.0 = Some(q.name.value.clone()));
        }
        self
    }
    /// The type of a plain column reference; types only matter for ASOF defaults.
    fn ty(&self, e: &Expr) -> Option<Type> {
        let Expr::Identifier(i) = e else { return None };
        self.0.iter().find(|c| c.1 == i.value).and_then(|c| c.2.clone())
    }
}

#[derive(Clone)]
enum Op {
    Project {
        filter: Option<Pred>,
        exprs: Vec<Ex>,
        /// `SELECT *` alone: each row it keeps is its input row as it is (`Plan::sink_filter`).
        all: bool,
    },
    Sort(Vec<Ex>),
    Hold(Box<Hold>),
    Window(Box<Window>),
    Join(Box<Join>),
    Over(Box<Over>),
    /// `orderbook_top_n`: order books by (exchange, symbol).
    Book(Box<TopN>),
    /// `gap_fill`: the buckets a window's output skipped, per key.
    Fill(Box<Fill>),
    /// `lead(...) OVER (...)`: rows held until the rows their `lead` reads come.
    Lead(Box<Lead>),
    /// `rank() OVER (PARTITION BY time ...)` and the like: each time's rows held until the next
    /// time's come.
    Section(Box<Section>),
}

/// A window group: (window start, key text) -> (key values, accumulators).
type GroupKey = (i64, String);
type Group = (Vec<Value>, Vec<Acc>);
/// The open groups of one window, by key text (see `key_text`).
type Groups = FxHashMap<String, Group>;
/// An ASOF right side's versions of one key, newest first.
type Versions = Vec<(Value, Row)>;

/// Engine state: per view, per operator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct State(Vec<Vec<OpState>>);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpState {
    Stateless,
    Window {
        watermark: i64,
        max_ts: i64,
        late: u64,
        open: Vec<(GroupKey, Group)>,
    },
    Join {
        #[serde(deserialize_with = "crate::checkpoint::nested")]
        sides: Vec<Vec<OpState>>,
        versions: Vec<Vec<(String, Versions)>>,
    },
    /// Per-row window functions: per PARTITION BY/ORDER BY group, its partitions.
    Over {
        groups: Vec<OverState>,
    },
    /// An exact ASOF join (`Asof::Exact`): a join's state and the left rows it holds, with
    /// each held row's time per right side, and each right side's newest time.
    JoinHeld {
        #[serde(deserialize_with = "crate::checkpoint::nested")]
        sides: Vec<Vec<OpState>>,
        versions: Vec<Vec<(String, Versions)>>,
        held: Vec<(Vec<i64>, Row)>,
        right_max: Vec<i64>,
        left_max: i64,
    },
    /// A held sort: its rows with their times (first keys), in key order; the newest time taken
    /// and the time of the last row released.
    Hold {
        held: Vec<(i64, Row)>,
        max: i64,
        released: i64,
    },
    /// `orderbook_top_n`'s books, in key order: (key text, book, replica guard, kept diffs).
    Book {
        books: Vec<(String, Book, Guard, Kept)>,
    },
    /// `gap_fill`'s keys, in key order: (key text, the last bucket out, the key's last row).
    Fill {
        last: Vec<(String, i64, Row)>,
    },
    /// `lead`'s partitions, in key order: (key text, the rows held, oldest first). Also an
    /// `Op::Section`'s rows held, oldest first, under one empty key text when it holds any: the
    /// same shape, so a section needs no new format.
    Lead {
        held: Vec<(String, Vec<Row>)>,
    },
}

/// `State` borrowed from the live engine, for `Checkpoint::encode_of`: it serializes to the
/// bytes of the `State` a `snapshot` copies, without the copy. Its types mirror `State`'s,
/// variant for variant and field for field (`tests/checkpoint.rs` checks the bytes).
#[derive(Serialize)]
#[serde(rename = "State")]
pub struct StateRef<'a>(Vec<Vec<OpStateRef<'a>>>);

#[derive(Serialize)]
#[serde(rename = "OpState")]
enum OpStateRef<'a> {
    Stateless,
    Window {
        watermark: i64,
        max_ts: i64,
        late: u64,
        open: Vec<((i64, &'a str), &'a Group)>,
    },
    Join {
        sides: Vec<Vec<OpStateRef<'a>>>,
        versions: Vec<Vec<(&'a str, &'a Versions)>>,
    },
    Over {
        groups: Vec<OverStateRef<'a>>,
    },
    JoinHeld {
        sides: Vec<Vec<OpStateRef<'a>>>,
        versions: Vec<Vec<(&'a str, &'a Versions)>>,
        held: &'a VecDeque<(Vec<i64>, Row)>,
        right_max: Vec<i64>,
        left_max: i64,
    },
    Hold {
        held: &'a VecDeque<(i64, Row)>,
        max: i64,
        released: i64,
    },
    Book {
        books: Vec<(&'a str, &'a Book, &'a Guard, &'a Kept)>,
    },
    Fill {
        last: Vec<(&'a str, i64, &'a Row)>,
    },
    Lead {
        held: Vec<(&'a str, &'a VecDeque<Row>)>,
    },
}

impl OpStateRef<'_> {
    fn to_owned(&self) -> OpState {
        let sides =
            |s: &[Vec<OpStateRef>]| s.iter().map(|ops| ops.iter().map(OpStateRef::to_owned).collect()).collect();
        let versions = |v: &[Vec<(&str, &Versions)>]| {
            v.iter().map(|r| r.iter().map(|(k, v)| (k.to_string(), (*v).clone())).collect()).collect()
        };
        match self {
            OpStateRef::Stateless => OpState::Stateless,
            OpStateRef::Window { watermark, max_ts, late, open } => OpState::Window {
                watermark: *watermark,
                max_ts: *max_ts,
                late: *late,
                open: open.iter().map(|((s, k), g)| ((*s, k.to_string()), (*g).clone())).collect(),
            },
            OpStateRef::Join { sides: s, versions: v } => OpState::Join { sides: sides(s), versions: versions(v) },
            OpStateRef::Over { groups } => {
                OpState::Over { groups: groups.iter().map(OverStateRef::to_owned).collect() }
            }
            OpStateRef::JoinHeld { sides: s, versions: v, held, right_max, left_max } => OpState::JoinHeld {
                sides: sides(s),
                versions: versions(v),
                held: held.iter().cloned().collect(),
                right_max: right_max.clone(),
                left_max: *left_max,
            },
            OpStateRef::Hold { held, max, released } => {
                OpState::Hold { held: held.iter().cloned().collect(), max: *max, released: *released }
            }
            OpStateRef::Book { books } => OpState::Book {
                books: books
                    .iter()
                    .map(|(k, b, g, kept)| (k.to_string(), (*b).clone(), **g, (*kept).clone()))
                    .collect(),
            },
            OpStateRef::Fill { last } => {
                OpState::Fill { last: last.iter().map(|(k, b, r)| (k.to_string(), *b, (*r).clone())).collect() }
            }
            OpStateRef::Lead { held } => {
                OpState::Lead { held: held.iter().map(|(k, h)| (k.to_string(), h.iter().cloned().collect())).collect() }
            }
        }
    }
}

impl Op {
    fn snapshot(&self) -> OpState {
        self.borrow().to_owned()
    }

    fn borrow(&self) -> OpStateRef<'_> {
        match self {
            Op::Project { .. } | Op::Sort(_) => OpStateRef::Stateless,
            Op::Window(w) => {
                OpStateRef::Window { watermark: w.watermark, max_ts: w.max_ts, late: w.late, open: w.snapshot() }
            }
            Op::Join(j) => {
                let sides = j.sides.iter().map(|ops| ops.iter().map(Op::borrow).collect()).collect();
                // in key order: the same state is always the same bytes
                let versions = j
                    .rights
                    .iter()
                    .map(|r| {
                        let mut v: Vec<_> = r.versions.iter().map(|(k, v)| (k.as_str(), &v.versions)).collect();
                        v.sort_unstable_by(|a, b| a.0.cmp(b.0));
                        v
                    })
                    .collect();
                match j.exact {
                    false => OpStateRef::Join { sides, versions },
                    true => OpStateRef::JoinHeld {
                        sides,
                        versions,
                        held: &j.held,
                        right_max: j.rights.iter().map(|r| r.max_ts).collect(),
                        left_max: j.left_max,
                    },
                }
            }
            Op::Over(o) => OpStateRef::Over { groups: o.groups.iter().map(OverGroup::snapshot).collect() },
            Op::Hold(h) => OpStateRef::Hold { held: &h.held, max: h.max, released: h.released },
            Op::Book(b) => OpStateRef::Book { books: b.snapshot() },
            Op::Fill(f) => {
                // in key order: the same state is always the same bytes
                let mut last: Vec<_> = f.last.iter().map(|(k, (b, r))| (k.as_str(), *b, r)).collect();
                last.sort_unstable_by(|a, b| a.0.cmp(b.0));
                OpStateRef::Fill { last }
            }
            Op::Section(s) if s.held.is_empty() => OpStateRef::Lead { held: vec![] },
            Op::Section(s) => OpStateRef::Lead { held: vec![("", &s.held)] },
            Op::Lead(l) => {
                let mut held: Vec<_> = l.held.iter().map(|(k, h)| (k.as_str(), h)).collect();
                held.sort_unstable_by(|a, b| a.0.cmp(b.0));
                OpStateRef::Lead { held }
            }
        }
    }
}

/// Checks that `state` fits `ops` before anything is restored: the same operators, and in them
/// groups and versions the plan could have made (their key text is their keys', aggregates are
/// the plan's, rows are as wide as the plan's). A corrupted or foreign snapshot is refused
/// rather than restored into output that is wrong or panics later.
fn check_ops(ops: &[Op], state: &[OpState]) -> R<()> {
    if ops.len() != state.len() {
        return Err("snapshot does not match the plan".into());
    }
    let mut text = String::new();
    for (op, st) in ops.iter().zip(state) {
        match (op, st) {
            (Op::Project { .. } | Op::Sort(_), OpState::Stateless) => {}
            (Op::Window(w), OpState::Window { open, .. }) => {
                let plan = accs(&w.aggs, &w.shares)?;
                let mut seen = std::collections::HashSet::new();
                for ((start, key), (keys, accs)) in open {
                    if keys.len() != w.keys.len() || accs.len() != plan.len() {
                        return Err(format!("a group with {} keys and {} aggregates", keys.len(), accs.len()));
                    }
                    key_text(&mut text, keys);
                    if text != *key || !seen.insert((*start, key)) {
                        return Err(format!("the group {key:?} at {start} is filed under another key or twice"));
                    }
                    for (acc, plan) in accs.iter().zip(&plan) {
                        acc.check_against(plan)?;
                    }
                }
            }
            (Op::Join(j), OpState::Join { sides, versions })
                if sides.len() == j.sides.len() && versions.len() == j.rights.len() && !j.exact =>
            {
                check_join(j, sides, versions, &mut text)?;
            }
            (Op::Join(j), OpState::JoinHeld { sides, versions, held, right_max, .. })
                if sides.len() == j.sides.len() && versions.len() == j.rights.len() && j.exact =>
            {
                check_join(j, sides, versions, &mut text)?;
                // the widths first: the order reads each held row's first time
                let wide = right_max.len() == j.rights.len() && held.iter().all(|(t, _)| t.len() == j.rights.len());
                if !wide || !held.windows(2).all(|p| p[0].0[0] <= p[1].0[0]) {
                    return Err("held rows that are not in time order, one time per right side".into());
                }
            }
            (Op::Over(o), OpState::Over { groups }) if groups.len() == o.groups.len() => {
                for (g, st) in o.groups.iter().zip(groups) {
                    g.check(st)?;
                }
            }
            (Op::Hold(h), OpState::Hold { held, max, .. }) => {
                // rows as wide as the plan's (the width first: keys read the row's columns), each
                // with its own time, at or before the newest taken, in key order
                let fits = held.iter().all(|(t, r)| r.len() == h.width && h.time(r) == Some(*t) && t <= max);
                if !fits || held.windows(2).any(|p| h.cmp(&p[0].1, &p[1].1).is_gt()) {
                    return Err("held rows that are not rows of the plan in key order, up to the newest time".into());
                }
            }
            (Op::Book(b), OpState::Book { books }) => b.check(books)?,
            (Op::Fill(f), OpState::Fill { last }) => {
                // rows as wide as the plan's (the width first: keys read the row's columns), each
                // filed under its key's text once, at or before the last bucket out
                for (key, b, r) in last {
                    let fits = r.len() == f.cols && r[f.time].i64().is_some_and(|t| t <= *b) && {
                        key_text(&mut text, f.keys.iter().map(|&k| &r[k]));
                        text == *key
                    };
                    if !fits || last.iter().filter(|x| x.0 == *key).count() > 1 {
                        return Err(format!("the gap_fill key {key:?} is not one of the plan's rows, once"));
                    }
                }
            }
            (Op::Lead(l), OpState::Lead { held }) => {
                // fewer rows than the farthest lead, as wide as the plan's, each of its key, once
                for (key, rows) in held {
                    let fits = rows.len() <= l.most
                        && rows.iter().all(|r| {
                            r.len() == l.width && {
                                key_text(&mut text, l.keys.iter().map(|k| k(r)));
                                text == *key
                            }
                        });
                    if !fits || held.iter().filter(|x| x.0 == *key).count() > 1 {
                        return Err(format!("the lead partition {key:?} is not the plan's rows, once"));
                    }
                }
            }
            (Op::Section(s), OpState::Lead { held }) => {
                // one time's rows, as wide as the plan's, under the empty key
                let fits = match held.as_slice() {
                    [] => true,
                    [(key, rows)] => {
                        key.is_empty()
                            && !rows.is_empty()
                            && rows
                                .iter()
                                .all(|r| r.len() == s.width && order_by(&(s.time)(r), &(s.time)(&rows[0])).is_eq())
                    }
                    _ => false,
                };
                if !fits {
                    return Err("the rows held for a ranking are not the plan's rows of one time".into());
                }
            }
            _ => return Err("snapshot does not match the plan".into()),
        }
    }
    Ok(())
}

/// A join's sides and versions: per key, at most what the join keeps, newest first, each a row
/// of its key.
fn check_join(j: &Join, sides: &[Vec<OpState>], versions: &[Vec<(String, Versions)>], text: &mut String) -> R<()> {
    for (ops, st) in j.sides.iter().zip(sides) {
        check_ops(ops, st)?;
    }
    let most = if j.exact { EXACT_MAX_VERSIONS } else { j.keep };
    for (r, keyed) in j.rights.iter().zip(versions) {
        let mut seen = std::collections::HashSet::new();
        for (key, v) in keyed {
            let ordered = v.windows(2).all(|p| compare(&p[0].0, &p[1].0) != Some(Ordering::Less));
            if v.is_empty() || v.len() > most || !ordered || !seen.insert(key) {
                return Err(format!("the versions of {key:?} are not the latest few, newest first"));
            }
            for (_, row) in v {
                // the width first: the key is read from the row's columns
                let wrong = row.len() != r.defaults.len() || {
                    key_text(text, r.key.iter().map(|k| k.eval(row)));
                    text != key
                };
                if wrong {
                    return Err(format!("a version of {key:?} that is not a row of its key"));
                }
            }
        }
    }
    Ok(())
}

/// Checks the times in a snapshot `check_ops` accepted against the newest times that bound
/// them, as the operators keep them: a window's watermark is `floor(max_ts - delay, width)`,
/// and its open windows start on a window boundary, at or before `max_ts`, and end after the
/// watermark (one ending at or before it is emitted); an exact join's held rows are at or
/// before `left_max` and less than `ASOF_HOLD_US` behind it (older ones are released), and a
/// right side's versions at or before its newest time.
fn check_times(ops: &[Op], state: &[OpState]) -> R<()> {
    for (op, st) in ops.iter().zip(state) {
        match (op, st) {
            (Op::Window(w), OpState::Window { watermark, max_ts, open, .. }) => {
                if *watermark != floor(max_ts.saturating_sub(w.delay), w.width) {
                    return Err(format!("the watermark {watermark} is not the newest time {max_ts}'s"));
                }
                for ((start, _), _) in open {
                    if floor(*start, w.width) != *start || start > max_ts || start.saturating_add(w.width) <= *watermark
                    {
                        return Err(format!(
                            "an open window at {start}, which the newest time {max_ts} and the watermark {watermark} \
                             do not leave open"
                        ));
                    }
                }
            }
            (Op::Join(j), OpState::Join { sides, .. }) => {
                for (ops, st) in j.sides.iter().zip(sides) {
                    check_times(ops, st)?;
                }
            }
            (Op::Join(j), OpState::JoinHeld { sides, versions, held, right_max, left_max }) => {
                for (ops, st) in j.sides.iter().zip(sides) {
                    check_times(ops, st)?;
                }
                if let Some((t, _)) =
                    held.iter().find(|(t, _)| t[0] > *left_max || t[0] < left_max.saturating_sub(ASOF_HOLD_US))
                {
                    return Err(format!(
                        "a row held at {} that is not within the hold of the newest left time {left_max}",
                        t[0]
                    ));
                }
                for (keyed, max) in versions.iter().zip(right_max) {
                    let mut times = keyed.iter().flat_map(|(_, v)| v).filter_map(|(t, _)| t.i64());
                    if let Some(t) = times.find(|t| t > max) {
                        return Err(format!("a version at {t}, after its right side's newest time {max}"));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Appends what the snapshot of `ops` holds to `out` (see `Engine::fingerprint`).
fn describe(ops: &[Op], out: &mut String) {
    use std::fmt::Write;
    for op in ops {
        match op {
            Op::Project { .. } => out.push_str(" project"),
            Op::Sort(_) => out.push_str(" sort"),
            Op::Hold(h) => {
                let _ = write!(out, " sort(hold {})", h.hold);
            }
            Op::Window(w) => {
                let _ = write!(out, " window({},{},{}:{})", w.width, w.delay, w.keys.len(), w.agg_keys.join(","));
            }
            Op::Join(j) => {
                out.push_str(" join(");
                if j.exact {
                    out.push_str("exact;");
                } else if j.keep != KEEP_VERSIONS {
                    let _ = write!(out, "keep {};", j.keep);
                }
                for side in &j.sides {
                    describe(side, out);
                    out.push(';');
                }
                for w in j.rights.iter().filter_map(|r| r.within.as_ref()) {
                    let _ = write!(out, "within({},{});", w.span, w.aggs.len());
                }
                out.push(')');
            }
            Op::Over(o) => {
                out.push_str(" over(");
                for g in &o.groups {
                    let specs: Vec<&str> = g.specs.iter().map(|s| s.describe.as_str()).collect();
                    let _ = write!(out, "{}:{}:{};", g.partition.len(), g.order.is_some(), specs.join(","));
                }
                out.push(')');
            }
            Op::Book(b) => {
                let _ = write!(out, " orderbook_top_n({},{})", b.depth, b.allow_seq_reset);
            }
            Op::Fill(f) => {
                let _ = write!(out, " gap_fill({},{},{:?})", f.time, f.width, f.keys);
            }
            Op::Lead(l) => {
                let _ = write!(out, " lead({},{})", l.keys.len(), l.most);
            }
            Op::Section(s) => {
                let _ = write!(out, " section({})", s.describe);
            }
        }
    }
}

/// Restores a snapshot `check_ops` accepted.
fn restore_ops(ops: &mut [Op], state: Vec<OpState>) {
    for (op, st) in ops.iter_mut().zip(state) {
        match (op, st) {
            (Op::Project { .. } | Op::Sort(_), OpState::Stateless) => {}
            (Op::Window(w), OpState::Window { watermark, max_ts, late, open }) => {
                (w.watermark, w.max_ts, w.late) = (watermark, max_ts, late);
                w.open = BTreeMap::new();
                (w.due, w.lead_at) = (BTreeMap::new(), if w.digests { i64::MIN } else { i64::MAX });
                for ((start, key), g) in open {
                    w.open.entry(start).or_default().insert(key, g);
                }
            }
            (Op::Join(j), OpState::Join { sides, versions }) => {
                for (ops, st) in j.sides.iter_mut().zip(sides) {
                    restore_ops(ops, st);
                }
                for (r, v) in j.rights.iter_mut().zip(versions) {
                    r.versions = Keyed::restore(v);
                }
            }
            (Op::Join(j), OpState::JoinHeld { sides, versions, held, right_max, left_max }) => {
                for (ops, st) in j.sides.iter_mut().zip(sides) {
                    restore_ops(ops, st);
                }
                for ((r, v), m) in j.rights.iter_mut().zip(versions).zip(right_max) {
                    (r.versions, r.max_ts) = (Keyed::restore(v), m);
                }
                (j.held, j.left_max) = (held.into_iter().collect(), left_max);
            }
            (Op::Over(o), OpState::Over { groups }) => {
                for (g, st) in o.groups.iter_mut().zip(groups) {
                    g.restore(st);
                }
            }
            (Op::Hold(h), OpState::Hold { held, max, released }) => {
                (h.held, h.max, h.released) = (held.into(), max, released);
            }
            (Op::Book(b), OpState::Book { books }) => b.restore(books),
            (Op::Fill(f), OpState::Fill { last }) => {
                f.last = last.into_iter().map(|(k, b, r)| (k, (b, r))).collect();
            }
            (Op::Lead(l), OpState::Lead { held }) => {
                l.held = held.into_iter().map(|(k, h)| (k, h.into())).collect();
            }
            (Op::Section(s), OpState::Lead { held }) => {
                s.held = held.into_iter().next().map(|(_, h)| h.into()).unwrap_or_default();
            }
            _ => unreachable!("checked by check_ops"),
        }
    }
}

/// Runs a chunk through `ops`. The chunk is borrowed: several views read the same stream, and
/// only an operator that must reorder or extend rows in place copies them.
/// `limit` is the latest event time a window or join takes (`Engine::set_time_limit`).
/// The earliest end of the windows `ops` (and their join sides) emitted when they last ran, of
/// those that did: what the rows they emitted hold (`Emit::window_end`; `i64::MAX`: no window).
/// ponytail: a held sort after a window releases its rows in a later run, where the window may
/// have closed nothing: such rows are not bounded. No view sorts its windows' output.
fn emitted_from(ops: &[Op]) -> i64 {
    ops.iter()
        .map(|op| match op {
            Op::Window(w) => w.emitted_from,
            Op::Join(j) => j.sides.iter().map(|side| emitted_from(side)).min().unwrap_or(i64::MAX),
            _ => i64::MAX,
        })
        .min()
        .unwrap_or(i64::MAX)
}

/// Every operator of `ops`, with those of joins' sides at any depth.
fn all_ops(ops: &[Op]) -> Vec<&Op> {
    fn walk<'a>(ops: &'a [Op], out: &mut Vec<&'a Op>) {
        for op in ops {
            out.push(op);
            if let Op::Join(j) = op {
                j.sides.iter().for_each(|side| walk(side, out));
            }
        }
    }
    let mut out = vec![];
    walk(ops, &mut out);
    out
}

/// The rows `ops` dropped: (late, without a time), as `Engine::late` and `null_time` count them.
fn drops(ops: &[Op]) -> (u64, u64) {
    all_ops(ops).into_iter().fold((0, 0), |(late, null), op| match op {
        Op::Window(w) => (late + w.late, null + w.dropped.null_time),
        Op::Section(s) => (late + s.late, null),
        Op::Join(j) => (late, null + j.dropped.null_time),
        _ => (late, null),
    })
}

fn run(ops: &mut [Op], side: usize, rows: &[Row], limit: i64) -> Vec<Row> {
    let mut rows = Cow::Borrowed(rows);
    for (i, op) in ops.iter_mut().enumerate() {
        if rows.is_empty() {
            break;
        }
        let side = if i == 0 { side } else { 0 };
        rows = Cow::Owned(match op {
            Op::Project { filter, exprs, .. } => rows
                .iter()
                .filter(|r| filter.as_ref().is_none_or(|f| f(r) == Some(true)))
                .map(|r| exprs.iter().map(|e| e(r)).collect())
                .collect(),
            Op::Sort(keys) => {
                let mut rows = rows.into_owned();
                rows.sort_by(|a, b| sorted(keys, a, b));
                rows
            }
            Op::Hold(h) => h.apply(rows.into_owned(), limit),
            Op::Window(w) => w.apply(&rows, limit),
            Op::Join(j) => j.apply(side, &rows, limit),
            Op::Over(o) => o.apply(&rows),
            Op::Book(b) => b.apply(&rows),
            Op::Fill(f) => f.apply(&rows),
            Op::Lead(l) => l.apply(&rows),
            Op::Section(s) => s.apply(&rows),
        });
    }
    rows.into_owned()
}

/// Closes `ops` as far as their input has reached without a row (`Engine::close_until`):
/// `reached` is that time, one per join side when `ops` start with a join. A window closes
/// what a row at that time would, a held sort releases what such a row would, an exact join
/// releases the rows it passes (`Join::close`); what they emit runs through the operators
/// after them first. Returns the rows out and the time the output has reached: no later row
/// out of `ops` is before it (a window's output is before its watermark, a held sort's `hold`
/// behind its input, a join's no later than the rows it still holds), for the operators and
/// views downstream, whose time is taken to be no earlier than that one: a window's
/// `window_start` or `window_end`, a row's own time through a projection.
fn close(ops: &mut [Op], reached: &[i64], limit: i64) -> (Vec<Row>, i64) {
    let (mut rows, mut at) = (vec![], reached[0]);
    for op in ops {
        match op {
            // only the first operator: a join's sides are the plan's inputs
            Op::Join(j) => (rows, at) = j.close(reached, limit),
            Op::Window(w) => {
                // the rows first, under the watermark they were sent under
                rows = w.apply(&rows, limit);
                rows.extend(w.advance(at));
                at = w.watermark;
            }
            Op::Hold(h) => {
                rows = h.apply(rows, limit);
                at = at.saturating_sub(h.hold);
                h.release(at, &mut rows);
            }
            Op::Fill(f) => {
                rows = f.apply(&rows);
                rows.extend(f.close(at));
            }
            Op::Lead(l) => {
                rows = l.apply(&rows);
                if at == i64::MAX {
                    rows.extend(l.flush());
                }
            }
            Op::Section(s) => {
                rows = s.apply(&rows);
                if at == i64::MAX {
                    rows.extend(s.release());
                } else if let Some(h) = s.held.front() {
                    // the rows held come out later, at their time
                    at = at.min((s.time)(h).i64().unwrap_or(i64::MIN));
                }
            }
            op => rows = run(std::slice::from_mut(op), 0, &rows, limit),
        }
    }
    (rows, at)
}

/// A group or join key as text, unambiguously: strings are length-prefixed, NULL has its own
/// tag, and no other value's text contains ';'.
/// Written into a reused buffer: the hot paths look keys up without allocating.
#[inline(always)] // in every window's and window function's row loop (see Acc::add)
pub(crate) fn key_text<V: std::borrow::Borrow<Value>>(k: &mut String, keys: impl IntoIterator<Item = V>) {
    k.clear();
    for v in keys {
        match v.borrow() {
            Value::Null => k.push('\0'),
            Value::Str(s) => {
                k.push_str(itoa::Buffer::new().format(s.len()));
                k.push(':');
                k.push_str(s);
            }
            v => {
                write_text(k, v);
                k.push(';');
            }
        }
    }
}

/// Start of the window of width `w` holding `ts`, saturating at the i64 bounds.
fn floor(ts: i64, w: i64) -> i64 {
    ts.saturating_sub(ts.rem_euclid(w))
}

/// Rows a held sort took after it had released a later one: released at once, out of order
/// (a runtime count, as `SEQUENCE_OUT_OF_ORDER`).
pub static HOLD_LATE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A view's `ORDER BY` with `SETTINGS order_hold_ms` (ADR-0013): one sorted buffer across
/// chunks. A row waits until the newest row's first key (a time) is `hold` past its own, and
/// leaves in key order, ties in arrival order. A row arriving after a later one has left is
/// released at once and counted (`HOLD_LATE`). Windows downstream need no delay of their own:
/// they see their rows sorted, `hold` after the newest.
#[derive(Clone)]
struct Hold {
    keys: Vec<Ex>,
    /// The first key's column, when it is a plain column: read without evaluating `keys[0]`.
    time_col: Option<usize>,
    /// µs
    hold: i64,
    /// The rows' width, to check a snapshot's.
    width: usize,
    /// (time, row): the time is the first key's, kept not to evaluate it again.
    held: VecDeque<(i64, Row)>,
    max: i64,
    released: i64,
}

impl Hold {
    fn apply(&mut self, rows: Vec<Row>, limit: i64) -> Vec<Row> {
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            // no time, or one past the limit (windows drop it): nothing to wait for, and it must
            // not release the others early
            let Some(t) = self.time(&r).filter(|t| *t <= limit) else {
                out.push(r);
                continue;
            };
            if t < self.released {
                HOLD_LATE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            // every held row is at or before the newest time taken (`max`), so a later time goes
            // last without comparing keys; otherwise after equal keys: ties keep their arrival order
            if t > self.max {
                self.max = t;
                self.held.push_back((t, r));
            } else {
                let at = self.held.partition_point(|b| self.cmp(&b.1, &r).is_le());
                self.held.insert(at, (t, r));
            }
        }
        self.release(self.max.saturating_sub(self.hold), &mut out);
        out
    }

    /// Releases, in key order, the rows whose time is before `until`.
    fn release(&mut self, until: i64, out: &mut Vec<Row>) {
        while let Some(&(t, _)) = self.held.front().filter(|(t, _)| *t < until) {
            self.released = t;
            out.push(self.held.pop_front().expect("a front").1);
        }
    }

    /// A row's first sort key as a time.
    fn time(&self, r: &Row) -> Option<i64> {
        match self.time_col {
            Some(i) => r[i].i64(),
            None => self.keys[0](r).i64(),
        }
    }

    /// Rows in `ORDER BY` order, as `Op::Sort` has them.
    fn cmp(&self, a: &Row, b: &Row) -> Ordering {
        sorted(&self.keys, a, b)
    }
}

/// `gap_fill(stream, time, 'width'[, start, finish], key ..., locf(column) ..., interpolate(column)
/// ...)` (TimescaleDB's `time_bucket_gapfill`): a window's output rows, and before each the
/// buckets its key skipped since its last row (from `start`, before its first), each a row of
/// NULLs but its time, its keys, its `locf` columns (the key's last row's) and its `interpolate`
/// ones (linear between the rows either side). A gap is filled when its key's next bucket comes,
/// live as over history; the buckets after a key's last row only up to `finish`, as the input
/// passes them (`close`). Without both `start` and `finish` to bound it, a gap of more than
/// `MAX_GAP` buckets is not filled (`Engine::unfilled`): one row with a stray time decades
/// off would have it write a row for every bucket between.
#[derive(Clone)]
struct Fill {
    time: usize,
    width: i64,
    start: Option<i64>,
    finish: Option<i64>,
    keys: Vec<usize>,
    locf: Vec<usize>,
    interpolate: Vec<usize>,
    /// The rows' width, to check a snapshot's.
    cols: usize,
    /// Per key text: the last bucket out (a row's or a gap's), and the key's last row.
    last: FxHashMap<String, (i64, Row)>,
    text: String,
    /// The gaps not filled for being longer than `MAX_GAP` buckets.
    unfilled: u64,
}

/// The most buckets an unbounded `gap_fill` fills in one gap: 18 hours of seconds, 45 days of
/// minutes, 7 years of hours.
const MAX_GAP: i128 = 1 << 16;

impl Fill {
    fn apply(&mut self, rows: &[Row]) -> Vec<Row> {
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let Some(t) = r[self.time].i64() else {
                out.push(r.clone());
                continue;
            };
            key_text(&mut self.text, self.keys.iter().map(|&k| &r[k]));
            let prev = self.last.get(self.text.as_str());
            match prev {
                // not after the key's last bucket: as it is
                Some((b, _)) if *b >= t => {
                    out.push(r.clone());
                    continue;
                }
                Some((b, p)) => {
                    let filled = self.gaps(b.saturating_add(self.width), t, Some(p), Some(r), &mut out);
                    self.unfilled += u64::from(!filled);
                }
                None => {
                    if let Some(s) = self.start {
                        let filled = self.gaps(floor(s, self.width), t, None, Some(r), &mut out);
                        self.unfilled += u64::from(!filled);
                    }
                }
            }
            out.push(r.clone());
            match self.last.get_mut(self.text.as_str()) {
                Some(l) => *l = (t, r.clone()),
                None => {
                    self.last.insert(self.text.clone(), (t, r.clone()));
                }
            }
        }
        out
    }

    /// The rows of the buckets `from` .. `to` within `start` .. `finish`, between a key's rows
    /// `prev` and `next` (either may be missing, not both).
    fn gaps(&self, from: i64, to: i64, prev: Option<&Row>, next: Option<&Row>, out: &mut Vec<Row>) -> bool {
        let Some(like) = next.or(prev) else { return true };
        let from = self.start.map_or(from, |s| from.max(floor(s, self.width)));
        let to = self.finish.map_or(to, |f| to.min(f));
        let unbounded = self.start.is_none() || self.finish.is_none();
        if unbounded && i128::from(to) - i128::from(from) > MAX_GAP * i128::from(self.width) {
            return false;
        }
        let mut b = from;
        while b < to {
            let mut row = vec![Value::Null; self.cols];
            row[self.time] = Value::Time(b);
            for &k in &self.keys {
                row[k] = like[k].clone();
            }
            if let Some(p) = prev {
                for &c in &self.locf {
                    row[c] = p[c].clone();
                }
                if let Some(n) = next {
                    for &c in &self.interpolate {
                        row[c] = match (p[self.time].i64(), n[self.time].i64(), p[c].f64(), n[c].f64()) {
                            (Some(t0), Some(t1), Some(v0), Some(v1)) if t1 > t0 => {
                                // times as far apart as i64's ends: their differences overflow i64
                                let (b, t0, t1) = (i128::from(b), i128::from(t0), i128::from(t1));
                                Value::F64(v0 + (v1 - v0) * (b - t0) as f64 / (t1 - t0) as f64)
                            }
                            _ => Value::Null,
                        };
                    }
                }
            }
            out.push(row);
            b = b.saturating_add(self.width);
        }
        true
    }

    /// The buckets every key skipped since its last row, before `finish` and `at`, the time the
    /// input has reached (no later row has an earlier time), key by key in key order.
    fn close(&mut self, at: i64) -> Vec<Row> {
        let Some(finish) = self.finish else { return vec![] };
        let to = finish.min(floor(at, self.width));
        let mut keys: Vec<String> = self.last.iter().filter(|(_, (b, _))| *b < to).map(|(k, _)| k.clone()).collect();
        keys.sort_unstable();
        let mut out = vec![];
        for k in keys {
            let (b, row) = &self.last[&k];
            let n = out.len();
            let filled = self.gaps(b.saturating_add(self.width), to, Some(row), None, &mut out);
            self.unfilled += u64::from(!filled);
            if let Some(t) = out[n..].last().and_then(|r| r[self.time].i64()) {
                self.last.get_mut(&k).expect("a key").0 = t;
            }
        }
        out
    }
}

/// `lead(x[, n[, default]]) OVER (PARTITION BY k ...)` after the window functions (`Op::Over`,
/// which puts each call's `x` of the row in its slot): each row held until its partition's
/// `most`-th next row comes (the farthest `n` of the SELECT's calls), each call's slot then the
/// `n`-th next row's. Rows of a partition leave in their order, partitions as their rows come:
/// a row comes when its partition's next ones do, live as over history. At the end of the
/// input (`close` to the end of time) the rows still held take the next rows there are, or the
/// defaults.
///
/// ponytail: every partition's rows held, unbounded; a partition that never trades again holds
/// its last rows until the end. Evict as `over::MAX_PARTITIONS` does if a feed needs it.
#[derive(Clone)]
struct Lead {
    keys: Vec<Ex>,
    /// (slot, n, default) per call.
    calls: Vec<(usize, usize, Value)>,
    most: usize,
    /// The rows' width, to check a snapshot's.
    width: usize,
    held: FxHashMap<String, VecDeque<Row>>,
    text: String,
}

impl Lead {
    fn apply(&mut self, rows: &[Row]) -> Vec<Row> {
        let mut out = vec![];
        for r in rows {
            key_text(&mut self.text, self.keys.iter().map(|k| k(r)));
            let q = match self.held.get_mut(self.text.as_str()) {
                Some(q) => q,
                None => self.held.entry(self.text.clone()).or_default(),
            };
            q.push_back(r.clone());
            if q.len() > self.most {
                let mut first = q.pop_front().expect("a row");
                for (slot, n, _) in &self.calls {
                    first[*slot] = q[n - 1][*slot].clone();
                }
                out.push(first);
            }
        }
        out
    }

    /// Every row held, partition by partition in key order, with the next rows there are.
    fn flush(&mut self) -> Vec<Row> {
        let mut held: Vec<(String, VecDeque<Row>)> = self.held.drain().collect();
        held.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut out = vec![];
        for (_, mut q) in held {
            while let Some(mut first) = q.pop_front() {
                for (slot, n, default) in &self.calls {
                    first[*slot] = q.get(n - 1).map_or_else(|| default.clone(), |r| r[*slot].clone());
                }
                out.push(first);
            }
        }
        out
    }
}

/// What a ranking function gives (`Op::Section`).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Ranked {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile(i64),
}

/// A ranking function's call: what it gives, its slot, its PARTITION BY keys after the time,
/// and its ORDER BY keys: (key, descending, NULLs first).
#[derive(Clone)]
struct RankCall {
    ranked: Ranked,
    slot: usize,
    keys: Vec<Ex>,
    order: Vec<(Ex, bool, bool)>,
}

/// The ranking functions of a SELECT (`row_number`, `rank`, `dense_rank`, `percent_rank`,
/// `cume_dist`, `ntile`), over whole partitions, after the window functions (`Op::Over`, which
/// leaves their slots NULL). Their first PARTITION BY key is the time, which rows come in the
/// order of: the rows of one time (a cross-section) are held until a later time's row comes, then
/// each call ranks each of its partitions (the rest of its PARTITION BY) by its ORDER BY, ties in
/// the order the rows came, as DuckDB and PostgreSQL define the functions, and the rows leave in
/// the order they came. A row of an earlier time than the held ones' is late: dropped and
/// counted. At the end of the input (`close` to the end of time) the rows held are ranked.
///
/// ponytail: a live view emits a time's ranks when the next time's first row comes, not when
/// its sources' clocks pass it; release in `close` when a feed goes quiet if that matters.
#[derive(Clone)]
struct Section {
    time: Ex,
    calls: Vec<RankCall>,
    /// The calls as text, for the plan's fingerprint.
    describe: String,
    /// The rows' width, to check a snapshot's.
    width: usize,
    held: VecDeque<Row>,
    late: u64,
}

impl Section {
    fn apply(&mut self, rows: &[Row]) -> Vec<Row> {
        let mut out = vec![];
        for r in rows {
            if let Some(h) = self.held.front() {
                match order_by(&(self.time)(r), &(self.time)(h)) {
                    Ordering::Less => {
                        self.late += 1;
                        continue;
                    }
                    Ordering::Greater => out.extend(self.release()),
                    Ordering::Equal => {}
                }
            }
            self.held.push_back(r.clone());
        }
        out
    }

    /// The rows held, each call's slot its rank in its partition.
    fn release(&mut self) -> Vec<Row> {
        let mut rows: Vec<Row> = std::mem::take(&mut self.held).into();
        let mut text = String::new();
        for c in &self.calls {
            let mut parts: Vec<Vec<usize>> = vec![];
            if c.keys.is_empty() {
                parts.push((0..rows.len()).collect());
            } else {
                let mut by_key: FxHashMap<String, usize> = FxHashMap::default();
                for (i, r) in rows.iter().enumerate() {
                    key_text(&mut text, c.keys.iter().map(|k| k(r)));
                    let p = *by_key.entry(text.clone()).or_insert_with(|| {
                        parts.push(vec![]);
                        parts.len() - 1
                    });
                    parts[p].push(i);
                }
            }
            for part in parts {
                // ORDER BY's values once a row; a stable sort keeps ties in the order they came
                let keys: Vec<Vec<Value>> =
                    part.iter().map(|&i| c.order.iter().map(|o| (o.0)(&rows[i])).collect()).collect();
                let mut sorted: Vec<usize> = (0..part.len()).collect();
                sorted.sort_by(|&a, &b| c.cmp(&keys[a], &keys[b]));
                let n = part.len();
                let (mut first, mut dense) = (0, 0);
                while first < n {
                    // the peers: rows equal on every ORDER BY key
                    let end = (first + 1..n).find(|&j| c.cmp(&keys[sorted[j]], &keys[sorted[first]]).is_ne());
                    let end = end.unwrap_or(n);
                    dense += 1;
                    for (j, &k) in sorted.iter().enumerate().take(end).skip(first) {
                        rows[part[k]][c.slot] = match c.ranked {
                            Ranked::RowNumber => Value::Int(j as i64 + 1),
                            Ranked::Rank => Value::Int(first as i64 + 1),
                            Ranked::DenseRank => Value::Int(dense),
                            Ranked::PercentRank if n > 1 => Value::F64(first as f64 / (n - 1) as f64),
                            Ranked::PercentRank => Value::F64(0.0),
                            Ranked::CumeDist => Value::F64(end as f64 / n as f64),
                            Ranked::Ntile(b) => Value::Int(ntile(j as i64, n as i64, b)),
                        };
                    }
                    first = end;
                }
            }
        }
        rows
    }
}

impl RankCall {
    /// Two rows' ORDER BY values in the call's order: NULLs first or last as it says, NaN after
    /// the numbers (`order_by`), each key descending or not.
    fn cmp(&self, a: &[Value], b: &[Value]) -> Ordering {
        for ((x, y), &(_, desc, nulls_first)) in a.iter().zip(b).zip(&self.order) {
            let o = match (x, y) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) if nulls_first => Ordering::Less,
                (Value::Null, _) => Ordering::Greater,
                (_, Value::Null) if nulls_first => Ordering::Greater,
                (_, Value::Null) => Ordering::Less,
                _ if desc => order_by(y, x),
                _ => order_by(x, y),
            };
            if o.is_ne() {
                return o;
            }
        }
        Ordering::Equal
    }
}

/// The bucket of the `i`-th of `n` rows (from 0) in `b` buckets as even as can be, the larger
/// first: DuckDB's and PostgreSQL's `ntile`.
fn ntile(i: i64, n: i64, b: i64) -> i64 {
    let b = b.min(n);
    let size = n / b;
    // the first `large` buckets hold a row more
    let large = n - b * size;
    let in_large = large * (size + 1);
    if i < in_large {
        1 + i / (size + 1)
    } else {
        1 + large + (i - in_large) / size
    }
}

/// Rows in `ORDER BY` order: by each key in turn, as `order_by` sorts values (NaN, then NULL,
/// after the others). A total order, as a sort needs. Keys are evaluated as far as needed
/// rather than kept: the first mostly decides (a time, a symbol).
fn sorted(keys: &[Ex], a: &Row, b: &Row) -> Ordering {
    keys.iter().map(|k| order_by(&k(a), &k(b))).find(|o| o.is_ne()).unwrap_or(Ordering::Equal)
}

/// Rows a time-ordered operator (a window, an ASOF join) dropped before ordering them by time.
/// Runtime counts: not checkpointed.
#[derive(Clone, Default)]
struct Dropped {
    /// Past the limit (`Engine::set_time_limit`).
    future: u64,
    /// NULL, or not a time.
    null_time: u64,
}

impl Dropped {
    /// A row's event time, if the operator takes it. A time past `limit` (a corrupt feed, a
    /// ms/µs mix-up) would move a watermark, or an ASOF join's newest time, there for good; a
    /// row without a time (an absent proto field) has no place in time order: a window filed it
    /// under year -290308, an ASOF join matched it to every left row or held it for ever. Either
    /// is dropped and counted instead.
    #[inline(always)] // in every window's and join's row loop
    fn admit(&mut self, t: Option<i64>, limit: i64) -> Option<i64> {
        match t {
            None => self.null_time += 1,
            Some(t) if t > limit => self.future += 1,
            t => return t,
        }
        None
    }
}

/// An aggregate call: name, parameters, compiled arguments.
type AggSpec = (String, Vec<Value>, Vec<Ex>);

#[derive(Clone)]
struct Window {
    ts: Arg,
    width: i64,
    delay: i64,
    keys: Vec<Arg>,
    aggs: Vec<AggSpec>,
    /// Per aggregate: for a quantile of an argument an earlier quantile already samples, that
    /// one (`Acc::QuantileOf`).
    shares: Vec<Option<usize>>,
    /// Each aggregate's call as planned (`name[params](args)`): the plan fingerprint's part.
    agg_keys: Vec<String>,
    /// Over the group row `[window_start, window_end, keys.., aggregate results..]`.
    out: Vec<Ex>,
    watermark: i64,
    max_ts: i64,
    late: u64,
    dropped: Dropped,
    /// Windows that began before this are closed without being emitted (`Engine::withhold`; not
    /// checkpointed: the runtime sets it on every start).
    withhold_start: i64,
    /// Groups closed without being emitted (not checkpointed: a runtime count).
    withheld: u64,
    /// Groups closed and emitted (not checkpointed: a runtime count).
    closed: u64,
    /// The earliest end of the windows the last `apply` emitted (`i64::MAX`: none), the
    /// `Emit::window_end` of what they write (not checkpointed: of one call).
    emitted_from: i64,
    /// Open windows by start.
    open: BTreeMap<i64, Groups>,
    /// Scratch: the current row's key values and key text.
    key: Vec<Value>,
    text: String,
    /// Scratch: the row of the group being closed, `[window_start, window_end, keys.., results..]`,
    /// which `out` reads: one row for every group, not one per group grown as it is built.
    group: Row,
    /// Whether a group holds a t-digest (`Acc::holds_digest`): else no window has a lead.
    digests: bool,
    /// Per open window in its lead (by start): its groups still to merge ahead of the close
    /// (`LEAD`), by (merge time, key text), and how many of them have. Derived from the groups
    /// and `max_ts`, so not checkpointed.
    due: BTreeMap<i64, (Vec<(i64, String)>, usize)>,
    /// The `max_ts` from which `merge_due` has work: the earliest merge time still to come, or
    /// the lead of a window without its `due` yet (i64::MIN: unknown, look after the next row).
    lead_at: i64,
}

/// A window's lead, in µs before its end: from 2 s to 0.5 s. A t-digest merges its values into
/// its centroids every 512 values and, at the close, the fewer than 512 since (ADR-0014): at
/// the top of the hour that last merge was ~46% of trade-size's and range's close. So
/// each group's digests also merge once in their window's lead, at the group's merge time
/// (`merge_at`), and the close finds nothing left to merge, or the values since (ADR-0015).
///
/// A group merges after the row that moves `max_ts`, row by row, from before its merge time to
/// at or past it (`advance` as a row would). A group opened past its merge time does not merge
/// ahead. A window that closes row by row (its end at or before the row-by-row watermark)
/// merges nothing more: a chunk that closes it later must not merge more than row by row
/// would. So which groups merge after which row depends only on the rows, their event times
/// and the `max_ts` before each, never on chunks or threads, and a checkpoint holds `max_ts`:
/// after a restore, the groups still due (`Window::due`) are rebuilt on the next row, as those
/// whose merge time is past it.
const LEAD: (i64, i64) = (2_000_000, 500_000);

/// The merge time of the group `key` in the window ending at `end`, in `[end - LEAD.0,
/// end - LEAD.1)`: a hash of the key text spreads a window's groups evenly over its lead, in an
/// order fixed for every build. FNV-1a, then MurmurHash3's finalizer: FNV-1a's top bits alone
/// barely move with the last byte, and keys differing only there ("1:X", "1:Y") would merge
/// together. The hash and `LEAD` decide which values each digest merges when, so a change to
/// either is an output change (ADR-0015).
fn merge_at(key: &str, end: i64) -> i64 {
    let mut h = key.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ b as u64).wrapping_mul(0x0100_0000_01b3));
    h = (h ^ (h >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    h = (h ^ (h >> 33)).wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    let span = (LEAD.0 - LEAD.1) as u128;
    end.saturating_sub(LEAD.0).saturating_add(((h as u128 * span) >> 64) as i64)
}

impl Window {
    fn apply(&mut self, rows: &[Row], limit: i64) -> Vec<Row> {
        let prev = self.watermark;
        for r in rows {
            // past the limit, every later row would be late; without a time, the row opened a
            // window at i64::MIN, emitted by the first real row
            let Some(ts) = self.dropped.admit(self.ts.eval(r).i64(), limit) else { continue };
            if ts < prev {
                self.late += 1;
                continue;
            }
            let before = self.max_ts;
            self.max_ts = self.max_ts.max(ts);
            // Key values are copied into a scratch vector for the key text. Writing the text from
            // borrowed values executes fewer instructions but ran 5-25% slower (wall clock, every
            // example pipeline).
            self.key.clear();
            self.key.extend(self.keys.iter().map(|k| k.value(r)));
            key_text(&mut self.text, self.key.iter());
            let start = floor(ts, self.width);
            let end = start.saturating_add(self.width);
            let groups = match self.open.entry(start) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    if self.digests {
                        self.lead_at = self.lead_at.min(end.saturating_sub(LEAD.0));
                    }
                    e.insert(Groups::default())
                }
            };
            let accs = match groups.get_mut(self.text.as_str()) {
                Some((_, accs)) => accs,
                None => {
                    if let Some((due, done)) = self.due.get_mut(&start) {
                        // opened in its window's lead: due if its merge time is still to come
                        let at = merge_at(&self.text, end);
                        if at > before {
                            // after the merged ones (theirs were at or before `before`), in order;
                            // a new group's key is not there yet
                            let d = (at, self.text.clone());
                            let i = *done + due[*done..].binary_search(&d).unwrap_or_else(|i| i);
                            due.insert(i, d);
                            self.lead_at = self.lead_at.min(at);
                        }
                    }
                    let g = (self.key.clone(), accs(&self.aggs, &self.shares).expect("checked at plan time"));
                    &mut groups.entry(self.text.clone()).or_insert(g).1
                }
            };
            for (acc, (_, _, args)) in accs.iter_mut().zip(&self.aggs) {
                match args.as_slice() {
                    [a, b] => acc.add2(a(r), b(r)),
                    args => acc.add(args.first().map_or(Value::Null, |a| a(r))),
                }
            }
            if self.max_ts >= self.lead_at {
                self.merge_due(before);
            }
        }
        self.ripe()
    }

    /// Moves the watermark to what `max_ts` gives, and closes the windows it passes: their
    /// groups' rows, in window then key text order.
    fn ripe(&mut self) -> Vec<Row> {
        if self.max_ts != i64::MIN {
            self.watermark = floor(self.max_ts.saturating_sub(self.delay), self.width);
        }
        let (mut out, g) = (vec![], &mut self.group);
        self.emitted_from = i64::MAX;
        while let Some(e) = self.open.first_entry() {
            if e.key().saturating_add(self.width) > self.watermark {
                break;
            }
            let (start, groups) = e.remove_entry();
            self.due.remove(&start);
            if start < self.withhold_start {
                self.withheld += groups.len() as u64;
                continue;
            }
            self.closed += groups.len() as u64;
            // windows close in order: the first emitted ends first
            self.emitted_from = self.emitted_from.min(start.saturating_add(self.width));
            // in key text order, as when windows and keys shared one ordered map
            let mut groups: Vec<(String, Group)> = groups.into_iter().collect();
            groups.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            out.reserve(groups.len());
            for (_, (keys, mut accs)) in groups {
                g.clear();
                g.extend([Value::Time(start), Value::Time(start.saturating_add(self.width))]);
                g.extend(keys);
                crate::agg::results(&mut accs, g);
                out.push(self.out.iter().map(|e| e(g)).collect());
            }
        }
        g.clear();
        out
    }

    /// After a row that moved `max_ts` from `before`: merges the digests of the groups whose
    /// merge time it passed (`LEAD`), and sets `lead_at` to the next.
    fn merge_due(&mut self, before: i64) {
        // the watermark row by row: a window ending at or before it is closed there
        let watermark = floor(self.max_ts.saturating_sub(self.delay), self.width);
        let mut next = i64::MAX;
        for (&start, groups) in self.open.iter_mut() {
            let end = start.saturating_add(self.width);
            if end <= watermark {
                continue;
            }
            let lead = end.saturating_sub(LEAD.0);
            if self.max_ts < lead {
                // later windows' leads are later still
                next = next.min(lead);
                break;
            }
            let (due, done) = self.due.entry(start).or_insert_with(|| {
                // entering its lead (or the first row after a restore): the groups still due
                let mut due: Vec<(i64, String)> =
                    groups.keys().map(|k| (merge_at(k, end), k.clone())).filter(|(at, _)| *at > before).collect();
                due.sort_unstable();
                (due, 0)
            });
            // the groups whose merge time this row passed
            let n = due[*done..].partition_point(|d| d.0 <= self.max_ts);
            for (at, key) in &due[*done..*done + n] {
                debug_assert!(*at > before);
                if let Some((_, accs)) = groups.get_mut(key.as_str()) {
                    accs.iter_mut().for_each(Acc::premerge);
                }
            }
            *done += n;
            if let Some((at, _)) = due.get(*done) {
                next = next.min(*at);
            }
        }
        self.lead_at = next;
    }

    /// Whether `rows` (in this window's input) would close a window: their latest time it takes
    /// moves the watermark past the end of the oldest window open. Only an estimate of the work
    /// `apply` will do, for `Engine::insert` to spread closes over threads: a window a chunk both
    /// opens and closes is not counted.
    fn closes(&self, rows: &[Row], limit: i64) -> usize {
        if self.open.is_empty() {
            return 0;
        }
        let latest = rows.iter().filter_map(|r| self.ts.eval(r).i64()).filter(|t| *t <= limit).max();
        let max_ts = latest.map_or(self.max_ts, |t| t.max(self.max_ts));
        if max_ts == i64::MIN {
            return 0;
        }
        let watermark = floor(max_ts.saturating_sub(self.delay), self.width);
        let closing = self.open.iter().take_while(|(start, _)| start.saturating_add(self.width) <= watermark);
        closing.map(|(_, groups)| groups.len()).sum()
    }

    /// Closes what an event at `ts` would close, without a row: the watermark moves as if one
    /// had arrived (never back).
    fn advance(&mut self, ts: i64) -> Vec<Row> {
        let before = self.max_ts;
        self.max_ts = self.max_ts.max(ts);
        if self.max_ts >= self.lead_at {
            self.merge_due(before);
        }
        self.apply(&[], i64::MAX)
    }

    /// Open groups in (window start, key text) order: the checkpoint format.
    fn snapshot(&self) -> Vec<((i64, &str), &Group)> {
        let mut v = vec![];
        for (start, groups) in &self.open {
            let at = v.len();
            v.extend(groups.iter().map(|(k, g)| ((*start, k.as_str()), g)));
            v[at..].sort_unstable_by(|a, b| a.0 .1.cmp(b.0 .1));
        }
        v
    }
}

/// A new group's accumulators (`Window::aggs`, `Window::shares`).
fn accs(aggs: &[AggSpec], shares: &[Option<usize>]) -> R<Vec<Acc>> {
    let mut accs = Vec::with_capacity(aggs.len());
    for ((n, p, a), of) in aggs.iter().zip(shares) {
        accs.push(match (Acc::new(n, p, a.len())?, of) {
            (Acc::Quantile { level, .. } | Acc::TDigest { level, .. } | Acc::Cont { level, .. }, Some(of)) => {
                Acc::QuantileOf { level, of: *of }
            }
            (acc, _) => acc,
        });
    }
    Ok(accs)
}

/// A sequence aggregate's argument (`run_structure((time, id, side, price))`) is a tuple of its
/// width, with no string where its fold takes a number: the fold reads the items by position.
/// `Acc::new` has checked the number of arguments.
fn check_tuple(src: &Scope, name: &str, args: &[Expr]) -> R<()> {
    let Some(items) = crate::agg::tuple_items(name) else { return Ok(()) };
    let want = || items.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ");
    let call = || args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
    let tuple = match &args[0] {
        Expr::Tuple(t) if t.len() == items.len() => t,
        _ => return Err(format!("{name} takes the tuple ({}), got {name}({})", want(), call())),
    };
    match items.iter().zip(tuple).find(|((_, number), e)| *number && Compiler::new(src).is_string(e)) {
        Some(((item, _), e)) => Err(format!("{name}: its {item} {e} is a string")),
        None => Ok(()),
    }
}

/// Collects the aggregates of a windowed SELECT; their arguments see the source row.
struct Collect<'a> {
    src: &'a Scope,
    base: usize,
    aggs: Vec<(String, AggSpec)>,
    /// Per aggregate: the earlier one whose sampler it reads (`Window::shares`).
    shares: Vec<Option<usize>>,
    /// Per aggregate that keeps a sampler: the sampler's function and arguments.
    samplers: Vec<Option<String>>,
}

impl Aggregates for Collect<'_> {
    fn aggregate(&mut self, name: &str, params: &[Value], args: &[Expr]) -> R<Option<usize>> {
        if !crate::agg::is_aggregate(name) {
            return Ok(None);
        }
        Acc::new(name, params, args.len())?;
        check_tuple(self.src, name, args)?;
        // the arguments that must be numbers (or times): arg_max/arg_min order any value
        let numeric = match crate::agg::if_base(name).unwrap_or(name) {
            "latest" | "earliest" | "min" | "max" | "count" | "uniq_exact" | "arg_max" | "arg_min" => 0,
            _ => args.len(),
        };
        let call = || args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
        if args[..numeric].iter().any(|a| Compiler::new(self.src).is_string(a)) {
            return Err(format!("{name} of a string: {name}({})", call()));
        }
        let key = format!("{name}{params:?}({})", args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(","));
        if let Some(i) = self.aggs.iter().position(|(k, _)| *k == key) {
            return Ok(Some(self.base + i));
        }
        let sampler = match name {
            "quantile" | "median" => Some(format!("quantile({})", call())),
            "quantile_t_digest" | "median_tdigest" => Some(format!("quantile_t_digest({})", call())),
            "quantile_cont" => Some(format!("quantile_cont({})", call())),
            "quantile_exact" => Some(format!("quantile_exact({})", call())),
            _ => None,
        };
        let of = self.samplers.iter().position(|s| s.is_some() && *s == sampler);
        self.shares.push(of);
        self.samplers.push(if of.is_some() { None } else { sampler });
        let args = args.iter().map(|a| Compiler::new(self.src).compile(a)).collect::<R<Vec<_>>>()?;
        self.aggs.push((key, (name.to_string(), params.to_vec(), args)));
        Ok(Some(self.base + self.aggs.len() - 1))
    }
}

/// Collects the window functions of a SELECT over a stream; their arguments, partition keys
/// and ORDER BY see the input row, and each result lands after the input's columns.
struct CollectWindows<'a> {
    src: &'a Scope,
    base: usize,
    named: &'a [ast::NamedWindowDefinition],
    groups: Vec<OverGroup>,
    /// `lead` calls: (slot, n, default, PARTITION BY ... ORDER BY ... as text, partition keys).
    leads: Vec<(usize, usize, Value, String, Vec<Ex>)>,
    /// Ranking calls (`Op::Section`): (the time, as text and compiled; the call; the call as text).
    ranks: Vec<(String, Ex, RankCall, String)>,
}

impl CollectWindows<'_> {
    /// The window specification an OVER clause holds or names (`WINDOW w AS (...)`, possibly
    /// through other names).
    fn spec<'s>(&'s self, over: &'s ast::WindowType) -> R<&'s ast::WindowSpec> {
        let mut name = match over {
            ast::WindowType::WindowSpec(s) if s.window_name.is_none() => return Ok(s),
            ast::WindowType::WindowSpec(s) => {
                return Err(format!("a window built on window {:?} is not supported", s.window_name))
            }
            ast::WindowType::NamedWindow(n) => n,
        };
        for _ in 0..=self.named.len() {
            let def = self.named.iter().find(|d| d.0.value == name.value).ok_or(format!("unknown window {name}"))?;
            match &def.1 {
                ast::NamedWindowExpr::WindowSpec(s) if s.window_name.is_none() => return Ok(s),
                ast::NamedWindowExpr::NamedWindow(n) => name = n,
                e => return Err(format!("unsupported window definition {e}")),
            }
        }
        Err(format!("window {name} is defined in terms of itself"))
    }
}

impl CollectWindows<'_> {
    /// The calls before this one: its result's slot is the next.
    fn slot(&self) -> usize {
        self.base + self.groups.iter().map(|g| g.specs.len()).sum::<usize>() + self.ranks.len()
    }

    /// A ranking function over whole partitions, its first PARTITION BY key the time
    /// (`Op::Section`).
    fn rank(
        &mut self,
        name: &str,
        params: &[Value],
        args: &[Expr],
        spec: &ast::WindowSpec,
        described: String,
    ) -> R<usize> {
        let ranked = match (name, args) {
            ("row_number", []) => Ranked::RowNumber,
            ("rank", []) => Ranked::Rank,
            ("dense_rank", []) => Ranked::DenseRank,
            ("percent_rank", []) => Ranked::PercentRank,
            ("cume_dist", []) => Ranked::CumeDist,
            ("ntile", [Expr::Value(ast::ValueWithSpan { value: ast::Value::Number(n, _), .. })])
                if n.parse::<i64>().is_ok_and(|n| n > 0) =>
            {
                Ranked::Ntile(n.parse().expect("checked"))
            }
            ("ntile", _) => return Err(format!("ntile takes a whole number of buckets from 1: {described}")),
            _ => return Err(format!("{name} takes no arguments: {described}")),
        };
        if !params.is_empty() {
            return Err(format!("{name} takes no parameters"));
        }
        if spec.window_frame.as_ref().is_some_and(|f| !whole_partition(f)) {
            return Err(format!("{name} ranks the whole partition: it takes no frame, got {described}"));
        }
        let Some((time, keys)) = spec.partition_by.split_first() else {
            return Err(format!(
                "{name} ranks the rows of each time: PARTITION BY the time first, then any keys: {described}"
            ));
        };
        if spec.order_by.is_empty() {
            return Err(format!("{name} needs ORDER BY what it ranks by: {described}"));
        }
        let compile = |e: &Expr| Compiler::new(self.src).compile(e);
        let mut order = vec![];
        for o in &spec.order_by {
            if o.with_fill.is_some() {
                return Err(format!("WITH FILL is not supported in a window: {o}"));
            }
            let desc = matches!(o.options.sort, Some(ast::OrderBySort::Desc));
            // NULLs sort as the largest value, as in PostgreSQL and the outermost ORDER BY
            order.push((compile(&o.expr)?, desc, o.options.nulls_first.unwrap_or(desc)));
        }
        let keys = keys.iter().map(compile).collect::<R<Vec<_>>>()?;
        let call = RankCall { ranked, slot: self.slot(), keys, order };
        self.ranks.push((time.to_string(), compile(time)?, call, described));
        Ok(self.ranks.last().expect("pushed").2.slot)
    }
}

/// `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`: the whole partition, which a
/// `row_number()` ranked over the rows of each time says it numbers.
fn whole_partition(f: &ast::WindowFrame) -> bool {
    use ast::WindowFrameBound as B;
    matches!((&f.start_bound, &f.end_bound), (B::Preceding(None), Some(B::Following(None))))
}

impl Windows for CollectWindows<'_> {
    fn window(&mut self, name: &str, params: &[Value], args: &[Expr], over: &ast::WindowType) -> R<usize> {
        let spec = self.spec(over)?.clone();
        let described = format!(
            "{name}{params:?}({}) OVER ({spec})",
            args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(",")
        );
        let whole = spec.window_frame.as_ref().is_some_and(whole_partition);
        if matches!(name, "rank" | "dense_rank" | "percent_rank" | "cume_dist" | "ntile")
            || name == "row_number" && whole
        {
            return self.rank(name, params, args, &spec, described);
        }
        let mut order = vec![];
        for o in &spec.order_by {
            let desc = o.options.sort.as_ref().is_some_and(|s| !matches!(s, ast::OrderBySort::Asc));
            if desc || o.options.nulls_first.is_some() || o.with_fill.is_some() {
                return Err(format!("only ascending ORDER BY is supported in a window: {o}"));
            }
            order.push(&o.expr);
        }
        let frame = frame(spec.window_frame.as_ref(), order.len())?;
        let literal = |e: &Expr| match e {
            Expr::Value(v) => Ok(match &v.value {
                ast::Value::Number(n, _) => number_value(n)?,
                ast::Value::SingleQuotedString(s) => Value::Str(s.as_str().into()),
                ast::Value::Null => Value::Null,
                v => return Err(format!("unsupported literal {v}")),
            }),
            Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => match expr.as_ref() {
                Expr::Value(ast::ValueWithSpan { value: ast::Value::Number(n, _), .. }) => {
                    number_value(&format!("-{n}"))
                }
                e => Err(format!("{name} expects a literal, got -{e}")),
            },
            e => Err(format!("{name} expects a literal, got {e}")),
        };
        let no_frame = |what: &str| match &spec.window_frame {
            Some(f) => Err(format!("{what} takes no frame, got {}", show(f))),
            None => Ok(()),
        };
        let mut compiled = args.iter().map(|a| Compiler::new(self.src).compile(a));
        let mut lead = None;
        let (func, args): (Func, Vec<Ex>) = match (name, args) {
            // the row's own `x` here; `Op::Lead` puts the next row's in
            ("lag" | "lead", [x, rest @ ..]) if rest.len() <= 2 => {
                no_frame(name)?;
                let offset = match rest.first().map(literal).transpose()? {
                    None => 1,
                    Some(v) => match v.i64() {
                        Some(n) if (1..=crate::over::MAX_ROWS as i64).contains(&n) => n as usize,
                        _ => {
                            return Err(format!(
                                "{name} offset {v:?} is not a whole number from 1 to {}",
                                crate::over::MAX_ROWS
                            ))
                        }
                    },
                };
                let default = rest.get(1).map(literal).transpose()?.unwrap_or(Value::Null);
                let x = vec![Compiler::new(self.src).compile(x)?];
                if name == "lag" {
                    (Func::Lag { offset, default }, x)
                } else {
                    lead = Some((offset, default));
                    (Func::Value { first: false, frame: Frame::Rows(0) }, x)
                }
            }
            ("nth_value", _) => return Err(format!("{name} is not supported as a window function")),
            ("row_number", []) => {
                no_frame("row_number")?;
                (Func::RowNumber, vec![])
            }
            ("first_value" | "last_value", [x]) => {
                let first = name == "first_value";
                // every frame ends at the current row, so its last value is the current row's
                let frame = if first { frame } else { Frame::Rows(0) };
                (Func::Value { first, frame }, vec![Compiler::new(self.src).compile(x)?])
            }
            ("avg", [x, kind, v]) => {
                no_frame("an exponential moving average")?;
                let v = literal(v)?.f64().ok_or(format!("{name}: {v} is not a number"))?;
                let alpha = match literal(kind)?.str() {
                    Some("alpha") if v > 0.0 && v <= 1.0 => v,
                    Some("period") if v >= 1.0 => 2.0 / (v + 1.0),
                    _ => {
                        return Err(format!(
                            "an exponential moving average takes 'alpha' in (0, 1] or 'period' >= 1: {described}"
                        ))
                    }
                };
                (Func::Ema { alpha }, vec![Compiler::new(self.src).compile(x)?])
            }
            (name, args) if crate::agg::is_aggregate(name) => {
                Acc::new(name, params, args.len())?;
                check_tuple(self.src, name, args)?;
                let args = compiled.by_ref().collect::<R<Vec<_>>>()?;
                (Func::Agg { name: name.to_string(), params: params.to_vec(), nargs: args.len(), frame }, args)
            }
            (name, args) => return Err(format!("unknown window function {name} of {} arguments", args.len())),
        };
        if !params.is_empty() && !matches!(func, Func::Agg { .. }) {
            return Err(format!("{name} takes no parameters"));
        }
        let partition = spec.partition_by.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",");
        let order_key = order.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",");
        // results follow the input's columns in the order the calls are met
        let slot = self.slot();
        let g =
            match self.groups.iter().position(|g| g.specs[0].describe.ends_with(&format!("[{partition}|{order_key}]")))
            {
                Some(i) => i,
                None => {
                    let keys =
                        spec.partition_by.iter().map(|k| Compiler::new(self.src).operand(k)).collect::<R<Vec<_>>>()?;
                    let order = order.first().map(|o| Compiler::new(self.src).operand(o)).transpose()?;
                    self.groups.push(OverGroup::new(keys, order));
                    self.groups.len() - 1
                }
            };
        let describe = format!("{described} [{partition}|{order_key}]");
        if let Some((n, default)) = lead {
            let keys = spec.partition_by.iter().map(|k| Compiler::new(self.src).compile(k)).collect::<R<Vec<_>>>()?;
            self.leads.push((slot, n, default, format!("{partition}|{order_key}"), keys));
        }
        self.groups[g].specs.push(Spec { func, args, slot, describe });
        Ok(slot)
    }
}

fn number_value(n: &str) -> R<Value> {
    if let Ok(i) = n.parse::<i64>() {
        return Ok(Value::Int(i));
    }
    n.parse::<f64>().map(Value::F64).map_err(|_| format!("{n:?} is not a number"))
}

/// A window frame: the default (no frame) and `UNBOUNDED PRECEDING` are every row so far;
/// `ROWS n PRECEDING` the row and n before it; `RANGE <interval | n> PRECEDING` the rows within
/// that distance of the row's ORDER BY value (a single one). It ends at the current row.
fn frame(f: Option<&ast::WindowFrame>, order_keys: usize) -> R<Frame> {
    use ast::{WindowFrameBound as B, WindowFrameUnits as U};
    let Some(f) = f else { return Ok(Frame::Cumulative) };
    if !matches!(f.end_bound, None | Some(B::CurrentRow)) || matches!(f.start_bound, B::Following(_)) {
        return Err(format!("a frame must end at the current row (a stream has not seen later rows): {}", show(f)));
    }
    let n = match &f.start_bound {
        B::Preceding(None) => return Ok(Frame::Cumulative),
        B::CurrentRow => None,
        B::Preceding(Some(e)) => Some(e.as_ref()),
        B::Following(_) => unreachable!("refused above"),
    };
    match f.units {
        U::Rows => {
            let rows = match n {
                None => 0,
                Some(Expr::Value(ast::ValueWithSpan { value: ast::Value::Number(n, _), .. })) => n
                    .parse::<usize>()
                    .ok()
                    .filter(|n| *n < crate::over::MAX_ROWS)
                    .ok_or(format!("ROWS {n} PRECEDING: a whole number below {} expected", crate::over::MAX_ROWS))?,
                Some(e) => return Err(format!("ROWS {e} PRECEDING: a number expected")),
            };
            Ok(Frame::Rows(rows))
        }
        U::Range if order_keys != 1 => Err(format!("a RANGE frame needs exactly one ORDER BY key: {}", show(f))),
        U::Range => Ok(Frame::Range(match n {
            None => 0,
            Some(e) => range_offset(e)?,
        })),
        U::Groups => Err(format!("GROUPS frames are not supported: {}", show(f))),
    }
}

fn show(f: &ast::WindowFrame) -> String {
    match &f.end_bound {
        Some(end) => format!("{} BETWEEN {} AND {end}", f.units, f.start_bound),
        None => format!("{} {}", f.units, f.start_bound),
    }
}

/// A time plus a constant (µs): `l.ts - INTERVAL '1 second'` is `(l.ts, -1_000_000)`.
fn offset_of(e: &Expr) -> (Expr, i64) {
    match e {
        Expr::Nested(x) => offset_of(x),
        Expr::BinaryOp { left, op: op @ (BinaryOperator::Plus | BinaryOperator::Minus), right } => {
            match range_offset(right) {
                Ok(n) => {
                    let (base, off) = offset_of(left);
                    (base, if *op == BinaryOperator::Plus { off + n } else { off - n })
                }
                Err(_) => (e.clone(), 0),
            }
        }
        e => (e.clone(), 0),
    }
}

/// A RANGE offset: `INTERVAL '30' SECOND` (and the like) in microseconds, for a time ORDER BY;
/// a plain number is in the ORDER BY value's own units.
fn range_offset(e: &Expr) -> R<i64> {
    let bad = || format!("RANGE {e} PRECEDING: an INTERVAL or a whole number expected");
    match e {
        Expr::Value(ast::ValueWithSpan { value: ast::Value::Number(n, _), .. }) => n.parse().map_err(|_| bad()),
        Expr::Interval(i) => {
            let text = match i.value.as_ref() {
                Expr::Value(ast::ValueWithSpan { value: ast::Value::SingleQuotedString(s), .. }) => s.clone(),
                Expr::Value(ast::ValueWithSpan { value: ast::Value::Number(n, _), .. }) => n.clone(),
                _ => return Err(bad()),
            };
            // `INTERVAL '5 minutes'`: the unit in the text
            let Some(unit) = i.leading_field.as_ref().map(|u| u.to_string()) else {
                return crate::value::duration_us(&text).ok_or_else(bad);
            };
            let n: i64 = text.trim().parse().map_err(|_| bad())?;
            let mul = match unit.to_ascii_uppercase().trim_end_matches('S') {
                "MICROSECOND" => 1,
                "MILLISECOND" => 1_000,
                "SECOND" => 1_000_000,
                "MINUTE" => 60_000_000,
                "HOUR" => 3_600_000_000,
                "DAY" => 86_400_000_000,
                _ => return Err(bad()),
            };
            n.checked_mul(mul).filter(|us| *us >= 0).ok_or_else(bad)
        }
        _ => Err(bad()),
    }
}

/// How an ASOF LEFT JOIN matches a left row (`Engine::set_asof`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Asof {
    /// Proton's: a left row is emitted at once, with the newest of the right rows that have
    /// arrived at or before its time, among the `keep_versions` (default 3) newest per key.
    #[default]
    Arrival,
    /// A batch engine's as-of join: a left row waits until every right side is
    /// `ASOF_LATENESS_US` past its time (at most `ASOF_HOLD_US` of left time; a right stream
    /// arrives only roughly in time order across keys), then takes the right row with the
    /// greatest time at or before its own. Rows released together come out in time order; a
    /// left row that arrives after a newer one was released still comes out after it.
    Exact,
}

/// The longest an exact ASOF join holds a left row for a stalled right side, in left time.
pub const ASOF_HOLD_US: i64 = 30_000_000;
/// How far out of time order an exact join's rows may arrive (across keys, on either side) and
/// still be matched exactly: a left row is released once every right side is this far past it,
/// and right versions are kept for left rows this far behind the ones seen.
pub const ASOF_LATENESS_US: i64 = 1_000_000;
/// Proton's default `keep_versions`.
const KEEP_VERSIONS: usize = 3;
/// An exact join's cap per key, which time-based eviction keeps far below in practice.
const EXACT_MAX_VERSIONS: usize = 100_000;
/// Keys an ASOF right side keeps by default: far more symbols than a venue lists, so only a
/// flood of distinct keys (a corrupt feed) ever reaches it.
pub const ASOF_MAX_KEYS: usize = 100_000;

#[derive(Clone)]
struct Right {
    left_key: Vec<Arg>,
    left_ts: Arg,
    key: Vec<Arg>,
    ts: Arg,
    defaults: Row,
    /// Per key text, the latest versions by descending time.
    versions: FxHashMap<String, Keyed>,
    /// At most this many keys: past it, the least recently updated are evicted.
    max_keys: usize,
    evicted: u64,
    /// Exact: right rows at or before a left row of their key already released (`Keyed`).
    late: u64,
    /// The newest right time seen (exact joins).
    max_ts: i64,
    /// Exact: how far past a left row's time this side must be before the row is released
    /// (`ASOF_LATENESS_US`, or `Engine::set_asof_lateness`).
    lateness: i64,
    /// A window join's: aggregates over the right rows of a time range, not the latest row.
    within: Option<Box<Within>>,
}

/// A window join's right side (kdb+'s `wj`, QuestDB's WINDOW JOIN): `LEFT JOIN LATERAL (SELECT
/// aggregates FROM r WHERE r.k = l.k AND r.ts BETWEEN l.ts - a AND l.ts + b) ON true`. The left
/// row's join time is its range's end (`l.ts + b`), so an exact join releases it once the right
/// side has passed that; it takes the aggregates of its key's right rows from `span` before that
/// end to it, both ends included, in time order (over no rows, `count` 0 and the others NULL).
///
/// ponytail: each left row adds up its range's rows anew, O(rows in range); slide the
/// aggregates as `over` does its frames if a range holds thousands of rows.
#[derive(Clone)]
struct Within {
    span: i64,
    /// The subquery's conditions on the right rows alone: the rows it does not keep are none.
    filter: Option<Pred>,
    aggs: Vec<AggSpec>,
    shares: Vec<Option<usize>>,
    /// The subquery's items, over the aggregates.
    out: Vec<Ex>,
    /// The aggregates over no rows.
    empty: Row,
}

impl Within {
    /// The items over `versions` (newest first) from `span` before `t` to `t`.
    fn results(&self, versions: &[(Value, Row)], t: Option<i64>) -> Vec<Value> {
        let mut g = self.empty.clone();
        if let Some(t) = t {
            let lo = t.saturating_sub(self.span);
            let mut accs = accs(&self.aggs, &self.shares).expect("checked at plan time");
            let mut any = false;
            for (_, r) in versions.iter().rev().filter(|(s, _)| s.i64().is_some_and(|s| lo <= s && s <= t)) {
                any = true;
                for (acc, (_, _, args)) in accs.iter_mut().zip(&self.aggs) {
                    match args.as_slice() {
                        [a, b] => acc.add2(a(r), b(r)),
                        args => acc.add(args.first().map_or(Value::Null, |a| a(r))),
                    }
                }
            }
            if any {
                g.clear();
                crate::agg::results(&mut accs, &mut g);
            }
        }
        self.out.iter().map(|e| e(&g)).collect()
    }
}

/// A right side's key: its versions, and the newest time of a left row an exact join has
/// released with them. A right row at or before that time arrived too late to be that row's
/// match, and is counted (`Engine::asof_late_right`): a lateness too short for the feed would
/// otherwise go unnoticed. Kept with the versions, so it costs no lookup of its own and goes with
/// them when the key is evicted. Not checkpointed (the format stays as it is): after a restore,
/// and for a key without versions when its left rows are released, nothing is counted until
/// the key's next release.
#[derive(Clone)]
struct Keyed {
    versions: Versions,
    released: Option<i64>,
}

impl Keyed {
    fn restore(v: Vec<(String, Versions)>) -> FxHashMap<String, Keyed> {
        v.into_iter().map(|(k, versions)| (k, Keyed { versions, released: None })).collect()
    }
}

impl Right {
    /// A left row's time against this side, if the join takes it.
    fn left_time(&self, row: &Row, dropped: &mut Dropped, limit: i64) -> Option<i64> {
        dropped.admit(self.left_ts.eval(row).i64(), limit)
    }

    /// Evicts the least recently updated tenth of the keys (the oldest newest version; key text
    /// breaks ties), so their left rows get the defaults, as before any version arrived.
    /// Without a bound, every key ever seen stays in memory and in every checkpoint.
    fn evict_stalest(&mut self) {
        let n = (self.versions.len() / 10).max(1);
        let newest = |k: &Keyed| k.versions.first().and_then(|t| t.0.i64()).unwrap_or(i64::MIN);
        let mut by_age: Vec<(i64, &String)> = self.versions.iter().map(|(k, v)| (newest(v), k)).collect();
        by_age.sort_unstable(); // by time, then key text: which keys go is deterministic
        let stalest: Vec<String> = by_age[..n].iter().map(|(_, k)| (*k).clone()).collect();
        for k in stalest {
            self.versions.remove(&k);
        }
        self.evicted += n as u64;
    }
}

#[derive(Clone)]
struct Join {
    /// Operators of each input before the join: the left side, then each right side.
    sides: Vec<Vec<Op>>,
    rights: Vec<Right>,
    /// `Asof::Exact`; otherwise versions are cut to `keep` per key (`SETTINGS keep_versions`).
    exact: bool,
    keep: usize,
    /// Exact: left rows waiting for the right sides, by time (one per right side).
    held: VecDeque<(Vec<i64>, Row)>,
    left_max: i64,
    dropped: Dropped,
    /// Scratch: the key text of the current row.
    text: String,
}

impl Join {
    fn apply(&mut self, side: usize, rows: &[Row], limit: i64) -> Vec<Row> {
        let rows = run(&mut self.sides[side], 0, rows, limit);
        self.take(side, rows, limit)
    }

    /// Rows that have passed their side's operators.
    fn take(&mut self, side: usize, rows: Vec<Row>, limit: i64) -> Vec<Row> {
        if side > 0 {
            let r = &mut self.rights[side - 1];
            let mut touched = vec![];
            let (span, filter) = r.within.as_ref().map_or((0, None), |w| (w.span, w.filter.clone()));
            for row in rows {
                if filter.as_ref().is_some_and(|f| f(&row) != Some(true)) {
                    continue;
                }
                let t = r.ts.eval(&row).into_owned();
                // past the limit, `max_ts` would pass every left row (released at once, exact
                // mode becoming arrival mode) and prune the versions they need; in arrival mode
                // the row would hold one of the `keep` slots for good. Without a time, it matched
                // every left row, and the pruning kept it instead of the real versions.
                let Some(ts) = self.dropped.admit(t.i64(), limit) else { continue };
                r.max_ts = r.max_ts.max(ts);
                key_text(&mut self.text, r.key.iter().map(|k| k.eval(&row)));
                if !r.versions.contains_key(self.text.as_str()) {
                    if r.versions.len() >= r.max_keys {
                        r.evict_stalest();
                    }
                    r.versions.insert(self.text.clone(), Keyed { versions: vec![], released: None });
                }
                let k = r.versions.get_mut(self.text.as_str()).expect("inserted above");
                // equal times match, so one at the released time is late too
                r.late += u64::from(k.released.is_some_and(|l| ts <= l));
                let v = &mut k.versions;
                let at = v.iter().position(|(s, _)| compare(s, &t) != Some(Ordering::Greater)).unwrap_or(v.len());
                v.insert(at, (t, row));
                match self.exact {
                    // a window join's rows of its longest range behind the newest
                    false if r.within.is_some() => {
                        let oldest = Value::Time(r.max_ts.saturating_sub(span).saturating_sub(r.lateness));
                        let keep = v.iter().position(|(s, _)| compare(s, &oldest) == Some(Ordering::Less));
                        v.truncate(keep.unwrap_or(v.len()));
                    }
                    false => v.truncate(self.keep),
                    true => touched.push(self.text.clone()),
                }
            }
            if !self.exact {
                return vec![];
            }
            let out = self.release(&[]);
            // after releasing: keep every version after the time a held or later left row can
            // still need, and the newest at or before it. With nothing held, a left row older
            // than the hold behind the right side is released as it comes, so that bounds it.
            let r = &mut self.rights[side - 1];
            let need = match self.held.front() {
                Some((t, _)) => t[side - 1].min(self.left_max),
                None => self.left_max.max(r.max_ts.saturating_sub(ASOF_HOLD_US)),
            };
            let need = Value::Time(need.saturating_sub(r.lateness).saturating_sub(span));
            touched.sort_unstable();
            touched.dedup();
            for text in touched {
                // an evicted key (a flood of keys) is gone already
                if let Some(Keyed { versions: v, .. }) = r.versions.get_mut(text.as_str()) {
                    let keep =
                        v.iter().position(|(s, _)| matches!(compare(s, &need), Some(Ordering::Less | Ordering::Equal)));
                    v.truncate(keep.map_or(v.len(), |i| i + 1).min(EXACT_MAX_VERSIONS));
                }
            }
            return out;
        }
        // past the limit, `left_max` would release every later left row at once (exact mode
        // becoming arrival mode, for good); without a time, an exact join held the row (and
        // checkpointed it) until a timed row came, then joined it to the newest version
        let (rights, dropped) = (&self.rights, &mut self.dropped);
        if !self.exact {
            let mut rows = rows;
            rows.retain(|row| rights.iter().all(|r| r.left_time(row, dropped, limit).is_some()));
            return rows.into_iter().map(|row| self.joined(row)).collect();
        }
        for row in rows {
            let t: Option<Vec<i64>> = rights.iter().map(|r| r.left_time(&row, dropped, limit)).collect();
            let Some(t) = t else { continue };
            self.left_max = self.left_max.max(t[0]);
            // after equal times: ties keep their arrival order
            let at = self.held.partition_point(|(h, _)| h[0] <= t[0]);
            self.held.insert(at, (t, row));
        }
        self.release(&[])
    }

    /// Exact: the held left rows every right side is its `lateness` past, or that waited
    /// `ASOF_HOLD_US`. `reached`, when not empty, is the time each side has reached without a
    /// row (`Join::close`), which counts as that side's newest time.
    fn release(&mut self, reached: &[i64]) -> Vec<Row> {
        let at = |side: usize, newest: i64| reached.get(side).map_or(newest, |r| newest.max(*r));
        let left_max = at(0, self.left_max);
        let mut out = vec![];
        while let Some((t, _)) = self.held.front() {
            let passed = (self.rights.iter().zip(t).enumerate())
                .all(|(i, (r, t))| at(i + 1, r.max_ts) >= t.saturating_add(r.lateness));
            if !passed && t[0] >= left_max.saturating_sub(ASOF_HOLD_US) {
                break;
            }
            let (_, row) = self.held.pop_front().expect("front");
            out.push(self.joined(row));
        }
        out
    }

    /// Closes each side as far as its input has reached (`close`; right sides first, so that
    /// what a closing right window emits is a version before any left row is matched), takes
    /// what they emit, and releases the held rows the sides' reached times pass, as rows at
    /// those times would. The join's own newest times are not moved: rows that come later are
    /// matched exactly as before. Returns the rows out and the time the output has reached:
    /// the left side's, or the oldest row still held.
    ///
    /// A row released this way is matched with the versions at hand; a version at or before
    /// its time arriving later can no longer be used for it. Idle close (the only caller) runs
    /// once every source is read to its end and has been silent for longer than such a version
    /// could be late (`ASOF_LATENESS_US`); without it, the row would be released when traffic
    /// resumed, below the watermark idle close gave the windows downstream, and dropped there.
    fn close(&mut self, reached: &[i64], limit: i64) -> (Vec<Row>, i64) {
        let mut sides = vec![i64::MIN; self.sides.len()];
        let mut out = vec![];
        for s in (1..self.sides.len()).chain([0]) {
            let (rows, at) = close(&mut self.sides[s], &reached[s..=s], limit);
            sides[s] = at;
            out.extend(self.take(s, rows, limit));
        }
        out.extend(self.release(&sides));
        let at = self.held.front().map_or(sides[0], |(t, _)| t[0].min(sides[0]));
        (out, at)
    }

    /// `row` with each right side's newest version at or before its time, or its defaults.
    /// Exact: notes the row's time as its key's newest released (`Keyed`).
    fn joined(&mut self, mut row: Row) -> Row {
        for r in &mut self.rights {
            key_text(&mut self.text, r.left_key.iter().map(|k| k.eval(&row)));
            let t = r.left_ts.eval(&row);
            let k = r.versions.get_mut(self.text.as_str()).map(|k| {
                if self.exact {
                    k.released = k.released.max(t.i64());
                }
                &*k
            });
            if let Some(w) = &r.within {
                let got = w.results(k.map_or(&[][..], |k| &k.versions), t.i64());
                row.extend(got);
                continue;
            }
            let hit = k.and_then(|k| {
                k.versions.iter().find(|(s, _)| matches!(compare(s, &t), Some(Ordering::Less | Ordering::Equal)))
            });
            row.extend(hit.map_or(&r.defaults, |(_, m)| m).iter().cloned());
        }
        row
    }
}

/// A planned relation: the streams feeding it (one per join side) and its operators.
struct Planned {
    inputs: Vec<String>,
    ops: Vec<Op>,
    schema: Schema,
    tumble: Option<(Arg, i64)>,
}

struct Planner<'a> {
    cat: &'a Catalog,
    view: &'a View,
}

fn duration_us(s: &str) -> R<i64> {
    let at = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let n: i64 = s[..at].parse().map_err(|_| format!("bad duration {s}"))?;
    let unit = match &s[at..] {
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60_000_000,
        "h" => 3_600_000_000,
        "d" => 86_400_000_000,
        u => return Err(format!("bad duration unit {u}")),
    };
    match n.checked_mul(unit) {
        Some(us) if us > 0 => Ok(us),
        _ => Err(format!("bad duration {s}")),
    }
}

fn arg(a: &FunctionArg) -> R<&Expr> {
    match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e),
        a => Err(format!("unsupported argument {a}")),
    }
}

fn name_of(e: &Expr) -> String {
    match e {
        Expr::Identifier(i) => i.value.clone(),
        Expr::CompoundIdentifier(ids) => ids.last().map_or(String::new(), |i| i.value.clone()),
        e => e.to_string(),
    }
}

fn aliases(s: &Select) -> Vec<(String, Expr)> {
    let items = s.projection.iter();
    items
        .filter_map(|i| {
            if let SelectItem::ExprWithAlias { expr, alias } = i {
                Some((alias.value.clone(), expr.clone()))
            } else {
                None
            }
        })
        .collect()
}

impl Planner<'_> {
    fn query(&self, q: &Query, ctes: &HashMap<String, Query>) -> R<Planned> {
        // what sqlparser accepts but the planner does not implement is an error, never ignored
        let unsupported = [
            (q.limit_clause.is_some(), "LIMIT/OFFSET"),
            (q.fetch.is_some(), "FETCH"),
            (!q.locks.is_empty(), "FOR UPDATE/SHARE"),
            (q.for_clause.is_some(), "FOR"),
            (q.settings.is_some(), "SETTINGS in a query"),
            (q.format_clause.is_some(), "FORMAT"),
            (!q.pipe_operators.is_empty(), "pipe operators"),
            (q.with.as_ref().is_some_and(|w| w.recursive), "WITH RECURSIVE"),
        ];
        if let Some((_, what)) = unsupported.iter().find(|u| u.0) {
            return Err(format!("{what} is not supported"));
        }
        let mut ctes = ctes.clone();
        for c in q.with.iter().flat_map(|w| &w.cte_tables) {
            ctes.insert(c.alias.name.value.clone(), (*c.query).clone());
        }
        let SetExpr::Select(s) = q.body.as_ref() else { return Err(format!("unsupported query body {}", q.body)) };
        let mut p = self.select(s, &ctes)?;
        if let Some(ob) = &q.order_by {
            let OrderByKind::Expressions(es) = &ob.kind else { return Err("ORDER BY ALL is not supported".into()) };
            let scope = p.schema.scope();
            if ob.interpolate.is_some() {
                return Err("ORDER BY ... INTERPOLATE is not supported".into());
            }
            let keys = es.iter().map(|e| {
                if e.options.sort.as_ref().is_some_and(|s| !matches!(s, ast::OrderBySort::Asc)) {
                    return Err("only ascending ORDER BY is supported".to_string());
                }
                if e.options.nulls_first.is_some() || e.with_fill.is_some() {
                    return Err("ORDER BY ... NULLS FIRST/LAST or WITH FILL is not supported".to_string());
                }
                Compiler::new(&scope).compile(&e.expr)
            });
            p.ops.push(Op::Sort(keys.collect::<R<_>>()?));
        }
        Ok(p)
    }

    fn stream(&self, name: &str) -> R<Planned> {
        let s = self.cat.streams.get(name).ok_or_else(|| format!("unknown stream {name}"))?;
        Ok(Planned { inputs: vec![name.to_string()], ops: vec![], schema: Schema::of(s), tumble: None })
    }

    fn relation(&self, t: &TableFactor, ctes: &HashMap<String, Query>) -> R<Planned> {
        // what a relation can say that is not planned is refused, never ignored; every field is named, so a new one sqlparser adds is seen here
        let (alias, unplanned) = match t {
            TableFactor::Table {
                name: _,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => (
                alias,
                [
                    args.as_ref().is_some_and(|a| a.settings.is_some()),
                    !with_hints.is_empty(),
                    version.is_some(),
                    *with_ordinality,
                    !partitions.is_empty(),
                    json_path.is_some(),
                    sample.is_some(),
                    !index_hints.is_empty(),
                ]
                .contains(&true),
            ),
            TableFactor::Derived { lateral, subquery: _, alias, sample } => {
                (alias, [*lateral, sample.is_some()].contains(&true))
            }
            t => return Err(format!("unsupported FROM {t}")),
        };
        let renamed = alias.as_ref().is_some_and(|a| !a.columns.is_empty() || a.at.is_some());
        if unplanned || renamed {
            return Err(format!("unsupported FROM {t}"));
        }
        match t {
            TableFactor::Table { name, args: None, alias, .. } => {
                let name = name.to_string();
                if let Some(q) = ctes.get(&name) {
                    let p = self.query(q, ctes)?;
                    return Ok(Planned { schema: p.schema.qualify(alias.as_ref()), ..p });
                }
                let p = self.stream(&name)?;
                Ok(Planned { schema: p.schema.qualify(alias.as_ref()), ..p })
            }
            TableFactor::Table { name, args: Some(a), .. } if name.to_string().eq_ignore_ascii_case("tumble") => {
                let [s, ts, w] = a.args.as_slice() else { return Err("tumble(stream, time, width) expected".into()) };
                let s = self.stream(&name_of(arg(s)?))?;
                let width = match arg(w)? {
                    Expr::Value(v) => match &v.value {
                        ast::Value::SingleQuotedString(d) => duration_us(d)?,
                        v => return Err(format!("unsupported tumble width {v}")),
                    },
                    e => return Err(format!("unsupported tumble width {e}")),
                };
                let ts = Compiler::new(&s.schema.scope()).operand(arg(ts)?)?;
                Ok(Planned { tumble: Some((ts, width)), ..s })
            }
            TableFactor::Table { name, args: Some(a), alias, .. }
                if name.to_string().eq_ignore_ascii_case("orderbook_top_n") =>
            {
                let (s, depth, reset) = match a.args.as_slice() {
                    [s, d] => (s, d, None),
                    [s, d, r] => (s, d, Some(r)),
                    _ => return Err("orderbook_top_n(stream, depth[, allow_seq_reset]) expected".into()),
                };
                let mut p = self.stream(&name_of(arg(s)?))?;
                let depth = match arg(depth)? {
                    Expr::Value(v) => match &v.value {
                        ast::Value::Number(n, _) => n.parse::<usize>().ok().filter(|d| (1..=1000).contains(d)),
                        _ => None,
                    },
                    _ => None,
                };
                let depth = depth.ok_or("orderbook_top_n: the depth must be a whole number from 1 to 1000")?;
                let reset = match reset.map(arg).transpose()? {
                    None => false,
                    Some(Expr::Value(ast::ValueWithSpan { value: ast::Value::Boolean(b), .. })) => *b,
                    Some(e) => return Err(format!("orderbook_top_n: allow_seq_reset must be true or false, not {e}")),
                };
                let names: Vec<&str> = p.schema.0.iter().map(|c| c.1.as_str()).collect();
                p.ops.push(Op::Book(Box::new(TopN::new(&names, depth, reset)?)));
                Ok(Planned { schema: p.schema.qualify(alias.as_ref()), ..p })
            }
            TableFactor::Table { name, args: Some(a), alias, .. }
                if name.to_string().eq_ignore_ascii_case("gap_fill") =>
            {
                let usage = "gap_fill(stream, time, 'width'[, start, finish], key ..., locf(column) ..., \
                             interpolate(column) ...)";
                let a: Vec<&Expr> = a.args.iter().map(arg).collect::<R<_>>()?;
                let [s, time, w, rest @ ..] = a.as_slice() else { return Err(usage.into()) };
                let mut p = self.stream(&name_of(s))?;
                let col = |e: &Expr| {
                    match e {
                        Expr::Identifier(i) => p.schema.0.iter().position(|c| c.1 == i.value),
                        _ => None,
                    }
                    .ok_or(format!("gap_fill: {e} is not a column of {}", name_of(s)))
                };
                let width = match w {
                    Expr::Value(v) => match &v.value {
                        ast::Value::SingleQuotedString(d) => duration_us(d)?,
                        _ => return Err(usage.into()),
                    },
                    _ => return Err(usage.into()),
                };
                // the range: two times (µs), either NULL
                let at = |e: &Expr| match e {
                    Expr::Value(v) => match &v.value {
                        ast::Value::Number(n, _) => n.parse::<i64>().ok().map(Some),
                        ast::Value::Null => Some(None),
                        _ => None,
                    },
                    _ => None,
                };
                let (start, finish, rest) = match rest {
                    [x, y, more @ ..] if at(x).is_some() && at(y).is_some() => (at(x).flatten(), at(y).flatten(), more),
                    _ => (None, None, rest),
                };
                let (mut keys, mut locf, mut interpolate) = (vec![], vec![], vec![]);
                for e in rest {
                    match e {
                        Expr::Function(f) => {
                            let mode = f.name.to_string().to_ascii_lowercase();
                            let [x] = crate::expr::plain_args(&f.args)?.try_into().map_err(|_| usage)?;
                            let x = &x;
                            match mode.as_str() {
                                "locf" => locf.push(col(x)?),
                                "interpolate" => interpolate.push(col(x)?),
                                _ => return Err(usage.into()),
                            }
                        }
                        e => keys.push(col(e)?),
                    }
                }
                let fill = Fill {
                    time: col(time)?,
                    width,
                    start,
                    finish,
                    keys,
                    locf,
                    interpolate,
                    cols: p.schema.0.len(),
                    last: FxHashMap::default(),
                    text: String::new(),
                    unfilled: 0,
                };
                p.ops.push(Op::Fill(Box::new(fill)));
                Ok(Planned { schema: p.schema.qualify(alias.as_ref()), ..p })
            }
            TableFactor::Derived { subquery, alias, .. } => {
                let p = self.query(subquery, ctes)?;
                Ok(Planned { schema: p.schema.qualify(alias.as_ref()), ..p })
            }
            t => Err(format!("unsupported FROM {t}")),
        }
    }

    fn select(&self, s: &Select, ctes: &HashMap<String, Query>) -> R<Planned> {
        let unsupported = [
            (s.distinct.is_some(), "DISTINCT"),
            (s.top.is_some(), "TOP"),
            (s.select_modifiers.is_some(), "SELECT modifiers"),
            (!s.optimizer_hints.is_empty(), "optimizer hints"),
            (s.exclude.is_some(), "EXCLUDE"),
            (s.into.is_some(), "SELECT INTO"),
            (!s.lateral_views.is_empty(), "LATERAL VIEW"),
            (s.prewhere.is_some(), "PREWHERE"),
            (!s.connect_by.is_empty(), "CONNECT BY"),
            (
                !(s.cluster_by.is_empty() && s.distribute_by.is_empty() && s.sort_by.is_empty()),
                "CLUSTER/DISTRIBUTE/SORT BY",
            ),
            (s.having.is_some(), "HAVING"),
            (
                !s.named_window.is_empty() && !matches!(&s.group_by, GroupByExpr::Expressions(k, _) if k.is_empty()),
                "WINDOW with GROUP BY",
            ),
            (s.qualify.is_some(), "QUALIFY"),
            (s.value_table_mode.is_some(), "SELECT AS VALUE/STRUCT"),
            (
                matches!(&s.group_by, GroupByExpr::Expressions(_, m) if !m.is_empty()),
                "GROUP BY modifiers (ROLLUP, CUBE, TOTALS)",
            ),
            (matches!(s.group_by, GroupByExpr::All(_)), "GROUP BY ALL"),
        ];
        if let Some((_, what)) = unsupported.iter().find(|u| u.0) {
            return Err(format!("{what} is not supported"));
        }
        let [from] = s.from.as_slice() else { return Err("exactly one FROM relation expected".into()) };
        let mut p = self.relation(&from.relation, ctes)?;
        if !from.joins.is_empty() {
            p = self.join(p, &from.joins, ctes)?;
        }
        match &s.group_by {
            GroupByExpr::Expressions(keys, _) if !keys.is_empty() => self.window(p, s, keys),
            _ => self.project(p, s),
        }
    }

    fn join(&self, left: Planned, joins: &[ast::Join], ctes: &HashMap<String, Query>) -> R<Planned> {
        if left.inputs.len() != 1 || left.tumble.is_some() {
            return Err("the left side of a join must read one stream".into());
        }
        let (mut inputs, mut schema) = (left.inputs, left.schema);
        let keep = match self.view.settings.get("keep_versions") {
            Some(k) => k.parse::<usize>().ok().filter(|k| *k > 0).ok_or(format!("keep_versions = {k}"))?,
            None => KEEP_VERSIONS,
        };
        let mut join = Join {
            sides: vec![left.ops],
            rights: vec![],
            exact: false,
            keep,
            held: VecDeque::new(),
            left_max: i64::MIN,
            dropped: Dropped::default(),
            text: String::new(),
        };
        for j in joins {
            let JoinOperator::Left(JoinConstraint::On(on)) = &j.join_operator else {
                return Err(format!("only ASOF LEFT JOIN ... ON is supported, got {}", j.relation));
            };
            if let TableFactor::Derived { lateral: true, subquery, alias, .. } = &j.relation {
                if !matches!(on, Expr::Value(v) if v.value == ast::Value::Boolean(true)) {
                    return Err(format!("a window join is LEFT JOIN LATERAL (...) ON true, not ON {on}"));
                }
                let (r, right, out) = self.window_join(subquery, alias.as_ref(), &schema.scope(), ctes)?;
                join.rights.push(right);
                join.sides.push(r.ops);
                inputs.extend(r.inputs);
                schema.0.extend(out.0);
                continue;
            }
            let r = self.relation(&j.relation, ctes)?;
            if r.inputs.len() != 1 || r.tumble.is_some() {
                return Err("the right side of a join must read one stream".into());
            }
            let (ls, rs) = (schema.scope(), r.schema.scope());
            let (mut right, mut conds) = (None, vec![on]);
            let (mut left_key, mut key) = (vec![], vec![]);
            while let Some(c) = conds.pop() {
                match c {
                    Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                        conds.extend([left.as_ref(), right.as_ref()])
                    }
                    Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                        left_key.insert(0, Compiler::new(&ls).operand(left)?);
                        key.insert(0, Compiler::new(&rs).operand(right)?);
                    }
                    Expr::BinaryOp { left, op: BinaryOperator::GtEq, right: r } if right.is_none() => {
                        right = Some((Compiler::new(&ls).operand(left)?, Compiler::new(&rs).operand(r)?));
                    }
                    c => return Err(format!("unsupported ASOF condition {c}")),
                }
            }
            let (left_ts, ts) = right.ok_or("ASOF JOIN needs one `left.time >= right.time` condition")?;
            let defaults = r
                .schema
                .0
                .iter()
                .map(|(_, n, t)| t.as_ref().map(Type::default_value).ok_or(format!("unknown type of {n}")));
            join.rights.push(Right {
                left_key,
                left_ts,
                key,
                ts,
                defaults: defaults.collect::<R<_>>()?,
                versions: FxHashMap::default(),
                max_keys: ASOF_MAX_KEYS,
                evicted: 0,
                late: 0,
                max_ts: i64::MIN,
                lateness: ASOF_LATENESS_US,
                within: None,
            });
            join.sides.push(r.ops);
            inputs.extend(r.inputs);
            schema.0.extend(r.schema.0);
        }
        Ok(Planned { inputs, ops: vec![Op::Join(Box::new(join))], schema, tumble: None })
    }

    /// A window join's right side (`Within`): its relation, its `Right`, and its items' schema.
    /// The subquery's WHERE holds the keys (`r.k = l.k`), the range (`r.ts BETWEEN l.ts -
    /// INTERVAL '1 second' AND l.ts`, or `r.ts >=`, `>`, `<=`, `<` the left time plus or minus a
    /// constant) and conditions on the right rows alone.
    fn window_join(
        &self,
        sub: &Query,
        alias: Option<&ast::TableAlias>,
        ls: &Scope,
        ctes: &HashMap<String, Query>,
    ) -> R<(Planned, Right, Schema)> {
        let usage = "a window join is LEFT JOIN LATERAL (SELECT aggregates FROM r WHERE r.key = l.key \
                     AND r.ts BETWEEN l.ts - INTERVAL '1 second' AND l.ts) ON true";
        let SetExpr::Select(sel) = sub.body.as_ref() else { return Err(usage.into()) };
        let plain = sub.with.is_none() && sub.order_by.is_none() && sub.limit_clause.is_none();
        let ungrouped = matches!(&sel.group_by, GroupByExpr::Expressions(k, _) if k.is_empty())
            && sel.having.is_none()
            && sel.distinct.is_none();
        let ([from], true) = (sel.from.as_slice(), plain && ungrouped) else { return Err(usage.into()) };
        if !from.joins.is_empty() {
            return Err(usage.into());
        }
        let r = self.relation(&from.relation, ctes)?;
        if r.inputs.len() != 1 || r.tumble.is_some() {
            return Err("the right side of a join must read one stream".into());
        }
        let rs = r.schema.scope();
        let on = |sc: &Scope, e: &Expr| Compiler::new(sc).operand(e).is_ok();
        let left_only = |e: &Expr| on(ls, e) && !on(&rs, e);
        let (mut left_key, mut key, mut filters) = (vec![], vec![], vec![]);
        let (mut lo, mut hi, mut time): (Option<i64>, Option<i64>, Option<(Expr, Expr)>) = (None, None, None);
        // a right time column: a column of the right relation's of a time type
        let time_col = |e: &Expr| {
            let name = match e {
                Expr::Identifier(i) => &i.value,
                Expr::CompoundIdentifier(ids) => &ids[ids.len() - 1].value,
                _ => return false,
            };
            on(&rs, e)
                && r.schema.0.iter().any(|(_, n, t)| {
                    let time = |t: &Type| matches!(t, Type::Time(_) | Type::Any);
                    n == name
                        && match t {
                            Some(Type::Nullable(t)) => time(t),
                            Some(t) => time(t),
                            None => true,
                        }
                })
        };
        let mut bound = |rt: &Expr, op: &BinaryOperator, e: &Expr| -> R<bool> {
            let (base, off) = offset_of(e);
            if !time_col(rt) || !left_only(&base) {
                return Ok(false);
            }
            match &time {
                Some((r, b)) if r.to_string() != rt.to_string() || b.to_string() != base.to_string() => {
                    return Err(format!("a window join's range is of one right time and one left time: {usage}"))
                }
                _ => time = Some((rt.clone(), base)),
            }
            match op {
                BinaryOperator::GtEq => lo = Some(off),
                BinaryOperator::Gt => lo = Some(off + 1),
                BinaryOperator::LtEq => hi = Some(off),
                BinaryOperator::Lt => hi = Some(off - 1),
                _ => return Ok(false),
            }
            Ok(true)
        };
        let mut conds = sel.selection.iter().cloned().collect::<Vec<_>>();
        while let Some(c) = conds.pop() {
            match &c {
                Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                    conds.extend([(**right).clone(), (**left).clone()]);
                    continue;
                }
                Expr::Nested(x) => {
                    conds.push((**x).clone());
                    continue;
                }
                Expr::Between { expr, negated: false, low, high }
                    if bound(expr, &BinaryOperator::GtEq, low)? && bound(expr, &BinaryOperator::LtEq, high)? =>
                {
                    continue;
                }
                Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
                    let (l, r) = match (left_only(left), left_only(right)) {
                        (false, true) if on(&rs, left) => (right, left),
                        (true, false) if on(&rs, right) => (left, right),
                        _ => (left, right),
                    };
                    if left_only(l) && on(&rs, r) {
                        left_key.push(Compiler::new(ls).operand(l)?);
                        key.push(Compiler::new(&rs).operand(r)?);
                        continue;
                    }
                }
                Expr::BinaryOp {
                    left,
                    op: op @ (BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq),
                    right,
                } => {
                    let flipped = match op {
                        BinaryOperator::Gt => BinaryOperator::Lt,
                        BinaryOperator::GtEq => BinaryOperator::LtEq,
                        BinaryOperator::Lt => BinaryOperator::Gt,
                        _ => BinaryOperator::GtEq,
                    };
                    if bound(left, op, right)? || bound(right, &flipped, left)? {
                        continue;
                    }
                }
                _ => {}
            }
            if !on(&rs, &c) {
                return Err(format!("unsupported window join condition {c}: {usage}"));
            }
            filters.push(c);
        }
        let (Some((rt, base)), Some(lo), Some(hi)) = (time, lo, hi) else {
            return Err(format!("a window join needs its range's both ends: {usage}"));
        };
        if lo > hi {
            return Err(format!("a window join's range ends before it starts: {usage}"));
        }
        let end = Expr::BinaryOp {
            left: Box::new(base),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::value(ast::Value::Number(hi.to_string(), false))),
        };
        let filter = match filters.into_iter().reduce(|a, b| Expr::BinaryOp {
            left: Box::new(a),
            op: BinaryOperator::And,
            right: Box::new(b),
        }) {
            Some(f) => Some(Compiler::new(&rs).condition(&f)?),
            None => None,
        };
        // the items over the aggregates, as a window's
        let mut collect = Collect { src: &rs, base: 0, aggs: vec![], shares: vec![], samplers: vec![] };
        let (mut out, mut schema) = (vec![], Schema::default());
        {
            let scope = Schema::default().scope();
            let mut c = Compiler::new(&scope);
            c.aggregates = Some(&mut collect);
            for item in &sel.projection {
                let (e, name) = match item {
                    SelectItem::UnnamedExpr(e) => (e, name_of(e)),
                    SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
                    i => return Err(format!("a window join's items are aggregates, not {i}")),
                };
                out.push(c.compile_item(e, &name)?);
                schema.0.push((None, name, None));
            }
        }
        let (_, aggs): (Vec<String>, Vec<AggSpec>) = collect.aggs.into_iter().unzip();
        let empty = aggs
            .iter()
            .map(|(n, _, _)| match n.as_str() {
                "count" | "count_if" | "uniq_exact" | "uniq_exact_if" => Value::UInt(0),
                _ => Value::Null,
            })
            .collect();
        let within = Within { span: hi - lo, filter, aggs, shares: collect.shares, out, empty };
        accs(&within.aggs, &within.shares)?;
        let defaults = r.schema.0.iter().map(|(_, _, t)| t.as_ref().map_or(Value::Null, Type::default_value)).collect();
        let right = Right {
            left_key,
            left_ts: Compiler::new(ls).operand(&end)?,
            key,
            ts: Compiler::new(&rs).operand(&rt)?,
            defaults,
            versions: FxHashMap::default(),
            max_keys: ASOF_MAX_KEYS,
            evicted: 0,
            late: 0,
            max_ts: i64::MIN,
            lateness: ASOF_LATENESS_US,
            within: Some(Box::new(within)),
        };
        Ok((r, right, schema.qualify(alias)))
    }

    fn project(&self, mut p: Planned, s: &Select) -> R<Planned> {
        if p.tumble.is_some() {
            return Err("tumble() without GROUP BY is not supported".into());
        }
        let scope = p.schema.scope();
        let mut windows = CollectWindows {
            src: &scope,
            base: p.schema.0.len(),
            named: &s.named_window,
            groups: vec![],
            leads: vec![],
            ranks: vec![],
        };
        let mut c = Compiler::new(&scope);
        c.aliases = aliases(s);
        c.windows = Some(&mut windows);
        let (mut exprs, mut out) = (vec![], Schema::default());
        for item in &s.projection {
            let (e, name) = match item {
                // `* EXCEPT (..)`, `* REPLACE (..)` and the like are refused, not read as `*` (F5)
                SelectItem::Wildcard(o) => {
                    let ast::WildcardAdditionalOptions {
                        wildcard_token: _,
                        opt_ilike: None,
                        opt_exclude: None,
                        opt_except: None,
                        opt_replace: None,
                        opt_rename: None,
                        opt_alias: None,
                    } = o
                    else {
                        return Err(format!("unsupported select item {item}"));
                    };
                    for (i, (_, n, t)) in p.schema.0.iter().enumerate() {
                        exprs.push(std::sync::Arc::new(move |r: &[Value]| r[i].clone()) as Ex);
                        out.0.push((None, n.clone(), t.clone()));
                    }
                    continue;
                }
                SelectItem::UnnamedExpr(e) => (e, name_of(e)),
                SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
                i => return Err(format!("unsupported select item {i}")),
            };
            exprs.push(c.compile_item(e, &name)?);
            out.0.push((None, name, p.schema.ty(e)));
        }
        // WHERE sees no window functions: it runs before them
        let mut c = Compiler::new(&scope);
        c.aliases = aliases(s);
        if s.selection.as_ref().is_some_and(|w| c.is_string(w)) {
            return Err("a string is not a condition: WHERE".into());
        }
        let filter = s.selection.as_ref().map(|w| c.condition(w)).transpose()?;
        if windows.groups.is_empty() && windows.ranks.is_empty() {
            if !s.named_window.is_empty() {
                return Err("a WINDOW clause without a window function".into());
            }
            let all = matches!(s.projection.as_slice(), [SelectItem::Wildcard(_)]);
            p.ops.push(Op::Project { filter, exprs, all });
        } else {
            let width = windows.slot();
            let ranks = std::mem::take(&mut windows.ranks);
            p.ops.push(Op::Over(Box::new(Over { filter, groups: windows.groups, width })));
            if let Some((time, ..)) = ranks.first() {
                // one held time for them all
                if let Some(other) = ranks.iter().find(|r| r.0 != *time) {
                    return Err(format!(
                        "the ranking functions of one SELECT share their first PARTITION BY key, the time: {time} and {}",
                        other.0
                    ));
                }
                let describe = ranks.iter().map(|r| r.3.as_str()).collect::<Vec<_>>().join(",");
                let time = ranks[0].1.clone();
                let calls = ranks.into_iter().map(|r| r.2).collect();
                p.ops.push(Op::Section(Box::new(Section {
                    time,
                    calls,
                    describe,
                    width,
                    held: VecDeque::new(),
                    late: 0,
                })));
            }
            if let Some((_, _, _, over, keys)) = windows.leads.first() {
                // one held order for them all: the next rows of one partition
                if windows.leads.iter().any(|l| l.3 != *over) {
                    return Err("lead calls of one SELECT share one PARTITION BY ... ORDER BY".into());
                }
                let calls: Vec<(usize, usize, Value)> = windows.leads.iter().map(|l| (l.0, l.1, l.2.clone())).collect();
                let most = calls.iter().map(|c| c.1).max().expect("a lead");
                let keys = keys.clone();
                p.ops.push(Op::Lead(Box::new(Lead {
                    keys,
                    calls,
                    most,
                    width,
                    held: FxHashMap::default(),
                    text: String::new(),
                })));
            }
            p.ops.push(Op::Project { filter: None, exprs, all: false });
        }
        p.schema = out;
        Ok(p)
    }

    fn window(&self, mut p: Planned, s: &Select, group: &[Expr]) -> R<Planned> {
        let (ts, width) = p.tumble.take().ok_or("GROUP BY needs FROM tumble(...)")?;
        let delay = self.view.emit_delay_us.ok_or("a window needs EMIT AFTER WINDOW CLOSE WITH DELAY")?;
        if s.selection.is_some() || s.having.is_some() {
            return Err("WHERE/HAVING on a window is not supported".into());
        }
        let src = p.schema.scope();
        let mut gs = Schema(vec![
            (None, "window_start".into(), Some(Type::Time(6))),
            (None, "window_end".into(), Some(Type::Time(6))),
        ]);
        let mut keys = vec![];
        for k in group.iter().filter(|k| !matches!(name_of(k).as_str(), "window_start" | "window_end")) {
            keys.push(Compiler::new(&src).operand(k)?);
            gs.0.push((None, name_of(k), p.schema.ty(k)));
        }
        let scope = gs.scope();
        let mut collect = Collect { src: &src, base: gs.0.len(), aggs: vec![], shares: vec![], samplers: vec![] };
        let mut out = (vec![], Schema::default());
        {
            let mut c = Compiler::new(&scope);
            c.aliases = aliases(s);
            c.aggregates = Some(&mut collect);
            for item in &s.projection {
                let (e, name) = match item {
                    SelectItem::UnnamedExpr(e) => (e, name_of(e)),
                    SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
                    i => return Err(format!("unsupported select item in a window {i}")),
                };
                out.0.push(c.compile_item(e, &name)?);
                out.1 .0.push((None, name, gs.ty(e)));
            }
        }
        let shares = collect.shares;
        let (agg_keys, aggs): (_, Vec<AggSpec>) = collect.aggs.into_iter().unzip();
        let digests = accs(&aggs, &shares)?.iter().any(Acc::holds_digest);
        let w = Window {
            ts,
            width,
            delay,
            keys,
            aggs,
            shares,
            agg_keys,
            out: out.0,
            watermark: i64::MIN,
            max_ts: i64::MIN,
            late: 0,
            dropped: Dropped::default(),
            withhold_start: i64::MIN,
            withheld: 0,
            closed: 0,
            emitted_from: i64::MAX,
            open: BTreeMap::new(),
            key: vec![],
            text: String::new(),
            group: vec![],
            digests,
            due: BTreeMap::new(),
            lead_at: if digests { i64::MIN } else { i64::MAX },
        };
        p.ops.push(Op::Window(Box::new(w)));
        p.schema = out.1;
        Ok(p)
    }
}

#[derive(Clone)]
struct Plan {
    target: String,
    /// The streams it reads, one per join side (`Engine::close_until`).
    inputs: Vec<String>,
    ops: Vec<Op>,
    /// Per target column: the view output column with that name.
    by_name: Vec<Option<usize>>,
    /// The view's output columns are the target's, in order: rows are inserted as they are.
    identity: bool,
    /// No output column feeds two target columns: values can be moved, not cloned.
    unique: bool,
    /// The view is `SELECT * FROM s [WHERE filter]` into a Kafka sink, as every example
    /// `*_out` view: `insert` writes the rows of `s` the filter keeps as they are, without the
    /// projection's copy of each.
    sink_filter: Option<Option<Pred>>,
}

#[derive(Clone)]
pub struct Engine {
    streams: BTreeMap<String, Stream>,
    plans: Vec<Plan>,
    readers: HashMap<String, Vec<(usize, usize)>>,
    /// Each stream's computed columns in column order, as (column, expression, `MATERIALIZED`):
    /// a `MATERIALIZED` one is computed on every insert, a `DEFAULT` one only when the view
    /// supplies no column of its name.
    computed: HashMap<String, Vec<(usize, Ex, bool)>>,
    /// Views that are not run: those writing to S3 tables.
    pub skipped: Vec<String>,
    /// What the snapshot of this plan holds, operator by operator (`fingerprint`).
    shape: String,
    /// The latest event time windows and joins take (`set_time_limit`).
    time_limit: i64,
    /// The `Emit::window_end` of what is being written: the views on the way to it take the
    /// earliest of their windows' ends (`write`).
    window_end: i64,
    /// Each Kafka sink's output, laid out once.
    sinks: HashMap<String, SinkLayout>,
    /// How many threads at most close windows at once (`set_close_threads`): 1, the calling
    /// thread only, by default.
    close_threads: usize,
    /// Per stream, which of its readers may run on another thread while the ones before them
    /// write (`Engine::independent`), in `readers` order.
    independent: HashMap<String, Vec<bool>>,
    /// The fewest groups an insert's views must close between them for it to close them on
    /// several threads (`set_close_threads`).
    close_groups: usize,
    /// Inserts that closed windows on several threads.
    parallel_inserts: u64,
    /// Scratch: the row `write_sink` lays each row out in, reused from row to row.
    scratch: Row,
}

/// The fewest groups an insert's views must close between them to be closed on several threads.
/// Fewer cost more on another core (their state is in the data thread's cache, a new thread's
/// scratch buffers are cold) than they save: on a trade-size pipeline at a busy feed's rates, closing every 30s
/// window of a feed on two threads took 65 ms a slice against 52 on one; from 2,048 groups (5m
/// and longer windows) two threads save 7-24%.
pub const CLOSE_GROUPS: usize = 2048;

/// A heavy reader's run (`Engine::insert_parallel`), by whichever thread takes it first: its
/// operators, taken out of its plan for the run, and its join side.
type Job = (Vec<Op>, usize);

/// What a run gives back: the operators, and the rows they emitted or the panic that stopped
/// them. The operators come back either way, so no plan is left without them.
type Ran = (Vec<Op>, Result<Vec<Row>, Box<dyn std::any::Any + Send>>);

/// Runs `job` over `rows`, catching a panic.
fn run_job((mut ops, side): Job, rows: &[Row], limit: i64) -> Ran {
    let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&mut ops, side, rows, limit)));
    (ops, ran)
}

/// What a Kafka sink writes of a row, laid out once (`Engine::new`) rather than for every write:
/// its topic, its JSONEachRow line (every column but `_tp_message_*`, in order) and its headers.
#[derive(Clone)]
struct SinkLayout {
    topic: Arc<str>,
    row: RowFormat,
    headers: SinkHeaders,
    /// The longest line written yet, each line's capacity: lines of a sink are about as long,
    /// and one reserved shorter grows as it is written.
    line: usize,
}

/// Where a sink's Kafka headers come from.
#[derive(Clone)]
enum SinkHeaders {
    /// No `_tp_message_headers` column: none.
    None,
    /// The `_tp_message_headers` column at this position, read by `headers`.
    Column(usize),
    /// A `MATERIALIZED` `_tp_message_headers` column at this position, of the usual shape
    /// (`Compiler::headers`): its headers are built from the row, and the column is not computed
    /// (`Engine::new` leaves it out of `computed`). Nothing reads it: a sink writes no
    /// `_tp_message_*` column, and no column is computed after it.
    Built(usize, Headers),
}

impl SinkLayout {
    fn of(st: &Stream) -> SinkLayout {
        let written = st.columns.iter().enumerate().filter(|(_, c)| !c.name.starts_with("_tp_message"));
        SinkLayout {
            topic: st.settings.get("topic").map_or("", String::as_str).into(),
            row: RowFormat::new(written.map(|(i, c)| (i, c.name.as_str(), &c.ty))),
            headers: SinkHeaders::of(st),
            line: 0,
        }
    }
}

impl SinkHeaders {
    fn of(st: &Stream) -> SinkHeaders {
        let Some(i) = st.columns.iter().position(|c| c.name == "_tp_message_headers") else {
            return SinkHeaders::None;
        };
        let (c, after) = (&st.columns[i], &st.columns[i + 1..]);
        let last = after.iter().all(|c| c.materialized.is_none() && c.default.is_none());
        // `headers` reads the map of a map-typed column only
        let Some(m) = c.materialized.as_ref().filter(|_| last && matches!(c.ty.base(), Type::Map(..))) else {
            return SinkHeaders::Column(i);
        };
        let scope = Scope::new(st.columns.iter().map(|c| c.name.clone()));
        // an expression that does not compile is the general compile's to report (`Engine::new`)
        match Compiler::new(&scope).headers(m) {
            Ok(Some(build)) => SinkHeaders::Built(i, build),
            _ => SinkHeaders::Column(i),
        }
    }

    /// The column built rather than computed, if any.
    fn built(&self) -> Option<usize> {
        match self {
            SinkHeaders::Built(i, _) => Some(*i),
            _ => None,
        }
    }

    /// The headers of the message of sink row `r`.
    fn of_row(&self, r: &[Value]) -> Vec<(String, String)> {
        match self {
            SinkHeaders::None => vec![],
            SinkHeaders::Column(i) => headers(&r[*i]),
            SinkHeaders::Built(_, build) => build(r),
        }
    }
}

impl Engine {
    pub fn new(cat: &Catalog) -> R<Engine> {
        let mut views = vec![];
        let mut e = Engine {
            streams: cat.streams.clone(),
            plans: vec![],
            readers: HashMap::new(),
            computed: HashMap::new(),
            skipped: vec![],
            shape: String::new(),
            time_limit: i64::MAX,
            window_end: i64::MAX,
            close_threads: 1,
            close_groups: CLOSE_GROUPS,
            independent: HashMap::new(),
            parallel_inserts: 0,
            scratch: vec![],
            // the streams views write that are no stream: Kafka sinks (views into S3 tables are
            // never planned)
            sinks: (cat.views.iter().filter_map(|v| cat.streams.get(&v.target)))
                .filter(|s| s.kind != Kind::Stream)
                .map(|s| (s.name.clone(), SinkLayout::of(s)))
                .collect(),
        };
        check_settings(cat)?;
        for v in &cat.views {
            let target =
                cat.streams.get(&v.target).ok_or_else(|| format!("{}: unknown target {}", v.name, v.target))?;
            if matches!(target.kind, Kind::Table(_)) {
                e.skipped.push(v.name.clone());
                continue;
            }
            if v.asof.contains(&false) {
                return Err(format!("{}: only ASOF LEFT JOIN is supported", v.name));
            }
            let mut p =
                Planner { cat, view: v }.query(&v.query, &HashMap::new()).map_err(|m| format!("{}: {m}", v.name))?;
            if let Some(ms) = v.settings.get("order_hold_ms") {
                let hold = ms.parse::<i64>().ok().filter(|h| *h >= 0);
                let hold = hold.ok_or_else(|| format!("{}: order_hold_ms = {ms}", v.name))?;
                let Some(Op::Sort(keys)) = p.ops.pop() else {
                    return Err(format!("{}: order_hold_ms holds the view's own ORDER BY, and it has none", v.name));
                };
                let width = p.schema.0.len();
                // the first key read straight from its column when it is one (as generated)
                let first = v.query.order_by.as_ref().and_then(|o| match &o.kind {
                    OrderByKind::Expressions(es) => es.first().map(|e| &e.expr),
                    _ => None,
                });
                let time_col = match first {
                    Some(Expr::Identifier(i)) => p.schema.0.iter().position(|c| c.1 == i.value),
                    _ => None,
                };
                let (held, max, released) = (VecDeque::new(), i64::MIN, i64::MIN);
                let h = Hold { keys, time_col, hold: hold * 1000, width, held, max, released };
                p.ops.push(Op::Hold(Box::new(h)));
            }
            let names: Vec<&String> = p.schema.0.iter().map(|c| &c.1).collect();
            let by_name: Vec<Option<usize>> =
                target.columns.iter().map(|c| names.iter().position(|n| **n == c.name)).collect();
            let identity = by_name.len() == names.len() && by_name.iter().enumerate().all(|(i, j)| *j == Some(i));
            let used: Vec<usize> = by_name.iter().flatten().copied().collect();
            let unique = used.iter().enumerate().all(|(k, i)| !used[..k].contains(i));
            for (side, s) in p.inputs.iter().enumerate() {
                // an external stream a view writes is a Kafka sink: `write` produces to it and
                // runs no reader, and the runtime consumes only sources, so this view never runs
                let sink = cat.streams[s].kind == Kind::External;
                if let Some(w) = sink.then(|| cat.views.iter().find(|w| w.target == *s)).flatten() {
                    return Err(format!(
                        "{}: reads {s}, which the view {} writes; brrrrr does not read its own sinks back: \
                         read from an internal stream instead",
                        v.name, w.name
                    ));
                }
                e.readers.entry(s.clone()).or_default().push((e.plans.len(), side));
            }
            let sink_filter = match p.ops.as_slice() {
                [Op::Project { filter, all: true, .. }] if target.kind == Kind::External => Some(filter.clone()),
                _ => None,
            };
            let (target, inputs, ops) = (v.target.clone(), p.inputs, p.ops);
            e.plans.push(Plan { target, inputs, ops, by_name, identity, unique, sink_filter });
            views.push(v.name.as_str());
        }
        if let Some(cycle) = e.cycle(&views) {
            return Err(format!("the views {cycle} form a cycle: a row would be inserted around it forever"));
        }
        e.independent = e.readers.keys().map(|s| (s.clone(), e.independent(s))).collect();
        for s in cat.streams.values() {
            let scope = Scope::new(s.columns.iter().map(|c| c.name.clone()));
            let built = e.sinks.get(&s.name).and_then(|k| k.headers.built());
            for (i, c) in s.columns.iter().enumerate() {
                if let Some(m) = c.materialized.as_ref().or(c.default.as_ref()) {
                    let ex = Compiler::new(&scope).compile(m).map_err(|m| format!("{}.{}: {m}", s.name, c.name))?;
                    if built != Some(i) {
                        e.computed.entry(s.name.clone()).or_default().push((i, ex, c.materialized.is_some()));
                    }
                }
            }
        }
        for p in &e.plans {
            e.shape.push_str(&p.target);
            describe(&p.ops, &mut e.shape);
            e.shape.push('\n');
        }
        Ok(e)
    }

    /// The source streams (those no view writes) that must be read in time order together: the streams
    /// of each connected set of views, where a view connects everything it reads, whether the sides of
    /// a join or the inputs it shares with another view, with the stream it writes. Two sets share no
    /// state and no stream, so the order of a row of one against a row of the other cannot matter:
    /// slippage's three per-exchange joins are three groups, trade-size's cross-exchange `top-3` aggregate
    /// makes its three exchanges one. Groups and the streams in them are sorted by name.
    pub fn source_groups(&self) -> Vec<Vec<String>> {
        let names: Vec<&String> = {
            let mut all: Vec<&String> = self.plans.iter().flat_map(|p| p.inputs.iter().chain([&p.target])).collect();
            all.sort();
            all.dedup();
            all
        };
        let index = |n: &String| names.binary_search(&n).expect("collected above");
        let mut parent: Vec<usize> = (0..names.len()).collect();
        fn root(parent: &mut [usize], mut i: usize) -> usize {
            while parent[i] != i {
                parent[i] = parent[parent[i]];
                i = parent[i];
            }
            i
        }
        for p in &self.plans {
            let first = root(&mut parent, index(&p.target));
            for n in &p.inputs {
                let r = root(&mut parent, index(n));
                parent[r] = first;
            }
        }
        let written: std::collections::HashSet<&String> = self.plans.iter().map(|p| &p.target).collect();
        let mut groups: std::collections::BTreeMap<usize, Vec<String>> = std::collections::BTreeMap::new();
        for (i, n) in names.iter().enumerate() {
            if !written.contains(n) {
                let r = root(&mut parent, i);
                groups.entry(r).or_default().push((*n).clone());
            }
        }
        let mut out: Vec<Vec<String>> = groups.into_values().collect();
        out.sort();
        out
    }

    /// A cycle of views, each inserting into a stream the next reads (inserts recurse through
    /// them), as `v -> w -> v`; `names` are the plans' view names.
    fn cycle(&self, names: &[&str]) -> Option<String> {
        // depth-first over views: 0 unvisited, 1 on the current path, 2 done
        fn visit(e: &Engine, v: usize, state: &mut [u8], path: &mut Vec<usize>) -> Option<Vec<usize>> {
            state[v] = 1;
            path.push(v);
            for &(next, _) in e.readers.get(&e.plans[v].target).into_iter().flatten() {
                match state[next] {
                    1 => return Some(path[path.iter().position(|&p| p == next).expect("on the path")..].to_vec()),
                    0 => {
                        if let Some(c) = visit(e, next, state, path) {
                            return Some(c);
                        }
                    }
                    _ => {}
                }
            }
            path.pop();
            state[v] = 2;
            None
        }
        let mut state = vec![0; self.plans.len()];
        for v in 0..self.plans.len() {
            if state[v] == 0 {
                if let Some(c) = visit(self, v, &mut state, &mut vec![]) {
                    let mut text: Vec<&str> = c.iter().map(|&v| names[v]).collect();
                    text.push(names[c[0]]);
                    return Some(text.join(" -> "));
                }
            }
        }
        None
    }

    /// A fingerprint of what this plan's snapshots hold: the views, their operators and, for each
    /// window, its width, delay, number of keys and aggregate calls in the order their
    /// accumulators are kept. The same SQL planned by a build that orders or dedups aggregates
    /// differently (a planner or sqlparser change) has another fingerprint, so its snapshots are
    /// refused instead of restored into the wrong aggregates. FNV-1a: stable across builds.
    pub fn fingerprint(&self) -> u64 {
        crate::checkpoint::fnv64(self.shape.as_bytes())
    }

    /// Lets `insert` close the windows of up to `n` views at once, on `n` threads, the calling one
    /// included:
    /// a row past the top of the hour closes trade-size's 15s to 1h windows of four feeds in one
    /// insert. 1 (the default) runs every view on the calling thread. The output is the same
    /// either way, message for message and in order.
    pub fn set_close_threads(&mut self, n: usize) {
        self.close_threads = n.max(1);
    }

    /// The fewest groups an insert's views must close between them to be closed on several
    /// threads: `CLOSE_GROUPS` unless set (tests set 0, to close every chunk that way).
    pub fn set_close_groups(&mut self, groups: usize) {
        self.close_groups = groups;
    }

    /// How many inserts closed windows on several threads so far.
    pub fn parallel_inserts(&self) -> u64 {
        self.parallel_inserts
    }

    /// Which readers of `stream` may run on another thread while the readers before them write:
    /// those reading `stream` once (a self-join's two sides run in turn) and no stream a reader
    /// of `stream` writes, directly or further down. Nothing a write of this insert does can then
    /// reach them, and they reach nothing another reader holds. Today only a reader whose first
    /// operator is a window can be heavy, and a window reads one stream, so each is independent
    /// (the views form no cycle); the rule keeps it so for plans to come.
    fn independent(&self, stream: &str) -> Vec<bool> {
        let readers = &self.readers[stream];
        let (mut below, mut todo) = (HashSet::new(), vec![stream]);
        while let Some(s) = todo.pop() {
            for &(v, _) in self.readers.get(s).into_iter().flatten() {
                let t = self.plans[v].target.as_str();
                if below.insert(t) {
                    todo.push(t);
                }
            }
        }
        let mut times: HashMap<usize, usize> = HashMap::new();
        readers.iter().for_each(|r| *times.entry(r.0).or_default() += 1);
        let once = |v: usize| times[&v] == 1;
        let apart = |v: usize| self.plans[v].inputs.iter().all(|i| !below.contains(i.as_str()));
        readers.iter().map(|&(v, _)| once(v) && apart(v)).collect()
    }

    /// The readers of `stream` worth a thread of their own for `rows`: independent ones whose
    /// window `rows` close, if there are two or more and they close `close_groups` groups or more
    /// between them (`None` otherwise, or with one thread).
    fn heavy(&self, stream: &str, rows: &[Row]) -> Option<Vec<bool>> {
        if self.close_threads < 2 {
            return None;
        }
        let readers = self.readers.get(stream)?;
        let closing: Vec<usize> = (readers.iter().zip(&self.independent[stream]))
            .map(|(&(v, _), &independent)| match self.plans[v].ops.first() {
                Some(Op::Window(w)) if independent => w.closes(rows, self.time_limit),
                _ => 0,
            })
            .collect();
        let heavy: Vec<bool> = closing.iter().map(|n| *n > 0).collect();
        let groups: usize = closing.iter().sum();
        (heavy.iter().filter(|h| **h).count() >= 2 && groups >= self.close_groups).then_some(heavy)
    }

    /// `insert`, the `heavy` readers' runs on up to `close_threads` threads, the calling one
    /// included. The calling thread goes through the readers in creation order: it runs the
    /// others, runs a heavy one itself unless a worker took it already (its state is in this
    /// thread's cache), and writes each reader's rows as soon as they are there, so a view's
    /// messages still go before the views after it run (`Output::flush`). A heavy reader's
    /// operators are taken out of its plan for its run: no write of this insert reaches them
    /// (`independent`). A run that panics gives its operators back; every reader still runs and
    /// gets its operators back, and nothing more is written, before the panic goes on.
    fn insert_parallel<O: Output + ?Sized>(&mut self, stream: &str, rows: Vec<Row>, out: &mut O, heavy: Vec<bool>) {
        use std::sync::Mutex;
        self.parallel_inserts += 1;
        let (readers, limit) = (self.readers[stream].clone(), self.time_limit);
        let (mut jobs, mut sends, mut results) = (vec![], vec![], vec![]);
        for (&(v, side), _) in readers.iter().zip(&heavy).filter(|(_, h)| **h) {
            let (send, receive) = std::sync::mpsc::sync_channel::<Ran>(1);
            jobs.push(Mutex::new(Some::<Job>((std::mem::take(&mut self.plans[v].ops), side))));
            sends.push(send);
            results.push(receive);
        }
        let take = |j: usize| jobs[j].lock().unwrap_or_else(|p| p.into_inner()).take();
        let (workers, next) =
            (self.close_threads.saturating_sub(1).min(jobs.len()), std::sync::atomic::AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| loop {
                    let j = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(send) = sends.get(j) else { break };
                    if let Some(job) = take(j) {
                        let _ = send.send(run_job(job, &rows, limit));
                    }
                });
            }
            let (mut job_ids, mut panicked) = (0..jobs.len(), None);
            for (i, &(v, side)) in readers.iter().enumerate() {
                let emitted = if heavy[i] {
                    let j = job_ids.next().expect("a job per heavy reader");
                    let (ops, ran) = match take(j) {
                        Some(job) => run_job(job, &rows, limit),
                        None => results[j].recv().expect("a worker answers each job it took"),
                    };
                    self.plans[v].ops = ops;
                    ran.unwrap_or_else(|panic| {
                        panicked.get_or_insert(panic);
                        vec![]
                    })
                } else {
                    run(&mut self.plans[v].ops, side, &rows, limit)
                };
                if !emitted.is_empty() && panicked.is_none() {
                    self.write(v, emitted, out);
                }
            }
            if let Some(panic) = panicked {
                std::panic::resume_unwind(panic);
            }
        });
    }

    /// One chunk arriving on `stream` (rows in its column order): runs every view reading it,
    /// in creation order. `out` is flushed each time a view has written a sink. With
    /// `set_close_threads`, views whose windows the chunk closes run on several threads.
    pub fn insert<O: Output + ?Sized>(&mut self, stream: &str, rows: Vec<Row>, out: &mut O) {
        if let Some(heavy) = self.heavy(stream, &rows) {
            return self.insert_parallel(stream, rows, out, heavy);
        }
        let n = self.readers.get(stream).map_or(0, Vec::len);
        for i in 0..n {
            let (v, side) = self.readers[stream][i];
            if let Some(filter) = self.plans[v].sink_filter.clone() {
                let kept = rows.iter().filter(|r| filter.as_ref().is_none_or(|f| f(r) == Some(true)));
                let mut kept = kept.map(Cow::Borrowed).peekable();
                if kept.peek().is_some() {
                    self.write_sink(v, kept, out);
                }
                continue;
            }
            let rows = run(&mut self.plans[v].ops, side, &rows, self.time_limit);
            if !rows.is_empty() {
                self.write(v, rows, out);
            }
        }
    }

    /// Writes the rows view `v` emitted into its target: a sink, or a stream whose readers run.
    /// They hold the windows `v` closed, if any (`Emit::window_end`).
    fn write<O: Output + ?Sized>(&mut self, v: usize, rows: Vec<Row>, out: &mut O) {
        let outer = self.window_end;
        self.window_end = outer.min(emitted_from(&self.plans[v].ops));
        self.write_target(v, rows, out);
        self.window_end = outer;
    }

    fn write_target<O: Output + ?Sized>(&mut self, v: usize, rows: Vec<Row>, out: &mut O) {
        let (plan, st) = (&self.plans[v], &self.streams[&self.plans[v].target]);
        if st.kind != Kind::Stream {
            // a Kafka sink (views into S3 tables are never planned)
            return self.write_sink(v, rows.into_iter().map(Cow::Owned), out);
        }
        // the target's column order and types; values that already conform are kept as they are
        let mut full: Vec<Row> = if plan.identity {
            let mut rows = rows;
            rows.iter_mut().for_each(|r| conform(r, st));
            rows
        } else {
            let width = st.columns.len();
            rows.into_iter()
                .map(|r| {
                    let mut full = Vec::with_capacity(width);
                    lay_out(plan, st, Cow::Owned(r), &mut full);
                    full
                })
                .collect()
        };
        let computed = self.computed.get(&st.name).map_or(&[][..], Vec::as_slice);
        full.iter_mut().for_each(|r| compute(plan, st, computed, r));
        let name = st.name.clone();
        self.insert(&name, full, out)
    }

    /// `write` into a Kafka sink: each row is laid out in the target's columns (in place if it is
    /// owned and already in them, else in `scratch`), formatted, and dropped.
    fn write_sink<'r, O: Output + ?Sized>(&mut self, v: usize, rows: impl Iterator<Item = Cow<'r, Row>>, out: &mut O) {
        let (plan, st) = (&self.plans[v], &self.streams[&self.plans[v].target]);
        let computed = self.computed.get(&st.name).map_or(&[][..], Vec::as_slice);
        let sink = self.sinks.get_mut(&st.name).expect("a layout per sink");
        let scratch = &mut self.scratch;
        for r in rows {
            let mut owned;
            let full = match r {
                Cow::Owned(r) if plan.identity => {
                    owned = r;
                    conform(&mut owned, st);
                    &mut owned
                }
                r => {
                    scratch.clear();
                    lay_out(plan, st, r, scratch);
                    &mut *scratch
                }
            };
            compute(plan, st, computed, full);
            if out.row(&st.name, full) {
                continue;
            }
            let mut payload = String::with_capacity(sink.line);
            sink.row.write(&mut payload, full);
            sink.line = sink.line.max(payload.len());
            let (topic, headers) = (sink.topic.clone(), sink.headers.of_row(full));
            out.push(Emit { topic, payload, headers, window_end: self.window_end });
        }
        scratch.clear();
        out.flush();
    }

    /// The mutable state of every operator, for checkpoints: windows' watermarks and open groups,
    /// joins' right-side versions. Plans are not part of it; they are recompiled from the SQL.
    pub fn snapshot(&self) -> State {
        State(self.plans.iter().map(|p| p.ops.iter().map(Op::snapshot).collect()).collect())
    }

    /// The state as `snapshot` has it, borrowed: what a checkpoint serializes without a copy of
    /// the state (`Checkpoint::encode_of`).
    pub fn state_ref(&self) -> StateRef<'_> {
        StateRef(self.plans.iter().map(|p| p.ops.iter().map(Op::borrow).collect()).collect())
    }

    /// Restores a snapshot taken from an engine built from the same SQL. The whole snapshot is
    /// checked first: a refused one leaves the engine as it was.
    pub fn restore(&mut self, state: State) -> R<()> {
        if state.0.len() != self.plans.len() {
            return Err(format!("snapshot has {} views, the SQL {}", state.0.len(), self.plans.len()));
        }
        for (i, (p, ops)) in self.plans.iter().zip(&state.0).enumerate() {
            check_ops(&p.ops, ops).and_then(|()| check_times(&p.ops, ops)).map_err(|e| format!("view {i}: {e}"))?;
        }
        for (p, ops) in self.plans.iter_mut().zip(state.0) {
            restore_ops(&mut p.ops, ops);
        }
        Ok(())
    }

    /// The lowest watermark of any window that has seen a row (µs): the start of the oldest
    /// window still open, so floored to the widest window (a 1d view's is midnight UTC). `None`
    /// before any window has a row. For freshness, see [`Engine::max_event_time`].
    pub fn min_watermark(&self) -> Option<i64> {
        let ops = self.ops().into_iter();
        ops.filter_map(|o| if let Op::Window(w) = o { Some(w.watermark) } else { None })
            .filter(|w| *w != i64::MIN)
            .min()
    }

    /// The latest event time any window has taken (µs), unrounded: how far the pipeline has
    /// read, whatever its window widths. `None` before any window has a row.
    pub fn max_event_time(&self) -> Option<i64> {
        let ops = self.ops().into_iter();
        ops.filter_map(|o| if let Op::Window(w) = o { Some(w.max_ts) } else { None }).filter(|t| *t != i64::MIN).max()
    }

    /// What every `orderbook_top_n` holds and has dropped, summed.
    pub fn books(&self) -> BookStats {
        let ops = self.plans.iter().flat_map(|p| &p.ops);
        ops.fold(BookStats::default(), |mut s, o| {
            if let Op::Book(b) = o {
                let b = b.stats();
                s.books += b.books;
                s.awaiting_snapshot += b.awaiting_snapshot;
                s.stale += b.stale;
                s.caught_up += b.caught_up;
                s.malformed += b.malformed;
            }
            s
        })
    }

    /// Rows dropped as late, summed over every window (in join sides too).
    pub fn late(&self) -> u64 {
        let ops = self.ops().into_iter();
        ops.map(|o| match o {
            Op::Window(w) => w.late,
            Op::Section(s) => s.late,
            _ => 0,
        })
        .sum()
    }

    /// Closes, in every view, the windows an event at `ts` (µs) would close, without a row,
    /// and inserts what they emit downstream as a row would: for sources that have gone quiet
    /// (the runtime's `--idle-close`). A held sort releases what an event at `ts` would release,
    /// and an exact ASOF join the rows every right side has passed by then (`Join::close`),
    /// before the windows after them close. Views run after the views writing what they read,
    /// whatever their creation order, and each is closed only as far as its input has reached:
    /// a source `ts`, a stream the time its writers' output has reached (a window's watermark,
    /// `hold` less for a held sort, ...), not `ts`, which would drop what they emit next as
    /// late. Windows and joins inside join sides are closed as well.
    pub fn close_until<O: Output + ?Sized>(&mut self, ts: i64, out: &mut O) {
        // the time each view's output has reached
        let mut reached = vec![ts; self.plans.len()];
        for v in self.upstream_first() {
            let written = |s: &String| {
                let writers = self.plans.iter().zip(&reached).filter(|(p, _)| p.target == *s);
                writers.fold(ts, |at, (_, r)| at.min(*r))
            };
            let inputs: Vec<i64> = self.plans[v].inputs.iter().map(written).collect();
            let rows;
            (rows, reached[v]) = close(&mut self.plans[v].ops, &inputs, self.time_limit);
            if !rows.is_empty() {
                self.write(v, rows, out);
            }
        }
    }

    /// The views, each after every view writing a stream it reads (`Engine::new` refuses a
    /// cycle), otherwise in creation order.
    fn upstream_first(&self) -> Vec<usize> {
        let mut by_target: HashMap<&str, Vec<usize>> = HashMap::new();
        for (w, p) in self.plans.iter().enumerate() {
            by_target.entry(p.target.as_str()).or_default().push(w);
        }
        let writers: Vec<Vec<usize>> = (self.plans.iter())
            .map(|p| p.inputs.iter().flat_map(|i| by_target.get(i.as_str()).into_iter().flatten().copied()).collect())
            .collect();
        let (mut order, mut placed) = (vec![], vec![false; self.plans.len()]);
        while order.len() < self.plans.len() {
            for v in 0..self.plans.len() {
                if !placed[v] && writers[v].iter().all(|w| placed[*w]) {
                    placed[v] = true;
                    order.push(v);
                }
            }
        }
        order
    }

    /// Sets the latest event time (µs) a window or an ASOF join takes; rows past it are dropped
    /// and counted (`future`) instead of moving the watermark or the join's newest time. Core
    /// has no clock: the runtime passes "now" plus the skew it allows. `None` (the default)
    /// takes any time, as Proton does.
    pub fn set_time_limit(&mut self, limit: Option<i64>) {
        self.time_limit = limit.unwrap_or(i64::MAX);
    }

    /// Withholds, in every window whose output no other window reads (it reaches the sinks only
    /// through views without a window), the windows that began before `start` (µs): they close,
    /// and their groups are dropped instead of emitted. For a start without a checkpoint whose
    /// sources lost their oldest records: such a window lacks part of its input. Only terminal
    /// windows: a withheld window feeding another window would make that one partial. Not
    /// checkpointed: the runtime sets it on every start (`i64::MIN` withholds nothing).
    pub fn withhold(&mut self, start: i64) {
        // a plan is terminal when no window reads what it writes, directly or through views
        fn windowed(ops: &[Op]) -> bool {
            ops.iter().any(|o| matches!(o, Op::Window(_)))
        }
        fn feeds_window(e: &Engine, v: usize, seen: &mut Vec<bool>) -> bool {
            for &(next, _) in e.readers.get(&e.plans[v].target).into_iter().flatten() {
                if std::mem::replace(&mut seen[next], true) {
                    continue;
                }
                if windowed(&e.plans[next].ops) || feeds_window(e, next, seen) {
                    return true;
                }
            }
            false
        }
        let terminal: Vec<bool> =
            (0..self.plans.len()).map(|v| !feeds_window(self, v, &mut vec![false; self.plans.len()])).collect();
        for (p, terminal) in self.plans.iter_mut().zip(terminal) {
            for op in &mut p.ops {
                if let Op::Window(w) = op {
                    w.withhold_start = if terminal { start } else { i64::MIN };
                }
            }
        }
    }

    /// Groups closed without being emitted (`withhold`), summed over every window.
    pub fn withheld(&self) -> u64 {
        let ops = self.plans.iter().flat_map(|p| &p.ops);
        ops.map(|o| if let Op::Window(w) = o { w.withheld } else { 0 }).sum()
    }

    /// Window groups closed and emitted so far, by every window (those in join sides too): the
    /// work a close does grows with them. Not checkpointed: this process's count.
    pub fn closed(&self) -> u64 {
        let ops = self.ops().into_iter();
        ops.map(|o| if let Op::Window(w) = o { w.closed } else { 0 }).sum()
    }

    /// Rows dropped for an event time past the limit, summed over every window and join.
    pub fn future(&self) -> u64 {
        self.dropped().map(|d| d.future).sum()
    }

    /// Rows dropped for an event time that is NULL or not a time, summed over every window and
    /// join.
    /// The gaps `gap_fill`s left unfilled for their length (`MAX_GAP`).
    pub fn unfilled(&self) -> u64 {
        self.ops().into_iter().map(|o| if let Op::Fill(f) = o { f.unfilled } else { 0 }).sum()
    }

    pub fn null_time(&self) -> u64 {
        self.dropped().map(|d| d.null_time).sum()
    }

    /// What each window and join dropped before ordering rows by time.
    fn dropped(&self) -> impl Iterator<Item = &Dropped> {
        self.ops().into_iter().filter_map(|o| match o {
            Op::Window(w) => Some(&w.dropped),
            Op::Join(j) => Some(&j.dropped),
            _ => None,
        })
    }

    /// Every operator, in join sides too: a window there counts what it drops as any other.
    fn ops(&self) -> Vec<&Op> {
        self.plans.iter().flat_map(|p| all_ops(&p.ops)).collect()
    }

    /// Every ASOF right side, in joins at any depth.
    fn rights_mut(&mut self) -> Vec<&mut Right> {
        fn walk<'a>(ops: &'a mut [Op], out: &mut Vec<&'a mut Right>) {
            for op in ops {
                if let Op::Join(j) = op {
                    for side in &mut j.sides {
                        walk(side, out);
                    }
                    out.extend(j.rights.iter_mut());
                }
            }
        }
        let mut out = vec![];
        for p in &mut self.plans {
            walk(&mut p.ops, &mut out);
        }
        out
    }

    /// How every ASOF join matches (default `Asof::Arrival`, Proton's). Call it before any
    /// row or restore: the mode is part of the plan's fingerprint.
    ///
    /// # Panics
    ///
    /// If it changes the mode of a join that has taken rows: its versions were cut, and its
    /// rows held, for the other mode, and would be matched wrongly or never released.
    pub fn set_asof(&mut self, asof: Asof) {
        fn walk(ops: &mut [Op], exact: bool) {
            for op in ops {
                if let Op::Join(j) = op {
                    // every right row leaves a version; an exact join holds every left row until one does
                    let fresh = j.held.is_empty() && j.rights.iter().all(|r| r.versions.is_empty());
                    assert!(
                        j.exact == exact || fresh,
                        "set_asof after a join took rows: call it before any row or restore"
                    );
                    j.sides.iter_mut().for_each(|side| walk(side, exact));
                    j.exact = exact;
                }
            }
        }
        self.shape.clear();
        for p in &mut self.plans {
            walk(&mut p.ops, asof == Asof::Exact);
            self.shape.push_str(&p.target);
            describe(&p.ops, &mut self.shape);
            self.shape.push('\n');
        }
    }

    /// How far past a left row's time every right side of an exact ASOF join must be before the
    /// row is released, in µs (default `ASOF_LATENESS_US`, 1 s). It is how far out of time order a
    /// right row may arrive (across keys) and still be matched exactly: a shorter wait is out
    /// sooner and misses a right row later than it (`asof_late_right` counts them), so size it
    /// from the feed's measured disorder. It does not change what an exact join does with rows
    /// that arrive in time.
    ///
    /// # Panics
    ///
    /// Unless `0 < lateness_us <= ASOF_HOLD_US`: zero would release a row before any right row at
    /// its time, and past the hold the hold decides.
    pub fn set_asof_lateness(&mut self, lateness_us: i64) {
        assert!(
            (1..=ASOF_HOLD_US).contains(&lateness_us),
            "an ASOF lateness is between 1 µs and the hold ({ASOF_HOLD_US} µs), not {lateness_us}"
        );
        self.rights_mut().into_iter().for_each(|r| r.lateness = lateness_us);
    }

    /// Bounds the keys each ASOF right side keeps (default `ASOF_MAX_KEYS`).
    pub fn set_asof_max_keys(&mut self, n: usize) {
        self.rights_mut().into_iter().for_each(|r| r.max_keys = n.max(1));
    }

    /// Keys held by ASOF right sides, and keys evicted from them so far.
    pub fn asof_keys(&self) -> (usize, u64) {
        fn walk(ops: &[Op], acc: &mut (usize, u64)) {
            for op in ops {
                if let Op::Join(j) = op {
                    j.sides.iter().for_each(|side| walk(side, acc));
                    for r in &j.rights {
                        (acc.0, acc.1) = (acc.0 + r.versions.len(), acc.1 + r.evicted);
                    }
                }
            }
        }
        let mut acc = (0, 0);
        self.plans.iter().for_each(|p| walk(&p.ops, &mut acc));
        acc
    }

    /// Right rows of exact ASOF joins that arrived at or before the time of a left row of their
    /// key already released, which may have matched them: what a lateness too short for the
    /// feed misses (`set_asof_lateness`). A runtime count: not checkpointed, and blind to a key
    /// until its first release after a restore.
    pub fn asof_late_right(&self) -> u64 {
        fn walk(ops: &[Op], acc: &mut u64) {
            for op in ops {
                if let Op::Join(j) = op {
                    j.sides.iter().for_each(|side| walk(side, acc));
                    *acc += j.rights.iter().map(|r| r.late).sum::<u64>();
                }
            }
        }
        let mut acc = 0;
        self.plans.iter().for_each(|p| walk(&p.ops, &mut acc));
        acc
    }
}

/// The settings of streams and tables: known keys only, and values brrrrr implements.
/// A sink always gets one JSONEachRow message per row; a source is JSONEachRow or ProtobufSingle.
/// `properties` (librdkafka settings for Proton's own client, such as
/// `enable.idempotence=false`) and `group_name` are Proton's client configuration and deliberately not used:
/// brrrrr's clients are configured with `--*kafka-config`.
fn check_settings(cat: &Catalog) -> R<()> {
    const KAFKA: [&str; 9] = [
        "type",
        "brokers",
        "topic",
        "data_format",
        "format_schema",
        "one_message_per_row",
        "seek_to",
        "properties",
        "group_name",
    ];
    // Proton's storage of internal streams; brrrrr stores none, so they change nothing
    const STORAGE: [&str; 6] = [
        "logstore_retention_bytes",
        "logstore_retention_ms",
        "logstore_codec",
        "ttl_only_drop_parts",
        "merge_with_ttl_timeout",
        "merge_max_block_size",
    ];
    for s in cat.streams.values() {
        let get = |k: &str| s.settings.get(k).map(String::as_str);
        let problem = match &s.kind {
            Kind::Stream => {
                s.settings.keys().find(|k| !STORAGE.contains(&k.as_str())).map(|k| format!("unknown setting {k}"))
            }
            Kind::Table(_) => (get("type") != Some("s3")).then(|| "an external table must be type = 's3'".into()),
            Kind::External => {
                let unknown = s.settings.keys().find(|k| !KAFKA.contains(&k.as_str()));
                let is_sink = cat.views.iter().any(|v| v.target == s.name);
                let format = get("data_format").unwrap_or("");
                if let Some(k) = unknown {
                    Some(format!("unknown setting {k}"))
                } else if get("type") != Some("kafka") {
                    Some("an external stream must be type = 'kafka'".into())
                } else if get("topic").is_none_or(str::is_empty) {
                    Some("no topic".into())
                } else if is_sink && format != "JSONEachRow" {
                    Some(format!("a sink's data_format must be JSONEachRow, not {format:?}"))
                } else if !is_sink
                    && format != "JSONEachRow"
                    && (format != "ProtobufSingle" || get("format_schema").is_none())
                {
                    Some(
                        "a source must be data_format = 'JSONEachRow', or 'ProtobufSingle' with a format_schema".into(),
                    )
                } else if get("one_message_per_row").is_some_and(|v| v != "true") {
                    Some("one_message_per_row must be true".into())
                } else if get("seek_to").is_some_and(|v| !matches!(v, "earliest" | "latest")) {
                    Some("seek_to must be earliest or latest".into())
                } else {
                    None
                }
            }
        };
        if let Some(p) = problem {
            return Err(format!("{}: {p}", s.name));
        }
    }
    Ok(())
}

/// Casts the values of a row in `st`'s column order to the column types; values that already
/// conform are kept as they are.
fn conform(r: &mut Row, st: &Stream) {
    for (v, c) in r.iter_mut().zip(&st.columns) {
        if !v.conforms(&c.ty) {
            *v = v.cast(&c.ty);
        }
    }
}

/// Appends to `full` a view's output row `r` in its target `st`'s column order and types,
/// moving the values out of an owned row whose values each feed one column (`Plan::unique`).
fn lay_out(plan: &Plan, st: &Stream, mut r: Cow<'_, Row>, full: &mut Row) {
    let col = |r: &mut Cow<'_, Row>, i: usize, t: &Type| match r {
        Cow::Owned(r) if plan.unique => std::mem::replace(&mut r[i], Value::Null).cast_into(t),
        r if r[i].conforms(t) => r[i].clone(),
        r => r[i].cast(t),
    };
    let cols = st.columns.iter().zip(&plan.by_name);
    full.extend(cols.map(|(c, i)| i.map_or_else(|| c.ty.default_value(), |i| col(&mut r, i, &c.ty))));
}

/// Computes a row's computed columns (`Engine::computed`): a `MATERIALIZED` one always, a
/// `DEFAULT` one when the view supplies no column of its name.
fn compute(plan: &Plan, st: &Stream, computed: &[(usize, Ex, bool)], r: &mut Row) {
    for (i, m, always) in computed {
        if *always || plan.by_name[*i].is_none() {
            r[*i] = m(r).cast_into(&st.columns[*i].ty);
        }
    }
}

/// `map(string, string)` built as `cast((keys, values), 'map(...)')`; anything else has no headers.
fn headers(v: &Value) -> Vec<(String, String)> {
    let pairs = |kv: &[Value]| match kv {
        [Value::Array(k), Value::Array(v)] => k.iter().zip(v.iter()).map(|(k, v)| (to_text(k), to_text(v))).collect(),
        _ => vec![],
    };
    if let Value::Array(kv) = v {
        pairs(kv)
    } else {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(sql: &str) -> Engine {
        Engine::new(&crate::sql::parse(sql).unwrap()).unwrap()
    }

    const VIEWS: &str = "
CREATE STREAM trades (t datetime64(6), symbol string, price float64);
CREATE STREAM marks (t datetime64(6), symbol string, mark float64);
CREATE STREAM marked (symbol string, price float64, mark float64);
CREATE EXTERNAL STREAM a_out (symbol string, n uint64) SETTINGS type = 'kafka', topic = 'a', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM b_out (symbol string, n uint64) SETTINGS type = 'kafka', topic = 'b', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW to_marks INTO marks AS SELECT window_start AS t, symbol, avg(price) AS mark FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW a INTO a_out AS SELECT symbol, count() AS n FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW c INTO marked AS SELECT tr.symbol AS symbol, tr.price AS price, m.mark AS mark
FROM (SELECT t, symbol, price FROM trades ORDER BY symbol, t) AS tr
ASOF LEFT JOIN (SELECT t, symbol, mark FROM marks ORDER BY symbol, t) AS m ON tr.symbol = m.symbol AND tr.t >= m.t;
CREATE MATERIALIZED VIEW b INTO b_out AS SELECT symbol, count() AS n FROM tumble(trades, t, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

    /// `insert` writes the rows a view keeps as they come, without its projection, only for a
    /// `SELECT * FROM s [WHERE ..]` into a Kafka sink: no other column list, no ORDER BY, no
    /// internal stream (`write` reorders and casts what goes into one).
    #[test]
    fn only_a_select_star_into_a_sink_skips_its_projection() {
        let e = engine(
            "
CREATE STREAM s (t datetime64(6), x float64);
CREATE STREAM i (t datetime64(6), x float64);
CREATE EXTERNAL STREAM k (t datetime64(6), x float64) SETTINGS type = 'kafka', topic = 'k', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW star_where INTO k AS SELECT * FROM s WHERE x > 1;
CREATE MATERIALIZED VIEW star INTO k AS SELECT * FROM s;
CREATE MATERIALIZED VIEW listed INTO k AS SELECT t, x FROM s;
CREATE MATERIALIZED VIEW ordered INTO k AS SELECT * FROM s ORDER BY t;
CREATE MATERIALIZED VIEW internal INTO i AS SELECT * FROM s;
",
        );
        let skips: Vec<bool> = e.plans.iter().map(|p| p.sink_filter.is_some()).collect();
        assert_eq!(skips, [true, true, false, false, false]);
    }

    /// The readers of a stream that may run apart: every one but a view reading what another
    /// reader writes (`c` joins the marks `to_marks` writes).
    #[test]
    fn a_reader_of_what_another_reader_writes_is_not_independent() {
        let e = engine(VIEWS);
        let names = |s: &str| -> Vec<(usize, bool)> {
            e.readers[s].iter().zip(&e.independent[s]).map(|(r, i)| (r.0, *i)).collect()
        };
        // plans in creation order: to_marks 0, a 1, c 2, b 3
        assert_eq!(names("trades"), [(0, true), (1, true), (2, false), (3, true)]);
        assert_eq!(names("marks"), [(2, true)], "marks' one reader writes nothing it reads");
    }

    /// A view reading one stream on both sides of a join runs twice per insert: never apart.
    #[test]
    fn a_self_join_is_not_independent() {
        let e = engine(
            "CREATE STREAM q (t datetime64(6), symbol string, bid float64);
CREATE EXTERNAL STREAM out (symbol string, bid float64, prev float64) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW v INTO out AS SELECT a.symbol AS symbol, a.bid AS bid, b.bid AS prev
FROM (SELECT t, symbol, bid FROM q ORDER BY symbol, t) AS a
ASOF LEFT JOIN (SELECT t, symbol, bid FROM q ORDER BY symbol, t) AS b ON a.symbol = b.symbol AND a.t >= b.t;",
        );
        assert_eq!(e.independent["q"], [false, false]);
    }

    fn trade(sec: f64) -> Row {
        vec![Value::Time((sec * 1e6) as i64), Value::Str("X".into()), Value::F64(1.0)]
    }

    /// The merge times are fixed: a restored checkpoint, or another build, merges the same
    /// groups at the same times (a change is an output change, ADR-0015).
    #[test]
    fn merge_times_are_fixed_and_spread_over_the_lead() {
        assert_eq!(merge_at("1:X", 15_000_000), 13_825_107);
        assert_eq!(merge_at("1:Y", 15_000_000), 13_560_218);
        assert_eq!(merge_at("3:BTC", 3_600_000_000), 3_598_367_283);
        assert_eq!(merge_at("", 60_000_000), 59_405_154);
        // a key whose time the finalizer's last `^=` sets: with `|=` it would be 14_111_986
        assert_eq!(merge_at("6:K47562", 15_000_000), 14_111_985);
        // a key whose time is the lead's first microsecond
        assert_eq!(merge_at("8:K3408657", 15_000_000), 13_000_000);
        let mut text = String::new();
        let mut tenths = [0; 15];
        for i in 0..3_000 {
            key_text(&mut text, [Value::Str(format!("S{i}").into()), Value::Int(i % 7)]);
            let at = merge_at(&text, 60_000_000);
            assert!((58_000_000..59_500_000).contains(&at), "{text}: {at}");
            tenths[(at - 58_000_000) as usize / 100_000] += 1;
        }
        assert!(tenths.iter().all(|&n| (150..250).contains(&n)), "{tenths:?}");
    }

    const DIGESTS: &str = "
CREATE STREAM trades (t datetime64(6), symbol string, price float64);
CREATE EXTERNAL STREAM out (symbol string, p50 float32) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW m INTO out AS SELECT symbol, quantile_t_digest(0.5)(price) AS p50 FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;";

    /// The values the digest of `symbol`'s group in [0, 15 s) has not merged yet.
    fn unmerged(e: &Engine, symbol: &str) -> usize {
        let Some(Op::Window(w)) = e.plans[0].ops.first() else { panic!("a window") };
        let mut text = String::new();
        key_text(&mut text, [Value::Str(symbol.into())]);
        w.open[&0][text.as_str()].1[0].unmerged()
    }

    /// `e` written to a checkpoint and restored into a new engine of `sql`.
    fn restored(e: &Engine, sql: &str) -> Engine {
        let bytes = postcard::to_allocvec(&e.snapshot()).unwrap();
        let mut back = engine(sql);
        back.restore(postcard::from_bytes(&bytes).unwrap()).unwrap();
        back
    }

    /// A group's digest merges once, after the row that moves the window's newest time past its
    /// merge time, also with a checkpoint restored in between; one under 512 values does not
    /// (ClickHouse's at its read); one opened past its merge time does not.
    #[test]
    fn a_digest_merges_ahead_of_the_close_at_its_merge_time() {
        let at = |t: i64, sym: &str| vec![Value::Time(t), Value::Str(sym.into()), Value::F64(t as f64)];
        let (x, y) = (merge_at("1:X", 15_000_000), merge_at("1:Y", 15_000_000));
        assert!(y < x);
        // restored before Y's time, or right after X's (the groups due are rebuilt from those
        // whose time is still to come: X is not merged again)
        for (before_y, after_x) in [(false, false), (true, false), (false, true)] {
            let mut e = engine(DIGESTS);
            // X 600 values (merged at 512), Y 100, before the lead
            let rows = (0..700).map(|i| at(i * 10_000, if i % 7 == 6 { "Y" } else { "X" })).collect();
            e.insert("trades", rows, &mut vec![]);
            assert_eq!((unmerged(&e, "X"), unmerged(&e, "Y")), (88, 100));
            e.insert("trades", vec![at(y - 1, "Z")], &mut vec![]);
            if before_y {
                e = restored(&e, DIGESTS);
            }
            e.insert("trades", vec![at(y, "X")], &mut vec![]);
            assert_eq!((unmerged(&e, "X"), unmerged(&e, "Y")), (89, 100), "Y's time: under 512, left alone");
            e.insert("trades", vec![at(x - 1, "X")], &mut vec![]);
            assert_eq!(unmerged(&e, "X"), 90, "not yet");
            e.insert("trades", vec![at(x, "Z")], &mut vec![]);
            assert_eq!(unmerged(&e, "X"), 0, "X's time");
            if after_x {
                e = restored(&e, DIGESTS);
            }
            e.insert("trades", vec![at(x + 1, "X"), at(14_000_000, "X")], &mut vec![]);
            assert_eq!(unmerged(&e, "X"), 2, "once");
            // W: 600 values in [15, 30 s) all at once, past its merge time
            let w = merge_at("1:W", 30_000_000);
            let rows: Vec<_> = (0..600).map(|_| at(w + 1, "W")).collect();
            e.insert("trades", rows, &mut vec![]);
            let Some(Op::Window(win)) = e.plans[0].ops.first() else { panic!() };
            assert_eq!(win.open[&15_000_000]["1:W"].1[0].unmerged(), 88, "opened past its time");
        }
    }

    /// A group opened in its window's lead before its merge time merges at it, in the order of
    /// the merge times: X, opened after Y's window entered its lead, does not hold Y back. Row by
    /// row or in one chunk, the same.
    #[test]
    fn a_group_opened_in_the_lead_merges_at_its_time_in_order() {
        let at = |t: i64, sym: &str| vec![Value::Time(t), Value::Str(sym.into()), Value::F64(t as f64)];
        let (x, y) = (merge_at("1:X", 15_000_000), merge_at("1:Y", 15_000_000));
        assert!(13_000_000 < y && y < x);
        let mut outs = vec![];
        for chunked in [false, true] {
            let mut e = engine(DIGESTS);
            let insert = |e: &mut Engine, rows: Vec<Row>, out: &mut Vec<Emit>| {
                if chunked {
                    e.insert("trades", rows, out);
                } else {
                    rows.into_iter().for_each(|r| e.insert("trades", vec![r], out));
                }
            };
            let mut out = vec![];
            // Y 600 values, then Y's window enters its lead
            insert(&mut e, (0..600).map(|i| at(i * 10_000, "Y")).collect(), &mut out);
            insert(&mut e, vec![at(13_000_000, "Y")], &mut out);
            assert_eq!(unmerged(&e, "Y"), 89);
            // X opens in the lead, before both merge times
            insert(&mut e, (1..=600).map(|i| at(13_000_000 + i, "X")).collect(), &mut out);
            assert_eq!(unmerged(&e, "X"), 88);
            insert(&mut e, vec![at(y, "Z")], &mut out);
            assert_eq!((unmerged(&e, "Y"), unmerged(&e, "X")), (0, 88), "Y's time");
            insert(&mut e, vec![at(x, "Z")], &mut out);
            assert_eq!(unmerged(&e, "X"), 0, "X's time");
            insert(&mut e, vec![at(30_000_000, "Z")], &mut out);
            outs.push(out);
        }
        assert_eq!(outs[0].len(), 3);
        assert!(outs[0] == outs[1]);
    }

    /// A group opened in the lead at its merge time, once the window's newest time is already
    /// there, is past it: it does not merge ahead.
    #[test]
    fn a_group_opened_at_its_time_does_not_merge_ahead() {
        let at = |t: i64, sym: &str| vec![Value::Time(t), Value::Str(sym.into()), Value::F64(t as f64)];
        let x = merge_at("1:X", 15_000_000);
        let mut e = engine(DIGESTS);
        e.insert("trades", vec![at(0, "Y"), at(13_000_000, "Y"), at(x, "Z")], &mut vec![]);
        e.insert("trades", (0..600).map(|i| at(x - i, "X")).collect(), &mut vec![]);
        e.insert("trades", vec![at(x + 1, "Z")], &mut vec![]);
        assert_eq!(unmerged(&e, "X"), 88);
    }

    /// The lead's edge: a group whose merge time is the lead's first microsecond merges after the
    /// row at exactly that time, which is also the row that brings its window into its lead.
    #[test]
    fn a_digest_merges_at_the_leads_first_microsecond() {
        let at = |t: i64, sym: &str| vec![Value::Time(t), Value::Str(sym.into()), Value::F64(t as f64)];
        let k = "K3408657";
        assert_eq!(merge_at("8:K3408657", 15_000_000), 13_000_000);
        let mut e = engine(DIGESTS);
        e.insert("trades", (0..600).map(|i| at(i * 10_000, k)).collect(), &mut vec![]);
        e.insert("trades", vec![at(12_999_999, "Z")], &mut vec![]);
        assert_eq!(unmerged(&e, k), 88, "not yet");
        e.insert("trades", vec![at(13_000_000, "Z")], &mut vec![]);
        assert_eq!(unmerged(&e, k), 0, "at the lead's first microsecond");
    }

    /// `close_until` moves a window's newest time as a row would, and merges what such a row would.
    #[test]
    fn close_until_merges_the_groups_whose_time_it_passes() {
        let at = |t: i64, sym: &str| vec![Value::Time(t), Value::Str(sym.into()), Value::F64(t as f64)];
        let x = merge_at("1:X", 15_000_000);
        let mut e = engine(DIGESTS);
        e.insert("trades", (0..600).map(|i| at(i * 10_000, "X")).collect(), &mut vec![]);
        let mut out = vec![];
        e.close_until(x - 1, &mut out);
        assert_eq!(unmerged(&e, "X"), 88, "not yet");
        e.close_until(x, &mut out);
        assert_eq!(unmerged(&e, "X"), 0, "X's time");
        assert!(out.is_empty(), "[0, 15 s) still open");
    }

    const DIGESTS_IF: &str = "
CREATE STREAM trades (t datetime64(6), symbol string, price float64);
CREATE EXTERNAL STREAM out (symbol string, p50 float32, p50_if float32) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM out_if (symbol string, p50_if float32) SETTINGS type = 'kafka', topic = 'i', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW m INTO out AS SELECT symbol, quantile_t_digest(0.5)(price) AS p50, quantile_t_digest_if(0.5)(price, price >= 0) AS p50_if FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW i INTO out_if AS SELECT symbol, quantile_t_digest_if(0.5)(price, price >= 0) AS p50_if FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;";

    /// A t-digest under `_if` merges ahead as a plain one does, beside one or alone in its view:
    /// over the same values, the same results bit for bit.
    #[test]
    fn a_digest_under_if_merges_ahead_as_a_plain_one() {
        let at = |t: i64| vec![Value::Time(t), Value::Str("X".into()), Value::F64((t % 7_919) as f64)];
        let x = merge_at("1:X", 15_000_000);
        let mut e = engine(DIGESTS_IF);
        e.insert("trades", (0..600).map(|i| at(i * 10_000)).collect(), &mut vec![]);
        e.insert("trades", vec![at(x)], &mut vec![]);
        let unmerged = |view: usize| -> Vec<usize> {
            let Some(Op::Window(w)) = e.plans[view].ops.first() else { panic!("a window") };
            w.open[&0]["1:X"].1.iter().map(Acc::unmerged).collect()
        };
        assert_eq!((unmerged(0), unmerged(1)), (vec![0, 0], vec![0]), "X's time");
        let mut out = vec![];
        e.insert("trades", (1..=100).map(|i| at(x + i)).chain([at(30_000_000)]).collect(), &mut out);
        let p50 = |m: &Emit, c: &str| -> u32 {
            let j: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
            (j[c].as_f64().unwrap() as f32).to_bits()
        };
        assert_eq!(out.len(), 2);
        let (m, i) = if &*out[0].topic == "o" { (&out[0], &out[1]) } else { (&out[1], &out[0]) };
        assert_eq!(p50(m, "p50"), p50(m, "p50_if"));
        assert_eq!(p50(m, "p50"), p50(i, "p50_if"));
    }

    /// A view without a t-digest has no lead: it keeps no groups due.
    #[test]
    fn a_window_without_a_digest_has_no_lead() {
        let mut e = engine(VIEWS);
        e.insert("trades", vec![trade(1.0), trade(13.5), trade(14.9)], &mut vec![]);
        let Some(Op::Window(w)) = e.plans[1].ops.first() else { panic!("a's first operator") };
        assert!(!w.digests);
        assert_eq!((w.due.len(), w.lead_at), (0, i64::MAX));
        let e = restored(&e, VIEWS);
        let Some(Op::Window(w)) = e.plans[1].ops.first() else { panic!() };
        assert_eq!((w.due.len(), w.lead_at), (0, i64::MAX));
    }

    /// A chunk closes a window once its latest time it takes, less the delay, is past the oldest
    /// open window's end; rows past the time limit, without a time, or a window with nothing
    /// open close nothing.
    #[test]
    fn a_window_knows_which_chunks_close_it() {
        let mut e = engine(VIEWS);
        let Some(Op::Window(w)) = e.plans[1].ops.first() else { panic!("a's first operator") };
        assert_eq!(w.closes(&[trade(1.0)], i64::MAX), 0, "nothing open");
        e.insert("trades", vec![trade(1.0)], &mut vec![]);
        let Some(Op::Window(w)) = e.plans[1].ops.first() else { panic!() };
        assert_eq!(w.closes(&[trade(14.0), trade(15.0)], i64::MAX), 0, "15 s less the 50 ms delay is in the window");
        assert_eq!(w.closes(&[trade(14.0), trade(15.05)], i64::MAX), 1, "15.05 s less the delay ends it");
        assert_eq!(w.closes(&[trade(15.05)], 15_000_000), 0, "past the time limit: not taken");
        let null = vec![Value::Null, Value::Str("X".into()), Value::F64(1.0)];
        assert_eq!(w.closes(&[null], i64::MAX), 0, "no time");
        assert_eq!(w.closes(&[trade(100.0), trade(2.0)], i64::MAX), 1, "the latest counts, in any order");
        // the groups of every window a chunk closes count
        let at = |sec: f64, sym: &str| vec![Value::Time((sec * 1e6) as i64), Value::Str(sym.into()), Value::F64(1.0)];
        e.insert("trades", vec![at(2.0, "Y"), at(3.0, "Z")], &mut vec![]);
        let Some(Op::Window(w)) = e.plans[1].ops.first() else { panic!() };
        assert_eq!(w.closes(&[trade(15.05)], i64::MAX), 3, "X, Y and Z in [0, 15)");
        e.insert("trades", vec![at(15.0, "X")], &mut vec![]); // [15, 30) opens; [0, 15) not yet closed
        let Some(Op::Window(w)) = e.plans[1].ops.first() else { panic!() };
        assert_eq!(w.closes(&[trade(30.06)], i64::MAX), 4, "[0, 15) and [15, 30) both close");
        assert_eq!(w.closes(&[trade(16.0)], i64::MAX), 3, "only [0, 15)");
    }

    /// A view whose close panics, on any thread: the panic goes on, every view's operators are
    /// back in its plan (none left taken out for a run), and nothing is written once it happened.
    #[test]
    fn a_panicking_close_gives_every_view_its_operators_back_and_writes_nothing_more() {
        for threads in [2, 3] {
            // the heavy views at 61 s: to_marks (0) and a (1) close [0, 15), b (3) closes [0, 60)
            for panicking in [0, 1, 3] {
                let mut e = engine(VIEWS);
                e.set_close_threads(threads);
                e.set_close_groups(0);
                e.insert("trades", vec![trade(1.0)], &mut vec![]);
                let boom: Ex = std::sync::Arc::new(|_: &[Value]| -> Value { panic!("a close that panics") });
                e.plans[panicking].ops.push(Op::Project { filter: None, exprs: vec![boom], all: false });
                let ops: Vec<usize> = e.plans.iter().map(|p| p.ops.len()).collect();
                let mut out = vec![];
                let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    e.insert("trades", vec![trade(61.0)], &mut out);
                }));
                assert!(ran.is_err(), "{threads} threads, view {panicking}: no panic");
                assert_eq!(e.parallel_inserts(), 1);
                let back: Vec<usize> = e.plans.iter().map(|p| p.ops.len()).collect();
                assert_eq!(back, ops, "{threads} threads, view {panicking}: operators missing");
                let written: Vec<&str> = out.iter().map(|m| &*m.topic).collect();
                let want: &[&str] = if panicking == 3 { &["a"] } else { &[] }; // a writes before b
                assert_eq!(written, want, "{threads} threads, view {panicking}");
            }
        }
    }

    /// `closed` counts the groups every window closed and emitted, in any view; groups a start
    /// without a checkpoint withholds are not.
    #[test]
    fn closed_counts_the_groups_windows_emit_not_those_withheld() {
        let mut e = engine(VIEWS);
        let at = |sec: f64, sym: &str| vec![Value::Time((sec * 1e6) as i64), Value::Str(sym.into()), Value::F64(1.0)];
        e.insert("trades", vec![at(1.0, "X"), at(2.0, "Y"), at(16.0, "X")], &mut vec![]);
        // the 15s windows of to_marks and a closed X and Y in [0, 15)
        assert_eq!(e.closed(), 4);
        let mut out = vec![];
        e.insert("trades", vec![at(61.0, "Z")], &mut out);
        // [15, 30): X in to_marks and a; [0, 60): X and Y in b
        assert_eq!(e.closed(), 8);
        let mut w = engine(VIEWS);
        w.withhold(100 * 1_000_000);
        w.insert("trades", vec![at(1.0, "X"), at(16.0, "X")], &mut vec![]);
        assert_eq!((w.closed(), w.withheld()), (0, 2), "withheld, not closed");
    }

    /// Several threads are worth it once a chunk closes two or more independent views' windows;
    /// with one thread, never.
    #[test]
    fn a_chunk_closing_two_independent_views_is_closed_on_several_threads() {
        let mut e = engine(VIEWS);
        e.insert("trades", vec![trade(1.0)], &mut vec![]);
        assert_eq!(e.heavy("trades", &[trade(15.05)]), None, "one thread");
        e.set_close_threads(2);
        e.set_close_groups(0);
        assert_eq!(e.heavy("trades", &[trade(15.05)]), Some(vec![true, true, false, false]), "two threads");
        e.set_close_groups(CLOSE_GROUPS);
        e.set_close_threads(4);
        assert_eq!(e.heavy("trades", &[trade(15.05)]), None, "two groups, fewer than CLOSE_GROUPS");
        e.set_close_groups(2);
        // to_marks and a close at 15 s, b (1m) does not, c is a join
        assert_eq!(e.heavy("trades", &[trade(15.05)]), Some(vec![true, true, false, false]));
        e.set_close_groups(3);
        assert_eq!(e.heavy("trades", &[trade(15.05)]), None, "two groups, one fewer than asked");
        e.set_close_groups(0);
        assert_eq!(e.heavy("trades", &[trade(60.05)]), Some(vec![true, true, false, true]));
        assert_eq!(e.heavy("trades", &[trade(3.0)]), None, "closes nothing");
        assert_eq!(e.heavy("marks", &[trade(60.05)]), None, "one reader");
        assert_eq!(e.heavy("nobody", &[trade(60.05)]), None);
        // a reader that is not independent runs in its turn, whatever its window closes
        e.independent.get_mut("trades").unwrap()[1] = false;
        assert_eq!(e.heavy("trades", &[trade(60.05)]), Some(vec![true, false, false, true]));
        e.independent.get_mut("trades").unwrap()[3] = false;
        assert_eq!(e.heavy("trades", &[trade(60.05)]), None, "one heavy reader left");
        e.independent = e.readers.keys().map(|s| (s.clone(), e.independent(s))).collect();
        let mut out = vec![];
        e.insert("trades", vec![trade(60.05)], &mut out);
        assert_eq!(e.parallel_inserts(), 1);
        e.set_close_threads(0);
        assert_eq!(e.close_threads, 1, "at least one");
    }

    /// A sink's headers are built from the row only when `_tp_message_headers` is MATERIALIZED,
    /// of the usual shape and the last computed column, a choice made once by `Engine::new`;
    /// any other sink computes the map into the row and reads it back. Either way the same headers.
    #[test]
    fn sink_headers_are_built_from_the_row_for_the_usual_shape_only() {
        let key = "cast((['dedup-key'], [concat(to_string(time), '|', to_string(exchange), '|', symbol)]), \
                   'map(string, string)')";
        let other = "cast(if(exchange > 0, (['k'], [symbol]), (['k'], ['none'])), 'map(string, string)')";
        let sink = |name: &str, headers: &str| {
            format!(
                "CREATE EXTERNAL STREAM {name} (time int64, exchange int32, symbol string, _tp_message_headers \
                 map(string, string) {headers}) SETTINGS type = 'kafka', topic = '{name}', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW to_{name} INTO {name} AS SELECT time, exchange, symbol FROM src;\n"
            )
        };
        let mut sql = "CREATE STREAM src (time int64, exchange int32, symbol string);\n".to_string();
        sql += &sink("usual", &format!("MATERIALIZED {key}"));
        sql += &sink("other", &format!("MATERIALIZED {other}"));
        sql += &sink("default", &format!("DEFAULT {key}"));
        sql += &sink("followed", &format!("MATERIALIZED {key}, n int64 MATERIALIZED exchange + 1"));
        let mut e = engine(&sql);
        let built = |s: &str| e.sinks[s].headers.built();
        assert_eq!(built("usual"), Some(3));
        assert_eq!(["other", "default", "followed"].map(built), [None; 3]);
        assert!(!e.computed.contains_key("usual"), "its one computed column is built instead");
        assert!(e.computed["followed"].iter().any(|(i, ..)| *i == 3));
        let mut out = vec![];
        e.insert(
            "src",
            vec![vec![Value::Int(1_759_503_600_000_000), Value::Int(2), Value::Str("BTC".into())]],
            &mut out,
        );
        let got = out.iter().map(|m| (&*m.topic, m.headers.clone())).collect::<Vec<_>>();
        let h = |k: &str, v: &str| vec![(k.to_string(), v.to_string())];
        let dedup = h("dedup-key", "1759503600000000|2|BTC");
        assert_eq!(
            got,
            [("usual", dedup.clone()), ("other", h("k", "BTC")), ("default", dedup.clone()), ("followed", dedup)]
        );
        assert!(out
            .iter()
            .all(|m| m.payload.starts_with("{\"time\":1759503600000000,\"exchange\":2,\"symbol\":\"BTC\"")));
    }
}
