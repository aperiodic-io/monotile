//! Per-row window functions: `f(...) OVER (PARTITION BY ... ORDER BY ... [frame])` in a SELECT
//! over a stream or an ASOF join. A tumbling window emits one row per group when it closes;
//! here every input row gets its results at once, over the rows its partition has seen so far,
//! as QuestDB's live views maintain them.
//!
//! - Frames: the default (no frame: every row so far), `ROWS [BETWEEN] n PRECEDING [AND
//!   CURRENT ROW]`, `RANGE [BETWEEN] <interval> PRECEDING [AND CURRENT ROW]` and `UNBOUNDED
//!   PRECEDING`. A frame cannot end after the current row: a stream has not seen those rows.
//! - Functions: every aggregate over a frame, `first_value`, `last_value`, `lag(x[, n[,
//!   default]])`, `row_number()`, and exponential moving averages `avg(x, 'alpha', a)` and
//!   `avg(x, 'period', n)` (alpha 2 / (n + 1)), as QuestDB spells them. `lead` is the row's own
//!   `x` here, which the engine's `Lead` after this replaces with the next rows'. Ranking
//!   functions over the rows of each time are the engine's `Section`'s, after this.
//! - Anchored running totals (QuestDB's `ANCHOR DAILY`) partition by the anchor:
//!   `sum(q) OVER (PARTITION BY symbol, to_start_of_day(t) ORDER BY t)`.
//! - Rows are taken in arrival order. A RANGE frame reaches back from the partition's latest
//!   ORDER BY value: a row that arrives with an earlier one takes effect at the latest time.
//! - Bounds: a partition keeps at most `MAX_ROWS` rows of a RANGE frame (the oldest go first,
//!   counted in `truncated`), a ROWS frame at most `MAX_ROWS`, and an operator at most
//!   `MAX_PARTITIONS` partitions (the least recently used tenth goes, counted in `evicted`).
//!
//! Sums, means, counts, weighted means, minima, maxima, first and last values over a bounded
//! frame are kept as the frame slides; any other aggregate is recomputed over the frame's rows.
//! What is kept is rebuilt from the frame's rows after a restore, so it depends only on them:
//! Float64 sums are exact, rounded once (to nearest even) for each result.
use crate::agg::Acc;
use crate::expr::{compare, Arg, Ex, Pred};
use crate::value::Value;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const MAX_PARTITIONS: usize = 100_000;
pub const MAX_ROWS: usize = 100_000;

type R<T> = Result<T, String>;

/// The rows a window function sees, ending at the current row.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    /// Every row of the partition so far.
    Cumulative,
    /// The current row and the `n` before it.
    Rows(usize),
    /// Rows at most this far before the current one in ORDER BY value.
    Range(i64),
}

#[derive(Clone, Debug)]
pub enum Func {
    /// An aggregate (`crate::agg`) of `nargs` arguments over a frame.
    Agg {
        name: String,
        params: Vec<Value>,
        nargs: usize,
        frame: Frame,
    },
    /// `first_value` / `last_value` over a frame; NULLs are values.
    Value {
        first: bool,
        frame: Frame,
    },
    /// `lag(x, offset, default)`: the value `offset` rows back, or `default`.
    Lag {
        offset: usize,
        default: Value,
    },
    /// Exponential moving average; NULL values leave it as it was.
    Ema {
        alpha: f64,
    },
    RowNumber,
}

impl Func {
    /// A fresh state for this function (`check` restores are held against it).
    fn state(&self) -> FnState {
        match self {
            Func::Agg { name, params, nargs, frame: Frame::Cumulative } => {
                FnState::Acc(Acc::new(name, params, *nargs).expect("checked at plan time"))
            }
            Func::Value { first: true, frame: Frame::Cumulative } => FnState::First(None),
            Func::Agg { .. } | Func::Value { .. } => FnState::Frame(VecDeque::new()),
            Func::Lag { .. } => FnState::Values(VecDeque::new()),
            Func::Ema { .. } => FnState::Ema(None),
            Func::RowNumber => FnState::Rows(0),
        }
    }

    fn frame(&self) -> Option<&Frame> {
        match self {
            Func::Agg { frame, .. } | Func::Value { frame, .. } => Some(frame),
            _ => None,
        }
    }
}

/// One window function of a SELECT: what it computes, its arguments (compiled against the
/// input row), and the output position that receives its result.
#[derive(Clone)]
pub struct Spec {
    pub func: Func,
    pub args: Vec<Ex>,
    pub slot: usize,
    /// For the plan fingerprint.
    pub describe: String,
}

/// The state of one window function in one partition. The checkpointed form: a bounded frame
/// keeps its rows, and what is kept as it slides is rebuilt from them.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FnState {
    /// A cumulative aggregate.
    Acc(Acc),
    /// A bounded frame's rows: (row number in the partition, ORDER BY value, argument). Two
    /// arguments are held as a two-element array.
    Frame(VecDeque<(u64, i64, Value)>),
    /// `lag`: the last `offset` values, oldest first.
    Values(VecDeque<Value>),
    Ema(Option<f64>),
    /// A cumulative `first_value`, once a row has come.
    First(Option<Value>),
    Rows(u64),
}

/// One partition: its key values, when it was last used (for eviction), the latest ORDER BY
/// value, the number of rows it has seen, and each window function's state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Part {
    pub keys: Vec<Value>,
    pub used: u64,
    pub latest: i64,
    pub rows: u64,
    pub fns: Vec<FnState>,
    /// What each bounded frame keeps as it slides; rebuilt from the frame after a restore.
    #[serde(skip)]
    slide: Vec<Slide>,
}

/// The window functions that share one PARTITION BY and ORDER BY.
#[derive(Clone)]
pub struct Group {
    pub(crate) partition: Vec<Arg>,
    pub(crate) order: Option<Arg>,
    pub specs: Vec<Spec>,
    pub parts: FxHashMap<String, Part>,
    pub used: u64,
    pub evicted: u64,
    pub truncated: u64,
    pub max_parts: usize,
    /// Scratch: the current row's key values and key text.
    key: Vec<Value>,
    text: String,
}

/// The checkpointed state of a group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GroupState {
    pub used: u64,
    pub evicted: u64,
    pub truncated: u64,
    /// In key order, so the same state is always the same bytes.
    pub parts: Vec<(String, Part)>,
}

/// A `GroupState` borrowed from the live group: the same bytes, without the copy.
#[derive(Serialize)]
#[serde(rename = "GroupState")]
pub struct GroupStateRef<'a> {
    pub used: u64,
    pub evicted: u64,
    pub truncated: u64,
    pub parts: Vec<(&'a str, &'a Part)>,
}

impl GroupStateRef<'_> {
    pub fn to_owned(&self) -> GroupState {
        let parts = self.parts.iter().map(|(k, p)| (k.to_string(), (*p).clone())).collect();
        GroupState { used: self.used, evicted: self.evicted, truncated: self.truncated, parts }
    }
}

/// The operator: filters (WHERE comes before window functions), then appends each window
/// function's result to the row, at its slot.
#[derive(Clone)]
pub struct Over {
    pub filter: Option<Pred>,
    pub groups: Vec<Group>,
    /// The width of the rows it returns: the input's plus one column per window function.
    pub width: usize,
}

impl Group {
    pub(crate) fn new(partition: Vec<Arg>, order: Option<Arg>) -> Group {
        Group {
            partition,
            order,
            specs: vec![],
            parts: FxHashMap::default(),
            used: 0,
            evicted: 0,
            truncated: 0,
            max_parts: MAX_PARTITIONS,
            key: vec![],
            text: String::new(),
        }
    }

    /// Drops the least recently used tenth of the partitions.
    fn evict(&mut self) {
        let n = (self.parts.len() / 10).max(1);
        let mut by_use: Vec<(u64, &String)> = self.parts.iter().map(|(k, p)| (p.used, k)).collect();
        by_use.sort_unstable();
        let stalest: Vec<String> = by_use[..n].iter().map(|(_, k)| (*k).clone()).collect();
        for k in stalest {
            self.parts.remove(&k);
        }
        self.evicted += n as u64;
    }

    /// Computes this group's window functions for `row` into `out`.
    fn apply(&mut self, row: &[Value], out: &mut [Value]) {
        self.key.clear();
        self.key.extend(self.partition.iter().map(|k| k.value(row)));
        crate::engine::key_text(&mut self.text, self.key.iter());
        if !self.parts.contains_key(self.text.as_str()) {
            if self.parts.len() >= self.max_parts {
                self.evict();
            }
            let fns = self.specs.iter().map(|s| s.func.state()).collect();
            let part = Part { keys: self.key.clone(), used: 0, latest: i64::MIN, rows: 0, fns, slide: vec![] };
            self.parts.insert(self.text.clone(), part);
        }
        self.used += 1;
        let part = self.parts.get_mut(self.text.as_str()).expect("inserted above");
        part.used = self.used;
        // a row earlier than the partition's latest takes effect at the latest
        let t = self.order.as_ref().and_then(|o| o.eval(row).i64()).unwrap_or(part.latest).max(part.latest);
        part.latest = t;
        part.rows += 1;
        if part.slide.len() != part.fns.len() {
            part.slide = self.specs.iter().zip(&part.fns).map(|(s, st)| Slide::rebuild(&s.func, st)).collect();
        }
        let id = part.rows;
        for ((spec, st), slide) in self.specs.iter().zip(&mut part.fns).zip(&mut part.slide) {
            let arg = |i: usize| spec.args.get(i).map_or(Value::Null, |a| a(row));
            out[spec.slot] = match (&spec.func, st) {
                (Func::Agg { .. }, FnState::Acc(acc)) => {
                    match spec.args.len() {
                        2 => acc.add2(arg(0), arg(1)),
                        _ => acc.add(arg(0)), // NULL for count()
                    }
                    acc.result()
                }
                (Func::Value { .. }, FnState::First(first)) => first.get_or_insert_with(|| arg(0)).clone(),
                (func, FnState::Frame(rows)) => {
                    let v = match spec.args.len() {
                        2 => Value::Array([arg(0), arg(1)].into()),
                        _ => arg(0),
                    };
                    slide.push(id, &v);
                    rows.push_back((id, t, v));
                    let keep = match func.frame() {
                        Some(Frame::Rows(n)) => *n + 1,
                        _ => MAX_ROWS,
                    };
                    while let Some(front) = rows.front() {
                        let old = match func.frame() {
                            Some(Frame::Range(w)) => front.1 < t.saturating_sub(*w),
                            _ => false,
                        };
                        if !old && rows.len() <= keep {
                            break;
                        }
                        if !old {
                            self.truncated += matches!(func.frame(), Some(Frame::Range(_))) as u64;
                        }
                        let (gone, _, v) = rows.pop_front().expect("front exists");
                        slide.pop(gone, &v);
                    }
                    slide.result(func, rows)
                }
                (Func::Lag { offset, default }, FnState::Values(last)) => {
                    let v = if last.len() == *offset {
                        last.front().cloned().unwrap_or(Value::Null)
                    } else {
                        default.clone()
                    };
                    last.push_back(arg(0));
                    if last.len() > *offset {
                        last.pop_front();
                    }
                    v
                }
                (Func::Ema { alpha }, FnState::Ema(ema)) => {
                    if let Some(x) = arg(0).f64() {
                        *ema = Some(ema.map_or(x, |e| alpha * x + (1.0 - alpha) * e));
                    }
                    ema.map_or(Value::Null, Value::F64)
                }
                (Func::RowNumber, FnState::Rows(n)) => {
                    *n += 1;
                    Value::UInt(*n)
                }
                _ => unreachable!("a state is made for its function"),
            };
        }
    }

    pub fn snapshot(&self) -> GroupStateRef<'_> {
        let mut parts: Vec<(&str, &Part)> = self.parts.iter().map(|(k, p)| (k.as_str(), p)).collect();
        parts.sort_unstable_by(|a, b| a.0.cmp(b.0));
        GroupStateRef { used: self.used, evicted: self.evicted, truncated: self.truncated, parts }
    }

    /// Checks a restored group: partitions the plan could have made, under their key text, with
    /// each function's state of the right kind and within its bounds.
    pub fn check(&self, st: &GroupState) -> R<()> {
        if st.parts.len() > self.max_parts {
            return Err(format!("{} partitions, more than the {} kept", st.parts.len(), self.max_parts));
        }
        let mut text = String::new();
        let mut seen = std::collections::HashSet::new();
        for (key, p) in &st.parts {
            crate::engine::key_text(&mut text, p.keys.iter());
            if p.keys.len() != self.partition.len() || text != *key || !seen.insert(key) {
                return Err(format!("the partition {key:?} is filed under another key or twice"));
            }
            if p.fns.len() != self.specs.len() || p.used > st.used {
                return Err(format!("the partition {key:?} does not hold this plan's window functions"));
            }
            for (spec, fs) in self.specs.iter().zip(&p.fns) {
                let fresh = spec.func.state();
                let ok = match (&spec.func, fs, &fresh) {
                    (_, FnState::Acc(a), FnState::Acc(plan)) => a.check_against(plan).is_ok(),
                    (f, FnState::Frame(rows), FnState::Frame(_)) => {
                        let keep = match f.frame() {
                            Some(Frame::Rows(n)) => *n + 1,
                            _ => MAX_ROWS,
                        };
                        let ordered = rows.iter().zip(rows.iter().skip(1)).all(|(a, b)| a.0 < b.0 && a.1 <= b.1);
                        rows.len() <= keep && ordered && rows.back().is_none_or(|r| r.0 <= p.rows && r.1 <= p.latest)
                    }
                    (Func::Lag { offset, .. }, FnState::Values(v), FnState::Values(_)) => v.len() <= *offset,
                    (_, FnState::Ema(_), FnState::Ema(_)) | (_, FnState::First(_), FnState::First(_)) => true,
                    (_, FnState::Rows(n), FnState::Rows(_)) => *n <= p.rows,
                    _ => false,
                };
                if !ok {
                    return Err(format!("the partition {key:?} holds a state {} cannot have", spec.describe));
                }
            }
        }
        Ok(())
    }

    /// Restores a state `check` accepted.
    pub fn restore(&mut self, st: GroupState) {
        (self.used, self.evicted, self.truncated) = (st.used, st.evicted, st.truncated);
        self.parts = st.parts.into_iter().collect();
    }
}

impl Over {
    /// Out of line: inlined into the engine's operator loop, it would push the projection's
    /// row loop there out of line instead.
    #[inline(never)]
    pub fn apply(&mut self, rows: &[Vec<Value>]) -> Vec<Vec<Value>> {
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            if self.filter.as_ref().is_some_and(|f| f(r) != Some(true)) {
                continue;
            }
            let mut o = Vec::with_capacity(self.width);
            o.extend_from_slice(r);
            o.resize(self.width, Value::Null);
            for g in &mut self.groups {
                g.apply(r, &mut o);
            }
            out.push(o);
        }
        out
    }
}

/// What a bounded frame keeps as it slides, so a result does not revisit every row. It is not
/// checkpointed but rebuilt from the frame's rows (`Slide::rebuild`), so it must be a function
/// of the rows in the frame, not of the ones that left it before: Float64 sums are exact
/// (`Exact`), so the order they were added and taken away in cannot show.
#[derive(Clone, Debug, Default)]
enum Slide {
    /// Recompute over the frame's rows (aggregates other than the ones below).
    #[default]
    Recompute,
    /// `sum`: the running sum in the type of the first value (Float64, Int64 or UInt64, as
    /// `Acc::Sum` would give), and how many non-NULL values it holds.
    Sum { f: Exact, i: i64, u: u64, kind: u8, n: u64 },
    /// `avg`: the Float64 sum and count.
    Avg { s: Exact, n: u64 },
    /// `count(x)` (non-NULL values) or `count()` (rows).
    Count { n: u64, rows: bool },
    /// `vwap` / `weighted_avg`: the sums of x * w and w.
    Weighted { xw: Exact, w: Exact },
    /// `min` / `max`: the candidates, oldest first, each at least as good as every later one
    /// (of equal values, 0 and -0, the oldest wins, as in `Acc`); and `odd`, the row number of
    /// the latest value that compares with nothing, not even itself (a NaN, a tuple holding a
    /// NULL), 0 for none. `Acc` keeps or skips such a value depending on what came before it,
    /// so while one is in the frame, the frame is recomputed.
    Best { max: bool, q: VecDeque<(u64, Value)>, odd: u64 },
    /// `first_value` / `last_value`: read off the frame itself.
    Ends,
}

impl Slide {
    fn new(func: &Func) -> Slide {
        match func {
            Func::Value { .. } => Slide::Ends,
            Func::Agg { name, nargs, .. } => match name.as_str() {
                "sum" => Slide::Sum { f: Exact::default(), i: 0, u: 0, kind: 0, n: 0 },
                "avg" => Slide::Avg { s: Exact::default(), n: 0 },
                "count" => Slide::Count { n: 0, rows: *nargs == 0 },
                "vwap" | "weighted_avg" => Slide::Weighted { xw: Exact::default(), w: Exact::default() },
                "min" | "max" => Slide::Best { max: name == "max", q: VecDeque::new(), odd: 0 },
                _ => Slide::Recompute,
            },
            _ => Slide::Recompute,
        }
    }

    /// The slide of a restored (or new) state: its frame's rows added again.
    fn rebuild(func: &Func, st: &FnState) -> Slide {
        let mut s = Slide::new(func);
        if let FnState::Frame(rows) = st {
            for (id, _, v) in rows {
                s.push(*id, v);
            }
        }
        s
    }

    fn push(&mut self, id: u64, v: &Value) {
        match self {
            Slide::Count { n, rows } => *n += (*rows || !v.is_null()) as u64,
            _ if v.is_null() => {}
            Slide::Sum { f, i, u, kind, n } => {
                if *kind == 0 {
                    *kind = match v {
                        Value::F32(_) | Value::F64(_) => 1,
                        Value::UInt(_) => 3,
                        _ => 2,
                    };
                }
                match kind {
                    1 => f.add(v.f64().unwrap_or(0.0), false),
                    3 => *u = u.wrapping_add(v.i64().unwrap_or(0) as u64),
                    _ => *i = i.wrapping_add(v.i64().unwrap_or(0)),
                }
                *n += 1;
            }
            Slide::Avg { s, n } => {
                s.add(v.f64().unwrap_or(0.0), false);
                *n += 1;
            }
            Slide::Weighted { xw, w } => {
                if let Some((x, y)) = pair(v) {
                    xw.add(x * y, false);
                    w.add(y, false);
                }
            }
            Slide::Best { odd, .. } if compare(v, v).is_none() => *odd = id,
            Slide::Best { max, q, .. } => {
                let better = if *max { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
                // a candidate worse than the new value can never be the best again
                while q.back().is_some_and(|(_, b)| compare(v, b) == Some(better)) {
                    q.pop_back();
                }
                q.push_back((id, v.clone()));
            }
            Slide::Recompute | Slide::Ends => {}
        }
    }

    /// `v`, the value of row `id`, has left the frame: what `push` added is taken away, exactly.
    fn pop(&mut self, id: u64, v: &Value) {
        match self {
            Slide::Count { n, rows } => *n -= (*rows || !v.is_null()) as u64,
            _ if v.is_null() => {}
            Slide::Sum { f, i, u, kind, n } => {
                match kind {
                    1 => f.add(v.f64().unwrap_or(0.0), true),
                    3 => *u = u.wrapping_sub(v.i64().unwrap_or(0) as u64),
                    _ => *i = i.wrapping_sub(v.i64().unwrap_or(0)),
                }
                *n -= 1;
            }
            Slide::Avg { s, n } => {
                s.add(v.f64().unwrap_or(0.0), true);
                *n -= 1;
            }
            Slide::Weighted { xw, w } => {
                if let Some((x, y)) = pair(v) {
                    xw.add(x * y, true);
                    w.add(y, true);
                }
            }
            Slide::Best { q, .. } => {
                if q.front().is_some_and(|(i, _)| *i == id) {
                    q.pop_front();
                }
            }
            Slide::Recompute | Slide::Ends => {}
        }
    }

    fn result(&self, func: &Func, rows: &VecDeque<(u64, i64, Value)>) -> Value {
        match (self, func) {
            (Slide::Sum { n: 0, .. }, _) | (Slide::Avg { n: 0, .. }, _) => Value::Null,
            (Slide::Sum { f, i, u, kind, .. }, _) => match kind {
                1 => Value::F64(f.value()),
                3 => Value::UInt(*u),
                _ => Value::Int(*i),
            },
            (Slide::Avg { s, n }, _) => Value::F64(s.value() / *n as f64),
            (Slide::Count { n, .. }, _) => Value::UInt(*n),
            (Slide::Weighted { xw, w }, _) => {
                let w = w.value();
                if w == 0.0 {
                    Value::Null
                } else {
                    Value::F64(xw.value() / w)
                }
            }
            (Slide::Best { q, odd, .. }, _) if rows.front().is_some_and(|r| *odd < r.0) => {
                q.front().map_or(Value::Null, |b| b.1.clone())
            }
            (Slide::Ends, Func::Value { first, .. }) => {
                let end = if *first { rows.front() } else { rows.back() };
                end.map_or(Value::Null, |r| r.2.clone())
            }
            (_, Func::Agg { name, params, nargs, .. }) => {
                let mut acc = Acc::new(name, params, *nargs).expect("checked at plan time");
                for (_, _, v) in rows {
                    match (nargs, v) {
                        (2, Value::Array(a)) => acc.add2(a[0].clone(), a[1].clone()),
                        _ => acc.add(v.clone()),
                    }
                }
                acc.result()
            }
            _ => Value::Null,
        }
    }
}

/// The limbs of an `Exact`: every finite Float64 is a whole number of 2^-1074 below 2^2098,
/// and a frame holds at most `MAX_ROWS` < 2^17 of them, so a sum fits in 2116 bits with its
/// sign.
const LIMBS: usize = 34;

/// The exact sum of a multiset of Float64 values, to which values are added and from which they
/// are taken away in any order: the same values give the same state and the same result (their
/// sum rounded once, to nearest even), whatever was added and taken away before. Finite values
/// are kept as one two's complement fixed-point number in units of 2^-1074 (a Kulisch
/// accumulator); infinities and NaNs are counted, and give what a Float64 sum of them gives.
#[derive(Clone, Debug, Default)]
struct Exact {
    /// Little-endian; empty until a finite value other than zero is added.
    limbs: Vec<u64>,
    /// The lowest and highest limbs written: the others are 0, and a result reads only these.
    lo: usize,
    hi: usize,
    nan: u64,
    inf: u64,
    neg_inf: u64,
}

impl Exact {
    /// Adds `x`, or takes it away if `sub`.
    fn add(&mut self, x: f64, sub: bool) {
        if !x.is_finite() {
            let n = if x.is_nan() {
                &mut self.nan
            } else if x == f64::INFINITY {
                &mut self.inf
            } else {
                &mut self.neg_inf
            };
            *n = if sub { *n - 1 } else { *n + 1 };
            return;
        }
        if x == 0.0 {
            return;
        }
        // x = ±m * 2^(p - 1074), m < 2^53, p <= 2045
        let bits = x.to_bits();
        let e = ((bits >> 52) & 0x7ff) as usize;
        let frac = bits & ((1 << 52) - 1);
        let (m, p) = if e == 0 { (frac, 0) } else { (frac + (1 << 52), e - 1) };
        let v = u128::from(m) << (p % 64);
        if self.limbs.is_empty() {
            (self.limbs, self.lo) = (vec![0; LIMBS], LIMBS);
        }
        let l = &mut self.limbs[p / 64..];
        let last = if x.is_sign_negative() != sub {
            carry(l, v, u64::overflowing_sub)
        } else {
            carry(l, v, u64::overflowing_add)
        };
        (self.lo, self.hi) = (self.lo.min(p / 64), self.hi.max(p / 64 + last));
    }

    /// The sum, rounded to nearest even.
    fn value(&self) -> f64 {
        if self.nan > 0 || (self.inf > 0 && self.neg_inf > 0) {
            return f64::NAN;
        } else if self.inf > 0 {
            return f64::INFINITY;
        } else if self.neg_inf > 0 {
            return f64::NEG_INFINITY;
        }
        // the top limb is all sign: 0, or all ones for a negative sum
        let Some(&last) = self.limbs.last() else { return 0.0 };
        if last == 0 {
            return round(&self.limbs[..=self.hi], self.lo);
        }
        // negative: the magnitude is its two's complement (invert, add one: the zeros below
        // `lo` stay zeros and carry the one)
        let mut mag = [0u64; LIMBS];
        let mut carry = true;
        for (m, l) in mag[self.lo..].iter_mut().zip(&self.limbs[self.lo..]) {
            (*m, carry) = (!*l).overflowing_add(u64::from(carry));
        }
        -round(&mag, self.lo)
    }
}

/// Adds (`op` overflowing_add) or takes away (overflowing_sub) `v` at the start of `l`, and
/// carries or borrows up the rest; the last limb written.
#[inline(always)]
fn carry(l: &mut [u64], v: u128, op: impl Fn(u64, u64) -> (u64, bool)) -> usize {
    let (a, c0) = op(l[0], v as u64);
    let (b, c1) = op(l[1], (v >> 64) as u64);
    let (b, c2) = op(b, u64::from(c0));
    (l[0], l[1]) = (a, b);
    let (mut carry, mut last) = (c1 || c2, 1);
    while carry && last + 1 < l.len() {
        last += 1;
        (l[last], carry) = op(l[last], 1);
    }
    last
}

/// `mag`, a whole number of 2^-1074 whose limbs below `lo` are 0, rounded to the nearest
/// Float64 (to even at halfway). An integer to Float64 conversion rounds so.
fn round(mag: &[u64], lo: usize) -> f64 {
    let Some(top) = mag.iter().rposition(|l| *l != 0) else { return 0.0 };
    if top == 0 {
        // below 2^-1010: rounded like any u64, then scaled exactly (the rounded value is still a
        // multiple of the least subnormal)
        return mag[0] as f64 * f64::from_bits(1);
    }
    // the 64 bits from the highest set bit down, the lowest one set if any bit below them is (it
    // is well below the rounding position), rounded like any u64: t * 2^shift
    let shift = top * 64 - mag[top].leading_zeros() as usize;
    let (w, s) = (shift / 64, shift % 64);
    let two = u128::from(mag[w]) + (u128::from(mag.get(w + 1).copied().unwrap_or(0)) << 64);
    let below = mag[..w].iter().skip(lo).any(|l| *l != 0) || mag[w].trailing_zeros() < s as u32;
    let t = ((two >> s) as u64 | u64::from(below)) as f64;
    // t * 2^(shift - 1074), a normal number (t >= 2^63) or past the largest
    let exp = (t.to_bits() >> 52) as usize + shift - 1074;
    if exp >= 0x7ff {
        f64::INFINITY
    } else {
        f64::from_bits(((exp as u64) << 52) + (t.to_bits() & ((1 << 52) - 1)))
    }
}

/// The two arguments of a weighted mean, when neither is NULL.
fn pair(v: &Value) -> Option<(f64, f64)> {
    let Value::Array(a) = v else { return None };
    Some((a.first()?.f64()?, a.get(1)?.f64()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum(xs: &[f64]) -> Exact {
        let mut e = Exact::default();
        xs.iter().for_each(|x| e.add(*x, false));
        e
    }

    /// Sums rounded once, to nearest even, whatever the order: ties, the largest and least
    /// values, infinities and NaNs.
    #[test]
    fn exact_sums_round_once() {
        let p = |e: i32| 2f64.powi(e);
        let cases: [(&[f64], f64); 19] = [
            (&[], 0.0),
            (&[0.1, -0.1], 0.0),
            (&[0.1; 10], 1.0),
            (&[1.0, 1e100, 1.0, -1e100], 2.0),
            // halfway: to the even neighbour, unless anything is below the halfway bit
            (&[1.0, p(-53)], 1.0),
            (&[1.0, p(-53), f64::from_bits(1)], 1.0 + p(-52)),
            (&[1.0 + p(-52), p(-53)], 1.0 + p(-51)),
            (&[-1.0, -p(-53)], -1.0),
            (&[-1.0, -p(-53), -f64::from_bits(1)], -1.0 - p(-52)),
            // what is below the halfway bit: in a limb of its own, and just below the 64 bits
            // rounded (the lowest of which is set)
            (&[1.0, p(-53), p(-1011)], 1.0 + p(-52)),
            (&[1.0, p(-53), p(-100)], 1.0 + p(-52)),
            (&[1.0, p(-53), p(-63), p(-100)], 1.0 + p(-52)),
            // subnormals, and the step from them to normals
            (&[f64::from_bits(1), f64::from_bits(1)], f64::from_bits(2)),
            (&[f64::MIN_POSITIVE, -f64::from_bits(1)], f64::from_bits((1 << 52) - 1)),
            (&[p(-1011), f64::from_bits(1)], p(-1011)),
            // past the largest finite value: up to infinity at halfway (its last bit is odd),
            // not below it
            (&[f64::MAX, p(970)], f64::INFINITY),
            (&[f64::MAX, p(970), -f64::from_bits(1)], f64::MAX),
            (&[-f64::MAX, -f64::MAX], f64::NEG_INFINITY),
            (&[f64::MAX, f64::MAX, -f64::MAX], f64::MAX),
        ];
        for (xs, want) in cases {
            let got = sum(xs).value();
            assert_eq!(got.to_bits(), want.to_bits(), "{xs:?}: {got:e}, not {want:e}");
            let back: Vec<f64> = xs.iter().rev().copied().collect();
            assert_eq!(sum(&back).value().to_bits(), want.to_bits(), "{back:?}");
        }
        // a value taken away leaves zeros below the halfway bit: halfway again, to even
        let mut e = sum(&[1.0, p(-53), p(-1011)]);
        e.add(p(-1011), true);
        assert_eq!(e.value(), 1.0);
        let mut e = sum(&[1.5, f64::INFINITY]);
        assert_eq!(e.value(), f64::INFINITY);
        e.add(f64::NEG_INFINITY, false);
        assert!(e.value().is_nan());
        e.add(f64::INFINITY, true);
        assert_eq!(e.value(), f64::NEG_INFINITY);
        e.add(f64::NAN, false);
        assert!(e.value().is_nan());
        e.add(f64::NEG_INFINITY, true);
        assert!(e.value().is_nan());
        e.add(f64::NAN, true);
        assert_eq!(e.value(), 1.5);
        e.add(1.5, true);
        assert_eq!(e.value().to_bits(), 0);
    }

    /// Random values, the sum of all of them and of those left after every third is taken
    /// away, held to Python's math.fsum (correctly rounded) of the same values: the same
    /// xorshift, with the exponents of prices, of every finite magnitude and of subnormals in
    /// turn.
    #[test]
    fn exact_sums_match_fsum() {
        const FSUM: [(u64, u64); 30] = [
            (0xc0890cc6f6c3a8ff, 0xc092b07cffea7249),
            (0x727df9be97f6523a, 0xe5f953a19b8988ec),
            (0x800587551562a719, 0x8036e73886d810e3),
            (0x402b0d145b91a4d1, 0x407961264b098628),
            (0xfb64c0a534192127, 0xfb64c0a534192127),
            (0x801ede1b374eaa51, 0x803168815093e307),
            (0x4081f00ceb71ac89, 0x405c67b2cf4ffef9),
            (0xfba3e395fd5a21d1, 0xfba3e395fd5a21d1),
            (0x8031aaa610157d1d, 0x803249d315c2b790),
            (0x409a7cc290b0cbb0, 0x40a03a669609b8dd),
            (0x7382db198c52552a, 0x5cdbc11c23aeafb7),
            (0x804ac157ac413073, 0x804e46a644d0a246),
            (0xc0a0cde17bfb3771, 0x401ff5c4b011da42),
            (0x74ac1ca7a5717d1c, 0x74ac1ca7a5717d1c),
            (0x0056eb36c82abc27, 0x0050181a32de2a2d),
            (0xc0a315823f4a714c, 0xc0a49897762bbf7e),
            (0x786c9cbfb6a3f1b5, 0x786c9cbfb6a3f1b5),
            (0x0055f695490a0c02, 0x0056080a5ce41eb8),
            (0xc065d8c4fb87e3ba, 0xc06f73563bdc2eac),
            (0x7853e0d8c430acfb, 0x7853e0d8c430acfb),
            (0x8028f618df43406a, 0x00510c8c90e0e202),
            (0x4080896060ee00e4, 0x4083568fd4f28602),
            (0xf4a52f189a641461, 0xf4a52f189a641461),
            (0x804457cc9cff7363, 0x8024921be9de8276),
            (0x402b797113d2e30e, 0x407f1517d4cd6a80),
            (0x7927c7cc853434cb, 0x7310084110d41465),
            (0x005c970144905ede, 0x006181c5705c993a),
            (0xc0a1965b724241e5, 0xc0940c13b3d70670),
            (0xf67d27d55262fa5d, 0xf3d0b09b82588063),
            (0x002ddffafdd09a5c, 0x00449595e11fb69c),
        ];
        let mut rng = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for (case, (whole, part)) in FSUM.iter().enumerate() {
            let (lo, hi) = [(1013, 1033), (0, 2000), (0, 3)][case % 3];
            let n = 1 + next() % 40;
            let xs: Vec<f64> = (0..n)
                .map(|_| {
                    let e = lo + next() % (hi - lo + 1);
                    let frac = next() & ((1 << 52) - 1);
                    f64::from_bits(((next() & 1) << 63) | (e << 52) | frac)
                })
                .collect();
            let mut e = sum(&xs);
            assert_eq!(e.value().to_bits(), *whole, "case {case}: {xs:?}");
            xs.iter().step_by(3).for_each(|x| e.add(*x, true));
            assert_eq!(e.value().to_bits(), *part, "case {case}, every third taken away");
            let kept: Vec<f64> = xs.iter().enumerate().filter(|(i, _)| i % 3 != 0).map(|(_, x)| *x).collect();
            assert_eq!(sum(&kept).value().to_bits(), *part, "case {case}, the rest added alone");
        }
    }
}
