//! Aggregate functions with ClickHouse semantics, ported from the Proton fork (2505bbba):
//! arrival-order accumulation, NULLs skipped, `VarMoments` (Moments.h), `ReservoirSampler`
//! (ReservoirSampler.h, pcg32_fast seeded 123456) and `QuantileTDigest` (QuantileTDigest.h; merged
//! on brrrrr's own schedule, see `TDigest`, so its results are no longer ClickHouse's bit for bit).
//! `run_structure` and `updownticks` are brrrrr's own (see `Sequence`). The two-argument
//! aggregates (`vwap`, `weighted_avg`, `twap`, `arg_max`, `arg_min`, `corr`) are QuestDB's,
//! with its NULL handling (checked against QuestDB 10.0.1) and arrival order for time order.
//! `skewness`, `kurtosis`, `kurtosis_pop`, `variance` and `stddev` have DuckDB's (1.5.5)
//! definitions (the last two are its `var_samp` and `stddev_samp`: NULL, not ClickHouse's NaN, for
//! one row) and NULL/NaN rules, computed from sums of powers of each row's distance from the
//! window's first row instead of DuckDB's (and ClickHouse's) raw power sums, which cancel
//! catastrophically on prices. `trade_returns` and `distinct_stats` are brrrrr's own ordered
//! aggregates for metrics a batch engine computes with LAG over a sorted window (see `Sequence`). `quantile_cont` is
//! DuckDB's `PERCENTILE_CONT` (`MEDIAN` at 0.5), exact for small windows (see `Cont`);
//! `quantile_exact` is the same, exact for any number of values (ClickHouse's name), and
//! `uniq_exact` the number of distinct values (ClickHouse's `uniqExact`, SQL's `count(DISTINCT x)`).
use crate::value::Value;

pub(crate) mod kernel;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// `<aggregate>_if(args.., cond)`, ClickHouse's -If combinator: the aggregate over the rows
/// whose last argument is true. For the aggregates of at most one argument.
pub fn if_base(name: &str) -> Option<&str> {
    name.strip_suffix("_if").filter(|b| is_aggregate(b) && arity(b) == 1)
}

/// The most `_if` combinators an aggregate takes (`sum_if_if(x, a, b)` has two): a checkpoint
/// nests them no deeper than `checkpoint::MAX_NESTING`, and a plan must write what it reads.
pub const MAX_IF: usize = 8;

pub fn is_aggregate(name: &str) -> bool {
    if if_base(name).is_some() {
        return true;
    }
    const ALL: [&str; 34] = [
        "latest",
        "earliest",
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "stddev_samp",
        "var_samp",
        "skew_samp",
        "kurt_samp",
        "quantile",
        "median",
        "quantile_t_digest",
        "median_tdigest",
        "run_structure",
        "updownticks",
        "vwap",
        "weighted_avg",
        "twap",
        "arg_max",
        "arg_min",
        "corr",
        "skewness",
        "kurtosis",
        "kurtosis_pop",
        "variance",
        "stddev",
        "trade_returns",
        "distinct_stats",
        "quantile_cont",
        "trade_reversal",
        "quantile_exact",
        "uniq_exact",
    ];
    ALL.contains(&name)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Moment {
    VarSamp,
    StddevSamp,
    SkewSamp,
    KurtSamp,
    /// DuckDB's `skewness`: the adjusted Fisher–Pearson G1.
    Skewness,
    /// DuckDB's `kurtosis`: the adjusted excess kurtosis G2.
    Kurtosis,
    /// DuckDB's `kurtosis_pop`: the population excess kurtosis.
    KurtosisPop,
    /// DuckDB's `var_samp` (`variance`): NULL for n <= 1.
    Variance,
    /// DuckDB's `stddev_samp` (`stddev`): NULL for n <= 1.
    Stddev,
}

/// One accumulator. Every group holds a `Vec<Acc>`, so the rare aggregates with a large state
/// keep it behind a `Box`, and the common ones (a `Value`, or two numbers) set the size.
/// The enum is the checkpointed state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Acc {
    Latest(Value),
    Earliest(Value),
    Sum(Value),
    Avg {
        sum: f64,
        n: u64,
    },
    Min(Value),
    Max(Value),
    Count {
        n: u64,
        rows: bool,
    },
    Moments {
        m: Box<[f64; 5]>,
        kind: Moment,
        f32: bool,
        seen: bool,
    },
    Quantile {
        level: f64,
        sampler: Box<Reservoir>,
    },
    TDigest {
        level: f64,
        digest: Box<TDigest>,
    },
    RunStructure(Box<Sequence<i64, Runs>>),
    UpDownTicks(Box<Sequence<Arc<str>, Ticks>>),
    /// `vwap(price, amount)`, `weighted_avg(x, weight)`: sum(x * w) / sum(w).
    Weighted {
        xw: f64,
        w: f64,
    },
    Twap(Box<Twap>),
    /// `arg_max(x, key)`, `arg_min(x, key)`: `x` of the first row with the largest (smallest) key.
    Arg {
        max: bool,
        key_x: Box<(Value, Value)>,
    },
    Corr(Box<Corr>),
    /// `skewness`, `kurtosis`, `kurtosis_pop`: n, the first row's value, and the sums of the
    /// 1st to 4th powers of each row's distance from it.
    ShiftedMoments {
        n: u64,
        shift: f64,
        s: Box<[f64; 4]>,
        kind: Moment,
    },
    /// `<aggregate>_if(.., cond)`: the inner aggregate over the rows whose `cond` is true.
    If(#[serde(deserialize_with = "crate::checkpoint::nested")] Box<Acc>),
    /// A quantile of an argument that the group's accumulator `of` already samples (slippage's
    /// `quantile(0.95)(x)` beside its `quantile(0.5)(x)`): `of`'s sampler read at `level`
    /// (`results`). Samplers with one seed fed the same values hold the same samples, so this is
    /// what a sampler of its own would give, without a second copy of the samples.
    QuantileOf {
        level: f64,
        of: usize,
    },
    TradeReturns(Box<Sequence<(Tie, Tie), Returns>>),
    DistinctStats(Box<Sequence<Tie, Distinct>>),
    /// `quantile_cont(level)(x)`: DuckDB's `PERCENTILE_CONT` (see `Cont`).
    Cont {
        level: f64,
        values: Box<Cont>,
    },
    /// `trade_reversal(k, level)((time, tie1, tie2, price, side, size))` (see `Reversal`).
    TradeReversal(Box<Sequence<(Tie, Tie), Reversal>>),
    /// `uniq_exact(x)`: the distinct values, by their text (`format::to_text`), in order.
    Uniq(Box<std::collections::BTreeSet<String>>),
}

// A Value and a tag: l2 alone holds 140k accumulators.
const _: () = assert!(std::mem::size_of::<Acc>() == 32);

/// `twap(price, time)`: each price holds until the next row's time; `last` is the previous
/// (price, time), and `sum`/`n` the plain mean when no time passes at all.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Twap {
    pub(crate) xt: f64,
    pub(crate) t: f64,
    pub(crate) last: Option<(f64, i64)>,
    pub(crate) sum: f64,
    pub(crate) n: u64,
}

/// `corr(x, y)`: Welford's running means and co-moments.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Corr {
    n: f64,
    mx: f64,
    my: f64,
    sxx: f64,
    syy: f64,
    sxy: f64,
}

/// The sequence aggregates' one argument, a tuple the fold reads by position: each item's name
/// and whether it must be a number (or a time).
pub fn tuple_items(name: &str) -> Option<&'static [(&'static str, bool)]> {
    Some(match if_base(name).unwrap_or(name) {
        "run_structure" => &[("time", true), ("id", false), ("side", false), ("price", true)],
        "updownticks" => &[("time", true), ("id", false), ("price", true), ("size", true)],
        "trade_returns" | "trade_reversal" => {
            &[("time", true), ("tie1", false), ("tie2", false), ("price", true), ("side", false), ("size", true)]
        }
        "distinct_stats" => &[("time", true), ("tie", false), ("x", true)],
        _ => return None,
    })
}

/// The aggregates of two arguments.
pub fn arity(name: &str) -> usize {
    match name {
        "vwap" | "weighted_avg" | "twap" | "arg_max" | "arg_min" | "corr" => 2,
        _ => 1,
    }
}

impl Acc {
    pub fn new(name: &str, params: &[Value], nargs: usize) -> Result<Acc, String> {
        if let Some(base) = if_base(name) {
            if std::iter::successors(Some(base), |b| if_base(b)).count() > MAX_IF {
                return Err(format!("{name}: more than {MAX_IF} _if combinators"));
            }
            return match nargs {
                0 => Err(format!("{name} needs a condition")),
                n => Ok(Acc::If(Box::new(Acc::new(base, params, n - 1)?))),
            };
        }
        if name == "trade_reversal" {
            // (trades ahead, quantile level)
            let (Some(k), Some(level)) = (params.first().and_then(Value::i64), params.get(1).and_then(Value::f64))
            else {
                return Err(format!("{name} takes (trades ahead, level), got {params:?}"));
            };
            if params.len() != 2 || k < 1 || !(0.0..=1.0).contains(&level) || nargs != 1 {
                return Err(format!("{name}(k >= 1, level in [0, 1]) of one tuple, got {params:?}"));
            }
            let summary = Reversal { k: k as u64, level, trades: vec![] };
            return Ok(Acc::TradeReversal(Box::new(Sequence { summary, ..Sequence::default() })));
        }
        let leveled = matches!(
            name,
            "quantile" | "median" | "quantile_t_digest" | "median_tdigest" | "quantile_cont" | "quantile_exact"
        );
        let level = match params {
            [] => 0.5,
            [p] if leveled && !matches!(p, Value::Str(_)) => {
                p.f64().ok_or_else(|| format!("{name} level {p:?} is not a number"))?
            }
            _ if leveled => return Err(format!("{name} takes one numeric level, got {params:?}")),
            _ => return Err(format!("{name} takes no parameters")),
        };
        if !(0.0..=1.0).contains(&level) {
            return Err(format!("{name} level {level} is outside [0, 1]"));
        }
        let want = if name == "count" { nargs <= 1 } else { nargs == arity(name) };
        if !want {
            let n = arity(name);
            return Err(format!("{name} expects {n} argument{}, got {nargs}", if n == 1 { "" } else { "s" }));
        }
        Ok(match name {
            "latest" => Acc::Latest(Value::Null),
            "earliest" => Acc::Earliest(Value::Null),
            "sum" => Acc::Sum(Value::Null),
            "avg" => Acc::Avg { sum: 0.0, n: 0 },
            "min" => Acc::Min(Value::Null),
            "max" => Acc::Max(Value::Null),
            "count" => Acc::Count { n: 0, rows: nargs == 0 },
            "var_samp" | "stddev_samp" | "skew_samp" | "kurt_samp" => {
                let kind = match name {
                    "var_samp" => Moment::VarSamp,
                    "stddev_samp" => Moment::StddevSamp,
                    "skew_samp" => Moment::SkewSamp,
                    _ => Moment::KurtSamp,
                };
                Acc::Moments { m: Box::new([0.0; 5]), kind, f32: false, seen: false }
            }
            "quantile" | "median" => Acc::Quantile { level, sampler: Box::new(Reservoir::new()) },
            "quantile_t_digest" | "median_tdigest" => Acc::TDigest { level, digest: Box::default() },
            "quantile_cont" => Acc::Cont { level, values: Box::default() },
            "quantile_exact" => Acc::Cont { level, values: Box::new(Cont::All(vec![])) },
            "uniq_exact" => Acc::Uniq(Box::default()),
            "run_structure" => Acc::RunStructure(Box::default()),
            "trade_returns" => Acc::TradeReturns(Box::default()),
            "distinct_stats" => Acc::DistinctStats(Box::default()),
            "updownticks" => Acc::UpDownTicks(Box::default()),
            "vwap" | "weighted_avg" => Acc::Weighted { xw: 0.0, w: 0.0 },
            "twap" => Acc::Twap(Box::new(Twap { xt: 0.0, t: 0.0, last: None, sum: 0.0, n: 0 })),
            "arg_max" | "arg_min" => Acc::Arg { max: name == "arg_max", key_x: Box::new((Value::Null, Value::Null)) },
            "corr" => Acc::Corr(Box::new(Corr { n: 0.0, mx: 0.0, my: 0.0, sxx: 0.0, syy: 0.0, sxy: 0.0 })),
            "skewness" | "kurtosis" | "kurtosis_pop" | "variance" | "stddev" => {
                let kind = match name {
                    "skewness" => Moment::Skewness,
                    "kurtosis" => Moment::Kurtosis,
                    "variance" => Moment::Variance,
                    "stddev" => Moment::Stddev,
                    _ => Moment::KurtosisPop,
                };
                Acc::ShiftedMoments { n: 0, shift: 0.0, s: Box::new([0.0; 4]), kind }
            }
            other => return Err(format!("unknown aggregate {other}")),
        })
    }

    /// Whether `merge` takes this aggregate's state: a window built from its narrower windows'
    /// (`BWindow::apply_many`, ADR-0017).
    pub(crate) fn mergeable(&self) -> bool {
        match self {
            Acc::Latest(_)
            | Acc::Earliest(_)
            | Acc::Sum(_)
            | Acc::Avg { .. }
            | Acc::Min(_)
            | Acc::Max(_)
            | Acc::Count { .. }
            | Acc::Weighted { .. }
            | Acc::TradeReturns(_) => true,
            Acc::If(inner) => inner.mergeable(),
            _ => false,
        }
    }

    /// Whether `merge` takes this aggregate's state of other rows, whatever their order: of
    /// another part of a run's rows (other keys, at any time), when a global aggregate is split by
    /// a key (`Historical::hold`). A float's sum adds the parts' sums (ADR-0017), and of rows of
    /// equal keys an `arg_min`/`arg_max` keeps the earlier part's.
    pub(crate) fn mergeable_across(&self) -> bool {
        match self {
            Acc::Sum(_)
            | Acc::Avg { .. }
            | Acc::Min(_)
            | Acc::Max(_)
            | Acc::Count { .. }
            | Acc::Weighted { .. }
            | Acc::Arg { .. }
            | Acc::Uniq(_) => true,
            Acc::Cont { values, .. } => matches!(**values, Cont::All(_)),
            Acc::If(inner) => inner.mergeable_across(),
            _ => false,
        }
    }

    /// `fresh`, a new state of this aggregate, to fold the rows after this state's in, to be
    /// merged into it: of a sequence of trades, its return on this state's last trade's price.
    pub(crate) fn seeded(&self, fresh: &Acc) -> Acc {
        match (self, fresh) {
            (Acc::TradeReturns(x), Acc::TradeReturns(y)) => {
                let mut y = y.clone();
                y.summary.last = x.pending.back().map(|(_, (_, price, _))| *price).or(x.summary.last);
                Acc::TradeReturns(y)
            }
            _ => fresh.clone(),
        }
    }

    /// A sequence of trades: whether it holds one, and the latest one's time, which a run merged
    /// into it must start after (its trades of one time are put in order together).
    pub(crate) fn trades(&self) -> Option<(bool, i64)> {
        match self {
            Acc::TradeReturns(q) => Some((q.folded.is_some() || !q.pending.is_empty(), q.newest)),
            _ => None,
        }
    }

    /// `later`, the state of the rows that come after this state's, folded in: the state of
    /// both runs of rows as `add` would make it, but for a float's sum (`sum`, `avg`,
    /// `weighted_avg`), which adds the two runs' sums rather than every row in turn (ADR-0017). A
    /// `min` or `max` is exact where `later` saw no NaN: one that comes first holds the fold
    /// (`add`), and `later` no longer says what came after it.
    pub(crate) fn merge(&mut self, later: &Acc) {
        match (self, later) {
            (Acc::Latest(x), Acc::Latest(y)) if !y.is_null() => *x = y.clone(),
            (Acc::Earliest(x), Acc::Earliest(y)) if x.is_null() => *x = y.clone(),
            (Acc::Sum(x), Acc::Sum(y)) if !y.is_null() => *x = sum(x, y),
            (Acc::Avg { sum, n }, Acc::Avg { sum: s, n: m }) => (*sum, *n) = (*sum + s, *n + m),
            (me @ (Acc::Min(_) | Acc::Max(_)), Acc::Min(y) | Acc::Max(y)) => me.add(y.clone()),
            (Acc::Count { n, .. }, Acc::Count { n: m, .. }) => *n += m,
            (Acc::Weighted { xw, w }, Acc::Weighted { xw: a, w: b }) => (*xw, *w) = (*xw + a, *w + b),
            (Acc::If(x), Acc::If(y)) => x.merge(y),
            (Acc::TradeReturns(x), Acc::TradeReturns(y)) => x.merge(y),
            (Acc::Arg { max, key_x }, Acc::Arg { key_x: other, .. }) => {
                let better = if *max { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
                if !other.0.is_null() && (key_x.0.is_null() || crate::expr::compare(&other.0, &key_x.0) == Some(better))
                {
                    *key_x = other.clone();
                }
            }
            (Acc::Uniq(x), Acc::Uniq(y)) => x.extend(y.iter().cloned()),
            (Acc::Cont { values: x, .. }, Acc::Cont { values: y, .. }) => {
                if let (Cont::All(a), Cont::All(b)) = (&mut **x, &**y) {
                    a.extend_from_slice(b);
                }
            }
            (Acc::Latest(_) | Acc::Earliest(_) | Acc::Sum(_), _) => {}
            (me, _) => unreachable!("a merge of {me:?}, which `mergeable` refuses"),
        }
    }

    /// Adds one row's argument (NULL for `count()`, which has none). NULLs are skipped.
    /// Inlined into each caller's loop even with several callers (windows and window functions):
    /// left to itself, the compiler stops inlining it once it has two, and every example
    /// pipeline runs 2-10% more instructions.
    #[inline(always)]
    pub fn add(&mut self, v: Value) {
        if v.is_null() {
            if let Acc::Count { n, rows: true } = self {
                *n += 1;
            }
            return;
        }
        match self {
            // `count_if(cond)`: the only argument is the condition
            Acc::If(inner) => if_add(inner, Value::Null, &v),
            Acc::Latest(x) => *x = v,
            Acc::Earliest(x) if x.is_null() => *x = v,
            Acc::Earliest(_) => {}
            Acc::Sum(s) => *s = sum(s, &v),
            Acc::Avg { sum, n } => {
                *sum += v.f64().unwrap_or(0.0);
                *n += 1;
            }
            Acc::Min(x) => {
                if x.is_null() || crate::expr::compare(&v, x) == Some(std::cmp::Ordering::Less) {
                    *x = v
                }
            }
            Acc::Max(x) => {
                if x.is_null() || crate::expr::compare(&v, x) == Some(std::cmp::Ordering::Greater) {
                    *x = v
                }
            }
            Acc::Count { n, .. } => *n += 1,
            Acc::Moments { m, f32, seen, .. } => {
                if !*seen {
                    (*seen, *f32) = (true, matches!(v, Value::F32(_)));
                }
                let x = v.f64().unwrap_or(0.0);
                if *f32 {
                    // VarMoments<Float32>: the accumulation itself is Float32
                    let x = x as f32;
                    let mut a = m.map(|y| y as f32);
                    a[0] += 1.0;
                    a[1] += x;
                    a[2] += x * x;
                    a[3] += x * x * x;
                    a[4] += x * x * x * x;
                    **m = a.map(|y| y as f64);
                } else {
                    m[0] += 1.0;
                    m[1] += x;
                    m[2] += x * x;
                    m[3] += x * x * x;
                    m[4] += x * x * x * x;
                }
            }
            Acc::Quantile { sampler, .. } => sampler.insert(v.f64().unwrap_or(0.0)),
            Acc::TDigest { digest, .. } => digest.add(v.f64().unwrap_or(0.0) as f32),
            Acc::Cont { values, .. } => values.add(v.f64().unwrap_or(0.0)),
            Acc::RunStructure(q) => add_run(q, v),
            Acc::UpDownTicks(q) => add_tick(q, v),
            Acc::TradeReturns(q) => add_return(q, v),
            Acc::TradeReversal(q) => add_reversal(q, v),
            Acc::DistinctStats(q) => add_distinct(q, v),
            Acc::Uniq(set) => {
                set.insert(crate::format::to_text(&v));
            }
            Acc::ShiftedMoments { n, shift, s, .. } => {
                // powers of the distance from the first row, not of x: a window's rows sit near
                // each other, so the sums keep the digits that sums of raw powers cancel away
                let x = v.f64().unwrap_or(0.0);
                if *n == 0 {
                    *shift = x;
                }
                let d = x - *shift;
                let d2 = d * d;
                *n += 1;
                s[0] += d;
                s[1] += d2;
                s[2] += d2 * d;
                s[3] += d2 * d2;
            }
            // two-argument aggregates are fed by add2; `of` takes the values of a quantile of another
            Acc::Weighted { .. } | Acc::Twap { .. } | Acc::Arg { .. } | Acc::Corr { .. } | Acc::QuantileOf { .. } => {}
        }
    }

    /// Adds one row's two arguments. A row with a NULL argument is skipped, except that
    /// `arg_max`/`arg_min` keep a NULL `x` (only a NULL key skips the row), as QuestDB does.
    #[inline(never)]
    pub fn add2(&mut self, a: Value, b: Value) {
        if b.is_null() || (a.is_null() && !matches!(self, Acc::Arg { .. })) {
            return;
        }
        match self {
            // `latest_if(x, cond)`: NULLs of `x` are skipped as the inner aggregate skips them
            Acc::If(inner) => if_add(inner, a, &b),
            Acc::Weighted { xw, w } => {
                let (x, y) = (a.f64().unwrap_or(0.0), b.f64().unwrap_or(0.0));
                *xw += x * y;
                *w += y;
            }
            Acc::Twap(tw) => {
                let Twap { xt, t, last, sum, n } = &mut **tw;
                let (x, mut now) = (a.f64().unwrap_or(0.0), b.i64().unwrap_or(0));
                if let Some((p, prev)) = *last {
                    // a row earlier than the previous one takes effect at the previous time
                    now = now.max(prev);
                    // unsigned: the span between extreme times does not fit an i64
                    let d = now.abs_diff(prev) as f64;
                    (*xt, *t) = (*xt + p * d, *t + d);
                }
                *last = Some((x, now));
                (*sum, *n) = (*sum + x, *n + 1);
            }
            Acc::Arg { max, key_x } => {
                let (key, x) = &mut **key_x;
                let better = if *max { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
                if key.is_null() || crate::expr::compare(&b, key) == Some(better) {
                    (*key, *x) = (b, a);
                }
            }
            Acc::Corr(c) => {
                let Corr { n, mx, my, sxx, syy, sxy } = &mut **c;
                let (x, y) = (a.f64().unwrap_or(0.0), b.f64().unwrap_or(0.0));
                *n += 1.0;
                let (dx, dy) = (x - *mx, y - *my);
                *mx += dx / *n;
                *my += dy / *n;
                *sxx += dx * (x - *mx);
                *syy += dy * (y - *my);
                *sxy += dx * (y - *my);
            }
            _ => {}
        }
    }

    /// Checks a restored accumulator against `plan`, a fresh one of the aggregate it must hold:
    /// the same function with the same parameters, and internally consistent (a corrupted one
    /// could panic when it is next added to or read).
    pub fn check_against(&self, plan: &Acc) -> Result<(), String> {
        let same = match (self, plan) {
            (Acc::Count { rows: a, .. }, Acc::Count { rows: b, .. }) => a == b,
            (Acc::Moments { kind: a, .. }, Acc::Moments { kind: b, .. }) => a == b,
            (Acc::Quantile { level: a, sampler }, Acc::Quantile { level: b, .. }) => {
                a.to_bits() == b.to_bits() && sampler.consistent()
            }
            (Acc::TDigest { level: a, digest }, Acc::TDigest { level: b, .. }) => {
                a.to_bits() == b.to_bits() && digest.consistent()
            }
            (Acc::Cont { level: a, values }, Acc::Cont { level: b, values: p }) => {
                // `quantile_exact`'s values where the plan has `quantile_cont`'s, or the reverse
                let all = |c: &Cont| matches!(c, Cont::All(_));
                a.to_bits() == b.to_bits() && values.consistent() && all(values) == all(p)
            }
            (Acc::RunStructure(q), Acc::RunStructure(_)) => q.consistent(),
            (Acc::UpDownTicks(q), Acc::UpDownTicks(_)) => q.consistent(),
            (Acc::TradeReturns(q), Acc::TradeReturns(_)) => q.consistent(),
            (Acc::TradeReversal(q), Acc::TradeReversal(r)) => {
                q.consistent() && q.summary.k == r.summary.k && q.summary.level.to_bits() == r.summary.level.to_bits()
            }
            (Acc::DistinctStats(q), Acc::DistinctStats(_)) => q.consistent(),
            (Acc::Arg { max: a, .. }, Acc::Arg { max: b, .. }) => a == b,
            (Acc::ShiftedMoments { kind: a, .. }, Acc::ShiftedMoments { kind: b, .. }) => a == b,
            (Acc::If(a), Acc::If(b)) => a.check_against(b).is_ok(),
            (Acc::QuantileOf { level: a, of: x }, Acc::QuantileOf { level: b, of: y }) => {
                a.to_bits() == b.to_bits() && x == y
            }
            (a, b) => std::mem::discriminant(a) == std::mem::discriminant(b),
        };
        if same {
            Ok(())
        } else {
            Err(format!("an accumulator {self:?} where the plan has {plan:?}"))
        }
    }

    pub fn result(&mut self) -> Value {
        match self {
            Acc::If(inner) => inner.result(),
            Acc::Latest(x) | Acc::Earliest(x) | Acc::Sum(x) | Acc::Min(x) | Acc::Max(x) => x.clone(),
            // aggregates over nothing but NULLs are NULL (the -Null combinator)
            Acc::Avg { n: 0, .. } | Acc::Moments { seen: false, .. } => Value::Null,
            Acc::Avg { sum, n } => Value::F64(*sum / *n as f64),
            Acc::Count { n, .. } => Value::UInt(*n),
            Acc::Uniq(set) => Value::UInt(set.len() as u64),
            Acc::Moments { m, kind, f32, .. } => moments(m, *kind, *f32),
            // Float64 for every input type; the digest's centroids make it Float32 for every input
            Acc::Quantile { level, .. } | Acc::TDigest { level, .. } | Acc::Cont { level, .. } => {
                let level = *level;
                self.result_at(level)
            }
            Acc::RunStructure(q) => {
                let r = q.finish();
                let mean = |i: usize| if r.runs[i] == 0 { 0.0 } else { r.len[i] as f64 / r.runs[i] as f64 };
                let change = if r.changes == 0 { Value::Null } else { Value::F64(r.change / r.changes as f64) };
                let f = |x: u64| Value::F64(x as f64);
                Value::Array(
                    [
                        f(r.max[1]),
                        f(r.max[0]),
                        Value::F64(mean(1)),
                        Value::F64(mean(0)),
                        f(r.flips),
                        f(r.trades),
                        change,
                    ]
                    .into(),
                )
            }
            Acc::UpDownTicks(q) => {
                let r = q.finish();
                let n = |x: u64| Value::Int(x as i64);
                Value::Array(
                    [n(r.n[0]), n(r.n[1]), n(r.n[2]), Value::F64(r.vol[0]), Value::F64(r.vol[1]), Value::F64(r.vol[2])]
                        .into(),
                )
            }
            Acc::TradeReturns(q) => q.finish().result(),
            Acc::TradeReversal(q) => q.finish().result(),
            Acc::DistinctStats(q) => {
                // [mean and stddev_samp of the distinct values, stddev_samp of the changes]
                let r = q.finish();
                Value::Array([r.values.mean(), r.values.stddev(), r.changes.stddev()].into())
            }
            // no weight (no rows, or weights summing to zero) is NULL, not a division by zero
            Acc::Weighted { w, .. } if *w == 0.0 => Value::Null,
            Acc::Weighted { xw, w } => Value::F64(*xw / *w),
            Acc::Twap(tw) if tw.n == 0 => Value::Null,
            Acc::Twap(tw) => Value::F64(if tw.t > 0.0 { tw.xt / tw.t } else { tw.sum / tw.n as f64 }),
            Acc::Arg { key_x, .. } => key_x.1.clone(),
            // fewer than two pairs, or a constant side: no correlation
            Acc::Corr(c) if c.n >= 2.0 && c.sxx > 0.0 && c.syy > 0.0 => Value::F64(c.sxy / (c.sxx * c.syy).sqrt()),
            Acc::Corr { .. } => Value::Null,
            Acc::ShiftedMoments { n, s, kind, .. } => central_moment(*n, s, *kind),
            Acc::QuantileOf { .. } => unreachable!("read through agg::results"),
        }
    }

    /// Merges a t-digest's unmerged values into its centroids ahead of its window's close (a
    /// window's lead, `engine::LEAD`), if it has merged before: a digest under `BUFFER` values is
    /// left to merge at its read, so that it still reads as ClickHouse's.
    pub fn premerge(&mut self) {
        match self {
            Acc::TDigest { digest, .. } => digest.premerge(),
            Acc::If(inner) => inner.premerge(),
            _ => {}
        }
    }

    /// A t-digest's values not merged into its centroids yet (also under `_if`).
    #[cfg(test)]
    pub(crate) fn unmerged(&self) -> usize {
        match self {
            Acc::TDigest { digest, .. } => digest.unmerged.len(),
            Acc::If(inner) => inner.unmerged(),
            _ => 0,
        }
    }

    /// Whether this is a t-digest, or one under `_if` (`premerge`).
    pub fn holds_digest(&self) -> bool {
        match self {
            Acc::TDigest { .. } => true,
            Acc::If(inner) => inner.holds_digest(),
            _ => false,
        }
    }

    /// A quantile or t-digest's sampler read at `level`.
    fn result_at(&mut self, level: f64) -> Value {
        match self {
            Acc::Cont { values, .. } => values.quantile(level).map_or(Value::Null, Value::F64),
            Acc::Quantile { sampler, .. } if sampler.samples.is_empty() => Value::Null,
            Acc::TDigest { digest, .. } if digest.is_empty() => Value::Null,
            Acc::Quantile { sampler, .. } => Value::F64(sampler.quantile(level)),
            Acc::TDigest { digest, .. } => Value::F32(digest.quantile(level)),
            other => unreachable!("a quantile of {other:?}"),
        }
    }
}

/// `run_structure((time, id, side, price))`: the id orders as COALESCE(TRY_CAST(id AS BIGINT), 0).
/// Out of line, so `Acc::add` stays small enough to inline into the engine's loop.
#[inline(never)]
fn add_run(q: &mut Sequence<i64, Runs>, v: Value) {
    // the width is checked at plan time (`tuple_items`); a row of another is skipped, not a panic
    let Value::Array(a) = v else { return };
    let [t, id, side, price] = &a[..] else { return };
    let (Some(t), Some(price)) = (t.i64(), price.f64()) else { return };
    let id = match id {
        Value::Str(s) => s.parse().unwrap_or(0),
        x => x.i64().unwrap_or(0),
    };
    match side.str() {
        Some("buy") => q.add((t, id), (true, price)),
        Some("sell") => q.add((t, id), (false, price)),
        _ => {}
    }
}

/// `updownticks((time, id, price, size))`: the id orders as a string.
#[inline(never)]
fn add_tick(q: &mut Sequence<Arc<str>, Ticks>, v: Value) {
    let Value::Array(a) = v else { return };
    let [t, id, price, size] = &a[..] else { return };
    let (Some(t), Some(price), Some(size)) = (t.i64(), price.f64(), size.f64()) else { return };
    let id = match id {
        Value::Str(s) => s.clone(),
        x => crate::format::to_text(x).into(),
    };
    q.add((t, id), (price, size));
}

/// A tie-break of a sequence aggregate's order after its time: a trade id as text or as a
/// number, or a second time. Mixed kinds order numbers first; a query passes one kind.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Tie {
    #[default]
    None,
    Int(i64),
    Str(Arc<str>),
}

fn tie(v: &Value) -> Tie {
    match v {
        Value::Null => Tie::None,
        Value::Str(s) => Tie::Str(s.clone()),
        v => v.i64().map_or(Tie::None, Tie::Int),
    }
}

/// `trade_returns((time, tie1, tie2, price, side, size))`, in `(time, tie1, tie2)` order; rows
/// whose side is neither buy nor sell are skipped.
#[inline(never)]
fn add_return(q: &mut Sequence<(Tie, Tie), Returns>, v: Value) {
    let Value::Array(a) = v else { return };
    let [t, t1, t2, price, side, size] = &a[..] else { return };
    let (Some(t), Some(price), Some(size)) = (t.i64(), price.f64(), size.f64()) else { return };
    let buy = match side.str() {
        Some("buy") => true,
        Some("sell") => false,
        _ => return,
    };
    q.add((t, (tie(t1), tie(t2))), (buy, price, size));
}

/// `trade_reversal`'s trades: a buy's or a sell's price and size, in `(time, tie1, tie2)` order.
#[inline(never)]
fn add_reversal(q: &mut Sequence<(Tie, Tie), Reversal>, v: Value) {
    let Value::Array(a) = v else { return };
    let [t, t1, t2, price, side, size] = &a[..] else { return };
    let (Some(t), Some(price), Some(size)) = (t.i64(), price.f64(), size.f64()) else { return };
    if !matches!(side.str(), Some("buy" | "sell")) {
        return;
    }
    q.add((t, (tie(t1), tie(t2))), (price, size));
}

/// `distinct_stats((time, tie, x))`, in `(time, tie)` order; `x` may be NULL.
#[inline(never)]
fn add_distinct(q: &mut Sequence<Tie, Distinct>, v: Value) {
    let Value::Array(a) = v else { return };
    let [t, t1, x] = &a[..] else { return };
    let Some(t) = t.i64() else { return };
    q.add((t, tie(t1)), x.f64());
}

/// `Acc::If`: `v` into the inner aggregate if `cond` is true. Out of line: called from
/// `Acc::add`, a recursive `add` would no longer inline into the window loop (+6.6%
/// instructions on the gate's range pipeline).
#[inline(never)]
fn if_add(inner: &mut Acc, v: Value, cond: &Value) {
    if crate::expr::truth(cond) == Some(true) {
        inner.add(v);
    }
}

#[inline(always)] // part of Acc::add (see there)
fn sum(acc: &Value, v: &Value) -> Value {
    match (acc, v) {
        // the sum starts at +0.0, so a lone -0.0 sums to 0 (as in ClickHouse)
        (Value::Null, Value::F32(f)) => Value::F64(0.0 + *f as f64),
        (Value::Null, Value::F64(f)) => Value::F64(0.0 + *f),
        (Value::Null, Value::UInt(u)) => Value::UInt(*u),
        (Value::Null, v) => Value::Int(v.i64().unwrap_or(0)),
        (Value::UInt(a), Value::UInt(b)) => Value::UInt(a.wrapping_add(*b)),
        (Value::F64(a), v) => Value::F64(a + v.f64().unwrap_or(0.0)),
        (a, v) if matches!(v, Value::F32(_) | Value::F64(_)) => {
            Value::F64(a.f64().unwrap_or(0.0) + v.f64().unwrap_or(0.0))
        }
        (a, v) => Value::Int(a.i64().unwrap_or(0).wrapping_add(v.i64().unwrap_or(0))),
    }
}

/// AggregateFunctionStatisticsSimple.h finalisation, in the accumulation type `$t` (Float32 for
/// Float32 input); only `pow` runs in Float64, as C++ `pow(float, double)` does, and it is the
/// fork's musl `pow` (crate::pow), not the host's.
macro_rules! moments {
    ($m:expr, $kind:expr, $t:ty) => {{
        let m: [$t; 5] = $m.map(|y| y as $t);
        let sample = if m[0] <= 1.0 { <$t>::NAN } else { ((m[2] - m[1] * m[1] / m[0]) / (m[0] - 1.0)).max(0.0) };
        // getMoment3/4's `m[0] == 1` shortcut is unreachable here: sample > 0 needs two values
        let m3 = || (m[3] - (3.0 * m[2] - 2.0 * m[1] * m[1] / m[0]) * m[1] / m[0]) / m[0];
        let m4 = || (m[4] - (4.0 * m[3] - (6.0 * m[2] - 3.0 * m[1] * m[1] / m[0]) * m[1] / m[0]) * m[1] / m[0]) / m[0];
        match $kind {
            Moment::VarSamp => sample,
            Moment::StddevSamp => sample.sqrt(),
            Moment::SkewSamp if sample > 0.0 => (m3() as f64 / crate::pow::pow(sample as f64, 1.5)) as $t,
            // the fork's compiler turns pow(var, 2) into var * var; pow(var, 1.5) stays a musl call
            Moment::KurtSamp if sample > 0.0 => (m4() as f64 / (sample as f64 * sample as f64)) as $t,
            _ => <$t>::NAN,
        }
    }};
}

/// DuckDB's definitions (SkewnessOperation and KurtosisOperation) on the central moments of
/// the sums of powers of the distances `d` from the first row:
/// - skewness, the adjusted Fisher–Pearson G1: NULL for n <= 2, NaN with no variance;
/// - kurtosis, the adjusted excess G2: NULL for n <= 3 or no variance;
/// - kurtosis_pop, the population excess m4 / m2^2 - 3: NULL for n <= 1 or no variance.
///
/// Where DuckDB raises "out of range" (a non-finite result), a stream cannot fail, so the
/// result is NULL.
fn central_moment(count: u64, s: &[f64; 4], kind: Moment) -> Value {
    let n = count as f64;
    // central moments from the moments of d about the first row: d has mean `a`
    let (a, e2, e3, e4) = (s[0] / n, s[1] / n, s[2] / n, s[3] / n);
    let m2 = e2 - a * a;
    let m3 = e3 - 3.0 * a * e2 + 2.0 * a * a * a;
    let m4 = e4 - 4.0 * a * e3 + 6.0 * a * a * e2 - 3.0 * a * a * a * a;
    let v = match kind {
        Moment::Variance | Moment::Stddev if count <= 1 => return Value::Null,
        // m2 rounded below 0 is no variance
        Moment::Variance => m2.max(0.0) * n / (n - 1.0),
        Moment::Stddev => (m2.max(0.0) * n / (n - 1.0)).sqrt(),
        Moment::Skewness if count <= 2 => return Value::Null,
        // no variance (every row equal: every d is 0); a rounded m2 below 0 is none either
        Moment::Skewness if m2 <= 0.0 => return Value::F64(f64::NAN),
        Moment::Skewness => (n * (n - 1.0)).sqrt() / (n - 2.0) * m3 / (m2 * m2.sqrt()),
        // the kurtoses need no test for their NULL cases: with no variance (m2 is exactly 0
        // when every d is) they divide by 0, and so does kurtosis at n <= 3 ((n - 2)(n - 3));
        // the result is not finite, so NULL below
        Moment::Kurtosis => (n - 1.0) * ((n + 1.0) * m4 / (m2 * m2) - 3.0 * (n - 1.0)) / ((n - 2.0) * (n - 3.0)),
        _ => m4 / (m2 * m2) - 3.0,
    };
    if v.is_finite() {
        Value::F64(v)
    } else {
        Value::Null
    }
}

/// A group's results, in order: an `Acc::QuantileOf` reads the sampler of the one it names.
pub fn results(accs: &mut [Acc], out: &mut Vec<Value>) {
    for i in 0..accs.len() {
        out.push(match accs[i] {
            Acc::QuantileOf { level, of } => accs[of].result_at(level),
            _ => accs[i].result(),
        });
    }
}

fn moments(m: &[f64; 5], kind: Moment, f32: bool) -> Value {
    if f32 {
        Value::F32(moments!(m, kind, f32))
    } else {
        Value::F64(moments!(m, kind, f64))
    }
}

/// ReservoirSampler<T> with 8192 samples and pcg32_fast seeded 123456.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reservoir {
    samples: Vec<f64>,
    total: u64,
    rng: u64,
    sorted: bool,
}

const RESERVOIR: usize = 8192;

impl Reservoir {
    fn new() -> Reservoir {
        Reservoir { samples: vec![], total: 0, rng: 123_456 | 3, sorted: false }
    }

    /// At most `RESERVOIR` samples out of at least as many seen, sorted if it says so.
    fn consistent(&self) -> bool {
        self.samples.len() <= RESERVOIR
            && self.total >= self.samples.len() as u64
            && (!self.sorted || self.samples.is_sorted_by(|a, b| a.total_cmp(b).is_le()))
    }

    /// pcg32_fast = mcg_xsh_rs_64_32: output from the previous state, then state *= multiplier.
    fn next_u32(&mut self) -> u32 {
        let s = self.rng;
        self.rng = s.wrapping_mul(6_364_136_223_846_793_005);
        let rshift = (s >> 61) & 7;
        let x = s ^ (s >> 22);
        (x >> (22 + rshift)) as u32
    }

    fn insert(&mut self, v: f64) {
        if v.is_nan() {
            return;
        }
        self.sorted = false;
        self.total += 1;
        if self.samples.len() < RESERVOIR {
            let n = self.samples.len();
            if n == self.samples.capacity() {
                // by a quarter, not doubling: a partly filled sampler (most of them) keeps ~10%
                // spare room, not ~33%, and a full one exactly RESERVOIR
                self.samples.reserve_exact((n / 4).max(16).min(RESERVOIR - n));
            }
            self.samples.push(v);
        } else {
            let rnd = (self.next_u32() % self.total.min(u32::MAX as u64) as u32) as usize;
            if rnd < RESERVOIR {
                self.samples[rnd] = v;
            }
        }
    }

    /// Only called with at least one sample (an empty sampler's result is NULL).
    fn quantile(&mut self, level: f64) -> f64 {
        if !self.sorted {
            self.samples.sort_by(f64::total_cmp);
            self.sorted = true;
        }
        let n = self.samples.len();
        let index = level * (n - 1) as f64; // level is in [0, 1] (checked in Acc::new)
        let left = index as usize;
        if left + 1 == n {
            return self.samples[left];
        }
        let (lc, rc) = ((left + 1) as f64 - index, index - left as f64);
        self.samples[left] * lc + self.samples[left + 1] * rc
    }
}

/// How many values `quantile_cont` keeps as they are. Up to this many, its result is DuckDB's
/// to the bit; a t-digest at the median starts merging values into centroids at 200 (its error
/// bound, 0.01 of the ranks, first covers two values there), so a window has left the exact
/// values before its digest gives any of them up. A full buffer is 2 kB per open group, as much
/// as the digest's own buffer of unmerged values (`BUFFER`) grows to.
pub const CONT_EXACT: usize = 256;

/// `quantile_cont(level)(x)`: DuckDB's `PERCENTILE_CONT(level) WITHIN GROUP (ORDER BY x)`, its
/// `MEDIAN` at 0.5. With n values in
/// order, the value at rank `level * (n - 1)`, between two values their blend
/// `lo * (1 - d) + hi * d` (DuckDB's arithmetic, to the bit). NULLs and NaNs are skipped, as
/// DuckDB skips them; none left is NULL.
///
/// A window's first `CONT_EXACT` values are kept as they are and the result is exact. One more,
/// and they move to a t-digest (the one `quantile_t_digest` keeps: bounded state for any
/// number of values), read the same way: each centroid's mean stands at the middle of the ranks
/// it holds, and a rank between two of them is their blend. Centroids of one value, which is all
/// of them until the digest's error bound covers two, give `PERCENTILE_CONT` of the values as
/// Float32.
///
/// `quantile_exact(level)(x)` keeps every value (`All`): exact for any number of them, in
/// memory that grows with them.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cont {
    /// In arrival order, or sorted once read.
    Exact(Vec<f64>),
    Digest(TDigest),
    /// `quantile_exact`'s: every value, as `Exact` keeps its first ones.
    All(Vec<f64>),
}

impl Default for Cont {
    fn default() -> Cont {
        Cont::Exact(vec![])
    }
}

impl Cont {
    fn add(&mut self, x: f64) {
        if x.is_nan() {
            return;
        }
        match self {
            Cont::Exact(v) if v.len() < CONT_EXACT => {
                let n = v.len();
                if n == v.capacity() {
                    // doubling, but never past CONT_EXACT
                    v.reserve_exact(n.max(4).min(CONT_EXACT - n));
                }
                v.push(x);
            }
            Cont::Exact(_) => self.spill(x),
            Cont::Digest(d) => d.add(x as f32),
            Cont::All(v) => v.push(x),
        }
    }

    /// The exact values and `x`, one too many, into a digest, in the order they arrived.
    #[cold]
    #[inline(never)]
    fn spill(&mut self, x: f64) {
        let Cont::Exact(v) = self else { return };
        let mut d = TDigest::default();
        v.iter().chain(&[x]).for_each(|x| d.add(*x as f32));
        *self = Cont::Digest(d);
    }

    /// At most `CONT_EXACT` exact values, none of them a NaN (`add` keeps none, and the sort
    /// would put it last, as the largest value); or a digest as `add` leaves one.
    fn consistent(&self) -> bool {
        match self {
            Cont::Exact(v) => v.len() <= CONT_EXACT && !v.iter().any(|x| x.is_nan()),
            Cont::Digest(d) => d.consistent(),
            Cont::All(v) => !v.iter().any(|x| x.is_nan()),
        }
    }

    fn quantile(&mut self, level: f64) -> Option<f64> {
        match self {
            Cont::Exact(v) | Cont::All(v) if v.is_empty() => None,
            // ponytail: a sort per read, O(n log n); a running quantile OVER (...) reads after
            // every row. A pair of heaps if a long partition's running median ever needs it.
            Cont::Exact(v) | Cont::All(v) => {
                v.sort_unstable_by(f64::total_cmp);
                // DuckDB's Interpolator: RN = (n - 1) * q, between floor(RN) and ceil(RN)
                let rank = (v.len() - 1) as f64 * level;
                let (lo, hi) = (v[rank.floor() as usize], v[rank.ceil() as usize]);
                Some(blend(lo, hi, rank - rank.floor()))
            }
            Cont::Digest(d) if d.is_empty() => None,
            Cont::Digest(d) => Some(d.quantile_cont(level)),
        }
    }
}

/// DuckDB's `CastInterpolation::Interpolate` for doubles, `lo * (1 - d) + hi * d`; at a rank
/// that is a value's own (`d` = 0) that value, with no arithmetic (an infinite `hi` times 0 is
/// NaN).
fn blend(lo: f64, hi: f64, d: f64) -> f64 {
    if d == 0.0 {
        lo
    } else {
        lo * (1.0 - d) + hi * d
    }
}

/// QuantileTDigest: Float32 centroids, epsilon 0.01, at most 2,048 centroids; ClickHouse's merge
/// pass, `compressBrute` and interpolation, on brrrrr's own schedule (ADR-0014).
///
/// ClickHouse merges a digest's values into its centroids once 2,048 are unmerged, and again when
/// a quantile is read: every closing window's digest sorted up to 2,048 values and ran its merge
/// pass over them and its centroids, most of the top of the hour's work. Here a digest also
/// merges every `BUFFER` (512) values as they come, so a close sorts fewer than 512 and merges
/// them; a quantile is read as ClickHouse reads it. A window's digest of 512 values or more also
/// merges once in the window's last seconds (`Acc::premerge`, ADR-0015), so that its close finds
/// only the values since to merge. A digest read before it holds 512 values (every window under
/// 512 values, every row of an OVER frame) is ClickHouse's bit for bit (bar two equal
/// infinities, see `interpolate`). Past that the centroids differ, as merged on another
/// schedule, and a result is about as close in rank as ClickHouse's (`rank_error_table`, 13
/// distributions, 1,000 to 1M values, 13 levels: at most 0.46% of the values off against
/// ClickHouse's 0.65% on heavily tied sizes, 0.38% as ClickHouse's on clustered prices, at most
/// 0.13% against 0.05% on the others, 0.15% merged ahead; mean errors below 0.06%, at most 2.1
/// times ClickHouse's, 2.7 times merged ahead).
/// Each value costs more on its way in (a merge pass every 512, not 2,048: ~60 against ~40 ns
/// a value for one digest alone).
///
/// The merged centroids are an exact-size slice and the unmerged values (all of count 1) a
/// separate `f32` buffer that grows to at most `BUFFER`, which `compress` merges with them in a
/// thread's scratch buffer. The buffer is kept for the next values: freeing it and growing it
/// again from 8 cost the range pipeline ~2% of its throughput for ~9 MB (of ~60) less. A
/// checkpoint written before this schedule may hold up to 2,048 unmerged values: they merge on
/// the next value or the next read, and the buffer goes back to `BUFFER`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TDigest {
    merged: Box<[(f32, f32)]>, // (mean, count)
    unmerged: Vec<f32>,
    count: f64,
}

const MAX_CENTROIDS: usize = 2048;

/// How many values a digest takes unmerged before it merges them (`TDigest`): fewer make a close
/// cheaper (it sorts them) and each value dearer (each merge walks every centroid).
const BUFFER: usize = 512;

/// While `compress` runs: the centroids and the unmerged values in order, and the unmerged
/// values' sort keys.
type Scratch = (Vec<(f32, f32)>, Vec<u32>);

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = const { std::cell::RefCell::new((Vec::new(), Vec::new())) };
}

/// RadixSortFloatTransform: order-preserving u32 key of a float (the LSD radix sort is stable).
fn radix_key(x: f32) -> u32 {
    let b = x.to_bits();
    if b >> 31 == 1 {
        !b
    } else {
        b + 0x8000_0000 // sign bit clear: same as setting it
    }
}

/// The float whose `radix_key` is `k`.
fn from_radix_key(k: u32) -> f32 {
    f32::from_bits(if k >> 31 == 1 { k - 0x8000_0000 } else { !k })
}

impl TDigest {
    fn add(&mut self, x: f32) {
        if x.is_nan() {
            return;
        }
        self.count += 1.0;
        let n = self.unmerged.len();
        if n == self.unmerged.capacity() {
            // doubling, but never past BUFFER (one at a time past it: a checkpoint's 2,048)
            self.unmerged.reserve_exact(n.max(8).min(BUFFER.saturating_sub(n).max(1)));
        }
        self.unmerged.push(x);
        if self.unmerged.len() >= BUFFER {
            self.compress();
        }
    }

    fn is_empty(&self) -> bool {
        self.merged.is_empty() && self.unmerged.is_empty()
    }

    /// At most 2,048 values unmerged, which it split into (see `count`): what any build wrote
    /// (ClickHouse's schedule held up to 2,048, this one `BUFFER`).
    fn consistent(&self) -> bool {
        self.unmerged.len() <= MAX_CENTROIDS
    }

    /// Merges the unmerged values into the centroids.
    fn compress(&mut self) {
        if self.unmerged.is_empty() && self.merged.len() <= MAX_CENTROIDS {
            return;
        }
        SCRATCH.with_borrow_mut(|(c, keys)| {
            in_order(c, keys, &self.merged, self.unmerged.iter().copied());
            self.count = merge(c, self.count);
            if let Some(count) = compress_brute(c) {
                self.count = count;
            }
            if self.merged.len() == c.len() {
                self.merged.copy_from_slice(c);
            } else {
                self.merged = c.as_slice().into();
            }
        });
        self.unmerged.clear();
        // restored with ClickHouse's 2,048 waiting: back to this schedule's size (else no-op)
        self.unmerged.shrink_to(BUFFER);
    }

    /// Merges the unmerged values ahead of a read (`Acc::premerge`), if the digest has merged
    /// before: one that has not (fewer than `BUFFER` values) is left to merge at its read, as
    /// ClickHouse's.
    fn premerge(&mut self) {
        if !self.merged.is_empty() {
            self.compress();
        }
    }

    /// The quantile at `level`, as ClickHouse reads it: the values since merged into the centroids
    /// first. Reading again reads the same centroids (nothing is left to merge), so the levels of
    /// one digest (`Acc::QuantileOf`) merge once. Only called with at least one value (an empty
    /// digest's result is NULL).
    fn quantile(&mut self, level: f64) -> f32 {
        self.compress();
        interpolate(&self.merged, self.count, level)
    }

    /// `PERCENTILE_CONT(level)` of what the centroids stand for (`Cont`): a centroid of `count`
    /// values holds that many consecutive ranks, its mean at their middle. Only called with at
    /// least one centroid.
    fn quantile_cont(&mut self, level: f64) -> f64 {
        self.compress();
        let rank = level * (self.count - 1.0);
        // values before the centroid, and the centroid before it: (its middle rank, its mean)
        let (mut before, mut prev) = (0f64, None);
        for &(mean, count) in &self.merged {
            let (mean, count) = (mean as f64, count as f64);
            let middle = before + (count - 1.0) / 2.0;
            if middle >= rank {
                return match prev {
                    // strictly between two middles: 0 < d < 1 (blend's arithmetic, not its d = 0 case)
                    Some((at, lo)) if middle > rank => {
                        let d = (rank - at) / (middle - at);
                        lo * (1.0 - d) + mean * d
                    }
                    _ => mean, // its own rank, or one before the first centroid's middle
                };
            }
            (before, prev) = (before + count, Some((middle, mean)));
        }
        self.merged[self.merged.len() - 1].0 as f64 // after the last centroid's middle
    }
}

/// ClickHouse's `getImpl`: the value at rank `level * count` of centroids in order, interpolated
/// between the two around it (a centroid of one value is that value, not a spread). Between two
/// equal means, that mean: two infinities blend into NaN (0 * inf) when the weight rounds to 0 or
/// 1 in Float32; equal finite means blend into themselves either way.
fn interpolate(c: &[(f32, f32)], count: f64, level: f64) -> f32 {
    // none left: a crafted checkpoint whose centroids `compressBrute` dropped as NaN (one
    // centroid reads as itself below, an interpolation between it and itself)
    if c.is_empty() {
        return f32::NAN;
    }
    let x = level * count;
    let (mut prev_x, mut sum) = (0f64, 0f32);
    let (mut prev_mean, mut prev_count) = c[0];
    for &(mean, count) in c {
        let current_x = sum as f64 + count as f64 * 0.5;
        if current_x >= x {
            let left = prev_x + 0.5 * (prev_count == 1.0) as u8 as f64;
            let right = current_x - 0.5 * (count == 1.0) as u8 as f64;
            return if x <= left {
                prev_mean
            } else if x >= right || prev_mean == mean {
                mean
            } else {
                // interpolate(): Float32 differences, Float64 blend
                let (xf, x1, x2) = (x as f32, left as f32, right as f32);
                let k = ((xf - x1) / (x2 - x1)) as f64;
                ((1.0 - k) * prev_mean as f64 + k * mean as f64) as f32
            };
        }
        sum += count;
        (prev_mean, prev_count, prev_x) = (mean, count, current_x);
    }
    c[c.len() - 1].0
}

/// Fills `c` with the centroids `merged` and the `unmerged` values (each of count 1) in the
/// order ClickHouse's stable radix sort of the two, appended, puts them: by `radix_key`, ties in
/// that order. Most of a window's close went into a stable sort of them all; the
/// centroids that `compress` leaves are already in that order, so only the unmerged values are
/// sorted, as bare keys (equal keys are equal values, all of count 1, so how a sort orders them
/// cannot be seen), and the two runs merged, a centroid before a value of the same key. The
/// order never depends on how the centroids came to be: ones out of order (none a well-formed
/// `compress` writes, but a restored checkpoint may hold anything) are sorted with the values as
/// before; only the speed differs.
fn in_order(c: &mut Vec<(f32, f32)>, keys: &mut Vec<u32>, merged: &[(f32, f32)], unmerged: impl Iterator<Item = f32>) {
    c.clear();
    keys.clear();
    keys.extend(unmerged.map(radix_key));
    if !merged.windows(2).all(|w| radix_key(w[0].0) <= radix_key(w[1].0)) {
        c.extend_from_slice(merged);
        c.extend(keys.iter().map(|&k| (from_radix_key(k), 1.0f32)));
        c.sort_by_key(|c| radix_key(c.0));
        return;
    }
    keys.sort_unstable();
    if few(keys.len(), merged.len()) {
        // a few values among many centroids: each found by binary search, the centroids before
        // it copied as a run
        let mut rest = merged;
        for &k in keys.iter() {
            let (before, after) = rest.split_at(rest.partition_point(|c| radix_key(c.0) <= k));
            c.extend_from_slice(before);
            c.push((from_radix_key(k), 1.0));
            rest = after;
        }
        c.extend_from_slice(rest);
        return;
    }
    let mut values = keys.iter().peekable();
    for &centroid in merged {
        while let Some(&k) = values.next_if(|&&k| k < radix_key(centroid.0)) {
            c.push((from_radix_key(k), 1.0));
        }
        c.push(centroid);
    }
    c.extend(values.map(|&k| (from_radix_key(k), 1.0f32)));
}

/// Whether `values` are few enough among `centroids` for `in_order` to place each by binary
/// search (fewer than one per 16 centroids) rather than merge the two runs: only its speed
/// depends on it, never the order it writes.
fn few(values: usize, centroids: usize) -> bool {
    values * 16 < centroids
}

/// compress()'s quantile-error pass over centroids in order (`in_order`): merges neighbours
/// within the error bound. Returns the new count.
fn merge(c: &mut Vec<(f32, f32)>, count: f64) -> f64 {
    let count_epsilon_4 = count * 0.01f32 as f64 * 4.0;
    let (mut l, mut sum) = (0usize, 0f64);
    let (mut l_mean, mut l_count) = (c[0].0 as f64, c[0].1 as f64);
    for r in 1..c.len() {
        let ql = (sum + l_count * 0.5) / count;
        let qr = (sum + l_count + c[r].1 as f64 * 0.5) / count;
        let err = (ql * (1.0 - ql)).min(qr * (1.0 - qr));
        let k = count_epsilon_4 * err;
        let mergeable = l_mean == c[r].0 as f64 || (!l_mean.is_infinite() && !c[r].0.is_infinite());
        if l_count + c[r].1 as f64 <= k && mergeable {
            l_count += c[r].1 as f64;
            if c[r].0 as f64 != l_mean {
                l_mean += c[r].1 as f64 * (c[r].0 as f64 - l_mean) / l_count;
            }
            c[l] = (l_mean as f32, l_count as f32);
        } else {
            sum += c[l].1 as f64;
            l += 1;
            if l != r {
                c[l] = c[r];
            }
            (l_mean, l_count) = (c[l].0 as f64, c[l].1 as f64);
        }
    }
    c.truncate(l + 1);
    sum + l_count
}

/// compressBrute: merges batches of neighbours while more than 2,048 centroids are left.
/// Returns the new count if it ran.
fn compress_brute(c: &mut Vec<(f32, f32)>) -> Option<f64> {
    if c.len() <= MAX_CENTROIDS {
        return None;
    }
    let batch = c.len().div_ceil(MAX_CENTROIDS);
    let (mut l, mut sum, mut pos) = (0usize, 0f64, 0usize);
    let (mut l_mean, mut l_count) = (c[0].0 as f64, c[0].1 as f64);
    for r in 1..c.len() {
        if pos < batch - 1 {
            l_count += c[r].1 as f64;
            if c[r].0 as f64 != l_mean {
                l_mean += c[r].1 as f64 * (c[r].0 as f64 - l_mean) / l_count;
            }
            c[l] = (l_mean as f32, l_count as f32);
            pos += 1;
        } else {
            if !c[l].0.is_nan() {
                sum += c[l].1 as f64;
                l += 1;
            }
            c[l] = c[r];
            (l_mean, l_count, pos) = (c[l].0 as f64, c[l].1 as f64, 0);
        }
    }
    if !c[l].0.is_nan() {
        c.truncate(l + 1);
        Some(sum + l_count)
    } else {
        c.truncate(l);
        Some(sum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// impact's large_trade_reversal: the mean return 2 trades ahead of the trades at or above the
    /// notionals' median (exact, interpolated), those without 2 trades after them left out.
    #[test]
    fn trade_reversal_is_the_mean_return_ahead_of_the_large_trades() {
        let mut a = Acc::new("trade_reversal", &[Value::Int(2), Value::F64(0.5)], 1).unwrap();
        // notionals 100, 1010, 102, 103, 1040, 105: their median 104 (between 103 and 105)
        for (i, (price, size)) in [(100.0, 1.0), (101.0, 10.0), (102.0, 1.0), (103.0, 1.0), (104.0, 10.0), (105.0, 1.0)]
            .into_iter()
            .enumerate()
        {
            let side = Value::Str(if i % 2 == 0 { "buy" } else { "sell" }.into());
            let t = [Value::Int(i as i64), Value::Str(format!("{i}").into()), Value::Int(0)];
            a.add(Value::Array(
                [t[0].clone(), t[1].clone(), t[2].clone(), Value::F64(price), side, Value::F64(size)].into(),
            ));
        }
        // the trade of 1010 (2 ahead: 103); the one of 1040 has no trade 2 ahead
        assert_eq!(a.result(), Value::F64(103.0 / 101.0 - 1.0));
        // the threshold between two notionals: 300 and 400 at rank 2.5 of 6, 350
        let mut b = Acc::new("trade_reversal", &[Value::Int(1), Value::F64(0.5)], 1).unwrap();
        for (i, notional) in [400.0, 100.0, 600.0, 200.0, 500.0, 300.0].into_iter().enumerate() {
            let (t, price) = (Value::Int(i as i64), 100.0 + i as f64);
            b.add(Value::Array(
                [
                    t,
                    Value::Int(0),
                    Value::Int(0),
                    Value::F64(price),
                    Value::Str("buy".into()),
                    Value::F64(notional / price),
                ]
                .into(),
            ));
        }
        let three = 0.0 + (101.0 / 100.0 - 1.0) + (103.0 / 102.0 - 1.0) + (105.0 / 104.0 - 1.0);
        assert_eq!(b.result(), Value::F64(three / 3.0));
        // restored state is held to its plan's parameters
        assert!(b.check_against(&b.clone()).is_ok());
        for other in [[Value::Int(2), Value::F64(0.5)], [Value::Int(1), Value::F64(0.9)]] {
            assert!(b.check_against(&Acc::new("trade_reversal", &other, 1).unwrap()).is_err(), "{other:?}");
        }
        let mut none = Acc::new("trade_reversal", &[Value::Int(2), Value::F64(0.5)], 1).unwrap();
        assert_eq!(none.result(), Value::Null);
        for bad in [vec![], vec![Value::Int(0), Value::F64(0.5)], vec![Value::Int(2), Value::F64(1.5)]] {
            assert!(Acc::new("trade_reversal", &bad, 1).is_err(), "{bad:?}");
        }
    }

    /// compressBrute only runs when compress() leaves more than 2,048 centroids, which needs
    /// astronomically large windows, so no Proton golden reaches it. Its contract: bounded memory,
    /// no lost weight, order kept.
    #[test]
    fn compress_brute_bounds_centroids_and_keeps_weight_and_order() {
        for n in [2_049usize, 4_097, 10_000] {
            let mut c: Vec<(f32, f32)> = (0..n).map(|i| (i as f32, 1.0 + (i % 3) as f32)).collect();
            let weight: f64 = c.iter().map(|c| c.1 as f64).sum();
            let count = compress_brute(&mut c);
            assert!(c.len() <= MAX_CENTROIDS, "{n}: {} centroids", c.len());
            assert_eq!(count, Some(weight), "{n}: weight lost");
            assert!(c.windows(2).all(|w| w[0].0 < w[1].0), "{n}: order lost");
        }
        // batches of ceil(n / 2048) neighbours: 4,097 centroids merge three at a time
        let mut c: Vec<(f32, f32)> = (0..4_097).map(|i| (i as f32, 1.0 + (i % 3) as f32)).collect();
        let count = compress_brute(&mut c);
        assert_eq!(c.len(), 1_366);
        // incremental weighted means: (0*1 + 1*2 + 2*3) / 6 and (3*1 + 4*2 + 5*3) / 6
        assert_eq!(c[..2], [((8.0f64 / 6.0) as f32, 6.0), ((26.0f64 / 6.0) as f32, 6.0)]);
        assert_eq!(c[1_365], ((4_095.0f64 + 2.0 / 3.0) as f32, 3.0)); // 4095 (x1) and 4096 (x2)
        assert_eq!(count, Some((0..4_097).map(|i| 1.0 + (i % 3) as f64).sum::<f64>()));
        // a trailing NaN centroid is dropped along with its weight, as in ClickHouse
        let mut c: Vec<(f32, f32)> = (0..3_000).map(|i| (i as f32, 1.0)).collect();
        c.push((f32::NAN, 1.0));
        let count = compress_brute(&mut c);
        assert!(c.iter().all(|c| !c.0.is_nan()));
        assert!(count.unwrap() <= 3_000.0);
    }

    /// compress() also runs its quantile-error merge pass when nothing is unmerged but the
    /// digest holds too many centroids, which compacts far harder than compressBrute alone.
    #[test]
    fn compress_merges_an_oversized_digest_even_with_nothing_unmerged() {
        let merged = (0..3_000).map(|i| (i as f32, 1.0)).collect();
        let mut d = TDigest { merged, unmerged: vec![], count: 3_000.0 };
        d.compress();
        assert!(d.merged.len() < 1_000, "{} centroids: the merge pass did not run", d.merged.len());
        assert_eq!(d.merged.iter().map(|c| c.1 as f64).sum::<f64>(), 3_000.0);
    }

    /// A sampler grows by a quarter to exactly `RESERVOIR` samples, also from a restored length.
    #[test]
    fn a_reservoir_grows_to_exactly_its_size() {
        for start in [0usize, 1, 100, 8_180, 8_191] {
            let mut r = Reservoir::new();
            r.samples = (0..start).map(|i| i as f64).collect();
            r.samples.shrink_to_fit();
            r.total = start as u64;
            let mut grown = 0;
            for i in start..20_000 {
                let (len, before) = (r.samples.len(), r.samples.capacity());
                r.insert(i as f64);
                let (n, cap) = (r.samples.len(), r.samples.capacity());
                assert!(cap <= RESERVOIR && cap <= n + (n / 4).max(16), "{start}: {n} in {cap}");
                if cap != before {
                    // by a quarter of what it holds (at least 16), up to exactly RESERVOIR
                    assert_eq!(cap, (len + (len / 4).max(16)).min(RESERVOIR), "{start}: {len} grew to {cap}");
                    grown += 1;
                }
            }
            assert_eq!(r.samples.capacity(), RESERVOIR);
            assert!(grown <= 30, "{start}: grown {grown} times");
        }
    }

    /// The unmerged values' buffer doubles from 8 to `BUFFER`, where they merge and it is kept
    /// for the next ones; a digest restored with more (ClickHouse's schedule kept up to 2,048)
    /// merges them all on its next value.
    #[test]
    fn the_unmerged_buffer_doubles_up_to_its_limit() {
        let mut d = TDigest::default();
        for i in 0..3 * BUFFER {
            d.add(i as f32);
            let (n, cap) = (d.unmerged.len(), d.unmerged.capacity());
            if i < BUFFER - 1 {
                assert_eq!(cap, n.next_power_of_two().max(8), "{n} values in {cap}");
            } else {
                assert_eq!((n, cap), ((i + 1) % BUFFER, BUFFER), "after {} values", i + 1);
            }
        }
        assert_eq!(d.count, (3 * BUFFER) as f64);
        for n in [BUFFER - 1, BUFFER, 1_500, MAX_CENTROIDS] {
            let mut d = TDigest { merged: Box::new([]), unmerged: vec![1.0; n], count: n as f64 };
            d.add(2.0);
            if n + 1 >= BUFFER {
                assert!(d.unmerged.is_empty() && !d.merged.is_empty(), "restored with {n}: merged");
            }
            assert_eq!(d.count, (n + 1) as f64);
        }
    }

    /// twap over the two extreme times neither panics nor wraps: the span is 2^64 - 1 µs.
    #[test]
    fn twap_spans_the_extreme_times() {
        let mut acc = Acc::new("twap", &[], 2).unwrap();
        acc.add2(Value::F64(1.0), Value::Int(i64::MIN));
        acc.add2(Value::F64(3.0), Value::Int(i64::MAX));
        acc.add2(Value::F64(5.0), Value::Int(i64::MAX));
        // 1 held for the whole span, 3 for none of it
        assert_eq!(acc.result(), Value::F64(1.0));
        let Acc::Twap(tw) = &acc else { panic!("twap") };
        assert_eq!(tw.t, u64::MAX as f64);
    }

    /// twap of no prices (every row's NULL) is NULL, not the 0 / 0 of its mean.
    #[test]
    fn twap_of_no_prices_is_null() {
        let mut acc = Acc::new("twap", &[], 2).unwrap();
        acc.add2(Value::Null, Value::Int(0));
        acc.add2(Value::Null, Value::Int(1));
        assert_eq!(acc.result(), Value::Null);
    }

    /// An integer sum stays an Int64, exact past 2^53 and wrapping at 2^63 as ClickHouse's does;
    /// only a float makes it a Float64.
    #[test]
    fn an_integer_sum_stays_an_integer() {
        let sum = |v: &[Value]| {
            let mut acc = Acc::new("sum", &[], 1).unwrap();
            v.iter().for_each(|v| acc.add(v.clone()));
            acc.result()
        };
        let big = (1 << 53) + 1;
        assert_eq!(sum(&[Value::Int(big), Value::Int(big)]), Value::Int(2 * big));
        assert_eq!(sum(&[Value::Int(i64::MAX), Value::Int(1)]), Value::Int(i64::MIN));
        assert_eq!(sum(&[Value::Int(1), Value::F64(0.5)]), Value::F64(1.5));
    }

    /// A digest that saw only NULLs and NaNs is empty, and its quantile NULL.
    #[test]
    fn an_empty_digest_is_null() {
        let mut acc = Acc::new("quantile_t_digest", &[], 1).unwrap();
        acc.add(Value::Null);
        acc.add(Value::F64(f64::NAN));
        assert_eq!(acc.result(), Value::Null);
    }

    /// At exactly the limit, with nothing unmerged, compress() leaves the digest alone.
    #[test]
    fn compress_leaves_a_full_merged_digest_alone() {
        let centroids: Vec<(f32, f32)> = (0..MAX_CENTROIDS).map(|i| (i as f32, 1.0)).collect();
        let mut d = TDigest { merged: centroids.clone().into(), unmerged: vec![], count: MAX_CENTROIDS as f64 };
        d.compress();
        assert_eq!(*d.merged, *centroids);
    }

    fn fnv(c: &[(f32, f32)]) -> u64 {
        c.iter()
            .flat_map(|(m, n)| [m.to_bits(), n.to_bits()])
            .fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3))
    }

    /// The merge pass's arithmetic and the schedule, pinned: 10,000 values on a grid with ties
    /// (19 merges of 512, 272 since) keep these centroids and read these quantiles, merging the
    /// values since into those centroids. The models share `merge` and `interpolate`, so only pinned bits catch
    /// a change to the error bound, the means or the interpolation.
    #[test]
    fn the_merge_pass_keeps_pinned_centroids() {
        let mut d = TDigest::default();
        for i in 0..10_000u32 {
            d.add(((i * 7_919) % 1_000) as f32 * 0.25 - 50.0);
        }
        assert_eq!((d.merged.len(), d.unmerged.len(), d.count, fnv(&d.merged)), PINNED.0);
        let levels = [0.0, 0.01, 0.25, 0.5, 0.9, 0.999, 1.0];
        assert_eq!(levels.map(|l| d.quantile(l)), PINNED.1);
        assert_eq!((d.merged.len(), d.count, fnv(&d.merged)), PINNED.2, "merged by the read");
        d.compress();
        assert_eq!((d.merged.len(), d.count, fnv(&d.merged)), PINNED.2, "nothing left to merge");
    }

    /// (centroids, values since, count, centroids' hash) before a read; the quantiles read;
    /// (centroids, count, hash) after.
    type Pinned = ((usize, usize, f64, u64), [f32; 7], (usize, f64, u64));

    const PINNED: Pinned = (
        (428, 272, 10_000.0, 16_878_072_407_231_467_970),
        [-50.0, -47.61111, 12.38903, 74.877556, 174.87498, 199.5, 199.75],
        (430, 10_000.0, 13_427_206_205_717_241_699),
    );

    /// `from_radix_key` undoes `radix_key` for every bit pattern (NaNs and both zeros included),
    /// so the unmerged values come back from their keys exactly.
    #[test]
    fn a_radix_key_gives_back_its_float() {
        for bits in (0..=u32::MAX).step_by(65_521).chain([0, 0x8000_0000, 0x7f80_0000, 0xff80_0000, 0x7fc0_0001, 1]) {
            let x = f32::from_bits(bits);
            assert_eq!(from_radix_key(radix_key(x)).to_bits(), bits, "{bits:#010x}");
        }
        let mut keys: Vec<u32> = [-1.5f32, -0.0, 0.0, 1e-45, 3.0, f32::INFINITY].map(radix_key).to_vec();
        let sorted = keys.clone();
        keys.sort_unstable();
        assert_eq!(keys, sorted, "keys order floats, -0 before +0");
    }

    /// The order the stable sort of centroids and values appended gives (ClickHouse's).
    fn appended_order(merged: &[(f32, f32)], unmerged: &[f32]) -> Vec<(f32, f32)> {
        let mut c: Vec<(f32, f32)> = merged.iter().copied().chain(unmerged.iter().map(|&x| (x, 1.0))).collect();
        c.sort_by_key(|c| radix_key(c.0));
        c
    }

    fn bits(c: &[(f32, f32)]) -> Vec<(u32, u32)> {
        c.iter().map(|(m, n)| (m.to_bits(), n.to_bits())).collect()
    }

    /// Centroids as `compress` leaves them and a few values (fewer than one per 16 centroids:
    /// each placed by binary search), many values (merged run by run) or none, among them values
    /// equal to centroids' means, below and above them all: the appended stable sort's order.
    #[test]
    fn a_few_values_or_many_go_where_the_appended_sort_puts_them() {
        let mut d = TDigest::default();
        for i in 0..20_000u32 {
            d.add(((i * 7_919) % 5_003) as f32 * 0.5 - 600.0);
        }
        d.compress();
        let means: Vec<f32> = d.merged.iter().map(|c| c.0).collect();
        assert!(means.len() > 200, "{} centroids", means.len());
        let picks = |n: usize| -> Vec<f32> {
            let mut v: Vec<f32> = (0..n).map(|i| means[(i * 37) % means.len()]).collect(); // ties
            v.extend([f32::NEG_INFINITY, -1e9, 0.25, 1e9, f32::INFINITY, -0.0, 0.0]);
            v.truncate(n);
            v
        };
        for n in [0, 1, 2, 7, means.len() / 16 - 1, means.len() / 16, means.len() / 16 + 1, 300, 2_048] {
            let unmerged = picks(n);
            let (mut c, mut keys) = (vec![], vec![]);
            in_order(&mut c, &mut keys, &d.merged, unmerged.iter().copied());
            assert_eq!(bits(&c), bits(&appended_order(&d.merged, &unmerged)), "{n} values");
        }
    }

    /// Centroids out of order, as a crafted or corrupt checkpoint could hold them, are sorted
    /// with the values: the order and the whole digest's compress are the appended one's.
    #[test]
    fn centroids_out_of_order_take_the_appended_sort() {
        let merged = vec![(5.0f32, 2.0f32), (1.0, 3.0), (5.0, 1.0), (-2.0, 1.0), (9.0, 4.0)];
        let unmerged = [5.0f32, 0.0, -2.0, 7.5, 1.0];
        let (mut c, mut keys) = (vec![], vec![]);
        in_order(&mut c, &mut keys, &merged, unmerged.iter().copied());
        assert_eq!(bits(&c), bits(&appended_order(&merged, &unmerged)));
        assert_eq!(c[..4], [(-2.0, 1.0), (-2.0, 1.0), (0.0, 1.0), (1.0, 3.0)], "the centroid before the value");
        let mut d = TDigest { merged: merged.clone().into(), unmerged: unmerged.to_vec(), count: 16.0 };
        let mut a = Appended {
            centroids: appended_order(&merged, &unmerged),
            count: 16.0,
            unmerged: 5,
            schedule: Schedule::Brrrrr,
        };
        d.compress();
        a.compress();
        assert_eq!((bits(&d.merged), d.count.to_bits()), (bits(&a.centroids), a.count.to_bits()));
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        /// Two runs of rows, each folded, then merged: the fold of both, bit for bit but for a
        /// float's sum, within rounding (ADR-0017). Rows are integers and floats, NULLs, both
        /// zeros, infinities; the later run has no NaN (`merge`: one first holds a min or max).
        #[test]
        fn a_merge_of_two_runs_is_the_fold_of_both(
            rows in prop::collection::vec(
                (prop_oneof![
                    Just(Value::Null),
                    (-1000i64..1000).prop_map(Value::Int),
                    (-1e6..1e6f64).prop_map(Value::F64),
                    Just(Value::F64(-0.0)),
                    Just(Value::F64(f64::INFINITY)),
                ], 0.0..10f64, any::<bool>()),
                0..40,
            ),
            cut in 0usize..40,
        ) {
            let cut = cut.min(rows.len());
            let ints = |r: &[(Value, f64, bool)]| -> Vec<(Value, f64, bool)> {
                r.iter().map(|(v, w, c)| (v.f64().map_or(Value::Null, |f| Value::Int(f as i64)), *w, *c)).collect()
            };
            for (name, nargs, rows) in [
                ("latest", 1, rows.clone()), ("earliest", 1, rows.clone()), ("min", 1, rows.clone()),
                ("max", 1, rows.clone()), ("count", 1, rows.clone()), ("count_if", 1, rows.clone()),
                ("sum", 1, ints(&rows)), ("sum", 1, rows.clone()), ("avg", 1, rows.clone()),
                ("weighted_avg", 2, rows.clone()), ("sum_if", 2, rows.clone()),
            ] {
                let fold = |r: &[(Value, f64, bool)]| {
                    let mut a = Acc::new(name, &[], nargs).unwrap();
                    for (v, w, c) in r {
                        match (name, nargs) {
                            ("count_if", _) => a.add(Value::Bool(*c)),
                            (_, 1) => a.add(v.clone()),
                            ("sum_if", _) => a.add2(v.clone(), Value::Bool(*c)),
                            _ => a.add2(v.clone(), Value::F64(*w)),
                        }
                    }
                    a
                };
                let mut whole = fold(&rows);
                let mut merged = fold(&rows[..cut]);
                prop_assert!(merged.mergeable(), "{}", name);
                merged.merge(&fold(&rows[cut..]));
                let (want, got) = (whole.result(), merged.result());
                let floats = matches!(name, "sum" | "avg" | "weighted_avg" | "sum_if") && !matches!(want, Value::Int(_));
                match (&want, &got) {
                    (Value::F64(a), Value::F64(b)) if floats => {
                        prop_assert!(a == b || (a - b).abs() <= 1e-12 * a.abs().max(b.abs()) || (a.is_nan() && b.is_nan()), "{} {} {}", name, a, b)
                    }
                    (Value::F64(a), Value::F64(b)) => prop_assert_eq!(a.to_bits(), b.to_bits(), "{}", name),
                    _ => prop_assert_eq!(&want, &got, "{}", name),
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Two runs of trades, the later folded from the earlier's last price (`Acc::seeded`)
        /// and merged: the fold of both, within rounding (ADR-0017). Ties of time within each
        /// run, none across (`apply_many` adds those row by row), sells and buys, zero prices.
        #[test]
        fn a_merge_of_two_runs_of_trades_is_the_fold_of_both(
            trades in prop::collection::vec((0i64..3, any::<bool>(), prop_oneof![Just(0.0), 1.0..200.0f64], 0.1..50.0f64), 0..60),
            cut in 0usize..60,
        ) {
            let mut t = 1_000i64;
            let rows: Vec<Value> = trades.iter().enumerate().map(|(i, (dt, buy, price, size))| {
                t += dt;
                Value::Array([Value::Int(t), Value::Int(i as i64), Value::Int(0), Value::F64(*price),
                    Value::Str(if *buy { "buy" } else { "sell" }.into()), Value::F64(*size)].into())
            }).collect();
            let time = |v: &Value| match v { Value::Array(a) => a[0].i64().unwrap(), _ => unreachable!() };
            // the cut moved past the trades of the time before it
            let mut cut = cut.min(rows.len());
            while cut > 0 && cut < rows.len() && time(&rows[cut - 1]) == time(&rows[cut]) {
                cut += 1;
            }
            let fresh = Acc::new("trade_returns", &[], 1).unwrap();
            let mut whole = fresh.clone();
            rows.iter().for_each(|r| whole.add(r.clone()));
            let mut merged = fresh.clone();
            rows[..cut].iter().for_each(|r| merged.add(r.clone()));
            let mut later = merged.seeded(&fresh);
            rows[cut..].iter().for_each(|r| later.add(r.clone()));
            if merged.trades().is_some_and(|(held, _)| held) {
                merged.merge(&later);
            } else {
                merged = later;
            }
            let (Value::Array(w), Value::Array(m)) = (whole.result(), merged.result()) else { unreachable!() };
            for (a, b) in w.iter().zip(m.iter()) {
                match (a, b) {
                    (Value::F64(a), Value::F64(b)) => prop_assert!(
                        a == b || (a - b).abs() <= 1e-12 * a.abs().max(b.abs()).max(1.0), "{:?} {:?}", w, m),
                    _ => prop_assert_eq!(a, b),
                }
            }
        }
    }

    /// A value from a coarse grid (ties with each other and with centroids), both zeros, both
    /// infinities, subnormals and the extremes.
    fn value() -> impl Strategy<Value = f32> {
        prop_oneof![
            (-8i32..8).prop_map(|i| i as f32 * 0.5),
            Just(0.0f32),
            Just(-0.0f32),
            Just(f32::INFINITY),
            Just(f32::NEG_INFINITY),
            Just(f32::MIN_POSITIVE / 4.0),
            Just(f32::MAX),
            Just(f32::MIN),
            any::<f32>().prop_filter("no NaN", |x| !x.is_nan()),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// Centroids as `compress` leaves them (in key order, their means often equal to values
        /// to merge) and any unmerged values: `in_order` puts them where the stable sort of the
        /// two appended does, bit for bit.
        #[test]
        fn centroids_and_values_are_in_the_appended_order(
            seed in proptest::collection::vec(value(), 0..300),
            unmerged in proptest::collection::vec(value(), 0..300),
        ) {
            let mut d = TDigest::default();
            seed.iter().for_each(|&x| d.add(x));
            d.compress();
            prop_assert!(d.merged.windows(2).all(|w| radix_key(w[0].0) <= radix_key(w[1].0)));
            let (mut c, mut keys) = (vec![], vec![]);
            in_order(&mut c, &mut keys, &d.merged, unmerged.iter().copied());
            prop_assert_eq!(bits(&c), bits(&appended_order(&d.merged, &unmerged)));
        }

        /// Centroids in no particular order (none `compress` writes, but a checkpoint could hold
        /// them) are sorted with the values, as before: still the appended order.
        #[test]
        fn centroids_out_of_order_are_sorted_with_the_values(
            merged in proptest::collection::vec((value(), 1u8..5), 2..50),
            unmerged in proptest::collection::vec(value(), 0..50),
        ) {
            let merged: Vec<(f32, f32)> = merged.into_iter().map(|(m, n)| (m, n as f32)).collect();
            let (mut c, mut keys) = (vec![], vec![]);
            in_order(&mut c, &mut keys, &merged, unmerged.iter().copied());
            prop_assert_eq!(bits(&c), bits(&appended_order(&merged, &unmerged)));
        }

        /// The whole digest, asked any quantiles after any values, agrees bit for bit with the
        /// model on brrrrr's schedule (`Appended`): centroids, values since, count and results;
        /// a read merges the values since, and reading again reads the same.
        #[test]
        fn a_digest_merges_and_reads_as_the_model(
            values in proptest::collection::vec(value(), 1..3_000),
            levels in proptest::collection::vec(0.0f64..=1.0, 1..4),
        ) {
            let (mut d, mut a) = (TDigest::default(), Appended::new(Schedule::Brrrrr));
            values.iter().for_each(|&x| { d.add(x); a.add(x); });
            prop_assert_eq!(bits(&d.merged), bits(a.merged()));
            prop_assert_eq!(d.count.to_bits(), a.count.to_bits());
            for &level in &levels {
                let first = d.quantile(level);
                prop_assert_eq!(first.to_bits(), a.quantile(level).to_bits());
                prop_assert!(d.unmerged.is_empty(), "a read merges");
                prop_assert_eq!(d.quantile(level).to_bits(), first.to_bits(), "read again");
                prop_assert_eq!(bits(&d.merged), bits(&a.centroids));
            }
        }

        /// A digest that never merged (fewer than `BUFFER` values) merges them all when read, as
        /// ClickHouse does: the result is ClickHouse's bit for bit, but where ClickHouse blends two
        /// equal infinities into NaN (the infinity here).
        #[test]
        fn a_small_digest_reads_what_clickhouse_reads(
            values in proptest::collection::vec(value(), 1..BUFFER),
            level in 0.0f64..=1.0,
        ) {
            let (mut d, mut ch) = (TDigest::default(), Appended::new(Schedule::ClickHouse));
            values.iter().for_each(|&x| { d.add(x); ch.add(x); });
            prop_assume!(!d.is_empty());
            let (ours, theirs) = (d.quantile(level), ch.quantile(level));
            if theirs.is_nan() {
                prop_assert!(ours.is_infinite(), "{} where ClickHouse blends infinities", ours);
            } else {
                prop_assert_eq!(ours.to_bits(), theirs.to_bits());
            }
        }
    }

    /// The schedule a model digest merges on.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Schedule {
        /// ClickHouse's: past 2,048 values unmerged, and before a quantile is read.
        ClickHouse,
        /// brrrrr's: at `BUFFER` values unmerged; a quantile reads the values since as they are.
        Brrrrr,
    }

    /// A digest as ClickHouse keeps it: values appended to the centroids, all sorted (stably) when
    /// merged, on either schedule.
    #[derive(Clone)]
    struct Appended {
        centroids: Vec<(f32, f32)>,
        count: f64,
        unmerged: usize,
        schedule: Schedule,
    }

    impl Appended {
        fn new(schedule: Schedule) -> Self {
            Appended { centroids: vec![], count: 0.0, unmerged: 0, schedule }
        }

        fn add(&mut self, x: f32) {
            if x.is_nan() {
                return;
            }
            self.centroids.push((x, 1.0));
            self.count += 1.0;
            self.unmerged += 1;
            if match self.schedule {
                Schedule::ClickHouse => self.unmerged > MAX_CENTROIDS,
                Schedule::Brrrrr => self.unmerged >= BUFFER,
            } {
                self.compress();
            }
        }

        fn compress(&mut self) {
            if self.unmerged > 0 || self.centroids.len() > MAX_CENTROIDS {
                // ClickHouse's stable radix sort of everything, then the merge pass
                self.centroids.sort_by_key(|c| radix_key(c.0));
                self.count = merge(&mut self.centroids, self.count);
                self.unmerged = 0;
            }
            if let Some(count) = compress_brute(&mut self.centroids) {
                self.count = count;
            }
        }

        /// The merged centroids.
        fn merged(&self) -> &[(f32, f32)] {
            &self.centroids[..self.centroids.len() - self.unmerged]
        }

        /// As ClickHouse reads: merged first, in place.
        fn quantile(&mut self, level: f64) -> f32 {
            self.compress();
            match self.schedule {
                Schedule::ClickHouse => clickhouse_interpolate(&self.centroids, self.count, level),
                Schedule::Brrrrr => interpolate(&self.centroids, self.count, level),
            }
        }
    }

    /// ClickHouse's `getImpl` as it is: no rule for equal means.
    fn clickhouse_interpolate(c: &[(f32, f32)], count: f64, level: f64) -> f32 {
        if c.len() == 1 {
            return c[0].0;
        }
        let x = level * count;
        let (mut prev_x, mut sum) = (0f64, 0f32);
        let (mut prev_mean, mut prev_count) = c[0];
        for &(mean, count) in c {
            let current_x = sum as f64 + count as f64 * 0.5;
            if current_x >= x {
                let left = prev_x + 0.5 * (prev_count == 1.0) as u8 as f64;
                let right = current_x - 0.5 * (count == 1.0) as u8 as f64;
                return if x <= left {
                    prev_mean
                } else if x >= right {
                    mean
                } else {
                    let (xf, x1, x2) = (x as f32, left as f32, right as f32);
                    let k = ((xf - x1) / (x2 - x1)) as f64;
                    ((1.0 - k) * prev_mean as f64 + k * mean as f64) as f32
                };
            }
            sum += count;
            (prev_mean, prev_count, prev_x) = (mean, count, current_x);
        }
        c[c.len() - 1].0
    }

    /// Split into merged centroids and unmerged values, the digest adds, merges and reads exactly
    /// as the model with the values appended (same centroids in the same order), and restores as
    /// it was, at every merge's edge.
    #[test]
    fn a_split_digest_is_the_appended_one() {
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let edges = [BUFFER - 1, BUFFER, BUFFER + 1, 2 * BUFFER - 1, 2 * BUFFER, 2 * BUFFER + 1];
        for n in [0usize, 1, 7, 2_048, 2_049, 4_098, 10_000, 50_000].into_iter().chain(edges) {
            for grid in [4u64, 1_000, 1 << 32] {
                let (mut d, mut a) = (TDigest::default(), Appended::new(Schedule::Brrrrr));
                for i in 0..n {
                    let r = next();
                    // ties (a coarse grid), both signs, and the odd infinity, zero sign and NaN
                    let x = match r % 997 {
                        0 => f32::INFINITY,
                        1 => f32::NEG_INFINITY,
                        2 => -0.0,
                        3 => f32::NAN,
                        _ => ((r >> 16) % grid) as f32 - (grid / 3) as f32,
                    };
                    d.add(x);
                    a.add(x);
                    assert!(d.unmerged.len() < BUFFER && d.unmerged.capacity() <= BUFFER);
                    if i % 3_001 == 0 || i + 1 == n || i % BUFFER >= BUFFER - 2 {
                        let unmerged = &a.centroids[a.centroids.len() - a.unmerged..];
                        assert_eq!(d.unmerged.len(), a.unmerged, "{n} values, grid {grid}, at {i}");
                        assert!(d.unmerged.iter().zip(unmerged).all(|(x, c)| x.to_bits() == c.0.to_bits()));
                        assert_eq!(bits(&d.merged), bits(a.merged()));
                        let bytes = postcard::to_allocvec(&d).unwrap();
                        let back: TDigest = postcard::from_bytes(&bytes).unwrap();
                        assert!(back.consistent());
                        assert_eq!(postcard::to_allocvec(&back).unwrap(), bytes);
                    }
                }
                if !d.is_empty() {
                    for level in [0.0, 0.01, 0.5, 0.99, 1.0] {
                        assert_eq!(d.quantile(level).to_bits(), a.quantile(level).to_bits(), "{n}, {grid}, {level}");
                    }
                    d.compress();
                    a.compress();
                    assert_eq!(bits(&d.merged), bits(&a.centroids));
                    assert_eq!(d.count.to_bits(), a.count.to_bits());
                }
            }
        }
    }

    /// A digest a checkpoint of ClickHouse's schedule left with up to 2,048 unmerged values merges
    /// them all with its next value, or its next read: the model fed the same values; its buffer
    /// goes back to `BUFFER`.
    #[test]
    fn a_digest_restored_with_clickhouses_buffer_merges_and_reads() {
        for n in [600, 2_048] {
            let values: Vec<f32> = (0..n).map(|i| ((i * 7_919) % 1_009) as f32 * 0.5).collect();
            let restored = || {
                let d = TDigest { merged: Box::new([]), unmerged: values.clone(), count: n as f64 };
                let mut a = Appended::new(Schedule::Brrrrr);
                a.centroids = values.iter().map(|&x| (x, 1.0)).collect();
                (a.count, a.unmerged) = (n as f64, n);
                assert!(d.consistent());
                (d, a)
            };
            let (mut d, mut a) = restored();
            d.add(1.5);
            a.add(1.5);
            assert!(d.unmerged.is_empty() && d.unmerged.capacity() <= BUFFER, "{n}: {}", d.unmerged.capacity());
            assert_eq!(bits(&d.merged), bits(a.merged()));
            let (mut d, mut a) = restored();
            for level in [0.0, 0.3, 0.5, 1.0] {
                assert_eq!(d.quantile(level).to_bits(), a.quantile(level).to_bits());
            }
            assert!(d.unmerged.capacity() <= BUFFER);
            assert_eq!(bits(&d.merged), bits(&a.centroids));
        }
    }

    /// With nothing unmerged, a digest's centroids are read as they are (a second merge pass would
    /// merge them further), at 2,048 of them too; more than 2,048, as a checkpoint could hold, are
    /// merged first, as `compress` would.
    #[test]
    fn merged_centroids_are_read_as_they_are_unless_too_many() {
        let mut d = TDigest::default();
        for i in 0..5_000u32 {
            d.add(((i * 7_919) % 1_009) as f32 * 0.5);
        }
        d.compress();
        assert!(d.unmerged.is_empty() && d.merged.len() > 100);
        let full: Vec<(f32, f32)> = (0..MAX_CENTROIDS).map(|i| (i as f32, (1 + i % 7) as f32)).collect();
        let full_count = full.iter().map(|c| c.1 as f64).sum();
        let mut at_limit = TDigest { merged: full.clone().into(), unmerged: vec![], count: full_count };
        for level in [0.0, 0.1, 0.37, 0.5, 0.9, 1.0] {
            assert_eq!(d.quantile(level).to_bits(), interpolate(&d.merged, d.count, level).to_bits(), "{level}");
            assert_eq!(at_limit.quantile(level).to_bits(), interpolate(&full, full_count, level).to_bits(), "{level}");
        }
        let over: Vec<(f32, f32)> = (0..3_000).map(|i| (i as f32, 1.0)).collect();
        let oversized = TDigest { merged: over.into(), unmerged: vec![], count: 3_000.0 };
        let mut merged = oversized.clone();
        merged.compress();
        assert!(merged.merged.len() < 3_000);
        for level in [0.0, 0.25, 0.5, 1.0] {
            let mut read = oversized.clone();
            assert_eq!(read.quantile(level).to_bits(), interpolate(&merged.merged, merged.count, level).to_bits());
        }
    }

    /// Between two equal means, a quantile is that mean: where ClickHouse's blend weight rounds to
    /// 0 in Float32 between two infinities (0 * inf is NaN), the infinity.
    #[test]
    fn two_infinities_blend_into_themselves_not_nan() {
        for x in [f32::INFINITY, f32::NEG_INFINITY] {
            let c = [(x, 2.0), (x, 2.0)];
            let level = (1.0 + 1e-12) / 4.0;
            assert!(clickhouse_interpolate(&c, 4.0, level).is_nan(), "ClickHouse's blend");
            assert_eq!(interpolate(&c, 4.0, level), x);
        }
        // equal finite means: ClickHouse's bits
        for level in [0.26, 0.3, 0.5, 0.74] {
            let c = [(3.25f32, 2.0), (3.25, 2.0)];
            assert_eq!(interpolate(&c, 4.0, level).to_bits(), clickhouse_interpolate(&c, 4.0, level).to_bits());
        }
    }

    /// A crafted checkpoint whose centroids `compressBrute` all drops (NaN means) reads NaN (a
    /// sink's null), not a panic on no centroids.
    #[test]
    fn a_digest_left_with_no_centroids_reads_nan() {
        let mut d = TDigest { merged: vec![(f32::NAN, 1.0); 3_000].into(), unmerged: vec![], count: 3_000.0 };
        assert!(d.quantile(0.5).is_nan());
        assert!(interpolate(&[], 0.0, 0.5).is_nan());
    }

    /// Between two equal means, a quantile is that mean: two infinities do not blend into NaN.
    #[test]
    fn equal_neighbours_read_as_themselves() {
        for x in [f32::INFINITY, f32::NEG_INFINITY, 3.25, -0.0, f32::MAX] {
            let mut d = TDigest::default();
            (0..3).for_each(|_| d.add(x));
            for level in [0.0, 0.2, 0.3, 0.5, 0.7, 1.0] {
                assert_eq!(d.quantile(level).to_bits(), x.to_bits(), "{x} at {level}");
            }
        }
    }

    /// How far in rank `r` is from the quantile at `level` of `sorted` (ascending, no NaN), as a
    /// fraction of the values: 0 if `r` lies between the values around rank `level * n - 0.5`
    /// (where the interpolation reads), else the distance in ranks to them, over `n`.
    fn rank_error(sorted: &[f32], r: f32, level: f64) -> f64 {
        let n = sorted.len();
        let t = (level * n as f64 - 0.5).clamp(0.0, (n - 1) as f64);
        let (below, at_most) = (sorted.partition_point(|&v| v < r), sorted.partition_point(|&v| v <= r));
        // r is at ranks below..at_most (exclusive) or, between values, at below - 0.5
        let (lo, hi) =
            if at_most > below { (below as f64, (at_most - 1) as f64) } else { (below as f64 - 1.0, below as f64) };
        if t + 1.0 < lo {
            (lo - t - 1.0) / n as f64
        } else if hi + 1.0 < t {
            (t - hi - 1.0) / n as f64
        } else {
            0.0
        }
    }

    /// A distribution: its name, and the value of the `i`th draw of `u` and `z`.
    type Distribution = (&'static str, fn(usize, f64, f64) -> f32);

    /// Distributions as market-data quantiles see them, and the hard cases: (name, value of
    /// the `i`th draw of a uniform `u` in [0, 1) and a normal `z`).
    fn distributions() -> Vec<Distribution> {
        vec![
            ("uniform", |_, u, _| u as f32),
            ("normal", |_, _, z| z as f32),
            ("lognormal (notional)", |_, _, z| (z * 2.0).exp() as f32 * 100.0),
            ("lots (sizes, ties)", |_, u, _| ((1.0 / (u + 1e-3)).floor() * 0.001) as f32),
            ("clustered price", |_, _, z| (60_000.0 + (z * 5.0 * 10.0).round() / 10.0) as f32),
            ("bimodal", |i, _, z| if i % 3 == 0 { (100.0 + z) as f32 } else { (-50.0 + 0.1 * z) as f32 }),
            ("slippage bps", |i, _, z| if i % 3 == 0 { 0.0 } else { (z * 3.0 + 0.5) as f32 }),
            ("ascending", |i, _, _| i as f32 * 0.01),
            ("descending", |i, _, _| -(i as f32) * 0.01),
            ("sawtooth", |i, _, _| if i % 2 == 0 { (i % 1_000) as f32 } else { -((i % 997) as f32) }),
            ("constant", |_, _, _| 42.0),
            ("two values", |i, _, _| if i % 4 == 0 { 1.0 } else { 2.0 }),
            ("outliers", |i, _, z| if i % 100 == 0 { 1e9 } else { z as f32 }),
        ]
    }

    /// Every distribution at every size and level: (name, n, level, our rank error, ClickHouse's).
    /// With `premerge_at`, our digest also merges ahead of its read (`Acc::premerge`) once that
    /// share of the values is in, as a window's digest does in its lead (`engine::LEAD`).
    fn rank_errors(sizes: &[usize], premerge_at: Option<f64>) -> Vec<(&'static str, usize, f64, f64, f64)> {
        let levels = [0.0, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1.0];
        let mut out = vec![];
        for (name, f) in distributions() {
            for &n in sizes {
                let mut rng = 0x2545_f491_4f6c_dd1du64 ^ n as u64;
                let mut uniform = move || {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    (rng >> 11) as f64 / (1u64 << 53) as f64
                };
                let values: Vec<f32> = (0..n)
                    .map(|i| {
                        let (u, v) = (uniform(), uniform());
                        let z = (-2.0 * (u + 1e-300).ln()).sqrt() * (std::f64::consts::TAU * v).cos();
                        f(i, u, z)
                    })
                    .collect();
                let (mut d, mut ch) = (TDigest::default(), Appended::new(Schedule::ClickHouse));
                let at = premerge_at.map_or(usize::MAX, |p| (n as f64 * p) as usize);
                values.iter().enumerate().for_each(|(i, &x)| {
                    if i == at {
                        d.premerge();
                    }
                    d.add(x);
                    ch.add(x);
                });
                let mut sorted = values.clone();
                sorted.sort_by(f32::total_cmp);
                ch.compress();
                for level in levels {
                    let ours = rank_error(&sorted, d.quantile(level), level);
                    let theirs = rank_error(&sorted, clickhouse_interpolate(&ch.centroids, ch.count, level), level);
                    out.push((name, n, level, ours, theirs));
                }
            }
        }
        out
    }

    /// Accuracy against the exact quantiles, for every distribution, size and level: each result
    /// within 0.5% of the values in rank (the t-digest's epsilon is 1%) and within 0.2% of
    /// ClickHouse's own error on the same values (0.5% for heavily tied sizes, which land on one
    /// side of a tie or the other, either digest; the largest gaps measured are 0.12% and 0.45%);
    /// per distribution, the mean error within 2.5 times ClickHouse's (+0.005%); below `BUFFER`
    /// values, ClickHouse's error exactly; up to 200, within a rank of the exact (`rank_error`'s
    /// tolerance: no merge merges anything).
    #[test]
    fn quantiles_are_as_close_in_rank_as_clickhouses() {
        within_bounds(None);
    }

    /// The same bounds for a digest merged ahead of its read, as in its window's lead, after
    /// `PREMERGES` of its values.
    #[test]
    fn premerged_quantiles_are_as_close_in_rank_as_clickhouses() {
        for p in PREMERGES {
            within_bounds(Some(p));
        }
    }

    /// Where a window's digest merges ahead of its close, as a share of its values: half, 87% (a
    /// 15s window's last 2 s at an even rate), 99% and 99.9% (a busy group taking values on).
    const PREMERGES: [f64; 4] = [0.5, 0.87, 0.99, 0.999];

    fn within_bounds(premerge_at: Option<f64>) {
        let sizes = [1, 2, 10, 100, 199, 200, 511, 512, 513, 1_000, 2_048, 2_049, 5_000, 20_000];
        let errors = rank_errors(&sizes, premerge_at);
        assert_eq!(errors.len(), 13 * 14 * 13);
        for &(name, n, level, ours, theirs) in &errors {
            let margin = if name.starts_with("lots") { 0.005 } else { 0.002 };
            assert!(ours <= 0.005, "{premerge_at:?} {name}, {n} values, level {level}: rank error {ours}");
            assert!(
                ours <= theirs + margin,
                "{premerge_at:?} {name}, {n} values, level {level}: {ours} against ClickHouse's {theirs}"
            );
            if n < BUFFER {
                assert_eq!(ours, theirs, "{premerge_at:?} {name}, {n} values, level {level}: not ClickHouse's");
            }
            if n <= 200 {
                assert_eq!(ours, 0.0, "{premerge_at:?} {name}, {n} values, level {level}: not within a rank");
            }
        }
        for (name, _) in distributions() {
            let e: Vec<_> = errors.iter().filter(|e| e.0 == name).collect();
            let (ours, theirs) = (e.iter().map(|e| e.3).sum::<f64>(), e.iter().map(|e| e.4).sum::<f64>());
            let (ours, theirs) = (ours / e.len() as f64, theirs / e.len() as f64);
            assert!(
                ours <= 2.5 * theirs + 0.000_05,
                "{premerge_at:?} {name}: mean rank error {ours} against ClickHouse's {theirs}"
            );
        }
    }

    /// The accuracy table for larger windows (run by hand, ~5 min in release): per distribution,
    /// the largest and mean rank error, ours, ours merged ahead (the worst of `PREMERGES`) and
    /// ClickHouse's.
    /// `cargo test --release -p brrrrr-core --lib -- --ignored --nocapture rank_error_table`
    #[test]
    #[ignore]
    fn rank_error_table() {
        let sizes = [1_000, 5_000, 20_000, 100_000, 1_000_000];
        let errors = rank_errors(&sizes, None);
        let ahead: Vec<_> = PREMERGES.iter().map(|&p| rank_errors(&sizes, Some(p))).collect();
        eprintln!(
            "{:<22} {:>10} {:>10} {:>10} {:>11} {:>11} {:>11}",
            "distribution", "max ours", "max ahead", "max CH", "mean ours", "mean ahead", "mean CH"
        );
        type E = (&'static str, usize, f64, f64, f64);
        let of = |errors: &[E], name: &str| -> (f64, f64, f64, f64) {
            let e: Vec<_> = errors.iter().filter(|e| e.0 == name).collect();
            let max = |f: fn(&&E) -> f64| e.iter().map(f).fold(0.0, f64::max);
            let mean = |f: fn(&&E) -> f64| e.iter().map(f).sum::<f64>() / e.len() as f64;
            (max(|e| e.3), max(|e| e.4), mean(|e| e.3), mean(|e| e.4))
        };
        for (name, _) in distributions() {
            let (max, max_ch, mean, mean_ch) = of(&errors, name);
            let worst = ahead.iter().map(|a| of(a, name)).fold((0.0, 0.0), |w, a| (a.0.max(w.0), a.2.max(w.1)));
            eprintln!(
                "{name:<22} {:>9.4}% {:>9.4}% {:>9.4}% {:>10.5}% {:>10.5}% {:>10.5}%",
                100.0 * max,
                100.0 * worst.0,
                100.0 * max_ch,
                100.0 * mean,
                100.0 * worst.1,
                100.0 * mean_ch
            );
        }
    }

    /// `quantile_cont` keeps a window's first `CONT_EXACT` values as they arrive, in a buffer
    /// that doubles up to exactly that many; the next value moves them all into the digest
    /// `quantile_t_digest` would hold of the same values in the same order. A NaN is no value:
    /// it neither counts nor moves a full buffer.
    #[test]
    fn cont_keeps_its_first_values_exactly_then_a_digest() {
        let x = |i: usize| ((i * 7_919) % 10_007) as f64 * 1.37 - 4_000.0;
        let mut c = Cont::default();
        for i in 0..CONT_EXACT {
            c.add(f64::NAN);
            c.add(x(i));
            let Cont::Exact(v) = &c else { panic!("{} values are exact", i + 1) };
            assert_eq!(v.len(), i + 1);
            assert_eq!(v.capacity(), (i + 1).next_power_of_two().max(4), "{} values", i + 1);
            assert!(v.iter().enumerate().all(|(j, v)| *v == x(j)), "in arrival order");
            assert!(c.consistent());
        }
        c.add(f64::NAN);
        assert!(matches!(&c, Cont::Exact(v) if v.len() == CONT_EXACT), "a NaN does not fill the buffer");
        let mut d = TDigest::default();
        for i in 0..CONT_EXACT + 40 {
            if i >= CONT_EXACT {
                c.add(x(i));
                c.add(f64::NAN);
            }
            d.add(x(i) as f32);
            if i >= CONT_EXACT {
                let Cont::Digest(got) = &c else { panic!("{} values are a digest", i + 1) };
                assert_eq!(postcard::to_allocvec(got).unwrap(), postcard::to_allocvec(&d).unwrap());
                assert_eq!(got.count, (i + 1) as f64);
                assert!(c.consistent());
            }
        }
        // a buffer restored at any length grows to exactly CONT_EXACT
        for start in [0usize, 1, 5, 100, CONT_EXACT - 1] {
            let mut v = vec![1.0; start];
            v.shrink_to_fit();
            let mut c = Cont::Exact(v);
            (start..CONT_EXACT).for_each(|i| c.add(i as f64));
            let Cont::Exact(v) = &c else { panic!("exact") };
            assert_eq!((v.len(), v.capacity()), (CONT_EXACT, CONT_EXACT), "from {start}");
        }
    }

    /// What `check_against` refuses of a restored `quantile_cont`: more exact values than `add`
    /// keeps, a NaN among them (sorted last, it would be the largest value), and an
    /// inconsistent digest.
    #[test]
    fn cont_refuses_what_it_never_keeps() {
        assert!(Cont::Exact(vec![]).consistent());
        assert!(Cont::Exact(vec![1.0; CONT_EXACT]).consistent());
        assert!(!Cont::Exact(vec![1.0; CONT_EXACT + 1]).consistent());
        assert!(Cont::Exact(vec![1.0, f64::INFINITY, -0.0]).consistent());
        assert!(!Cont::Exact(vec![1.0, f64::NAN]).consistent());
        let d = |n: usize| Cont::Digest(TDigest { merged: Box::new([]), unmerged: vec![1.0; n], count: n as f64 });
        assert!(d(MAX_CENTROIDS).consistent());
        assert!(!d(MAX_CENTROIDS + 1).consistent());
    }

    /// The exact values' quantile is DuckDB's Interpolator: the value at rank level * (n - 1),
    /// between two values `lo * (1 - d) + hi * d`. A rank that is a value's own is that value
    /// with no arithmetic: an infinity times 0 would be NaN.
    #[test]
    fn cont_interpolates_between_the_two_values_around_the_rank() {
        let q = |xs: &[f64], level: f64| Cont::Exact(xs.to_vec()).quantile(level);
        assert_eq!(q(&[], 0.5), None);
        assert_eq!(q(&[7.0], 0.0), Some(7.0));
        assert_eq!(q(&[7.0], 0.95), Some(7.0));
        assert_eq!(q(&[132.7346, 16.4054], 0.5), Some(74.57));
        // ranks 0, 1, 2, 3: level 0.95 is rank 2.85, 0.85 of the way from 30 to 40
        let xs = [40.0, 10.0, 30.0, 20.0];
        assert_eq!(q(&xs, 0.95), Some(30.0 * (1.0 - 0.8500000000000001) + 40.0 * 0.8500000000000001));
        assert_eq!(q(&xs, 0.5), Some(25.0));
        assert_eq!(q(&xs, 1.0 / 3.0), Some(20.0));
        assert_eq!((q(&xs, 0.0), q(&xs, 1.0)), (Some(10.0), Some(40.0)));
        // negative and mixed signs: ordered as numbers, not by magnitude
        assert_eq!(q(&[-1.0, 2.0, -5.0], 0.5), Some(-1.0));
        assert_eq!(q(&[2.0, -1.0], 0.5), Some(0.5));
        // DuckDB's arithmetic, not lo + (hi - lo) * d: they differ in the last bit here
        let (lo, hi, d) = (0.47, 0.85, 0.75);
        assert_ne!(lo * (1.0 - d) + hi * d, lo + (hi - lo) * d);
        assert_eq!(q(&[hi, lo], d), Some(lo * (1.0 - d) + hi * d));
        // infinities: at its own rank an infinity is itself; between values DuckDB's blend
        assert_eq!(q(&[f64::INFINITY], 0.5), Some(f64::INFINITY));
        assert_eq!(q(&[1.0, f64::INFINITY], 1.0), Some(f64::INFINITY));
        assert_eq!(q(&[f64::NEG_INFINITY, 1.0, 2.0], 0.0), Some(f64::NEG_INFINITY));
        assert_eq!(q(&[1.0, f64::INFINITY], 0.5), Some(f64::INFINITY));
        assert!(q(&[f64::NEG_INFINITY, f64::INFINITY], 0.5).unwrap().is_nan());
        // read twice (a median and a p95 of one state), and added to after a read
        let mut c = Cont::Exact(vec![3.0, 1.0, 2.0]);
        assert_eq!((c.quantile(0.5), c.quantile(1.0), c.quantile(0.5)), (Some(2.0), Some(3.0), Some(2.0)));
        c.add(0.0);
        assert_eq!(c.quantile(0.5), Some(1.5));
    }

    /// The digest read as PERCENTILE_CONT: a centroid holds `count` consecutive ranks and its
    /// mean stands at their middle; a rank between two middles is the blend of their means, one
    /// before the first middle or after the last is that centroid's mean.
    #[test]
    fn a_digest_is_read_as_percentile_cont_of_its_centroids() {
        let digest = |c: &[(f32, f32)]| TDigest {
            merged: c.into(),
            unmerged: vec![],
            count: c.iter().map(|c| c.1 as f64).sum(),
        };
        // single values: PERCENTILE_CONT itself
        let mut d = digest(&[(10.0, 1.0), (20.0, 1.0), (30.0, 1.0), (40.0, 1.0)]);
        assert_eq!(d.quantile_cont(0.5), 25.0);
        assert_eq!(d.quantile_cont(0.95), 30.0 * (1.0 - 0.8500000000000001) + 40.0 * 0.8500000000000001);
        assert_eq!(d.quantile_cont(1.0 / 3.0), 20.0);
        assert_eq!((d.quantile_cont(0.0), d.quantile_cont(1.0)), (10.0, 40.0));
        // where quantile_t_digest has the lower of the two middle values
        assert_eq!(d.quantile(0.5), 20.0);
        // ranks 0 | 1 to 4, middle 2.5 | 5
        let mut d = digest(&[(10.0, 1.0), (20.0, 4.0), (30.0, 1.0)]);
        assert_eq!(d.quantile_cont(0.5), 20.0); // rank 2.5
        assert_eq!(d.quantile_cont(0.3), 16.0); // rank 1.5: 0.6 of the way from rank 0 to 2.5
        assert_eq!(d.quantile_cont(0.9), 28.0); // rank 4.5: 0.8 of the way from rank 2.5 to 5
        assert_eq!((d.quantile_cont(0.0), d.quantile_cont(1.0)), (10.0, 30.0));
        // ranks 0 to 2, middle 1 | 3 to 5, middle 4: the ends are past the outer middles
        let mut d = digest(&[(10.0, 3.0), (20.0, 3.0)]);
        assert_eq!((d.quantile_cont(0.0), d.quantile_cont(0.1), d.quantile_cont(0.2)), (10.0, 10.0, 10.0));
        assert_eq!(d.quantile_cont(0.5), 15.0);
        assert_eq!((d.quantile_cont(0.8), d.quantile_cont(0.9), d.quantile_cont(1.0)), (20.0, 20.0, 20.0));
        // one centroid, of one value or many
        assert_eq!(digest(&[(7.0, 1.0)]).quantile_cont(0.95), 7.0);
        assert_eq!(digest(&[(7.0, 9.0)]).quantile_cont(0.5), 7.0);
        // an infinity at its own rank is itself (times 0 it would be NaN), and a rank that is a
        // centroid's own is that centroid's mean, not a blend with the next one (an infinity)
        let mut d = digest(&[(1.0, 1.0), (f32::INFINITY, 1.0), (f32::INFINITY, 1.0)]);
        assert_eq!((d.quantile_cont(0.5), d.quantile_cont(1.0)), (f64::INFINITY, f64::INFINITY));
        assert_eq!(digest(&[(1.0, 1.0), (2.0, 1.0), (f32::INFINITY, 1.0)]).quantile_cont(0.5), 2.0);
        // unmerged values are merged first, and a Float32 mean is read as the Float64 it is
        let mut d = TDigest::default();
        [0.3f32, 0.1, 0.2].iter().for_each(|x| d.add(*x));
        assert_eq!(d.quantile_cont(0.75), 0.2f32 as f64 * 0.5 + 0.3f32 as f64 * 0.5);
        // through the aggregate: empty is NULL, anything else a Float64
        let mut acc = Acc::Cont { level: 0.5, values: Box::new(Cont::Digest(TDigest::default())) };
        assert_eq!(acc.result(), Value::Null);
        acc.add(Value::Int(3));
        acc.add(Value::F32(4.5));
        assert_eq!(acc.result(), Value::F64(3.75));
    }

    /// A restored digest with more unmerged values than `add` ever keeps is refused.
    #[test]
    fn a_digest_with_too_many_unmerged_values_is_inconsistent() {
        let d = |n: usize| TDigest { merged: Box::new([]), unmerged: vec![1.0; n], count: n as f64 };
        assert!(d(MAX_CENTROIDS).consistent());
        assert!(!d(MAX_CENTROIDS + 1).consistent());
    }
}

/// How many trades sharing a window's newest time a sequence aggregate (`run_structure`,
/// `updownticks`, `trade_returns`, `distinct_stats`) sorts by its own tie order before folding
/// them anyway: a safety cap. Their input arrives sorted by time (a view's `ORDER BY` with
/// `SETTINGS order_hold_ms`, ADR-0013); only the trades of the newest time wait, since each
/// metric breaks ties its own way (by the id as a number or as text, or by exchange time). A
/// busy market's largest burst of trades sharing a time is a few hundred.
pub const SEQUENCE_REORDER: usize = 2048;

/// Trades older than one a sequence aggregate already folded (an unsorted input, or more than
/// `SEQUENCE_REORDER` sharing a time): folded where they arrived, so their window's sequence
/// metrics differ from sorting it.
pub static SEQUENCE_OUT_OF_ORDER: AtomicU64 = AtomicU64::new(0);

/// A metric over a window's trades in `(time, id)` order, as a batch engine computes with
/// LAG over a sorted window: trades arrive sorted by time, those of the newest time wait in a
/// short buffer sorted by the metric's tie order, and the rest are folded into a fixed-size
/// summary. A trade older than the newest folded one is folded where it arrives and counted.
#[derive(Clone, Debug, Default)]
pub struct Sequence<I, S: Fold> {
    pending: VecDeque<((i64, I), S::Trade)>,
    /// The key of the last folded trade.
    folded: Option<(i64, I)>,
    /// The latest trade's time.
    newest: i64,
    summary: S,
}

// Serialized as a tuple of its fields, which postcard writes as it writes the struct: a
// generic struct is one name for serde-reflection, which cannot then trace the checkpoint
// layout of both instances.
impl<I: Serialize, S: Fold> Serialize for Sequence<I, S> {
    fn serialize<Se: serde::Serializer>(&self, s: Se) -> Result<Se::Ok, Se::Error> {
        (&self.pending, &self.folded, self.newest, &self.summary).serialize(s)
    }
}

impl<'de, I: serde::de::DeserializeOwned, S: Fold> Deserialize<'de> for Sequence<I, S> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (pending, folded, newest, summary) = Deserialize::deserialize(d)?;
        Ok(Sequence { pending, folded, newest, summary })
    }
}

/// A summary of which the fold of a later run of trades can be merged in (ADR-0017).
pub trait Merge {
    fn merge(&mut self, later: &Self);
}

pub trait Fold: Clone + Default + std::fmt::Debug + Serialize + serde::de::DeserializeOwned {
    type Trade: Clone + std::fmt::Debug + Serialize + serde::de::DeserializeOwned;
    fn fold(&mut self, t: &Self::Trade);
    /// After the window's last trade.
    fn done(&mut self) {}
}

impl<I: Ord + Clone, S: Fold> Sequence<I, S> {
    fn add(&mut self, key: (i64, I), t: S::Trade) {
        if self.folded.as_ref().is_some_and(|f| key < *f) {
            SEQUENCE_OUT_OF_ORDER.fetch_add(1, Relaxed);
        }
        if self.pending.len() == SEQUENCE_REORDER {
            // full: fold the earliest of the waiting trades and this one (the buffer never grows
            // past SEQUENCE_REORDER, nor its allocation)
            if self.pending.front().is_some_and(|(k, _)| key < *k) {
                self.fold(key, &t);
                return;
            }
            let (k, t) = self.pending.pop_front().expect("full");
            self.fold(k, &t);
        }
        self.newest = self.newest.max(key.0);
        // after equal keys: ties keep their arrival order
        if self.pending.back().is_none_or(|(k, _)| *k <= key) {
            self.pending.push_back((key, t));
        } else {
            let at = self.pending.partition_point(|(k, _)| *k <= key);
            self.pending.insert(at, (key, t));
        }
        while self.pending.front().is_some_and(|((t, _), _)| *t < self.newest) {
            let (k, t) = self.pending.pop_front().expect("not empty");
            self.fold(k, &t);
        }
        // a burst sharing one time grows the buffer; give its memory back once a later time has
        // drained it, leaving only its own first trade (every group of every window keeps a
        // buffer), never while it is still growing
        if self.pending.capacity() > 64 && self.pending.len() == 1 {
            self.pending.shrink_to(64);
        }
    }

    /// `add` of trades `0..n` in key order, the first at or after every trade pending or folded
    /// (`time(i)`, `key(i)`, `trade(i)`): the same state, with the keys made only of the trades
    /// left pending and of the last folded. Every trade but those of the newest time is folded in
    /// order; of those, the latest `SEQUENCE_REORDER` wait, as `add` leaves them.
    pub(crate) fn add_ordered(
        &mut self,
        n: usize,
        time: impl Fn(usize) -> i64,
        key: impl Fn(usize) -> I,
        trade: impl Fn(usize) -> S::Trade,
    ) {
        if n == 0 {
            return;
        }
        let newest = time(n - 1).max(self.newest);
        // trades of the newest time, from the end: the new ones, then the pending ones
        let tail_new = (0..n).rev().take_while(|&i| time(i) == newest).count();
        let tail =
            tail_new + if tail_new == n { self.pending.iter().filter(|((t, _), _)| *t == newest).count() } else { 0 };
        let keep = tail.min(SEQUENCE_REORDER);
        let mut fold = self.pending.len() + n - keep;
        while fold > 0 {
            let Some((k, t)) = self.pending.pop_front() else { break };
            self.fold(k, &t);
            fold -= 1;
        }
        for i in 0..fold {
            self.summary.fold(&trade(i));
        }
        if fold > 0 {
            self.folded = self.folded.take().max(Some((time(fold - 1), key(fold - 1))));
        }
        for i in fold..n {
            self.pending.push_back(((time(i), key(i)), trade(i)));
        }
        self.newest = newest;
    }

    /// The latest key added, pending or folded: an ordered run must start at or after it.
    pub(crate) fn last_key(&self) -> Option<&(i64, I)> {
        self.pending.back().map(|(k, _)| k).or(self.folded.as_ref())
    }

    /// Folds a trade into the summary. `folded` only moves forward: a trade folded out of order
    /// (older than one already folded) must not lower the key later trades are counted against.
    fn fold(&mut self, key: (i64, I), t: &S::Trade) {
        self.summary.fold(t);
        self.folded = self.folded.take().max(Some(key));
    }

    /// At most `SEQUENCE_REORDER` trades waiting, in key order: `add` never grows the buffer
    /// past it only because it is never past it.
    fn consistent(&self) -> bool {
        self.pending.len() <= SEQUENCE_REORDER
            && self.pending.iter().zip(self.pending.iter().skip(1)).all(|(a, b)| a.0 <= b.0)
    }

    /// `later`, a sequence of the trades after this one's (seeded by `Acc::seeded`, every one
    /// of a later time than this one's newest) folded in: this one's waiting trades folded,
    /// then `later`'s summary merged, its waiting trades this one's.
    fn merge(&mut self, later: &Sequence<I, S>)
    where
        S: Merge,
    {
        while let Some((k, t)) = self.pending.pop_front() {
            self.fold(k, &t);
        }
        self.summary.merge(&later.summary);
        self.pending.clone_from(&later.pending);
        self.newest = self.newest.max(later.newest);
        self.folded = self.folded.take().max(later.folded.clone());
    }

    /// The summary with every waiting trade folded in (the state itself is left as it is).
    fn finish(&self) -> S {
        let mut s = self.summary.clone();
        self.pending.iter().for_each(|(_, t)| s.fold(t));
        s.done();
        s
    }
}

/// impact/final.sql's large_trade_reversal: the window's trades in order, and at its end the mean
/// return `k` trades ahead within the window (`LEAD(price, k) / price - 1`) of the trades whose
/// notional (|size * price|) is at least the window's `level` quantile of them
/// (`PERCENTILE_CONT`, exact); NULL without one.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Reversal {
    k: u64,
    level: f64,
    /// (price, notional) of each trade, in order.
    trades: Vec<(f64, f64)>,
}

impl Fold for Reversal {
    type Trade = (f64, f64);
    fn fold(&mut self, &(price, size): &(f64, f64)) {
        self.trades.push((price, (size * price).abs()));
    }
}

impl Reversal {
    fn result(&self) -> Value {
        let n = self.trades.len();
        if n == 0 {
            return Value::Null;
        }
        let mut notionals: Vec<f64> = self.trades.iter().map(|t| t.1).collect();
        notionals.sort_unstable_by(f64::total_cmp);
        // DuckDB's Interpolator: RN = (n - 1) * q, between floor(RN) and ceil(RN)
        let rank = (n - 1) as f64 * self.level;
        let p = blend(notionals[rank.floor() as usize], notionals[rank.ceil() as usize], rank - rank.floor());
        let (mut sum, mut count) = (0.0, 0u64);
        for (i, &(price, notional)) in self.trades.iter().enumerate().take(n.saturating_sub(self.k as usize)) {
            if notional >= p && price != 0.0 {
                sum += self.trades[i + self.k as usize].0 / price - 1.0;
                count += 1;
            }
        }
        if count == 0 {
            Value::Null
        } else {
            Value::F64(sum / count as f64)
        }
    }
}

/// run_structure/final.sql: runs of same-side trades, flips between them.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Runs {
    /// The previous trade's side (buy) and price.
    last: Option<(bool, f64)>,
    run: u64,
    /// Per side (0 sell, 1 buy): longest run, total length and number of runs, current run
    /// included once `finish` closes it.
    max: [u64; 2],
    len: [u64; 2],
    runs: [u64; 2],
    flips: u64,
    trades: u64,
    /// Sum and count of |price / previous price - 1| over flips with a non-zero previous price.
    change: f64,
    changes: u64,
}

impl Runs {
    fn close(&mut self, buy: bool) {
        let i = buy as usize;
        self.max[i] = self.max[i].max(self.run);
        self.len[i] += self.run;
        self.runs[i] += 1;
    }
}

impl Fold for Runs {
    type Trade = (bool, f64);
    fn fold(&mut self, &(buy, price): &(bool, f64)) {
        self.trades += 1;
        match self.last {
            Some((b, _)) if b == buy => self.run += 1,
            Some((b, prev)) => {
                self.close(b);
                self.flips += 1;
                if prev != 0.0 {
                    self.change += (price / prev - 1.0).abs();
                    self.changes += 1;
                }
                self.run = 1;
            }
            None => self.run = 1,
        }
        self.last = Some((buy, price));
    }

    fn done(&mut self) {
        if let Some((b, _)) = self.last {
            self.close(b);
        }
    }
}

/// updownticks/create_ticks.sql: each trade against the previous one's price.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Ticks {
    last: Option<f64>,
    /// Up, down, unchanged: count and size.
    n: [u64; 3],
    vol: [f64; 3],
}

impl Fold for Ticks {
    type Trade = (f64, f64);
    fn fold(&mut self, &(price, size): &(f64, f64)) {
        if let Some(prev) = self.last {
            // DuckDB's order: NaN above every number and equal to itself
            let k = match (price.is_nan(), prev.is_nan()) {
                (true, false) => 0,
                (false, true) => 1,
                _ if price > prev => 0,
                _ if price < prev => 1,
                _ => 2,
            };
            self.n[k] += 1;
            self.vol[k] += size;
        }
        self.last = Some(price);
    }
}

/// Count, mean and sample variance of a series, from sums of each value's distance from the
/// first (as `ShiftedMoments`): DuckDB's AVG, VAR_SAMP and STDDEV_SAMP, NULL where it is.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Shifted {
    n: u64,
    shift: f64,
    s1: f64,
    s2: f64,
}

impl Shifted {
    fn add(&mut self, x: f64) {
        if self.n == 0 {
            self.shift = x;
        }
        let d = x - self.shift;
        self.n += 1;
        self.s1 += d;
        self.s2 += d * d;
    }

    fn mean(&self) -> Value {
        match self.n {
            0 => Value::Null,
            n => Value::F64(self.shift + self.s1 / n as f64),
        }
    }

    fn var(&self) -> Option<f64> {
        let n = self.n as f64;
        (self.n > 1).then(|| ((self.s2 - self.s1 * self.s1 / n) / (n - 1.0)).max(0.0))
    }

    fn stddev(&self) -> Value {
        self.var().map_or(Value::Null, |v| Value::F64(v.sqrt()))
    }

    /// `later`'s values added, its sums moved onto this one's shift: `x - a = (x - b) + (b - a)`.
    fn merge(&mut self, later: &Shifted) {
        if later.n == 0 {
            return;
        }
        if self.n == 0 {
            *self = later.clone();
            return;
        }
        let (d, n) = (later.shift - self.shift, later.n as f64);
        self.s1 += later.s1 + n * d;
        self.s2 += later.s2 + 2.0 * d * later.s1 + n * d * d;
        self.n += later.n;
    }
}

/// Trade-return metrics over a window's trades in order: each trade's return on the previous one, `price / NULLIF(LAG(price), 0) - 1`.
/// A fixed-size summary, whatever the window's length.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Returns {
    first: Option<f64>,
    last: Option<f64>,
    /// Sum of squared returns (flow's short_horizon_realized_vol, its square).
    sq: f64,
    /// Co-moment of (return, signed size) over the trades with a return (COVAR_SAMP), Welford.
    cn: f64,
    cmr: f64,
    cms: f64,
    crs: f64,
    /// Signed size over every trade (VAR_SAMP).
    size: Shifted,
    /// Buy returns, and sell returns negated (AVG of each).
    buy: (f64, u64),
    sell: (f64, u64),
}

impl Fold for Returns {
    type Trade = (bool, f64, f64);
    fn fold(&mut self, &(buy, price, size): &(bool, f64, f64)) {
        if let Some(prev) = self.last.filter(|p| *p != 0.0) {
            let r = price / prev - 1.0;
            self.sq += r * r;
            let signed = if buy { size } else { -size };
            self.cn += 1.0;
            let (dr, ds) = (r - self.cmr, signed - self.cms);
            self.cmr += dr / self.cn;
            self.cms += ds / self.cn;
            self.crs += dr * (signed - self.cms);
            match buy {
                true => self.buy = (self.buy.0 + r, self.buy.1 + 1),
                false => self.sell = (self.sell.0 - r, self.sell.1 + 1),
            }
        }
        self.size.add(if buy { size } else { -size });
        self.first.get_or_insert(price);
        self.last = Some(price);
    }
}

impl Merge for Returns {
    /// `later` folded from a summary seeded with this one's last price (`Acc::seeded`): its first
    /// return is on this one's last trade. The co-moment as Chan et al. combine two.
    fn merge(&mut self, later: &Returns) {
        self.sq += later.sq;
        let (na, nb) = (self.cn, later.cn);
        if nb > 0.0 {
            let n = na + nb;
            let (dr, ds) = (later.cmr - self.cmr, later.cms - self.cms);
            self.crs += later.crs + dr * ds * na * nb / n;
            self.cmr += dr * nb / n;
            self.cms += ds * nb / n;
            self.cn = n;
        }
        self.size.merge(&later.size);
        self.buy = (self.buy.0 + later.buy.0, self.buy.1 + later.buy.1);
        self.sell = (self.sell.0 + later.sell.0, self.sell.1 + later.sell.1);
        if self.first.is_none() {
            self.first = later.first;
        }
        if later.last.is_some() {
            self.last = later.last;
        }
    }
}

impl Returns {
    /// `[realized_vol, covar_samp(return, signed size), var_samp(signed size), buy mean return,
    /// sell mean return (negated), first price, last price]`.
    fn result(self) -> Value {
        let f = |x: Option<f64>| x.map_or(Value::Null, Value::F64);
        let mean = |(s, n): (f64, u64)| f((n > 0).then(|| s / n as f64));
        Value::Array(
            [
                f(self.first.map(|_| self.sq.sqrt())),
                f((self.cn >= 2.0).then(|| self.crs / (self.cn - 1.0))),
                f(self.size.var()),
                mean(self.buy),
                mean(self.sell),
                f(self.first),
                f(self.last),
            ]
            .into(),
        )
    }
}

/// A series in order, as a batch engine reads a forward-filled series with LAG over the window:
/// its values where they differ from the previous row's (`x IS DISTINCT FROM LAG(x)`, so the
/// first row always counts), and its changes on the previous row, `(x - prev) / NULLIF(prev, 0)`
/// where `x != prev`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Distinct {
    /// The previous row's value, once there is one.
    prev: Option<Option<f64>>,
    values: Shifted,
    changes: Shifted,
}

impl Fold for Distinct {
    type Trade = Option<f64>;
    fn fold(&mut self, x: &Option<f64>) {
        // DuckDB's order: NaN equal to itself (a NULL is never counted, so which NULLs are
        // "distinct" does not matter)
        let same = match (self.prev, x) {
            (Some(Some(p)), Some(x)) => p == *x || (p.is_nan() && x.is_nan()),
            _ => false,
        };
        if let (false, Some(x)) = (same, x) {
            self.values.add(*x);
            if let Some(Some(p)) = self.prev.filter(|p| *p != Some(0.0)) {
                self.changes.add((x - p) / p);
            }
        }
        self.prev = Some(*x);
    }
}
