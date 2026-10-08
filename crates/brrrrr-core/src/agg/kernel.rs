//! Aggregates over columns (the historical executor, ADR-0016): a group's rows of a batch
//! added to its accumulator at once, in row order, as a window adds them one at a time
//! (`Window::apply`: two arguments by `add2`, else the first by `add`). A typed loop where the
//! accumulator and the columns are of the kinds pipelines commonly use, the values one by
//! one otherwise; the state after is the same either way, bit for bit.
use super::*;
use crate::column::{Col, Data};

/// The rows of a batch a group takes: a range, or rows picked out in order.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Rows<'a> {
    Range(usize, usize),
    Pick(&'a [usize]),
}

impl Rows<'_> {
    fn len(&self) -> usize {
        match self {
            Rows::Range(a, b) => b - a,
            Rows::Pick(p) => p.len(),
        }
    }

    fn each(&self, f: impl FnMut(usize)) {
        match *self {
            Rows::Range(a, b) => (a..b).for_each(f),
            Rows::Pick(p) => p.iter().copied().for_each(f),
        }
    }

    /// `v`'s values at these rows, in order: a slice's own loop for a range.
    #[inline(always)]
    fn over<T: Copy>(&self, v: &[T], mut f: impl FnMut(T)) {
        match *self {
            Rows::Range(a, b) => v[a..b].iter().for_each(|x| f(*x)),
            Rows::Pick(p) => p.iter().for_each(|&i| f(v[i])),
        }
    }

    /// `v`'s values and NULL marks at these rows, in order.
    #[inline(always)]
    fn over_masked<T: Copy>(&self, v: &[T], nulls: &[bool], mut f: impl FnMut(T, bool)) {
        match *self {
            Rows::Range(a, b) => v[a..b].iter().zip(&nulls[a..b]).for_each(|(x, n)| f(*x, *n)),
            Rows::Pick(p) => p.iter().for_each(|&i| f(v[i], nulls[i])),
        }
    }
}

/// An aggregate's arguments over a batch.
pub(crate) enum Args<'a> {
    /// Its arguments' columns.
    Cols(Vec<&'a Col>),
    /// A two-argument aggregate whose second argument is a tuple (`arg_max(x, (t, id))`): the
    /// first's column and the tuple's items'.
    Keyed(&'a Col, Vec<&'a Col>),
    /// A sequence aggregate: its tuple's items' columns.
    Tuple(Vec<&'a Col>),
}

/// Adds `rows` of `args` to `acc`, as the window's row loop would.
pub(crate) fn add_rows(acc: &mut Acc, args: &Args<'_>, rows: Rows<'_>) {
    if rows.len() == 0 {
        return;
    }
    let done = match args {
        Args::Cols(cols) => typed(acc, cols, rows),
        Args::Keyed(x, key) => keyed(acc, x, key, rows),
        Args::Tuple(items) => sequence(acc, items, rows),
    };
    if done {
        return;
    }
    // the window's own loop, value by value
    match args {
        Args::Cols(cols) => match cols.as_slice() {
            [a, b] => rows.each(|r| acc.add2(a.get(r), b.get(r))),
            cols => rows.each(|r| acc.add(cols.first().map_or(Value::Null, |a| a.get(r)))),
        },
        Args::Keyed(x, key) => rows.each(|r| acc.add2(x.get(r), Value::Array(key.iter().map(|c| c.get(r)).collect()))),
        Args::Tuple(items) => rows.each(|r| acc.add(Value::Array(items.iter().map(|c| c.get(r)).collect()))),
    }
}

/// A column of Float64s: its values and NULL marks (`None`: none NULL).
fn floats(c: &Col) -> Option<(&[f64], Option<&[bool]>)> {
    match &c.data {
        Data::F64(v) => Some((v, c.nulls.as_deref())),
        _ => None,
    }
}

/// `f` over the values of a Float64 column at `rows` that are not NULL, in order.
#[inline(always)]
fn each_float(v: &[f64], nulls: Option<&[bool]>, rows: Rows<'_>, mut f: impl FnMut(f64)) {
    match nulls {
        None => rows.over(v, f),
        Some(n) => rows.over_masked(v, n, |x, null| {
            if !null {
                f(x)
            }
        }),
    }
}

/// A row's SQL truth (`truth`), read in place.
#[inline(always)]
fn truth_at(c: &Col, r: usize) -> Option<bool> {
    match (&c.data, &c.nulls) {
        (Data::Bool(v), None) => Some(v[r]),
        (Data::Bool(v), Some(n)) => (!n[r]).then_some(v[r]),
        _ => crate::expr::truth(&c.get(r)),
    }
}

/// The typed loops: whether `rows` were added.
fn typed(acc: &mut Acc, cols: &[&Col], rows: Rows<'_>) -> bool {
    let max = matches!(acc, Acc::Max(_));
    match (acc, cols) {
        (Acc::Count { n, rows: true }, []) => {
            // NULL or not, every row counts
            *n += rows.len() as u64;
            true
        }
        (Acc::Count { n, rows: false }, [x]) => {
            *n += if x.no_nulls() {
                rows.len() as u64
            } else {
                let mut k = 0;
                rows.each(|r| k += u64::from(!x.is_null(r)));
                k
            };
            true
        }
        (Acc::Sum(s), [x]) if matches!(s, Value::Null | Value::F64(_)) && floats(x).is_some() => {
            let (v, nulls) = floats(x).expect("checked");
            // the sum starts at +0.0 (`sum`), which a NULL row's 0.0 leaves as it is
            let mut t = s.f64().unwrap_or(0.0);
            let mut any = matches!(s, Value::F64(_));
            match nulls {
                None => {
                    rows.over(v, |x| t += x);
                    any = true;
                }
                Some(n) => rows.over_masked(v, n, |x, null| {
                    t += if null { 0.0 } else { x };
                    any |= !null;
                }),
            }
            if any {
                *s = Value::F64(t);
            }
            true
        }
        (Acc::Sum(s), [x]) => sum_choose(s, x, rows),
        (Acc::Avg { sum, n }, [x]) => {
            let Some((v, nulls)) = floats(x) else { return false };
            // in locals: through the references, a store and a load a row
            let (mut s, mut k) = (*sum, *n);
            each_float(v, nulls, rows, |x| {
                s += x;
                k += 1;
            });
            (*sum, *n) = (s, k);
            true
        }
        (Acc::Min(m) | Acc::Max(m), [x]) if matches!(m, Value::Null | Value::F64(_)) && floats(x).is_some() => {
            let (v, nulls) = floats(x).expect("checked");
            let mut cur = m.f64();
            // `compare` of two floats: a NaN is neither less nor greater
            each_float(v, nulls, rows, |x| match cur {
                Some(c) if (max && x > c) || (!max && x < c) => cur = Some(x),
                Some(_) => {}
                None => cur = Some(x),
            });
            if let Some(c) = cur {
                *m = Value::F64(c);
            }
            true
        }
        (Acc::Min(m) | Acc::Max(m), [x]) => {
            // times and integers, without NULLs
            let (Some(v), time) = (x.i64s(), matches!(x.data, Data::Time(_))) else { return false };
            let mut cur = match (&*m, time) {
                (Value::Null, _) => None,
                (Value::Time(t), true) | (Value::Int(t), false) => Some(*t),
                _ => return false,
            };
            // equal integers are the same value: which of them is kept cannot matter
            rows.over(v, |x| cur = Some(cur.map_or(x, |c| if max { c.max(x) } else { c.min(x) })));
            if let Some(c) = cur {
                *m = if time { Value::Time(c) } else { Value::Int(c) };
            }
            true
        }
        (Acc::Latest(l), [x]) => {
            // the last row not NULL
            let last = match (rows, x.no_nulls()) {
                (Rows::Range(_, b), true) => Some(b - 1),
                (Rows::Pick(p), true) => p.last().copied(),
                _ => {
                    let mut last = None;
                    rows.each(|r| {
                        if !x.is_null(r) {
                            last = Some(r)
                        }
                    });
                    last
                }
            };
            if let Some(r) = last {
                *l = x.get(r);
            }
            true
        }
        (Acc::Earliest(e), [x]) => {
            if e.is_null() {
                let first = match (rows, x.no_nulls()) {
                    (Rows::Range(a, _), true) => Some(a),
                    (Rows::Pick(p), true) => p.first().copied(),
                    _ => {
                        let mut first = None;
                        rows.each(|r| {
                            if first.is_none() && !x.is_null(r) {
                                first = Some(r)
                            }
                        });
                        first
                    }
                };
                if let Some(r) = first {
                    *e = x.get(r);
                }
            }
            true
        }
        (Acc::ShiftedMoments { n, shift, s, .. }, [x]) => {
            let Some((v, nulls)) = floats(x) else { return false };
            let (mut k, mut sh, mut t) = (*n, *shift, **s);
            each_float(v, nulls, rows, |x| {
                if k == 0 {
                    sh = x;
                }
                let d = x - sh;
                let d2 = d * d;
                k += 1;
                t[0] += d;
                t[1] += d2;
                t[2] += d2 * d;
                t[3] += d2 * d2;
            });
            (*n, *shift, **s) = (k, sh, t);
            true
        }
        (Acc::Cont { values, .. }, [x]) => {
            let Some((v, nulls)) = floats(x) else { return false };
            each_float(v, nulls, rows, |x| values.add(x));
            true
        }
        (Acc::Twap(tw), [x, at]) => {
            // `add2`'s fold over the rows with a value (times without NULLs only)
            let (Some((v, nulls)), Some(ts)) = (floats(x), ints(at).filter(|_| at.no_nulls())) else { return false };
            let crate::agg::Twap { xt, t, last, sum, n } = &mut **tw;
            let (mut sxt, mut st, mut prev, mut s, mut k) = (*xt, *t, *last, *sum, *n);
            rows.each(|r| {
                if nulls.is_some_and(|m| m[r]) {
                    return;
                }
                let (x, mut now) = (v[r], ts[r]);
                if let Some((p, before)) = prev {
                    now = now.max(before);
                    let d = now.abs_diff(before) as f64;
                    (sxt, st) = (sxt + p * d, st + d);
                }
                prev = Some((x, now));
                (s, k) = (s + x, k + 1);
            });
            (*xt, *t, *last, *sum, *n) = (sxt, st, prev, s, k);
            true
        }
        (Acc::Weighted { xw, w }, [x, y]) => {
            let (Some((a, an)), Some((b, bn))) = (floats(x), floats(y)) else { return false };
            let (mut sxw, mut sw) = (*xw, *w);
            rows.each(|r| {
                if !an.is_some_and(|n| n[r]) && !bn.is_some_and(|n| n[r]) {
                    sxw += a[r] * b[r];
                    sw += b[r];
                }
            });
            (*xw, *w) = (sxw, sw);
            true
        }
        (Acc::Corr(c), [x, y]) => {
            let (Some((a, an)), Some((b, bn))) = (floats(x), floats(y)) else { return false };
            let Corr { n, mx, my, sxx, syy, sxy } = &mut **c;
            rows.each(|r| {
                if !an.is_some_and(|m| m[r]) && !bn.is_some_and(|m| m[r]) {
                    let (x, y) = (a[r], b[r]);
                    *n += 1.0;
                    let (dx, dy) = (x - *mx, y - *my);
                    *mx += dx / *n;
                    *my += dy / *n;
                    *sxx += dx * (x - *mx);
                    *syy += dy * (y - *my);
                    *sxy += dx * (y - *my);
                }
            });
            true
        }
        (Acc::Arg { max, key_x }, [x, key]) => {
            let (Some(k), time) = (ints(key), matches!(key.data, Data::Time(_))) else { return false };
            let nulls = key.nulls.as_deref();
            let best = best_row(*max, rows, |r| (!nulls.is_some_and(|m| m[r])).then(|| k[r]), |a, b| a.cmp(b));
            let Some((b, r)) = best else { return true };
            let b = if time { Value::Time(b) } else { Value::Int(b) };
            let better = if *max { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
            let (key, val) = &mut **key_x;
            if key.is_null() || crate::expr::compare(&b, key) == Some(better) {
                (*key, *val) = (b, x.get(r));
            }
            true
        }
        (Acc::QuantileOf { .. }, _) => true,
        (Acc::If(inner), [cond]) => {
            // `count_if(c)`: the inner aggregate takes a NULL for each row where `c` is true
            let k = count_true(cond, rows);
            match &mut **inner {
                Acc::Count { n, rows: true } => *n += k,
                inner => (0..k).for_each(|_| inner.add(Value::Null)),
            }
            true
        }
        (Acc::If(inner), [x, cond]) => {
            // the rows whose condition is true and whose value is not NULL, into the inner one
            let mut picked = Vec::with_capacity(rows.len());
            match (&cond.data, &cond.nulls, x.no_nulls()) {
                // a condition of two values, a value never NULL: the rows where it holds
                (Data::Bool(v), None, true) => rows.each(|r| {
                    if v[r] {
                        picked.push(r)
                    }
                }),
                _ => rows.each(|r| {
                    if truth_at(cond, r) == Some(true) && !x.is_null(r) {
                        picked.push(r)
                    }
                }),
            }
            add_rows(inner, &Args::Cols(vec![x]), Rows::Pick(&picked));
            true
        }
        _ => false,
    }
}

/// How many of `rows` of `cond` are true: a loop of its own over a Bool column's range.
fn count_true(cond: &Col, rows: Rows<'_>) -> u64 {
    match (&cond.data, &cond.nulls, rows) {
        (Data::Bool(v), None, Rows::Range(a, b)) => v[a..b].iter().map(|x| u64::from(*x)).sum(),
        (Data::Bool(v), Some(n), Rows::Range(a, b)) => {
            v[a..b].iter().zip(&n[a..b]).map(|(x, null)| u64::from(*x & !*null)).sum()
        }
        _ => {
            let mut k = 0;
            rows.each(|r| k += u64::from(truth_at(cond, r) == Some(true)));
            k
        }
    }
}

/// The values of an integer or time column (NULLs as placeholders), if it is one.
fn ints(c: &Col) -> Option<&[i64]> {
    match &c.data {
        Data::Int(v) | Data::Time(v) => Some(v),
        _ => None,
    }
}

/// The row of the best key at `rows` (greatest with `max`, else least; the first of equal
/// ones, as `add2` keeps the first), skipping rows without one.
fn best_row<K: Copy>(
    max: bool,
    rows: Rows<'_>,
    key: impl Fn(usize) -> Option<K>,
    cmp: impl Fn(&K, &K) -> std::cmp::Ordering,
) -> Option<(K, usize)> {
    let better = if max { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
    let mut best: Option<(K, usize)> = None;
    rows.each(|r| {
        if let Some(k) = key(r) {
            if best.is_none_or(|(b, _)| cmp(&k, &b) == better) {
                best = Some((k, r));
            }
        }
    });
    best
}

/// `sum` over a column of two kinds of rows (`if(c, x, 0)`: a Float64 or a numeric constant),
/// row by row as `sum` takes them until the sum is a Float64, then every row as a float in a
/// loop of its own.
fn sum_choose(s: &mut Value, x: &Col, rows: Rows<'_>) -> bool {
    let Data::Choose(pick, a, b) = &x.data else { return false };
    // one branch a Float64 column, the other a number
    let (v, nulls, c, then_is_column) = match (floats(a), floats(b), &a.data, &b.data) {
        (Some((v, n)), _, _, Data::Const(c, _)) => (v, n, c, true),
        (_, Some((v, n)), Data::Const(c, _), _) => (v, n, c, false),
        _ => return false,
    };
    let Some(cf) = c.f64().filter(|_| matches!(c, Value::Int(_) | Value::UInt(_) | Value::F64(_))) else {
        return false;
    };
    let value = |r: usize| if pick[r] == then_is_column { (nulls.is_some_and(|n| n[r]), v[r]) } else { (false, cf) };
    let rows: Vec<usize> = match rows {
        // a range in a loop of its own once the sum is a Float64
        Rows::Range(a, z) => {
            let mut r = a;
            while r < z && !matches!(s, Value::F64(_)) {
                let (null, f) = value(r);
                if !null {
                    *s = sum(s, &if pick[r] == then_is_column { Value::F64(f) } else { c.clone() });
                }
                r += 1;
            }
            if let Value::F64(t) = s {
                match nulls {
                    // a NULL row adds 0.0, which leaves a sum begun at +0.0 as it is
                    None => {
                        for (p, x) in pick[r..z].iter().zip(&v[r..z]) {
                            *t += if *p == then_is_column { *x } else { cf };
                        }
                    }
                    Some(n) => {
                        for ((p, x), null) in pick[r..z].iter().zip(&v[r..z]).zip(&n[r..z]) {
                            *t += if *p != then_is_column {
                                cf
                            } else if *null {
                                0.0
                            } else {
                                *x
                            };
                        }
                    }
                }
            }
            return true;
        }
        Rows::Pick(p) => p.to_vec(),
    };
    for r in rows {
        let (null, f) = value(r);
        if null {
            continue;
        }
        match s {
            Value::F64(t) => *t += f,
            _ => *s = sum(s, &if pick[r] == then_is_column { Value::F64(f) } else { c.clone() }),
        }
    }
    true
}

/// `arg_max(x, (a, b))` and `arg_min` of a key of two integers or times, without NULLs.
fn keyed(acc: &mut Acc, x: &Col, key: &[&Col], rows: Rows<'_>) -> bool {
    let Acc::Arg { max, key_x } = acc else { return false };
    let [a, b] = key else { return false };
    let (Some(ka), Some(kb)) = (a.i64s(), b.i64s()) else { return false };
    let Some(((ta, tb), r)) = best_row(*max, rows, |r| Some((ka[r], kb[r])), |p, q| p.cmp(q)) else { return true };
    let value = |c: &Col, v: i64| if matches!(c.data, Data::Time(_)) { Value::Time(v) } else { Value::Int(v) };
    let k = Value::Array([value(a, ta), value(b, tb)].into());
    let better = if *max { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
    let (key, val) = &mut **key_x;
    if key.is_null() || crate::expr::compare(&k, key) == Some(better) {
        (*key, *val) = (k, x.get(r));
    }
    true
}

/// A sequence aggregate over its tuple's item columns: each trade added as `add_run` & co add
/// it, without building the tuple; a run of trades already in the aggregate's order (a view's
/// ORDER BY sorts them so) at once, with the keys made only of those left waiting.
fn sequence(acc: &mut Acc, items: &[&Col], rows: Rows<'_>) -> bool {
    match (acc, items) {
        (Acc::RunStructure(q), [t, id, side, price]) => {
            let (Some(t), Some(side), Some(price), Some(ids)) = (t.i64s(), Sides::of(side), price.f64s(), Ids::of(id))
            else {
                return false;
            };
            let taken = taken(rows, |r| side.at(r).is_some());
            let tie = |a: usize, b: usize| ids.at(a).cmp(&ids.at(b));
            let in_order = ordered(&taken, t, tie, q.last_key().map(|(lt, li)| (*lt, |r: usize| ids.at(r).cmp(li))));
            if in_order {
                let at = |i: usize| taken[i];
                q.add_ordered(
                    taken.len(),
                    |i| t[at(i)],
                    |i| ids.at(at(i)),
                    |i| (side.at(at(i)) == Some(true), price[at(i)]),
                );
            } else {
                taken.iter().for_each(|&r| q.add((t[r], ids.at(r)), (side.at(r) == Some(true), price[r])));
            }
            true
        }
        (Acc::UpDownTicks(q), [t, id, price, size]) => {
            let (Some(t), Some(id), Some(price), Some(size)) = (t.i64s(), id.strs(), price.f64s(), size.f64s()) else {
                return false;
            };
            let taken = taken(rows, |_| true);
            let tie = |a: usize, b: usize| id.bytes(a).cmp(id.bytes(b));
            let in_order =
                ordered(&taken, t, tie, q.last_key().map(|(lt, li)| (*lt, |r: usize| id.bytes(r).cmp(li.as_bytes()))));
            if in_order {
                let at = |i: usize| taken[i];
                q.add_ordered(taken.len(), |i| t[at(i)], |i| id.get(at(i)).into(), |i| (price[at(i)], size[at(i)]));
            } else {
                taken.iter().for_each(|&r| q.add((t[r], id.get(r).into()), (price[r], size[r])));
            }
            true
        }
        (Acc::TradeReturns(q), [t, t1, t2, price, side, size]) => {
            let (Some(t), Some(price), Some(size)) = (t.i64s(), price.f64s(), size.f64s()) else { return false };
            let (Some(t1), Some(t2), Some(buy)) = (Ties::of(t1), Ties::of(t2), Sides::of(side)) else { return false };
            let taken = taken(rows, |r| buy.at(r).is_some());
            let tie = |a: usize, b: usize| t1.cmp_rows(a, b).then_with(|| t2.cmp_rows(a, b));
            let last =
                q.last_key().map(|(lt, (k1, k2))| (*lt, |r: usize| t1.at(r).cmp(k1).then_with(|| t2.at(r).cmp(k2))));
            let at = |i: usize| taken[i];
            let trade = |r: usize| (buy.at(r) == Some(true), price[r], size[r]);
            if ordered(&taken, t, tie, last) {
                q.add_ordered(taken.len(), |i| t[at(i)], |i| (t1.at(at(i)), t2.at(at(i))), |i| trade(at(i)));
            } else {
                taken.iter().for_each(|&r| q.add((t[r], (t1.at(r), t2.at(r))), trade(r)));
            }
            true
        }
        (Acc::TradeReversal(q), [t, t1, t2, price, side, size]) => {
            let (Some(t), Some(price), Some(size)) = (t.i64s(), price.f64s(), size.f64s()) else { return false };
            let (Some(t1), Some(t2), Some(buy)) = (Ties::of(t1), Ties::of(t2), Sides::of(side)) else { return false };
            let taken = taken(rows, |r| buy.at(r).is_some());
            let tie = |a: usize, b: usize| t1.cmp_rows(a, b).then_with(|| t2.cmp_rows(a, b));
            let last =
                q.last_key().map(|(lt, (k1, k2))| (*lt, |r: usize| t1.at(r).cmp(k1).then_with(|| t2.at(r).cmp(k2))));
            let at = |i: usize| taken[i];
            if ordered(&taken, t, tie, last) {
                q.add_ordered(
                    taken.len(),
                    |i| t[at(i)],
                    |i| (t1.at(at(i)), t2.at(at(i))),
                    |i| (price[at(i)], size[at(i)]),
                );
            } else {
                taken.iter().for_each(|&r| q.add((t[r], (t1.at(r), t2.at(r))), (price[r], size[r])));
            }
            true
        }
        (Acc::DistinctStats(q), [t, t1, x]) => {
            let (Some(t), Some(t1)) = (t.i64s(), Ties::of(t1)) else { return false };
            let Some((v, nulls)) = floats(x) else { return false };
            let taken = taken(rows, |_| true);
            let value = |r: usize| (!nulls.is_some_and(|n| n[r])).then(|| v[r]);
            let last = q.last_key().map(|(lt, k)| (*lt, |r: usize| t1.at(r).cmp(k)));
            let at = |i: usize| taken[i];
            if ordered(&taken, t, |a, b| t1.cmp_rows(a, b), last) {
                q.add_ordered(taken.len(), |i| t[at(i)], |i| t1.at(at(i)), |i| value(at(i)));
            } else {
                taken.iter().for_each(|&r| q.add((t[r], t1.at(r)), value(r)));
            }
            true
        }
        _ => false,
    }
}

/// The rows a sequence aggregate takes, in order.
fn taken(rows: Rows<'_>, keep: impl Fn(usize) -> bool) -> Vec<usize> {
    let mut out = Vec::with_capacity(rows.len());
    rows.each(|r| {
        if keep(r) {
            out.push(r)
        }
    });
    out
}

/// Whether `rows` are in key order (time `t`, then `tie`), the first at or after the key the
/// aggregate last took: its time, and its tie against a row.
fn ordered(
    rows: &[usize],
    t: &[i64],
    tie: impl Fn(usize, usize) -> std::cmp::Ordering,
    last: Option<(i64, impl Fn(usize) -> std::cmp::Ordering)>,
) -> bool {
    let first = match (rows.first(), last) {
        (Some(&r), Some((lt, against))) => t[r] > lt || (t[r] == lt && against(r).is_ge()),
        _ => true,
    };
    first && rows.windows(2).all(|w| t[w[0]] < t[w[1]] || (t[w[0]] == t[w[1]] && tie(w[0], w[1]).is_le()))
}

/// A sequence aggregate's tie-break column, read as `tie` reads its values.
enum Ties<'a> {
    Int(&'a [i64]),
    Str(&'a crate::column::Strs),
    Const(Tie),
}

impl<'a> Ties<'a> {
    fn of(c: &'a Col) -> Option<Ties<'a>> {
        if let Data::Const(v, _) = &c.data {
            return c.nulls.is_none().then(|| Ties::Const(tie(v)));
        }
        if let Some(v) = c.i64s() {
            return Some(Ties::Int(v));
        }
        c.strs().map(Ties::Str)
    }

    #[inline]
    fn at(&self, r: usize) -> Tie {
        match self {
            Ties::Int(v) => Tie::Int(v[r]),
            Ties::Str(s) => Tie::Str(s.get(r).into()),
            Ties::Const(t) => t.clone(),
        }
    }

    /// Rows `a` and `b` in `Tie` order, without making the ties.
    #[inline]
    fn cmp_rows(&self, a: usize, b: usize) -> std::cmp::Ordering {
        match self {
            Ties::Int(v) => v[a].cmp(&v[b]),
            Ties::Str(s) => s.bytes(a).cmp(s.bytes(b)),
            Ties::Const(_) => std::cmp::Ordering::Equal,
        }
    }
}

/// A side column: buy, sell, or neither.
enum Sides<'a> {
    Str(&'a crate::column::Strs),
    Const(Option<bool>),
}

impl<'a> Sides<'a> {
    fn of(c: &'a Col) -> Option<Sides<'a>> {
        match &c.data {
            Data::Const(v, _) if c.nulls.is_none() => Some(Sides::Const(side(v.str()))),
            _ => c.strs().map(Sides::Str),
        }
    }

    #[inline]
    fn at(&self, r: usize) -> Option<bool> {
        match self {
            Sides::Str(s) => side(Some(s.get(r))),
            Sides::Const(b) => *b,
        }
    }
}

fn side(s: Option<&str>) -> Option<bool> {
    match s {
        Some("buy") => Some(true),
        Some("sell") => Some(false),
        _ => None,
    }
}

/// A trade id as `add_run` orders it: a string as the number it reads as (else 0), a number as
/// it is.
enum Ids<'a> {
    Int(&'a [i64]),
    Str(&'a crate::column::Strs),
}

impl<'a> Ids<'a> {
    fn of(c: &'a Col) -> Option<Ids<'a>> {
        match c.i64s() {
            Some(v) => Some(Ids::Int(v)),
            None => c.strs().map(Ids::Str),
        }
    }

    #[inline]
    fn at(&self, r: usize) -> i64 {
        match self {
            Ids::Int(v) => v[r],
            Ids::Str(s) => s.get(r).parse().unwrap_or(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The window's loop, value by value.
    fn by_row(acc: &mut Acc, args: &Args<'_>, rows: &[usize]) {
        for &r in rows {
            match args {
                Args::Cols(cols) => match cols.as_slice() {
                    [a, b] => acc.add2(a.get(r), b.get(r)),
                    cols => acc.add(cols.first().map_or(Value::Null, |a| a.get(r))),
                },
                Args::Keyed(x, key) => acc.add2(x.get(r), Value::Array(key.iter().map(|c| c.get(r)).collect())),
                Args::Tuple(items) => acc.add(Value::Array(items.iter().map(|c| c.get(r)).collect())),
            }
        }
    }

    fn float() -> impl Strategy<Value = Value> {
        prop_oneof![
            1 => Just(Value::Null),
            1 => Just(Value::F64(f64::NAN)),
            1 => Just(Value::F64(-0.0)),
            6 => (-1e3..1e3f64).prop_map(Value::F64),
        ]
    }

    fn side() -> impl Strategy<Value = Value> {
        prop_oneof![Just(Value::Str("buy".into())), Just(Value::Str("sell".into())), Just(Value::Str("x".into()))]
    }

    fn data(n: usize) -> impl Strategy<Value = Vec<Vec<Value>>> {
        let col = |s: BoxedStrategy<Value>| prop::collection::vec(s, n);
        (
            col(float().boxed()),
            col(float().boxed()),
            col(prop_oneof![Just(Value::Bool(true)), Just(Value::Bool(false)), Just(Value::Null)].boxed()),
            col(side().boxed()),
            col((0i64..50).prop_map(Value::Time).boxed()),
            col((0i64..20).prop_map(|i| Value::Str(i.to_string().into())).boxed()),
            col((0i64..5).prop_map(Value::Int).boxed()),
            col(prop_oneof![(0i64..50).prop_map(Value::Time), Just(Value::Null)].boxed()),
        )
            .prop_map(|(a, b, c, s, t, id, k, tn)| vec![a, b, c, s, t, id, k, tn])
    }

    /// Without NULLs: a Float64 column of them takes the loops for columns without NULLs.
    fn no_nulls(v: &[Value]) -> Col {
        Col::from_values(v.iter().map(|x| if x.is_null() { Value::F64(1.0) } else { x.clone() }).collect())
    }

    /// Trades in time order with ties: (time, id) sorted, the ids numbers whose text order is
    /// their number order (zero-padded), as a view's ORDER BY gives them.
    fn sorted_trades(times: Vec<u8>) -> Vec<Vec<Value>> {
        let mut t: Vec<i64> = times.into_iter().map(i64::from).collect();
        t.sort_unstable();
        let mut rows = vec![vec![], vec![], vec![], vec![], vec![]];
        for (i, t) in t.iter().enumerate() {
            rows[0].push(Value::Time(*t));
            rows[1].push(Value::Str(format!("{i:06}").into()));
            rows[2].push(Value::Str(["buy", "sell", "x"][i * 7 % 3].into()));
            rows[3].push(Value::F64(100.0 + (i * 13 % 17) as f64));
            rows[4].push(Value::Int(i as i64));
        }
        rows
    }

    /// The common shapes take their typed loops (a shortcut not taken leaves the same state,
    /// which the property tests cannot tell): each, over columns with and without NULLs, in a
    /// range and at picked rows.
    #[test]
    fn common_shapes_take_their_typed_loops() {
        let f = |v: &[f64]| Col::new(Data::F64(v.to_vec().into()));
        let (x, y) = (f(&[1.0, 2.0, 3.0, 4.0]), f(&[2.0, 1.0, 4.0, 3.0]));
        let xn = Col {
            data: Data::F64(vec![1.0, 0.0, 3.0, 4.0].into()),
            nulls: Some(vec![false, true, false, false].into()),
        };
        let t = Col::new(Data::Time(vec![1, 2, 2, 3].into()));
        let tn = Col { data: Data::Time(vec![1, 0, 2, 3].into()), nulls: Some(vec![false, true, false, false].into()) };
        let k = Col::new(Data::Int(vec![1, 2, 3, 4].into()));
        let c = Col::new(Data::Bool(vec![true, false, true, false].into()));
        let side = Col::new(Data::Str(["buy", "sell", "buy", "sell"].into_iter().collect()));
        let id = Col::new(Data::Str(["1", "2", "3", "4"].into_iter().collect()));
        let zero = Arc::new(Col::new(Data::Const(Value::Int(0), 4)));
        let choose = Col::new(Data::Choose(vec![true, false, true, true].into(), Arc::new(x.clone()), zero.clone()));
        let choose2 = Col::new(Data::Choose(vec![true, false, true, true].into(), zero.clone(), Arc::new(x.clone())));
        let cases: Vec<(&str, Vec<Value>, Args)> = vec![
            ("sum", vec![], Args::Cols(vec![&x])),
            ("sum", vec![], Args::Cols(vec![&xn])),
            ("sum", vec![], Args::Cols(vec![&choose])),
            ("sum", vec![], Args::Cols(vec![&choose2])),
            ("avg", vec![], Args::Cols(vec![&xn])),
            ("min", vec![], Args::Cols(vec![&xn])),
            ("max", vec![], Args::Cols(vec![&t])),
            ("count", vec![], Args::Cols(vec![&xn])),
            ("count", vec![], Args::Cols(vec![])),
            ("latest", vec![], Args::Cols(vec![&xn])),
            ("earliest", vec![], Args::Cols(vec![&side])),
            ("kurtosis", vec![], Args::Cols(vec![&xn])),
            ("quantile_cont", vec![Value::F64(0.5)], Args::Cols(vec![&xn])),
            ("vwap", vec![], Args::Cols(vec![&xn, &y])),
            ("twap", vec![], Args::Cols(vec![&xn, &t])),
            ("corr", vec![], Args::Cols(vec![&x, &xn])),
            ("count_if", vec![], Args::Cols(vec![&c])),
            ("sum_if", vec![], Args::Cols(vec![&xn, &c])),
            ("arg_max", vec![], Args::Cols(vec![&x, &tn])),
            ("arg_max", vec![], Args::Keyed(&x, vec![&t, &k])),
            ("run_structure", vec![], Args::Tuple(vec![&t, &id, &side, &x])),
            ("updownticks", vec![], Args::Tuple(vec![&t, &id, &x, &y])),
            ("distinct_stats", vec![], Args::Tuple(vec![&t, &k, &x])),
            ("trade_returns", vec![], Args::Tuple(vec![&t, &id, &zero, &x, &side, &y])),
            ("trade_reversal", vec![Value::Int(1), Value::F64(0.5)], Args::Tuple(vec![&t, &id, &zero, &x, &side, &y])),
        ];
        for (name, params, args) in cases {
            let nargs = match &args {
                Args::Cols(c) => c.len(),
                Args::Keyed(..) => 2,
                Args::Tuple(_) => 1,
            };
            // a group's first run, and one after it (its state carried on)
            for runs in [[Rows::Range(0, 2), Rows::Range(2, 4)], [Rows::Pick(&[0]), Rows::Pick(&[2, 3])]] {
                let mut a = Acc::new(name, &params, nargs).unwrap();
                for rows in runs {
                    let typed = match &args {
                        Args::Cols(cols) => typed(&mut a, cols, rows),
                        Args::Keyed(x, key) => keyed(&mut a, x, key, rows),
                        Args::Tuple(items) => sequence(&mut a, items, rows),
                    };
                    assert!(typed, "{name} over {rows:?}");
                }
            }
        }
        // of two equal floats, min and max keep the first, as `compare` has them equal: 0.0 and -0.0
        let z = f(&[0.0, -0.0]);
        for (name, first) in [("max", 0.0f64), ("min", 0.0)] {
            let mut a = Acc::new(name, &[], 1).unwrap();
            add_rows(&mut a, &Args::Cols(vec![&z]), Rows::Range(0, 2));
            assert_eq!(a.result().f64().map(f64::to_bits), Some(first.to_bits()), "{name}");
        }
    }

    /// Every sequence aggregate over `rows` cut into runs at `cuts` (and `pick`ed rows dropped):
    /// the same state as row by row.
    fn sequences_match(rows: &[Vec<Value>], cuts: &[usize], pick: bool) {
        let n = rows[0].len();
        let (t, id, side, price, k) = (
            Col::from_values(rows[0].clone()),
            Col::from_values(rows[1].clone()),
            Col::from_values(rows[2].clone()),
            Col::from_values(rows[3].clone()),
            Col::from_values(rows[4].clone()),
        );
        let zeros = Col::new(Data::Const(Value::Int(0), n));
        let cases: Vec<(&str, Args)> = vec![
            ("run_structure", Args::Tuple(vec![&t, &id, &side, &price])),
            ("updownticks", Args::Tuple(vec![&t, &id, &price, &price])),
            ("trade_returns", Args::Tuple(vec![&t, &id, &zeros, &price, &side, &price])),
            ("trade_returns", Args::Tuple(vec![&t, &k, &k, &price, &side, &price])),
            ("trade_reversal", Args::Tuple(vec![&t, &id, &zeros, &price, &side, &price])),
            ("distinct_stats", Args::Tuple(vec![&t, &k, &price])),
        ];
        for (name, args) in cases {
            let params = if name == "trade_reversal" { vec![Value::Int(2), Value::F64(0.9)] } else { vec![] };
            let (mut a, mut b) = (Acc::new(name, &params, 1).unwrap(), Acc::new(name, &params, 1).unwrap());
            let mut at = 0;
            for &c in cuts.iter().chain([&n]) {
                let end = c.clamp(at, n);
                let rows: Vec<usize> = (at..end).filter(|r| !pick || r % 5 != 3).collect();
                add_rows(&mut a, &args, Rows::Pick(&rows));
                by_row(&mut b, &args, &rows);
                at = end;
            }
            assert_eq!(format!("{a:?}"), format!("{b:?}"), "{name}");
            assert_eq!(format!("{:?}", a.result()), format!("{:?}", b.result()), "{name}");
        }
    }

    #[test]
    fn a_burst_of_trades_of_one_time_past_the_reorder_cap() {
        // 5,000 trades of one time between others: the latest SEQUENCE_REORDER of them wait
        let times: Vec<u8> = (0..5_100)
            .map(|i| {
                if i < 50 {
                    1
                } else if i < 5_050 {
                    2
                } else {
                    3
                }
            })
            .collect();
        let rows = sorted_trades(times);
        sequences_match(&rows, &[10, 3_000, 5_060], false);
        sequences_match(&rows, &[4_000], true);
    }

    proptest! {
        #[test]
        fn trades_in_order_take_the_state_trade_by_trade_does(
            times in prop::collection::vec(0u8..20, 1..200),
            cuts in prop::collection::vec(0usize..200, 0..6),
            pick in any::<bool>(),
        ) {
            let mut cuts = cuts;
            cuts.sort_unstable();
            sequences_match(&sorted_trades(times), &cuts, pick);
        }

        #[test]
        fn typed_loops_leave_the_state_rows_leave(
            seed in any::<u64>(),
            d in (1usize..40).prop_flat_map(data),
        ) {
            let n = d[0].len();
            let x = Col::from_values(d[0].clone());
            let (xs, ys) = (no_nulls(&d[0]), no_nulls(&d[1]));
            let y = Col::from_values(d[1].clone());
            let c = Col::from_values(d[2].clone());
            let side = Col::from_values(d[3].clone());
            let (t, id, k, tn) = (Col::from_values(d[4].clone()), Col::from_values(d[5].clone()), Col::from_values(d[6].clone()), Col::from_values(d[7].clone()));
            // if(c, x, 0) and if(c, 0, x): rows of two kinds
            let picks: Vec<bool> = d[2].iter().map(|v| *v == Value::Bool(true)).collect();
            let zero = Arc::new(Col::new(Data::Const(Value::Int(0), n)));
            let choose = Col::new(Data::Choose(picks.clone().into(), Arc::new(x.clone()), zero.clone()));
            let choose2 = Col::new(Data::Choose(picks.into(), zero, Arc::new(xs.clone())));
            let zeros = Col::new(Data::Const(Value::Int(0), n));
            let cases: Vec<(&str, Vec<Value>, Args)> = vec![
                ("sum", vec![], Args::Cols(vec![&x])),
                ("sum", vec![], Args::Cols(vec![&xs])),
                ("sum", vec![], Args::Cols(vec![&choose])),
                ("sum", vec![], Args::Cols(vec![&choose2])),
                ("avg", vec![], Args::Cols(vec![&x])),
                ("min", vec![], Args::Cols(vec![&x])),
                ("max", vec![], Args::Cols(vec![&xs])),
                ("max", vec![], Args::Cols(vec![&t])),
                ("min", vec![], Args::Cols(vec![&k])),
                ("count", vec![], Args::Cols(vec![&x])),
                ("count", vec![], Args::Cols(vec![&choose])),
                ("count", vec![], Args::Cols(vec![])),
                ("latest", vec![], Args::Cols(vec![&x])),
                ("latest", vec![], Args::Cols(vec![&choose])),
                ("earliest", vec![], Args::Cols(vec![&side])),
                ("stddev", vec![], Args::Cols(vec![&x])),
                ("kurtosis", vec![], Args::Cols(vec![&xs])),
                ("quantile_cont", vec![Value::F64(0.5)], Args::Cols(vec![&x])),
                ("vwap", vec![], Args::Cols(vec![&x, &y])),
                ("vwap", vec![], Args::Cols(vec![&xs, &ys])),
                ("twap", vec![], Args::Cols(vec![&x, &t])),
                ("twap", vec![], Args::Cols(vec![&x, &tn])),
                ("corr", vec![], Args::Cols(vec![&x, &y])),
                ("corr", vec![], Args::Cols(vec![&xs, &ys])),
                ("count_if", vec![], Args::Cols(vec![&c])),
                ("sum_if", vec![], Args::Cols(vec![&x, &c])),
                ("avg_if", vec![], Args::Cols(vec![&xs, &c])),
                ("max_if", vec![], Args::Cols(vec![&y, &c])),
                ("arg_max", vec![], Args::Cols(vec![&x, &t])),
                ("arg_max", vec![], Args::Cols(vec![&x, &tn])),
                ("arg_min", vec![], Args::Cols(vec![&side, &k])),
                ("arg_max", vec![], Args::Keyed(&x, vec![&t, &k])),
                ("run_structure", vec![], Args::Tuple(vec![&t, &id, &side, &xs])),
                ("updownticks", vec![], Args::Tuple(vec![&t, &id, &xs, &ys])),
                ("trade_returns", vec![], Args::Tuple(vec![&t, &id, &zeros, &xs, &side, &ys])),
                ("trade_returns", vec![], Args::Tuple(vec![&t, &k, &k, &xs, &side, &ys])),
                ("trade_reversal", vec![Value::Int(2), Value::F64(0.9)], Args::Tuple(vec![&t, &id, &zeros, &xs, &side, &ys])),
                ("distinct_stats", vec![], Args::Tuple(vec![&t, &k, &x])),
            ];
            // rows in segments, some picked out
            let mut rng = seed;
            let mut next = |m: usize| {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (rng >> 33) as usize % m
            };
            for (name, params, args) in cases {
                let nargs = match &args { Args::Cols(c) => c.len(), Args::Keyed(..) => 2, Args::Tuple(_) => 1 };
                let (mut a, mut b) = (Acc::new(name, &params, nargs).unwrap(), Acc::new(name, &params, nargs).unwrap());
                let mut at = 0;
                while at < n {
                    let end = (at + 1 + next(8)).min(n);
                    if next(2) == 0 {
                        add_rows(&mut a, &args, Rows::Range(at, end));
                        by_row(&mut b, &args, &(at..end).collect::<Vec<_>>());
                    } else {
                        let picked: Vec<usize> = (at..end).filter(|_| next(3) > 0).collect();
                        add_rows(&mut a, &args, Rows::Pick(&picked));
                        by_row(&mut b, &args, &picked);
                    }
                    at = end;
                }
                prop_assert_eq!(format!("{a:?}"), format!("{b:?}"), "{}", name);
                prop_assert_eq!(format!("{:?}", a.result()), format!("{:?}", b.result()), "{}", name);
            }
        }
    }
}
