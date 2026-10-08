//! What metrics a batch engine computes with LAG over sorted windows need beyond plain
//! aggregates, each held to the SQL it stands for, run naively (sort the window, then scan it),
//! whatever the arrival order:
//! - `arg_min(x, (time, tie))` / `arg_max(x, (time, tie))`: FIRST/LAST(x ORDER BY time, tie);
//! - `variance`, `stddev`: DuckDB's VAR_SAMP and STDDEV_SAMP (NULL, not NaN, for one row);
//! - `trade_returns`: flow's and impact's LAG trade returns, in a state of fixed size whatever
//!   the window's length;
//! - `distinct_stats`: basis's `IS DISTINCT FROM LAG` dedupe and open interest's tick changes;
//! - `to_int64_or_zero`: COALESCE(TRY_CAST(id AS BIGINT), 0), impact's id order.
//!
//! The ordered ones take their input sorted by time, as a pipeline gives it to them (a view's
//! `ORDER BY` with `SETTINGS order_hold_ms`, ADR-0013), and order equal times their own way.
use brrrrr_core::engine::Engine;
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;
use proptest::prelude::*;
use serde_json::Value as J;
use std::collections::BTreeMap;

const SQL: &str = "
CREATE STREAM IF NOT EXISTS t_raw (ts datetime64(6), other int64, id string, side string, price float64, size float64,
  x nullable(float64));
CREATE STREAM IF NOT EXISTS t (ts datetime64(6), other int64, id string, side string, price float64, size float64,
  x nullable(float64));
CREATE MATERIALIZED VIEW IF NOT EXISTS sort INTO t AS
SELECT ts, other, id, side, price, size, x FROM t_raw ORDER BY ts SETTINGS order_hold_ms = 1000;
CREATE EXTERNAL STREAM IF NOT EXISTS out (
  g string, first nullable(float64), last nullable(float64), last_other nullable(float64),
  var nullable(float64), sd nullable(float64), sd_one nullable(float64),
  rv nullable(float64), cov nullable(float64), var_size nullable(float64),
  buy nullable(float64), sell nullable(float64), open nullable(float64), close nullable(float64),
  impact_open nullable(float64),
  mean nullable(float64), dsd nullable(float64), csd nullable(float64)
) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS
SELECT
  'a' AS g,
  arg_min(price, (ts, id)) AS first,
  arg_max(price, (ts, id)) AS last,
  arg_max(price, (ts, other)) AS last_other,
  variance(price) AS var,
  stddev(price) AS sd,
  stddev(if(id = '1', price, NULL)) AS sd_one,
  trade_returns((ts, id, 0, price, side, size))[1] AS rv,
  trade_returns((ts, id, 0, price, side, size))[2] AS cov,
  trade_returns((ts, id, 0, price, side, size))[3] AS var_size,
  trade_returns((ts, id, 0, price, side, size))[4] AS buy,
  trade_returns((ts, id, 0, price, side, size))[5] AS sell,
  trade_returns((ts, id, 0, price, side, size))[6] AS open,
  trade_returns((ts, id, 0, price, side, size))[7] AS close,
  trade_returns((ts, other, to_int64_or_zero(id), price, side, size))[6] AS impact_open,
  distinct_stats((ts, other, x))[1] AS mean,
  distinct_stats((ts, other, x))[2] AS dsd,
  distinct_stats((ts, other, x))[3] AS csd
FROM tumble(t, ts, 1m)
GROUP BY window_start
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;
";

/// One row: seconds, exchange time, id, buy, price, size, x.
#[derive(Clone, Debug)]
struct Row {
    t: f64,
    other: i64,
    id: String,
    buy: bool,
    price: f64,
    size: f64,
    x: Option<f64>,
}

fn value(r: &Row) -> Vec<Value> {
    vec![
        Value::Time((r.t * 1e6).round() as i64),
        Value::Int(r.other),
        Value::Str(r.id.as_str().into()),
        Value::Str(if r.buy { "buy" } else { "sell" }.into()),
        Value::F64(r.price),
        Value::F64(r.size),
        r.x.map_or(Value::Null, Value::F64),
    ]
}

/// The rows in `chunks`, sorted by time with a 1 s hold, then the window closed: its output.
fn run(chunks: Vec<Vec<Row>>) -> J {
    let mut e = Engine::new(&parse(SQL).unwrap()).unwrap();
    let mut out = vec![];
    for c in chunks {
        e.insert("t_raw", c.iter().map(value).collect(), &mut out);
    }
    e.close_until(i64::MAX / 4, &mut out);
    assert_eq!(out.len(), 1, "one window");
    serde_json::from_str(&out[0].payload).unwrap()
}

fn close(got: &J, want: Option<f64>, what: &str) {
    match (got.as_f64(), want) {
        (Some(a), Some(b)) => assert!((a - b).abs() <= 1e-9 * b.abs().max(1.0), "{what}: {a} != {b}"),
        (None, None) => assert!(got.is_null(), "{what}: {got} is not NULL"),
        _ => panic!("{what}: brrrrr {got}, SQL {want:?}"),
    }
}

fn var_samp(v: &[f64]) -> Option<f64> {
    (v.len() > 1).then(|| {
        let m = v.iter().sum::<f64>() / v.len() as f64;
        v.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (v.len() - 1) as f64
    })
}

fn mean(v: &[f64]) -> Option<f64> {
    (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
}

/// The window's metrics as their SQL definitions compute them over the sorted rows.
fn reference(rows: &[Row]) -> Vec<(&'static str, Option<f64>)> {
    let by_id = {
        let mut r = rows.to_vec();
        r.sort_by(|a, b| a.t.total_cmp(&b.t).then(a.id.cmp(&b.id)));
        r
    };
    let by_other = {
        let mut r = rows.to_vec();
        r.sort_by(|a, b| a.t.total_cmp(&b.t).then(a.other.cmp(&b.other)));
        r
    };
    let by_num = {
        let num = |id: &str| id.parse::<i64>().unwrap_or(0);
        let mut r = rows.to_vec();
        r.sort_by(|a, b| a.t.total_cmp(&b.t).then(a.other.cmp(&b.other)).then(num(&a.id).cmp(&num(&b.id))));
        r
    };
    let prices: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let ones: Vec<f64> = rows.iter().filter(|r| r.id == "1").map(|r| r.price).collect();
    // trade_return = price / NULLIF(LAG(price), 0) - 1, over the rows in (time, id) order
    let ret: Vec<Option<f64>> = (0..by_id.len())
        .map(|i| (i > 0 && by_id[i - 1].price != 0.0).then(|| by_id[i].price / by_id[i - 1].price - 1.0))
        .collect();
    let signed: Vec<f64> = by_id.iter().map(|r| if r.buy { r.size } else { -r.size }).collect();
    let pairs: Vec<(f64, f64)> = ret.iter().zip(&signed).filter_map(|(r, s)| r.map(|r| (r, *s))).collect();
    let cov = (pairs.len() > 1).then(|| {
        let (mr, ms) = (
            mean(&pairs.iter().map(|p| p.0).collect::<Vec<_>>()).unwrap(),
            mean(&pairs.iter().map(|p| p.1).collect::<Vec<_>>()).unwrap(),
        );
        pairs.iter().map(|(r, s)| (r - mr) * (s - ms)).sum::<f64>() / (pairs.len() - 1) as f64
    });
    let side = |buy: bool| -> Vec<f64> {
        ret.iter()
            .zip(&by_id)
            .filter(|(r, t)| r.is_some() && t.buy == buy)
            .map(|(r, _)| if buy { r.unwrap() } else { -r.unwrap() })
            .collect()
    };
    // x IS DISTINCT FROM LAG(x) over (time, other); changes where x != prev
    let (mut distinct, mut changes) = (vec![], vec![]);
    for (i, r) in by_other.iter().enumerate() {
        let prev = (i > 0).then(|| by_other[i - 1].x);
        if prev != Some(r.x) {
            if let Some(x) = r.x {
                distinct.push(x);
                if let Some(Some(p)) = prev.filter(|p| *p != Some(0.0)) {
                    changes.push((x - p) / p);
                }
            }
        }
    }
    vec![
        ("first", Some(by_id[0].price)),
        ("last", Some(by_id[by_id.len() - 1].price)),
        ("last_other", Some(by_other[by_other.len() - 1].price)),
        ("var", var_samp(&prices)),
        ("sd", var_samp(&prices).map(f64::sqrt)),
        ("sd_one", var_samp(&ones).map(f64::sqrt)),
        ("rv", Some(ret.iter().flatten().map(|r| r * r).sum::<f64>().sqrt())),
        ("cov", cov),
        ("var_size", var_samp(&signed)),
        ("buy", mean(&side(true))),
        ("sell", mean(&side(false))),
        ("open", Some(by_id[0].price)),
        ("close", Some(by_id[by_id.len() - 1].price)),
        ("impact_open", Some(by_num[0].price)),
        ("mean", mean(&distinct)),
        ("dsd", var_samp(&distinct).map(f64::sqrt)),
        ("csd", var_samp(&changes).map(f64::sqrt)),
    ]
}

fn check(rows: &[Row], got: &J) {
    for (k, want) in reference(rows) {
        close(&got[k], want, k);
    }
}

#[test]
fn a_hand_worked_window() {
    // ids 10 and 9 share a time: as text "10" < "9", as numbers 9 < 10
    let r = |t: f64, other: i64, id: &str, buy: bool, price: f64, x: Option<f64>| Row {
        t,
        other,
        id: id.into(),
        buy,
        price,
        size: 1.0,
        x,
    };
    let rows = vec![
        r(1.0, 5, "9", true, 100.0, Some(1.0)),
        r(1.0, 5, "10", false, 101.0, Some(1.0)),
        r(1.5, 1, "11", true, 102.0, None),
        r(1.9, 1, "12", true, 102.0, Some(2.0)),
    ];
    let got = run(vec![rows.iter().rev().cloned().collect()]);
    // FIRST(price ORDER BY ts, id) is id "10"'s; impact's (ts, other, numeric id) puts id 9 first
    close(&got["first"], Some(101.0), "first");
    close(&got["impact_open"], Some(100.0), "impact_open");
    // (ts, other) order: 1, 1, NULL, 2 -> distinct 1, then NULL, then 2
    close(&got["mean"], Some(1.5), "mean");
    // 1 -> 1 is no change; NULL -> 2 has no previous value to change from
    close(&got["csd"], None, "csd");
    // a single row has no sample variance: NULL, not NaN
    close(&got["sd_one"], None, "sd_one");
    check(&rows, &got);
}

#[test]
fn distinct_stats_orders_nan_as_duckdb_does() {
    // IS DISTINCT FROM: NaN differs from every number and not from NaN, so 1, NaN, NaN, 2 counts
    // 1, NaN and 2, whose mean is NaN (written as null); NaN swallowing its neighbours would
    // leave 1 alone
    let r = |t: f64, x: f64| Row { t, other: t as i64, id: "1".into(), buy: true, price: 1.0, size: 1.0, x: Some(x) };
    let rows = vec![r(1.0, 1.0), r(2.0, f64::NAN), r(3.0, f64::NAN), r(4.0, 2.0)];
    let got = run(vec![rows]);
    assert!(got["mean"].is_null(), "mean of 1, NaN, 2 is NaN, not {}", got["mean"]);
    let rows = vec![r(1.0, 1.0), r(2.0, 3.0), r(3.0, 3.0), r(4.0, 2.0)];
    close(&run(vec![rows])["mean"], Some(2.0), "mean");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]
    /// Rows that each arrive within the hold (a second) of their time, in any chunking, give what sorting
    /// the window first gives. Times repeat (quarter seconds), prices repeat and include 0, ids
    /// order differently as text and as numbers.
    #[test]
    fn equal_sorting_the_window_first(
        rows in prop::collection::vec((0u32..200, any::<bool>(), 0u32..5, 1u32..4, prop::option::of(0u32..3), 0.0f64..1.0), 1..400),
        cuts in prop::collection::vec(1usize..100, 1..10),
    ) {
        let rows: Vec<Row> = rows.iter().enumerate().map(|(i, &(t, buy, p, s, x, _))| Row {
            t: t as f64 / 4.0,
            // unique, so (time, other) has no ties: which of two tied rows is last is arbitrary
            other: ((i * 31) % 1009) as i64,
            id: ((i * 7919) % 100_003).to_string(),
            buy,
            price: p as f64 * 0.5,
            size: s as f64,
            x: x.map(|x| x as f64),
        }).collect();
        let mut keyed: Vec<_> = rows.iter().cloned().zip(&rows).enumerate().map(|(i, (r, _))| {
            let jitter = [0.0f64, 0.3, 0.9, 0.1][i % 4];
            (r.t + jitter, r)
        }).collect();
        keyed.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut rest: Vec<Row> = keyed.into_iter().map(|(_, r)| r).collect();
        let mut chunks = vec![];
        for c in cuts.iter().cycle() {
            if rest.is_empty() { break; }
            let tail = rest.split_off((*c).min(rest.len()));
            chunks.push(std::mem::replace(&mut rest, tail));
        }
        check(&rows, &run(chunks));
    }
}

/// A day-long window of a busy symbol must not keep its trades: `trade_returns` is a fixed-size
/// summary, so the checkpoint of one open window with 200,000 trades stays small. Keeping every
/// trade, as a first version did, took 313 MiB of checkpoint for an hour of a busy feed's trades.
#[test]
fn a_long_window_keeps_a_bounded_state() {
    let mut e = Engine::new(&parse(SQL).unwrap()).unwrap();
    let mut out = vec![];
    let rows: Vec<Row> = (0..200_000)
        .map(|i| Row {
            t: i as f64 * 0.0002,
            other: i,
            id: i.to_string(),
            buy: i % 3 != 0,
            price: 100.0 + (i % 17) as f64 * 0.1,
            size: 1.0 + (i % 7) as f64,
            x: Some((i % 5) as f64),
        })
        .collect();
    // sorted: straight into the aggregates' stream
    for c in rows.chunks(1000) {
        e.insert("t", c.iter().map(value).collect(), &mut out);
    }
    assert!(out.is_empty(), "the window is still open");
    let bytes = brrrrr_core::checkpoint::Checkpoint::of(&e, 1, vec![], vec![]).encode().len();
    assert!(bytes < 400_000, "{bytes} bytes of checkpoint for one open window");
}

/// A busy symbol trades in bursts of hundreds within milliseconds, a trade arriving hundreds of
/// places behind its sorted place: a burst of 600 trades at one time, arriving in
/// reverse order of their ids (and of their exchange times), is still folded in sorted order.
#[test]
fn a_burst_of_600_trades_arriving_in_reverse_is_folded_in_order() {
    let rows: Vec<Row> = (0..600)
        .map(|i| Row {
            t: 1.0,
            other: 10_000 + i,
            id: (100_000 + i).to_string(),
            buy: i % 3 == 0,
            price: 100.0 + ((i * 7) % 13) as f64 * 0.5,
            size: 1.0 + (i % 5) as f64,
            x: Some((i % 4) as f64),
        })
        .collect();
    let arrival: Vec<Row> = rows.iter().rev().cloned().collect();
    check(&rows, &run(vec![arrival]));
}

/// A window's last value is that of its last *captured* row (`arg_max(x, captured)`, as a batch
/// engine's `MAX_BY(x, time)` reads it), whatever order the rows arrive in. The example quotes
/// pipeline (fixtures/pipelines/quotes.sql) reads quotes
/// - in capture order;
/// - late within one partition: every fifth quote up to 2 s behind its capture time;
/// - as two partitions, each in capture order, one 1 s behind the other (what `--max-drift`
///   allows);
///
/// each quote arriving at most 40 ms after its 1m window, inside the 50 ms the windows wait, so
/// that none misses its window (that loss is not this test's). Every order writes the same
/// windows, each with the spread and imbalance of its last captured quote (a quote without a bid
/// has no spread, and does not count for it), and the same means within rounding.
#[test]
fn the_last_value_of_a_window_is_the_last_captured_whatever_order_rows_arrive_in() {
    let cat = parse(&std::fs::read_to_string(crate::common::root("fixtures/pipelines/quotes.sql")).unwrap()).unwrap();
    // quotes: time, exchange, symbol, bid_price, bid_amount, ask_price, ask_amount, local_timestamp
    let t0 = 1_788_220_800_000_000i64;
    let mut seed = 7u64;
    let mut rand = move |m: u64| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (seed >> 33) % m
    };
    let mut quotes: Vec<Vec<Value>> = vec![];
    for i in 0..3 * 2 * 60 * 20 {
        let t = t0 + (i / 3) as i64 * 500_000 + rand(400_000) as i64;
        let bid = if rand(25) == 0 { 0.0 } else { 100.0 + rand(100) as f64 / 100.0 };
        quotes.push(vec![
            Value::Int(t),
            Value::Int(7),
            Value::Str(["A", "B", "C"][i % 3].into()),
            Value::F64(bid),
            Value::F64(1.0 + rand(50) as f64 / 10.0),
            Value::F64(101.0 + rand(100) as f64 / 100.0),
            Value::F64(1.0 + rand(50) as f64 / 10.0),
            Value::Int(t),
        ]);
    }
    quotes.sort_by_key(|q| q[7].i64());
    let f = |q: &[Value], k: usize| q[k].f64().unwrap();
    // the oracle: per (symbol, window), the last captured quote with a spread, and with an imbalance
    let mut last: BTreeMap<(String, i64), (Option<f64>, f64)> = BTreeMap::new();
    for q in &quotes {
        let key = (q[2].str().unwrap().to_string(), q[7].i64().unwrap().div_euclid(60_000_000) * 60_000_000);
        let e = last.entry(key).or_insert((None, 0.0));
        if f(q, 3) != 0.0 {
            e.0 = Some((f(q, 5) - f(q, 3)).abs());
        }
        e.1 = f(q, 4) - f(q, 6);
    }
    /// `t` delayed by `lag`, at most until 40 ms after its 1m window.
    fn late(t: i64, lag: i64) -> i64 {
        (t + lag).min((t.div_euclid(60_000_000) + 1) * 60_000_000 + 40_000).max(t)
    }
    type Arrival = fn(usize, i64) -> i64;
    let orders: [(&str, Arrival); 3] = [
        ("capture order", |_, t| t),
        ("late within a partition", |i, t| {
            late(t, if i % 5 == 0 { (i as i64 * 2_654_435_761) % 2_000_000 } else { 0 })
        }),
        ("two partitions, one 1 s behind", |i, t| late(t, if i % 2 == 1 { 1_000_000 } else { 0 })),
    ];
    let mut first: Option<Vec<J>> = None;
    for (order, arrival) in orders {
        let mut arriving: Vec<(i64, usize)> =
            quotes.iter().enumerate().map(|(i, q)| (arrival(i, q[7].i64().unwrap()), i)).collect();
        arriving.sort();
        let out_of_order = arriving.windows(2).filter(|w| w[1].1 < w[0].1).count();
        assert!(order == "capture order" || out_of_order > 50, "{order}: {out_of_order} quotes out of capture order");
        let mut e = Engine::new(&cat).unwrap();
        let mut out = vec![];
        for c in arriving.chunks(97) {
            e.insert("quotes", c.iter().map(|(_, i)| quotes[*i].clone()).collect(), &mut out);
        }
        e.close_until(i64::MAX / 4, &mut out);
        let mut got: Vec<J> = out
            .iter()
            .filter(|m| &*m.topic == "quotes.spread")
            .map(|m| serde_json::from_str(&m.payload).unwrap())
            .collect();
        got.sort_by_key(|j| (j["symbol"].as_str().unwrap().to_string(), j["time"].as_i64().unwrap()));
        assert_eq!(got.len(), last.len(), "{order}: windows");
        for j in &got {
            let (spread, imbalance) = last[&(j["symbol"].as_str().unwrap().to_string(), j["time"].as_i64().unwrap())];
            assert_eq!((j["spread"].as_f64(), j["imbalance"].as_f64()), (spread, Some(imbalance)), "{order}: {j}");
        }
        match &first {
            None => first = Some(got),
            Some(want) => {
                for (g, w) in got.iter().zip(want) {
                    for (k, v) in w.as_object().unwrap() {
                        let close = match (v.as_f64(), g[k].as_f64()) {
                            (Some(a), Some(b)) => (a - b).abs() <= 1e-12 * a.abs().max(b.abs()),
                            _ => *v == g[k],
                        };
                        assert!(close, "{order}: {k} {} where capture order has {v}", g[k]);
                    }
                }
            }
        }
    }
}
