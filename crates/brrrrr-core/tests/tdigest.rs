//! `quantile_t_digest` on brrrrr's schedule (ADR-0014), through the engine: in windows of
//! tens of thousands of values a result is within 0.25% of the values in rank of the exact quantile
//! (prices clustered within a few dollars of 60,000, slippage around zero, notionals over five
//! decades); the quantiles of one argument read one digest; and whatever the chunking and wherever
//! a checkpoint is restored, also inside a window's lead (ADR-0015), the messages are the same.
use brrrrr_core::engine::{Emit, Engine};
use brrrrr_core::sql::{parse, Catalog};
use brrrrr_core::value::Value;
use proptest::prelude::*;

const SQL: &str = "
CREATE STREAM trades (t datetime64(6), symbol string, price float64, bps float64, notional float64);
CREATE EXTERNAL STREAM out (symbol string, start int64, price_median float32, bps_median float32, bps_p95 float32,
  notional_median float32, n uint64) SETTINGS type = 'kafka', topic = 'out', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM tail (symbol string, start int64, p01 float32, p99 float32) SETTINGS type = 'kafka', topic = 'tail', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW m INTO out AS SELECT symbol, to_unix_timestamp64_micro(window_start) AS start,
  quantile_t_digest(0.5)(price) AS price_median, quantile_t_digest(0.5)(bps) AS bps_median,
  quantile_t_digest(0.95)(bps) AS bps_p95, median_tdigest(notional) AS notional_median, count() AS n
FROM tumble(trades, t, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW q INTO tail AS SELECT symbol, to_unix_timestamp64_micro(window_start) AS start,
  quantile_t_digest(0.01)(price) AS p01, quantile_t_digest(0.99)(price) AS p99
FROM tumble(trades, t, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

struct Rng(u64);

impl Rng {
    fn uniform(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }

    fn normal(&mut self) -> f64 {
        let (u, v) = (self.uniform(), self.uniform());
        (-2.0 * (u + 1e-300).ln()).sqrt() * (std::f64::consts::TAU * v).cos()
    }
}

/// `n` trades a minute for `minutes` minutes over two symbols: prices wander around 60,000 in
/// cents, slippage a third zero and the rest around 0.5 bps, notionals lognormal over decades.
fn trades(n: usize, minutes: usize, seed: u64) -> Vec<Vec<Value>> {
    let mut rng = Rng(seed | 1);
    let mut price = 60_000.0;
    (0..n * minutes)
        .map(|i| {
            price += rng.normal() * 0.5;
            let bps = if i % 3 == 0 { 0.0 } else { rng.normal() * 3.0 + 0.5 };
            let notional = (rng.normal() * 2.0).exp() * 100.0;
            let t = (i as f64 / n as f64 * 60e6) as i64;
            let symbol = if i % 5 == 0 { "ETH" } else { "BTC" };
            vec![
                Value::Time(t),
                Value::Str(symbol.into()),
                Value::F64((price * 100.0f64).round() / 100.0),
                Value::F64(bps),
                Value::F64(notional),
            ]
        })
        .collect()
}

/// What `chunks` emit, restored from a checkpoint after the chunks `restores` names.
fn run(cat: &Catalog, chunks: &[Vec<Vec<Value>>], restores: &[usize]) -> Vec<Emit> {
    let (mut e, mut out) = (Engine::new(cat).unwrap(), vec![]);
    for (i, rows) in chunks.iter().enumerate() {
        e.insert("trades", rows.clone(), &mut out);
        if restores.contains(&i) {
            let bytes = postcard::to_allocvec(&e.snapshot()).unwrap();
            e = Engine::new(cat).unwrap();
            e.restore(postcard::from_bytes(&bytes).unwrap()).unwrap();
        }
    }
    e.close_until(i64::MAX / 2, &mut out);
    out
}

/// How far in rank `r` is from the quantile at `level` of `sorted`, over the values' count: 0 if
/// it lies between the values around rank `level * n - 0.5`.
fn rank_error(sorted: &[f32], r: f32, level: f64) -> f64 {
    let n = sorted.len();
    let t = (level * n as f64 - 0.5).clamp(0.0, (n - 1) as f64);
    let (below, at_most) = (sorted.partition_point(|&v| v < r), sorted.partition_point(|&v| v <= r));
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

fn message(m: &Emit) -> serde_json::Value {
    serde_json::from_str(&m.payload).unwrap()
}

/// Every minute's quantiles of every symbol, in windows of 16,000 and 64,000 values: within 0.25%
/// of the values in rank of the exact quantile of the values as Float32 (the digest's precision;
/// 0.07% at most measured, ClickHouse's own epsilon is 1%).
#[test]
fn big_windows_read_quantiles_close_in_rank_to_the_exact_ones() {
    let cat = parse(SQL).unwrap();
    for (n, seed) in [(20_000, 7), (80_000, 11)] {
        let rows = trades(n, 3, seed);
        let out = run(&cat, std::slice::from_ref(&rows), &[]);
        let mut checked = 0;
        for m in &out {
            let j = message(m);
            let (symbol, start) = (j["symbol"].as_str().unwrap(), j["start"].as_i64().unwrap());
            let column = |c: usize| -> Vec<f32> {
                let mut v: Vec<f32> = rows
                    .iter()
                    .filter(|r| {
                        r[1] == Value::Str(symbol.into())
                            && matches!(r[0], Value::Time(t) if t / 60_000_000 * 60_000_000 == start)
                    })
                    .map(|r| if let Value::F64(x) = r[c] { x as f32 } else { unreachable!() })
                    .collect();
                v.sort_by(f32::total_cmp);
                v
            };
            let cols: &[(&str, usize, f64)] = if &*m.topic == "out" {
                &[("price_median", 2, 0.5), ("bps_median", 3, 0.5), ("bps_p95", 3, 0.95), ("notional_median", 4, 0.5)]
            } else {
                &[("p01", 2, 0.01), ("p99", 2, 0.99)]
            };
            for &(name, c, level) in cols {
                let sorted = column(c);
                assert!(sorted.len() > 3_000, "{symbol} {start}: {} values", sorted.len());
                let r = j[name].as_f64().unwrap() as f32;
                let e = rank_error(&sorted, r, level);
                assert!(e <= 0.0025, "{symbol} {start} {name}: {r} is {:.3}% off in rank", e * 100.0);
                checked += 1;
            }
        }
        assert_eq!(checked, 2 * 3 * 6, "two symbols, three minutes, six quantiles");
    }
}

/// The median and p95 of one argument read one digest: the same results as views of one quantile
/// each would give (the median's view here, and a p95 view of its own).
#[test]
fn quantiles_of_one_argument_read_one_digest() {
    let cat = parse(&format!(
        "{SQL}
CREATE EXTERNAL STREAM p95 (symbol string, start int64, bps_p95 float32) SETTINGS type = 'kafka', topic = 'p95', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW p INTO p95 AS SELECT symbol, to_unix_timestamp64_micro(window_start) AS start, quantile_t_digest(0.95)(bps) AS bps_p95
FROM tumble(trades, t, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;"
    ))
    .unwrap();
    let out = run(&cat, &[trades(30_000, 2, 3)], &[]);
    let alone: Vec<_> = out.iter().filter(|m| &*m.topic == "p95").map(message).collect();
    let shared: Vec<_> = out.iter().filter(|m| &*m.topic == "out").map(message).collect();
    assert_eq!(alone.len(), 4);
    for (a, s) in alone.iter().zip(&shared) {
        assert_eq!((&a["symbol"], &a["start"], &a["bps_p95"]), (&s["symbol"], &s["start"], &s["bps_p95"]));
    }
}

/// Row by row, in one chunk, and row by row restored from a checkpoint after every row of each
/// window's lead (its last 2 s but 0.5 s, where its groups' digests merge ahead of the close,
/// ADR-0015): the same messages (the views' in another order).
#[test]
fn digests_merged_ahead_of_the_close_emit_the_same_row_by_row() {
    let cat = parse(SQL).unwrap();
    let rows = trades(3_000, 3, 5);
    // a chunk closing several windows marks each with the earliest end (`Emit::window_end`), row by
    // row each its own: what is written is the same
    let sorted = |mut v: Vec<Emit>| {
        v.sort_by(|a, b| (&a.topic, &a.payload).cmp(&(&b.topic, &b.payload)));
        v.iter_mut().for_each(|e| e.window_end = 0);
        v
    };
    let want = sorted(run(&cat, std::slice::from_ref(&rows), &[]));
    assert_eq!(want.len(), 12);
    let one_by_one: Vec<_> = rows.iter().map(|r| vec![r.clone()]).collect();
    assert!(sorted(run(&cat, &one_by_one, &[])) == want);
    let in_lead =
        |r: &Vec<Value>| matches!(r[0], Value::Time(t) if (58_000_000..59_500_000).contains(&(t % 60_000_000)));
    let restores: Vec<_> = (0..rows.len()).filter(|&i| in_lead(&rows[i])).collect();
    assert_eq!(restores.len(), 3 * 75);
    assert!(sorted(run(&cat, &one_by_one, &restores)) == want);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Any chunking of windows that merge many times, restored from a checkpoint after any
    /// chunks: the messages of one chunk, uninterrupted.
    #[test]
    fn chunked_and_restored_digests_emit_the_same(
        pieces in proptest::collection::vec(1usize..5_000, 1..12),
        restores in proptest::collection::vec(0usize..60, 0..4),
        seed in any::<u64>(),
    ) {
        let cat = parse(SQL).unwrap();
        let rows = trades(6_000, 2, seed);
        let want = run(&cat, std::slice::from_ref(&rows), &[]);
        let (mut chunks, mut at, mut i) = (vec![], 0, 0);
        while at < rows.len() {
            let n = pieces[i % pieces.len()].min(rows.len() - at);
            chunks.push(rows[at..at + n].to_vec());
            (at, i) = (at + n, i + 1);
        }
        prop_assert!(run(&cat, &chunks, &restores) == want);
    }
}
