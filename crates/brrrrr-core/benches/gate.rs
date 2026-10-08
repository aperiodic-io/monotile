//! Instruction-count gate (ADR-0010). Every hot path gets a benchmark here; CI fails a PR that
//! adds more than 2% instructions over its merge base.
use brrrrr_core::book::{Book, Level};
use brrrrr_core::engine::Engine;
use brrrrr_core::format::float;
use brrrrr_core::proto::{parse_proto, Codec};
use brrrrr_core::value::{Type, Value};
use gungraun::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

fn floats() -> Vec<f64> {
    (0..2_000u64).map(|i| (i as f64 * 1.37 + 0.001).powf(1.7) / 3.0).collect()
}

#[library_benchmark]
#[bench::two_thousand(floats())]
fn float_layout(xs: Vec<f64>) -> usize {
    let mut s = String::with_capacity(64 * xs.len());
    for x in &xs {
        float(&mut s, *x, false);
        float(&mut s, *x, true);
    }
    black_box(s.len())
}

fn order_book_messages() -> (Codec, Vec<Vec<u8>>) {
    let msgs = parse_proto(include_str!("../../../fixtures/market.proto")).unwrap();
    let cols: Vec<(String, Type)> = [
        ("time", "int64"),
        ("bid_price", "array(float64)"),
        ("bid_amount", "array(float64)"),
        ("ask_price", "array(float64)"),
        ("ask_amount", "array(float64)"),
        ("is_snapshot", "bool"),
    ]
    .iter()
    .map(|(n, t)| (n.to_string(), Type::parse(t).unwrap()))
    .collect();
    let codec = Codec::new(&msgs["BookUpdate"], &cols);
    let bytes = (0..500)
        .map(|i| {
            let lv = |k: i64| Value::Array((0..20).map(|j| Value::F64((i * 7 + j * k) as f64)).collect());
            let row = vec![Value::Int(i), lv(1), lv(2), lv(3), lv(4), Value::Bool(false)];
            let mut b = vec![];
            codec.encode(&row, &mut b);
            b
        })
        .collect();
    (codec, bytes)
}

#[library_benchmark]
#[bench::five_hundred(order_book_messages())]
fn decode_order_book((codec, msgs): (Codec, Vec<Vec<u8>>)) -> usize {
    msgs.iter().map(|m| codec.decode(m).unwrap().len()).sum()
}

fn book_stream() -> Vec<(Vec<Level>, Vec<Level>)> {
    (0..5_000i64)
        .map(|i| {
            let mid = 10_000 + (i * 7919 % 21) - 10;
            let b = vec![Level { price: (mid - 1 - i % 40) as f64, amount: (i % 5) as f64 }];
            let a = vec![Level { price: (mid + 1 + i % 37) as f64, amount: (i % 3) as f64 }];
            (b, a)
        })
        .collect()
}

#[library_benchmark]
#[bench::five_thousand(book_stream())]
fn book_apply(msgs: Vec<(Vec<Level>, Vec<Level>)>) -> usize {
    let mut book = Book::new(25);
    let snap_b: Vec<Level> = (1..=500).map(|j| Level { price: (10_000 - j) as f64, amount: 1.0 }).collect();
    let snap_a: Vec<Level> = (1..=500).map(|j| Level { price: (10_000 + j) as f64, amount: 1.0 }).collect();
    book.apply(true, &snap_b, &snap_a);
    msgs.iter().filter(|(b, a)| book.apply(false, b, a)).count()
}

/// 3,000 in-order trades of three symbols, `step_us` apart, as rows of fixtures/market.proto's
/// Trade (the example pipelines' `trades` source), in chunks of 100.
fn trades(step_us: i64, price: impl Fn(i64) -> f64, side: impl Fn(i64) -> &'static str) -> Vec<Vec<Vec<Value>>> {
    let rows: Vec<Vec<Value>> = (0..3_000i64)
        .map(|i| {
            let local = 1_788_220_800_000_000 + i * step_us;
            let s = |x: &str| Value::Str(x.into());
            let p = price(i);
            vec![
                Value::Int(local),
                s(&i.to_string()),
                Value::Int(1),
                s(["A", "B", "C"][i as usize % 3]),
                Value::F64(p),
                Value::Int(local),
                s(side(i)),
                Value::F64(1.0),
                Value::F64(p),
            ]
        })
        .collect();
    rows.chunks(100).map(<[_]>::to_vec).collect()
}

/// The `stats` example pipeline (two sources merged into one stream, `quantile_cont` and t-digest
/// medians, moments, size buckets, JSON sinks) over 3,000 in-order trades a second apart: the
/// engine's whole hot path. A bench's id names its workload: a new workload gets a new id, so the
/// gate compares like with like and its first run is the baseline.
fn stats_pipeline() -> (Engine, Vec<Vec<Vec<Value>>>) {
    let cat = brrrrr_core::sql::parse(include_str!("../../../fixtures/pipelines/stats.sql")).unwrap();
    let chunks = trades(1_000_000, |i| 100.0 + (i * 37 % 1000) as f64 / 100.0, |_| "buy");
    (Engine::new(&cat).unwrap(), chunks)
}

#[library_benchmark]
#[bench::three_thousand_trades(stats_pipeline())]
fn engine_stats((mut engine, chunks): (Engine, Vec<Vec<Vec<Value>>>)) -> usize {
    let mut out = vec![];
    for c in chunks {
        engine.insert("trades", c, &mut out);
    }
    black_box(out.len())
}

/// The `returns` example pipeline (lagged returns, `run_structure`, `updownticks` and
/// `trade_returns` per minute, behind a sorted input) over 3,000 in-order trades 20 ms apart.
fn returns_pipeline() -> (Engine, Vec<Vec<Vec<Value>>>) {
    let cat = brrrrr_core::sql::parse(include_str!("../../../fixtures/pipelines/returns.sql")).unwrap();
    let chunks =
        trades(20_000, |i| 100.0 + (i * 37 % 11) as f64 / 10.0, |i| ["buy", "sell", "buy"][(i * 7 % 3) as usize]);
    (Engine::new(&cat).unwrap(), chunks)
}

#[library_benchmark]
#[bench::three_thousand_trades(returns_pipeline())]
fn engine_returns((mut engine, chunks): (Engine, Vec<Vec<Vec<Value>>>)) -> usize {
    let mut out = vec![];
    for c in chunks {
        engine.insert("trades", c, &mut out);
    }
    black_box(out.len())
}

library_benchmark_group!(
    name = gate;
    benchmarks = float_layout, decode_order_book, book_apply, engine_stats, engine_returns
);
main!(library_benchmark_groups = gate);
