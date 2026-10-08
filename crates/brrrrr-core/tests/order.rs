//! A view's `ORDER BY` sorts as Proton (ClickHouse) does, NULL and NaN keys included:
//! first the values, then NaN, then NULL ("Sorting of Special Values", ClickHouse's ORDER BY
//! reference: the default, which is `ASC NULLS LAST`, the only order the planner takes). Rust's
//! sort is free to panic on a comparison that is not a total order, as `compare` (`None` for NULL
//! and NaN) taken as `Equal` was not: a window view ordered by an aggregate that can be NULL, or
//! an ASOF join's subquery ordered by a time that can be NULL, panicked the data loop.
use crate::common;
use brrrrr_core::engine::{Asof, Engine, State};
use brrrrr_core::expr::{compare, order_by};
use brrrrr_core::sql::parse;
use brrrrr_core::value::{Type, Value};
use proptest::prelude::*;
use std::cmp::Ordering;
use std::sync::Arc;

fn engine(sql: &str) -> Engine {
    Engine::new(&parse(sql).unwrap()).unwrap()
}

fn secs(s: f64) -> Value {
    Value::Time((s * 1e6).round() as i64)
}

/// A JSONEachRow payload's string field (NaN and NULL both read `null`: a test that must tell
/// them apart names its rows).
fn field(payload: &str, name: &str) -> String {
    let j: serde_json::Value = serde_json::from_str(payload).unwrap();
    j[name].as_str().unwrap_or_else(|| panic!("{name} in {payload}")).to_string()
}

/// Symbols in `a_window_ordered_by_...`: the old sort panicked on them.
const N: usize = 40;

/// The next of a xorshift sequence: a fixed seed, the same draws on every run.
fn xorshift(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

const LOWS: &str = "
CREATE STREAM trades (t datetime64(6), symbol string, price nullable(float64));
CREATE EXTERNAL STREAM lows (symbol string, low nullable(float64))
  SETTINGS type = 'kafka', topic = 'lows', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW v INTO lows AS
SELECT symbol, min(price) AS low FROM tumble(trades, t, 1s) GROUP BY window_start, symbol ORDER BY low
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;
";

/// A window view ordered by its own `min(price)`, NULL for a symbol whose prices all are and NaN
/// for one whose only price is: its windows' rows close in one sort, which used to panic.
#[test]
fn a_window_ordered_by_an_aggregate_that_can_be_null_puts_nan_then_null_last() {
    let mut e = engine(LOWS);
    // more symbols than an insertion sort takes (20), one price each: a tenth NULL, a tenth
    // NaN, the others on 80 prices (ties among them), in an order the sort found inconsistent
    let mut x = 0x2545_f491_4f6c_dd1du64;
    let draws: Vec<u64> = (0..N).map(|_| xorshift(&mut x) % 100).collect();
    let price = |i: usize| match draws[i] {
        0..10 => Value::Null,
        10..20 => Value::F64(f64::NAN),
        h => Value::F64(h as f64 - 58.0),
    };
    let rows: Vec<_> = (0..N).map(|i| vec![secs(0.5), Value::Str(format!("s{i:04}").into()), price(i)]).collect();
    let mut out = vec![];
    e.insert("trades", rows, &mut out);
    e.insert("trades", vec![vec![secs(1.5), Value::Str("z".into()), Value::F64(0.0)]], &mut out);
    let got: Vec<String> = out.iter().map(|m| field(&m.payload, "symbol")).collect();
    // the window closes its groups in key text order; the sort is stable: equal lows keep it
    let mut want: Vec<usize> = (0..N).collect();
    let rank = |i: &usize| match price(*i) {
        Value::F64(f) if f.is_nan() => (1, 0),
        Value::F64(f) => (0, f as i64),
        _ => (2, 0),
    };
    want.sort_by_key(rank);
    let want: Vec<String> = want.iter().map(|i| format!("s{i:04}")).collect();
    assert_eq!(got, want);
}

const ASOF: &str = "
CREATE STREAM IF NOT EXISTS trades (event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS quotes (event_time datetime64(6), symbol string, ask float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (event_time datetime64(6), symbol string, price float64, ask nullable(float64))
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS joined INTO out AS
SELECT t.event_time AS event_time, t.symbol AS symbol, t.price AS price, null_if(q.ask, 0) AS ask
FROM (SELECT event_time, symbol, price FROM trades ORDER BY symbol, event_time) AS t
ASOF LEFT JOIN (SELECT event_time, symbol, ask FROM quotes ORDER BY symbol, event_time) AS q
ON t.symbol = q.symbol AND t.event_time >= q.event_time
SETTINGS keep_versions = 1000;
";

/// An exact ASOF join whose subqueries (`ORDER BY symbol, event_time`) take a chunk in which
/// every third row has no time: the sort puts those last, the join drops and counts them, and
/// every timed trade still takes its symbol's quote at or before it, in time order.
#[test]
fn an_exact_asof_join_whose_subquery_sorts_rows_without_a_time_matches_the_others() {
    // 300 quotes in a scrambled order, 5 symbols, at distinct ms (7919 is prime to 300), every
    // third without a time; trades 0.5 ms after them, a different third without one. The ask
    // and the price name the quote and the trade.
    let ms = |i: usize| (i * 7919 % 300) as f64 / 1000.0;
    let symbol = |i: usize| Value::Str(["A", "B", "C", "D", "E"][i % 5].into());
    let timed = |q: usize| !q.is_multiple_of(3);
    let at = |t: bool, s: f64| if t { secs(s) } else { Value::Null };
    let quotes: Vec<_> = (0..300).map(|i| vec![at(timed(i), ms(i)), symbol(i), Value::F64(i as f64 + 1.0)]).collect();
    let trades: Vec<_> =
        (0..300).map(|i| vec![at(timed(i + 1), ms(i) + 0.0005), symbol(i), Value::F64(i as f64)]).collect();
    for asof in [Asof::Exact, Asof::Arrival] {
        let mut e = engine(ASOF);
        e.set_asof(asof);
        let mut out = vec![];
        e.insert("quotes", quotes.clone(), &mut out);
        e.insert("trades", trades.clone(), &mut out);
        // quotes far later release every held trade
        let late: Vec<_> = (0..5).map(|i| vec![secs(1000.0), symbol(i), Value::F64(-1.0)]).collect();
        e.insert("quotes", late, &mut out);
        let got: Vec<(f64, Option<f64>)> = out
            .iter()
            .map(|m| {
                let j: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
                (j["price"].as_f64().unwrap(), j["ask"].as_f64())
            })
            .collect();
        // the model: each timed trade, in time order (exact) or its subquery's order (arrival:
        // symbol, then time), with the ask of its symbol's latest timed quote at or before it
        let time = |i: usize| i * 7919 % 300;
        let mut want: Vec<(usize, usize, usize)> =
            (0..300).filter(|i| timed(i + 1)).map(|i| (time(i), i % 5, i)).collect();
        match asof {
            Asof::Exact => want.sort(),
            Asof::Arrival => want.sort_by_key(|&(t, s, i)| (s, t, i)),
        }
        let want: Vec<(f64, Option<f64>)> = want
            .iter()
            .map(|&(t, s, i)| {
                let q = (0..300).filter(|q| timed(*q) && q % 5 == s && time(*q) <= t).max_by_key(|q| time(*q));
                (i as f64, q.map(|q| q as f64 + 1.0))
            })
            .collect();
        assert_eq!(got, want, "{asof:?}");
        assert_eq!(e.null_time(), 200, "{asof:?}: 100 quotes and 100 trades without a time");
    }
}

const HOLD: &str = "
CREATE STREAM IF NOT EXISTS raw (local_event_time datetime64(6), id string, price nullable(float64));
CREATE EXTERNAL STREAM IF NOT EXISTS rows_out (local_event_time datetime64(6), id string, price nullable(float64))
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'rows', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS sort INTO rows_out AS
SELECT local_event_time, id, price FROM raw ORDER BY local_event_time, price
SETTINGS order_hold_ms = 50;
";

fn priced(s: f64, id: &str, price: Option<f64>) -> Vec<Value> {
    vec![secs(s), Value::Str(id.into()), price.map_or(Value::Null, Value::F64)]
}

fn ids(out: &[brrrrr_core::engine::Emit]) -> Vec<String> {
    out.iter().map(|m| field(&m.payload, "id")).collect()
}

/// The rows of one time, in a held sort (`SETTINGS order_hold_ms`) on (time, price): by price,
/// -0 and 0 equal (in arrival order), the infinities at either end of the numbers, then NaN,
/// then NULL; and the order survives a checkpoint (a restored hold is checked to be in it).
#[test]
fn a_held_sort_puts_nan_then_null_last_among_rows_of_one_time() {
    let rows = vec![
        priced(1.0, "two", Some(2.0)),
        priced(1.0, "null1", None),
        priced(1.0, "nan1", Some(f64::NAN)),
        priced(1.0, "one", Some(1.0)),
        priced(1.0, "-zero", Some(-0.0)),
        priced(1.0, "zero", Some(0.0)),
        priced(1.0, "nan2", Some(-f64::NAN)),
        priced(1.0, "null2", None),
        priced(1.0, "-inf", Some(f64::NEG_INFINITY)),
        priced(1.0, "inf", Some(f64::INFINITY)),
    ];
    let want = ["-inf", "-zero", "zero", "one", "two", "inf", "nan1", "nan2", "null1", "null2"];
    let mut out = vec![];
    let mut e = engine(HOLD);
    e.insert("raw", rows.clone(), &mut out);
    assert!(out.is_empty(), "held");
    e.insert("raw", vec![priced(2.0, "later", Some(0.0))], &mut out);
    assert_eq!(ids(&out), want);
    // one at a time, and checkpointed and restored after each: the same order
    let mut e = engine(HOLD);
    for r in rows {
        e.insert("raw", vec![r], &mut vec![]);
        let state: State = postcard::from_bytes(&postcard::to_allocvec(&e.snapshot()).unwrap()).unwrap();
        e = engine(HOLD);
        e.restore(state).unwrap();
    }
    let mut out = vec![];
    e.insert("raw", vec![priced(2.0, "later", Some(0.0))], &mut out);
    assert_eq!(ids(&out), want);
}

/// Values in `order_by` order, a step at a time: those of one step are equal (whatever their
/// type, and as `compare` has them), each step is before every later one. Integers compare
/// exactly with floats on either side of 2^53, 2^63 and 2^64, where doubles round them.
fn ladder() -> Vec<Vec<Value>> {
    use Value::*;
    let arr = |v: &[Value]| Array(v.to_vec().into());
    let two = |e: i32| 2f64.powi(e);
    vec![
        vec![F64(f64::NEG_INFINITY), F32(f32::NEG_INFINITY)],
        vec![F64(-two(64))],
        vec![Int(i64::MIN), Time(i64::MIN), F64(-two(63))],
        vec![Int(i64::MIN + 1)],
        vec![Int(-2), Time(-2), F32(-2.0)],
        vec![F64(-1.5), F32(-1.5)],
        vec![Int(-1), F64(-1.0)],
        vec![F64(-0.5)],
        vec![Bool(false), Int(0), UInt(0), Time(0), F64(0.0), F64(-0.0), F32(-0.0)],
        vec![F64(f64::MIN_POSITIVE)],
        vec![F32(0.5)],
        vec![Bool(true), UInt(1), F64(1.0)],
        vec![F64(1.5)],
        vec![Int(1 << 53), UInt(1 << 53), F64(two(53))],
        vec![Int((1 << 53) + 1), UInt((1 << 53) + 1), Time((1 << 53) + 1)],
        vec![F64(two(53) + 2.0), Int((1 << 53) + 2)],
        vec![Int(i64::MAX)],
        vec![UInt(1 << 63), F64(two(63))],
        vec![UInt(u64::MAX)],
        vec![F64(two(64)), F32(two(64) as f32)],
        vec![F64(f64::MAX)],
        vec![F64(f64::INFINITY), F32(f32::INFINITY)],
        vec![Str("".into())],
        vec![Str("A".into())],
        vec![Str("a".into())],
        vec![Str("ab".into())],
        vec![arr(&[])],
        vec![arr(&[Int(1)]), arr(&[F64(1.0)])],
        vec![arr(&[Int(1), Int(0)])],
        vec![arr(&[Int(1), Str("x".into())])],
        vec![arr(&[Int(1), F64(f64::NAN)])],
        vec![arr(&[Int(1), Null])],
        vec![arr(&[UInt(2)])],
        vec![arr(&[Str("x".into())])],
        vec![arr(&[F64(f64::NAN)]), arr(&[F32(-f32::NAN)])],
        vec![arr(&[Null])],
        vec![F64(f64::NAN), F64(-f64::NAN), F32(f32::NAN), F32(-f32::NAN), F64(f64::from_bits(0x7ff0_0000_0000_0001))],
        vec![Null],
    ]
}

#[test]
fn values_sort_by_number_then_string_then_array_then_nan_then_null() {
    let ladder = ladder();
    for (i, a) in ladder.iter().enumerate() {
        for (j, b) in ladder.iter().enumerate() {
            for x in a {
                for y in b {
                    assert_eq!(order_by(x, y), i.cmp(&j), "{x:?} against {y:?}");
                }
            }
        }
    }
}

/// Any value: NULL, NaN of either sign (and of either float type), the zeros and infinities,
/// integers of every type near 0 (ties across types) and anywhere, near 2^53 (where doubles
/// round integers), floats anywhere and at quarters, times, strings, and arrays of them.
fn any_value() -> impl Strategy<Value = Value> {
    let two53 = 1i64 << 53;
    let scalar = prop_oneof![
        Just(Value::Null),
        prop_oneof![Just(f64::NAN), Just(-f64::NAN), Just(0.0), Just(-0.0), Just(f64::INFINITY), Just(-f64::INFINITY)]
            .prop_map(Value::F64),
        prop_oneof![Just(f32::NAN), Just(-f32::NAN), Just(-0.0), Just(f32::INFINITY)].prop_map(Value::F32),
        any::<bool>().prop_map(Value::Bool),
        prop_oneof![-3i64..3, any::<i64>(), two53 - 3..two53 + 3].prop_map(Value::Int),
        prop_oneof![0u64..3, any::<u64>(), two53 as u64 - 3..two53 as u64 + 3].prop_map(Value::UInt),
        prop_oneof![-3i64..3, any::<i64>()].prop_map(Value::Time),
        prop_oneof![
            (-12i32..12).prop_map(|q| q as f64 / 4.0),
            any::<f64>(),
            (two53 - 3..two53 + 3).prop_map(|i| i as f64)
        ]
        .prop_map(Value::F64),
        (-12i32..12).prop_map(|q| Value::F32(q as f32 / 4.0)),
        "[ab]{0,2}".prop_map(|s| Value::Str(s.into())),
    ];
    scalar.prop_recursive(2, 12, 3, |inner| proptest::collection::vec(inner, 0..3).prop_map(|v| Value::Array(v.into())))
}

/// Whether `compare` reads `v` exactly: a 64-bit integer of 2^53 or more may be compared with
/// a value of another type as the double it rounds to.
fn exact(v: &Value) -> bool {
    match v {
        Value::Int(i) | Value::Time(i) => i.unsigned_abs() <= 1 << 53,
        Value::UInt(u) => *u <= 1 << 53,
        Value::Array(a) => a.iter().all(exact),
        _ => true,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// A total order: each value equals itself, the order reversed is the reverse, and `<=` and
    /// `=` are transitive. And `compare`'s, wherever it has an answer, but where it rounds
    /// integers to doubles that tie.
    #[test]
    fn order_by_is_a_total_order_that_agrees_with_compare(a in any_value(), b in any_value(), c in any_value()) {
        prop_assert_eq!(order_by(&a, &a), Ordering::Equal);
        for (x, y, z) in [(&a, &b, &c), (&a, &c, &b), (&b, &a, &c), (&b, &c, &a), (&c, &a, &b), (&c, &b, &a)] {
            prop_assert_eq!(order_by(x, y), order_by(y, x).reverse(), "{:?} against {:?}", x, y);
            if order_by(x, y).is_le() && order_by(y, z).is_le() {
                prop_assert!(order_by(x, z).is_le(), "{:?} <= {:?} <= {:?}", x, y, z);
            }
            if order_by(x, y).is_eq() && order_by(y, z).is_eq() {
                prop_assert!(order_by(x, z).is_eq(), "{:?} = {:?} = {:?}", x, y, z);
            }
            // a rounded tie decides nothing but an array's next element: before or after it,
            // two scalars are ordered as `compare` orders them
            let scalars = !matches!(x, Value::Array(_)) && !matches!(y, Value::Array(_));
            match compare(x, y) {
                Some(o) if exact(x) && exact(y) || scalars && o.is_ne() => {
                    prop_assert_eq!(order_by(x, y), o, "{:?} against {:?}", x, y)
                }
                _ => {}
            }
        }
    }

    /// Any values, sorted by `order_by` as `Op::Sort` sorts rows: no panic, in order, NULLs at
    /// the end, the NaNs just before them.
    #[test]
    fn any_values_sort_with_nan_then_null_last(mut v in proptest::collection::vec(any_value(), 0..200)) {
        v.sort_by(order_by);
        prop_assert!(v.is_sorted_by(|a, b| order_by(a, b).is_le()));
        let nan = |x: &Value| matches!(x, Value::F64(f) if f.is_nan()) || matches!(x, Value::F32(f) if f.is_nan());
        let tail = v.iter().rev().skip_while(|x| x.is_null()).skip_while(|x| nan(x));
        prop_assert!(tail.clone().all(|x| !x.is_null() && !nan(x)), "{:?}", v);
    }
}

/// A random value of type `t`: NULL `null` times in a hundred, whatever the type (a source's
/// absent field is NULL), and NaN or an infinity as often for floats. Times within ten
/// minutes, a few symbols and ids: windows close, keys tie.
fn random(t: &Type, x: &mut u64, null: u64) -> Value {
    let r = xorshift(x) >> 8;
    if r % 100 < null {
        return Value::Null;
    }
    let float = |r: u64| match r % 100 {
        n if n < null => f64::NAN,
        n if n < null + 2 => [f64::INFINITY, f64::NEG_INFINITY][(r / 100 % 2) as usize],
        _ => (r / 100 % 2001) as f64 / 10.0 - 100.0,
    };
    match t.base() {
        Type::Bool => Value::Bool(r & 1 == 1),
        Type::Int(_) => Value::Int((r % 7) as i64 - 3),
        Type::UInt(_) => Value::UInt(r % 7),
        Type::F32 => Value::F32(float(r / 7) as f32),
        Type::F64 => Value::F64(float(r / 7)),
        Type::Str => Value::Str(["A", "B", "C", "1", "2", ""][(r % 6) as usize].into()),
        // near the times the pipelines make of random integers (`-3` µs): a key's rows decades
        // apart would have state.sql's one-second gap fill write every second between them
        Type::Time(_) => Value::Time((r % 600_000) as i64 * 1000),
        Type::Array(e) => Value::Array((0..r % 3).map(|_| random(e, x, null)).collect()),
        _ => Value::Array(Arc::new([])),
    }
}

/// Every example pipeline (fixtures/pipelines) with an `ORDER BY`.
fn sorting_pipelines() -> Vec<(String, String)> {
    let dirs = ["pipelines"];
    let mut v: Vec<(String, String)> = dirs
        .iter()
        .flat_map(|d| std::fs::read_dir(common::root(&format!("fixtures/{d}"))).unwrap())
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .map(|p| (p.display().to_string(), std::fs::read_to_string(&p).unwrap()))
        .filter(|(_, sql)| sql.contains("ORDER BY"))
        .collect();
    v.sort();
    assert_eq!(v.len(), 6, "the pipelines that sort");
    v
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    /// Every pipeline that sorts, its every stream (sources and the edges its sorting views
    /// read) fed chunks of random rows whose keys may be NULL, NaN or infinite, then closed:
    /// nothing panics.
    #[test]
    fn no_pipeline_panics_on_null_or_nan_keys(seed in any::<u64>(), rows in 1usize..120, null in 1u64..40) {
        let mut x = seed | 1;
        for (path, sql) in sorting_pipelines() {
            let cat = parse(&sql).unwrap();
            let mut e = Engine::new(&cat).unwrap();
            let mut out = vec![];
            let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for _ in 0..2 {
                    for s in cat.streams.values() {
                        let chunk = (0..rows).map(|_| s.columns.iter().map(|c| random(&c.ty, &mut x, null)).collect());
                        e.insert(&s.name, chunk.collect(), &mut out);
                    }
                }
                // an hour past the rows' last time (`random`) closes every window they opened; much
                // further, and state.sql's one-second gap fill writes a row for every second between
                e.close_until(600_000_000 + 3_600_000_000, &mut out);
            }));
            prop_assert!(ran.is_ok(), "{} panicked", path);
        }
    }
}

