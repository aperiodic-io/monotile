//! `run_structure` and `updownticks`: sequence metrics as a batch engine computes them with LAG
//! over a sorted window, comparing each trade with the previous one in its window in `(time, id)`
//! order. Window functions in Proton can only follow arrival order and keep every trade of a
//! window, so both are brrrrr aggregates: they fold trades in sorted order into a fixed-size
//! summary. Their input is sorted by time upstream (`sorted`: a view's `ORDER BY`
//! with `SETTINGS order_hold_ms`, ADR-0013); each holds back only the trades of the newest time,
//! to order ties its own way.
use brrrrr_core::agg::SEQUENCE_REORDER;
use brrrrr_core::engine::Engine;
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;
use proptest::prelude::*;
use serde_json::Value as J;

const SQL: &str = "
CREATE STREAM IF NOT EXISTS trades_raw (local_event_time datetime64(6), symbol string, id string, side string, price float64, size float64);
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, id string, side string, price float64, size float64);
CREATE MATERIALIZED VIEW IF NOT EXISTS sorted INTO trades AS
SELECT local_event_time, symbol, id, side, price, size FROM trades_raw ORDER BY local_event_time
SETTINGS order_hold_ms = 1000;
CREATE EXTERNAL STREAM IF NOT EXISTS rs_out (
  symbol string, time int64,
  buy_run_max_len float64, sell_run_max_len float64, buy_run_mean_len float64, sell_run_mean_len float64,
  run_imbalance float64, flip_rate float64, price_change_on_flip nullable(float64)
) SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'rs.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE EXTERNAL STREAM IF NOT EXISTS ud_out (
  symbol string, time int64,
  uptick_count int32, downtick_count int32, unchanged_count int32,
  uptick_volume float64, downtick_volume float64, unchanged_volume float64
) SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'ud.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS rs INTO rs_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time,
  run_structure((local_event_time, id, side, price))[1] AS buy_run_max_len, run_structure((local_event_time, id, side, price))[2] AS sell_run_max_len,
  run_structure((local_event_time, id, side, price))[3] AS buy_run_mean_len, run_structure((local_event_time, id, side, price))[4] AS sell_run_mean_len,
  coalesce((buy_run_max_len - sell_run_max_len) / null_if(buy_run_max_len + sell_run_max_len, 0), 0) AS run_imbalance,
  run_structure((local_event_time, id, side, price))[5] / run_structure((local_event_time, id, side, price))[6] AS flip_rate, run_structure((local_event_time, id, side, price))[7] AS price_change_on_flip
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS ud INTO ud_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time,
  updownticks((local_event_time, id, price, size))[1] AS uptick_count, updownticks((local_event_time, id, price, size))[2] AS downtick_count, updownticks((local_event_time, id, price, size))[3] AS unchanged_count,
  updownticks((local_event_time, id, price, size))[4] AS uptick_volume, updownticks((local_event_time, id, price, size))[5] AS downtick_volume, updownticks((local_event_time, id, price, size))[6] AS unchanged_volume
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;
";

/// One trade: seconds, id, side, price, size.
type Trade = (f64, &'static str, &'static str, f64, f64);
type Owned = (f64, String, &'static str, f64, f64);

/// `sorted`'s hold, in seconds: the proptest's arrival order moves a trade by under 1 s.
const HOLD: f64 = 1.0;

fn row(symbol: &str, (secs, id, side, price, size): (f64, String, &str, f64, f64)) -> Vec<Value> {
    vec![
        Value::Time((secs * 1e6).round() as i64),
        Value::Str(symbol.into()),
        Value::Str(id.into()),
        Value::Str(side.into()),
        Value::F64(price),
        Value::F64(size),
    ]
}

/// Inserts `chunks` of rows for symbol A, then closes every window, and returns the messages
/// per topic, keyed by window time.
fn run(chunks: Vec<Vec<Owned>>) -> (Vec<J>, Vec<J>) {
    let mut e = Engine::new(&parse(SQL).unwrap()).unwrap();
    let mut out = vec![];
    for c in chunks {
        e.insert("trades_raw", c.into_iter().map(|t| row("A", t)).collect(), &mut out);
    }
    e.close_until(i64::MAX / 4, &mut out);
    let by = |topic: &str| {
        let mut v: Vec<J> = out
            .iter()
            .filter(|m| &*m.topic == topic)
            .map(|m| serde_json::from_str::<J>(&m.payload).unwrap())
            .filter(|j| j["symbol"] == "A")
            .collect();
        v.sort_by_key(|j| j["time"].as_i64());
        v
    };
    (by("rs.1m"), by("ud.1m"))
}

fn owned(ts: &[Trade]) -> Vec<Owned> {
    ts.iter().map(|&(t, id, side, p, s)| (t, id.to_string(), side, p, s)).collect()
}

fn close(a: &J, b: f64) {
    let a = a.as_f64().unwrap_or_else(|| panic!("{a} is not a number"));
    assert!((a - b).abs() <= 1e-9 * b.abs().max(1.0), "{a} != {b}");
}

#[test]
fn run_structure_of_a_worked_example() {
    let sides = ["buy", "buy", "sell", "sell", "sell", "buy", "sell", "buy"];
    let prices = [100.0, 101.0, 100.0, 99.0, 98.0, 99.0, 97.0, 98.0];
    let mut ts: Vec<_> = (0..8).map(|i| (i as f64 * 5.0, (i + 1).to_string(), sides[i], prices[i], 1.0)).collect();
    ts.extend((0..3).map(|i| (60.0 + i as f64 * 10.0, (100 + i).to_string(), "sell", 97.0 - i as f64, 1.0)));
    let (rs, _) = run(vec![ts]);
    assert_eq!(rs.len(), 2);
    let (a, b) = (&rs[0], &rs[1]);
    close(&a["buy_run_max_len"], 2.0);
    close(&a["sell_run_max_len"], 3.0);
    close(&a["buy_run_mean_len"], 4.0 / 3.0);
    close(&a["sell_run_mean_len"], 2.0);
    close(&a["run_imbalance"], -0.2);
    close(&a["flip_rate"], 0.5);
    let flips = [(100.0, 101.0), (99.0, 98.0), (97.0, 99.0), (98.0, 97.0)];
    close(&a["price_change_on_flip"], flips.iter().map(|(p, q)| (p / q - 1.0f64).abs()).sum::<f64>() / 4.0);
    close(&b["buy_run_max_len"], 0.0);
    close(&b["sell_run_max_len"], 3.0);
    close(&b["buy_run_mean_len"], 0.0);
    close(&b["sell_run_mean_len"], 3.0);
    close(&b["run_imbalance"], -1.0);
    close(&b["flip_rate"], 0.0);
    // no flip: NULL, not 0
    assert_eq!(b["price_change_on_flip"], J::Null);
}

#[test]
fn updownticks_compares_each_trade_with_the_previous_one_in_its_window_only() {
    // the second window must not see the first one's prices
    let fixture = |last: f64| {
        owned(&[(0.0, "1", "buy", 100.0, 1.0), (10.0, "2", "buy", last, 1.0), (60.0, "3", "buy", 101.0, 2.0)])
            .into_iter()
            .chain(owned(&[(70.0, "4", "buy", 100.5, 1.5), (80.0, "5", "buy", 100.5, 3.0)]))
            .collect::<Vec<_>>()
    };
    let (_, a) = run(vec![fixture(100.2)]);
    let (_, b) = run(vec![fixture(250.0)]);
    assert_eq!(a[1], b[1]);
    // 101 -> 100.5 down, -> 100.5 unchanged; the window's first trade is none of the three
    assert_eq!((a[1]["uptick_count"].as_i64(), a[1]["downtick_count"].as_i64()), (Some(0), Some(1)));
    assert_eq!(a[1]["unchanged_count"].as_i64(), Some(1));
    close(&a[1]["downtick_volume"], 1.5);
    close(&a[1]["unchanged_volume"], 3.0);
    close(&a[1]["uptick_volume"], 0.0);
    close(&a[0]["uptick_volume"], 1.0);
}

#[test]
fn trades_are_ordered_by_time_then_id_whatever_order_they_arrive_in() {
    // equal times: run_structure orders by TRY_CAST(id AS BIGINT) (9 < 10), updownticks by the
    // id string ("10" < "9")
    let ts = [(5.0, "9", "buy", 1.0, 1.0), (5.0, "10", "sell", 2.0, 1.0), (1.0, "11", "sell", 3.0, 1.0)];
    for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0]] {
        let (rs, ud) = run(order.iter().map(|&i| owned(&ts[i..=i])).collect());
        // run_structure: 11 sell@3, 9 buy@1, 10 sell@2: flips at 9 and 10
        close(&rs[0]["flip_rate"], 2.0 / 3.0);
        close(&rs[0]["price_change_on_flip"], ((1.0f64 / 3.0 - 1.0).abs() + 1.0) / 2.0);
        // updownticks: 3, then "10" (2), then "9" (1): two downticks
        assert_eq!(ud[0]["downtick_count"].as_i64(), Some(2), "{order:?}");
    }
}

fn one_by_one(ts: &[Trade]) -> (Vec<J>, Vec<J>) {
    run(ts.iter().map(|t| owned(std::slice::from_ref(t))).collect())
}

#[test]
fn a_trade_later_than_the_hold_is_folded_where_it_arrives_and_counted() {
    crate::common::isolated("sequence::a_trade_later_than_the_hold_is_folded_where_it_arrives_and_counted", || {
        // `sorted` releases a trade once one HOLD newer arrives; a trade arriving after that is
        // passed on out of order. The aggregates still sort it among the trades of their newest
        // time; one older than a folded trade is folded where it arrives, and counted. The counter is
        // global: the test runs in a process of its own (`isolated`).
        use std::sync::atomic::Ordering::Relaxed;
        let count = || brrrrr_core::agg::SEQUENCE_OUT_OF_ORDER.load(Relaxed);
        let before = count();
        // a tie with the last released trade is not out of order: ties keep their arrival order
        let tie = [(1.0, "1", "buy", 1.0, 1.0), (1.5 + HOLD, "3", "buy", 1.0, 1.0), (1.0, "1", "sell", 2.0, 1.0)];
        let (rs, _) = one_by_one(&tie);
        close(&rs[0]["flip_rate"], 2.0 / 3.0);
        assert_eq!(count(), before, "a tie with a released trade was counted");
        // nor with a trade the aggregates already folded: 3.5 s releases 1 s and 2 s, the
        // aggregates fold 1 s, then a trade with the same time and id arrives
        let folded_tie = [
            (1.0, "1", "buy", 1.0, 1.0),
            (2.0, "2", "buy", 1.0, 1.0),
            (3.5, "3", "buy", 1.0, 1.0),
            (1.0, "1", "sell", 2.0, 1.0),
        ];
        one_by_one(&folded_tie);
        assert_eq!(count(), before, "a tie with a folded trade was counted");
        // the trade at 3.5 s releases those at 1 s and 2 s (the aggregates fold the first); the one
        // at 1.5 s comes after them, and is still sorted in before the waiting 2 s
        let late = [
            (1.0, "1", "buy", 1.0, 1.0),
            (2.0, "2", "buy", 2.0, 1.0),
            (1.5 + HOLD * 2.0, "4", "buy", 4.0, 1.0),
            (1.5, "3", "sell", 8.0, 1.0),
        ];
        let (rs, _) = one_by_one(&late);
        close(&rs[0]["price_change_on_flip"], (7.0 + 0.75) / 2.0);
        assert_eq!(count(), before, "a trade sorted in was counted");
        // the one at 0.5 s is older than the folded 1 s: folded after it, where sorting would put it
        // first (flips 1 -> 8 and 8 -> 2 instead of 8 -> 1)
        let later = [late[0], late[1], late[2], (0.5, "3", "sell", 8.0, 1.0)];
        let (rs, _) = one_by_one(&later);
        close(&rs[0]["price_change_on_flip"], (7.0 + 0.75) / 2.0);
        // run_structure and updownticks each count it once
        assert_eq!(count(), before + 2);
    });
}

#[test]
fn a_trade_exactly_the_hold_behind_the_latest_is_still_sorted_in() {
    let ts = [(1.0, "1", "buy", 1.0, 1.0), (1.0 + HOLD, "3", "buy", 1.0, 1.0), (1.0, "0", "sell", 1.0, 1.0)];
    let (rs, _) = one_by_one(&ts);
    // sorted: sell, buy, buy
    close(&rs[0]["flip_rate"], 1.0 / 3.0);
}

#[test]
fn a_full_buffer_still_sorts_a_trade_before_all_waiting_ones_and_keeps_ties_in_arrival_order() {
    // a burst sharing one time: only the count fills the buffer
    let buys = (1..SEQUENCE_REORDER).map(|i| (1.0, (1000 + i).to_string(), "buy", 1.0, 1.0));
    // SEQUENCE_REORDER waiting buys, then a sell before all of them by id: sell first, one flip
    let waiting: Vec<Owned> = std::iter::once((1.0, "1000".to_string(), "buy", 1.0, 1.0)).chain(buys.clone()).collect();
    let early = vec![(1.0, "1".to_string(), "sell", 1.0, 1.0)];
    let (rs, _) = run(vec![waiting, early]);
    close(&rs[0]["flip_rate"], 1.0 / (SEQUENCE_REORDER + 1) as f64);
    // the earliest waiting trade is a sell; a buy with the same time and id arrives after it
    let waiting: Vec<Owned> = std::iter::once((1.0, "1000".to_string(), "sell", 1.0, 1.0)).chain(buys).collect();
    let tie = vec![(1.0, "1000".to_string(), "buy", 1.0, 1.0)];
    let (rs, _) = run(vec![waiting, tie]);
    close(&rs[0]["flip_rate"], 1.0 / (SEQUENCE_REORDER + 1) as f64);
}

#[test]
fn nan_prices_compare_as_duckdb_compares_them() {
    // NaN > 1, NaN = NaN, 2 < NaN (checked on DuckDB 1.5)
    let ts = [
        (1.0, "1", "buy", 1.0, 1.0),
        (2.0, "2", "buy", f64::NAN, 2.0),
        (3.0, "3", "buy", f64::NAN, 3.0),
        (4.0, "4", "buy", 2.0, 4.0),
    ];
    let (_, ud) = one_by_one(&ts);
    let n = |k: &str| ud[0][k].as_i64();
    assert_eq!((n("uptick_count"), n("downtick_count"), n("unchanged_count")), (Some(1), Some(1), Some(1)));
    close(&ud[0]["uptick_volume"], 2.0);
    close(&ud[0]["unchanged_volume"], 3.0);
    close(&ud[0]["downtick_volume"], 4.0);
}

/// A sequence aggregate reads its tuple by position: a tuple of another width, or a string where
/// it folds a number, is refused at plan time (it used to pass, then panic on the first row),
/// in a window, with `_if` and as a window function.
#[test]
fn a_sequence_aggregate_of_the_wrong_tuple_is_refused_at_plan_time() {
    let base = "CREATE STREAM IF NOT EXISTS s (t datetime64(6), id string, side string, p float64, q float64);
CREATE STREAM IF NOT EXISTS o (x float64);";
    let windowed = |agg: &str| {
        format!("SELECT {agg}[1] AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND")
    };
    for (view, why) in [
        (
            windowed("run_structure((t, id, side))"),
            "run_structure takes the tuple (time, id, side, price), got run_structure((t, id, side))",
        ),
        (windowed("run_structure((t, id, side, p, q))"), "run_structure takes the tuple (time, id, side, price)"),
        (windowed("run_structure(t)"), "run_structure takes the tuple (time, id, side, price), got run_structure(t)"),
        (
            windowed("run_structure_if((t, id, side), p > 0)"),
            "run_structure_if takes the tuple (time, id, side, price), got run_structure_if((t, id, side), p > 0)",
        ),
        (windowed("updownticks((t, id, p))"), "updownticks takes the tuple (time, id, price, size)"),
        (
            windowed("trade_returns((t, id, p, side, q))"),
            "trade_returns takes the tuple (time, tie1, tie2, price, side, size)",
        ),
        (windowed("distinct_stats((t, id))"), "distinct_stats takes the tuple (time, tie, x)"),
        (
            "SELECT run_structure((t, id, side)) OVER (ORDER BY t)[1] AS x FROM s".into(),
            "run_structure takes the tuple",
        ),
        (windowed("run_structure((t, id, side, side))"), "run_structure: its price side is a string"),
        (windowed("updownticks((id, id, p, q))"), "updownticks: its time id is a string"),
        (windowed("trade_returns((t, id, id, p, side, side))"), "trade_returns: its size side is a string"),
        (windowed("distinct_stats((t, id, 'x'))"), "distinct_stats: its x 'x' is a string"),
    ] {
        let sql = format!("{base}\nCREATE MATERIALIZED VIEW IF NOT EXISTS bad INTO o AS {view};");
        let err = Engine::new(&parse(&sql).unwrap()).err().unwrap_or_else(|| panic!("{view} was accepted"));
        assert!(err.contains(why), "{view}: {err}");
    }
    // ids, sides and ties may be strings
    for agg in [
        "run_structure((t, id, side, p))",
        "updownticks((t, id, p, q))",
        "trade_returns((t, id, side, p, side, q))",
        "distinct_stats((t, id, p))",
        "run_structure_if((t, id, side, p), p > 0)",
    ] {
        let sql = format!("{base}\nCREATE MATERIALIZED VIEW IF NOT EXISTS ok INTO o AS {};", windowed(agg));
        Engine::new(&parse(&sql).unwrap()).unwrap_or_else(|e| panic!("{agg}: {e}"));
    }
}

/// The folds themselves skip a row of the wrong width instead of indexing past its end.
#[test]
fn a_sequence_fold_skips_a_tuple_of_the_wrong_width() {
    use brrrrr_core::agg::Acc;
    let t = |items: &[Value]| Value::Array(items.into());
    let (time, id) = (Value::Time(1), Value::Str("1".into()));
    for (name, short, long) in [
        ("run_structure", t(&[time.clone(), id.clone(), Value::Str("buy".into())]), 5),
        ("updownticks", t(&[time.clone(), id.clone(), Value::F64(1.0)]), 5),
        ("trade_returns", t(&[time.clone(), id.clone(), id.clone(), Value::F64(1.0), Value::Str("buy".into())]), 7),
        ("distinct_stats", t(&[time.clone(), id.clone()]), 4),
    ] {
        let mut acc = Acc::new(name, &[], 1).unwrap();
        let empty = format!("{:?}", Acc::new(name, &[], 1).unwrap().result());
        acc.add(short);
        acc.add(t(&vec![Value::F64(1.0); long]));
        assert_eq!(format!("{:?}", acc.result()), empty, "{name}");
    }
}

#[test]
fn an_id_that_is_not_an_integer_sorts_as_0_in_run_structure() {
    // COALESCE(TRY_CAST(id AS BIGINT), 0): "x" goes first among equal times
    let (rs, _) = run(vec![owned(&[(1.0, "5", "sell", 2.0, 1.0), (1.0, "x", "buy", 1.0, 1.0)])]);
    close(&rs[0]["price_change_on_flip"], 1.0);
}

/// The metrics' SQL definition, run naively: sort the window, then scan it.
fn reference(ts: &[Owned]) -> ([f64; 6], Option<f64>, [f64; 6]) {
    let mut rs = ts.to_vec();
    let num = |id: &str| id.parse::<i64>().unwrap_or(0);
    rs.sort_by(|a, b| a.0.total_cmp(&b.0).then(num(&a.1).cmp(&num(&b.1))));
    let (mut runs, mut flips, mut changes): (Vec<(&str, usize)>, f64, Vec<f64>) = (vec![], 0.0, vec![]);
    for (i, t) in rs.iter().enumerate() {
        match runs.last_mut() {
            Some((side, n)) if *side == t.2 => *n += 1,
            _ => {
                if i > 0 {
                    flips += 1.0;
                    changes.push((t.3 / rs[i - 1].3 - 1.0).abs());
                }
                runs.push((t.2, 1));
            }
        }
    }
    let side = |s: &str| runs.iter().filter(|r| r.0 == s).map(|r| r.1 as f64).collect::<Vec<_>>();
    let max = |v: &[f64]| v.iter().cloned().fold(0.0, f64::max);
    let mean = |v: &[f64]| if v.is_empty() { 0.0 } else { v.iter().sum::<f64>() / v.len() as f64 };
    let (b, s) = (side("buy"), side("sell"));
    let pcof = (!changes.is_empty()).then(|| changes.iter().sum::<f64>() / changes.len() as f64);
    let mut ud = ts.to_vec();
    ud.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut u = [0.0; 6];
    for w in ud.windows(2) {
        let k = if w[1].3 > w[0].3 {
            0
        } else if w[1].3 < w[0].3 {
            1
        } else {
            2
        };
        u[k] += 1.0;
        u[k + 3] += w[1].4;
    }
    ([max(&b), max(&s), mean(&b), mean(&s), flips, rs.len() as f64], pcof, u)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]
    /// Any window whose trades each arrive before any trade 1 s newer than it (so within
    /// `sorted`'s hold), in any chunking, equals sorting the whole window first. Ids are unique and of
    /// varying length, so string and integer id order differ.
    #[test]
    fn equals_sorting_the_window_first(
        trades in prop::collection::vec((0u32..160, any::<bool>(), 1u32..6, 1u32..4, 0.0f64..1.0), 1..1500),
        cuts in prop::collection::vec(1usize..400, 1..20),
    ) {
        let ts: Vec<_> = trades.iter().enumerate().map(|(i, &(t, buy, p, s, _))| {
            (t as f64 / 4.0, ((i * 7919) % 100_003).to_string(), if buy { "buy" } else { "sell" }, p as f64, s as f64)
        }).collect();
        let (rs_want, pcof, ud_want) = reference(&ts);
        // arrival order: by time plus a random delay under 1 s
        let mut keyed: Vec<_> = ts.into_iter().zip(&trades).map(|(t, x)| (t.0 + x.4 * 0.999, t)).collect();
        keyed.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut rest: Vec<_> = keyed.into_iter().map(|(_, t)| t).collect();
        let mut chunks = vec![];
        for c in cuts.iter().cycle() {
            if rest.is_empty() { break; }
            let tail = rest.split_off((*c).min(rest.len()));
            chunks.push(std::mem::replace(&mut rest, tail));
        }
        let (rs, ud) = run(chunks);
        let r = &rs[0];
        for (k, want) in ["buy_run_max_len", "sell_run_max_len", "buy_run_mean_len", "sell_run_mean_len"].iter().zip(rs_want) {
            close(&r[*k], want);
        }
        close(&r["flip_rate"], rs_want[4] / rs_want[5]);
        match pcof {
            Some(p) => close(&r["price_change_on_flip"], p),
            None => prop_assert_eq!(&r["price_change_on_flip"], &J::Null),
        }
        let u = &ud[0];
        for (k, want) in ["uptick_count", "downtick_count", "unchanged_count", "uptick_volume", "downtick_volume", "unchanged_volume"].iter().zip(ud_want) {
            close(&u[*k], want);
        }
    }
}

#[test]
fn a_million_trades_in_one_open_window_keep_the_state_small() {
    // a 1-minute window of a busy symbol, or a day of one: the state must not grow with it
    let mut e = Engine::new(&parse(SQL).unwrap()).unwrap();
    let mut out = vec![];
    for c in 0..1000 {
        let rows = (0..1000)
            .map(|i| {
                let n = c * 1000 + i;
                let t = n as f64 * 50e-6; // 50 s of trades, all in [0, 60)
                row("A", (t, n.to_string(), if n % 3 == 0 { "buy" } else { "sell" }, 100.0 + (n % 7) as f64, 1.0))
            })
            .collect();
        e.insert("trades", rows, &mut out);
    }
    assert!(out.is_empty());
    // sorted trades (straight into `trades`): the trade of the newest time per aggregate, and
    // not a byte per trade more
    let bytes = postcard::to_allocvec(&e.snapshot()).unwrap().len();
    println!("state of 1M trades in one window: {bytes} bytes");
    assert!(bytes < 4096, "state of 1M trades in one window: {bytes} bytes");
}

/// Trades per second through each view alone, in-order trades of 50 symbols, 1,000 per
/// chunk (`cargo test --release -- --ignored --nocapture throughput`).
#[test]
#[ignore]
fn throughput() {
    let only = |view: &str| {
        let start = SQL.find(&format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {view} ")).unwrap();
        let end = SQL[start..].find("MILLISECOND;").unwrap() + start + "MILLISECOND;".len();
        format!("{}{}", &SQL[..SQL.find("CREATE MATERIALIZED VIEW").unwrap()], &SQL[start..end])
    };
    let baseline = SQL.replace(
        &SQL[SQL.find("CREATE MATERIALIZED VIEW IF NOT EXISTS ud").unwrap()..],
        "CREATE MATERIALIZED VIEW IF NOT EXISTS base INTO ud_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS uptick_count,
  latest(price) AS uptick_volume, avg(size) AS downtick_volume
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;",
    );
    let symbols: Vec<String> = (0..50).map(|i| format!("S{i}")).collect();
    let n = 2_000_000usize;
    for (name, sql) in
        [("count+latest+avg", only_view(&baseline, "base")), ("run_structure", only("rs")), ("updownticks", only("ud"))]
    {
        let mut e = Engine::new(&parse(&sql).unwrap()).unwrap();
        let chunks: Vec<Vec<Vec<Value>>> = (0..n / 1000)
            .map(|c| {
                (0..1000)
                    .map(|i| {
                        let k = c * 1000 + i;
                        let side = if k % 3 == 0 { "buy" } else { "sell" };
                        let t = (k as f64 * 100e-6, (1_000_000_000 + k).to_string(), side, 100.0 + (k % 7) as f64, 1.0);
                        row(&symbols[k % 50], t)
                    })
                    .collect()
            })
            .collect();
        let (mut out, t0) = (vec![], std::time::Instant::now());
        for c in chunks {
            e.insert("trades", c, &mut out);
        }
        let secs = t0.elapsed().as_secs_f64();
        println!("{name:>18}: {:>10.0} trades/s ({} messages)", n as f64 / secs, out.len());
    }
}

fn only_view(sql: &str, view: &str) -> String {
    let start = sql.find(&format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {view} ")).unwrap();
    format!("{}{}", &sql[..sql.find("CREATE MATERIALIZED VIEW").unwrap()], &sql[start..])
}

/// Resident memory per open window of one symbol, 5,000 symbols with 200 trades each (so
/// every buffer is full), against count+latest+avg
/// (`cargo test --release -- --ignored --nocapture memory_per_window`).
#[test]
#[ignore]
fn memory_per_window() {
    let rss = || {
        let s = std::fs::read_to_string("/proc/self/status").unwrap();
        let l = s.lines().find(|l| l.starts_with("VmRSS:")).unwrap();
        l.split_whitespace().nth(1).unwrap().parse::<f64>().unwrap() * 1024.0
    };
    let baseline = format!(
        "{}CREATE MATERIALIZED VIEW IF NOT EXISTS base INTO ud_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS uptick_count,
  latest(price) AS uptick_volume, avg(size) AS downtick_volume
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;",
        &SQL[..SQL.find("CREATE MATERIALIZED VIEW").unwrap()]
    );
    let symbols = 5_000;
    for (name, sql) in [
        ("count+latest+avg", baseline),
        (
            "run_structure",
            only_view(SQL, "rs").replace(&SQL[SQL.find("CREATE MATERIALIZED VIEW IF NOT EXISTS ud").unwrap()..], ""),
        ),
        ("updownticks", only_view(SQL, "ud")),
    ] {
        let mut e = Engine::new(&parse(&sql).unwrap()).unwrap();
        let before = rss();
        let mut out = vec![];
        for c in 0..200 {
            let rows = (0..symbols)
                .map(|s| {
                    let k = c * symbols + s;
                    let side = if k % 3 == 0 { "buy" } else { "sell" };
                    row(
                        &format!("S{s}"),
                        (c as f64 * 0.1, (1_000_000_000 + k).to_string(), side, 100.0 + (k % 7) as f64, 1.0),
                    )
                })
                .collect();
            e.insert("trades", rows, &mut out);
        }
        println!("{name:>18}: {:>7.0} bytes per open window", (rss() - before) / symbols as f64);
        drop(e);
    }
}

/// The example returns pipeline (fixtures/pipelines/returns.sql, its runs view) fed its trades
/// (fixtures/pipeline-inputs/returns.json.gz) in a realistic arrival order computes, for every
/// window of every symbol, what sorting the window first and scanning it does (`reference`): the
/// same windows, counts exactly, the rest within rounding.
#[test]
fn the_returns_pipeline_matches_sorting_each_window_first() {
    let fixture = crate::common::fixtures().into_iter().find(|f| f["pipeline"] == "returns").unwrap();
    // trades: time, id, exchange, symbol, price, local_timestamp, side, quantity, amount
    let trades: Vec<&J> = (fixture["chunks"].as_array().unwrap().iter())
        .flat_map(|c| c["rows"].as_array().unwrap())
        .filter(|t| t[3] != "Z")
        .collect();
    // arrival: trades sharing a time in any order, others delayed by up to 40 ms, within the
    // pipeline's 50 ms hold (an LCG, so the order is the same on every run)
    let mut seed = 42u64;
    let mut rand = move || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (seed >> 33) as f64 / (1u64 << 31) as f64
    };
    let mut arrival: Vec<(f64, &J)> = trades.iter().map(|t| (t[5].as_f64().unwrap() + rand() * 40_000.0, *t)).collect();
    arrival.sort_by(|a, b| a.0.total_cmp(&b.0));
    let cat = parse(&std::fs::read_to_string(crate::common::root("fixtures/pipelines/returns.sql")).unwrap()).unwrap();
    let mut e = Engine::new(&cat).unwrap();
    let columns = &cat.streams["trades"].columns;
    let row = |t: &J| -> Vec<Value> {
        t.as_array().unwrap().iter().zip(columns).map(|(v, c)| crate::common::value(v, &c.ty)).collect()
    };
    let (mut out, mut i) = (vec![], 0);
    while i < arrival.len() {
        let n = 1 + (rand() * 300.0) as usize;
        let chunk = arrival[i..(i + n).min(arrival.len())].iter().map(|(_, t)| row(t)).collect();
        e.insert("trades", chunk, &mut out);
        i += n;
    }
    e.close_until(i64::MAX / 4, &mut out);
    // each (symbol, minute)'s trades, as seconds into the minute
    let mut windows: std::collections::BTreeMap<(String, i64), Vec<Owned>> = Default::default();
    for t in &trades {
        let ts = t[5].as_i64().unwrap();
        let minute = ts.div_euclid(60_000_000) * 60_000_000;
        let side = if t[6] == "buy" { "buy" } else { "sell" };
        let trade = (
            (ts - minute) as f64 / 1e6,
            t[1].as_str().unwrap().to_string(),
            side,
            t[4].as_f64().unwrap(),
            t[7].as_f64().unwrap(),
        );
        windows.entry((t[3].as_str().unwrap().to_string(), minute)).or_default().push(trade);
    }
    let got: Vec<J> =
        out.iter().filter(|m| &*m.topic == "runs").map(|m| serde_json::from_str(&m.payload).unwrap()).collect();
    assert_eq!(got.len(), windows.len(), "windows");
    let mut compared = 0;
    for j in &got {
        let key = (j["symbol"].as_str().unwrap().to_string(), j["time"].as_i64().unwrap());
        let (rs, pcof, ud) = reference(&windows[&key]);
        for (k, want) in ["buy_run_max_len", "sell_run_max_len", "buy_run_mean_len", "sell_run_mean_len"].iter().zip(rs)
        {
            close(&j[*k], want);
        }
        close(&j["flip_rate"], rs[4] / rs[5]);
        match pcof {
            Some(p) => close(&j["price_change_on_flip"], p),
            None => assert_eq!(j["price_change_on_flip"], J::Null, "{key:?}"),
        }
        for (k, want) in
            ["uptick_count", "downtick_count", "unchanged_count", "uptick_volume", "downtick_volume"].iter().zip(ud)
        {
            close(&j[*k], want);
        }
        compared += 1;
    }
    assert!(compared > 30, "{compared} windows compared");
}
