//! Restoring a checkpoint checks what it restores. A snapshot of the same operators whose groups
//! or versions the plan could not have made (corrupted bytes, a snapshot of other SQL with the
//! same shape) is refused, instead of restoring into output that is silently wrong or that
//! panics later. A refused snapshot leaves the engine as it was. Each case edits a real
//! snapshot through JSON into exactly one inconsistency.
use brrrrr_core::engine::{Asof, Emit, Engine, State};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;
use serde_json::{json, Value as Json};

const WINDOW: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
CREATE STREAM IF NOT EXISTS out (symbol string, time int64, n uint64, p float64, d float32, v float64);
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS n, quantile(0.9)(price) AS p,
  median_tdigest(price) AS d, var_samp(price) AS v
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

const ASOF: &str = "
CREATE STREAM IF NOT EXISTS l (event_time datetime64(6), symbol string, x float64);
CREATE STREAM IF NOT EXISTS r (event_time datetime64(6), symbol string, y float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (event_time datetime64(6), symbol string, x float64, y float64)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS j INTO out AS
SELECT a.event_time AS event_time, a.symbol AS symbol, a.x AS x, b.y AS y
FROM l AS a
ASOF LEFT JOIN r AS b
ON a.symbol = b.symbol AND a.event_time >= b.event_time;
";

fn engine(sql: &str) -> Engine {
    Engine::new(&parse(sql).unwrap()).unwrap()
}

fn trade(sec: i64, symbol: &str, price: f64) -> Vec<Value> {
    vec![Value::Time(sec * 1_000_000), Value::Str(symbol.into()), Value::F64(price)]
}

fn insert(e: &mut Engine, stream: &str, rows: Vec<Vec<Value>>) -> Vec<Emit> {
    let mut out = vec![];
    e.insert(stream, rows, &mut out);
    out
}

/// A window engine with two open groups (A: two prices, B: one).
fn windowed() -> Engine {
    let mut e = engine(WINDOW);
    insert(&mut e, "trades", vec![trade(1, "A", 1.5), trade(2, "A", 2.5), trade(3, "B", 7.0)]);
    e
}

/// A join engine with right versions for A (two) and B (one).
fn joined() -> Engine {
    let mut e = engine(ASOF);
    insert(&mut e, "r", vec![trade(1, "A", 10.0), trade(2, "A", 20.0), trade(1, "B", 30.0)]);
    e
}

fn to_json(e: &Engine) -> Json {
    serde_json::to_value(e.snapshot()).unwrap()
}

/// The open groups of the first view's window: `[[[start, key text], [[keys], [accs]]], ..]`.
fn groups(j: &mut Json) -> &mut Vec<Json> {
    j[0][0]["Window"]["open"].as_array_mut().unwrap()
}

/// The accumulators of group `g`: count, quantile, tdigest, var_samp.
fn accs(j: &mut Json, g: usize) -> &mut Vec<Json> {
    groups(j)[g][1][1].as_array_mut().unwrap()
}

/// The right versions of the join: `[[key text, [[time, row], ..]], ..]`.
fn versions(j: &mut Json) -> &mut Vec<Json> {
    j[0][0]["Join"]["versions"][0].as_array_mut().unwrap()
}

/// Restoring `edited` (made from `make`'s snapshot) is refused with `why`, and the engine still
/// produces what an engine that never saw the snapshot produces.
fn refused(make: fn() -> Engine, edit: impl FnOnce(&mut Json), why: &str, then: (&str, Vec<Vec<Value>>)) {
    let mut j = to_json(&make());
    edit(&mut j);
    let state: State = serde_json::from_value(j).expect("the edit keeps the snapshot decodable");
    let (mut e, mut untouched) = (make(), make());
    let err = e.restore(state).unwrap_err();
    assert!(err.contains(why), "{why:?} not in {err:?}");
    assert_eq!(insert(&mut e, then.0, then.1.clone()), insert(&mut untouched, then.0, then.1));
}

/// A row that closes the open windows.
fn closing() -> (&'static str, Vec<Vec<Value>>) {
    ("trades", vec![trade(600, "C", 1.0)])
}

/// A left row for A and one for B.
fn lookups() -> (&'static str, Vec<Vec<Value>>) {
    ("l", vec![trade(5, "A", 1.0), trade(5, "B", 2.0)])
}

#[test]
fn an_unedited_snapshot_restores_through_json() {
    let mut e = engine(WINDOW);
    e.restore(serde_json::from_value(to_json(&windowed())).unwrap()).unwrap();
    assert_eq!(insert(&mut e, "trades", closing().1), insert(&mut windowed(), "trades", closing().1));
    let mut e = engine(ASOF);
    e.restore(serde_json::from_value(to_json(&joined())).unwrap()).unwrap();
    assert_eq!(insert(&mut e, "l", lookups().1), insert(&mut joined(), "l", lookups().1));
}

#[test]
fn a_group_missing_an_aggregate_is_refused() {
    refused(windowed, |j| drop(accs(j, 0).pop()), "a group with 1 keys and 3 aggregates", closing());
}

#[test]
fn a_group_with_another_number_of_keys_is_refused() {
    let edit = |j: &mut Json| groups(j)[0][1][0].as_array_mut().unwrap().push(json!({"Str": "x"}));
    refused(windowed, edit, "a group with 2 keys", closing());
}

#[test]
fn aggregates_in_another_order_are_refused() {
    refused(windowed, |j| accs(j, 0).swap(1, 2), "an accumulator", closing());
}

#[test]
fn a_count_of_values_where_the_plan_counts_rows_is_refused() {
    refused(windowed, |j| accs(j, 0)[0]["Count"]["rows"] = json!(false), "an accumulator", closing());
}

#[test]
fn another_moment_is_refused() {
    refused(windowed, |j| accs(j, 0)[3]["Moments"]["kind"] = json!("SkewSamp"), "an accumulator", closing());
}

#[test]
fn a_quantile_of_another_level_is_refused() {
    // 7.0 would index past the samples when the window closes
    for level in [0.25, 7.0] {
        refused(windowed, |j| accs(j, 0)[1]["Quantile"]["level"] = json!(level), "an accumulator", closing());
        refused(windowed, |j| accs(j, 0)[2]["TDigest"]["level"] = json!(level), "an accumulator", closing());
    }
}

/// The reservoir of group 0's quantile.
fn sampler(j: &mut Json) -> &mut Json {
    &mut accs(j, 0)[1]["Quantile"]["sampler"]
}

#[test]
fn an_inconsistent_reservoir_is_refused() {
    // fewer seen than kept: a full reservoir would then take a remainder by zero
    refused(windowed, |j| sampler(j)["total"] = json!(1), "an accumulator", closing());
    // more samples than a reservoir keeps
    let many = |j: &mut Json| {
        sampler(j)["samples"] = json!(vec![1.0; 8193]);
        sampler(j)["total"] = json!(9000);
    };
    refused(windowed, many, "an accumulator", closing());
    // said to be sorted, but not
    let unsorted = |j: &mut Json| {
        sampler(j)["samples"] = json!([2.5, 1.5]);
        sampler(j)["sorted"] = json!(true);
    };
    refused(windowed, unsorted, "an accumulator", closing());
}

#[test]
fn a_digest_with_more_unmerged_values_than_it_ever_keeps_is_refused() {
    let edit = |j: &mut Json| accs(j, 0)[2]["TDigest"]["digest"]["unmerged"] = json!(vec![1.0; 2_049]);
    refused(windowed, edit, "an accumulator", closing());
}

const SEQUENCE: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, id string, side string, price float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (symbol string, time int64, flips float64)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, run_structure((local_event_time, id, side, price))[5] AS flips
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

fn sequence_trade(sec: i64, id: &str, side: &str) -> Vec<Value> {
    vec![
        Value::Time(sec * 1_000_000),
        Value::Str("A".into()),
        Value::Str(id.into()),
        Value::Str(side.into()),
        Value::F64(1.0),
    ]
}

/// A run_structure window with two trades waiting to be sorted.
fn sequenced() -> Engine {
    let mut e = engine(SEQUENCE);
    insert(&mut e, "trades", vec![sequence_trade(1, "1", "buy"), sequence_trade(1, "2", "sell")]);
    e
}

/// The trades waiting in group 0's run_structure.
fn pending(j: &mut Json) -> &mut Vec<Json> {
    accs(j, 0)[0]["RunStructure"][0].as_array_mut().unwrap()
}

#[test]
fn a_trade_sequence_restores_through_json() {
    let close = || vec![sequence_trade(600, "3", "buy")];
    let mut e = engine(SEQUENCE);
    e.restore(serde_json::from_value(to_json(&sequenced())).unwrap()).unwrap();
    let got = insert(&mut e, "trades", close());
    assert!(!got.is_empty());
    assert_eq!(got, insert(&mut sequenced(), "trades", close()));
}

#[test]
fn an_inconsistent_trade_sequence_is_refused() {
    let close = || ("trades", vec![sequence_trade(600, "3", "buy")]);
    // out of key order: later trades would be sorted in at the wrong place
    refused(sequenced, |j| pending(j).reverse(), "an accumulator", close());
    // more waiting than the buffer holds: it would never fold again, and grow with every trade
    let many = |j: &mut Json| {
        let t = pending(j)[0].clone();
        pending(j).resize(129, t);
    };
    refused(sequenced, many, "an accumulator", close());
}

#[test]
fn a_group_filed_under_another_key_is_refused() {
    // its rows would open a second group for A, and A would come out twice
    refused(windowed, |j| groups(j)[0][0][1] = json!("1:Z"), "filed under another key or twice", closing());
}

#[test]
fn a_group_listed_twice_is_refused() {
    let edit = |j: &mut Json| {
        let g = groups(j)[0].clone();
        groups(j).push(g);
    };
    refused(windowed, edit, "filed under another key or twice", closing());
}

#[test]
fn a_join_version_narrower_than_its_rows_is_refused() {
    let edit = |j: &mut Json| drop(versions(j)[0][1][0][1].as_array_mut().unwrap().pop());
    refused(joined, edit, "not a row of its key", lookups());
    // too short to hold its key column: refused before the key is read from it
    refused(joined, |j| versions(j)[0][1][0][1] = json!([]), "not a row of its key", lookups());
}

#[test]
fn a_join_version_under_another_key_is_refused() {
    refused(joined, |j| versions(j)[0][0] = json!("1:Z"), "not a row of its key", lookups());
}

#[test]
fn join_versions_listed_twice_are_refused() {
    let edit = |j: &mut Json| {
        let v = versions(j)[0].clone();
        versions(j).push(v);
    };
    refused(joined, edit, "not the latest few", lookups());
}

#[test]
fn join_versions_oldest_first_are_refused() {
    refused(joined, |j| versions(j)[0][1].as_array_mut().unwrap().reverse(), "not the latest few", lookups());
}

#[test]
fn more_join_versions_than_kept_are_refused() {
    let edit = |j: &mut Json| {
        let v = versions(j)[0][1].as_array_mut().unwrap();
        v.extend([v[1].clone(), v[1].clone()]);
    };
    refused(joined, edit, "not the latest few", lookups());
}

#[test]
fn a_key_without_versions_is_refused() {
    refused(joined, |j| versions(j)[0][1] = json!([]), "not the latest few", lookups());
}

#[test]
fn versions_for_another_number_of_right_sides_are_refused() {
    let edit = |j: &mut Json| j[0][0]["Join"]["versions"].as_array_mut().unwrap().push(json!([]));
    refused(joined, edit, "does not match the plan", lookups());
}

// ---- times against the newest times that bound them ------------------------------------------

/// A window that a row opened at its very start: the newest time is that window's start.
fn at_bounds() -> Engine {
    let mut e = windowed();
    insert(&mut e, "trades", vec![trade(60, "A", 1.0)]);
    e
}

/// An exact join holding left rows for A at 5 s and 35 s (`ASOF_HOLD_US` apart: the first is
/// exactly the hold behind the newest), with A's right versions at 1 s and 2 s.
fn held() -> Engine {
    let mut e = engine(ASOF);
    e.set_asof(Asof::Exact);
    insert(&mut e, "r", vec![trade(1, "A", 10.0), trade(2, "A", 20.0)]);
    insert(&mut e, "l", vec![trade(5, "A", 1.0), trade(35, "A", 2.0)]);
    e
}

fn join_held(j: &mut Json) -> &mut Json {
    &mut j[0][0]["JoinHeld"]
}

/// A right row that releases both held rows.
fn releasing() -> (&'static str, Vec<Vec<Value>>) {
    ("r", vec![trade(40, "A", 30.0)])
}

/// The bounds are inclusive where the operators' are: a window starting at the newest time, a
/// row held at the newest left time and one exactly the hold behind it, a version at its right
/// side's newest time.
#[test]
fn times_at_their_bounds_restore() {
    let mut e = engine(WINDOW);
    e.restore(at_bounds().snapshot()).unwrap();
    assert_eq!(to_json(&e), to_json(&at_bounds()));
    assert_eq!(groups(&mut to_json(&e)).last().unwrap()[0][0], json!(60_000_000));
    assert_eq!(join_held(&mut to_json(&held()))["held"].as_array().unwrap().len(), 2);
    let mut e = engine(ASOF);
    e.set_asof(Asof::Exact);
    e.restore(held().snapshot()).unwrap();
    let got = insert(&mut e, releasing().0, releasing().1);
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got, insert(&mut held(), releasing().0, releasing().1));
}

#[test]
fn a_watermark_that_is_not_the_newest_times_is_refused() {
    // rows before 60 s would be dropped as late
    refused(windowed, |j| j[0][0]["Window"]["watermark"] = json!(60_000_000), "the watermark", closing());
    refused(windowed, |j| j[0][0]["Window"]["max_ts"] = json!(120_000_000), "the watermark", closing());
}

#[test]
fn an_open_window_the_times_do_not_leave_open_is_refused() {
    let why = "do not leave open";
    // not on a window boundary: its window_start would be 1 s
    refused(windowed, |j| groups(j)[0][0][0] = json!(1_000_000), why, closing());
    // after the newest time: no row opened it
    refused(windowed, |j| groups(j)[0][0][0] = json!(60_000_000), why, closing());
    // ending at the watermark: emitted already
    refused(windowed, |j| groups(j)[0][0][0] = json!(-60_000_000), why, closing());
}

#[test]
fn held_rows_outside_the_hold_of_the_newest_left_time_are_refused() {
    let why = "not within the hold of the newest left time";
    // before the newest held row: it was taken after that time
    refused(held, |j| join_held(j)["left_max"] = json!(34_000_000), why, releasing());
    // more than the hold past the oldest: it would have been released
    refused(held, |j| join_held(j)["left_max"] = json!(35_000_001), why, releasing());
}

#[test]
fn a_version_after_its_right_sides_newest_time_is_refused() {
    let why = "after its right side's newest time";
    refused(held, |j| join_held(j)["right_max"] = json!([1_000_000]), why, releasing());
}

/// A window on a join side: its times are checked as a top-level window's, in both join modes.
const WINDOWED_SIDE: &str = "
CREATE STREAM IF NOT EXISTS l (event_time datetime64(6), symbol string, x float64);
CREATE STREAM IF NOT EXISTS r (event_time datetime64(6), symbol string, y float64);
CREATE STREAM IF NOT EXISTS out (event_time datetime64(6), symbol string, n uint64, y float64);
CREATE MATERIALIZED VIEW IF NOT EXISTS j INTO out AS
SELECT a.t AS event_time, a.symbol AS symbol, a.n AS n, b.y AS y
FROM (SELECT window_start AS t, symbol, count() AS n FROM tumble(l, event_time, 1m) GROUP BY window_start, symbol) AS a
ASOF LEFT JOIN r AS b ON a.symbol = b.symbol AND a.t >= b.event_time
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND;
";

fn side_windowed(asof: Asof) -> Engine {
    let mut e = engine(WINDOWED_SIDE);
    e.set_asof(asof);
    insert(&mut e, "l", vec![trade(1, "A", 1.0)]);
    e
}

#[test]
fn a_join_sides_window_is_checked_too() {
    let edit = |j: &mut Json| {
        let join = j[0][0].as_object_mut().unwrap().values_mut().next().unwrap();
        let ops = join["sides"][0].as_array_mut().unwrap();
        ops.iter_mut().find_map(|o| o.get_mut("Window")).unwrap()["watermark"] = json!(60_000_000);
    };
    let then = || ("l", vec![trade(600, "A", 1.0)]);
    refused(|| side_windowed(Asof::Arrival), edit, "the watermark", then());
    refused(|| side_windowed(Asof::Exact), edit, "the watermark", then());
}

#[test]
fn a_refused_snapshot_leaves_the_engine_as_it_was() {
    // a later view's inconsistency must not leave the earlier views restored
    const TWO: &str = "
    CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
    CREATE STREAM IF NOT EXISTS out (symbol string, n uint64);
    CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO out AS SELECT symbol, count() AS n
    FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol
    EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
    CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO out AS SELECT symbol, count() AS n
    FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol
    EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
    ";
    let mut fed = engine(TWO);
    insert(&mut fed, "trades", vec![trade(1, "A", 1.0)]);
    let mut j = to_json(&fed);
    j[1][0]["Window"]["open"][0][1][1].as_array_mut().unwrap().clear();
    let mut e = engine(TWO);
    assert!(e.restore(serde_json::from_value(j).unwrap()).unwrap_err().starts_with("view 1: "));
    assert!(insert(&mut e, "trades", vec![trade(600, "C", 1.0)]).is_empty(), "view 0 was restored");
}

/// Join versions live in a hash map; the snapshot lists them in key order, so two engines with
/// the same state write the same checkpoint bytes.
#[test]
fn equal_join_states_snapshot_to_equal_bytes() {
    let rows: Vec<_> = (0..64).map(|i| trade(1, &format!("S{i}"), i as f64)).collect();
    let (mut a, mut b) = (engine(ASOF), engine(ASOF));
    insert(&mut a, "r", rows.clone());
    insert(&mut b, "r", rows.into_iter().rev().collect());
    let bytes = |e: &Engine| postcard::to_allocvec(&e.snapshot()).unwrap();
    assert_eq!(bytes(&a), bytes(&b));
}

/// Two quantiles of one argument: the second reads the first's sampler (`QuantileOf`).
const SHARED: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
CREATE STREAM IF NOT EXISTS out (symbol string, p float64, q float64);
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO out AS
SELECT symbol, quantile(0.9)(price) AS p, quantile(0.5)(price) AS q
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

fn shared() -> Engine {
    let mut e = engine(SHARED);
    insert(&mut e, "trades", vec![trade(1, "A", 1.5), trade(2, "A", 2.5), trade(3, "B", 7.0)]);
    e
}

#[test]
fn a_quantile_reading_another_level_or_accumulator_is_refused() {
    assert_eq!(accs(&mut to_json(&shared()), 0)[1], json!({"QuantileOf": {"level": 0.5, "of": 0}}));
    refused(shared, |j| accs(j, 0)[1]["QuantileOf"]["level"] = json!(0.25), "an accumulator", closing());
    // reading itself would never find a sampler
    refused(shared, |j| accs(j, 0)[1]["QuantileOf"]["of"] = json!(1), "an accumulator", closing());
}

/// Format version 2 kept a full sampler in the second quantile: one of the plan's level restores
/// (tests/engine.rs), one of another level is another aggregate.
#[test]
fn a_full_sampler_of_another_level_where_the_plan_reads_one_is_refused() {
    let edit = |j: &mut Json| {
        let mut full = accs(j, 0)[0].clone();
        full["Quantile"]["level"] = json!(0.25);
        accs(j, 0)[1] = full;
    };
    refused(shared, edit, "an accumulator", closing());
}
