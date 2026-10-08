//! A view's `ORDER BY` with `SETTINGS order_hold_ms` (ADR-0013): one sorted buffer per source,
//! so that windows downstream see their rows in capture order with no delay of their own. The
//! hold replaces the windows' straggler delay: a row within it is sorted in, a later one is
//! passed on at once and counted, and a result is out when it was before (the window's end
//! plus the hold, not plus both).
use brrrrr_core::engine::{Engine, State};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;

const SQL: &str = "
CREATE STREAM IF NOT EXISTS raw (local_event_time datetime64(6), id string, price float64);
CREATE STREAM IF NOT EXISTS sorted (local_event_time datetime64(6), id string, price float64);
CREATE EXTERNAL STREAM IF NOT EXISTS rows_out (local_event_time datetime64(6), id string, price float64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'rows', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE EXTERNAL STREAM IF NOT EXISTS bars (time int64, open float64, close float64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'bars', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS sort INTO sorted AS
SELECT local_event_time, id, price FROM raw ORDER BY local_event_time, id
SETTINGS order_hold_ms = 50;
CREATE MATERIALIZED VIEW IF NOT EXISTS copy INTO rows_out AS SELECT local_event_time, id, price FROM sorted;
CREATE MATERIALIZED VIEW IF NOT EXISTS bar INTO bars AS
SELECT to_unix_timestamp64_micro(window_start) AS time, earliest(price) AS open, latest(price) AS close, count() AS n
FROM tumble(sorted, local_event_time, 1s)
GROUP BY window_start
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;
";

fn row(secs: f64, id: &str, price: f64) -> Vec<Value> {
    vec![Value::Time((secs * 1e6).round() as i64), Value::Str(id.into()), Value::F64(price)]
}

/// Inserts each chunk; returns the ids passed on (`rows`) and the bars (open, close, n), in
/// output order.
fn feed(e: &mut Engine, chunks: Vec<Vec<Vec<Value>>>) -> (Vec<String>, Vec<(f64, f64, u64)>) {
    let mut out = vec![];
    for c in chunks {
        e.insert("raw", c, &mut out);
    }
    split(out)
}

fn split(out: Vec<brrrrr_core::engine::Emit>) -> (Vec<String>, Vec<(f64, f64, u64)>) {
    let (mut ids, mut bars) = (vec![], vec![]);
    for m in out {
        let j: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
        match &*m.topic {
            "rows" => ids.push(j["id"].as_str().unwrap().to_string()),
            _ => bars.push((j["open"].as_f64().unwrap(), j["close"].as_f64().unwrap(), j["n"].as_u64().unwrap())),
        }
    }
    (ids, bars)
}

fn engine() -> Engine {
    Engine::new(&parse(SQL).unwrap()).unwrap()
}

#[test]
fn rows_leave_in_key_order_once_the_newest_is_the_hold_past_them() {
    let mut e = engine();
    // across chunks, out of order within 50 ms; "b" and "a" share a time: by id
    let (ids, _) = feed(&mut e, vec![vec![row(1.02, "c", 3.0), row(1.0, "b", 2.0)], vec![row(1.0, "a", 1.0)]]);
    assert!(ids.is_empty(), "nothing is 50 ms behind the newest yet: {ids:?}");
    // exactly 50 ms behind still waits, as a row exactly the delay behind a window's end is
    // still in time for it
    let (ids, _) = feed(&mut e, vec![vec![row(1.05, "d", 4.0)]]);
    assert!(ids.is_empty(), "{ids:?}");
    // the rows at 1.0 s are more than 50 ms behind, 1.02 s is not
    let (ids, _) = feed(&mut e, vec![vec![row(1.0501, "d", 4.0)]]);
    assert_eq!(ids, ["a", "b"]);
    let (ids, _) = feed(&mut e, vec![vec![row(1.0701, "e", 5.0)]]);
    assert_eq!(ids, ["c"]);
}

#[test]
fn equal_keys_keep_their_arrival_order() {
    let mut e = engine();
    let (ids, _) = feed(&mut e, vec![vec![row(1.0, "x", 1.0)], vec![row(1.0, "x", 2.0)], vec![row(2.0, "y", 0.0)]]);
    assert_eq!(ids, ["x", "x"]);
    let mut out = vec![];
    e.close_until(i64::MAX / 4, &mut out);
    // the window saw 1 then 2: open 1, close 2
    assert_eq!(split(out).1[0], (1.0, 2.0, 2));
}

#[test]
fn a_window_downstream_closes_the_hold_after_its_end_and_sees_its_rows_in_order() {
    let mut e = engine();
    // the close arrives first
    let (_, bars) = feed(&mut e, vec![vec![row(0.9, "c", 3.0), row(0.2, "a", 1.0), row(0.5, "b", 2.0)]]);
    assert!(bars.is_empty());
    // 1.049 s releases every row of [0, 1), but none past it: the window stays open
    let (_, bars) = feed(&mut e, vec![vec![row(1.049, "d", 9.0)]]);
    assert!(bars.is_empty(), "{bars:?}");
    // 1.1 s releases d: the window closes, 1.1 s after its start, as it did with a 50 ms delay
    let (_, bars) = feed(&mut e, vec![vec![row(1.1, "e", 9.0)]]);
    assert_eq!(bars, [(1.0, 3.0, 3)], "open and close in time order, whatever the arrival order");
}

#[test]
fn a_row_later_than_the_hold_is_passed_on_at_once_and_counted() {
    crate::common::isolated("hold::a_row_later_than_the_hold_is_passed_on_at_once_and_counted", || {
        use std::sync::atomic::Ordering::Relaxed;
        // the counter is global: the test runs in a process of its own (`isolated`)
        let count = || brrrrr_core::engine::HOLD_LATE.load(Relaxed);
        let before = count();
        let mut e = engine();
        let (ids, _) = feed(&mut e, vec![vec![row(1.0, "a", 1.0), row(1.2, "c", 3.0)]]);
        assert_eq!(ids, ["a"]);
        // a tie with the released row's time is not late
        let (ids, _) = feed(&mut e, vec![vec![row(1.0, "b", 2.0)]]);
        assert_eq!((ids, count()), (vec!["b".to_string()], before));
        let (ids, _) = feed(&mut e, vec![vec![row(0.9, "z", 0.0)]]);
        assert_eq!((ids, count()), (vec!["z".to_string()], before + 1));
    });
}

#[test]
fn a_row_past_the_time_limit_does_not_release_the_others() {
    let mut e = engine();
    e.set_time_limit(Some(10_000_000));
    // 1e6 s is past the limit: passed on (windows drop it), the row at 1 s still waits
    let (ids, _) = feed(&mut e, vec![vec![row(1.0, "a", 1.0), row(1e6, "future", 0.0)]]);
    assert_eq!(ids, ["future"]);
}

#[test]
fn close_until_releases_what_a_row_at_that_time_would_and_closes_windows_the_hold_behind() {
    let mut e = engine();
    assert_eq!(feed(&mut e, vec![vec![row(0.96, "a", 1.0), row(0.99, "b", 2.0)]]), (vec![], vec![]));
    let at = |s: f64| (s * 1e6).round() as i64;
    let mut out = vec![];
    // an event at 1.03 s releases rows before 0.98 s; windows close up to 0.98 s: none
    e.close_until(at(1.03), &mut out);
    assert_eq!(split(std::mem::take(&mut out)), (vec!["a".to_string()], vec![]));
    // at 1.05 s: b is released, and [0, 1) closes, as a row at 1.05 s would close it
    e.close_until(at(1.05), &mut out);
    assert_eq!(split(out), (vec!["b".to_string()], vec![(1.0, 2.0, 2)]));
}

#[test]
fn a_restart_from_a_checkpoint_mid_hold_changes_nothing() {
    let chunks = || {
        vec![
            vec![row(0.3, "c", 3.0), row(0.1, "a", 1.0)],
            vec![row(0.2, "b", 2.0), row(0.96, "d", 4.0)],
            vec![row(0.9, "e", 5.0), row(1.2, "f", 6.0)],
            vec![row(2.5, "g", 7.0)],
        ]
    };
    let (want_ids, want_bars) = feed(&mut engine(), chunks());
    let mut e = engine();
    let (mut ids, mut bars) = feed(&mut e, chunks()[..2].to_vec());
    let state: State = postcard::from_bytes(&postcard::to_allocvec(&e.snapshot()).unwrap()).unwrap();
    let mut back = engine();
    back.restore(state).unwrap();
    let (i, b) = feed(&mut back, chunks()[2..].to_vec());
    ids.extend(i);
    bars.extend(b);
    assert_eq!((ids, bars), (want_ids, want_bars));
}

#[test]
fn the_hold_is_part_of_the_plan_and_needs_the_views_own_order_by() {
    let other = Engine::new(&parse(&SQL.replace("order_hold_ms = 50", "order_hold_ms = 60")).unwrap()).unwrap();
    assert_ne!(engine().fingerprint(), other.fingerprint());
    for (bad, want) in
        [("order_hold_ms = -1", "order_hold_ms = -1"), ("order_hold_ms = 'soon'", "order_hold_ms = soon")]
    {
        let err = Engine::new(&parse(&SQL.replace("order_hold_ms = 50", bad)).unwrap()).err().unwrap();
        assert!(err.contains(want), "{err}");
    }
    let unsorted = SQL.replace(" ORDER BY local_event_time, id", "");
    let err = Engine::new(&parse(&unsorted).unwrap()).err().unwrap();
    assert!(err.contains("has none"), "{err}");
}

#[test]
fn a_first_key_that_is_an_expression_sorts_as_its_column_does() {
    // a plain first column is read directly; any other expression is evaluated
    let chunks = || {
        vec![
            vec![row(0.3, "c", 3.0), row(0.1, "a", 1.0)],
            vec![row(0.2, "b", 2.0), row(0.96, "d", 4.0)],
            vec![row(0.9, "e", 5.0), row(1.2, "f", 6.0), row(2.5, "g", 7.0)],
        ]
    };
    let expr = SQL.replace("ORDER BY local_event_time, id", "ORDER BY (local_event_time), id");
    let mut e = Engine::new(&parse(&expr).unwrap()).unwrap();
    let (ids, bars) = feed(&mut e, chunks());
    assert_eq!(ids, ["a", "b", "c", "e", "d", "f"]);
    assert_eq!((ids, bars), feed(&mut engine(), chunks()));
}

/// A change to a snapshot's hold (its JSON).
type Corruption = fn(&mut serde_json::Value);

/// A restored hold must be one the plan could have made: its rows as wide as the view's, each
/// under its own time, none past the newest time taken, in key order. Anything else is refused
/// (it would release rows out of order, or read columns a row does not have), and the engine
/// is left as it was.
#[test]
fn a_corrupted_hold_is_refused_on_restore() {
    let mut e = engine();
    feed(&mut e, vec![vec![row(0.96, "a", 1.0), row(0.99, "b", 2.0)]]);
    let good = serde_json::to_value(e.snapshot()).unwrap();
    let held = |j: &mut serde_json::Value| -> serde_json::Value {
        // view 0 (sort): its operators; the hold is the one with rows
        let ops = j[0].as_array_mut().unwrap();
        ops.iter_mut().find_map(|o| o.get_mut("Hold")).expect("a hold").clone()
    };
    assert_eq!(held(&mut good.clone())["held"].as_array().unwrap().len(), 2, "both rows held");
    let corrupt = |f: Corruption| {
        let mut j = good.clone();
        let ops = j[0].as_array_mut().unwrap();
        f(ops.iter_mut().find_map(|o| o.get_mut("Hold")).unwrap());
        serde_json::from_value::<State>(j).unwrap()
    };
    let cases: [(&str, Corruption); 4] = [
        ("out of key order", |h| h["held"].as_array_mut().unwrap().swap(0, 1)),
        ("filed under another time", |h| h["held"][1][0] = serde_json::json!(960_000)),
        ("wider than the view's rows", |h| h["held"][0][1].as_array_mut().unwrap().push(serde_json::json!("Null"))),
        ("past the newest time taken", |h| h["max"] = serde_json::json!(980_000)),
    ];
    for (what, f) in cases {
        let mut back = engine();
        let err = back.restore(corrupt(f)).err().unwrap_or_else(|| panic!("{what}: restored"));
        assert!(err.contains("held rows"), "{what}: {err}");
    }
    // the uncorrupted snapshot restores
    engine().restore(serde_json::from_value(good).unwrap()).unwrap();
}
