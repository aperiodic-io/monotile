//! ASOF LEFT JOIN as a batch engine computes it (each left row takes the right row with the
//! greatest time at or before its own), and the `_if` aggregates over what it joins.
//!
//! Proton emits a left row at once with whatever right rows have arrived and keeps 3 versions
//! per key, so a row whose right side arrives later or behind a burst of newer rows joins a
//! stale version or 0 (on a venue whose quotes arrive seconds after its trades, a slippage
//! metric drifts by a fifth). `Asof::Exact` holds each left row until every right side has
//! passed its time; `Asof::Arrival` is Proton's behaviour, with `keep_versions`.
use brrrrr_core::engine::{Asof, Engine, ASOF_HOLD_US, ASOF_LATENESS_US};

const _: () = assert!(ASOF_LATENESS_US >= 1_000_000); // the proptest's delays are under 1 s
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;

const SQL: &str = "
CREATE STREAM IF NOT EXISTS trades (event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS quotes (event_time datetime64(6), symbol string, ask float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (event_time datetime64(6), symbol string, price float64, ask nullable(float64))
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS joined INTO out AS
SELECT t.event_time AS event_time, t.symbol AS symbol, t.price AS price, null_if(q.ask, 0) AS ask
FROM (SELECT event_time, symbol, price FROM trades ORDER BY symbol, event_time) AS t
ASOF LEFT JOIN (SELECT event_time, symbol, ask FROM quotes ORDER BY symbol, event_time) AS q
ON t.symbol = q.symbol AND t.event_time >= q.event_time
SETTINGS keep_versions = KEEP;
";

fn t(s: f64) -> Value {
    Value::Time((s * 1e6).round() as i64)
}

fn trade(s: f64, price: f64) -> Vec<Value> {
    vec![t(s), Value::Str("A".into()), Value::F64(price)]
}

fn quote(s: f64, ask: f64) -> Vec<Value> {
    vec![t(s), Value::Str("A".into()), Value::F64(ask)]
}

fn engine(asof: Asof, keep: &str) -> Engine {
    let mut e = Engine::new(&parse(&SQL.replace("KEEP", keep)).unwrap()).unwrap();
    e.set_asof(asof);
    e
}

/// Inserts (stream, row) one at a time; returns each emitted row's (price, ask). The tests
/// give every trade a distinct price, so the price names the trade.
fn feed(e: &mut Engine, rows: Vec<(&str, Vec<Value>)>) -> Vec<(f64, Option<f64>)> {
    let mut out = vec![];
    for (stream, row) in rows {
        e.insert(stream, vec![row], &mut out);
    }
    out.iter()
        .map(|m| {
            let j: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
            (j["price"].as_f64().unwrap(), j["ask"].as_f64())
        })
        .collect()
}

#[test]
fn a_trade_joins_the_quote_at_or_before_its_time_even_when_that_quote_arrives_after_it() {
    // a venue whose quotes arrive seconds after the trades they precede
    let rows = || {
        vec![
            ("quotes", quote(0.0, 10.0)),
            ("trades", trade(5.0, 1.0)),
            ("quotes", quote(4.0, 14.0)),
            ("quotes", quote(6.0, 16.0)),
        ]
    };
    // the batch as-of join: trade at 5 s takes the quote at 4 s
    assert_eq!(feed(&mut engine(Asof::Exact, "3"), rows()), [(1.0, Some(14.0))]);
    // arrival: the trade is emitted at once, with the quote at 0 s
    assert_eq!(feed(&mut engine(Asof::Arrival, "3"), rows()), [(1.0, Some(10.0))]);
}

/// `Engine::set_time_limit` in the tests below: 1000 s.
const LIMIT_S: f64 = 1000.0;
const YEAR_3000: f64 = 32_503_680_000.0;

fn limited(asof: Asof) -> Engine {
    let mut e = engine(asof, "3");
    e.set_time_limit(Some((LIMIT_S * 1e6) as i64));
    e
}

/// A far-future row used to reach a join unfiltered. A trade moved
/// the newest left time there, a quote the newest right time, and from then on every trade was
/// released at once with whatever quote had arrived: exact mode became arrival mode, for good
/// (both times are checkpointed). With a time limit such rows are dropped and counted, as
/// windows drop them.
#[test]
fn a_row_past_the_time_limit_does_not_degrade_an_exact_join() {
    let rows = |poison: (&'static str, Vec<Value>)| {
        vec![
            ("quotes", quote(0.0, 10.0)),
            poison,
            ("trades", trade(5.0, 1.0)),
            ("quotes", quote(4.0, 14.0)),
            ("quotes", quote(6.0, 16.0)),
        ]
    };
    for poison in [("trades", trade(YEAR_3000, 2.0)), ("quotes", quote(YEAR_3000, 99.0))] {
        let side = poison.0;
        assert_eq!(feed(&mut engine(Asof::Exact, "3"), rows(poison.clone())), [(1.0, Some(10.0))], "no limit, {side}");
        let mut e = limited(Asof::Exact);
        assert_eq!(feed(&mut e, rows(poison)), [(1.0, Some(14.0))], "{side}");
        assert_eq!((e.future(), e.late()), (1, 0), "{side}");
    }
}

/// In arrival mode a far-future quote held one of the `keep_versions` slots for good, and a
/// far-future trade was emitted; both are dropped and counted. A time at the limit is taken.
#[test]
fn a_row_past_the_time_limit_does_not_reach_an_arrival_join() {
    let rows = || {
        vec![
            ("quotes", quote(YEAR_3000, 99.0)),
            ("quotes", quote(1.0, 11.0)),
            ("quotes", quote(2.0, 12.0)),
            ("quotes", quote(3.0, 13.0)),
            ("trades", trade(1.5, 1.0)),
            ("trades", trade(YEAR_3000, 2.0)),
            ("trades", trade(LIMIT_S, 3.0)),
        ]
    };
    let mut arrival = engine(Asof::Arrival, "3");
    assert_eq!(feed(&mut arrival, rows()), [(1.0, None), (2.0, Some(99.0)), (3.0, Some(13.0))]);
    let mut e = limited(Asof::Arrival);
    assert_eq!(feed(&mut e, rows()), [(1.0, Some(11.0)), (3.0, Some(13.0))]);
    assert_eq!(e.future(), 2);
}

/// A row without a time (NULL, an absent proto field) has no
/// place in an as-of join. A quote without one compared as neither before nor after any trade,
/// so it matched every trade, and an exact join pruned the real quotes for it; a trade without
/// one was held by an exact join until a timed row came. Both are dropped and counted, in
/// either mode (ClickHouse refuses a Nullable ASOF column outright).
#[test]
fn a_row_without_a_time_is_dropped_by_either_join_mode() {
    let untimed = |mut row: Vec<Value>| {
        row[0] = Value::Null;
        row
    };
    let rows = vec![
        ("quotes", quote(10.0, 10.0)),
        ("quotes", untimed(quote(0.0, 77.0))),
        ("trades", untimed(trade(0.0, 9.0))),
        ("quotes", quote(20.0, 20.0)),
        ("trades", trade(1.0, 1.0)),
        ("trades", trade(15.0, 2.0)),
        ("trades", trade(25.0, 3.0)),
        ("quotes", quote(40.0, 40.0)),
    ];
    for asof in [Asof::Arrival, Asof::Exact] {
        let mut e = engine(asof, "3");
        assert_eq!(feed(&mut e, rows.clone()), [(1.0, None), (2.0, Some(10.0)), (3.0, Some(20.0))], "{asof:?}");
        assert_eq!((e.null_time(), e.future(), e.late()), (2, 0, 0), "{asof:?}");
    }
}

#[test]
fn a_trade_behind_a_burst_of_newer_quotes_still_finds_its_quote() {
    // 20 newer quotes land before the trade; keeping 3 versions joins 0
    let mut rows = vec![("quotes", quote(1.0, 11.0))];
    rows.extend((0..20).map(|i| ("quotes", quote(10.0 + i as f64, 100.0 + i as f64))));
    rows.push(("trades", trade(2.0, 1.0)));
    rows.push(("quotes", quote(40.0, 1.0)));
    assert_eq!(
        feed(&mut engine(Asof::Arrival, "3"), rows.clone()),
        [(1.0, None)],
        "arrival's miss, NULL through null_if"
    );
    assert_eq!(feed(&mut engine(Asof::Arrival, "5000"), rows.clone()), [(1.0, Some(11.0))], "keep_versions = 5000");
    assert_eq!(feed(&mut engine(Asof::Exact, "3"), rows), [(1.0, Some(11.0))], "exact, whatever keep_versions says");
}

#[test]
fn held_trades_come_out_in_time_order_once_the_quotes_pass_them() {
    let rows = vec![
        ("trades", trade(3.0, 3.0)),
        ("trades", trade(1.0, 1.0)),
        ("trades", trade(2.0, 2.0)),
        ("quotes", quote(0.5, 5.0)),
        ("quotes", quote(2.5, 25.0)),
        ("quotes", quote(9.0, 90.0)),
    ];
    // 1 is released when the quote at 2.5 s arrives (1 s past it), 2 and 3 when the one at 9 s does
    assert_eq!(feed(&mut engine(Asof::Exact, "3"), rows), [(1.0, Some(5.0)), (2.0, Some(5.0)), (3.0, Some(25.0))]);
}

#[test]
fn a_stalled_quote_stream_delays_trades_by_at_most_the_hold() {
    let hold = ASOF_HOLD_US as f64 / 1e6;
    let rows = vec![("quotes", quote(0.0, 10.0)), ("trades", trade(1.0, 1.0)), ("trades", trade(1.0 + hold, 2.0))];
    let out = feed(&mut engine(Asof::Exact, "3"), rows.clone());
    assert!(out.is_empty(), "released before the hold: {out:?}");
    let mut rows = rows;
    rows.push(("trades", trade(1.5 + hold, 3.0)));
    assert_eq!(feed(&mut engine(Asof::Exact, "3"), rows), [(1.0, Some(10.0))]);
}

#[test]
fn a_checkpoint_taken_while_trades_are_held_resumes_exactly() {
    let rows = |n: usize| {
        (0..n)
            .map(|i| {
                if i % 3 == 0 {
                    ("trades", trade(i as f64, i as f64))
                } else {
                    ("quotes", quote(i as f64 - 1.5, i as f64))
                }
            })
            .collect::<Vec<_>>()
    };
    let whole = feed(&mut engine(Asof::Exact, "3"), rows(60));
    for cut in [1, 7, 31, 58] {
        let mut first = engine(Asof::Exact, "3");
        let mut out = feed(&mut first, rows(60)[..cut].to_vec());
        let mut second = engine(Asof::Exact, "3");
        second.restore(first.snapshot()).unwrap();
        out.extend(feed(&mut second, rows(60)[cut..].to_vec()));
        assert_eq!(out, whole, "cut at {cut}");
    }
}

/// The join mode is set before any row or restore: changing it on a join that has taken rows
/// (versions cut to `keep_versions`, or trades held for quotes an arrival join never waits for)
/// panics instead of matching them wrongly or holding them for good.
#[test]
fn the_join_mode_changes_only_before_the_join_takes_rows() {
    let changed = |mut e: Engine, to: Asof| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || e.set_asof(to))).is_err()
    };
    let fed = |asof: Asof, rows: Vec<(&str, Vec<Value>)>| {
        let mut e = engine(asof, "3");
        feed(&mut e, rows);
        e
    };
    // a quote leaves a version; an exact join holds a trade even without one
    assert!(changed(fed(Asof::Arrival, vec![("quotes", quote(0.0, 1.0))]), Asof::Exact));
    assert!(changed(fed(Asof::Exact, vec![("trades", trade(1.0, 1.0))]), Asof::Arrival));
    let mut restored = engine(Asof::Exact, "3");
    restored.restore(fed(Asof::Exact, vec![("quotes", quote(0.0, 1.0))]).snapshot()).unwrap();
    assert!(changed(restored, Asof::Arrival));
    // setting the same mode again changes nothing; an arrival join keeps nothing of a trade
    assert!(!changed(fed(Asof::Exact, vec![("quotes", quote(0.0, 1.0)), ("trades", trade(1.0, 1.0))]), Asof::Exact));
    assert!(!changed(fed(Asof::Arrival, vec![("trades", trade(1.0, 1.0))]), Asof::Exact));
}

#[test]
fn memory_stays_bounded_while_trades_stall() {
    // quotes only, 100 a second: a version is kept only while a trade can still need it (the
    // hold's worth behind the newest quote), so the state stops growing with the quote count
    let size = |seconds: usize| {
        let mut e = engine(Asof::Exact, "5000");
        let mut out = vec![];
        for c in 0..seconds {
            e.insert("quotes", (0..100).map(|i| quote(c as f64 + i as f64 * 0.01, 1.0)).collect(), &mut out);
        }
        postcard::to_allocvec(&e.snapshot()).unwrap().len()
    };
    // (500 s and 1000 s: timestamps of the same varint width)
    let (a, b) = (size(500), size(1000));
    assert!(b <= a + a / 20, "{a} bytes after 500 s of quotes, {b} after 1000 s");
}

const LATEST_IF: &str = "
CREATE STREAM IF NOT EXISTS s (event_time datetime64(6), symbol string, x nullable(float64));
CREATE EXTERNAL STREAM IF NOT EXISTS o (symbol string, last nullable(float64), n uint64)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO o AS
SELECT symbol, latest_if(x, x IS NOT NULL) AS last, count_if(x > 1) AS n
FROM tumble(s, event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;
";

#[test]
fn an_if_aggregate_skips_the_rows_its_condition_rejects() {
    // latest_if(last_price_value, last_price_value IS NOT NULL)
    let mut e = Engine::new(&parse(LATEST_IF).unwrap()).unwrap();
    let mut out = vec![];
    let row = |s: f64, x: Option<f64>| vec![t(s), Value::Str("A".into()), x.map_or(Value::Null, Value::F64)];
    let rows = vec![row(1.0, Some(2.0)), row(2.0, Some(3.0)), row(2.5, Some(4.0)), row(3.0, Some(0.5)), row(4.0, None)];
    e.insert("s", rows.into_iter().chain([row(61.0, None)]).collect(), &mut out);
    assert_eq!(out.len(), 1);
    // three rows above 1 and one below: the condition, not its negation, selects
    assert_eq!(out[0].payload.trim_end(), r#"{"symbol":"A","last":0.5,"n":3}"#);
}

#[test]
fn only_an_aggregate_of_one_argument_takes_if() {
    // vwap takes two; `foo` is no aggregate at all
    for call in ["vwap_if(x, x, x > 1)", "foo_if(x, x > 1)"] {
        let sql = LATEST_IF.replace("count_if(x > 1)", call);
        assert!(
            parse(&sql).map_err(|e| e.to_string()).and_then(|c| Engine::new(&c).map(|_| ())).is_err(),
            "{call} was accepted"
        );
    }
}

#[test]
fn a_held_snapshot_that_is_out_of_order_or_misshapen_is_refused() {
    let mut e = engine(Asof::Exact, "3");
    feed(&mut e, vec![("quotes", quote(0.0, 1.0)), ("trades", trade(2.0, 2.0)), ("trades", trade(1.0, 1.0))]);
    let good = serde_json::to_value(e.snapshot()).unwrap();
    let held = |j: &mut serde_json::Value| j[0][0]["JoinHeld"]["held"].as_array_mut().unwrap().clone();
    assert_eq!(held(&mut good.clone()).len(), 2, "{good}");
    let broken: [fn(&mut serde_json::Value); 5] = [
        |j| j[0][0]["JoinHeld"]["held"].as_array_mut().unwrap().reverse(),
        |j| j[0][0]["JoinHeld"]["right_max"].as_array_mut().unwrap().push(0.into()),
        |j| j[0][0]["JoinHeld"]["held"][0][0].as_array_mut().unwrap().push(0.into()),
        // no time at all: the order check used to index it and panic
        |j| j[0][0]["JoinHeld"]["held"][0][0].as_array_mut().unwrap().clear(),
        |j| j[0][0]["JoinHeld"]["held"][1][0].as_array_mut().unwrap().clear(),
    ];
    for (i, f) in broken.iter().enumerate() {
        let mut j = good.clone();
        f(&mut j);
        let state = serde_json::from_value(j).unwrap();
        assert!(engine(Asof::Exact, "3").restore(state).is_err(), "broken snapshot {i} restored");
    }
    assert!(engine(Asof::Exact, "3").restore(serde_json::from_value(good).unwrap()).is_ok());
}

#[test]
fn views_see_a_backlog_whatever_order_they_were_created_in() {
    // Proton starts the Kafka-reading views first, and a replayed backlog passes through
    // the intermediate streams before the window views read it. brrrrr builds every view
    // before it reads anything.
    let sql = "
CREATE STREAM IF NOT EXISTS raw (event_time datetime64(6), symbol string, x float64);
CREATE STREAM IF NOT EXISTS mid (event_time datetime64(6), symbol string, x float64);
CREATE EXTERNAL STREAM IF NOT EXISTS o (symbol string, n uint64) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS ingest INTO mid AS SELECT event_time, symbol, x FROM raw;
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO o AS
SELECT symbol, count() AS n FROM tumble(mid, event_time, 1m) GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' MILLISECOND;
";
    let mut e = Engine::new(&parse(sql).unwrap()).unwrap();
    let mut out = vec![];
    let rows = (0..7).map(|i| vec![t(i as f64), Value::Str("A".into()), Value::F64(1.0)]).chain([vec![
        t(61.0),
        Value::Str("A".into()),
        Value::F64(1.0),
    ]]);
    e.insert("raw", rows.collect(), &mut out);
    assert_eq!(out[0].payload.trim_end(), r#"{"symbol":"A","n":7}"#);
}

proptest::proptest! {
    // PROPTEST_CASES overrides the 200 (CI runs the default)
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|c| c.parse().ok()).unwrap_or(200)
    ))]
    /// Trades and quotes of three symbols, each delayed by under ASOF_LATENESS_US (1 s) on
    /// its way to us and interleaved in any order: the exact join equals a batch as-of join.
    #[test]
    fn exact_equals_a_batch_asof_join(
        trades in proptest::collection::vec((0u32..3, 0u32..600, 0.0f64..1.0), 1..200),
        quotes in proptest::collection::vec((0u32..3, 0u32..600, 0.0f64..1.0), 0..300),
    ) {
        let sym = |s: u32| Value::Str(["A", "B", "C"][s as usize].into());
        // times in 0.1 s steps, prices unique so each output row names its trade
        let tr: Vec<(u32, f64, f64, f64)> = trades.iter().enumerate().map(|(i, &(s, t, d))| (s, t as f64 / 10.0, i as f64 + 1.0, d)).collect();
        let qu: Vec<(u32, f64, f64, f64)> = quotes.iter().enumerate().map(|(i, &(s, t, d))| (s, t as f64 / 10.0, 1000.0 + i as f64, d)).collect();
        // the batch as-of join: the latest quote at or before the trade's time (on equal times,
        // the one that arrived last, as brrrrr inserts a new version first)
        let mut want: Vec<(f64, f64, Option<f64>)> = tr.iter().map(|&(s, t, p, _)| {
            let q = qu.iter().filter(|q| q.0 == s && q.1 <= t)
                .max_by(|a, b| a.1.total_cmp(&b.1).then((a.1 + a.3).total_cmp(&(b.1 + b.3))));
            (t, p, q.map(|q| q.2))
        }).collect();
        want.sort_by(|a, b| a.0.total_cmp(&b.0));
        // arrival: by time plus delay, both streams interleaved
        let mut arrivals: Vec<(f64, &str, Vec<Value>)> = tr.iter().map(|&(s, t, p, d)| (t + d, "trades", vec![Value::Time((t * 1e6) as i64), sym(s), Value::F64(p)]))
            .chain(qu.iter().map(|&(s, t, a, d)| (t + d, "quotes", vec![Value::Time((t * 1e6) as i64), sym(s), Value::F64(a)])))
            .collect();
        arrivals.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut e = engine(Asof::Exact, "3");
        let mut out = vec![];
        for (_, stream, row) in arrivals {
            e.insert(stream, vec![row], &mut out);
        }
        // a quote far in the future releases every held trade
        e.insert("quotes", vec![vec![Value::Time(1_000_000_000_000), sym(0), Value::F64(1.0)]], &mut out);
        let got: Vec<(f64, f64, Option<f64>)> = out.iter().map(|m| {
            let j: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
            let t = tr.iter().find(|x| x.2 == j["price"].as_f64().unwrap()).unwrap().1;
            (t, j["price"].as_f64().unwrap(), j["ask"].as_f64())
        }).collect();
        let key = |v: &mut Vec<(f64, f64, Option<f64>)>| v.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.total_cmp(&b.1)));
        let (mut got, mut want) = (got, want);
        key(&mut got);
        key(&mut want);
        proptest::prop_assert_eq!(got, want);
    }
}

#[test]
fn a_checkpoint_restores_only_into_the_join_mode_that_wrote_it() {
    // an exact join's held rows mean nothing to an arrival join, and an arrival checkpoint has
    // none for an exact one (brrrrr run keeps each mode's checkpoints under its own prefix)
    let mut exact = engine(Asof::Exact, "3");
    feed(&mut exact, vec![("quotes", quote(0.0, 1.0)), ("trades", trade(1.0, 1.0))]);
    let arrival = engine(Asof::Arrival, "3");
    assert!(engine(Asof::Arrival, "3").restore(exact.snapshot()).is_err());
    assert!(engine(Asof::Exact, "3").restore(arrival.snapshot()).is_err());
    assert!(engine(Asof::Exact, "3").restore(exact.snapshot()).is_ok());
}

/// Trades joined to their quote feed 1m bars (slippage and derivatives are built this way). The
/// window view is created first: views are closed upstream first whatever their order.
const IDLE: &str = "
CREATE STREAM IF NOT EXISTS trades (event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS quotes (event_time datetime64(6), symbol string, ask float64);
CREATE STREAM IF NOT EXISTS joined (event_time datetime64(6), price float64, ask float64);
CREATE EXTERNAL STREAM IF NOT EXISTS bars (time int64, n uint64, ask float64)
  SETTINGS type = 'kafka', topic = 'b', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS bar INTO bars AS
SELECT to_unix_timestamp64_micro(window_start) AS time, count() AS n, latest(ask) AS ask
FROM tumble(joined, event_time, 1m) GROUP BY window_start
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS j INTO joined AS
SELECT t.event_time AS event_time, t.price AS price, q.ask AS ask
FROM trades AS t ASOF LEFT JOIN quotes AS q ON t.symbol = q.symbol AND t.event_time >= q.event_time;
";

/// Rows in, one at a time, and idle closes (`("close", at)`); returns the bars as (start in
/// seconds, trades, latest ask).
fn idle(e: &mut Engine, steps: Vec<(&str, Vec<Value>)>) -> Vec<(i64, u64, f64)> {
    let mut out = vec![];
    for (stream, row) in steps {
        match stream {
            "close" => e.close_until(row[0].i64().unwrap(), &mut out),
            stream => e.insert(stream, vec![row], &mut out),
        }
    }
    out.iter()
        .map(|m| {
            let j: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
            (j["time"].as_i64().unwrap() / 1_000_000, j["n"].as_u64().unwrap(), j["ask"].as_f64().unwrap())
        })
        .collect()
}

fn idle_engine(asof: Asof) -> Engine {
    let mut e = Engine::new(&parse(IDLE).unwrap()).unwrap();
    e.set_asof(asof);
    e
}

fn close(s: f64) -> (&'static str, Vec<Value>) {
    ("close", vec![t(s)])
}

/// An idle close used to advance the bars' window to the idle
/// time but leave the trade the exact join held; the quote that came when traffic resumed
/// released it below that watermark, and the bar dropped it as late. The close now releases
/// the held trades every quote side has passed by then (as a quote at that time would) and
/// feeds them to the window before closing it.
#[test]
fn an_idle_close_releases_what_an_exact_join_holds_before_it_closes_the_windows_after_it() {
    for asof in [Asof::Arrival, Asof::Exact] {
        let mut e = idle_engine(asof);
        let steps = vec![("quotes", quote(0.0, 10.0)), ("trades", trade(5.0, 1.0)), close(200.0)];
        assert_eq!(idle(&mut e, steps), [(0, 1, 10.0)], "{asof:?}");
        assert_eq!((idle(&mut e, vec![("quotes", quote(300.0, 30.0))]), e.late()), (vec![], 0), "{asof:?}");
    }
    // a trade is released once the idle time is 1 s (the lateness a quote may have) past it, and
    // the window after the join is closed no further than the trades it still holds
    let lateness = ASOF_LATENESS_US as f64 / 1e6;
    let steps = |at: f64| vec![("quotes", quote(0.0, 10.0)), ("trades", trade(119.5, 1.0)), close(at)];
    assert_eq!(idle(&mut idle_engine(Asof::Exact), steps(119.5 + lateness - 0.000_001)), []);
    assert_eq!(idle(&mut idle_engine(Asof::Exact), steps(119.5 + lateness)), [(60, 1, 10.0)]);
}

/// A trade an idle close releases is matched with the quotes at hand: a quote at or before its
/// time that arrives after the close is too late for it. The join's own newest times do not
/// move: a trade after the close waits for its quotes as before, and is matched exactly.
#[test]
fn an_idle_close_moves_no_time_of_the_exact_join() {
    let steps = vec![
        ("quotes", quote(0.0, 10.0)),
        ("trades", trade(5.0, 1.0)),
        close(200.0),
        // would have been the trade's quote without the close
        ("quotes", quote(4.0, 14.0)),
        // not released with the quote at 4 s: no quote has passed 199 s
        ("trades", trade(198.0, 2.0)),
        ("quotes", quote(197.0, 19.7)),
        ("quotes", quote(300.0, 30.0)),
        close(10_000.0),
    ];
    let mut e = idle_engine(Asof::Exact);
    assert_eq!(idle(&mut e, steps), [(0, 1, 10.0), (180, 1, 19.7)]);
    assert_eq!(e.late(), 0);
}

/// Trades counted in 1m bars, each joined to its quote: a window inside a join side.
const NESTED: &str = "
CREATE STREAM IF NOT EXISTS trades (event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS quotes (event_time datetime64(6), symbol string, ask float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (time int64, n uint64, ask nullable(float64))
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS joined INTO out AS
SELECT to_unix_timestamp64_micro(b.event_time) AS time, b.n AS n, null_if(q.ask, 0) AS ask
FROM (SELECT window_start AS event_time, symbol, count() AS n FROM tumble(trades, event_time, 1m)
  GROUP BY window_start, symbol) AS b
ASOF LEFT JOIN quotes AS q ON b.symbol = q.symbol AND b.event_time >= q.event_time
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
";

/// The emitted rows' `(time in seconds, n, ask)`.
fn counted(out: &[brrrrr_core::engine::Emit]) -> Vec<(i64, u64, f64)> {
    out.iter()
        .map(|m| {
            let j: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
            (j["time"].as_i64().unwrap() / 1_000_000, j["n"].as_u64().unwrap(), j["ask"].as_f64().unwrap())
        })
        .collect()
}

/// The counters, the watermark and idle close used to see top-level
/// windows only, so a window inside a join side dropped rows unreported and was never closed.
#[test]
fn a_window_inside_a_join_side_counts_what_it_drops_and_is_idle_closed() {
    let mut e = Engine::new(&parse(NESTED).unwrap()).unwrap();
    e.set_asof(Asof::Exact);
    e.set_time_limit(Some((LIMIT_S * 1e6) as i64));
    let mut untimed = trade(0.0, 0.0);
    untimed[0] = Value::Null;
    let rows = vec![
        ("quotes", quote(0.0, 10.0)),
        ("trades", trade(10.0, 1.0)),
        ("trades", trade(20.0, 2.0)),
        // closes the bar of [0, 60), which the join holds for a quote 1 s past it
        ("trades", trade(70.0, 3.0)),
        ("trades", trade(30.0, 4.0)),
        ("trades", trade(YEAR_3000, 5.0)),
        ("trades", untimed),
    ];
    let mut out = vec![];
    for (stream, row) in rows {
        e.insert(stream, vec![row], &mut out);
    }
    assert!(out.is_empty(), "{out:?}");
    assert_eq!((e.late(), e.future(), e.null_time()), (1, 1, 1));
    assert_eq!((e.min_watermark(), e.max_event_time()), (Some(60_000_000), Some(70_000_000)));
    // by 90 s the quotes have passed the bar of [0, 60); the bar of [60, 120) closes at 120 s
    e.close_until(90_000_000, &mut out);
    assert_eq!(counted(&out), [(0, 2, 10.0)]);
    e.close_until(125_000_000, &mut out);
    assert_eq!(counted(&out), [(0, 2, 10.0), (60, 1, 10.0)]);
}

/// A window inside a join side is what a message of the join holds: its end is the message's
/// `window_end`, by which a sink's lost history withholds it.
#[test]
fn a_message_of_a_window_inside_a_join_side_carries_its_end() {
    let mut e = Engine::new(&parse(NESTED).unwrap()).unwrap();
    e.set_asof(Asof::Arrival);
    let mut out = vec![];
    for (stream, row) in [("quotes", quote(0.0, 10.0)), ("trades", trade(10.0, 1.0)), ("trades", trade(70.0, 2.0))] {
        e.insert(stream, vec![row], &mut out);
    }
    assert_eq!(counted(&out), [(0, 1, 10.0)]);
    assert_eq!(out[0].window_end, 60_000_000);
}

/// Trades joined to the 1m bars of their quotes, which another view makes.
const BARRED: &str = "
CREATE STREAM IF NOT EXISTS trades (event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS quotes (event_time datetime64(6), symbol string, ask float64);
CREATE STREAM IF NOT EXISTS quote_bars (event_time datetime64(6), symbol string, ask float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (event_time datetime64(6), symbol string, price float64, ask nullable(float64))
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS joined INTO out AS
SELECT t.event_time AS event_time, t.symbol AS symbol, t.price AS price, null_if(q.ask, 0) AS ask
FROM trades AS t ASOF LEFT JOIN quote_bars AS q ON t.symbol = q.symbol AND t.event_time >= q.event_time;
CREATE MATERIALIZED VIEW IF NOT EXISTS bars INTO quote_bars AS
SELECT window_start AS event_time, symbol, max(ask) AS ask FROM tumble(quotes, event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
";

/// An idle close takes each join side only as far as its own input has reached: the quote bars
/// reach the start of the bar still open, not the idle time, and a trade in that bar waits for
/// it. A stalled quote side holds a trade for at most `ASOF_HOLD_US` behind the trades' idle
/// time, as a trade at that time would.
#[test]
fn an_idle_close_releases_a_held_trade_as_far_as_each_join_side_has_reached() {
    let barred = || {
        let mut e = Engine::new(&parse(BARRED).unwrap()).unwrap();
        e.set_asof(Asof::Exact);
        // the bar of [0, 60) closes at 70 s: a version at 0 s, ask 1
        let rows = vec![("quotes", quote(10.0, 1.0)), ("quotes", quote(70.0, 2.0)), ("trades", trade(65.0, 1.0))];
        assert_eq!(feed(&mut e, rows), []);
        e
    };
    let at = |s: i64, out: &mut Vec<brrrrr_core::engine::Emit>, e: &mut Engine| -> Vec<Option<f64>> {
        e.close_until(s, out);
        out.iter().map(|m| serde_json::from_str::<serde_json::Value>(&m.payload).unwrap()["ask"].as_f64()).collect()
    };
    let (mut e, mut out) = (barred(), vec![]);
    // at 90 s the bars have reached 60 s only: the trade at 65 s waits for [60, 120)
    assert_eq!(at(90_000_000, &mut out, &mut e), [] as [Option<f64>; 0]);
    // at 125 s the close closes that bar first, and the trade takes it
    assert_eq!(at(125_000_000, &mut out, &mut e), [Some(2.0)]);
    let (mut e, mut out) = (barred(), vec![]);
    // held while it is no more than the hold behind, with the bar it has once it is
    let held = 65_000_000 + ASOF_HOLD_US;
    assert_eq!(at(held, &mut out, &mut e), [] as [Option<f64>; 0]);
    assert_eq!(at(held + 1, &mut out, &mut e), [Some(1.0)]);
}

/// An exact join holds a trade until every quote stream is `lateness` past it (1 s by default,
/// `set_asof_lateness` to change it): slippage and basis are that late for no other reason,
/// where quotes' real disorder is a fraction of a second.
#[test]
fn a_shorter_lateness_releases_a_trade_sooner() {
    let rows = || vec![("quotes", quote(9.9, 10.0)), ("trades", trade(10.0, 1.0)), ("quotes", quote(10.15, 11.0))];
    // the default, a second: a quote 150 ms past the trade does not release it
    assert_eq!(feed(&mut engine(Asof::Exact, "3"), rows()), []);
    let mut e = engine(Asof::Exact, "3");
    e.set_asof_lateness(100_000);
    assert_eq!(feed(&mut e, rows()), [(1.0, Some(10.0))]);
}

/// What the shorter wait costs, and what it does not: a quote out of order by less than the
/// lateness is still matched exactly; one later than that arrives after its trade was released
/// and is missed. The lateness is therefore sized from the quotes' measured disorder.
#[test]
fn a_quote_inside_the_lateness_is_matched_exactly_and_one_outside_is_missed() {
    let rows = |straggler_at: f64| {
        vec![
            ("quotes", quote(9.9, 10.0)),
            ("trades", trade(10.0, 1.0)),
            ("quotes", quote(10.05, 12.0)), // newest 10.05: not yet 100 ms past the trade
            ("quotes", quote(straggler_at, 13.0)),
            ("quotes", quote(10.2, 14.0)), // releases the trade
        ]
    };
    let join = |straggler_at: f64| {
        let mut e = engine(Asof::Exact, "3");
        e.set_asof_lateness(100_000);
        feed(&mut e, rows(straggler_at))
    };
    // 70 ms behind the newest quote, inside the 100 ms: the trade takes it
    assert_eq!(join(9.98), [(1.0, Some(13.0))]);
    // the straggler reaches us after a quote 150 ms past the trade has already released it
    let late = vec![
        ("quotes", quote(9.9, 10.0)),
        ("trades", trade(10.0, 1.0)),
        ("quotes", quote(10.15, 11.0)),
        ("quotes", quote(9.98, 13.0)),
    ];
    let mut e = engine(Asof::Exact, "3");
    e.set_asof_lateness(100_000);
    assert_eq!(feed(&mut e, late), [(1.0, Some(10.0))]);
}

/// A lateness the join cannot honour is refused, not clamped: zero would release a trade before
/// any quote at its time, and past the hold the hold decides anyway.
#[test]
fn a_lateness_outside_zero_to_the_hold_is_refused() {
    for bad in [0, -1, ASOF_HOLD_US + 1] {
        let refused = std::panic::catch_unwind(move || engine(Asof::Exact, "3").set_asof_lateness(bad)).is_err();
        assert!(refused, "{bad}");
    }
    engine(Asof::Exact, "3").set_asof_lateness(ASOF_HOLD_US);
}

fn quote_of(symbol: &str, s: f64, ask: f64) -> Vec<Value> {
    vec![t(s), Value::Str(symbol.into()), Value::F64(ask)]
}

/// A quote that reaches an exact join after a trade of its symbol was released, at or before
/// that trade's time, may have been its match: it is counted, so a lateness too short for the
/// feed shows. Equal times match, so a quote at the trade's own time counts. A quote after the
/// trade, or of another symbol, is no miss. Counting changes no output.
#[test]
fn a_quote_at_or_before_a_released_trade_of_its_symbol_is_counted_late() {
    let late = |straggler: Vec<Value>| {
        let mut e = engine(Asof::Exact, "3");
        e.set_asof_lateness(100_000);
        let rows = vec![
            ("quotes", quote(9.9, 10.0)),
            ("quotes", quote_of("B", 9.0, 20.0)),
            ("trades", trade(10.0, 1.0)),
            ("trades", vec![t(9.5), Value::Str("B".into()), Value::F64(2.0)]),
            ("quotes", quote(10.15, 11.0)), // releases both trades
            ("quotes", straggler),
        ];
        let out = feed(&mut e, rows);
        assert_eq!(out, [(2.0, Some(20.0)), (1.0, Some(10.0))]);
        e.asof_late_right()
    };
    assert_eq!(late(quote(9.98, 13.0)), 1);
    assert_eq!(late(quote(10.0, 13.0)), 1, "at the trade's time");
    assert_eq!(late(quote(10.01, 13.0)), 0, "after the trade");
    // B's released trade is at 9.5: A's at 10 s is not B's
    assert_eq!(late(quote_of("B", 9.98, 23.0)), 0, "another symbol");
    assert_eq!(late(quote_of("B", 9.5, 23.0)), 1, "B's own");
}

/// The same straggler inside the default second is matched, not late; arrival mode, which
/// holds nothing, counts nothing.
#[test]
fn a_quote_inside_the_lateness_or_in_arrival_mode_is_not_counted_late() {
    let rows = || {
        vec![
            ("quotes", quote(9.9, 10.0)),
            ("trades", trade(10.0, 1.0)),
            ("quotes", quote(10.15, 11.0)),
            ("quotes", quote(9.98, 13.0)),
            ("quotes", quote(11.1, 14.0)), // releases the trade under the default second
        ]
    };
    let mut e = engine(Asof::Exact, "3");
    assert_eq!(feed(&mut e, rows()), [(1.0, Some(13.0))]);
    assert_eq!(e.asof_late_right(), 0);
    let mut e = engine(Asof::Arrival, "3");
    assert_eq!(feed(&mut e, rows()), [(1.0, Some(10.0))]);
    assert_eq!(e.asof_late_right(), 0);
}

/// A trade an idle close released is released as any other: a quote at or before it that
/// arrives afterwards is counted.
#[test]
fn a_quote_after_an_idle_close_released_its_trade_is_counted_late() {
    let mut e = engine(Asof::Exact, "3");
    assert_eq!(feed(&mut e, vec![("quotes", quote(9.9, 10.0)), ("trades", trade(10.0, 1.0))]), []);
    let mut out = vec![];
    e.close_until(11_000_000, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(feed(&mut e, vec![("quotes", quote(9.95, 13.0)), ("quotes", quote(10.5, 14.0))]), []);
    assert_eq!(e.asof_late_right(), 1);
}
