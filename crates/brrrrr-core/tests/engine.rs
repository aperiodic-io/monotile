//! The dataflow engine's streaming semantics. Each rule below was established from Timeplus
//! Proton's source, and confirmed on Proton running: a wrong rule changes which windows and rows
//! a pipeline emits, not just how fast we compute them.
use brrrrr_core::agg::Acc;
use brrrrr_core::engine::{Emit, Engine};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;

fn engine(sql: &str) -> Engine {
    Engine::new(&parse(sql).unwrap_or_else(|e| panic!("{e}"))).unwrap_or_else(|e| panic!("{e}"))
}

/// Seconds since the epoch as a datetime64(6).
fn t(s: f64) -> Value {
    Value::Time((s * 1e6).round() as i64)
}

fn s(x: &str) -> Value {
    Value::Str(x.into())
}

fn f(x: f64) -> Value {
    Value::F64(x)
}

fn push(e: &mut Engine, stream: &str, rows: Vec<Vec<Value>>) -> Vec<String> {
    push_all(e, stream, rows).into_iter().map(|m| m.payload.trim_end().to_string()).collect()
}

fn push_all(e: &mut Engine, stream: &str, rows: Vec<Vec<Value>>) -> Vec<Emit> {
    let mut out = vec![];
    e.insert(stream, rows, &mut out);
    out
}

const WINDOW: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
CREATE EXTERNAL STREAM IF NOT EXISTS out (
  symbol string,
  time int64,
  n uint64,
  last float64,
  mean float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['dedup-key'], [concat(to_string(time), '|', symbol)]), 'map(string, string)')
) SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'm.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS n, latest(price) AS last, avg(price) AS mean
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

#[test]
fn a_window_closes_only_once_the_stream_passes_its_end_plus_the_delay() {
    let mut e = engine(WINDOW);
    assert!(push(&mut e, "trades", vec![vec![t(10.0), s("A"), f(1.0)]]).is_empty());
    // 60.049999 s: max_ts - delay is still inside [0, 60)
    assert!(push(&mut e, "trades", vec![vec![t(60.049_999), s("B"), f(2.0)]]).is_empty());
    let out = push(&mut e, "trades", vec![vec![t(60.05), s("B"), f(3.0)]]);
    assert_eq!(out, [r#"{"symbol":"A","time":0,"n":1,"last":1,"mean":1}"#]);
}

#[test]
fn one_watermark_per_view_so_any_symbol_closes_every_symbols_window() {
    let mut e = engine(WINDOW);
    push(&mut e, "trades", vec![vec![t(1.0), s("B"), f(5.0)], vec![t(2.0), s("A"), f(7.0)]]);
    // only C advances the stream; A and B close with it, in key order within the window
    let out = push(&mut e, "trades", vec![vec![t(125.0), s("C"), f(1.0)]]);
    assert_eq!(
        out,
        [r#"{"symbol":"A","time":0,"n":1,"last":7,"mean":7}"#, r#"{"symbol":"B","time":0,"n":1,"last":5,"mean":5}"#]
    );
    // and windows come out in start order: [60, 120) had nothing, [120, 180) is still open
    assert_eq!(
        push(&mut e, "trades", vec![vec![t(180.05), s("A"), f(1.0)]]),
        [r#"{"symbol":"C","time":120000000,"n":1,"last":1,"mean":1}"#]
    );
}

#[test]
fn several_windows_closed_by_one_chunk_come_out_in_start_order() {
    let mut e = engine(WINDOW);
    // 60.0 opens [60, 120) while [0, 60) stays open (60.0 - delay is still before 60)
    assert!(push(&mut e, "trades", vec![vec![t(10.0), s("A"), f(1.0)], vec![t(60.0), s("A"), f(2.0)]]).is_empty());
    let out = push(&mut e, "trades", vec![vec![t(200.0), s("B"), f(0.0)]]);
    assert_eq!(
        out,
        [
            r#"{"symbol":"A","time":0,"n":1,"last":1,"mean":1}"#,
            r#"{"symbol":"A","time":60000000,"n":1,"last":2,"mean":2}"#
        ]
    );
}

#[test]
fn a_row_older_than_the_previous_watermark_is_dropped_but_an_open_window_still_takes_it() {
    let mut e = engine(WINDOW);
    push(&mut e, "trades", vec![vec![t(10.0), s("A"), f(1.0)]]);
    // max_ts 60.02: watermark floor(59.97, 60) = 0, window [0, 60) stays open
    assert!(push(&mut e, "trades", vec![vec![t(60.02), s("B"), f(1.0)]]).is_empty());
    // 59.9 is older than max_ts - delay but its window is open: aggregated as the latest row
    assert!(push(&mut e, "trades", vec![vec![t(59.9), s("A"), f(9.0)]]).is_empty());
    let out = push(&mut e, "trades", vec![vec![t(60.06), s("B"), f(1.0)]]);
    assert_eq!(out, [r#"{"symbol":"A","time":0,"n":2,"last":9,"mean":5}"#]);
    assert_eq!(e.late(), 0);
    // [0, 60) was emitted: a row for it is late and never re-opens the window
    assert!(push(&mut e, "trades", vec![vec![t(59.0), s("A"), f(100.0)]]).is_empty());
    assert_eq!(e.late(), 1);
    let out = push(&mut e, "trades", vec![vec![t(200.0), s("B"), f(1.0)]]);
    assert_eq!(out, [r#"{"symbol":"B","time":60000000,"n":2,"last":1,"mean":1}"#]);
}

#[test]
fn the_watermark_moves_per_chunk_so_lateness_depends_on_chunk_boundaries() {
    // same rows, one chunk: 59 s arrives before the watermark moves, so it counts
    let mut one = engine(WINDOW);
    let rows = vec![vec![t(10.0), s("A"), f(1.0)], vec![t(61.0), s("B"), f(1.0)], vec![t(59.0), s("A"), f(2.0)]];
    let out = push(&mut one, "trades", rows.clone());
    assert_eq!(out, [r#"{"symbol":"A","time":0,"n":2,"last":2,"mean":1.5}"#]);
    // one row per chunk: 61 s closes [0, 60) first, so 59 s is late
    let mut many = engine(WINDOW);
    let mut out = vec![];
    for r in rows {
        out.extend(push(&mut many, "trades", vec![r]));
    }
    assert_eq!(out, [r#"{"symbol":"A","time":0,"n":1,"last":1,"mean":1}"#]);
    assert_eq!(many.late(), 1);
}

#[test]
fn latest_and_avg_skip_nulls_and_a_window_of_only_nulls_still_counts_rows() {
    let mut e = engine(WINDOW);
    push(
        &mut e,
        "trades",
        vec![vec![t(1.0), s("A"), f(4.0)], vec![t(2.0), s("A"), Value::Null], vec![t(3.0), s("N"), Value::Null]],
    );
    let out = push(&mut e, "trades", vec![vec![t(100.0), s("Z"), f(0.0)]]);
    // the sink column is float64 (not nullable): NULL is inserted as its default, 0
    assert_eq!(
        out,
        [r#"{"symbol":"A","time":0,"n":2,"last":4,"mean":4}"#, r#"{"symbol":"N","time":0,"n":1,"last":0,"mean":0}"#]
    );
}

#[test]
fn a_sink_message_carries_the_materialized_dedup_header() {
    let mut e = engine(WINDOW);
    push_all(&mut e, "trades", vec![vec![t(1.0), s("A"), f(4.0)]]);
    let out = push_all(&mut e, "trades", vec![vec![t(100.0), s("Z"), f(0.0)]]);
    assert_eq!(
        out,
        [Emit {
            topic: "m.1m".into(),
            payload: "{\"symbol\":\"A\",\"time\":0,\"n\":1,\"last\":4,\"mean\":4}\n".into(),
            headers: vec![("dedup-key".into(), "0|A".into())],
            window_end: 60_000_000,
        }]
    );
}

#[test]
fn the_lowest_window_watermark_is_reported() {
    let mut e = engine(UNION);
    assert_eq!(e.min_watermark(), None, "no window has seen a row");
    push(&mut e, "trades", vec![vec![t(110.0), s("A"), f(1.0)]]);
    // the 1-minute view is at floor(109.95, 60) = 60 s, the 2-minute view at floor(109.95, 120) = 0
    assert_eq!(e.min_watermark(), Some(0));
    push(&mut e, "trades", vec![vec![t(250.0), s("A"), f(1.0)]]);
    assert_eq!(e.min_watermark(), Some(240_000_000));
}

/// Freshness is the latest event time a window has taken, unrounded: the lowest watermark is
/// floored to the widest window (a 1d view's is midnight UTC), so it reads hours behind on a
/// pipeline that is fully caught up and cannot tell a stalled pipeline from a healthy one.
#[test]
fn the_latest_event_time_is_reported_whatever_the_window_widths() {
    let mut e = engine(UNION);
    assert_eq!(e.max_event_time(), None, "no window has seen a row");
    push(&mut e, "trades", vec![vec![t(110.0), s("A"), f(1.0)]]);
    assert_eq!((e.max_event_time(), e.min_watermark()), (Some(110_000_000), Some(0)));
    // a late row does not move it back, a later one moves it forward to the second
    push(&mut e, "trades", vec![vec![t(1.0), s("A"), f(1.0)]]);
    assert_eq!(e.max_event_time(), Some(110_000_000));
    push(&mut e, "trades", vec![vec![t(250.5), s("A"), f(1.0)]]);
    assert_eq!((e.max_event_time(), e.min_watermark()), (Some(250_500_000), Some(240_000_000)));
}

#[test]
fn an_empty_chunk_changes_nothing() {
    let mut e = engine(WINDOW);
    push(&mut e, "trades", vec![vec![t(1.0), s("A"), f(4.0)]]);
    assert!(push(&mut e, "trades", vec![]).is_empty());
    assert_eq!(push(&mut e, "trades", vec![vec![t(60.05), s("A"), f(4.0)]]).len(), 1);
}

const UNION: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS agg (symbol string, interval string, time int64, hi float64, lo float64, range float64, range_sq float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out_1m (symbol string, interval string, time int64, hi float64, range float64, range_sq float32)
  SETTINGS type = 'kafka', topic = 'r.1m', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM IF NOT EXISTS out_2m (symbol string, interval string, time int64, lo float64, range float64)
  SETTINGS type = 'kafka', topic = 'r.2m', data_format = 'JSONEachRow';
CREATE EXTERNAL TABLE IF NOT EXISTS parquet_out (symbol string, time int64) SETTINGS type = 's3', format = 'Parquet';
CREATE MATERIALIZED VIEW IF NOT EXISTS agg_1m INTO agg AS
SELECT symbol, '1m' AS interval, to_unix_timestamp64_micro(window_start) AS time, max(price) AS hi, min(price) AS lo,
  (hi - lo) AS range, (range * range) AS range_sq
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS agg_2m INTO agg AS
WITH base AS (
  SELECT symbol, window_start, max(price) AS hi, min(price) AS lo
  FROM tumble(trades, local_event_time, 2m) GROUP BY window_start, symbol
)
SELECT symbol, '2m' AS interval, to_unix_timestamp64_micro(window_start) AS time, hi, lo, hi - lo AS range
FROM base
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS out_1m_mv INTO out_1m AS SELECT * FROM agg WHERE interval = '1m';
CREATE MATERIALIZED VIEW IF NOT EXISTS out_2m_mv INTO out_2m AS SELECT * FROM agg WHERE interval = '2m';
CREATE MATERIALIZED VIEW IF NOT EXISTS parquet_mv INTO parquet_out AS SELECT symbol, time FROM agg;
";

#[test]
fn views_union_into_one_stream_and_sinks_pick_their_interval_by_name() {
    let mut e = engine(UNION);
    assert_eq!(e.skipped, ["parquet_mv"], "S3 exports are another job's");
    let mut out = push_all(
        &mut e,
        "trades",
        vec![vec![t(1.0), s("A"), f(10.0)], vec![t(2.0), s("A"), f(12.5)], vec![t(61.0), s("A"), f(9.0)]],
    );
    out.extend(push_all(&mut e, "trades", vec![vec![t(125.0), s("A"), f(1.0)]]));
    let got: Vec<(String, String)> =
        out.iter().map(|m| (m.topic.to_string(), m.payload.trim_end().to_string())).collect();
    assert_eq!(
        got,
        [
            // 61 s closes the first minute; 125 s closes the second minute and the first 2 minutes,
            // agg_1m's output first (views run in creation order)
            ("r.1m".into(), r#"{"symbol":"A","interval":"1m","time":0,"hi":12.5,"range":2.5,"range_sq":6.25}"#.into()),
            ("r.1m".into(), r#"{"symbol":"A","interval":"1m","time":60000000,"hi":9,"range":0,"range_sq":0}"#.into()),
            ("r.2m".into(), r#"{"symbol":"A","interval":"2m","time":0,"lo":9,"range":3.5}"#.into()),
        ]
    );
}

#[test]
fn inserts_are_by_name_with_casts_to_the_target_types_and_defaults_for_missing_columns() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS src (a float64, b int64, c string);
CREATE EXTERNAL STREAM IF NOT EXISTS out (c string, missing int32, a float32, b int8, n nullable(float64))
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS SELECT b, a, c, null_if(a, 0) AS n, a * 2 FROM src WHERE b > 0;
",
    );
    let out = push(
        &mut e,
        "src",
        vec![
            vec![f(0.1), Value::Int(300), s("x")],
            vec![f(0.0), Value::Int(-1), s("dropped by WHERE")],
            vec![f(0.0), Value::Int(1), s("y")],
        ],
    );
    assert_eq!(
        out,
        [r#"{"c":"x","missing":0,"a":0.1,"b":44,"n":0.1}"#, r#"{"c":"y","missing":0,"a":0,"b":1,"n":null}"#]
    );
}

/// A view whose output columns are the target's, by name and in order, is inserted without
/// reordering: its values are still cast to the target's types.
#[test]
fn a_view_matching_the_target_columns_still_casts_its_values() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS src (a int64, b float64, c string);
CREATE EXTERNAL STREAM IF NOT EXISTS out (a string, b float32, c string)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS SELECT a, b, c FROM src;
",
    );
    let out = push(&mut e, "src", vec![vec![Value::Int(5), f(0.1), s("x")]]);
    assert_eq!(out, [r#"{"a":"5","b":0.1,"c":"x"}"#]);
}

/// A `DEFAULT` expression fills only a column the view does not supply (it applies to the
/// columns an insert omits); a `MATERIALIZED` one is computed on every insert, whatever the view
/// supplies. `DEFAULT` used to be compiled as `MATERIALIZED`: `SELECT 1 AS x` emitted `"x":42`.
#[test]
fn a_default_fills_only_an_omitted_column_and_materialized_always_applies() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS src (a int64);
CREATE EXTERNAL STREAM IF NOT EXISTS given (x float64 DEFAULT 42, y int64, z int64 MATERIALIZED y * 10)
  SETTINGS type = 'kafka', topic = 'g', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM IF NOT EXISTS omitted (x float64 DEFAULT y + 0.5, y int64, z int64 MATERIALIZED y * 10)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS supplied INTO given AS SELECT a AS x, a + 1 AS y, 7 AS z FROM src;
CREATE MATERIALIZED VIEW IF NOT EXISTS partial INTO omitted AS SELECT a + 1 AS y FROM src;
",
    );
    let out = push(&mut e, "src", vec![vec![Value::Int(1)]]);
    assert_eq!(out, [r#"{"x":1,"y":2,"z":20}"#, r#"{"x":2.5,"y":2,"z":20}"#]);
}

const ASOF: &str = "
CREATE STREAM IF NOT EXISTS aj_m (event_time datetime64(6), local_event_time datetime64(6), exchange string, symbol string, mark_price_value float64);
CREATE STREAM IF NOT EXISTS aj_i (event_time datetime64(6), symbol string, index_price_value float64);
CREATE STREAM IF NOT EXISTS aj_t (event_time datetime64(6), symbol string, last_price_value float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (event_time datetime64(6), symbol string, mark_price_value float64, index_price_value float64, last_price_value float64)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS enriched INTO out AS
SELECT
  m.event_time AS event_time,
  m.local_event_time AS local_event_time,
  m.exchange AS exchange,
  m.symbol AS symbol,
  m.mark_price_value AS mark_price_value,
  i.index_price_value AS index_price_value,
  t.last_price_value AS last_price_value
FROM (
  SELECT event_time, local_event_time, exchange, symbol, mark_price_value
  FROM aj_m
  ORDER BY symbol, event_time
) AS m
ASOF LEFT JOIN (
  SELECT event_time, symbol, index_price_value
  FROM aj_i
  ORDER BY symbol, event_time
) AS i
ON m.symbol = i.symbol AND m.event_time >= i.event_time
ASOF LEFT JOIN (
  SELECT event_time, symbol, last_price_value
  FROM aj_t
  ORDER BY symbol, event_time
) AS t
ON m.symbol = t.symbol AND m.event_time >= t.event_time;
";

fn mark(time: f64, price: f64) -> Vec<Value> {
    vec![t(time), t(time), s("x"), s("A"), f(price)]
}

fn right(time: f64, price: f64) -> Vec<Value> {
    vec![t(time), s("A"), f(price)]
}

fn row(time: &str, m: &str, i: &str, l: &str) -> String {
    format!(
        r#"{{"event_time":"1970-01-01 00:00:{time}","symbol":"A","mark_price_value":{m},"index_price_value":{i},"last_price_value":{l}}}"#
    )
}

/// The sequence observed on the Timeplus fork (derivatives' three-stream shape), step by step.
#[test]
fn asof_left_join_reproduces_the_observed_sequence() {
    let mut e = engine(ASOF);
    // a left row with no right version yet comes out at once, with defaults (not NULL)
    assert_eq!(push(&mut e, "aj_m", vec![mark(10.0, 1.0)]), [row("10.000000", "1", "0", "0")]);
    // right rows emit nothing; the left row takes the latest version <= its time
    assert!(push(&mut e, "aj_i", vec![right(5.0, 100.0)]).is_empty());
    assert!(push(&mut e, "aj_t", vec![right(6.0, 200.0)]).is_empty());
    assert_eq!(push(&mut e, "aj_m", vec![mark(10.0, 2.0)]), [row("10.000000", "2", "100", "200")]);
    // equal times: the most recently inserted version wins, and >= is inclusive
    push(&mut e, "aj_i", vec![right(8.0, 101.0)]);
    push(&mut e, "aj_i", vec![right(8.0, 102.0)]);
    assert_eq!(push(&mut e, "aj_m", vec![mark(8.0, 3.0)]), [row("08.000000", "3", "102", "200")]);
    // ORDER BY sorts within the chunk; 00:04.999999 is before every kept version
    assert_eq!(
        push(&mut e, "aj_m", vec![mark(5.0, 4.0), mark(4.999_999, 5.0)]),
        [row("04.999999", "5", "0", "0"), row("05.000000", "4", "100", "0")]
    );
    // only 3 versions per key are kept: 00:05 and one 00:08 are evicted by these
    push(&mut e, "aj_i", vec![right(9.0, 103.0)]);
    push(&mut e, "aj_i", vec![right(20.0, 104.0)]);
    push(&mut e, "aj_i", vec![right(30.0, 105.0)]);
    assert_eq!(
        push(&mut e, "aj_m", vec![mark(8.5, 6.0), mark(9.0, 7.0), mark(25.0, 8.0)]),
        [row("08.500000", "6", "0", "200"), row("09.000000", "7", "103", "200"), row("25.000000", "8", "104", "200")]
    );
    // a late right row older than every kept version is evicted as it is inserted
    push(&mut e, "aj_i", vec![right(1.0, 106.0)]);
    assert_eq!(push(&mut e, "aj_m", vec![mark(2.0, 9.0)]), [row("02.000000", "9", "0", "0")]);
    // two versions with one time in one chunk: the later in the chunk wins
    push(&mut e, "aj_i", vec![right(12.0, 110.0), right(12.0, 111.0)]);
    assert_eq!(push(&mut e, "aj_m", vec![mark(13.0, 10.0)]), [row("13.000000", "10", "111", "200")]);
}

#[test]
fn asof_keys_are_separate_and_a_right_row_never_re_emits_left_rows() {
    let mut e = engine(ASOF);
    push(&mut e, "aj_i", vec![vec![t(1.0), s("B"), f(50.0)]]);
    // the B version is not visible to A
    assert_eq!(push(&mut e, "aj_m", vec![mark(2.0, 1.0)]), [row("02.000000", "1", "0", "0")]);
    assert!(push(&mut e, "aj_i", vec![right(1.0, 60.0)]).is_empty());
    assert_eq!(push(&mut e, "aj_m", vec![mark(2.0, 1.0)]), [row("02.000000", "1", "60", "0")]);
}

/// fixtures/pipelines/quotes.sql's slippage views, verbatim: the ASOF join of trades with
/// quotes, and the view computing slippage from it, into a sink with the enriched stream's columns.
fn slippage() -> Engine {
    let sql =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/pipelines/quotes.sql")).unwrap();
    let stmt = |start: &str| -> String {
        let at = sql.find(start).unwrap_or_else(|| panic!("{start} not in quotes.sql"));
        sql[at..at + sql[at..].find(';').unwrap()].to_string()
    };
    let view = stmt("CREATE MATERIALIZED VIEW IF NOT EXISTS slippage_enriched_mv")
        .replace("INTO slippage_enriched AS", "INTO sink AS");
    let sql = format!(
        "{};\n{};\n{};\n{};\nCREATE EXTERNAL STREAM IF NOT EXISTS sink (local_event_time datetime64(6), exchange int64, symbol string, side string, size_value float64, slippage_value float64, slippage_bps_value float64) SETTINGS type = 'kafka', topic = 's', data_format = 'JSONEachRow';\n{view};",
        stmt("CREATE STREAM IF NOT EXISTS slippage_quotes"),
        stmt("CREATE STREAM IF NOT EXISTS slippage_trades"),
        stmt("CREATE STREAM IF NOT EXISTS slippage_joined"),
        stmt("CREATE MATERIALIZED VIEW IF NOT EXISTS slippage_joined_mv"),
    );
    engine(&sql)
}

fn trade(time: f64, price: f64, side: &str) -> Vec<Value> {
    vec![t(time), Value::Int(1), s("BTC"), s(side), Value::Int(7), f(price), f(2.0)]
}

fn quote(time: f64, ask: f64, bid: f64) -> Vec<Value> {
    vec![t(time), s("BTC"), f(bid), f(ask)]
}

#[test]
fn slippage_before_any_quote_is_dropped() {
    // join_use_nulls = 0: the join gives ask/bid 0.0, which null_if turns into NULL, and the
    // `IS NOT NULL` filter drops the trade instead of emitting a slippage against a price of 0
    let mut e = slippage();
    let out = push(&mut e, "slippage_trades", vec![trade(10.0, 50.0, "buy"), trade(10.0, 50.0, "sell")]);
    assert_eq!(out, Vec::<String>::new());
    push(&mut e, "slippage_quotes", vec![quote(10.0, 49.0, 48.0)]);
    let out = push(&mut e, "slippage_trades", vec![trade(10.0, 50.0, "buy")]);
    assert_eq!(
        out,
        [
            r#"{"local_event_time":"1970-01-01 00:00:10.000000","exchange":1,"symbol":"BTC","side":"buy","size_value":2,"slippage_value":1,"slippage_bps_value":206.18556701030928}"#
        ]
    );
}

#[test]
fn slippage_uses_the_quote_inserted_last_among_equal_times() {
    let mut e = slippage();
    push(&mut e, "slippage_quotes", vec![quote(5.0, 49.0, 48.0), quote(5.0, 47.5, 47.0)]);
    let out = push(&mut e, "slippage_trades", vec![trade(11.0, 50.0, "buy"), trade(11.0, 47.5, "sell")]);
    // the chunk is sorted by (symbol, local_event_time) only: equal keys keep their order
    assert_eq!(
        out,
        [
            r#"{"local_event_time":"1970-01-01 00:00:11.000000","exchange":1,"symbol":"BTC","side":"buy","size_value":2,"slippage_value":2.5,"slippage_bps_value":529.1005291005291}"#,
            r#"{"local_event_time":"1970-01-01 00:00:11.000000","exchange":1,"symbol":"BTC","side":"sell","size_value":2,"slippage_value":-0.5,"slippage_bps_value":-105.82010582010581}"#,
        ]
    );
}

fn pipelines() -> Vec<(String, String)> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/pipelines");
    let mut files: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
    files.retain(|p| p.extension().is_some_and(|x| x == "sql"));
    files.sort();
    files
        .into_iter()
        .map(|p| (p.file_name().unwrap().to_string_lossy().into_owned(), std::fs::read_to_string(&p).unwrap()))
        .collect()
}

#[test]
fn every_pipeline_compiles_into_an_engine_that_skips_only_views_into_s3() {
    let (mut planned, mut skipped) = (0, 0);
    for (file, sql) in pipelines() {
        let cat = parse(&sql).unwrap_or_else(|e| panic!("{file}: {e}"));
        let e = Engine::new(&cat).unwrap_or_else(|e| panic!("{file}: {e}"));
        let s3 = cat
            .views
            .iter()
            .filter(|v| matches!(cat.streams[&v.target].kind, brrrrr_core::sql::Kind::Table(_)))
            .count();
        assert_eq!(e.skipped.len(), s3, "{file}: only views into S3 tables are skipped");
        planned += cat.views.len() - s3;
        skipped += s3;
    }
    assert_eq!((planned, skipped), (58, 1), "bars.sql's Parquet export is the one view into S3");
}

#[test]
fn unsupported_sql_is_rejected_at_plan_time_with_the_view_name() {
    let base = "CREATE STREAM IF NOT EXISTS s (t datetime64(6), x float64, k string);
CREATE EXTERNAL STREAM IF NOT EXISTS o (x float64) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';";
    for (view, why) in [
        ("SELECT sum(x) AS x FROM s", "aggregate sum outside of GROUP BY"),
        // `GROUP BY k` without tumble() is a view in ad-hoc SQL, refused as one when parsed (sql.rs)
        ("SELECT x FROM tumble(s, t, 1m)", "tumble() without GROUP BY"),
        ("SELECT max(x) AS x FROM tumble(s, t, 1m) GROUP BY window_start", "EMIT AFTER WINDOW CLOSE"),
        ("SELECT nope FROM s", "unknown column nope"),
        ("SELECT x FROM missing", "unknown stream missing"),
        ("SELECT x FROM s ORDER BY x DESC", "only ascending ORDER BY"),
        ("SELECT a.x FROM s AS a LEFT JOIN s AS b ON a.k = b.k AND a.t >= b.t", "only ASOF LEFT JOIN"),
        ("SELECT a.x FROM s AS a ASOF LEFT JOIN s AS b ON a.k = b.k", "needs one `left.time >= right.time`"),
        ("SELECT quantile(0.5)(x, x) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "expects 1 argument"),
        ("SELECT nope(x) AS x FROM s", "unknown function nope"),
        ("SELECT if(x > 0, x) AS x FROM s", "if expects 3 arguments, got 2"),
        ("SELECT count(DISTINCT x) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "count(DISTINCT x) is uniq_exact(x)"),
        ("SELECT quantile(x)(x) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "aggregate parameter must be a literal"),
        ("SELECT format_datetime(t, 5) AS x FROM s", "format must be a string literal"),
        ("SELECT format_datetime(t, '%H') AS x FROM s", "unsupported format_datetime format"),
        ("SELECT x FROM s WHERE x > 1_000", r#""1_000" is not a number"#),
        ("SELECT x & 2 AS x FROM s", "unsupported operator"),
        ("SELECT s.* FROM s", "unsupported select item"),
        // a wildcard's options were read as `*`, emitting x as it is
        ("SELECT * REPLACE (99 AS x) FROM s", "unsupported select item * REPLACE"),
        ("SELECT * EXCEPT (x) FROM s", "unsupported select item * EXCEPT"),
        ("SELECT x FROM s AS a (y)", "unsupported FROM"),
        ("SELECT x FROM s SAMPLE 0.5", "unsupported FROM"),
        ("SELECT x FROM (SELECT x FROM s) AS a (y)", "unsupported FROM"),
        ("SELECT * FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "unsupported select item in a window"),
        ("SELECT max(x) AS x FROM tumble(s, t, 1m) WHERE x > 0 GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "WHERE/HAVING on a window"),
        ("SELECT max(x) AS x FROM tumble(s, t, 5) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "unsupported tumble width"),
        ("SELECT max(x) AS x FROM tumble(s, t, k) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "unsupported tumble width"),
        ("SELECT max(x) AS x FROM tumble(s, *, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "unsupported argument"),
        ("SELECT max(x) AS x FROM tumble(s, t) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "tumble(stream, time, width) expected"),
        ("SELECT x FROM hop(s, t, 1m, 5m)", "unsupported FROM"),
        ("SELECT a.x FROM tumble(s, t, 1m) AS a ASOF LEFT JOIN s AS b ON a.k = b.k AND a.t >= b.t", "left side of a join"),
        ("SELECT a.x FROM s AS a ASOF LEFT JOIN tumble(s, t, 1m) AS b ON a.k = b.k AND a.t >= b.t", "right side of a join"),
        ("SELECT a.x FROM s AS a INNER JOIN s AS b ON a.k = b.k", "only ASOF LEFT JOIN ... ON"),
        ("SELECT a.x FROM s AS a ASOF LEFT JOIN s AS b ON a.k = b.k AND a.t > b.t", "unsupported ASOF condition"),
        ("SELECT a.x FROM s AS a ASOF LEFT JOIN s AS b ON a.k = b.k AND a.t >= b.t AND a.x >= b.x", "unsupported ASOF condition"),
        ("SELECT a.x FROM s AS a ASOF LEFT JOIN s AS b ON a.k = b.k AND a.t >= b.t SETTINGS keep_versions = 0", "keep_versions = 0"),
        // a refused frame is quoted whole
        ("SELECT avg(x) OVER (ORDER BY t GROUPS 1 PRECEDING) AS x FROM s", "GROUPS frames are not supported: GROUPS 1 PRECEDING"),
        ("SELECT avg(x) OVER (ORDER BY t ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS x FROM s", "later rows): ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"),
        ("SELECT x FROM s UNION ALL SELECT x FROM s", "unsupported query body"),
        ("SELECT a.x FROM s AS a, s AS b", "exactly one FROM relation"),
        ("SELECT max(x) AS x FROM tumble(s, t, '0m') GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "bad duration 0m"),
        ("SELECT max(x) AS x FROM tumble(s, t, '1w') GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "bad duration unit"),
        // clauses sqlparser accepts and the planner used to ignore, silently changing results
        ("SELECT DISTINCT x FROM s", "DISTINCT is not supported"),
        ("SELECT x FROM s CLUSTER BY x", "CLUSTER/DISTRIBUTE/SORT BY is not supported"),
        ("SELECT x FROM s DISTRIBUTE BY x", "CLUSTER/DISTRIBUTE/SORT BY is not supported"),
        ("SELECT x FROM s SORT BY x", "CLUSTER/DISTRIBUTE/SORT BY is not supported"),
        ("SELECT x FROM s LIMIT 1", "LIMIT/OFFSET is not supported"),
        ("SELECT x FROM s PREWHERE x > 0", "PREWHERE is not supported"),
        ("SELECT x FROM s HAVING x > 0", "HAVING is not supported"),
        ("SELECT x FROM tumble(s, t, 1m) GROUP BY ALL", "GROUP BY ALL is not supported"),
        ("SELECT max(x) AS x FROM tumble(s, t, 1m) GROUP BY window_start WITH TOTALS EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "GROUP BY modifiers"),
        ("SELECT x FROM s ORDER BY x NULLS FIRST", "NULLS FIRST/LAST or WITH FILL"),
        ("SELECT x FROM s ORDER BY x WITH FILL", "NULLS FIRST/LAST or WITH FILL"),
        ("SELECT x FROM (SELECT x FROM s LIMIT 5)", "LIMIT/OFFSET is not supported"),
        ("SELECT to_string(1)(x) AS x FROM s", "to_string takes no parameters"),
        ("SELECT sum(2)(x) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "sum takes no parameters"),
        ("SELECT quantile('0.9')(x) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "takes one numeric level, got [Str"),
        ("SELECT quantile(NULL)(x) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "is not a number"),
        ("SELECT quantile(0.1, 0.2)(x) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "takes one numeric level"),
    ] {
        let sql = format!("{base}\nCREATE MATERIALIZED VIEW IF NOT EXISTS bad INTO o AS {view};");
        let cat = parse(&sql).unwrap_or_else(|e| panic!("{view}: {e}"));
        let err = Engine::new(&cat).err().unwrap_or_else(|| panic!("{view} was accepted"));
        assert!(err.starts_with("bad: ") && err.contains(why), "{view}: {err}");
    }
}

#[test]
fn a_header_expression_that_is_not_a_key_value_pair_yields_no_headers() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS src (a string);
CREATE EXTERNAL STREAM IF NOT EXISTS o1 (a string, _tp_message_headers map(string, string) MATERIALIZED cast(a, 'map(string, string)'))
  SETTINGS type = 'kafka', topic = 'o1', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM IF NOT EXISTS o2 (a string, _tp_message_headers map(string, string) MATERIALIZED cast([a], 'map(string, string)'))
  SETTINGS type = 'kafka', topic = 'o2', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM IF NOT EXISTS o3 (a string) SETTINGS type = 'kafka', topic = 'o3', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v1 INTO o1 AS SELECT a FROM src;
CREATE MATERIALIZED VIEW IF NOT EXISTS v2 INTO o2 AS SELECT a FROM src;
CREATE MATERIALIZED VIEW IF NOT EXISTS v3 INTO o3 AS SELECT a FROM src;
",
    );
    let out = push_all(&mut e, "src", vec![vec![s("x")]]);
    assert_eq!(out.iter().map(|m| (&*m.topic, m.headers.len())).collect::<Vec<_>>(), [("o1", 0), ("o2", 0), ("o3", 0)]);
    assert!(out.iter().all(|m| m.payload == "{\"a\":\"x\"}\n"), "{out:?}");
}

/// flow's shape: `CASE ... THEN notional ELSE 0 END` mixes an integer 0 with floats, and a
/// window whose first rows take the ELSE branch must still sum to a Float64.
#[test]
fn sums_of_case_expressions_mixing_integer_zero_and_floats_are_float_sums() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, side string, notional float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (symbol string, buy_volume float64, buy_count int32, sell_volume float64)
  SETTINGS type = 'kafka', topic = 'f', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS flow INTO out AS
WITH base AS (
  SELECT symbol, window_start,
    sum(CASE WHEN side = 'buy' THEN notional ELSE 0 END) AS buy_volume,
    sum(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) AS buy_count,
    sum(CASE WHEN side = 'sell' THEN notional ELSE 0 END) AS sell_volume
  FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol
)
SELECT symbol, buy_volume, to_int32(buy_count) AS buy_count, sell_volume FROM base
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
",
    );
    let trade = |at: f64, side: &str, n: f64| vec![t(at), s("A"), s(side), f(n)];
    push(
        &mut e,
        "trades",
        vec![trade(1.0, "sell", 10.25), trade(2.0, "buy", 0.1), trade(3.0, "buy", 0.2), trade(4.0, "sell", 1.0)],
    );
    let out = push(&mut e, "trades", vec![trade(100.0, "sell", 1.0)]);
    // 0 + 0.1 + 0.2 in Float64 is 0.30000000000000004
    assert_eq!(out, [r#"{"symbol":"A","buy_volume":0.30000000000000004,"buy_count":2,"sell_volume":11.25}"#]);
}

#[test]
fn a_row_exactly_at_the_previous_watermark_is_not_late() {
    // only rows *older* than the watermark are dropped: the window starting at it is still open
    let mut e = engine(WINDOW);
    push(&mut e, "trades", vec![vec![t(130.0), s("A"), f(1.0)]]); // watermark 120 s
    push(&mut e, "trades", vec![vec![t(120.0), s("A"), f(2.0)]]);
    assert_eq!(e.late(), 0);
    let out = push(&mut e, "trades", vec![vec![t(200.0), s("A"), f(3.0)]]);
    assert_eq!(out, [r#"{"symbol":"A","time":120000000,"n":2,"last":2,"mean":1.5}"#]);
}

#[test]
fn inside_its_own_expression_an_alias_means_the_column() {
    // on the fork: SELECT x * 2 AS x, x + 1 AS y, to_int32(z) AS z FROM (SELECT 1.5 AS x, 2.7 AS z)
    // gives {"x":3,"y":4,"z":2}
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS src (x float64, z float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (x float64, y float64, z int32) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS SELECT x * 2 AS x, x + 1 AS y, to_int32(z) AS z FROM src;
",
    );
    assert_eq!(push(&mut e, "src", vec![vec![f(1.5), f(2.7)]]), [r#"{"x":3,"y":4,"z":2}"#]);
}

#[test]
fn a_cte_read_through_an_alias_is_qualified_by_it() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS src (a float64, k string);
CREATE EXTERNAL STREAM IF NOT EXISTS out (k string, a float64) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS
WITH base AS (SELECT k, a * 2 AS a FROM src)
SELECT b.k AS k, b.a AS a FROM base AS b WHERE b.a > 1;
",
    );
    assert_eq!(push(&mut e, "src", vec![vec![f(0.25), s("x")], vec![f(1.5), s("y")]]), [r#"{"k":"y","a":3}"#]);
}

#[test]
fn a_snapshot_of_a_join_with_other_sides_is_refused() {
    // same views and operators, one ASOF side fewer: restoring must not silently drop state
    let two = engine(ASOF);
    let one_side = ASOF
        .replace(
            "ASOF LEFT JOIN (
  SELECT event_time, symbol, last_price_value
  FROM aj_t
  ORDER BY symbol, event_time
) AS t
ON m.symbol = t.symbol AND m.event_time >= t.event_time",
            "",
        )
        .replace("t.last_price_value AS last_price_value", "0 AS last_price_value");
    let mut one = engine(&one_side);
    let err = one.restore(two.snapshot()).unwrap_err();
    assert!(err.contains("does not match"), "{err}");
}

/// An unmatched ASOF row takes the right column's type default: 0 for a float64 column, which
/// `to_string` makes "0" (not the default of some other column of the right side).
#[test]
fn unmatched_asof_columns_default_by_their_own_type() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS l (t datetime64(6), k string);
CREATE STREAM IF NOT EXISTS r (t datetime64(6), k string, v float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (k string, v string) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS j INTO out AS
SELECT a.k AS k, to_string(b.v) AS v
FROM (SELECT t, k FROM l) AS a
ASOF LEFT JOIN (SELECT t, k, v FROM r) AS b ON a.k = b.k AND a.t >= b.t;
",
    );
    assert_eq!(push(&mut e, "l", vec![vec![t(1.0), s("A")]]), [r#"{"k":"A","v":"0"}"#]);
}

/// Timestamps at the i64 bounds (a corrupt feed) saturate instead of wrapping into nonsense
/// windows or panicking, and one far-future row holds the watermark there. A row
/// without a time (NULL, an absent proto field) is dropped and counted: it used to be read as
/// i64::MIN and emitted in a window of year -290308.
#[test]
fn extreme_timestamps_neither_panic_nor_wrap() {
    let mut e = engine(WINDOW);
    let at = |us: i64| Value::Time(us);
    let untimed = vec![Value::Null, s("N"), f(4.0)];
    push(&mut e, "trades", vec![vec![at(i64::MIN), s("A"), f(1.0)], untimed, vec![at(i64::MIN + 1), s("A"), f(2.0)]]);
    let out = push(&mut e, "trades", vec![vec![at(i64::MAX), s("B"), f(3.0)]]);
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].contains(r#""symbol":"A","time":-9223372036854775808,"n":2"#), "{out:?}");
    assert_eq!(e.null_time(), 1);
    assert!(push(&mut e, "trades", vec![vec![t(10.0), s("C"), f(1.0)]]).is_empty(), "far behind the watermark: late");
    assert_eq!((e.late(), e.null_time()), (1, 1));
}

/// Group keys must not collide: a NULL symbol, the string 'NULL', and strings built to look
/// like two keys joined are separate groups.
#[test]
fn group_keys_are_unambiguous() {
    let mut e = engine(
        "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), a nullable(string), b string, price float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (a nullable(string), b string, n uint64) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO out AS
SELECT a, b, count() AS n FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, a, b
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
",
    );
    let row = |a: Value, b: &str| vec![t(1.0), a, s(b), f(1.0)];
    push(&mut e, "trades", vec![row(Value::Null, "x"), row(s("NULL"), "x"), row(s("1:x"), ""), row(s("1"), "x;")]);
    let out = push(
        &mut e,
        "trades",
        vec![row(s("z"), "z").into_iter().enumerate().map(|(i, v)| if i == 0 { t(100.0) } else { v }).collect()],
    );
    assert_eq!(out.len(), 4, "{out:?}");
    assert!(out.iter().all(|m| m.contains(r#""n":1"#)), "{out:?}");
}

/// One row far in the future used to move the watermark there for good (and into every
/// checkpoint): every later row was late and no window closed again. With a time limit such a
/// row is dropped and counted instead.
#[test]
fn a_row_past_the_time_limit_is_dropped_instead_of_moving_the_watermark() {
    let year_3000 = t(32_503_680_000.0);
    let rows = |e: &mut Engine| {
        let mut out = push(e, "trades", vec![vec![t(10.0), s("A"), f(1.0)]]);
        out.extend(push(e, "trades", vec![vec![year_3000.clone(), s("A"), f(9.0)]]));
        out.extend(push(e, "trades", vec![vec![t(60.05), s("B"), f(2.0)]]));
        out
    };
    let mut limited = engine(WINDOW);
    limited.set_time_limit(Some(1_000_000_000));
    assert_eq!(rows(&mut limited), [r#"{"symbol":"A","time":0,"n":1,"last":1,"mean":1}"#]);
    assert_eq!((limited.future(), limited.late()), (1, 0));
    // without a limit (the default), the far row takes the watermark along
    let mut unlimited = engine(WINDOW);
    assert_eq!(rows(&mut unlimited).len(), 1, "the far row closes window 0 itself");
    assert_eq!(push(&mut unlimited, "trades", vec![vec![t(70.0), s("C"), f(3.0)]]), Vec::<String>::new());
    assert_eq!((unlimited.future(), unlimited.late()), (0, 2));
    // a limit can be lifted again
    limited.set_time_limit(None);
    assert!(push(&mut limited, "trades", vec![vec![year_3000, s("A"), f(9.0)]]).len() == 1);
}

/// A view inserting into a stream it reads, directly or through other views, would insert every
/// row around the cycle forever (a stack overflow on the first row): refused at plan time.
#[test]
fn views_that_form_a_cycle_are_refused() {
    let streams = "CREATE STREAM IF NOT EXISTS s (x float64);
CREATE STREAM IF NOT EXISTS t (x float64);
CREATE STREAM IF NOT EXISTS u (x float64);";
    for (views, cycle) in [
        ("CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO s AS SELECT x FROM s;", "a -> a"),
        (
            "CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO t AS SELECT x FROM s;
             CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO u AS SELECT x FROM t;
             CREATE MATERIALIZED VIEW IF NOT EXISTS c INTO s AS SELECT x FROM u;",
            "a -> b -> c -> a",
        ),
        (
            "CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO t AS SELECT x FROM s;
             CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO u AS SELECT x FROM t;
             CREATE MATERIALIZED VIEW IF NOT EXISTS c INTO t AS SELECT x FROM u;",
            "b -> c -> b",
        ),
    ] {
        let cat = parse(&format!("{streams}\n{views}")).unwrap();
        let err = Engine::new(&cat).err().unwrap_or_else(|| panic!("{views}: accepted"));
        assert_eq!(err, format!("the views {cycle} form a cycle: a row would be inserted around it forever"));
    }
    // a stream written by two views and read by a third is no cycle
    let diamond = "CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO t AS SELECT x FROM s;
        CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO t AS SELECT x FROM s;
        CREATE MATERIALIZED VIEW IF NOT EXISTS c INTO u AS SELECT x FROM t;
        CREATE MATERIALIZED VIEW IF NOT EXISTS d INTO u AS SELECT x FROM s;";
    let mut e = engine(&format!("{streams}\n{diamond}"));
    assert!(push(&mut e, "s", vec![vec![f(1.0)]]).is_empty());
}

/// A view reading an external stream another view writes would read that Kafka sink back in
/// the Timeplus fork. brrrrr produces to a sink without running its readers, and the runtime consumes only
/// sources, so such a view was planned and never ran: refused at plan time, however it reads it.
#[test]
fn a_view_reading_a_sink_is_refused() {
    let streams = "CREATE STREAM IF NOT EXISTS src (t datetime64(6), symbol string, x float64);
CREATE EXTERNAL STREAM IF NOT EXISTS k (t datetime64(6), symbol string, x float64, exchange int64, is_snapshot bool,
  time int64, venue_sequence int64, bid_price array(float64), bid_amount array(float64), ask_price array(float64),
  ask_amount array(float64)) SETTINGS type = 'kafka', topic = 'k', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM IF NOT EXISTS k2 (x float64) SETTINGS type = 'kafka', topic = 'k2', data_format = 'JSONEachRow';";
    let a = "CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO k AS SELECT t, symbol, x FROM src;";
    let side = |s: &str| format!("(SELECT t, symbol, x FROM {s} ORDER BY symbol, t)");
    let join = |l: &str, r: &str| {
        format!(
            "SELECT l.x AS x FROM {} AS l ASOF LEFT JOIN {} AS r ON l.symbol = r.symbol AND l.t >= r.t",
            side(l),
            side(r)
        )
    };
    for from in [
        "SELECT x FROM k".to_string(),
        "SELECT x FROM (SELECT x * 2 AS x FROM k) AS q".into(),
        "WITH q AS (SELECT x FROM k) SELECT x FROM q".into(),
        join("k", "src"),
        join("src", "k"),
        "SELECT max(x) AS x FROM tumble(k, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE".into(),
        "SELECT to_float64(time) AS x FROM orderbook_top_n(k, 5)".into(),
    ] {
        let b = format!("CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO k2 AS {from};");
        // whichever of the two is declared first
        for views in [format!("{a}\n{b}"), format!("{b}\n{a}")] {
            let cat = parse(&format!("{streams}\n{views}")).unwrap();
            let err = Engine::new(&cat).err().unwrap_or_else(|| panic!("{from}: accepted"));
            assert_eq!(
                err,
                "b: reads k, which the view a writes; brrrrr does not read its own sinks back: \
                 read from an internal stream instead"
            );
        }
    }
    // a view reading its own sink
    let c = "CREATE MATERIALIZED VIEW IF NOT EXISTS c INTO k2 AS SELECT x FROM src;";
    let own = "CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO k AS SELECT t, symbol, x + 1 AS x FROM k;";
    let err = Engine::new(&parse(&format!("{streams}\n{own}\n{c}")).unwrap()).err().unwrap();
    assert!(err.starts_with("a: reads k, which the view a writes;"), "{err}");
    // the same views over an internal stream run, and a sink no view reads is produced to
    let internal = streams.replacen("EXTERNAL STREAM IF NOT EXISTS k ", "STREAM IF NOT EXISTS k ", 1);
    let internal = internal.replacen(" SETTINGS type = 'kafka', topic = 'k', data_format = 'JSONEachRow'", "", 1);
    let b = "CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO k2 AS SELECT x * 2 AS x FROM k;";
    let mut e = engine(&format!("{internal}\n{a}\n{b}"));
    assert_eq!(push(&mut e, "src", vec![vec![t(1.0), s("A"), f(1.5)]]), [r#"{"x":3}"#]);
    let mut e = engine(&format!("{streams}\n{a}\n{c}"));
    let topics: Vec<String> =
        push_all(&mut e, "src", vec![vec![t(1.0), s("A"), f(1.5)]]).into_iter().map(|m| m.topic.to_string()).collect();
    assert_eq!(topics, ["k", "k2"]);
}

/// No pipeline in the repository reads a sink: every one still plans.
#[test]
fn every_pipeline_in_the_repository_plans() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../");
    let dirs = ["fixtures/pipelines"];
    let mut files: Vec<std::path::PathBuf> =
        dirs.iter().flat_map(|d| std::fs::read_dir(format!("{root}{d}")).unwrap()).map(|e| e.unwrap().path()).collect();
    // the acceptance suite's broken.sql and unsupported.sql are refused on purpose
    let acceptance = ["orderbook", "sorted", "two_intervals", "windows"];
    files.extend(acceptance.iter().map(|f| format!("{root}tests/acceptance/sql/{f}.sql").into()));
    let mut planned = 0;
    for p in files.iter().filter(|p| p.extension().is_some_and(|e| e == "sql")) {
        let sql = std::fs::read_to_string(p).unwrap();
        Engine::new(&parse(&sql).unwrap()).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
        planned += 1;
    }
    assert_eq!(planned, 12);
}

/// The settings of streams: known keys only, with values brrrrr implements. A sink declared
/// Avro, or without a topic, used to be run as a JSONEachRow sink to the topic "".
#[test]
fn stream_settings_outside_what_brrrrr_implements_are_refused() {
    let source =
        "type = 'kafka', topic = 'raw', data_format = 'ProtobufSingle', format_schema = 'raw:T', seek_to = 'earliest'";
    let sink = "type = 'kafka', topic = 'o', data_format = 'JSONEachRow', one_message_per_row = true, properties = 'enable.idempotence=false'";
    let sql = |source: &str, sink: &str, internal: &str| {
        format!(
            "CREATE EXTERNAL STREAM IF NOT EXISTS src (x float64) SETTINGS {source};
CREATE STREAM IF NOT EXISTS mid (x float64) {internal};
CREATE EXTERNAL STREAM IF NOT EXISTS o (x float64) SETTINGS {sink};
CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO mid AS SELECT x FROM src;
CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO o AS SELECT x FROM mid;"
        )
    };
    let storage = "SETTINGS logstore_retention_ms = 600000, logstore_codec = 'lz4', merge_max_block_size = 4096";
    assert!(Engine::new(&parse(&sql(source, sink, storage)).unwrap()).is_ok());
    let json = "type = 'kafka', topic = 'raw', data_format = 'JSONEachRow'";
    assert!(Engine::new(&parse(&sql(json, sink, storage)).unwrap()).is_ok());
    let (so, si) = (source.to_string(), sink.to_string());
    for (source, sink, internal, why) in [
        (so.clone(), sink.replace("JSONEachRow", "Avro"), "", "o: a sink's data_format must be JSONEachRow"),
        (so.clone(), sink.replace("topic = 'o', ", ""), "", "o: no topic"),
        (
            so.clone(),
            sink.replace("type = 'kafka'", "type = 'pulsar'"),
            "",
            "o: an external stream must be type = 'kafka'",
        ),
        (
            so.clone(),
            sink.replace("one_message_per_row = true", "one_message_per_row = false"),
            "",
            "o: one_message_per_row must be true",
        ),
        (so.clone(), format!("{sink}, sasl_password = 'x'"), "", "o: unknown setting sasl_password"),
        (
            source.replace(", format_schema = 'raw:T'", ""),
            si.clone(),
            "",
            "src: a source must be data_format = 'JSONEachRow', or 'ProtobufSingle' with a format_schema",
        ),
        (
            source.replace("ProtobufSingle", "Avro"),
            si.clone(),
            "",
            "src: a source must be data_format = 'JSONEachRow', or 'ProtobufSingle'",
        ),
        (source.replace("earliest", "tomorrow"), si.clone(), "", "src: seek_to must be earliest or latest"),
        (so.clone(), si.clone(), "SETTINGS mode = 'versioned_kv'", "mid: unknown setting mode"),
    ] {
        let err = Engine::new(&parse(&sql(&source, &sink, internal)).unwrap())
            .err()
            .unwrap_or_else(|| panic!("{why}: accepted"));
        assert!(err.starts_with(why), "{err} is not {why}");
    }
}

/// Parquet file streams (`type = 'file'`): a source if read, a sink if written, with the settings
/// each takes; a pipeline reads Kafka or files, several file sources merged by a time column.
#[test]
fn file_stream_settings_are_checked() {
    let file = |name: &str, cols: &str, settings: &str| {
        format!("CREATE EXTERNAL STREAM {name} ({cols}) SETTINGS type = 'file', data_format = 'Parquet', {settings};")
    };
    let kafka = "CREATE EXTERNAL STREAM k (t datetime64(3), x float64) SETTINGS type = 'kafka', topic = 'k', data_format = 'JSONEachRow';";
    let view = |from: &str| format!("CREATE MATERIALIZED VIEW v INTO o AS SELECT t, x, 'a' AS day FROM {from};");
    let sink = |settings: &str| file("o", "t datetime64(3), x float64, day string", settings);
    let src = |settings: &str| file("s", "t datetime64(3), x float64", settings);
    let ok = |sql: String| {
        let e = Engine::new(&parse(&sql).unwrap());
        assert!(e.is_ok(), "{sql}: {:?}", e.err());
    };
    ok(format!("{}\n{}\n{}", src("path = 'in/*.parquet'"), sink("path = 'out/'"), view("s")));
    ok(format!(
        "{}\n{}\n{}",
        src("path = 's3://b/in', time_column = 't'"),
        sink("path = 'out', partition_by = 'day'"),
        view("s")
    ));
    ok(format!("{kafka}\n{}\n{}", sink("path = 'out', partition_by = ' day , x '"), view("k")));
    // several file sources, each with its time column
    let two = format!(
        "{}\n{}\n{}\nCREATE MATERIALIZED VIEW w INTO o AS SELECT t, x, 'b' AS day FROM s2;",
        src("path = 'a', time_column = 't'"),
        file("s2", "t int64, x float64", "path = 'b', time_column = 't'"),
        sink("path = 'out'")
    );
    ok(format!("{two}\n{}", view("s")));
    for (sql, why) in [
        (
            format!("{}\n{}\n{}", src("path = 'in'"), sink("path = 'o', topic = 'x'"), view("s")),
            "o: unknown setting topic",
        ),
        (format!("{}\n{}\n{}", src("path = ''"), sink("path = 'o'"), view("s")), "s: no path"),
        (
            format!("{}\n{}\n{}", src("path = 'in'"), sink("path = 'o/*.parquet'"), view("s")),
            "o: a sink's path is a directory, not a pattern",
        ),
        (
            format!("{}\n{}\n{}", src("path = 'in'"), sink("path = 'o'").replace("'Parquet'", "'CSV'"), view("s")),
            "o: a file stream's data_format must be 'Parquet', not \"CSV\"",
        ),
        (
            format!(
                "{}\n{}\n{}",
                src("path = 'in'").replace(", data_format = 'Parquet'", ""),
                sink("path = 'o'"),
                view("s")
            ),
            "s: a file stream's data_format must be 'Parquet', not \"\"",
        ),
        (
            format!("{}\n{}\n{}", src("path = 'in', partition_by = 'x'"), sink("path = 'o'"), view("s")),
            "s: partition_by is a sink's setting",
        ),
        (
            format!("{}\n{}\n{}", src("path = 'in'"), sink("path = 'o', time_column = 't'"), view("s")),
            "o: time_column is a source's setting",
        ),
        (
            format!("{}\n{}\n{}", src("path = 'in'"), sink("path = 'o', partition_by = 'day, sym'"), view("s")),
            "o: partition_by sym: no column sym",
        ),
        (
            format!("{}\n{}\n{}", src("path = 'in'"), sink("path = 'o', partition_by = 'day,x,t'"), view("s")),
            "o: partition_by every column: no column is left",
        ),
        (
            format!("{}\n{}\n{}", src("path = 'in', time_column = 'y'"), sink("path = 'o'"), view("s")),
            "s: time_column y: no column y",
        ),
        (
            format!("{}\n{}\n{}", src("path = 'in', time_column = 'x'"), sink("path = 'o'"), view("s")),
            "s: time_column x is F64, not a time",
        ),
        (
            format!(
                "{}\n{}\n{}",
                file("s", "t datetime64(3), x float64, a array(int64)", "path = 'in'"),
                sink("path = 'o'"),
                view("s")
            ),
            "s: a: a file stream's columns are numbers, text, times or booleans",
        ),
        (
            format!("{kafka}\n{}\n{}\n{}", src("path = 'in'"), sink("path = 'o'"), view("s")),
            "s: a pipeline reads Kafka topics or files, not both (k reads a topic)",
        ),
        (
            format!("{two}\n{}", view("s")).replace(", time_column = 't'", ""),
            "s: several file sources are read merged in time order",
        ),
    ] {
        let err = Engine::new(&parse(&sql).unwrap()).err().unwrap_or_else(|| panic!("{why}: accepted: {sql}"));
        assert!(err.starts_with(why), "{err} is not {why}");
    }
}

/// Types that are certain (literals, typed columns) are checked at plan time, as the Timeplus
/// fork checks them at CREATE: arithmetic on a string, a numeric aggregate of one, or one as a condition
/// used to run and give 0, NULL or false for every row.
#[test]
fn expressions_of_certain_wrong_types_are_refused() {
    let base = "CREATE STREAM IF NOT EXISTS s (t datetime64(6), x float64, n int64, k string);
CREATE STREAM IF NOT EXISTS o (x float64);";
    for (view, why) in [
        ("SELECT k * 2 AS x FROM s", "arithmetic on a string: k * 2"),
        ("SELECT x + 'a' AS x FROM s", "arithmetic on a string"),
        ("SELECT x FROM s WHERE k", "a string is not a condition"),
        ("SELECT x FROM s WHERE x > 0 AND k", "a string is not a condition"),
        ("SELECT x FROM s WHERE NOT k", "a string is not a condition"),
        ("SELECT x FROM s WHERE k = x", "compares a string with a number: k = x"),
        // qualified and parenthesised columns, and signed numbers, have their certain types too
        ("SELECT a.k * 2 AS x FROM s AS a", "arithmetic on a string: a.k * 2"),
        ("SELECT (k) * 2 AS x FROM s", "arithmetic on a string"),
        // three parts name no column (not `a.k`): unsupported, not typed as a string
        ("SELECT a.k.z * 2 AS x FROM s AS a", "unsupported expression"),
        ("SELECT x FROM s WHERE k = 5", "compares a string with a number: k = 5"),
        ("SELECT x FROM s WHERE k = -5", "compares a string with a number: k = -5"),
        ("SELECT x FROM s WHERE x = 'abc'", "'abc' is not a F64 to compare with"),
        ("SELECT x FROM s WHERE t > '2025-13-01'", "is not a Time(6) to compare with"),
        ("SELECT sum(k) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "sum of a string: sum(k)"),
        ("SELECT quantile(0.9)(k) AS x FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND", "quantile of a string"),
    ] {
        let sql = format!("{base}\nCREATE MATERIALIZED VIEW IF NOT EXISTS bad INTO o AS {view};");
        let err = Engine::new(&parse(&sql).unwrap()).err().unwrap_or_else(|| panic!("{view} was accepted"));
        assert!(err.contains(why), "{view}: {err}");
    }
    // what is not certain, or is fine, still compiles: min/max/latest/count of strings, string
    // comparisons, string functions, and aliases (of unknown type here)
    for view in [
        "SELECT max(k) AS k, count(k) AS n FROM tumble(s, t, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND",
        "SELECT x FROM s WHERE k = 'BTC' AND k != 'ETH'",
        "SELECT length(k) + 1 AS x FROM s",
        "SELECT k AS x FROM s WHERE x = 'a'",
    ] {
        let sql = format!("{base}\nCREATE MATERIALIZED VIEW IF NOT EXISTS ok INTO o AS {view};");
        Engine::new(&parse(&sql).unwrap()).unwrap_or_else(|e| panic!("{view}: {e}"));
    }
}

/// A string literal compared with a number or datetime is read as one, as ClickHouse converts a
/// constant string compared with another type. It used to compare as unordered: always false.
#[test]
fn a_string_literal_compared_with_a_number_or_datetime_is_read_as_one() {
    let mut e = engine(
        "CREATE STREAM IF NOT EXISTS s (t datetime64(6), x float64, n int64);
CREATE EXTERNAL STREAM IF NOT EXISTS o (x float64) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO o AS
SELECT x FROM s WHERE x >= '1.5' AND n = '7' AND (t) < '2025-09-01 00:00:01' AND '2' < x",
    );
    let row = |x: f64, n: i64, t: i64| vec![Value::Time(t), f(x), Value::Int(n)];
    let at = brrrrr_core::value::parse_datetime("2025-09-01").unwrap();
    let out = push(&mut e, "s", vec![row(2.5, 7, at), row(1.0, 7, at), row(2.5, 8, at), row(2.5, 7, at + 1_000_000)]);
    assert_eq!(out, [r#"{"x":2.5}"#]);
}

/// An ASOF right side keeps at most a bound of keys: past it, the least recently updated are
/// evicted (their left rows get the defaults, as before any version). Without the bound, every
/// key ever seen (a corrupt feed's garbage symbols) stayed in memory and every checkpoint.
#[test]
fn asof_right_sides_keep_a_bounded_number_of_keys() {
    let mut e = engine(ASOF);
    assert_eq!(e.asof_keys(), (0, 0));
    e.set_asof_max_keys(10);
    // 25 symbols on the index side, one second apart
    for i in 0..25 {
        push(&mut e, "aj_i", vec![vec![t(i as f64), s(&format!("S{i:02}")), f(i as f64)]]);
    }
    let (keys, evicted) = e.asof_keys();
    assert!(keys <= 10 && evicted == 25 - keys as u64, "{keys} keys, {evicted} evicted");
    let mark = |sym: &str| vec![t(100.0), t(100.0), s("x"), s(sym), f(1.0)];
    let out = push(&mut e, "aj_m", vec![mark("S24"), mark("S00")]);
    let row = |sym: &str| out.iter().find(|r| r.contains(&format!(r#""symbol":"{sym}""#))).unwrap().clone();
    assert!(row("S24").contains(r#""index_price_value":24"#), "the newest key is kept: {}", row("S24"));
    assert!(row("S00").contains(r#""index_price_value":0"#), "the stalest got the default: {}", row("S00"));
    // past a bound of 100, exactly the stalest tenth goes: S000..S009, and S010 stays
    let mut e = engine(ASOF);
    e.set_asof_max_keys(100);
    for i in 0..101 {
        push(&mut e, "aj_i", vec![vec![t(i as f64), s(&format!("S{i:03}")), f(i as f64 + 1.0)]]);
    }
    assert_eq!(e.asof_keys(), (91, 10));
    let out = push(&mut e, "aj_m", vec![mark("S009"), mark("S010")]);
    let row = |sym: &str| out.iter().find(|r| r.contains(&format!(r#""symbol":"{sym}""#))).unwrap().clone();
    assert!(row("S009").contains(r#""index_price_value":0"#), "evicted: {}", row("S009"));
    assert!(row("S010").contains(r#""index_price_value":11"#), "kept: {}", row("S010"));
    // the default bound leaves a large market's key set alone
    let mut e = engine(ASOF);
    for i in 0..2_000 {
        push(&mut e, "aj_i", vec![vec![t(i as f64), s(&format!("S{i}")), f(1.0)]]);
    }
    assert_eq!(e.asof_keys(), (2_000, 0));
}

/// A stream that goes quiet leaves its last window open until the next row.
/// close_until closes what a row at a given time would, without one, and inserts the output
/// downstream like a row's.
#[test]
fn close_until_closes_what_a_later_row_would_without_one() {
    let mut e = engine(WINDOW);
    assert!(push(&mut e, "trades", vec![vec![t(10.0), s("A"), f(1.0)], vec![t(70.0), s("B"), f(2.0)]]).len() == 1);
    let mut out = vec![];
    e.close_until(t(119.0).i64().unwrap(), &mut out);
    assert!(out.is_empty(), "60..120 is open until 120.05");
    e.close_until(t(120.05).i64().unwrap(), &mut out);
    assert_eq!(
        out.iter().map(|m| m.payload.trim_end()).collect::<Vec<_>>(),
        [r#"{"symbol":"B","time":60000000,"n":1,"last":2,"mean":2}"#]
    );
    // the watermark moved: an older row is now late, and closing again emits nothing
    assert!(push(&mut e, "trades", vec![vec![t(100.0), s("C"), f(3.0)]]).is_empty());
    assert_eq!(e.late(), 1);
    out.clear();
    e.close_until(t(50.0).i64().unwrap(), &mut out);
    assert!(out.is_empty(), "never back");
    // an engine without windows has nothing to close
    let mut plain = engine("CREATE STREAM IF NOT EXISTS s (x float64);");
    plain.close_until(i64::MAX, &mut out);
    assert!(out.is_empty());
}

/// One view with several quantiles of one argument (slippage's median and p95), and views with
/// one quantile each: the first samples each argument once, the others once per quantile.
const QUANTILES: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
CREATE EXTERNAL STREAM IF NOT EXISTS out (symbol string, time int64, a float64, b float64, c float64, d float32, e float32, g float64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'm.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE EXTERNAL STREAM IF NOT EXISTS one (symbol string, time int64, a float64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'one.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE EXTERNAL STREAM IF NOT EXISTS onef (symbol string, time int64, a float32)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'onef.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS all_levels INTO out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time,
  quantile(0.5)(price) AS a, quantile(0.95)(price) AS b, median(price) AS c,
  median_tdigest(price) AS d, quantile_t_digest(0.9)(price) AS e,
  quantile(0.95)(price * 2) AS g
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS q50 INTO one AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, quantile(0.5)(price) AS a
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS q95 INTO one AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, quantile(0.95)(price) AS a
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS t50 INTO onef AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, quantile_t_digest(0.5)(price) AS a
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS t90 INTO onef AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, quantile_t_digest(0.9)(price) AS a
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS g95 INTO one AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, quantile(0.95)(price * 2) AS a
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '0' SECOND;
";

/// 30,000 prices in the first minute, past the reservoir's 8,192 samples and several t-digest
/// compressions, some NULL; the window closes at 60 s.
fn many_prices(from: usize, to: usize) -> Vec<Vec<Value>> {
    (from..to)
        .map(|i| {
            let price = if i % 97 == 0 { Value::Null } else { f(((i * 7919) % 10_007) as f64 / 3.0) };
            vec![t(i as f64 / 1_000.0), s("A"), price]
        })
        .collect()
}

/// The accumulators of the first group of `view`'s window, in a snapshot as JSON.
fn first_group(state: &mut serde_json::Value, view: usize) -> &mut Vec<serde_json::Value> {
    let ops = state[view].as_array_mut().unwrap();
    let window = ops.iter_mut().find(|op| op.get("Window").is_some()).unwrap();
    window["Window"]["open"][0][1][1].as_array_mut().unwrap()
}

fn quantile_results(out: &[Emit]) -> (serde_json::Value, Vec<serde_json::Value>) {
    let json = |m: &Emit| serde_json::from_str::<serde_json::Value>(&m.payload).unwrap();
    let all = out.iter().find(|m| &*m.topic == "m.1m").map(json).unwrap();
    let one = out.iter().filter(|m| m.topic.starts_with("one")).map(|m| json(m)["a"].clone()).collect();
    (all, one)
}

/// Samplers with one seed fed the same values hold the same samples, so quantiles of one argument
/// can read one sampler: the results are exactly those of one sampler per quantile, at
/// a fraction of the state. The quantile reservoirs were 98% of slippage's checkpoints, two
/// identical ones per group (median and p95).
#[test]
fn quantiles_of_one_argument_read_one_sampler_with_the_results_of_one_each() {
    let mut e = engine(QUANTILES);
    e.insert("trades", many_prices(0, 30_000), &mut vec![]);
    // all_levels keeps three samplers: price's reservoir and t-digest, and price * 2's reservoir
    let mut snapshot = serde_json::to_value(e.snapshot()).unwrap();
    let mut bytes = |view: usize| {
        let accs: Vec<Acc> =
            serde_json::from_value(serde_json::Value::Array(first_group(&mut snapshot, view).clone())).unwrap();
        postcard::to_allocvec(&accs).unwrap().len()
    };
    let (shared, q50, t50, g95) = (bytes(0), bytes(1), bytes(3), bytes(5));
    assert!(q50 > 8 * 8_192 && t50 > 1_000, "full samplers");
    assert!(
        shared <= q50 + t50 + g95 + 3 * 16,
        "the shared view keeps {shared} bytes, its three samplers {}",
        q50 + t50 + g95
    );
    let mut out = vec![];
    e.insert("trades", vec![vec![t(60.0), s("A"), f(1.0)]], &mut out);
    let (all, one) = quantile_results(&out);
    assert_eq!(one.len(), 5);
    assert_eq!([&all["a"], &all["b"], &all["d"], &all["e"], &all["g"]], [&one[0], &one[1], &one[2], &one[3], &one[4]]);
    assert_eq!(all["c"], all["a"], "median is quantile(0.5)");
    assert_ne!(all["a"], all["b"]);
    assert_ne!(all["d"], all["e"]);
}

/// A state with its own sampler where the plan reads another's (what format version 2 kept) is
/// refused: version 4 restores only what it writes (ADR-0008).
#[test]
fn a_state_with_one_sampler_per_quantile_is_refused() {
    let mut e = engine(QUANTILES);
    e.insert("trades", many_prices(0, 20_000), &mut vec![]);
    let mut state = serde_json::to_value(e.snapshot()).unwrap();
    let accs = first_group(&mut state, 0);
    let mut copied = 0;
    for i in 0..accs.len() {
        if let Some(of) = accs[i].get("QuantileOf").cloned() {
            let mut full = accs[of["of"].as_u64().unwrap() as usize].clone();
            let kind = if full.get("Quantile").is_some() { "Quantile" } else { "TDigest" };
            full[kind]["level"] = of["level"].clone();
            accs[i] = full;
            copied += 1;
        }
    }
    assert_eq!(copied, 3, "b and c read a's reservoir, e reads d's t-digest");
    let err = engine(QUANTILES).restore(serde_json::from_value(state).unwrap()).unwrap_err();
    assert!(err.contains("an accumulator"), "{err}");
}

// A start without a checkpoint replays whatever its sources still hold. A window that began
// before their oldest record is missing part of its input, and a window the sinks no longer
// hold cannot be told apart from one never written: republished, it reaches consumers hours
// late, the oldest ones partial. `withhold` closes such windows without emitting them.

#[test]
fn withheld_windows_close_without_being_emitted() {
    let mut e = engine(WINDOW);
    // windows that began before 70 s
    e.withhold(70_000_000);
    let rows = |at: &[f64]| at.iter().map(|&x| vec![t(x), s("A"), f(x)]).collect::<Vec<_>>();
    // [0, 60) and [60, 120) began before 70 s; [120, 180) is published
    let out = push(&mut e, "trades", rows(&[10.0, 70.0, 130.0, 190.0]));
    assert_eq!(out, [r#"{"symbol":"A","time":120000000,"n":1,"last":130,"mean":130}"#]);
    assert_eq!(e.withheld(), 2);
    // the watermark moved on as if they had been emitted: a late row is still late
    assert!(push(&mut e, "trades", rows(&[65.0])).is_empty());
    assert_eq!(e.late(), 1);
}

#[test]
fn withholding_nothing_emits_every_window() {
    let mut e = engine(WINDOW);
    e.withhold(i64::MIN);
    let out = push(&mut e, "trades", vec![vec![t(10.0), s("A"), f(1.0)], vec![t(70.0), s("A"), f(2.0)]]);
    assert_eq!(out, [r#"{"symbol":"A","time":0,"n":1,"last":1,"mean":1}"#]);
    assert_eq!(e.withheld(), 0);
}

const CHAINED: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
CREATE STREAM IF NOT EXISTS bars (bar_time datetime64(6), symbol string, n uint64);
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1m INTO bars AS
SELECT window_start AS bar_time, symbol, count() AS n
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE EXTERNAL STREAM IF NOT EXISTS out (symbol string, time int64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'm.5m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_5m INTO out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, sum(n) AS n
FROM tumble(bars, bar_time, 5m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

#[test]
fn only_the_windows_that_reach_the_sinks_are_withheld() {
    // withholding the 1m windows 5m windows are built of would leave those partial: the 1m
    // view feeds a window, so its windows are all emitted, and only the 5m view withholds
    let mut e = engine(CHAINED);
    e.withhold(120_000_000);
    let rows = (0..1000).step_by(30).map(|x| vec![t(x as f64), s("A"), f(1.0)]).collect();
    let out = push(&mut e, "trades", rows);
    // [0, 300) began before 120 s; [300, 600) holds its ten trades (the bar at 900 s closes it;
    // [600, 900) is still open)
    assert_eq!(out, [r#"{"symbol":"A","time":300000000,"n":10}"#]);
    assert_eq!(e.withheld(), 1);
}

/// CHAINED with the 1m bars reaching the sink through a projection instead of a 5m window.
const PROJECTED: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
CREATE STREAM IF NOT EXISTS bars (bar_time datetime64(6), symbol string, n uint64);
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1m INTO bars AS
SELECT window_start AS bar_time, symbol, count() AS n
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE EXTERNAL STREAM IF NOT EXISTS out (symbol string, time int64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'm.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_out INTO out AS
SELECT symbol, to_unix_timestamp64_micro(bar_time) AS time, n FROM bars;
";

#[test]
fn a_window_reaching_the_sinks_through_a_view_without_a_window_is_withheld() {
    // no window reads the 1m bars, only a projection: they are what reaches the sink
    let mut e = engine(PROJECTED);
    e.withhold(120_000_000);
    let rows = (0..240).step_by(30).map(|x| vec![t(x as f64), s("A"), f(1.0)]).collect();
    // [0, 60) and [60, 120) began before 120 s; [120, 180) is emitted ([180, 240) is still open)
    assert_eq!(push(&mut e, "trades", rows), [r#"{"symbol":"A","time":120000000,"n":2}"#]);
    assert_eq!(e.withheld(), 2);
}

// A start without a checkpoint withholds what a sink
// topic can no longer vouch for, the windows that ended before its oldest record, by each
// message's `window_end`: the end of the window it holds. How far the pipeline has read says
// nothing of it: a source behind the others closes old windows.
const TWO_SOURCES: &str = "
CREATE STREAM IF NOT EXISTS fast (local_event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS slow (local_event_time datetime64(6), symbol string, price float64);
CREATE EXTERNAL STREAM IF NOT EXISTS fast_out (symbol string, time int64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'fast', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE EXTERNAL STREAM IF NOT EXISTS slow_out (symbol string, time int64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'slow', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS fast_1m INTO fast_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS n
FROM tumble(fast, local_event_time, 1m) GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS slow_1m INTO slow_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS n
FROM tumble(slow, local_event_time, 1m) GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

#[test]
fn a_message_carries_its_own_windows_end_however_far_another_source_has_read() {
    let mut e = engine(TWO_SOURCES);
    let row = |at: i64| vec![t(at as f64), s("A"), f(1.0)];
    assert!(push_all(&mut e, "slow", vec![row(1)]).is_empty());
    for m in 0..=120 {
        push_all(&mut e, "fast", vec![row(m * 60)]);
    }
    // the pipeline has read 2 hours on; the slow source closes its first minute only now
    assert_eq!(e.max_event_time(), Some(7_200_000_000));
    let slow = push_all(&mut e, "slow", vec![row(61)]);
    assert_eq!(slow.len(), 1);
    assert_eq!((&*slow[0].topic, slow[0].payload.trim_end()), ("slow", r#"{"symbol":"A","time":0,"n":1}"#));
    assert_eq!(slow[0].window_end, 60_000_000, "not 7,200 s: a sink bounded at 3,600 s withholds it");
}

/// A common shape (fixtures/pipelines/bars.sql): every interval's window into one stream, a view per interval writing its
/// own sink from it. Each message carries its own window's end, its interval's.
const SHARED: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS agg (symbol string, interval string, time int64, n uint64);
CREATE EXTERNAL STREAM IF NOT EXISTS out_1m (symbol string, interval string, time int64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'm.1m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE EXTERNAL STREAM IF NOT EXISTS out_5m (symbol string, interval string, time int64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'm.5m', data_format = 'JSONEachRow', one_message_per_row = true;
CREATE MATERIALIZED VIEW IF NOT EXISTS w_1m INTO agg AS
SELECT symbol, '1m' AS interval, to_unix_timestamp64_micro(window_start) AS time, count() AS n
FROM tumble(trades, local_event_time, 1m) GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS w_5m INTO agg AS
SELECT symbol, '5m' AS interval, to_unix_timestamp64_micro(window_start) AS time, count() AS n
FROM tumble(trades, local_event_time, 5m) GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW IF NOT EXISTS w_1m_out INTO out_1m AS SELECT * FROM agg WHERE interval = '1m';
CREATE MATERIALIZED VIEW IF NOT EXISTS w_5m_out INTO out_5m AS SELECT * FROM agg WHERE interval = '5m';
";

/// The end of the window a message of `SHARED` or `CHAINED` holds: its `time` plus its width.
fn end_of(m: &Emit) -> i64 {
    let v: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
    let width = if m.topic.ends_with("5m") { 300_000_000 } else { 60_000_000 };
    v["time"].as_i64().unwrap() + width
}

#[test]
fn messages_of_one_stream_written_to_several_sinks_carry_their_own_windows_ends() {
    let mut e = engine(SHARED);
    let mut out = vec![];
    for at in (0..=900).step_by(20) {
        out.extend(push_all(&mut e, "trades", vec![vec![t(at as f64), s("A"), f(1.0)]]));
    }
    assert_eq!(out.iter().filter(|m| &*m.topic == "m.5m").count(), 2);
    assert_eq!(out.iter().filter(|m| &*m.topic == "m.1m").count(), 14);
    assert!(out.iter().all(|m| m.window_end == end_of(m)), "{out:?}");
}

#[test]
fn a_window_of_windows_carries_the_end_of_its_own_window() {
    let mut e = engine(CHAINED);
    let mut out = vec![];
    for at in (0..1000).step_by(30) {
        out.extend(push_all(&mut e, "trades", vec![vec![t(at as f64), s("A"), f(1.0)]]));
    }
    // the 1m bars that close the 5m windows end later than they do
    let ends: Vec<_> = out.iter().map(|m| (m.window_end, end_of(m))).collect();
    assert_eq!(ends, [(300_000_000, 300_000_000), (600_000_000, 600_000_000)]);
}

/// One chunk closing several windows writes them together: each carries the earliest end, never
/// a later one than its own (conservative, so a replay that crosses a sink's bound in one chunk
/// withholds the windows past it in that chunk too, which the sink holds if they were written).
#[test]
fn windows_closed_together_carry_the_earliest_end() {
    let mut e = engine(TWO_SOURCES);
    let out = push_all(&mut e, "fast", (0..=4).map(|m| vec![t(m as f64 * 60.0), s("A"), f(1.0)]).collect());
    assert_eq!(out.len(), 3);
    assert!(out.iter().all(|m| m.window_end == 60_000_000), "{out:?}");
}

/// A 1m window with a 60 s delay feeding a 1m window with a 1 s delay (by the first's
/// window_start); `{A}` and `{B}` are the two views, to create them in either order.
const IDLE_CHAINED: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol low_cardinality(string), price nullable(float64));
CREATE STREAM IF NOT EXISTS bars (bar_time datetime64(6), n uint64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (time int64, bars uint64, n uint64)
SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'm.1m', data_format = 'JSONEachRow', one_message_per_row = true;
{A}
{B}
";
const IDLE_A: &str = "CREATE MATERIALIZED VIEW IF NOT EXISTS a INTO bars AS
SELECT window_start AS bar_time, count() AS n FROM tumble(trades, local_event_time, 1m) GROUP BY window_start
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '60' SECOND;";
const IDLE_B: &str = "CREATE MATERIALIZED VIEW IF NOT EXISTS b INTO out AS
SELECT to_unix_timestamp64_micro(window_start) AS time, count() AS bars, sum(n) AS n
FROM tumble(bars, bar_time, 1m) GROUP BY window_start
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND;";

fn close(e: &mut Engine, ts: Value) -> Vec<String> {
    let mut out = vec![];
    e.close_until(ts.i64().unwrap(), &mut out);
    out.into_iter().map(|m| m.payload.trim_end().to_string()).collect()
}

/// close_until used to advance every window to the idle time,
/// but a window fed by another window's output sees times that lag it by the upstream's width
/// and delay. B's watermark went to 300 s while A, at 240 s, still had [240, 300) to emit, which
/// B then dropped as late; created first, B was also advanced before A emitted what the same
/// close closed, and dropped that too. Views are now closed upstream first, each only as far as
/// its input has reached: idle close changes when windows come out, not which.
#[test]
fn a_window_fed_by_a_window_is_idle_closed_only_as_far_as_its_input_has_reached() {
    let trades = |secs: &[f64]| secs.iter().map(|x| vec![t(*x), s("A"), f(1.0)]).collect::<Vec<_>>();
    let bar = |start: i64| format!(r#"{{"time":{},"bars":1,"n":1}}"#, start * 1_000_000);
    for (first, second) in [(IDLE_A, IDLE_B), (IDLE_B, IDLE_A)] {
        let sql = IDLE_CHAINED.replace("{A}", first).replace("{B}", second);
        let run = |idle: bool| {
            let mut e = engine(&sql);
            let mut out = push(&mut e, "trades", trades(&[10.0, 70.0, 130.0, 190.0, 250.0]));
            let closed = if idle { close(&mut e, t(305.0)) } else { vec![] };
            out.extend(closed.clone());
            out.extend(push(&mut e, "trades", trades(&[400.0])));
            out.extend(close(&mut e, t(10_000.0)));
            (out, closed, e.late())
        };
        let (out, closed, late) = run(true);
        // A closes [180, 240) and waits for 360 s for [240, 300): B closes up to 180 s
        assert_eq!(closed, [bar(60), bar(120)], "{first}");
        assert_eq!((&out, late), (&[0, 60, 120, 180, 240, 360].map(bar).to_vec(), 0), "{first}");
        assert_eq!(run(false), (out, vec![], 0), "{first}");
    }
}

// The source streams that have to be read in time order together are those of a connected set of views
// (`Engine::source_groups`): per-venue joins are independent of each other, so a quiet venue's
// partition need not delay another venue's row. A view that reads several streams, or a stream several
// views write, connects them.
const GROUP_STREAMS: &str = "
CREATE STREAM IF NOT EXISTS a_src (event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS b_src (event_time datetime64(6), symbol string, price float64);
CREATE STREAM IF NOT EXISTS mid (event_time datetime64(6), symbol string, price float64);
CREATE EXTERNAL STREAM IF NOT EXISTS a_out (event_time datetime64(6), symbol string, price float64)
  SETTINGS type = 'kafka', topic = 'a', data_format = 'JSONEachRow';
";

fn groups(views: &str) -> Vec<Vec<String>> {
    let cat = brrrrr_core::sql::parse(&format!("{GROUP_STREAMS}{views}")).unwrap();
    brrrrr_core::engine::Engine::new(&cat).unwrap().source_groups()
}

#[test]
fn views_that_share_no_stream_are_independent_source_groups() {
    let g = groups(
        "CREATE EXTERNAL STREAM IF NOT EXISTS b_out (event_time datetime64(6), symbol string, price float64)
           SETTINGS type = 'kafka', topic = 'b', data_format = 'JSONEachRow';
         CREATE MATERIALIZED VIEW IF NOT EXISTS a_mv INTO a_out AS SELECT event_time, symbol, price FROM a_src;
         CREATE MATERIALIZED VIEW IF NOT EXISTS b_mv INTO b_out AS SELECT event_time, symbol, price FROM b_src;",
    );
    assert_eq!(g, [vec!["a_src".to_string()], vec!["b_src".to_string()]]);
}

#[test]
fn a_stream_written_by_two_views_connects_the_sources_of_both() {
    // a_src and b_src both feed mid, which a_out reads: a row of either is ordered against the other's
    let g = groups(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS a_mid INTO mid AS SELECT event_time, symbol, price FROM a_src;
         CREATE MATERIALIZED VIEW IF NOT EXISTS b_mid INTO mid AS SELECT event_time, symbol, price FROM b_src;
         CREATE MATERIALIZED VIEW IF NOT EXISTS mid_out INTO a_out AS SELECT event_time, symbol, price FROM mid;",
    );
    assert_eq!(g, [vec!["a_src".to_string(), "b_src".to_string()]]);
}

#[test]
fn the_sides_of_a_join_are_one_source_group() {
    let g = groups(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS joined INTO a_out AS
         SELECT t.event_time AS event_time, t.symbol AS symbol, t.price AS price
         FROM (SELECT event_time, symbol, price FROM a_src ORDER BY symbol, event_time) AS t
         ASOF LEFT JOIN (SELECT event_time, symbol, price FROM b_src ORDER BY symbol, event_time) AS q
         ON t.symbol = q.symbol AND t.event_time >= q.event_time;",
    );
    assert_eq!(g, [vec!["a_src".to_string(), "b_src".to_string()]]);
}

#[test]
fn the_example_pipelines_group_as_their_joins_and_aggregates_connect_them() {
    let groups_of = |file: &str| {
        let sql =
            std::fs::read_to_string(format!("{}/../../fixtures/pipelines/{file}", env!("CARGO_MANIFEST_DIR"))).unwrap();
        brrrrr_core::engine::Engine::new(&brrrrr_core::sql::parse(&sql).unwrap()).unwrap().source_groups()
    };
    // derivatives: marks joined with indexes and trades, and four sources on their own
    let derivatives = groups_of("derivatives.sql");
    let mut sizes: Vec<_> = derivatives.iter().map(Vec::len).collect();
    sizes.sort();
    assert_eq!(sizes, [1, 1, 1, 1, 3], "{derivatives:?}");
    // flow's all-venue aggregate reads both venues' trades: one group
    let flow = groups_of("flow.sql");
    assert_eq!(flow, [vec!["spot_trades".to_string(), "trades".to_string()]]);
}

/// A `gap_fill` without both a start and a finish fills a gap of at most 65,536 buckets: a row
/// with a stray time decades off is written as it is, and the gap before it is counted
/// (`unfilled`) rather than filled with a row a second. One bounded by both is filled whole,
/// and times at i64's ends interpolate without overflowing.
#[test]
fn an_unbounded_gap_fill_leaves_gaps_too_long_to_fill() {
    let sql = |bounds: &str| {
        format!(
            "CREATE STREAM IF NOT EXISTS bars (t datetime64(6), k string, m nullable(float64));
         CREATE EXTERNAL STREAM IF NOT EXISTS out (t datetime64(6), k string, m nullable(float64))
           SETTINGS type = 'kafka', topic = 'out', data_format = 'JSONEachRow';
         CREATE MATERIALIZED VIEW IF NOT EXISTS f INTO out AS
         SELECT t, k, m FROM gap_fill(bars, t, '1s', {bounds}k, interpolate(m));"
        )
    };
    let row = |us: i64, m: f64| vec![Value::Time(us), s("a"), f(m)];
    let count = |e: &mut Engine, rows: Vec<Vec<Value>>| push(e, "bars", rows).len();
    let mut e = engine(&sql(""));
    // a gap of 65,536 buckets is filled; one more is not
    assert_eq!(count(&mut e, vec![row(0, 1.0), row(65_537_000_000, 2.0)]), 65_538);
    assert_eq!(e.unfilled(), 0);
    assert_eq!(count(&mut e, vec![row(65_537_000_000 + 65_538_000_000, 3.0)]), 1);
    assert_eq!(e.unfilled(), 1);
    // a stray time at either end of time: no overflow, nothing filled
    assert_eq!(count(&mut e, vec![row(i64::MAX - 1, 4.0), row(i64::MIN + 1, 5.0)]), 2);
    assert_eq!(e.unfilled(), 2);
    // bounded by start and finish, a gap is filled whole, however long
    let mut e = engine(&sql("0, 100000000000, "));
    assert_eq!(count(&mut e, vec![row(0, 1.0), row(70_000_000_000, 2.0)]), 70_001);
    assert_eq!(e.unfilled(), 0);
    // with either end NULL, unbounded
    for bounds in ["NULL, 100000000000, ", "0, NULL, "] {
        let mut e = engine(&sql(bounds));
        assert_eq!(count(&mut e, vec![row(0, 1.0), row(70_000_000_000, 2.0)]), 2, "{bounds}");
        assert_eq!(e.unfilled(), 1, "{bounds}");
        // after a key's last row, up to finish as the input passes it (a gap short enough)
        let mut out = vec![];
        e.close_until(i64::MAX, &mut out);
        assert_eq!((out.len(), e.unfilled()), (if bounds.starts_with("NULL") { 29_999 } else { 0 }, 1), "{bounds}");
        // finish excluded
    }
    // a key's first row, from start: too far from it, left unfilled
    let mut e = engine(&sql("0, NULL, "));
    assert_eq!(count(&mut e, vec![row(70_000_000_000, 1.0)]), 1);
    assert_eq!(e.unfilled(), 1);
    let mut e = engine(&sql("NULL, 100000000000, "));
    count(&mut e, vec![row(0, 1.0)]);
    let mut out = vec![];
    e.close_until(i64::MAX, &mut out);
    assert_eq!((out.len(), e.unfilled()), (0, 1), "100,000 buckets to finish: left unfilled");
    // between rows at i64's ends, the bounded gap's rows interpolate without overflowing
    let mut e = engine(&sql("0, 3000000, "));
    let got = push(&mut e, "bars", vec![row(i64::MIN + 1, 0.0), row(i64::MAX - 1, 1.0)]);
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(got[1].contains(r#""m":0.5"#), "{got:?}");
}

/// `gap_fill` over a stream's rows as they come: a key's skipped buckets before its next row,
/// rows without a time or not after the key's last bucket as they are, and the buckets after a
/// key's last row up to `finish` only as the input passes them (`close_until`).
#[test]
fn gap_fill_adds_each_keys_skipped_buckets() {
    let mut e = engine(
        "CREATE STREAM IF NOT EXISTS bars (t nullable(datetime64(6)), k string, v nullable(float64), m nullable(float64));
         CREATE EXTERNAL STREAM IF NOT EXISTS out (t nullable(int64), k string, v nullable(float64), m nullable(float64))
           SETTINGS type = 'kafka', topic = 'out', data_format = 'JSONEachRow';
         CREATE MATERIALIZED VIEW IF NOT EXISTS f INTO out AS
         SELECT to_unix_timestamp64_micro(t) / 1000000 AS t, k, v, m FROM gap_fill(bars, t, '1s', 0, 6000000, k, locf(v), interpolate(m));",
    );
    let row = |sec: Option<f64>, k: &str, v: Option<f64>| {
        vec![sec.map_or(Value::Null, t), s(k), v.map_or(Value::Null, f), v.map_or(Value::Null, f)]
    };
    let got = push(
        &mut e,
        "bars",
        vec![
            row(Some(1.0), "a", Some(1.0)),
            row(Some(3.0), "a", Some(7.0)),
            row(None, "a", Some(9.0)),
            row(Some(2.0), "a", Some(5.0)),
        ],
    );
    assert_eq!(
        got,
        [
            r#"{"t":0,"k":"a","v":null,"m":null}"#,
            r#"{"t":1,"k":"a","v":1,"m":1}"#,
            r#"{"t":2,"k":"a","v":1,"m":4}"#,
            r#"{"t":3,"k":"a","v":7,"m":7}"#,
            r#"{"t":null,"k":"a","v":9,"m":9}"#,
            r#"{"t":2,"k":"a","v":5,"m":5}"#,
        ]
    );
    let mut out = vec![];
    e.close_until(i64::MAX, &mut out);
    assert_eq!(
        out.iter().map(|m| m.payload.trim_end()).collect::<Vec<_>>(),
        [r#"{"t":4,"k":"a","v":7,"m":null}"#, r#"{"t":5,"k":"a","v":7,"m":null}"#]
    );
}
