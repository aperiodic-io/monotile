//! The historical executor (`engine::Historical`, ADR-0016) against the engine: every example
//! pipeline (fixtures/pipelines) over its input (fixtures/pipeline-inputs), one symbol at a
//! time, must write the messages the engine writes for the same rows in time order (exact ASOF
//! joins), message for message, however the sources are cut into batches and chunks.
use brrrrr_core::column::Batch;
use brrrrr_core::engine::{Asof, Emit, Engine, Historical, Pool, Serial, Source, Task};
use brrrrr_core::sql::{parse, Catalog};
use brrrrr_core::value::Value;
use serde_json::Value as J;
use std::collections::{BTreeSet, VecDeque};

use crate::common::{catalog, fixtures, value};

/// A message as compared: topic, payload, headers.
type Message = (String, String, Vec<(String, String)>);

/// A source's rows of `symbol` in the fixture's chunks, in clock order (ties in arrival order):
/// its clock is `local_timestamp`. Each source is a venue of its own (`exchange`): rows of
/// several writers of one stream meet in a group only where the stream copies their clock, as a
/// view over `orderbook_top_n` does not (`Historical::merge_order`).
fn rows(cat: &Catalog, fixture: &J, stream: &str, symbol: &str, venue: i64) -> Vec<Vec<Value>> {
    let st = &cat.streams[stream];
    let exchange = st.columns.iter().position(|c| c.name == "exchange");
    let clock = st.columns.iter().position(|c| c.name == "local_timestamp").unwrap();
    let sym = st.columns.iter().position(|c| c.name == "symbol").unwrap();
    let mut rows: Vec<Vec<Value>> = fixture["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["stream"] == stream)
        .flat_map(|c| c["rows"].as_array().unwrap())
        .map(|r| r.as_array().unwrap())
        .filter(|r| r[sym] == symbol)
        .map(|r| {
            let mut row: Vec<Value> = r.iter().zip(&st.columns).map(|(v, c)| value(v, &c.ty)).collect();
            if let Some(k) = exchange {
                row[k] = Value::Int(venue);
            }
            row
        })
        .collect();
    rows.sort_by_key(|r: &Vec<Value>| r[clock].i64().unwrap());
    rows
}

struct Batches(VecDeque<Batch>);

impl Source for Batches {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        self.0.pop_front().map(Ok)
    }
}

/// `rows` in batches of 1, 2, 3, ... rows, then of `size`.
fn batches(rows: &[Vec<Value>], width: usize, size: usize) -> Batches {
    let (mut out, mut at, mut n) = (VecDeque::new(), 0, 1);
    while at < rows.len() {
        let end = (at + n.min(size)).min(rows.len());
        out.push_back(Batch::from_rows(&rows[at..end], width));
        (at, n) = (end, n + 1);
    }
    Batches(out)
}

fn canonical(mut out: Vec<Emit>) -> Vec<Message> {
    let mut v: Vec<_> = out.drain(..).map(|e| (e.topic.to_string(), e.payload, e.headers)).collect();
    v.sort();
    v
}

/// Whether `got` are `want`'s messages, a float's sum (`sum`, `avg`, `weighted_avg`) of a window
/// built from its narrower windows' within rounding (ADR-0017): 1e-12 of it, and one Float32
/// step where it is cast to one. Every other value, topic and header is the same.
fn agree(got: &[Message], want: &[Message]) -> bool {
    fn close(x: &J, y: &J) -> bool {
        match (x, y) {
            (J::Number(a), J::Number(b)) if a != b => {
                let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) else { return false };
                let (d, m) = ((a - b).abs(), a.abs().max(b.abs()));
                d <= 1e-12 * m || (a as f32 as f64 == a && b as f32 as f64 == b && d <= m * f32::EPSILON as f64)
            }
            (J::Object(a), J::Object(b)) => {
                a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|w| close(v, w)))
            }
            _ => x == y,
        }
    }
    got.len() == want.len()
        && got.iter().zip(want).all(|((t, p, h), (u, q, i))| {
            t == u && h == i && close(&serde_json::from_str(p).unwrap(), &serde_json::from_str(q).unwrap())
        })
}

const DAY: i64 = 86_400_000_000;

/// The engine's messages for `sources` (per source, its rows in clock order), merged by clock.
fn engine_run(cat: &Catalog, sources: &[(String, Vec<Vec<Value>>)]) -> Vec<Emit> {
    engine_run_chunks(cat, sources, 100)
}

fn engine_run_chunks(cat: &Catalog, sources: &[(String, Vec<Vec<Value>>)], max: usize) -> Vec<Emit> {
    let mut e = Engine::new(cat).unwrap();
    e.set_asof(Asof::Exact);
    let mut all: Vec<(i64, usize, usize)> = vec![];
    for (k, (name, rows)) in sources.iter().enumerate() {
        let st = &cat.streams[name];
        let clock = st.columns.iter().position(|c| c.name.starts_with("local_timestamp")).unwrap();
        all.extend(rows.iter().enumerate().map(|(i, r)| (r[clock].i64().unwrap(), k, i)));
    }
    all.sort();
    let mut out = vec![];
    let mut i = 0;
    while i < all.len() {
        // runs of one source, as the runtime inserts them
        let k = all[i].1;
        let mut chunk = vec![];
        while i < all.len() && all[i].1 == k && chunk.len() < max {
            chunk.push(sources[k].1[all[i].2].clone());
            i += 1;
        }
        e.insert(&sources[k].0, chunk, &mut out);
    }
    e.close_until(i64::MAX, &mut out);
    out
}

/// Each job on a thread of its own, batches cut into parts of a few rows: every job of a chunk
/// runs at once.
struct Threads;

impl Pool for Threads {
    fn run<'a>(&self, jobs: Vec<Task<'a>>) {
        // a few workers taking the jobs in turn
        let queue = std::sync::Mutex::new(jobs.into_iter());
        std::thread::scope(|s| {
            for _ in 0..3 {
                s.spawn(|| {
                    while let Some(j) = queue.lock().unwrap().next() {
                        j()
                    }
                });
            }
        });
    }

    fn threads(&self) -> usize {
        4
    }

    fn part_rows(&self) -> usize {
        64
    }
}

/// The historical executor's messages, and the views it ran on rows (the engine's operators).
fn historical_run(
    cat: &Catalog,
    sources: &[(String, Vec<Vec<Value>>)],
    batch: usize,
    chunk: usize,
) -> (Vec<Emit>, Vec<String>) {
    let mut h = Historical::new(cat).unwrap();
    h.set_chunk_rows(chunk);
    let (mut lo, mut hi) = (i64::MAX, i64::MIN);
    let mut inputs: Vec<(String, Box<dyn Source>)> = vec![];
    for (name, rows) in sources {
        let st = &cat.streams[name];
        let clock = st.columns.iter().position(|c| c.name.starts_with("local_timestamp")).unwrap();
        for r in rows {
            let t = r[clock].i64().unwrap();
            (lo, hi) = (lo.min(t), hi.max(t));
        }
        inputs.push((name.clone(), Box::new(batches(rows, st.columns.len(), batch))));
    }
    let range = lo.div_euclid(DAY) * DAY..(hi.div_euclid(DAY) + 1) * DAY;
    let mut out = vec![];
    // threads for chunks of many rows (a thread per run costs: chunks of a few rows go serially)
    let pool: &dyn Pool = if chunk % 2 == 1 && chunk > 100 { &Threads } else { &Serial };
    let stats = h.run(inputs, range, pool, &mut out).unwrap();
    (out, stats.row_views)
}

/// The historical executor's messages, every view run on columns: none falls back to the
/// engine's operators, whose output would be the same.
fn columnar_run(cat: &Catalog, sources: &[(String, Vec<Vec<Value>>)], batch: usize, chunk: usize) -> Vec<Emit> {
    let (out, rows) = historical_run(cat, sources, batch, chunk);
    assert!(rows.is_empty(), "views run on rows: {rows:?}");
    out
}

/// Every example pipeline, one symbol at a time.
fn each_pipeline(mut check: impl FnMut(&str, &str, &Catalog, &[(String, Vec<Vec<Value>>)])) {
    for fixture in fixtures() {
        let name = fixture["pipeline"].as_str().unwrap();
        let cat = catalog(&fixture);
        let h = Historical::new(&cat).unwrap();
        let names: Vec<String> = h.sources().iter().map(|s| s.name.clone()).collect();
        let symbols: BTreeSet<String> = fixture["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|c| {
                let st = &cat.streams[c["stream"].as_str().unwrap()];
                let sym = st.columns.iter().position(|c| c.name == "symbol").unwrap();
                c["rows"].as_array().unwrap().iter().map(move |r| r[sym].as_str().unwrap().to_string())
            })
            .collect();
        for symbol in &symbols {
            let sources: Vec<(String, Vec<Vec<Value>>)> =
                names.iter().zip(1..).map(|(n, venue)| (n.clone(), rows(&cat, &fixture, n, symbol, venue))).collect();
            check(name, symbol, &cat, &sources);
        }
    }
}

#[test]
fn every_pipeline_writes_what_the_engine_writes() {
    let (mut runs, mut written, mut on_rows) = (0, BTreeSet::new(), BTreeSet::new());
    each_pipeline(|name, symbol, cat, sources| {
        let want = canonical(engine_run(cat, sources));
        if !want.is_empty() {
            written.insert(name.to_string());
        }
        for (batch, chunk) in [(1 << 20, 1 << 16), (7, 5), (1000, 333), (1 << 20, 4001)] {
            let (got, rows) = historical_run(cat, sources, batch, chunk);
            on_rows.extend(rows.into_iter().map(|v| format!("{name}.{v}")));
            let got = canonical(got);
            if !agree(&got, &want) {
                let missing: Vec<_> = want.iter().filter(|m| !got.contains(m)).take(3).collect();
                let extra: Vec<_> = got.iter().filter(|m| !want.contains(m)).take(3).collect();
                panic!(
                    "{name} {symbol} (batches of {batch}, chunks of {chunk}): {} messages, the engine's {}\n\
                     missing {missing:#?}\nextra {extra:#?}",
                    got.len(),
                    want.len()
                );
            }
        }
        runs += 1;
    });
    assert!(runs >= 20, "{runs} pipeline runs");
    // what has no columnar form runs the engine's operators, nothing else: a table function
    // (orderbook_top_n), a window over a subquery, a frame's window functions and lead, a window
    // holding a t-digest
    let rows = ["book.depth_mv", "book.spot_depth_mv", "flow.all_venues_1m", "state.next_v", "state.over_v"];
    let want: BTreeSet<String> = rows.iter().chain(&["stats.merged_range_15s"]).map(|v| v.to_string()).collect();
    assert_eq!(on_rows, want, "views run on rows");
    assert_eq!(written.len(), fixtures().len(), "a pipeline that writes nothing proves little: {written:?}");
}

/// Keys that vary within a partition (a join's, a lag's partition, a window's group), NULLs in
/// the sources, ties of time: the general paths, against the engine.
const KEYED: &str = "
CREATE EXTERNAL STREAM a_source (local_timestamp int64, k string, x nullable(float64), id string)
  SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'a', data_format = 'ProtobufSingle', format_schema = 's:A';
CREATE EXTERNAL STREAM b_source (local_timestamp int64, k string, y float64)
  SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'b', data_format = 'ProtobufSingle', format_schema = 's:B';
CREATE STREAM a_in (t datetime64(6), k string, x nullable(float64), id string);
CREATE STREAM b_in (t datetime64(6), k string, y float64);
CREATE STREAM j (t datetime64(6), k string, x nullable(float64), y float64);
CREATE STREAM l (t datetime64(6), k string, x nullable(float64), y float64, px nullable(float64));
CREATE EXTERNAL STREAM out (k string, time int64, n uint64, sx nullable(float64), ay nullable(float64), lp nullable(float64), mx nullable(float64), nulls uint64, ret nullable(float64))
  SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'out', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW a_mv INTO a_in AS
  SELECT from_unix_timestamp64_micro(local_timestamp) AS t, k, x, id FROM a_source
  WHERE x IS NULL OR x > -50;
CREATE MATERIALIZED VIEW b_mv INTO b_in AS SELECT from_unix_timestamp64_micro(local_timestamp) AS t, k, y FROM b_source;
CREATE MATERIALIZED VIEW j_mv INTO j AS
  SELECT a.t AS t, a.k AS k, a.x AS x, b.y AS y
  FROM (SELECT t, k, x FROM a_in) AS a ASOF LEFT JOIN (SELECT t, k, y FROM b_in) AS b ON a.k = b.k AND a.t >= b.t;
CREATE MATERIALIZED VIEW l_mv INTO l AS SELECT t, k, x, y, lag(x) OVER (PARTITION BY k) AS px FROM j;
CREATE MATERIALIZED VIEW w INTO out AS
  SELECT k, to_unix_timestamp64_micro(window_start) AS time, count() AS n, sum(x) AS sx, avg(y) AS ay,
    latest(px) AS lp, max(x) AS mx, count_if(x IS NULL) AS nulls, sum(if(x > 0, x, 0)) AS ret
  FROM tumble(l, t, 1m) GROUP BY window_start, k EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

#[test]
fn keys_that_vary_within_a_partition_and_nulls_give_what_the_engine_gives() {
    let cat = parse(KEYED).unwrap();
    let mut rng = 0x2545_f491_4f6c_dd1du64;
    let mut next = |m: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % m
    };
    let keys = ["a", "b", "c"];
    for case in 0..6 {
        let base = 1_789_946_100_000_000i64;
        let (mut a, mut b) = (vec![], vec![]);
        let mut t = base;
        for i in 0..20_000 {
            // ties of time, and minutes without rows (no held ORDER BY before the join: the
            // engine's exact join is exact then, see below)
            t += [0, 0, 1, 7, 1_000, 900_000, 60_000_000][next(7) as usize];
            let k = Value::Str(keys[next(3) as usize].into());
            let x = if next(7) == 0 { Value::Null } else { Value::F64((next(2_000) as f64 - 1_000.0) / 10.0) };
            a.push(vec![Value::Int(t), k, x, Value::Str(format!("{:05}", next(100) * 1_000 + i % 1_000).into())]);
            if next(3) == 0 {
                b.push(vec![
                    Value::Int(t - next(3) as i64),
                    Value::Str(keys[next(3) as usize].into()),
                    Value::F64(next(100) as f64),
                ]);
            }
        }
        // each source in clock order
        a.sort_by_key(|r| r[0].i64());
        b.sort_by_key(|r| r[0].i64());
        let sources = vec![("a_source".to_string(), a), ("b_source".to_string(), b)];
        let want = canonical(engine_run(&cat, &sources));
        assert!(want.len() > 100, "case {case}: {} messages", want.len());
        for (batch, chunk) in [(1 << 20, 1 << 16), (7, 5), (1000, 333), (64, 4001)] {
            let got = canonical(columnar_run(&cat, &sources, batch, chunk));
            if got != want {
                let missing: Vec<_> = want.iter().filter(|m| !got.contains(m)).take(3).map(|m| &m.1).collect();
                let extra: Vec<_> = got.iter().filter(|m| !want.contains(m)).take(3).map(|m| &m.1).collect();
                panic!("case {case}, batches of {batch}, chunks of {chunk}: {} messages, the engine's {}\nmissing {missing:#?}\nextra {extra:#?}", got.len(), want.len());
            }
        }
    }
}

/// The engine's exact ASOF join keeps the versions a left row may need for `ASOF_HOLD_US` of
/// the right side's time: a left side behind a held ORDER BY whose rows wait (for a later one,
/// on a quiet feed) gets the versions at hand when they come, those it needed pruned. The
/// historical executor has every row of both sides: a left row takes the right row of its key
/// with the greatest time at or before its own, however long either side was quiet.
#[test]
fn an_asof_join_takes_the_latest_version_however_long_a_side_was_quiet() {
    let held = KEYED.replace(
        "WHERE x IS NULL OR x > -50;",
        "WHERE x IS NULL OR x > -50 ORDER BY t, id SETTINGS order_hold_ms = 50;",
    );
    let sql = held
        .replace(
            "CREATE STREAM j (t datetime64(6), k string, x nullable(float64), y float64);",
            "CREATE EXTERNAL STREAM j (t datetime64(6), k string, x nullable(float64), y float64)
               SETTINGS type = 'kafka', brokers = 'b:9092', topic = 'j', data_format = 'JSONEachRow';",
        )
        .split("CREATE MATERIALIZED VIEW l_mv")
        .next()
        .unwrap()
        .replace(
            "CREATE STREAM l (t datetime64(6), k string, x nullable(float64), y float64, px nullable(float64));",
            "",
        );
    let sql = sql.split("CREATE EXTERNAL STREAM out").next().unwrap().to_string()
        + &held
            [held.find("CREATE MATERIALIZED VIEW a_mv").unwrap()..held.find("CREATE MATERIALIZED VIEW l_mv").unwrap()];
    assert!(sql.contains("order_hold_ms"));
    let cat = parse(&sql).unwrap();
    let s = 1_789_951_110_000_000i64;
    let a = |t: i64, k: &str| vec![Value::Int(s + t), Value::Str(k.into()), Value::F64(1.0), Value::Str("1".into())];
    let b = |t: i64, k: &str, y: f64| vec![Value::Int(s + t), Value::Str(k.into()), Value::F64(y)];
    // a's side quiet for a minute while b's goes on
    let sources = vec![
        (
            "a_source".to_string(),
            vec![a(0, "a"), a(30_000_001, "a"), a(30_000_003, "b"), a(90_000_000, "a"), a(90_000_002, "c")],
        ),
        (
            "b_source".to_string(),
            vec![
                b(-5, "a", 47.0),
                b(30_000_002, "a", 51.0),
                b(60_000_000, "b", 9.0),
                b(89_999_999, "a", 71.0),
                b(90_000_001, "c", 3.0),
            ],
        ),
    ];
    let got: Vec<(String, f64)> = columnar_run(&cat, &sources, 1 << 20, 1 << 16)
        .iter()
        .map(|e| {
            let p: serde_json::Value = serde_json::from_str(&e.payload).unwrap();
            (p["k"].as_str().unwrap().to_string(), p["y"].as_f64().unwrap())
        })
        .collect();
    let want = [("a", 47.0), ("a", 47.0), ("b", 0.0), ("a", 71.0), ("c", 3.0)];
    assert_eq!(got, want.map(|(k, y)| (k.to_string(), y)));
}
