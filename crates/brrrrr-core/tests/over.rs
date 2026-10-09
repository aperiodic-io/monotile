//! Per-row window functions (`f(...) OVER (...)`, src/over.rs): held to QuestDB 10.0.1's results
//! (fixtures/questdb-vectors, scripts/record-questdb-over.py), to a from-scratch recomputation
//! of every frame on random input, and to what a stream allows (no frame past the current row,
//! bounded state, checkpoints that restore exactly).
use brrrrr_core::agg::Acc;
use brrrrr_core::engine::{Emit, Engine, State};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;
use serde_json::Value as Json;

fn engine(sql: &str) -> Result<Engine, String> {
    Engine::new(&parse(sql).map_err(|e| e.to_string())?)
}

fn insert(e: &mut Engine, stream: &str, rows: Vec<Vec<Value>>) -> Vec<Json> {
    let mut out: Vec<Emit> = vec![];
    e.insert(stream, rows, &mut out);
    out.iter().map(|m| serde_json::from_str(&m.payload).unwrap()).collect()
}

/// A view over `t (ts, g, p)` writing `cols` (name, expression) to a Kafka topic as nullable
/// Float64 columns.
fn view(cols: &[(String, String)], extra: &str) -> String {
    let types: Vec<String> = cols.iter().map(|(n, _)| format!("{n} nullable(float64)")).collect();
    let select: Vec<String> = cols.iter().map(|(n, e)| format!("{e} AS {n}")).collect();
    format!(
        "CREATE STREAM IF NOT EXISTS t (ts datetime64(6), g string, p nullable(float64));
CREATE EXTERNAL STREAM IF NOT EXISTS out ({}) SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS SELECT {} FROM t{extra};",
        types.join(", "),
        select.join(", ")
    )
}

fn row(ts: i64, g: &str, p: Option<f64>) -> Vec<Value> {
    vec![Value::Time(ts), Value::Str(g.into()), p.map_or(Value::Null, Value::F64)]
}

fn close(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= 1e-9 * a.abs().max(b.abs())
}

#[test]
fn window_functions_match_questdb() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/questdb-vectors/over.json.gz");
    let d: Json = serde_json::from_reader(flate2::read::GzDecoder::new(std::fs::File::open(path).unwrap())).unwrap();
    let cols: Vec<(String, String)> = d["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| (c["name"].as_str().unwrap().to_string(), c["brrrrr"].as_str().unwrap().to_string()))
        .collect();
    let mut e = engine(&view(&cols, "")).unwrap();
    let rows: Vec<Vec<Value>> = d["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| row(r[0].as_i64().unwrap(), r[1].as_str().unwrap(), r[2].as_f64()))
        .collect();
    let mut got = vec![];
    for chunk in rows.chunks(17) {
        got.extend(insert(&mut e, "t", chunk.to_vec()));
    }
    let want = d["results"].as_array().unwrap();
    assert_eq!(got.len(), want.len());
    let mut wrong = vec![];
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        for (j, (name, sql)) in cols.iter().enumerate() {
            let (a, b) = (&g[name.as_str()], &w[j]);
            let same = match (a.as_f64(), b.as_f64()) {
                (Some(x), Some(y)) => close(x, y),
                _ => a.is_null() && b.is_null(),
            };
            if !same {
                wrong.push(format!("row {i} {name}: brrrrr {a}, QuestDB {b} ({sql})"));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} values differ from QuestDB:\n{}",
        wrong.len(),
        wrong[..wrong.len().min(20)].join("\n")
    );
}

/// The frame a row sees, recomputed from scratch: the partition's rows so far in arrival order,
/// the last n + 1 of them, or those within `w` of the partition's latest time (a late row takes
/// effect at the latest).
fn reference(rows: &[(i64, String, Value)], frame: &str, i: usize) -> Vec<Value> {
    let (_, g, _) = &rows[i];
    let part: Vec<(i64, &Value)> = rows[..=i]
        .iter()
        .filter(|r| &r.1 == g)
        .scan(i64::MIN, |latest, r| {
            *latest = (*latest).max(r.0);
            Some((*latest, &r.2))
        })
        .collect();
    let now = part.last().unwrap().0;
    let take: Vec<Value> = match frame.split_once(' ') {
        Some(("rows", n)) => {
            let n: usize = n.parse().unwrap();
            part[part.len().saturating_sub(n + 1)..].iter().map(|r| r.1.clone()).collect()
        }
        Some(("range", w)) => {
            let w: i64 = w.parse().unwrap();
            part.iter().filter(|r| r.0 >= now - w).map(|r| r.1.clone()).collect()
        }
        _ => part.iter().map(|r| r.1.clone()).collect(),
    };
    take
}

/// Every aggregate kept as its frame slides gives what recomputing it over the frame gives, on
/// random rows with NULLs, many partitions, rows out of time order, integers and floats.
#[test]
fn sliding_frames_equal_a_recomputation() {
    let mut rng = 0x2545_f491_4f6c_dd1du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for (ty, gen) in [("nullable(float64)", 0), ("nullable(int64)", 1)] {
        let mut rows = vec![];
        let mut ts = 1_000_000_000i64;
        for _ in 0..2_000 {
            ts += (next() % 3_000_000) as i64;
            // one row in ten arrives up to 5 s early of its partition's latest
            let t = if next() % 10 == 0 { ts - (next() % 5_000_000) as i64 } else { ts };
            let g = format!("k{}", next() % 7);
            let v = match (next() % 9, gen) {
                (0, _) => Value::Null,
                (_, 0) => Value::F64((next() % 100_000) as f64 / 100.0 - 300.0),
                _ => Value::Int((next() % 2001) as i64 - 1000),
            };
            rows.push((t, g, v));
        }
        for frame in ["all", "rows 0", "rows 4", "rows 60", "range 0", "range 7000000", "range 60000000"] {
            let over = match frame.split_once(' ') {
                Some(("rows", n)) => format!("ROWS BETWEEN {n} PRECEDING AND CURRENT ROW"),
                Some(("range", w)) => format!("RANGE BETWEEN INTERVAL '{w}' MICROSECOND PRECEDING AND CURRENT ROW"),
                _ => String::new(),
            };
            let fns = ["sum", "avg", "count", "min", "max", "first_value", "last_value", "stddev_samp"];
            let cols: Vec<(String, String)> = fns
                .iter()
                .map(|f| (f.to_string(), format!("{f}(p) OVER (PARTITION BY g ORDER BY ts {over})")))
                .collect();
            let sql = view(&cols, "").replace("p nullable(float64));", &format!("p {ty});"));
            let mut e = engine(&sql).unwrap();
            let input: Vec<Vec<Value>> =
                rows.iter().map(|(t, g, v)| vec![Value::Time(*t), Value::Str(g.as_str().into()), v.clone()]).collect();
            let mut got = vec![];
            for chunk in input.chunks(97) {
                got.extend(insert(&mut e, "t", chunk.to_vec()));
            }
            for (i, g) in got.iter().enumerate() {
                let fr = reference(&rows, frame, i);
                for f in fns {
                    let want = match f {
                        "first_value" => fr.first().cloned().unwrap_or(Value::Null),
                        "last_value" => fr.last().cloned().unwrap_or(Value::Null),
                        f => {
                            let mut acc = Acc::new(f, &[], 1).unwrap();
                            fr.iter().for_each(|v| acc.add(v.clone()));
                            acc.result()
                        }
                    };
                    let have = &g[f];
                    let ok = match (&want, have.as_f64()) {
                        (Value::Null, _) => have.is_null(),
                        (w, Some(h)) => w.f64().is_some_and(|w| close(w, h) || (w - h).abs() < 1e-7),
                        _ => want.f64().is_some_and(f64::is_nan) && have.is_null(),
                    };
                    assert!(ok, "{ty} {frame} row {i} {f}: brrrrr {have}, recomputed {want:?}");
                }
            }
        }
    }
}

#[test]
fn window_functions_refuse_what_a_stream_cannot_do() {
    let err = |sel: &str, extra: &str| engine(&view(&[("x".into(), sel.into())], extra)).err().unwrap_or_default();
    let cases = [
        ("lead(p) OVER (ORDER BY ts ROWS 1 PRECEDING)", "lead takes no frame"),
        ("rank() OVER (ORDER BY ts)", "rank ranks the rows of each time: PARTITION BY the time first"),
        ("nth_value(p, 2) OVER (ORDER BY ts)", "nth_value is not supported as a window function"),
        ("avg(p) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)", "a frame must end at the current row"),
        ("avg(p) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)", "a frame must end at the current row"),
        ("avg(p) OVER (ORDER BY ts GROUPS 1 PRECEDING)", "GROUPS frames are not supported"),
        (
            "avg(p) OVER (RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW)",
            "a RANGE frame needs exactly one ORDER BY key",
        ),
        ("avg(p) OVER (ORDER BY ts DESC)", "only ascending ORDER BY"),
        ("avg(p) OVER (ORDER BY ts NULLS FIRST)", "only ascending ORDER BY"),
        ("avg(p) OVER (ORDER BY ts WITH FILL)", "only ascending ORDER BY"),
        ("avg(p) OVER (ORDER BY ts ROWS 100000 PRECEDING)", "a whole number below 100000"),
        ("lag(p) OVER (ORDER BY ts ROWS 2 PRECEDING)", "lag takes no frame"),
        ("lag(p, 0) OVER (ORDER BY ts)", "lag offset"),
        ("lag(p, 1_000) OVER (ORDER BY ts)", r#""1_000" is not a number"#),
        ("avg(p, 'alpha', 2) OVER (ORDER BY ts)", "'alpha' in (0, 1]"),
        ("avg(p, 'beta', 0.5) OVER (ORDER BY ts)", "'alpha' in (0, 1]"),
        ("avg(p, 'alpha', 0) OVER (ORDER BY ts)", "'alpha' in (0, 1]"),
        ("avg(p, 'period', 0.5) OVER (ORDER BY ts)", "'period' >= 1"),
        ("lag(p, 1, 0, 5) OVER (ORDER BY ts)", "unknown window function lag of 4 arguments"),
        ("lag(2)(p) OVER (ORDER BY ts)", "lag takes no parameters"),
        ("median_of(p) OVER (ORDER BY ts)", "unknown window function median_of"),
        ("sum(p) OVER nowhere", "unknown window nowhere"),
        ("first_value(p) IGNORE NULLS OVER (ORDER BY ts)", "IGNORE/RESPECT NULLS"),
        ("sum(lag(p) OVER (ORDER BY ts)) OVER (ORDER BY ts)", "outside the SELECT list"),
    ];
    for (sel, why) in cases {
        let e = err(sel, "");
        assert!(e.contains(why), "{sel}: {e:?}");
    }
    // no window function in WHERE (it runs first), in a windowed SELECT, or a WINDOW with none
    assert!(err("p", " WHERE lag(p) OVER (ORDER BY ts) > 0").contains("outside the SELECT list"));
    assert!(err("p", " WINDOW w AS (ORDER BY ts)").contains("a WINDOW clause without a window function"));
    // a window refined from another (`OVER (w ...)`, `w2 AS (w ...)`) is not supported
    let e = err("sum(p) OVER (w ORDER BY ts)", " WINDOW w AS (PARTITION BY g)");
    assert!(e.contains("a window built on window"), "{e}");
    let e = err("sum(p) OVER w2", " WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY ts)");
    assert!(e.contains("unsupported window definition"), "{e}");
    let windowed = "CREATE STREAM IF NOT EXISTS t (ts datetime64(6), g string, p float64);
CREATE STREAM IF NOT EXISTS o (x float64);
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO o AS SELECT sum(p) OVER (ORDER BY window_start) AS x
FROM tumble(t, ts, 1m) GROUP BY window_start EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND;";
    assert!(engine(windowed).err().unwrap().contains("outside the SELECT list"));
}

/// Named windows, a filter that runs before the window functions, several PARTITION BYs in one
/// SELECT, and a running total anchored to the UTC day.
#[test]
fn named_windows_filters_and_anchored_totals() {
    let day = 86_400_000_000i64;
    let cols: Vec<(String, String)> = [
        ("s", "sum(p) OVER w"),
        ("n", "row_number() OVER w"),
        ("all_n", "count() OVER (ORDER BY ts)"),
        ("daily", "sum(p) OVER (PARTITION BY g, to_start_of_day(ts) ORDER BY ts)"),
        ("prev", "p - lag(p) OVER w"),
    ]
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .into();
    let mut e = engine(&view(&cols, " WHERE p > 0 WINDOW w AS (PARTITION BY g ORDER BY ts)")).unwrap();
    let got = insert(
        &mut e,
        "t",
        vec![
            row(day - 2, "a", Some(1.0)),
            row(day - 1, "b", Some(5.0)),
            row(day - 1, "a", Some(-7.0)), // filtered out before any window function sees it
            row(day, "a", Some(2.0)),      // a new UTC day: the daily total starts again
            row(day + 1, "a", Some(4.0)),
        ],
    );
    let col = |c: &str| got.iter().map(|r| r[c].as_f64()).collect::<Vec<_>>();
    assert_eq!(col("s"), [Some(1.0), Some(5.0), Some(3.0), Some(7.0)]);
    assert_eq!(col("n"), [Some(1.0), Some(1.0), Some(2.0), Some(3.0)]);
    assert_eq!(col("all_n"), [Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    assert_eq!(col("daily"), [Some(1.0), Some(5.0), Some(2.0), Some(6.0)]);
    assert_eq!(col("prev"), [None, None, Some(1.0), Some(2.0)]);
}

/// A window function over an ASOF join: the rolling mean of the quote each trade met.
#[test]
fn window_functions_run_over_an_asof_join() {
    let sql = "
CREATE STREAM IF NOT EXISTS trades (ts datetime64(6), s string, p float64);
CREATE STREAM IF NOT EXISTS quotes (ts datetime64(6), s string, bid float64);
CREATE EXTERNAL STREAM IF NOT EXISTS out (p float64, bid float64, mb nullable(float64))
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS
SELECT t.p AS p, q.bid AS bid, avg(q.bid) OVER (PARTITION BY t.s ORDER BY t.ts ROWS 1 PRECEDING) AS mb
FROM trades AS t ASOF LEFT JOIN quotes AS q ON t.s = q.s AND t.ts >= q.ts;";
    let mut e = engine(sql).unwrap();
    let r = |t: i64, v: f64| vec![Value::Time(t), Value::Str("X".into()), Value::F64(v)];
    insert(&mut e, "quotes", vec![r(1, 10.0), r(3, 20.0)]);
    let got = insert(&mut e, "trades", vec![r(2, 1.0), r(4, 2.0), r(5, 3.0)]);
    let mb: Vec<f64> = got.iter().map(|r| r["mb"].as_f64().unwrap()).collect();
    assert_eq!(mb, [10.0, 15.0, 20.0]);
}

const CKPT: &str = "avg(p) OVER (PARTITION BY g ORDER BY ts ROWS 3 PRECEDING)|max(p) OVER (PARTITION BY g ORDER BY ts RANGE INTERVAL 5 SECOND PRECEDING)|sum(p) OVER (PARTITION BY g ORDER BY ts)|lag(p, 2) OVER (PARTITION BY g ORDER BY ts)|avg(p, 'alpha', 0.3) OVER (PARTITION BY g ORDER BY ts)|first_value(p) OVER (PARTITION BY g ORDER BY ts)|row_number() OVER (ORDER BY ts)";

fn ckpt_sql() -> String {
    let cols: Vec<(String, String)> =
        CKPT.split('|').enumerate().map(|(i, e)| (format!("c{i}"), e.to_string())).collect();
    view(&cols, "")
}

fn ckpt_rows() -> Vec<Vec<Value>> {
    (0..40).map(|i| row(i * 700_000, ["a", "b", "c"][i as usize % 3], (i % 5 != 0).then_some(i as f64 * 1.5))).collect()
}

/// A checkpoint taken at any row restores (through its encoding) into exactly the output of an
/// uninterrupted run, including what each bounded frame keeps as it slides.
#[test]
fn window_state_survives_a_checkpoint_at_any_row() {
    let mut whole = engine(&ckpt_sql()).unwrap();
    let want = insert(&mut whole, "t", ckpt_rows());
    for cut in [1, 7, 20, 39] {
        let mut a = engine(&ckpt_sql()).unwrap();
        let mut got = insert(&mut a, "t", ckpt_rows()[..cut].to_vec());
        let bytes = postcard::to_allocvec(&a.snapshot()).unwrap();
        let mut b = engine(&ckpt_sql()).unwrap();
        b.restore(postcard::from_bytes(&bytes).unwrap()).unwrap();
        got.extend(insert(&mut b, "t", ckpt_rows()[cut..].to_vec()));
        assert_eq!(got, want, "cut at {cut}");
    }
}

/// What a bounded frame keeps as it slides is not checkpointed but rebuilt from the frame's rows,
/// so a sliding Float64 sum, mean or weighted mean must not depend on the rows that left the
/// frame before the checkpoint. Values inexact in binary (sums of them round differently in
/// another order), ROWS and RANGE frames, a checkpoint at every row: a restore gives, bit for
/// bit, what the uninterrupted run gives.
#[test]
fn sliding_float_sums_restore_bit_for_bit() {
    let frames = ["ROWS 3 PRECEDING", "ROWS 40 PRECEDING", "RANGE INTERVAL 5 SECOND PRECEDING"];
    let cols: Vec<(String, String)> = frames
        .iter()
        .enumerate()
        .flat_map(|(i, f)| {
            let over = format!("OVER (PARTITION BY g ORDER BY ts {f})");
            [
                (format!("s{i}"), format!("sum(p) {over}")),
                (format!("a{i}"), format!("avg(p) {over}")),
                (format!("w{i}"), format!("vwap(p, p * 0.7 + 0.1) {over}")),
            ]
        })
        .collect();
    let sql = view(&cols, "");
    let mut rng = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    // prices around 100 with six decimals, some NULLs, some far larger: sums inexact in binary
    let rows: Vec<Vec<Value>> = (0..300)
        .map(|i| {
            let p = match next() % 23 {
                0 => None,
                1 => Some(1e9 + (next() % 1_000_000) as f64 / 1e6),
                _ => Some(100.0 + (next() % 10_000_000) as f64 / 1e6),
            };
            row(i * 400_000, ["a", "b"][(next() % 2) as usize], p)
        })
        .collect();
    let mut whole = engine(&sql).unwrap();
    let want = insert(&mut whole, "t", rows.clone());
    let mut differ = vec![];
    for cut in 1..rows.len() {
        let mut a = engine(&sql).unwrap();
        let mut got = insert(&mut a, "t", rows[..cut].to_vec());
        let bytes = postcard::to_allocvec(&a.snapshot()).unwrap();
        let mut b = engine(&sql).unwrap();
        b.restore(postcard::from_bytes(&bytes).unwrap()).unwrap();
        got.extend(insert(&mut b, "t", rows[cut..].to_vec()));
        for (i, (g, w)) in got.iter().zip(&want).enumerate().skip(cut) {
            for (name, _) in &cols {
                // bits, not JSON numbers: serde_json prints the shortest round-trip form
                let bits = |j: &Json| j[name.as_str()].as_f64().map(f64::to_bits);
                if bits(g) != bits(w) {
                    differ.push(format!("cut {cut}, row {i}, {name}: {} after a restore, {}", g[name], w[name]));
                }
            }
        }
    }
    assert!(differ.is_empty(), "{} values differ:\n{}", differ.len(), differ[..differ.len().min(10)].join("\n"));
}

/// A sliding Float64 sum, mean and weighted mean are exact, rounded once: 1 + 1e100 + 1 - 1e100
/// is 2, where adding in order gives 0 (and a weighted mean of total weight 0, NULL).
#[test]
fn sliding_float_sums_are_exact() {
    let over = "OVER (ORDER BY ts ROWS 3 PRECEDING)";
    let cols: Vec<(String, String)> = [("s", "sum(p)"), ("a", "avg(p)"), ("w", "vwap(2.0, p)")]
        .map(|(n, f)| (n.to_string(), format!("{f} {over}")))
        .into();
    let mut e = engine(&view(&cols, "")).unwrap();
    let got = insert(
        &mut e,
        "t",
        [1.0, 1e100, 1.0, -1e100].iter().enumerate().map(|(i, p)| row(i as i64, "a", Some(*p))).collect(),
    );
    let last = got.last().unwrap();
    assert_eq!((last["s"].as_f64(), last["a"].as_f64(), last["w"].as_f64()), (Some(2.0), Some(0.5), Some(2.0)));
}

/// A snapshot whose window state the plan could not have made is refused.
#[test]
fn window_state_the_plan_could_not_make_is_refused() {
    let mut a = engine(&ckpt_sql()).unwrap();
    insert(&mut a, "t", ckpt_rows());
    let json = serde_json::to_value(a.snapshot()).unwrap();
    type Edit = (&'static str, fn(&mut Json));
    fn part(j: &mut Json) -> &mut Json {
        &mut j[0][0]["Over"]["groups"][0]["parts"][0][1]
    }
    fn frame(j: &mut Json, f: usize) -> &mut Vec<Json> {
        part(j)["fns"][f]["Frame"].as_array_mut().unwrap()
    }
    let edits: [Edit; 8] = [
        // one of the plan's two window groups missing
        ("does not match the plan", |j| {
            j[0][0]["Over"]["groups"].as_array_mut().unwrap().pop();
        }),
        // a ROWS 3 frame holding five rows, in order: the row before its first added back
        ("holds a state", |j| {
            let r = frame(j, 0);
            let mut before = r[0].clone();
            before[0] = (before[0].as_u64().unwrap() - 1).into();
            before[1] = (before[1].as_i64().unwrap() - 1).into();
            r.insert(0, before);
        }),
        // a RANGE frame holding one row twice
        ("holds a state", |j| {
            let r = frame(j, 1);
            r.insert(0, r[0].clone());
        }),
        // a RANGE frame whose first row is later than its second
        ("holds a state", |j| {
            let r = frame(j, 1);
            r[0][1] = (r[1][1].as_i64().unwrap() + 1).into();
        }),
        // a frame's last row later than the partition's latest time, or past its row count
        ("holds a state", |j| {
            let latest = part(j)["latest"].as_i64().unwrap();
            part(j)["latest"] = (latest - 1).into();
        }),
        ("holds a state", |j| {
            let rows = part(j)["rows"].as_u64().unwrap();
            part(j)["rows"] = (rows - 1).into();
        }),
        // a partition filed under another key
        ("filed under another key", |j| {
            j[0][0]["Over"]["groups"][0]["parts"][0][0] = "nope".into();
        }),
        // one window function missing from a partition
        ("does not hold this plan's window functions", |j| {
            j[0][0]["Over"]["groups"][0]["parts"][0][1]["fns"].as_array_mut().unwrap().pop();
        }),
    ];
    for (why, edit) in edits {
        let mut j = json.clone();
        edit(&mut j);
        let st: State = serde_json::from_value(j).unwrap();
        let err = engine(&ckpt_sql()).unwrap().restore(st).unwrap_err();
        assert!(err.contains(why), "{why}: {err}");
    }
}

/// Partitions are bounded: past `over::MAX_PARTITIONS`, the least recently used tenth goes. A
/// snapshot of exactly that many restores; one of more is refused.
#[test]
fn partitions_are_bounded() {
    let cols = [("x".to_string(), "count() OVER (PARTITION BY g)".to_string())];
    let mut e = engine(&view(&cols, "")).unwrap();
    let n = brrrrr_core::over::MAX_PARTITIONS;
    let rows: Vec<Vec<Value>> = (0..n).map(|i| row(i as i64, &format!("k{i}"), Some(1.0))).collect();
    insert(&mut e, "t", rows);
    let full = serde_json::to_value(e.snapshot()).unwrap();
    engine(&view(&cols, "")).unwrap().restore(serde_json::from_value(full.clone()).unwrap()).unwrap();
    let mut more = full;
    let parts = more[0][0]["Over"]["groups"][0]["parts"].as_array_mut().unwrap();
    parts.push(parts[0].clone());
    let err = engine(&view(&cols, "")).unwrap().restore(serde_json::from_value(more).unwrap()).unwrap_err();
    assert!(err.contains(&format!("more than the {n} kept")), "{err}");
    // k0 is used again, so k1 is now the least recently used; a new key makes room
    let t = n as i64;
    insert(&mut e, "t", vec![row(t, "k0", Some(1.0)), row(t + 1, "new", Some(1.0))]);
    let again = insert(&mut e, "t", vec![row(t + 2, "k0", Some(1.0)), row(t + 3, "k1", Some(1.0))]);
    let x: Vec<_> = again.iter().map(|r| r["x"].as_f64()).collect();
    assert_eq!(x, [Some(3.0), Some(1.0)], "k0 kept its count, k1 was evicted and counts from 1 again");
    let json = serde_json::to_value(e.snapshot()).unwrap();
    let g = &json[0][0]["Over"]["groups"][0];
    assert_eq!(g["evicted"].as_u64(), Some(n as u64 / 10));
    assert_eq!(g["parts"].as_array().unwrap().len(), n - n / 10 + 2);
}

/// A RANGE frame keeps at most `over::MAX_ROWS` rows; what it drops early is counted, and only
/// for RANGE frames (a ROWS frame drops its oldest row by definition).
#[test]
fn range_frames_are_bounded_and_count_what_they_drop() {
    let cols = [
        ("r".to_string(), "count(p) OVER (ORDER BY ts RANGE INTERVAL 1 HOUR PRECEDING)".to_string()),
        ("w".to_string(), "count(p) OVER (ORDER BY ts ROWS 1 PRECEDING)".to_string()),
    ];
    let mut e = engine(&view(&cols, "")).unwrap();
    let n = brrrrr_core::over::MAX_ROWS;
    let got = insert(&mut e, "t", (0..n + 3).map(|i| row(i as i64, "a", Some(1.0))).collect());
    assert_eq!(got.last().unwrap()["r"].as_f64(), Some(n as f64));
    assert_eq!(got.last().unwrap()["w"].as_f64(), Some(2.0));
    let json = serde_json::to_value(e.snapshot()).unwrap();
    assert_eq!(json[0][0]["Over"]["groups"][0]["truncated"].as_u64(), Some(3));
}

/// RANGE offsets in every unit and spelling: a plain number (in the ORDER BY's units, here
/// microseconds), `INTERVAL '2' UNIT` and `INTERVAL 2 UNIT` (the parser wants the unit outside
/// the quotes).
#[test]
fn range_offsets_in_every_unit() {
    let units = [
        ("MICROSECOND", 1),
        ("MILLISECOND", 1_000),
        ("SECOND", 1_000_000),
        ("MINUTE", 60_000_000),
        ("HOUR", 3_600_000_000),
        ("DAY", 86_400_000_000i64),
    ];
    for (unit, us) in units {
        let spellings = [format!("{}", 2 * us), format!("INTERVAL '2' {unit}"), format!("INTERVAL 2 {unit}")];
        for offset in spellings {
            let cols = [("n".to_string(), format!("count() OVER (ORDER BY ts RANGE {offset} PRECEDING)"))];
            let mut e = engine(&view(&cols, "")).unwrap_or_else(|e| panic!("{offset}: {e}"));
            // the frame of the row at 2us + 1 no longer holds the row at 0
            let got = insert(&mut e, "t", [0, 2 * us, 2 * us + 1].map(|t| row(t, "a", Some(1.0))).into());
            let n: Vec<_> = got.iter().map(|r| r["n"].as_f64().unwrap()).collect();
            assert_eq!(n, [1.0, 2.0, 2.0], "{offset}");
        }
    }
}

/// Two-argument aggregates over a running and a sliding frame, a parametric aggregate, and the
/// hour and minute anchors.
#[test]
fn multi_argument_and_parametric_aggregates_and_anchors() {
    let hour = 3_600_000_000i64;
    let cols: Vec<(String, String)> = [
        ("vw", "vwap(p, p) OVER (PARTITION BY g ORDER BY ts)"),
        ("vw2", "vwap(p, p) OVER (PARTITION BY g ORDER BY ts ROWS 1 PRECEDING)"),
        // recomputed over the frame: the two arguments of each of its rows
        ("am", "arg_min(p * 10, p) OVER (PARTITION BY g ORDER BY ts ROWS 1 PRECEDING)"),
        ("q", "quantile(0.5)(p) OVER (PARTITION BY g ORDER BY ts)"),
        ("first", "first_value(p) OVER (PARTITION BY g ORDER BY ts ROWS 1 PRECEDING)"),
        ("last", "last_value(p) OVER (PARTITION BY g ORDER BY ts ROWS 1 PRECEDING)"),
        ("h", "count() OVER (PARTITION BY to_start_of_hour(ts) ORDER BY ts)"),
        ("m", "count() OVER (PARTITION BY to_start_of_minute(ts) ORDER BY ts)"),
    ]
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .into();
    let mut e = engine(&view(&cols, "")).unwrap();
    let rows = [(hour + 1, 1.0), (hour + 2, 3.0), (hour + 60_000_001, 5.0), (2 * hour + 1, 7.0)];
    let got = insert(&mut e, "t", rows.map(|(t, p)| row(t, "a", Some(p))).into());
    let col = |c: &str| got.iter().map(|r| r[c].as_f64().unwrap()).collect::<Vec<_>>();
    // sum(p²) / sum(p)
    assert_eq!(col("vw"), [1.0, 10.0 / 4.0, 35.0 / 9.0, 84.0 / 16.0]);
    assert_eq!(col("vw2"), [1.0, 10.0 / 4.0, 34.0 / 8.0, 74.0 / 12.0]);
    assert_eq!(col("am"), [10.0, 10.0, 30.0, 50.0]);
    assert_eq!(col("q"), [1.0, 2.0, 3.0, 4.0]);
    assert_eq!(col("first"), [1.0, 1.0, 3.0, 5.0]);
    assert_eq!(col("last"), [1.0, 3.0, 5.0, 7.0]);
    assert_eq!(col("h"), [1.0, 2.0, 3.0, 1.0]);
    assert_eq!(col("m"), [1.0, 2.0, 1.0, 1.0]);
}

/// A sliding min or max finds what recomputing it over the frame finds, also on values that
/// compare equal but differ (0 and -0: the oldest wins) and on values that compare with nothing
/// (NaN: one first in the frame is the result, one after it is skipped), as frames slide over
/// them: [3, NaN, 1] is 1, [NaN, 1] and [NaN, 1, 2, NaN] are NaN (NULL once written).
#[test]
fn sliding_min_and_max_equal_a_recomputation_on_ties_and_nan() {
    let nan = f64::NAN;
    let ps = [0.0, -0.0, 0.0, 3.0, nan, 1.0, 2.0, nan, 1.5, 4.0, -0.0, 0.0, -0.0, nan, nan, 5.0, 6.0, 5.5];
    let frames = ["rows 1", "rows 2", "rows 3"];
    let cols: Vec<(String, String)> = frames
        .iter()
        .flat_map(|fr| {
            let n = fr.split_once(' ').unwrap().1;
            ["min", "max"].map(|f| (format!("{f}{n}"), format!("{f}(p) OVER (ORDER BY ts ROWS {n} PRECEDING)")))
        })
        .collect();
    let mut e = engine(&view(&cols, "")).unwrap();
    let got = insert(&mut e, "t", ps.iter().enumerate().map(|(i, p)| row(i as i64, "a", Some(*p))).collect());
    let rows: Vec<(i64, String, Value)> =
        ps.iter().enumerate().map(|(i, p)| (i as i64, "a".into(), Value::F64(*p))).collect();
    for (i, g) in got.iter().enumerate() {
        for fr in frames {
            for f in ["min", "max"] {
                let mut acc = Acc::new(f, &[], 1).unwrap();
                reference(&rows, fr, i).into_iter().for_each(|v| acc.add(v));
                let want = acc.result().f64().filter(|x| !x.is_nan()).map(f64::to_bits);
                let name = format!("{f}{}", fr.split_once(' ').unwrap().1);
                let have = g[name.as_str()].as_f64().map(f64::to_bits);
                assert_eq!(have, want, "{f} over {fr} at row {i}: {}", g[name.as_str()]);
            }
        }
    }
}

/// A sliding sum of UInt64 is a UInt64 and wraps as one, as `Acc` sums it: past 2^63 it is not
/// read as an Int64.
#[test]
fn sliding_uint64_sums_stay_uint64() {
    let cols = [("s".to_string(), "sum(p) OVER (ORDER BY ts ROWS 1 PRECEDING)".to_string())];
    let mut e = engine(&view(&cols, "").replace("p nullable(float64));", "p uint64);")).unwrap();
    let big = 1u64 << 63;
    let ps = [big + 1, big + 3, 5, 7];
    let rows = ps.iter().enumerate().map(|(i, p)| vec![Value::Time(i as i64), Value::Str("a".into()), Value::UInt(*p)]);
    let got = insert(&mut e, "t", rows.collect());
    let s: Vec<_> = got.iter().map(|r| r["s"].as_f64().unwrap()).collect();
    assert_eq!(s, [big + 1, 4, big + 8, 12].map(|u| u as f64));
}
