//! `quantile_cont(level)(x)` is DuckDB's `PERCENTILE_CONT(level) WITHIN GROUP (ORDER BY x)`, and
//! at 0.5 its `MEDIAN`. Held to what
//! DuckDB 1.5.5 itself returns (fixtures/duckdb-vectors/quantiles.jsonl.gz,
//! scripts/record-duckdb-quantiles.py): to the bit while a window holds at most
//! `agg::CONT_EXACT` values, and within the t-digest's error above that, where it must also be
//! no further from DuckDB than `quantile_t_digest` is.
//!
//! Why it exists: `quantile_t_digest` returns one of a window's values, never the
//! midpoint of two: its median of two trades of 132.7346 and 16.4054 USD is 16.4054 where
//! `MEDIAN` is 74.57, and a slippage median in basis points could take the other sign.
use brrrrr_core::agg::{Acc, CONT_EXACT};
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::{Emit, Engine};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;
use std::io::BufRead;

/// A t-digest merges its values every this many as they come (`agg`'s `BUFFER`, ADR-0014): past
/// `CONT_EXACT`, `quantile_cont`'s digest too.
const MERGES_EVERY: usize = 512;

const LEVELS: [&str; 9] = ["0.0", "0.05", "0.25", "0.5", "0.75", "0.9", "0.95", "0.99", "1.0"];

/// Values in arrival order (NULL is `None`), or results.
type Numbers = Vec<Option<f64>>;

/// DuckDB's windows: the values and its result per level, MEDIAN's first.
fn vectors() -> Vec<(Numbers, Numbers)> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/duckdb-vectors/quantiles.jsonl.gz");
    let file = flate2::read::GzDecoder::new(std::fs::File::open(path).unwrap());
    let number = |j: &serde_json::Value| match j.as_str().unwrap() {
        "null" => None,
        "nan" => Some(f64::NAN),
        hex => Some(f64::from_bits(u64::from_str_radix(hex, 16).unwrap())),
    };
    std::io::BufReader::new(file)
        .lines()
        .map(|l| {
            let case: serde_json::Value = serde_json::from_str(&l.unwrap()).unwrap();
            let xs = case["xs"].as_array().unwrap().iter().map(number).collect();
            let results = std::iter::once("median").chain(LEVELS).map(|l| number(&case[l])).collect();
            (xs, results)
        })
        .collect()
}

fn aggregate(name: &str, level: f64, xs: &[Option<f64>]) -> Value {
    let mut acc = Acc::new(name, &[Value::F64(level)], 1).unwrap();
    xs.iter().for_each(|x| acc.add(x.map_or(Value::Null, Value::F64)));
    acc.result()
}

fn cont(level: f64, xs: &[Option<f64>]) -> Option<f64> {
    match aggregate("quantile_cont", level, xs) {
        Value::Null => None,
        Value::F64(v) => Some(v),
        other => panic!("quantile_cont is a Float64 or NULL, got {other:?}"),
    }
}

/// The same number: the same bits, a NaN for a NaN, and a zero of either sign for a zero (DuckDB
/// takes whichever of `-0.0` and `0.0` its selection left at the rank: they compare equal).
fn same(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()) || (a == 0.0 && b == 0.0),
        (a, b) => a.is_none() && b.is_none(),
    }
}

/// The values DuckDB's quantiles are of: NULLs and NaNs are skipped.
fn kept(xs: &[Option<f64>]) -> Vec<f64> {
    xs.iter().flatten().copied().filter(|x| !x.is_nan()).collect()
}

/// Every window of at most `CONT_EXACT` values has DuckDB's result to the bit, at every level,
/// whatever order the values arrive in; MEDIAN is the level 0.5.
#[test]
fn small_windows_have_duckdbs_median_and_percentiles_to_the_bit() {
    let (mut sizes, mut compared) = (std::collections::BTreeSet::new(), 0);
    for (xs, results) in vectors() {
        let n = kept(&xs).len();
        if n > CONT_EXACT {
            continue;
        }
        sizes.insert(n);
        assert!(same(results[0], results[4]), "DuckDB's MEDIAN is its PERCENTILE_CONT(0.5)");
        for (level, want) in LEVELS.iter().zip(&results[1..]) {
            let got = cont(level.parse().unwrap(), &xs);
            assert!(same(got, *want), "{n} values at {level}: brrrrr {got:?}, DuckDB {want:?} of {xs:?}");
            compared += 1;
        }
    }
    // the sizes the issue names, every size a thin window has, and the buffer's last two
    for n in [0, 1, 2, 3, 4, 5, CONT_EXACT - 1, CONT_EXACT] {
        assert!(sizes.contains(&n), "no window of {n} values");
    }
    assert!((1..=40).all(|n| sizes.contains(&n)) && compared > 4_000, "{compared} results of {sizes:?}");
}

/// The window the issue found: two trades of 132.7346 and 16.4054 USD have the median 74.57,
/// their midpoint, in either order. The t-digest has the smaller one.
#[test]
fn the_median_of_two_trades_is_their_midpoint() {
    for xs in [[Some(132.7346), Some(16.4054)], [Some(16.4054), Some(132.7346)]] {
        assert_eq!(cont(0.5, &xs), Some(74.57));
        assert_eq!(aggregate("quantile_t_digest", 0.5, &xs), Value::F32(16.4054));
    }
    // slippage in basis points around a quote: the median of -1 and +2 bps is +0.5, not -1
    assert_eq!(cont(0.5, &[Some(2.0), Some(-1.0)]), Some(0.5));
    assert_eq!(aggregate("quantile_t_digest", 0.5, &[Some(2.0), Some(-1.0)]), Value::F32(-1.0));
}

/// Relative distance, on the scale of the window's own values.
fn error(got: f64, want: f64, scale: f64) -> f64 {
    (got - want).abs() / scale
}

/// Past `CONT_EXACT` values the result is a t-digest's, read as PERCENTILE_CONT reads values.
/// It stays within the digest's rank error of DuckDB's (the values 1% of the window's count
/// either side of the rank, and Float32's rounding), and is never further from DuckDB than
/// `quantile_t_digest` of the same values is by more than that rounding: on these windows it
/// is closer in total at every level. Windows either side of the digest's first two merges
/// (`MERGES_EVERY`) are among them.
#[test]
fn large_windows_stay_within_the_digests_error_of_duckdb() {
    let (mut windows, mut sizes) = (0, std::collections::BTreeSet::new());
    let (mut total_cont, mut total_digest) = ([0f64; 9], [0f64; 9]);
    for (xs, results) in vectors() {
        let mut sorted = kept(&xs);
        let n = sorted.len();
        if n <= CONT_EXACT {
            continue;
        }
        windows += 1;
        sizes.insert(n);
        sorted.sort_by(f64::total_cmp);
        let scale = sorted[n - 1].abs().max(sorted[0].abs());
        for (i, (level, want)) in LEVELS.iter().zip(&results[1..]).enumerate() {
            let (level, want) = (level.parse::<f64>().unwrap(), want.unwrap());
            let got = cont(level, &xs).unwrap();
            let Value::F32(digest) = aggregate("quantile_t_digest", level, &xs) else { panic!("a digest") };
            let rank = level * (n - 1) as f64;
            let slack = (0.01 * n as f64).ceil() as usize;
            let lo = sorted[(rank.floor() as usize).saturating_sub(slack)];
            let hi = sorted[(rank.ceil() as usize + slack).min(n - 1)];
            let rounding = 1e-6 * scale;
            assert!(
                lo - rounding <= got && got <= hi + rounding,
                "{n} values at {level}: {got} outside [{lo}, {hi}], DuckDB {want}"
            );
            total_cont[i] += error(got, want, scale);
            total_digest[i] += error(digest as f64, want, scale);
        }
    }
    assert!(windows >= 30, "{windows} windows past the exact buffer");
    let m = MERGES_EVERY;
    for n in [m - 1, m, m + 1, 2 * m - 1, 2 * m, 2 * m + 1] {
        assert!(sizes.contains(&n), "no window of {n} values, at the digest's merges");
    }
    println!("summed relative error per level, quantile_cont: {total_cont:?}\nquantile_t_digest: {total_digest:?}");
    for (i, level) in LEVELS.iter().enumerate() {
        assert!(
            total_cont[i] <= total_digest[i] + 1e-6 * windows as f64,
            "at {level}: quantile_cont is {} from DuckDB in total, quantile_t_digest {}",
            total_cont[i],
            total_digest[i]
        );
    }
}

/// The first window past the buffer holds `CONT_EXACT + 1` values, each its own centroid: the
/// result is PERCENTILE_CONT of those values as Float32, so within Float32's rounding of
/// DuckDB's, not a digest's approximation.
#[test]
fn the_first_windows_past_the_buffer_are_exact_in_float32() {
    let mut seen = 0;
    for (xs, results) in vectors() {
        let values = kept(&xs);
        // the median's error bound covers two values from 200 on; the buffer's first digest
        // holds CONT_EXACT + 1
        if values.len() <= CONT_EXACT || values.len() > CONT_EXACT + 2 {
            continue;
        }
        seen += 1;
        let scale = values.iter().fold(0f64, |m, x| m.max(x.abs()));
        for (level, want) in LEVELS.iter().zip(&results[1..]) {
            // the tails keep single values far longer than the middle does
            if !["0.0", "0.05", "0.95", "0.99", "1.0"].contains(level) {
                continue;
            }
            let got = cont(level.parse().unwrap(), &xs).unwrap();
            assert!(error(got, want.unwrap(), scale) <= 1.2e-7, "{level}: {got}, DuckDB {want:?}");
        }
    }
    assert!(seen >= 6, "{seen} windows just past the buffer");
}

const SQL: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, side string, x nullable(float64));
CREATE EXTERNAL STREAM IF NOT EXISTS out (symbol string, n uint64, median nullable(float64), p95 nullable(float64),
  buys nullable(float64), digest nullable(float32))
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS
SELECT symbol, count() AS n, quantile_cont(0.5)(x) AS median, quantile_cont(0.95)(x) AS p95,
  quantile_cont_if(0.5)(x, side = 'buy') AS buys, quantile_t_digest(0.5)(x) AS digest
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

fn engine() -> Engine {
    Engine::new(&parse(SQL).unwrap()).unwrap()
}

/// `xs` as symbol A's trades of the first minute, every third one a sell.
fn trades(xs: &[Option<f64>]) -> Vec<Vec<Value>> {
    xs.iter()
        .enumerate()
        .map(|(i, x)| {
            let side = if i % 3 == 2 { "sell" } else { "buy" };
            let time = Value::Time(i as i64 * 1_000);
            vec![time, Value::Str("A".into()), Value::Str(side.into()), x.map_or(Value::Null, Value::F64)]
        })
        .collect()
}

/// A trade of the next hour: closes the window.
fn closing() -> Vec<Vec<Value>> {
    vec![vec![Value::Time(3_600_000_000), Value::Str("Z".into()), Value::Str("buy".into()), Value::F64(1.0)]]
}

fn insert(e: &mut Engine, rows: Vec<Vec<Value>>) -> Vec<Emit> {
    let mut out = vec![];
    e.insert("trades", rows, &mut out);
    out
}

/// The window's row, once closed.
fn closed(e: &mut Engine) -> serde_json::Value {
    let out = insert(e, closing());
    assert_eq!(out.len(), 1, "{out:?}");
    serde_json::from_str(&out[0].payload).unwrap()
}

/// PERCENTILE_CONT as DuckDB defines it, written out: the oracle of the tests below.
fn percentile_cont(xs: &[Option<f64>], level: f64) -> Option<f64> {
    let mut v = kept(xs);
    v.sort_by(f64::total_cmp);
    let rank = (v.len().checked_sub(1)? as f64) * level;
    let (lo, hi) = (v[rank.floor() as usize], v[rank.ceil() as usize]);
    Some(if lo == hi { lo } else { lo * (1.0 - (rank - rank.floor())) + hi * (rank - rank.floor()) })
}

/// A notional-like value per position, none repeated.
fn notionals(n: usize) -> Vec<Option<f64>> {
    (0..n).map(|i| Some(5.0 + ((i * 7_919) % 10_007) as f64 * 1.37)).collect()
}

/// Through SQL, as a pipeline calls it: the window's median and p95 are DuckDB's for every thin
/// window, `quantile_cont_if` is the median of the rows its condition keeps, and a window of
/// NULLs has NULL.
#[test]
fn a_pipelines_thin_windows_publish_duckdbs_values() {
    let mut e = engine();
    insert(&mut e, trades(&[Some(132.7346), Some(16.4054)]));
    let want = r#"{"symbol":"A","n":2,"median":74.57,"p95":126.91814,"buys":74.57,"digest":16.4054}"#;
    assert_eq!(insert(&mut e, closing())[0].payload.trim_end(), want);
    for n in [1, 2, 3, 4, 5, 17, CONT_EXACT - 1, CONT_EXACT] {
        let xs = notionals(n);
        let mut e = engine();
        insert(&mut e, trades(&xs));
        let row = closed(&mut e);
        let buys: Vec<Option<f64>> = xs.iter().enumerate().filter(|(i, _)| i % 3 != 2).map(|(_, x)| *x).collect();
        assert_eq!(row["n"], n, "{row}");
        assert_eq!(row["median"].as_f64(), percentile_cont(&xs, 0.5), "{n} values: {row}");
        assert_eq!(row["p95"].as_f64(), percentile_cont(&xs, 0.95), "{n} values: {row}");
        assert_eq!(row["buys"].as_f64(), percentile_cont(&buys, 0.5), "{n} values: {row}");
    }
    // a window whose values are all NULL (rows counted, nothing to take a quantile of)
    let mut e = engine();
    insert(&mut e, trades(&[None, None, None]));
    let want = r#"{"symbol":"A","n":3,"median":null,"p95":null,"buys":null,"digest":null}"#;
    assert_eq!(insert(&mut e, closing())[0].payload.trim_end(), want);
    // NULLs among values are skipped
    let mut e = engine();
    insert(&mut e, trades(&[None, Some(-3.0), None, Some(1.0), Some(f64::NAN)]));
    let row = closed(&mut e);
    let two = [Some(-3.0), Some(1.0)];
    assert_eq!((row["median"].as_f64(), row["p95"].as_f64()), (Some(-1.0), percentile_cont(&two, 0.95)), "{row}");
}

/// The median and the p95 of one argument read one state (slippage's two columns): the second
/// is an `Acc::QuantileOf`, and the values are kept once.
#[test]
fn two_levels_of_one_argument_keep_its_values_once() {
    let mut e = engine();
    insert(&mut e, trades(&notionals(10)));
    let state = serde_json::to_value(e.snapshot()).unwrap();
    let accs = state[0][0]["Window"]["open"][0][1][1].as_array().unwrap();
    assert_eq!(accs[1]["Cont"]["level"], 0.5);
    assert_eq!(accs[1]["Cont"]["values"]["Exact"].as_array().unwrap().len(), 10);
    assert_eq!(accs[2], serde_json::json!({"QuantileOf": {"level": 0.95, "of": 1}}));
    // the condition's rows are another argument: a state of its own
    assert_eq!(accs[3]["If"]["Cont"]["values"]["Exact"].as_array().unwrap().len(), 7);
}

/// A checkpoint taken after `cut` of a window's values, restored into a new engine, which gets
/// the rest: the window closes as it does uninterrupted, whether the checkpoint held exact
/// values or a digest, whichever side of the buffer's size the window ends on, and either side
/// of the digest's merges (`MERGES_EVERY`): cut with values unmerged or just merged.
#[test]
fn a_window_continues_across_a_checkpoint_on_either_side_of_the_buffer() {
    let (n, m) = (CONT_EXACT, MERGES_EVERY);
    for (cut, len) in [
        (1, 2),
        (3, 5),
        (n - 1, n),
        (n, n),
        (n - 1, n + 1),
        (n, n + 1),
        (n + 1, n + 1),
        (n + 1, 3 * n),
        (2 * n, 3 * n),
        (5, 3 * n),
        (m - 1, 2 * m + 1),
        (m, 2 * m + 1),
        (m + 1, 2 * m + 1),
        (2 * m - 1, 2 * m),
        (2 * m, 3 * m),
    ] {
        let xs = notionals(len);
        let mut whole = engine();
        insert(&mut whole, trades(&xs));
        let mut e = engine();
        insert(&mut e, trades(&xs)[..cut].to_vec());
        let bytes = Checkpoint::of(&e, 1, vec![], vec![]).encode();
        // what the checkpoint holds: the values themselves up to the buffer's size, then a digest
        let state = serde_json::to_value(Checkpoint::decode(&bytes).unwrap().state).unwrap();
        let values = &state[0][0]["Window"]["open"][0][1][1][1]["Cont"]["values"];
        if cut <= n {
            assert_eq!(values["Exact"].as_array().unwrap().len(), cut, "{cut} of {len}");
        } else {
            assert_eq!(values["Digest"]["count"], cut as f64, "{cut} of {len}");
        }
        let mut restored = engine();
        Checkpoint::decode(&bytes).unwrap().restore(&mut restored).unwrap();
        assert_eq!(Checkpoint::of(&restored, 1, vec![], vec![]).encode(), bytes, "{cut} of {len}");
        insert(&mut restored, trades(&xs)[cut..].to_vec());
        let (got, want) = (closed(&mut restored), closed(&mut whole));
        assert_eq!(got, want, "{cut} of {len}");
        if len <= n {
            assert_eq!(got["median"].as_f64(), percentile_cont(&xs, 0.5), "{cut} of {len}");
            assert_eq!(got["p95"].as_f64(), percentile_cont(&xs, 0.95), "{cut} of {len}");
        }
    }
}

/// One value past the buffer, the window's values are a t-digest's: the one `quantile_t_digest`
/// keeps of the same values in the same order, so the state is as bounded as its state is,
/// either side of its merges (`MERGES_EVERY`) too.
#[test]
fn past_the_buffer_the_state_is_quantile_t_digests() {
    let digest_of = |n: usize| {
        let mut e = engine();
        insert(&mut e, trades(&notionals(n)));
        let state = serde_json::to_value(e.snapshot()).unwrap();
        let accs = &state[0][0]["Window"]["open"][0][1][1];
        (accs[1]["Cont"]["values"]["Digest"].clone(), accs[4]["TDigest"]["digest"].clone())
    };
    assert!(digest_of(CONT_EXACT).0.is_null(), "exact up to the buffer's size");
    let m = MERGES_EVERY;
    for n in [CONT_EXACT + 1, m - 1, m, m + 1, 2 * m - 1, 2 * m, 2 * m + 1, 2_048, 2_049, 30_000] {
        let (cont, digest) = digest_of(n);
        assert!(!cont.is_null() && cont == digest, "{n} values: {cont} is not {digest}");
    }
    // bounded: 30,000 values are no more state than 3,000
    let bytes = |n: usize| postcard::to_allocvec(&engine_with(n).snapshot()).unwrap().len();
    assert!(bytes(30_000) < 2 * bytes(3_000), "{} bytes for 30,000 values, {} for 3,000", bytes(30_000), bytes(3_000));
}

fn engine_with(n: usize) -> Engine {
    let mut e = engine();
    insert(&mut e, trades(&notionals(n)));
    e
}

/// The level is a number in [0, 1], as the other quantiles'; the aggregate has one argument.
#[test]
fn the_level_and_the_argument_are_checked() {
    let err = |call: &str| Engine::new(&parse(&SQL.replace("quantile_cont(0.95)(x)", call)).unwrap()).err().unwrap();
    assert!(err("quantile_cont(1.5)(x)").contains("outside [0, 1]"), "{}", err("quantile_cont(1.5)(x)"));
    assert!(err("quantile_cont('a')(x)").contains("one numeric level"), "{}", err("quantile_cont('a')(x)"));
    assert!(err("quantile_cont(0.5)(x, x)").contains("expects 1 argument"), "{}", err("quantile_cont(0.5)(x, x)"));
    assert!(err("quantile_cont(0.5)(side)").contains("of a string"), "{}", err("quantile_cont(0.5)(side)"));
    // no level is the median
    let mut e = Engine::new(&parse(&SQL.replace("quantile_cont(0.95)(x)", "quantile_cont(x)")).unwrap()).unwrap();
    insert(&mut e, trades(&[Some(1.0), Some(4.0)]));
    assert_eq!(closed(&mut e)["p95"], 2.5);
}

/// A restored state the aggregate could not have made is refused: another level, more exact
/// values than the buffer holds, or a digest with more unmerged values than one keeps.
#[test]
fn an_inconsistent_state_is_refused() {
    let edited = |n: usize, edit: &dyn Fn(&mut serde_json::Value)| {
        let mut j = serde_json::to_value(engine_with(n).snapshot()).unwrap();
        edit(&mut j[0][0]["Window"]["open"][0][1][1][1]["Cont"]);
        engine().restore(serde_json::from_value(j).unwrap())
    };
    assert!(edited(3, &|_| ()).is_ok());
    assert!(edited(CONT_EXACT + 1, &|_| ()).is_ok());
    let full = |n: usize| serde_json::json!(vec![1.5; n]);
    assert!(edited(3, &|c| c["values"]["Exact"] = full(CONT_EXACT)).is_ok());
    for (n, edit) in [
        (3, (&|c: &mut serde_json::Value| c["level"] = serde_json::json!(0.25)) as &dyn Fn(&mut serde_json::Value)),
        (3, &|c| c["values"]["Exact"] = full(CONT_EXACT + 1)),
        (CONT_EXACT + 1, &|c| c["values"]["Digest"]["unmerged"] = serde_json::json!(vec![1.0; 2_049])),
    ] {
        let err = edited(n, edit).unwrap_err();
        assert!(err.contains("an accumulator"), "{err}");
    }
}
