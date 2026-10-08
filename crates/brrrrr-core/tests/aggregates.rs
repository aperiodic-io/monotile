//! The two-argument aggregates (QuestDB's `vwap`, `weighted_avg`, `twap`, `arg_max`, `arg_min`
//! and `corr`), held to what QuestDB 10.0.1 returns for the same rows: every expected value
//! below is QuestDB's output for this input, bit for bit.
use brrrrr_core::engine::{Emit, Engine, State};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;

const SQL: &str = "
CREATE STREAM IF NOT EXISTS t (ts datetime64(6), g string, p nullable(float64), q nullable(float64));
CREATE EXTERNAL STREAM IF NOT EXISTS out (g string, vwap nullable(float64), wavg nullable(float64),
  twap nullable(float64), amax nullable(float64), amin nullable(float64), amaxq nullable(float64),
  c nullable(float64), c0 float64, n uint64)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO out AS
SELECT g, vwap(p, q) AS vwap, weighted_avg(p, q) AS wavg, twap(p, ts) AS twap, arg_max(p, q) AS amax,
  arg_min(p, q) AS amin, arg_max(q, p) AS amaxq, corr(p, q) AS c,
  coalesce(corr(p, q), -1) AS c0, count() AS n
FROM tumble(t, ts, 1m)
GROUP BY window_start, g
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND;
";

fn row(sec: i64, g: &str, p: Option<f64>, q: Option<f64>) -> Vec<Value> {
    let v = |x: Option<f64>| x.map_or(Value::Null, Value::F64);
    vec![Value::Time(sec * 1_000_000), Value::Str(g.into()), v(p), v(q)]
}

/// The rows QuestDB was given, in its timestamp order: group a has a zero weight, a NULL price
/// and a NULL weight; b a lone zero weight; c one row; d two rows at one time; e a price that
/// does not move.
fn rows() -> Vec<Vec<Value>> {
    vec![
        row(0, "a", Some(10.0), Some(1.0)),
        row(1, "a", Some(20.0), Some(3.0)),
        row(4, "a", Some(30.0), Some(2.0)),
        row(4, "a", Some(5.0), Some(0.0)),
        row(10, "a", None, Some(4.0)),
        row(11, "a", Some(40.0), None),
        row(0, "b", Some(7.0), Some(0.0)),
        row(0, "c", Some(9.0), Some(2.0)),
        row(2, "d", Some(1.0), Some(1.0)),
        row(2, "d", Some(3.0), Some(1.0)),
        row(3, "e", Some(5.0), Some(1.0)),
        row(5, "e", Some(5.0), Some(2.0)),
    ]
}

/// A row in the next minute: closes the window.
fn closing() -> Vec<Vec<Value>> {
    vec![row(120, "z", Some(1.0), Some(1.0))]
}

fn insert(e: &mut Engine, rows: Vec<Vec<Value>>) -> Vec<Emit> {
    let mut out = vec![];
    e.insert("t", rows, &mut out);
    out
}

fn by_group(out: &[Emit]) -> Vec<serde_json::Value> {
    let mut v: Vec<serde_json::Value> = out.iter().map(|e| serde_json::from_str(&e.payload).unwrap()).collect();
    v.sort_by(|a, b| a["g"].as_str().cmp(&b["g"].as_str()));
    v
}

#[test]
fn two_argument_aggregates_match_questdb() {
    let mut e = Engine::new(&parse(SQL).unwrap()).unwrap();
    insert(&mut e, rows());
    let got = by_group(&insert(&mut e, closing()));
    let (null, f) = (None, Some);
    // g, vwap, weighted_avg, twap, arg_max(p, q), arg_min(p, q), arg_max(q, p), corr,
    // coalesce(corr, -1) (NULL, not NaN, where there is no correlation), count
    let r = 0.7568892626614566;
    let want = [
        ("a", f(130.0 / 6.0), f(130.0 / 6.0), f(105.0 / 11.0), null, f(5.0), null, f(r), f(r), 6),
        ("b", null, null, f(7.0), f(7.0), f(7.0), f(0.0), null, f(-1.0), 1),
        ("c", f(9.0), f(9.0), f(9.0), f(9.0), f(9.0), f(2.0), null, f(-1.0), 1),
        ("d", f(2.0), f(2.0), f(2.0), f(1.0), f(1.0), f(1.0), null, f(-1.0), 2),
        ("e", f(5.0), f(5.0), f(5.0), f(5.0), f(5.0), f(1.0), null, f(-1.0), 2),
    ];
    assert_eq!(got.len(), want.len(), "{got:?}");
    for (g, (name, vwap, wavg, twap, amax, amin, amaxq, c, c0, n)) in got.iter().zip(want) {
        assert_eq!(g["g"], name);
        for (col, want) in [("vwap", vwap), ("wavg", wavg), ("twap", twap), ("amax", amax), ("amin", amin)]
            .into_iter()
            .chain([("amaxq", amaxq), ("c", c), ("c0", c0)])
        {
            assert_eq!(g[col].as_f64(), want, "{name}.{col}");
        }
        assert_eq!(g["n"], n, "{name}.n");
    }
}

/// twap weighs each price by the time until the next row; a row earlier than the one before it
/// takes effect at that row's time instead of weighing back in time.
#[test]
fn twap_weighs_prices_by_how_long_they_held() {
    let mut e = Engine::new(&parse(SQL).unwrap()).unwrap();
    let rows = vec![
        row(0, "a", Some(10.0), Some(1.0)),
        row(10, "a", Some(20.0), Some(1.0)),
        row(5, "a", Some(40.0), Some(1.0)), // late: 40 holds from 10s, not 5s
        row(20, "a", Some(0.0), Some(1.0)),
    ];
    insert(&mut e, rows);
    let got = by_group(&insert(&mut e, closing()));
    // 10 for 10s, 20 for 0s, 40 for 10s, over 20s
    assert_eq!(got[0]["twap"].as_f64(), Some((10.0 * 10.0 + 40.0 * 10.0) / 20.0));
}

#[test]
fn two_argument_aggregates_check_their_arguments() {
    let view = |select: &str| {
        let sql = SQL.replace("vwap(p, q) AS vwap", &format!("{select} AS vwap"));
        Engine::new(&parse(&sql).unwrap()).err()
    };
    assert_eq!(view("vwap(p)").unwrap(), "v: vwap expects 2 arguments, got 1");
    assert_eq!(view("sum(p, q)").unwrap(), "v: sum expects 1 argument, got 2");
    assert_eq!(view("corr(p, q, p)").unwrap(), "v: corr expects 2 arguments, got 3");
    assert_eq!(view("vwap(g, q)").unwrap(), "v: vwap of a string: vwap(g, q)");
    assert_eq!(view("twap(p, g)").unwrap(), "v: twap of a string: twap(p, g)");
    // arg_max/arg_min order and return any value
    assert_eq!(view("arg_max(g, p)"), None);
    assert_eq!(view("arg_min(p, g)"), None);
}

/// A checkpoint taken mid-window restores into the output of an uninterrupted run; an arg_min
/// accumulator where the plan has arg_max is refused.
#[test]
fn two_argument_accumulators_survive_a_checkpoint() {
    let mut first = rows();
    let rest = first.split_off(5);
    let mut whole = Engine::new(&parse(SQL).unwrap()).unwrap();
    insert(&mut whole, rows());
    let want = by_group(&insert(&mut whole, closing()));

    let mut a = Engine::new(&parse(SQL).unwrap()).unwrap();
    insert(&mut a, first);
    let json = serde_json::to_value(a.snapshot()).unwrap();
    let mut b = Engine::new(&parse(SQL).unwrap()).unwrap();
    b.restore(serde_json::from_value(json.clone()).unwrap()).unwrap();
    insert(&mut b, rest);
    assert_eq!(by_group(&insert(&mut b, closing())), want);

    let flipped: State = serde_json::from_str(&json.to_string().replace("\"max\":true", "\"max\":false")).unwrap();
    let err = Engine::new(&parse(SQL).unwrap()).unwrap().restore(flipped).unwrap_err();
    assert!(err.contains("an accumulator Arg"), "{err}");
}
