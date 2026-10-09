//! Ranking functions over the rows of each time (cross-sections): `row_number`, `rank`,
//! `dense_rank`, `percent_rank`, `cume_dist` and `ntile`, held to two references: DuckDB's
//! answers to random queries over random tables (fixtures/duckdb-vectors/ranks.jsonl.gz,
//! scripts/record-duckdb-ranks.py), and the functions' definitions, counted out row against row
//! (`oracle`), over tables proptest makes. Each query runs over its table in batches of one row
//! and more, on one thread and on several, partitioned or not; and in the engine, live, a
//! checkpoint taken while a time's rows are held restores into the same output.
use brrrrr_core::column::Batch;
use brrrrr_core::engine::{Engine, Pool, Row, Serial, Source, Task};
use brrrrr_core::query::{compile, execute, split, text, Input, Table};
use brrrrr_core::value::{Type, Value};
use proptest::prelude::*;
use std::cmp::Ordering;
use std::io::BufRead;

struct Rows(Vec<Batch>);

impl Source for Rows {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        (!self.0.is_empty()).then(|| Ok(self.0.remove(0)))
    }
}

/// Jobs on scoped threads, in parts of one row.
struct Threads;

impl Pool for Threads {
    fn run<'a>(&self, jobs: Vec<Task<'a>>) {
        std::thread::scope(|s| {
            for j in jobs {
                s.spawn(j);
            }
        });
    }
    fn threads(&self) -> usize {
        4
    }
    fn part_rows(&self) -> usize {
        1
    }
}

/// The table every query reads: `rid` (its row's place in the table), a time `ts`, a sector
/// `g`, a float `x`, an integer `y` and a text `s`, each but `rid` and `ts` sometimes NULL.
fn columns() -> Vec<(String, Type)> {
    [("rid", Type::Int(64)), ("ts", Type::Time(6)), ("g", Type::Str), ("x", Type::F64), ("y", Type::Int(64))]
        .into_iter()
        .chain([("s", Type::Str)])
        .map(|(n, t)| (n.to_string(), t))
        .collect()
}

/// `sql` over `rows` as `t`, in batches of `batch` rows on `pool`: each result row as text.
fn run_on(sql: &str, rows: &[Row], pool: &dyn Pool, batch: usize) -> Result<Vec<Vec<String>>, String> {
    let cols = columns();
    let c = compile(sql, &mut |t| match t {
        Table::Named(n) if n == "t" => Ok(cols.clone()),
        t => Err(format!("no table {t}")),
    })?;
    let mut open = |_: &brrrrr_core::query::Source, _: &[bool], parts: Option<(usize, usize)>| {
        let batches = rows.chunks(batch).map(|r| Batch::from_rows(r, cols.len())).collect();
        let s: Input = Box::new(Rows(batches));
        Ok(match parts {
            Some((key, n)) => split(s, key, n),
            None => vec![s],
        })
    };
    let out = execute(&c, &mut open, pool).map_err(|e| format!("{e}\n{}", c.explain()))?;
    Ok(out.iter().map(|r| r.iter().map(text).collect()).collect())
}

/// `sql`'s rows, the same in batches of 1, 3 and 1,000 rows, on one thread and on four.
fn run(sql: &str, rows: &[Row]) -> Vec<Vec<String>> {
    let first = run_on(sql, rows, &Serial, 1000).unwrap_or_else(|e| panic!("{sql}\n{e}"));
    for (pool, batch) in [(&Serial as &dyn Pool, 1), (&Threads, 3), (&Threads, 1000)] {
        let again = run_on(sql, rows, pool, batch).unwrap_or_else(|e| panic!("{sql}\n{e}"));
        assert_eq!(again, first, "{sql}: in batches of {batch} on {} threads", pool.threads());
    }
    first
}

fn row(rid: i64, ts: i64, g: Option<&str>, x: Option<f64>, y: Option<i64>, s: Option<&str>) -> Row {
    let text = |v: Option<&str>| v.map_or(Value::Null, |v| Value::Str(v.into()));
    vec![
        Value::Int(rid),
        Value::Time(ts),
        text(g),
        x.map_or(Value::Null, Value::F64),
        y.map_or(Value::Null, Value::Int),
        text(s),
    ]
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter().map(|r| r.iter().map(|x| x.to_string()).collect()).collect()
}

const SEC: i64 = 1_000_000;

/// Returns ranked across symbols at each time: ties share a rank, and NULLs sort as the largest
/// value, as in the outermost ORDER BY (PostgreSQL's), unless NULLS FIRST or LAST says otherwise.
/// A ranking is keyed by its partition: on several threads, the table is split by its time.
#[test]
fn ranks_each_time_across_symbols() {
    let c = compile("SELECT rank() OVER (PARTITION BY ts, g ORDER BY x) AS r FROM t", &mut |_| Ok(columns())).unwrap();
    assert_eq!(c.partition.map(|p| p.into_values().collect::<Vec<_>>()), Some(vec!["ts".to_string()]));
    let rows = vec![
        row(0, SEC, Some("a"), Some(0.3), None, Some("A")),
        row(1, SEC, Some("a"), Some(-0.1), None, Some("B")),
        row(2, SEC, Some("b"), Some(0.3), None, Some("C")),
        row(3, SEC, Some("b"), None, None, Some("D")),
        row(4, 2 * SEC, Some("a"), Some(1.0), None, Some("A")),
        row(5, 2 * SEC, Some("a"), Some(2.0), None, Some("B")),
    ];
    assert_eq!(
        run(
            "SELECT s, rank() OVER w AS r, dense_rank() OVER w AS d, row_number() OVER w AS n, \
             percent_rank() OVER w AS p, cume_dist() OVER w AS c, ntile(3) OVER w AS q, \
             rank() OVER (PARTITION BY ts ORDER BY x DESC) AS nulls_first \
             FROM t WINDOW w AS (PARTITION BY ts ORDER BY x DESC NULLS LAST) ORDER BY ts, s",
            &rows
        ),
        strings(&[
            &["A", "1", "1", "1", "0", "0.5", "1", "2"],
            &["B", "3", "2", "3", "0.6666666666666666", "0.75", "2", "4"],
            &["C", "1", "1", "2", "0", "0.5", "1", "2"],
            &["D", "4", "3", "4", "1", "1", "3", "1"],
            &["A", "2", "2", "2", "1", "1", "2", "2"],
            &["B", "1", "1", "1", "0", "0.5", "1", "1"],
        ])
    );
}

fn f64_of(hex: &str) -> f64 {
    f64::from_bits(u64::from_str_radix(hex, 16).unwrap())
}

/// A JSON cell of the fixture as a value of `t`'s column `i`.
fn value(j: &serde_json::Value, i: usize) -> Value {
    match (j, i) {
        (serde_json::Value::Null, _) => Value::Null,
        (j, 1) => Value::Time(j.as_i64().unwrap()),
        (serde_json::Value::String(s), _) => Value::Str(s.as_str().into()),
        (serde_json::Value::Object(o), _) => Value::F64(f64_of(o["f64"].as_str().unwrap())),
        (j, _) => Value::Int(j.as_i64().unwrap()),
    }
}

/// Whether brrrrr's text of a cell is DuckDB's value: the same integer, text or NULL, or the
/// same float to the bit (one NaN as good as another).
fn same(got: &str, want: &serde_json::Value) -> bool {
    match want {
        serde_json::Value::Null => got == "NULL",
        serde_json::Value::String(s) => got == s,
        serde_json::Value::Object(o) => {
            let want = f64_of(o["f64"].as_str().unwrap());
            got.parse::<f64>().is_ok_and(|g| g.to_bits() == want.to_bits() || g.is_nan() && want.is_nan())
        }
        n => got.parse::<i64>().ok() == n.as_i64(),
    }
}

#[test]
fn every_ranking_gives_duckdb_s_answer() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/duckdb-vectors/ranks.jsonl.gz");
    let file = flate2::read::GzDecoder::new(std::fs::File::open(path).unwrap());
    let (mut cases, mut cells, mut wrong) = (0, 0, vec![]);
    for line in std::io::BufReader::new(file).lines() {
        let case: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let rows: Vec<Row> = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r.as_array().unwrap().iter().enumerate().map(|(i, j)| value(j, i)).collect())
            .collect();
        let sql = case["sql"].as_str().unwrap();
        let want = case["expected"].as_array().unwrap();
        let got = run(sql, &rows);
        cases += 1;
        cells += want.iter().map(|r| r.as_array().unwrap().len()).sum::<usize>();
        let ok = got.len() == want.len()
            && got.iter().zip(want).all(|(g, w)| {
                let w = w.as_array().unwrap();
                g.len() == w.len() && g.iter().zip(w).all(|(g, w)| same(g, w))
            });
        if !ok {
            wrong.push(format!("{sql}\n  DuckDB: {}\n  brrrrr: {got:?}", serde_json::Value::Array(want.clone())));
        }
    }
    assert!(wrong.is_empty(), "{} of {cases} queries differ from DuckDB:\n{}", wrong.len(), wrong.join("\n"));
    assert!(cases >= 1000 && cells > 40_000, "{cases} cases, {cells} cells");
}

/// A ranking call proptest makes: the function (`ntile`'s buckets, else 0), the PARTITION BY
/// keys after `ts` (columns of `t`), and the ORDER BY keys: (column, descending, NULLs first
/// as written: `None` for the default).
#[derive(Clone, Debug)]
struct Call {
    func: &'static str,
    buckets: i64,
    keys: Vec<usize>,
    order: Vec<(usize, bool, Option<bool>)>,
}

const NAMES: [&str; 6] = ["rid", "ts", "g", "x", "y", "s"];

impl Call {
    fn sql(&self) -> String {
        let f = match self.func {
            "ntile" => format!("ntile({})", self.buckets),
            f => format!("{f}()"),
        };
        let keys: String = self.keys.iter().map(|&k| format!(", {}", NAMES[k])).collect();
        let order: Vec<String> = (self.order.iter())
            .map(|&(k, desc, nulls)| {
                let nulls = nulls.map_or("", |first| if first { " NULLS FIRST" } else { " NULLS LAST" });
                format!("{}{}{nulls}", NAMES[k], if desc { " DESC" } else { "" })
            })
            .collect();
        format!("{f} OVER (PARTITION BY ts{keys} ORDER BY {})", order.join(", "))
    }

    /// Row `a` against row `b` by the definition of the ORDER BY: each key in turn, NULLs first
    /// or last as written (else as the largest value), NaN above every number, -0 equal to 0.
    fn cmp(&self, a: &Row, b: &Row) -> Ordering {
        for &(k, desc, nulls) in &self.order {
            let o = match (&a[k], &b[k]) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) if nulls.unwrap_or(desc) => Ordering::Less,
                (Value::Null, _) => Ordering::Greater,
                (_, Value::Null) if nulls.unwrap_or(desc) => Ordering::Greater,
                (_, Value::Null) => Ordering::Less,
                (x, y) => {
                    let o = match (x, y) {
                        (Value::F64(x), Value::F64(y)) => match (x.is_nan(), y.is_nan()) {
                            (true, true) => Ordering::Equal,
                            (true, false) => Ordering::Greater,
                            (false, true) => Ordering::Less,
                            _ => x.partial_cmp(y).unwrap(),
                        },
                        (Value::Int(x), Value::Int(y)) => x.cmp(y),
                        (Value::Str(x), Value::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
                        (x, y) => panic!("{x:?} against {y:?}"),
                    };
                    if desc {
                        o.reverse()
                    } else {
                        o
                    }
                }
            };
            if o.is_ne() {
                return o;
            }
        }
        Ordering::Equal
    }

    /// Its value for row `p` of `rows` (in the order they come), counted out against every
    /// other row of `p`'s partition: no sort.
    fn oracle(&self, rows: &[Row], p: usize) -> Value {
        let part: Vec<usize> =
            (0..rows.len()).filter(|&q| [1].iter().chain(&self.keys).all(|&k| rows[q][k] == rows[p][k])).collect();
        let n = part.len() as i64;
        let before = |q: usize| self.cmp(&rows[q], &rows[p]).is_lt();
        let rank = 1 + part.iter().filter(|&&q| before(q)).count() as i64;
        // ties in the order the rows came
        let number = 1 + part.iter().filter(|&&q| before(q) || self.cmp(&rows[q], &rows[p]).is_eq() && q < p).count();
        match self.func {
            "row_number" => Value::Int(number as i64),
            "rank" => Value::Int(rank),
            "dense_rank" => {
                // the distinct ORDER BY values before p's: one row of each
                let firsts = part
                    .iter()
                    .filter(|&&q| before(q) && part.iter().all(|&e| e >= q || self.cmp(&rows[e], &rows[q]).is_ne()));
                Value::Int(1 + firsts.count() as i64)
            }
            "percent_rank" => Value::F64(if n > 1 { (rank - 1) as f64 / (n - 1) as f64 } else { 0.0 }),
            "cume_dist" => {
                let peers_and_before = part.iter().filter(|&&q| self.cmp(&rows[q], &rows[p]).is_le()).count();
                Value::F64(peers_and_before as f64 / n as f64)
            }
            "ntile" => {
                // buckets of n / b rows, the first n % b of them a row more, filled in turn
                let (b, mut left, mut bucket) = (self.buckets, number as i64, 0);
                while left > 0 {
                    left -= n / b + i64::from(bucket < n % b);
                    bucket += 1;
                }
                Value::Int(bucket)
            }
            f => panic!("{f}"),
        }
    }
}

fn call() -> impl Strategy<Value = Call> {
    let func = prop::sample::select(vec!["row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile"]);
    let key = (prop::sample::select(vec![2usize, 3, 4, 5]), any::<bool>(), prop::option::of(any::<bool>()));
    (func, 1i64..9, prop::sample::subsequence(vec![2usize, 4], 0..=2), prop::collection::vec(key, 1..4))
        .prop_map(|(func, buckets, keys, order)| Call { func, buckets, keys, order })
}

fn table_rows() -> impl Strategy<Value = Vec<Row>> {
    let xs = [-0.0, 0.0, 1.5, -2.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.25];
    let r = (
        0i64..4,
        prop::option::of(prop::sample::select(vec!["a", "b", "c"])),
        prop::option::of(prop::sample::select(xs.to_vec())),
        prop::option::of(-2i64..3),
        prop::option::of(prop::sample::select(vec!["", "a", "B", "ab"])),
    );
    prop::collection::vec(r, 1..60).prop_map(|rs| {
        let rows = rs.into_iter().enumerate();
        rows.map(|(i, (t, g, x, y, s))| row(i as i64, (1 + t) * SEC, g, x, y, s)).collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(if cfg!(miri) { 2 } else { 200 }))]

    /// Every function, over tables of ties, NULLs, NaN and -0 in and out of time order, is
    /// what its definition counts out row against row.
    #[test]
    fn rankings_are_their_definitions(rows in table_rows(), calls in prop::collection::vec(call(), 1..5)) {
        let items: Vec<String> = calls.iter().enumerate().map(|(i, c)| format!("{} AS c{i}", c.sql())).collect();
        let sql = format!("SELECT rid, {} FROM t ORDER BY rid", items.join(", "));
        let got = run(&sql, &rows);
        // in time order, rows of one time in the order they came: a stable sort, as brrrrr's
        let mut sorted = rows.clone();
        sorted.sort_by_key(|r| r[1].i64());
        let mut want: Vec<(i64, Vec<String>)> = (0..sorted.len())
            .map(|p| {
                let rid = sorted[p][0].i64().unwrap();
                (rid, std::iter::once(rid.to_string()).chain(calls.iter().map(|c| text(&c.oracle(&sorted, p)))).collect())
            })
            .collect();
        want.sort_by_key(|w| w.0);
        let want: Vec<Vec<String>> = want.into_iter().map(|w| w.1).collect();
        prop_assert_eq!(got, want, "{}", sql);
    }
}

// ---- in the engine, live -------------------------------------------------------------------

fn engine(sql: &str) -> Engine {
    Engine::new(&brrrrr_core::sql::parse(sql).unwrap()).unwrap()
}

fn insert(e: &mut Engine, rows: Vec<Row>) -> Vec<serde_json::Value> {
    let mut out = vec![];
    e.insert("t", rows, &mut out);
    out.iter().map(|m| serde_json::from_str(&m.payload).unwrap()).collect()
}

fn close(e: &mut Engine) -> Vec<serde_json::Value> {
    let mut out = vec![];
    e.close_until(i64::MAX, &mut out);
    out.iter().map(|m| serde_json::from_str(&m.payload).unwrap()).collect()
}

const LIVE: &str = "
CREATE STREAM t (ts datetime64(6), g string, p nullable(float64));
CREATE EXTERNAL STREAM out (ts datetime64(6), g string, r int64, q int64, c float64, n int64)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW v INTO out AS
SELECT ts, g, rank() OVER w AS r, ntile(2) OVER w AS q, cume_dist() OVER w AS c,
       row_number() OVER (PARTITION BY ts ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS n
FROM t WINDOW w AS (PARTITION BY ts ORDER BY p DESC NULLS LAST);";

fn trade(sec: i64, g: &str, p: Option<f64>) -> Row {
    vec![Value::Time(sec * SEC), Value::Str(g.into()), p.map_or(Value::Null, Value::F64)]
}

fn fields(rows: &[serde_json::Value]) -> Vec<String> {
    rows.iter().map(|r| format!("{} {} {} {} {}", r["g"], r["r"], r["q"], r["c"], r["n"])).collect()
}

/// Live, a time's rows come out ranked when the next time's first row comes, in the order they
/// came; a row of an earlier time is late, dropped and counted; the last time's rows come at
/// the end. In the engine's own SQL, a `row_number()` ranks the rows of each time when its frame
/// is the whole partition (else it numbers the rows of a partition as they come).
#[test]
fn live_a_time_is_ranked_when_the_next_one_starts() {
    let mut e = engine(LIVE);
    assert!(insert(&mut e, vec![trade(1, "b", Some(2.0)), trade(1, "a", None)]).is_empty());
    assert!(insert(&mut e, vec![trade(1, "c", Some(5.0))]).is_empty(), "the time's rows held");
    let out = insert(&mut e, vec![trade(2, "a", Some(1.0)), trade(1, "d", Some(9.0)), trade(2, "b", Some(1.0))]);
    assert_eq!(fields(&out), ["\"b\" 2 1 0.6666666666666666 2", "\"a\" 3 2 1 1", "\"c\" 1 1 0.3333333333333333 3"]);
    assert_eq!(e.late(), 1, "d came after time 2's first row");
    assert_eq!(fields(&close(&mut e)), ["\"a\" 1 1 1 1", "\"b\" 1 2 1 2"]);
}

/// A checkpoint taken at any row, the rows of a time held among them, restores (through its
/// encoding) into exactly the output of an uninterrupted run.
#[test]
fn held_rows_survive_a_checkpoint_at_any_row() {
    let rows: Vec<Row> = (0..30)
        .map(|i| trade(i / 4, ["a", "b", "c"][i as usize % 3], (i % 5 != 0).then_some((i * 7 % 11) as f64)))
        .collect();
    let mut whole = engine(LIVE);
    let mut want = insert(&mut whole, rows.clone());
    want.extend(close(&mut whole));
    for cut in 0..rows.len() {
        let mut a = engine(LIVE);
        let mut got = insert(&mut a, rows[..cut].to_vec());
        let bytes = postcard::to_allocvec(&a.snapshot()).unwrap();
        let mut b = engine(LIVE);
        b.restore(postcard::from_bytes(&bytes).unwrap()).unwrap();
        got.extend(insert(&mut b, rows[cut..].to_vec()));
        got.extend(close(&mut b));
        assert_eq!(got, want, "cut at {cut}");
    }
}

/// Closing a quiet feed (`close_until`) does not take a window after the ranking past the time
/// the ranking still holds: its rows, out when the next time's come, are not late there.
#[test]
fn a_window_after_a_ranking_waits_for_its_held_rows() {
    let sql = "
CREATE STREAM t (ts datetime64(6), g string, p nullable(float64));
CREATE STREAM ranked (ts datetime64(6), g string, r int64);
CREATE EXTERNAL STREAM out (g string, n int64, best int64)
  SETTINGS type = 'kafka', topic = 'o', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW v INTO ranked AS
SELECT ts, g, rank() OVER (PARTITION BY ts ORDER BY p DESC) AS r FROM t;
CREATE MATERIALIZED VIEW w INTO out AS
SELECT g, count() AS n, min(r) AS best FROM tumble(ranked, ts, 10s) GROUP BY window_start, g
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' MILLISECOND;";
    let mut e = engine(sql);
    insert(&mut e, vec![trade(1, "a", Some(1.0)), trade(1, "b", Some(2.0)), trade(2, "a", Some(3.0))]);
    let mut out = vec![];
    e.close_until(60 * SEC, &mut out);
    assert!(out.is_empty(), "time 2's row is still held");
    let mut got = insert(&mut e, vec![trade(61, "a", Some(1.0))]);
    got.extend(close(&mut e));
    let got: Vec<String> = got.iter().map(|r| format!("{} {} {}", r["g"], r["n"], r["best"])).collect();
    assert_eq!(got, ["\"a\" 2 1", "\"b\" 1 1", "\"a\" 1 1"]);
    assert_eq!(e.late(), 0);
}

// ---- with the rest of the SQL --------------------------------------------------------------

fn prices() -> Vec<Row> {
    let p = [[10.0, 20.0, 5.0], [11.0, 19.0, 5.5], [12.1, 19.0, 5.5], [12.1, 22.8, 6.6]];
    let mut rows = vec![];
    for (t, ps) in p.iter().enumerate() {
        for (k, (g, px)) in ["a", "b", "c"].iter().zip(ps).enumerate() {
            rows.push(row((t * 3 + k) as i64, (t as i64 + 1) * 60 * SEC, Some(g), Some(*px), None, None));
        }
    }
    rows
}

/// Momentum quantiles: returns over `lag` in a subquery, ranked at each time; ranked beside
/// running window functions and `lead` in one SELECT.
#[test]
fn ranks_returns_from_a_subquery_and_beside_other_window_functions() {
    let rows = prices();
    assert_eq!(
        run(
            "SELECT epoch_us(ts) / 60000000 AS m, g, round(ret, 4) AS ret, \
             rank() OVER (PARTITION BY ts ORDER BY ret DESC) AS r, ntile(2) OVER (PARTITION BY ts ORDER BY ret) AS half \
             FROM (SELECT ts, g, x / lag(x) OVER (PARTITION BY g ORDER BY ts) - 1 AS ret FROM t) \
             ORDER BY m, g",
            &rows
        ),
        strings(&[
            // ties in the order the rows came, NULLs last
            &["1", "a", "NULL", "1", "1"],
            &["1", "b", "NULL", "1", "1"],
            &["1", "c", "NULL", "1", "2"],
            &["2", "a", "0.1", "1", "1"],
            &["2", "b", "-0.05", "3", "1"],
            &["2", "c", "0.1", "1", "2"],
            &["3", "a", "0.1", "1", "2"],
            &["3", "b", "0", "2", "1"],
            &["3", "c", "0", "2", "1"],
            &["4", "a", "0", "3", "1"],
            &["4", "b", "0.2", "1", "1"],
            &["4", "c", "0.2", "1", "2"],
        ])
    );
    assert_eq!(
        run(
            "SELECT g, x, x - lag(x) OVER (PARTITION BY g ORDER BY ts) AS d, lead(x) OVER (PARTITION BY g ORDER BY ts) AS next, \
             dense_rank() OVER (PARTITION BY ts ORDER BY x) AS r FROM t WHERE ts < '1970-01-01 00:03:00' ORDER BY ts, g",
            &rows
        ),
        strings(&[
            &["a", "10", "NULL", "11", "2"],
            &["b", "20", "NULL", "19", "3"],
            &["c", "5", "NULL", "5.5", "1"],
            &["a", "11", "1", "NULL", "2"],
            &["b", "19", "-1", "NULL", "3"],
            &["c", "5.5", "0.5", "NULL", "1"],
        ])
    );
}

/// What a ranking cannot be is refused with what to write instead.
#[test]
fn rankings_say_what_to_write() {
    let rows = prices();
    for (sql, why) in [
        ("SELECT rank() OVER (ORDER BY x) FROM t", "PARTITION BY a time first"),
        ("SELECT rank() OVER (PARTITION BY g ORDER BY x) FROM t", "PARTITION BY a time first"),
        ("SELECT ntile(4) OVER (PARTITION BY y ORDER BY x) FROM t", "PARTITION BY a time first"),
        ("SELECT rank() OVER (PARTITION BY ts) FROM t", "needs ORDER BY what it ranks by"),
        ("SELECT ntile(0) OVER (PARTITION BY ts ORDER BY x) FROM t", "a whole number of buckets from 1"),
        ("SELECT ntile(y) OVER (PARTITION BY ts ORDER BY x) FROM t", "a whole number of buckets from 1"),
        ("SELECT ntile(2.5) OVER (PARTITION BY ts ORDER BY x) FROM t", "a whole number of buckets from 1"),
        ("SELECT rank(x) OVER (PARTITION BY ts ORDER BY x) FROM t", "rank takes no arguments"),
        ("SELECT rank() OVER (PARTITION BY ts ORDER BY x ROWS 2 PRECEDING) FROM t", "takes no frame"),
        ("SELECT g, rank() OVER (PARTITION BY ts ORDER BY count(*)) FROM t GROUP BY ts, g", "over GROUP BY"),
        ("SELECT nth_value(x, 2) OVER (PARTITION BY ts ORDER BY x) FROM t", "not supported"),
    ] {
        let e = run_on(sql, &rows, &Serial, 10).expect_err(sql);
        assert!(e.contains(why), "{sql}: {e}");
    }
    // in the engine's SQL too, where nothing chose the time
    let two = LIVE.replace("PARTITION BY ts ORDER BY g", "PARTITION BY g ORDER BY g");
    let e = Engine::new(&brrrrr_core::sql::parse(&two).unwrap()).err().unwrap();
    assert!(e.contains("share their first PARTITION BY key, the time"), "{e}");
}

/// `row_number()` not partitioned by a time first, or without ORDER BY, numbers the rows of
/// each partition as they come, as before rankings: it is not one.
#[test]
fn a_row_number_by_other_keys_or_unordered_numbers_rows_as_they_come() {
    let rows = prices();
    assert_eq!(
        run(
            "SELECT g, row_number() OVER (PARTITION BY g ORDER BY ts) AS by_g, \
             row_number() OVER (PARTITION BY ts) AS in_time FROM t WHERE x > 6 ORDER BY ts, g",
            &rows
        ),
        strings(&[
            &["a", "1", "1"],
            &["b", "1", "2"],
            &["a", "2", "1"],
            &["b", "2", "2"],
            &["a", "3", "1"],
            &["b", "3", "2"],
            &["a", "4", "1"],
            &["b", "4", "2"],
            &["c", "1", "3"],
        ])
    );
}

/// A snapshot whose rows held for a ranking are not the plan's rows of one time, under the
/// empty key, is refused before anything is restored.
#[test]
fn a_ranking_s_snapshot_is_checked_before_it_is_restored() {
    let mut e = engine(LIVE);
    insert(&mut e, vec![trade(1, "a", Some(1.0)), trade(1, "b", Some(2.0))]);
    let good = serde_json::to_value(e.snapshot()).unwrap();
    assert_eq!(good[0][1]["Lead"]["held"][0][0], "", "the section's rows under the empty key: {good}");
    let restore = |state: &serde_json::Value| engine(LIVE).restore(serde_json::from_value(state.clone()).unwrap());
    restore(&good).unwrap();
    let held = |f: &dyn Fn(&mut serde_json::Value)| {
        let mut s = good.clone();
        f(&mut s[0][1]["Lead"]["held"]);
        s
    };
    for (what, bad) in [
        ("another key", held(&|h| h[0][0] = "x".into())),
        ("no rows", held(&|h| h[0][1] = serde_json::json!([]))),
        ("a narrower row", held(&|h| _ = h[0][1][1].as_array_mut().unwrap().pop())),
        ("another time", held(&|h| h[0][1][1][0] = serde_json::json!({"Time": 2 * SEC}))),
        ("two keys", held(&|h| h.as_array_mut().unwrap().push(serde_json::json!(["", []])))),
    ] {
        let e = restore(&bad).expect_err(what);
        assert!(e.contains("not the plan's rows of one time"), "{what}: {e}");
    }
}

/// The historical executor counts a ranking's late rows (rows of a time before the one held) as
/// the engine does.
#[test]
fn the_historical_executor_counts_a_ranking_s_late_rows() {
    let sql = LIVE
        .replace("PARTITION BY ts ORDER BY g", "PARTITION BY g ORDER BY g")
        .replace("PARTITION BY ts", "PARTITION BY g");
    let cat = brrrrr_core::sql::parse(&sql).unwrap();
    // g in order but for the third row: a, then b releases a, then a is late
    let rows: Vec<Row> =
        ["a", "b", "a", "b", "c"].iter().enumerate().map(|(i, g)| trade(i as i64 + 1, g, Some(1.0))).collect();
    let mut e = Engine::new(&cat).unwrap();
    insert(&mut e, rows.clone());
    assert_eq!(e.late(), 1);
    let mut h = brrrrr_core::engine::Historical::new(&cat).unwrap();
    h.set_clock("t", "ts").unwrap();
    let input: Box<dyn Source> = Box::new(Rows(vec![Batch::from_rows(&rows, 3)]));
    let mut out = vec![];
    let stats = h.run(vec![("t".into(), input)], 0..86_400 * SEC, &Serial, &mut out).unwrap();
    assert_eq!((stats.late, out.len()), (1, 4), "{stats:?}");
}
