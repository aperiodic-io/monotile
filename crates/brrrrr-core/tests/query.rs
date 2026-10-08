//! Ad-hoc queries (`query`, ADR-0018) end to end: compiled into views and run by the
//! historical executor over small tables, in batches of a few rows so that every query crosses
//! chunks, on one thread and on several.
use brrrrr_core::column::{Batch, Col, Data, Strs};
use brrrrr_core::engine::{Pool, Row, Serial, Source, Task};
use brrrrr_core::query::{compile, execute, split, text, Input, Table, MAX_DEPTH, MAX_UNION};
use brrrrr_core::value::{parse_datetime, Type, Value};

struct Rows(Vec<Batch>);

impl Source for Rows {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        (!self.0.is_empty()).then(|| Ok(self.0.remove(0)))
    }
}

/// Jobs on scoped threads: the same output as `Serial`, whatever runs them.
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

fn t(s: &str) -> Value {
    Value::Time(parse_datetime(&format!("2024-01-01 {s}")).unwrap())
}

fn s(x: &str) -> Value {
    Value::Str(x.into())
}

fn f(x: f64) -> Value {
    Value::F64(x)
}

type TableRows = (Vec<(String, Type)>, Vec<Row>);

fn table(name: &str) -> Option<TableRows> {
    let cols = |c: &[(&str, Type)]| c.iter().map(|(n, t)| (n.to_string(), t.clone())).collect::<Vec<_>>();
    Some(match name {
        "trades" | "trades.parquet" => (
            cols(&[("ts", Type::Time(6)), ("symbol", Type::Str), ("price", Type::F64), ("size", Type::F64)]),
            vec![
                vec![t("00:00:10"), s("A"), f(100.0), f(1.0)],
                vec![t("00:00:20"), s("B"), f(50.0), f(2.0)],
                vec![t("00:00:30"), s("A"), f(101.0), f(2.0)],
                vec![t("00:01:05"), s("A"), f(99.0), f(1.0)],
                vec![t("00:01:10"), s("B"), f(51.0), f(1.0)],
                vec![t("00:02:00"), s("A"), f(102.0), f(3.0)],
            ],
        ),
        "quotes" => (
            cols(&[("ts", Type::Time(6)), ("symbol", Type::Str), ("bid", Type::F64), ("ask", Type::F64)]),
            vec![
                vec![t("00:00:05"), s("A"), f(99.5), f(100.5)],
                vec![t("00:00:15"), s("B"), f(49.5), f(50.5)],
                vec![t("00:00:30"), s("A"), f(100.5), f(101.5)],
                vec![t("00:01:00"), s("A"), f(98.5), f(99.5)],
                vec![t("00:02:30"), s("B"), f(52.0), f(53.0)],
            ],
        ),
        "instruments" => (
            cols(&[("symbol", Type::Str), ("venue", Type::Str), ("tick", Type::F64)]),
            vec![vec![s("A"), s("X"), f(0.5)], vec![s("B"), s("Y"), f(0.01)], vec![s("C"), s("Z"), f(1.0)]],
        ),
        "O'Brien.csv" => table("instruments")?,
        // trades out of time order, and a trade without a time
        "shuffled" => {
            let (cols, mut rows) = table("trades")?;
            rows.reverse();
            rows.push(vec![Value::Null, s("A"), f(1000.0), f(1.0)]);
            (cols, rows)
        }
        // 1,001 values, 0/7 ... 1000/7 out of order, a second apart: past the engine's exact
        // `quantile_cont` (256 values)
        "many" => (
            cols(&[("ts", Type::Time(6)), ("k", Type::Str), ("v", Type::F64)]),
            (0..1001)
                .map(|i| {
                    let ts = Value::Time(parse_datetime("2024-01-01 00:00:00").unwrap() + i * 1_000_000);
                    vec![ts, s(["a", "b", "c"][i as usize % 3]), f(((i * 7919) % 1001) as f64 / 7.0)]
                })
                .collect(),
        ),
        // around New York's DST changes: its day of 2024-03-10 is 23 hours long, that of
        // 2024-11-03 25 hours, and that night reads 01:30 twice
        "dst" => (
            cols(&[("ts", Type::Time(6)), ("k", Type::Str), ("v", Type::F64)]),
            [
                ("2024-03-09 15:00", "a", 1.0),
                ("2024-03-10 04:59", "b", 2.0),
                ("2024-03-10 05:00", "a", 3.0),
                ("2024-03-11 03:59", "a", 4.0),
                ("2024-03-11 04:00", "a", 5.0),
                ("2024-11-03 05:30", "a", 6.0),
                ("2024-11-03 06:30", "a", 7.0),
                ("2024-11-04 04:59", "b", 8.0),
                ("2024-11-04 05:00", "a", 9.0),
            ]
            .iter()
            .map(|(ts, k, v)| vec![Value::Time(parse_datetime(ts).unwrap()), s(k), f(*v)])
            .collect(),
        ),
        "nulls" => (
            cols(&[("k", Type::Str), ("v", Type::F64)]),
            vec![vec![s("a"), f(1.0)], vec![s("a"), Value::Null], vec![Value::Null, f(3.0)], vec![s("b"), f(4.0)]],
        ),
        // keys of every kind, NULLs, ties, -0 and NaN: rows `i` in a shuffled order
        "sortme" => (
            cols(&[("i", Type::Int(64)), ("k", Type::Str), ("x", Type::F64), ("n", Type::Int(64))]),
            (0..300i64)
                .map(|j| {
                    let i = j * 7 % 300;
                    let k = if i % 11 == 0 {
                        Value::Null
                    } else {
                        s(["b", "a", "cc", "", "ab", "z", "b2"][(i % 7) as usize])
                    };
                    let xs = [-0.0, 0.0, f64::NAN, f64::INFINITY, 1.5, -2.0, 1.5, f64::NEG_INFINITY];
                    let x = if i % 13 == 0 { Value::Null } else { f(xs[(i % 8) as usize]) };
                    let n = if i % 5 == 0 { Value::Null } else { Value::Int(i % 17 - 8) };
                    vec![Value::Int(i), k, x, n]
                })
                .collect(),
        ),
        _ => return None,
    })
}

fn name(t: &Table) -> String {
    match t {
        Table::Named(n) => n.clone(),
        Table::Path { path, .. } => path.clone(),
    }
}

fn run_on(sql: &str, pool: &dyn Pool, batch: usize) -> Result<Vec<Vec<String>>, String> {
    run_coded(sql, pool, batch, false)
}

/// Text without NULLs as a dictionary's (Parquet's): each batch's distinct strings in reverse
/// order, and one of them twice (a dictionary may), its rows by code.
fn coded(b: Batch) -> Batch {
    let cols = b.cols.iter().map(|c| match &c.data {
        Data::Str(s) if c.nulls.is_none() && !s.is_empty() => {
            let mut entries: Vec<&str> = s.iter().collect();
            entries.sort_unstable();
            entries.dedup();
            entries.reverse();
            entries.push(entries[0]);
            let last = entries.len() as u32 - 1;
            let codes = (0..s.len())
                .map(|i| match entries.iter().position(|e| *e == s.get(i)).unwrap() as u32 {
                    0 if i % 2 == 1 => last,
                    k => k,
                })
                .collect();
            let dictionary: Strs = entries.into_iter().collect();
            std::sync::Arc::new(Col::new(Data::Str(Strs::from_dictionary(&dictionary, codes).unwrap())))
        }
        _ => c.clone(),
    });
    Batch::new(b.len, cols.collect())
}

fn run_coded(sql: &str, pool: &dyn Pool, batch: usize, dictionary: bool) -> Result<Vec<Vec<String>>, String> {
    let c = compile(sql, &mut |t| table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}")))?;
    let mut open =
        |src: &brrrrr_core::query::Source, _: &[bool], parts: Option<(usize, usize)>| -> Result<Vec<Input>, String> {
            let (cols, rows) = table(&name(&src.table)).unwrap();
            let batches = rows.chunks(batch).map(|r| Batch::from_rows(r, cols.len()));
            let batches = batches.map(|b| if dictionary { coded(b) } else { b }).collect();
            let s: Input = Box::new(Rows(batches));
            Ok(match parts {
                Some((key, n)) => split(s, key, n),
                None => vec![s],
            })
        };
    let rows = execute(&c, &mut open, pool).map_err(|e| format!("{e}\n{}", c.explain()))?;
    Ok(rows.iter().map(|r| r.iter().map(text).collect()).collect())
}

/// The query's rows as text, the same on one thread or four and in batches of 1, 2 or 100 rows.
fn run(sql: &str) -> Vec<Vec<String>> {
    let first = run_on(sql, &Serial, 2).unwrap_or_else(|e| panic!("{sql}\n{e}"));
    for (pool, batch) in [(&Serial as &dyn Pool, 1), (&Threads, 2), (&Threads, 100)] {
        assert_eq!(run_on(sql, pool, batch).unwrap(), first, "{sql}: another pool or batching");
    }
    // text read as a dictionary's
    for (pool, batch) in [(&Serial as &dyn Pool, 3), (&Threads, 100)] {
        assert_eq!(run_coded(sql, pool, batch, true).unwrap(), first, "{sql}: text as a dictionary's");
    }
    first
}

fn rows(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected.iter().map(|r| r.iter().map(|x| x.to_string()).collect()).collect()
}

#[test]
fn select_where_and_expressions() {
    assert_eq!(
        run("SELECT symbol, price * size AS notional FROM trades WHERE price > 100 ORDER BY notional DESC"),
        rows(&[&["A", "306"], &["A", "202"]])
    );
    assert_eq!(run("SELECT * FROM 'trades.parquet' LIMIT 1").len(), 1);
    assert_eq!(run("SELECT count(*) AS n FROM trades"), rows(&[&["6"]]));
}

#[test]
fn group_by_keys_without_time() {
    assert_eq!(
        run("SELECT symbol, count(*) AS n, sum(size) AS volume, max(price) AS high FROM trades GROUP BY symbol ORDER BY symbol"),
        rows(&[&["A", "4", "7", "102"], &["B", "2", "3", "51"]])
    );
    // positions and aliases as keys, HAVING on an aggregate not selected
    assert_eq!(
        run("SELECT symbol AS s, avg(price) AS p FROM trades GROUP BY 1 HAVING count(*) > 2"),
        rows(&[&["A", "100.5"]])
    );
    assert_eq!(run("SELECT DISTINCT symbol FROM trades ORDER BY symbol"), rows(&[&["A"], &["B"]]));
}

#[test]
fn ohlcv_bars() {
    assert_eq!(
        run("SELECT time_bucket('1m', ts) AS minute, symbol, first(price, ts) AS open, max(price) AS high, \
             min(price) AS low, last(price, ts) AS close, sum(size) AS volume, vwap(price, size) AS vwap \
             FROM trades GROUP BY minute, symbol ORDER BY minute, symbol"),
        rows(&[
            &["2024-01-01 00:00:00", "A", "100", "101", "100", "101", "3", "100.66666666666667"],
            &["2024-01-01 00:00:00", "B", "50", "50", "50", "50", "2", "50"],
            &["2024-01-01 00:01:00", "A", "99", "99", "99", "99", "1", "99"],
            &["2024-01-01 00:01:00", "B", "51", "51", "51", "51", "1", "51"],
            &["2024-01-01 00:02:00", "A", "102", "102", "102", "102", "3", "102"],
        ])
    );
}

#[test]
fn asof_join_takes_the_quote_at_or_before_each_trade() {
    let left = run("SELECT t.ts, t.symbol, q.bid FROM trades t ASOF LEFT JOIN quotes q \
                    ON t.symbol = q.symbol AND t.ts >= q.ts ORDER BY t.ts");
    assert_eq!(
        left.iter().map(|r| r[2].as_str()).collect::<Vec<_>>(),
        ["99.5", "49.5", "100.5", "98.5", "49.5", "98.5"]
    );
}

/// Markouts (the quote some time after each trade), the quote strictly before, and a tolerance:
/// an as-of join's times offset by constant intervals.
#[test]
fn asof_joins_at_an_offset_strictly_and_within_a_tolerance() {
    let markouts = "SELECT t.ts, t.symbol, q.bid AS b0, q30.bid AS b30, q90.bid AS b90 FROM trades t \
         ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts \
         ASOF LEFT JOIN quotes q30 ON t.symbol = q30.symbol AND t.ts + INTERVAL '30 seconds' >= q30.ts \
         ASOF LEFT JOIN quotes q90 ON q90.symbol = t.symbol AND q90.ts - INTERVAL '90' SECOND <= t.ts \
         ORDER BY t.ts";
    let want = rows(&[
        &["2024-01-01 00:00:10", "A", "99.5", "100.5", "98.5"],
        &["2024-01-01 00:00:20", "B", "49.5", "49.5", "49.5"],
        &["2024-01-01 00:00:30", "A", "100.5", "98.5", "98.5"],
        &["2024-01-01 00:01:05", "A", "98.5", "98.5", "98.5"],
        &["2024-01-01 00:01:10", "B", "49.5", "49.5", "52"],
        &["2024-01-01 00:02:00", "A", "98.5", "98.5", "98.5"],
    ]);
    assert_eq!(run(markouts), want);
    // the table read at an offset first, then as it is
    let first = run("SELECT t.ts, q30.bid AS b30, q.bid AS b0 FROM trades t \
         ASOF JOIN quotes q30 ON t.symbol = q30.symbol AND t.ts >= q30.ts - INTERVAL '30 seconds' \
         ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts ORDER BY t.ts");
    assert_eq!(
        first.iter().map(|r| [r[1].as_str(), r[2].as_str()]).collect::<Vec<_>>(),
        want.iter().map(|r| [r[3].as_str(), r[2].as_str()]).collect::<Vec<_>>()
    );
    // aggregated: the mean move of the bid 30 seconds on
    assert_eq!(
        run("SELECT t.symbol, avg(q30.bid - q.bid) AS move FROM trades t \
             ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts \
             ASOF JOIN quotes q30 ON t.symbol = q30.symbol AND t.ts + INTERVAL '30 seconds' >= q30.ts \
             GROUP BY t.symbol ORDER BY 1"),
        rows(&[&["A", "-0.25"], &["B", "0"]])
    );
    let col = |sql: &str| run(sql).into_iter().map(|r| r[1].clone()).collect::<Vec<_>>();
    // a minute before
    assert_eq!(
        col("SELECT t.ts, q.bid FROM trades t ASOF LEFT JOIN quotes q \
             ON t.symbol = q.symbol AND t.ts - INTERVAL '1 minute' >= q.ts ORDER BY t.ts"),
        ["NULL", "NULL", "NULL", "99.5", "NULL", "98.5"]
    );
    // strictly before: not the quote of the trade's own time
    assert_eq!(
        col("SELECT t.ts, q.bid FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts > q.ts ORDER BY t.ts"),
        ["99.5", "49.5", "99.5", "98.5", "49.5", "98.5"]
    );
    // within 10 seconds: an older quote is none (NULLs, or no row)
    for within in ["t.ts - q.ts <= INTERVAL '10 seconds'", "q.ts >= t.ts - INTERVAL '10 seconds'"] {
        assert_eq!(
            run(&format!(
                "SELECT t.ts, q.bid, q.ts FROM trades t ASOF LEFT JOIN quotes q \
                 ON t.symbol = q.symbol AND t.ts >= q.ts AND {within} ORDER BY t.ts"
            ))
            .into_iter()
            .map(|r| r[1..].join(" "))
            .collect::<Vec<_>>(),
            [
                "99.5 2024-01-01 00:00:05",
                "49.5 2024-01-01 00:00:15",
                "100.5 2024-01-01 00:00:30",
                "98.5 2024-01-01 00:01:00",
                "NULL NULL",
                "NULL NULL"
            ]
        );
        assert_eq!(
            run(&format!(
                "SELECT count(*) AS n, sum(q.bid) AS s FROM trades t ASOF JOIN quotes q \
                 ON t.symbol = q.symbol AND t.ts >= q.ts AND {within}"
            )),
            rows(&[&["4", "348"]])
        );
    }
    let c = compile(markouts, &mut |t| table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}"))).unwrap();
    assert!(c.explain().contains("in ts - 30s order"), "{}", c.explain());
}

/// Buckets of a zone's wall clock, its days 23 or 25 hours long when DST changes, and times as
/// its clock reads them.
#[test]
fn time_zones() {
    assert_eq!(
        run("SELECT time_bucket('1 day', ts, 'America/New_York') AS day, count(*) AS n, sum(v) AS v \
             FROM dst GROUP BY day ORDER BY day"),
        rows(&[
            &["2024-03-09 05:00:00", "2", "3"],
            &["2024-03-10 05:00:00", "2", "7"],
            &["2024-03-11 04:00:00", "1", "5"],
            &["2024-11-03 04:00:00", "3", "21"],
            &["2024-11-04 05:00:00", "1", "9"],
        ])
    );
    // with a key, after a WHERE; the hour read twice is two buckets, whatever the width
    for w in ["1h", "30m", "2h"] {
        assert_eq!(
            run(&format!(
                "SELECT k, time_bucket('{w}', ts, 'America/New_York') AS b, sum(v) AS v FROM dst \
                 WHERE ts >= '2024-11-03' AND ts < '2024-11-04' GROUP BY k, b ORDER BY b"
            )),
            match w {
                "2h" => rows(&[&["a", "2024-11-03 04:00:00", "13"]]),
                _ => rows(&[&["a", "2024-11-03 05:30:00", "6"], &["a", "2024-11-03 06:30:00", "7"]])
                    .into_iter()
                    .map(|mut r| {
                        if w == "1h" {
                            r[1] = r[1].replace(":30:", ":00:");
                        }
                        r
                    })
                    .collect(),
            },
            "{w}"
        );
    }
    assert_eq!(
        run("SELECT timezone('America/New_York', ts) AS ny, ts AT TIME ZONE 'Europe/London' AS london, \
             strftime(timezone('Asia/Kolkata', ts), '%H:%M') AS kolkata, \
             time_bucket('1 day', ts, 'America/New_York') AS day FROM dst WHERE v = 6 OR v = 7"),
        rows(&[
            &["2024-11-03 01:30:00", "2024-11-03 05:30:00", "11:00", "2024-11-03 04:00:00"],
            &["2024-11-03 01:30:00", "2024-11-03 06:30:00", "12:00", "2024-11-03 04:00:00"],
        ])
    );
    // a window narrower than an hour waits out the hour a DST end reads twice
    let c = compile("SELECT time_bucket('30m', ts, 'UTC') AS b, count(*) AS n FROM dst GROUP BY b", &mut |t| {
        table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}"))
    })
    .unwrap();
    assert!(c.explain().contains("WITH DELAY INTERVAL '3600000000' MICROSECOND"), "{}", c.explain());
}

/// TimescaleDB's gap filling: every bucket between a key's first and last (or over a range),
/// its aggregates NULL, `locf` carrying the last one on, `interpolate` drawing a line.
#[test]
fn gap_filling() {
    let bars =
        "SELECT time_bucket_gapfill('30s', ts{range}) AS b, symbol, count(*) AS n, coalesce(sum(size), 0) AS v, \
                locf(last(price, ts)) AS close, interpolate(avg(price)) AS mid \
                FROM trades GROUP BY b, symbol ORDER BY symbol, b";
    assert_eq!(
        run(&bars.replace("{range}", "")),
        rows(&[
            &["2024-01-01 00:00:00", "A", "1", "1", "100", "100"],
            &["2024-01-01 00:00:30", "A", "1", "2", "101", "101"],
            &["2024-01-01 00:01:00", "A", "1", "1", "99", "99"],
            &["2024-01-01 00:01:30", "A", "NULL", "0", "99", "100.5"],
            &["2024-01-01 00:02:00", "A", "1", "3", "102", "102"],
            &["2024-01-01 00:00:00", "B", "1", "2", "50", "50"],
            &["2024-01-01 00:00:30", "B", "NULL", "0", "50", "50.5"],
            &["2024-01-01 00:01:00", "B", "1", "1", "51", "51"],
        ])
    );
    // over a range: from its start (nothing to carry yet) to its finish (nothing to draw to)
    let ranged = run(&bars.replace("{range}", ", '2023-12-31 23:59:30', TIMESTAMP '2024-01-01 00:02:30'"));
    let b: Vec<String> =
        ranged.iter().filter(|r| r[1] == "B").map(|r| format!("{} {} {}", &r[0][11..], r[4], r[5])).collect();
    assert_eq!(
        b,
        [
            "23:59:30 NULL NULL",
            "00:00:00 50 50",
            "00:00:30 50 50.5",
            "00:01:00 51 51",
            "00:01:30 51 NULL",
            "00:02:00 51 NULL"
        ]
    );
    assert_eq!(ranged.len(), 12);
    // without keys
    assert_eq!(
        run("SELECT time_bucket_gapfill(INTERVAL '30 seconds', ts) AS b, count(*) AS n FROM trades \
             WHERE symbol = 'B' GROUP BY b ORDER BY b"),
        rows(
            &[["2024-01-01 00:00:00", "1"], ["2024-01-01 00:00:30", "NULL"], ["2024-01-01 00:01:00", "1"]]
                .iter()
                .map(|r| &r[..])
                .collect::<Vec<_>>()
        )
    );
}

/// `lead`: a row comes when its partition's next rows do; at the end of the input, with the
/// next rows there are or the default. With `lag` and other window functions in one SELECT.
#[test]
fn lead_reads_the_next_rows() {
    assert_eq!(
        run("SELECT ts, symbol, lead(price) OVER w AS next, lead(ts, 2) OVER w AS ts2, \
             lead(price, 1, 0) OVER w AS next0, lag(price) OVER w AS prev, count(*) OVER w AS n \
             FROM trades WINDOW w AS (PARTITION BY symbol ORDER BY ts) ORDER BY ts"),
        rows(&[
            &["2024-01-01 00:00:10", "A", "101", "2024-01-01 00:01:05", "101", "NULL", "1"],
            &["2024-01-01 00:00:20", "B", "51", "NULL", "51", "NULL", "1"],
            &["2024-01-01 00:00:30", "A", "99", "2024-01-01 00:02:00", "99", "100", "2"],
            &["2024-01-01 00:01:05", "A", "102", "NULL", "102", "101", "3"],
            &["2024-01-01 00:01:10", "B", "NULL", "NULL", "0", "50", "2"],
            &["2024-01-01 00:02:00", "A", "NULL", "NULL", "0", "99", "4"],
        ])
    );
    // the time to the next trade, averaged; without a partition, the next row of all
    assert_eq!(
        run("SELECT symbol, avg(gap) AS gap FROM (SELECT symbol, \
             epoch_us(lead(ts) OVER (PARTITION BY symbol ORDER BY ts)) - epoch_us(ts) AS gap FROM trades) \
             GROUP BY symbol ORDER BY symbol"),
        rows(&[&["A", "36666666.666666664"], &["B", "50000000"]])
    );
    assert_eq!(
        run("SELECT price, lead(price) OVER (ORDER BY ts) AS next FROM trades WHERE price > 60 ORDER BY ts"),
        rows(&[&["100", "101"], &["101", "99"], &["99", "102"], &["102", "NULL"]])
    );
}

/// Window joins (kdb+'s `wj`): for each left row, aggregates over the right rows of its key in a
/// time range around it, a LATERAL subquery.
#[test]
fn window_joins() {
    let wj = |range: &str| {
        run(&format!(
            "SELECT t.ts, t.symbol, w.n, w.bid FROM trades t LEFT JOIN LATERAL \
             (SELECT count(*) AS n, avg(q.bid) AS bid FROM quotes q WHERE q.symbol = t.symbol AND {range}) w \
             ON true ORDER BY t.ts"
        ))
        .into_iter()
        .map(|r| r[2..].join(" "))
        .collect::<Vec<_>>()
    };
    let before = ["1 99.5", "1 49.5", "2 100", "1 98.5", "0 NULL", "0 NULL"];
    assert_eq!(wj("q.ts BETWEEN t.ts - INTERVAL '30 seconds' AND t.ts"), before);
    assert_eq!(wj("t.ts >= q.ts AND q.ts >= t.ts - INTERVAL '30' SECOND"), before);
    // after: rows later than the left one's
    assert_eq!(
        wj("q.ts BETWEEN t.ts AND t.ts + INTERVAL '1 minute'"),
        ["2 99.5", "0 NULL", "2 99.5", "0 NULL", "0 NULL", "0 NULL"]
    );
    // strict ends, and a condition on the right rows
    assert_eq!(
        wj("q.ts > t.ts - INTERVAL '30 seconds' AND q.ts < t.ts AND q.bid > 0"),
        ["1 99.5", "1 49.5", "1 99.5", "1 98.5", "0 NULL", "0 NULL"]
    );
    assert_eq!(
        run("SELECT t.symbol, sum(w.n) AS quotes, max(w.spread) AS spread FROM trades t, \
             LATERAL (SELECT count(*) AS n, max(ask - bid) AS spread FROM quotes WHERE symbol = t.symbol \
             AND ts BETWEEN t.ts - INTERVAL '1 minute' AND t.ts) w GROUP BY t.symbol ORDER BY 1"),
        rows(&[&["A", "7", "1"], &["B", "2", "1"]])
    );
}

/// A global aggregate, or one by other keys, over a join keyed by symbol: each part of the run
/// (its symbols) aggregates its rows, and the parts' aggregates are merged (`Compiled::merge`);
/// one not merged so runs as one part. The same answers on one thread and on several (`run`).
#[test]
fn global_aggregates_over_keyed_steps_merge_the_parts() {
    let join = "FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts";
    let compiled =
        |sql: &str| compile(sql, &mut |t| table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}"))).unwrap();
    let all = format!(
        "SELECT count(*) AS n, sum(q.bid) AS s, avg(t.price - q.bid) AS edge, min(q.bid) AS lo, max(t.price) AS hi, \
         first(t.price, t.ts) AS open, last(t.price, t.ts) AS close, median(q.bid) AS med, \
         count(DISTINCT q.bid) AS bids, count(*) FILTER (WHERE t.size > 1) AS big {join}"
    );
    assert_eq!(compiled(&all).merge.len(), 1);
    assert_eq!(compiled(&all).partition.as_ref().map(|p| p.len()), Some(2));
    assert_eq!(run(&all), rows(&[&["6", "496", "1.1666666666666667", "49.5", "102", "100", "102", "98.5", "4", "3"]]));
    // by keys other than the symbol, kept by HAVING
    assert_eq!(
        run(&format!("SELECT t.size AS size, count(*) AS n, sum(t.price) AS p {join} GROUP BY t.size HAVING count(*) > 1 ORDER BY 1")),
        rows(&[&["1", "3", "250"], &["2", "2", "151"]])
    );
    // a lookup join's count: one row of every part's, on one thread or four (`run`)
    let lookup = "SELECT count(*) AS n FROM trades t JOIN instruments i ON t.symbol = i.symbol";
    assert_eq!(compiled(lookup).merge.len(), 1);
    assert_eq!(run(lookup), rows(&[&["6"]]));
    assert_eq!(run_on(lookup, &Threads, 1).unwrap(), rows(&[&["6"]]));
    // stddev does not merge: one part
    let one = format!("SELECT stddev(t.price) AS sd {join}");
    assert!(!compiled(&one).merge.is_empty());
    assert_eq!(run(&one), rows(&[&["25.841181603530956"]]));
    // a window after it: no merge
    let nested = format!("SELECT count(*) AS k FROM (SELECT t.size AS size, count(*) AS n {join} GROUP BY t.size)");
    assert!(compiled(&nested).merge.is_empty());
    assert_eq!(run(&nested), rows(&[&["3"]]));
}

/// A quote in a string literal is written twice, and stays one through every rewrite of the
/// query's text: in SELECT, WHERE, a file's path, MATCH_CONDITION, a reader's options.
#[test]
fn quotes_in_string_literals() {
    assert_eq!(run("SELECT 'it''s' AS s"), rows(&[&["it's"]]));
    assert_eq!(run("SELECT count(*) AS n FROM 'O''Brien.csv' WHERE venue <> 'O''Brien'"), rows(&[&["3"]]));
    assert_eq!(
        run("SELECT q.bid, 'it''s' AS s FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) \
             ON t.symbol = q.symbol WHERE t.symbol <> 'it''s' ORDER BY t.ts LIMIT 1"),
        rows(&[&["99.5", "it's"]])
    );
    // a reader's options ride in its table, shown as written
    let c = compile(
        "SELECT * FROM read_csv('O''Brien.csv', delim = '|', nullstr = ['it''s'], types = {'k': 'VARCHAR'})",
        &mut |t| {
            match t {
                Table::Path { options, .. } => {
                    assert_eq!(options.as_deref(), Some("delim = '|', nullstr = ['it''s'], types = {'k': 'VARCHAR'}"))
                }
                _ => panic!("a path"),
            }
            table("instruments").map(|x| x.0).ok_or("no table".into())
        },
    )
    .unwrap();
    assert!(
        c.explain().contains("read_csv('O''Brien.csv', delim = '|', nullstr = ['it''s'], types = {'k': 'VARCHAR'})"),
        "{}",
        c.explain()
    );
    assert!(refused("SELECT * FROM read_csv('trades', 1)").contains("is not an option"));
    // a table format names its kind: the caller finds its files
    for (scan, kind) in [("delta_scan", "delta"), ("iceberg_scan", "iceberg"), ("read_dbn", "dbn")] {
        let c = compile(&format!("SELECT count(*) AS n FROM {scan}('s3://b/t')"), &mut |t| {
            assert_eq!(*t, Table::Path { path: "s3://b/t".into(), format: Some(kind.into()), options: None });
            table("instruments").map(|x| x.0).ok_or("no table".into())
        })
        .unwrap();
        let shown = if kind == "dbn" { "'s3://b/t'".to_string() } else { format!("{scan}('s3://b/t')") };
        assert!(c.explain().contains(&shown), "{}", c.explain());
    }
}

/// Queries nested deep or wide compile and run on a thread of a server's stack (2 MiB), or are
/// refused for it: never a stack overflow.
#[test]
fn deep_queries_run_or_are_refused_on_a_small_stack() {
    let small = |sql: String| {
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || run_on(&sql, &Serial, 100).map(|r| r.len()))
            .unwrap()
            .join()
            .expect("no stack overflow")
    };
    let sum = |n: usize| format!("SELECT {} AS x", vec!["1"; n].join(" + "));
    assert_eq!(small(sum(MAX_DEPTH - 2)), Ok(1));
    assert!(small(sum(30_000)).unwrap_err().contains("nested more than 1000 deep"));
    let union = |n: usize| format!("SELECT count(*) AS n FROM ({})", vec!["SELECT 1 AS x"; n].join(" UNION ALL "));
    assert_eq!(small(union(2_000)), Ok(1));
    assert!(small(union(MAX_UNION + 1)).unwrap_err().contains("a UNION of more than"));
    let parens = format!("SELECT {}1{} AS x", "(".repeat(500), ")".repeat(500));
    assert!(small(parens).unwrap_err().contains("nested more than 50 deep"));
}

/// PIVOT: a column's values as columns, each its aggregate's FILTER; SQL's form, which groups by
/// every other column, and DuckDB's simplified statement; UNPIVOT the other way.
#[test]
fn pivot_and_unpivot() {
    // FILTER on an aggregate of two arguments
    assert_eq!(
        run("SELECT last(price, ts) FILTER (WHERE size < 3) AS p, vwap(price, size) FILTER (WHERE symbol = 'B') AS v FROM trades"),
        rows(&[&["51", "50.333333333333336"]])
    );
    assert_eq!(
        run("SELECT * FROM (SELECT symbol, size FROM trades) PIVOT (sum(size) FOR symbol IN ('A', 'B'))"),
        rows(&[&["7", "3"]])
    );
    let bars = "SELECT * FROM (SELECT time_bucket('1m', ts) AS minute, symbol, price, ts FROM trades) \
                PIVOT (last(price, ts) AS close, count(*) AS n FOR symbol IN ('A', 'B' AS b)) ORDER BY minute";
    let c = compile(bars, &mut |t| table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}"))).unwrap();
    assert_eq!(c.columns, ["minute", "A_close", "A_n", "b_close", "b_n"]);
    assert_eq!(
        run(bars),
        rows(&[
            &["2024-01-01 00:00:00", "101", "2", "50", "1"],
            &["2024-01-01 00:01:00", "99", "1", "51", "1"],
            &["2024-01-01 00:02:00", "102", "1", "NULL", "0"],
        ])
    );
    // DuckDB's statement, its keys a time bucket
    let simple = "PIVOT trades ON symbol IN ('A', 'B') USING sum(size) GROUP BY time_bucket('1m', ts) ORDER BY 1";
    assert_eq!(
        run(simple),
        rows(&[
            &["2024-01-01 00:00:00", "3", "2"],
            &["2024-01-01 00:01:00", "1", "1"],
            &["2024-01-01 00:02:00", "3", "NULL"]
        ])
    );
    let two = "PIVOT (SELECT symbol, price FROM trades) ON symbol IN ('A') USING max(price), min(price)";
    assert_eq!(run(two), rows(&[&["102", "99"]]));
    let c = compile(two, &mut |t| table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}"))).unwrap();
    assert_eq!(c.columns, ["A_max(price)", "A_min(price)"]);
    // its values run for first, when not named
    let mut asked = vec![];
    let c = brrrrr_core::query::compile_with(
        "PIVOT trades ON symbol USING sum(size) GROUP BY 1 + 1",
        &mut |t| table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}")),
        &mut |sql| {
            asked.push(sql.to_string());
            Ok(vec![s("A"), Value::Null])
        },
    )
    .unwrap();
    assert_eq!(asked, ["SELECT DISTINCT symbol AS v FROM trades ORDER BY v"]);
    assert_eq!(c.columns, ["1 + 1", "A", "NULL"]);
    assert!(refused("PIVOT trades ON symbol USING sum(size)").contains("PIVOT without IN"));
    assert!(refused("PIVOT trades USING sum(size)").contains("PIVOT table ON column"));
    assert!(refused("SELECT * FROM trades PIVOT (sum(size) FOR (symbol, ts) IN (('A', 1)))").contains("one column"));
    assert_eq!(
        run("SELECT * FROM (SELECT symbol, price, size FROM trades WHERE symbol = 'B') UNPIVOT (v FOR k IN (price, size AS qty)) \
             ORDER BY k, v"),
        rows(&[&["B", "price", "50"], &["B", "price", "51"], &["B", "qty", "1"], &["B", "qty", "2"]])
    );
}

#[test]
fn lookup_join_and_subqueries() {
    assert_eq!(
        run("SELECT i.venue, count(*) AS n FROM trades t JOIN instruments i ON t.symbol = i.symbol GROUP BY i.venue ORDER BY 1"),
        rows(&[&["X", "4"], &["Y", "2"]])
    );
    assert_eq!(
        run("WITH big AS (SELECT * FROM trades WHERE size >= 2) SELECT symbol, count(*) AS n FROM big GROUP BY symbol ORDER BY symbol"),
        rows(&[&["A", "2"], &["B", "1"]])
    );
    assert_eq!(
        run("SELECT n FROM (SELECT symbol, count(*) AS n FROM trades GROUP BY symbol) ORDER BY n"),
        rows(&[&["2"], &["4"]])
    );
}

#[test]
fn union_all_and_window_functions() {
    assert_eq!(run("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes").len(), 11);
    assert_eq!(
        run("SELECT symbol, price - lag(price) OVER (PARTITION BY symbol ORDER BY ts) AS change FROM trades WHERE symbol = 'A' ORDER BY 1")
            .iter()
            .map(|r| r[1].clone())
            .collect::<Vec<_>>(),
        ["NULL", "1", "-2", "3"]
    );
}

#[test]
fn nulls_and_offsets() {
    assert_eq!(
        run("SELECT k, sum(v) AS s FROM nulls GROUP BY k ORDER BY k NULLS FIRST"),
        rows(&[&["NULL", "3"], &["a", "1"], &["b", "4"]])
    );
    assert_eq!(run("SELECT v FROM nulls ORDER BY v LIMIT 2 OFFSET 1"), rows(&[&["3"], &["4"]]));
}

#[test]
fn tables_out_of_time_order_are_sorted_and_rows_without_a_time_left_out() {
    let bars =
        "SELECT time_bucket('1m', ts) AS m, symbol, sum(size) AS v FROM {t} GROUP BY m, symbol ORDER BY m, symbol";
    assert_eq!(run(&bars.replace("{t}", "shuffled")), run(&bars.replace("{t}", "trades")));
    // without a time operation, every row in the order read
    assert_eq!(run("SELECT count(*) AS n FROM shuffled"), rows(&[&["7"]]));
}

#[test]
fn an_aggregate_over_no_rows_is_one_row() {
    assert_eq!(run("SELECT count(*) AS n, sum(price) AS s FROM trades WHERE price > 1000"), rows(&[&["0", "NULL"]]));
    // with GROUP BY, no groups: no rows
    assert!(run("SELECT symbol, count(*) AS n FROM trades WHERE price > 1000 GROUP BY symbol").is_empty());
}

#[test]
fn percentiles_are_exact_however_many_values() {
    // DuckDB's: the value at rank level * (n - 1) of the values in order, between two their blend
    let at = |rank: i64| text(&Value::F64(rank as f64 / 7.0));
    assert_eq!(
        run("SELECT median(v) AS m, quantile_cont(v, 0.9) AS p90, percentile_cont(0.25) WITHIN GROUP (ORDER BY v) AS p25, \
             quantile(v, 0) AS lo FROM many"),
        rows(&[&[&at(500), &at(900), &at(250), &at(0)]])
    );
    // the engine's own quantile_cont is a t-digest of Float32 past 256 values
    let engine = &run("SELECT quantile_cont(0.5)(v) AS m FROM many")[0][0];
    assert_ne!(*engine, at(500));
    // per group, per bucket, and FILTER: k = 'a' is every third value from 0
    let a: Vec<f64> = (0..1001).filter(|i| i % 3 == 0).map(|i| ((i * 7919) % 1001) as f64 / 7.0).collect();
    let mut sorted = a.clone();
    sorted.sort_by(f64::total_cmp);
    // 334 values: rank 166.5, halfway between two
    assert_eq!(sorted.len(), 334);
    let median_a = text(&Value::F64(sorted[166] * 0.5 + sorted[167] * 0.5));
    assert_eq!(run("SELECT k, median(v) AS m FROM many GROUP BY k ORDER BY k")[0], [s_("a"), median_a.clone()]);
    assert_eq!(run("SELECT median(v) FILTER (WHERE k = 'a') AS m FROM many"), rows(&[&[&median_a]]));
    assert_eq!(
        run("SELECT time_bucket('1h', ts) AS h, median(v) AS m FROM many GROUP BY h"),
        rows(&[&["2024-01-01 00:00:00", &at(500)]])
    );
    // a running median
    assert_eq!(
        run("SELECT median(price) OVER (PARTITION BY symbol ORDER BY ts) AS m FROM trades WHERE symbol = 'A' ORDER BY ts"),
        rows(&[&["100"], &["100.5"], &["100"], &["100.5"]])
    );
}

fn s_(x: &str) -> String {
    x.to_string()
}

#[test]
fn count_distinct() {
    assert_eq!(
        run("SELECT count(DISTINCT symbol) AS n, approx_count_distinct(size) AS z, count(DISTINCT price) FILTER (WHERE size > 1) AS p FROM trades"),
        rows(&[&["2", "3", "3"]])
    );
    assert_eq!(
        run("SELECT symbol, count(DISTINCT size) AS n FROM trades GROUP BY symbol ORDER BY symbol"),
        rows(&[&["A", "3"], &["B", "2"]])
    );
    assert_eq!(
        run("SELECT time_bucket('1m', ts) AS m, count(DISTINCT symbol) AS n FROM trades GROUP BY m ORDER BY m"),
        rows(&[&["2024-01-01 00:00:00", "2"], &["2024-01-01 00:01:00", "2"], &["2024-01-01 00:02:00", "1"]])
    );
    assert_eq!(run("SELECT count(DISTINCT k) AS n FROM nulls"), rows(&[&["2"]]), "NULL is no value");
    assert_eq!(run("SELECT count(DISTINCT symbol) AS n FROM trades WHERE price > 1000"), rows(&[&["0"]]));
    assert_eq!(run("SELECT max(DISTINCT price) AS m FROM trades"), rows(&[&["102"]]));
    let e = run_on("SELECT sum(DISTINCT size) FROM trades", &Serial, 2).unwrap_err();
    assert!(e.contains("count(DISTINCT x) is uniq_exact(x)"), "{e}");
}

#[test]
fn unions_in_subqueries() {
    let trades_and_quotes = "(SELECT ts, symbol, price AS p FROM trades UNION ALL SELECT ts, symbol, bid FROM quotes)";
    assert_eq!(
        run(&format!("SELECT symbol, count(*) AS n FROM {trades_and_quotes} GROUP BY symbol ORDER BY symbol")),
        rows(&[&["A", "7"], &["B", "4"]])
    );
    assert_eq!(
        run("SELECT symbol, count(*) AS n FROM (SELECT symbol FROM trades UNION SELECT symbol FROM instruments) \
             GROUP BY symbol ORDER BY symbol"),
        rows(&[&["A", "1"], &["B", "1"], &["C", "1"]])
    );
    assert_eq!(
        run("SELECT symbol FROM trades UNION SELECT symbol FROM quotes ORDER BY symbol"),
        rows(&[&["A"], &["B"]])
    );
    // in time order, both tables: buckets, and a window function over the rows of both
    assert_eq!(
        run(&format!(
            "SELECT time_bucket('1m', ts) AS m, count(*) AS n FROM {trades_and_quotes} GROUP BY m ORDER BY m"
        )),
        rows(&[&["2024-01-01 00:00:00", "6"], &["2024-01-01 00:01:00", "3"], &["2024-01-01 00:02:00", "2"]])
    );
    assert_eq!(
        run(&format!(
            "SELECT d FROM (SELECT ts, symbol, p - lag(p) OVER (PARTITION BY symbol ORDER BY ts) AS d FROM {trades_and_quotes}) \
             WHERE symbol = 'B' ORDER BY ts"
        )),
        rows(&[&["NULL"], &["0.5"], &["1"], &["1"]])
    );
    // a window's output beside a table's rows
    assert_eq!(
        run("SELECT count(*) AS n FROM (SELECT symbol, count(*) AS c FROM trades GROUP BY symbol \
             UNION ALL SELECT symbol, 1 FROM instruments)"),
        rows(&[&["5"]])
    );
}

/// The scalar functions of ad-hoc queries (`expr::functions`), each over constants: DuckDB's
/// answers.
#[test]
fn scalar_functions() {
    let t = "TIMESTAMP '2024-03-15 13:45:30.123456'";
    let cases = [
        (r#"floor(2.7)"#.to_string(), r#"2"#),
        (r#"floor(3)"#.to_string(), r#"3"#),
        (r#"ceil(2.1)"#.to_string(), r#"3"#),
        (r#"ceiling(-2.1)"#.to_string(), r#"-2"#),
        (r#"trunc(-2.7)"#.to_string(), r#"-2"#),
        (r#"round(2.5)"#.to_string(), r#"3"#),
        (r#"round(1.2345, 2)"#.to_string(), r#"1.23"#),
        (r#"exp(0)"#.to_string(), r#"1"#),
        (r#"log10(1000)"#.to_string(), r#"3"#),
        (r#"log2(8)"#.to_string(), r#"3"#),
        (r#"log(100)"#.to_string(), r#"2"#),
        (r#"log(2, 8)"#.to_string(), r#"3"#),
        (r#"power(2, 10)"#.to_string(), r#"1024"#),
        (r#"pow(4, 0.5)"#.to_string(), r#"2"#),
        (r#"sign(-3)"#.to_string(), r#"-1"#),
        (r#"sign(0)"#.to_string(), r#"0"#),
        (r#"signum(2.5)"#.to_string(), r#"1"#),
        (r#"pi()"#.to_string(), r#"3.141592653589793"#),
        (r#"greatest(1, 3, NULL, 2)"#.to_string(), r#"3"#),
        (r#"least(4, 2)"#.to_string(), r#"2"#),
        (r#"greatest(NULL, NULL)"#.to_string(), r#"NULL"#),
        (r#"nullif(1, 1)"#.to_string(), r#"NULL"#),
        (r#"ifnull(NULL, 'x')"#.to_string(), r#"x"#),
        (r#"nvl('y', 'x')"#.to_string(), r#"y"#),
        (r#"lower('AbC')"#.to_string(), r#"abc"#),
        (r#"lcase('D')"#.to_string(), r#"d"#),
        (r#"upper('abc')"#.to_string(), r#"ABC"#),
        (r#"ucase('e')"#.to_string(), r#"E"#),
        (r#"trim('  x  ')"#.to_string(), r#"x"#),
        (r#"ltrim('  x ')"#.to_string(), r#"x "#),
        (r#"rtrim(' x  ')"#.to_string(), r#" x"#),
        (r#"reverse('abc')"#.to_string(), r#"cba"#),
        (r#"char_length('héllo')"#.to_string(), r#"5"#),
        (r#"strlen('ab')"#.to_string(), r#"2"#),
        (r#"starts_with('brrrrr', 'br')"#.to_string(), r#"true"#),
        (r#"prefix('brrrrr', 'r')"#.to_string(), r#"false"#),
        (r#"ends_with('brrrrr', 'rr')"#.to_string(), r#"true"#),
        (r#"suffix('ab', 'a')"#.to_string(), r#"false"#),
        (r#"contains('hello', 'ell')"#.to_string(), r#"true"#),
        (r#"strpos('hello', 'l')"#.to_string(), r#"3"#),
        (r#"instr('hello', 'z')"#.to_string(), r#"0"#),
        (r#"left('hello', 2)"#.to_string(), r#"he"#),
        (r#"left('hello', -1)"#.to_string(), r#""#),
        (r#"right('hello', 3)"#.to_string(), r#"llo"#),
        (r#"repeat('ab', 3)"#.to_string(), r#"ababab"#),
        (r#"substr('hello', 2, 3)"#.to_string(), r#"ell"#),
        (r#"substring('hello', 2)"#.to_string(), r#"ello"#),
        (r#"substr('hello', 0, 2)"#.to_string(), r#"h"#),
        (r#"substr('hello', 2, NULL)"#.to_string(), r#"NULL"#),
        (r#"replace('aXbX', 'X', '-')"#.to_string(), r#"a-b-"#),
        (r#"replace('ab', '', 'z')"#.to_string(), r#"ab"#),
        (r#"split_part('a,b,c', ',', 2)"#.to_string(), r#"b"#),
        (r#"split_part('a,b', ',', 5)"#.to_string(), r#""#),
        (r#"lpad('7', 3, '0')"#.to_string(), r#"007"#),
        (r#"rpad('ab', 5, 'xy')"#.to_string(), r#"abxyx"#),
        (r#"lpad('hello', 2, '*')"#.to_string(), r#"he"#),
        (r#"concat_ws('-', 'a', NULL, 'b')"#.to_string(), r#"a-b"#),
        (r#"'abc' LIKE 'a%'"#.to_string(), r#"true"#),
        (r#"'ABC' ILIKE 'a_c'"#.to_string(), r#"true"#),
        (r#"regexp_matches('abc123', '[0-9]+')"#.to_string(), r#"true"#),
        (r#"regexp_extract('abc123', '([a-z]+)([0-9]+)', 2)"#.to_string(), r#"123"#),
        (r#"regexp_replace('a1b2', '[0-9]', 'X')"#.to_string(), r#"aXb2"#),
        (r#"regexp_replace('a1b2', '[0-9]', 'X', 'g')"#.to_string(), r#"aXbX"#),
        (format!(r#"date_trunc('microsecond', {t})"#), r#"2024-03-15 13:45:30.123456"#),
        (format!(r#"date_trunc('ms', {t})"#), r#"2024-03-15 13:45:30.123"#),
        (format!(r#"date_trunc('second', {t})"#), r#"2024-03-15 13:45:30"#),
        (format!(r#"date_trunc('minute', {t})"#), r#"2024-03-15 13:45:00"#),
        (format!(r#"date_trunc('hour', {t})"#), r#"2024-03-15 13:00:00"#),
        (format!(r#"date_trunc('day', {t})"#), r#"2024-03-15 00:00:00"#),
        (format!(r#"date_trunc('week', {t})"#), r#"2024-03-11 00:00:00"#),
        (format!(r#"date_trunc('month', {t})"#), r#"2024-03-01 00:00:00"#),
        (format!(r#"date_trunc('quarter', {t})"#), r#"2024-01-01 00:00:00"#),
        (r#"date_trunc('year', '2024-03-15 13:45:30')"#.to_string(), r#"2024-01-01 00:00:00"#),
        (format!(r#"date_part('year', {t})"#), r#"2024"#),
        (format!(r#"date_part('quarter', {t})"#), r#"1"#),
        (format!(r#"date_part('month', {t})"#), r#"3"#),
        (format!(r#"date_part('day', {t})"#), r#"15"#),
        (format!(r#"date_part('hour', {t})"#), r#"13"#),
        (format!(r#"date_part('minute', {t})"#), r#"45"#),
        (format!(r#"date_part('second', {t})"#), r#"30"#),
        (format!(r#"date_part('millisecond', {t})"#), r#"30123"#),
        (format!(r#"date_part('microsecond', {t})"#), r#"30123456"#),
        (format!(r#"date_part('dow', {t})"#), r#"5"#),
        (format!(r#"date_part('isodow', {t})"#), r#"5"#),
        (format!(r#"date_part('doy', {t})"#), r#"75"#),
        (r#"date_part('week', TIMESTAMP '2024-01-01 00:00:00')"#.to_string(), r#"1"#),
        (r#"date_part('epoch', TIMESTAMP '1970-01-01 00:00:01.5')"#.to_string(), r#"1.5"#),
        (r#"extract(year FROM TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"2024"#),
        (r#"year(TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"2024"#),
        (r#"quarter(TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"1"#),
        (r#"month(TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"3"#),
        (r#"day(TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"15"#),
        (r#"hour(TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"13"#),
        (r#"minute(TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"45"#),
        (r#"second(TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"30"#),
        (r#"dayofweek(TIMESTAMP '2024-03-17 13:45:30')"#.to_string(), r#"0"#),
        (r#"isodow(TIMESTAMP '2024-03-17 13:45:30')"#.to_string(), r#"7"#),
        (r#"dayofyear(TIMESTAMP '2024-12-31 00:00:00')"#.to_string(), r#"366"#),
        (r#"weekofyear(TIMESTAMP '2021-01-03 00:00:00')"#.to_string(), r#"53"#),
        (r#"epoch(TIMESTAMP '1970-01-02 00:00:00')"#.to_string(), r#"86400"#),
        (r#"epoch_ms(TIMESTAMP '1970-01-01 00:00:01')"#.to_string(), r#"1000"#),
        (r#"epoch_us(TIMESTAMP '1970-01-01 00:00:01')"#.to_string(), r#"1000000"#),
        (r#"epoch_ns(TIMESTAMP '1970-01-01 00:00:01')"#.to_string(), r#"1000000000"#),
        (r#"epoch_ms(1500)"#.to_string(), r#"1970-01-01 00:00:01.5"#),
        (r#"to_timestamp(86400)"#.to_string(), r#"1970-01-02 00:00:00"#),
        (r#"to_timestamp('2024-01-02 03:04:05Z')"#.to_string(), r#"2024-01-02 03:04:05"#),
        (r#"from_unixtime(TIMESTAMP '2024-01-01 00:00:00')"#.to_string(), r#"2024-01-01 00:00:00"#),
        (
            format!(r#"strftime({t}, '%Y-%m-%d %H:%M:%S.%f %j %a %b %% %q %')"#),
            r#"2024-03-15 13:45:30.123456 075 Fri Mar % %q %"#,
        ),
        (r#"time_bucket('15 minutes', TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"2024-03-15 13:45:00"#),
        (
            r#"time_bucket('1h', TIMESTAMP '2024-03-15 13:45:30', TIMESTAMP '2024-01-01 00:30:00')"#.to_string(),
            r#"2024-03-15 13:30:00"#,
        ),
        (r#"date_bin(INTERVAL '1 day', TIMESTAMP '2024-03-15 13:45:30')"#.to_string(), r#"2024-03-15 00:00:00"#),
        (r#"date_diff('day', TIMESTAMP '2024-03-15 23:00:00', TIMESTAMP '2024-03-17 01:00:00')"#.to_string(), r#"2"#),
        (r#"datediff('month', TIMESTAMP '2023-11-30 00:00:00', TIMESTAMP '2024-03-01 00:00:00')"#.to_string(), r#"4"#),
        (
            r#"date_diff('quarter', TIMESTAMP '2023-11-30 00:00:00', TIMESTAMP '2024-03-01 00:00:00')"#.to_string(),
            r#"1"#,
        ),
        (r#"date_diff('year', TIMESTAMP '2023-11-30 00:00:00', TIMESTAMP '2024-03-01 00:00:00')"#.to_string(), r#"1"#),
        (
            r#"date_diff('hours', TIMESTAMP '2024-03-15 13:45:30', TIMESTAMP '2024-03-15 10:00:00')"#.to_string(),
            r#"-3"#,
        ),
        (r#"upper(NULL)"#.to_string(), r#"NULL"#),
        (r#"CAST('1.5' AS DOUBLE)"#.to_string(), r#"1.5"#),
        (r#"CAST(2.7 AS BIGINT)"#.to_string(), r#"2"#),
        (r#"CAST('7' AS UBIGINT)"#.to_string(), r#"7"#),
        (r#"CAST(12 AS VARCHAR)"#.to_string(), r#"12"#),
        (r#"CAST('true' AS BOOLEAN)"#.to_string(), r#"true"#),
        (r#"CAST('2024-01-02 03:04:05' AS TIMESTAMP)"#.to_string(), r#"2024-01-02 03:04:05"#),
        (r#"CAST('2024-01-02 03:04:05' AS DATE)"#.to_string(), r#"2024-01-02 00:00:00"#),
        (r#"CAST('1.25' AS DECIMAL(10, 2))"#.to_string(), r#"1.25"#),
        (r#"'2.5'::DOUBLE + 1"#.to_string(), r#"3.5"#),
        (r#"DATE '2024-01-02'"#.to_string(), r#"2024-01-02 00:00:00"#),
        (r#"TRIM(' x ')"#.to_string(), r#"x"#),
        (r#"SUBSTRING('hello' FROM 2 FOR 3)"#.to_string(), r#"ell"#),
        (r#"SUBSTRING('hello' FROM 3)"#.to_string(), r#"llo"#),
        (r#"CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END"#.to_string(), r#"two"#),
        (r#"(1 < 2) IS TRUE"#.to_string(), r#"true"#),
        (r#"(1 < 2) IS FALSE"#.to_string(), r#"false"#),
        (r#"POSITION('l' IN 'hello')"#.to_string(), r#"3"#),
        (r#"'abc' NOT LIKE 'a%'"#.to_string(), r#"false"#),
        (r#"'abc' NOT ILIKE 'B%'"#.to_string(), r#"true"#),
        (r#"CEIL(1.2)"#.to_string(), r#"2"#),
        (r#"FLOOR(-1.2)"#.to_string(), r#"-2"#),
        (r#"'a' || 'b' || 1"#.to_string(), r#"ab1"#),
        (r#"-CAST(1.5 AS FLOAT)"#.to_string(), r#"-1.5"#),
        (r#"-CAST(3 AS UBIGINT)"#.to_string(), r#"-3"#),
        (r#"TRUE AND NOT FALSE"#.to_string(), r#"true"#),
        (r#"INTERVAL '90' SECOND"#.to_string(), r#"90000000"#),
        (r#"INTERVAL '2 hours'"#.to_string(), r#"7200000000"#),
        (r#"power(NULL, 2)"#.to_string(), r#"NULL"#),
    ];
    for (e, want) in cases {
        let got = run_on(&format!("SELECT {e} AS x"), &Serial, 1).unwrap_or_else(|err| panic!("{e}: {err}"));
        assert_eq!(got, [[want]], "{e}");
    }
    // what they refuse, and why
    for (e, why) in [
        ("date_trunc('nope', ts)", "unknown unit 'nope'"),
        ("date_trunc(symbol, ts)", "the unit is a string literal"),
        ("date_part('nope', ts)", "unknown part 'nope'"),
        ("date_part(symbol, ts)", "the part is a string literal"),
        ("strftime(ts, symbol)", "the format is a string literal"),
        ("time_bucket(symbol, ts)", "the width is a constant duration"),
        ("time_bucket('1m')", "expects 2 or 3 arguments"),
        ("date_diff(symbol, ts, ts)", "the unit is a string literal"),
        ("date_diff('day', ts)", "expects 3 arguments"),
        ("substr('a', 1, 2, 3)", "expects 2 or 3 arguments"),
        ("replace('a', 'b')", "expects 3 arguments"),
        ("split_part('a', 'b')", "expects 3 arguments"),
        ("lpad('a', 1)", "expects 3 arguments"),
        ("upper('a', 'b')", "expects 1 argument"),
        ("power(1)", "expects 2 arguments"),
        ("symbol LIKE symbol", "the pattern is a string literal"),
        ("regexp_matches(symbol, '(')", "unclosed group"),
        ("regexp_replace(symbol, 'a', symbol)", "replacement[, 'g']"),
        ("CAST(1 AS INTERVAL)", "CAST to INTERVAL is not supported"),
        ("INTERVAL 'soon'", "INTERVAL 'soon': a whole number"),
        ("X'0A'", "unsupported literal"),
    ] {
        let err = run_on(&format!("SELECT {e} AS x FROM trades"), &Serial, 1).unwrap_err();
        assert!(err.contains(why), "{e}: {err}");
    }
}

fn refused(sql: &str) -> String {
    run_on(sql, &Serial, 2).expect_err(sql)
}

/// What does not map is refused with what to write instead (ADR-0018), never ignored.
#[test]
fn refusals_say_what_to_write() {
    for (sql, why) in [
        ("INSERT INTO trades VALUES (1)", "only SELECT queries run here"),
        ("", "no query"),
        ("SELECT 1; SELECT 2", "one query at a time"),
        ("SELECT * FROM trades LIMIT 'x'", "is not a whole number"),
        ("SELECT * FROM trades LIMIT price", "is not a whole number"),
        ("SELECT * FROM trades LIMIT 1 BY symbol", "LIMIT BY is not supported"),
        ("SELECT symbol FROM trades ORDER BY nope", "unknown column nope"),
        ("SELECT symbol FROM trades ORDER BY 3", "ORDER BY 3: there are 1 columns"),
        ("SELECT * FROM trades GLOBAL JOIN quotes ON trades.symbol = quotes.symbol", "GLOBAL joins"),
        (
            "SELECT * FROM trades t ASOF JOIN quotes q MATCH_CONDITION t.ts >= q.ts ON t.symbol = q.symbol",
            "in parentheses",
        ),
        (
            "SELECT time_bucket_gapfill('1ms', ts, '1970-01-01', '2100-01-01') AS b, count(*) AS n FROM trades GROUP BY b",
            "more than 10000000 buckets",
        ),
        // found by fuzzing (fuzz/fuzz_targets/query.rs)
        (
            "SELECT * FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts ON t.symbol = q.symbol",
            "MATCH_CONDITION's parenthesis does not close",
        ),
        ("SELECT * FROM read_text('trades')", "unknown table function read_text"),
        ("SELECT * FROM read_csv(1)", "the path is a string"),
        ("SELECT * FROM read_csv(path => 'trades')", "the path is a string"),
        ("SELECT * FROM trades t RIGHT JOIN quotes q ON t.symbol = q.symbol", "RIGHT JOIN is not supported"),
        ("SELECT * FROM trades t FULL JOIN quotes q ON t.symbol = q.symbol", "FULL JOIN"),
        ("SELECT * FROM trades t CROSS JOIN quotes q", "CROSS JOIN"),
        ("SELECT * FROM trades t LEFT SEMI JOIN quotes q ON t.symbol = q.symbol", "SEMI JOIN"),
        ("SELECT * FROM trades t LEFT ANTI JOIN quotes q ON t.symbol = q.symbol", "ANTI JOIN"),
        ("SELECT * FROM trades t JOIN quotes q USING (symbol)", "a join needs ON"),
        ("SELECT x.* FROM trades t", "x.*: no table x"),
        (
            "SELECT * FROM trades t JOIN instruments i ON t.symbol = i.symbol OR t.price > 1",
            "unsupported join condition",
        ),
        ("SELECT * FROM trades t JOIN instruments i ON t.symbol <> i.symbol", "unsupported join condition"),
        ("SELECT * FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol", "ASOF JOIN needs the time"),
        (
            "SELECT * FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.price > q.bid * 2",
            "compare the times",
        ),
        (
            "SELECT * FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts + q.ts >= t.ts",
            "compare the times",
        ),
        (
            "SELECT * FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts > q.bid",
            "q.bid must be a time column",
        ),
        (
            "SELECT * FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts AND t.ts > q.ts",
            "one time condition",
        ),
        (
            "SELECT * FROM trades t ASOF JOIN (SELECT * FROM quotes) q ON t.symbol = q.symbol \
             AND t.ts + INTERVAL '1 second' >= q.ts",
            "must be a table, not a subquery",
        ),
        (
            "SELECT * FROM trades t JOIN (SELECT * FROM instruments) i ON t.symbol = i.symbol",
            "join a file or table, not a subquery",
        ),
        ("SELECT * FROM trades t JOIN instruments i ON 1 = 1", "join keys are a column of each table, not 1 = 1"),
        ("SELECT * FROM trades t JOIN instruments i ON t.symbol = 'A'", "not t.symbol = 'A'"),
        (
            "SELECT sum(price) OVER (PARTITION BY symbol) AS s FROM trades",
            "without ORDER BY is the whole partition's total",
        ),
        ("SELECT lag(price) OVER (ORDER BY ts, symbol) AS l FROM trades", "ORDER BY one time column"),
        (
            "SELECT time_bucket('1m', ts) AS m, count(*) AS n FROM \
             (SELECT ts, lead(price) OVER (PARTITION BY symbol ORDER BY ts) AS x FROM trades) GROUP BY m",
            "come when their next rows do",
        ),
        (
            "SELECT lead(price) OVER (PARTITION BY symbol ORDER BY ts) AS a, lead(price) OVER (ORDER BY ts) AS b FROM trades",
            "share one PARTITION BY",
        ),
        ("SELECT lead(price, 0) OVER (ORDER BY ts) AS a FROM trades", "lead offset"),
        (
            "SELECT w.n FROM trades t LEFT JOIN LATERAL (SELECT count(*) AS n FROM quotes q WHERE q.symbol = t.symbol) w ON true",
            "a window join needs its range",
        ),
        (
            "SELECT w.n FROM trades t LEFT JOIN LATERAL (SELECT count(*) AS n FROM quotes q \
             WHERE q.ts BETWEEN t.ts - INTERVAL '1 second' AND t.ts) w ON t.ts > 0",
            "JOIN LATERAL (...) ON true",
        ),
        (
            "SELECT w.n FROM trades t LEFT JOIN LATERAL (SELECT count(*) AS n FROM quotes q \
             WHERE q.ts BETWEEN t.ts AND t.ts - INTERVAL '1 second') w ON true",
            "ends before it starts",
        ),
        (
            "SELECT w.n FROM trades t LEFT JOIN LATERAL (SELECT count(*) AS n FROM quotes q \
             WHERE q.ts BETWEEN t.ts - INTERVAL '1 second' AND t.ts AND q.bid > t.price) w ON true",
            "unsupported window join condition",
        ),
        (
            "SELECT w.b FROM trades t LEFT JOIN LATERAL (SELECT q.bid AS b FROM quotes q \
             WHERE q.ts BETWEEN t.ts - INTERVAL '1 second' AND t.ts) w ON true",
            "unknown column",
        ),
        (
            "SELECT w.n FROM trades t LEFT JOIN LATERAL (SELECT count(*) AS n FROM quotes q JOIN instruments i \
             ON q.symbol = i.symbol WHERE q.ts BETWEEN t.ts - INTERVAL '1 second' AND t.ts) w ON true",
            "a window join is",
        ),
        ("SELECT lag(price) OVER (ORDER BY ts DESC) AS l FROM trades", "ascending"),
        ("SELECT lag(price) OVER (ORDER BY ts + 1) AS l FROM trades", "time order of their table's time column"),
        ("SELECT lag(price) OVER w AS l FROM trades", "OVER w: no WINDOW w AS (...)"),
        ("SELECT lag(price) OVER w AS l FROM trades WINDOW v AS (ORDER BY ts), w AS v", "write the window out"),
        (
            "SELECT symbol, count(*) OVER (ORDER BY ts) AS n FROM trades GROUP BY symbol",
            "window functions over GROUP BY",
        ),
        ("SELECT symbol, count(*) AS n FROM trades GROUP BY ROLLUP (symbol)", "ROLLUP"),
        ("SELECT symbol, count(*) AS n FROM trades GROUP BY symbol WITH TOTALS", "GROUP BY modifiers"),
        ("SELECT DISTINCT symbol, count(*) AS n FROM trades GROUP BY symbol", "DISTINCT with aggregates"),
        (
            "SELECT time_bucket('1m', ts) AS a, time_bucket('1h', ts) AS b, count(*) AS n FROM trades GROUP BY a, b",
            "one time_bucket",
        ),
        ("SELECT count(*) AS n FROM trades GROUP BY price + nope", "unknown column nope"),
        ("SELECT nope FROM trades", "unknown column nope"),
        (
            "SELECT time_bucket('1m', ts + INTERVAL '1 hour') AS m, count(*) AS n FROM trades GROUP BY m",
            "the time is a column of the table",
        ),
        ("SELECT time_bucket(price, ts) AS m, count(*) AS n FROM trades GROUP BY m", "a bucket width is a duration"),
        ("SELECT time_bucket(1, ts) AS m, count(*) AS n FROM trades GROUP BY m", "a bucket width is a duration"),
        ("SELECT time_bucket('1 fortnight', ts) AS m, count(*) AS n FROM trades GROUP BY m", "is not a duration"),
        (
            "SELECT time_bucket(INTERVAL (1 + 1) MINUTE, ts) AS m, count(*) AS n FROM trades GROUP BY m",
            "unsupported interval",
        ),
        ("SELECT symbol FROM trades UNION ALL SELECT symbol, price FROM trades", "UNION ALL of 1 and 2 columns"),
        ("SELECT symbol FROM trades QUALIFY price > 1", "QUALIFY"),
        ("SELECT * FROM trades, quotes", "FROM a, b is not supported"),
        ("SELECT * FROM (VALUES (1))", "unsupported query: VALUES"),
        ("SELECT * FROM trades INTERSECT SELECT * FROM trades", "unsupported query"),
        ("SELECT * FROM nope", "no table nope"),
        ("SELECT timezone('Mars/Olympus', ts) AS t FROM trades", "unknown time zone 'Mars/Olympus'"),
        (
            "SELECT time_bucket_gapfill('1m', ts) AS m, count(*) AS n FROM trades GROUP BY m HAVING count(*) > 1",
            "HAVING with time_bucket_gapfill",
        ),
        (
            "SELECT time_bucket_gapfill('1m', ts) AS m, locf(sum(size), 1) AS n FROM trades GROUP BY m",
            "locf(aggregate)",
        ),
        (
            "SELECT time_bucket_gapfill('1m', ts, 'x', 'y') AS m, count(*) AS n FROM trades GROUP BY m",
            "'x' is not a time",
        ),
        (
            "SELECT time_bucket_gapfill('1m', ts, 'UTC') AS m, count(*) AS n FROM trades GROUP BY m",
            "time_bucket_gapfill(width, time[, start, finish])",
        ),
        ("SELECT timezone(symbol, ts) AS t FROM trades", "unknown time zone (a string literal)"),
        (
            "SELECT time_bucket('1d', ts, 'Nowhere') AS d, count(*) AS n FROM trades GROUP BY d",
            "unknown time zone 'Nowhere'",
        ),
        (
            "SELECT time_bucket('1d', ts, symbol) AS d, count(*) AS n FROM trades GROUP BY d",
            "unknown time zone (a string literal)",
        ),
        (
            "SELECT time_bucket('1d', ts, 1, 2) AS d, count(*) AS n FROM trades GROUP BY d",
            "time_bucket(width, time[, zone])",
        ),
    ] {
        let err = refused(sql);
        assert!(err.contains(why), "{sql}: {err}");
    }
}

#[test]
fn clauses_and_table_functions() {
    // a table function names the reader; MATCH_CONDITION is an as-of join; ORDER BY an item's
    // expression, ALL, and LIMIT offset, count
    assert_eq!(run("SELECT count(*) AS n FROM read_parquet('trades')"), rows(&[&["6"]]));
    assert_eq!(run("SELECT count(*) AS n FROM read_csv_auto('trades') t"), rows(&[&["6"]]));
    assert_eq!(run("SELECT count(*) AS n FROM read_json('trades')"), rows(&[&["6"]]));
    assert_eq!(
        run("SELECT q.bid FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.symbol = q.symbol ORDER BY t.ts LIMIT 2"),
        rows(&[&["99.5"], &["49.5"]])
    );
    // the time condition either way round
    assert_eq!(
        run("SELECT q.bid FROM trades t ASOF JOIN quotes q ON q.ts <= t.ts AND q.symbol = t.symbol ORDER BY t.ts LIMIT 2"),
        rows(&[&["99.5"], &["49.5"]])
    );
    assert_eq!(run("SELECT t.*, i.venue FROM trades t JOIN instruments i ON t.symbol = i.symbol LIMIT 1")[0].len(), 5);
    assert_eq!(run("SELECT price * 2 FROM trades ORDER BY price * 2 DESC LIMIT 1"), rows(&[&["204"]]));
    assert_eq!(run("SELECT symbol, price FROM trades ORDER BY ALL LIMIT 1"), rows(&[&["A", "99"]]));
    assert_eq!(run("SELECT symbol, price FROM trades ORDER BY ALL DESC LIMIT 1"), rows(&[&["B", "51"]]));
    assert_eq!(run("SELECT price FROM trades ORDER BY price LIMIT 1, 2"), rows(&[&["51"], &["99"]]));
    assert_eq!(run("SELECT k FROM nulls ORDER BY k DESC NULLS LAST"), rows(&[&["b"], &["a"], &["a"], &["NULL"]]));
    assert_eq!(run("SELECT k FROM nulls ORDER BY k NULLS FIRST LIMIT 2"), rows(&[&["NULL"], &["a"]]));
    // GROUP BY ALL, an ema, a named window, a time bucket written as an interval
    assert_eq!(
        run("SELECT symbol, count(*) AS n FROM trades GROUP BY ALL ORDER BY symbol"),
        rows(&[&["A", "4"], &["B", "2"]])
    );
    assert_eq!(
        run("SELECT round(ema(price, 0.5) OVER w, 4) AS a, round(ema(price, 3) OVER w, 4) AS p FROM trades \
             WHERE symbol = 'B' WINDOW w AS (PARTITION BY symbol ORDER BY ts)"),
        rows(&[&["50", "50"], &["50.5", "50.5"]])
    );
    assert_eq!(
        run("SELECT time_bucket(INTERVAL '2' MINUTE, ts) AS m, count(*) AS n FROM trades GROUP BY m ORDER BY m"),
        rows(&[&["2024-01-01 00:00:00", "5"], &["2024-01-01 00:02:00", "1"]])
    );
}

/// `explain` shows the pipeline as SQL: each table's stream and how it is read, the views
/// (as-of joins as such) and the result.
#[test]
fn explain_shows_the_pipeline() {
    let c = compile(
        "SELECT time_bucket('1m', t.ts) AS m, i.venue, count(*) AS n FROM trades t \
         ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts JOIN instruments i ON t.symbol = i.symbol \
         GROUP BY m, i.venue",
        &mut |t| table(&name(t)).map(|x| x.0).ok_or(format!("no table {t}")),
    )
    .unwrap();
    let e = c.explain();
    for want in
        ["in ts order", "read first", "ASOF LEFT JOIN", "-- the result", "EMIT AFTER WINDOW CLOSE", "datetime64(6)"]
    {
        assert!(e.contains(want), "{want}:\n{e}");
    }
    let c = compile("SELECT * FROM types", &mut |_| {
        Ok(vec![
            ("b".into(), Type::Bool),
            ("u".into(), Type::UInt(64)),
            ("f".into(), Type::F32),
            ("a".into(), Type::Array(Box::new(Type::Int(64)))),
        ])
    })
    .unwrap();
    let e = c.explain();
    for want in ["b nullable(bool)", "u nullable(uint64)", "f nullable(float32)", "array(int64)", "in file order"] {
        assert!(e.contains(want), "{want}:\n{e}");
    }
    // an unknown column says what the tables have
    let err = refused("SELECT symbol FROM trades WHERE nope > 1");
    assert!(err.contains("trades has ts, symbol, price, size"), "{err}");
}

#[test]
fn order_by_sorts_keys_of_every_kind_stably_and_its_limit_keeps_the_first() {
    use std::cmp::Ordering;
    let (_, table) = table("sortme").unwrap();
    // the order SQL says, ties in the order the rows came
    let oracle = |keys: &[(usize, bool, bool)], offset: usize, limit: usize| -> Vec<Vec<String>> {
        let value = |a: &Value, b: &Value| match (a, b) {
            (Value::F64(x), Value::F64(y)) if x == y => Ordering::Equal,
            (Value::F64(x), Value::F64(y)) => match (x.is_nan(), y.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                _ => x.total_cmp(y),
            },
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::Str(x), Value::Str(y)) => x.cmp(y),
            _ => unreachable!(),
        };
        let mut rows = table.clone();
        rows.sort_by(|a, b| {
            keys.iter()
                .map(|&(k, desc, nulls_first)| match (a[k].is_null(), b[k].is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        if nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    _ if desc => value(&b[k], &a[k]),
                    _ => value(&a[k], &b[k]),
                })
                .find(|o| o.is_ne())
                .unwrap_or(Ordering::Equal)
        });
        rows.iter().skip(offset).take(limit).map(|r| vec![text(&r[0])]).collect()
    };
    for (order, keys, offset, limit) in [
        ("x NULLS LAST", vec![(2, false, false)], 0, usize::MAX),
        ("x DESC NULLS FIRST", vec![(2, true, true)], 0, usize::MAX),
        ("k DESC NULLS LAST, n NULLS FIRST", vec![(1, true, false), (3, false, true)], 0, usize::MAX),
        ("x DESC NULLS LAST, k NULLS FIRST", vec![(2, true, false), (1, false, true)], 3, 25),
        ("n NULLS LAST, x NULLS LAST", vec![(3, false, false), (2, false, false)], 0, 5),
        ("k NULLS FIRST, i DESC", vec![(1, false, true), (0, true, true)], 0, usize::MAX),
    ] {
        let paging = match (offset, limit) {
            (0, usize::MAX) => String::new(),
            (o, l) => format!(" LIMIT {l} OFFSET {o}"),
        };
        assert_eq!(
            run(&format!("SELECT i FROM sortme ORDER BY {order}{paging}")),
            oracle(&keys, offset, limit),
            "{order}"
        );
    }
}
