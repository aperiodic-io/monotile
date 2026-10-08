//! Statements over real files: CSV (plain, gzipped, tab-separated), JSON lines, Parquet, globs,
//! directories with Hive partitions, views, and COPY's round trips.
use brrrrr_core::query::text;
use brrrrr_lake::{Answer, Lake};
use std::io::Write;
use std::path::Path;

const TRADES: &str = "ts,symbol,price,size,side\n\
2024-01-01T00:00:10,A,100,1,buy\n\
2024-01-01T00:00:20,B,50,2,sell\n\
2024-01-01T00:00:30,A,101,2,buy\n\
2024-01-01T00:01:05,A,99,1,sell\n\
2024-01-01T00:01:10,B,51,1,buy\n\
2024-01-01T00:02:00,A,102,3,buy\n";

fn texts(a: &Answer) -> Vec<Vec<String>> {
    a.rows.iter().map(|r| r.iter().map(text).collect()).collect()
}

fn lake(dir: &Path) -> Lake {
    std::env::set_var("BRRRRR_CACHE", dir.join("cache"));
    let mut l = Lake::new();
    l.threads = 2;
    l
}

fn q(l: &mut Lake, sql: &str) -> Vec<Vec<String>> {
    texts(&l.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}")))
}

#[test]
fn csv_gzip_tsv_and_json_are_read_alike() {
    let d = tempfile::tempdir().unwrap();
    let p = |n: &str| d.path().join(n).display().to_string();
    std::fs::write(p("t.csv"), TRADES).unwrap();
    let mut gz = flate2::write::GzEncoder::new(std::fs::File::create(p("t.csv.gz")).unwrap(), Default::default());
    gz.write_all(TRADES.as_bytes()).unwrap();
    gz.finish().unwrap();
    std::fs::write(p("t.tsv"), TRADES.replace(',', "\t")).unwrap();
    std::fs::write(p("semi.csv"), TRADES.replace(',', ";")).unwrap();
    let json: String = TRADES
        .lines()
        .skip(1)
        .map(|l| {
            let f: Vec<&str> = l.split(',').collect();
            format!(r#"{{"ts":"{}","symbol":"{}","price":{},"size":{},"side":"{}"}}"#, f[0], f[1], f[2], f[3], f[4])
                + "\n"
        })
        .collect();
    std::fs::write(p("t.jsonl"), json).unwrap();
    let mut l = lake(d.path());
    let bars = |f: &str| {
        format!(
            "SELECT time_bucket('1m', ts) AS m, symbol, first(price, ts) AS o, last(price, ts) AS c, sum(size) AS v \
             FROM '{f}' GROUP BY m, symbol ORDER BY m, symbol"
        )
    };
    let expected = q(&mut l, &bars(&p("t.csv")));
    assert_eq!(expected.len(), 5);
    assert_eq!(expected[0], ["2024-01-01 00:00:00", "A", "100", "101", "3"]);
    for f in ["t.csv.gz", "t.tsv", "semi.csv", "t.jsonl"] {
        assert_eq!(q(&mut l, &bars(&p(f))), expected, "{f}");
    }
}

#[test]
fn parquet_written_by_copy_reads_back_the_same() {
    let d = tempfile::tempdir().unwrap();
    let p = |n: &str| d.path().join(n).display().to_string();
    std::fs::write(p("t.csv"), TRADES).unwrap();
    let mut l = lake(d.path());
    let all = q(&mut l, &format!("SELECT * FROM '{}' ORDER BY ts", p("t.csv")));
    let msg = l.execute(&format!("COPY (SELECT * FROM '{}') TO '{}'", p("t.csv"), p("out/t.parquet"))).unwrap();
    assert_eq!(msg.message.as_deref(), Some(format!("6 rows written to {}", p("out/t.parquet")).as_str()));
    assert_eq!(q(&mut l, &format!("SELECT * FROM '{}' ORDER BY ts", p("out/t.parquet"))), all);
    // and CSV and JSON
    for f in ["out/t.csv", "out/t.json"] {
        l.execute(&format!("COPY (FROM '{}') TO '{}'", p("t.csv"), p(f))).unwrap();
        assert_eq!(q(&mut l, &format!("FROM '{}' ORDER BY ts", p(f))), all, "{f}");
    }
    let desc = q(&mut l, &format!("DESCRIBE '{}'", p("out/t.parquet")));
    assert_eq!(desc[0], ["ts", "timestamp"]);
    assert_eq!(desc[2], ["price", "bigint"]); // the CSV's whole numbers
}

#[test]
fn globs_directories_and_hive_partitions() {
    let d = tempfile::tempdir().unwrap();
    let lines: Vec<&str> = TRADES.lines().collect();
    for (day, rows) in [("2024-01-01", &lines[1..4]), ("2024-01-02", &lines[4..7])] {
        let dir = d.path().join(format!("trades/date={day}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("part-0.csv"), format!("{}\n{}\n", lines[0], rows.join("\n"))).unwrap();
    }
    let mut l = lake(d.path());
    let dir = d.path().join("trades").display().to_string();
    assert_eq!(
        q(&mut l, &format!("SELECT date, count(*) AS n FROM '{dir}' GROUP BY date ORDER BY date")),
        [["2024-01-01", "3"], ["2024-01-02", "3"]]
    );
    assert_eq!(q(&mut l, &format!("SELECT count(*) AS n FROM '{dir}/*/*.csv'")), [["6"]]);
    assert_eq!(q(&mut l, &format!("SELECT count(*) AS n FROM '{dir}/date=2024-01-02/*.csv'")), [["3"]]);
}

#[test]
fn a_where_on_partitions_reads_only_their_files() {
    let d = tempfile::tempdir().unwrap();
    let csv = d.path().join("t.csv").display().to_string();
    std::fs::write(&csv, TRADES).unwrap();
    let t = d.path().join("trades").display().to_string();
    let mut l = lake(d.path());
    for (day, hour) in [("2024-01-01", 9), ("2024-01-01", 10), ("2024-01-02", 9)] {
        l.execute(&format!("COPY (FROM '{csv}') TO '{t}/date={day}/hour={hour}/part-0.parquet'")).unwrap();
    }
    // a partition the queries below leave out: not Parquet, so reading it fails
    std::fs::create_dir_all(format!("{t}/date=2024-01-03/hour=9")).unwrap();
    std::fs::write(format!("{t}/date=2024-01-03/hour=9/part-0.parquet"), "not Parquet").unwrap();
    let err = format!("{:#}", l.execute(&format!("SELECT count(*) FROM '{t}'")).unwrap_err());
    assert!(err.contains("date=2024-01-03/hour=9/part-0.parquet: not a Parquet file"), "{err}");
    // a partition of whole numbers is a number, compared as one (9 < 10)
    assert_eq!(q(&mut l, &format!("DESCRIBE '{t}'"))[5..], [["date", "varchar"], ["hour", "bigint"]]);
    let n = |l: &mut Lake, w: &str| q(l, &format!("SELECT count(*) FROM '{t}' WHERE {w}"))[0][0].clone();
    assert_eq!(n(&mut l, "date = '2024-01-01'"), "12");
    assert_eq!(n(&mut l, "date IN ('2024-01-01', '2024-01-02') AND hour = 9"), "12");
    assert_eq!(n(&mut l, "date BETWEEN '2024-01-02' AND '2024-01-02' AND symbol = 'A'"), "4");
    assert_eq!(n(&mut l, "hour >= 10"), "6");
    assert_eq!(n(&mut l, "hour < 10 AND date < '2024-01-03'"), "12");
    assert_eq!(n(&mut l, "date > '2024-01-03'"), "0");
    // a named table, a view of it, and a join: each table by its own conditions
    l.execute(&format!("CREATE TABLE trades AS '{t}'")).unwrap();
    l.execute("CREATE VIEW hours AS SELECT date, hour, price FROM trades").unwrap();
    assert_eq!(
        q(&mut l, "SELECT date, hour, max(price) FROM hours WHERE date <= '2024-01-02' GROUP BY date, hour ORDER BY date, hour"),
        [["2024-01-01", "9", "102"], ["2024-01-01", "10", "102"], ["2024-01-02", "9", "102"]]
    );
    let join =
        format!("SELECT count(*) FROM trades JOIN '{csv}' AS c ON trades.ts = c.ts WHERE trades.date = '2024-01-02'");
    assert_eq!(q(&mut l, &join), [["6"]]);
}

#[test]
fn a_where_fixing_leading_partitions_lists_only_their_directories() {
    let d = tempfile::tempdir().unwrap();
    let csv = d.path().join("t.csv").display().to_string();
    std::fs::write(&csv, TRADES).unwrap();
    let t = d.path().join("trades").display().to_string();
    let mut l = lake(d.path());
    // hours are numbers on the first two days; on the third, one is not (`hour` is text when
    // its directories are listed)
    for (day, hour) in [("2024-01-01", "9"), ("2024-01-02", "9"), ("2024-01-02", "10"), ("2024-01-03", "x")] {
        l.execute(&format!("COPY (FROM '{csv}') TO '{t}/date={day}/hour={hour}/part-0.parquet'")).unwrap();
    }
    let n = |l: &mut Lake, w: &str| {
        l.execute(&format!("SELECT count(*) FROM '{t}' WHERE {w}"))
            .map(|a| texts(&a)[0][0].clone())
            .map_err(|e| format!("{e:#}"))
    };
    let err = n(&mut l, "hour >= 10").unwrap_err();
    assert!(err.contains("compares a string with a number"), "{err}");
    assert_eq!(n(&mut l, "date = '2024-01-02' AND hour >= 10").unwrap(), "6");
    assert_eq!(n(&mut l, "date IN ('2024-01-01', '2024-01-02') AND hour = 9").unwrap(), "12");
    // no directory of the value: no rows (the columns are the table's first file's)
    assert_eq!(n(&mut l, "date = '2023-12-31'").unwrap(), "0");
    assert_eq!(q(&mut l, &format!("SELECT * FROM '{t}' WHERE date = '2023-12-31'")).len(), 0);
    // through a view
    l.execute(&format!("CREATE VIEW v AS SELECT price, hour, date FROM '{t}'")).unwrap();
    assert_eq!(
        q(&mut l, "SELECT hour, count(*) FROM v WHERE date = '2024-01-02' AND hour = 10 GROUP BY hour"),
        [["10", "6"]]
    );
}

#[test]
fn copy_partition_by_writes_hive_directories_that_read_back() {
    let d = tempfile::tempdir().unwrap();
    let csv = d.path().join("t.csv").display().to_string();
    std::fs::write(&csv, TRADES).unwrap();
    let out = d.path().join("out/trades").display().to_string();
    let mut l = lake(d.path());
    let copy =
        |opts: &str| format!("COPY (SELECT strftime(ts, '%Y-%m-%d') AS date, * FROM '{csv}') TO '{out}' ({opts})");
    let msg = l.execute(&copy("FORMAT parquet, PARTITION_BY (date, symbol)")).unwrap();
    assert_eq!(msg.message.unwrap(), format!("6 rows written to {out}, in 2 partitions"));
    // DuckDB's layout: the partition columns in the path, not in the files
    let a = format!("{out}/date=2024-01-01/symbol=A/data_0.parquet");
    let b = format!("{out}/date=2024-01-01/symbol=B/data_0.parquet");
    assert!(Path::new(&b).is_file());
    assert_eq!(
        q(&mut l, &format!("DESCRIBE '{a}'")).iter().map(|r| r[0].as_str()).collect::<Vec<_>>(),
        ["ts", "price", "size", "side"]
    );
    // read back: the same rows, the partitions' columns from the path, pruned by them
    let cols = "ts, symbol, price, size, side, date";
    assert_eq!(
        q(&mut l, &format!("SELECT {cols} FROM '{out}' ORDER BY ts")),
        q(
            &mut l,
            &format!("SELECT ts, symbol, price, size, side, strftime(ts, '%Y-%m-%d') AS date FROM '{csv}' ORDER BY ts")
        )
    );
    let good = std::fs::read(&b).unwrap();
    std::fs::write(&b, "not Parquet").unwrap();
    assert_eq!(q(&mut l, &format!("SELECT count(*) FROM '{out}' WHERE symbol = 'A'")), [["4"]]);
    std::fs::write(&b, good).unwrap();
    // into a directory with files: refused, unless OVERWRITE_OR_IGNORE (same names replaced)
    let err = format!("{:#}", l.execute(&copy("PARTITION_BY (date, symbol)")).unwrap_err());
    assert!(err.contains("is not empty: write into it with OVERWRITE_OR_IGNORE"), "{err}");
    l.execute(&copy("PARTITION_BY (date, symbol), OVERWRITE_OR_IGNORE")).unwrap();
    assert_eq!(q(&mut l, &format!("SELECT count(*) FROM '{out}'")), [["6"]]);
    // values escaped as DuckDB escapes them, NULL as Hive's default partition; CSV files
    let tags = d.path().join("tags").display().to_string();
    l.execute(&format!(
        "COPY (SELECT price, CASE WHEN side = 'buy' THEN 'a b/c' END AS tag FROM '{csv}') TO '{tags}' (FORMAT csv, PARTITION_BY tag)"
    ))
    .unwrap();
    assert!(Path::new(&format!("{tags}/tag=a%20b%2Fc/data_0.csv")).is_file());
    assert!(Path::new(&format!("{tags}/tag=__HIVE_DEFAULT_PARTITION__/data_0.csv")).is_file());
    assert_eq!(
        q(&mut l, &format!("SELECT tag, count(*), sum(price) FROM '{tags}' GROUP BY tag ORDER BY tag")),
        [["a b/c", "4", "354"], ["NULL", "2", "149"]]
    );
    assert_eq!(q(&mut l, &format!("SELECT count(*) FROM '{tags}' WHERE tag = 'a b/c'")), [["4"]]);
    let err = format!(
        "{:#}",
        l.execute(&format!("COPY (FROM '{csv}') TO '{}' (PARTITION_BY (nope))", d.path().join("x").display()))
            .unwrap_err()
    );
    assert!(err.contains("PARTITION_BY nope: the result has no column nope"), "{err}");
}

#[test]
fn a_join_looks_up_a_subquery_cte_or_view_run_first() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("t.csv").display().to_string();
    std::fs::write(&path, TRADES).unwrap();
    let mut l = lake(d.path());
    let avgs = format!("SELECT symbol, avg(size) AS a FROM '{path}' GROUP BY symbol");
    let big = [["2024-01-01 00:00:20", "B", "2"], ["2024-01-01 00:00:30", "A", "2"], ["2024-01-01 00:02:00", "A", "3"]];
    let each =
        |from: &str| format!("SELECT t.ts, t.symbol, t.size FROM '{path}' t {from} WHERE t.size > s.a ORDER BY t.ts");
    assert_eq!(q(&mut l, &each(&format!("JOIN ({avgs}) s ON t.symbol = s.symbol"))), big);
    assert_eq!(q(&mut l, &format!("WITH s AS ({avgs}) {}", each("JOIN s ON t.symbol = s.symbol"))), big);
    l.execute(&format!("CREATE VIEW avgs AS {avgs}")).unwrap();
    assert_eq!(q(&mut l, &each("LEFT JOIN avgs AS s ON t.symbol = s.symbol")), big);
    // a live view's pipeline runs as rows arrive: a subquery's rows would be those of its start
    let err = format!("{:#}", l.compile(&each("JOIN avgs AS s ON t.symbol = s.symbol")).unwrap_err());
    assert!(err.contains("a live view joins tables, not subqueries"), "{err}");
    // ASOF: a subquery's computed time ordered as a table's
    let bars =
        format!("SELECT time_bucket('1m', ts) AS m, symbol, max(price) AS high FROM '{path}' GROUP BY m, symbol");
    assert_eq!(
        q(&mut l, &format!("SELECT count(b.high), sum(b.high) FROM '{path}' t ASOF JOIN ({bars}) b ON t.symbol = b.symbol AND t.ts >= b.m")),
        [["6", "504"]] // 101 + 50 + 101 + 99 + 51 + 102
    );
}

#[test]
fn tables_views_and_the_statements_around_them() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("t.csv").display().to_string();
    std::fs::write(&path, TRADES).unwrap();
    let mut l = lake(d.path());
    l.execute(&format!("CREATE TABLE trades AS '{path}'")).unwrap();
    l.execute("CREATE VIEW buys AS SELECT * FROM trades WHERE side = 'buy'").unwrap();
    l.execute("CREATE OR REPLACE VIEW big AS SELECT symbol, sum(size) AS v FROM buys GROUP BY symbol").unwrap();
    assert_eq!(q(&mut l, "FROM big ORDER BY symbol"), [["A", "6"], ["B", "1"]]);
    assert_eq!(q(&mut l, "SHOW TABLES").len(), 3);
    let plan = q(&mut l, "EXPLAIN SELECT symbol, count(*) FROM trades GROUP BY symbol");
    assert!(plan.iter().any(|r| r[0].contains("tumble")), "{plan:?}");
    l.execute("DROP VIEW big").unwrap();
    assert!(l.execute("FROM big").is_err());
    let err = format!("{:#}", l.execute("SELECT * FROM nope").unwrap_err());
    assert!(err.contains("no table nope") && err.contains("CREATE TABLE"), "{err}");
}

#[test]
fn a_table_shows_fractions_to_read_and_the_other_formats_keep_every_digit() {
    use brrrrr_core::value::Value;
    let rows: brrrrr_lake::write::Rows =
        vec![vec![Value::F64(18.19910000000001), Value::F64(42202.06860687289), Value::Int(3)]].into();
    let cols = ["v".to_string(), "w".to_string(), "n".to_string()];
    let t = brrrrr_lake::write::table(&cols, &rows, 40);
    assert!(t.contains("│ 18.1991 │ 42202.06861 │ 3 │"), "{t}");
    let mut csv = vec![];
    brrrrr_lake::write::write(&mut csv, brrrrr_lake::write::Out::Csv, &cols, &rows, usize::MAX).unwrap();
    assert!(String::from_utf8(csv).unwrap().contains("18.19910000000001,42202.06860687289,3"));
}

#[test]
fn files_whose_columns_differ_read_by_name() {
    let d = tempfile::tempdir().unwrap();
    let p = |n: &str| d.path().join(n).display().to_string();
    let mut l = lake(d.path());
    for (f, q) in [("1", "SELECT 1 AS a, 10 AS b"), ("2", "SELECT 2 AS a"), ("3", "SELECT 3.5 AS a, 30 AS b, 'x' AS c")]
    {
        l.execute(&format!("COPY ({q}) TO '{}'", p(&format!("pq/f{f}.parquet")))).unwrap();
        let body = q.replace("SELECT ", "");
        let csv = body.split(", ").map(|c| c.split(" AS ").collect::<Vec<_>>()).collect::<Vec<_>>();
        let head: Vec<&str> = csv.iter().map(|c| c[1]).collect();
        let row: Vec<&str> = csv.iter().map(|c| c[0].trim_matches('\'')).collect();
        std::fs::create_dir_all(p("cs")).unwrap();
        std::fs::write(p(&format!("cs/f{f}.csv")), format!("{}\n{}\n", head.join(","), row.join(","))).unwrap();
    }
    // by default the first file's columns: one a later file lacks is NULL there
    assert_eq!(
        q(&mut l, &format!("SELECT a, b FROM '{}' ORDER BY a", p("pq/f[12].parquet"))),
        [["1", "10"], ["2", "NULL"]]
    );
    // ... and a column of another type in a later file is an error that says what to do
    let err = format!("{:#}", l.execute(&format!("SELECT * FROM '{}'", p("pq/*.parquet"))).unwrap_err());
    assert!(err.contains("f3.parquet: column a is double here and ubigint in the table's first file"), "{err}");
    assert!(err.contains("union_by_name = true"), "{err}");
    // union_by_name: every file's columns, by name, types widened
    for (r, dir) in [("read_parquet", "pq/*.parquet"), ("read_csv", "cs/*.csv")] {
        let src = format!("{r}('{}', union_by_name = true)", p(dir));
        assert_eq!(
            q(&mut l, &format!("SELECT a, b, c FROM {src} ORDER BY a")),
            [["1", "10", "NULL"], ["2", "NULL", "NULL"], ["3.5", "30", "x"]],
            "{r}"
        );
        // brrrrr writes its whole numbers unsigned; CSV's are read signed
        let b = if r == "read_csv" { "bigint" } else { "ubigint" };
        assert_eq!(q(&mut l, &format!("DESCRIBE {src}")), [["a", "double"], ["b", b], ["c", "varchar"]], "{r}");
    }
}

#[test]
fn times_as_exports_write_them() {
    let d = tempfile::tempdir().unwrap();
    let p = |n: &str| d.path().join(n).display().to_string();
    let mut l = lake(d.path());
    // kdb+'s, and nanoseconds: kept to the microsecond
    std::fs::write(
        p("kdb.csv"),
        "ts,px\n2024.01.02D09:30:00.123456789,1\n2024.01.02T09:30:01.500,2\n2024-01-02 09:30:02.000000999,3\n",
    )
    .unwrap();
    assert_eq!(
        q(&mut l, &format!("SELECT ts, time_bucket('1s', ts) AS s FROM '{}' ORDER BY ts", p("kdb.csv"))),
        [
            ["2024-01-02 09:30:00.123456", "2024-01-02 09:30:00"],
            ["2024-01-02 09:30:01.5", "2024-01-02 09:30:01"],
            ["2024-01-02 09:30:02", "2024-01-02 09:30:02"]
        ]
    );
    std::fs::write(p("kdb.jsonl"), "{\"ts\":\"2024.01.02D09:30:00.123456789\",\"px\":1}\n").unwrap();
    assert_eq!(q(&mut l, &format!("SELECT ts FROM '{}'", p("kdb.jsonl"))), [["2024-01-02 09:30:00.123456"]]);
    // a format of one's own
    std::fs::write(p("eu.csv"), "when,px\n02/01/2024 09:30,1\n").unwrap();
    let eu = format!("read_csv('{}', timestampformat = '%d/%m/%Y %H:%M')", p("eu.csv"));
    assert_eq!(q(&mut l, &format!("SELECT \"when\" FROM {eu}")), [["2024-01-02 09:30:00"]]);
    // a vendor's trades: times as microseconds since the epoch, read as times
    let trades = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n\
        venue,BTCUSDT,1704067200012000,1704067200015123,1,buy,42000.5,0.01\n\
        venue,BTCUSDT,1704067259999000,1704067260001000,2,sell,42001,0.5\n\
        venue,BTCUSDT,1704067260000500,1704067260002000,3,buy,42002,1\n";
    let mut gz = flate2::write::GzEncoder::new(std::fs::File::create(p("trades.csv.gz")).unwrap(), Default::default());
    gz.write_all(trades.as_bytes()).unwrap();
    gz.finish().unwrap();
    let t = format!(
        "read_csv('{}', types = {{'timestamp': 'TIMESTAMP_US', 'local_timestamp': 'TIMESTAMP_US'}})",
        p("trades.csv.gz")
    );
    assert_eq!(
        q(
            &mut l,
            &format!("SELECT time_bucket('1m', timestamp) AS m, count(*), sum(amount) FROM {t} GROUP BY m ORDER BY m")
        ),
        [["2024-01-01 00:00:00", "2", "0.51"], ["2024-01-01 00:01:00", "1", "1"]]
    );
    assert_eq!(q(&mut l, &format!("SELECT local_timestamp FROM {t} LIMIT 1")), [["2024-01-01 00:00:00.015123"]]);
    // a TIMESTAMP of numbers says which unit to read them in
    let err = format!(
        "{:#}",
        l.execute(&format!("SELECT * FROM read_csv('{}', types = {{'timestamp': 'TIMESTAMP'}})", p("trades.csv.gz")))
            .unwrap_err()
    );
    assert!(
        err.contains("trades.csv.gz: line 2, column timestamp: 1704067200012000 is a number: TIMESTAMP_S"),
        "{err}"
    );
}

#[test]
fn csv_as_it_comes() {
    let d = tempfile::tempdir().unwrap();
    let p = |n: &str| d.path().join(n).display().to_string();
    let mut l = lake(d.path());
    // a byte order mark, CRLF line ends, quoted delimiters and line breaks
    std::fs::write(p("excel.csv"), "\u{feff}name,note\r\n\"Smith, J\",\"two\r\nlines\"\r\nLee,\"\"\r\n").unwrap();
    assert_eq!(
        q(&mut l, &format!("SELECT name, note FROM '{}'", p("excel.csv"))),
        [["Smith, J", "two\r\nlines"], ["Lee", "NULL"]]
    );
    // NULL's texts; without a header; lines to skip; a delimiter of one's own
    std::fs::write(p("na.csv"), "x,y\nNA,1\n2,null\n3,\n").unwrap();
    assert_eq!(
        q(&mut l, &format!("SELECT x, y FROM read_csv('{}', nullstr = ['NA', 'null']) ORDER BY x", p("na.csv"))),
        [["2", "NULL"], ["3", ""], ["NULL", "1"]]
    );
    std::fs::write(p("raw.txt"), "# exported 2024-01-02\n1;a\n2;b\n").unwrap();
    let raw = format!("read_csv('{}', header = false, skip = 1, delim = ';')", p("raw.txt"));
    assert_eq!(q(&mut l, &format!("SELECT column0, column1 FROM {raw}")), [["1", "a"], ["2", "b"]]);
    let named =
        format!("read_csv('{}', skip = 1, delim = ';', columns = {{'n': 'BIGINT', 's': 'VARCHAR'}})", p("raw.txt"));
    assert_eq!(q(&mut l, &format!("SELECT sum(n) FROM {named}")), [["3"]]);
    // a value not of its column's type: the file, line and column
    std::fs::write(p("bad.csv"), "a,b\n1,x\n2,y\nthree,z\n").unwrap();
    let err = format!(
        "{:#}",
        l.execute(&format!("SELECT * FROM read_csv('{}', types = {{'a': 'BIGINT'}})", p("bad.csv"))).unwrap_err()
    );
    assert!(
        err.contains("bad.csv: Parser error: Error while parsing value 'three' as type 'Int64' for column a at line 4"),
        "{err}"
    );
    // an unknown option is refused, naming those there are
    let err = format!("{:#}", l.execute(&format!("FROM read_csv('{}', hedaer = true)", p("bad.csv"))).unwrap_err());
    assert!(err.contains("reader option hedaer is not one brrrrr reads: header, delim"), "{err}");
    // thousands of columns
    let wide: Vec<String> = (0..3000).map(|i| format!("c{i}")).collect();
    let row: Vec<String> = (0..3000).map(|i| i.to_string()).collect();
    std::fs::write(p("wide.csv"), format!("{}\n{}\n", wide.join(","), row.join(","))).unwrap();
    assert_eq!(q(&mut l, &format!("SELECT c0 + c2999 FROM '{}'", p("wide.csv"))), [["2999"]]);
}

/// The fixtures' tables: `fixtures/tables/` (generated by delta-rs and pyiceberg).
fn fixture(name: &str) -> String {
    format!("{}/../../fixtures/tables/{name}", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn delta_and_iceberg_tables_read_as_their_logs_say() {
    let d = tempfile::tempdir().unwrap();
    let mut l = lake(d.path());
    let (delta, iceberg) = (fixture("delta_trades"), fixture("iceberg_trades"));
    // Delta: a checkpoint, then a commit; day 1 rewritten (its old file still on disk, removed in
    // the log), partitions from the log
    let by_day = [["2024-01-01", "2"], ["2024-01-02", "2"], ["2024-01-03", "1"]];
    for from in [format!("delta_scan('{delta}')"), format!("'{delta}'")] {
        assert_eq!(
            q(&mut l, &format!("SELECT date, count(*) FROM {from} GROUP BY date ORDER BY date")),
            by_day,
            "{from}"
        );
    }
    // Iceberg: the current snapshot's files (two appends); a table copied elsewhere is read
    // where it is, not where its metadata says it was written
    let two_days = [["2024-01-01", "3"], ["2024-01-02", "2"]];
    let metadata = format!("{iceberg}/metadata/00003-7c60c6c1-d3a5-4d2f-88f9-ca340b678d9a.metadata.json");
    for from in [format!("iceberg_scan('{iceberg}')"), format!("'{iceberg}'"), format!("iceberg_scan('{metadata}')")] {
        assert_eq!(
            q(&mut l, &format!("SELECT date, count(*) FROM {from} GROUP BY date ORDER BY date")),
            two_days,
            "{from}"
        );
    }
    // an earlier snapshot: its metadata file
    let first = format!("iceberg_scan('{iceberg}/metadata/00002-70c923ad-e7c4-4dc0-82ce-088f6061a6c2.metadata.json')");
    assert_eq!(q(&mut l, &format!("SELECT count(*) FROM {first}")), [["3"]]);
    assert_eq!(
        q(&mut l, &format!("DESCRIBE '{delta}'")),
        [["ts", "timestamp"], ["symbol", "varchar"], ["price", "double"], ["size", "bigint"], ["date", "varchar"]]
    );
    // a WHERE on the partitions reads only their files: a broken copy of each table shows it
    for (name, gone) in [("delta_trades", "date=2024-01-02"), ("iceberg_trades", "data/date=2024-01-02")] {
        let copy = d.path().join(name);
        copy_dir(Path::new(&fixture(name)), &copy);
        for f in std::fs::read_dir(copy.join(gone)).unwrap() {
            std::fs::write(f.unwrap().path(), "not Parquet").unwrap();
        }
        let t = copy.display();
        let day1 = if name == "delta_trades" { "2" } else { "3" };
        assert_eq!(q(&mut l, &format!("SELECT count(*) FROM '{t}' WHERE date = '2024-01-01'")), [[day1]], "{name}");
        let err = format!("{:#}", l.execute(&format!("SELECT count(*) FROM '{t}'")).unwrap_err());
        assert!(err.contains("not a Parquet file"), "{name}: {err}");
    }
}

#[test]
fn what_a_table_changes_beyond_its_files_is_refused() {
    let d = tempfile::tempdir().unwrap();
    let mut l = lake(d.path());
    let t = d.path().join("dv");
    copy_dir(Path::new(&fixture("delta_trades")), &t);
    // a commit that deletes rows of a file with a deletion vector
    let add = r#"{"add":{"path":"date=2024-01-03/part-00000-c2151924-5f83-4f9e-a0e8-ffa3297e3446-c000.snappy.parquet","partitionValues":{"date":"2024-01-03"},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{"storageType":"u","pathOrInlineDv":"x","offset":1,"sizeInBytes":1,"cardinality":1}}}"#;
    std::fs::write(t.join("_delta_log/00000000000000000004.json"), format!("{add}\n")).unwrap();
    let err = format!("{:#}", l.execute(&format!("SELECT count(*) FROM '{}'", t.display())).unwrap_err());
    assert!(err.contains("has deleted rows (a deletion vector), which brrrrr does not read yet"), "{err}");
    // a reader feature brrrrr has not
    let proto =
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz","variantType"]}}"#;
    std::fs::write(t.join("_delta_log/00000000000000000004.json"), format!("{proto}\n")).unwrap();
    let err = format!("{:#}", l.execute(&format!("SELECT count(*) FROM '{}'", t.display())).unwrap_err());
    assert!(err.contains("it needs the reader feature variantType, which brrrrr does not have"), "{err}");
    // a commit missing from the log
    std::fs::rename(t.join("_delta_log/00000000000000000004.json"), t.join("_delta_log/00000000000000000005.json"))
        .unwrap();
    let err = format!("{:#}", l.execute(&format!("SELECT count(*) FROM '{}'", t.display())).unwrap_err());
    assert!(err.contains("its log has no commit 4"), "{err}");
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.path().is_dir() {
            copy_dir(&e.path(), &to.join(e.file_name()));
        } else {
            std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

#[test]
fn databento_dbn_reads_as_databento_writes_its_csv() {
    let d = tempfile::tempdir().unwrap();
    let mut l = lake(d.path());
    let dir = format!("{}/../../fixtures/dbn", env!("CARGO_MANIFEST_DIR"));
    // each schema field by field as Databento's own CSV of it (pretty prices and times, symbols
    // mapped: `fixtures/dbn/gen.py`), plain and zstd-compressed, DBN versions 1 and 3
    for (dbn, csv) in [
        ("trades.dbn.zst", "trades"),
        ("trades.dbn", "trades"),
        ("trades.v1.dbn", "trades"),
        ("mbp-1.dbn.zst", "mbp-1"),
        ("ohlcv-1m.dbn.zst", "ohlcv-1m"),
        ("mbo.dbn.zst", "mbo"),
    ] {
        let ours = q(&mut l, &format!("SELECT * FROM '{dir}/xnas-itch-20240102.{dbn}'"));
        let theirs = q(&mut l, &format!("SELECT * FROM '{dir}/databento-csv/xnas-itch-20240102.{csv}.csv'"));
        assert_eq!(ours, theirs, "{dbn}");
        let mut cols =
            |f: &str| q(&mut l, &format!("DESCRIBE '{f}'")).into_iter().map(|r| r[0].clone()).collect::<Vec<_>>();
        assert_eq!(
            cols(&format!("{dir}/xnas-itch-20240102.{dbn}")),
            cols(&format!("{dir}/databento-csv/xnas-itch-20240102.{csv}.csv")),
            "{dbn}"
        );
    }
    let t = format!("'{dir}/xnas-itch-20240102.trades.dbn.zst'");
    assert_eq!(q(&mut l, &format!("DESCRIBE {t}"))[..2], [["ts_recv", "timestamp"], ["ts_event", "timestamp"]]);
    // nanoseconds read to the microsecond; an undefined price NULL
    assert_eq!(q(&mut l, &format!("SELECT ts_event, price FROM {t} LIMIT 1")), [["2024-01-02 14:30:00", "185.64"]]);
    let mbo = format!("'{dir}/xnas-itch-20240102.mbo.dbn.zst'");
    assert_eq!(q(&mut l, &format!("SELECT price FROM {mbo} WHERE action = 'R'")), [["NULL"]]);
    // bars, and each trade with the quote it met
    let q1 = format!("'{dir}/xnas-itch-20240102.mbp-1.dbn.zst'");
    assert_eq!(
        q(
            &mut l,
            &format!(
                "SELECT t.symbol, t.ts_event, t.price, q.bid_px_00, q.ask_px_00 FROM {t} t \
             ASOF JOIN {q1} q ON t.symbol = q.symbol AND t.ts_event >= q.ts_event ORDER BY t.ts_event"
            )
        ),
        [
            ["AAPL", "2024-01-02 14:30:00", "185.64", "185.63", "185.65"],
            ["MSFT", "2024-01-02 14:30:00.005", "370.01", "370", "370.02"],
            ["AAPL", "2024-01-02 14:31:01", "185.7", "185.68", "185.71"],
            ["AAPL", "2024-01-02 14:31:02.5", "185.6505", "185.68", "185.71"],
            ["MSFT", "2024-01-02 14:32:05", "370.12", "370.1", "370.13"]
        ]
    );
    let err = format!("{:#}", l.execute(&format!("SELECT * FROM '{dir}/gen.py.dbn'")).unwrap_err());
    assert!(err.contains("no file"), "{err}");
    std::fs::write(d.path().join("x.dbn"), "not DBN").unwrap();
    let err = format!("{:#}", l.execute(&format!("SELECT * FROM '{}'", d.path().join("x.dbn").display())).unwrap_err());
    assert!(err.contains("x.dbn: not a DBN file"), "{err}");
}

#[test]
fn a_table_or_file_not_there_suggests_the_closest_one() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("trades.csv").display().to_string();
    std::fs::write(&path, TRADES).unwrap();
    let mut l = lake(d.path());
    l.execute(&format!("CREATE TABLE trades AS '{path}'")).unwrap();
    let err = |l: &mut Lake, sql: &str| format!("{:#}", l.execute(sql).unwrap_err());
    assert!(err(&mut l, "SELECT * FROM trdes").contains("no table trdes: did you mean trades?"));
    let typo = d.path().join("trade.csv").display().to_string();
    let e = err(&mut l, &format!("SELECT * FROM '{typo}'"));
    assert!(e.contains(&format!("no file {typo}: did you mean {path}?")), "{e}");
    // nothing close: the general advice
    let e = err(&mut l, "SELECT * FROM orders");
    assert!(e.contains("no table orders: query a file"), "{e}");
}

/// A directory served over HTTP as a store serves objects (HEAD, GET and ranged GETs), counting
/// the bytes of the ranges it sends.
fn serve_ranges(root: std::path::PathBuf) -> (String, std::sync::Arc<std::sync::atomic::AtomicU64>) {
    use std::io::{BufRead, BufReader};
    let sent = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let count = sent.clone();
    std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            let (root, count) = (root.clone(), count.clone());
            std::thread::spawn(move || {
                let mut r = BufReader::new(s.try_clone().unwrap());
                let mut s = s;
                // requests one after another on a connection kept open
                loop {
                    let mut head = vec![];
                    loop {
                        let mut line = String::new();
                        if r.read_line(&mut line).unwrap_or(0) == 0 {
                            return;
                        }
                        if line == "\r\n" {
                            break;
                        }
                        head.push(line);
                    }
                    let mut first = head[0].split_whitespace();
                    let (method, path) =
                        (first.next().unwrap_or("").to_string(), first.next().unwrap_or("/").to_string());
                    let range = head.iter().find_map(|h| {
                        let (k, v) = h.split_once(':')?;
                        (k.eq_ignore_ascii_case("range")).then(|| v.trim().trim_start_matches("bytes=").to_string())
                    });
                    let Ok(body) = std::fs::read(root.join(path.trim_start_matches('/'))) else {
                        let _ = s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
                        continue;
                    };
                    let n = body.len();
                    let (status, part, extra) = match range.and_then(|r| {
                        let (a, b) = r.split_once('-')?;
                        let a: usize = a.parse().ok()?;
                        let b: usize = b.parse::<usize>().map_or(n - 1, |b| b.min(n - 1));
                        Some((a, b))
                    }) {
                        Some((a, b)) => {
                            ("206 Partial Content", &body[a..=b], format!("Content-Range: bytes {a}-{b}/{n}\r\n"))
                        }
                        None => ("200 OK", &body[..], String::new()),
                    };
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{extra}Last-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\nETag: \"{n}\"\r\nAccept-Ranges: bytes\r\n\r\n",
                        part.len()
                    );
                    let _ = s.write_all(head.as_bytes());
                    if method == "GET" {
                        let _ = s.write_all(part);
                        // the bytes of ranges asked for (a listing's GET has its body left unread)
                        if status.starts_with("206") {
                            count.fetch_add(part.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });
    (format!("http://127.0.0.1:{port}"), sent)
}

#[test]
fn a_large_remote_parquet_file_is_read_by_the_ranges_a_query_needs() {
    use arrow_array::{ArrayRef, Float64Array, RecordBatch};
    use std::sync::atomic::Ordering::Relaxed;
    let d = tempfile::tempdir().unwrap();
    let served = d.path().join("served");
    std::fs::create_dir_all(&served).unwrap();
    // 10 columns of 300,000 numbers that do not compress: 24 MB, in 3 row groups
    let mut x: u64 = 7;
    let cols: Vec<(String, ArrayRef)> = (0..10)
        .map(|c| {
            let v: Vec<f64> = (0..300_000)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    (x % 1_000_000) as f64 / 1000.0
                })
                .collect();
            (format!("c{c}"), std::sync::Arc::new(Float64Array::from(v)) as ArrayRef)
        })
        .collect();
    let batch = RecordBatch::try_from_iter(cols).unwrap();
    let props =
        parquet::file::properties::WriterProperties::builder().set_max_row_group_row_count(Some(100_000)).build();
    let file = std::fs::File::create(served.join("wide.parquet")).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let size = std::fs::metadata(served.join("wide.parquet")).unwrap().len();
    std::fs::write(served.join("small.csv"), "x\n1\n2\n").unwrap();
    let local = served.join("wide.parquet").display().to_string();
    let (url, sent) = serve_ranges(served);
    let mut l = lake(d.path());
    let two = |from: &str| format!("SELECT count(*), sum(c0), max(c7) FROM {from}");
    let expected = q(&mut l, &two(&format!("'{local}'")));
    // two columns of ten: about a fifth of the file fetched, and the same answer
    assert_eq!(q(&mut l, &two(&format!("'{url}/wide.parquet'"))), expected);
    let first = sent.load(Relaxed);
    assert!(first < size / 4, "{first} bytes of {size} for 2 of 10 columns");
    // asked again: from the cache
    assert_eq!(q(&mut l, &two(&format!("'{url}/wide.parquet'"))), expected);
    assert_eq!(sent.load(Relaxed), first, "the ranges are cached");
    // every column: the file once, about
    let all = "SELECT count(*), sum(c0 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9)";
    let mut cold = lake(&d.path().join("other"));
    assert_eq!(
        q(&mut cold, &format!("{all} FROM '{url}/wide.parquet'")),
        q(&mut cold, &format!("{all} FROM '{local}'"))
    );
    let every = sent.load(Relaxed) - first;
    assert!(every < size + size / 10, "{every} bytes of {size} for every column");
    // a small file: downloaded whole, not by ranges
    let before = sent.load(Relaxed);
    assert_eq!(q(&mut cold, &format!("SELECT sum(x) FROM '{url}/small.csv'")), [["3"]]);
    assert_eq!(sent.load(Relaxed), before);
}

#[test]
fn a_result_stays_in_columns_and_its_rows_are_made_only_when_asked() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("t.csv").display().to_string();
    std::fs::write(&path, TRADES).unwrap();
    let mut l = lake(d.path());
    let a = l.execute(&format!("SELECT ts, symbol, price FROM '{path}'")).unwrap();
    assert_eq!(a.rows.len(), 6);
    assert!(a.rows.batches().iter().all(|b| b.cols.len() == 3));
    // to Arrow from the columns, as from the rows
    let cols = brrrrr_lake::write::record_batches(&a.columns, a.rows.batches()).unwrap();
    let rows = brrrrr_lake::write::record_batch(&a.columns, &a.rows).unwrap();
    assert_eq!(arrow_select::concat::concat_batches(&rows.schema(), &cols).unwrap(), rows);
    // the outermost ORDER BY, OFFSET and LIMIT on the columns; a LIMIT alone keeps its rows only
    assert_eq!(
        q(&mut l, &format!("SELECT price, symbol FROM '{path}' ORDER BY price DESC, symbol LIMIT 2 OFFSET 1")),
        [["101", "A"], ["100", "A"]]
    );
    let a = l.execute(&format!("SELECT * FROM '{path}' LIMIT 2")).unwrap();
    assert_eq!(a.rows.batches().iter().map(|b| b.len).sum::<usize>(), 2);
    assert_eq!(texts(&a)[1][1], "B");
    // a global aggregate over no rows still gives its row
    assert_eq!(q(&mut l, &format!("SELECT count(*), max(price) FROM '{path}' WHERE price > 1000")), [["0", "NULL"]]);
}

#[test]
fn copy_writes_as_the_query_runs_more_partitions_than_it_keeps_open() {
    let d = tempfile::tempdir().unwrap();
    // two files (two batches at least), each with the 150 keys, interleaved
    for f in ["a", "b"] {
        let rows: String = (0..600).map(|i| format!("{},{i}\n", i % 150)).collect();
        std::fs::write(d.path().join(format!("{f}.csv")), format!("k,v\n{rows}")).unwrap();
    }
    let glob = d.path().join("*.csv").display().to_string();
    let out = d.path().join("out").display().to_string();
    let mut l = lake(d.path());
    let msg = l.execute(&format!("COPY (FROM '{glob}') TO '{out}' (PARTITION_BY (k))")).unwrap();
    assert_eq!(msg.message.unwrap(), format!("1200 rows written to {out}, in 150 partitions"));
    // past 100 open, the least recently written are closed and the next of theirs is data_1, ...
    let files = (0..150).map(|k| std::fs::read_dir(format!("{out}/k={k}")).unwrap().count()).sum::<usize>();
    assert!(files > 150, "{files}");
    assert!(!std::fs::read_dir(format!("{out}/k=0")).unwrap().any(|e| e
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with('.')));
    assert_eq!(
        q(&mut l, &format!("SELECT count(*), count(DISTINCT k), sum(v), sum(k) FROM '{out}'")),
        q(&mut l, &format!("SELECT count(*), count(DISTINCT k), sum(v), sum(k) FROM '{glob}'"))
    );
    // no row: a file of the columns still
    let none = d.path().join("none.parquet").display().to_string();
    l.execute(&format!("COPY (SELECT k, v FROM '{glob}' WHERE v < 0) TO '{none}'")).unwrap();
    assert_eq!(q(&mut l, &format!("SELECT count(*) FROM '{none}'")), [["0"]]);
}

#[test]
fn parquet_is_written_a_row_group_at_a_time_in_order() {
    use brrrrr_core::column::{Batch, Col, Data};
    use brrrrr_lake::write::{Out, ResultWriter};
    use std::sync::Arc;
    let ints =
        |from: i64, n: usize| Batch::new(n, vec![Arc::new(Col::new(Data::Int((from..from + n as i64).collect())))]);
    let f = tempfile::tempfile().unwrap();
    let mut w = ResultWriter::new(f, Out::Parquet, &["x".to_string()], None).unwrap();
    // a row group of 1 << 20 rows: three, the last part of one
    let n = 3 << 19;
    for i in 0..6 {
        w.push(&ints(i * (n / 6) as i64, n / 6)).unwrap();
    }
    // its column's type is set: a value of another kind is refused, not lost
    let text = Batch::new(1, vec![Arc::new(Col::from_values(vec![brrrrr_core::value::Value::Str("a".into())]))]);
    let err = w.push(&text).unwrap_err().to_string();
    assert!(err.contains("the column x is Int in the first 1048576 rows, then Text"), "{err}");
    let f = w.finish().unwrap();
    let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    assert_eq!(r.metadata().num_row_groups(), 2);
    let mut next = 0;
    for b in r.build().unwrap() {
        let b = b.unwrap();
        let x = b.column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert!(x.values().iter().all(|v| {
            next += 1;
            *v == next - 1
        }));
    }
    assert_eq!(next, n as i64);
}

/// `s3://b` as an in-memory store.
fn in_store(l: &Lake) -> std::sync::Arc<object_store::memory::InMemory> {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new());
    l.files.insert_store("s3://b", store.clone());
    store
}

#[test]
fn a_store_s_parquet_is_read_by_ranges_and_results_are_written_to_it_as_they_come() {
    let d = tempfile::tempdir().unwrap();
    let csv = d.path().join("t.csv").display().to_string();
    std::fs::write(&csv, TRADES).unwrap();
    let mut l = lake(d.path());
    let store = in_store(&l);
    let all = q(&mut l, &format!("SELECT * FROM '{csv}' ORDER BY ts"));
    for f in ["t.parquet", "t.csv", "t.json"] {
        let msg = l.execute(&format!("COPY (FROM '{csv}') TO 's3://b/out/{f}'")).unwrap();
        assert_eq!(msg.message.unwrap(), format!("6 rows written to s3://b/out/{f}"));
        assert_eq!(q(&mut l, &format!("FROM 's3://b/out/{f}' ORDER BY ts")), all, "{f}");
    }
    // Parquet, however small, by ranges; the others downloaded
    for (f, ranged) in [("t.parquet", true), ("t.csv", false)] {
        let mut files = l.files.list(&format!("s3://b/out/{f}"), None).unwrap();
        l.files.fetch(&mut files).unwrap();
        assert_eq!(files[0].ranged.is_some(), ranged, "{f}");
    }
    // Hive partitions, each file straight to its key
    l.execute(&format!("COPY (SELECT symbol, price, size FROM '{csv}') TO 's3://b/parts' (PARTITION_BY (symbol))"))
        .unwrap();
    let keys: Vec<String> = l.files.names("s3://b/parts", false).unwrap().into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, ["s3://b/parts/symbol=A/data_0.parquet", "s3://b/parts/symbol=B/data_0.parquet"]);
    let sums = "SELECT symbol, count(*), sum(price * size) FROM";
    assert_eq!(
        q(&mut l, &format!("{sums} 's3://b/parts/' GROUP BY symbol ORDER BY symbol")),
        q(&mut l, &format!("{sums} '{csv}' GROUP BY symbol ORDER BY symbol"))
    );
    // a result of several parts' bytes: uploaded in parts, there whole
    let big = d.path().join("big.csv").display().to_string();
    let rows: String =
        (0..1_000_000u64).map(|i| format!("{i},{}\n", i.wrapping_mul(2_654_435_761) % 1_000_003)).collect();
    std::fs::write(&big, format!("i,x\n{rows}")).unwrap();
    l.execute(&format!("COPY (FROM '{big}') TO 's3://b/big.csv'")).unwrap();
    let size = l.files.list("s3://b/big.csv", None).unwrap()[0].object.as_ref().unwrap().size;
    assert!(size > 10 << 20, "{size} bytes: more than two parts'");
    let mut sum = |from: &str| q(&mut l, &format!("SELECT count(*), sum(x), max(i) FROM '{from}'"));
    assert_eq!(sum("s3://b/big.csv"), sum(&big));
    drop(store);
}
