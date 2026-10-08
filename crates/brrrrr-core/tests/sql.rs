//! The SQL surface: brrrrr's input language is Proton's pipeline DDL (ADR-0001). Every statement
//! of the example pipelines must parse; anything else must fail with a located error.
use brrrrr_core::sql::{parse, Kind};
use brrrrr_core::value::Type;

/// The example pipelines (fixtures/pipelines), by file name.
fn pipelines() -> Vec<(String, String)> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/pipelines");
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .map(|p| (p.file_name().unwrap().to_string_lossy().into_owned(), std::fs::read_to_string(&p).unwrap()))
        .collect();
    v.sort();
    v
}

/// Counts of each statement kind in the example pipelines, as the text has them (`CREATE
/// MATERIALIZED VIEW`, `CREATE STREAM`, ...): none is lost in parsing, and each plans.
#[test]
fn every_pipeline_statement_parses_and_plans() {
    let (mut views, mut streams, mut external, mut tables, mut delayed, mut undelayed, mut asof) =
        (0, 0, 0, 0, 0, 0, 0);
    for (name, sql) in pipelines() {
        let cat = parse(&sql).unwrap_or_else(|e| panic!("{name}: {e}"));
        brrrrr_core::engine::Engine::new(&cat).unwrap_or_else(|e| panic!("{name}: {e}"));
        views += cat.views.len();
        for s in cat.streams.values() {
            match s.kind {
                Kind::Stream => streams += 1,
                Kind::External => external += 1,
                Kind::Table(_) => tables += 1,
            }
        }
        delayed += cat.views.iter().filter(|v| v.emit_delay_us == Some(50_000)).count();
        undelayed += cat.views.iter().filter(|v| v.emit_delay_us == Some(0)).count();
        asof += cat.views.iter().filter(|v| v.asof.contains(&true)).count();
        for v in &cat.views {
            assert!(cat.streams.contains_key(&v.target), "{name}: {} writes into unknown {}", v.name, v.target);
        }
    }
    assert_eq!((views, streams, external, tables), (59, 27, 43, 1));
    assert_eq!((delayed, undelayed), (15, 10), "windows that emit 50ms after close, and at close");
    assert_eq!(asof, 2, "the slippage and price-context joins");
}

#[test]
fn external_streams_keep_their_settings_and_headers() {
    let (_, sql) = pipelines().into_iter().find(|(n, _)| n == "bars.sql").unwrap();
    let cat = parse(&sql).unwrap();
    let src = cat.streams.values().find(|s| s.settings.get("topic").is_some_and(|t| t == "trades")).unwrap();
    assert_eq!(src.settings["data_format"], "ProtobufSingle");
    assert_eq!(src.settings["format_schema"], "market:Trade");
    assert_eq!(src.settings["seek_to"], "earliest");
    let sink = cat.streams.values().find(|s| s.settings.get("topic").is_some_and(|t| t == "bars.1m")).unwrap();
    assert_eq!(sink.settings["data_format"], "JSONEachRow");
    let headers = sink.columns.iter().find(|c| c.name == "_tp_message_headers").unwrap();
    assert!(headers.materialized.is_some() && matches!(headers.ty, Type::Map(..)));
    let symbol = sink.columns.iter().find(|c| c.name == "symbol").unwrap();
    assert_eq!(symbol.ty, Type::Str, "low_cardinality is a storage hint");
    let table = cat.streams.values().find(|s| matches!(s.kind, Kind::Table(_))).unwrap();
    assert!(matches!(table.kind, Kind::Table(Some(_))), "PARTITION BY is kept");
    assert_eq!(table.settings["type"], "s3");
}

/// `DEFAULT` fills a column the insert omits and `MATERIALIZED` is computed on every insert, so
/// the two are kept apart (tests/engine.rs holds what each emits).
#[test]
fn default_and_materialized_expressions_are_kept_apart() {
    let cat = parse("CREATE STREAM s (a int64 DEFAULT 1, b int64 MATERIALIZED a + 1, c int64)").unwrap();
    let cols = &cat.streams["s"].columns;
    let exprs = |i: usize| {
        let c = &cols[i];
        (c.materialized.as_ref().map(ToString::to_string), c.default.as_ref().map(ToString::to_string))
    };
    assert_eq!(exprs(0), (None, Some("1".into())));
    assert_eq!(exprs(1), (Some("a + 1".into()), None));
    assert_eq!(exprs(2), (None, None));
}

#[test]
fn asof_flags_follow_the_joins_in_order() {
    let (_, sql) = pipelines().into_iter().find(|(n, _)| n == "derivatives.sql").unwrap();
    let cat = parse(&sql).unwrap();
    let v = cat.views.iter().find(|v| v.name.ends_with("price_context_mv")).unwrap();
    assert_eq!(v.asof, vec![true, true]);
    let q = v.query.to_string();
    assert!(q.contains("LEFT JOIN") && !q.contains("ASOF"), "{q}");
}

#[test]
fn protons_lexical_forms_are_accepted() {
    let cat = parse(
        "CREATE STREAM s (time int64, interval string);
         CREATE STREAM o (interval string, n uint64);
         CREATE MATERIALIZED VIEW v INTO o AS
           SELECT '1m' AS interval, count() AS n FROM tumble(s, time, 15m) GROUP BY window_start
           EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '2' SECOND SETTINGS max_threads = 1;
         -- a comment between statements
         CREATE MATERIALIZED VIEW w INTO o AS SELECT interval, 1 AS n FROM o WHERE interval = '1m'",
    )
    .unwrap();
    assert_eq!(cat.views[0].emit_delay_us, Some(2_000_000));
    let q = cat.views[0].query.to_string();
    assert!(q.contains("'15m'") && q.contains("AS `interval`"), "{q}");
    assert_eq!(cat.views[1].emit_delay_us, None);
    assert!(cat.views[1].query.to_string().contains("`interval` = '1m'"));
}

#[test]
fn round_trip_of_every_pipeline_query() {
    // parse -> print -> parse gives the same AST: the printed plan in `explain` is faithful
    let dialect = sqlparser::dialect::ClickHouseDialect {};
    for (name, sql) in pipelines() {
        for v in parse(&sql).unwrap().views {
            let printed = v.query.to_string();
            let again = sqlparser::parser::Parser::new(&dialect).try_with_sql(&printed).unwrap().parse_query();
            assert_eq!(again.as_deref().ok(), Some(&v.query), "{name} {}", v.name);
        }
    }
}

#[test]
fn errors_are_located() {
    let cases = [
        ("CREATE TABLE x (a int64)", "expected STREAM"),
        ("CREATE STREAM x (a decimal(10, 2))", "unknown type"),
        ("CREATE STREAM x (a int64", "expected , or )"),
        ("CREATE STREAM x (a int64) SETTINGS k", "expected ="),
        ("CREATE MATERIALIZED VIEW v INTO o AS SELECT FROM", "Expected"),
        ("CREATE MATERIALIZED VIEW v INTO o AS SELECT 1 EMIT AFTER WINDOW", "expected CLOSE"),
        (
            "CREATE MATERIALIZED VIEW v INTO o AS SELECT 1 EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '5' YEAR",
            "unsupported interval unit",
        ),
        ("DROP STREAM x", "expected CREATE"),
        ("CREATE STREAM x (a int64) garbage", "unexpected"),
        ("CREATE STREAM IF x (a int64)", "expected NOT"),
        ("CREATE STREAM (a int64)", "expected an identifier"),
        ("CREATE STREAM x (a int64) SETTINGS k = (", "expected a setting value"),
        ("CREATE STREAM x (a int64 MATERIALIZED )", "Expected"),
        ("CREATE STREAM x (a int64);\nCREATE STREAM y (b int64) oops", "unexpected"),
        ("CREATE STREAM x (a 'unterminated", "Unterminated"),
        // what a query or an expression leaves unparsed is refused
        ("CREATE MATERIALIZED VIEW v INTO o AS SELECT x FROM s WHERE x > 0 totally_invalid_tokens", "unexpected"),
        ("CREATE STREAM x (a int64 MATERIALIZED 1 2)", "unexpected 2 after the end"),
    ];
    for (sql, want) in cases {
        let e = parse(sql).expect_err(sql);
        assert!(e.to_string().contains(want), "{sql}: {e}");
        assert!(e.line >= 1, "{sql}: error without a location: {e}");
    }
    assert_eq!(parse("CREATE STREAM x (a int64);\nCREATE STREAM y (b int64) oops").unwrap_err().line, 2);
    // located at the first token left over
    let e = parse("CREATE MATERIALIZED VIEW v INTO o AS\nSELECT x FROM s WHERE x > 0 junk").unwrap_err();
    assert_eq!((e.line, e.column, e.message.as_str()), (2, 29, "unexpected junk after the end"));
    // no token to locate an empty expression at: an error, not an underflow
    assert!(brrrrr_core::sql::parse_expr("").is_err());
}

/// An envelope error is located at the token it is about (`TABLE`, a `(`), not the one after it:
/// the cursor steps back over the token it read and refused. A missing `)` is at the last token.
#[test]
fn errors_are_located_at_the_offending_token() {
    for (sql, want, column) in [
        ("CREATE TABLE x (a int64)", "expected STREAM", 8),
        ("CREATE STREAM (a int64)", "expected an identifier", 15),
        ("CREATE STREAM x (a int64) SETTINGS k = (1)", "expected a setting value", 40),
        ("CREATE STREAM x (a int64) SETTINGS k = -(1)", "expected a setting value", 41),
        ("CREATE STREAM x (a int64) SETTINGS k = 1, (b) = 2", "expected an identifier", 43),
        ("CREATE STREAM x (a int64, b int64", "expected , or ) in column list, found EOF", 29),
    ] {
        let e = parse(&format!("CREATE STREAM s (x int64);\n{sql}")).expect_err(sql);
        assert!(e.message.contains(want), "{sql}: {e}");
        assert_eq!((e.line, e.column), (2, column), "{sql}: {e}");
    }
}

/// Every unit Proton writes up to hours is converted.
#[test]
fn a_delay_interval_converts_every_unit() {
    for (interval, us) in [
        ("'7' MICROSECOND", 7),
        ("7 microseconds", 7),
        ("'7' MILLISECOND", 7_000),
        ("'7' SECONDS", 7_000_000),
        ("'7' MINUTE", 420_000_000),
        ("7 minutes", 420_000_000),
        ("'7' HOUR", 25_200_000_000),
    ] {
        let sql = format!(
            "CREATE STREAM s (time int64, x float64);
CREATE MATERIALIZED VIEW v INTO s AS SELECT max(x) AS x FROM tumble(s, time, 1m) GROUP BY window_start
  EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL {interval}"
        );
        let cat = parse(&sql).unwrap_or_else(|e| panic!("{interval}: {e}"));
        assert_eq!(cat.views[0].emit_delay_us, Some(us), "{interval}");
    }
}

#[test]
fn settings_accept_numbers_words_and_negatives() {
    let cat = parse("CREATE STREAM x (a int64) SETTINGS a = 1, b = 'two', c = three, d = -4").unwrap();
    let s = &cat.streams["x"].settings;
    assert_eq!((s["a"].as_str(), s["b"].as_str(), s["c"].as_str(), s["d"].as_str()), ("1", "two", "three", "-4"));
}

mod robustness {
    use brrrrr_core::engine::Engine;
    use brrrrr_core::sql::parse;
    use proptest::prelude::*;

    fn pipeline() -> String {
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/pipelines/quotes.sql")).unwrap()
    }

    proptest! {
        /// Operators feed brrrrr whatever SQL they write: bad SQL must be an error with a
        /// location, never a panic, in the parser or the planner.
        #[test]
        fn arbitrary_text_is_an_error_not_a_panic(text in "\\PC{0,400}") {
            if let Ok(cat) = parse(&text) {
                let _ = Engine::new(&cat);
            }
        }

        #[test]
        fn corrupted_pipeline_sql_is_an_error_not_a_panic(at in 0usize..60_000, len in 0usize..40, junk in "[(),;'`a-z0-9 =*]{0,12}") {
            let sql = pipeline();
            let at = sql.char_indices().map(|(i, _)| i).find(|i| *i >= at % sql.len()).unwrap_or(0);
            let end = sql[at..].char_indices().map(|(i, _)| at + i).nth(len).unwrap_or(sql.len());
            let broken = format!("{}{junk}{}", &sql[..at], &sql[end..]);
            if let Ok(cat) = parse(&broken) {
                let _ = Engine::new(&cat);
            }
        }
    }
}

/// A bare duration is a number and a unit written together. Whitespace used to be dropped before
/// the check, so a number followed by any unit-named word fused: `SELECT 1 m` (1, aliased m)
/// became the string '1m'. The same text also parsed differently in parse_expr, which kept it.
#[test]
fn only_a_number_and_unit_written_together_are_a_duration() {
    let view = |select: &str| {
        let sql = format!("CREATE STREAM s (time int64, x float64);\nCREATE MATERIALIZED VIEW v INTO s AS {select}");
        parse(&sql).map(|c| c.views[0].query.to_string())
    };
    assert_eq!(view("SELECT 1 m FROM s").unwrap(), "SELECT 1 AS m FROM s");
    assert_eq!(view("SELECT 1 /* one */m FROM s").unwrap(), "SELECT 1 AS m FROM s");
    let q = view("SELECT max(x) FROM tumble(s, time, 15m) GROUP BY window_start").unwrap();
    assert!(q.contains("tumble(s, time, '15m')"), "{q}");
    assert!(view("SELECT max(x) FROM tumble(s, time, 15 m) GROUP BY window_start").is_err());
    // an expression parses the same wherever it is written
    use brrrrr_core::sql::parse_expr;
    assert_eq!(parse_expr("f(15m)").unwrap().to_string(), "f('15m')");
    assert!(parse_expr("f(15 m)").is_err());
    // a quoted word is an identifier, never a unit
    assert!(parse_expr("f(15\"m\")").is_err());
    // lowercase `interval` before a literal stays the keyword (only a bare one is the column)
    assert!(parse_expr("interval '1' second").unwrap().to_string().starts_with("INTERVAL '1'"));
}

#[test]
fn a_delay_interval_that_overflows_or_is_negative_is_an_error() {
    for (n, unit) in [("9999999999999999", "HOUR"), ("-5", "SECOND")] {
        let sql = format!(
            "CREATE STREAM s (time int64, x float64);
CREATE MATERIALIZED VIEW v INTO s AS SELECT max(x) AS x FROM tumble(s, time, 1m) GROUP BY window_start
  EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '{n}' {unit}"
        );
        let err = parse(&sql).unwrap_err();
        // located at the number, after `INTERVAL `
        assert!(err.message.contains("is negative or too long") && err.line == 3, "{n} {unit}: {err}");
        assert_eq!(err.column, 47, "{n} {unit}: {err}");
    }
}

#[test]
fn a_delay_interval_that_is_not_a_number_is_an_error() {
    let sql = "CREATE STREAM s (time int64, x float64);
CREATE MATERIALIZED VIEW v INTO s AS SELECT max(x) AS x FROM tumble(s, time, 1m) GROUP BY window_start
  EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL 'abc' SECOND";
    let err = parse(sql).unwrap_err();
    assert!(err.message.contains(r#"interval "abc" is not a whole number"#), "{err}");
}

/// A name defined twice used to be last-wins (CREATE ... IF NOT EXISTS is first-wins): a script
/// concatenated from two files could run another schema than it declares first. Now an error.
#[test]
fn a_name_defined_twice_is_an_error() {
    for second in [
        "CREATE STREAM IF NOT EXISTS s (y string)",
        "CREATE EXTERNAL STREAM s (x float64) SETTINGS type = 'kafka', topic = 't'",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS v INTO s AS SELECT x FROM s",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS s INTO s AS SELECT x FROM s",
    ] {
        let sql = format!("CREATE STREAM IF NOT EXISTS s (x float64);\nCREATE MATERIALIZED VIEW v INTO s AS SELECT x FROM s;\n{second}");
        let err = parse(&sql).unwrap_err();
        assert!(err.message.ends_with("is already defined") && err.line == 3, "{second}: {err}");
    }
}

const BARS_STREAMS: &str =
    "CREATE EXTERNAL STREAM trades (time datetime64(3), symbol string, price float64, quantity float64)
  SETTINGS type = 'kafka', topic = 'trades', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM bars_1m (window_start datetime64(3), symbol string, open float64, high float64, low float64,
  close float64, volume float64, trade_count uint64)
  SETTINGS type = 'kafka', topic = 'bars.1m', data_format = 'JSONEachRow', one_message_per_row = true;
";

/// Trades of 3 symbols, 7 s apart for 10 minutes and a price that comes back each 37 trades,
/// in time order, then one an hour later that closes every window; `bad`: with a trade priced
/// -1 after every 11th.
fn bar_trades(bad: bool) -> Vec<Vec<brrrrr_core::value::Value>> {
    use brrrrr_core::value::Value;
    let t0 = 1_704_187_800_000_000; // 2024-01-02 09:30 UTC
    let trade = |t: i64, sym: &str, price: f64| {
        vec![Value::Time(t), Value::Str(sym.into()), Value::F64(price), Value::F64(0.5)]
    };
    let mut rows = vec![];
    for i in 0..90i64 {
        let t = t0 + i * 7_000_000;
        rows.push(trade(t, ["A", "B", "C"][i as usize % 3], 100.0 + (i * 13 % 37) as f64 / 4.0));
        if bad && i % 11 == 5 {
            rows.push(trade(t, "A", -1.0));
        }
    }
    rows.push(trade(t0 + 3_600_000_000, "A", 1.0));
    rows
}

fn bars(view: &str, rows: Vec<Vec<brrrrr_core::value::Value>>) -> Vec<String> {
    let cat = parse(&format!("{BARS_STREAMS}{view}")).unwrap_or_else(|e| panic!("{e}"));
    let mut engine = brrrrr_core::engine::Engine::new(&cat).unwrap();
    let mut out = vec![];
    engine.insert("trades", rows, &mut out);
    out.into_iter().map(|e| format!("{} {}", e.topic, e.payload)).collect()
}

/// A view in `brrrrr sql`'s dialect (time_bucket, first/last, WHERE before the window) runs as
/// the Proton view it means: the same messages, its windows closed as late after their end.
#[test]
fn a_view_in_ad_hoc_sql_writes_what_the_proton_view_it_means_writes() {
    let proton = bars(
        "CREATE MATERIALIZED VIEW bars INTO bars_1m AS
SELECT window_start, symbol, earliest(price) AS open, max(price) AS high, min(price) AS low, latest(price) AS close,
       sum(quantity) AS volume, count() AS trade_count
FROM tumble(trades, time, 1m) GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND",
        bar_trades(false),
    );
    let adhoc = "CREATE MATERIALIZED VIEW bars INTO bars_1m AS
SELECT time_bucket('1m', time) AS window_start, symbol, first(price, time) AS open, max(price) AS high,
       min(price) AS low, last(price, time) AS close, sum(quantity) AS volume, count(*) AS trade_count
FROM trades WHERE price > 0
GROUP BY window_start, symbol";
    assert_eq!(proton.len(), 33, "{proton:#?}");
    assert!(
        proton[0].starts_with(r#"bars.1m {"window_start":"2024-01-02 09:30:00.000","symbol":"A","open":100,"#),
        "{}",
        proton[0]
    );
    assert_eq!(bars(adhoc, bar_trades(true)), proton);
    let cat = parse(&format!("{BARS_STREAMS}{adhoc} EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '1' SECOND")).unwrap();
    let names: Vec<_> = cat.views.iter().map(|v| (v.name.as_str(), v.target.as_str(), v.emit_delay_us)).collect();
    assert_eq!(names.last().unwrap().1, "bars_1m", "{names:?}");
    assert!(names.iter().all(|(n, ..)| n.starts_with("bars__")), "{names:?}");
    assert!(names.iter().any(|(.., d)| *d == Some(1_000_000)), "{names:?}");
}

/// An ad-hoc view whose tables a stream cannot be (a window over every row, a file) is refused
/// with what to write, located at the query; so is ORDER BY.
#[test]
fn an_ad_hoc_view_a_stream_cannot_run_is_refused_with_what_to_write() {
    for (query, why) in [
        ("SELECT symbol, count(*) AS trade_count FROM trades GROUP BY symbol", "groups by time_bucket(width"),
        (
            "SELECT symbol, count(*) AS trade_count FROM 'trades.parquet' GROUP BY symbol",
            "reads its streams, not 'trades.parquet'",
        ),
        (
            "SELECT time_bucket('1m', time) AS window_start, count(*) AS trade_count FROM trades GROUP BY 1 ORDER BY 1",
            "ORDER BY and LIMIT",
        ),
        (
            "SELECT time_bucket('1m', time) AS window_start, count(*) AS trade_count FROM nowhere GROUP BY 1",
            "no stream nowhere",
        ),
    ] {
        let err = parse(&format!("{BARS_STREAMS}CREATE MATERIALIZED VIEW bars INTO bars_1m AS\n{query}")).unwrap_err();
        assert!(err.message.contains(why) && err.line == 7, "{query}: {err}");
    }
}

/// Every view of the committed pipelines is read as Proton reads it, as before views in ad-hoc
/// SQL were taken: one would be spliced in as `<view>__v1`, ..., another plan and checkpoint.
#[test]
fn the_committed_pipelines_views_stay_in_protons_dialect() {
    let mut n = 0;
    for (name, sql) in pipelines() {
        let cat = parse(&sql).unwrap();
        assert!(cat.views.iter().all(|v| !v.name.contains("__")), "{name}");
        n += 1;
    }
    assert!(n > 5, "{n} pipelines");
}
