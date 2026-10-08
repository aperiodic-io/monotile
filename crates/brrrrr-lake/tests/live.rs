//! Live tables: rows written are queried at once, a flush moves them into Parquet without a
//! query seeing any twice or not at all, and a restart replays the rows not yet flushed.
use brrrrr_core::query::text;
use brrrrr_lake::live::Live;
use brrrrr_lake::read::parse_rows;
use brrrrr_lake::Lake;
use std::sync::Arc;

fn rows(l: &Lake, sql: &str) -> Vec<Vec<String>> {
    l.query(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}")).rows.iter().map(|r| r.iter().map(text).collect()).collect()
}

fn write(t: &Live, body: &str) -> usize {
    let schema = t.state.read().unwrap().schema.clone();
    t.append(parse_rows(body.as_bytes(), false, schema.as_ref()).unwrap()).unwrap()
}

#[test]
fn written_rows_are_queried_flushed_and_recovered() {
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("trades");
    let t = Arc::new(Live::open("trades", &dir, Some("ts".into()), false).unwrap());
    let mut lake = Lake::new();
    lake.register_live(t.clone());
    assert_eq!(write(&t, "{\"ts\":\"2024-01-01T23:59:59\",\"symbol\":\"A\",\"price\":1.5}\n{\"ts\":\"2024-01-02T00:00:01\",\"symbol\":\"B\",\"price\":2}\n"), 2);
    let all = "SELECT symbol, price FROM trades ORDER BY ts";
    assert_eq!(rows(&lake, all), [["A", "1.5"], ["B", "2"]]);
    // a flush: the same answer, from two days' Parquet files
    assert_eq!(t.flush().unwrap(), 2);
    assert_eq!(t.files().len(), 2, "{:?}", t.files());
    assert_eq!(rows(&lake, all), [["A", "1.5"], ["B", "2"]]);
    // newer rows on top of the history; a missing column is NULL; an unknown one is refused
    write(&t, "{\"ts\":\"2024-01-02T00:00:02\",\"symbol\":\"C\"}\n");
    assert_eq!(rows(&lake, all).len(), 3);
    assert_eq!(rows(&lake, "SELECT price FROM trades WHERE symbol = 'C'"), [["NULL"]]);
    let schema = t.state.read().unwrap().schema.clone();
    let err = parse_rows(b"{\"ts\":\"2024-01-02T00:00:03\",\"venue\":\"X\"}\n", false, schema.as_ref()).unwrap_err();
    assert!(format!("{err:#}").contains("venue is not a column"), "{err:#}");
    // a restart replays the log: nothing written is lost, nothing flushed comes twice
    drop(lake);
    drop(t);
    let t = Arc::new(Live::open("trades", &dir, Some("ts".into()), false).unwrap());
    let mut lake = Lake::new();
    lake.register_live(t.clone());
    assert_eq!(rows(&lake, "SELECT count(*) AS n FROM trades"), [["3"]]);
    write(&t, "{\"ts\":\"2024-01-02T00:00:04\",\"symbol\":\"D\",\"price\":4}\n");
    assert_eq!(t.flush().unwrap(), 2);
    assert_eq!(rows(&lake, "SELECT symbol FROM trades ORDER BY ts"), [["A"], ["B"], ["C"], ["D"]]);
    // the bars of a table: today and history in one query
    assert_eq!(
        rows(&lake, "SELECT time_bucket('1d', ts) AS day, count(*) AS n FROM trades GROUP BY day ORDER BY day"),
        [["2024-01-01 00:00:00", "1"], ["2024-01-02 00:00:00", "3"]]
    );
}

#[test]
fn queries_during_flushes_see_every_row_once() {
    let d = tempfile::tempdir().unwrap();
    let t = Arc::new(Live::open("t", &d.path().join("t"), Some("ts".into()), false).unwrap());
    let mut lake = Lake::new();
    lake.threads = 2;
    lake.register_live(t.clone());
    // the first rows set the table's columns: before them, a query of it is refused
    let row = |i: usize| format!("{{\"ts\":\"2024-01-01T00:00:{:02}.{i:03}\",\"x\":1}}\n", i % 60);
    write(&t, &row(0));
    let writer = {
        let t = t.clone();
        std::thread::spawn(move || {
            for i in 1..200 {
                write(&t, &row(i));
                if i % 20 == 19 {
                    t.flush().unwrap();
                }
            }
        })
    };
    let mut last = 0;
    while !writer.is_finished() {
        let n: usize = rows(&lake, "SELECT count(*) AS n FROM t")[0][0].parse().unwrap();
        assert!(n >= last, "a count went back: {n} after {last}");
        last = n;
    }
    writer.join().unwrap();
    assert_eq!(rows(&lake, "SELECT count(*) AS n, sum(x) AS s FROM t"), [["200", "200"]]);
}
