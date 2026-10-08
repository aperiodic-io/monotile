//! The engine's output as the runtime takes it: a view's messages are flushed as soon as
//! the view has written its sink, so the runtime produces them before the views after it run,
//! and nothing is reordered, added or lost on the way.
use brrrrr_core::engine::{Emit, Engine, Output};
use brrrrr_core::sql::{parse, Catalog, Kind};
use common::{catalog, value};

use crate::common;

/// An output that keeps what each flush released, and what is still pending.
#[derive(Default)]
struct Recording {
    pending: Vec<Emit>,
    flushed: Vec<Vec<Emit>>,
}

impl Output for Recording {
    fn push(&mut self, e: Emit) {
        self.pending.push(e);
    }

    fn flush(&mut self) {
        self.flushed.push(std::mem::take(&mut self.pending));
    }
}

impl Recording {
    fn all(&self) -> Vec<Emit> {
        self.flushed.iter().flatten().chain(&self.pending).cloned().collect()
    }
}

fn engine(cat: &Catalog) -> Engine {
    Engine::new(cat).unwrap_or_else(|e| panic!("{e}"))
}

/// Feeds `fixture`'s chunks to `out`, then closes every window as an idle close would, calling
/// `after` each time an insert or the close has returned.
fn feed<O: Output>(cat: &Catalog, e: &mut Engine, fixture: &serde_json::Value, out: &mut O, after: impl Fn(&O)) {
    for chunk in fixture["chunks"].as_array().unwrap() {
        let stream = &cat.streams[chunk["stream"].as_str().unwrap()];
        let rows = chunk["rows"].as_array().unwrap().iter();
        let rows =
            rows.map(|r| r.as_array().unwrap().iter().zip(&stream.columns).map(|(v, c)| value(v, &c.ty)).collect());
        e.insert(&stream.name, rows.collect(), out);
        after(out);
    }
    e.close_until(i64::MAX / 2, out);
    after(out);
}

/// Every example pipeline, flushed: the same
/// messages in the same order as collected in a `Vec`; each flush releases one sink write (its
/// messages, all of one topic, none empty); nothing is left pending after an insert or a close.
#[test]
fn a_flushing_output_gets_every_message_in_order_one_sink_write_per_flush() {
    let fixtures = common::fixtures();
    let mut pipelines = 0;
    for fixture in fixtures {
        let name = fixture["pipeline"].as_str().unwrap().to_string();
        let cat = catalog(&fixture);
        let mut collected = vec![];
        feed(&cat, &mut engine(&cat), &fixture, &mut collected, |_| {});
        let mut recording = Recording::default();
        let none_pending = |r: &Recording| assert!(r.pending.is_empty(), "{name}: {} left pending", r.pending.len());
        feed(&cat, &mut engine(&cat), &fixture, &mut recording, none_pending);
        assert!(!collected.is_empty(), "{name}: emits nothing, the test proves little");
        assert!(recording.all() == collected, "{name}: flushed other messages, or in another order");
        for batch in &recording.flushed {
            assert!(!batch.is_empty(), "{name}: an empty flush");
            assert!(batch.iter().all(|e| e.topic == batch[0].topic), "{name}: a flush of several topics");
        }
        assert!(recording.flushed.len() > 1, "{name}: one flush, the test proves little");
        pipelines += 1;
    }
    assert!(pipelines >= 8, "{pipelines} pipelines");
}

/// `cat` with each `SELECT * FROM s [WHERE ..]` into a Kafka sink written `SELECT <every column
/// of s> FROM s [WHERE ..]`: the same view, planned and written the general way. How many.
fn columns_listed(cat: &Catalog) -> (Catalog, usize) {
    use sqlparser::ast::{Expr, Ident, SelectItem, SetExpr, TableFactor};
    let (mut listed, mut n) = (cat.clone(), 0);
    for v in &mut listed.views {
        let SetExpr::Select(s) = v.query.body.as_mut() else { continue };
        let ([SelectItem::Wildcard(_)], [from]) = (s.projection.as_slice(), s.from.as_slice()) else { continue };
        let TableFactor::Table { name, args: None, .. } = &from.relation else { continue };
        let source = cat.streams.get(&name.to_string());
        let Some(source) = source.filter(|_| from.joins.is_empty() && cat.streams[&v.target].kind == Kind::External)
        else {
            continue;
        };
        let column = |c: &brrrrr_core::sql::Column| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(&c.name)));
        s.projection = source.columns.iter().map(column).collect();
        n += 1;
    }
    (listed, n)
}

/// A `SELECT * [WHERE]` view into a sink (an `*_out` view) writes the rows it keeps
/// as they come, without the projection's copy of each: every example pipeline emits the
/// messages it emits with those views' columns listed instead, in the same order and flushes.
#[test]
fn a_select_star_into_a_sink_emits_what_its_columns_listed_do() {
    let mut views = 0;
    for fixture in common::fixtures() {
        let name = fixture["pipeline"].as_str().unwrap().to_string();
        let cat = catalog(&fixture);
        let (listed, n) = columns_listed(&cat);
        let (mut star, mut columns) = (Recording::default(), Recording::default());
        feed(&cat, &mut engine(&cat), &fixture, &mut star, |_| {});
        feed(&listed, &mut engine(&listed), &fixture, &mut columns, |_| {});
        assert!(!star.flushed.is_empty(), "{name}: emits nothing, the test proves little");
        assert!(star.flushed == columns.flushed, "{name}: other messages, or in another order");
        views += n;
    }
    assert!(views >= 2, "only {views} `SELECT *` views into a sink");
}

/// The payloads `sql`'s views write for `rows` inserted into `stream`, in order.
fn payloads(sql: &str, stream: &str, rows: Vec<Vec<brrrrr_core::value::Value>>) -> Vec<String> {
    let cat = parse(sql).unwrap();
    let mut out = vec![];
    engine(&cat).insert(stream, rows, &mut out);
    out.into_iter().map(|e| e.payload).collect()
}

/// A `SELECT *` view into a sink whose columns have other types than its stream's: its rows go
/// to the sink without the projection's copy (`a_select_star_into_a_sink_emits_what_its_columns_listed_do`),
/// and each value that does not conform to its sink column is still cast.
#[test]
fn a_select_star_into_a_sink_casts_what_its_sink_types_differently() {
    use brrrrr_core::value::Value;
    let sql = "
CREATE STREAM quotes (symbol string, size float32, n int32);
CREATE EXTERNAL STREAM quotes_out (symbol string, size float64, n int64) SETTINGS type = 'kafka', topic = 'q', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW v INTO quotes_out AS SELECT * FROM quotes WHERE symbol = 'A';
";
    let rows = vec![
        vec![Value::Str("A".into()), Value::F32(0.1), Value::Int(7)],
        vec![Value::Str("B".into()), Value::F32(0.2), Value::Int(8)],
    ];
    // 0.1 as a Float32 is 0.100000001490116..., which a Float64 column writes in full
    assert_eq!(payloads(sql, "quotes", rows), ["{\"symbol\":\"A\",\"size\":0.10000000149011612,\"n\":7}\n"]);
}

/// A sink that names a column twice gets the view's value in both: the value is not moved out
/// of the view's row for the first (`Plan::unique`).
#[test]
fn a_column_a_sink_names_twice_is_written_twice() {
    use brrrrr_core::value::Value;
    let sql = "
CREATE STREAM trades (symbol string, price float64);
CREATE EXTERNAL STREAM twice_out (symbol string, price float64, price float64) SETTINGS type = 'kafka', topic = 't', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW w INTO twice_out AS SELECT symbol, price FROM trades;
";
    let rows = vec![vec![Value::Str("A".into()), Value::F64(1.5)]];
    assert_eq!(payloads(sql, "trades", rows), ["{\"symbol\":\"A\",\"price\":1.5,\"price\":1.5}\n"]);
}

const TWO_VIEWS: &str = "
CREATE STREAM trades (t datetime64(6), symbol string, price float64);
CREATE EXTERNAL STREAM short_out (symbol string, n uint64) SETTINGS type = 'kafka', topic = 'short', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM long_out (symbol string, n uint64) SETTINGS type = 'kafka', topic = 'long', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW short INTO short_out AS SELECT symbol, count() AS n FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW long INTO long_out AS SELECT symbol, count() AS n FROM tumble(trades, t, 1h) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

fn trade(sec: i64, symbol: &str) -> Vec<brrrrr_core::value::Value> {
    use brrrrr_core::value::Value;
    vec![Value::Time(sec * 1_000_000), Value::Str(symbol.into()), Value::F64(1.0)]
}

/// One row closing both a 15s and a 1h window: the 15s view's messages are flushed by
/// themselves, before the 1h view has run, within the one insert. A plain `Vec` gets them in
/// the same order.
#[test]
fn a_row_closing_several_views_flushes_each_views_messages_before_the_next_view_runs() {
    let cat = parse(TWO_VIEWS).unwrap();
    let topics = |r: &Recording, from: usize| -> Vec<Vec<String>> {
        r.flushed[from..].iter().map(|b| b.iter().map(|e| e.topic.to_string()).collect()).collect()
    };
    let mut e = engine(&cat);
    let mut out = Recording::default();
    e.insert("trades", vec![trade(10, "A"), trade(20, "B"), trade(1_000, "A")], &mut out);
    assert_eq!(topics(&out, 0), [vec!["short", "short"]], "the first two 15s windows close at 1000 s");
    e.insert("trades", vec![trade(3_601, "B")], &mut out);
    assert_eq!(topics(&out, 1), [vec!["short"], vec!["long", "long"]], "one insert, two flushes, the 15s one first");
    assert!(out.pending.is_empty());
    let (mut again, mut collected) = (engine(&cat), vec![]);
    again.insert("trades", vec![trade(10, "A"), trade(20, "B"), trade(1_000, "A")], &mut collected);
    again.insert("trades", vec![trade(3_601, "B")], &mut collected);
    assert_eq!(out.all(), collected, "a plain Vec gets the same messages in the same order");
}

/// A row that closes nothing flushes nothing: a flush is a sink write, not an insert.
#[test]
fn a_row_that_closes_nothing_flushes_nothing() {
    let cat = parse(TWO_VIEWS).unwrap();
    let mut e = engine(&cat);
    let mut out = Recording::default();
    e.insert("trades", vec![trade(10, "A"), trade(11, "B")], &mut out);
    assert!(out.flushed.is_empty() && out.pending.is_empty());
}

/// An idle close (`close_until`) flushes each view's messages too.
#[test]
fn an_idle_close_flushes_each_views_messages() {
    let cat = parse(TWO_VIEWS).unwrap();
    let mut e = engine(&cat);
    e.insert("trades", vec![trade(10, "A")], &mut vec![]);
    let mut out = Recording::default();
    e.close_until(4_000 * 1_000_000, &mut out);
    let topics: Vec<Vec<&str>> = out.flushed.iter().map(|b| b.iter().map(|e| &*e.topic).collect()).collect();
    assert_eq!(topics, [vec!["short"], vec!["long"]]);
}
