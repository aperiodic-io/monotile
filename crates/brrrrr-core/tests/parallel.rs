//! Windows closed on several threads (`Engine::set_close_threads`): the same messages in
//! the same order, and the same state, as on one, for every pipeline and any chunking.
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::{Emit, Engine, Output};
use brrrrr_core::sql::{parse, Catalog};
use brrrrr_core::value::Value;
use common::{catalog, fixtures, value};
use proptest::prelude::*;

use crate::common;

/// `fixture`'s chunks as (stream, rows), each split into pieces of at most `piece` rows.
fn chunks(cat: &Catalog, fixture: &serde_json::Value, piece: usize) -> Vec<(String, Vec<Vec<Value>>)> {
    let mut out = vec![];
    for chunk in fixture["chunks"].as_array().unwrap() {
        let stream = &cat.streams[chunk["stream"].as_str().unwrap()];
        let rows: Vec<Vec<Value>> = (chunk["rows"].as_array().unwrap().iter())
            .map(|r| r.as_array().unwrap().iter().zip(&stream.columns).map(|(v, c)| value(v, &c.ty)).collect())
            .collect();
        for p in rows.chunks(piece.max(1)) {
            out.push((stream.name.clone(), p.to_vec()));
        }
    }
    out
}

/// An output that keeps what it is given and refuses an empty flush: a flush is a view's sink
/// write, on any thread count.
#[derive(Default)]
struct Kept(Vec<Emit>, usize);

impl Output for Kept {
    fn push(&mut self, e: Emit) {
        self.0.push(e);
        self.1 += 1;
    }

    fn flush(&mut self) {
        assert!(self.1 > 0, "a flush with nothing written since the last");
        self.1 = 0;
    }
}

/// The messages and final state (checkpoint bytes) of `chunks` on `threads` threads, and how
/// many inserts ran on several.
fn run(cat: &Catalog, chunks: &[(String, Vec<Vec<Value>>)], threads: usize) -> (Vec<Emit>, Vec<u8>, u64) {
    let mut e = Engine::new(cat).unwrap();
    e.set_close_threads(threads);
    e.set_close_groups(0); // every chunk that closes two views, whatever their size
    let mut out = Kept::default();
    for (stream, rows) in chunks {
        e.insert(stream, rows.clone(), &mut out);
    }
    e.close_until(i64::MAX / 2, &mut out);
    (out.0, Checkpoint::of(&e, 1, vec![], vec![]).encode(), e.parallel_inserts())
}

/// Every pipeline on 2, 3 and 8 threads: the same messages in the same order and the same state
/// as on one. The pipelines with several views of one input close them on several threads.
#[test]
fn every_pipeline_closes_on_several_threads_as_on_one() {
    let (mut parallel, mut pipelines) = (vec![], 0);
    for fixture in fixtures() {
        let name = fixture["pipeline"].as_str().unwrap().to_string();
        let cat = catalog(&fixture);
        let chunks = chunks(&cat, &fixture, usize::MAX);
        let (want, state, none) = run(&cat, &chunks, 1);
        assert_eq!(none, 0, "{name}: one thread ran inserts on several");
        for threads in [2, 3, 8] {
            let (got, got_state, n) = run(&cat, &chunks, threads);
            assert!(got == want, "{name} on {threads} threads: other messages, or in another order");
            assert!(got_state == state, "{name} on {threads} threads: another state");
            if threads == 8 && n > 0 {
                parallel.push(name.clone());
            }
        }
        pipelines += 1;
    }
    assert!(pipelines >= 8);
    for p in ["bars", "flow", "returns", "state", "stats"] {
        assert!(parallel.iter().any(|n| n.contains(p)), "{p} never closed on several threads: {parallel:?}");
    }
}

const JOINED: &str = "
CREATE STREAM trades (t datetime64(6), symbol string, price float64);
CREATE STREAM marks (t datetime64(6), symbol string, mark float64);
CREATE EXTERNAL STREAM a_out (symbol string, n uint64) SETTINGS type = 'kafka', topic = 'a', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM b_out (symbol string, n uint64) SETTINGS type = 'kafka', topic = 'b', data_format = 'JSONEachRow';
CREATE EXTERNAL STREAM c_out (symbol string, price float64, mark float64) SETTINGS type = 'kafka', topic = 'c', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW to_marks INTO marks AS SELECT window_start AS t, symbol, avg(price) AS mark FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW a INTO a_out AS SELECT symbol, count() AS n FROM tumble(trades, t, 15s) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW b INTO b_out AS SELECT symbol, count() AS n FROM tumble(trades, t, 1m) GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
CREATE MATERIALIZED VIEW c INTO c_out AS SELECT tr.symbol AS symbol, tr.price AS price, m.mark AS mark
FROM (SELECT t, symbol, price FROM trades ORDER BY symbol, t) AS tr
ASOF LEFT JOIN (SELECT t, symbol, mark FROM marks ORDER BY symbol, t) AS m ON tr.symbol = m.symbol AND tr.t >= m.t;
";

fn trade(sec: f64, symbol: &str, price: f64) -> Vec<Value> {
    vec![Value::Time((sec * 1e6) as i64), Value::Str(symbol.into()), Value::F64(price)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A view reading what another reader of its input writes (here `c`, joined with the marks
    /// `to_marks` writes from the same trades) runs in its turn, after that write, never on
    /// another thread: any trades in any chunking give the messages and state of one thread.
    #[test]
    fn a_view_reading_what_another_reader_writes_runs_in_its_turn(
        trades in proptest::collection::vec((0u32..400, 0usize..3, 1u32..100), 1..300),
        pieces in proptest::collection::vec(1usize..40, 1..20),
        threads in 2usize..6,
    ) {
        let cat = parse(JOINED).unwrap();
        let mut trades: Vec<Vec<Value>> = trades
            .into_iter()
            .map(|(t, s, p)| trade(t as f64 * 0.7, ["X", "Y", "Z"][s], p as f64))
            .collect();
        trades.sort_by_key(|r| if let Value::Time(t) = r[0] { t } else { 0 });
        let mut chunks = vec![];
        let (mut at, mut i) = (0, 0);
        while at < trades.len() {
            let n = pieces[i % pieces.len()].min(trades.len() - at);
            chunks.push(("trades".to_string(), trades[at..at + n].to_vec()));
            (at, i) = (at + n, i + 1);
        }
        chunks.push(("trades".to_string(), vec![trade(10_000.0, "X", 1.0)]));
        let (want, state, _) = run(&cat, &chunks, 1);
        let (got, got_state, _) = run(&cat, &chunks, threads);
        prop_assert!(got == want);
        prop_assert!(got_state == state);
    }

    /// The example pipelines in random chunkings on several threads: as on one.
    #[test]
    fn pipelines_in_any_chunking_close_on_several_threads_as_on_one(
        which in 0usize..12,
        piece in 1usize..400,
        threads in 2usize..9,
    ) {
        let fixtures = common::fixtures();
        let fixture = &fixtures[which % fixtures.len()];
        let cat = catalog(fixture);
        let chunks = chunks(&cat, fixture, piece);
        let (want, state, _) = run(&cat, &chunks, 1);
        let (got, got_state, _) = run(&cat, &chunks, threads);
        prop_assert!(got == want, "{}", fixture["pipeline"]);
        prop_assert!(got_state == state, "{}", fixture["pipeline"]);
    }
}
