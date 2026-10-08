//! `orderbook_top_n(stream, depth[, allow_seq_reset])`: the L2 book as SQL. L2 metrics read venue
//! messages through it, so what it emits must be the book's top 25 row for row (held to a naive
//! model of the book), per symbol, whatever two producer replicas publish twice or late, and
//! across a restart.
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::{Engine, State};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;

use crate::book::{feed, feeds, Msg, Naive};

/// A stream of book messages (fixtures/market.proto's BookUpdate), the book's top 25 as its own
/// stream, and that stream as JSON.
fn sql(args: &str) -> String {
    format!(
        "
CREATE STREAM ob (
  time int64, exchange int64, symbol string, is_snapshot bool, local_timestamp int64,
  bid_price array(float64), bid_amount array(float64), ask_price array(float64), ask_amount array(float64),
  venue_sequence int64
);
CREATE EXTERNAL STREAM out (
  time int64, exchange int64, symbol string, local_timestamp int64,
  bid_price array(float64), bid_amount array(float64), ask_price array(float64), ask_amount array(float64)
) SETTINGS type = 'kafka', topic = 'top', data_format = 'JSONEachRow';
CREATE MATERIALIZED VIEW top INTO out AS
SELECT time, exchange, symbol, local_timestamp, bid_price, bid_amount, ask_price, ask_amount
FROM orderbook_top_n(ob, {args});
"
    )
}

fn engine(args: &str) -> Engine {
    Engine::new(&parse(&sql(args)).unwrap()).unwrap()
}

fn plan_error(args: &str) -> String {
    Engine::new(&parse(&sql(args)).unwrap()).err().expect("planned")
}

fn arr(v: &[f64]) -> Value {
    Value::Array(v.iter().map(|x| Value::F64(*x)).collect())
}

/// BookUpdate's venue_sequence: the venue's book version, 0 where the venue has none.
fn sequence(seq: Option<i64>) -> Value {
    Value::Int(seq.unwrap_or(0))
}

#[derive(Clone, Copy)]
struct M<'a> {
    time: i64,
    symbol: &'a str,
    snapshot: bool,
    seq: Option<i64>,
    bids: &'a [(f64, f64)],
    asks: &'a [(f64, f64)],
}

fn row(m: &M) -> Vec<Value> {
    let side =
        |ls: &[(f64, f64)], price: bool| arr(&ls.iter().map(|l| if price { l.0 } else { l.1 }).collect::<Vec<_>>());
    vec![
        Value::Int(m.time),
        Value::Int(1),
        Value::Str(m.symbol.into()),
        Value::Bool(m.snapshot),
        Value::Int(m.time + 7),
        side(m.bids, true),
        side(m.bids, false),
        side(m.asks, true),
        side(m.asks, false),
        sequence(m.seq),
    ]
}

/// Each emitted row as (time, symbol, best bid, best ask, bid levels, ask levels).
type Top = (i64, String, Option<f64>, Option<f64>, usize, usize);

fn tops(e: &mut Engine, ms: &[M]) -> Vec<Top> {
    let mut out = vec![];
    e.insert("ob", ms.iter().map(row).collect(), &mut out);
    out.iter()
        .map(|m| {
            let v: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
            let first = |k: &str| v[k].as_array().unwrap().first().map(|x| x.as_f64().unwrap());
            let len = |k: &str| v[k].as_array().unwrap().len();
            (
                v["time"].as_i64().unwrap(),
                v["symbol"].as_str().unwrap().to_string(),
                first("bid_price"),
                first("ask_price"),
                len("bid_price"),
                len("ask_price"),
            )
        })
        .collect()
}

fn snap<'a>(time: i64, symbol: &'a str, seq: Option<i64>, bid: f64, ask: f64) -> M<'a> {
    M {
        time,
        symbol,
        snapshot: true,
        seq,
        bids: Box::leak(Box::new([(bid, 1.0), (bid - 1.0, 2.0)])),
        asks: Box::leak(Box::new([(ask, 1.0), (ask + 1.0, 2.0)])),
    }
}

fn diff<'a>(time: i64, symbol: &'a str, seq: Option<i64>, bids: &'a [(f64, f64)]) -> M<'a> {
    M { time, symbol, snapshot: false, seq, bids, asks: &[] }
}

// ---- the naive model's top 25 through SQL ----------------------------------------------------

/// Generated venue messages (`crate::book::feed`) as rows of `ob` for `symbol`, each with its
/// position as its sequence.
fn messages(msgs: &[Msg], symbol: &str) -> Vec<Vec<Value>> {
    let col = |ls: &[brrrrr_core::book::Level], price: bool| {
        arr(&ls.iter().map(|l| if price { l.price } else { l.amount }).collect::<Vec<_>>())
    };
    (msgs.iter().enumerate())
        .map(|(i, m)| {
            vec![
                Value::Int(m.ts),
                Value::Int(1),
                Value::Str(symbol.into()),
                Value::Bool(m.snapshot),
                Value::Int(m.local),
                col(&m.bids, true),
                col(&m.bids, false),
                col(&m.asks, true),
                col(&m.asks, false),
                sequence(Some(i as i64 + 1)),
            ]
        })
        .collect()
}

/// Feeds `msgs` through the SQL in chunks of `chunk` and checks every emitted row against the
/// naive model's top 25 after each message that changed it, in order; returns the number of rows.
fn naive_parity(msgs: &[Msg], chunk: usize) -> usize {
    let col = |ls: &[brrrrr_core::book::Level], price: bool| -> Vec<f64> {
        ls.iter().map(|l| if price { l.price } else { l.amount }).collect()
    };
    let (mut naive, mut want) = (Naive::default(), vec![]);
    for m in msgs {
        if naive.apply(m.snapshot, &m.bids, &m.asks) {
            let (b, a) = &naive.last;
            want.push((m, (col(b, true), col(b, false), col(a, true), col(a, false))));
        }
    }
    let mut e = engine("25");
    let mut rows = 0;
    for c in messages(msgs, "A").chunks(chunk) {
        let mut out = vec![];
        e.insert("ob", c.to_vec(), &mut out);
        for m in out {
            let got: serde_json::Value = serde_json::from_str(&m.payload).unwrap();
            let (msg, levels) =
                want.get(rows).unwrap_or_else(|| panic!("emitted row {rows} but the model has no more"));
            assert_eq!(got["time"].as_i64(), Some(msg.ts), "row {rows} time");
            assert_eq!(got["local_timestamp"].as_i64(), Some(msg.local), "row {rows} local_timestamp");
            let have =
                |k: &str| -> Vec<f64> { got[k].as_array().unwrap().iter().map(|x| x.as_f64().unwrap()).collect() };
            let (bp, ba, ap, aa) = levels;
            assert_eq!((have("bid_price"), have("bid_amount")), (bp.clone(), ba.clone()), "row {rows} bids");
            assert_eq!((have("ask_price"), have("ask_amount")), (ap.clone(), aa.clone()), "row {rows} asks");
            rows += 1;
        }
    }
    assert_eq!(rows, want.len(), "the model has rows the SQL did not emit");
    rows
}

#[test]
fn generated_messages_through_sql_are_the_naive_models_top_25() {
    let [deep, shallow] = feeds();
    // one message per chunk as a live partition delivers them, and chunks of many: the same rows
    let rows = naive_parity(&deep, 1);
    assert!(rows > deep.len() / 3, "{rows} rows");
    assert_eq!(naive_parity(&deep, 1000), rows);
    assert!(naive_parity(&shallow, 100) > shallow.len() / 3);
}

// ---- books, the replica guard and malformed messages -----------------------------------------

#[test]
fn each_symbol_has_its_own_book() {
    let mut e = engine("25");
    let got = tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0), snap(2, "B", Some(10), 50.0, 51.0)]);
    assert_eq!(got, [(1, "A".into(), Some(100.0), Some(101.0), 2, 2), (2, "B".into(), Some(50.0), Some(51.0), 2, 2)]);
    // a diff of A's is A's alone: B's book is as it was
    let got = tops(&mut e, &[diff(3, "A", Some(11), &[(100.5, 1.0)]), diff(4, "B", Some(11), &[(49.0, 0.0)])]);
    assert_eq!(got, [(3, "A".into(), Some(100.5), Some(101.0), 3, 2), (4, "B".into(), Some(50.0), Some(51.0), 1, 2)]);
}

#[test]
fn the_same_symbol_on_two_exchanges_has_two_books() {
    let mut e = engine("25");
    let mut a = row(&snap(1, "A", Some(1), 100.0, 101.0));
    let mut b = row(&snap(2, "A", Some(1), 50.0, 51.0));
    (a[1], b[1]) = (Value::Int(1), Value::Int(2));
    let mut out = vec![];
    e.insert("ob", vec![a, b], &mut out);
    let c = row(&diff(3, "A", Some(2), &[(100.5, 1.0)])); // exchange 1's
    e.insert("ob", vec![c], &mut out);
    let best: Vec<f64> = out
        .iter()
        .map(|m| serde_json::from_str::<serde_json::Value>(&m.payload).unwrap()["bid_price"][0].as_f64().unwrap())
        .collect();
    assert_eq!(best, [100.0, 50.0, 100.5]);
}

#[test]
fn a_replicas_late_snapshot_is_brought_up_to_date_not_applied_as_is() {
    // replica A's diffs moved the book to seq 12; replica B's snapshot at seq 11 (its own dedup
    // key, so the broker admits it) lands after them. As is it would rewind the book to 11;
    // brought up to date with the diff after it, it is the book now: nothing changed, no row
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0), diff(2, "A", Some(12), &[(100.5, 1.0)])]);
    assert!(tops(&mut e, &[snap(3, "A", Some(11), 100.0, 101.0)]).is_empty());
    assert_eq!((e.books().stale, e.books().caught_up), (0, 1));
    // a replayed diff (not newer than the book) is dropped; a newer one applies
    assert!(tops(&mut e, &[diff(4, "A", Some(12), &[(100.7, 1.0)])]).is_empty());
    assert_eq!(tops(&mut e, &[diff(5, "A", Some(13), &[(100.8, 1.0)])])[0].2, Some(100.8));
    assert_eq!(e.books().stale, 1);
}

#[test]
fn a_late_snapshot_corrects_a_book_that_missed_a_diff() {
    // diff 11 (a bid at 100.2) never reached the book; diff 12 did. Producers snapshot every
    // symbol periodically, and a replica's snapshot at 11 has the bid: brought up to date with
    // diff 12, the book gets it back, and the corrected top is a row
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0), diff(3, "A", Some(12), &[(99.5, 3.0)])]);
    let late = M {
        time: 2,
        symbol: "A",
        snapshot: true,
        seq: Some(11),
        bids: &[(100.2, 1.0), (100.0, 1.0), (99.0, 2.0)],
        asks: &[(101.0, 1.0), (102.0, 2.0)],
    };
    assert_eq!(tops(&mut e, &[late]), [(2, "A".into(), Some(100.2), Some(101.0), 4, 2)]);
    assert_eq!(e.books().caught_up, 1);
}

#[test]
fn a_snapshot_older_than_the_diffs_kept_is_dropped() {
    // the book keeps the diffs since its last snapshot: one older than that snapshot cannot be
    // brought up to date (and that snapshot is newer anyway)
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0), diff(2, "A", Some(12), &[(100.5, 1.0)])]);
    assert!(tops(&mut e, &[snap(3, "A", Some(9), 90.0, 91.0)]).is_empty());
    assert_eq!((e.books().stale, e.books().caught_up), (1, 0));
    // nor once the diffs it would need have gone: at most KEEP_DIFFS are kept
    let n = brrrrr_core::book::KEEP_DIFFS as i64;
    let rows: Vec<Vec<Value>> =
        (0..=n).map(|i| row(&diff(10 + i, "A", Some(13 + i), &[(99.0, 1.0 + i as f64)]))).collect();
    e.insert("ob", rows, &mut vec![]);
    // diffs 12 and 13 went: a snapshot at 13 has diff 13 in it and every later diff is kept,
    // one at 12 would need diff 13
    assert!(tops(&mut e, &[snap(20 + n, "A", Some(12), 100.0, 101.0)]).is_empty());
    assert_eq!((e.books().stale, e.books().caught_up), (2, 0));
    // and nor past KEEP_DIFFS_US of venue time
    let mut e = engine("25");
    let later = brrrrr_core::book::KEEP_DIFFS_US + 10;
    tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0), diff(2, "A", Some(12), &[(100.5, 1.0)])]);
    tops(&mut e, &[diff(later, "A", Some(14), &[(100.6, 1.0)])]);
    assert!(tops(&mut e, &[snap(later + 1, "A", Some(11), 100.0, 101.0)]).is_empty());
    assert_eq!((e.books().stale, e.books().caught_up), (1, 0));
}

#[test]
fn with_seq_reset_a_late_snapshot_is_brought_up_to_date_in_time_order() {
    // with seq reset, time orders, seq only within one timestamp: a snapshot at an earlier time than the book
    // is brought up to date with the diffs of later times
    let mut e = engine("25, true");
    tops(&mut e, &[snap(1, "A", Some(500), 100.0, 101.0), diff(3, "A", Some(502), &[(100.5, 1.0)])]);
    let late = M { time: 2, symbol: "A", snapshot: true, seq: Some(501), bids: &[(100.3, 1.0)], asks: &[(101.0, 1.0)] };
    assert_eq!(tops(&mut e, &[late]), [(2, "A".into(), Some(100.5), Some(101.0), 2, 1)]);
    assert_eq!(e.books().caught_up, 1);
}

#[test]
fn without_a_seq_a_late_snapshot_is_brought_up_to_date_in_time_order() {
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", None, 100.0, 101.0), diff(3, "A", None, &[(100.5, 1.0)])]);
    let late = M { time: 2, symbol: "A", snapshot: true, seq: None, bids: &[(100.3, 1.0)], asks: &[(101.0, 1.0)] };
    assert_eq!(tops(&mut e, &[late]), [(2, "A".into(), Some(100.5), Some(101.0), 2, 1)]);
    // older than the book's own snapshot: dropped
    assert!(tops(&mut e, &[snap(0, "A", None, 90.0, 91.0)]).is_empty());
    assert_eq!((e.books().stale, e.books().caught_up), (1, 1));
}

#[test]
fn the_bounds_on_kept_diffs_are_inclusive() {
    use brrrrr_core::book::{KEEP_DIFFS, KEEP_DIFFS_US};
    let diffs = |from: i64, n: i64| -> Vec<Vec<Value>> {
        (0..n).map(|i| row(&diff(2 + i, "A", Some(from + i), &[(99.0, 1.0 + i as f64)]))).collect()
    };
    // exactly KEEP_DIFFS diffs after the snapshot at 10: all kept, so a snapshot at 10 is caught up
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0)]);
    e.insert("ob", diffs(11, KEEP_DIFFS as i64), &mut vec![]);
    // a book holding the most diffs it keeps restores: a checkpoint of a busy book is no refusal
    engine("25").restore(e.snapshot()).unwrap();
    tops(&mut e, &[snap(1_000, "A", Some(10), 100.0, 101.0)]);
    assert_eq!((e.books().stale, e.books().caught_up), (0, 1));
    // one more: diff 11 goes, so a snapshot at 10 is dropped and one at 11 is caught up
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0)]);
    e.insert("ob", diffs(11, KEEP_DIFFS as i64 + 1), &mut vec![]);
    tops(&mut e, &[snap(1_000, "A", Some(10), 100.0, 101.0), snap(1_001, "A", Some(11), 100.0, 101.0)]);
    assert_eq!((e.books().stale, e.books().caught_up), (1, 1));
    // a diff exactly KEEP_DIFFS_US older than the newest is kept
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(10), 100.0, 101.0), diff(2, "A", Some(12), &[(100.5, 1.0)])]);
    tops(&mut e, &[diff(2 + KEEP_DIFFS_US, "A", Some(14), &[(100.6, 1.0)])]);
    tops(&mut e, &[snap(3 + KEEP_DIFFS_US, "A", Some(11), 100.0, 101.0)]);
    assert_eq!((e.books().stale, e.books().caught_up), (0, 1));
}

#[test]
fn diffs_before_a_symbols_first_snapshot_are_not_kept() {
    // they build nothing, and thousands of books can wait hours for their first snapshot
    let mut e = engine("25");
    tops(&mut e, &[diff(1, "A", Some(1), &[(100.0, 1.0)])]);
    let s = serde_json::to_value(e.snapshot()).unwrap();
    let book = s[0].as_array().unwrap().iter().find(|o| o.get("Book").is_some()).unwrap();
    assert_eq!(book["Book"]["books"][0][3]["diffs"], json!([]));
}

#[test]
fn a_restore_keeps_the_diffs_a_late_snapshot_needs() {
    let mut first = engine("25");
    tops(&mut first, &[snap(1, "A", Some(10), 100.0, 101.0), diff(3, "A", Some(12), &[(99.5, 3.0)])]);
    let bytes = Checkpoint::of(&first, 1, vec![], vec![]).encode();
    let mut second = engine("25");
    Checkpoint::decode(&bytes).unwrap().restore(&mut second).unwrap();
    let late = M {
        time: 2,
        symbol: "A",
        snapshot: true,
        seq: Some(11),
        bids: &[(100.2, 1.0), (100.0, 1.0), (99.0, 2.0)],
        asks: &[(101.0, 1.0), (102.0, 2.0)],
    };
    assert_eq!(tops(&mut second, &[late]), tops(&mut first, &[late]));
    assert_eq!(second.books().caught_up, 1);
}

#[test]
fn with_seq_reset_a_lower_seq_at_a_newer_time_is_taken() {
    // a venue whose sequence starts over after maintenance: the next messages have a lower seq
    // and a newer time
    let msgs = |e: &mut Engine| {
        tops(e, &[snap(1, "A", Some(500), 100.0, 101.0)]);
        tops(e, &[snap(2, "A", Some(3), 90.0, 91.0)])
    };
    assert_eq!(msgs(&mut engine("25, true")).len(), 1);
    assert!(msgs(&mut engine("25, false")).is_empty());
    assert!(msgs(&mut engine("25")).is_empty(), "no reset unless asked for");
}

#[test]
fn without_a_seq_messages_are_ordered_by_time() {
    let mut e = engine("25");
    tops(&mut e, &[snap(10, "A", None, 100.0, 101.0)]);
    assert!(tops(&mut e, &[diff(9, "A", None, &[(100.5, 1.0)])]).is_empty());
    assert_eq!(tops(&mut e, &[diff(10, "A", None, &[(100.5, 1.0)])]).len(), 1);
}

/// The book's order is the venue's: a stream that does not declare it is refused rather than
/// ordered by time behind the user's back (a venue without one sends 0).
#[test]
fn a_stream_without_a_sequence_is_refused() {
    let text = sql("25").replace(",\n  venue_sequence int64", "");
    let err = Engine::new(&parse(&text).unwrap()).err().expect("planned");
    assert!(err.contains("orderbook_top_n needs a column venue_sequence"), "{err}");
}

#[test]
fn a_message_whose_prices_and_amounts_differ_in_length_is_dropped_and_counted() {
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(1), 100.0, 101.0)]);
    let mut bad = row(&diff(2, "A", Some(2), &[(100.5, 1.0)]));
    bad[6] = arr(&[1.0, 2.0]); // two amounts for one price
    let mut out = vec![];
    e.insert("ob", vec![bad], &mut out);
    assert!(out.is_empty());
    assert_eq!(e.books().malformed, 1);
    // nor did it move the guard: the same seq, well formed, applies
    assert_eq!(tops(&mut e, &[diff(2, "A", Some(2), &[(100.5, 1.0)])]).len(), 1);
    // both arrays absent (NULL) is a message without levels on that side
    let mut none = row(&diff(3, "A", Some(3), &[]));
    (none[5], none[6], none[7], none[8]) = (Value::Null, Value::Null, arr(&[101.0]), arr(&[0.0]));
    e.insert("ob", vec![none], &mut out);
    assert_eq!(out.len(), 1, "the best ask was deleted");
    assert_eq!(e.books().malformed, 1);
}

#[test]
fn messages_before_a_symbols_first_snapshot_build_nothing() {
    let mut e = engine("25");
    assert!(tops(&mut e, &[diff(1, "A", Some(1), &[(100.0, 1.0)])]).is_empty());
    let b = e.books();
    assert_eq!((b.books, b.awaiting_snapshot), (1, 1));
    tops(&mut e, &[snap(2, "A", Some(2), 100.0, 101.0)]);
    let b = e.books();
    assert_eq!((b.books, b.awaiting_snapshot), (1, 0));
}

#[test]
fn a_smaller_depth_emits_only_when_its_own_top_changes() {
    let mut e = engine("1");
    assert_eq!(tops(&mut e, &[snap(1, "A", Some(1), 100.0, 101.0)]), [(1, "A".into(), Some(100.0), Some(101.0), 1, 1)]);
    // a change below the best level is not a row of a depth-1 book
    assert!(tops(&mut e, &[diff(2, "A", Some(2), &[(99.0, 5.0)])]).is_empty());
    assert_eq!(tops(&mut e, &[diff(3, "A", Some(3), &[(100.0, 5.0)])]).len(), 1);
}

#[test]
fn every_other_column_is_the_messages_own() {
    let mut e = engine("25");
    let mut out = vec![];
    e.insert("ob", vec![row(&snap(1_700_000_000_000_000, "A", Some(1), 100.0, 101.0))], &mut out);
    assert_eq!(
        out[0].payload,
        "{\"time\":1700000000000000,\"exchange\":1,\"symbol\":\"A\",\"local_timestamp\":1700000000000007,\
         \"bid_price\":[100,99],\"bid_amount\":[1,2],\"ask_price\":[101,102],\"ask_amount\":[1,2]}\n"
    );
}

// ---- checkpoints -------------------------------------------------------------------------------

#[test]
fn a_restore_mid_stream_continues_every_book_exactly() {
    let [ms, _] = feeds();
    let ms = messages(&ms, "A");
    let (head, tail) = ms.split_at(ms.len() / 2);
    let mut straight = engine("25");
    let (mut a, mut b) = (vec![], vec![]);
    straight.insert("ob", head.to_vec(), &mut a);
    straight.insert("ob", tail.to_vec(), &mut a);
    let mut first = engine("25");
    first.insert("ob", head.to_vec(), &mut b);
    let bytes = Checkpoint::of(&first, 1, vec![], vec![]).encode();
    let mut second = engine("25");
    Checkpoint::decode(&bytes).unwrap().restore(&mut second).unwrap();
    second.insert("ob", tail.to_vec(), &mut b);
    assert!(a.len() > ms.len() / 3);
    assert_eq!(a, b);
}

/// Restores `e`'s snapshot, its books edited through JSON by `edit`, into a fresh engine: the
/// error it is refused with.
fn refused(e: &Engine, edit: impl FnOnce(&mut Vec<Json>)) -> String {
    refused_by(e, "25", edit)
}

/// `refused` into a fresh engine planned with `args`.
fn refused_by(e: &Engine, args: &str, edit: impl FnOnce(&mut Vec<Json>)) -> String {
    let mut s = serde_json::to_value(e.snapshot()).unwrap();
    let book = s[0].as_array_mut().unwrap().iter_mut().find(|o| o.get("Book").is_some()).expect("a book operator");
    edit(book["Book"]["books"].as_array_mut().unwrap());
    let s: State = serde_json::from_value(s).unwrap();
    engine(args).restore(s).expect_err("refused")
}

#[test]
fn a_restore_refuses_books_the_plan_could_not_have_built() {
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(1), 100.0, 101.0), snap(1, "B", Some(1), 50.0, 51.0)]);
    tops(&mut e, &[diff(2, "A", Some(2), &[(100.5, 1.0)]), diff(3, "A", Some(3), &[(100.6, 1.0)])]);
    for (what, edit, want) in [
        (
            "keys out of order",
            Box::new(|b: &mut Vec<Json>| b.swap(0, 1)) as Box<dyn FnOnce(&mut Vec<Json>)>,
            "key order",
        ),
        ("a key twice", Box::new(|b: &mut Vec<Json>| b[1][0] = b[0][0].clone()), "key order"),
        // under another depth or seq rule the book would emit other rows
        ("another depth", Box::new(|b: &mut Vec<Json>| b[0][1]["depth"] = json!(10)), "depth"),
        ("another seq rule", Box::new(|b: &mut Vec<Json>| b[0][2]["allow_seq_reset"] = json!(true)), "seq reset"),
        ("a top that is not the book's", Box::new(|b: &mut Vec<Json>| reverse(&mut b[0][1]["top_bids"])), "top"),
        ("levels out of order", Box::new(|b: &mut Vec<Json>| reverse(&mut b[0][1]["asks"]["levels"])), "order"),
        ("bids kept as asks", Box::new(|b: &mut Vec<Json>| b[0][1]["bids"]["bids"] = json!(false)), "order"),
        (
            "a level of amount 0",
            Box::new(|b: &mut Vec<Json>| b[0][1]["bids"]["levels"][0]["amount"] = json!(0.0)),
            "order",
        ),
        // the kept diffs are in the book's order, after the position they are kept from
        ("kept diffs out of order", Box::new(|b: &mut Vec<Json>| reverse(&mut b[0][3]["diffs"])), "kept diffs"),
        ("kept from after a kept diff", Box::new(|b: &mut Vec<Json>| b[0][3]["from"][1] = json!(99)), "kept diffs"),
        (
            "a kept diff twice",
            Box::new(|b: &mut Vec<Json>| {
                let d = b[0][3]["diffs"][0].clone();
                b[0][3]["diffs"].as_array_mut().unwrap().insert(0, d)
            }),
            "kept diffs",
        ),
        (
            "more kept diffs than kept",
            Box::new(|b: &mut Vec<Json>| {
                let d = b[0][3]["diffs"][0].clone();
                let many: Vec<Json> = (0..=brrrrr_core::book::KEEP_DIFFS as i64)
                    .map(|i| {
                        let mut d = d.clone();
                        (d["time"], d["seq"]) = (json!(2 + i), json!(2 + i));
                        d
                    })
                    .collect();
                b[0][3]["diffs"] = Json::Array(many)
            }),
            "at most",
        ),
    ] {
        let err = refused(&e, edit);
        assert!(err.contains(want), "{what}: {err}");
    }
    // the untouched state restores
    engine("25").restore(e.snapshot()).unwrap();
    // with seq resets, time then seq: a kept diff at the same position twice is refused too
    let mut e = engine("25, true");
    tops(&mut e, &[snap(1, "A", Some(1), 100.0, 101.0), diff(2, "A", Some(2), &[(100.5, 1.0)])]);
    let err = refused_by(&e, "25, true", |b| {
        let d = b[0][3]["diffs"][0].clone();
        b[0][3]["diffs"].as_array_mut().unwrap().push(d)
    });
    assert!(err.contains("kept diffs"), "{err}");
    // kept from a message without seq (a seq-less snapshot) later than kept diffs that have one:
    // time orders the pair, so they are not after it, whatever their seqs
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", Some(1), 100.0, 101.0), diff(2, "A", Some(2), &[(100.5, 1.0)])]);
    let err = refused(&e, |b| b[0][3]["from"] = json!([10, 0]));
    assert!(err.contains("kept diffs"), "{err}");
}

fn reverse(v: &mut Json) {
    v.as_array_mut().unwrap().reverse();
}

#[test]
fn the_plan_fingerprint_holds_the_depth_and_the_seq_reset() {
    let fp = |a: &str| engine(a).fingerprint();
    assert_ne!(fp("25"), fp("10"));
    assert_ne!(fp("25"), fp("25, true"));
    assert_eq!(fp("25"), fp("25, false"));
}

// ---- planning --------------------------------------------------------------------------------

#[test]
fn what_the_book_cannot_take_is_refused_at_plan_time() {
    for (args, want) in [
        ("0", "depth"),
        ("1001", "depth"),
        ("2.5", "depth"),
        ("-1", "depth"),
        ("'25'", "depth"),
        ("25, 1", "allow_seq_reset"),
        ("25, true, 1", "orderbook_top_n(stream, depth[, allow_seq_reset])"),
    ] {
        let err = plan_error(args);
        assert!(err.contains(want), "{args}: {err}");
    }
    let err = Engine::new(&parse(&sql("25").replace("FROM orderbook_top_n(ob", "FROM orderbook_top_n(nope")).unwrap())
        .err()
        .unwrap();
    assert!(err.contains("unknown stream nope"), "{err}");
    let no_symbol = sql("25").replace("symbol string, is_snapshot", "sym string, is_snapshot");
    let err =
        Engine::new(&parse(&no_symbol.replace("SELECT time, exchange, symbol,", "SELECT time, exchange,")).unwrap())
            .err()
            .unwrap();
    assert!(err.contains("orderbook_top_n needs a column symbol"), "{err}");
}

#[test]
fn a_book_can_be_aliased_and_filtered_like_any_relation() {
    let text =
        sql("25").replace("FROM orderbook_top_n(ob, 25);", "FROM orderbook_top_n(ob, 25) AS b WHERE b.symbol = 'A';");
    let mut e = Engine::new(&parse(&text).unwrap()).unwrap();
    let got = tops(&mut e, &[snap(1, "A", Some(1), 100.0, 101.0), snap(1, "B", Some(1), 50.0, 51.0)]);
    assert_eq!(got.len(), 1);
    assert_eq!(e.books().books, 2, "B's book is still kept: the filter reads the book's rows");
}

// ---- the example book pipeline end to end ---------------------------------------------------

/// fixtures/pipelines/book.sql on generated messages of two symbols on each of its two sources
/// (the second's book allowing a sequence reset), in capture order, then a snapshot of each book
/// days later that closes every window: its depth windows are those the naive model's books
/// give, computed here from the model's top 25 after each message that changed it (the rows
/// `orderbook_top_n` emits): each window's last depths of the top 5, 10 and 25 levels, the mean
/// of the top 5's, and the mean imbalance of the top 25.
#[test]
fn the_book_pipelines_depth_windows_are_the_naive_models() {
    let sql = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/pipelines/book.sql"));
    let cat = parse(&sql.unwrap()).unwrap();
    let mut e = Engine::new(&cat).unwrap();
    // (capture time, source, row) of every message, and the model's depths of each row
    let mut msgs = vec![];
    type Depths = [Option<f64>; 6];
    let mut want_rows: Vec<(i64, i64, String, Depths)> = vec![];
    for (source, exchange, seed) in [("book", 1, 3), ("spot_book", 2, 5)] {
        for symbol in ["A", "B"] {
            let feed = feed(seed * 10 + (symbol == "B") as u64, 3_000, 40, 100.0);
            let mut naive = Naive::default();
            for (i, mut row) in messages(&feed, symbol).into_iter().enumerate() {
                let m = &feed[i];
                row[1] = Value::Int(exchange);
                msgs.push((m.local, source, row));
                if naive.apply(m.snapshot, &m.bids, &m.asks) {
                    let (b, a) = &naive.last;
                    let sum = |ls: &[brrrrr_core::book::Level], n: usize| {
                        (!ls.is_empty()).then(|| ls.iter().take(n).map(|l| l.amount).sum::<f64>())
                    };
                    let d = [sum(a, 5), sum(b, 5), sum(a, 10), sum(b, 10), sum(a, 25), sum(b, 25)];
                    want_rows.push((m.local, exchange, symbol.to_string(), d));
                }
            }
        }
    }
    msgs.sort_by_key(|m| m.0); // stable: each book's messages keep their order
    let end = msgs.last().unwrap().0 + 4 * 86_400_000_000;
    for source in ["book", "spot_book"] {
        for symbol in ["A", "B"] {
            let closing = vec![
                Value::Int(end),
                Value::Int(if source == "book" { 1 } else { 2 }),
                Value::Str(symbol.into()),
                Value::Bool(true),
                Value::Int(end),
                arr(&[0.1]),
                arr(&[1.0]),
                arr(&[0.2]),
                arr(&[1.0]),
                sequence(Some(1_000_000)),
            ];
            msgs.push((end, source, closing));
        }
    }
    let mut out = vec![];
    for c in msgs.chunk_by(|a, b| a.1 == b.1) {
        e.insert(c[0].1, c.iter().map(|m| m.2.clone()).collect(), &mut out);
    }
    // the model's windows of 15 s of capture time: (exchange, symbol, start) -> rows' depths
    let mut windows: BTreeMap<(i64, String, i64), Vec<Depths>> = BTreeMap::new();
    for (local, exchange, symbol, d) in want_rows {
        windows.entry((exchange, symbol, local - local.rem_euclid(15_000_000))).or_default().push(d);
    }
    let got: BTreeMap<(i64, String, i64), Json> = (out.iter())
        .map(|m| serde_json::from_str::<Json>(&m.payload).unwrap())
        .map(|r| {
            (
                (
                    r["exchange"].as_i64().unwrap(),
                    r["symbol"].as_str().unwrap().to_string(),
                    r["time"].as_i64().unwrap(),
                ),
                r,
            )
        })
        .collect();
    assert_eq!(got.keys().collect::<Vec<_>>(), windows.keys().collect::<Vec<_>>(), "windows differ");
    // within `rel` of the model's: the means of the top 5 are cast to float32
    let close = |a: Option<f64>, b: Option<f64>, rel: f64, what: &str| match (a, b) {
        (Some(a), Some(b)) => assert!((a - b).abs() <= rel * b.abs().max(1.0), "{what}: {a} against {b}"),
        _ => assert_eq!(a, b, "{what}"),
    };
    for (k, rows) in &windows {
        let g = &got[k];
        let last = rows.last().unwrap();
        for (i, col) in ["ask_5", "bid_5", "ask_10", "bid_10", "ask_25", "bid_25"].iter().enumerate() {
            assert_eq!(g[col].as_f64(), last[i], "{k:?} {col}");
        }
        let mean = |xs: Vec<f64>| (!xs.is_empty()).then(|| xs.iter().sum::<f64>() / xs.len() as f64);
        close(
            g["ask_5_avg"].as_f64(),
            mean(rows.iter().filter_map(|d| d[0]).collect()),
            1e-7,
            &format!("{k:?} ask_5_avg"),
        );
        close(
            g["bid_5_avg"].as_f64(),
            mean(rows.iter().filter_map(|d| d[1]).collect()),
            1e-7,
            &format!("{k:?} bid_5_avg"),
        );
        let imbalance = rows.iter().filter_map(|d| match (d[5], d[4]) {
            (Some(b), Some(a)) if b + a != 0.0 => Some((b - a) / (b + a)),
            _ => None,
        });
        close(g["imbalance_25_avg"].as_f64(), mean(imbalance.collect()), 1e-12, &format!("{k:?} imbalance_25_avg"));
    }
    assert!(windows.len() > 30, "{} windows", windows.len());
}

/// For profiling (`valgrind --tool=callgrind`): a generated feed through the example book
/// pipeline, in chunks of 1000 messages.
#[test]
#[ignore]
fn book_profile() {
    let sql = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/pipelines/book.sql"));
    let mut e = Engine::new(&parse(&sql.unwrap()).unwrap()).unwrap();
    let [msgs, _] = feeds();
    let rows = messages(&msgs, "A");
    let (mut out, started) = (vec![], std::time::Instant::now());
    for c in rows.chunks(1000) {
        e.insert("book", c.to_vec(), &mut out);
    }
    eprintln!("{} messages in {:?}, {} emitted", rows.len(), started.elapsed(), out.len());
}

// ---- late snapshots against in-order application ------------------------------------------------

/// Restores `e`'s own checkpoint into a fresh engine of `args`.
fn roundtrip(e: &Engine, args: &str) -> Result<(), String> {
    let bytes = Checkpoint::of(e, 1, vec![], vec![]).encode();
    Checkpoint::decode(&bytes).unwrap().restore(&mut engine(args))
}

#[test]
fn every_state_the_guard_takes_restores() {
    // messages without a seq at one time are all taken (the guard drops only older ones), so
    // kept diffs at one time, or at their snapshot's, are a state the engine makes: its own
    // checkpoint of it must restore, or the pipeline cannot start again
    for ms in [
        vec![
            snap(1, "A", None, 100.0, 101.0),
            diff(2, "A", None, &[(100.5, 1.0)]),
            diff(2, "A", None, &[(100.6, 1.0)]),
        ],
        vec![snap(1, "A", None, 100.0, 101.0), diff(1, "A", None, &[(100.5, 1.0)])],
        vec![snap(5, "A", Some(10), 100.0, 101.0), diff(5, "A", None, &[(100.5, 1.0)])],
    ] {
        let mut e = engine("25");
        tops(&mut e, &ms);
        assert_eq!(e.books().stale, 0);
        roundtrip(&e, "25").unwrap();
    }
}

/// A book's levels and top, as JSON.
fn levels_of(e: &Engine) -> Json {
    let s = serde_json::to_value(e.snapshot()).unwrap();
    let book = s[0].as_array().unwrap().iter().find(|o| o.get("Book").is_some()).unwrap().clone();
    let b = &book["Book"]["books"][0][1];
    json!([b["bids"], b["asks"], b["top_bids"], b["top_asks"]])
}

#[test]
fn without_a_seq_diffs_at_the_late_snapshots_time_are_applied_after_it() {
    // in order, a diff at the snapshot's own time comes after it: brought up to date, the late
    // snapshot must get that diff too
    let (s1, d2, d3, s2) = (
        snap(1, "A", None, 100.0, 101.0),
        diff(2, "A", None, &[(100.5, 1.0)]),
        diff(3, "A", None, &[(99.5, 1.0)]),
        snap(2, "A", None, 100.0, 101.0),
    );
    let mut in_order = engine("25");
    tops(&mut in_order, &[s1, s2, d2, d3]);
    let mut late = engine("25");
    tops(&mut late, &[s1, d2, d3, s2]);
    assert_eq!(late.books().caught_up, 1);
    assert_eq!(levels_of(&in_order), levels_of(&late));
}

#[test]
fn without_a_seq_a_snapshot_at_a_dropped_diffs_time_is_not_covered() {
    // the diff dropped at time 2 may have come after a snapshot at 2: that one is dropped; one
    // at 3 is covered
    use brrrrr_core::book::KEEP_DIFFS_US;
    let mut e = engine("25");
    tops(&mut e, &[snap(1, "A", None, 100.0, 101.0), diff(2, "A", None, &[(100.5, 1.0)])]);
    tops(&mut e, &[diff(3 + KEEP_DIFFS_US, "A", None, &[(100.6, 1.0)])]);
    tops(&mut e, &[snap(2, "A", None, 100.0, 101.0)]);
    assert_eq!((e.books().stale, e.books().caught_up), (1, 0));
    tops(&mut e, &[snap(3, "A", None, 100.0, 101.0)]);
    assert_eq!((e.books().stale, e.books().caught_up), (1, 1));
}

#[test]
fn a_late_snapshot_brought_up_to_date_is_the_book_in_order_application_makes() {
    // random books and diffs over a narrow price range (crossed books are frequent), by seq and
    // with seq resets: the snapshot applied late and brought up to date must leave the same book
    // as the snapshot applied in order
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut rnd = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let leak = |v: Vec<(f64, f64)>| -> &'static [(f64, f64)] { Box::leak(v.into_boxed_slice()) };
    for case in 0..1000 {
        for args in ["3", "3, true"] {
            let side = |n: usize, zero: bool, rnd: &mut dyn FnMut() -> u64| -> Vec<(f64, f64)> {
                let mut v: Vec<(f64, f64)> = (0..n)
                    .map(|_| {
                        (
                            95.0 + (rnd() % 12) as f64,
                            if zero && rnd().is_multiple_of(3) { 0.0 } else { 1.0 + (rnd() % 3) as f64 },
                        )
                    })
                    .collect();
                v.sort_by(|a, b| a.0.total_cmp(&b.0));
                v.dedup_by(|a, b| a.0 == b.0);
                v
            };
            let (sb, sa) = (leak(side(4, false, &mut rnd)), leak(side(4, false, &mut rnd)));
            let first =
                M { time: 5, symbol: "A", snapshot: true, seq: Some(5), bids: &[(90.0, 1.0)], asks: &[(110.0, 1.0)] };
            let snapshot = M { time: 10, symbol: "A", snapshot: true, seq: Some(10), bids: sb, asks: sa };
            let n = 1 + rnd() % 8;
            let diffs: Vec<M> = (0..n)
                .map(|i| {
                    let (nb, na) = ((rnd() % 3) as usize, (rnd() % 3) as usize);
                    let (bids, asks) = (leak(side(nb, true, &mut rnd)), leak(side(na, true, &mut rnd)));
                    M { time: 11 + i as i64, symbol: "A", snapshot: false, seq: Some(11 + i as i64), bids, asks }
                })
                .collect();
            let mut in_order = engine(args);
            tops(&mut in_order, &[&[first, snapshot][..], &diffs].concat());
            let mut late = engine(args);
            tops(&mut late, &[&[first][..], &diffs, &[snapshot]].concat());
            assert_eq!(late.books().caught_up, 1, "case {case} {args}");
            assert_eq!(levels_of(&in_order), levels_of(&late), "case {case} {args}");
        }
    }
}
