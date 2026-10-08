//! L2 order book. The L2 metrics are only as correct as the reconstructed book, so it is
//! checked message for message against a naive model: on long generated feeds, and on random
//! messages (crossed books, deletes, deep changes and re-snapshots).
use brrrrr_core::book::{Book, Guard, Level};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

/// A venue message: a snapshot of the book, or the levels that changed (amount 0 deletes).
#[derive(Clone)]
pub struct Msg {
    pub ts: i64,
    pub local: i64,
    pub snapshot: bool,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// A deterministic feed of `n` venue messages of one book, prices on a tick of `1 / ticks` (a
/// decimal price, as venues send them): a snapshot of `depth` levels a side first and about
/// every 3,000 messages, then changes of 1-6 levels each: new amounts at and near the top, new
/// levels, deletes (mostly of a level the book has, now and then of one it has not), changes
/// deep below the top 25, and now and then a level across the other side's best. Exchange
/// times strictly increase, some messages microseconds apart; each is captured 3 ms later.
pub fn feed(seed: u64, n: usize, depth: i64, ticks: f64) -> Vec<Msg> {
    let mut state = seed | 1;
    let mut rnd = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let price = |k: i64| k as f64 / ticks;
    let (mut bids, mut asks) = (BTreeSet::<i64>::new(), BTreeSet::<i64>::new());
    let (mut mid, mut ts, mut out) = (100_000i64, 1_788_220_800_000_000i64, Vec::with_capacity(n));
    for i in 0..n {
        ts += [1, 50, 900, 20_000, 250_000][(rnd() % 5) as usize];
        let amount = |rnd: &mut dyn FnMut() -> u64| (1 + rnd() % 50_000) as f64 / 1_000.0;
        if i == 0 || rnd().is_multiple_of(3_000) {
            if let (Some(b), Some(a)) = (bids.last(), asks.first()) {
                mid = (b + a) / 2;
            }
            mid += (rnd() % 21) as i64 - 10;
            bids = (1..=depth).map(|j| mid - j).collect();
            asks = (1..=depth).map(|j| mid + j).collect();
            let side = |ks: &BTreeSet<i64>, rnd: &mut dyn FnMut() -> u64| -> Vec<Level> {
                ks.iter().map(|k| Level { price: price(*k), amount: amount(rnd) }).collect()
            };
            let (b, a) = (side(&bids, &mut rnd), side(&asks, &mut rnd));
            out.push(Msg { ts, local: ts + 3_000, snapshot: true, bids: b, asks: a });
            continue;
        }
        let (mut b, mut a) = (vec![], vec![]);
        for _ in 0..1 + rnd() % 6 {
            let is_bid = rnd().is_multiple_of(2);
            let (own, other) = if is_bid { (&mut bids, &mut asks) } else { (&mut asks, &mut bids) };
            // `j` levels from the top of `own`, the top being its best
            let nth =
                |s: &BTreeSet<i64>, j: usize| if is_bid { s.iter().rev().nth(j) } else { s.iter().nth(j) }.copied();
            let inward = |k: i64, by: i64| if is_bid { k + by } else { k - by };
            let best = nth(own, 0).unwrap_or(if is_bid { mid - 1 } else { mid + 1 });
            let (k, amt) = match rnd() % 10 {
                0..=3 => (nth(own, (rnd() % 10) as usize).unwrap_or(best), amount(&mut rnd)),
                4 | 5 => match nth(own, (rnd() % 40) as usize) {
                    Some(k) => (k, 0.0),
                    None => (best, amount(&mut rnd)),
                },
                6 => (inward(best, -(200 + (rnd() % 50) as i64)), 0.0),
                7 => (inward(best, (rnd() % 3) as i64), amount(&mut rnd)),
                8 => (inward(best, -(30 + (rnd() % 30) as i64)), amount(&mut rnd)),
                _ => {
                    // across the other side's best when it has one, once in four
                    let across = if is_bid { other.first() } else { other.last() };
                    match across.filter(|_| rnd().is_multiple_of(4)) {
                        Some(&o) => (inward(o, (rnd() % 2) as i64), amount(&mut rnd)),
                        None => (inward(best, 1), amount(&mut rnd)),
                    }
                }
            };
            if amt == 0.0 {
                own.remove(&k);
            } else {
                own.insert(k);
                // a crossed book drops the other side's levels it crosses, as the book does
                other.retain(|o| if is_bid { *o > k } else { *o < k });
            }
            if let (Some(bb), Some(ba)) = (bids.last(), asks.first()) {
                mid = (bb + ba) / 2;
            }
            let l = Level { price: price(k), amount: amt };
            if is_bid {
                b.push(l)
            } else {
                a.push(l)
            }
        }
        out.push(Msg { ts, local: ts + 3_000, snapshot: false, bids: b, asks: a });
    }
    out
}

/// The two generated feeds of 20,000 messages: a deep book on a fine tick, and a shallow one on
/// a coarse tick whose deletes and crosses empty its top more often.
pub fn feeds() -> [Vec<Msg>; 2] {
    [feed(7, 20_000, 400, 100_000.0), feed(11, 20_000, 30, 100.0)]
}

/// `msgs` through `Book` and the naive model, message for message: the same "row emitted?"
/// answer and the same top 25. The rows emitted.
fn parity(msgs: &[Msg]) -> usize {
    let (mut book, mut naive, mut rows) = (Book::new(25), Naive::default(), 0);
    for (i, m) in msgs.iter().enumerate() {
        let got = book.apply(m.snapshot, &m.bids, &m.asks);
        assert_eq!(got, naive.apply(m.snapshot, &m.bids, &m.asks), "message {i}: row emitted?");
        assert_eq!(book.top_bids, naive.last.0, "message {i} bids");
        assert_eq!(book.top_asks, naive.last.1, "message {i} asks");
        rows += got as usize;
    }
    rows
}

#[test]
fn generated_feeds_build_the_naive_models_top_25() {
    for msgs in feeds() {
        let rows = parity(&msgs);
        // most messages change the top; some only below it, or nothing
        assert!(rows > msgs.len() / 3 && rows < msgs.len(), "{rows} rows of {} messages", msgs.len());
    }
}

/// The obviously-correct model: maps sorted on every read.
#[derive(Default)]
pub struct Naive {
    bids: BTreeMap<u64, f64>,
    asks: BTreeMap<u64, f64>,
    init: bool,
    /// The top 25 bids and asks after the last message.
    pub last: (Vec<Level>, Vec<Level>),
}

fn key(p: f64) -> u64 {
    p.to_bits()
}

impl Naive {
    fn top(m: &BTreeMap<u64, f64>, bids: bool) -> Vec<Level> {
        let mut v: Vec<Level> = m.iter().map(|(k, a)| Level { price: f64::from_bits(*k), amount: *a }).collect();
        v.sort_by(|a, b| if bids { b.price.total_cmp(&a.price) } else { a.price.total_cmp(&b.price) });
        v.truncate(25);
        v
    }

    /// Applies a message; true iff the top 25 changed.
    pub fn apply(&mut self, snap: bool, bids: &[Level], asks: &[Level]) -> bool {
        if snap {
            (self.bids, self.asks, self.init) = (BTreeMap::new(), BTreeMap::new(), true);
        }
        if !self.init {
            return false;
        }
        for (m, ls) in [(&mut self.asks, asks), (&mut self.bids, bids)] {
            for l in ls {
                if l.amount == 0.0 {
                    m.remove(&key(l.price));
                } else {
                    m.insert(key(l.price), l.amount);
                }
            }
        }
        let best = |m: &BTreeMap<u64, f64>, bids: bool| Naive::top(m, bids).first().map(|l| l.price);
        if let (Some(bb), Some(ba)) = (best(&self.bids, true), best(&self.asks, false)) {
            if bb >= ba {
                let remove_bid = asks.iter().any(|l| l.price == ba);
                while let (Some(bb), Some(ba)) = (best(&self.bids, true), best(&self.asks, false)) {
                    if bb < ba {
                        break;
                    }
                    if remove_bid {
                        self.bids.remove(&key(bb));
                    } else {
                        self.asks.remove(&key(ba));
                    }
                }
            }
        }
        let now = (Naive::top(&self.bids, true), Naive::top(&self.asks, false));
        let changed = now != self.last;
        self.last = now;
        changed
    }
}

fn levels(mid: i64, bids: bool) -> impl Strategy<Value = Vec<Level>> {
    proptest::collection::vec((0i64..60, 0u8..5, 0u8..20), 0..10).prop_map(move |v| {
        v.into_iter()
            .map(|(off, amt, cross)| {
                // mostly on the right side of mid (deep books), sometimes crossing
                let off = if cross == 0 { -(off % 6) } else { off };
                let p = if bids { mid - off } else { mid + off };
                Level { price: p as f64 / 2.0, amount: amt as f64 }
            })
            .collect()
    })
}

fn snapshot(mid: i64) -> (Vec<Level>, Vec<Level>) {
    let bids = (1..=60).map(|j| Level { price: (mid - j) as f64 / 2.0, amount: (1 + j % 7) as f64 }).collect();
    let asks = (1..=60).map(|j| Level { price: (mid + j) as f64 / 2.0, amount: (1 + j % 5) as f64 }).collect();
    (bids, asks)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    /// Every message: same "row emitted?" answer and same top-25 as the naive model,
    /// including crossed books, deletes, deep changes (fast path) and re-snapshots.
    #[test]
    fn matches_naive_model(steps in proptest::collection::vec((levels(400, true), levels(400, false), 0u8..40, -2i64..3), 1..400)) {
        let (mut book, mut naive, mut mid) = (Book::new(25), Naive::default(), 400i64);
        for (i, (b, a, snap, drift)) in steps.into_iter().enumerate() {
            mid += drift;
            let shift = |ls: Vec<Level>| ls.into_iter().map(|l| Level { price: l.price + (mid - 400) as f64 / 2.0, amount: l.amount }).collect::<Vec<_>>();
            let (b, a, snap) = if i == 0 || snap == 0 { let (b, a) = snapshot(mid); (b, a, true) } else { (shift(b), shift(a), false) };
            let got = book.apply(snap, &b, &a);
            let want = naive.apply(snap, &b, &a);
            prop_assert_eq!(got, want, "message {}", i);
            prop_assert_eq!(&book.top_bids, &naive.last.0, "bids at {}", i);
            prop_assert_eq!(&book.top_asks, &naive.last.1, "asks at {}", i);
        }
    }
}

fn l(price: f64, amount: f64) -> Level {
    Level { price, amount }
}

#[test]
fn updates_before_the_first_snapshot_are_ignored() {
    let mut b = Book::new(25);
    assert!(!b.apply(false, &[l(100.0, 1.0)], &[l(101.0, 1.0)]), "no book is known before a snapshot");
    assert!(b.apply(true, &[l(99.0, 1.0)], &[l(102.0, 1.0)]));
    assert_eq!(b.top_bids, vec![l(99.0, 1.0)]);
}

#[test]
fn crossed_book_removes_the_stale_side() {
    let mut b = Book::new(25);
    b.apply(true, &[l(100.5, 1.0), l(99.0, 1.0)], &[l(101.0, 1.0)]);
    b.apply(false, &[], &[l(100.0, 2.0)]); // new ask crosses the resting bid: the bid is stale
    assert_eq!((b.top_bids[0].price, b.top_asks[0].price), (99.0, 100.0));
    b.apply(false, &[l(101.5, 3.0)], &[]); // new bid crosses the asks: asks go
    assert!(b.top_asks.is_empty());
}

#[test]
fn identical_resnapshot_emits_nothing() {
    // a producer reconnect that reproduces the same top-N must not add a tick to every average
    let mut b = Book::new(25);
    b.apply(true, &[l(99.0, 1.0)], &[l(101.0, 1.0)]);
    assert!(!b.apply(true, &[l(99.0, 1.0)], &[l(101.0, 1.0)]));
}

#[test]
fn change_below_a_full_top_is_not_a_row_but_the_boundary_is() {
    let mut b = Book::new(2);
    b.apply(true, &[l(99.0, 1.0), l(98.0, 1.0), l(97.0, 1.0)], &[l(101.0, 1.0), l(102.0, 1.0), l(103.0, 1.0)]);
    assert!(!b.apply(false, &[l(96.0, 5.0)], &[l(104.0, 5.0)]), "strictly below the top-2");
    assert!(b.apply(false, &[l(98.0, 7.0)], &[]), "at the boundary level: the top changed");
    assert!(b.apply(false, &[l(99.0, 0.0)], &[]), "deleting a top level pulls a deeper one up");
    assert_eq!(b.top_bids, vec![l(98.0, 7.0), l(97.0, 1.0)]);
    assert!(!b.apply(false, &[l(50.0, 0.0)], &[]), "deleting an absent price is a no-op");
}

#[test]
fn checkpoint_restore_mid_stream_is_invisible() {
    let [_, msgs] = feeds();
    let run = |cut: Option<usize>| {
        let mut b = Book::new(25);
        let mut out = vec![];
        for (i, m) in msgs.iter().enumerate() {
            if Some(i) == cut {
                let bytes = serde_json::to_vec(&b).unwrap();
                b = serde_json::from_slice(&bytes).unwrap();
            }
            if b.apply(m.snapshot, &m.bids, &m.asks) {
                out.push((b.top_bids.clone(), b.top_asks.clone()));
            }
        }
        out
    };
    let want = run(None);
    for cut in [1, 777, 10_000, 19_998] {
        assert!(run(Some(cut)) == want, "restore at message {cut} changed the rows");
    }
}

// --- replica guard -------------------------------------------------------------------------

#[test]
fn guard_rejects_a_stale_replica_snapshot() {
    let mut g = Guard::new(false);
    assert!(g.accept(true, 1000, 100));
    for seq in 101..=105 {
        assert!(g.accept(false, 1000 + seq, seq));
    }
    assert!(!g.accept(true, 1003, 103), "an older snapshot would roll the book back");
    assert!(g.accept(true, 1200, 180), "a newer snapshot (real resync) is applied");
}

#[test]
fn guard_drops_replayed_diffs() {
    let mut g = Guard::new(false);
    g.accept(true, 1000, 100);
    assert!(g.accept(false, 1001, 101));
    assert!(!g.accept(false, 1001, 101), "a diff must not be applied twice");
}

#[test]
fn guard_accepts_a_sequence_reset_with_newer_time() {
    let mut g = Guard::new(true);
    g.accept(true, 1000, 5_000_000);
    g.accept(false, 2000, 5_000_010);
    assert!(g.accept(true, 9000, 12), "a snapshot after maintenance, its sequence started over");
    assert!(g.accept(false, 9001, 13));
    assert!(!g.accept(false, 9001, 13), "a replayed diff after the reset is still dropped");
    assert!(!g.accept(true, 2000, 5_000_010), "a pre-reset snapshot (older time) is stale");
}

#[test]
fn guard_with_seq_reset_orders_same_millisecond_updates_by_seq() {
    let mut g = Guard::new(true);
    g.accept(true, 1000, 10);
    assert!(g.accept(false, 1000, 11), "same ms, newer seq");
    assert!(!g.accept(false, 1000, 11), "same ms, same seq: replay");
    assert!(!g.accept(false, 1000, 9), "same ms, older seq");
    assert!(g.accept(true, 1000, 11), "a snapshot at the current position is accepted");
}

#[test]
fn guard_by_seq_rejects_a_stale_rest_snapshot_with_a_later_timestamp() {
    let mut g = Guard::new(false);
    g.accept(true, 1000, 100);
    g.accept(false, 1010, 110);
    assert!(!g.accept(true, 1020, 105), "REST snapshot time is the response time; seq alone decides");
}

#[test]
fn guard_without_sequence_orders_by_time() {
    let mut g = Guard::new(false);
    g.accept(true, 1000, 0);
    assert!(g.accept(true, 2000, 0));
    assert!(!g.accept(true, 1500, 0), "a lagging replica's older snapshot");
    assert!(g.accept(true, 2500, 0));
}

#[test]
fn guard_passes_diffs_before_the_first_snapshot() {
    // the book ignores them anyway; the guard must not seed on a diff
    let mut g = Guard::new(false);
    assert!(g.accept(false, 5, 5));
    assert!(g.accept(true, 1, 1), "the first snapshot still seeds even with a lower seq");
}

fn lv(price: f64, amount: f64) -> Level {
    Level { price, amount }
}

/// A NaN price never equals or orders against another: before this was rejected, each such
/// level was inserted undeletably at the worst end of its side (quadratic work as they piled
/// up), reached a shallow book's top as `null`, and could hide a crossed book.
#[test]
fn non_finite_levels_are_skipped_and_counted() {
    let mut b = Book::new(25);
    assert!(b.apply(true, &[lv(100.0, 1.0), lv(99.0, 1.0)], &[lv(101.0, 1.0)]));
    let bad = [
        lv(f64::NAN, 1.0),
        lv(f64::INFINITY, 1.0),
        lv(98.0, f64::NAN),
        lv(f64::NEG_INFINITY, 2.0),
        lv(99.0, f64::INFINITY),
    ];
    for _ in 0..1_000 {
        assert!(!b.apply(false, &bad, &bad), "a non-finite level changed the book");
    }
    assert_eq!(b.rejected, 10_000);
    assert_eq!((b.top_bids.clone(), b.top_asks.clone()), (vec![lv(100.0, 1.0), lv(99.0, 1.0)], vec![lv(101.0, 1.0)]));
    // the book still updates, deletes and uncrosses
    assert!(b.apply(false, &[lv(99.0, 0.0)], &[]));
    assert_eq!(b.top_bids, [lv(100.0, 1.0)]);
    assert!(b.apply(false, &[lv(102.0, 1.0)], &[]));
    assert_eq!((b.top_bids.clone(), b.top_asks.clone()), (vec![lv(102.0, 1.0), lv(100.0, 1.0)], vec![]));
}

#[test]
fn a_book_of_depth_zero_takes_changes_and_emits_nothing() {
    let mut b = Book::new(0);
    assert!(!b.apply(true, &[lv(100.0, 1.0)], &[lv(101.0, 1.0)]));
    assert!(!b.apply(false, &[lv(100.5, 1.0)], &[]));
}

/// A message whose key lacks the sequence (0) used to count as older than every sequence, and
/// a key that is a bare timestamp read as a huge one: either froze the book. Time now orders
/// such a message, and the sequence ordering resumes where it was.
#[test]
fn guard_orders_a_message_without_seq_by_time_and_the_seq_resumes() {
    let mut g = Guard::new(false);
    g.accept(true, 1000, 100);
    assert!(g.accept(false, 1001, 101));
    assert!(g.accept(false, 1002, 0), "a newer message without seq is applied");
    assert!(!g.accept(false, 999, 0), "an older one is not");
    assert!(g.accept(false, 1003, 102), "the sequence resumes after 101");
    assert!(!g.accept(false, 1004, 101), "and still drops a replayed diff");
}
