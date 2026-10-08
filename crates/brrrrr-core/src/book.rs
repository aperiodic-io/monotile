//! L2 order book that turns venue messages (snapshots and level changes) into top-N snapshots,
//! the replica guard for book topics written by more than one producer, and the operator that
//! runs them in SQL: `orderbook_top_n` (ADR-0011).
use crate::engine::{key_text, Row};
use crate::value::Value;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Level {
    pub price: f64,
    pub amount: f64,
}

fn better(bids: bool, a: f64, b: f64) -> bool {
    if bids {
        a > b
    } else {
        a < b
    }
}

/// One side, sorted worst first so the busy top of the book sits at the Vec's tail.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Side {
    bids: bool,
    levels: Vec<Level>,
}

impl Side {
    /// Sets the amount at `price` (0 deletes). False if the level is refused: a price or amount
    /// that is not a finite number (NaN never equals itself or orders, so such a level could not
    /// be deleted, would sit out of order and could keep a crossed book from ever uncrossing).
    /// Only storing a value is checked: a delete stores nothing.
    fn set(&mut self, price: f64, amount: f64) -> bool {
        // one search per side, so the comparison does not test the side at every step
        let i = if self.bids {
            self.levels.partition_point(|l| price > l.price)
        } else {
            self.levels.partition_point(|l| price < l.price)
        };
        let found = i < self.levels.len() && self.levels[i].price == price;
        match (amount == 0.0, found) {
            (true, true) => {
                self.levels.remove(i);
            }
            (true, false) => {}
            (false, true) if amount.is_finite() => self.levels[i].amount = amount,
            (false, false) if price.is_finite() && amount.is_finite() => self.levels.insert(i, Level { price, amount }),
            (false, _) => return false,
        }
        true
    }

    fn best(&self) -> Option<Level> {
        self.levels.last().copied()
    }

    fn top(&self, n: usize, out: &mut Vec<Level>) {
        out.clear();
        out.extend(self.levels.iter().rev().take(n));
    }

    /// Whether `set` could have built this side: strictly ordered worst first, every price
    /// finite and every amount finite and not 0.
    fn check(&self, bids: bool) -> Result<(), String> {
        let ordered = self.levels.windows(2).all(|p| better(bids, p[1].price, p[0].price));
        let valid = self.levels.iter().all(|l| l.price.is_finite() && l.amount.is_finite() && l.amount != 0.0);
        if self.bids != bids || !ordered || !valid {
            return Err(format!("{} that are not in order or not levels", if bids { "bids" } else { "asks" }));
        }
        Ok(())
    }
}

/// The book and its last emitted top-N. Rules:
/// - a snapshot clears the book; updates before the first snapshot are ignored;
/// - asks are applied before bids; amount 0 deletes (absent price: no-op);
/// - crossed levels are removed: the best bid if the change touched the best ask price,
///   otherwise the best ask, until uncrossed;
/// - a row is emitted iff any of the 2N (price, amount) slots changed; the previous row
///   survives snapshots.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Book {
    depth: usize,
    bids: Side,
    asks: Side,
    initialized: bool,
    pub top_bids: Vec<Level>,
    pub top_asks: Vec<Level>,
    /// Levels skipped because their price or amount is not a finite number.
    #[serde(default)]
    pub rejected: u64,
    #[serde(skip)]
    scratch: Vec<Level>,
}

impl Book {
    pub fn new(depth: usize) -> Book {
        Book {
            depth,
            bids: Side { bids: true, levels: vec![] },
            asks: Side::default(),
            initialized: false,
            top_bids: vec![],
            top_asks: vec![],
            rejected: 0,
            scratch: vec![],
        }
    }

    /// Whether a snapshot has been applied: until then, messages build nothing.
    pub fn initialized(&self) -> bool {
        self.initialized
    }

    /// Whether `apply` could have built this book at `depth` (a restored checkpoint's): each
    /// side in order, and the top-N the sides' own.
    pub fn check(&self, depth: usize) -> Result<(), String> {
        if self.depth != depth {
            return Err(format!("a book of depth {} where the plan keeps {depth}", self.depth));
        }
        self.bids.check(true)?;
        self.asks.check(false)?;
        let mut top = vec![];
        for (side, kept) in [(&self.bids, &self.top_bids), (&self.asks, &self.top_asks)] {
            side.top(depth, &mut top);
            if top != *kept {
                return Err("a top-N that is not the book's".into());
            }
        }
        Ok(())
    }

    /// Applies one venue message; true iff the top-N changed (a top-N snapshot row).
    pub fn apply(&mut self, snapshot: bool, bids: &[Level], asks: &[Level]) -> bool {
        let mut dirty = snapshot;
        if snapshot {
            self.bids.levels.clear();
            self.asks.levels.clear();
            self.initialized = true;
        }
        if !self.initialized || self.depth == 0 {
            return false;
        }
        let mut rejected = 0;
        for (side, top, changes, is_bid) in
            [(&mut self.asks, &self.top_asks, asks, false), (&mut self.bids, &self.top_bids, bids, true)]
        {
            for l in changes {
                // A change strictly below a full top-N cannot alter it: skip the rebuild.
                dirty |= top.len() != self.depth || !better(is_bid, top[top.len() - 1].price, l.price);
                rejected += u64::from(!side.set(l.price, l.amount));
            }
        }
        self.rejected += rejected;
        if let (Some(bb), Some(ba)) = (self.bids.best(), self.asks.best()) {
            if bb.price >= ba.price {
                dirty = true;
                let remove_bid = asks.iter().any(|l| l.price == ba.price);
                while let (Some(bb), Some(ba)) = (self.bids.best(), self.asks.best()) {
                    if bb.price < ba.price {
                        break;
                    }
                    let (side, p) = if remove_bid { (&mut self.bids, bb.price) } else { (&mut self.asks, ba.price) };
                    side.set(p, 0.0);
                }
            }
        }
        if !dirty {
            return false;
        }
        let mut changed = false;
        for (side, top) in [(&self.bids, &mut self.top_bids), (&self.asks, &mut self.top_asks)] {
            side.top(self.depth, &mut self.scratch);
            if self.scratch != *top {
                std::mem::swap(&mut self.scratch, top);
                changed = true;
            }
        }
        changed
    }
}

/// Drops messages that are not newer than the book: a producer replica's late snapshot (its own
/// dedup key, so broker dedup admits it) or a replayed diff, by the message's venue sequence.
/// Venues whose sequence can reset (after maintenance, `allow_seq_reset`) are ordered by exchange
/// time, the sequence breaking ties within one timestamp; others by sequence alone (a REST
/// snapshot carries the response time). A message without seq (0: a seq-less venue, or
/// a key missing it) is ordered by time, and the seq ordering resumes after it.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Guard {
    pub allow_seq_reset: bool,
    last_seq: i64,
    last_time: i64,
    seeded: bool,
}

impl Guard {
    pub fn new(allow_seq_reset: bool) -> Guard {
        Guard { allow_seq_reset, ..Guard::default() }
    }

    /// Where the message at `a` (venue time, seq) stands against the one at `b`, in the order
    /// `accept` keeps: by time where either has no seq, by time then seq where the seq can
    /// reset, by seq otherwise. Two messages at one time of which one has no seq are `Equal`
    /// without being ordered: `accept` takes the later of them in either order (`by_time`).
    pub fn order(&self, a: (i64, i64), b: (i64, i64)) -> Ordering {
        if by_time(a, b) {
            a.0.cmp(&b.0)
        } else if self.allow_seq_reset {
            a.cmp(&b)
        } else {
            a.1.cmp(&b.1)
        }
    }

    pub fn accept(&mut self, snapshot: bool, time: i64, seq: i64) -> bool {
        if !self.seeded {
            if snapshot {
                (self.seeded, self.last_seq, self.last_time) = (true, seq, time);
            }
            return true;
        }
        // A snapshot may equal the current position; a diff must be strictly newer.
        let seq_stale = if snapshot { seq < self.last_seq } else { seq <= self.last_seq };
        let stale = if seq == 0 {
            time < self.last_time
        } else if self.allow_seq_reset {
            // time decides; seq only orders messages within the same exchange timestamp
            time < self.last_time || (time == self.last_time && seq_stale)
        } else {
            seq_stale
        };
        if !stale {
            if seq != 0 {
                self.last_seq = seq;
            }
            self.last_time = time;
        }
        !stale
    }
}

/// Whether two messages are ordered by time alone: one of them has no seq.
fn by_time(a: (i64, i64), b: (i64, i64)) -> bool {
    a.1 == 0 || b.1 == 0
}

/// The diffs a book keeps: at most this many, and none more than `KEEP_DIFFS_US` of venue time
/// older than the newest (see `Kept`). Two replicas of a feed run milliseconds apart; a
/// replica's snapshot further behind than this is dropped, as before there were kept diffs.
pub const KEEP_DIFFS: usize = 256;
pub const KEEP_DIFFS_US: i64 = 2_000_000;

/// A diff a book applied: its position (venue time, seq) and levels.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Applied {
    pub time: i64,
    pub seq: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// The diffs a book applied since `from`, in the guard's order: `from` is its last snapshot's
/// position, or the newest dropped diff's once the bounds (`KEEP_DIFFS`, `KEEP_DIFFS_US`) drop
/// the oldest. Two producer replicas publish into one topic, so a replica's snapshot can land
/// after the other replica's diffs past its version, which the guard finds stale. Applied as
/// is it would rewind the book; dropped, the producers' periodic snapshots could never correct
/// a book that missed a diff. Brought up to date with the kept diffs from its position on
/// (`covers`, `from_on`), it is the book now, corrected.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Kept {
    pub from: (i64, i64),
    pub diffs: VecDeque<Applied>,
}

impl Kept {
    /// A snapshot at `at` was applied: nothing before it is needed any more.
    pub fn reset(&mut self, at: (i64, i64)) {
        (self.from, self.diffs) = (at, VecDeque::new());
    }

    pub fn push(&mut self, d: Applied) {
        let newest = d.time;
        self.diffs.push_back(d);
        while self.diffs.len() > KEEP_DIFFS
            || self.diffs.front().is_some_and(|o| newest.saturating_sub(o.time) > KEEP_DIFFS_US)
        {
            let old = self.diffs.pop_front().expect("not empty");
            self.from = (old.time, old.seq);
        }
    }

    /// Whether every diff the book applied after `at` is kept: `at` is after `from`, or at it
    /// where the order is certain (a message at `from`'s time without a seq may have come
    /// after a snapshot at that time, and may be a dropped diff).
    pub fn covers(&self, at: (i64, i64), guard: &Guard) -> bool {
        match guard.order(at, self.from) {
            Ordering::Greater => true,
            Ordering::Equal => !by_time(at, self.from),
            Ordering::Less => false,
        }
    }

    /// The kept diffs from `at` on, in order. A diff at `at` itself is in the snapshot there
    /// already, or (without a seq) may have come after it: applying it again sets levels to
    /// the amounts they have, so both are right.
    pub fn from_on<'a>(&'a self, at: (i64, i64), guard: &'a Guard) -> impl Iterator<Item = &'a Applied> {
        self.diffs.iter().filter(move |d| guard.order((d.time, d.seq), at) != Ordering::Less)
    }

    /// Whether a restored `Kept` is one `push` and `reset` could have made: each diff after the
    /// one before it (or `from`), or at its time where either has no seq.
    pub fn check(&self, guard: &Guard) -> Result<(), String> {
        let mut at = self.from;
        for d in &self.diffs {
            let ordered = match guard.order((d.time, d.seq), at) {
                Ordering::Greater => true,
                Ordering::Equal => by_time((d.time, d.seq), at),
                Ordering::Less => false,
            };
            if !ordered {
                return Err("kept diffs that are not in order after the position they are kept from".into());
            }
            at = (d.time, d.seq);
        }
        if self.diffs.len() > KEEP_DIFFS {
            return Err(format!("{} kept diffs, at most {KEEP_DIFFS}", self.diffs.len()));
        }
        Ok(())
    }
}

/// The columns `orderbook_top_n` reads, as fixtures/market.proto's BookUpdate names them.
/// `venue_sequence` is the venue's book version (an update id), 0 where it has none.
const COLUMNS: [&str; 9] = [
    "exchange",
    "symbol",
    "is_snapshot",
    "time",
    "venue_sequence",
    "bid_price",
    "bid_amount",
    "ask_price",
    "ask_amount",
];

/// What the books hold and have dropped, summed over every `orderbook_top_n` (`Engine::books`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BookStats {
    /// Books kept: one per (exchange, symbol) seen.
    pub books: usize,
    /// Books that have not had a snapshot yet, so build nothing.
    pub awaiting_snapshot: usize,
    /// Messages the replica guard dropped as not newer than their book (and not a snapshot the
    /// book's kept diffs could bring up to date).
    pub stale: u64,
    /// Late snapshots brought up to date with the kept diffs (`Kept`) and applied.
    pub caught_up: u64,
    /// Messages dropped for price and amount arrays of different lengths.
    pub malformed: u64,
}

/// `orderbook_top_n(stream, depth[, allow_seq_reset])`: per (exchange, symbol), the book the
/// stream's venue messages build, and for each message that changes its top `depth` levels the
/// message's row with those levels (best first) in place of its own: a top-`depth` book
/// snapshot. The stream has BookUpdate's columns (`COLUMNS`). The
/// replica guard drops a message not newer than its book, by its `venue_sequence`, or by time
/// where that is 0; `allow_seq_reset` is `Guard`'s.
#[derive(Clone, Debug)]
pub(crate) struct TopN {
    pub depth: usize,
    pub allow_seq_reset: bool,
    /// Positions of `COLUMNS` in the stream's rows.
    cols: [usize; 9],
    /// By key text of (exchange, symbol).
    pub books: FxHashMap<String, (Book, Guard, Kept)>,
    /// Runtime counts (`BookStats`), not checkpointed.
    pub stale: u64,
    pub caught_up: u64,
    pub malformed: u64,
    /// Scratch: the current message's key text and levels.
    text: String,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

impl TopN {
    pub fn new(columns: &[&str], depth: usize, allow_seq_reset: bool) -> Result<TopN, String> {
        let mut cols = [0; 9];
        for (c, name) in cols.iter_mut().zip(COLUMNS) {
            *c = columns.iter().position(|n| *n == name).ok_or(format!("orderbook_top_n needs a column {name}"))?;
        }
        Ok(TopN {
            depth,
            allow_seq_reset,
            cols,
            books: FxHashMap::default(),
            stale: 0,
            caught_up: 0,
            malformed: 0,
            text: String::new(),
            bids: vec![],
            asks: vec![],
        })
    }

    pub fn apply(&mut self, rows: &[Row]) -> Vec<Row> {
        let [exchange, symbol, snapshot, time, seq, bid_price, bid_amount, ask_price, ask_amount] = self.cols;
        let mut out = vec![];
        for r in rows {
            if !levels(&mut self.bids, &r[bid_price], &r[bid_amount])
                || !levels(&mut self.asks, &r[ask_price], &r[ask_amount])
            {
                self.malformed += 1;
                continue;
            }
            key_text(&mut self.text, [&r[exchange], &r[symbol]]);
            let (book, guard, kept) = match self.books.get_mut(self.text.as_str()) {
                Some(b) => b,
                None => self.books.entry(self.text.clone()).or_insert((
                    Book::new(self.depth),
                    Guard::new(self.allow_seq_reset),
                    Kept::default(),
                )),
            };
            let snapshot = r[snapshot].i64().is_some_and(|v| v != 0);
            let at = (r[time].i64().unwrap_or(0), r[seq].i64().unwrap_or(0));
            let changed = if guard.accept(snapshot, at.0, at.1) {
                let changed = book.apply(snapshot, &self.bids, &self.asks);
                if snapshot {
                    kept.reset(at);
                } else if book.initialized() {
                    let (bids, asks) = (self.bids.clone(), self.asks.clone());
                    kept.push(Applied { time: at.0, seq: at.1, bids, asks });
                }
                changed
            } else if snapshot && kept.covers(at, guard) {
                // a late snapshot: the book at its version, brought up to now
                let (bids, asks) = (book.top_bids.clone(), book.top_asks.clone());
                book.apply(true, &self.bids, &self.asks);
                for d in kept.from_on(at, guard) {
                    book.apply(false, &d.bids, &d.asks);
                }
                self.caught_up += 1;
                book.top_bids != bids || book.top_asks != asks
            } else {
                self.stale += 1;
                false
            };
            if changed {
                let mut row = r.clone();
                row[bid_price] = column(&book.top_bids, |l| l.price);
                row[bid_amount] = column(&book.top_bids, |l| l.amount);
                row[ask_price] = column(&book.top_asks, |l| l.price);
                row[ask_amount] = column(&book.top_asks, |l| l.amount);
                out.push(row);
            }
        }
        out
    }

    /// The books in key order: the checkpoint format.
    pub fn snapshot(&self) -> Vec<(&str, &Book, &Guard, &Kept)> {
        let mut v: Vec<_> = self.books.iter().map(|(k, (b, g, kept))| (k.as_str(), b, g, kept)).collect();
        v.sort_unstable_by(|a, b| a.0.cmp(b.0));
        v
    }

    /// Whether `books` are ones this operator could have kept: in strict key order, each built
    /// at its depth under its seq rule.
    pub fn check(&self, books: &[(String, Book, Guard, Kept)]) -> Result<(), String> {
        if !books.windows(2).all(|p| p[0].0 < p[1].0) {
            return Err("books that are not in key order, or a key twice".into());
        }
        for (key, book, guard, kept) in books {
            if guard.allow_seq_reset != self.allow_seq_reset {
                return Err(format!("the book of {key:?} has another seq reset rule than the plan"));
            }
            book.check(self.depth).map_err(|e| format!("the book of {key:?}: {e}"))?;
            kept.check(guard).map_err(|e| format!("the book of {key:?}: {e}"))?;
        }
        Ok(())
    }

    pub fn restore(&mut self, books: Vec<(String, Book, Guard, Kept)>) {
        self.books = books.into_iter().map(|(k, b, g, kept)| (k, (b, g, kept))).collect();
    }

    pub fn stats(&self) -> BookStats {
        BookStats {
            books: self.books.len(),
            awaiting_snapshot: self.books.values().filter(|(b, ..)| !b.initialized()).count(),
            stale: self.stale,
            caught_up: self.caught_up,
            malformed: self.malformed,
        }
    }
}

/// One side of a message as levels; false if its prices and amounts differ in length. A side
/// that is NULL (absent from the message) has no levels; a value that is not a number is NaN,
/// which the book refuses.
fn levels(out: &mut Vec<Level>, prices: &Value, amounts: &Value) -> bool {
    out.clear();
    fn side(v: &Value) -> Option<&[Value]> {
        match v {
            Value::Array(a) => Some(a),
            Value::Null => Some(&[]),
            _ => None,
        }
    }
    let (Some(p), Some(a)) = (side(prices), side(amounts)) else { return false };
    if p.len() != a.len() {
        return false;
    }
    let num = |v: &Value| v.f64().unwrap_or(f64::NAN);
    out.extend(p.iter().zip(a).map(|(p, a)| Level { price: num(p), amount: num(a) }));
    true
}

fn column(levels: &[Level], f: impl Fn(&Level) -> f64) -> Value {
    Value::Array(levels.iter().map(|l| Value::F64(f(l))).collect())
}
