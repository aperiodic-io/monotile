//! Aligned consumption (`--max-drift`, ADR-0006's `aligned` mode at the source). Proton keeps
//! one watermark per window, the latest time seen on any partition, so a partition that is
//! behind another has its rows dropped as late: hours behind after downtime, or the tens of ms
//! a fetch takes under load, which cost a window its last rows. Here the source
//! partitions are merged by record timestamp: a message goes to the engine only once no
//! partition may still deliver an older one. The engine's semantics are unchanged; it just sees
//! the partitions in time order.
//!
//! Each partition holds at most one message (its head); a partition nobody takes from is not
//! read ahead, so memory stays bounded without pausing anything.
//!
//! A partition without a head may still deliver a message older than a head of time `ts`, unless
//! - it has delivered one of `ts` or later (a partition's records are in time order), or
//! - it is at its end (librdkafka's partition EOF) and the clock is `wait` past `ts`: its
//!   records older than `ts` were produced and read by then. Its end alone says nothing of the
//!   records produced since it was reached and not fetched yet, or
//! - it has said nothing for `stale`: an empty or unreachable partition holds nobody back.
//!
//! So a record is overtaken by a newer one of another partition only if it is read more than
//! `wait` after that one's time, or after `stale` of silence. Times are Unix ms, the clock of
//! the record timestamps.
//!
//! Partitions are in groups (`Merge::grouped`): the ones whose streams reach the same views. A head
//! waits only for the partitions of its own group, so a quiet partition of one group (a slow exchange's
//! quotes) does not hold back the rows of another that no view orders against it.

use std::collections::BTreeMap;

/// One source message as the data loop needs it.
#[derive(Clone, Debug, PartialEq)]
pub struct Msg {
    pub offset: i64,
    /// Record timestamp, ms.
    pub ts: i64,
    pub payload: Vec<u8>,
}

/// What a partition's queue gives next.
#[derive(Debug)]
pub enum Polled {
    Msg(Msg),
    /// The partition's end: every record it held when it was fetched last has been given.
    Eof,
    /// An Iggy poll confirmed this end at this Unix timestamp (ms), not when the queue is drained.
    EofAt(i64),
    Empty,
    /// The source can no longer confirm an earlier end (e.g. a disconnected Iggy transport).
    Unavailable,
}

#[derive(Debug)]
struct Part {
    /// The set of partitions this one is ordered against.
    group: usize,
    head: Option<Msg>,
    /// The newest timestamp this partition delivered: its next one is not older.
    last: Option<i64>,
    eof: bool,
    /// Iggy broker observation time; Kafka EOF retains its existing queue/clock behavior.
    eof_at: Option<i64>,
    heard_at: i64,
    /// When this partition's queue was last read (`fill`): a clock close's evidence that nothing is waiting in it.
    polled_at: i64,
}

#[derive(Debug)]
pub struct Merge {
    wait: i64,
    stale: i64,
    parts: Vec<Part>,
}

impl Merge {
    #[cfg(test)]
    pub fn new(parts: usize, wait_ms: i64, stale_ms: i64, now: i64) -> Merge {
        Merge::grouped(&vec![0; parts], wait_ms, stale_ms, now)
    }

    /// One partition per entry of `groups`: its group. Only partitions of one group are ordered against each other.
    pub fn grouped(groups: &[usize], wait_ms: i64, stale_ms: i64, now: i64) -> Merge {
        let part = |&group| Part {
            group,
            head: None,
            last: None,
            eof: false,
            eof_at: None,
            heard_at: now,
            polled_at: i64::MIN,
        };
        Merge { wait: wait_ms, stale: stale_ms, parts: groups.iter().map(part).collect() }
    }

    /// Reads partition `i`'s queue up to its next message, if it has no head, and says whether
    /// it got one. An end with a message behind it is no end: the queue was just not read since.
    pub fn fill(&mut self, i: usize, now: i64, mut poll: impl FnMut() -> Polled) -> bool {
        let p = &mut self.parts[i];
        while p.head.is_none() {
            let polled = poll();
            p.polled_at = now;
            match polled {
                Polled::Msg(m) => {
                    p.last = Some(p.last.map_or(m.ts, |l| l.max(m.ts)));
                    (p.head, p.eof, p.heard_at) = (Some(m), false, now);
                    return true;
                }
                Polled::Eof => {
                    (p.eof, p.heard_at, p.eof_at) = (true, now, None);
                }
                Polled::EofAt(at) => {
                    (p.eof, p.heard_at, p.polled_at, p.eof_at) = (true, at, at, Some(at));
                    break;
                }
                Polled::Empty => break,
                Polled::Unavailable => {
                    p.eof = false;
                    break;
                }
            }
        }
        false
    }

    /// The time (Unix ms) up to which every record has reached the engine, as far as the
    /// partitions can tell without a later record: what `engine.close_until` may close to, with
    /// no row to close it (`--close-after-ms`). The oldest of what each partition vouches for:
    /// - one holding a head, to just before it;
    /// - one at its end, to the time its queue was last read less `margin` (what it was owed was
    ///   produced and read by then: the argument of the wait of `next`, with a longer margin),
    ///   and never less than it delivered. It is the time of that read and not the clock now:
    ///   a loop that stalled since (a long batch, a throttled CPU) has not read what arrived meanwhile;
    /// - any other partition, a backlog whose next fetch is on its way, or one that went silent
    ///   (unlike `next`, no silence vouches for it: a partition not at its end may hold anything), to what it
    ///   delivered, and nothing if it has delivered nothing.
    ///
    /// Never later than `now - margin`, whatever a record's timestamp says: one stamped in the future (a clock
    /// jump, a field in the wrong unit) must not close every window for good. `None` when some partition
    /// vouches for nothing.
    pub fn vouched_until(&self, now: i64, margin: i64) -> Option<i64> {
        let oldest = self
            .parts
            .iter()
            .map(|p| match (&p.head, p.eof) {
                (Some(h), _) => Some(h.ts.saturating_sub(1)),
                (None, true) => {
                    let read = p.eof_at.unwrap_or(p.polled_at).saturating_sub(margin);
                    Some(p.last.map_or(read, |l| l.max(read)))
                }
                (None, false) => p.last,
            })
            .collect::<Option<Vec<i64>>>()?
            .into_iter()
            .min()?;
        Some(oldest.min(now.saturating_sub(margin)))
    }

    /// The oldest head, once no partition of its group may still deliver something older: `(partition,
    /// message)`. Of the groups that have one to give, the one with the oldest head goes first.
    pub fn next(&mut self, now: i64) -> Option<(usize, Msg)> {
        let mut oldest: BTreeMap<usize, (usize, i64)> = BTreeMap::new();
        for (i, p) in self.parts.iter().enumerate() {
            if let Some(h) = &p.head {
                let e = oldest.entry(p.group).or_insert((i, h.ts));
                if h.ts < e.1 {
                    *e = (i, h.ts);
                }
            }
        }
        let free = |g: usize, ts: i64| {
            self.parts.iter().filter(|p| p.group == g).all(|p| {
                p.head.is_some()
                    || p.last.is_some_and(|l| l >= ts)
                    || now.saturating_sub(p.heard_at) >= self.stale
                    || p.eof && p.eof_at.unwrap_or(now) >= ts.saturating_add(self.wait)
            })
        };
        let (i, _) = oldest.into_iter().filter(|&(g, (_, ts))| free(g, ts)).map(|(_, x)| x).min_by_key(|x| x.1)?;
        Some((i, self.parts[i].head.take().expect("a head")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brrrrr_core::engine::{Engine, Row};
    use brrrrr_core::value::Value;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, VecDeque};

    const STALE: i64 = 30_000;

    fn msg(ts: i64) -> Msg {
        Msg { offset: ts, ts, payload: vec![] }
    }

    /// Partition `i`'s queue gives a message of time `ts`.
    fn offer(m: &mut Merge, i: usize, ts: i64, now: i64) {
        let mut q = Some(msg(ts));
        assert!(m.fill(i, now, || q.take().map_or(Polled::Empty, Polled::Msg)), "partition {i} had a head");
    }

    /// Partition `i`'s queue gives its end, and nothing after it.
    fn eof(m: &mut Merge, i: usize, now: i64) {
        let mut q = Some(Polled::Eof);
        assert!(!m.fill(i, now, || q.take().unwrap_or(Polled::Empty)));
    }

    fn drain(m: &mut Merge, now: i64) -> Vec<(usize, i64)> {
        std::iter::from_fn(|| m.next(now)).map(|(i, m)| (i, m.ts)).collect()
    }

    #[test]
    fn an_iggy_end_proof_cannot_release_a_newer_head_while_the_next_poll_is_stalled() {
        let mut m = Merge::new(2, 50, STALE, 0);
        m.fill(0, 2000, || Polled::EofAt(1000));
        offer(&mut m, 1, 1500, 2000);
        assert!(m.next(2000).is_none(), "partition zero may still owe a row between 1000 and 1500");
        m.fill(0, 2000, || Polled::EofAt(1549));
        assert!(m.next(2000).is_none(), "the producer delay margin is required");
        m.fill(0, 2000, || Polled::EofAt(1550));
        assert_eq!(m.next(2000).unwrap().0, 1);
    }

    #[test]
    fn a_disconnected_source_no_longer_vouches_for_the_clock_after_its_previous_end() {
        let mut m = Merge::new(1, 0, STALE, 0);
        offer(&mut m, 0, 100, 100);
        drain(&mut m, 100);
        eof(&mut m, 0, 1000);
        assert_eq!(m.vouched_until(1000, 10), Some(990));
        m.fill(0, 2000, || Polled::EofAt(1000));
        assert_eq!(m.vouched_until(2000, 10), Some(990), "draining an old end is not a new broker observation");
        m.fill(0, 2000, || Polled::Unavailable);
        assert_eq!(m.vouched_until(2000, 10), Some(100));
    }

    #[test]
    fn partitions_merge_in_timestamp_order() {
        let mut m = Merge::new(2, 0, STALE, 10);
        let mut queues =
            [[1, 3, 5], [2, 4, 6]].map(|q| q.map(|ts| Polled::Msg(msg(ts))).into_iter().chain([Polled::Eof]));
        let mut got = vec![];
        loop {
            for (i, q) in queues.iter_mut().enumerate() {
                m.fill(i, 10, || q.next().unwrap_or(Polled::Empty));
            }
            let Some((i, x)) = m.next(10) else { break };
            got.push((i, x.ts));
        }
        assert_eq!(got, [(0, 1), (1, 2), (0, 3), (1, 4), (0, 5), (1, 6)]);
    }

    /// A partition that is not at its end (a backlog whose next fetch is on its way) holds the
    /// others back to the newest time it delivered, however old their heads are; one never
    /// heard from holds everything.
    #[test]
    fn a_partition_that_may_deliver_older_holds_the_others_back() {
        let mut m = Merge::new(2, 50, STALE, 1_000);
        offer(&mut m, 0, 10, 1_000);
        assert_eq!(drain(&mut m, 1_000), [], "partition 1 was never heard from");
        offer(&mut m, 1, 5, 1_000);
        assert_eq!(drain(&mut m, 1_000), [(1, 5)]);
        assert_eq!(drain(&mut m, 1_000), [], "partition 1 may still deliver 6..10");
        assert_eq!(drain(&mut m, 20_000), [], "it is not at its end: no wait releases the others");
        offer(&mut m, 1, 10, 20_000);
        assert_eq!(drain(&mut m, 20_000), [(0, 10), (1, 10)], "equal times: the lower partition first");
        // a partition's records are in time order: after 10 it delivers nothing older than 10
        offer(&mut m, 0, 10, 20_000);
        assert_eq!(drain(&mut m, 20_000), [(0, 10)], "partition 1 delivered 10 already");
        offer(&mut m, 0, 11, 20_000);
        assert_eq!(drain(&mut m, 20_000), [], "partition 1 may still deliver 10..11");
    }

    /// A partition delivers nothing older than the newest time it delivered, also when its own
    /// records are a little out of order.
    #[test]
    fn a_partitions_newest_time_is_kept_when_an_older_record_follows() {
        let mut m = Merge::new(2, 50, STALE, 0);
        offer(&mut m, 0, 10, 0);
        offer(&mut m, 1, 10, 0);
        assert_eq!(drain(&mut m, 0), [(0, 10), (1, 10)]);
        offer(&mut m, 0, 8, 0);
        offer(&mut m, 1, 10, 0);
        assert_eq!(drain(&mut m, 0), [(0, 8), (1, 10)], "partition 0's newest is 10, not 8");
    }

    /// A partition at its end holds a head back until the clock is the wait past the head's
    /// time: a record older than the head, produced before it, is read by then.
    #[test]
    fn a_partition_at_its_end_is_waited_for_until_the_wait_past_the_heads_time() {
        let mut m = Merge::new(2, 50, STALE, 1_000);
        eof(&mut m, 1, 1_000);
        offer(&mut m, 0, 1_000, 1_010);
        assert_eq!(drain(&mut m, 1_010), []);
        assert_eq!(drain(&mut m, 1_049), []);
        assert_eq!(drain(&mut m, 1_050), [(0, 1_000)]);
        // a head already that old is not held at all: a replay, a record read late
        offer(&mut m, 0, 1_010, 1_060);
        assert_eq!(drain(&mut m, 1_060), [(0, 1_010)]);
        // the end may also come after the head
        let mut m = Merge::new(2, 50, STALE, 1_000);
        offer(&mut m, 0, 1_000, 1_010);
        assert_eq!(drain(&mut m, 1_100), [], "partition 1 was never heard from");
        eof(&mut m, 1, 1_100);
        assert_eq!(drain(&mut m, 1_100), [(0, 1_000)]);
    }

    /// A lagging partition: partition 1, at its end, delivers records older than partition 0's head
    /// after that head was read. Within the wait they go first, as do every partition's.
    #[test]
    fn records_a_partition_at_its_end_delivers_within_the_wait_go_first() {
        let mut m = Merge::new(3, 50, STALE, 14_000);
        eof(&mut m, 1, 14_000);
        eof(&mut m, 2, 14_000);
        offer(&mut m, 0, 15_060, 15_062); // past a window's end (15 000) and its 50 ms delay
        assert_eq!(drain(&mut m, 15_062), []);
        offer(&mut m, 1, 14_971, 15_090);
        offer(&mut m, 2, 14_999, 15_095);
        assert_eq!(drain(&mut m, 15_095), [(1, 14_971)]);
        assert_eq!(drain(&mut m, 15_109), [], "partition 1 delivered again: it is not at its end");
        offer(&mut m, 1, 15_000, 15_109);
        assert_eq!(drain(&mut m, 15_109), [(2, 14_999)]);
        eof(&mut m, 2, 15_109);
        assert_eq!(drain(&mut m, 15_109), [(1, 15_000)], "older than the wait, and the others at their ends");
        eof(&mut m, 1, 15_109);
        assert_eq!(drain(&mut m, 15_109), [], "partition 0's head is not the wait old yet");
        assert_eq!(drain(&mut m, 15_110), [(0, 15_060)]);
    }

    #[test]
    fn without_a_wait_a_partition_at_its_end_holds_nobody_back() {
        let mut m = Merge::new(2, 0, STALE, 1_000);
        eof(&mut m, 1, 1_000);
        offer(&mut m, 0, 1_010, 1_010);
        assert_eq!(drain(&mut m, 1_010), [(0, 1_010)]);
        offer(&mut m, 0, 1_011, 1_010);
        assert_eq!(drain(&mut m, 1_010), [], "a record from the future waits for its time");
    }

    /// A partition that delivers again after its end is not at its end: it holds the others
    /// back until it is there again.
    #[test]
    fn a_partition_delivering_again_after_its_end_holds_the_others_back_again() {
        let mut m = Merge::new(2, 50, STALE, 0);
        eof(&mut m, 1, 0);
        offer(&mut m, 0, 100, 200);
        assert_eq!(drain(&mut m, 200), [(0, 100)]);
        eof(&mut m, 1, STALE * 10); // partition 1 is read again and again, still at its end
        offer(&mut m, 1, 150, 210);
        assert_eq!(drain(&mut m, 210), [], "partition 0 is not at its end");
        eof(&mut m, 0, 211);
        assert_eq!(drain(&mut m, 211), [(1, 150)]);
        offer(&mut m, 0, 300, 305);
        assert_eq!(drain(&mut m, 5_000), [], "partition 1's end was before it delivered 150");
        eof(&mut m, 1, 5_000);
        assert_eq!(drain(&mut m, 5_000), [(0, 300)]);
    }

    /// A partition that says nothing (an empty one whose end was reported long ago, or one whose
    /// broker does not answer) holds nobody back once the stale period has passed, and no head
    /// waits for it: output is not delayed by a partition that is truly quiet.
    #[test]
    fn a_partition_silent_for_the_stale_period_holds_nobody_back() {
        let mut m = Merge::new(3, 50, STALE, 0);
        offer(&mut m, 0, 10, 0);
        eof(&mut m, 1, 0);
        assert_eq!(drain(&mut m, 29_999), [], "partition 2 is neither at its end nor silent that long");
        assert_eq!(drain(&mut m, 30_000), [(0, 10)]);
        offer(&mut m, 0, 30_000, 30_000);
        assert_eq!(drain(&mut m, 30_000), [(0, 30_000)], "no wait for silent partitions, at their end or not");
        // one that delivers again holds the others back again, and so does the one it overtook
        offer(&mut m, 1, 30_010, 30_010);
        assert_eq!(drain(&mut m, 30_010), [], "partition 0 delivered 10 ms ago and is not at its end");
        eof(&mut m, 0, 30_020);
        assert_eq!(drain(&mut m, 30_059), []);
        assert_eq!(drain(&mut m, 30_060), [(1, 30_010)]);
    }

    /// A partition's end is news of it: its silence counts from there.
    #[test]
    fn silence_counts_from_a_partitions_end() {
        let mut m = Merge::new(2, 100_000, STALE, 0);
        eof(&mut m, 1, 29_000);
        offer(&mut m, 0, 30_000, 30_000);
        assert_eq!(drain(&mut m, 58_999), []);
        assert_eq!(drain(&mut m, 59_000), [(0, 30_000)]);
    }

    /// One partition has nothing to be merged with: its messages go in its own order at once.
    #[test]
    fn a_single_partition_is_read_in_its_own_order_without_waiting() {
        let mut m = Merge::new(1, 50, STALE, 0);
        for ts in [1_000_000, 5, i64::MIN, i64::MAX] {
            offer(&mut m, 0, ts, 0);
            assert_eq!(drain(&mut m, 0), [(0, ts)]);
        }
    }

    /// A record without a timestamp (the oldest there is) and one at the end of time neither
    /// overflow nor wait for ever.
    #[test]
    fn extreme_timestamps_do_not_overflow() {
        let mut m = Merge::new(2, 50, STALE, 0);
        eof(&mut m, 1, 0);
        offer(&mut m, 0, i64::MIN, 0);
        assert_eq!(drain(&mut m, 0), [(0, i64::MIN)]);
        offer(&mut m, 0, i64::MAX, 0);
        assert_eq!(drain(&mut m, 29_999), []);
        assert_eq!(drain(&mut m, 30_000), [(0, i64::MAX)], "partition 1 has been silent for the stale period");
    }

    /// Memory is bounded by not reading ahead: a partition with a head is not polled.
    #[test]
    fn a_partition_with_a_head_is_not_read_ahead() {
        let mut m = Merge::new(2, 0, STALE, 0);
        offer(&mut m, 0, 10, 0);
        assert!(!m.fill(0, 0, || panic!("polled with a head")));
        assert!(!m.fill(1, 29_000, || Polled::Empty));
        assert_eq!(drain(&mut m, 29_999), []);
        assert_eq!(drain(&mut m, 30_000), [(0, 10)], "an empty queue is no news: partition 1 stays silent");
    }

    /// An end read together with the message behind it was reported before that message was
    /// fetched, while the partition had a head and its queue was not read: it is not at its end.
    #[test]
    fn an_end_with_a_message_behind_it_is_no_end() {
        let mut m = Merge::new(2, 0, STALE, 0);
        let mut q = [Polled::Eof, Polled::Msg(msg(5)), Polled::Msg(msg(6))].into_iter();
        assert!(m.fill(1, 0, || q.next().unwrap_or(Polled::Empty)));
        assert!(matches!(q.next(), Some(Polled::Msg(x)) if x.ts == 6), "one head, no more");
        offer(&mut m, 0, 10, 100);
        assert_eq!(drain(&mut m, 100), [(1, 5)]);
        assert_eq!(drain(&mut m, 100), [], "partition 1 delivered after its end");
        eof(&mut m, 1, 100);
        assert_eq!(drain(&mut m, 100), [(0, 10)]);
    }

    #[test]
    fn nothing_to_merge_is_nothing() {
        assert_eq!(Merge::new(0, 0, 0, 0).next(0), None);
        assert_eq!(Merge::new(2, 0, 0, 0).next(0), None);
    }

    /// A record of a simulated partition: when it is in the log, when the consumer has read it
    /// (both Unix ms), and the message.
    type Record = (i64, i64, Msg);

    /// A topic's partitions as librdkafka queues them for the data loop: each record once it is
    /// read, and the partition's end `eof_after` after the record before, unless the next one
    /// was in the log by then. (The fetch that finds a partition at its end returns after
    /// `fetch.wait.max.ms` at most, or at once with another partition's records; the end is
    /// reported once.) The consumer starts at the logs' ends at `start`.
    fn queues(parts: &[Vec<Record>], start: i64, eof_after: i64) -> Vec<VecDeque<(i64, Polled)>> {
        let queue = |p: &Vec<Record>| {
            let (mut q, mut reached) = (VecDeque::new(), start);
            for (logged, read, m) in p {
                if *logged > reached + eof_after {
                    q.push_back((reached + eof_after, Polled::Eof));
                }
                q.push_back((*read, Polled::Msg(m.clone())));
                reached = *read;
            }
            q.push_back((reached + eof_after, Polled::Eof));
            q
        };
        parts.iter().map(queue).collect()
    }

    /// The data loop's aligned polling (`run`) on a clock of its own, from `from` to `until`:
    /// every partition without a head is read, what may go goes, and when nothing moves the
    /// loop sleeps (1 ms here, at most 2 ms there). Returns what went and when. The merge never
    /// holds more than a message per partition.
    fn consume(m: &mut Merge, queues: &mut [VecDeque<(i64, Polled)>], from: i64, until: i64) -> Vec<(i64, usize, Msg)> {
        let (mut out, mut read, mut now) = (vec![], 0, from);
        while now <= until {
            let mut moved = false;
            for (i, q) in queues.iter_mut().enumerate() {
                let due = |q: &VecDeque<(i64, Polled)>| q.front().is_some_and(|e| e.0 <= now);
                let got = m.fill(i, now, || if due(q) { q.pop_front().unwrap().1 } else { Polled::Empty });
                (read, moved) = (read + usize::from(got), moved | got);
            }
            assert!(read - out.len() <= queues.len(), "more than a message per partition is held");
            while let Some((i, x)) = m.next(now) {
                out.push((now, i, x));
                moved = true;
            }
            // nothing held and nothing to read: on to the next record or end
            let next = queues.iter().filter_map(|q| q.front()).map(|e| e.0).min().unwrap_or(until + 1);
            let idle = m.parts.iter().all(|p| p.head.is_none());
            now = if moved { now } else { (now + 1).max(if idle { next } else { 0 }) };
        }
        out
    }

    /// A replay: partition 0 is an hour behind with 3000 records, read in fetches of 100 that
    /// take 300 ms each (far more than the wait, and never at its end); partition 1 has the
    /// newest 50 records ready; partition 2 is empty. Nothing overtakes anything, and the merge
    /// never holds more than a message per partition.
    #[test]
    fn a_partition_far_behind_is_caught_up_in_time_order() {
        let now = 10_000_000;
        let behind: Vec<Record> = (0..3_000).map(|k| (0, now + 300 * (k / 100), msg(k))).collect();
        let ahead: Vec<Record> = (0..50).map(|k| (0, now, msg(2_950 + k))).collect();
        let mut queues = queues(&[behind, ahead, vec![]], now, 0);
        let mut m = Merge::new(3, 50, STALE, now);
        let got = consume(&mut m, &mut queues, now, now + 12_000);
        let times: Vec<i64> = got.iter().map(|x| x.2.ts).collect();
        assert_eq!(times.len(), 3_050);
        assert!(times.is_sorted(), "{times:?}");
        assert!(got.iter().all(|(at, _, m)| *at <= now + 300 * (m.ts.min(2_999) / 100)), "no record waits for more");
    }

    /// A partition waits for the ones of its own group only: a partition never heard from, or one at its
    /// end that is not the wait past the head's time, in another group is nobody's concern. In the same group
    /// it holds the head back, as ever.
    #[test]
    fn a_quiet_partition_of_another_group_holds_nobody_back() {
        let mut m = Merge::grouped(&[0, 0, 1], 50, STALE, 1_000);
        offer(&mut m, 0, 1_000, 1_000);
        eof(&mut m, 1, 1_000);
        assert_eq!(drain(&mut m, 1_000), [], "partition 1 is in the head's group, at its end, within the wait");
        assert_eq!(drain(&mut m, 1_050), [(0, 1_000)], "partition 2 was never heard from, in another group");
        offer(&mut m, 2, 1_100, 1_060);
        assert_eq!(drain(&mut m, 1_060), [(2, 1_100)], "partitions 0 and 1 are in another group");
        // the same partition in the head's group holds it back until it has been heard from
        let mut m = Merge::grouped(&[0, 0], 50, STALE, 1_000);
        offer(&mut m, 0, 1_000, 1_000);
        assert_eq!(drain(&mut m, 1_000), [], "partition 1 was never heard from");
    }

    /// Of the groups with a head to give, the oldest head goes first; within a group, time order holds.
    #[test]
    fn groups_go_oldest_head_first_and_each_in_time_order() {
        let mut m = Merge::grouped(&[0, 1, 0, 1], 0, STALE, 0);
        for (i, ts) in [(0, 10), (1, 5), (2, 20), (3, 7)] {
            offer(&mut m, i, ts, 100);
        }
        assert_eq!(drain(&mut m, 100), [(1, 5), (0, 10)], "a partition that delivered 5 may deliver 6 next");
        eof(&mut m, 1, 100);
        eof(&mut m, 0, 100);
        assert_eq!(
            drain(&mut m, 100),
            [(3, 7), (2, 20)],
            "at their ends, with no wait: the oldest head of the groups first"
        );
        // group 0's 15 waits for partition 0, which may deliver 11..15; group 1's 30 is not held by either
        let mut m = Merge::grouped(&[0, 0, 1], 0, STALE, 0);
        offer(&mut m, 0, 10, 100);
        offer(&mut m, 1, 15, 100);
        offer(&mut m, 2, 30, 100);
        assert_eq!(drain(&mut m, 100), [(0, 10), (2, 30)]);
    }

    /// The stale period is a partition's own: one silent since the start lets its group's heads go, and says
    /// nothing for another's.
    #[test]
    fn silence_releases_the_partitions_group_only() {
        let mut m = Merge::grouped(&[0, 0, 1, 1], 50, STALE, 0);
        offer(&mut m, 0, 10, 29_000); // partition 1 has not been heard from since the start
        offer(&mut m, 2, 20, 29_000);
        offer(&mut m, 3, 20, 29_000);
        assert_eq!(drain(&mut m, 29_999), [(2, 20), (3, 20)], "group 1's partitions hold heads or delivered");
        assert_eq!(drain(&mut m, 29_999), [], "partition 1 is not silent for the stale period yet");
        assert_eq!(drain(&mut m, 30_000), [(0, 10)]);
        offer(&mut m, 2, 40, 30_000);
        assert_eq!(drain(&mut m, 30_000), [], "partition 3 may still deliver 20..40: its silence is its own");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Grouping changes when a group's records go, not their order: what a group gets is what a merge of
        /// its partitions alone would give, however many partitions the other groups hold and however
        /// slowly they are read.
        #[test]
        fn a_group_is_merged_as_if_the_other_groups_were_not_there(
            groups in proptest::collection::vec(0usize..3, 2..7),
            wait in 0i64..100,
            eof_after in 0i64..40,
            records in proptest::collection::vec((0usize..7, 0i64..120, 0i64..60), 1..120),
        ) {
            let n = groups.len();
            let mut parts: Vec<Vec<Record>> = vec![vec![]; n];
            let mut at = 0;
            for (k, (p, gap, fetch)) in records.into_iter().enumerate() {
                at += gap;
                let p = p % n;
                let before = parts[p].last();
                let ts = (W0 + at).max(before.map_or(i64::MIN, |r| r.2.ts));
                let read = (ts + 1 + fetch).max(before.map_or(i64::MIN, |r| r.1));
                parts[p].push((ts + 1, read, Msg { offset: k as i64, ts, payload: vec![] }));
            }
            let until = W0 + at + 10_000;
            let mut all = Merge::grouped(&groups, wait, 3_600_000, W0);
            let got = consume(&mut all, &mut queues(&parts, W0, eof_after), W0, until);
            for g in 0..3 {
                let own: Vec<usize> = (0..n).filter(|&i| groups[i] == g).collect();
                let alone_parts: Vec<Vec<Record>> = own.iter().map(|&i| parts[i].clone()).collect();
                let mut alone = Merge::new(own.len(), wait, 3_600_000, W0);
                let want = consume(&mut alone, &mut queues(&alone_parts, W0, eof_after), W0, until);
                let in_group: Vec<i64> = got.iter().filter(|x| groups[x.1] == g).map(|x| x.2.offset).collect();
                let alone: Vec<i64> = want.iter().map(|x| x.2.offset).collect();
                prop_assert_eq!(in_group, alone, "group {}", g);
            }
        }
    }

    const W0: i64 = 1_700_000_040_000; // a 15 s window's start, Unix ms

    /// A quote captured at `us`, as the source decodes it.
    fn quote(symbol: &str, us: i64) -> Row {
        let f = Value::F64;
        vec![
            Value::Int(us - 3_000),
            Value::Int(1),
            Value::Str(symbol.into()),
            f(100.0),
            f(1.0),
            f(100.1),
            f(2.0),
            Value::Int(us),
        ]
    }

    /// A field of a JSONEachRow line, as text.
    fn field<'a>(json: &'a str, name: &str) -> &'a str {
        let at = json.find(&format!("\"{name}\":")).unwrap_or_else(|| panic!("no {name} in {json}")) + name.len() + 3;
        json[at..].split([',', '}']).next().unwrap().trim_matches('"')
    }

    /// Quotes counted per symbol in 15 s windows of their capture time, closed 50 ms late.
    const QUOTES_15S: &str = "
        CREATE EXTERNAL STREAM quotes (time int64, exchange int64, symbol low_cardinality(string),
          bid_price float64, bid_amount float64, ask_price float64, ask_amount float64, local_timestamp int64)
        SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'quotes', data_format = 'ProtobufSingle',
          format_schema = 'market:Quote';
        CREATE STREAM quotes_input (local_event_time datetime64(6), symbol string);
        CREATE MATERIALIZED VIEW quotes_input_mv INTO quotes_input AS
        SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, symbol FROM quotes;
        CREATE EXTERNAL STREAM quotes_15s_out (symbol string, time int64, quote_update_frequency int32)
        SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'quotes.15s', data_format = 'JSONEachRow',
          one_message_per_row = true;
        CREATE MATERIALIZED VIEW quotes_15s INTO quotes_15s_out AS
        SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, to_int32(count()) AS quote_update_frequency
        FROM tumble(quotes_input, local_event_time, 15s) GROUP BY window_start, symbol
        EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;";

    /// A lagging partition's fetch: two partitions of quotes, one a millisecond each, produced 10 ms after
    /// capture and read a millisecond later. Partition 1's quotes captured from 14.971 s into a
    /// 15 s window are read late, in one fetch at 15.092 s: after partition 0's quote captured at
    /// 15.050 s, which closes the window. Returns how many quotes of that window were counted per
    /// symbol (1000 were captured of each), with the partitions' ends reported `eof_after` ms
    /// after they are reached.
    fn quotes_counted(wait: i64, eof_after: i64) -> BTreeMap<String, String> {
        let mut engine = Engine::new(&brrrrr_core::sql::parse(QUOTES_15S).unwrap()).unwrap();
        let (mut rows, mut parts) = (vec![], vec![vec![], vec![]]);
        for ms in 14_000..15_400 {
            for (p, symbol) in ["BTC", "ETH"].into_iter().enumerate() {
                let ts = W0 + ms + 10;
                let read = if p == 1 && (14_971..15_081).contains(&ms) { W0 + 15_092 } else { ts + 1 };
                parts[p].push((ts + 1, read, Msg { offset: rows.len() as i64, ts, payload: vec![] }));
                rows.push(quote(symbol, (W0 + ms) * 1000 + 500 * p as i64));
            }
        }
        let start = W0 + 14_000;
        let mut m = Merge::new(2, wait, STALE, start);
        let mut out = vec![];
        for (_, _, msg) in consume(&mut m, &mut queues(&parts, start, eof_after), start, W0 + 16_000) {
            engine.insert("quotes", vec![rows[msg.offset as usize].clone()], &mut out);
        }
        out.iter()
            .filter(|e| &*e.topic == "quotes.15s" && field(&e.payload, "time") == (W0 * 1000).to_string())
            .map(|e| (field(&e.payload, "symbol").to_string(), field(&e.payload, "quote_update_frequency").to_string()))
            .collect()
    }

    fn counts(btc: &str, eth: &str) -> BTreeMap<String, String> {
        [("BTC".to_string(), btc.to_string()), ("ETH".to_string(), eth.to_string())].into()
    }

    /// The window counts every quote captured in it, whether the lagging partition's end was
    /// reported before its late quotes (at once: it shares a broker with a busy partition) or
    /// not yet (the fetch at its end is still waiting).
    #[test]
    fn a_window_counts_the_quotes_a_lagging_partition_delivers_after_its_end() {
        for eof_after in [0, 50] {
            assert_eq!(quotes_counted(50, eof_after), counts("1000", "1000"), "ends reported after {eof_after} ms");
        }
    }

    /// What a lagging fetch measured: with a partition at its end holding nobody back, the window is
    /// emitted without the 29 quotes partition 1 captured in its last 29 ms. (With its end not
    /// reported yet it holds the others back as any partition that is behind.)
    #[test]
    fn without_the_wait_the_window_loses_the_lagging_partitions_last_quotes() {
        assert_eq!(quotes_counted(0, 0), counts("1000", "971"));
        assert_eq!(quotes_counted(0, 50), counts("1000", "1000"));
    }

    /// A trade captured at `us`, as windows.sql's source decodes it.
    fn trade(symbol: &str, us: i64) -> Row {
        let (s, f) = (|s: &str| Value::Str(s.into()), Value::F64);
        vec![Value::Int(us), s("1"), Value::Int(1), s(symbol), f(100.0), Value::Int(us), s("buy"), f(1.0), f(100.0)]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Live consumption of 1 to 4 partitions: each record is produced up to 25 ms after its
        /// capture (in time order within its partition) and read up to the wait, less 5 ms,
        /// after it is produced, so no record is read later than the wait past a newer one's
        /// time. However the partitions interleave, the engine gets the records in time order,
        /// every 1m window counts the rows captured in it, no row is late, and no record waits
        /// longer than twice the wait and an end's report past its time once it is read.
        #[test]
        fn live_interleavings_within_the_wait_lose_no_row(
            n in 1usize..5,
            wait in 10i64..200,
            eof_after in 0i64..60,
            records in proptest::collection::vec((0usize..4, 0i64..400, 0i64..25, 0i64..1_000), 1..300),
        ) {
            let (mut rows, mut parts, mut captured) = (vec![], vec![vec![]; n], 0);
            let mut want: BTreeMap<(String, i64), usize> = BTreeMap::new();
            let closing = (0, 120_000, 0, 0); // closes every window before it
            for (k, (p, gap, produce, fetch)) in records.into_iter().chain([closing]).enumerate() {
                captured += gap;
                let (p, symbol) = (p % n, ["A", "B", "C"][k % 3]);
                let before: Option<&Record> = parts[p].last();
                let ts = (W0 + captured + produce).max(before.map_or(i64::MIN, |r| r.2.ts));
                let read = (ts + 1 + fetch % (wait - 5)).max(before.map_or(i64::MIN, |r| r.1));
                parts[p].push((ts + 1, read, Msg { offset: k as i64, ts, payload: vec![] }));
                rows.push(trade(symbol, (W0 + captured) * 1000));
                *want.entry((symbol.to_string(), (W0 + captured).div_euclid(60_000) * 60_000_000)).or_default() += 1;
            }
            // the closing trade's window stays open
            prop_assert_eq!(want.remove(&("ABC"[(rows.len() - 1) % 3..][..1].to_string(), (W0 + captured).div_euclid(60_000) * 60_000_000)), Some(1));
            let reads: BTreeMap<i64, i64> = parts.iter().flatten().map(|r| (r.2.offset, r.1)).collect();
            let mut m = Merge::new(n, wait, 3_600_000, W0); // no partition is silent for the stale period
            let until = W0 + captured + 1_000;
            let got = consume(&mut m, &mut queues(&parts, W0, eof_after), W0, until);
            prop_assert_eq!(got.len(), rows.len());
            prop_assert!(got.iter().map(|x| x.2.ts).is_sorted());
            for (at, _, msg) in &got {
                let bound = reads[&msg.offset].max(msg.ts + 2 * wait + eof_after) + 2;
                prop_assert!(*at <= bound, "{msg:?} read at {} went at {at}", reads[&msg.offset]);
            }
            let sql = include_str!("../../../tests/acceptance/sql/windows.sql");
            let mut engine = Engine::new(&brrrrr_core::sql::parse(sql).unwrap()).unwrap();
            let mut out = vec![];
            for (_, _, msg) in got {
                engine.insert("trades_source", vec![rows[msg.offset as usize].clone()], &mut out);
            }
            prop_assert_eq!(engine.late(), 0);
            let counted: BTreeMap<(String, i64), usize> = out
                .iter()
                .map(|e| ((field(&e.payload, "symbol").to_string(), field(&e.payload, "time").parse().unwrap()), field(&e.payload, "n").parse().unwrap()))
                .collect();
            prop_assert_eq!(counted, want);
        }
    }

    /// The time every record up to which has reached the engine, for a clock close (`--close-after-ms`):
    /// the oldest of what each partition can vouch for.
    #[test]
    fn the_merge_vouches_for_the_time_every_partition_has_delivered_up_to() {
        let mut m = Merge::new(2, 50, STALE, 1_000);
        assert_eq!(m.vouched_until(1_000, 300), None, "never heard from: it may owe anything");
        eof(&mut m, 0, 1_000);
        assert_eq!(m.vouched_until(1_000, 300), None, "partition 1 may owe anything");
        eof(&mut m, 1, 1_000);
        assert_eq!(m.vouched_until(1_000, 300), Some(700), "both at their end: when they were read, less the margin");
        assert_eq!(m.vouched_until(1_500, 300), Some(700), "not later because the clock is: nothing was read since");

        // a partition that delivered newer than that vouches for what it delivered; the other holds it back
        offer(&mut m, 0, 1_400, 1_500);
        assert_eq!(drain(&mut m, 1_500), [(0, 1_400)]);
        eof(&mut m, 0, 1_500);
        assert_eq!(m.vouched_until(1_500, 300), Some(700), "partition 1 was read at 1000");
        eof(&mut m, 1, 1_500);
        assert_eq!(m.vouched_until(1_500, 300), Some(1_200));
        assert_eq!(m.vouched_until(1_500, 100), Some(1_400), "partition 0 delivered 1400, and 1500 less 100 is 1400");

        // a head vouches to just before it, and holds back the others
        let mut m = Merge::new(2, 50, STALE, 1_000);
        eof(&mut m, 1, 1_500);
        offer(&mut m, 0, 1_300, 1_510);
        assert_eq!(m.vouched_until(2_000, 100), Some(1_299), "partition 1 was read at 1500 and vouches for 1400");

        // not at its end, nothing in hand: a backlog whose fetch is on its way vouches for what it gave
        let mut m = Merge::new(2, 50, STALE, 1_000);
        eof(&mut m, 1, 1_000);
        offer(&mut m, 0, 900, 1_000);
        assert_eq!(drain(&mut m, 1_100), [(0, 900)]);
        assert_eq!(m.vouched_until(1_100, 50), Some(900), "partition 0 delivered 900 and is not at its end");
    }

    /// A loop that stalled after it read a queue has not read what arrived since: the close counts from the
    /// time of the read, not from the clock when it is made (a long batch, a throttled CPU).
    #[test]
    fn a_stall_after_the_read_does_not_move_what_a_partition_vouches_for() {
        let mut m = Merge::new(1, 50, STALE, 1_000);
        eof(&mut m, 0, 1_000);
        // a record of 1010 reaches the queue while the loop is busy; the close is made at 1400
        assert_eq!(m.vouched_until(1_400, 300), Some(700), "not 1100, which would close the window of 1010");
        offer(&mut m, 0, 1_010, 1_400);
        assert_eq!(drain(&mut m, 1_400), [(0, 1_010)]);
        eof(&mut m, 0, 1_410);
        assert_eq!(m.vouched_until(1_410, 300), Some(1_110));
    }

    /// A record stamped in the future (a producer's clock jump, a field in the wrong unit) must not close
    /// every window for good: nothing is vouched for later than the clock less the margin.
    #[test]
    fn a_record_from_the_future_does_not_move_what_is_vouched_for_past_the_clock() {
        let mut m = Merge::new(2, 50, STALE, 1_000);
        offer(&mut m, 0, 1_000 + 3_600_000, 1_000);
        eof(&mut m, 1, 1_000);
        assert_eq!(drain(&mut m, 1_100), [], "partition 1 may still owe an older record");
        assert_eq!(drain(&mut m, 1_000 + 3_600_000 + 50), [(0, 1_000 + 3_600_000)]);
        eof(&mut m, 0, 1_000 + 3_600_000 + 50);
        eof(&mut m, 1, 1_000 + 3_600_000 + 50);
        assert_eq!(m.vouched_until(2_000, 300), Some(1_700), "the clock is 2000, not an hour on");
    }

    /// A partition that is not at its end and has been silent for `STALE` holds nobody back for `next`, but
    /// for a clock close it may hold anything (a replay's backlog): the wall clock vouches for none of it.
    #[test]
    fn a_silent_partition_not_at_its_end_does_not_let_the_clock_close_past_its_data() {
        let mut m = Merge::new(2, 50, STALE, 0);
        eof(&mut m, 1, 0);
        offer(&mut m, 0, 100, 0);
        assert_eq!(drain(&mut m, 200), [(0, 100)]);
        eof(&mut m, 1, STALE * 10); // partition 1 is read again and again, still at its end
        assert_eq!(
            m.vouched_until(STALE * 10, 300),
            Some(100),
            "partition 0 delivered 100, then said nothing for ages"
        );
        let mut never = Merge::new(2, 50, STALE, 0);
        eof(&mut never, 1, 0);
        assert_eq!(never.vouched_until(STALE * 10, 300), None, "partition 0 never delivered and is not at its end");
    }
}
