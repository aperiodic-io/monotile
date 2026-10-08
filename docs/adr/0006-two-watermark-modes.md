# ADR-0006: Two watermark modes, `proton` (bit-exact) and `aligned` (per-partition)

- Status: accepted. `proton` is built and the default; `aligned` is built at the source, off
  by default (amendments below)
- Date: 2026-09-23

## Context
- **Proton keeps one watermark per query:**
  `floor_to_window(max(ts) − delay)` over all partitions, advanced per chunk,
  with no idle timer. Late means "the row's window was already emitted".
- **Consequence:** during catch-up the lagging partition's rows are dropped.
  One 4-symbol input produced 2,312 and 2,892 windows of 3,816 in two runs.
- Bit-exact parity requires reproducing this.
- Correctness argues for Flink/Arroyo-style per-partition watermarks with
  idleness.

## Decision
Implement both:

- **`proton`** follows Proton's rules above exactly (one watermark per query,
  `floor_to_window(max(ts) − delay)`, advanced per chunk), with a
  **deterministic** merge order (any deterministic order is a legal
  Proton execution) and key-ordered emission within a window.
- **`aligned`** takes the minimum over non-idle partitions, uses
  `idle_timeout`, and pauses partitions beyond `max_drift`.

`proton` is the default. Switching to `aligned` is a separate, announced
decision.

## Consequences
- Parity tests are meaningful, and running both modes side by side quantifies
  Proton's losses.
- Two code paths in the tumble operator. They share the accumulators and
  differ only in the watermark tracker, which is behind a trait.
- Proton's arrival-order and hash-order non-determinism is not reproduced. It
  can't be, and there is no reason to.

## Alternatives considered
- **Only `aligned`.** Loses the ability to prove parity.
- **Only `proton`.** Bakes known data loss into the new engine.

## Amendment (2026-09-28): `aligned` is built at the source
The drift, and a partition at its end holding nobody back, are replaced in the amendment after
this one.
- `--max-drift SECONDS` (`crates/brrrrr/src/align.rs`) merges the source partitions by
  record timestamp: a message goes to the engine only once no partition that may still deliver
  an older one is more than the drift behind it. A partition at its end, or silent for 30 s,
  holds nobody back (the idleness). A partition holds at most its next message and is not read
  ahead, so memory stays bounded without pausing any partition.
- The engine keeps `proton`'s one watermark and just sees the partitions in time order: one
  code path in the window operator, not two watermark trackers behind a trait.
- `--idle-close SECONDS`: once nothing has been consumed that long and every source
  partition is read to its end, the windows an event that long ago would close are closed.
  Proton keeps a quiet stream's last windows open until its next row.
- Both are off by default: switching is still the separate, announced decision above.

## Amendment (2026-09-30): no drift, and a partition at its end is waited for
- **What was lost.** On a live quote feed, 0.45% of one exchange's and 0.20% of another's 15 s
  quote windows counted fewer quotes than the source topic held: one window missed the last 250 quotes of one of its two partitions, captured in its last 29 ms. They were
  in the topic, in order, produced within 25 ms of capture, and read after the other partition's
  quote 50 ms past the window's end, which emitted it. Two rules of the merge let that quote
  through: a partition could run `--max-drift` (1 s) ahead of one that might still deliver, and
  a partition at its end held nobody back. But live, a partition is at its end most of the time,
  and its end is that of the log when it was fetched: it says nothing of the rows produced since.
- **The rule.** A partition still holds at most its next message. The oldest of those goes to
  the engine once every other partition either has one too (then not an older one), or has
  delivered that record time or a later one, or is at its end while the clock is
  `--partition-wait-ms` (50) past that record time, or has said nothing for 30 s. A partition
  that is not at its end (a backlog whose fetch is on its way) holds the others back until it
  delivers, as before.
- **Groups.** The partitions are merged per group, not all together: the ones whose source streams
  reach the same views (`Engine::source_groups`: a view's inputs and the stream it writes are
  connected, and so are the views that write one stream). A head waits for the partitions of its own
  group only, so a partition of one group that has not been read yet (a replaying exchange, a slow
  quote feed) does not hold back the rows of another that no view orders against it. A pipeline of
  per-exchange joins over three exchanges is three groups; a cross-exchange aggregate makes its exchanges one. A topic read
  by streams of two groups joins them. The order within a group is the rule above, unchanged;
  `--close-after-ms` still vouches for the oldest partition of all, as the engine closes windows
  globally.
- **Why nothing is overtaken.** A partition's records are in time order, so one that has a
  message waiting or has delivered `t` holds nothing older than `t` back. A partition at its end
  has delivered every record it held when it was fetched; a record older than `t` that it still
  owes was produced by about `t` and is read a fetch later: within the wait, it is the oldest
  message when the clock gets there. So a record is overtaken by a newer one of another partition
  only when it is read later than the wait past that one's record time (or its partition said
  nothing for 30 s), and then it still reaches its window unless the window's own delay has
  passed too. `brrrrr_late_events_total` counts what did not.
- **No drift.** Any drift let a partition's rows pass the ones another still owed, which a window
  ending in between lost. `--max-drift` still turns the merge on; its seconds are accepted, so
  existing command lines keep working, and not used.
- **The end is reported soon.** librdkafka reports a partition's end when the fetch that finds
  nothing returns: after `fetch.wait.max.ms`, 500 ms by default, unless another partition of
  that broker has records first. With `--max-drift` the consumer sets 50 ms (a user setting
  still wins), so a partition that goes quiet holds the others for no longer than the wait. An
  end that is read together with a message behind it is not the partition's end any more
  (`Merge::fill`).
- **Cost.** A message waits until every other source partition of the pipeline (of every topic:
  the merge is over all of them) has delivered one as new, or, where a partition is at its end,
  until the clock is 50 ms past the message's record time. Partitions that all deliver steadily
  wait for the slowest one's next message: milliseconds for six busy quote partitions. A pipeline
  that reads a sparse topic beside a busy one (one message in less than 30 s, but not every few
  ms) passes its rows on about 50 ms after their record time, which is some 25 to 40 ms later
  than before, since a record is read 10 to 25 ms after its time. A single partition, or others
  silent for 30 s, wait for nothing. A replay's records are older than the wait: no clock is
  waited for, and the merge is as before with a drift of 0. The engine, its one watermark and
  the checkpoint format are untouched.
- **Limits.** The clocks of the producers (record timestamps) and of brrrrr are compared: a
  record stamped ahead of brrrrr's clock waits for its time, one stamped behind waits less than
  the wait. A fetch later than the wait is not waited for.

### Alternatives considered
- **A watermark per partition in the window operator**, the minimum taken (this ADR's first
  design): every row would carry its partition through every operator into the checkpoint
  (a format change), and `latest`, the held sort and the ASOF join would still see the partitions
  out of order. The merge gives every operator one time order.
- **Waiting a fixed time after a message is read**: the wait starts again with each message, so a
  partition next to a quiet one would pass one message per wait.
- **A partition idle some time after its last message holds nobody back** (Flink's idleness):
  the wait would have to outlast the longest late fetch of a busy partition, and every partition
  of a middling rate would delay the others by it.
- **A longer window delay, or sorting the quotes as trades are sorted** (ADR-0013): every
  window of every source waits longer; sorting cost a quote pipeline 40% of its throughput.

## Amendment (2026-10-01): `--close-after-ms`, a window closes on the clock when its feed is quiet
A window closes when a row of a later time arrives, so a feed that goes quiet keeps its last
windows open until its next row: polled data (long/short ratios), rare events (liquidations) and slow
pollers (funding rates, open interest) arrived 0.6 s to 23 s after their window. `--idle-close`
does not help them: it needs *nothing* consumed for whole seconds across the pipeline, which an
instance that also reads a busy feed never meets (running such pipelines with `--idle-close 1`
changed no latency and no row).

With `--max-drift` (the aligned merge) and `--close-after-ms N`, the merge says to which time every
record has reached the engine (`Merge::vouched_until`): the oldest of what each partition vouches
for. A partition holding a head vouches to just before it; one at its end to the clock less `N`
(what it was owed was produced and read by then, the argument of `--partition-wait-ms` with a
longer margin), and never less than it delivered; a partition with a backlog whose next fetch is on
its way to what it delivered; and one silent for 30 s holds nobody back. The loop then calls
`Engine::close_until` to that time, which closes the windows an event at it would close, releases a
held sort and, for an exact ASOF join, the rows every right side has passed, as `--idle-close` does.

- It holds back whatever could still lose a row: a partition read empty a moment ago is trusted, a
  partition with unread messages (a backlog, a replay, a stalled loop) is not, because the queues
  are read before every close.
- A row that still arrives after the close is late and dropped (`brrrrr_late_events_total`), as
  any row past its window's watermark. `N` must therefore exceed `--partition-wait-ms` (refused
  otherwise) and the capture-to-broker-to-consumer time: on live exchange feeds we measured 16 ms
  p50, 47-60 ms p99, so a few hundred ms.
- A row stamped more than `N` before it reaches the engine is dropped as late once the clock close has run,
  where the data-driven close would have kept a quiet feed's window open for it: a producer that backfills
  after an outage longer than `N` loses those rows from the windows already closed. Producers stamp rows at
  capture, so a live feed is within tens of ms; the acceptance scenarios that use back-dated synthetic
  trades must not send a second batch after a clock close. A record stamped in the future never moves the
  close past the clock less `N` (`Merge::vouched_until`).
- The clock close complements the data-driven one, it does not replace it: a busy feed's windows
  still close when a row of a later time arrives, at 50 ms past their end, before `N`.
- Like an idle close, a restart must not write again a window an earlier run closed on the clock:
  suppression stays until a clock close has run after the replay caught up.
- Not deterministic in replay by construction, as `--idle-close`: a window closed on the clock
  before a late row is written without it, and a replay that has the row would write another bar;
  suppression by dedup key keeps the topic exactly-once, with the bar the live run wrote.
