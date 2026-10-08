# ADR-0007: Exactly-once output via output high-water marks

- Status: accepted (amended 2026-09-23: suppression by dedup key, no new header)
- Date: 2026-09-23

## Context
- Inputs and outputs may be on two different clusters (raw feeds on one,
  metrics on another). A Kafka transaction cannot span two clusters, so EOS v2
  (KIP-447) and Arroyo's transactional sink do not apply.
- Broker dedup is capped (`dedup_max_entries_per_partition`) and fails open.
  Relying on it leaked 205,008 duplicates in a crash test of a prototype.
- The same prototype proved the alternative: source-offset headers plus replay
  without producing up to the output's high-water mark gave **byte-identical**
  topics after 3× `kill -9`.

## Decision

> Superseded in part by the amendment below: suppression keys on `redpanda-dedup-key`,
> and no `brrrrr-seq` header is written.

- Outputs are deterministic given the checkpoint and input positions (ADR-0006
  determinism).
- Every output message carries `brrrrr-seq = <epoch>:<index>` per sink
  topic-partition, and uses the idempotent producer.
- On restore, the sink reads each output partition's last message and drops
  replayed messages at or below that `seq`.
- Epoch commit order: sinks `pre_commit` (all acked), then snapshots, then the
  manifest, then sinks `commit`, then Kafka offset commits.

## Consequences
- Exactly-once at the topic level with no broker features and no
  transactions.
- One extra header on consumer-facing messages. Without it, recovery must
  derive positions from `redpanda-dedup-key`: possible, more complex.
- Determinism becomes a hard invariant, tested by the chaos suite.

## Alternatives considered
- **Kafka transactions on the output cluster only.** Offsets live on the other
  cluster, so atomicity is lost anyway.
- **At-least-once plus dedup header** (Proton's approach). Duplicates leak when
  the dedup index saturates.

## Amendment (2026-09-23): suppress by `redpanda-dedup-key`, no `brrrrr-seq`
Offsets or sequence numbers only identify a replayed message if replay is deterministic.
Proton's window semantics make output depend on chunk boundaries, and those depend on
arrival timing, so replay after a crash is not guaranteed to cut chunks the same way. The
key a sink of market-data pipelines typically writes, `redpanda-dedup-key` (time | metric | exchange
| symbol | interval), is unique per window. It identifies a message whatever the chunking.

- A checkpoint records each sink partition's end offset once everything before the
  checkpoint has been acked.
- On restore, brrrrr reads each sink partition from that offset to its current end and
  counts the dedup keys it finds. Replay skips a message whose key is still counted. The
  payload stands in for the key on sinks without the header.
- With no checkpoint, the sinks are read from their start. This covers a crash before the
  first checkpoint, and a switch from a Proton pipeline that wrote the same keys.
- **Verified** (`tests/acceptance/features/kafka.feature`): 30,000 trades with six
  `kill -9`s, each landing after output that no checkpoint covered, leave all 3,500 windows
  in the topic exactly once. The same scenario finds 10,479 duplicates with suppression
  disabled.
- Consequence: consumer-facing messages keep Proton's header set exactly.


## Amendment (2026-09-26): what a start without a checkpoint cannot vouch for
Suppression only knows what the sinks still hold. A switch of a set of Proton pipelines to brrrrr
started every pipeline without a checkpoint over sources holding 6-20 hours, while the busiest
metric topics held about 2 hours (retention by bytes). Every window the replay closed that the topics had already dropped was
written again, hours late: 28 of 62 1m topics, millions of windows. The oldest were computed from
part of their input, their start being older than the sources' oldest record, and so were that
day's 1d windows, still open at the edge of the sources' retention.

Two opt-in bounds, for a start without a checkpoint:

- `--history <hours>` withholds the windows that ended more than that long before the start: set it
  to at most what the sinks keep, so every window written is one suppression can check.
  *Replaced below by a bound per sink topic.*
- `--complete-windows` withholds the windows that began before the latest of the sources' oldest
  records (message timestamps, which must be event times, as exchange-feed producers write them).
  *Amended below: the default, and bounded only by partitions that lost records.*

A withheld window closes like any other (its watermark moves on, a late row stays late) but is not
emitted: `Engine::withhold`, counted in `brrrrr_withheld_windows_total`. Only windows no other
window reads are withheld, as a withheld window feeding another would make that one partial. The
bounds are not checkpointed: they are kept beside the checkpoints (`withhold`, `<start µs> <end µs>`)
and applied on every start of that SQL's pipeline, so a restart before the replay caught up
withholds the same windows. A later start without a checkpoint replaces them.

A restored checkpoint's sink offsets on topics the run no longer writes are dropped rather than
read back: a trial instance's checkpoint adopted by the live instance of its pipeline names the
trial's topics, which are removed once nothing writes them.

- **Verified** (`kafka.feature`): over 3 hours of source with `--history 1`, the topic holds
  exactly the engine's windows of the last hour; without the withholding the same scenario writes
  718 windows it should not. `--complete-windows` withholds the window the first trade falls in;
  a restart keeps withholding; an adopted trial checkpoint restores after the trial's topic is
  deleted and writes none of the trial's windows again.
- Consequence: windows withheld are gaps, not duplicates or partial windows. A consumer misses
  what the replay could not write whole and once, instead of receiving it wrong or twice.

## Amendment (2026-09-27): complete windows by default
Only complete windows are written, unless `--partial-windows` asks for Proton's behaviour. A
partition bounds them only if retention has deleted records from it (its log start is past 0): one
that still holds its first record lost nothing, however late it starts, so a source with its whole
history withholds nothing. The bound is per window, so per interval: after a start whose sources
lost everything before 03:59, the 1m and 1h windows are written from 04:00 on and the 4h from
08:00, and that day's 1d window is not written.

- **Verified** (`kafka.feature`): with the source's first 100 trades deleted, the window the oldest
  retained one falls in is withheld and every later one written; with none deleted nothing is
  withheld; `--partial-windows` writes the partial window as before.

## Amendment (2026-09-27): history bounded per sink topic, not by `--history`
One number of hours for every sink was the wrong shape: what a start without a checkpoint can check
depends on each sink topic's retention, and each interval has its own topics (a 1m topic keeps about
2 hours, a 1d topic a week). `--history` is gone. By default a window is withheld when its own sink
topic has lost records written after it closed: the topic's oldest retained record, where
retention has deleted older ones, is newer than the window. A topic that lost no record bounds
nothing, so a replay backfills any window it is missing. `--unverified-history` writes them all, as
Proton does. The bounds are kept beside the checkpoints with the partial-window one (`start <µs>`,
`<topic> <µs>` lines).

A sink message does not carry its window's end, but a window that closes in a batch ended no
earlier than the engine's event time before that batch (less its delay): the runtime withholds a
message when that event time is older than its topic's bound, at most one batch conservative.
Timestamps are compared as event times: a sink record's is when it was written, near its window's
close (later during a replay, which only withholds more).

- **Verified** (`kafka.feature`): a 1m topic that kept the last hour and a 5m topic that kept all
  but one window never written. The replay writes none of the 1m windows the topic dropped and
  backfills the missing 5m one; without the bound, the 1m ones are written again.

## Amendment (2026-09-27): first candles are full
A partition that still holds its first record bounded nothing (the amendment above), so a feed that
started after a window did (a new pipeline on a new feed, or a cluster started afresh) had that
first window written from part of its input. A source's data now starts where its topic's feed
does:

- In a topic that lost no record, at the earliest oldest record of its partitions. The producer
  ran from then, and a partition that starts later (a quiet symbol) had nothing to hold before, so
  it moves nothing.
- Where retention deleted records, no earlier than the latest oldest retained record of those
  partitions.
- The windows that began before the latest source's start are withheld, as before.

A source with no record at all when a start without a checkpoint probes it (brrrrr up before the
producers, as after a cluster reset) is awaited. Its first record raises the bound to its time,
recorded beside the checkpoints (`await <topic>` until then), so no window that began before its
feed is written. A source that stays empty holds nothing back.

- **Verified** (`kafka.feature`):
  - A feed that started mid-window has that window withheld and every later one written.
  - brrrrr started on an empty source waits for its first record and withholds likewise.
  - A partition starting later than its topic's feed withholds nothing (the two-partition
    aligned-consumption scenario).
- Negative controls: each fails when the corresponding rule is off.
- The acceptance harness's records are now stamped with their trades' times, as a live feed's are
  (the first trade is at yesterday's midnight), instead of an anchor a day away from them.
