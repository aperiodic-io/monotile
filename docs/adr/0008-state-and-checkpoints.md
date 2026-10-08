# ADR-0008: In-memory state, epoch checkpoints to object storage, versioned postcard

- Status: accepted, as amended below
- Date: 2026-09-23

## Context
- State is small: books (up to ~1,000 levels per symbol), open windows, asof
  buffers.
- Arroyo keeps state in memory and snapshots to object storage (no RocksDB);
  its blog documents why RocksDB is hard to tune.
- RisingWave separates barriers from checkpoints.
- `bincode` is dead (RUSTSEC-2025-0141).

## Decision
- `StateTable<K, V>` in memory, keyed into 256 key groups.
- A barrier every 1 s; every 10th barrier is a checkpoint.
- Snapshots use `postcard` inside a versioned envelope, then zstd, written
  through `object_store` (S3-compatible stores, MinIO, a file system, memory).
- The manifest is written last. The last 5 epochs are retained.

## Consequences
- Pods are stateless apart from scratch, so rescheduling is simple.
- State migrations are explicit `From` impls, each with a committed fixture
  test.
- Restore is possible onto a different key-group assignment.

## Alternatives considered
- **Compacted Kafka topic** (proven in a proof of concept). It puts load on the
  source broker and needs chunking for large states. Kept as a fallback
  implementation of `CheckpointStore`.
- **RocksDB.** Unnecessary for our state sizes, and operationally heavy.

## Amendment (2026-09-24): what is built
- One checkpoint object per epoch: the engine state (postcard, uncompressed), the source
  positions and the sink end offsets together, so a checkpoint exists iff it is complete.
- Under `<pipeline>/<hash of its SQL>/`: a changed SQL starts fresh (as Proton does)
  instead of loading one view's state into another.
- Written with `PutMode::Create`: a second instance of the pipeline fails at its first
  checkpoint (on S3, `If-None-Match`: brrrrr sets `AWS_CONDITIONAL_PUT=etag` itself and refuses
  any other value, and at start proves that the store refuses a second create of one name).
- A checkpoint every `--interval` seconds on the data thread, and on SIGTERM before exit.
- Not built: key groups, barriers separate from checkpoints, zstd. One data thread holds
  all state, so there is nothing to redistribute. Sizes are reported as
  `brrrrr_checkpoint_bytes`, so compression can be added if a pipeline needs it.
- Verified by: checkpoint and restore at many points of every test pipeline (parity
  suite); a differential fuzzer (restore at a fuzzer-chosen chunk equals an uninterrupted
  run); and acceptance tests with Redpanda. Those cover `kill -9` storms, SIGTERM, a second
  instance, a changed SQL, lost checkpoints and two source partitions.


## Amendment (2026-09-25): a versioned format that cannot change unnoticed
Postcard writes no names, so bytes of one layout decode into another whenever the shapes line
up (fields of one type swapped, an enum variant inserted, a trailing field dropped): the state
restores "successfully" into the wrong aggregates. The format is now (`brrrrr_core::checkpoint`):

- A fixed header, checked before anything is decoded: `BRRR`, the format `VERSION` (u16 LE) and
  `LAYOUT` (u64 LE), a hash of the serialized types as traced by serde-reflection. Then the
  body: one zstd frame (level 3) of the postcard encoding, with nothing after it.
- The body carries `Engine::fingerprint` of the plan it was taken of (operators, each window's
  width, delay, keys and aggregate calls in accumulator order). The same SQL planned
  differently by another build (a planner or sqlparser change) is refused, not restored.
- `tests/checkpoint.rs` fails when the traced layout differs from the committed one, when this
  build writes other bytes than the committed checkpoints of the test pipelines
  (`fixtures/checkpoints/v<VERSION>/`), or when any committed checkpoint no longer restores
  into exactly the output of an uninterrupted run.
- Changing the format means bumping `VERSION` and committing the new version's checkpoints.
- **Append-only changes keep older versions readable.** A version that only appends enum
  variants (and the types only they reach) leaves every older encoding as it was:
  `checkpoint::READS` lists the older versions a build reads, each with the one layout its build
  traced, and the first checkpoint after an upgrade is written in the new version. Any other
  change drops every older version from `READS`, and an upgrade starts every pipeline fresh.
  An older build never reads a newer version: a rollback is a human's call.
- **A stale version is pruned, not crash-looped on.** `Checkpoint::stale_version` reads a
  header alone and says whether the build can never read it (older, not in `READS`). On a plain
  restart, when every retained checkpoint is refused for that reason and no other, `restore`
  deletes them and starts fresh, as `--fresh` would. `--restore-epoch` still fails with a plain
  error; a newer version, or any other refusal (corruption, a layout drift, another plan), or a
  mix of both, is left alone and stays fatal, so an unrelated bug is never masked as "just an
  old version".
- **0.1.0 starts fresh.** The first public release reads no checkpoint written by a build
  before it: format versions of earlier, unreleased builds are not in `READS`.
- Encoding streams the body from the live state through the compressor, and restoring streams
  it back out of the decompressor, so neither the uncompressed body nor a second copy of the
  state is in memory for it (but see the copy at the cut, below).
- Large, rare accumulator variants keep their state behind a `Box`, so an accumulator is 32
  bytes, not 64; a quantile of an argument that an earlier quantile of the same window already
  samples reads that sampler (`Acc::QuantileOf`): samplers with one seed fed the same values
  hold the same samples. On market-data pipelines whose state is almost all quantile samplers
  (two per group: a median and a p95 of one argument), the two cut a thirty-minute state from
  218.7 MB to 23.7 MB, at about 0.7 s of encoding instead of 0.2 s.

## Amendment (2026-09-25): on the pod's volume by default; the object store optional
Pipelines run as StatefulSets with a PersistentVolumeClaim each, so the checkpoints go there:
no bucket, credentials or network hop on the checkpoint path.

- `--checkpoints` (or `BRRRRR_CHECKPOINTS`) takes a directory, and defaults to
  `/var/lib/brrrrr/checkpoints`: the claim is mounted at `/var/lib/brrrrr`. Its layout is the
  object store's: `<pipeline>[@<sinks>]/<hash>/<epoch:020>.ckpt`, the last 5 kept.
- The store is a four-call trait (`crates/brrrrr/src/store.rs`: list, get, create-if-absent,
  delete), blocking like the rest of the data thread. A directory creates a checkpoint by
  writing a hidden temporary file, syncing it, hard-linking it to its name (which fails if the
  name exists) and syncing the directory. That keeps both properties the design rests on: a
  checkpoint exists iff it is complete, and exactly one of two instances creating an epoch
  succeeds (the lease), on any POSIX file system, NFS included. Hidden files are not listed;
  a crashed write's temporary file is removed by the collector after 10 minutes.
- A directory that cannot be written stops brrrrr at start (it writes a probe), naming the
  fix (`securityContext.fsGroup`), instead of failing at its first checkpoint after recovery.
  So does one that cannot create a file only if its name is free: the probe is also created
  twice by a hard link, the second must be refused (CIFS/SMB volumes such as Azure Files, some
  FUSE-based CSI drivers and 9p shares refuse hard links, and the claim would fail after
  recovery, then be retried for ever). An object store is checked the same way at start: a
  probe object created twice, the second refused, so a server that ignores `If-None-Match` is
  refused. Probes have hidden names ending in `.tmp`: a crash leaves nothing listed, and a
  directory's collector removes them like any temporary file.
- The object store is a cargo feature, `object-store`, off by default. The default image has
  no object_store, reqwest or AWS code and refuses `s3://` URLs, naming the image built with
  `FEATURES=object-store`, which takes them. Both
  have tokio: the Kafka admin calls at start (`replay_horizon`, `create_topics`) block on a
  current-thread runtime.
- Verified by: unit tests of the trait's contract on a directory and on `memory:///`
  (concurrent creates of one name, crashes mid-write, a directory emptied while open, reopening
  on the same volume), of restore, fallback, `--restore-epoch`, the lease and collection on a
  directory; acceptance scenarios that run every pipeline with `BRRRRR_CHECKPOINTS` as a pod
  sets it, move the volume between runs and empty it under a running instance; and an image
  smoke test of the default directory with and without a mounted volume, in both images.

## Amendment (2026-09-25): failed checkpoint writes
In a live deployment, pods exited about a minute after claiming their pipeline: a checkpoint PUT to MinIO
failed with `Error performing PUT ... in 30.0s - HTTP error: error sending request`, whatever its
size, while MinIO was idle.

- **Cause:** the checkpoint collector shared the object store, and with it one connection pool,
  from a current-thread tokio runtime of its own. Such a runtime drives its connections only
  inside `block_on`, and the collector's ran only after the next checkpoint. A checkpoint that
  picked up a connection the collector had pooled waited for that runtime until the client's
  30 s timeout.
- **Fix:** the object store (`store::Remote`) owns one runtime with one worker thread. It serves
  every call, from the data thread and the collector alike, and drives the pooled connections
  between calls. `the_data_thread_and_the_collector_share_connections_against_s3` reproduces
  the crash against a real S3 server (adobe/s3mock in CI): with a runtime per calling thread it
  fails at about the tenth checkpoint, with the fix it writes all 40.
- **Deadlines:** every object store call has one of its own, `max(30 s, size at 4 MiB/s)`: a
  flat 30 s cut 190 MB checkpoints short. The client's own timeout is set out of the way (1 h).
  Past its deadline a call is dropped and fails. Directories have no deadline: a blocked write
  to a volume cannot be cancelled.
- **A failed write is not fatal.** It is logged (throttled), counted in
  `brrrrr_checkpoint_failures_total` and tried again an interval later under the same epoch
  (`run::Lease`):
  - The failed write may have landed, or a standing-by instance may have claimed that epoch.
    Either way the retry finds the epoch taken and this instance stops, and its restart restores
    whichever checkpoint is there.
  - Moving on to the next epoch instead would let this instance run next to one that claimed the
    failed epoch.
- **What waits for a written checkpoint:** until one is written, the lease is not renewed, no
  old checkpoint is collected, no epoch is released, and no offsets are committed. Once the
  lease is `--takeover / 2` old (the fence), nothing more is produced, until a checkpoint renews
  it. This holds whatever made the lease old, not only a failure.
- **Still fatal:**
  - an epoch found taken, whether at the claim or on a retry;
  - a failed sink delivery;
  - a failed claim at start;
  - a failed last checkpoint on SIGTERM, which exits with an error and without a release, so
    the restart replays.
- **Health:** `/health` still reports only a stalled data loop. A paused instance keeps its loop
  running, and a restart would not help it. Alert on `brrrrr_checkpoint_age_seconds` above
  3 × `--interval` (the default `--takeover`, when a standby may take over), and on any increase
  of `brrrrr_checkpoint_failures_total`.
- **Tested:**
  - unit tests of `Lease`, including that written epochs stay consecutive whatever fails;
  - classification of `Taken` against `Failed`;
  - a hanging call failing at its deadline;
  - the store's contract against S3;
  - the stall regression;
  - acceptance scenarios: writes failing for a while without stopping brrrrr or costing
    exactly-once, and a pipeline killed repeatedly with its checkpoints in S3.
- **Found on the way:** s3mock's conditional PUT is not atomic, and several of 8 simultaneous
  creates of one name succeed. The lease rests on the server's `If-None-Match` being atomic,
  which S3 and MinIO guarantee. The S3 tests therefore check conditional creates one at a time,
  and the one-winner race is tested on directories, where brrrrr's own hard link provides it.
  One at a time, s3mock (5.2.3) does refuse a create of an existing name (412), so it passes
  the startup check; no check at start can prove the refusal atomic.

## Amendment (2026-09-27): state that stays bounded for DuckDB-style aggregates
SQL written for DuckDB needs state Proton's did not: `VAR_SAMP` and `STDDEV_SAMP`
(`Moment::Variance`, `Moment::Stddev` on `Acc::ShiftedMoments`), trade-sequence aggregates
(`Acc::TradeReturns`, `Acc::DistinctStats`, holding a `Sequence` like `Acc::RunStructure`, with
`agg::Tie` in their keys), and `OpState::Hold`, the rows a view's held sort (`SETTINGS
order_hold_ms`, ADR-0013) holds.

- Every new state is a fixed-size summary, whatever the window's length (day-long windows run
  live). A first version also kept every trade of a window for a percentile of large-trade
  reversals: replaying an hour of exchange trades grew one pipeline's checkpoint to 313 MiB and
  its RSS to 1.95 GiB (0.6 MiB and 64 MiB with Proton's SQL). That column was dropped: no
  bounded state reproduces it (a sample of the trades, or a histogram of their notionals, was
  off by 27-158% in the windows it could not keep whole).
- A `Sequence` no longer sorts its window's trades: its input is sorted by time once per source
  (ADR-0013), and it holds only the trades of the newest time, to order ties its own way.
  Sorting per aggregate had needed a buffer of up to 2048 trades and 5 s per group of every
  window: bursts on a live feed put trades up to 535 places behind their sorted place, and the
  buffers left RSS at 4-5x Proton's. `SEQUENCE_REORDER` (2048) now caps the trades sharing one
  time (the largest burst seen: 535).
- `orderbook_top_n`'s books, their replica guards and kept diffs are `OpState::Book`.

## Amendment (2026-10-01): checkpoints written off the data thread
In a live deployment, `brrrrr_checkpoint_duration_seconds` ran from 0.06 s to 1.75 s (a 55 MB state),
and all of it was time the data loop consumed nothing: every `--interval` it flushed the
producer, encoded the live state and wrote the store before reading another message. A window
due to close waited out the rest of the stall, and across nine instances emit p99 tracked the
checkpoint's duration (Spearman 0.82; 0.93 with p90).

- **The cut, on the data thread:** between two batches, a copy of the engine state
  (`Checkpoint::of`, as `Engine::snapshot`) and of the source positions, and the producer moves
  to its next *generation*. The loop then goes on.
- **The checkpoint's thread:** waits until every message of the cut's generation and before is
  acked, polling the producer itself (librdkafka reports deliveries to whoever polls), for at
  most 30 s as the flush did. It then takes the sink offsets from those acks alone, encodes the
  copy (`Checkpoint::try_encode`: the same bytes as `encode_of`) and creates the checkpoint. Only
  one is written at a time: no cut is taken while one is in flight. The data thread applies the
  outcome (`run::step`) once the thread is done: written, failed (tried again an interval later
  under the same epoch, as before), or its epoch taken (the instance stops).
- **One cut (ADR-0007):** every message carries its generation as librdkafka's delivery opaque,
  and `run::Ledger` keeps the acks by generation. A cut's sink offsets are those after the
  highest offset acked among each partition's messages sent before it, never one sent after it:
  a restart from the checkpoint would replay that message and find it unsuppressed. The
  idempotent producer keeps each partition's messages in the order they were sent, so these
  offsets are exactly what was produced before the cut. A message sent before the cut that
  failed, or was not acked within 30 s, stops the instance without the checkpoint, as the flush
  did.
- **The lease is unchanged.** A written checkpoint renews it from its cut. While a write is in
  flight the loop produces under the previous lease and pauses at the fence as before, so a
  lease lost meanwhile lets nothing through: what was sent went out under a lease that a
  standing-by instance waits out. An epoch found taken stops the instance as soon as the thread
  reports it.
- **SIGTERM:** the loop waits for a checkpoint in flight, then cuts the last one and waits for it
  too. Nothing is produced after the last cut, which is released once written. The last one
  copies nothing, since nothing runs beside it: the loop waits for its acks and encodes it from
  the live state, and its thread only writes it. A source topic found grown (its new partitions
  are only assigned at start) restarts the instance from the next checkpoint written, except on
  a stop signal: the checkpoint in flight then leads to the last one, not to an error, and the
  restart consumes the new partitions all the same (found in review).
- **A state too large to copy:** once a checkpoint encodes to more than 128 MiB
  (`run::COPY_CHECKPOINT_UP_TO`), the next ones are taken as before this amendment: the loop waits for the
  acks and encodes from the live state, and only the write leaves it. The copy measures about
  1.4 times the encoded checkpoint on a 55 MB state at a live feed's rates, and a stall
  costs less than a pod over its memory limit.
- **Metrics:** `brrrrr_checkpoint_duration_seconds` now runs from the cut until the checkpoint
  is written. `brrrrr_checkpoint_stall_seconds` is how long the cut held the loop up.
- **Cost: the copy.** Encoding streamed from the live state so as not to hold a copy; the copy
  is back, for as long as it takes to encode it. Measured by `benches/memory.rs` (`cut_cpu`,
  `cut_peak`) at a live feed's rates, against encoding on the data thread (`ckpt_cpu`, `ckpt_peak`),
  the flush and the write not counted:

  | pipeline | state | data loop held up | heap on top of the state while checkpointing |
  | --- | --- | --- | --- |
  | trade-size distributions | 75.6 MB | 0.406 s → 0.038 s | 50.8 MB → 103.2 MB |
  | price ranges | 72.2 MB | 0.326 s → 0.041 s | 14.1 MB → 62.6 MB |
  | slippage (trades as-of quotes) | 44.8 MB | 0.166 s → 0.030 s | 11.8 MB → 41.4 MB |
  | order flow | 21.1 MB | 0.055 s → 0.028 s | 1.7 MB → 18.4 MB |
  | price impact | 10.6 MB | 0.026 s → 0.012 s | 1.7 MB → 8.2 MB |

  The stall still grows with the state, at about a tenth of the encode's rate. Not done: state
  that is incremental or copy-on-write (the other option considered), which would make the cut
  independent of the state's size and drop the copy.
- **Tested:**
  - `Ledger` against every interleaving of sends, acks and cuts (a property test), and by
    example: a cut that waits for its own acks and not later ones, takes no offset of a later
    message, and does not settle past a failed delivery.
  - `finish` on a directory: it writes the cut's state with its acks' offsets, times out without
    writing, does not write after a failed delivery, and reports a taken or failed write.
  - A checkpoint written on its thread while another thread feeds the engine and acks later
    messages: it restores to the cut and continues as the running engine did.
  - A copy taken at the cut encodes, byte for byte, to that cut's checkpoint after the engine
    took the rest of every pipeline's input (`tests/checkpoint.rs`).
  - `tests/acceptance/features/checkpoint.feature`:
    - on a volume, a large state (`heavy.sql`): its checkpoints hold the loop up for less than a
      quarter of their duration; it stays exactly once when killed at any time, on SIGTERM, and
      on a standby's takeover; with aligned consumption, it catches up exactly.
    - a source topic that gains a partition restarts brrrrr from its checkpoint;
    - against S3 behind a proxy that holds every request: windows keep reaching their sink
      while a write hangs (on `main`, 112 of 350 did); a hang past the fence pauses producing
      until the write lands; it stays exactly once when killed mid-write and on SIGTERM
      mid-write; it yields to a standby that took over while its write hung; and SIGTERM while
      a checkpoint is in flight after a source grew stops cleanly after a last checkpoint (with
      the growth acted on during the stop, it exited with an error).
  - `finish` writes a checkpoint encoded on the data thread as it is, waiting for no ack;
    `copies_state` copies up to 128 MiB and no more.

## Amendment (2026-10-02): a batch's messages go out view by view
The data loop no longer produces a batch's messages once every view has run: the engine writes
to an `engine::Output` that is flushed each time a view has written a sink, and the loop's
(`run::Producing`) produces on each flush. A row closing a pipeline's 15s to 1h windows sent the
15s bars only after the 1h close (~0.5 s); they now go before the views after them run.

- Unchanged: what is produced and in what order (flushes keep the order of the pushes), the
  suppression, withholding and lease checks of each message (`produce`), and the cut: checkpoints
  are still cut between batches, so a cut's generation covers every message produced before it.
- A failed `produce` stops the output for good: what it had not sent is gone, so nothing pushed
  after it is sent either, and the loop stops on the error after the chunk, as it stopped after
  the batch before. A stopped instance may have sent part of a batch; it could before (a batch's
  messages were sent one by one), and the restart's suppression covers it either way.

## Amendment (2026-10-03): `quantile_cont`, DuckDB's interpolated quantiles
DuckDB's `MEDIAN` and `PERCENTILE_CONT` blend the two values around a rank; `quantile_t_digest`
returns one of them. On a live feed a median trade amount computed with `quantile_t_digest`
differed from DuckDB's in 56% of windows, by up to 10x in windows of a handful of trades (two
trades of 132.73 and 16.41 USD: 16.41 where DuckDB has 74.57), and a slippage median in basis
points could take the other sign.

An appended variant, `Acc::Cont` (`quantile_cont(level)(x)`), and `agg::Cont`, which only it
reaches:

- `Cont::Exact`: a window's first `CONT_EXACT` (256) values as they are, 8 bytes each. The
  result is DuckDB's to the bit (`tests/quantile_cont.rs`, against what DuckDB 1.5.5 returns).
- `Cont::Digest`: from the 257th value on, the t-digest `quantile_t_digest` keeps of the same
  values, so the state is as bounded as its state is, whatever the window's length. It is read
  as `PERCENTILE_CONT` reads values (each centroid's mean at the middle of the ranks it holds),
  not as ClickHouse reads it: single-value centroids, which is all of them until the digest's
  error bound covers two (200 values at the median, about 1,050 at the 95th percentile), give
  DuckDB's result of the values as Float32.
- 256 because a digest at the median starts merging values at 200: a window has left the exact
  buffer before its digest gives any value up. A full buffer is 2 kB per open group, as much as
  the digest's own buffer of unmerged values (`BUFFER`, 512 Float32, ADR-0014); the 257 values
  a spill moves are unmerged until the digest's first merge at 512 or its first read. Measured on an hour of
  exchange trades and quotes, against `quantile_t_digest` on the same build.

## Amendment (2026-10-05): the lease is the pipeline's, not its revision's
The checkpoints of a changed SQL or another `--asof` are elsewhere (`<pipeline>[@<sinks>]/<hash>/`),
and so was the lease: two overlapping instances of two revisions of one pipeline each claimed
their own, and both wrote its sinks. Each epoch, a
claim's or a checkpoint's, is now first taken as `<pipeline>[@<sinks>]/<epoch:020>.lease`
(create-if-absent, holding the instance's name), then written as the checkpoint under its
revision; a SIGTERM's release marker is written beside the leases. An instance stands by while
the newest epoch of any revision (or a revision's checkpoint a build before leases wrote) is
fresh, and claims the epoch after the newest it found free. Epochs are therefore shared by every
revision of a pipeline: a changed SQL claims after the old revision's, not epoch 1. A trial instance
(`--sink-topic-prefix` or `-template`) writes other sinks and keeps a lease of its own.
Tested by `of_two_revisions_of_a_pipeline_only_one_holds_an_epoch` and the overlapping
revision and `--asof` scenarios of `kafka.feature`. Not done: pipelines that write the same sinks
under different SQL file names, or templates that name the same topics, are still separate
owners.

## Amendment (2026-10-05): what a start without a checkpoint withholds is in the checkpoint
A start without a checkpoint withholds the windows that began before its sources lost records,
the windows that ended before a sink topic lost its own, and awaits a source that had no record
yet (`--partial-windows`, `--unverified-history`, ADR-0007). The runtime kept that beside the
checkpoints, in an object it deleted and created again when an awaited source delivered its
first record: a crash between the two lost it while a checkpoint remained, and a restart from
that checkpoint withheld nothing. It was also written before the claim, by an instance that
might lose it. `Checkpoint::withhold` now carries it: every checkpoint, the claim's first, as of
its cut. A source's first record raises it in memory only: a restart from a checkpoint cut
before that record reads the record again.
