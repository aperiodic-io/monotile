# brrrrr vs QuestDB 10, one CPU core each

The same two continuous queries over the same 5 million trades, with each engine held to a single
core:

- **bars:** one-minute OHLCV plus VWAP per symbol. For brrrrr this is `tumble` with
  `EMIT AFTER WINDOW CLOSE`; for QuestDB, a materialized view with `SAMPLE BY 1m`.
- **rolling:** one output row per trade, carrying five per-row window functions:
  - a 100-row moving average;
  - a one-minute `RANGE` max;
  - an EMA (α = 0.1);
  - `price - lag(price)`;
  - a daily cumulative volume.

  For brrrrr this is `OVER`, from `rolling.sql`; for QuestDB, a live view. QuestDB 10 introduced
  live views for this shape of query.

Both engines produce the same numbers. On 100,000 trades, brrrrr's Kafka output and QuestDB's views
agree column for column (sums equal to the last digit). `crates/brrrrr-core/tests/over.rs` and
`tests/aggregates.rs` hold brrrrr's functions to QuestDB's results row by row.

## Results

5,000,000 trades (1,000 symbols, skewed; 83 minutes of event time, in order). Each figure is the
median of 3 runs, and runs varied by less than 5%. Throughput is trades divided by wall-clock time.

| | wall | CPU (s) | trades/s | memory peak |
| --- | --- | --- | --- | --- |
| **bars**, brrrrr: Kafka → brrrrr → Kafka | **7.3 s** | 6.7 | **690k** | 65 MB RSS |
| **bars**, QuestDB: ILP → table + materialized view | 8.0 s | 7.9 | 620k | 1.3 GB cgroup¹ |
| **rolling**, brrrrr: Kafka → brrrrr → Kafka | 27.9 s | 27.0 | 180k | 85 MB RSS |
| **rolling**, QuestDB: ILP → table + live view | **18.5 s** | 18.3 | **270k** | 2.0 GB cgroup¹ |
| *QuestDB ILP ingest alone (no views)* | 6.9 s | 6.8 | 725k | 1.2 GB cgroup¹ |

¹ The container's cgroup peak, which includes the page cache of the files QuestDB wrote. It
cannot be compared with brrrrr's RSS. QuestDB's JVM ran with its default heap.

Where the time goes:

- **QuestDB, views over the already-stored table** ("backfill": the view is created after ingest):
  - bars: 1.2 s;
  - rolling: 10.4 s.
- **QuestDB, the same queries as plain batch SQL** over the table:
  - bars: 0.47 s;
  - rolling: 2.0 s.
- **brrrrr's engine alone.** This is in-process, from protobuf bytes in memory to JSON strings, with
  no Kafka:
  - bars: 2.8 s, of which protobuf decode is 1.5 s;
  - rolling: 14.6 s, of which decode is 1.6 s.

  The remaining CPU is librdkafka's:
  - consuming 5M messages costs about 3.8 s;
  - producing 5M messages about 8.6 s.

Reading the table:

- **brrrrr is ahead on windowed aggregates.** It needs about 1.3 µs of CPU per trade from Kafka
  to Kafka. QuestDB's materialized view adds only about 1.1 s to its own ingest.
- **QuestDB is ahead on per-row output**, by 1.5×. It appends each output row to a column
  store. brrrrr formats each output row as a JSON message and produces it to Kafka, which is 5M
  messages. brrrrr's engine and JSON take 14.6 s alone. Callgrind, on 100k trades, splits that
  time as follows:

  | where | share |
  | --- | --- |
  | JSON formatting | 44% |
  | └ floats, through `format!`, which allocates | 30% |
  | └ the `to_start_of_day(...)` partition key, formatted as text for every row | 12% |
  | the window functions themselves | 22% |
- **The batch queries show the headroom that incremental maintenance leaves.** QuestDB answers
  the whole rolling query in 2.0 s as batch SQL, but its live view needs 10.4 s over the same
  rows.
- **Memory:** brrrrr's whole process peaks at 65–85 MB.

### Before this PR, brrrrr replayed at ~115k trades/s whatever the query

The first run gave:

| workload | wall | CPU |
| --- | --- | --- |
| bars | 42.5 s | 7.2 s |
| rolling | 43.8 s | 27.4 s |

The process was idle most of the time. librdkafka stops fetching a partition once
`queued.min.messages` (100,000) messages are queued. It then waits `fetch.queue.backoff.ms`,
1 s by default, before fetching again, although the queue drains in about 0.15 s. brrrrr now
defaults that backoff to 10 ms (`CONSUMER_DEFAULTS` in `crates/brrrrr/src/run.rs`), and
`--source-kafka-config` still overrides it. The 5M-trade bars replay went from 42.5 s to 7.3 s.
Any catch-up after downtime had the same cap, per partition.

## What is and is not measured

- **The engine under test gets core 1:**
  - brrrrr through `taskset -c 1`, which covers every thread, librdkafka's included;
  - QuestDB through `docker run --cpuset-cpus=1 --memory=8g`, with its default configuration. With
    one CPU visible, QuestDB sizes its worker pools for one CPU.
- **Everything else runs on other cores:**
  - Redpanda on core 2 (`--smp 1`);
  - the load generator and the poller on core 3.

  Redpanda's CPU for serving brrrrr's reads and writes is *not* counted. QuestDB's storage work
  is counted, since it is QuestDB.
- **brrrrr: a replay.**
  - All 5M messages are in the topic (1 partition) before brrrrr starts.
  - The time runs from process start until the last output reached the sink topic, as its high
    watermark.
  - It includes startup (≈0.5 s to the first row), a checkpoint every 10 s to a local directory,
    and an idempotent Kafka producer.
  - For bars, the last minute of each symbol stays open (no later row closes it), so 82,999 bars
    are emitted.
- **QuestDB: live ingest.**
  - The table and view are created first. Then the ILP text (`trades.ilp`, 5M lines) is streamed
    over TCP to port 9009 as fast as QuestDB accepts it.
  - The time runs until `count()` on the table and on the view (the sum of `trades` for bars)
    reaches 5M.
  - The poll runs every 100 ms, and its queries run on QuestDB's core too.
- **Output durability differs:**
  - brrrrr's outputs are Kafka messages, acknowledged by the broker.
  - QuestDB's are rows in its own tables (WAL tables, QuestDB's default commit mode).
- **Not measured:**
  - Latency under a steady rate. brrrrr emits a bar 50 ms after its minute closes; QuestDB's
    views refresh after WAL apply, and the live view flushes every 1 s.
  - More than one core, and more than one partition.
- **QuestDB 10's live views require an `ANCHOR` for every unbounded window:** EMA, `lag`, and
  running sums. The `ANCHOR` is accepted only on a named `WINDOW`, and only inside a live view;
  batch SQL rejects it. The rolling query therefore resets EMA, `lag` and the running sum daily
  in both engines (`timestamp_floor('1d', ts)` / `to_start_of_day(ts)` in the partition). The
  83 minutes of data sit within one day.

Host: 4 vCPU Intel Xeon @ 2.10 GHz, 15 GB RAM, Linux 6.18, Docker 29.3. Images:
`questdb/questdb:10.0.1` and `redpandadata/redpanda:v24.2.7`.

## Running it

```sh
cargo build --release -p brrrrr
pip install confluent-kafka
python3 bench/questdb/gen.py 5000000 /tmp/qb        # trades.pb (Kafka) and trades.ilp (QuestDB)
python3 bench/questdb/bench.py /tmp/qb 5000000      # --reps 3, --only brrrrr|questdb
```

`bench.py` does the following:

- starts and removes its own containers (`bench-redpanda`, `bench-qdb`) on the host network;
- needs ports 19092, 9000 and 9009 free;
- prints each run and the medians, and appends the runs to `<dir>/results.jsonl`.

These environment variables adjust it:

- `ENGINE_CORE`, `BROKER_CORE` and `LOAD_CORE` move the pinning;
- `BRRRRR` points at another binary;
- `BRRRRR_ARGS` adds `brrrrr run` flags, for example
  `--source-kafka-config fetch.queue.backoff.ms=1000` for the old behaviour.
