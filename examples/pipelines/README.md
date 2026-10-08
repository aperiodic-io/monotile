# Example pipelines

[`bars.sql`](bars.sql) turns trades on a Kafka topic into one-minute OHLCV bars on another,
exactly once: a crash or restart neither loses a bar nor writes one twice. Each trade is a JSON
object; each bar is one too. Its view is plain `brrrrr sql` SQL, so the same query over a file of
trades gives the same bars: a backfill and the live pipeline agree. See [Streaming pipelines](https://aperiodic-io.github.io/monotile/docs/streaming.html)
for what `brrrrr run` does and the SQL it takes.

## Run it

You need Docker, and `brrrrr` (`curl -fsSL https://aperiodic-io.github.io/monotile/install.sh | sh`,
or the image, below). Run the commands from the repository's root.

Start Redpanda (any Kafka-compatible broker works) and create the two topics. `rpk`, Redpanda's
CLI, runs inside its container:

```sh
docker run -d --name redpanda -p 19092:19092 redpandadata/redpanda:v24.2.7 \
  redpanda start --mode dev-container --smp 1 --kafka-addr 0.0.0.0:19092 --advertise-kafka-addr localhost:19092
rpk() { docker exec -i redpanda rpk -X brokers=localhost:19092 "$@"; }
rpk topic create trades bars.1m
```

Start the pipeline. It keeps its checkpoints in `./checkpoints`; stop it with Ctrl-C, which
takes a last checkpoint, and start it again the same way to carry on where it stopped:

```sh
brrrrr run examples/pipelines/bars.sql --checkpoints ./checkpoints --partial-windows
# or with the image:
docker run --rm -it --network host --user "$(id -u):$(id -g)" -v "$PWD:/w" -w /w ghcr.io/aperiodic-io/monotile \
  run examples/pipelines/bars.sql --checkpoints /w/checkpoints --partial-windows
```

`--partial-windows` is there because these trades are from 2024 but their Kafka records are
stamped now: on a first start brrrrr does not write the windows that began before a source's
oldest record (part of their input may be missing), judged by the records' timestamps. When your
producer stamps records with their trades' times, leave it out.

In another terminal, produce the trades of [`trades.jsonl`](trades.jsonl). A time is ISO 8601
text, or a number (epoch milliseconds for this `datetime64(3)` column):

```sh
rpk topic produce trades < examples/pipelines/trades.jsonl
```

The last trade, at 09:32, closes the 09:31 bars. Read the bars:

```sh
rpk topic consume bars.1m --num 4 --format '%v'
```

```json
{"minute":"2024-01-02 09:30:00.000","symbol":"BTC","open":42011.5,"high":42020,"low":42011.5,"close":42020,"volume":0.75,"trade_count":2}
{"minute":"2024-01-02 09:30:00.000","symbol":"ETH","open":2301.25,"high":2301.25,"low":2301.25,"close":2301.25,"volume":2,"trade_count":1}
{"minute":"2024-01-02 09:31:00.000","symbol":"BTC","open":41998.25,"high":41998.25,"low":41998.25,"close":41998.25,"volume":1,"trade_count":1}
{"minute":"2024-01-02 09:31:00.000","symbol":"ETH","open":2299.75,"high":2299.75,"low":2299.75,"close":2299.75,"volume":1.5,"trade_count":1}
```

The same query over the file, as a backfill would run it, gives the same bars, and the 09:32 one
the pipeline is still waiting to close (`-t` names the file as the table `trades`):

```sh
brrrrr sql -t trades=examples/pipelines/trades.jsonl "
  SELECT time_bucket('1m', time) AS minute, symbol,
         first(price, time) AS open, max(price) AS high, min(price) AS low,
         last(price, time) AS close, sum(quantity) AS volume, count(*) AS trade_count
  FROM trades GROUP BY minute, symbol ORDER BY minute, symbol"
```

Its Prometheus metrics are at http://localhost:9464/metrics (`brrrrr_received_events_total`,
`brrrrr_sent_events_total`, `brrrrr_decode_errors_total` for messages that are not a trade).

Clean up with `docker rm -f redpanda && rm -r checkpoints`.

## Make it yours

- Your topics and brokers: `topic` and `brokers` in the SQL, or `--brokers host:9092` for all.
- Your fields: declare the columns your messages have, by name. Fields not declared are ignored;
  a declared one that is missing is the column's default (`0`, `''`), or NULL for
  `nullable(...)` columns.
- Another interval: `time_bucket('5m', time)`; another bar: a second view into a second sink.
- Filter with `WHERE`, keep groups with `HAVING`, join quotes with `ASOF JOIN`: the view takes
  what `brrrrr sql` takes, as long as its tables are streams read in time order (see the docs).

## Parquet files in and out

[`parquet.sql`](parquet.sql) is the same pipeline over Parquet files: it reads the trades under
`trades/` (and the files that land there later, in name order) and writes the bars under
`bars/`, a directory a day, exactly once. No broker is needed:

```sh
mkdir -p trades && brrrrr sql "COPY (FROM 'examples/pipelines/trades.jsonl') TO 'trades/2024-01-02.parquet'"
brrrrr run examples/pipelines/parquet.sql --checkpoints ./checkpoints --idle-close 1
brrrrr sql "FROM 'bars/' ORDER BY minute, symbol"   # in another shell
```

`--idle-close 1` writes the last minute's bars once a second has passed with no new trade.
