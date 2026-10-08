<div align="center">

# brrrrr

**SQL for tick data.** Query Parquet, CSV and JSON where they are, on disk or in S3, GCS and Azure.
Bars, as-of joins, VWAP and rolling windows in plain SQL. Serve live tables over HTTP and the
PostgreSQL protocol. Run the same SQL over history and as data arrives.

[Website](https://aperiodic-io.github.io/monotile/) ·
[Docs](https://aperiodic-io.github.io/monotile/docs/) ·
[Cookbook](https://aperiodic-io.github.io/monotile/docs/cookbook.html) ·
[Benchmarks](bench/sql/README.md)

[![CI](https://github.com/aperiodic-io/monotile/actions/workflows/ci.yml/badge.svg)](https://github.com/aperiodic-io/monotile/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

</div>

```sql
-- one-minute bars from a day of trades in S3, no loading, no schema
SELECT time_bucket('1m', ts) AS minute, symbol,
       first(price, ts) AS open, max(price) AS high, min(price) AS low,
       last(price, ts) AS close, sum(size) AS volume
FROM 's3://my-bucket/trades/date=2024-01-02/*.parquet'
GROUP BY minute, symbol;

-- every trade with the quote it met, and what it paid against the mid
SELECT t.ts, t.symbol, t.price, q.bid, q.ask,
       (t.price - (q.bid + q.ask) / 2) / ((q.bid + q.ask) / 2) * 1e4 AS slippage_bps
FROM 'trades.parquet' t
ASOF JOIN 'quotes.parquet' q ON t.symbol = q.symbol AND t.ts >= q.ts;
```

## Install

```bash
pip install brrrrr                                              # Python, and the brrrrr-sql shell
curl -fsSL https://aperiodic-io.github.io/monotile/install.sh | sh  # the binary (Linux, macOS)
docker run --rm -it --user "$(id -u):$(id -g)" -v "$PWD:/data" -w /data ghcr.io/aperiodic-io/monotile sql
```

One binary, no dependencies, no server to start for a query.

## Use it

**From the shell.** `brrrrr` opens an SQL shell; `brrrrr sql "..."` runs a statement and prints a
table, CSV (`--format csv`) or JSON lines, or writes Parquet (`-o bars.parquet`).

```
$ brrrrr sql "SELECT symbol, count(*) AS trades, vwap(price, size) AS vwap FROM 'trades.csv' GROUP BY symbol ORDER BY symbol"
┌────────┬────────┬─────────────┐
│ symbol │ trades │ vwap        │
├────────┼────────┼─────────────┤
│ BTC    │    827 │ 42202.06861 │
│ ETH    │    887 │ 2300.399444 │
│ SOL    │    851 │ 98.67539508 │
└────────┴────────┴─────────────┘
3 rows
```

(`trades.csv` is in [fixtures/cookbook](fixtures/cookbook); try it with
`brrrrr sql "FROM 'https://raw.githubusercontent.com/aperiodic-io/monotile/main/fixtures/cookbook/trades.csv' LIMIT 5"`.)

**From Python.** Results go to Polars, pandas and pyarrow as Arrow (the Arrow C stream
interface), and a DataFrame in a variable is a table by its name.

```python
import brrrrr, polars as pl

trades = pl.read_parquet("trades.parquet")
bars = brrrrr.sql("""
    SELECT time_bucket('5m', ts) AS t, symbol, vwap(price, size) AS vwap
    FROM trades GROUP BY t, symbol
""").pl()
```

**As a server.** `brrrrr serve` holds today's rows in memory and the rest in Parquet by day, and
answers one query over both: over HTTP (with a console in the browser), the PostgreSQL protocol
(psql, Grafana, any driver), and subscriptions. Rows arrive over HTTP or from Kafka.

```bash
brrrrr serve --data ./data &                  # http://127.0.0.1:4242 and postgresql://127.0.0.1:5433
curl -X POST 'localhost:4242/write/trades?format=csv' --data-binary @trades.csv
curl -X POST localhost:4242/query --data "CREATE LIVE VIEW bars AS
  SELECT time_bucket('1m', ts) AS minute, symbol, last(price, ts) AS close, sum(size) AS volume
  FROM trades GROUP BY minute, symbol"
psql -h localhost -p 5433 -c "SELECT * FROM bars ORDER BY minute DESC LIMIT 10"
```

A live view runs its query over the rows so far, then as each row arrives: the same answer the
query gives over the table, emitted as each bar closes.

## Why brrrrr

- **The data where it is.** Parquet, CSV (gzipped too), JSON lines; files, globs, Hive-partitioned
  directories; S3 (and MinIO, R2), GCS, Azure, HTTP. No load step, no proprietary format.
- **The time-series vocabulary.** `time_bucket` (in any time zone), `first`/`last`, `vwap`,
  `twap`, `ASOF JOIN` (strict, with a tolerance, or at an offset for markouts), gap filling with
  `locf` and `interpolate`, `ema`, rolling windows, `lag`, exact percentiles, `count(DISTINCT)`,
  `FILTER`: the queries quants and engineers run, [in the cookbook](fixtures/cookbook/cookbook.sql),
  each tested against DuckDB's answer.
- **One semantics, live and historical.** An ad-hoc query, a live view and a streaming pipeline
  are the same engine running the same plan ([ADR-0018](docs/adr/0018-ad-hoc-sql-as-engine-views.md)).
  A backfill gives what live would have given.
- **Fast on time series.** On a day of 10M trades and 20M quotes, one-minute bars and hourly VWAP
  run 1.6x faster than DuckDB and TCA per symbol 1.2x; plain group-bys and filters are on par
  or slower ([bench/sql](bench/sql/README.md)).
- **Exactly-once streaming.** `brrrrr run` runs the same SQL as a pipeline from Kafka topics of
  JSON (or Apache Iggy) to Kafka, with checkpoints, surviving `kill -9` without a lost or repeated
  message ([an example](examples/pipelines)).
- **Errors that say what to do.** What brrrrr cannot run is refused with the reason and what to
  write instead, never run as something else.

## How it compares

| | brrrrr | kdb+ | DuckDB | QuestDB |
| --- | --- | --- | --- | --- |
| Query files and object stores in place | yes | Parquet module | yes | no (ingest first) |
| As-of joins, bars, VWAP, rolling windows | yes | yes (q) | partly | yes |
| The same SQL live and over history | yes | in q, by hand | no (batch) | separate paths |
| Server: HTTP, PostgreSQL protocol, subscriptions | yes | IPC, q | no | yes |
| Exactly-once Kafka pipelines | yes | by hand | no | no |
| Licence | Apache-2.0 | proprietary | MIT | Apache-2.0 |

[More, with where each is the better choice.](https://aperiodic-io.github.io/monotile/compare.html)

## Documentation

- [Getting started](https://aperiodic-io.github.io/monotile/docs/) and the [SQL reference](https://aperiodic-io.github.io/monotile/docs/sql.html)
- [Cookbook](https://aperiodic-io.github.io/monotile/docs/cookbook.html): bars, VWAP, TCA, markouts, volatility, order flow, data quality, IoT
- [Python](https://aperiodic-io.github.io/monotile/docs/python.html) · [Server](https://aperiodic-io.github.io/monotile/docs/server.html) · [Files and object stores](https://aperiodic-io.github.io/monotile/docs/files.html) · [Streaming pipelines](https://aperiodic-io.github.io/monotile/docs/streaming.html)
- Design: [ADRs](docs/adr/) and the [roadmap](docs/ROADMAP.md)

## Status

The streaming engine and `brrrrr run` are the oldest and most tested parts (crash and exactly-once
acceptance tests in CI). The SQL shell, the Python package and the server are new: their
interfaces may still change before 1.0.
What comes next, and what never will: the [roadmap](docs/ROADMAP.md).

## Contributing

Issues, ideas and pull requests are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md). Report
security problems privately: [SECURITY.md](SECURITY.md).

## License

[Apache-2.0](LICENSE). See [NOTICE](NOTICE) for the works brrrrr builds on.
