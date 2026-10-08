# Roadmap

brrrrr is a SQL engine for tick data and other time series. It reads the files you already
keep (Parquet, CSV, JSON, on disk or in object stores), runs the same query live as rows arrive,
and runs it as an exactly-once pipeline from Kafka to Kafka. The aim: the research and real-time
work people do in kdb+ today, in SQL, over open files, from one binary or `pip install`.

Three releases, each a workflow done well before the next one starts. Status as of October 2026.

## v1: research on files

One person goes from `pip install brrrrr` to an as-of join over a day of trades and quotes in
minutes, on a laptop, on their own files.

| feature | status |
| --- | --- |
| Ad-hoc SQL over files, returning Arrow; partitions pruned by their keys | built: `brrrrr sql`, Python, the server; Hive partitions pruned before download |
| One dialect: DuckDB's and PostgreSQL's SQL plus the time-series functions, in queries and pipelines alike; Timeplus Proton's dialect accepted for pipelines | built |
| Dataset layout: Hive-partitioned directories, sorted by time within a partition | built: read and written (`COPY ... PARTITION_BY`); Iceberg and Delta read |
| Types: timestamps, signed and unsigned integers, doubles, text, bool | partly: timestamps in microseconds (nanoseconds read to the microsecond); decimals read as doubles |
| Time-series primitives: as-of joins (strict, tolerance, offsets for markouts), `time_bucket` with time zones, gap filling (`locf`, interpolation), `lag`/`lead`, `ema`, rolling frames, `vwap`/`twap`, exact percentiles, `count(DISTINCT)`, window joins | built |
| Python package: wheels for Linux, macOS and Windows; DataFrames as tables; Arrow, Polars and pandas out | built |
| Reading in place: CSV (kdb+ and epoch times), Parquet, JSON lines, Databento DBN | built |
| Streaming pipelines with JSON sources, no schema file needed | built |
| Packaging: release binaries, wheels, a Docker image, an install script | built |
| Docs: website, a cookbook of 40+ recipes checked against DuckDB, a notebook, a live dashboard example | built |
| Benchmarks against DuckDB, Polars and ClickHouse, reproducible from one script ([bench/sql](../bench/sql/README.md)) | built; QuestDB not yet |

## v2: real time and history in one query

A server that holds today's rows in memory, writes them to Parquet, and answers one query over
today and history.

| feature | status |
| --- | --- |
| `brrrrr serve`: live tables with a write-ahead log, daily Parquet, Kafka ingest | built |
| One query across today's rows and the files of earlier days | built |
| Subscriptions: a table's new rows and a query's changing answer, as server-sent events | built; Arrow Flight not built |
| Protocols: PostgreSQL wire, checked with psql, psycopg, JDBC, node-postgres, pgx and Grafana | built; Flight SQL not built |
| Joins and windows: window joins, markouts, lookups of subqueries, `UNION ALL` | built; `hop` and `session` windows not built; `PIVOT` in progress |
| Calendars and time zones | time zones built; exchange calendars not built |
| Object storage: S3, GCS, Azure and HTTP, with a local cache, pruning and prefix listing | built |
| Schema evolution: columns missing from older files read as NULL; `union_by_name` | built |
| Concurrency: query time limits, cancellation, admission | built; per-query memory limits not built |
| kdb+ migration: the "Coming from kdb+" guide | built; an HDB reader and a q-to-SQL parity checker not built |
| An open benchmark suite in the style of STAC-M3 (snapshots, VWAP, volume curves; 1 to 100 users) | not built |
| Grafana and Jupyter: provisioning, panel queries, `%sql` magic | built |

## v3: production at scale

What a firm running it for many desks needs.

| feature | status |
| --- | --- |
| Hot-hot replicas consuming the same log, failover, reconciliation by offsets | not built |
| Security: TLS, SSO (OIDC) and mTLS, role-based access, table, column and row entitlements, an audit log | not built (a bearer token guards the API and the PostgreSQL protocol today) |
| Bitemporal queries: "as known at" with corrections | not built |
| Scale-out: tables sharded by symbol across nodes, a router, resource groups | not built |
| A kdb+ IPC endpoint, so q clients and existing feed handlers connect unchanged | not built |
| Operations: Helm chart, rolling upgrades with checkpoints compatible across versions, backup and restore, a slow-query log, query profiles | not built |

## Never

- **A q interpreter.** q compatibility is a sinkhole; the IPC endpoint and the migration guide
  cover the integration need.
- **A storage format of its own.** Parquet, sorted by time and partitioned, is fast enough.
- **General distributed SQL** (arbitrary shuffled joins). Tick data partitions by symbol.

Something missing that you need? [Open a discussion](https://github.com/aperiodic-io/monotile/discussions).
