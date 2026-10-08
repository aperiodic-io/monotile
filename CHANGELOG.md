# Changelog

## 0.1.0 (unreleased)

The first public release: brrrrr as an open-source SQL engine for tick data and other time
series, live and historical.

**Query files where they are.** `brrrrr sql` and `pip install brrrrr` query Parquet, CSV
(gzipped, kdb+ and epoch times) and JSON lines, globs, Hive-partitioned directories, Iceberg and
Delta tables, on disk and in S3, GCS, Azure and over HTTP. A `WHERE` on partitions skips what it
excludes before anything is downloaded. Results print as tables, or go to CSV, JSON, Parquet,
partitioned directories, and Polars, pandas and pyarrow.

**The time-series vocabulary.** `time_bucket` in any time zone, `first`/`last`, `vwap`, `twap`,
as-of joins (strict, with a tolerance, at an offset for markouts), window joins, gap filling
(`locf`, `interpolate`), `lag`/`lead`, `ema` and rolling frames, exact percentiles,
`count(DISTINCT)`, `FILTER`; joins to subqueries, CTEs and views; `UNION`. What a stream cannot
run is refused with what to write instead. The [cookbook](fixtures/cookbook/cookbook.sql) holds
the queries people run, each checked against DuckDB's answer.

**Live.** `brrrrr serve` holds today's rows in memory and history in daily Parquet, ingests over
HTTP and from Kafka, keeps live views of any query, streams new rows and answers, and speaks
the PostgreSQL protocol (psql, Grafana, psycopg, JDBC, node-postgres, pgx). A browser console,
query time limits, cancellation and admission come with it.

**Pipelines.** `brrrrr run` runs the same SQL from Kafka topics of JSON to Kafka, exactly once
across crashes, with checkpoints.

Pipelines written in Timeplus Proton's dialect run unchanged. Checkpoints are format 10; those
of format 9, written by builds before this release, are restored and written back as 10 at the
next checkpoint. Nothing older is read.
