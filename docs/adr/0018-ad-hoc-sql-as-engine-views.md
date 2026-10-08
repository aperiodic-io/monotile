# ADR-0018: Ad-hoc SQL is compiled into the engine's own views

- Status: accepted
- Date: 2026-10-08
- Amends: the original scope, in which ad-hoc querying was a non-goal; builds on
  [ADR-0016](0016-historical-columnar-executor.md)

## Context
brrrrr is opened as a tick-data engine for anyone ([the roadmap](../ROADMAP.md)): its users query
files and object stores ad hoc, from a shell, Python and a server. Two ways were on the table:

- **Embed a general SQL engine** (DataFusion) for ad-hoc queries, beside brrrrr's engine for
  pipelines. Breadth for free, but two implementations with two semantics (float evaluation
  order, quantiles, as-of joins), about ten times the memory brrrrr's executor takes on the same
  queries (DuckDB, of the same class, measured 2-30x, ADR-0016),
  and a large dependency.
- **Compile ad-hoc SQL into what the engine already runs**: a pipeline of views over source
  streams, run by the historical executor (columns, sorted runs, the pool).

## Decision
The second. `brrrrr_core::query::compile` turns one query into a `Catalog`:

- each table (a file, a glob, a URL, a name) is a source stream of its columns;
- `GROUP BY time_bucket(w, ts), keys` is a tumbling window on `ts`, the table read in `ts` order;
  `GROUP BY keys` alone (or aggregates alone) is one window over every row;
- `WHERE`, joins and computed keys go in a view before the window, `HAVING` in one after it;
- `ASOF [LEFT] JOIN` is the engine's exact as-of join; `[LEFT] JOIN ... ON keys` is a lookup of
  the right table, read before anything else. An as-of join at a time offset (`l.ts + INTERVAL
  '5 seconds' >= r.ts`, a markout) reads the right table on its own, in the order of its time
  shifted the other way, so both sides stay in time order; `l.ts > r.ts` is `>=` 1 µs on; a
  tolerance (`l.ts - r.ts <= INTERVAL '1 second'`) makes an older match none;
- `time_bucket(w, ts, 'America/New_York')` is a tumbling window over the zone's wall clock time
  (computed in the view before it), grouped by the bucket's start as a UTC time too: the hour a
  DST end reads twice is two buckets. `time_bucket_gapfill` (TimescaleDB's) is the window's output
  through `gap_fill`, an operator that adds each group's empty buckets when its next one comes
  (`locf`, `interpolate`), live as over history;
- `lead(x, n) OVER (PARTITION BY k ...)` holds each partition's rows until their `n`-th next
  ones come (an operator after the window functions; at the end of the input, the defaults), so
  its rows come out of time order and no time operation reads them;
- a window join, `LEFT JOIN LATERAL (SELECT aggregates FROM r WHERE r.k = l.k AND r.ts BETWEEN
  l.ts - a AND l.ts + b) ON true` (kdb+'s `wj`), is the exact as-of join holding the left row
  until the right side passes `l.ts + b`, then aggregating its key's right rows of the range;
- subqueries and CTEs are views into streams the outer query reads; `UNION ALL` is views writing
  one stream, which the historical executor hands its readers in its tables' time order, as it
  comes live; `UNION` is a `SELECT DISTINCT` of that;
- DuckDB's percentiles (`median`, `quantile_cont`, `percentile_cont`) are the engine's
  `quantile_exact`, which keeps every value (its own `quantile_cont` keeps 256, then a t-digest:
  bounded state for pipelines that run for months); `count(DISTINCT x)` is `uniq_exact`;
- the outermost `ORDER BY`/`LIMIT`/`OFFSET` are applied to the result.

What does not map is refused with what to write instead. `brrrrr_lake` resolves the tables (files,
object stores, DataFrames, live tables) and runs the pipeline; `EXPLAIN` shows it.

Speed comes from the executor plus three additions: a query whose stateful steps are all keyed by
one column of each table runs in parts side by side, the table split by that key as its files are
decoded (`Compiled::partition`); a source not in time order is sorted once; window functions in
time order run on columns.

## Consequences
- **One semantics.** An ad-hoc query, a live view (`brrrrr serve`), a backfill and a streaming
  pipeline are the same engine running the same views. The cookbook's 40 recipes give DuckDB's
  answers (`crates/brrrrr-lake/tests/cookbook.rs`).
- **Light.** No SQL engine beside the engine: the binary, the Python wheel and their memory stay
  small.
- **Narrower SQL than a general database:** no full outer joins or many-to-many joins (a join on
  keys is a lookup), no correlated subqueries, no `ORDER BY`/`LIMIT` inside subqueries, window
  functions ordered by a table's time only. Each is refused with the reason, and the cookbook shows
  the time-series queries people run fit.
- **Faster than DuckDB on the time-series shapes** (bars, as-of joins per symbol, rolling windows),
  slower on plain scans and aggregates over non-time keys ([bench/sql](../../bench/sql/README.md)).
