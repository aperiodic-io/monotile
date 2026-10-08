# ADR-0016: A columnar executor runs the live SQL over days of Parquet files

- Status: proposed
- Date: 2026-10-06
- Amends: ADR-0012's "rows, not Arrow": the historical executor holds columns (typed `Vec`s, not
  Arrow, as ADR-0004's gate allowed); the live engine stays row by row

## Context
- **Backfills run another implementation.** A common setup computes the same metrics live in a
  streaming engine and over the archive in DuckDB, with SQL of its own; brrrrr's tests held
  such a DuckDB implementation and brrrrr to each other. A day
  recomputed by DuckDB is not the day brrrrr would have written: every difference brrrrr's
  parity tests allow (t-digest medians past 256 values, float32 casts, ordering ties) is a
  difference between the live data and the backfill.
- **The POC fed the engine rows**: Parquet decoded into `Vec<Value>` rows through
  `Engine::insert`, symbols on threads. It was 3x slower than DuckDB on OHLCV and only won run
  structure. Measured again on a real day (one exchange's ETHUSDT perpetual, 6.6M
  trades, one core): the engine took 7.2 s for OHLCV's two intervals, 2.4 s of it building rows; the
  same aggregation over columns of the sorted trades took 0.05 s, the Parquet decode 0.26 s.
  Row by row, the engine spends ~1 µs a trade on what a column loop does in ~8 ns.
- **The input is sorted.** A historical source is a symbol's day of one stream in the order of
  its capture time (archived exchange files are; 12 symbols checked: no time goes back, ties of one
  time 17% of trades, ids within a tie in order). A window's rows are then a run of rows, a
  group a slice, an ASOF join a merge.

## Decision
`brrrrr historical` (cargo feature `historical`, not in the live image) runs a pipeline's
SQL over Parquet files, symbol by symbol, and writes each symbol's messages, the
engine's for the same rows, as JSON lines.

- **One SQL, the engine's semantics.** `engine::Historical` plans the views as the engine does
  (`Engine::new`, exact ASOF joins) and takes the operators of the views that read source rows:
  their projections, ORDER BY, `lag`, ASOF joins and tumbling windows run on columns
  (`column::Batch`); every view after a window (OVER over windows' rows, sinks, headers) runs in
  the engine, which a window's closed groups are written into as live. Whatever has no columnar
  form runs the engine's own operators on the batch's rows: a window holding a t-digest (its
  merge schedule is the engine's, ADR-0015), a window function other than `lag`.
- **Expressions and aggregates as shortcuts of the row semantics.** `expr::vector` compiles a
  view's expressions with `Compiler`'s names and aliases into nodes (equal subexpressions once):
  a typed loop where the operands are of one kind, `arith`/`compare`/the scalar function itself
  value by value otherwise, the `Compiler`'s closure for anything else. `agg::kernel` adds a
  group's rows to the engine's own accumulators (`Acc`): a typed loop for the common shapes
  (`sum(if(c, x, 0))`, `count_if`, `arg_max(x, (t, id))`, sequence aggregates over rows already in
  their tie order, ...), `Acc::add` otherwise. The state after is the same bit for bit; property
  tests hold every typed path to its row path. (Amended by ADR-0017: a float's sum in a window
  built from its narrower windows' is the same within rounding.)
- **Chunks of time.** Each source's rows are read in clock order and cut into chunks (128K rows
  of the busiest source) that end on a clock value: rows of one time never straddle two, and
  every right row an ASOF join's left row of a chunk can take is in it or before it.
- **Parallel, deterministically.** Symbols run side by side, largest first, on one pool, which
  also runs a chunk's parts: views that read nothing another view of the chunk writes, a
  projection's row ranges, a window's runs of windows (each group's rows added by one job, in
  order). `engine::Pool` is the runtime's (rayon); core has no threads (ADR-0012). The output
  does not depend on the pool, the batches or the chunks: tests run every pipeline on two pools
  and chunks of 5 to 65,536 rows.
- **Files on disk or in an object store.** A `--source` path and `--out` may be a store's URL
  (`s3://`, `gs://`, `az://`). A store's source files are found by listing each directory once,
  and each column of a row group is fetched as its byte range (`brrrrr_lake::ranged`), not the
  file. The output is uploaded as it is written (`brrrrr_lake::files::Writer`): a file is there
  whole or not at all, and `_SUCCESS`, written last, says every symbol's is.
- **Parquet decoded once per column.** Only the columns the views read (a projection's items no
  reader reads are not computed, nor their inputs decoded), each on threads of its own a row
  group ahead (more threads per column as fewer symbols are left), a source of few rows on the
  reader's thread. `--symbol-from-path` takes the symbol from the file name, as archives of
  one file per symbol and day need; `--trust-order` takes rows of one time to be in ORDER BY order already (a trade's id is
  then not read).
- **All or nothing.** Each symbol writes a part file; once every symbol has, every file is
  synced, renamed, and `_SUCCESS` (the run's manifest) written and synced with the directory.

## Consequences
- The backfill is what live would have written for the same input (`tests/historical_run.rs`:
  every fixture pipeline, every symbol, two pools, four chunkings; keyed joins, lags and windows,
  NULLs), and holds to DuckDB as live does: on the real day, the same windows for all 18 metrics
  and every column within its parity budget but slippage's median past 256 values (a
  t-digest's, p90 relative error 0.17 against 0.1), as live's.
- **Faster than the DuckDB code it can replace**, on every metric: on the real day's 12 symbols, 3x to 26x
  DuckDB's query time in its fastest configuration (1.4x on liquidation totals' 10K rows), 4x to
  19x process against process, with 2x to 30x less memory.
- **One divergence from the engine, on purpose.** The engine's exact ASOF join keeps the right
  versions a left row may need for `ASOF_HOLD_US` (30 s) of the right side's time. A left side
  behind a held ORDER BY whose feed is quiet for longer (its rows wait in the hold for a later
  one) reaches the join after the versions it needed were pruned, and takes the defaults. The
  historical executor has both sides whole: a left row takes the latest right row at or before
  it, however long a side was quiet (`an_asof_join_takes_the_latest_version_however_long_a_side_was_quiet`).
  None of the test pipelines holds a join's left side (the slippage pipeline's trades are not
  held); a live pipeline that did would differ from its backfill on quiet feeds.
- **Input contract.** Each source's rows are in clock order (refused otherwise, with the row);
  a range starts and ends on every window's boundary; a held ORDER BY's keys follow the clock
  across chunks (refused otherwise). A window over another time than the clock drops late rows
  per chunk, as the engine drops them per insert: its output depends on the chunks, as live's on
  inserts.
- **Memory.** A symbol in flight holds a chunk and its decode lookahead: ~30 MiB of trades for
  OHLCV, more with more decode threads (a row group each).
- **Not yet.** Partitions other than symbols (a symbol's day across processes), exact
  `quantile_cont` past 256 values (DuckDB's) as an option, output to Kafka or Parquet, a symbol's
  state carried from the day before (a `lag` restarts each run, as a daily DuckDB run's does).

## Alternatives considered
- **The POC: rows into the engine**. Simplest, and the engine's semantics by
  construction; ~1 µs a trade, 3x DuckDB's time on OHLCV.
- **DataFusion or DuckDB over the SQL.** Different semantics: the reason brrrrr exists
  (ADR-0001). The parity work would start again.
- **Arrow arrays as the batch format** (ADR-0004's first decision). The kernels need typed slices and NULL masks,
  not Arrow's buffers; columns are copied out of Arrow once, at the reader, which costs ~1 ns a
  value against ~5-20 ns to decode it.
- **A native columnar store** (kdb+'s splayed tables). Uncompressed Parquet decodes in ~1.5 ns a
  value (5-13 ns zstd-9): the same files serve the speed without a format of our own, and DuckDB
  reads them too.
