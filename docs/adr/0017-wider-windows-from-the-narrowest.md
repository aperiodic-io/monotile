# ADR-0017: Wider windows are built from the narrowest's, a float's sum within rounding

- Status: proposed
- Date: 2026-10-07
- Amends: ADR-0016's "the same bit for bit", for a float's sum in a window that runs fused with
  narrower ones

## Context
- **Every interval is a window of its own.** A pipeline computes one window per interval (15s to
  1d: nine in our test pipelines). `brrrrr historical` already runs those that differ in width alone
  together (`fused_windows`): each block's arguments evaluated once, then added to each window's
  group, row by row. On OHLCV over a 7-day archive (192M trades, 4 threads), adding every row to
  nine windows' accumulators was 25% of the run, and the nine intervals took 13.2 s against 8.4 s
  for the narrowest alone.
- **Most aggregates merge exactly.** The fold of two runs of rows is the merge of their folds for
  `earliest`, `latest`, `count`, `count_if`, an integer's `sum`, and `min`/`max` of a run with
  no NaN (a NaN first holds the fold: `Acc::add`). A float's `sum`, `avg` and `weighted_avg`
  merge by adding two sums: the same value but for rounding, which a fold of every row in turn
  does in another order.
- **A backfill is held to DuckDB within 1e-4.** The parity tests hold brrrrr to DuckDB to 1e-4
  of a value; no consumer reads a backfill's sums to the last bit.

## Decision
- **The wider windows of a fused set take the narrowest's.** Each block of a group's rows is
  added to the narrowest window's accumulator, row by row as before, and folded once more into
  a state of its own, which is merged into each wider window's group (`Acc::merge`). Two passes
  over the rows rather than nine.
- **Exact where a merge is.** `earliest`, `latest`, `count`, `count_if`, integer sums, and
  `min`/`max` of blocks with no NaN are the fold of every row, bit for bit; a block with a NaN
  adds its rows to every window, as before. The narrowest window is always the row-by-row fold.
- **A float's sum within rounding.** A wider window's `sum`, `avg`, `weighted_avg` (and their
  `_if`) of floats differ from the row-by-row fold by rounding: a few units in the last place of
  the sum of the blocks' magnitudes; one Float32 step where the SQL casts the result to one.
- **A sequence of trades' returns (`trade_returns`) merged too.** A trade's return is on the
  price of the one before it: every wider window that holds trades holds the one just before a
  block, so the block is folded once more from that price (`Acc::seeded`), and merged (the
  window's waiting trades folded first, then the sums, and the co-moment as Chan et al. combine
  two). A window the block starts takes the narrowest's state, which the block also starts.
  Trades of one time on both sides of a block are put in order together, row by row.
- **Every other aggregate as before**: other sequences, digests, moments, `arg_max` add every row
  to every window (`Acc::mergeable`).
- **Tests.** A property test holds the merge of two runs to the fold of both: bit for bit for the
  exact ones, 1e-12 for a float's sum. The differential tests against the engine compare a float
  to 1e-12 of it (or one Float32 step where it is one), every other value exactly.

## Consequences
- OHLCV over 7 days: 12.0 s to 10.4 s (the aggregation kernels 25% of the profile to 8%); what is
  left is mostly reading the archive (zstd, text prices), which DuckDB pays too. Order flow, whose
  `trade_returns` was 40% of its profile: 37.6 s to 24.9 s.
- A backfill's float sums no longer equal the live engine's to the last bit in the wider
  windows; within rounding, they do. A consumer that compares them exactly must not.
- A merge is what parallel or vectorised sums would need too; they are not done here.
