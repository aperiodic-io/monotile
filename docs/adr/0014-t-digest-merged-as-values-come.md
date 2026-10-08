# ADR-0014: `quantile_t_digest` merges as values come, not at the window's close

- Status: proposed
- Date: 2026-10-03
- Amends: ADR-0001 (outputs bit for bit with Proton), for `quantile_t_digest` and `median_tdigest`
- Amended by: ADR-0015 (a window's digests also merge in its last seconds, ahead of the close)

## Context
- **The stall is the closes themselves.** At the top of the hour every interval's windows of
  every feed close at once, on the data thread, and every output waits behind them. In a live
  deployment (2026-10-02), a trade-size pipeline's newest output trailed the wall clock by up to
  1.25 s at the top of the hour.
- **Most of a close was ClickHouse's t-digest, as ported bit for bit.**
  - ClickHouse merges a digest's values into its centroids when 2,048 are unmerged, and again
    whenever a quantile is read.
  - So every closing window's digest sorted up to 2,048 values and ran its merge pass, a chain of
    divisions, over them and its centroids, all at the close.
  - In a perf profile of the trade-size pipeline's 1h close (replayed from a snapshot), `TDigest::compress`
    was 43% of the time and 63% with what calls it.
- **Faster code and threads did not remove it.**
  - Sorting the values while idle and formatting floats faster cut a part of it.
  - `--close-threads 3` live made the price-range, OHLCV and quote pipelines' closes slower and
    left the trade-size one's unchanged (ADR-0005).
- **The merge pass cannot be made faster bit for bit.** Its running-mean update divides on each
  merged value, a dependency chain that cannot be reordered without changing the result.
- **Market-data pipelines use t-digests on values that need a rank guarantee, not a relative
  one.**
  - A median price: prices within dollars of 60,000.
  - Slippage's median and p95 in basis points: around zero and negative.
  - A median trade amount: notionals over five decades.
  - A relative-error sketch (DDSketch at 1%) would put a BTC median ±600 off.

## Decision
- **Merge as values come.** A digest merges its unmerged values into its centroids every 512
  values (`BUFFER`), as they come.
  - It uses ClickHouse's merge pass, `compressBrute`, epsilon 0.01, Float32 centroids and 2,048
    centroids at most.
- **Read as ClickHouse reads.** A quantile merges the fewer than 512 values since into the
  centroids, then interpolates as ClickHouse does.
  - Reading again finds nothing to merge and reads the same centroids. So the quantiles of one
    argument (a median and a p95 of slippage) merge once, and an OVER frame's per-row reads stay
    ClickHouse's.
- **Between two equal means, a quantile is that mean.** ClickHouse blends two infinities into NaN
  there.
- **Checkpoints are unchanged.** The state is the same three fields, so the layout and
  `checkpoint::VERSION` stay.
  - A digest restored from a checkpoint of the old schedule, holding up to 2,048 unmerged
    values, is read as it is and merges them with its next value.

## Consequences
- **A close sorts fewer than 512 values per digest instead of up to 2,048.** It merges them with
  the centroids, and each value merges about four times as often on the way in: about 60 ns
  instead of 40 ns a value for one digest alone, and -4% to +1% of a pipeline's throughput.
  - Measured on the 1h close, replayed from a snapshot (median of 5): 289 → 205 ms on the
    trade-size pipeline and 216 → 147 ms on the price-range one.
- **Results are no longer ClickHouse's bit for bit once a window holds 512 values or more.**
  - A digest read before it holds 512 values is merged at the read either way, so the result is
    ClickHouse's exactly (property-tested), barring two equal infinities. That covers every
    window under 512 values and every row of an OVER frame.
  - Above it, the centroids come from another merge schedule.
  - Which windows cross 512 values depends on each symbol's trade rate and the
    window's length. Any window under 512 values is unchanged.
- **Accuracy stays ClickHouse's: rank error against the exact quantile.** `rank_error_table` in
  `agg.rs` covers 13 distributions, 1,000 to 1,000,000 values and 13 levels.
  - Heavily tied sizes: the worst result is 0.46% of the values off in rank, against
    ClickHouse's 0.65%.
  - Clustered prices: 0.38%, the same as ClickHouse.
  - The other distributions: at most 0.13%, against ClickHouse's 0.05%.
  - Mean errors are below 0.06%, at most 2.1 times ClickHouse's.
  - `quantiles_are_as_close_in_rank_as_clickhouses` keeps this in CI: every result within 0.5% in
    rank, and within 0.2% of ClickHouse's error on the same values (0.5% for tied sizes).
  - The engine tests check market-data-shaped windows of tens of thousands of values against
    exact quantiles.
- **Values differ from ClickHouse's in the last digits.** A median computed by ClickHouse over
  the same window can differ from brrrrr's by a fraction of a percent of the window's values in
  rank.
  - The historical parity tests compare t-digest columns with a rank band of 2% of the values
    and still pass.
- **Determinism is unchanged.** The schedule depends only on the sequence of a group's values,
  not on chunks, threads or restores.

## Alternatives considered
- **Keep ClickHouse's schedule and make the close cheaper around it.** Sorting ahead, a
  faster float format and threads were each measured: a smaller part of the
  close, or no gain live.
- **Merge as values come, but read without merging the values since.** This was cheaper still,
  but 2-4 times ClickHouse's mean rank error. A value inside a big centroid's spread is ranked
  wholly before or after its mass until a merge folds it in.
- **Merge the values since on a copy at a read, leaving the digest as it was.** Two levels of one
  digest (a median and a p95) then merged twice: under 512 values, 22 µs against 14 µs a group. And an
  OVER frame, which reads every row, would sort and merge up to 511 values each row, with results
  that are no longer ClickHouse's.
- **A larger or smaller buffer.** 256 makes each value dearer for about the same accuracy. 1,024
  and 2,048 make the close dearer, and 2,048 is no more accurate.
- **DDSketch (relative error).** Adding a value and closing are O(1), but its error is relative
  to the value. On prices around 60,000, 1% is ±600, far wider than a window's spread.
- **KLL or another rank sketch.** It is randomized (determinism needs a seeded RNG in the state)
  and of another family, so it would sit further from ClickHouse's values than a t-digest on
  another schedule.
