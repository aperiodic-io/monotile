# ADR-0015: A window's t-digests merge in its last seconds, ahead of the close

- Status: proposed
- Date: 2026-10-04
- Amends: ADR-0014 (`quantile_t_digest` merges as values come), for windows of 512 values or more

## Context
- **The close still merged every digest.** Since ADR-0014 a digest merges every 512 values as they
  come, and a read merges the fewer than 512 since. A window's read is its close, so at the top of
  the hour every closing window's digests still sorted and merged up to 511 values each, and ran
  the merge pass over all their centroids.
- **That last merge was most of the close.** In a perf profile of a trade-size pipeline's top-of-hour close
  (the latency bench, `benches/latency.rs`), `TDigest::compress` called from the read was 46% of
  the time: 32% the merge pass itself, 10.7% the sort of the unmerged values. A price-range pipeline is alike.
- **The merge pass cannot be made faster bit for bit** (ADR-0014). It can run earlier: most of a
  window's values have arrived seconds before its close.
- **The output must stay deterministic.** A merge changes the centroids of a digest past 512
  values, so when it runs decides the result. It must not depend on the wall clock, on how rows
  are chunked, on threads, or on where a checkpoint was restored.

## Decision
- **Each window has a lead, from 2 s to 0.5 s before its end** (`LEAD` in `engine.rs`).
- **Each group of a window has a merge time in its window's lead**, from a hash of its key text
  (`merge_at`): FNV-1a, then MurmurHash3's 64-bit finalizer, scaled to the 1.5 s span. This
  spreads a window's groups evenly over its lead, in a fixed order.
  - Plain FNV-1a was not enough: its top bits barely move with the last byte, so keys that differ
    only there (`1:X`, `1:Y`) got the same merge time.
- **A group's digests merge once, after the row that moves the window's newest event time
  (`max_ts`), row by row, from before its merge time to at or past it.** `Engine::close_until`
  moves the newest time without a row, and merges as a row would.
  - A group opened after its merge time does not merge ahead.
  - A window that closes row by row (its end at or before the row-by-row watermark) merges
    nothing more. A chunk that closes it later must not merge more than row by row would.
- **Only a digest that has merged before (512 values or more) merges ahead.** A smaller one is
  left to merge at its read, so a window under 512 values is still ClickHouse's bit for bit, as
  ADR-0014 promises.
- **Constants, not settings.** A lead set per process would give two processes different outputs
  for the same input.
- **Determinism.** Which groups merge after which row depends only on the rows, their event times
  and the `max_ts` before each row. It does not depend on chunks, threads or restores.
  - The groups still due in each window (`Window::due`) are derived state, not checkpointed.
    After a restore they are rebuilt on the next row: the groups whose merge time is past the
    restored `max_ts`, which is the set an uninterrupted run holds at that point.
  - The hash and `LEAD` are part of the output. They are written out in `merge_at`, pinned by
    `merge_times_are_fixed_and_spread_over_the_lead`, and must stay as they are. Changing either
    is an output change and needs its own decision, as any change to the merge schedule does.

## Consequences
- **The top of the hour's close is 22-29% shorter on the t-digest pipelines.** Latency bench
  (`benches/latency.rs`), ms, the median of three paired runs (each the median of 5 reps), base
  and this change side by side on one host:

  | pipeline | stall | p50 | p99 | max | max15s | next15s |
  |---|---|---|---|---|---|---|
  | trade sizes | 211.1 → 151.0 | 95.8 → 75.5 | 214.3 → 151.3 | 214.3 → 151.3 (-29%) | 153.7 → 114.5 | 17.9 → 19.9 |
  | price ranges | 159.6 → 121.7 | 68.3 → 57.4 | 160.1 → 124.7 | 160.1 → 124.7 (-22%) | 110.4 → 90.3 | 13.5 → 14.2 |
  | slippage | 70.9 → 51.8 | 34.4 → 29.7 | 82.9 → 64.1 | 82.9 → 64.1 (-23%) | 72.6 → 56.5 | 5.9 → 6.1 |

  The order-flow and OHLCV pipelines have no t-digest and do not change beyond the host's noise.
- **It gains less on SQL whose medians are `quantile_cont` (measured 2026-10-04).**
  `quantile_cont` (a window's first 256 values exact, then a t-digest of its own) is not covered
  by this ADR's merge (`Acc::holds_digest` is true for `Acc::TDigest` only). With most medians
  moved to `quantile_cont`: trade sizes −16%, price ranges −15% (9 of their 36 median views
  still `quantile_t_digest`), slippage unchanged (all its quantiles `quantile_cont`; its output
  digest is the same with and without this ADR). Merging a `Acc::Cont` digest ahead as well
  would be a change of its own, of values past 256, and needs a decision of its own.
- **It is not the whole 46%.** Most groups of a busy window take values after their merge time,
  and their close merges those and walks all their centroids again.
- **Results change for windows of 512 values or more.** Their centroids come from another merge
  schedule again. The bench baselines' digests for the price-range, slippage and trade-size pipelines were
  updated. Windows under 512
  values do not change.
- **Accuracy stays ClickHouse's: rank error against the exact quantile.** `rank_error_table`
  (13 distributions, 1,000 to 1,000,000 values, 13 levels) now also merges each digest ahead
  after 50%, 87%, 99% and 99.9% of its values. The worst of the four, against ADR-0014's schedule
  and ClickHouse's:

  | distribution | max | max, merged ahead | max, ClickHouse | mean | mean, merged ahead | mean, ClickHouse |
  |---|---|---|---|---|---|---|
  | lots (sizes, ties) | 0.46% | 0.46% | 0.65% | 0.048% | 0.048% | 0.051% |
  | clustered price | 0.38% | 0.38% | 0.38% | 0.051% | 0.051% | 0.051% |
  | lognormal (notional) | 0.13% | 0.13% | 0.05% | 0.0048% | 0.0048% | 0.0046% |
  | bimodal | 0.13% | 0.15% | 0.05% | 0.0066% | 0.0084%[^1] | 0.0031% |
  | slippage bps | 0.018% | 0.05% | 0.013% | 0.0018% | 0.0027% | 0.0014% |
  | outliers | 0.05% | 0.05% | 0.028% | 0.0048% | 0.0055% | 0.0033% |
  | uniform | 0.05% | 0.05% | 0.05% | 0.0036% | 0.0043% | 0.0028% |

  The other six distributions do not change. An extra merge costs about as much accuracy as
  ADR-0014's 512-value schedule did, no more.
  - `premerged_quantiles_are_as_close_in_rank_as_clickhouses` keeps ADR-0014's CI bounds with the
    four merge points: every result within 0.5% in rank, and within 0.2% of ClickHouse's error on
    the same values (0.5% for tied sizes); the mean within 2.5 times ClickHouse's plus 0.005
    percentage points; under 512 values, ClickHouse's error exactly.
  - The historical parity tests (a rank band of 2% of the values) still pass.

  [^1]: 2.7 times ClickHouse's mean, against "at most 2.1 times" in ADR-0014. That is bimodal
  values merged ahead after 99% of them, and still within the CI bound.
- **A checkpoint holds nothing new**: each group's digests (centroids, unmerged values, count) and
  the window's `max_ts`, as before. The layout and `checkpoint::VERSION` stay.
  - A checkpoint of an older build restores here. Its digests are valid on any schedule. Only
    the windows in their lead at the restore can differ from an uninterrupted run of either
    build: the groups whose merge time had passed were not merged ahead, and are merged at the
    close as before.
  - A checkpoint of this build restores in an older build, for the same reason: a digest merged
    ahead holds fewer than 512 unmerged values and at most 2,048 centroids, as any digest does.
    The older build goes on with ADR-0014's schedule.
- **Throughput does not move beyond noise.** Paired runs of the throughput bench (median of 7)
  were within ±3% on every pipeline. Allocations per row are 0.1-0.6% higher on the t-digest
  pipelines: a window in its lead copies each group's key text once into its list of groups
  due, freed at the close. A view without a t-digest has no lead and does none of this.

## Alternatives considered
- **A cursor over the groups in key-text order, a few groups per row.** It was slower at the
  close (197 ms against about 150 ms on trade sizes), and a restore inside a lead needed the
  cursor and the snapshot of keys in the checkpoint.
- **Stateless: a row within 2 s of its window's end merges its own group's digests once they
  hold 64 or more unmerged values.** Nothing to checkpoint, but weaker and noisier: 162-223 ms
  on trade sizes.
- **A later lead, from 1 s to 0.1 s.** No better (163 ms on trade sizes): more groups take values
  after their merge time.
- **Also merging digests under 512 values ahead.** 4-5% better still, within the host's noise,
  but a window under 512 values would no longer be ClickHouse's bit for bit.
- **Merging on the wall clock or when the data thread is idle.** Faster to react, but the merge
  points, and so the results, would depend on the host's load.
