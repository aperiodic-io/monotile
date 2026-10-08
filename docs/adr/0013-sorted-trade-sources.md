# ADR-0013: Sort a trade source once, instead of in every ordered aggregate

- Status: accepted (implemented with the DuckDB parity work)
- Date: 2026-09-27

## Context
Many trade metrics depend on the order of a window's trades: first and last prices, returns on
the previous trade (order flow, price impact), runs (`run_structure`), ticks (`updownticks`).
Their reference implementations, in DuckDB, order by capture time, then exchange time or trade
id. brrrrr receives trades roughly, not exactly, in that order. On an hour of live exchange
trades (9.4M trades):

- 1.6% of 1m windows have a different first or last trade by arrival than by capture time;
  for quotes it is 0.06% and for mark prices none (their last values use `latest()`).
- Out-of-order trades arrive at most 34 ms after a later-captured one, but in bursts: a trade
  arrives up to 535 places behind its sorted place.

Before this, each ordered aggregate sorted on its own (`agg::Sequence`): every group of every window
keeps a sorted buffer of up to `SEQUENCE_REORDER` (2048) trades and `SEQUENCE_HOLD_US` (1.5 s),
and folds a trade once it is older than that. With four ordered pipelines on seven intervals, a
trade is inserted into about 35 sorted buffers. Replaying that hour, the sequence aggregates
cost the order-flow pipeline about 20% of its throughput and the price-impact one about 45%,
and the buffers about 40 MiB of RSS each on the run and tick pipelines. The first and last prices
of OHLCV bars and price ranges used `arg_min`/`arg_max` with a `(time, id)` tuple key, which
allocates a tuple per row (OHLCV: -24% throughput).

## Decision
Sort each trade source once, with the latency every window already allows for stragglers, and
fold every ordered metric in time order after it.

- **A held sort.** A view's own `ORDER BY` with `SETTINGS order_hold_ms = N` (`engine::Hold`)
  keeps its rows in one sorted buffer and releases them once the newest row's first key (a
  time) is more than N ms past theirs, in key order, equal keys in arrival order. A row that
  arrives after a later one has left is passed on at once and counted (`HOLD_LATE`,
  `brrrrr_hold_late_total`); one past the time limit is passed on without releasing the others.
  Its rows are checkpointed (`OpState::Hold`). `Engine::close_until(ts)` releases what a row at
  `ts` would, and closes the windows it feeds as far as `ts` less the hold.
- **No added latency.** The pipelines sort their trade inputs by `(local_event_time, id)` with
  the 50 ms their other windows wait for stragglers, and their windows wait no more (`EMIT AFTER
  WINDOW CLOSE`): a window's result is out 50 ms after its end, as before. 50 ms is above the
  largest disorder measured (34 ms).
- **Aggregates fold in time order.** A `Sequence` keeps only the trades of the newest time, at
  most `SEQUENCE_REORDER` (2048), sorted by its own tie order: the reference SQL breaks equal
  times differently per metric (by the id as a number, by the id as text, after exchange time),
  so the source sorts by time and each aggregate sorts its ties. `SEQUENCE_HOLD_US` is gone.
  OHLCV's first and last prices are `earliest`/`latest` over the sorted input instead of a
  tuple-keyed `arg_min`/`arg_max` (`(time, id)`, the source's order). Price ranges keep
  `arg_max` on `(time, exchange time)`: they do not read a sorted input.

A metric that does not depend on its trades' order (ranges, trade sizes, VTWAP, slippage) keeps
reading them unsorted, with its windows' 50 ms delay.

## Results
The same hour of trades, replayed at one CPU, per-aggregate sorting against this:

| pipeline | msgs / CPU s | RSS MiB | checkpoint MiB |
|---|---|---|---|
| order flow | 80,129 -> 83,098 | 131 -> 74 | 1.7 -> 1.7 |
| price impact | 116,749 -> 127,363 | 110 -> 46 | 1.0 -> 0.9 |
| OHLCV | 142,876 -> 168,089 | 39 -> 36 | 0.4 -> 0.3 |
| run structure | 234,844 -> 247,626 | 53 -> 28 | 0.3 -> 0.2 |
| up/down ticks | 226,228 -> 250,271 | 62 -> 30 | 0.3 -> 0.2 |

Next to Proton's SQL on the same build, order flow takes 1.22x the CPU and 1.51x the RSS, price
impact 1.20x and 1.31x, OHLCV 1.06x and 1.06x. Every order-dependent column agrees with the
DuckDB reference in 100% of windows, as with per-aggregate sorting (Proton's SQL, in arrival
order: 98.9% and 99.3% for OHLCV's open and close); order flow's columns agree as before.

## Consequences
- The engine has one more stateful operator; the exact ASOF join's held rows already work the
  same way.
- `order_hold_ms` must stay above the sources' disorder: `brrrrr_hold_late_total` counts rows
  that were not, and `brrrrr_sequence_out_of_order_total` the ones an aggregate then folded out
  of order. A trade later than the hold still counts in its window while that is open.
- A pipeline that relied on per-aggregate sorting must add the held sort to its SQL when it
  upgrades: the new build folds an unsorted input in arrival order.

## Alternatives considered
- **Keep sorting per aggregate** (before this): exact, no delay, but its cost grows with every ordered
  metric and interval.
- **Sort later than the hold allows, e.g. 250 ms**: adds 200 ms to every trade metric for no
  measured gain.
- **Publish in capture order at the producer**: would make arrival order the order everywhere,
  but needs the producers' concurrent connections and replicas, and the broker's first-wins
  dedup, to preserve it. Worth measuring; not a substitute for a bounded sort in the engine.
- **LAG in a window function on the sorted stream** instead of the fold aggregates: standard
  SQL, but a window's first trade must not see the previous window's, which needs a bucket
  guard per interval (`to_start_of_interval`) and `covar_samp`, `sqrt` in the engine; the folds
  are already exact and tested.
