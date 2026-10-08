# Example pipelines

Streaming pipelines over market data, in Proton's SQL, with fixtures/market.proto as the schema
of their protobuf Kafka sources. Each file is one pipeline (`brrrrr run fixtures/pipelines/bars.sql
--proto fixtures/market.proto ...`), and the tests run them all on generated inputs
(crates/brrrrr-core/tests/common/mod.rs).

| Pipeline | What it computes | Sources (topic: message) |
|---|---|---|
| bars | OHLCV, VWAP and TWAP bars of 15s, 1m and 5m; Parquet export to S3 | trades: Trade |
| flow | order flow: buy/sell volumes, imbalance, entropy, running z-scores; all venues together | trades, spot_trades: Trade |
| quotes | top of book: spread and depth; slippage of each trade against the quote before it | quotes: Quote, trades: Trade |
| book | depth of the top 25 levels of the L2 book built from snapshots and updates | book, spot_book: BookUpdate |
| returns | log returns, realized volatility, autocorrelation; runs of same-side trades; upticks; price impact | trades: Trade |
| derivatives | basis of the mark over the index and last prices; funding; open interest; liquidations; long/short | mark_prices, index_prices, trades, funding_rates, open_interest, liquidations, long_short_ratios |
| stats | price and trade-size distributions: range, moments, medians, size buckets; two venues merged | trades, spot_trades: Trade |
| state | state of every kind a checkpoint holds (window functions, every aggregate, gap fill, lead) | trades_source |

## Inventory

Every function, setting and clause the pipelines use, and which pipelines use it.

| Function | Pipelines |
|---|---|
| `abs` | quotes, returns |
| `arg_max` | bars, quotes, state, stats |
| `arg_min` | bars, state |
| `array_slice` | book |
| `array_sum` | book |
| `avg` | book, flow, quotes, state, stats |
| `avg_if` | quotes, state |
| `cast` | bars, flow |
| `coalesce` | flow, returns, state, stats |
| `concat` | bars, flow |
| `corr` | returns, state |
| `count` | bars, derivatives, quotes, state, stats |
| `count_if` | derivatives, flow, quotes, state, stats |
| `distinct_stats` | derivatives, state |
| `earliest` | bars, derivatives |
| `first_value` | state |
| `format_datetime` | bars, book, derivatives, flow, quotes, returns, stats |
| `from_unix_timestamp64_micro` | bars, book, derivatives, flow, quotes, returns, state, stats |
| `gap_fill` | state |
| `if` | book, flow, quotes, returns, stats |
| `interpolate` | state |
| `kurt_samp` | state, stats |
| `kurtosis` | stats |
| `kurtosis_pop` | state |
| `lag` | derivatives, returns, state |
| `last_value` | state |
| `latest` | bars, book, derivatives, state, stats |
| `lead` | state |
| `length` | book |
| `ln` | flow, returns |
| `locf` | state |
| `match` | bars, stats |
| `max` | bars, quotes, state, stats |
| `max_if` | state |
| `median` | state, stats |
| `min` | bars, quotes, state, stats |
| `null_if` | book, derivatives, flow, quotes, returns, stats |
| `orderbook_top_n` | book |
| `quantile_cont` | quotes, state, stats |
| `quantile_cont_if` | state |
| `quantile_exact` | state, stats |
| `quantile_t_digest` | stats |
| `replace_regexp_all` | bars, stats |
| `row_number` | state |
| `run_structure` | returns |
| `skew_samp` | state, stats |
| `skewness` | stats |
| `sqrt` | returns |
| `stddev` | flow, quotes, state, stats |
| `stddev_samp` | state, stats |
| `sum` | bars, derivatives, flow, quotes, returns, state, stats |
| `sum_if` | state |
| `to_datetime` | bars |
| `to_float32` | book, derivatives, flow, quotes, returns |
| `to_float64` | flow, returns |
| `to_int32` | bars, derivatives, flow, returns, stats |
| `to_int64_or_zero` | quotes, returns, state |
| `to_start_of_day` | derivatives |
| `to_string` | bars, flow |
| `to_unix_timestamp64_micro` | bars, book, derivatives, flow, quotes, returns, state, stats |
| `trade_returns` | flow, returns, state |
| `trade_reversal` | state |
| `tumble` | bars, book, derivatives, flow, quotes, returns, state, stats |
| `twap` | bars, state |
| `uniq_exact` | state |
| `updownticks` | returns |
| `var_samp` | state |
| `variance` | returns, state, stats |
| `vwap` | bars, state |

| Setting | Pipelines |
|---|---|
| `access_key_id` | bars |
| `brokers` | bars, book, derivatives, flow, quotes, returns, stats |
| `bucket` | bars |
| `data_format` | bars, book, derivatives, flow, quotes, returns, state, stats |
| `endpoint` | bars |
| `format_schema` | bars, book, derivatives, flow, quotes, returns, stats |
| `group_name` | bars, book, derivatives, flow, quotes, returns, stats |
| `keep_versions` | derivatives, quotes |
| `logstore_codec` | bars |
| `logstore_retention_bytes` | bars |
| `logstore_retention_ms` | bars |
| `max_bytes_before_external_group_by` | bars |
| `max_bytes_before_external_sort` | bars |
| `merge_max_block_size` | bars |
| `merge_with_ttl_timeout` | bars |
| `one_message_per_row` | bars, book, flow, quotes, returns |
| `order_hold_ms` | bars, flow, returns |
| `properties` | bars |
| `region` | bars |
| `secret_access_key` | bars |
| `seek_to` | bars, book, derivatives, flow, quotes, returns, stats |
| `topic` | bars, book, derivatives, flow, quotes, returns, state, stats |
| `ttl_only_drop_parts` | bars |
| `type` | bars, book, derivatives, flow, quotes, returns, state, stats |
| `write_to` | bars |

| Clause or form | Pipelines |
|---|---|
| `EXTERNAL STREAM` (Kafka source and sink), `CREATE STREAM` (internal) | all |
| `EXTERNAL TABLE ... PARTITION BY` (S3) | bars |
| `TTL` and logstore settings on a stream | bars |
| `_tp_message_headers map(string, string) MATERIALIZED cast(([...], [...]), 'map(string, string)')` | bars, flow |
| types `datetime64(6)`, `low_cardinality(...)`, `nullable(...)`, `map(...)`, `array(...)` | all |
| `tumble(stream, time, width)` ... `GROUP BY window_start, ...` | all |
| windows of one input differing in width alone (fused) | bars |
| `EMIT AFTER WINDOW CLOSE` | flow, returns, state |
| `EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND` | bars, book, derivatives, flow, quotes, stats |
| `ORDER BY ... SETTINGS order_hold_ms` (sorted source) | bars, flow, returns |
| `ASOF LEFT JOIN` of `ORDER BY` subqueries, `SETTINGS keep_versions` | derivatives, quotes |
| `OVER (PARTITION BY ...)` (lag in arrival order) | derivatives, returns |
| `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | flow |
| `WINDOW w AS (... ROWS 3 PRECEDING)`, `RANGE INTERVAL 1 SECOND PRECEDING` | state |
| `avg(x, 'alpha', a)`, `avg(x, 'period', n)` OVER (moving averages) | state |
| `WITH base AS (...) SELECT ... FROM base` | flow |
| `CASE WHEN ... THEN ... ELSE ... END` | bars, derivatives, flow, stats |
| `IN (...)`, `IS NULL`, `IS NOT NULL` | derivatives, flow, quotes, returns, stats |
| tuple aggregates indexed `[1]`..`[7]` | derivatives, flow, returns, state |
| `arg_max(x, (t, t_us))` (a tuple as the order) | stats |
| several views `INTO` one stream | flow, stats |
| `SELECT * FROM agg WHERE interval = '1m'` sink views | bars |
| `FROM orderbook_top_n(stream, depth[, allow_seq_reset])` | book |
| `FROM gap_fill(stream, time, '1s', key, locf(x), interpolate(y))` | state |
