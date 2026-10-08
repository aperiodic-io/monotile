-- State of every kind a checkpoint holds, for the committed checkpoints
-- (fixtures/checkpoints/v<VERSION>/state.ckpt; tests/checkpoint.rs,
-- the_committed_checkpoints_hold_every_kind_of_state): window functions (OpState::Over, every
-- FnState, and every way a bounded frame is kept as it slides), the _if combinators, the
-- moments, a reservoir median, the sequence aggregates' tie-breaks, quantile_cont past its exact
-- buffer (Cont::Digest: an hour holds more than agg::CONT_EXACT trades of a symbol), exact
-- quantiles and distinct counts (Cont::All, Acc::Uniq), trade_reversal, gap_fill (OpState::Fill)
-- and lead (OpState::Lead).
-- Fed generated trades (tests/common/mod.rs, state_fixtures).
CREATE STREAM IF NOT EXISTS trades_source (
  time int64,
  id string,
  exchange int64,
  symbol low_cardinality(string),
  price float64,
  local_timestamp int64,
  side low_cardinality(string),
  quantity float64,
  amount float64
);

CREATE STREAM IF NOT EXISTS trades (
  local_event_time datetime64(6),
  id string,
  exchange int64,
  symbol low_cardinality(string),
  side low_cardinality(string),
  price float64,
  quantity float64
);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_mv INTO trades AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, id, exchange, symbol, side, price, quantity
FROM trades_source;

CREATE EXTERNAL STREAM IF NOT EXISTS over_out (
  symbol string,
  running_sum float64,
  running_buys nullable(float64),
  sum_3 float64,
  avg_3 float64,
  count_3 uint64,
  rows_3 uint64,
  vwap_3 nullable(float64),
  min_1s float64,
  max_1s float64,
  first_3 float64,
  last_buy_3 bool,
  stddev_3 nullable(float64),
  lag_2 nullable(float64),
  lag_buy nullable(bool),
  lag_f32 nullable(float32),
  ema_alpha nullable(float64),
  ema_period nullable(float64),
  first_price float64,
  n uint64
) SETTINGS type = 'kafka', topic = 'over', data_format = 'JSONEachRow';

-- ROWS 3 PRECEDING keeps sums, means, counts, weighted means and the frame's ends as it
-- slides, RANGE keeps minima and maxima, and stddev_samp is recomputed: a restore rebuilds each
-- from the frame's rows. The sliding sums are of quantities, inexact in binary: a sliding
-- Float64 sum is exact (over.rs, Exact), so the one rebuilt from the frame's rows is, bit for
-- bit, the one that slid there
CREATE MATERIALIZED VIEW IF NOT EXISTS over_v INTO over_out AS
SELECT
  symbol,
  sum(quantity) OVER (PARTITION BY symbol ORDER BY local_event_time) AS running_sum,
  sum_if(quantity, side = 'buy') OVER (PARTITION BY symbol ORDER BY local_event_time) AS running_buys,
  sum(quantity) OVER w AS sum_3,
  avg(quantity) OVER w AS avg_3,
  count(price) OVER w AS count_3,
  count() OVER w AS rows_3,
  vwap(price, quantity) OVER w AS vwap_3,
  min(price) OVER (PARTITION BY symbol ORDER BY local_event_time RANGE INTERVAL 1 SECOND PRECEDING) AS min_1s,
  max(price) OVER (PARTITION BY symbol ORDER BY local_event_time RANGE INTERVAL 1 SECOND PRECEDING) AS max_1s,
  first_value(price) OVER w AS first_3,
  last_value(side = 'buy') OVER w AS last_buy_3,
  stddev_samp(price) OVER w AS stddev_3,
  lag(price, 2) OVER (PARTITION BY symbol ORDER BY local_event_time) AS lag_2,
  lag(side = 'buy') OVER (PARTITION BY symbol ORDER BY local_event_time) AS lag_buy,
  lag(to_float32(price)) OVER (PARTITION BY symbol ORDER BY local_event_time) AS lag_f32,
  avg(price, 'alpha', 0.3) OVER (PARTITION BY symbol ORDER BY local_event_time) AS ema_alpha,
  avg(price, 'period', 10) OVER (PARTITION BY symbol ORDER BY local_event_time) AS ema_period,
  first_value(price) OVER (PARTITION BY symbol ORDER BY local_event_time) AS first_price,
  row_number() OVER (ORDER BY local_event_time) AS n
FROM trades
WINDOW w AS (PARTITION BY symbol ORDER BY local_event_time ROWS 3 PRECEDING);

CREATE EXTERNAL STREAM IF NOT EXISTS window_out (
  symbol string,
  time int64,
  var_samp nullable(float64),
  skew_samp nullable(float64),
  kurt_samp nullable(float64),
  kurtosis_pop nullable(float64),
  variance nullable(float64),
  stddev nullable(float64),
  twap nullable(float64),
  arg_max nullable(float64),
  arg_min nullable(float64),
  corr nullable(float64),
  buy_volume nullable(float64),
  sell_count uint64,
  buy_price nullable(float64),
  sell_max nullable(float64),
  last_buy nullable(bool),
  realized_vol nullable(float64),
  impact nullable(float64),
  distinct_prices nullable(float64),
  price_median nullable(float64),
  vwap nullable(float64)
) SETTINGS type = 'kafka', topic = 'window', data_format = 'JSONEachRow';

-- trade_returns breaks ties by a text id and a number, distinct_stats by NULL
CREATE MATERIALIZED VIEW IF NOT EXISTS window_v INTO window_out AS
SELECT
  symbol,
  to_unix_timestamp64_micro(window_start) AS time,
  var_samp(price) AS var_samp,
  skew_samp(price) AS skew_samp,
  kurt_samp(price) AS kurt_samp,
  kurtosis_pop(price) AS kurtosis_pop,
  variance(quantity) AS variance,
  stddev(quantity) AS stddev,
  twap(price, local_event_time) AS twap,
  arg_max(price, quantity) AS arg_max,
  arg_min(price, quantity) AS arg_min,
  corr(price, quantity) AS corr,
  sum_if(quantity, side = 'buy') AS buy_volume,
  count_if(side = 'sell') AS sell_count,
  avg_if(price, side = 'buy') AS buy_price,
  max_if(price, side = 'sell') AS sell_max,
  latest(side = 'buy') AS last_buy,
  trade_returns((local_event_time, id, 0, price, side, quantity))[1] AS realized_vol,
  trade_returns((local_event_time, 0, to_int64_or_zero(id), price, side, quantity))[2] AS impact,
  distinct_stats((local_event_time, NULL, price))[1] AS distinct_prices,
  median(price) AS price_median,
  vwap(price, quantity) AS vwap
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE;

CREATE EXTERNAL STREAM IF NOT EXISTS hour_out (
  symbol string,
  time int64,
  price_median nullable(float64),
  price_p95 nullable(float64),
  buy_quantity_median nullable(float64)
) SETTINGS type = 'kafka', topic = 'hour', data_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS hour_v INTO hour_out AS
SELECT
  symbol,
  to_unix_timestamp64_micro(window_start) AS time,
  quantile_cont(0.5)(price) AS price_median,
  quantile_cont(0.95)(price) AS price_p95,
  quantile_cont_if(0.5)(quantity, side = 'buy') AS buy_quantity_median
FROM tumble(trades_source, from_unix_timestamp64_micro(local_timestamp), 1h)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE;

CREATE EXTERNAL STREAM IF NOT EXISTS exact_out (
  symbol string,
  time int64,
  price_median nullable(float64),
  price_p95 nullable(float64),
  sides uint64,
  prices uint64
) SETTINGS type = 'kafka', topic = 'exact', data_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS exact_v INTO exact_out AS
SELECT
  symbol,
  to_unix_timestamp64_micro(window_start) AS time,
  quantile_exact(0.5)(price) AS price_median,
  quantile_exact(0.95)(price) AS price_p95,
  uniq_exact(side) AS sides,
  uniq_exact(price) AS prices
FROM tumble(trades_source, from_unix_timestamp64_micro(local_timestamp), 1h)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE;

CREATE STREAM IF NOT EXISTS bars (
  second datetime64(6),
  symbol string,
  close nullable(float64),
  mean nullable(float64),
  volume nullable(float64)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_v INTO bars AS
SELECT window_start AS second, symbol, arg_max(price, local_timestamp) AS close, avg(price) AS mean, sum(quantity) AS volume
FROM tumble(trades_source, from_unix_timestamp64_micro(local_timestamp), 1s)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE;

CREATE EXTERNAL STREAM IF NOT EXISTS filled_out (
  time int64,
  symbol string,
  close nullable(float64),
  mean nullable(float64),
  volume float64
) SETTINGS type = 'kafka', topic = 'filled', data_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS filled_v INTO filled_out AS
SELECT to_unix_timestamp64_micro(second) AS time, symbol, close, mean, coalesce(volume, 0) AS volume
FROM gap_fill(bars, second, '1s', symbol, locf(close), interpolate(mean));

CREATE EXTERNAL STREAM IF NOT EXISTS next_out (
  time int64,
  symbol string,
  price float64,
  next_price nullable(float64),
  next_time nullable(int64),
  previous nullable(float64)
) SETTINGS type = 'kafka', topic = 'next', data_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS next_v INTO next_out AS
SELECT
  local_timestamp AS time,
  symbol,
  price,
  lead(price) OVER (PARTITION BY symbol ORDER BY local_timestamp) AS next_price,
  lead(local_timestamp, 3) OVER (PARTITION BY symbol ORDER BY local_timestamp) AS next_time,
  lag(price) OVER (PARTITION BY symbol ORDER BY local_timestamp) AS previous
FROM trades_source;

CREATE EXTERNAL STREAM IF NOT EXISTS reversal_out (
  symbol string,
  time int64,
  reversal nullable(float64)
) SETTINGS type = 'kafka', topic = 'reversal', data_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS reversal_v INTO reversal_out AS
SELECT
  symbol,
  to_unix_timestamp64_micro(window_start) AS time,
  trade_reversal(5, 0.9)((from_unix_timestamp64_micro(local_timestamp), time, to_int64_or_zero(id), price, side, quantity)) AS reversal
FROM tumble(trades_source, from_unix_timestamp64_micro(local_timestamp), 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE;
