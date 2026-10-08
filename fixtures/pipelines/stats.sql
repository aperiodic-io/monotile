-- Price and trade-size distributions per window: range, moments, medians and size buckets per
-- venue, and the same over trades of two venues merged into one stream (symbols normalized to
-- one quote currency).

CREATE EXTERNAL STREAM IF NOT EXISTS trades (
  time int64,
  id string,
  exchange int64,
  symbol low_cardinality(string),
  price float64,
  local_timestamp int64,
  side low_cardinality(string),
  quantity float64,
  amount float64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'trades',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Trade',
  group_name = 'stats',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS spot_trades (
  time int64,
  id string,
  exchange int64,
  symbol low_cardinality(string),
  price float64,
  local_timestamp int64,
  side low_cardinality(string),
  quantity float64,
  amount float64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'spot_trades',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Trade',
  group_name = 'stats',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS stats_input (
  local_event_time datetime64(6),
  event_time_us int64,
  exchange int64,
  symbol low_cardinality(string),
  price_value float64,
  notional_value float64
);

CREATE STREAM IF NOT EXISTS merged_trades (
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  price_value float64
);

CREATE EXTERNAL STREAM IF NOT EXISTS range_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  range float64,
  range_bps nullable(float64),
  price_mean float64,
  price_median float64,
  price_std nullable(float64),
  price_variance nullable(float64),
  price_skewness nullable(float64),
  price_kurtosis nullable(float64),
  price_cv nullable(float64)
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'range', data_format = 'JSONEachRow';

CREATE EXTERNAL STREAM IF NOT EXISTS trade_size_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  small_volume float64,
  small_count int32,
  large_volume float64,
  large_count int32,
  volume float64,
  n_trades int32,
  small_share nullable(float64),
  amount_median float64,
  amount_exact_median float64,
  amount_skewness nullable(float64),
  amount_kurtosis nullable(float64),
  amount_range_ratio nullable(float64)
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'trade_size', data_format = 'JSONEachRow';

CREATE EXTERNAL STREAM IF NOT EXISTS merged_range_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  time int64,
  range float64,
  range_bps float64,
  price_median float64,
  price_std float64,
  price_variance float64,
  price_skewness float64,
  price_kurtosis float64,
  price_cv float64
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'merged_range', data_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS stats_input_mv INTO stats_input AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  time AS event_time_us,
  exchange,
  symbol,
  price AS price_value,
  amount AS notional_value
FROM trades;

-- arg_max by a tuple: the price of the latest trade, ties broken by the venue's time
CREATE MATERIALIZED VIEW IF NOT EXISTS range_1m INTO range_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  max(price_value) - min(price_value) AS range,
  (max(price_value) - min(price_value)) / null_if(arg_max(price_value, (local_event_time, event_time_us)), 0) * 10000 AS range_bps,
  avg(price_value) AS price_mean,
  quantile_cont(0.5)(price_value) AS price_median,
  stddev(price_value) AS price_std,
  variance(price_value) AS price_variance,
  skewness(price_value) AS price_skewness,
  kurtosis(price_value) AS price_kurtosis,
  price_std / null_if(price_mean, 0) AS price_cv
FROM tumble(stats_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS trade_size_1m INTO trade_size_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  sum(if(notional_value < 1000, notional_value, 0)) AS small_volume,
  to_int32(count_if(notional_value < 1000)) AS small_count,
  sum(if(notional_value >= 1000, notional_value, 0)) AS large_volume,
  to_int32(count_if(notional_value >= 1000)) AS large_count,
  sum(notional_value) AS volume,
  to_int32(count()) AS n_trades,
  small_volume / null_if(volume, 0) AS small_share,
  median(notional_value) AS amount_median,
  quantile_exact(0.5)(notional_value) AS amount_exact_median,
  skewness(notional_value) AS amount_skewness,
  kurtosis(notional_value) AS amount_kurtosis,
  max(notional_value) / null_if(min(notional_value), 0) AS amount_range_ratio
FROM tumble(stats_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

-- two views into one stream: both venues' trades, in one quote currency
CREATE MATERIALIZED VIEW IF NOT EXISTS merged_trades_mv INTO merged_trades AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  exchange,
  CASE WHEN match(symbol, '^(spot|perp)-') THEN replace_regexp_all(symbol, '-(?i:USDT|USDC)$', '-USD') ELSE symbol END AS symbol,
  price AS price_value
FROM trades
WHERE price IS NOT NULL AND amount IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS merged_spot_trades_mv INTO merged_trades AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  exchange,
  CASE WHEN match(symbol, '^(spot|perp)-') THEN replace_regexp_all(symbol, '-(?i:USDT|USDC)$', '-USD') ELSE symbol END AS symbol,
  price AS price_value
FROM spot_trades
WHERE price IS NOT NULL AND amount IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS merged_range_15s INTO merged_range_out AS
SELECT
  exchange,
  symbol,
  '15s' AS interval,
  to_unix_timestamp64_micro(window_start) AS time,
  max(price_value) - min(price_value) AS range,
  coalesce((max(price_value) - min(price_value)) / null_if(latest(price_value), 0) * 10000, 0) AS range_bps,
  quantile_t_digest(0.5)(price_value) AS price_median,
  stddev_samp(price_value) AS price_std,
  price_std * price_std AS price_variance,
  skew_samp(price_value) AS price_skewness,
  kurt_samp(price_value) AS price_kurtosis,
  coalesce(stddev_samp(price_value) / null_if(avg(price_value), 0), 0) AS price_cv
FROM tumble(merged_trades, local_event_time, 15s)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
