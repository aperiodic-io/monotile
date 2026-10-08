-- Sequences of trades: log returns against the previous trade (window functions in arrival
-- order), realized volatility, bipower variation and the return autocorrelation per window;
-- runs of same-side trades; upticks and downticks; and price impact from the sequence of
-- trades of each window (`trade_returns`).

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
  group_name = 'returns',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS sequence_input (
  local_event_time datetime64(6),
  event_time_us int64,
  id string,
  exchange int64,
  symbol low_cardinality(string),
  side low_cardinality(string),
  price_value float64,
  size float64,
  notional_value float64
);

CREATE STREAM IF NOT EXISTS returns_lagged (
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  time_us int64,
  previous_time_us nullable(int64),
  logret nullable(float64),
  previous_logret nullable(float64)
);

CREATE EXTERNAL STREAM IF NOT EXISTS returns_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  logret_var nullable(float32),
  realized_vol float32,
  bipower_variation float32,
  ret_autocorr nullable(float32),
  trendiness nullable(float32)
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'returns',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE EXTERNAL STREAM IF NOT EXISTS runs_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  buy_run_max_len nullable(float64),
  sell_run_max_len nullable(float64),
  buy_run_mean_len nullable(float64),
  sell_run_mean_len nullable(float64),
  run_imbalance nullable(float64),
  flip_rate nullable(float64),
  price_change_on_flip nullable(float64),
  uptick_count int32,
  downtick_count int32,
  unchanged_count int32,
  uptick_volume nullable(float64),
  downtick_volume nullable(float64),
  uptick_share nullable(float64)
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'runs',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE EXTERNAL STREAM IF NOT EXISTS impact_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  impact_per_notional nullable(float32),
  lambda nullable(float32),
  impact_asymmetry nullable(float32),
  realized_vol nullable(float32)
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'impact',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS sequence_input_mv INTO sequence_input AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  time AS event_time_us,
  id,
  exchange,
  symbol,
  side,
  price AS price_value,
  quantity AS size,
  amount AS notional_value
FROM trades
WHERE side IN ('buy', 'sell') AND price IS NOT NULL
ORDER BY local_event_time, id
SETTINGS order_hold_ms = 50;

-- each trade with the one before it, in arrival order
CREATE MATERIALIZED VIEW IF NOT EXISTS returns_lagged_mv INTO returns_lagged AS
SELECT
  local_event_time,
  exchange,
  symbol,
  to_unix_timestamp64_micro(local_event_time) AS time_us,
  to_unix_timestamp64_micro(lag(local_event_time) OVER (PARTITION BY symbol)) AS previous_time_us,
  ln(price_value / lag(price_value) OVER (PARTITION BY symbol)) AS logret,
  ln(lag(price_value) OVER (PARTITION BY symbol) / lag(price_value, 2) OVER (PARTITION BY symbol)) AS previous_logret
FROM sequence_input;

-- a return counts in the window both of its trades are in
CREATE MATERIALIZED VIEW IF NOT EXISTS returns_1m INTO returns_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  to_float32(variance(if(previous_time_us - previous_time_us % 60000000 = time_us - time_us % 60000000, logret, NULL))) AS logret_var,
  to_float32(sqrt(sum(if(previous_time_us - previous_time_us % 60000000 = time_us - time_us % 60000000, logret * logret, 0)))) AS realized_vol,
  to_float32(1.5707963267948966 * sum(abs(logret) * abs(previous_logret))) AS bipower_variation,
  to_float32(corr(logret, previous_logret)) AS ret_autocorr,
  to_float32(abs(sum(logret)) / null_if(sum(abs(logret)), 0)) AS trendiness
FROM tumble(returns_lagged, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE;

CREATE MATERIALIZED VIEW IF NOT EXISTS runs_1m INTO runs_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  run_structure((local_event_time, id, side, price_value))[1] AS buy_run_max_len,
  run_structure((local_event_time, id, side, price_value))[2] AS sell_run_max_len,
  run_structure((local_event_time, id, side, price_value))[3] AS buy_run_mean_len,
  run_structure((local_event_time, id, side, price_value))[4] AS sell_run_mean_len,
  coalesce((buy_run_max_len - sell_run_max_len) / null_if(buy_run_max_len + sell_run_max_len, 0), 0) AS run_imbalance,
  run_structure((local_event_time, id, side, price_value))[5] / run_structure((local_event_time, id, side, price_value))[6] AS flip_rate,
  run_structure((local_event_time, id, side, price_value))[7] AS price_change_on_flip,
  to_int32(updownticks((local_event_time, id, price_value, size))[1]) AS uptick_count,
  to_int32(updownticks((local_event_time, id, price_value, size))[2]) AS downtick_count,
  to_int32(updownticks((local_event_time, id, price_value, size))[3]) AS unchanged_count,
  updownticks((local_event_time, id, price_value, size))[4] AS uptick_volume,
  updownticks((local_event_time, id, price_value, size))[5] AS downtick_volume,
  to_float64(uptick_count) / null_if(uptick_count + downtick_count + unchanged_count, 0) AS uptick_share
FROM tumble(sequence_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE;

CREATE MATERIALIZED VIEW IF NOT EXISTS impact_1m INTO impact_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  to_float32(abs((trade_returns((local_event_time, event_time_us, to_int64_or_zero(id), price_value, side, size))[7] / null_if(trade_returns((local_event_time, event_time_us, to_int64_or_zero(id), price_value, side, size))[6], 0) - 1.0)) / null_if(abs(sum(if(side = 'buy', notional_value, -notional_value))), 0)) AS impact_per_notional,
  to_float32(trade_returns((local_event_time, event_time_us, to_int64_or_zero(id), price_value, side, size))[2] / null_if(trade_returns((local_event_time, event_time_us, to_int64_or_zero(id), price_value, side, size))[3], 0)) AS lambda,
  to_float32(trade_returns((local_event_time, event_time_us, to_int64_or_zero(id), price_value, side, size))[4] - trade_returns((local_event_time, event_time_us, to_int64_or_zero(id), price_value, side, size))[5]) AS impact_asymmetry,
  to_float32(trade_returns((local_event_time, event_time_us, to_int64_or_zero(id), price_value, side, size))[1]) AS realized_vol
FROM tumble(sequence_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE;
