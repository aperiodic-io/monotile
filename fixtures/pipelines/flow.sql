-- Order flow from trades: taker buy and sell volume and counts by order size, their imbalance
-- and entropy, a short-horizon realized volatility, and a z-score of the imbalance against the
-- day so far (window functions over the windows' output). Then the same flow over two venues
-- together: their trades into one stream, a window over it in a CTE.

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
  group_name = 'flow',
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
  topic = 'spot.trades',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Trade',
  group_name = 'flow',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS flow_input (
  local_event_time datetime64(6),
  id string,
  exchange int64,
  symbol low_cardinality(string),
  side low_cardinality(string),
  price float64,
  size float64,
  notional float64
);

CREATE STREAM IF NOT EXISTS flow_base (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  buy_notional float64,
  sell_notional float64,
  buy_count int32,
  sell_count int32,
  volume_delta float64,
  notional_delta float64,
  entropy float32,
  buy_sell_ratio nullable(float32),
  buy_share nullable(float32),
  small_buy_notional float64,
  large_buy_notional float64,
  small_sell_count int32,
  large_sell_count int32,
  realized_vol float32
);

CREATE EXTERNAL STREAM IF NOT EXISTS flow_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  buy_notional float64,
  sell_notional float64,
  buy_count int32,
  sell_count int32,
  volume_delta float64,
  notional_delta float64,
  entropy float32,
  buy_sell_ratio nullable(float32),
  buy_share nullable(float32),
  small_buy_notional float64,
  large_buy_notional float64,
  small_sell_count int32,
  large_sell_count int32,
  realized_vol float32,
  delta_zscore nullable(float32),
  _tp_message_headers map(string, string) MATERIALIZED cast((['dedup-key'], [concat(to_string(time), '|', 'flow', '|', to_string(exchange), '|', symbol, '|', interval)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'flow',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE STREAM IF NOT EXISTS all_venues_input (
  local_event_time datetime64(6),
  symbol low_cardinality(string),
  side low_cardinality(string),
  notional float64
);

CREATE EXTERNAL STREAM IF NOT EXISTS all_venues_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  buy_notional float64,
  sell_notional float64,
  notional_delta float64,
  buy_count int32,
  sell_count int32,
  buy_sell_ratio float64,
  buy_count_share float64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'flow.all-venues',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS flow_input_mv INTO flow_input AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  id,
  exchange,
  symbol,
  side,
  price,
  quantity AS size,
  amount AS notional
FROM trades
WHERE side IN ('buy', 'sell')
ORDER BY local_event_time, id
SETTINGS order_hold_ms = 50;

CREATE MATERIALIZED VIEW IF NOT EXISTS flow_15s INTO flow_base AS
SELECT
  exchange,
  symbol,
  '15s' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  sum(if(side = 'buy', notional, 0)) AS buy_notional,
  sum(if(side = 'sell', notional, 0)) AS sell_notional,
  to_int32(count_if(side = 'buy')) AS buy_count,
  to_int32(count_if(side = 'sell')) AS sell_count,
  sum(if(side = 'buy', size, 0)) - sum(if(side = 'sell', size, 0)) AS volume_delta,
  buy_notional - sell_notional AS notional_delta,
  to_float32(if(buy_notional > 0, -((buy_notional / (buy_notional + sell_notional)) * ln(buy_notional / (buy_notional + sell_notional))), 0) + if(sell_notional > 0, -((sell_notional / (buy_notional + sell_notional)) * ln(sell_notional / (buy_notional + sell_notional))), 0)) AS entropy,
  buy_notional / null_if(sell_notional, 0) AS buy_sell_ratio,
  buy_notional / null_if(buy_notional + sell_notional, 0) AS buy_share,
  sum(if(side = 'buy' AND notional < 100, notional, 0)) AS small_buy_notional,
  sum(if(side = 'buy' AND notional >= 1000, notional, 0)) AS large_buy_notional,
  to_int32(count_if(side = 'sell' AND notional < 100)) AS small_sell_count,
  to_int32(count_if(side = 'sell' AND notional >= 1000)) AS large_sell_count,
  to_float32(trade_returns((local_event_time, id, 0, price, side, size))[1]) AS realized_vol
FROM tumble(flow_input, local_event_time, 15s)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE;

CREATE MATERIALIZED VIEW IF NOT EXISTS flow_1m INTO flow_base AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  sum(if(side = 'buy', notional, 0)) AS buy_notional,
  sum(if(side = 'sell', notional, 0)) AS sell_notional,
  to_int32(count_if(side = 'buy')) AS buy_count,
  to_int32(count_if(side = 'sell')) AS sell_count,
  sum(if(side = 'buy', size, 0)) - sum(if(side = 'sell', size, 0)) AS volume_delta,
  buy_notional - sell_notional AS notional_delta,
  to_float32(if(buy_notional > 0, -((buy_notional / (buy_notional + sell_notional)) * ln(buy_notional / (buy_notional + sell_notional))), 0) + if(sell_notional > 0, -((sell_notional / (buy_notional + sell_notional)) * ln(sell_notional / (buy_notional + sell_notional))), 0)) AS entropy,
  buy_notional / null_if(sell_notional, 0) AS buy_sell_ratio,
  buy_notional / null_if(buy_notional + sell_notional, 0) AS buy_share,
  sum(if(side = 'buy' AND notional < 100, notional, 0)) AS small_buy_notional,
  sum(if(side = 'buy' AND notional >= 1000, notional, 0)) AS large_buy_notional,
  to_int32(count_if(side = 'sell' AND notional < 100)) AS small_sell_count,
  to_int32(count_if(side = 'sell' AND notional >= 1000)) AS large_sell_count,
  to_float32(trade_returns((local_event_time, id, 0, price, side, size))[1]) AS realized_vol
FROM tumble(flow_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE;

-- the imbalance against the day's so far
CREATE MATERIALIZED VIEW IF NOT EXISTS flow_out_mv INTO flow_out AS
SELECT
  exchange, symbol, interval, day, time, buy_notional, sell_notional, buy_count, sell_count,
  volume_delta, notional_delta, entropy, buy_sell_ratio, buy_share, small_buy_notional,
  large_buy_notional, small_sell_count, large_sell_count, realized_vol,
  to_float32((notional_delta - avg(notional_delta) OVER (PARTITION BY exchange, symbol, interval, day ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) / null_if(stddev(notional_delta) OVER (PARTITION BY exchange, symbol, interval, day ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS delta_zscore
FROM flow_base;

-- both venues' trades in one stream
CREATE MATERIALIZED VIEW IF NOT EXISTS all_venues_perp_mv INTO all_venues_input AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  symbol,
  side,
  amount AS notional
FROM trades
WHERE amount IS NOT NULL AND side IN ('buy', 'sell');

CREATE MATERIALIZED VIEW IF NOT EXISTS all_venues_spot_mv INTO all_venues_input AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  symbol,
  side,
  amount AS notional
FROM spot_trades
WHERE amount IS NOT NULL AND side IN ('buy', 'sell');

CREATE MATERIALIZED VIEW IF NOT EXISTS all_venues_1m INTO all_venues_out AS
WITH base AS (
  SELECT
    window_start,
    symbol,
    sum(CASE WHEN side = 'buy' THEN notional ELSE 0 END) AS buy_notional,
    sum(CASE WHEN side = 'sell' THEN notional ELSE 0 END) AS sell_notional,
    sum(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) AS buy_count,
    sum(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) AS sell_count
  FROM tumble(all_venues_input, local_event_time, 1m)
  GROUP BY window_start, symbol
)
SELECT
  0 AS exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  buy_notional,
  sell_notional,
  buy_notional - sell_notional AS notional_delta,
  to_int32(buy_count) AS buy_count,
  to_int32(sell_count) AS sell_count,
  coalesce(buy_notional / null_if(sell_notional, 0), 0) AS buy_sell_ratio,
  coalesce(to_float64(buy_count) / null_if(buy_count + sell_count, 0), 0) AS buy_count_share
FROM base
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
