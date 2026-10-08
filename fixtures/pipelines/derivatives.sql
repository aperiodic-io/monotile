-- Perpetual-swap context: the basis of the mark price over the index and last prices (each mark
-- joined to the latest index and trade before it), funding-rate updates, open interest,
-- liquidations by side, and long/short positioning.

CREATE EXTERNAL STREAM IF NOT EXISTS mark_prices (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  mark_price float64,
  local_timestamp int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'mark_prices',
  data_format = 'ProtobufSingle',
  format_schema = 'market:MarkPrice',
  group_name = 'derivatives',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS index_prices (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  index_price float64,
  local_timestamp int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'index_prices',
  data_format = 'ProtobufSingle',
  format_schema = 'market:IndexPrice',
  group_name = 'derivatives',
  seek_to = 'earliest';

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
  group_name = 'derivatives',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS funding_rates (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  funding_rate float64,
  local_timestamp int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'funding_rates',
  data_format = 'ProtobufSingle',
  format_schema = 'market:FundingRate',
  group_name = 'derivatives',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS open_interest (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  local_timestamp int64,
  open_interest float64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'open_interest',
  data_format = 'ProtobufSingle',
  format_schema = 'market:OpenInterest',
  group_name = 'derivatives',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS liquidations (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  price float64,
  local_timestamp int64,
  side low_cardinality(string),
  quantity float64,
  amount float64,
  id string
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'liquidations',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Liquidation',
  group_name = 'derivatives',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS long_short_ratios (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  long_short_ratio float64,
  long_share float64,
  short_share float64,
  local_timestamp int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'long_short_ratios',
  data_format = 'ProtobufSingle',
  format_schema = 'market:LongShortRatio',
  group_name = 'derivatives',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS marks (
  local_event_time datetime64(6),
  event_time_us int64,
  exchange int64,
  symbol low_cardinality(string),
  mark_price_value float64
);

CREATE STREAM IF NOT EXISTS indexes (
  local_event_time datetime64(6),
  symbol low_cardinality(string),
  index_price_value float64
);

CREATE STREAM IF NOT EXISTS lasts (
  local_event_time datetime64(6),
  symbol low_cardinality(string),
  last_price_value float64
);

CREATE STREAM IF NOT EXISTS price_context (
  local_event_time datetime64(6),
  event_time_us int64,
  exchange int64,
  symbol low_cardinality(string),
  mark_price_value nullable(float64),
  index_price_value nullable(float64),
  last_price_value nullable(float64)
);

CREATE STREAM IF NOT EXISTS funding_input (
  local_event_time datetime64(6),
  event_time_us int64,
  exchange int64,
  symbol low_cardinality(string),
  funding_rate_value float64,
  previous_funding_rate_value nullable(float64)
);

CREATE STREAM IF NOT EXISTS open_interest_input (
  local_event_time datetime64(6),
  event_time_us int64,
  exchange int64,
  symbol low_cardinality(string),
  open_interest_value nullable(float64)
);

CREATE STREAM IF NOT EXISTS liquidation_input (
  event_time datetime64(6),
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  side_value low_cardinality(string),
  quantity_value float64,
  amount_value float64
);

CREATE STREAM IF NOT EXISTS long_short_input (
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  long_short_ratio float64,
  long_share float64,
  short_share float64
);

CREATE EXTERNAL STREAM IF NOT EXISTS basis_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  basis nullable(float32),
  basis_bps nullable(float32),
  mark_spread nullable(float32),
  basis_mean nullable(float32),
  basis_bps_std nullable(float32)
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'basis', data_format = 'JSONEachRow';

CREATE EXTERNAL STREAM IF NOT EXISTS funding_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  funding_rate float32,
  updates_per_second float32,
  seconds_per_update nullable(float32)
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'funding', data_format = 'JSONEachRow';

CREATE EXTERNAL STREAM IF NOT EXISTS open_interest_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  open_interest nullable(float64),
  open_interest_first nullable(float64),
  open_interest_volatility nullable(float32)
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'open_interest_1m', data_format = 'JSONEachRow';

CREATE EXTERNAL STREAM IF NOT EXISTS liquidations_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  quantity float64,
  amount float64,
  buy_amount float64,
  sell_amount float64,
  liquidation_count int32,
  notional_delta float64,
  notional_imbalance nullable(float64)
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'liquidations_15s', data_format = 'JSONEachRow';

CREATE EXTERNAL STREAM IF NOT EXISTS long_short_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  long_short_ratio float64,
  long_share float64,
  short_share float64
) SETTINGS type = 'kafka', brokers = 'localhost:9092', topic = 'long_short', data_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS marks_mv INTO marks AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, time AS event_time_us, exchange, symbol, mark_price AS mark_price_value
FROM mark_prices
WHERE mark_price != 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS indexes_mv INTO indexes AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, symbol, index_price AS index_price_value
FROM index_prices
WHERE index_price != 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS lasts_mv INTO lasts AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, symbol, price AS last_price_value
FROM trades
WHERE price != 0;

-- every mark with the latest index and trade price at or before it
CREATE MATERIALIZED VIEW IF NOT EXISTS price_context_mv INTO price_context AS
SELECT
  m.local_event_time AS local_event_time,
  m.event_time_us AS event_time_us,
  m.exchange AS exchange,
  m.symbol AS symbol,
  m.mark_price_value AS mark_price_value,
  null_if(i.index_price_value, 0) AS index_price_value,
  null_if(t.last_price_value, 0) AS last_price_value
FROM (
  SELECT local_event_time, event_time_us, exchange, symbol, mark_price_value
  FROM marks
  ORDER BY symbol, local_event_time
) AS m
ASOF LEFT JOIN (
  SELECT local_event_time, symbol, index_price_value
  FROM indexes
  ORDER BY symbol, local_event_time
) AS i
ON m.symbol = i.symbol AND m.local_event_time >= i.local_event_time
ASOF LEFT JOIN (
  SELECT local_event_time, symbol, last_price_value
  FROM lasts
  ORDER BY symbol, local_event_time
) AS t
ON m.symbol = t.symbol AND m.local_event_time >= t.local_event_time
SETTINGS keep_versions = 5000;

-- distinct_stats: the mean, standard deviation and volatility of the values that changed
CREATE MATERIALIZED VIEW IF NOT EXISTS basis_1m INTO basis_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  to_float32(latest(mark_price_value - index_price_value)) AS basis,
  to_float32(latest((mark_price_value - index_price_value) / index_price_value * 10000)) AS basis_bps,
  to_float32(latest(mark_price_value - last_price_value)) AS mark_spread,
  to_float32(distinct_stats((local_event_time, event_time_us, mark_price_value - index_price_value))[1]) AS basis_mean,
  to_float32(distinct_stats((local_event_time, event_time_us, (mark_price_value - index_price_value) / index_price_value * 10000))[2]) AS basis_bps_std
FROM tumble(price_context, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

-- the previous rate of the same symbol and day
CREATE MATERIALIZED VIEW IF NOT EXISTS funding_input_mv INTO funding_input AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  time AS event_time_us,
  exchange,
  symbol,
  funding_rate AS funding_rate_value,
  lag(funding_rate) OVER (PARTITION BY symbol, to_start_of_day(from_unix_timestamp64_micro(local_timestamp))) AS previous_funding_rate_value
FROM funding_rates;

CREATE MATERIALIZED VIEW IF NOT EXISTS funding_1m INTO funding_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  to_float32(latest(funding_rate_value)) AS funding_rate,
  to_float32(count_if(previous_funding_rate_value IS NULL OR funding_rate_value != previous_funding_rate_value) / 60.0) AS updates_per_second,
  to_float32(60.0 / null_if(count_if(previous_funding_rate_value IS NULL OR funding_rate_value != previous_funding_rate_value), 0)) AS seconds_per_update
FROM tumble(funding_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS open_interest_input_mv INTO open_interest_input AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, time AS event_time_us, exchange, symbol, null_if(open_interest, 0) AS open_interest_value
FROM open_interest;

CREATE MATERIALIZED VIEW IF NOT EXISTS open_interest_1m INTO open_interest_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  latest(open_interest_value) AS open_interest,
  earliest(open_interest_value) AS open_interest_first,
  to_float32(distinct_stats((local_event_time, event_time_us, open_interest_value))[3]) AS open_interest_volatility
FROM tumble(open_interest_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS liquidation_input_mv INTO liquidation_input AS
SELECT
  from_unix_timestamp64_micro(time) AS event_time,
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  exchange,
  symbol,
  side AS side_value,
  quantity AS quantity_value,
  amount AS amount_value
FROM liquidations
WHERE quantity IS NOT NULL AND amount IS NOT NULL AND side IN ('buy', 'sell');

CREATE MATERIALIZED VIEW IF NOT EXISTS liquidations_15s INTO liquidations_out AS
SELECT
  exchange,
  symbol,
  '15s' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  sum(quantity_value) AS quantity,
  sum(amount_value) AS amount,
  sum(CASE WHEN side_value = 'buy' THEN amount_value ELSE 0 END) AS buy_amount,
  sum(CASE WHEN side_value = 'sell' THEN amount_value ELSE 0 END) AS sell_amount,
  to_int32(count()) AS liquidation_count,
  sell_amount - buy_amount AS notional_delta,
  (sell_amount - buy_amount) / null_if(sell_amount + buy_amount, 0) AS notional_imbalance
FROM tumble(liquidation_input, local_event_time, 15s)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS long_short_input_mv INTO long_short_input AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, exchange, symbol, long_short_ratio, long_share, short_share
FROM long_short_ratios
WHERE long_short_ratio IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS long_short_15s INTO long_short_out AS
SELECT
  exchange,
  symbol,
  '15s' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  latest(long_short_ratio) AS long_short_ratio,
  latest(long_share) AS long_share,
  latest(short_share) AS short_share
FROM tumble(long_short_input, local_event_time, 15s)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
