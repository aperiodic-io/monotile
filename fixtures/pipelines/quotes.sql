-- Top-of-book metrics from quotes (spread, depth, imbalance: the last value of each window and
-- its mean), and the slippage of trades against the quote they met: an ASOF join of trades to
-- quotes, then its mean, spread and percentiles per side.

CREATE EXTERNAL STREAM IF NOT EXISTS quotes (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  bid_price float64,
  bid_amount float64,
  ask_price float64,
  ask_amount float64,
  local_timestamp int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'quotes',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Quote',
  group_name = 'quotes',
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
  group_name = 'quotes',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS top_of_book (
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  bid_price_value nullable(float64),
  ask_price_value nullable(float64),
  spread_value nullable(float64),
  spread_bps_value nullable(float64),
  total_depth_value nullable(float64),
  imbalance_value nullable(float64),
  imbalance_ratio_value nullable(float64),
  spread_captured nullable(datetime64(6)),
  imbalance_captured nullable(datetime64(6))
);

CREATE EXTERNAL STREAM IF NOT EXISTS spread_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  spread nullable(float64),
  spread_avg nullable(float64),
  spread_min nullable(float64),
  spread_max nullable(float64),
  spread_bps_avg nullable(float32),
  depth_avg nullable(float64),
  imbalance nullable(float64),
  imbalance_ratio_avg nullable(float64),
  quotes uint64,
  quotes_without_spread uint64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'quotes.spread',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE STREAM IF NOT EXISTS slippage_trades (
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  side low_cardinality(string),
  trade_id int64,
  price_value float64,
  size_value float64
);

CREATE STREAM IF NOT EXISTS slippage_quotes (
  local_event_time datetime64(6),
  symbol low_cardinality(string),
  bid_price_value float64,
  ask_price_value float64
);

CREATE STREAM IF NOT EXISTS slippage_joined (
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  side low_cardinality(string),
  price_value float64,
  size_value float64,
  ask_price_value nullable(float64),
  bid_price_value nullable(float64)
);

CREATE STREAM IF NOT EXISTS slippage_enriched (
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  side low_cardinality(string),
  size_value float64,
  slippage_value float64,
  slippage_bps_value float64
);

CREATE EXTERNAL STREAM IF NOT EXISTS slippage_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  slippage_mean float64,
  slippage_bps_std nullable(float64),
  slippage_bps_median nullable(float64),
  slippage_bps_p95 nullable(float64),
  slippage_bps_vwap nullable(float64),
  slippage_bps_buy_mean nullable(float64),
  slippage_bps_sell_mean nullable(float64),
  buy_sell_ratio nullable(float64)
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'slippage',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS top_of_book_mv INTO top_of_book AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  exchange,
  symbol,
  null_if(bid_price, 0) AS bid_price_value,
  null_if(ask_price, 0) AS ask_price_value,
  abs(ask_price_value - bid_price_value) AS spread_value,
  abs(ask_price_value - bid_price_value) / null_if((ask_price_value + bid_price_value) / 2, 0) * 10000 AS spread_bps_value,
  null_if(bid_amount, 0) + null_if(ask_amount, 0) AS total_depth_value,
  bid_amount - ask_amount AS imbalance_value,
  (bid_amount - ask_amount) / null_if(bid_amount + ask_amount, 0) AS imbalance_ratio_value,
  if(spread_value IS NULL, NULL, local_event_time) AS spread_captured,
  if(imbalance_value IS NULL, NULL, local_event_time) AS imbalance_captured
FROM quotes;

-- the last value of a window is the one captured last (a quote without it does not count)
CREATE MATERIALIZED VIEW IF NOT EXISTS spread_1m INTO spread_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  arg_max(spread_value, spread_captured) AS spread,
  avg(spread_value) AS spread_avg,
  min(spread_value) AS spread_min,
  max(spread_value) AS spread_max,
  to_float32(avg(spread_bps_value)) AS spread_bps_avg,
  avg(total_depth_value) AS depth_avg,
  arg_max(imbalance_value, imbalance_captured) AS imbalance,
  avg(imbalance_ratio_value) AS imbalance_ratio_avg,
  count() AS quotes,
  count_if(spread_value IS NULL) AS quotes_without_spread
FROM tumble(top_of_book, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

CREATE MATERIALIZED VIEW IF NOT EXISTS slippage_trades_mv INTO slippage_trades AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  exchange,
  symbol,
  side,
  to_int64_or_zero(id) AS trade_id,
  price AS price_value,
  quantity AS size_value
FROM trades
WHERE side IN ('buy', 'sell');

CREATE MATERIALIZED VIEW IF NOT EXISTS slippage_quotes_mv INTO slippage_quotes AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  symbol,
  bid_price AS bid_price_value,
  ask_price AS ask_price_value
FROM quotes;

-- each trade with the latest quote of its symbol at or before it
CREATE MATERIALIZED VIEW IF NOT EXISTS slippage_joined_mv INTO slippage_joined AS
SELECT
  t.local_event_time AS local_event_time,
  t.exchange AS exchange,
  t.symbol AS symbol,
  t.side AS side,
  t.price_value AS price_value,
  t.size_value AS size_value,
  null_if(q.ask_price_value, 0) AS ask_price_value,
  null_if(q.bid_price_value, 0) AS bid_price_value
FROM (
  SELECT local_event_time, exchange, symbol, side, price_value, size_value
  FROM slippage_trades
  ORDER BY symbol, local_event_time
) AS t
ASOF LEFT JOIN (
  SELECT local_event_time, symbol, ask_price_value, bid_price_value
  FROM slippage_quotes
  ORDER BY symbol, local_event_time
) AS q
ON t.symbol = q.symbol AND t.local_event_time >= q.local_event_time
SETTINGS keep_versions = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS slippage_enriched_mv INTO slippage_enriched AS
SELECT
  local_event_time,
  exchange,
  symbol,
  side,
  size_value,
  if(side = 'buy', price_value - ask_price_value, bid_price_value - price_value) AS slippage_value,
  slippage_value / null_if((ask_price_value + bid_price_value) / 2, 0) * 10000 AS slippage_bps_value
FROM slippage_joined
WHERE ask_price_value IS NOT NULL AND bid_price_value IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS slippage_1m INTO slippage_out AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  avg(slippage_value) AS slippage_mean,
  stddev(slippage_bps_value) AS slippage_bps_std,
  quantile_cont(0.5)(slippage_bps_value) AS slippage_bps_median,
  quantile_cont(0.95)(slippage_bps_value) AS slippage_bps_p95,
  sum(slippage_bps_value * size_value) / null_if(sum(size_value), 0) AS slippage_bps_vwap,
  avg_if(slippage_bps_value, side = 'buy') AS slippage_bps_buy_mean,
  avg_if(slippage_bps_value, side = 'sell') AS slippage_bps_sell_mean,
  slippage_bps_buy_mean / null_if(slippage_bps_sell_mean, 0) AS buy_sell_ratio
FROM tumble(slippage_enriched, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
