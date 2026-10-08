-- Order book depth: books built from snapshots and changes (`orderbook_top_n`), the amount
-- within the top 5, 10 and 25 levels of each side, then its last value and mean per window.
-- Two venues: one whose book versions only grow, one whose versions may start over.

CREATE EXTERNAL STREAM IF NOT EXISTS book (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  is_snapshot bool,
  local_timestamp int64,
  bid_price array(float64),
  bid_amount array(float64),
  ask_price array(float64),
  ask_amount array(float64),
  venue_sequence int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'book',
  data_format = 'ProtobufSingle',
  format_schema = 'market:BookUpdate',
  group_name = 'book',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS spot_book (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  is_snapshot bool,
  local_timestamp int64,
  bid_price array(float64),
  bid_amount array(float64),
  ask_price array(float64),
  ask_amount array(float64),
  venue_sequence int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'spot.book',
  data_format = 'ProtobufSingle',
  format_schema = 'market:BookUpdate',
  group_name = 'book',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS depth (
  time_us int64,
  exchange int64,
  symbol low_cardinality(string),
  local_timestamp_us int64,
  ask_5 nullable(float64),
  bid_5 nullable(float64),
  ask_10 nullable(float64),
  bid_10 nullable(float64),
  ask_25 nullable(float64),
  bid_25 nullable(float64)
);

CREATE STREAM IF NOT EXISTS depth_input (
  event_time datetime64(6),
  local_event_time datetime64(6),
  exchange int64,
  symbol low_cardinality(string),
  ask_5 nullable(float64),
  bid_5 nullable(float64),
  ask_10 nullable(float64),
  bid_10 nullable(float64),
  ask_25 nullable(float64),
  bid_25 nullable(float64)
);

CREATE EXTERNAL STREAM IF NOT EXISTS depth_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  ask_5 nullable(float64),
  bid_5 nullable(float64),
  ask_5_avg nullable(float32),
  bid_5_avg nullable(float32),
  ask_10 nullable(float64),
  bid_10 nullable(float64),
  ask_25 nullable(float64),
  bid_25 nullable(float64),
  imbalance_25_avg nullable(float64)
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'book.depth',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS depth_mv INTO depth AS
SELECT
  time AS time_us,
  exchange,
  symbol,
  local_timestamp AS local_timestamp_us,
  if(length(ask_amount) > 0, array_sum(array_slice(ask_amount, 1, 5)), NULL) AS ask_5,
  if(length(bid_amount) > 0, array_sum(array_slice(bid_amount, 1, 5)), NULL) AS bid_5,
  if(length(ask_amount) > 0, array_sum(array_slice(ask_amount, 1, 10)), NULL) AS ask_10,
  if(length(bid_amount) > 0, array_sum(array_slice(bid_amount, 1, 10)), NULL) AS bid_10,
  if(length(ask_amount) > 0, array_sum(array_slice(ask_amount, 1, 25)), NULL) AS ask_25,
  if(length(bid_amount) > 0, array_sum(array_slice(bid_amount, 1, 25)), NULL) AS bid_25
FROM orderbook_top_n(book, 25, false);

CREATE MATERIALIZED VIEW IF NOT EXISTS spot_depth_mv INTO depth AS
SELECT
  time AS time_us,
  exchange,
  symbol,
  local_timestamp AS local_timestamp_us,
  if(length(ask_amount) > 0, array_sum(array_slice(ask_amount, 1, 5)), NULL) AS ask_5,
  if(length(bid_amount) > 0, array_sum(array_slice(bid_amount, 1, 5)), NULL) AS bid_5,
  if(length(ask_amount) > 0, array_sum(array_slice(ask_amount, 1, 10)), NULL) AS ask_10,
  if(length(bid_amount) > 0, array_sum(array_slice(bid_amount, 1, 10)), NULL) AS bid_10,
  if(length(ask_amount) > 0, array_sum(array_slice(ask_amount, 1, 25)), NULL) AS ask_25,
  if(length(bid_amount) > 0, array_sum(array_slice(bid_amount, 1, 25)), NULL) AS bid_25
FROM orderbook_top_n(spot_book, 25, true);

CREATE MATERIALIZED VIEW IF NOT EXISTS depth_input_mv INTO depth_input AS
SELECT
  from_unix_timestamp64_micro(time_us) AS event_time,
  from_unix_timestamp64_micro(local_timestamp_us) AS local_event_time,
  exchange,
  symbol,
  ask_5, bid_5, ask_10, bid_10, ask_25, bid_25
FROM depth;

CREATE MATERIALIZED VIEW IF NOT EXISTS depth_15s INTO depth_out AS
SELECT
  exchange,
  symbol,
  '15s' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  latest(ask_5) AS ask_5,
  latest(bid_5) AS bid_5,
  to_float32(avg(ask_5)) AS ask_5_avg,
  to_float32(avg(bid_5)) AS bid_5_avg,
  latest(ask_10) AS ask_10,
  latest(bid_10) AS bid_10,
  latest(ask_25) AS ask_25,
  latest(bid_25) AS bid_25,
  avg((bid_25 - ask_25) / null_if(bid_25 + ask_25, 0)) AS imbalance_25_avg
FROM tumble(depth_input, local_event_time, 15s)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
