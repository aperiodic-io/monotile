-- raw.orderbook as the producers publish it, its books' top 5.
CREATE EXTERNAL STREAM IF NOT EXISTS ob (
  time int64,
  exchange int64,
  symbol low_cardinality(string),
  is_snapshot bool,
  bid_price array(float64),
  bid_amount array(float64),
  ask_price array(float64),
  ask_amount array(float64),
  local_timestamp int64,
  venue_sequence int64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'raw.orderbook.test',
  data_format = 'ProtobufSingle',
  format_schema = 'market:BookUpdate',
  seek_to = 'earliest';

CREATE EXTERNAL STREAM IF NOT EXISTS top (
  time int64,
  symbol string,
  bid_price array(float64),
  ask_price array(float64),
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [concat(to_string(time), '|', symbol)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'book.top',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS top_mv INTO top AS
SELECT time, symbol, bid_price, ask_price
FROM orderbook_top_n(ob, 5);
