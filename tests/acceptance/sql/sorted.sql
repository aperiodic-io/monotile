-- windows.sql's trades, some without a price (the protobuf field absent: NULL in a nullable
-- column), through two views that sort on it: one passes each trade on in price order,
-- one orders its 1m windows by their lowest price, NULL for a symbol whose trades of the minute
-- all had none. A sort used to panic on NULL keys, and the data loop with it.
CREATE EXTERNAL STREAM IF NOT EXISTS trades_source (
  time int64,
  id string,
  exchange int64,
  symbol low_cardinality(string),
  price nullable(float64),
  local_timestamp int64,
  side low_cardinality(string),
  quantity float64,
  amount float64
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'raw.trades.test',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Trade',
  group_name = 'test-trades-source',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS trades_input (local_event_time datetime64(6), id string, symbol string, price nullable(float64));

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_input_mv INTO trades_input AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, id, symbol, price FROM trades_source;

CREATE EXTERNAL STREAM IF NOT EXISTS test_sorted_kafka_out (
  id string,
  symbol string,
  price nullable(float64),
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [id]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.sorted',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_sorted INTO test_sorted_kafka_out AS
SELECT id, symbol, price FROM trades_input ORDER BY price;

CREATE EXTERNAL STREAM IF NOT EXISTS test_1m_kafka_out (
  symbol string,
  time int64,
  n uint64,
  low nullable(float64),
  high nullable(float64),
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [concat(to_string(time), '|', symbol)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.1m',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_1m INTO test_1m_kafka_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS n, min(price) AS low, max(price) AS high
FROM tumble(trades_input, local_event_time, 1m)
GROUP BY window_start, symbol
ORDER BY low
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
