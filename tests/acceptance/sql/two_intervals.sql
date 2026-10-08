-- windows.sql with two sinks named <metric>.v<N>.<venue>.<interval>, and the interval in the
-- message and its dedup key.
CREATE EXTERNAL STREAM IF NOT EXISTS trades_source (
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
  brokers = 'redpanda:9092',
  topic = 'raw.trades.test',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Trade',
  group_name = 'test-trades-source',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS trades_input (local_event_time datetime64(6), symbol string, price float64);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_input_mv INTO trades_input AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, symbol, price FROM trades_source;

CREATE EXTERNAL STREAM IF NOT EXISTS test_1m_kafka_out (
  symbol string,
  interval string,
  time int64,
  n uint64,
  last float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [concat(to_string(time), '|', symbol, '|', interval)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.v1.test.1m',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_1m INTO test_1m_kafka_out AS
SELECT symbol, '1m' AS interval, to_unix_timestamp64_micro(window_start) AS time, count() AS n, latest(price) AS last
FROM tumble(trades_input, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

CREATE EXTERNAL STREAM IF NOT EXISTS test_5m_kafka_out (
  symbol string,
  interval string,
  time int64,
  n uint64,
  last float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [concat(to_string(time), '|', symbol, '|', interval)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.v1.test.5m',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_5m INTO test_5m_kafka_out AS
SELECT symbol, '5m' AS interval, to_unix_timestamp64_micro(window_start) AS time, count() AS n, latest(price) AS last
FROM tumble(trades_input, local_event_time, 5m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
