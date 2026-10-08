-- windows.sql with its bars written to Parquet files (`{dir}`: the scenario's directory).
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
  time int64,
  n uint64,
  last float64,
  mean float64
) SETTINGS
  type = 'file',
  data_format = 'Parquet',
  path = '{dir}/test.1m',
  partition_by = 'symbol';

CREATE MATERIALIZED VIEW IF NOT EXISTS test_1m INTO test_1m_kafka_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS n, latest(price) AS last, avg(price) AS mean
FROM tumble(trades_input, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
