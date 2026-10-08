-- windows.sql with a large state, shaped like a market-data pipeline's: besides its 1m windows, a day-long
-- window per symbol keeps 16 quantile samplers of up to 8192 values each, so a checkpoint has
-- megabytes to copy, encode and write. Both views write test.1m, under keys of their own,
-- so the topic holds exactly the engine's messages as for windows.sql.
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
  mean float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [concat(to_string(time), '|', symbol)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.1m',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_1m INTO test_1m_kafka_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, count() AS n, latest(price) AS last, avg(price) AS mean
FROM tumble(trades_input, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;

CREATE EXTERNAL STREAM IF NOT EXISTS test_1d_kafka_out (
  symbol string,
  time int64,
  p0 float64,
  p1 float64,
  p2 float64,
  p3 float64,
  p4 float64,
  p5 float64,
  p6 float64,
  p7 float64,
  p8 float64,
  p9 float64,
  p10 float64,
  p11 float64,
  p12 float64,
  p13 float64,
  p14 float64,
  p15 float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [concat(to_string(time), '|1d|', symbol)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.1m',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

-- a sampler of up to 8192 values per quantile, symbol and day
CREATE MATERIALIZED VIEW IF NOT EXISTS test_1d INTO test_1d_kafka_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time,
  quantile(0.50)(price + 0) AS p0,
  quantile(0.51)(price + 1) AS p1,
  quantile(0.52)(price + 2) AS p2,
  quantile(0.53)(price + 3) AS p3,
  quantile(0.54)(price + 4) AS p4,
  quantile(0.55)(price + 5) AS p5,
  quantile(0.56)(price + 6) AS p6,
  quantile(0.57)(price + 7) AS p7,
  quantile(0.58)(price + 8) AS p8,
  quantile(0.59)(price + 9) AS p9,
  quantile(0.60)(price + 10) AS p10,
  quantile(0.61)(price + 11) AS p11,
  quantile(0.62)(price + 12) AS p12,
  quantile(0.63)(price + 13) AS p13,
  quantile(0.64)(price + 14) AS p14,
  quantile(0.65)(price + 15) AS p15
FROM tumble(trades_input, local_event_time, 1d)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
