-- windows.sql's 1m windows, then a view whose windows are costly to close: a day-long window per
-- trade, with a t-digest median and seven columns, so the closing trade closes tens of
-- thousands of groups after the 1m view's last windows. Each view writes a topic of its
-- own; the 1m view is created first, so it runs first.
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

CREATE STREAM IF NOT EXISTS trades_input (local_event_time datetime64(6), id string, symbol string, price float64);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_input_mv INTO trades_input AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, id, symbol, price FROM trades_source;

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

CREATE EXTERNAL STREAM IF NOT EXISTS test_ids_kafka_out (
  id string,
  symbol string,
  time int64,
  first float64,
  last float64,
  low float64,
  high float64,
  median float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['redpanda-dedup-key'], [concat(to_string(time), '|', id)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.ids',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_ids INTO test_ids_kafka_out AS
SELECT id, symbol, to_unix_timestamp64_micro(window_start) AS time, earliest(price) AS first,
  latest(price) AS last, min(price) AS low, max(price) AS high, quantile_t_digest(0.5)(price) AS median
FROM tumble(trades_input, local_event_time, 1d)
GROUP BY window_start, id, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
