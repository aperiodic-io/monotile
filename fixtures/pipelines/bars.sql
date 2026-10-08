-- Bars from trades: open, high, low, close, volume, VWAP and TWAP of every 15 seconds, minute
-- and 5 minutes per exchange and symbol, to a Kafka topic per width and to Parquet files on S3.
-- The windows of one input that differ in width alone are computed together (ADR-0017).

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
  group_name = 'bars',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS bars_input (
  local_event_time datetime64(6),
  event_time_us int64,
  id string,
  exchange int64,
  symbol low_cardinality(string),
  side low_cardinality(string),
  price float64,
  size float64,
  notional float64
)
TTL to_datetime(_tp_time) + INTERVAL 1 HOUR
SETTINGS
  logstore_retention_bytes = 104857600,
  logstore_retention_ms = 600000,
  logstore_codec = 'lz4',
  ttl_only_drop_parts = 1,
  merge_with_ttl_timeout = 600,
  merge_max_block_size = 4096;

CREATE STREAM IF NOT EXISTS bars_agg (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  open float64,
  high float64,
  low float64,
  close float64,
  volume float64,
  notional float64,
  trades int32,
  vwap nullable(float64),
  twap nullable(float64),
  open_by_time float64,
  close_by_time float64
);

CREATE EXTERNAL STREAM IF NOT EXISTS bars_1m_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  open float64,
  high float64,
  low float64,
  close float64,
  volume float64,
  notional float64,
  trades int32,
  vwap nullable(float64),
  twap nullable(float64),
  open_by_time float64,
  close_by_time float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['dedup-key'], [concat(to_string(time), '|', 'bars', '|', to_string(exchange), '|', symbol, '|', interval)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'bars.1m',
  data_format = 'JSONEachRow',
  one_message_per_row = true,
  properties = 'enable.idempotence=false';

CREATE EXTERNAL STREAM IF NOT EXISTS bars_5m_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  open float64,
  high float64,
  low float64,
  close float64,
  volume float64,
  notional float64,
  trades int32,
  vwap nullable(float64),
  twap nullable(float64),
  open_by_time float64,
  close_by_time float64,
  _tp_message_headers map(string, string) MATERIALIZED cast((['dedup-key'], [concat(to_string(time), '|', 'bars', '|', to_string(exchange), '|', symbol, '|', interval)]), 'map(string, string)')
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'bars.5m',
  data_format = 'JSONEachRow',
  one_message_per_row = true,
  properties = 'enable.idempotence=false';

CREATE EXTERNAL STREAM IF NOT EXISTS bars_15s_out (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  open float64,
  close float64,
  volume float64,
  trades int32
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:9092',
  topic = 'bars.15s',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

-- the bars as Parquet, a directory per width and day
CREATE EXTERNAL TABLE IF NOT EXISTS bars_parquet (
  exchange int64,
  symbol low_cardinality(string),
  interval string,
  day string,
  time int64,
  open float64,
  high float64,
  low float64,
  close float64,
  volume float64
) PARTITION BY concat(interval, '/year=', format_datetime(from_unix_timestamp64_micro(time), '%Y'), '/month=', format_datetime(from_unix_timestamp64_micro(time), '%m'), '/day=', format_datetime(from_unix_timestamp64_micro(time), '%d'))
 SETTINGS
  type = 's3',
  region = 'us-east-1',
  bucket = 'bars',
  endpoint = 'http://localhost:9000',
  access_key_id = '__AWS_ACCESS_KEY_ID__',
  secret_access_key = '__AWS_SECRET_ACCESS_KEY__',
  write_to = 'bars/{_partition_id}/part.parquet',
  data_format = 'Parquet';

-- trades of a known side, symbols named alike across venues (`perpetual-BTC-USDT` and
-- `perpetual-BTC-USDC` are `perpetual-BTC-USD`), held 50 ms to come in time order
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_input_mv INTO bars_input AS
SELECT
  from_unix_timestamp64_micro(local_timestamp) AS local_event_time,
  time AS event_time_us,
  id,
  exchange,
  CASE WHEN match(symbol, '^(spot|perpetual|future)-') THEN
    replace_regexp_all(symbol, '-(?i:USDT|USDC)$', '-USD')
  ELSE symbol END AS symbol,
  side,
  price,
  quantity AS size,
  amount AS notional
FROM trades
WHERE side IN ('buy', 'sell')
ORDER BY local_event_time, id
SETTINGS order_hold_ms = 50;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_15s INTO bars_agg AS
SELECT
  exchange,
  symbol,
  '15s' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  earliest(price) AS open,
  max(price) AS high,
  min(price) AS low,
  latest(price) AS close,
  sum(size) AS volume,
  sum(notional) AS notional,
  to_int32(count()) AS trades,
  vwap(price, size) AS vwap,
  twap(price, local_event_time) AS twap,
  arg_min(price, local_event_time) AS open_by_time,
  arg_max(price, local_event_time) AS close_by_time
FROM tumble(bars_input, local_event_time, 15s)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND
SETTINGS max_bytes_before_external_group_by = 500000000, max_bytes_before_external_sort = 500000000;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1m INTO bars_agg AS
SELECT
  exchange,
  symbol,
  '1m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  earliest(price) AS open,
  max(price) AS high,
  min(price) AS low,
  latest(price) AS close,
  sum(size) AS volume,
  sum(notional) AS notional,
  to_int32(count()) AS trades,
  vwap(price, size) AS vwap,
  twap(price, local_event_time) AS twap,
  arg_min(price, local_event_time) AS open_by_time,
  arg_max(price, local_event_time) AS close_by_time
FROM tumble(bars_input, local_event_time, 1m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND
SETTINGS max_bytes_before_external_group_by = 500000000, max_bytes_before_external_sort = 500000000;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_5m INTO bars_agg AS
SELECT
  exchange,
  symbol,
  '5m' AS interval,
  format_datetime(window_start, '%Y-%m-%d') AS day,
  to_unix_timestamp64_micro(window_start) AS time,
  earliest(price) AS open,
  max(price) AS high,
  min(price) AS low,
  latest(price) AS close,
  sum(size) AS volume,
  sum(notional) AS notional,
  to_int32(count()) AS trades,
  vwap(price, size) AS vwap,
  twap(price, local_event_time) AS twap,
  arg_min(price, local_event_time) AS open_by_time,
  arg_max(price, local_event_time) AS close_by_time
FROM tumble(bars_input, local_event_time, 5m)
GROUP BY window_start, exchange, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND
SETTINGS max_bytes_before_external_group_by = 500000000, max_bytes_before_external_sort = 500000000;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_15s_out_mv INTO bars_15s_out AS
SELECT exchange, symbol, interval, day, time, open, close, volume, trades FROM bars_agg WHERE interval = '15s';

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1m_out_mv INTO bars_1m_out AS
SELECT * FROM bars_agg WHERE interval = '1m';

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_5m_out_mv INTO bars_5m_out AS
SELECT * FROM bars_agg WHERE interval = '5m';

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_parquet_mv INTO bars_parquet AS
SELECT exchange, symbol, interval, day, time, open, high, low, close, volume FROM bars_agg;
