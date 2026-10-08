-- Workload "rolling": one output row per trade with five per-row window functions, the shape
-- QuestDB 10's live views maintain. The QuestDB equivalent is the live view in bench.py
-- (ROLLING_QUERY); its unbounded windows need an ANCHOR, which here is the day in the PARTITION BY.
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
  brokers = 'localhost:19092',
  topic = 'bench.trades',
  data_format = 'ProtobufSingle',
  format_schema = 'market:Trade',
  group_name = 'bench-rolling',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS trades_input (local_event_time datetime64(6), symbol string, price float64, amount float64);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_input_mv INTO trades_input AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, symbol, price, amount FROM trades_source;

CREATE EXTERNAL STREAM IF NOT EXISTS rolling_out (
  symbol string,
  time int64,
  price float64,
  ma100 nullable(float64),
  hi1m nullable(float64),
  ema nullable(float64),
  dp nullable(float64),
  cumvol nullable(float64)
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:19092',
  topic = 'bench.rolling',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS rolling INTO rolling_out AS
SELECT symbol, to_unix_timestamp64_micro(local_event_time) AS time, price,
  avg(price) OVER (PARTITION BY symbol ORDER BY local_event_time ROWS 99 PRECEDING) AS ma100,
  max(price) OVER (PARTITION BY symbol ORDER BY local_event_time RANGE INTERVAL 1 MINUTE PRECEDING) AS hi1m,
  avg(price, 'alpha', 0.1) OVER d AS ema,
  price - lag(price) OVER d AS dp,
  sum(amount) OVER d AS cumvol
FROM trades_input
WINDOW d AS (PARTITION BY symbol, to_start_of_day(local_event_time) ORDER BY local_event_time);
