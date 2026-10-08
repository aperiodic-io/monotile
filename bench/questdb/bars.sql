-- Workload "bars": one-minute OHLCV + VWAP per symbol, emitted when each minute closes.
-- The QuestDB equivalent is the materialized view in bench.py (BARS_QUERY).
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
  group_name = 'bench-bars',
  seek_to = 'earliest';

CREATE STREAM IF NOT EXISTS trades_input (local_event_time datetime64(6), symbol string, price float64, amount float64);

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_input_mv INTO trades_input AS
SELECT from_unix_timestamp64_micro(local_timestamp) AS local_event_time, symbol, price, amount FROM trades_source;

CREATE EXTERNAL STREAM IF NOT EXISTS bars_out (
  symbol string,
  time int64,
  open float64,
  high float64,
  low float64,
  close float64,
  volume float64,
  vwap nullable(float64),
  trades uint64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:19092',
  topic = 'bench.bars',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars INTO bars_out AS
SELECT symbol, to_unix_timestamp64_micro(window_start) AS time, earliest(price) AS open, max(price) AS high,
  min(price) AS low, latest(price) AS close, sum(amount) AS volume, vwap(price, amount) AS vwap, count() AS trades
FROM tumble(trades_input, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
