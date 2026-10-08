-- Bars in Proton's dialect (tumble, earliest/latest) over the scenarios' trades as JSON objects on
-- their topic: a datetime column (text in the messages), no dedup header (each message its own key).
CREATE EXTERNAL STREAM trades_source (
  time int64,
  id string,
  exchange int64,
  symbol string,
  price float64,
  local_timestamp datetime64(6),
  side string,
  quantity float64,
  amount float64
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'raw.trades.test',
  data_format = 'JSONEachRow';

CREATE EXTERNAL STREAM bars_1m (
  window_start datetime64(3),
  symbol string,
  open float64,
  high float64,
  low float64,
  close float64,
  volume float64,
  trade_count uint64
) SETTINGS
  type = 'kafka',
  brokers = 'redpanda:9092',
  topic = 'test.1m',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

CREATE MATERIALIZED VIEW bars_1m_mv INTO bars_1m AS
SELECT
  window_start,
  symbol,
  earliest(price) AS open,
  max(price) AS high,
  min(price) AS low,
  latest(price) AS close,
  sum(quantity) AS volume,
  count() AS trade_count
FROM tumble(trades_source, local_timestamp, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
