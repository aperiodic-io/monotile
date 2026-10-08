-- examples/pipelines/bars.sql's view, in `brrrrr sql`'s dialect, over the scenarios' JSON trades:
-- the same query `brrrrr sql` answers over the trades as a file (`-t trades_source=<file>`).
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
  minute datetime64(3),
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

CREATE MATERIALIZED VIEW bars INTO bars_1m AS
SELECT time_bucket('1m', local_timestamp) AS minute, symbol,
       first(price, local_timestamp) AS open, max(price) AS high, min(price) AS low,
       last(price, local_timestamp) AS close, sum(quantity) AS volume, count(*) AS trade_count
FROM trades_source
WHERE price > 0
GROUP BY minute, symbol;
