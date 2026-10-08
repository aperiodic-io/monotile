-- Trades in, one-minute OHLCV bars out, exactly once: `brrrrr run examples/pipelines/bars.sql`
-- (see README.md). Each trade is a JSON object on the topic `trades`:
--   {"time": "2024-01-02T09:30:00.120Z", "symbol": "BTC", "price": 42011.5, "quantity": 0.25}
-- and each bar one on `bars.1m`, written once its minute has closed. The view's query is the
-- one `brrrrr sql -t trades=trades.jsonl "SELECT ..."` answers over a file of the same trades.

CREATE EXTERNAL STREAM trades (
  time datetime64(3),
  symbol string,
  price float64,
  quantity float64
) SETTINGS
  type = 'kafka',
  brokers = 'localhost:19092',
  topic = 'trades',
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
  brokers = 'localhost:19092',
  topic = 'bars.1m',
  data_format = 'JSONEachRow',
  one_message_per_row = true;

-- a minute's bars are written once a trade of a later minute has arrived
CREATE MATERIALIZED VIEW bars INTO bars_1m AS
SELECT time_bucket('1m', time) AS minute, symbol,
       first(price, time) AS open, max(price) AS high, min(price) AS low,
       last(price, time) AS close, sum(quantity) AS volume, count(*) AS trade_count
FROM trades
GROUP BY minute, symbol;
