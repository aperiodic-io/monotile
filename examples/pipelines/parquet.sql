-- Trades in Parquet files, one-minute OHLCV bars out to Parquet files, a directory a day, exactly
-- once: `brrrrr run examples/pipelines/parquet.sql` from a directory with `trades/` in it (see
-- README.md). It reads the files under trades/ in name order, then the ones that land there
-- later: name them in time order (trades/2024-01-02T09.parquet, ...) and put each in place whole.
-- Each checkpoint's bars go to files of their own under bars/day=2024-01-02/, which
-- `brrrrr sql "FROM 'bars/'"` reads back, the day from the directory's name.

CREATE EXTERNAL STREAM trades (
  time datetime64(3),
  symbol string,
  price float64,
  quantity float64
) SETTINGS type = 'file', data_format = 'Parquet', path = 'trades/';

CREATE EXTERNAL STREAM bars (
  day string,
  minute datetime64(3),
  symbol string,
  open float64,
  high float64,
  low float64,
  close float64,
  volume float64,
  trade_count uint64
) SETTINGS type = 'file', data_format = 'Parquet', path = 'bars/', partition_by = 'day';

-- the same query as bars.sql's, with the day it is in
CREATE MATERIALIZED VIEW bars_v INTO bars AS
SELECT strftime(minute, '%Y-%m-%d') AS day, time_bucket('1m', time) AS minute, symbol,
       first(price, time) AS open, max(price) AS high, min(price) AS low,
       last(price, time) AS close, sum(quantity) AS volume, count(*) AS trade_count
FROM trades
GROUP BY minute, symbol;
