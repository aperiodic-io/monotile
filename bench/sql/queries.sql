-- name: count
SELECT count(*) AS n FROM '{dir}/trades.parquet';
-- name: per_symbol
SELECT symbol, count(*) AS n, sum(size) AS volume, avg(price) AS avg_price, max(price) AS high FROM '{dir}/trades.parquet' GROUP BY symbol ORDER BY volume DESC LIMIT 10;
-- name: ohlcv_1m
SELECT time_bucket('1m', ts) AS minute, symbol, first(price, ts) AS open, max(price) AS high, min(price) AS low, last(price, ts) AS close, sum(size) AS volume FROM '{dir}/trades.parquet' GROUP BY minute, symbol ORDER BY minute, symbol LIMIT 5;
-- name: vwap_1h
SELECT time_bucket('1h', ts) AS hour, symbol, sum(price * size) / sum(size) AS vwap FROM '{dir}/trades.parquet' GROUP BY hour, symbol ORDER BY hour, symbol LIMIT 5;
-- name: filter_buys
SELECT symbol, count(*) AS n FROM '{dir}/trades.parquet' WHERE side = 'buy' AND size > 10 GROUP BY symbol ORDER BY n DESC LIMIT 5;
-- name: spread_bps
SELECT symbol, avg((ask - bid) / ((ask + bid) / 2)) * 10000 AS spread_bps FROM '{dir}/quotes.parquet' GROUP BY symbol ORDER BY spread_bps DESC LIMIT 5;
-- name: asof_trades_quotes
SELECT count(*) AS n, avg(t.price - (q.bid + q.ask) / 2) AS avg_vs_mid FROM '{dir}/trades.parquet' t ASOF JOIN '{dir}/quotes.parquet' q ON t.symbol = q.symbol AND t.ts >= q.ts;
-- name: tca_per_symbol
SELECT t.symbol, count(*) AS n, avg(t.price - (q.bid + q.ask) / 2) AS avg_vs_mid FROM '{dir}/trades.parquet' t ASOF JOIN '{dir}/quotes.parquet' q ON t.symbol = q.symbol AND t.ts >= q.ts GROUP BY t.symbol ORDER BY n DESC LIMIT 5;
-- name: rolling_return
SELECT symbol, avg(r) AS mean_return FROM (SELECT symbol, price / lag(price) OVER (PARTITION BY symbol ORDER BY ts) - 1 AS r FROM '{dir}/trades.parquet') GROUP BY symbol ORDER BY symbol LIMIT 5;
