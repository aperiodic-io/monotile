-- The queries of queries.sql in ClickHouse's SQL, as clickhouse-local runs them over the Parquet
-- files: toStartOfMinute/Hour for time buckets, argMin/argMax for first/last, ASOF JOIN, and
-- lagInFrame (of a Nullable, so that a symbol's first trade has no previous price, as lag()).
-- name: count
SELECT count() AS n FROM file('{dir}/trades.parquet', Parquet);
-- name: per_symbol
SELECT symbol, count() AS n, sum(size) AS volume, avg(price) AS avg_price, max(price) AS high FROM file('{dir}/trades.parquet', Parquet) GROUP BY symbol ORDER BY volume DESC LIMIT 10;
-- name: ohlcv_1m
SELECT toStartOfMinute(ts) AS minute, symbol, argMin(price, ts) AS open, max(price) AS high, min(price) AS low, argMax(price, ts) AS close, sum(size) AS volume FROM file('{dir}/trades.parquet', Parquet) GROUP BY minute, symbol ORDER BY minute, symbol LIMIT 5;
-- name: vwap_1h
SELECT toStartOfHour(ts) AS hour, symbol, sum(price * size) / sum(size) AS vwap FROM file('{dir}/trades.parquet', Parquet) GROUP BY hour, symbol ORDER BY hour, symbol LIMIT 5;
-- name: filter_buys
SELECT symbol, count() AS n FROM file('{dir}/trades.parquet', Parquet) WHERE side = 'buy' AND size > 10 GROUP BY symbol ORDER BY n DESC LIMIT 5;
-- name: spread_bps
SELECT symbol, avg((ask - bid) / ((ask + bid) / 2)) * 10000 AS spread_bps FROM file('{dir}/quotes.parquet', Parquet) GROUP BY symbol ORDER BY spread_bps DESC LIMIT 5;
-- name: asof_trades_quotes
SELECT count() AS n, avg(t.price - (q.bid + q.ask) / 2) AS avg_vs_mid FROM file('{dir}/trades.parquet', Parquet) AS t ASOF JOIN file('{dir}/quotes.parquet', Parquet) AS q ON t.symbol = q.symbol AND t.ts >= q.ts;
-- name: tca_per_symbol
SELECT t.symbol, count() AS n, avg(t.price - (q.bid + q.ask) / 2) AS avg_vs_mid FROM file('{dir}/trades.parquet', Parquet) AS t ASOF JOIN file('{dir}/quotes.parquet', Parquet) AS q ON t.symbol = q.symbol AND t.ts >= q.ts GROUP BY t.symbol ORDER BY n DESC LIMIT 5;
-- name: rolling_return
SELECT symbol, avg(r) AS mean_return FROM (SELECT symbol, price / lagInFrame(toNullable(price)) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) - 1 AS r FROM file('{dir}/trades.parquet', Parquet)) GROUP BY symbol ORDER BY symbol LIMIT 5;
