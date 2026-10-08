CREATE OR REPLACE LIVE VIEW bars_5s AS
SELECT time_bucket('5s', ts) AS bucket, symbol,
       first(price, ts) AS open, max(price) AS high, min(price) AS low, last(price, ts) AS close,
       sum(size) AS volume, count(*) AS trades
FROM trades GROUP BY bucket, symbol;

CREATE OR REPLACE LIVE VIEW bars_1m AS
SELECT time_bucket('1m', ts) AS minute, symbol,
       first(price, ts) AS open, max(price) AS high, min(price) AS low, last(price, ts) AS close,
       sum(size) AS volume, vwap(price, size) AS vwap
FROM trades GROUP BY minute, symbol;

CREATE OR REPLACE LIVE VIEW spread_1s AS
SELECT time_bucket('1s', ts) AS second, symbol,
       avg((ask - bid) / ((ask + bid) / 2)) * 1e4 AS spread_bps, avg(bid_size + ask_size) AS depth
FROM quotes GROUP BY second, symbol;
