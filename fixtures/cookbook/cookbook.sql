-- The brrrrr cookbook: the queries people run on market data and other time series, each
-- tested. `crates/brrrrr-lake/tests/cookbook.rs` runs every recipe over the files of this
-- directory and holds its answer to DuckDB's for the same files (`expected/<name>.csv`,
-- recorded by `scripts/record-cookbook.py`; a recipe with `-- duckdb:` lines gives DuckDB its
-- own spelling of the query). The website's cookbook page is made from this file.
--
-- Each recipe: `-- name:`, `-- title:`, `-- section:`, `-- about:` (one or more lines), the
-- query, and optionally `-- duckdb:` lines.

-- name: ohlcv
-- section: Bars and prices
-- title: OHLCV bars
-- about: Candles from trades: the first, highest, lowest and last price of each minute and
-- about: symbol, and the volume traded.
SELECT time_bucket('1m', ts) AS minute, symbol,
       first(price, ts) AS open, max(price) AS high, min(price) AS low, last(price, ts) AS close,
       sum(size) AS volume, count(*) AS trades
FROM 'trades.csv'
GROUP BY minute, symbol
ORDER BY minute, symbol;

-- name: vwap
-- section: Bars and prices
-- title: VWAP per bucket
-- about: The volume-weighted average price of every 5 minutes, per symbol.
SELECT time_bucket('5m', ts) AS bucket, symbol, vwap(price, size) AS vwap, sum(size) AS volume
FROM 'trades.csv'
GROUP BY bucket, symbol
ORDER BY bucket, symbol;
-- duckdb: SELECT time_bucket(INTERVAL '5 minutes', ts) AS bucket, symbol, sum(price * size) / sum(size) AS vwap, sum(size) AS volume
-- duckdb: FROM 'trades.csv' GROUP BY bucket, symbol ORDER BY bucket, symbol

-- name: running_vwap
-- section: Bars and prices
-- title: Running VWAP through the day
-- about: Each trade with the day's volume-weighted average price so far.
SELECT ts, symbol, price,
       sum(price * size) OVER (PARTITION BY symbol ORDER BY ts) / sum(size) OVER (PARTITION BY symbol ORDER BY ts) AS vwap_so_far
FROM 'trades.csv'
ORDER BY symbol, ts
LIMIT 20;

-- name: latest
-- section: Bars and prices
-- title: Latest price per symbol
-- about: The last trade of each symbol: a market snapshot (kdb+'s `select last price by sym`,
-- about: QuestDB's `LATEST ON`).
SELECT symbol, last(price, ts) AS price, max(ts) AS at
FROM 'trades.csv'
GROUP BY symbol
ORDER BY symbol;

-- name: gapfill_bars
-- section: Bars and prices
-- title: Bars with the empty minutes filled
-- about: One-minute bars of block trades (2 units or more), every minute shown: one without a
-- about: trade carries the last close forward (`locf`) at no volume (TimescaleDB's
-- about: `time_bucket_gapfill`; `interpolate` draws a line instead).
SELECT time_bucket_gapfill('1m', ts) AS minute, symbol,
       locf(last(price, ts)) AS close, coalesce(sum(size), 0) AS volume, count(*) AS trades
FROM 'trades.csv'
WHERE symbol = 'SOL' AND size >= 2
GROUP BY minute, symbol
ORDER BY minute;
-- duckdb: WITH bars AS (SELECT time_bucket(INTERVAL '1 minute', ts) AS minute, symbol, arg_max(price, ts) AS close,
-- duckdb:                      sum(size) AS volume, count(*) AS trades
-- duckdb:               FROM 'trades.csv' WHERE symbol = 'SOL' AND size >= 2 GROUP BY minute, symbol),
-- duckdb: grid AS (SELECT symbol, unnest(generate_series(min(minute), max(minute), INTERVAL '1 minute')) AS minute
-- duckdb:          FROM bars GROUP BY symbol)
-- duckdb: SELECT g.minute, g.symbol, last_value(b.close IGNORE NULLS) OVER (PARTITION BY g.symbol ORDER BY g.minute) AS close,
-- duckdb:        coalesce(b.volume, 0) AS volume, b.trades
-- duckdb: FROM grid g LEFT JOIN bars b ON g.symbol = b.symbol AND g.minute = b.minute ORDER BY g.minute

-- name: daily_new_york
-- section: Bars and prices
-- title: Daily bars by New York's day
-- about: Daily candles of New York's calendar days, from its midnight (a day of 23 or 25 hours
-- about: where its clocks change), with each day's first trade on its wall clock. The half hour
-- about: after midnight UTC is New York's evening of December 31st.
SELECT time_bucket('1 day', ts, 'America/New_York') AS day, symbol,
       first(price, ts) AS open, max(price) AS high, min(price) AS low, last(price, ts) AS close,
       sum(size) AS volume, strftime(timezone('America/New_York', min(ts)), '%H:%M:%S') AS first_trade_local
FROM 'trades.csv'
GROUP BY day, symbol
ORDER BY day, symbol;
-- duckdb: SELECT time_bucket(INTERVAL '1 day', ts::TIMESTAMPTZ, 'America/New_York') AS day, symbol,
-- duckdb:        arg_min(price, ts) AS open, max(price) AS high, min(price) AS low, arg_max(price, ts) AS close,
-- duckdb:        sum(size) AS volume, strftime(timezone('America/New_York', min(ts)::TIMESTAMPTZ), '%H:%M:%S') AS first_trade_local
-- duckdb: FROM 'trades.csv' GROUP BY day, symbol ORDER BY day, symbol

-- name: pivot_closes
-- section: Bars and prices
-- title: Minute closes side by side
-- about: A research table: each symbol's last price of every minute in a column of its own,
-- about: ready for correlations (DuckDB's `PIVOT ... ON ... USING`; its values are found first).
PIVOT (SELECT time_bucket('1m', ts) AS minute, symbol, price, ts FROM 'trades.csv')
ON symbol USING last(price, ts) GROUP BY minute
ORDER BY minute
LIMIT 10;
-- duckdb: PIVOT (SELECT time_bucket(INTERVAL '1 minute', ts) AS minute, symbol, price, ts FROM 'trades.csv')
-- duckdb: ON symbol USING arg_max(price, ts) GROUP BY minute ORDER BY minute LIMIT 10

-- name: snapshot_as_of
-- section: Bars and prices
-- title: Prices as of a moment
-- about: Each symbol's last quote at or before a time: the market as it stood then.
SELECT symbol, last(bid, ts) AS bid, last(ask, ts) AS ask, max(ts) AS quoted_at
FROM 'quotes.csv'
WHERE ts <= '2024-01-01 00:10:00'
GROUP BY symbol
ORDER BY symbol;

-- name: twap
-- section: Bars and prices
-- title: TWAP per bucket
-- about: The time-weighted average price: each price weighted by how long it stood.
SELECT time_bucket('10m', ts) AS bucket, symbol, twap(price, ts) AS twap
FROM 'trades.csv'
GROUP BY bucket, symbol
ORDER BY bucket, symbol;
-- duckdb: SELECT bucket, symbol, sum(price * d) / sum(d) AS twap FROM (
-- duckdb:   SELECT time_bucket(INTERVAL '10 minutes', ts) AS bucket, symbol, price,
-- duckdb:          epoch_us(lead(ts) OVER (PARTITION BY symbol, time_bucket(INTERVAL '10 minutes', ts) ORDER BY ts)) - epoch_us(ts) AS d
-- duckdb:   FROM 'trades.csv') GROUP BY bucket, symbol ORDER BY bucket, symbol

-- name: spread
-- section: Liquidity and costs
-- title: Quoted spread in basis points
-- about: The average bid-ask spread of every minute, relative to the mid price.
SELECT time_bucket('1m', ts) AS minute, symbol,
       avg((ask - bid) / ((ask + bid) / 2)) * 10000 AS spread_bps
FROM 'quotes.csv'
GROUP BY minute, symbol
ORDER BY minute, symbol;

-- name: asof_quotes
-- section: Liquidity and costs
-- title: Each trade with the quote it met
-- about: An as-of join: every trade takes the latest quote of its symbol at or before it.
SELECT t.ts, t.symbol, t.side, t.price, q.bid, q.ask
FROM 'trades.csv' t
ASOF JOIN 'quotes.csv' q ON t.symbol = q.symbol AND t.ts >= q.ts
ORDER BY t.ts, t.symbol
LIMIT 25;

-- name: slippage
-- section: Liquidity and costs
-- title: Slippage against the mid (TCA)
-- about: What each trade paid against the mid price it met, in basis points, signed by its
-- about: side: positive is a cost. Averaged per symbol and side.
SELECT t.symbol, t.side, count(*) AS trades,
       avg(CASE WHEN t.side = 'buy' THEN 1 ELSE -1 END * (t.price - (q.bid + q.ask) / 2) / ((q.bid + q.ask) / 2)) * 10000 AS slippage_bps
FROM 'trades.csv' t
ASOF JOIN 'quotes.csv' q ON t.symbol = q.symbol AND t.ts >= q.ts
GROUP BY t.symbol, t.side
ORDER BY t.symbol, t.side;

-- name: effective_spread
-- section: Liquidity and costs
-- title: Effective spread per minute
-- about: Twice the distance from trade price to mid: the spread trades actually paid.
SELECT time_bucket('1m', t.ts) AS minute, t.symbol,
       avg(2 * abs(t.price - (q.bid + q.ask) / 2) / ((q.bid + q.ask) / 2)) * 10000 AS effective_spread_bps
FROM 'trades.csv' t
ASOF JOIN 'quotes.csv' q ON t.symbol = q.symbol AND t.ts >= q.ts
GROUP BY minute, t.symbol
ORDER BY minute, t.symbol;

-- name: markouts
-- section: Liquidity and costs
-- title: Markouts at 1, 5 and 60 seconds
-- about: Where the mid went after each trade, against its price in basis points and signed by its
-- about: side: positive, the trade was on the right side of the move. An as-of join at a time
-- about: offset per horizon (QuestDB's `HORIZON JOIN`), the same table joined three times.
SELECT t.symbol, t.side, count(*) AS trades,
       avg(CASE WHEN t.side = 'buy' THEN 1 ELSE -1 END * ((q1.bid + q1.ask) / 2 - t.price) / t.price) * 10000 AS markout_1s_bps,
       avg(CASE WHEN t.side = 'buy' THEN 1 ELSE -1 END * ((q5.bid + q5.ask) / 2 - t.price) / t.price) * 10000 AS markout_5s_bps,
       avg(CASE WHEN t.side = 'buy' THEN 1 ELSE -1 END * ((q60.bid + q60.ask) / 2 - t.price) / t.price) * 10000 AS markout_60s_bps
FROM 'trades.csv' t
ASOF JOIN 'quotes.csv' q1 ON t.symbol = q1.symbol AND t.ts + INTERVAL '1 second' >= q1.ts
ASOF JOIN 'quotes.csv' q5 ON t.symbol = q5.symbol AND t.ts + INTERVAL '5 seconds' >= q5.ts
ASOF JOIN 'quotes.csv' q60 ON t.symbol = q60.symbol AND t.ts + INTERVAL '60 seconds' >= q60.ts
GROUP BY t.symbol, t.side
ORDER BY t.symbol, t.side;

-- name: fresh_quotes
-- section: Liquidity and costs
-- title: The quote before each trade, if fresh
-- about: An as-of join strictly before each trade (`>`: not a quote of its own microsecond), with
-- about: a tolerance: a quote more than half a second old is none, its columns NULL.
SELECT t.ts, t.symbol, t.price, q.bid, q.ask, q.ts AS quoted_at
FROM 'trades.csv' t
ASOF LEFT JOIN 'quotes.csv' q
  ON t.symbol = q.symbol AND t.ts > q.ts AND t.ts - q.ts <= INTERVAL '500 milliseconds'
ORDER BY t.ts, t.symbol
LIMIT 25;
-- duckdb: SELECT t.ts, t.symbol, t.price,
-- duckdb:        CASE WHEN t.ts - q.ts <= INTERVAL '500 milliseconds' THEN q.bid END AS bid,
-- duckdb:        CASE WHEN t.ts - q.ts <= INTERVAL '500 milliseconds' THEN q.ask END AS ask,
-- duckdb:        CASE WHEN t.ts - q.ts <= INTERVAL '500 milliseconds' THEN q.ts END AS quoted_at
-- duckdb: FROM 'trades.csv' t ASOF LEFT JOIN 'quotes.csv' q ON t.symbol = q.symbol AND t.ts > q.ts
-- duckdb: ORDER BY t.ts, t.symbol LIMIT 25

-- name: quotes_before_trades
-- section: Liquidity and costs
-- title: The quotes in the second before each trade
-- about: A window join (kdb+'s `wj`, QuestDB's `WINDOW JOIN`): for each trade, aggregates over its
-- about: symbol's quotes in a time range around it, here how many came in the second before it
-- about: and their average spread. A `LATERAL` subquery, as in PostgreSQL and DuckDB.
SELECT t.ts, t.symbol, t.price, w.quotes, w.avg_spread_bps
FROM 'trades.csv' t
LEFT JOIN LATERAL (
  SELECT count(*) AS quotes, avg((q.ask - q.bid) / ((q.ask + q.bid) / 2)) * 10000 AS avg_spread_bps
  FROM 'quotes.csv' q
  WHERE q.symbol = t.symbol AND q.ts BETWEEN t.ts - INTERVAL '1 second' AND t.ts
) w ON true
ORDER BY t.ts, t.symbol
LIMIT 25;

-- name: book_imbalance
-- section: Liquidity and costs
-- title: Top-of-book imbalance
-- about: How lopsided the best bid and ask sizes are, from -1 (all ask) to 1 (all bid).
SELECT time_bucket('5m', ts) AS bucket, symbol,
       avg((bid_size - ask_size) / (bid_size + ask_size)) AS imbalance
FROM 'quotes.csv'
GROUP BY bucket, symbol
ORDER BY bucket, symbol;

-- name: flow
-- section: Order flow
-- title: Buy and sell volume
-- about: Aggressor volume by side, and the net flow, per minute.
SELECT time_bucket('1m', ts) AS minute, symbol,
       sum(size) FILTER (WHERE side = 'buy') AS buy_volume,
       sum(size) FILTER (WHERE side = 'sell') AS sell_volume,
       sum(CASE WHEN side = 'buy' THEN size ELSE -size END) AS net_flow
FROM 'trades.csv'
GROUP BY minute, symbol
ORDER BY minute, symbol;

-- name: trade_counts_by_side
-- section: Order flow
-- title: Trade counts with FILTER
-- about: `count(*) FILTER (WHERE ...)`: several conditional counts in one pass.
SELECT symbol,
       count(*) FILTER (WHERE side = 'buy') AS buys,
       count(*) FILTER (WHERE side = 'sell') AS sells,
       count(*) FILTER (WHERE size > 1) AS large
FROM 'trades.csv'
GROUP BY symbol
ORDER BY symbol;

-- name: volume_profile
-- section: Order flow
-- title: Volume profile
-- about: Volume traded at each price level (kdb+'s `xbar`): where the market did business.
SELECT symbol, floor(price / 10) * 10 AS level, sum(size) AS volume
FROM 'trades.csv'
WHERE symbol = 'ETH'
GROUP BY symbol, level
ORDER BY level;

-- name: returns
-- section: Returns and risk
-- title: Minute returns
-- about: Bars, then each bar's return on the one before it, per symbol.
SELECT minute, symbol, close / lag(close) OVER (PARTITION BY symbol ORDER BY minute) - 1 AS ret
FROM (
  SELECT time_bucket('1m', ts) AS minute, symbol, last(price, ts) AS close
  FROM 'trades.csv' GROUP BY minute, symbol
)
ORDER BY symbol, minute
LIMIT 20;

-- name: next_trade
-- section: Returns and risk
-- title: The next trade
-- about: Each trade with its symbol's next one: its price, and how long until it came (`lead`).
SELECT ts, symbol, price, lead(price) OVER w AS next_price,
       (epoch_us(lead(ts) OVER w) - epoch_us(ts)) / 1000.0 AS ms_to_next
FROM 'trades.csv'
WINDOW w AS (PARTITION BY symbol ORDER BY ts)
ORDER BY ts, symbol
LIMIT 20;

-- name: realized_vol
-- section: Returns and risk
-- title: Realized volatility
-- about: The standard deviation of trade-to-trade log returns per symbol, scaled to the hour.
SELECT symbol, stddev(r) * sqrt(count(*) * 2) AS hourly_vol, count(*) AS returns
FROM (
  SELECT symbol, ln(price / lag(price) OVER (PARTITION BY symbol ORDER BY ts)) AS r
  FROM 'trades.csv'
)
WHERE r IS NOT NULL
GROUP BY symbol
ORDER BY symbol;

-- name: drawdown
-- section: Returns and risk
-- title: Drawdown from the running high
-- about: How far each trade's price is below the highest price before it.
SELECT ts, symbol, price, price / max(price) OVER (PARTITION BY symbol ORDER BY ts) - 1 AS drawdown
FROM 'trades.csv'
WHERE symbol = 'SOL'
ORDER BY ts
LIMIT 20;

-- name: bollinger
-- section: Returns and risk
-- title: Rolling mean, deviation and z-score
-- about: Bollinger bands: the mean and standard deviation of the last 20 trades, and each
-- about: trade's distance from them.
SELECT ts, price, avg(price) OVER w AS mean20, stddev(price) OVER w AS sd20,
       (price - avg(price) OVER w) / stddev(price) OVER w AS z
FROM 'trades.csv'
WHERE symbol = 'BTC'
WINDOW w AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
ORDER BY ts
LIMIT 30;
-- duckdb: SELECT ts, price, avg(price) OVER w AS mean20, stddev_samp(price) OVER w AS sd20,
-- duckdb:        (price - avg(price) OVER w) / stddev_samp(price) OVER w AS z
-- duckdb: FROM 'trades.csv' WHERE symbol = 'BTC'
-- duckdb: WINDOW w AS (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ORDER BY ts LIMIT 30

-- name: cumulative
-- section: Returns and risk
-- title: Cumulative volume and position
-- about: Running totals per symbol: the volume traded so far, and the net position of a
-- about: trader who took every buy and sold into every sell.
SELECT ts, symbol, sum(size) OVER (PARTITION BY symbol ORDER BY ts) AS volume_so_far,
       sum(CASE WHEN side = 'buy' THEN size ELSE -size END) OVER (PARTITION BY symbol ORDER BY ts) AS position
FROM 'trades.csv'
ORDER BY symbol, ts
LIMIT 20;

-- name: top_movers
-- section: Screens
-- title: Top movers
-- about: Each symbol's change over the period, largest first.
SELECT symbol, first(price, ts) AS open, last(price, ts) AS close, last(price, ts) / first(price, ts) - 1 AS change
FROM 'trades.csv'
GROUP BY symbol
ORDER BY change DESC;

-- name: cross_sectional_rank
-- section: Screens
-- title: Symbols ranked against each other every minute
-- about: Each minute's return per symbol, ranked across the symbols of that minute (1 the
-- about: best), as a percentile, and in halves: cross-sectional ranks, for momentum and
-- about: relative-value screens. Ranking functions partition by the time first.
SELECT minute, symbol, ret,
       rank() OVER (PARTITION BY minute ORDER BY ret DESC) AS place,
       percent_rank() OVER (PARTITION BY minute ORDER BY ret) AS pct,
       ntile(2) OVER (PARTITION BY minute ORDER BY ret, symbol) AS half
FROM (SELECT time_bucket('1m', ts) AS minute, symbol, last(price, ts) / first(price, ts) - 1 AS ret
      FROM 'trades.csv' GROUP BY minute, symbol)
ORDER BY minute, place, symbol
LIMIT 30;

-- name: hourly_profile
-- section: Screens
-- title: Activity by minute of the hour
-- about: An intraday curve: trades and volume by the minute within the hour.
SELECT minute(ts) AS minute_of_hour, count(*) AS trades, sum(size) AS volume
FROM 'trades.csv'
GROUP BY minute_of_hour
ORDER BY minute_of_hour;

-- name: reference_join
-- section: Screens
-- title: Enrich with reference data
-- about: A join on keys looks up each row's reference data: names, sectors, tick sizes.
SELECT i.name, i.sector, count(*) AS trades, sum(t.size * t.price) AS notional
FROM 'trades.csv' t
JOIN 'instruments.csv' i ON t.symbol = i.symbol
GROUP BY i.name, i.sector
ORDER BY notional DESC;

-- name: large_trades
-- section: Screens
-- title: Trades much larger than their symbol's average
-- about: Each row against its group (kdb+'s `fby`): a join back to an aggregate, here the
-- about: trades of more than twice their symbol's average size.
SELECT t.ts, t.symbol, t.size, s.avg_size, t.size / s.avg_size AS ratio
FROM 'trades.csv' t
JOIN (SELECT symbol, avg(size) AS avg_size FROM 'trades.csv' GROUP BY symbol) s ON t.symbol = s.symbol
WHERE t.size > 2 * s.avg_size
ORDER BY ratio DESC
LIMIT 20;

-- name: gaps
-- section: Data quality
-- title: Gaps in a feed
-- about: Where a feed went quiet: rows that came more than 5 seconds after the one before.
SELECT device, ts, gap_s
FROM (
  SELECT device, ts, epoch(ts) - epoch(lag(ts) OVER (PARTITION BY device ORDER BY ts)) AS gap_s
  FROM 'readings.csv'
)
WHERE gap_s > 5
ORDER BY device, ts;

-- name: duplicates
-- section: Data quality
-- title: Duplicate rows
-- about: Rows that appear more than once: the same time, symbol and price.
SELECT ts, symbol, price, count(*) AS copies
FROM 'trades.csv'
GROUP BY ts, symbol, price
HAVING count(*) > 1
ORDER BY ts;

-- name: downsample
-- section: Data quality
-- title: Downsample for a chart
-- about: Thousands of points to one per bucket, keeping the shape: first, min, max and last.
SELECT time_bucket('1m', ts) AS minute, first(temperature, ts) AS first, min(temperature) AS min,
       max(temperature) AS max, last(temperature, ts) AS last
FROM 'readings.csv'
WHERE device = 'a'
GROUP BY minute
ORDER BY minute;

-- name: counter_rate
-- section: Sensors and operations
-- title: Rate of a counter
-- about: Per-second rate from a cumulative counter, ignoring its resets.
SELECT device, ts, rate
FROM (
  SELECT device, ts,
         CASE WHEN counter >= lag(counter) OVER (PARTITION BY device ORDER BY ts)
              THEN (counter - lag(counter) OVER (PARTITION BY device ORDER BY ts)) / (epoch(ts) - epoch(lag(ts) OVER (PARTITION BY device ORDER BY ts)))
         END AS rate
  FROM 'readings.csv'
)
WHERE rate IS NOT NULL
ORDER BY device, ts
LIMIT 20;

-- name: latency_percentiles
-- section: Sensors and operations
-- title: Latency percentiles per minute
-- about: The median and 99th percentile of latency: exact up to 256 values a bucket.
SELECT time_bucket('1m', ts) AS minute, device, median(latency_ms) AS p50,
       quantile_cont(latency_ms, 0.99) AS p99, max(latency_ms) AS worst
FROM 'readings.csv'
GROUP BY minute, device
ORDER BY minute, device;

-- name: anomalies
-- section: Sensors and operations
-- title: Readings outside 3 standard deviations
-- about: Each reading against the mean and deviation of the last 30 readings.
SELECT device, ts, temperature, z
FROM (
  SELECT device, ts, temperature,
         (temperature - avg(temperature) OVER w) / stddev(temperature) OVER w AS z
  FROM 'readings.csv'
  WINDOW w AS (PARTITION BY device ORDER BY ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)
)
WHERE abs(z) > 3
ORDER BY device, ts;
-- duckdb: SELECT device, ts, temperature, z FROM (SELECT device, ts, temperature,
-- duckdb:   (temperature - avg(temperature) OVER w) / stddev_samp(temperature) OVER w AS z
-- duckdb:   FROM 'readings.csv' WINDOW w AS (PARTITION BY device ORDER BY ts ROWS BETWEEN 30 PRECEDING AND CURRENT ROW))
-- duckdb: WHERE abs(z) > 3 ORDER BY device, ts

-- name: sessions
-- section: Sensors and operations
-- title: Sessions between gaps
-- about: Numbers each run of readings that follow each other within 5 seconds: a running
-- about: count of the gaps before each row.
SELECT device, session, min(ts) AS started, max(ts) AS ended, count(*) AS readings
FROM (
  SELECT device, ts, sum(CASE WHEN gap_s > 5 THEN 1 ELSE 0 END) OVER (PARTITION BY device ORDER BY ts) AS session
  FROM (
    SELECT device, ts, epoch(ts) - epoch(lag(ts) OVER (PARTITION BY device ORDER BY ts)) AS gap_s
    FROM 'readings.csv'
  )
)
GROUP BY device, session
ORDER BY device, session;

-- name: funnel
-- section: Product events
-- title: A funnel per day
-- about: How many visits, sign-ups and purchases each day: one pass, a `FILTER` per step.
SELECT time_bucket('1d', ts) AS day,
       count(*) FILTER (WHERE event = 'visit') AS visits,
       count(*) FILTER (WHERE event = 'signup') AS signups,
       count(*) FILTER (WHERE event = 'purchase') AS purchases
FROM 'events.csv'
GROUP BY day
ORDER BY day;

-- name: busiest_hours
-- section: Product events
-- title: Activity by hour of the day
-- about: Events and time spent per hour of the day, over every day: when the app is used.
SELECT hour(ts) AS hour, count(*) AS events, round(sum(duration_ms) / 1000 / 60, 1) AS minutes
FROM 'events.csv'
GROUP BY hour
ORDER BY hour;

-- name: first_seen
-- section: Product events
-- title: Each user's first and last visit
-- about: When each user was first and last seen, and what they did in between.
SELECT user_id, min(ts) AS first_seen, max(ts) AS last_seen, count(*) AS events,
       count(*) FILTER (WHERE event = 'purchase') > 0 AS bought
FROM 'events.csv'
GROUP BY user_id
ORDER BY first_seen
LIMIT 10;

-- name: time_to_purchase
-- section: Product events
-- title: Time from sign-up to purchase
-- about: For each buyer, the minutes from their sign-up to their purchase: the time of the
-- about: user's previous event, by `lag`, where that event was the sign-up.
SELECT user_id, ts AS purchased, round((epoch(ts) - epoch(prev_ts)) / 60, 1) AS minutes
FROM (
  SELECT user_id, ts, event, lag(event) OVER (PARTITION BY user_id ORDER BY ts) AS prev_event,
         lag(ts) OVER (PARTITION BY user_id ORDER BY ts) AS prev_ts
  FROM 'events.csv'
)
WHERE event = 'purchase' AND prev_event = 'signup'
ORDER BY purchased
LIMIT 10;
