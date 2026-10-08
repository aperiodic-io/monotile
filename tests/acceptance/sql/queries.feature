Feature: Time-series SQL
  Bars, as-of joins, lookups, window functions and the aggregates quants use, over files.

  Background:
    Given a file "trades.csv" with:
      """
      ts,symbol,price,size,side
      2024-01-01T00:00:10,A,100,1,buy
      2024-01-01T00:00:20,B,50,2,sell
      2024-01-01T00:00:30,A,101,2,buy
      2024-01-01T00:01:05,A,99,1,sell
      2024-01-01T00:01:10,B,51,1,buy
      2024-01-01T00:02:00,A,102,3,buy
      """
    And a file "quotes.csv" with:
      """
      ts,symbol,bid,ask
      2024-01-01T00:00:05,A,99.5,100.5
      2024-01-01T00:00:15,B,49.5,50.5
      2024-01-01T00:00:30,A,100.5,101.5
      2024-01-01T00:01:00,A,98.5,99.5
      2024-01-01T00:02:30,B,52,53
      """
    And a file "instruments.csv" with:
      """
      symbol,venue
      A,X
      B,Y
      """

  Scenario: OHLCV bars
    When I run brrrrr sql:
      """
      SELECT time_bucket('1m', ts) AS minute, symbol, first(price, ts) AS open, max(price) AS high,
             min(price) AS low, last(price, ts) AS close, sum(size) AS volume
      FROM trades GROUP BY minute, symbol ORDER BY minute, symbol
      """
    Then the output is:
      """
      minute,symbol,open,high,low,close,volume
      2024-01-01 00:00:00,A,100,101,100,101,3
      2024-01-01 00:00:00,B,50,50,50,50,2
      2024-01-01 00:01:00,A,99,99,99,99,1
      2024-01-01 00:01:00,B,51,51,51,51,1
      2024-01-01 00:02:00,A,102,102,102,102,3
      """

  Scenario: VWAP, and bucket widths written every way
    When I run brrrrr sql:
      """
      SELECT time_bucket(INTERVAL '5 minutes', ts) AS b, vwap(price, size) AS vwap FROM trades WHERE symbol = 'A' GROUP BY b
      """
    Then the output is:
      """
      b,vwap
      2024-01-01 00:00:00,101
      """

  Scenario: An as-of join takes the quote at or before each trade
    When I run brrrrr sql:
      """
      SELECT t.ts, t.symbol, q.bid, q.ask FROM trades t
      ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
      ORDER BY t.ts
      """
    Then the output is:
      """
      ts,symbol,bid,ask
      2024-01-01 00:00:10,A,99.5,100.5
      2024-01-01 00:00:20,B,49.5,50.5
      2024-01-01 00:00:30,A,100.5,101.5
      2024-01-01 00:01:05,A,98.5,99.5
      2024-01-01 00:01:10,B,49.5,50.5
      2024-01-01 00:02:00,A,98.5,99.5
      """

  Scenario: ASOF JOIN drops the trades without a quote before them; ASOF LEFT JOIN keeps them
    Given a file "late_quotes.csv" with:
      """
      ts,symbol,bid
      2024-01-01T00:01:00,A,98.5
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS inner_rows FROM trades t ASOF JOIN late_quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
      """
    Then the output is:
      """
      inner_rows
      2
      """
    When I run brrrrr sql:
      """
      SELECT t.ts, q.bid FROM trades t ASOF LEFT JOIN late_quotes q ON t.symbol = q.symbol AND t.ts >= q.ts WHERE t.symbol = 'A' ORDER BY t.ts
      """
    Then the output is:
      """
      ts,bid
      2024-01-01 00:00:10,
      2024-01-01 00:00:30,
      2024-01-01 00:01:05,98.5
      2024-01-01 00:02:00,98.5
      """

  Scenario: Snowflake's MATCH_CONDITION is an as-of join too
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.symbol = q.symbol
      """
    Then the output is:
      """
      n
      6
      """

  Scenario: Markouts: as-of joins at a time offset, strictly before, and within a tolerance
    When I run brrrrr sql:
      """
      SELECT t.ts, t.symbol, q.bid AS bid_then, q30.bid AS bid_30s_on FROM trades t
      ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
      ASOF JOIN quotes q30 ON t.symbol = q30.symbol AND t.ts + INTERVAL '30 seconds' >= q30.ts
      ORDER BY t.ts
      """
    Then the output is:
      """
      ts,symbol,bid_then,bid_30s_on
      2024-01-01 00:00:10,A,99.5,100.5
      2024-01-01 00:00:20,B,49.5,49.5
      2024-01-01 00:00:30,A,100.5,98.5
      2024-01-01 00:01:05,A,98.5,98.5
      2024-01-01 00:01:10,B,49.5,49.5
      2024-01-01 00:02:00,A,98.5,98.5
      """
    When I run brrrrr sql:
      """
      SELECT t.ts, q.bid, q.ts AS quoted FROM trades t ASOF LEFT JOIN quotes q
      ON t.symbol = q.symbol AND t.ts > q.ts AND t.ts - q.ts <= INTERVAL '30 seconds' ORDER BY t.ts
      """
    Then the output is:
      """
      ts,bid,quoted
      2024-01-01 00:00:10,99.5,2024-01-01 00:00:05
      2024-01-01 00:00:20,49.5,2024-01-01 00:00:15
      2024-01-01 00:00:30,99.5,2024-01-01 00:00:05
      2024-01-01 00:01:05,98.5,2024-01-01 00:01:00
      2024-01-01 00:01:10,,
      2024-01-01 00:02:00,,
      """

  Scenario: Gap filling: every bucket, the last close carried forward or a line drawn
    When I run brrrrr sql:
      """
      SELECT time_bucket_gapfill('30s', ts) AS b, symbol, locf(last(price, ts)) AS close,
             coalesce(sum(size), 0) AS volume, interpolate(avg(price)) AS mid
      FROM trades GROUP BY b, symbol ORDER BY symbol, b
      """
    Then the output is:
      """
      b,symbol,close,volume,mid
      2024-01-01 00:00:00,A,100,1,100
      2024-01-01 00:00:30,A,101,2,101
      2024-01-01 00:01:00,A,99,1,99
      2024-01-01 00:01:30,A,99,0,100.5
      2024-01-01 00:02:00,A,102,3,102
      2024-01-01 00:00:00,B,50,2,50
      2024-01-01 00:00:30,B,50,0,50.5
      2024-01-01 00:01:00,B,51,1,51
      """

  Scenario: Time zones: a zone's days, 23 hours long when DST begins, and its wall clock
    Given a file "dst.csv" with:
      """
      ts,v
      2024-03-09T15:00:00,1
      2024-03-10T04:59:00,2
      2024-03-10T05:00:00,3
      2024-03-11T03:59:00,4
      2024-03-11T04:00:00,5
      """
    When I run brrrrr sql:
      """
      SELECT time_bucket('1 day', ts, 'America/New_York') AS day, count(*) AS n,
             min(timezone('America/New_York', ts)) AS first_local, max(ts AT TIME ZONE 'Asia/Tokyo') AS last_tokyo
      FROM dst GROUP BY day ORDER BY day
      """
    Then the output is:
      """
      day,n,first_local,last_tokyo
      2024-03-09 05:00:00,2,2024-03-09 10:00:00,2024-03-10 13:59:00
      2024-03-10 05:00:00,2,2024-03-10 00:00:00,2024-03-11 12:59:00
      2024-03-11 04:00:00,1,2024-03-11 00:00:00,2024-03-11 13:00:00
      """

  Scenario: lead reads the next row of each partition, beside lag
    When I run brrrrr sql:
      """
      SELECT ts, symbol, price, lead(price) OVER w AS next, lag(price) OVER w AS prev
      FROM trades WINDOW w AS (PARTITION BY symbol ORDER BY ts) ORDER BY ts
      """
    Then the output is:
      """
      ts,symbol,price,next,prev
      2024-01-01 00:00:10,A,100,101,
      2024-01-01 00:00:20,B,50,51,
      2024-01-01 00:00:30,A,101,99,100
      2024-01-01 00:01:05,A,99,102,101
      2024-01-01 00:01:10,B,51,,50
      2024-01-01 00:02:00,A,102,,99
      """

  Scenario: A window join: aggregates of the quotes in a time range before each trade
    When I run brrrrr sql:
      """
      SELECT t.ts, t.symbol, w.n, w.bid FROM trades t
      LEFT JOIN LATERAL (
        SELECT count(*) AS n, avg(q.bid) AS bid FROM quotes q
        WHERE q.symbol = t.symbol AND q.ts BETWEEN t.ts - INTERVAL '30 seconds' AND t.ts
      ) w ON true
      ORDER BY t.ts
      """
    Then the output is:
      """
      ts,symbol,n,bid
      2024-01-01 00:00:10,A,1,99.5
      2024-01-01 00:00:20,B,1,49.5
      2024-01-01 00:00:30,A,2,100
      2024-01-01 00:01:05,A,1,98.5
      2024-01-01 00:01:10,B,0,
      2024-01-01 00:02:00,A,0,
      """

  Scenario: PIVOT: symbols as columns, their values found first; UNPIVOT back
    When I run brrrrr sql:
      """
      PIVOT (SELECT time_bucket('1m', ts) AS minute, symbol, price, ts FROM trades)
      ON symbol USING last(price, ts) GROUP BY minute ORDER BY minute
      """
    Then the output is:
      """
      minute,A,B
      2024-01-01 00:00:00,101,50
      2024-01-01 00:01:00,99,51
      2024-01-01 00:02:00,102,
      """
    When I run brrrrr sql:
      """
      SELECT * FROM (SELECT symbol, price, size FROM trades WHERE symbol = 'B')
      UNPIVOT (value FOR field IN (price, size)) ORDER BY field, value
      """
    Then the output is:
      """
      symbol,field,value
      B,price,50
      B,price,51
      B,size,1
      B,size,2
      """

  Scenario: A join on keys looks up reference data
    When I run brrrrr sql:
      """
      SELECT i.venue, count(*) AS trades, sum(t.size) AS volume FROM trades t
      JOIN instruments i ON t.symbol = i.symbol GROUP BY i.venue ORDER BY i.venue
      """
    Then the output is:
      """
      venue,trades,volume
      X,4,7
      Y,2,3
      """

  Scenario: A join looks up a grouped subquery: each row against its group
    When I run brrrrr sql:
      """
      SELECT t.ts, t.symbol, t.size, s.avg_size FROM trades t
      JOIN (SELECT symbol, avg(size) AS avg_size FROM trades GROUP BY symbol) s ON t.symbol = s.symbol
      WHERE t.size > s.avg_size ORDER BY t.ts
      """
    Then the output is:
      """
      ts,symbol,size,avg_size
      2024-01-01 00:00:20,B,2,1.5
      2024-01-01 00:00:30,A,2,1.75
      2024-01-01 00:02:00,A,3,1.75
      """

  Scenario: A join looks up a CTE or a view, and a LEFT JOIN a filtered subquery
    When I run brrrrr sql:
      """
      WITH avgs AS (SELECT symbol, avg(size) AS avg_size FROM trades GROUP BY symbol)
      SELECT t.symbol, count(*) AS n FROM trades t JOIN avgs a ON t.symbol = a.symbol
      WHERE t.size > a.avg_size GROUP BY t.symbol ORDER BY t.symbol
      """
    Then the output is:
      """
      symbol,n
      A,2
      B,1
      """
    When I run brrrrr sql:
      """
      CREATE VIEW avgs AS SELECT symbol, avg(size) AS avg_size FROM trades GROUP BY symbol;
      SELECT count(*) AS n FROM trades t JOIN avgs ON t.symbol = avgs.symbol WHERE t.size > avgs.avg_size
      """
    Then the output is:
      """
      n
      3
      """
    When I run brrrrr sql:
      """
      SELECT t.ts, t.symbol, x.venue FROM trades t
      LEFT JOIN (SELECT * FROM instruments WHERE venue = 'X') x ON t.symbol = x.symbol ORDER BY t.ts
      """
    Then the output is:
      """
      ts,symbol,venue
      2024-01-01 00:00:10,A,X
      2024-01-01 00:00:20,B,
      2024-01-01 00:00:30,A,X
      2024-01-01 00:01:05,A,X
      2024-01-01 00:01:10,B,
      2024-01-01 00:02:00,A,X
      """

  Scenario: An as-of join of a filtered subquery, and of one whose time is computed
    When I run brrrrr sql:
      """
      SELECT t.ts, q.bid FROM trades t
      ASOF JOIN (SELECT * FROM quotes WHERE bid > 99) q ON t.symbol = q.symbol AND t.ts >= q.ts ORDER BY t.ts
      """
    Then the output is:
      """
      ts,bid
      2024-01-01 00:00:10,99.5
      2024-01-01 00:00:30,100.5
      2024-01-01 00:01:05,100.5
      2024-01-01 00:02:00,100.5
      """
    When I run brrrrr sql:
      """
      SELECT t.ts, t.symbol, b.high_bid FROM trades t
      ASOF JOIN (SELECT time_bucket('1m', ts) AS minute, symbol, max(bid) AS high_bid FROM quotes GROUP BY minute, symbol) b
      ON t.symbol = b.symbol AND t.ts >= b.minute ORDER BY t.ts
      """
    Then the output is:
      """
      ts,symbol,high_bid
      2024-01-01 00:00:10,A,100.5
      2024-01-01 00:00:20,B,49.5
      2024-01-01 00:00:30,A,100.5
      2024-01-01 00:01:05,A,98.5
      2024-01-01 00:01:10,B,49.5
      2024-01-01 00:02:00,A,98.5
      """

  Scenario: Window functions over each symbol, in time order
    When I run brrrrr sql:
      """
      SELECT ts, price, price - lag(price) OVER (PARTITION BY symbol ORDER BY ts) AS change,
             sum(size) OVER (PARTITION BY symbol ORDER BY ts) AS volume_so_far,
             avg(price) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg2
      FROM trades WHERE symbol = 'A' ORDER BY ts
      """
    Then the output is:
      """
      ts,price,change,volume_so_far,avg2
      2024-01-01 00:00:10,100,,1,100
      2024-01-01 00:00:30,101,1,3,100.5
      2024-01-01 00:01:05,99,-2,4,100
      2024-01-01 00:02:00,102,3,7,100.5
      """

  Scenario: An exponential moving average
    When I run brrrrr sql:
      """
      SELECT price, ema(price, 0.5) OVER (PARTITION BY symbol ORDER BY ts) AS ema FROM trades WHERE symbol = 'A' ORDER BY ts
      """
    Then the output is:
      """
      price,ema
      100,100
      101,100.5
      99,99.75
      102,100.875
      """

  Scenario: Conditional aggregates, HAVING, DISTINCT
    When I run brrrrr sql:
      """
      SELECT symbol, sum(size) FILTER (WHERE side = 'buy') AS bought, count(*) FILTER (WHERE side = 'sell') AS sells
      FROM trades GROUP BY symbol HAVING count(*) > 2
      """
    Then the output is:
      """
      symbol,bought,sells
      A,6,1
      """
    When I run brrrrr sql:
      """
      SELECT DISTINCT side FROM trades ORDER BY side
      """
    Then the output is:
      """
      side
      buy
      sell
      """

  Scenario: Subqueries, CTEs and UNION ALL
    When I run brrrrr sql:
      """
      WITH bars AS (SELECT time_bucket('1m', ts) AS m, symbol, last(price, ts) AS close FROM trades GROUP BY m, symbol)
      SELECT symbol, count(*) AS bars, max(close) AS best FROM bars GROUP BY symbol
      UNION ALL
      SELECT 'all', count(*), max(price) FROM trades
      ORDER BY bars DESC
      """
    Then the output is:
      """
      symbol,bars,best
      all,6,102
      A,3,102
      B,2,51
      """

  Scenario: Times: literals, intervals, parts, truncation and formats
    When I run brrrrr sql:
      """
      SELECT ts, ts + INTERVAL '1 hour' AS later, date_trunc('minute', ts) AS minute, hour(ts) AS h,
             strftime(ts, '%H:%M') AS hm, epoch_ms(ts) AS ms
      FROM trades WHERE ts BETWEEN '2024-01-01 00:00:20' AND TIMESTAMP '2024-01-01 00:00:30'
      """
    Then the output is:
      """
      ts,later,minute,h,hm,ms
      2024-01-01 00:00:20,2024-01-01 01:00:20,2024-01-01 00:00:00,0,00:00,1704067220000
      2024-01-01 00:00:30,2024-01-01 01:00:30,2024-01-01 00:00:00,0,00:00,1704067230000
      """

  Scenario: A quote in a string is written twice: in literals, paths, reader options and views
    Given a file "O'Brien.csv" with:
      """
      name|note
      O'Brien|it's
      Smith|NA
      """
    When I run brrrrr sql:
      """
      SELECT 'it''s' AS s, name, note FROM read_csv('O''Brien.csv', delim = '|', nullstr = ['NA']) WHERE name = 'O''Brien' OR note IS NULL
      """
    Then the output is:
      """
      s,name,note
      it's,O'Brien,it's
      it's,Smith,
      """
    When I run brrrrr sql:
      """
      CREATE VIEW irish AS SELECT name FROM read_csv('O''Brien.csv', delim = '|') WHERE name = 'O''Brien';
      SELECT count(*) AS n FROM irish
      """
    Then the output is:
      """
      n
      1
      """

  Scenario: Text, math and conditional functions
    When I run brrrrr sql:
      """
      SELECT upper(side) AS s, round(price / 3, 2) AS r, CAST(size AS VARCHAR) || 'x' AS t,
             CASE side WHEN 'buy' THEN 1 ELSE -1 END AS sign, coalesce(NULL, side) AS c, length(side) AS n
      FROM trades WHERE symbol = 'B' ORDER BY ts
      """
    Then the output is:
      """
      s,r,t,sign,c,n
      SELL,16.67,2x,-1,sell,4
      BUY,17,1x,1,buy,3
      """

  Scenario: SELECT without FROM evaluates once
    When I run brrrrr sql:
      """
      SELECT 1 + 1 AS two, upper('brrrrr') AS name
      """
    Then the output is:
      """
      two,name
      2,BRRRRR
      """

  Scenario: Percentiles and spread statistics
    When I run brrrrr sql:
      """
      SELECT median(price) AS p50, quantile_cont(price, 0.25) AS p25, stddev(price) AS sd, corr(price, size) AS c FROM trades WHERE symbol = 'A'
      """
    Then the output is:
      """
      p50,p25,sd,c
      100.5,99.75,1.2909944487358056,0.9438798074485389
      """

  Scenario: Percentiles are exact however many rows
    Given the cookbook's files
    When I run brrrrr sql:
      """
      SELECT symbol, median(price) AS p50, percentile_cont(0.9) WITHIN GROUP (ORDER BY price) AS p90
      FROM trades GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,p50,p90
      BTC,42220.9502,42475.25748
      ETH,2299.6065,2313.68872
      SOL,98.555,100.18
      """

  Scenario: count(DISTINCT x)
    When I run brrrrr sql:
      """
      SELECT symbol, count(DISTINCT side) AS sides, count(DISTINCT size) AS sizes FROM trades GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,sides,sizes
      A,2,3
      B,2,2
      """

  Scenario: UNION and UNION ALL in a subquery
    When I run brrrrr sql:
      """
      SELECT symbol, count(*) AS n FROM (SELECT symbol FROM trades UNION SELECT symbol FROM instruments) GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,n
      A,1
      B,1
      """
    When I run brrrrr sql:
      """
      SELECT time_bucket('1m', ts) AS minute, count(*) AS n
      FROM (SELECT ts, symbol FROM trades UNION ALL SELECT ts, symbol FROM quotes) GROUP BY minute ORDER BY minute
      """
    Then the output is:
      """
      minute,n
      2024-01-01 00:00:00,6
      2024-01-01 00:01:00,3
      2024-01-01 00:02:00,2
      """
