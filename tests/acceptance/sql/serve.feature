Feature: brrrrr serve: live tables, live views, and their history
  A server holds the newest rows in memory and the rest in Parquet by day, and answers one query
  over both, over HTTP and the PostgreSQL protocol. Live views run the same SQL as rows arrive.

  Scenario: Rows written are queried at once, today and history in one table
    Given a brrrrr server
    When I write to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100, "size": 1}
      {"ts": "2024-01-01T00:00:20Z", "symbol": "B", "price": 50, "size": 2}
      """
    Then the answer is:
      """
      {"table":"trades","rows":2}
      """
    When I flush
    And I write to "trades":
      """
      {"ts": "2024-01-02T00:00:10Z", "symbol": "A", "price": 101, "size": 3}
      """
    And I query over HTTP:
      """
      SELECT time_bucket('1d', ts) AS day, count(*) AS n, sum(size) AS volume FROM trades GROUP BY day ORDER BY day
      """
    Then the answer is:
      """
      day,n,volume
      2024-01-01 00:00:00,2,3
      2024-01-02 00:00:00,1,3
      """

  Scenario: CSV rows, and JSON answers with their types
    Given a brrrrr server
    When I write CSV to "quotes":
      """
      ts,symbol,bid,ask
      2024-01-01T00:00:05,A,99.5,100.5
      2024-01-01T00:00:15,B,49.5,50.5
      """
    And I query for JSON:
      """
      SELECT symbol, (ask - bid) AS spread, ts FROM quotes ORDER BY ts
      """
    Then the answer is:
      """
      {"columns":["symbol","spread","ts"],"types":["text","float","time"],"rows":[["A",1,"2024-01-01T00:00:05Z"],["B",1,"2024-01-01T00:00:15Z"]]
      """

  Scenario: A live view runs the query over the rows so far, then as rows arrive
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100, "size": 1}
      {"ts": "2024-01-01T00:00:30Z", "symbol": "A", "price": 101, "size": 2}
      """
    When I query over HTTP:
      """
      CREATE LIVE VIEW bars AS
      SELECT time_bucket('1m', ts) AS minute, symbol, first(price, ts) AS open, last(price, ts) AS close, sum(size) AS volume
      FROM trades GROUP BY minute, symbol
      """
    Then the answer says "live view bars created over 2 rows so far"
    When I write to "trades":
      """
      {"ts": "2024-01-01T00:01:05Z", "symbol": "A", "price": 99, "size": 1}
      {"ts": "2024-01-01T00:02:00Z", "symbol": "A", "price": 102, "size": 3}
      """
    And I query over HTTP:
      """
      SELECT * FROM bars ORDER BY minute
      """
    Then the answer is:
      """
      minute,symbol,open,close,volume
      2024-01-01 00:00:00,A,100,101,3
      2024-01-01 00:01:00,A,99,99,1
      """

  # a view's table is typed by the view's columns: read back from its first rows as JSON, a float
  # whose first values were whole was a column of integers, and later fractions were cut off
  Scenario: A live view keeps the types of its columns whatever its first rows hold
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100.0, "size": 1.0}
      """
    When I query over HTTP:
      """
      CREATE LIVE VIEW bars AS SELECT time_bucket('1m', ts) AS minute, max(price) AS high, sum(size) AS volume FROM trades GROUP BY minute
      """
    And I write to "trades":
      """
      {"ts": "2024-01-01T00:01:10Z", "symbol": "A", "price": 99.75, "size": 0.5}
      {"ts": "2024-01-01T00:02:10Z", "symbol": "A", "price": 99, "size": 1}
      """
    And I query for JSON:
      """
      SELECT * FROM bars ORDER BY minute
      """
    Then the answer is:
      """
      {"columns":["minute","high","volume"],"types":["time","float","float"],"rows":[["2024-01-01T00:00:00Z",100,1],["2024-01-01T00:01:00Z",99.75,0.5]]
      """

  Scenario: A live view's answer is the same query's over the table
    Given a brrrrr server
    When I query over HTTP:
      """
      CREATE LIVE VIEW volume AS SELECT time_bucket('1m', ts) AS minute, symbol, sum(size) AS v FROM trades GROUP BY minute, symbol
      """
    Then the answer is 400
    And the answer says "no table trades"
    When I write to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100, "size": 1}
      """
    And I query over HTTP:
      """
      CREATE LIVE VIEW volume AS SELECT time_bucket('1m', ts) AS minute, symbol, sum(size) AS v FROM trades GROUP BY minute, symbol
      """
    And I write to "trades":
      """
      {"ts": "2024-01-01T00:00:40Z", "symbol": "B", "price": 50, "size": 2}
      {"ts": "2024-01-01T00:00:50Z", "symbol": "A", "price": 101, "size": 4}
      {"ts": "2024-01-01T00:01:10Z", "symbol": "A", "price": 102, "size": 1}
      """
    And I query over HTTP:
      """
      SELECT minute, symbol, v FROM volume ORDER BY minute, symbol
      """
    Then the answer is:
      """
      minute,symbol,v
      2024-01-01 00:00:00,A,5
      2024-01-01 00:00:00,B,2
      """
    When I query over HTTP:
      """
      SELECT time_bucket('1m', ts) AS minute, symbol, sum(size) AS v FROM trades WHERE ts < '2024-01-01 00:01:00' GROUP BY minute, symbol ORDER BY minute, symbol
      """
    Then the answer is:
      """
      minute,symbol,v
      2024-01-01 00:00:00,A,5
      2024-01-01 00:00:00,B,2
      """

  Scenario: A live view fills a key's gap when its next bucket comes; an as-of join at an offset is a query
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100, "size": 1}
      """
    When I query over HTTP:
      """
      CREATE LIVE VIEW bars AS
      SELECT time_bucket_gapfill('1m', ts) AS minute, symbol, locf(last(price, ts)) AS close, coalesce(sum(size), 0) AS volume
      FROM trades GROUP BY minute, symbol
      """
    And I write to "trades":
      """
      {"ts": "2024-01-01T00:03:05Z", "symbol": "A", "price": 99, "size": 1}
      {"ts": "2024-01-01T00:04:00Z", "symbol": "A", "price": 102, "size": 3}
      """
    And I query over HTTP:
      """
      SELECT * FROM bars ORDER BY minute
      """
    Then the answer is:
      """
      minute,symbol,close,volume
      2024-01-01 00:00:00,A,100,1
      2024-01-01 00:01:00,A,100,0
      2024-01-01 00:02:00,A,100,0
      2024-01-01 00:03:00,A,99,1
      """
    When I query over HTTP:
      """
      CREATE LIVE VIEW later AS SELECT t.ts, q.price FROM trades t
      ASOF JOIN trades q ON t.symbol = q.symbol AND t.ts + INTERVAL '1 minute' >= q.ts
      """
    Then the answer is 400
    And the answer says "at a time offset"

  Scenario: A live view with lead emits a row when its next one comes; a window join is a query
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100, "size": 1}
      {"ts": "2024-01-01T00:00:30Z", "symbol": "A", "price": 101, "size": 2}
      """
    When I query over HTTP:
      """
      CREATE LIVE VIEW nexts AS SELECT ts, symbol, price, lead(price) OVER (PARTITION BY symbol ORDER BY ts) AS next FROM trades
      """
    And I write to "trades":
      """
      {"ts": "2024-01-01T00:01:05Z", "symbol": "A", "price": 99, "size": 1}
      """
    And I query over HTTP:
      """
      SELECT * FROM nexts ORDER BY ts
      """
    Then the answer is:
      """
      ts,symbol,price,next
      2024-01-01 00:00:10,A,100,101
      2024-01-01 00:00:30,A,101,99
      """
    When I query over HTTP:
      """
      CREATE LIVE VIEW busy AS SELECT t.ts, w.n FROM trades t LEFT JOIN LATERAL
      (SELECT count(*) AS n FROM trades q WHERE q.symbol = t.symbol AND q.ts BETWEEN t.ts - INTERVAL '1 minute' AND t.ts) w ON true
      """
    Then the answer is 400
    And the answer says "window join"

  Scenario: A subscription receives each new row
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:01Z", "symbol": "A", "price": 1}
      """
    When I subscribe to "trades"
    And I write to "trades":
      """
      {"ts": "2024-01-01T00:00:02Z", "symbol": "B", "price": 2}
      {"ts": "2024-01-01T00:00:03Z", "symbol": "C", "price": 3}
      """
    Then the subscription receives:
      """
      {"ts":"2024-01-01T00:00:02Z","symbol":"B","price":2}
      {"ts":"2024-01-01T00:00:03Z","symbol":"C","price":3}
      """

  Scenario: A stop flushes; a kill loses nothing written, and a live view goes on without repeating itself
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100, "size": 1}
      {"ts": "2024-01-01T00:01:10Z", "symbol": "A", "price": 101, "size": 1}
      """
    When I query over HTTP:
      """
      CREATE LIVE VIEW per_minute AS SELECT time_bucket('1m', ts) AS minute, count(*) AS n FROM trades GROUP BY minute
      """
    And the server stops and starts again
    And I write to "trades":
      """
      {"ts": "2024-01-01T00:02:10Z", "symbol": "A", "price": 102, "size": 1}
      """
    And the server is killed and starts again
    And I write to "trades":
      """
      {"ts": "2024-01-01T00:03:10Z", "symbol": "A", "price": 103, "size": 1}
      """
    And I query over HTTP:
      """
      SELECT count(*) AS trades, sum(price) AS total FROM trades
      """
    Then the answer is:
      """
      trades,total
      4,406
      """
    When I query over HTTP:
      """
      SELECT minute, n FROM per_minute ORDER BY minute
      """
    Then the answer is:
      """
      minute,n
      2024-01-01 00:00:00,1
      2024-01-01 00:01:00,1
      2024-01-01 00:02:00,1
      """

  Scenario: PostgreSQL clients: the simple protocol, and the extended one with typed values
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100.5, "size": 1}
      {"ts": "2024-01-01T00:00:20Z", "symbol": "B", "price": 50.25, "size": 2}
      """
    When I query over PostgreSQL:
      """
      SET application_name = 'test'; SELECT symbol, price FROM trades ORDER BY ts
      """
    Then the answer is:
      """
      symbol,price
      A,100.5
      B,50.25
      """
    When I query over PostgreSQL's extended protocol:
      """
      SELECT max(ts) AS last, sum(size) AS volume, avg(price) AS avg FROM trades
      """
    Then the answer is:
      """
      last:timestamptz,volume:int8,avg:float8
      2024-01-01 00:00:20,3,75.375
      """
    When I query over PostgreSQL:
      """
      SELECT version()
      """
    Then the answer says "brrrrr"

  # what drivers send: psycopg, JDBC, node-postgres and pgx bind typed parameters, in text or in
  # binary; Grafana's $__timeFilter writes ISO 8601 times. tokio-postgres describes a statement
  # before its parameters are bound, when its columns' types are not known yet: they are text
  # (psycopg, JDBC and node-postgres describe the bound statement, and get them typed)
  Scenario: PostgreSQL clients: typed parameters, a driver's setup statements, and ISO 8601 times
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100.5, "size": 1}
      {"ts": "2024-01-01T00:00:20Z", "symbol": "B", "price": 50.25, "size": 2}
      {"ts": "2024-01-01T00:00:30Z", "symbol": "A", "price": 101, "size": 3}
      """
    And the PostgreSQL parameters:
      | text        | A                    |
      | int8        | 0                    |
      | float8      | 100.75               |
      | timestamptz | 2024-01-01T00:00:15Z |
      | int4        | 2                    |
      | timestamp   | 2024-01-01 00:00:35  |
    When I query over PostgreSQL's extended protocol:
      """
      SELECT ts, size, price FROM trades WHERE symbol = $1 AND size > $2 AND price > $3 AND ts >= $4 AND size > $5 AND ts < $6
      """
    Then the answer is:
      """
      ts:varchar,size:varchar,price:varchar
      2024-01-01 00:00:30,3,101
      """
    When I query over PostgreSQL:
      """
      SET extra_float_digits = 3; SHOW server_version; SELECT current_schema(); SHOW transaction isolation level
      """
    Then the answer is:
      """
      server_version
      16.0
      public
      read committed
      """
    When I query over PostgreSQL:
      """
      SELECT count(*) AS n FROM trades WHERE ts BETWEEN '2024-01-01T00:00:15Z' AND '2024-01-01T01:00:25+01:00'
      """
    Then the answer is:
      """
      n
      1
      """

  # a shared server: a statement has a time limit (--query-timeout, a session's SET
  # statement_timeout), and stops when its client hangs up or cancels it (psql's Ctrl-C), at its
  # next batch of rows; at most --max-queries run at once, the others wait their turn
  Scenario: A slow statement stops at its time limit, and when its client hangs up or cancels it
    Given a brrrrr server with "--query-timeout 1s --max-queries 1"
    And a CSV file "big.csv" of 1000000 trades
    When I query over HTTP:
      """
      {slow}
      """
    Then the answer is 400
    And the answer says "the statement ran past its time limit of 1s"
    When I query over PostgreSQL:
      """
      SET statement_timeout = 200; {slow}
      """
    Then the answer says "the statement ran past its time limit of 200ms"
    When I query over HTTP, hanging up after 200 ms:
      """
      {slow}
      """
    Then within 5 seconds the server's metrics say "brrrrr_statements_abandoned_total 1"
    When I query over PostgreSQL, canceling after 200 ms:
      """
      {slow}
      """
    Then the answer says "canceling statement due to user request"
    And within 5 seconds the server's metrics say "brrrrr_statements_canceled_total 1"
    And within 5 seconds the server's metrics say "brrrrr_statements_timed_out_total 2"
    # a stop is counted when it is made; the statement ends at its next batch of rows, and only
    # then gives its turn (--max-queries 1) to the next
    And within 5 seconds the server's metrics say "brrrrr_statements_running 0"
    When I query over HTTP:
      """
      SELECT 1 AS one
      """
    Then the answer is:
      """
      one
      1
      """

  Scenario: A query of thousands of UNION ALLs answers, and one past the limit says so
    Given a brrrrr server
    When I query over HTTP for the count of 3000 queries' UNION ALL
    Then the answer is:
      """
      n
      3000
      """
    When I query over HTTP for the count of 20000 queries' UNION ALL
    Then the answer is 400
    And the answer says "a UNION of more than 10000 queries"

  Scenario: A token guards the API and the PostgreSQL protocol
    Given a brrrrr server with the token "s3cret"
    When I send "POST /query" without the token
    Then the answer is 401
    And the answer says "a token is needed"
    When I connect over PostgreSQL with the password "wrong"
    Then the answer is 401
    When I connect over PostgreSQL with the password "s3cret"
    Then the answer is 200
    When I write to "t":
      """
      {"x": 1}
      """
    Then the answer is 200

  Scenario: Writes that do not fit the table are refused, and say why
    Given a brrrrr server
    And rows written to "trades":
      """
      {"ts": "2024-01-01T00:00:10Z", "symbol": "A", "price": 100}
      """
    When I write to "trades":
      """
      {"ts": "2024-01-01T00:00:11Z", "symbol": "A", "venue": "X"}
      """
    Then the answer is 400
    And the answer says "venue is not a column of the table"
    When I write to "bad-name":
      """
      {"x": 1}
      """
    Then the answer is 400
    And the answer says "is not a table name"

  Scenario: Kafka topics flow into live tables
    Given a brrrrr server ingesting "ticks" from the Kafka topic "ticks-{bucket}"
    When the Kafka topic "ticks-{bucket}" gets:
      """
      {"ts": "2024-01-01T00:00:01Z", "symbol": "A", "price": 1.5}
      {"ts": "2024-01-01T00:00:02Z", "symbol": "B", "price": 2.5}
      {"ts": "2024-01-01T00:00:03Z", "symbol": "A", "price": 3.5}
      """
    Then within 30 seconds the query answers:
      """
      SELECT symbol, count(*) AS n, sum(price) AS total FROM ticks GROUP BY symbol ORDER BY symbol
      ---
      symbol,n,total
      A,2,5
      B,1,2.5
      """
