Feature: Query files where they are
  brrrrr reads Parquet, CSV (plain, gzipped, tab- or semicolon-separated) and JSON lines
  directly: a path, a glob or a directory, with nothing loaded or registered first.

  Background:
    Given a file "trades.csv" with:
      """
      ts,symbol,price,size
      2024-01-01T00:00:10,BTC,42000.5,0.1
      2024-01-01T00:00:20,ETH,2300,1.5
      2024-01-01T00:01:05,BTC,42010,0.3
      """

  Scenario: A CSV file
    When I run brrrrr sql:
      """
      SELECT symbol, price FROM 'trades.csv' WHERE size > 0.2
      """
    Then the output is:
      """
      symbol,price
      ETH,2300
      BTC,42010
      """

  Scenario: FROM alone is SELECT *
    When I run brrrrr sql:
      """
      FROM 'trades.csv' LIMIT 1
      """
    Then the output is:
      """
      ts,symbol,price,size
      2024-01-01 00:00:10,BTC,42000.5,0.1
      """

  Scenario Outline: Every format reads alike
    Given "trades.csv" written as "<file>"
    When I run brrrrr sql:
      """
      SELECT symbol, count(*) AS n, sum(size) AS volume FROM '<file>' GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,n,volume
      BTC,2,0.4
      ETH,1,1.5
      """

    Examples:
      | file           |
      | t.parquet      |
      | t.json         |
      | out/t.csv      |

  Scenario: A gzipped CSV file, as market data archives keep them
    Given a gzipped file "trades.csv.gz" with:
      """
      ts,symbol,price,size
      2024-01-01T00:00:10,BTC,42000.5,0.1
      2024-01-01T00:00:20,ETH,2300,1.5
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 'trades.csv.gz'
      """
    Then the output is:
      """
      n
      2
      """

  Scenario: Tab- and semicolon-separated files
    Given a file "t.tsv" with:
      """
      a	b
      1	x
      """
    And a file "s.csv" with:
      """
      a;b
      2;y
      """
    When I run brrrrr sql:
      """
      SELECT a, b FROM 't.tsv' UNION ALL SELECT a, b FROM 's.csv'
      """
    Then the output is:
      """
      a,b
      1,x
      2,y
      """

  Scenario: JSON lines, times in text read as times
    Given a file "events.jsonl" with:
      """
      {"at": "2024-01-01T00:00:01Z", "kind": "login", "ms": 12}
      {"at": "2024-01-01T00:00:59Z", "kind": "login", "ms": 30}
      {"at": "2024-01-01T00:01:02Z", "kind": "logout", "ms": 7}
      """
    When I run brrrrr sql:
      """
      SELECT time_bucket('1m', at) AS minute, count(*) AS n, max(ms) AS slowest FROM 'events.jsonl' GROUP BY minute ORDER BY minute
      """
    Then the output is:
      """
      minute,n,slowest
      2024-01-01 00:00:00,2,30
      2024-01-01 00:01:00,1,7
      """

  Scenario: A glob over files, and a directory of Hive partitions
    Given a file "data/date=2024-01-01/part-0.csv" with:
      """
      ts,symbol,price
      2024-01-01T10:00:00,BTC,1
      2024-01-01T11:00:00,BTC,2
      """
    And a file "data/date=2024-01-02/part-0.csv" with:
      """
      ts,symbol,price
      2024-01-02T10:00:00,BTC,3
      """
    When I run brrrrr sql:
      """
      SELECT date, count(*) AS n, max(price) AS high FROM 'data' GROUP BY date ORDER BY date
      """
    Then the output is:
      """
      date,n,high
      2024-01-01,2,2
      2024-01-02,1,3
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 'data/*/part-*.csv'
      """
    Then the output is:
      """
      n
      3
      """

  Scenario: A WHERE on Hive partitions reads only their files
    A partition of whole numbers is a number (hour 9 is before hour 10); the others are text.
    Given "trades.csv" written as "data/date=2024-01-01/hour=9/part-0.parquet"
    And "trades.csv" written as "data/date=2024-01-02/hour=9/part-0.parquet"
    And "trades.csv" written as "data/date=2024-01-02/hour=10/part-0.parquet"
    And a file "data/date=2024-01-03/hour=9/part-0.parquet" with:
      """
      not Parquet
      """
    When I run "brrrrr sql --format csv --table trades=data" on:
      """
      SELECT date, hour, count(*) AS n FROM trades
      WHERE date BETWEEN '2024-01-01' AND '2024-01-02' AND hour < 10 GROUP BY date, hour ORDER BY date, hour
      """
    Then the output is:
      """
      date,hour,n
      2024-01-01,9,3
      2024-01-02,9,3
      """
    When I run "brrrrr sql --format csv --table trades=data" on:
      """
      SELECT count(*) AS n FROM trades WHERE date IN ('2024-01-02', '2024-01-04') AND symbol = 'BTC'
      """
    Then the output is:
      """
      n
      4
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 'data'
      """
    Then it fails with "part-0.parquet: not a Parquet file"

  Scenario: read_csv and read_parquet name the format of a file named otherwise
    Given a file "export.txt.data" with:
      """
      x
      1
      """
    When I run brrrrr sql:
      """
      SELECT x * 10 AS y FROM read_csv('export.txt.data')
      """
    Then the output is:
      """
      y
      10
      """

  Scenario: A table name finds its file in the working directory
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM trades
      """
    Then the output is:
      """
      n
      3
      """

  Scenario: A file that is not there
    When I run brrrrr sql:
      """
      SELECT * FROM 'nope.parquet'
      """
    Then it fails with "no file nope.parquet"

  Scenario: Files whose columns differ: the first file's by default, every file's by name with union_by_name
    Given a file "days/2024-01-01.csv" with:
      """
      ts,price
      2024-01-01T10:00:00,1
      """
    And a file "days/2024-01-02.csv" with:
      """
      ts,price,size
      2024-01-02T10:00:00,2.5,7
      """
    When I run brrrrr sql:
      """
      SELECT * FROM 'days/*.csv' ORDER BY ts
      """
    Then it fails with "2024-01-02.csv: column price is double here and bigint in the table's first file: read the files with union_by_name = true"
    When I run brrrrr sql:
      """
      SELECT ts, price, size FROM read_csv('days/*.csv', union_by_name = true) ORDER BY ts
      """
    Then the output is:
      """
      ts,price,size
      2024-01-01 10:00:00,1,
      2024-01-02 10:00:00,2.5,7
      """

  Scenario: Times as kdb+ and other exports write them
    Given a file "kdb.csv" with:
      """
      time,sym,price
      2024.01.02D09:30:00.123456789,AAPL,185.5
      2024.01.02D09:30:01.000000000,AAPL,185.6
      """
    And a file "eu.csv" with:
      """
      when,price
      02/01/2024 09:30,1
      """
    When I run brrrrr sql:
      """
      SELECT time, time_bucket('1s', time) AS second, price FROM 'kdb.csv' ORDER BY time
      """
    Then the output is:
      """
      time,second,price
      2024-01-02 09:30:00.123456,2024-01-02 09:30:00,185.5
      2024-01-02 09:30:01,2024-01-02 09:30:01,185.6
      """
    When I run brrrrr sql:
      """
      SELECT "when", price FROM read_csv('eu.csv', timestampformat = '%d/%m/%Y %H:%M')
      """
    Then the output is:
      """
      when,price
      2024-01-02 09:30:00,1
      """

  Scenario: An exchange's trades download: times in microseconds since the epoch, read as times
    Given a gzipped file "trades_2024-01-01_BTCUSDT.csv.gz" with:
      """
      exchange,symbol,timestamp,local_timestamp,id,side,price,amount
      venue-a,BTCUSDT,1704067200012000,1704067200015123,1,buy,42000.5,0.01
      venue-a,BTCUSDT,1704067259999000,1704067260001000,2,sell,42001,0.5
      venue-a,BTCUSDT,1704067260000500,1704067260002000,3,buy,42002,1
      """
    When I run brrrrr sql:
      """
      SELECT time_bucket('1m', timestamp) AS minute, count(*) AS trades, sum(amount) AS volume, last(price, timestamp) AS close
      FROM read_csv('trades_2024-01-01_BTCUSDT.csv.gz', types = {'timestamp': 'TIMESTAMP_US', 'local_timestamp': 'TIMESTAMP_US'})
      GROUP BY minute ORDER BY minute
      """
    Then the output is:
      """
      minute,trades,volume,close
      2024-01-01 00:00:00,2,0.51,42001
      2024-01-01 00:01:00,1,1,42002
      """
    When I run brrrrr sql:
      """
      SELECT max(timestamp) FROM read_csv('trades_2024-01-01_BTCUSDT.csv.gz', types = {'timestamp': 'TIMESTAMP'})
      """
    Then it fails with "line 2, column timestamp: 1704067200012000 is a number: TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_US or TIMESTAMP_NS reads it"

  Scenario: CSV as exports write it: no header, lines to skip, NULL's texts, quoted delimiters and line breaks
    Given a file "export.txt" with:
      """
      # exported 2024-01-02 by the desk
      1;"Smith; J";NA
      2;"two
      lines";3.5
      """
    When I run brrrrr sql:
      """
      SELECT column0 AS id, column1 AS name, column2 AS qty
      FROM read_csv('export.txt', header = false, skip = 1, delim = ';', nullstr = 'NA') ORDER BY id
      """
    Then the output is:
      """
      id,name,qty
      1,Smith; J,
      2,"two
      lines",3.5
      """
    When I run brrrrr sql:
      """
      SELECT sum(n) AS n FROM read_csv('export.txt', skip = 1, delim = ';', columns = {'n': 'BIGINT', 'name': 'VARCHAR', 'qty': 'VARCHAR'})
      """
    Then the output is:
      """
      n
      3
      """
    When I run brrrrr sql:
      """
      SELECT * FROM read_csv('export.txt', skip = 1, delim = ';', header = false, types = {'column2': 'DOUBLE'})
      """
    Then it fails with "export.txt: Parser error: Error while parsing value 'NA' as type 'Float64' for column column2 at line 2"

  Scenario: Delta Lake and Iceberg tables, as their logs and snapshots say
    Given the table "delta_trades" from the fixtures
    And the table "iceberg_trades" from the fixtures
    When I run brrrrr sql:
      """
      SELECT date, count(*) AS n, sum(size) AS volume FROM delta_scan('delta_trades') GROUP BY date ORDER BY date
      """
    Then the output is:
      """
      date,n,volume
      2024-01-01,2,3
      2024-01-02,2,9
      2024-01-03,1,6
      """
    When I run brrrrr sql:
      """
      SELECT date, count(*) AS n FROM iceberg_scan('iceberg_trades') WHERE date = '2024-01-02' GROUP BY date
      """
    Then the output is:
      """
      date,n
      2024-01-02,2
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM delta_scan('iceberg_trades')
      """
    Then it fails with "no Delta table at iceberg_trades"
    # the files on disk, as a glob reads them: a removed one too
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 'delta_trades/date=*/*.parquet'
      """
    Then the output is:
      """
      n
      8
      """

  Scenario: Databento DBN files: bars from trades, and each trade with the quote it met
    Given the fixture "dbn/xnas-itch-20240102.trades.dbn.zst"
    And the fixture "dbn/xnas-itch-20240102.mbp-1.dbn.zst"
    When I run brrrrr sql:
      """
      SELECT time_bucket('1m', ts_event) AS minute, symbol, first(price, ts_event) AS open, max(price) AS high,
             min(price) AS low, last(price, ts_event) AS close, sum(size) AS volume
      FROM 'xnas-itch-20240102.trades.dbn.zst' GROUP BY minute, symbol ORDER BY minute, symbol
      """
    Then the output is:
      """
      minute,symbol,open,high,low,close,volume
      2024-01-02 14:30:00,AAPL,185.64,185.64,185.64,185.64,100
      2024-01-02 14:30:00,MSFT,370.01,370.01,370.01,370.01,50
      2024-01-02 14:31:00,AAPL,185.7,185.7,185.6505,185.6505,225
      2024-01-02 14:32:00,MSFT,370.12,370.12,370.12,370.12,10
      """
    When I run brrrrr sql:
      """
      SELECT t.ts_event, t.symbol, t.price, q.bid_px_00 AS bid, q.ask_px_00 AS ask
      FROM 'xnas-itch-20240102.trades.dbn.zst' t
      ASOF JOIN 'xnas-itch-20240102.mbp-1.dbn.zst' q ON t.symbol = q.symbol AND t.ts_event >= q.ts_event
      ORDER BY t.ts_event
      """
    Then the output is:
      """
      ts_event,symbol,price,bid,ask
      2024-01-02 14:30:00,AAPL,185.64,185.63,185.65
      2024-01-02 14:30:00.005,MSFT,370.01,370,370.02
      2024-01-02 14:31:01,AAPL,185.7,185.68,185.71
      2024-01-02 14:31:02.5,AAPL,185.6505,185.68,185.71
      2024-01-02 14:32:05,MSFT,370.12,370.1,370.13
      """
    # a file named otherwise: read_dbn names the format, as read_parquet does
    Given the fixture "dbn/xnas-itch-20240102.trades.dbn" as "trades.bin"
    When I run brrrrr sql:
      """
      SELECT count(*) AS n, sum(size) AS volume FROM read_dbn('trades.bin')
      """
    Then the output is:
      """
      n,volume
      5,385
      """
