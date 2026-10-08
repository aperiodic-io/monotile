Feature: Tables, views and exports
  Name a location once, name a query, look at a table's columns, see the pipeline a query
  becomes, and write results to files in any format.

  Background:
    Given a file "data/trades.csv" with:
      """
      ts,symbol,price,size
      2024-01-01T00:00:10,BTC,42000,0.5
      2024-01-01T00:00:20,ETH,2300,2
      2024-01-01T00:01:05,BTC,42010,0.25
      """

  Scenario: CREATE TABLE names a location; CREATE VIEW names a query; both read as tables
    When I run brrrrr sql:
      """
      CREATE TABLE trades AS 'data/trades.csv';
      CREATE VIEW btc AS SELECT * FROM trades WHERE symbol = 'BTC';
      SELECT count(*) AS n, sum(size) AS volume FROM btc
      """
    Then the output is:
      """
      n,volume
      2,0.75
      """

  Scenario: DESCRIBE shows a file's columns and their types
    When I run brrrrr sql:
      """
      DESCRIBE 'data/trades.csv'
      """
    Then the output is:
      """
      column,type
      ts,timestamp
      symbol,varchar
      price,bigint
      size,double
      """

  Scenario: SHOW TABLES lists what was named
    When I run brrrrr sql:
      """
      CREATE TABLE trades AS 'data/trades.csv';
      CREATE VIEW big AS SELECT * FROM trades WHERE size > 1;
      SHOW TABLES
      """
    Then the output is:
      """
      name,kind,definition
      trades,table,data/trades.csv
      big,view,SELECT * FROM trades WHERE size > 1
      """

  Scenario: EXPLAIN shows the pipeline a query becomes, as the live engine would run it
    When I run brrrrr sql:
      """
      EXPLAIN SELECT time_bucket('1m', ts) AS m, symbol, count(*) AS n FROM 'data/trades.csv' GROUP BY m, symbol
      """
    Then the output contains "tumble("
    And the output contains "EMIT AFTER WINDOW CLOSE"
    And the output contains "in ts order"

  Scenario Outline: COPY writes a result as Parquet, CSV or JSON, and it reads back the same
    When I run brrrrr sql:
      """
      COPY (SELECT symbol, sum(size) AS volume FROM 'data/trades.csv' GROUP BY symbol) TO 'out/<file>'
      """
    Then it says "2 rows written to out/<file>"
    And the file "out/<file>" exists
    When I run brrrrr sql:
      """
      SELECT * FROM 'out/<file>' ORDER BY symbol
      """
    Then the output is:
      """
      symbol,volume
      BTC,0.75
      ETH,2
      """

    Examples:
      | file           |
      | volume.parquet |
      | volume.csv     |
      | volume.json    |

  Scenario: COPY with a FORMAT option, whatever the file's name
    When I run brrrrr sql:
      """
      COPY (SELECT symbol FROM 'data/trades.csv' LIMIT 1) TO 'out/x.dat' (FORMAT csv)
      """
    Then the file "out/x.dat" reads:
      """
      symbol
      BTC
      """

  Scenario: COPY with PARTITION_BY writes a directory of Hive partitions, as DuckDB does
    When I run brrrrr sql:
      """
      COPY (SELECT strftime(ts, '%Y-%m-%d') AS date, * FROM 'data/trades.csv') TO 'archive' (FORMAT parquet, PARTITION_BY (date, symbol))
      """
    Then it says "3 rows written to archive, in 2 partitions"
    And the file "archive/date=2024-01-01/symbol=BTC/data_0.parquet" exists
    And the file "archive/date=2024-01-01/symbol=ETH/data_0.parquet" exists
    When I run brrrrr sql:
      """
      SELECT date, symbol, count(*) AS n, sum(size) AS volume FROM 'archive' WHERE symbol = 'BTC' GROUP BY date, symbol
      """
    Then the output is:
      """
      date,symbol,n,volume
      2024-01-01,BTC,2,0.75
      """
    When I run brrrrr sql:
      """
      COPY (SELECT strftime(ts, '%Y-%m-%d') AS date, * FROM 'data/trades.csv') TO 'archive' (FORMAT parquet, PARTITION_BY (date, symbol))
      """
    Then it fails with "archive is not empty: write into it with OVERWRITE_OR_IGNORE"
    When I run brrrrr sql:
      """
      COPY (SELECT strftime(ts, '%Y-%m-%d') AS date, * FROM 'data/trades.csv') TO 'archive' (FORMAT parquet, PARTITION_BY (date, symbol), OVERWRITE_OR_IGNORE);
      SELECT count(*) AS n FROM 'archive'
      """
    Then the output is:
      """
      n
      3
      """

  Scenario: A dropped view is gone
    When I run brrrrr sql:
      """
      CREATE VIEW v AS SELECT * FROM 'data/trades.csv';
      DROP VIEW v;
      SELECT * FROM v
      """
    Then it fails with "no table v"
