Feature: Object stores, read and written natively
  s3://, gs://, az:// and https:// locations read and write like paths: a file, a glob, a
  directory of Hive partitions. Credentials come from the environment, as each cloud's own
  tools take them. Remote files are cached, and fetched again when they change.

  Background:
    Given a file "trades.csv" with:
      """
      ts,symbol,price,size
      2024-01-01T00:00:10,BTC,42000,0.5
      2024-01-01T00:00:20,ETH,2300,2
      2024-01-01T00:01:05,BTC,42010,0.25
      """

  Scenario: S3: a file, a glob and a directory of Hive partitions
    Given S3 credentials
    And an S3 bucket
    And the file "trades.csv" in S3 at "raw/date=2024-01-01/trades.csv"
    And the file "trades.csv" in S3 at "raw/date=2024-01-02/trades.csv"
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 's3://{bucket}/raw/date=2024-01-01/trades.csv'
      """
    Then the output is:
      """
      n
      3
      """
    When I run brrrrr sql:
      """
      SELECT date, sum(size) AS volume FROM 's3://{bucket}/raw/' GROUP BY date ORDER BY date
      """
    Then the output is:
      """
      date,volume
      2024-01-01,2.75
      2024-01-02,2.75
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 's3://{bucket}/raw/*/trades.csv'
      """
    Then the output is:
      """
      n
      6
      """

  Scenario: S3: a WHERE on Hive partitions fetches and reads only their objects
    Given S3 credentials
    And an S3 bucket
    And "trades.csv" written as "trades.parquet"
    And a file "broken.parquet" with:
      """
      not Parquet
      """
    And the file "trades.parquet" in S3 at "hist/date=2024-01-01/hour=9/trades.parquet"
    And the file "trades.parquet" in S3 at "hist/date=2024-01-02/hour=9/trades.parquet"
    And the file "trades.parquet" in S3 at "hist/date=2024-01-02/hour=10/trades.parquet"
    And the file "broken.parquet" in S3 at "hist/date=2024-01-03/hour=9/trades.parquet"
    When I run brrrrr sql:
      """
      SELECT date, hour, sum(size) AS volume FROM 's3://{bucket}/hist/'
      WHERE date >= '2024-01-02' AND date < '2024-01-03' OR hour = 9 AND date = '2024-01-01'
      GROUP BY date, hour ORDER BY date, hour
      """
    Then the output is:
      """
      date,hour,volume
      2024-01-01,9,2.75
      2024-01-02,9,2.75
      2024-01-02,10,2.75
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 's3://{bucket}/hist/'
      """
    Then it fails with "date=2024-01-03/hour=9/trades.parquet: not a Parquet file"

  Scenario: S3: a WHERE fixing the leading partitions lists only their directories
    Years of data cost one small listing. Here the hours of the days not asked for are not
    even listed: one of them is not a number, which would make `hour` text.
    Given S3 credentials
    And an S3 bucket
    And "trades.csv" written as "trades.parquet"
    And the file "trades.parquet" in S3 at "hist/date=2024-01-01/hour=9/trades.parquet"
    And the file "trades.parquet" in S3 at "hist/date=2024-01-02/hour=9/trades.parquet"
    And the file "trades.parquet" in S3 at "hist/date=2024-01-02/hour=10/trades.parquet"
    And the file "trades.parquet" in S3 at "hist/date=2024-01-03/hour=x/trades.parquet"
    When I run brrrrr sql:
      """
      SELECT date, hour, count(*) AS n FROM 's3://{bucket}/hist/'
      WHERE date IN ('2024-01-01', '2024-01-02') AND hour >= 9 GROUP BY date, hour ORDER BY date, hour
      """
    Then the output is:
      """
      date,hour,n
      2024-01-01,9,3
      2024-01-02,9,3
      2024-01-02,10,3
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 's3://{bucket}/hist/' WHERE hour >= 9
      """
    Then it fails with "compares a string with a number"

  Scenario Outline: S3: COPY writes <format>, and it reads back the same
    Given S3 credentials
    And an S3 bucket
    When I run brrrrr sql:
      """
      COPY (SELECT time_bucket('1m', ts) AS minute, symbol, sum(size) AS volume FROM 'trades.csv' GROUP BY minute, symbol)
      TO 's3://{bucket}/bars/bars.<format>'
      """
    Then it says "3 rows written to s3://{bucket}/bars/bars.<format>"
    When I run brrrrr sql:
      """
      SELECT * FROM 's3://{bucket}/bars/bars.<format>' ORDER BY minute, symbol
      """
    Then the output is:
      """
      minute,symbol,volume
      2024-01-01 00:00:00,BTC,0.5
      2024-01-01 00:00:00,ETH,2
      2024-01-01 00:01:00,BTC,0.25
      """

    Examples:
      | format  |
      | parquet |
      | csv     |
      | json    |

  Scenario: S3: a changed object is read anew, not from the cache
    Given S3 credentials
    And an S3 bucket
    And the file "trades.csv" in S3 at "t.csv"
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 's3://{bucket}/t.csv'
      """
    Then the output is:
      """
      n
      3
      """
    Given a file "more.csv" with:
      """
      ts,symbol,price,size
      2024-01-01T00:00:10,BTC,42000,0.5
      """
    And the file "more.csv" in S3 at "t.csv"
    When I run brrrrr sql:
      """
      SELECT count(*) AS n FROM 's3://{bucket}/t.csv'
      """
    Then the output is:
      """
      n
      1
      """

  Scenario: S3: wrong credentials are refused, and the error says what to check
    Given S3 credentials
    And an S3 bucket
    And the file "trades.csv" in S3 at "t.csv"
    And the S3 secret is "not-the-secret"
    When I run brrrrr sql:
      """
      SELECT * FROM 's3://{bucket}/t.csv'
      """
    Then it fails with "403"
    And it fails with "hint: check the store's credentials"

  Scenario: S3: an endpoint nobody answers fails fast and says so
    Given S3 credentials
    And the S3 endpoint is "http://127.0.0.1:9"
    When I run brrrrr sql:
      """
      SELECT * FROM 's3://somewhere/t.csv'
      """
    Then it fails with "the store is not reachable"

  Scenario: S3: no files where the glob looks
    Given S3 credentials
    And an S3 bucket
    When I run brrrrr sql:
      """
      SELECT * FROM 's3://{bucket}/none/*.parquet'
      """
    Then it fails with "no files at s3://{bucket}/none/*.parquet"

  Scenario: GCS: read a file and a glob
    Given a GCS bucket
    And the file "trades.csv" in GCS at "daily/2024-01-01.csv"
    And the file "trades.csv" in GCS at "daily/2024-01-02.csv"
    When I run brrrrr sql:
      """
      SELECT symbol, count(*) AS n FROM 'gs://{bucket}/daily/*.csv' GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,n
      BTC,4
      ETH,2
      """

  Scenario: Azure: write a result and read it back
    Given an Azure container
    And the file "trades.csv" in Azure at "raw/trades.csv"
    When I run brrrrr sql:
      """
      COPY (SELECT symbol, max(price) AS high FROM 'az://{bucket}/raw/trades.csv' GROUP BY symbol) TO 'az://{bucket}/out/high.parquet'
      """
    Then it says "2 rows written to az://{bucket}/out/high.parquet"
    When I run brrrrr sql:
      """
      SELECT * FROM 'az://{bucket}/out/' ORDER BY symbol
      """
    Then the output is:
      """
      symbol,high
      BTC,42010
      ETH,2300
      """

  Scenario: S3: COPY with PARTITION_BY writes Hive partitions, and they read back
    Given S3 credentials
    And an S3 bucket
    When I run brrrrr sql:
      """
      COPY (SELECT strftime(ts, '%Y-%m-%d') AS date, CASE WHEN size > 1 THEN 'big lots' ELSE 'small' END AS lot, * FROM 'trades.csv')
      TO 's3://{bucket}/archive' (FORMAT parquet, PARTITION_BY (date, lot))
      """
    Then it says "3 rows written to s3://{bucket}/archive, in 2 partitions"
    When I run brrrrr sql:
      """
      SELECT date, lot, symbol, count(*) AS n FROM 's3://{bucket}/archive/' WHERE date = '2024-01-01'
      GROUP BY date, lot, symbol ORDER BY lot
      """
    Then the output is:
      """
      date,lot,symbol,n
      2024-01-01,big lots,ETH,1
      2024-01-01,small,BTC,2
      """
    When I run brrrrr sql:
      """
      COPY (SELECT 'x' AS k, 1 AS v) TO 's3://{bucket}/archive' (PARTITION_BY (k))
      """
    Then it fails with "archive is not empty: write into it with OVERWRITE_OR_IGNORE"

  Scenario: Azure: COPY with PARTITION_BY writes Hive partitions, and they read back
    Given an Azure container
    When I run brrrrr sql:
      """
      COPY (SELECT strftime(ts, '%Y-%m-%d') AS date, CASE WHEN size > 1 THEN 'big lots' ELSE 'small' END AS lot, * FROM 'trades.csv')
      TO 'az://{bucket}/archive' (FORMAT parquet, PARTITION_BY (date, lot))
      """
    Then it says "3 rows written to az://{bucket}/archive, in 2 partitions"
    When I run brrrrr sql:
      """
      SELECT date, lot, symbol, count(*) AS n FROM 'az://{bucket}/archive/' WHERE date = '2024-01-01'
      GROUP BY date, lot, symbol ORDER BY lot
      """
    Then the output is:
      """
      date,lot,symbol,n
      2024-01-01,big lots,ETH,1
      2024-01-01,small,BTC,2
      """
    When I run brrrrr sql:
      """
      COPY (SELECT 'x' AS k, 1 AS v) TO 'az://{bucket}/archive' (PARTITION_BY (k))
      """
    Then it fails with "archive is not empty: write into it with OVERWRITE_OR_IGNORE"

  Scenario: S3: Delta Lake and Iceberg tables
    Given S3 credentials
    And an S3 bucket
    And the table "delta_trades" from the fixtures
    And the table "iceberg_trades" from the fixtures
    And the directory "delta_trades" in S3 at "lake/trades_delta"
    And the directory "iceberg_trades" in S3 at "lake/trades_iceberg"
    When I run brrrrr sql:
      """
      SELECT date, count(*) AS n FROM delta_scan('s3://{bucket}/lake/trades_delta') WHERE date >= '2024-01-02' GROUP BY date ORDER BY date
      """
    Then the output is:
      """
      date,n
      2024-01-02,2
      2024-01-03,1
      """
    When I run brrrrr sql:
      """
      SELECT symbol, count(*) AS n, max(price) AS high FROM iceberg_scan('s3://{bucket}/lake/trades_iceberg') GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,n,high
      A,3,102
      B,2,51
      """

  Scenario: S3: a Databento DBN file
    Given S3 credentials
    And an S3 bucket
    And the fixture "dbn/xnas-itch-20240102.trades.dbn.zst"
    And the file "xnas-itch-20240102.trades.dbn.zst" in S3 at "databento/xnas-itch-20240102.trades.dbn.zst"
    When I run brrrrr sql:
      """
      SELECT symbol, count(*) AS trades, sum(size) AS volume FROM 's3://{bucket}/databento/*.dbn.zst' GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,trades,volume
      AAPL,3,325
      MSFT,2,60
      """

  Scenario: S3: a key that is not there: the closest one there
    Given S3 credentials
    And an S3 bucket
    And the file "trades.csv" in S3 at "raw/trades.csv"
    When I run brrrrr sql:
      """
      SELECT count(*) FROM 's3://{bucket}/raw/trade.csv'
      """
    Then it fails with "no files at s3://{bucket}/raw/trade.csv: did you mean s3://{bucket}/raw/trades.csv?"

  Scenario: S3: a large Parquet file is read by the byte ranges a query needs
    Its footer, then the column chunks of the columns read (a file of 16 MiB or more; here every
    one, to read a small one so), not the whole file: the same answer.
    Given S3 credentials
    And an S3 bucket
    And "trades.csv" written as "trades.parquet"
    And the file "trades.parquet" in S3 at "big/trades.parquet"
    And the environment variable BRRRRR_RANGED_FROM is "0"
    When I run brrrrr sql:
      """
      SELECT symbol, count(*) AS n, sum(size) AS volume FROM 's3://{bucket}/big/trades.parquet' GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,n,volume
      BTC,2,0.75
      ETH,1,2
      """
    When I run brrrrr sql:
      """
      SELECT count(*) AS n, max(price) AS high FROM 's3://{bucket}/big/trades.parquet' WHERE symbol = 'BTC'
      """
    Then the output is:
      """
      n,high
      2,42010
      """

  Scenario: HTTP(S): a file at a URL
    Given "trades.csv" written as "pub/trades.parquet"
    And the directory is served over HTTP
    When I run brrrrr sql:
      """
      SELECT symbol, sum(size) AS volume FROM '{http}/pub/trades.parquet' GROUP BY symbol ORDER BY symbol
      """
    Then the output is:
      """
      symbol,volume
      BTC,0.75
      ETH,2
      """

  Scenario: HTTP(S): a URL with nothing there
    Given the directory is served over HTTP
    When I run brrrrr sql:
      """
      SELECT * FROM '{http}/nothing.csv'
      """
    Then it fails with "nothing.csv"
