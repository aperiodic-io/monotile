Feature: Pipelines read and write Parquet files
  A file stream (type = 'file', data_format = 'Parquet') is a source if a view reads it and a
  sink if a view writes it. A source reads its files in name order and goes on with the files
  that appear; a sink writes the rows of each checkpoint interval to files of their own, in
  place before the checkpoint is. Either way, killed at any time, a pipeline writes each row
  once.

  Background:
    Given a Redpanda broker

  Scenario: Trades from Kafka to Parquet files, killed 3 times, each window once
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/windows_to_parquet.sql" into Parquet files over 20000 trades, killed 3 times
    Then the Parquet files of "test.1m" hold exactly the engine's rows for those trades

  Scenario: Trades from Parquet files as they appear to Kafka, killed 3 times, each window once
    Given the topic "test.1m" with 3 partitions
    When brrrrr runs "tests/acceptance/sql/parquet_to_windows.sql" over 20000 trades in Parquet files, killed 3 times
    Then "test.1m" holds exactly the engine's messages for those trades
