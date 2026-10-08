Feature: The command line
  `brrrrr sql` runs statements from its argument, a file or standard input, and writes results
  as a table, CSV, JSON lines or Parquet.

  Background:
    Given a file "t.csv" with:
      """
      symbol,price
      BTC,42000
      ETH,2300
      """

  Scenario: CSV by default when the output is not a terminal
    When I run "brrrrr sql" on:
      """
      SELECT * FROM 't.csv' ORDER BY price
      """
    Then the output is:
      """
      symbol,price
      ETH,2300
      BTC,42000
      """

  Scenario: Output into a pipe that closes early ends quietly, as head expects
    Given the cookbook's files
    And a file "all.sql" with:
      """
      SELECT * FROM 'quotes.csv'
      """
    When I run "brrrrr sql -f all.sql" and read only its first line
    Then the output is:
      """
      ts,symbol,bid,ask,bid_size,ask_size
      """
    And it says nothing

  Scenario: A table, as the shell shows it
    When I run "brrrrr sql --format table" on:
      """
      SELECT * FROM 't.csv'
      """
    Then the output is:
      """
      ┌────────┬───────┐
      │ symbol │ price │
      ├────────┼───────┤
      │ BTC    │ 42000 │
      │ ETH    │  2300 │
      └────────┴───────┘
      2 rows
      """

  Scenario: JSON lines
    When I run "brrrrr sql --format json" on:
      """
      SELECT * FROM 't.csv'
      """
    Then the output is:
      """
      {"symbol":"BTC","price":42000}
      {"symbol":"ETH","price":2300}
      """

  Scenario: -o writes the result to a file, in the format its name says
    When I run "brrrrr sql -o out.parquet" on:
      """
      SELECT * FROM 't.csv'
      """
    Then it says "2 rows written to out.parquet"
    When I run "brrrrr sql" on:
      """
      SELECT count(*) AS n FROM 'out.parquet'
      """
    Then the output is:
      """
      n
      2
      """

  Scenario: --table names a location for the statements
    When I run "brrrrr sql --table prices=t.csv" on:
      """
      SELECT max(price) AS top FROM prices
      """
    Then the output is:
      """
      top
      42000
      """

  Scenario: Statements from standard input, several in a row
    When I run "brrrrr sql" with the input:
      """
      CREATE VIEW cheap AS SELECT * FROM 't.csv' WHERE price < 10000;
      -- a comment; with a semicolon
      SELECT symbol FROM cheap;
      """
    Then the output is:
      """
      symbol
      ETH
      """

  Scenario: Statements from a file
    Given a file "report.sql" with:
      """
      SELECT count(*) AS n FROM 't.csv';
      """
    When I run "brrrrr sql -f report.sql"
    Then the output is:
      """
      n
      2
      """

  Scenario: A failing statement fails the command, with the reason
    When I run "brrrrr sql" on:
      """
      SELECT nope FROM 't.csv'
      """
    Then it fails with "unknown column nope"
    And it fails with "has symbol, price"

  Scenario: The version
    When I run "brrrrr --version"
    Then the output contains "brrrrr"
