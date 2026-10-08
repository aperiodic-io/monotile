Feature: Errors that say what to do
  What brrrrr cannot run is refused with the reason and what to write instead, never run as
  something else.

  Background:
    Given a file "t.csv" with:
      """
      ts,symbol,price
      2024-01-01T00:00:00,A,1
      """

  Scenario Outline: Refusals name the fix
    When I run brrrrr sql:
      """
      <sql>
      """
    Then it fails with "<says>"

    Examples:
      | sql                                                                                   | says                                         |
      | SELECT * FROM missing_table                                                           | no table missing_table                       |
      | SELECT nope FROM 't.csv'                                                              | unknown column nope                          |
      | SELECT frobnicate(price) FROM 't.csv'                                                 | unknown function frobnicate                  |
      | SELECT * FROM 't.csv' a, 't.csv' b                                                    | write a JOIN                                 |
      | SELECT * FROM (SELECT * FROM 't.csv' LIMIT 1)                                         | limit the outermost query                    |
      | SELECT * FROM 't.csv' a ASOF JOIN 't.csv' b ON a.symbol = b.symbol AND a.ts > b.price  | b.price must be a time column                |
      | SELECT * FROM 't.csv' a ASOF JOIN 't.csv' b ON a.symbol = b.symbol                    | ASOF JOIN needs the time                     |
      | SELECT sum(price) OVER (PARTITION BY symbol) FROM 't.csv'                             | the whole partition's total                  |
      | SELECT time_bucket('1 fortnight', ts) AS b, count(*) FROM 't.csv' GROUP BY b          | is not a duration                            |
      | VACUUM                                                                                | brrrrr runs SELECT                           |

  Scenario: A table or file that is not there: the closest one there
    Given a file "data/trades.csv" with:
      """
      ts,symbol,price
      2024-01-01T00:00:00,A,1
      """
    And a file "quotes.csv" with:
      """
      ts,symbol,bid
      2024-01-01T00:00:00,A,1
      """
    When I run brrrrr sql:
      """
      SELECT * FROM qoutes
      """
    Then it fails with "no table qoutes: did you mean quotes?"
    When I run brrrrr sql:
      """
      SELECT * FROM 'data/trade.csv'
      """
    Then it fails with "no file data/trade.csv: did you mean data/trades.csv?"
    When I run brrrrr sql:
      """
      CREATE TABLE trades AS 'data/trades.csv'; SELECT * FROM trdes
      """
    Then it fails with "no table trdes: did you mean trades?"
