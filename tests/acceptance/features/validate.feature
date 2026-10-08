Feature: Validating a pipeline's SQL
  Operators check a pipeline's SQL file before deploying it: every view must compile,
  and the Kafka sources and sinks it will touch are listed.

  Scenario Outline: Every example pipeline validates
    When I run brrrrr with "validate fixtures/pipelines/<pipeline>.sql"
    Then it exits successfully
    And its output contains "<summary>"

    Examples:
      | pipeline    | summary                               |
      | bars        | 7 views run, 1 skipped (S3 exports)   |
      | book        | 4 views run, 0 skipped (S3 exports)   |
      | derivatives | 13 views run, 0 skipped (S3 exports)  |
      | flow        | 7 views run, 0 skipped (S3 exports)   |
      | quotes      | 7 views run, 0 skipped (S3 exports)   |
      | returns     | 5 views run, 0 skipped (S3 exports)   |
      | state       | 9 views run, 0 skipped (S3 exports)   |
      | stats       | 6 views run, 0 skipped (S3 exports)   |

  Scenario: Sources and sinks are listed with their topics
    When I run brrrrr with "validate fixtures/pipelines/quotes.sql"
    Then its output contains "spread_out -> quotes.spread"
    And its output contains "quotes <- quotes"

  Scenario: SQL that cannot run is refused with the view that needs it
    When I run brrrrr with "validate tests/acceptance/sql/unsupported.sql"
    Then it exits with an error
    And its output contains "error: v: only ASOF LEFT JOIN is supported"

  Scenario: A syntax error is located
    When I run brrrrr with "validate tests/acceptance/sql/broken.sql"
    Then it exits with an error
    And its output contains "broken.sql:2:"

  # both once validated, and the second wrote x as it came
  Scenario Outline: SQL the engine would not run as written is refused, not run otherwise
    When I run brrrrr with "validate tests/acceptance/sql/<file>.sql"
    Then it exits with an error
    And its output contains "<error>"

    Examples:
      | file     | error                                                     |
      | trailing | trailing.sql:3:80: unexpected totally_invalid_tokens      |
      | replace  | error: v: unsupported select item * REPLACE (99 AS x)     |
