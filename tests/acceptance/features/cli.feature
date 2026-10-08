Feature: Command line interface
  Operators and CI drive brrrrr through its CLI; its contract (exit codes,
  output) must be stable.

  Scenario: The binary reports its version
    When I run brrrrr with "--version"
    Then it exits successfully
    And its output contains "brrrrr 0.1.0"

  Scenario: Checkpoints go to the pod's volume unless told otherwise
    When I run brrrrr with "run --help"
    Then it exits successfully
    And its output contains "[env: BRRRRR_CHECKPOINTS=]"
    And its output contains "/var/lib/brrrrr/checkpoints]"

  Scenario: A checkpoint directory that cannot be written stops brrrrr at once, saying how to fix it
    When I run brrrrr with "run tests/acceptance/sql/windows.sql --proto fixtures/market.proto --brokers 127.0.0.1:1 --metrics 127.0.0.1:0 --checkpoints /dev/null/brrrrr"
    Then it exits with an error
    And its output contains "checkpoint directory /dev/null/brrrrr/windows/"
    And its output contains "is not writable"
    And its output contains "securityContext.fsGroup"

  Scenario: The checkpoint directory comes from BRRRRR_CHECKPOINTS, as a pod sets it
    When I run brrrrr with "run tests/acceptance/sql/windows.sql --proto fixtures/market.proto --brokers 127.0.0.1:1 --metrics 127.0.0.1:0" and BRRRRR_CHECKPOINTS="/dev/null/from-env"
    Then it exits with an error
    And its output contains "checkpoint directory /dev/null/from-env/windows/"

  Scenario: The default build refuses an object store, pointing at the -extended image
    When I run brrrrr with "run tests/acceptance/sql/windows.sql --proto fixtures/market.proto --brokers 127.0.0.1:1 --metrics 127.0.0.1:0 --checkpoints s3://bucket/checkpoints"
    Then its output says whether this build has the object store
