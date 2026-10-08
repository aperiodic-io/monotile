Feature: Checkpoints are written while the pipeline runs on
  A checkpoint's cut copies the engine state and the source positions between two batches. The
  rest (waiting for the sinks to ack what was produced before the cut, encoding, the write)
  happens on a thread of its own while the data loop goes on consuming and producing.
  A checkpoint still describes one cut (ADR-0007): its sink offsets cover what was produced
  before it and nothing after it, so a restart from it neither loses nor duplicates a window.
  While a write is in flight nothing is produced under a lease older than --takeover / 2, a lease
  lost to another instance still stops this one, and SIGTERM's last checkpoint comes after the
  one being written.

  Background:
    Given a Redpanda broker
    And the topic "test.1m" with 3 partitions

  # heavy.sql keeps megabytes of quantile samplers, as market-data pipelines do: copying them
  # takes a fraction of encoding and writing them
  Scenario: A large state's checkpoints hold the data loop up only to copy the state
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/heavy.sql"
    And 80000 more trades are produced
    Then its checkpoints of over 1 MB held the data loop up for less than a quarter of their duration
    When the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And its log says "writing checkpoint" never

  Scenario: Killing brrrrr with a large state at any time, mid-checkpoint too, neither loses nor duplicates a message
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/heavy.sql" over 60000 trades, killed 5 times
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: With aligned consumption, catching up on a large state checkpointed on the way loses and repeats no window
    Given the topic "raw.trades.test" with 2 partitions
    And 60000 trades are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/heavy.sql" with "--max-drift 0"
    And the closing trade is produced
    And brrrrr has read every trade
    Then "test.1m" holds exactly the engine's messages for those trades
    And its metrics say "brrrrr_late_events_total 0"

  # trades two days old leave a partition at its end holding nobody back: the live edge reads
  # in arrival order, so which rows are late depends on it, and the windows are not fixed
  Scenario: With aligned consumption and a large state, killing brrrrr at any time never writes a window twice
    Given the topic "raw.trades.test" with 2 partitions
    And brrrrr runs with "--max-drift 0"
    When brrrrr runs "tests/acceptance/sql/heavy.sql" over 40000 trades, killed 3 times
    Then "test.1m" holds each window at most once, all of them windows the engine emits

  Scenario: SIGTERM with a large state takes the last checkpoint after the one being written, so the restart has nothing to repeat
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/heavy.sql"
    And 60000 more trades are produced
    And brrrrr is stopped with SIGTERM
    Then it stopped cleanly after a last checkpoint
    When brrrrr is started again on "tests/acceptance/sql/heavy.sql"
    Then its log says "was released by its instance: taking over"
    And its log says ": 0 messages already written will be suppressed"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: On SIGTERM a standing-by instance takes a large state over at once, writing nothing twice
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/heavy.sql"
    And 30000 more trades are produced
    And a second brrrrr is started on "tests/acceptance/sql/heavy.sql"
    And 30000 more trades are produced
    And brrrrr is stopped with SIGTERM
    Then it stopped cleanly after a last checkpoint
    And its log says "was released by its instance: taking over"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  # new source partitions are only assigned at start: brrrrr restarts from its checkpoint
  Scenario: A source topic that gains a partition makes brrrrr restart from its checkpoint to consume it
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And the topic "raw.trades.test" grows to 2 partitions
    Then within 30 seconds it stops with "raw.trades.test grew to 2 partitions; restarting from checkpoint"
    And its log says "writing checkpoint" never

  # Through a proxy that can hold every request the first instance sends its S3 store: its
  # checkpoint writes hang, as on a store slow to answer, while Kafka works on. --takeover 40
  # puts the fence 20 s past the last checkpoint, beyond the hang
  @object-store
  Scenario: While its checkpoint write hangs, brrrrr goes on consuming and writing windows
    Given the topic "raw.trades.test" with 1 partition
    And its checkpoints go to the S3 store through a proxy
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--takeover 40"
    And 1000 more trades are produced
    And the S3 store holds every request
    And 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" already holds every window those trades close
    And no checkpoint was written while the S3 store held its requests
    And its log says "producing nothing until one is written" never
    When the S3 store answers again
    Then within 10 seconds it writes a checkpoint again
    And "test.1m" holds exactly the engine's messages for those trades
    When brrrrr is killed
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "restored checkpoint"
    And "test.1m" holds exactly the engine's messages for those trades

  @object-store
  Scenario: A checkpoint write that hangs past the fence pauses producing until it lands, losing and repeating nothing
    Given the topic "raw.trades.test" with 1 partition
    And its checkpoints go to the S3 store through a proxy
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And the S3 store holds every request
    And 2000 more trades are produced
    And the closing trade is produced, its windows not waited for
    Then its log says "producing nothing until one is written"
    And "test.1m" holds fewer windows than those trades close
    And no checkpoint was written while the S3 store held its requests
    And it is still running
    When the S3 store answers again
    Then within 15 seconds it writes a checkpoint again
    And "test.1m" holds exactly the engine's messages for those trades
    And it is still running

  @object-store
  Scenario: Killed while its checkpoint write hangs, brrrrr restarts from a checkpoint and writes nothing twice
    Given the topic "raw.trades.test" with 1 partition
    And its checkpoints go to the S3 store through a proxy
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--takeover 40"
    And 1000 more trades are produced
    And the S3 store holds every request
    And 1000 more trades are produced
    And brrrrr is killed
    And the S3 store answers again
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "restored checkpoint"
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  @object-store
  Scenario: SIGTERM while its checkpoint write hangs waits for it, then takes a last checkpoint and releases the pipeline
    Given the topic "raw.trades.test" with 1 partition
    And its checkpoints go to the S3 store through a proxy
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--takeover 40"
    And 1000 more trades are produced
    And the S3 store holds every request
    And 1000 more trades are produced
    And brrrrr is sent SIGTERM
    And 3 seconds pass
    Then it is still running
    When the S3 store answers again
    Then within 30 seconds it stops cleanly after a last checkpoint
    When brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "was released by its instance: taking over"
    And its log says ": 0 messages already written will be suppressed"
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  # the second instance reaches the store directly: only the first one's writes hang
  @object-store
  Scenario: An instance whose checkpoint write hangs past --takeover yields to the one standing by, writing nothing twice
    Given the topic "raw.trades.test" with 1 partition
    And its checkpoints go to the S3 store through a proxy
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And a second brrrrr is started on "tests/acceptance/sql/windows.sql"
    Then its log says "standing by"
    When the S3 store holds every request
    Then its log says "taking over"
    And its log says "claimed the pipeline" 2 times
    When 1000 more trades are produced
    And the S3 store answers again
    Then within 30 seconds it stops with "another instance is running this pipeline"
    When the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  # the growth is found after a checkpoint (the bookkeeper asks then) and acted on at the next
  # one, here the one in flight when SIGTERM comes: the last checkpoint still follows it, and the
  # restart consumes the new partition (found in review)
  @object-store
  Scenario: SIGTERM while a checkpoint is in flight after a source gained a partition still stops cleanly after a last checkpoint
    Given the topic "raw.trades.test" with 1 partition
    And its checkpoints go to the S3 store through a proxy
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--interval 8 --takeover 40"
    And 1000 more trades are produced
    And the topic "raw.trades.test" grows to 2 partitions
    And brrrrr writes a checkpoint
    And 1 seconds pass
    And the S3 store holds every request
    And 9 seconds pass
    And brrrrr is sent SIGTERM
    And 2 seconds pass
    And the S3 store answers again
    Then within 30 seconds it stops cleanly after a last checkpoint
    And its log says "grew to" never
    When brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "was released by its instance: taking over"
    And its log says ": 0 messages already written will be suppressed"

  # several views' windows closed at once on threads of their own (--close-threads): the
  # 5m boundaries close both views of two_intervals.sql in one batch
  Scenario: With windows closed on 4 threads, killing brrrrr at any time neither loses nor duplicates a message
    Given the topic "raw.trades.test" with 1 partition
    And the sink topic "test.v1.test.1m" with 1 partition
    And the sink topic "test.v1.test.5m" with 1 partition
    And brrrrr runs with "--close-threads 4 --close-groups 0"
    When brrrrr runs "tests/acceptance/sql/two_intervals.sql" over 20000 trades, killed 3 times
    Then "test.v1.test.1m" holds exactly the engine's messages to it for those trades
    And "test.v1.test.5m" holds exactly the engine's messages to it for those trades
    And its last metrics count closes on several threads

  Scenario: With windows closed on one thread only, killing brrrrr at any time neither loses nor duplicates a message
    Given the topic "raw.trades.test" with 1 partition
    And the sink topic "test.v1.test.1m" with 1 partition
    And the sink topic "test.v1.test.5m" with 1 partition
    And brrrrr runs with "--close-threads 1"
    When brrrrr runs "tests/acceptance/sql/two_intervals.sql" over 20000 trades, killed 3 times
    Then "test.v1.test.1m" holds exactly the engine's messages to it for those trades
    And "test.v1.test.5m" holds exactly the engine's messages to it for those trades
