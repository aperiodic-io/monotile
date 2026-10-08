Feature: Running a pipeline against Kafka
  brrrrr consumes the raw protobuf (or JSON) topics, runs the pipeline's views and produces each
  window's JSONEachRow message, with its redpanda-dedup-key header, exactly once: a crash at any
  point may neither lose nor duplicate a message (ADR-0007).

  Background:
    Given a Redpanda broker
    And the topic "test.1m" with 3 partitions

  Scenario: Window messages reach the sink topic with their dedup header
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 3000 trades
    Then "test.1m" holds exactly the engine's messages for those trades
    And its metrics count 3001 received and 350 sent messages
    And its health check passes
    And its last metrics say "brrrrr_window_groups_closed_total 350"
    And its last metrics say "brrrrr_batch_closed_groups_sum 350"
    And its last metrics say "brrrrr_batch_emitted_messages_sum 350"
    And its last metrics count its batches

  # An absent protobuf field is NULL in a nullable column: a sort on it used to panic,
  # and brrrrr restarted, replayed the same trades and panicked again
  Scenario: Trades whose sort key is NULL are sorted, written and brrrrr keeps running
    Given the topic "raw.trades.test" with 1 partition
    And the topic "test.sorted" with 3 partitions
    When brrrrr runs "tests/acceptance/sql/sorted.sql" over 3000 trades, every 7th without a price
    Then "test.sorted" holds exactly the engine's messages to it for those trades
    And "test.1m" holds exactly the engine's messages to it for those trades
    And its metrics count 3001 received and 3351 sent messages
    And its health check passes

  # JSON in and out, as users run it: no --proto, no dedup header (a message read back from the
  # sink is known by its payload)
  Scenario: JSON trades become JSON bars
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/json.sql" over 3000 trades
    Then "test.1m" holds exactly the engine's messages for those trades
    And its metrics count 3001 received and 350 sent messages
    And its last metrics say "brrrrr_decode_errors_total 0"

  Scenario: Killing brrrrr at any time neither loses nor duplicates a JSON bar
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/json.sql" over 12000 trades, killed 2 times
    Then "test.1m" holds exactly the engine's messages for those trades

  # examples/pipelines/bars.sql's view as `brrrrr sql` takes it (time_bucket, first/last, WHERE):
  # what the pipeline writes live is what the same query answers over the same trades in a file
  Scenario: A view in brrrrr sql's dialect writes the bars brrrrr sql answers over the same trades
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/adhoc.sql" over 3000 trades
    Then "test.1m" holds exactly the engine's messages for those trades
    And "test.1m" holds what brrrrr sql answers for its view over those trades in a file
    And its last metrics say "brrrrr_decode_errors_total 0"

  Scenario: Killing brrrrr at any time neither loses nor duplicates a bar of a view in brrrrr sql's dialect
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/adhoc.sql" over 12000 trades, killed 2 times
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: Killing brrrrr at any time neither loses nor duplicates a message
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 30000 trades, killed 6 times
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: A restart without any checkpoint does not repeat what is already written
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 3000 trades
    And its checkpoints are lost and it runs again
    Then "test.1m" holds exactly the engine's messages for those trades
    And its log says "the replay caught up: 0 messages it did not write again are no longer suppressed"

  Scenario: Without a checkpoint, windows written within the sources' retention are not written again
    Given the topic "raw.trades.test" with 1 partition and "retention.ms=259200000"
    And 3000 trades are waiting in the source
    And "test.1m" already holds the windows those trades close, written 60 hours ago
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 3000 more trades are produced
    And the closing trade is produced
    And brrrrr has read every trade
    Then its log says "the sources keep 72h: reading the sinks back that far (--lookback 48h)"
    And "test.1m" holds exactly the engine's messages for those trades

  Scenario: Without a checkpoint, a source that keeps its data forever has the sinks read from their start
    Given the topic "raw.trades.test" with 1 partition and "retention.ms=-1"
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 3000 trades
    Then its log says "the sinks are read back from their start"
    And "test.1m" holds exactly the engine's messages for those trades

  Scenario: SIGTERM takes a last checkpoint, so the restart has nothing to repeat
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 2000 more trades are produced
    And brrrrr is stopped with SIGTERM
    Then it stopped cleanly after a last checkpoint
    When brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says ": 0 messages already written will be suppressed"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: A sink whose last offsets hold another writer's aborted transaction is read back to its end
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 2000 more trades are produced
    And brrrrr is stopped with SIGTERM
    And another writer aborts a transaction on "test.1m"
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "claimed the pipeline"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And its log says "reading the sinks back made no progress" never

  Scenario: Checkpoints on the pod's volume survive the pod moving with its volume
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 2000 more trades are produced
    And 7 seconds pass
    Then its volume holds the checkpoints of "windows" and nothing else
    When brrrrr is killed
    And its volume moves to another place
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "restored checkpoint"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And its volume holds the checkpoints of "windows" and nothing else

  Scenario: A volume emptied under a running brrrrr is written again, and a restart resumes from it
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 2000 more trades are produced
    And 3 seconds pass
    And its volume is emptied
    And 3 seconds pass
    Then its volume holds the checkpoints of "windows" and nothing else
    And it is still running
    When brrrrr is killed
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "restored checkpoint"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: A newest checkpoint that does not restore is skipped for the one before it
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 2000 more trades are produced
    And brrrrr is stopped with SIGTERM
    And its newest checkpoint is corrupted
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "does not restore"
    And its log says "restored checkpoint"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: --fresh starts without a checkpoint, writes nothing twice and keeps checkpointing
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 2000 more trades are produced
    And brrrrr is killed
    And brrrrr is started again on "tests/acceptance/sql/windows.sql" with "--fresh"
    Then its log says "--fresh: not restoring any checkpoint"
    When 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And its log says "restored" never
    And its log says "another instance is running this pipeline" never

  Scenario: --restore-epoch restores an older checkpoint and writes nothing twice
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 2000 more trades are produced
    And brrrrr is stopped with SIGTERM
    And brrrrr is started again on "tests/acceptance/sql/windows.sql" from its oldest checkpoint
    And 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And its log says "another instance is running this pipeline" never

  Scenario: Checkpoints that cannot be written for a while neither stop brrrrr nor cost exactly-once
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And its checkpoints cannot be written
    And 1000 more trades are produced
    Then its log says "writing checkpoint"
    And its log says "producing nothing until one is written"
    When 5 seconds pass
    Then it is still running
    And its metrics count failed checkpoint writes
    When its checkpoints can be written again
    Then its log says "failed attempts"
    When 1000 more trades are produced
    And brrrrr is killed
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "restored checkpoint"
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And its log says "another instance is running this pipeline" never

  @object-store
  Scenario: With checkpoints in an S3 store, killing brrrrr at any time neither loses nor duplicates a message
    Given the topic "raw.trades.test" with 1 partition
    And its checkpoints go to the S3 store
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 30000 trades, killed 4 times
    Then "test.1m" holds exactly the engine's messages for those trades
    And its log says "checkpoints: s3://checkpoints/it-"
    And its log says "restored checkpoint"
    And its log says "writing checkpoint" never

  Scenario: A second instance stands by while the first runs and takes over when it dies, writing nothing twice
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And a second brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    Then its log says "standing by"
    When brrrrr is killed
    Then its log says "taking over"
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: On SIGTERM a standing-by instance takes over at once, writing nothing twice
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And a second brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And brrrrr is stopped with SIGTERM
    Then its log says "was released by its instance: taking over"
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: Of two instances started together, exactly one claims the pipeline
    Given the topic "raw.trades.test" with 1 partition
    When two brrrrr are started together on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    Then exactly one of them claimed the pipeline
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: A shadow instance and the live instance of one pipeline keep their own state
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And a shadow brrrrr is started on "tests/acceptance/sql/windows.sql" with the sink prefix "shadow."
    And 2000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And "shadow.test.1m" holds exactly the engine's messages for those trades
    And its log says "standing by" never

  # A changed SQL or --asof has state of its own, but the pipeline's sinks are the same: an
  # instance of the new revision deployed while the old one still runs stands by as one of the
  # same revision does
  Scenario: An instance of a changed SQL file stands by while the old one runs, writing nothing twice
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And a second brrrrr is started on a changed "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    Then its log says "standing by"
    When brrrrr is killed
    Then its log says "taking over"
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: An instance with another --asof stands by while the old one runs, writing nothing twice
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And a second brrrrr is started on "tests/acceptance/sql/windows.sql" with "--asof arrival"
    And 1000 more trades are produced
    Then its log says "standing by"
    When brrrrr is stopped with SIGTERM
    Then its log says "was released by its instance: taking over"
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: Of two revisions of a pipeline started together, exactly one claims it
    Given the topic "raw.trades.test" with 1 partition
    When two brrrrr are started together on "tests/acceptance/sql/windows.sql", one of them changed
    And 1000 more trades are produced
    Then exactly one of them claimed the pipeline
    When 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: A changed SQL file starts from scratch without repeating any window
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 3000 more trades are produced
    And the closing trade is produced
    And brrrrr is killed
    And the SQL file "tests/acceptance/sql/windows.sql" changes and brrrrr restarts on it
    Then its log says ": 350 messages already written will be suppressed"
    And "test.1m" holds exactly the engine's messages for those trades

  Scenario: Switching --asof starts from scratch without repeating any window
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 3000 more trades are produced
    And the closing trade is produced
    And brrrrr is killed
    And brrrrr is started again on "tests/acceptance/sql/windows.sql" with "--asof arrival"
    Then its log says ": 350 messages already written will be suppressed"
    And "test.1m" holds exactly the engine's messages for those trades
    And its log says "does not restore" never

  Scenario: Two source partitions, killed repeatedly, never write a window twice
    Given the topic "raw.trades.test" with 2 partitions
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 20000 trades, killed 4 times
    Then "test.1m" holds each window at most once, all of them windows the engine emits

  @stress
  Scenario: A long run with frequent crashes keeps every window exactly once
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 100000 trades, killed 10 times
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: With a sink topic prefix, brrrrr writes its own shadow topics
    Given the topic "raw.trades.test" with 1 partition
    And brrrrr writes its sinks with the prefix "shadow."
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 3000 trades, killed 1 times
    Then "shadow.test.1m" holds exactly the engine's messages for those trades
    And "test.1m" holds no message

  Scenario: With a sink topic template, several sinks share one topic, exactly once
    Given the topic "raw.trades.test" with 1 partition
    And brrrrr writes its sinks to "shadow.{1}.{2}"
    When brrrrr runs "tests/acceptance/sql/two_intervals.sql" over 3000 trades, killed 1 times
    Then "shadow.test.v1" holds exactly the engine's messages for those trades
    And "test.v1.test.1m" holds no message

  Scenario: Sources and sinks on separate brokers stay exactly once
    Given the topic "raw.trades.test" with 1 partition
    And a separate Redpanda broker for the sinks
    And the sink topic "test.1m" with 3 partitions
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 20000 trades, killed 3 times
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: Sinks on a cluster that requires SASL/SCRAM, killed repeatedly, stay exactly once
    Given the topic "raw.trades.test" with 1 partition
    And a separate Redpanda broker for the sinks
    And the sinks' broker requires SASL/SCRAM
    And the sink topic "test.1m" with 3 partitions
    When brrrrr runs "tests/acceptance/sql/windows.sql" over 5000 trades, killed 2 times
    Then "test.1m" holds exactly the engine's messages for those trades

  @fatal-producer
  Scenario: An anonymous writer denied InitProducerId reports the broker's fatal cause
    Given the topic "raw.trades.test" with 1 partition
    And a separate Redpanda broker at version "v26.2.1" for the sinks
    And the sink topic "test.1m" with 3 partitions
    And the sinks' broker permits anonymous topic reads but denies idempotent writes
    And 1000 trades are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    Then within 60 seconds it stops with "; producer fatal error ClusterAuthorizationFailed:"
    And its log says "Failed to acquire idempotence PID from broker"
    And its log says "Broker: Cluster authorization failed"
    And its log says "(permanent: the restart replays this message and it fails again"
    And "test.1m" holds no message

  Scenario: A message its sink refuses stops brrrrr, saying which and why
    Given the topic "raw.trades.test" with 1 partition
    And the topic "test.1m" takes messages of at most 100 bytes
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    Then within 60 seconds it stops with "test.1m/"
    And its log says "Message size too large"
    And its log says "(permanent: the restart replays this message and it fails again"

  Scenario: A row from the far future is dropped instead of stopping every window
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And a trade from the year 3000 is produced
    And 1000 more trades are produced
    And the closing trade is produced
    Then "test.1m" holds exactly the engine's messages for those trades
    And its metrics say "brrrrr_future_events_total 1"

  Scenario: With aligned consumption, catching up on two partitions loses no window
    Given the topic "raw.trades.test" with 2 partitions
    And 20000 trades are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--max-drift 0"
    And the closing trade is produced
    And brrrrr has read every trade
    Then "test.1m" holds exactly the engine's messages for those trades
    And its metrics say "brrrrr_late_events_total 0"
    And its log says "aligned consumption: 2 partitions, one at its end waited for 50 ms"

  # live, a partition is at its end most of the time: that alone must not let another partition
  # close a window whose last trades it has produced but brrrrr has not read yet
  Scenario: With aligned consumption, a partition at its end is waited for and its late trade reaches its window
    Given the topic "raw.trades.test" with 2 partitions
    And trades every 7 seconds from 30 minutes ago until now are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--max-drift 0 --partition-wait-ms 5000 --partial-windows"
    And partition 1 delivers a trade 2 seconds after partition 0 delivered the one that closes its window
    Then "test.1m" holds exactly the engine's messages for the recent trades
    And its metrics say "brrrrr_late_events_total 0"

  # the trades in the source before brrrrr starts: a quiet source closes its windows on the clock,
  # and trades of an earlier time that came only after that would be late
  Scenario: With --idle-close, a quiet stream's last windows close without a later row
    Given the topic "raw.trades.test" with 1 partition
    And 1000 trades are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--idle-close 2"
    Then "test.1m" holds exactly the engine's messages for those trades

  # a quiet feed's last windows close on the clock, in ms and with other partitions running:
  # --idle-close needs the whole pipeline silent for whole seconds, which a pipeline that also reads a busy feed never is
  # as above: a partition at its end vouches for the clock less the margin (`vouched_until`), and
  # trades of days ago produced only after brrrrr's first close were all late (seen on CI)
  Scenario: With --close-after-ms, a quiet stream's last windows close without a later row
    Given the topic "raw.trades.test" with 1 partition
    And 1000 trades are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--max-drift 0 --close-after-ms 500"
    Then "test.1m" holds exactly the engine's messages for those trades
    And its metrics say "brrrrr_late_events_total 0"

  # the margin outlasts a partition's delay, so its late trade still reaches the window the other partition's
  # trade would have closed on the clock (the lagging-partition scenario above, with --close-after-ms)
  Scenario: With --close-after-ms, a partition's late trade reaches its window while the margin outlasts its delay
    Given the topic "raw.trades.test" with 2 partitions
    And trades every 7 seconds from 30 minutes ago until now are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--max-drift 0 --partition-wait-ms 5000 --close-after-ms 8000 --partial-windows"
    And partition 1 delivers a trade 2 seconds after partition 0 delivered the one that closes its window
    Then "test.1m" holds exactly the engine's messages for the recent trades
    And its metrics say "brrrrr_late_events_total 0"

  # the case the clock close is for, with the traffic it is for: rows stamped now, on two partitions, and a
  # minute left quiet. Back-dated trades prove nothing here: only a row stamped now can be late for a clock
  Scenario: With --close-after-ms, live trades on two partitions lose no row and a quiet minute reaches the sink within seconds
    Given the topic "raw.trades.test" with 2 partitions
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--max-drift 0 --close-after-ms 500 --partial-windows"
    And live trades are produced every 40 ms on both partitions until 1 second before a minute ends
    Then the quiet minute reaches "test.1m" within 4 seconds of its end, as the engine writes it
    And its metrics say "brrrrr_late_events_total 0"

  Scenario: Without --close-after-ms the same quiet minute stays open
    Given the topic "raw.trades.test" with 2 partitions
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--max-drift 0 --partial-windows"
    And live trades are produced every 40 ms on both partitions until 1 second before a minute ends
    Then the quiet minute is not in "test.1m" 5 seconds after its end

  Scenario: With --close-after-ms, a restart without a checkpoint does not write the windows it closed on the clock again
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--max-drift 0 --close-after-ms 500"
    And 1000 more trades are produced
    Then "test.1m" holds exactly the engine's messages for those trades
    When brrrrr is killed
    And its checkpoints are lost
    And brrrrr is started again on "tests/acceptance/sql/windows.sql" with "--max-drift 0 --close-after-ms 500"
    Then its log says "the replay caught up"
    When 5 seconds pass
    Then "test.1m" holds exactly the engine's messages for those trades

  Scenario: With --idle-close, a restart without a checkpoint does not write the windows it idle-closed again
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--idle-close 2"
    And 1000 more trades are produced
    Then "test.1m" holds exactly the engine's messages for those trades
    When brrrrr is killed
    And its checkpoints are lost
    And brrrrr is started again on "tests/acceptance/sql/windows.sql" with "--idle-close 2"
    Then its log says "the replay caught up"
    When 5 seconds pass
    Then "test.1m" holds exactly the engine's messages for those trades

  # the windows an idle close emits once went to the SQL's topic, not the prefixed one: a shadow
  # wrote them into the live topic
  Scenario: With a sink topic prefix and --idle-close, idle-closed windows go to the prefixed topic too, exactly once
    Given the topic "raw.trades.test" with 1 partition
    And brrrrr writes its sinks with the prefix "shadow."
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--idle-close 2"
    And 1000 more trades are produced
    Then "shadow.test.1m" holds exactly the engine's messages for those trades
    And "test.1m" holds no message
    When brrrrr is killed
    And its checkpoints are lost
    And brrrrr is started again on "tests/acceptance/sql/windows.sql" with "--idle-close 2"
    Then its log says "the replay caught up"
    When 5 seconds pass
    Then "shadow.test.1m" holds exactly the engine's messages for those trades
    And "test.1m" holds no message

  Scenario: With a sink topic template and --idle-close, idle-closed windows go to the templated topic
    Given the topic "raw.trades.test" with 1 partition
    And brrrrr writes its sinks to "shadow.{1}.{2}"
    When brrrrr is started on "tests/acceptance/sql/two_intervals.sql" with "--idle-close 2"
    And 1000 more trades are produced
    Then "shadow.test.v1" holds exactly the engine's messages for those trades
    And "test.v1.test.1m" holds no message
    And "test.v1.test.5m" holds no message

  # a switch to brrrrr starts every pipeline without a checkpoint, over sources that may hold
  # more than its sink topics: the windows the sinks no longer hold must not be written again,
  # hours late, the oldest of them partial.

  Scenario: A start without a checkpoint writes no window its sink topic can no longer vouch for, and backfills those it can, per interval
    # the 1m topic kept the last hour, the 5m topic all of it but a window never written: the
    # replay writes none of the 1m windows the topic dropped, and the missing 5m one
    Given the topic "raw.trades.test" with 1 partition and "retention.ms=-1"
    And the sink topic "test.v1.test.1m" with 1 partition
    And the sink topic "test.v1.test.5m" with 1 partition
    And trades every 10 seconds from 180 minutes ago until now are waiting in the source of "tests/acceptance/sql/two_intervals.sql"
    And "test.v1.test.1m" already holds the windows the recent trades close, written as they closed
    And "test.v1.test.5m" already holds the windows the recent trades close but the one from 120 minutes ago, written as they closed
    And "test.v1.test.1m" lost its records older than 60 minutes to retention
    When brrrrr is started on "tests/acceptance/sql/two_intervals.sql"
    And the recent trades close their windows
    Then its log says "on test.v1.test.1m, the windows closed before"
    And "test.v1.test.1m" holds each window once, none that closed before it lost its records
    And "test.v1.test.5m" holds each of the engine's windows for the recent trades once
    And its metrics count withheld unverified windows

  Scenario: A start without a checkpoint withholds the windows whose start its sources no longer hold
    Given the topic "raw.trades.test" with 1 partition
    And trades every 7 seconds from 30 minutes ago until now are waiting in the source
    And the source's first 100 trades are gone with its retention
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And the recent trades close their windows
    Then brrrrr withheld the windows that began before the first trade
    And "test.1m" holds exactly the engine's messages for the recent trades, withholding what brrrrr withheld
    And its metrics count withheld partial windows

  Scenario: A source whose feed started after a window did withholds that window: its first candles are full
    # a new pipeline on a new feed (a cluster started afresh): the window its first record falls
    # in began before the feed, so it would be written from part of its trades
    Given the topic "raw.trades.test" with 1 partition
    And trades every 7 seconds from 30 minutes ago until now are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And the recent trades close their windows
    Then brrrrr withheld the windows that began before the first trade
    And "test.1m" holds exactly the engine's messages for the recent trades, withholding what brrrrr withheld
    And its metrics count withheld partial windows

  Scenario: Started before its source's first record, brrrrr waits for it and withholds the windows that began before it
    # a cluster reset: brrrrr may be up before the producers write anything
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And brrrrr has claimed the pipeline
    And trades every 7 seconds from 30 minutes ago until now are produced
    And the recent trades close their windows
    Then its log says "raw.trades.test started: withholding"
    And brrrrr withheld the windows that began before the first trade
    And "test.1m" holds exactly the engine's messages for the recent trades, withholding what brrrrr withheld

  Scenario: With --partial-windows, a start without a checkpoint writes the windows its sources hold only part of
    Given the topic "raw.trades.test" with 1 partition
    And trades every 7 seconds from 30 minutes ago until now are waiting in the source
    And the source's first 100 trades are gone with its retention
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--partial-windows"
    And the recent trades close their windows
    Then its log says "withholding" never
    And "test.1m" holds exactly the engine's messages for the recent trades

  Scenario: A restart after a start without a checkpoint keeps withholding the same windows
    Given the topic "raw.trades.test" with 1 partition
    And trades every 7 seconds from 30 minutes ago until now are waiting in the source
    And the source's first 100 trades are gone with its retention
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 5 seconds pass
    And brrrrr is killed
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    And the recent trades close their windows
    Then its log says "restored checkpoint"
    And its log says "withholding what a start without a checkpoint cannot write whole or once"
    And "test.1m" holds exactly the engine's messages for the recent trades, withholding what brrrrr withheld

  # a checkpoint is no complete recovery once retention deleted what came after it
  # not fatal: the instance goes on from what the source holds, says so and counts it for an alert
  Scenario: A checkpoint whose source lost the records after it is resumed past the gap, which is reported
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql"
    And 1000 more trades are produced
    And brrrrr is stopped with SIGTERM
    And 1000 more trades are produced
    And the source lost its records before offset 1500 to retention
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    Then its log says "but it starts at 1500: retention deleted offsets 1000 to 1499: going on from offset 1500"
    And its metrics say "brrrrr_source_resume_gap_partitions 1"
    And its metrics say "brrrrr_source_resume_skipped_records_total 500"
    And it is still running

  Scenario: Restored after its sink lost what it wrote since the checkpoint, brrrrr writes none of it again
    # a checkpoint every minute: the oldest is the claim's, taken before any window was written
    Given the topic "raw.trades.test" with 1 partition
    When brrrrr is started on "tests/acceptance/sql/windows.sql" with "--interval 60"
    And 3000 more trades are produced
    And the closing trade is produced
    And brrrrr is stopped with SIGTERM
    And "test.1m" lost every record it holds to retention
    And brrrrr is started again on "tests/acceptance/sql/windows.sql" from its oldest checkpoint
    And 5 seconds pass
    Then its log says "on test.1m, the windows closed before"
    And "test.1m" holds no message

  Scenario: Switched from shadow, brrrrr restores the shadow's checkpoint after its sink topics are gone
    Given the topic "raw.trades.test" with 1 partition
    When a shadow brrrrr with the sink prefix "shadow." runs "tests/acceptance/sql/windows.sql" over 3000 trades and stops
    And its shadow's checkpoints are adopted
    And the sink topic "shadow.test.1m" is deleted
    And brrrrr is started again on "tests/acceptance/sql/windows.sql"
    And 3000 more trades are produced
    And the closing trade is produced
    Then its log says "restored checkpoint"
    And its log says "the restored checkpoint names 1 sink partitions this run does not write: not read back"
    And "test.1m" holds none of the windows the shadow wrote over 3000 trades
    And "test.1m" holds each window at most once, all of them windows the engine emits

  # one row closing a cheap view's windows and a costly one's: the cheap view's messages are
  # produced as soon as it has written them, not after the costly view's close. Produced
  # after the batch instead, both come out within ~2 ms; the costly close takes ~90 ms on CI
  # so 30 ms tells the two apart and leaves room for the close to get cheaper still.
  Scenario: A view's messages are produced before the views after it close their windows
    Given the topic "raw.trades.test" with 1 partition
    And the sink topic "test.ids" with 1 partition
    And 60000 trades are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/staggered.sql" with "--takeover 30"
    And brrrrr has claimed the pipeline
    And 5 seconds pass
    And the closing trade is produced, its windows not waited for
    Then the last messages on "test.1m" were produced at least 30 ms before the first on "test.ids"
    And "test.1m" holds exactly the engine's messages to it for those trades
    And "test.ids" holds exactly the engine's messages to it for those trades

  # closed on 4 threads, the cheap view's windows are still written before the costly view's
  Scenario: With windows closed on 4 threads, a view's messages are still produced before the views after it close theirs
    Given the topic "raw.trades.test" with 1 partition
    And the sink topic "test.ids" with 1 partition
    And 60000 trades are waiting in the source
    When brrrrr is started on "tests/acceptance/sql/staggered.sql" with "--takeover 30 --close-threads 4 --close-groups 0"
    And brrrrr has claimed the pipeline
    And 5 seconds pass
    And the closing trade is produced, its windows not waited for
    Then the last messages on "test.1m" were produced at least 30 ms before the first on "test.ids"
    And "test.1m" holds exactly the engine's messages to it for those trades
    And "test.ids" holds exactly the engine's messages to it for those trades
