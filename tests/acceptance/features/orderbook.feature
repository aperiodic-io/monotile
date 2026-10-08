Feature: Order books from a venue's snapshots and changes
  orderbook_top_n rebuilds each symbol's book from raw.orderbook, which two replicas of a feed
  handler publish. A replica's REST snapshot can land after newer diffs (its own dedup key, so the broker
  admits it): the book orders it by the venue's book version, BookUpdate.venue_sequence (fixtures/market.proto).
  One older than the book's own snapshot is dropped; by time alone it would be taken.

  Background:
    Given a Redpanda broker
    And the topic "book.top" with 1 partition

  Scenario: A replica's late snapshot is dropped by its venue sequence
    Given the topic "raw.orderbook.test" with 1 partition
    When brrrrr runs "tests/acceptance/sql/orderbook.sql" over order books with a replica's late snapshot
    Then "book.top" holds exactly the engine's book rows, none of them the late snapshot's
