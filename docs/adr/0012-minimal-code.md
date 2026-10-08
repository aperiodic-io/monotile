# ADR-0012: Minimal code (amends 0002, 0004, 0005)

- Status: accepted
- Date: 2026-09-23

## Context
The project directive is to write **as little code as possible** outside the
tests, with tests as long as they need to be. Several choices in the original plan
cost code without a measured need:

- sixteen crates;
- Arrow batches with hand-written kernels;
- a thread per task.

The first prototypes were row-based and single-threaded. They reproduced
Proton byte for byte at about 100× Proton's CPU efficiency (510k rows/s on a
14-query L2 order-book pipeline).

## Decision
- **Two crates** (amends 0002):
  - `brrrrr-core` is pure: SQL, expressions, aggregates, operators, order
    book, formats, protobuf, engine state.
  - `brrrrr` holds the runtime, Kafka, checkpoints and CLI.

  The purity rule is enforced by an architecture test: `brrrrr-core` may not
  depend on rdkafka, tokio or object_store, and may not use `std::net`,
  `std::fs` or `std::thread`.
- **Rows, not Arrow** (amends 0004). A row is a `Vec<Value>`. SQL expressions
  are compiled once into closures. Order-sensitive aggregates are sequential
  loops by construction.
- **One deterministic data thread per pipeline** (amends 0005). librdkafka
  does its I/O on its own threads. A single data thread makes merge order,
  replay and exactly-once trivially deterministic.

## Consequences
- Much less code, and one execution path to test.
- The performance budgets (ADR-0010) remain the gate. If a macro case
  misses its budget, the fix is targeted (e.g. a typed fast path for one
  operator) and justified by the benchmark, not added up front.
