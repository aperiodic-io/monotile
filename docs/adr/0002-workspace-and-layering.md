# ADR-0002: Hexagonal workspace, pure core crates, adapters at the edge

- Status: proposed
- Date: 2026-09-23

## Context
- Vector (`vector-core`, `vector-buffers`, `vector-config`) and Arroyo
  (`arroyo-types`, `-operator`, `-state`, `-connectors`) both converge on this
  layout: traits and data model in small crates with no I/O, connectors
  separate, and a runtime that wires them together.
- Vector documented the cost of one giant crate (RFC 7694).
- Our correctness strategy depends on driving operators without Kafka.

## Decision
- Small crates by layer: the data model, SQL front end and operators in core
  crates; Kafka, object stores and the runtime as adapters around them.
- Pure core crates have no network, filesystem, threads or wall clock (time
  comes through `Clock`).
- Only `kafka` and `objstore` do I/O; only `runtime` spawns threads.
- An architecture test (reading `cargo metadata`) enforces the rules in CI.

## Consequences
- Core logic is testable with `testkit::Driver`, deterministic and fast.
- Adapters get their own conformance suites.
- Compile times stay manageable.
- More crates means more `pub` API surface, handled with `#[doc(hidden)]` and
  `pub(crate)` discipline.

## Alternatives considered
- **A single crate with modules.** Its rules are unenforceable and it compiles
  slower.
- **A plugin system with dynamic loading.** Unnecessary: registries at compile
  time are enough (ADR-0011).

## Amendment (2026-10-02): scoped threads in core
A row past the top of the hour closes every interval's windows of a feed in one `Engine::insert`:
on a pipeline of many trade-size views, ~0.35 s on one thread, every message of the batch waiting for it. Closing those
views' windows on several threads is data-parallel work on state no two threads share, so core
may now use threads, in one form only:

- `std::thread::scope`: the threads are joined before the call returns, so none outlives it, and
  results are taken in a fixed order (the views in creation order).
  The output and the state are those of one thread, message for message and bit for bit.
- Nothing else: no `thread::spawn`, no `thread::Builder`, no sleeping, no I/O. The architecture
  test allows `std::thread` only as `std::thread::scope(` and forbids the rest.
- Off unless asked: `Engine::set_close_threads(1)` (the default) runs everything on the calling
  thread; the runtime sets it from `--close-threads`, 1 by default too (ADR-0005).
