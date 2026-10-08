# Contributing to brrrrr

Thank you for helping. brrrrr is small on purpose, tested hard, and measured: a change is welcome
when it keeps all three.

## Getting started

```bash
git clone https://github.com/aperiodic-io/monotile && cd brrrrr
cargo build -p brrrrr            # the binary: target/debug/brrrrr
cargo test -p brrrrr-core -p brrrrr-lake
```

Rust 1.95 (pinned in `rust-toolchain.toml`). The SQL acceptance tests start object-store
emulators and Kafka in Docker: `cargo test -p brrrrr --test sql`. The Python package:
`cd crates/brrrrr-py && maturin develop && pytest tests`.

## How the code is laid out

| crate | what |
| --- | --- |
| `brrrrr-core` | the engine: SQL, expressions, aggregates, windows, joins, the historical executor, the query compiler (`query.rs`). Pure: no I/O, no threads of its own (`tests/architecture.rs` enforces it). |
| `brrrrr-lake` | tables in files and object stores, live tables, statements (`Lake`), output formats |
| `brrrrr` | the binary: `sql`, `serve` (HTTP, PostgreSQL, Kafka), `run` (streaming pipelines), `historical` |
| `brrrrr-py` | the Python package (PyO3, maturin) |

The design decisions are recorded as ADRs in `docs/adr/`; [ADR-0018](docs/adr/0018-ad-hoc-sql-as-engine-views.md)
explains how an ad-hoc query becomes the engine's own pipeline.

## The rules

- **Tests first.** A change comes with the test that fails without it: a unit test in the crate,
  a cookbook recipe (`fixtures/cookbook/cookbook.sql`, held to DuckDB's answer), or a Gherkin
  scenario (`tests/acceptance/sql/*.feature`) for anything a user does from the command line or
  the server.
- **Refuse, never guess.** SQL brrrrr cannot run as written is an error that says what to write
  instead. Nothing is silently ignored.
- **Measure.** A change on a hot path comes with its numbers (`bench/sql`; the instruction-count gate, `cargo bench -p brrrrr-core --bench gate`).
- **Little code.** Reuse what is there; add a dependency only when a few lines cannot do it.
  A new dependency's licence must be one `deny.toml` allows (`cargo deny check`).
- `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` pass.

## Sign your work

Commits carry a `Signed-off-by:` line (`git commit -s`): the
[Developer Certificate of Origin](https://developercertificate.org/), that you may contribute it
under the project's licence (Apache-2.0).

## Questions

Open a [discussion](https://github.com/aperiodic-io/monotile/discussions) or an issue. Security
problems: see [SECURITY.md](SECURITY.md).
