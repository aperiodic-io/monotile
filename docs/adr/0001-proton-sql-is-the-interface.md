# ADR-0001: Proton's streaming SQL is brrrrr's input language

- Status: proposed
- Date: 2026-09-23

## Context
- Market-data pipelines written for Timeplus Proton are files of SQL
  statements (streams, external streams, materialized views), often rendered
  by a generator from metric specs. The SQL surface such a corpus uses is
  closed and small: 1,892 statements in the corpus we measured, 28 functions.
- The requirement is to run those pipelines unchanged and to match Proton's
  outputs bit for bit.

## Decision
brrrrr executes a Proton pipeline file unchanged. The supported dialect is
that corpus's surface, plus extensions added through ADRs (e.g.
`orderbook_top_n`). A new metric is a SQL change, not Rust work.

## Consequences
- Parity can be tested directly: the same SQL and input go into both engines
  and the outputs are diffed.
- A pipeline that introduces an unsupported construct must fail fast, at
  validation, naming the construct (the SQL surface gate in CI).
- brrrrr inherits some of Proton's SQL quirks (alias visibility, `interval` as
  an identifier) and must implement them faithfully.

## Alternatives considered
- **A YAML metric spec, with the generator emitting brrrrr configs.** It is
  simpler to parse, but there would be two definitions of every metric, no
  direct differential testing, and every future metric shape would need Rust
  work.
- **A general streaming SQL engine (DataFusion/Arroyo).** Its semantics differ
  from ClickHouse's (NULLs, casts, float order, quantiles), so matching Proton
  would mean fighting a far larger surface than the 28 functions we actually
  use.
