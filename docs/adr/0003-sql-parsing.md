# ADR-0003: Own DDL parser plus a `sqlparser-rs` Proton dialect

- Status: proposed
- Date: 2026-09-23

## Context
A spike parsed a corpus of Proton pipeline statements with `sqlparser-rs`
0.58:

- 0/1,892 whole statements parse. Proton's `CREATE STREAM`,
  `CREATE EXTERNAL STREAM`, `MATERIALIZED VIEW ... INTO` and `PARTITION BY` on
  external tables are unknown to it.
- For MV `SELECT` bodies: 126/1,245 → 868 (after treating `interval` as an
  identifier) → **1,239/1,245 (99.5%)** (after also accepting bare duration
  literals).
- The remaining 6 fail only on `ASOF LEFT JOIN`.

## Decision
- A hand-written recursive-descent parser for the DDL envelope (`CREATE
  [EXTERNAL] STREAM`, `CREATE MATERIALIZED VIEW ... INTO`, their settings).
- Query bodies are parsed by `sqlparser-rs` through a `ProtonDialect` that adds:
  - `interval` as an identifier;
  - duration literals as window arguments;
  - `ASOF [LEFT] JOIN`.
- The dialect changes are proposed upstream.

## Consequences
- We reuse a mature expression and query parser (precedence, literals,
  ClickHouse parametric aggregates) and own only the Proton-specific envelope.
- We track `sqlparser` releases. The SQL surface gate catches breakage.

## Alternatives considered
- **A full custom SQL parser.** More code, and it re-solves solved problems.
- **Regex preprocessing** (the spike's method). Fragile and loses error
  locations.
- **Forking `sqlparser`.** A last resort if the upstream dialect hooks prove
  insufficient.
