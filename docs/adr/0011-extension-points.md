# ADR-0011: Registries for functions, aggregates, table functions, sources, sinks and codecs

- Status: proposed
- Date: 2026-09-23

## Context
- brrrrr must run every query of the pipelines it replaces and grow with new
  metrics.
- Some capabilities (L2 order books) cannot be expressed in Proton SQL.
- Benthos's component registry with declarative specs is its most praised
  feature.

## Decision
- Compile-time registries for scalar functions, aggregates (incl. parametric),
  table functions, sources, sinks and codecs.
- Each registration carries a signature, docs and golden vectors; the
  registry test refuses entries without vectors.
- L2 arrives as the table function `orderbook_top_n(stream, depth)`, so the
  L2 metrics stay ordinary SQL. (Built in `brrrrr-core`'s `book`
  module, planned beside `tumble`, with an optional third argument
  `allow_seq_reset`: the registries themselves are not built yet.)

## Consequences
- New capabilities are additive and self-testing, with no planner edits.
- Extensions are brrrrr-only SQL: Proton cannot run them. That is acceptable,
  and they are documented as such.

## Alternatives considered
- **Dynamic plugins** (WASM or shared objects). Unneeded complexity for now;
  revisit if users need to extend brrrrr without rebuilding it.
