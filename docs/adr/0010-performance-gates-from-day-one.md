# ADR-0010: Performance gates from day one

- Status: proposed
- Date: 2026-09-23

## Context
- Vector's RFC 6531 lists regressions that reached users and that criterion
  missed: instrumentation overhead, a `metrics` upgrade (−20%), a channel swap,
  musl.
- Wall-clock benchmarks are noisy on shared CI runners; instruction counts are
  deterministic.

## Decision
- **Every PR:** `gungraun` micro-benchmarks with `--callgrind-limits='ir=2%'`
  against `main`.
- **Nightly, and on PRs touching hot crates:** a Vector-style macro regression
  suite. Real pipelines, recorded inputs, fixed CPU and memory, 5 alternating
  replicates, Mann–Whitney p < 0.01 and > 5% fails.
- **Always:** a Proton baseline that is reported, not gating.
- **Allocations:** counting-allocator tests enforce zero allocations per record
  on hot paths.
- The machinery lands first, before hot code exists.

## Consequences
- Performance claims are reproducible, and regressions are caught at PR time.
- Intentional costs need an explicit `perf-budget` label and justification.

## Alternatives considered
- **Criterion-only in CI.** Too noisy to gate.
- **SaaS-only (CodSpeed).** Fine as an addition. Bencher is self-hostable and
  has adapters for both our tools.
