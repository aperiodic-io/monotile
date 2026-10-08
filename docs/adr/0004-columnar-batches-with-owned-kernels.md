# ADR-0004: Arrow arrays as the batch format, with our own order-preserving kernels

- Status: proposed
- Date: 2026-09-23

## Context
- Arroyo rewrote its row-based engine around Arrow for speed (0.10).
- Vector's move to `EventArray` batches was a large win.
- Proton's float aggregates are **sequential, in arrival order** (`+=` per
  row). SIMD or pairwise reductions change the last bits.
- Byte-exact parity with Proton was demonstrated with sequential kernels
  (954/954 messages of a recorded pipeline's output).

## Decision
- Batches are Arrow `RecordBatch`es (`arrow-array`/`-buffer`/`-schema`).
- All aggregate kernels and every order-sensitive float computation are our
  own sequential loops.
- `arrow-compute` may be used only where results are provably identical:
  comparisons, filters, integer ops and casts. Each such use needs a golden
  test.

## Consequences
- We get columnar decoding, vectorised projections and filters, and a direct
  path to Parquet.
- We maintain our own aggregate kernels. That is required anyway for
  ClickHouse semantics.
- A gate: compare Arrow batches against plain typed `Vec` batches on the
  book and window hot paths. If Arrow costs more than 10% there, the hot
  operators get typed row buffers inside, with Arrow kept at the edges.

## Alternatives considered
- **Row structs only.** Simpler, but projections and the Parquet path suffer,
  and Arroyo's experience argues against it.
- **DataFusion execution.** Different semantics (see ADR-0001).
