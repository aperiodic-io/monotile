# ADR-0005: OS threads and bounded channels on the data path; tokio only for control

- Status: proposed
- Date: 2026-09-23

## Context
- librdkafka performs network I/O on its own threads.
- Our work per record is CPU-bound: decode, book, windows, JSON.
- The thread-per-core async runtimes (glommio, monoio) are effectively
  unmaintained.
- A proof of concept of the order-book stage ran on a sync `BaseConsumer` at
  73.8 CPU-s per 6.3M records.

## Decision
- A sync `BaseConsumer` per external stream, with `split_partition_queue`.
- One OS thread per task, owning its state exclusively (no locks).
- `crossbeam-channel` bounded channels between threads, with
  `pause`/`resume` for backpressure.
- A single `current_thread` tokio runtime for HTTP (`/metrics`, `/health`)
  and timers.

## Consequences
- Predictable latency and simple profiling. No async colouring in core crates.
- Backpressure is explicit.
- We must not block the control runtime with checkpoint compression; it gets a
  dedicated thread.

## Alternatives considered
- **tokio everywhere** with `StreamConsumer`. It adds scheduling overhead and
  hides backpressure, for no benefit here.
- **io_uring runtimes.** Irrelevant while librdkafka does the I/O.

## Amendment (2026-10-02): closes on several threads
One data thread per pipeline still consumes, runs the engine and produces. When a batch closes
the windows of several views of one input at once (a row past the top of the hour closes every
interval's), `--close-threads N` lets the engine run those views' closes on N threads, the data
thread included (ADR-0002, amended). The data thread goes through the views in creation order,
runs a heavy one itself unless a worker took it already, and writes each view's messages as soon
as they are there. Output and state are those of one thread.

- **Off by default** (`--close-threads 1`). On a trade-size pipeline at a live feed's rates
  (engine ms per 100 ms slice, single runs side by side on a shared host, ±15%): 3 threads cut the
  1h close from ~370 to ~300 ms, 30m ~250 to ~210, 15m ~200 to ~160; 2 threads gained little and
  inconsistently, and the total engine time did not move. Worth trying with 3, not a default.
- Only when the views close `--close-groups` groups or more between them (default 2,048):
  smaller closes are faster on the data thread, whose cache holds their groups (closing every
  30s window of a feed on two threads took 65 ms a slice against 52 on one).
- The views' writes stay on the data thread: formatting a sink's lines on several threads
  measured slower, not faster.
- A close that panics gives every view its operators back before the panic goes on.
- **Measured again (2026-10-04), with the latency bench:** close threads made every pipeline's
  top-of-the-hour close slower. The bench is `benches/latency.rs`: wall clock, 7 replays, the
  hour's windows of 500/250/150 symbols, and the default `--close-groups`.
  - 2 threads were 28-136% slower; 3 threads were 6-89% slower.
  - This agrees with what a live deployment measured. The close got cheaper on one thread
    instead.
