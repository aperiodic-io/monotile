//! Prometheus `/metrics` and `/health` on a plain std listener: the data thread only bumps
//! atomics, a small thread renders them on request.
use std::io::{Read, Write};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A Prometheus histogram of fixed buckets (`le`, inclusive), lock-free: observed by the data
/// thread, rendered by the metrics thread. A value past the last bound goes to an overflow slot,
/// and `+Inf` and `_count` are rendered from the buckets read, so a scrape amid an observation
/// is still cumulative (only `_sum` may lag it by that one value).
pub struct Histogram {
    bounds: &'static [f64],
    /// One per bound, then the overflow.
    buckets: Vec<AtomicU64>,
    /// The sum's f64 bits.
    sum: AtomicU64,
}

impl Histogram {
    fn with(bounds: &'static [f64]) -> Histogram {
        let buckets = (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Histogram { bounds, buckets, sum: AtomicU64::new(0f64.to_bits()) }
    }

    /// Seconds, from a millisecond to ten.
    pub fn seconds() -> Histogram {
        Histogram::with(&[0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    }

    /// Counts, by powers of ten.
    pub fn counts() -> Histogram {
        Histogram::with(&[0.0, 1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0])
    }

    pub fn observe(&self, v: f64) {
        let slot = self.bounds.iter().position(|b| v <= *b).unwrap_or(self.bounds.len());
        self.buckets[slot].fetch_add(1, Relaxed);
        let add = |sum: u64| Some((f64::from_bits(sum) + v).to_bits());
        let _ = self.sum.fetch_update(Relaxed, Relaxed, add); // never None
    }

    /// Its exposition: cumulative buckets, `+Inf`, the sum and the count.
    fn render(&self, name: &str, help: &str) -> String {
        let mut out = format!("# HELP {name} {help}\n# TYPE {name} histogram\n");
        let mut below = 0;
        for (b, n) in self.bounds.iter().zip(&self.buckets) {
            below += n.load(Relaxed);
            out += &format!("{name}_bucket{{le=\"{b}\"}} {below}\n");
        }
        let count = below + self.buckets[self.bounds.len()].load(Relaxed);
        out += &format!("{name}_bucket{{le=\"+Inf\"}} {count}\n");
        out += &format!("{name}_sum {}\n{name}_count {count}\n", f64::from_bits(self.sum.load(Relaxed)));
        out
    }
}

/// What a batch of the data loop cost: where a window close's stall shows.
pub struct Batches {
    /// Decoding a batch and running it through the views, closes included, producing excluded.
    pub engine: Histogram,
    /// The same, of the batches that closed window groups only: a busy pipeline runs thousands
    /// of batches between two closes, so the closes do not show in `engine`'s quantiles.
    pub closing: Histogram,
    /// Producing what the views wrote, flush by flush.
    pub produce: Histogram,
    /// Messages the views wrote.
    pub emitted: Histogram,
    /// Window groups the batch closed.
    pub closed: Histogram,
}

impl Default for Batches {
    fn default() -> Batches {
        Batches {
            engine: Histogram::seconds(),
            closing: Histogram::seconds(),
            produce: Histogram::seconds(),
            emitted: Histogram::counts(),
            closed: Histogram::counts(),
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    pub received: AtomicU64,
    pub sent: AtomicU64,
    pub decode_errors: AtomicU64,
    pub consumer_errors: AtomicU64,
    pub suppressed: AtomicU64,
    pub late: AtomicU64,
    pub future: AtomicU64,
    pub null_time: AtomicU64,
    pub unfilled: AtomicU64,
    /// Quotes an exact ASOF join took after a trade they may have matched was released
    /// (`Engine::asof_late_right`): `--asof-lateness-ms` is too short for the feed when it grows.
    pub asof_late_right: AtomicU64,
    /// Window groups a start without a checkpoint closed without writing: partial ones, and
    /// messages of windows their sink topic can no longer vouch for.
    pub withheld_partial: AtomicU64,
    pub withheld_unverified: AtomicU64,
    pub checkpoints: AtomicU64,
    /// Checkpoint writes that failed (not fatal: the next interval tries again).
    pub checkpoint_failures: AtomicU64,
    /// Keys of messages already in the sinks that a replay must not write again.
    pub suppress_keys: AtomicU64,
    /// 1 while waiting for another instance to stop running the pipeline.
    pub standby: AtomicU64,
    /// Checkpoints skipped at start because they did not decode or fit the plan.
    pub checkpoints_refused: AtomicU64,
    /// Source partitions this run resumed past a gap: the restored checkpoint's offset was no
    /// longer in the partition (`run::unresumable`), so it went on from the oldest record there.
    pub source_resume_gaps: AtomicU64,
    /// Offsets between those checkpoints' offsets and the records the partitions still held:
    /// input the restored windows never saw.
    pub source_resume_skipped: AtomicU64,
    /// The last checkpoint's duration, from its cut until its thread wrote it.
    pub checkpoint_micros: AtomicU64,
    /// How long the last checkpoint held the data loop up: its cut, copying the state.
    pub checkpoint_stall_micros: AtomicU64,
    pub checkpoint_bytes: AtomicU64,
    /// Unix seconds of the last checkpoint (0: none yet).
    pub checkpoint_at: AtomicU64,
    pub consumer_lag: AtomicI64,
    /// Lowest window watermark, µs (i64::MIN: none yet).
    pub watermark: AtomicI64,
    /// Latest event time a window has taken, µs (i64::MIN: none yet).
    pub event_time: AtomicI64,
    /// Unix ms of the data loop's last iteration.
    pub alive_at: AtomicU64,
    /// Unix ms a source message was last consumed (0: none yet).
    pub consumed_at: AtomicU64,
    /// `orderbook_top_n`'s books (brrrrr_core::book::BookStats).
    pub books: AtomicU64,
    pub books_awaiting_snapshot: AtomicU64,
    pub book_stale: AtomicU64,
    pub book_caught_up: AtomicU64,
    pub book_malformed: AtomicU64,
    /// Window groups the engine closed and emitted (`Engine::closed`).
    pub closed_groups: AtomicU64,
    /// Inserts (chunks) whose closes ran on several threads (`Engine::parallel_inserts`).
    pub parallel_closes: AtomicU64,
    pub batches: Batches,
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

impl Metrics {
    pub(crate) fn render(&self) -> String {
        let g = |a: &AtomicU64| a.load(Relaxed);
        let age = match g(&self.checkpoint_at) {
            0 => f64::NAN,
            at => (now_ms() / 1000).saturating_sub(at) as f64,
        };
        let secs = |a: &AtomicI64| match a.load(Relaxed) {
            i64::MIN => f64::NAN,
            us => us as f64 / 1e6,
        };
        [
            ("brrrrr_received_events_total", "counter", "source messages consumed", g(&self.received) as f64),
            ("brrrrr_sent_events_total", "counter", "Kafka messages produced", g(&self.sent) as f64),
            (
                "brrrrr_decode_errors_total",
                "counter",
                "source messages that failed to decode",
                g(&self.decode_errors) as f64,
            ),
            (
                "brrrrr_sink_suppressed_total",
                "counter",
                "replayed messages already in their sink",
                g(&self.suppressed) as f64,
            ),
            ("brrrrr_consumer_errors_total", "counter", "transient consumer errors", g(&self.consumer_errors) as f64),
            ("brrrrr_late_events_total", "counter", "rows dropped as late by windows", g(&self.late) as f64),
            (
                "brrrrr_withheld_partial_windows_total",
                "counter",
                "window groups a start without a checkpoint closed without writing: they began before a source lost records",
                g(&self.withheld_partial) as f64,
            ),
            (
                "brrrrr_withheld_unverified_windows_total",
                "counter",
                "window messages a start without a checkpoint did not write: their sink topic no longer holds records from when they closed",
                g(&self.withheld_unverified) as f64,
            ),
            (
                "brrrrr_sequence_out_of_order_total",
                "counter",
                "trades folded out of order by the sequence aggregates (run_structure, updownticks, trade_returns, distinct_stats)",
                brrrrr_core::agg::SEQUENCE_OUT_OF_ORDER.load(Relaxed) as f64,
            ),
            (
                "brrrrr_hold_late_total",
                "counter",
                "rows a sorting view (SETTINGS order_hold_ms) took after releasing a later one: passed on out of order",
                brrrrr_core::engine::HOLD_LATE.load(Relaxed) as f64,
            ),
            (
                "brrrrr_future_events_total",
                "counter",
                "rows dropped for an event time past now plus --max-future-skew",
                g(&self.future) as f64,
            ),
            (
                "brrrrr_null_time_events_total",
                "counter",
                "rows dropped by windows and ASOF joins for an event time that is NULL or not a time",
                g(&self.null_time) as f64,
            ),
            (
                "brrrrr_unfilled_gaps_total",
                "counter",
                "gaps gap_fill did not fill: more than 65536 buckets, without both a start and a finish",
                g(&self.unfilled) as f64,
            ),
            (
                "brrrrr_asof_late_right_total",
                "counter",
                "right rows (quotes) an exact ASOF join took at or before the time of a left row of their key \
                 it had already released, which they may have matched: --asof-lateness-ms is too short when it grows",
                g(&self.asof_late_right) as f64,
            ),
            ("brrrrr_checkpoints_total", "counter", "checkpoints written", g(&self.checkpoints) as f64),
            (
                "brrrrr_checkpoint_failures_total",
                "counter",
                "checkpoint writes that failed and were tried again",
                g(&self.checkpoint_failures) as f64,
            ),
            (
                "brrrrr_suppression_keys",
                "gauge",
                "messages already in the sinks that the replay must not write again",
                g(&self.suppress_keys) as f64,
            ),
            (
                "brrrrr_standby",
                "gauge",
                "1 while waiting for another instance to stop running the pipeline",
                g(&self.standby) as f64,
            ),
            (
                "brrrrr_checkpoints_refused_total",
                "counter",
                "checkpoints skipped at start: they did not decode or fit the plan",
                g(&self.checkpoints_refused) as f64,
            ),
            (
                "brrrrr_source_resume_gap_partitions",
                "gauge",
                "source partitions this run resumed past a gap: the restored checkpoint's offset was gone \
                 (retention) or past the end (a topic created again), so windows open across it are written \
                 without the records in between",
                g(&self.source_resume_gaps) as f64,
            ),
            (
                "brrrrr_source_resume_skipped_records_total",
                "counter",
                "source offsets between restored checkpoints' offsets and the oldest records their partitions \
                 still held: input the restored windows never saw",
                g(&self.source_resume_skipped) as f64,
            ),
            (
                "brrrrr_checkpoint_duration_seconds",
                "gauge",
                "last checkpoint's duration, from its cut until written: the data loop runs on meanwhile, \
                 held up only for brrrrr_checkpoint_stall_seconds",
                g(&self.checkpoint_micros) as f64 / 1e6,
            ),
            (
                "brrrrr_checkpoint_stall_seconds",
                "gauge",
                "how long the last checkpoint held the data loop up, consuming and emitting nothing: copying \
                 the state at its cut",
                g(&self.checkpoint_stall_micros) as f64 / 1e6,
            ),
            ("brrrrr_checkpoint_bytes", "gauge", "last checkpoint's size", g(&self.checkpoint_bytes) as f64),
            ("brrrrr_checkpoint_age_seconds", "gauge", "time since the last checkpoint", age),
            (
                "brrrrr_consumer_lag",
                "gauge",
                "source messages not yet read (at the last checkpoint)",
                self.consumer_lag.load(Relaxed) as f64,
            ),
            (
                "brrrrr_watermark_seconds",
                "gauge",
                "lowest window watermark: the oldest open window's start, floored to the widest window \
                 (midnight UTC with a 1d window); not a freshness signal",
                secs(&self.watermark),
            ),
            (
                "brrrrr_event_time_seconds",
                "gauge",
                "latest event time any window has taken: how far the pipeline has read (time() minus it \
                 is how far behind it is)",
                secs(&self.event_time),
            ),
            ("brrrrr_books", "gauge", "order books kept by orderbook_top_n, one per symbol", g(&self.books) as f64),
            (
                "brrrrr_books_awaiting_snapshot",
                "gauge",
                "order books that have had no snapshot yet, so build nothing",
                g(&self.books_awaiting_snapshot) as f64,
            ),
            (
                "brrrrr_book_stale_total",
                "counter",
                "order-book messages dropped as not newer than their book (a replica's late snapshot, a replay)",
                g(&self.book_stale) as f64,
            ),
            (
                "brrrrr_book_snapshots_caught_up_total",
                "counter",
                "late order-book snapshots (a replica's, behind the other's diffs) brought up to date and applied",
                g(&self.book_caught_up) as f64,
            ),
            (
                "brrrrr_book_malformed_total",
                "counter",
                "order-book messages dropped for price and amount arrays of different lengths",
                g(&self.book_malformed) as f64,
            ),
            (
                "brrrrr_source_idle_seconds",
                "gauge",
                "time since a source message was last consumed: alive but consuming nothing when high",
                match g(&self.consumed_at) {
                    0 => f64::NAN,
                    at => now_ms().saturating_sub(at) as f64 / 1e3,
                },
            ),
        ]
        .iter()
        .map(|(n, t, h, v)| format!("# HELP {n} {h}\n# TYPE {n} {t}\n{n} {v}\n"))
        .chain([
            (
                "brrrrr_window_groups_closed_total",
                "counter",
                "window groups closed and written: what a window close's work grows with",
                g(&self.closed_groups) as f64,
            ),
            (
                "brrrrr_parallel_closes_total",
                "counter",
                "inserts whose views' windows closed on several threads (--close-threads)",
                g(&self.parallel_closes) as f64,
            ),
        ]
        .iter()
        .map(|(n, t, h, v)| format!("# HELP {n} {h}\n# TYPE {n} {t}\n{n} {v}\n")))
        .chain([
            self.batches.engine.render(
                "brrrrr_batch_engine_seconds",
                "per batch: decoding it and running it through the views, window closes included, producing excluded \
                 (and, once per start without a checkpoint, a source's first record's withholding)",
            ),
            self.batches.closing.render(
                "brrrrr_batch_closing_engine_seconds",
                "brrrrr_batch_engine_seconds of the batches that closed window groups only: where a close's stall shows",
            ),
            self.batches.produce.render("brrrrr_batch_produce_seconds", "per batch: producing what its views wrote"),
            self.batches.emitted.render("brrrrr_batch_emitted_messages", "per batch: messages its views wrote"),
            self.batches.closed.render("brrrrr_batch_closed_groups", "per batch: window groups it closed"),
        ])
        .collect()
    }
}

/// The status and body answering request `req` at `now` (Unix ms).
fn respond(req: &str, m: &Metrics, now: u64) -> (&'static str, String) {
    if req.starts_with("GET /metrics") {
        ("200 OK", m.render())
    } else if req.starts_with("GET /health") {
        match now.saturating_sub(m.alive_at.load(Relaxed)) < 120_000 {
            true => ("200 OK", "ok\n".to_string()),
            false => ("503 Service Unavailable", "data loop stalled\n".to_string()),
        }
    } else {
        ("404 Not Found", String::new())
    }
}

/// Serves until the process exits; `/health` fails when the data loop stalls for 2 minutes
/// (the last checkpoint, on SIGTERM, waits up to 30 s for the sinks' acks before its write).
pub fn serve(addr: &str, m: Arc<Metrics>) -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind(addr)?;
    std::thread::spawn(move || {
        for mut s in listener.incoming().flatten() {
            // one silent client must not stall the probes
            let _ = s.set_read_timeout(Some(std::time::Duration::from_secs(2)));
            let _ = s.set_write_timeout(Some(std::time::Duration::from_secs(2)));
            // read the whole request head, so closing never resets an unread request
            let (mut buf, mut n) = ([0u8; 1024], 0);
            while n < buf.len() && !buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                match s.read(&mut buf[n..]) {
                    Ok(0) | Err(_) => break,
                    Ok(k) => n += k,
                }
            }
            let (status, body) = respond(&String::from_utf8_lossy(&buf[..n]), &m, now_ms());
            let _ = write!(s, "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(text: &str, name: &str) -> String {
        let line = text.lines().find(|l| l.starts_with(&format!("{name} "))).unwrap_or_else(|| panic!("{name}"));
        line[name.len() + 1..].to_string()
    }

    /// A histogram counts each value in the first bucket at or above it (`le` is inclusive),
    /// renders its buckets cumulatively, counts what is past the last in `+Inf` only, and keeps
    /// the sum and the count.
    #[test]
    fn a_histogram_renders_cumulative_inclusive_buckets_its_sum_and_count() {
        let h = Histogram::counts();
        for v in [0.0, 1.0, 1.5, 10.0, 99.0, 100.0, 100_000.0, 250_000.0] {
            h.observe(v);
        }
        let text = h.render("x", "what x is");
        let want = "# HELP x what x is\n# TYPE x histogram\n\
                    x_bucket{le=\"0\"} 1\nx_bucket{le=\"1\"} 2\nx_bucket{le=\"10\"} 4\nx_bucket{le=\"100\"} 6\n\
                    x_bucket{le=\"1000\"} 6\nx_bucket{le=\"10000\"} 6\nx_bucket{le=\"100000\"} 7\n\
                    x_bucket{le=\"+Inf\"} 8\nx_sum 350211.5\nx_count 8\n";
        assert_eq!(text, want);
        let s = Histogram::seconds();
        s.observe(0.0004);
        s.observe(0.001);
        s.observe(0.3);
        s.observe(60.0);
        let text = s.render("t", "time");
        assert_eq!(value(&text, "t_bucket{le=\"0.001\"}"), "2");
        assert_eq!(value(&text, "t_bucket{le=\"0.25\"}"), "2");
        assert_eq!(value(&text, "t_bucket{le=\"0.5\"}"), "3");
        assert_eq!(value(&text, "t_bucket{le=\"10\"}"), "3");
        assert_eq!(value(&text, "t_bucket{le=\"+Inf\"}"), "4");
        assert_eq!(value(&text, "t_count"), "4");
        assert!((value(&text, "t_sum").parse::<f64>().unwrap() - 60.3014).abs() < 1e-9, "{text}");
        // what is past the last bound is in `+Inf` and the count only, the buckets read
        let o = Histogram::counts();
        o.observe(1e9);
        o.observe(f64::INFINITY);
        let text = o.render("o", "overflow");
        assert_eq!(value(&text, "o_bucket{le=\"100000\"}"), "0");
        assert_eq!((value(&text, "o_bucket{le=\"+Inf\"}"), value(&text, "o_count")), ("2".into(), "2".into()));
        // observed from several threads, nothing is lost
        let c = Histogram::seconds();
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| (0..1_000).for_each(|_| c.observe(0.5)));
            }
        });
        let text = c.render("c", "concurrent");
        assert_eq!((value(&text, "c_count"), value(&text, "c_sum")), ("4000".into(), "2000".into()));
        let empty = Histogram::seconds().render("e", "none");
        assert_eq!((value(&empty, "e_sum"), value(&empty, "e_count")), ("0".into(), "0".into()));
    }

    /// The batch histograms and the close counters are on `/metrics`.
    #[test]
    fn the_batch_histograms_and_close_counters_are_rendered() {
        let m = Metrics::default();
        m.closed_groups.store(350, Relaxed);
        m.parallel_closes.store(4, Relaxed);
        m.batches.engine.observe(0.02);
        m.batches.closed.observe(350.0);
        m.batches.closing.observe(0.4);
        let text = m.render();
        assert_eq!(value(&text, "brrrrr_batch_closing_engine_seconds_bucket{le=\"0.25\"}"), "0");
        assert_eq!(value(&text, "brrrrr_batch_closing_engine_seconds_bucket{le=\"0.5\"}"), "1");
        assert_eq!(value(&text, "brrrrr_window_groups_closed_total"), "350");
        assert_eq!(value(&text, "brrrrr_parallel_closes_total"), "4");
        assert_eq!(value(&text, "brrrrr_batch_engine_seconds_count"), "1");
        assert_eq!(value(&text, "brrrrr_batch_engine_seconds_bucket{le=\"0.025\"}"), "1");
        assert_eq!(value(&text, "brrrrr_batch_closed_groups_sum"), "350");
        for name in [
            "brrrrr_batch_produce_seconds",
            "brrrrr_batch_emitted_messages",
            "brrrrr_batch_closed_groups",
            "brrrrr_batch_closing_engine_seconds",
        ] {
            assert!(text.contains(&format!("# TYPE {name} histogram\n")), "{name}");
            assert_eq!(value(&text, &format!("{name}_bucket{{le=\"+Inf\"}}")), value(&text, &format!("{name}_count")));
        }
    }

    /// `/health` fails once the data loop has not come round for 2 minutes.
    #[test]
    fn health_fails_when_the_data_loop_stalls_for_2_minutes() {
        let m = Metrics::default();
        m.alive_at.store(1_000_000, Relaxed);
        let health = |now| respond("GET /health HTTP/1.1\r\n\r\n", &m, now);
        assert_eq!(health(1_119_999), ("200 OK", "ok\n".to_string()));
        assert_eq!(health(999_000), ("200 OK", "ok\n".to_string()), "a clock step back is no stall");
        assert_eq!(health(1_120_000), ("503 Service Unavailable", "data loop stalled\n".to_string()));
        let (status, body) = respond("GET /metrics HTTP/1.1\r\n\r\n", &m, 0);
        assert_eq!((status, body), ("200 OK", m.render()));
        assert_eq!(respond("GET / HTTP/1.1\r\n\r\n", &m, 0), ("404 Not Found", String::new()));
    }

    /// Every metric has its HELP and TYPE lines; units are converted (µs to s, ms to s) and
    /// "never" is NaN rather than a misleading number.
    #[test]
    fn metrics_render_in_prometheus_text_with_their_units() {
        let m = Metrics::default();
        m.watermark.store(i64::MIN, Relaxed);
        m.event_time.store(i64::MIN, Relaxed);
        let text = m.render();
        for name in [
            "brrrrr_checkpoint_age_seconds",
            "brrrrr_watermark_seconds",
            "brrrrr_event_time_seconds",
            "brrrrr_source_idle_seconds",
        ] {
            assert_eq!(value(&text, name), "NaN", "{name}");
        }
        let n = text.lines().filter(|l| l.starts_with("# TYPE ")).count();
        assert_eq!(text.lines().filter(|l| l.starts_with("# HELP ")).count(), n);
        assert!(n >= 15, "{n} metrics");
        m.received.store(7, Relaxed);
        m.checkpoint_failures.store(3, Relaxed);
        m.watermark.store(1_500_000, Relaxed);
        m.event_time.store(1_750_000, Relaxed);
        m.checkpoint_micros.store(2_500_000, Relaxed);
        m.checkpoint_stall_micros.store(37_500, Relaxed);
        m.checkpoint_at.store(now_ms() / 1000 - 100, Relaxed); // a checkpoint 100 s ago
        m.consumed_at.store(now_ms() - 30_000, Relaxed); // a message 30 s ago
        m.books_awaiting_snapshot.store(4, Relaxed);
        m.book_stale.store(9, Relaxed);
        m.book_caught_up.store(2, Relaxed);
        m.null_time.store(5, Relaxed);
        m.unfilled.store(6, Relaxed);
        m.asof_late_right.store(6, Relaxed);
        m.source_resume_gaps.store(2, Relaxed);
        m.source_resume_skipped.store(500, Relaxed);
        let text = m.render();
        assert_eq!(value(&text, "brrrrr_source_resume_gap_partitions"), "2");
        assert_eq!(value(&text, "brrrrr_source_resume_skipped_records_total"), "500");
        assert_eq!(value(&text, "brrrrr_books_awaiting_snapshot"), "4");
        assert_eq!(value(&text, "brrrrr_book_stale_total"), "9");
        assert_eq!(value(&text, "brrrrr_book_snapshots_caught_up_total"), "2");
        assert_eq!(value(&text, "brrrrr_received_events_total"), "7");
        assert_eq!(value(&text, "brrrrr_null_time_events_total"), "5");
        assert_eq!(value(&text, "brrrrr_unfilled_gaps_total"), "6");
        assert_eq!(value(&text, "brrrrr_asof_late_right_total"), "6");
        assert_eq!(value(&text, "brrrrr_checkpoint_failures_total"), "3");
        assert_eq!(value(&text, "brrrrr_watermark_seconds"), "1.5");
        assert_eq!(value(&text, "brrrrr_event_time_seconds"), "1.75");
        assert_eq!(value(&text, "brrrrr_checkpoint_duration_seconds"), "2.5");
        assert_eq!(value(&text, "brrrrr_checkpoint_stall_seconds"), "0.0375");
        let secs = |name| value(&text, name).parse::<f64>().unwrap();
        assert!((100.0..105.0).contains(&secs("brrrrr_checkpoint_age_seconds")), "{text}");
        assert!((30.0..35.0).contains(&secs("brrrrr_source_idle_seconds")), "{text}");
    }
}
