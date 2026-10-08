//! `brrrrr run`: one data thread per pipeline (ADR-0012). librdkafka does the I/O on its own
//! threads; this loop polls, decodes each partition's run of messages as one chunk, runs the
//! engine and produces the sink messages. Every `interval` it checkpoints the engine state with
//! the source positions and the sinks' end offsets as one object (ADR-0008). The loop only takes
//! the checkpoint's cut, a copy of the state and positions between two batches, and goes on; a
//! thread of the checkpoint's own waits for the sinks to ack what was produced before the cut
//! (`Ledger`: their offsets after it are the cut's), encodes it and writes it.
//!
//! Exactly-once output (ADR-0007, amended): on restore, each sink partition is read from its
//! checkpointed end offset to its current end, and every `redpanda-dedup-key` found there is
//! suppressed once when replay produces it again. Keys are unique per window and symbol, so a
//! crash at any point leaves each window in its topic exactly once, whatever chunk boundaries
//! the replay sees. Without a checkpoint (first start, a switch from another engine, a changed
//! SQL file), the sinks are read back as far as the replay reaches: the sources'
//! retention, at least `--lookback` hours.
//!
//! Checkpoints live in a directory on the pod's volume (`--checkpoints`, by default
//! `/var/lib/brrrrr/checkpoints`: mount a PVC at `/var/lib/brrrrr`) or an object store (see
//! `store`), under `<pipeline>[@<sink topic prefix>]/<hash of the SQL file>/`: a changed
//! SQL starts fresh, as Proton resets its state when its SQL changes. They are created, never
//! overwritten (a store proves at start that it refuses to), and double as a lease, which is the
//! pipeline's, not its revision's (`Stores`): each checkpoint's epoch is first taken as
//! `<epoch>.lease` in `<pipeline>[@<sink topic prefix>]/`, so an instance of another revision
//! (a changed SQL, another --asof) writing the same sinks never runs beside it. An instance
//! that starts while another may still run the pipeline stands by until that one releases it
//! (SIGTERM takes a last checkpoint and marks it released) or checkpoints no more for
//! `--takeover` seconds; it then claims the next epoch before producing anything. A running
//! instance sends nothing later than `--takeover / 2` after the cut of its last checkpoint
//! written (`produce`; a checkpoint being written renews nothing until it is), and its producer
//! fails what is not delivered `--takeover / 2` after it was sent (`producer_config`, which the
//! user's settings cannot change), so nothing it sent reaches a sink once another instance may
//! take over. A checkpoint that fails to be written is tried
//! again every `--interval` under the same epoch (`Lease`): only finding its epoch taken, a
//! failed sink delivery, the lease running out while producing or a failed last checkpoint on
//! SIGTERM stop the instance.
use crate::align;
use crate::metrics::{self, now_ms, Metrics};
use crate::store::{self, Store};
use anyhow::{anyhow, bail, Context, Result};
use brrrrr_core::checkpoint::{fnv64, Checkpoint, Withhold};
use brrrrr_core::engine::{Asof, Emit, Engine, Output};
use brrrrr_core::proto::{parse_proto, Codec};
use brrrrr_core::sql::{Catalog, Kind, Stream};
use brrrrr_core::value::{parse_datetime, Type, Value};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use rdkafka::{ClientConfig, ClientContext, Offset, TopicPartitionList};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum AsofArg {
    Exact,
    Arrival,
}

#[derive(clap::Args)]
pub struct Args {
    /// ASOF LEFT JOIN matching: `exact` (a batch engine's as-of join, as DuckDB's; left rows
    /// wait up to 1 s past their time for the right side) or `arrival` (Proton's, `keep_versions`).
    #[arg(long, value_enum, default_value_t = AsofArg::Exact)]
    asof: AsofArg,
    /// With `--asof exact`, how long a left row waits past its time for every right side, in
    /// ms: how far out of time order a right row may arrive (across keys) and still be matched
    /// exactly. A shorter wait is out sooner and misses a right row later than it, so size it
    /// from the feed's measured disorder (one exchange's quotes: p99.9 ~150 ms, max ~350 ms over minutes).
    /// At most the hold (30 s). Not part of a checkpoint's identity: a restart with another
    /// value resumes, and rows held or right versions kept under the old one are used under the new.
    #[arg(long, value_name = "MS", default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..=30_000))]
    asof_lateness_ms: u64,
    /// The pipeline's SQL file.
    sql: std::path::PathBuf,
    /// The protobuf schema the ProtobufSingle sources' `format_schema` refers to
    /// (e.g. fixtures/market.proto); JSONEachRow sources need none.
    #[arg(long)]
    proto: Option<std::path::PathBuf>,
    /// Where checkpoints go: a directory on the pod's volume (by default the PersistentVolumeClaim
    /// mounted at /var/lib/brrrrr), or with the object-store feature s3://bucket/prefix (AWS_* env)
    /// or memory:///.
    #[arg(long, env = "BRRRRR_CHECKPOINTS", value_name = "DIR|URL", default_value = store::DEFAULT)]
    checkpoints: String,
    /// Seconds between checkpoints.
    #[arg(long, default_value_t = 10)]
    interval: u64,
    /// On a start without a checkpoint, also writes the windows that began before a source's
    /// oldest record where retention has deleted older ones, as Proton does. By default they
    /// are withheld (they close without being written): part of their input is gone. Only
    /// partitions that lost records bound them, by their oldest record's timestamp, which must
    /// be an event time, as a feed handler writes it; a partition that still holds its first
    /// record lost nothing, however late it starts.
    #[arg(long)]
    partial_windows: bool,
    /// On a start without a checkpoint, also writes the windows a sink topic can no longer vouch
    /// for, as Proton does. By default they are withheld: a window closed before the oldest record
    /// its topic still holds, where retention has deleted older ones, cannot be told apart from
    /// one never written, so the replay would write it again, hours late. Per topic, so per
    /// interval: a 1m topic keeping 2 hours bounds its windows there, a 1d topic keeping a week
    /// bounds its own a week back. A topic that lost no record bounds nothing.
    #[arg(long)]
    unverified_history: bool,
    /// Restore this epoch's checkpoint instead of the newest one that restores.
    #[arg(long, value_name = "EPOCH", conflicts_with = "fresh")]
    restore_epoch: Option<u64>,
    /// Start without restoring any checkpoint. The sinks are still read back from --lookback
    /// hours ago, so windows already written are not written again.
    #[arg(long)]
    fresh: bool,
    /// Rows whose event time is more than this many seconds past now are dropped and counted
    /// (brrrrr_future_events_total) instead of moving their windows' watermark there for good,
    /// which would make every later row late, or an ASOF join's newest time, which would release
    /// every later left row at once. 0: take any time, as Proton does.
    #[arg(long, value_name = "SECONDS", default_value_t = 3600)]
    max_future_skew: u64,
    /// Merges the source partitions by record timestamp (ADR-0006's aligned mode, at the
    /// source): a message goes to the engine only once no partition may still deliver an older
    /// one, so a partition that is behind (after downtime, or for a fetch under load) no longer
    /// has its rows dropped as late while another runs ahead. SECONDS is accepted and no longer
    /// used: it let a partition run that far ahead, which lost the rows of the one behind at
    /// every window boundary in between. Off by default: partitions in arrival order, as
    /// Proton reads them.
    #[arg(long, value_name = "SECONDS")]
    max_drift: Option<u64>,
    /// With --max-drift: a source partition at its end holds the others' messages back until
    /// the clock is this long past their record timestamps, by when its own older messages are
    /// produced and read. It is what a quiet partition adds to the latency of a message that is
    /// read sooner than that, and how late a partition's fetch may be without losing its rows to
    /// a window that another partition closed. 0: a partition at its end holds nobody back.
    #[arg(long, value_name = "MS", default_value_t = 50, value_parser = clap::value_parser!(u64).range(..=60_000))]
    partition_wait_ms: u64,
    /// How long a batch of source messages waits for more after its first before the engine
    /// runs it: the most a message waits to be read (with --max-drift, up to 2 ms more). What
    /// is already queued is taken at once, up to 1000 messages, so a replay runs in full
    /// batches even at 0. At most 1000: the lease (--takeover) is only checked between batches.
    #[arg(long, value_name = "MS", default_value_t = 1, value_parser = clap::value_parser!(u64).range(..=1000))]
    batch_wait_ms: u64,
    /// When nothing has been consumed for this many seconds and every source partition is read
    /// to its end, closes the windows an event that long ago would close. Otherwise a stream
    /// that goes quiet keeps its last windows open until its next row. Off by default:
    /// Proton's rule.
    #[arg(long, value_name = "SECONDS")]
    idle_close: Option<u64>,
    /// With --max-drift: closes a window without waiting for a later row once the source
    /// partitions have all been read to their ends and the clock is this many ms past what they vouch
    /// for (and past the window's own delay): the most a window can be late when its feed goes quiet,
    /// polled data, rare events. A partition with a backlog, or a pipeline that is replaying or stalled, holds
    /// the close back. A row that still arrives later is dropped as late (`brrrrr_late_events_total`), so it
    /// must exceed what a record takes from capture to the broker and to this consumer (--partition-wait-ms
    /// and more: about 50 ms at p99 for a market-data feed). Whole-pipeline and in ms, unlike --idle-close. Off by default.
    #[arg(long, value_name = "MS", value_parser = clap::value_parser!(u64).range(1..=60_000))]
    close_after_ms: Option<u64>,
    /// Seconds without a new checkpoint after which a starting instance takes the pipeline over
    /// from one that may still be running (default: 3 × --interval; more than 2 × --interval).
    /// A running instance stops producing half this long after its last checkpoint.
    #[arg(long, value_name = "SECONDS")]
    takeover: Option<u64>,
    /// Kafka consumer group, used for lag visibility (positions come from checkpoints).
    #[arg(long)]
    group: Option<String>,
    /// Read sources from Apache Iggy instead of Kafka (iggy+tcp://user:password@host:port).
    /// The SQL's topic market.trades.spot maps to Iggy stream market, topic trades.spot.
    #[arg(long, env = "BRRRRR_IGGY", value_name = "CONNECTION")]
    iggy: Option<String>,
    /// Overrides every stream's `brokers` setting (tests, port-forwards).
    #[arg(long)]
    brokers: Option<String>,
    /// Overrides the sinks' `brokers` (defaults to --brokers, then the SQL's setting).
    #[arg(long)]
    sink_brokers: Option<String>,
    /// A librdkafka setting for every Kafka client, e.g. `security.protocol=sasl_ssl`; or
    /// `@<file>` for a file of `key=value` lines (keeps secrets off the command line). Repeatable.
    #[arg(long, value_name = "KEY=VALUE|@FILE")]
    kafka_config: Vec<String>,
    /// As --kafka-config, for the clients of the sources' cluster only (applied after it).
    #[arg(long, value_name = "KEY=VALUE|@FILE")]
    source_kafka_config: Vec<String>,
    /// As --kafka-config, for the clients of the sinks' cluster only (applied after it).
    #[arg(long, value_name = "KEY=VALUE|@FILE")]
    sink_kafka_config: Vec<String>,
    /// Without a checkpoint, how many hours of sink history to read back for windows a replay
    /// could produce again. Raised to the sources' retention, which is how far back a replay
    /// reaches (read from their topic configs); if a source is compacted or keeps its data
    /// without limit, the sinks are read from their start.
    #[arg(long, default_value_t = 48)]
    lookback: u64,
    /// Writes every sink to `<prefix><topic>` (created if missing), e.g. `shadow.` to run next
    /// to another instance or engine and compare outputs.
    #[arg(long, default_value = "")]
    sink_topic_prefix: String,
    /// Writes every sink to the topic this template makes of its own (created if missing):
    /// `{N}` is the Nth dot-separated part of the SQL's topic. `shadow.{1}.{2}` writes
    /// `bars.v1.spot.1m` and `bars.v1.spot.5m` both to `shadow.bars.v1`, for
    /// messages that name their exchange and interval themselves (and in their dedup key).
    /// Not with --sink-topic-prefix.
    #[arg(long, default_value = "")]
    sink_topic_template: String,
    /// Threads, the data thread included, that close several views' windows at once when a
    /// batch closes them: a row past the top of the hour closes every interval's windows, for
    /// every feed. Each view's messages are the same and go out in the same order as on one
    /// thread. 1 (the default): on the data thread only. Measured on trade-size, 3 threads cut
    /// its 1h close from ~370 to ~300 ms; 2 gained little, and small closes are kept on the data
    /// thread (--close-groups).
    #[arg(long, value_name = "N", default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..=64))]
    close_threads: u64,
    /// With --close-threads above 1: the fewest groups a batch's views must close between them
    /// to be closed on several threads. Fewer close faster on the data thread, whose cache holds
    /// them.
    #[arg(long, value_name = "GROUPS", default_value_t = brrrrr_core::engine::CLOSE_GROUPS as u64)]
    close_groups: u64,
    /// Where `/metrics` (Prometheus) and `/health` are served.
    #[arg(long, default_value = "0.0.0.0:9464")]
    metrics: String,
}

const TIMEOUT: Duration = Duration::from_secs(30);
/// For bookkeeping (lag, partition growth, --idle-close), on a thread of its own: best effort.
const BOOKKEEPING: Duration = Duration::from_secs(5);
/// How long the data loop waits for a first message before it comes around anyway (checkpoints,
/// --idle-close, a stop signal).
const IDLE: Duration = Duration::from_millis(50);
const DEDUP: &str = "redpanda-dedup-key";

/// How a source's messages become rows of its columns.
enum Decoder {
    Proto(Codec),
    /// `JSONEachRow`: one JSON object per message (`json_row`).
    Json(Vec<(String, Type)>),
}

impl Decoder {
    fn decode(&self, b: &[u8]) -> std::result::Result<Vec<Value>, String> {
        match self {
            Decoder::Proto(c) => c.decode(b),
            Decoder::Json(cols) => json_row(cols, b),
        }
    }
}

/// A `JSONEachRow` message as a row of `cols`, read as ClickHouse reads one: fields by column
/// name, a missing one or `null` is the column's default (NULL if nullable), others are ignored.
/// A datetime is text (`2024-01-02 09:30:00.123`, ISO 8601 `2024-01-02T09:30:00.123Z` or with an
/// offset, `+01:00`; UTC without one) or a number: an integer in the column's precision (epoch
/// ms in a `datetime64(3)`, µs in a `datetime64(6)`, seconds in a `datetime`), one with a
/// fraction in seconds. A number may also come as text (`"42.5"`). A message that is not an
/// object, or a value that does not fit its column, is an error: the message is skipped.
// ponytail: a map per message; a borrowing visitor if JSON sources ever carry a full feed's rates
fn json_row(cols: &[(String, Type)], b: &[u8]) -> std::result::Result<Vec<Value>, String> {
    let obj: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(b).map_err(|e| format!("not a JSON object: {e}"))?;
    cols.iter()
        .map(|(name, t)| match obj.get(name) {
            None => Ok(t.default_value()),
            Some(v) => json_value(v, t).map(|v| v.cast_into(t)).map_err(|e| format!("{name}: {e}")),
        })
        .collect()
}

fn json_value(v: &serde_json::Value, t: &Type) -> std::result::Result<Value, String> {
    use serde_json::Value as J;
    Ok(match (v, t.base()) {
        (J::Null, _) => Value::Null,
        (J::String(s), Type::Time(_)) => Value::Time(parse_datetime(s).ok_or(format!("{s:?} is not a datetime"))?),
        (J::Number(n), Type::Time(p)) => Value::Time(match n.as_i64() {
            Some(i) => i.saturating_mul(10i64.pow(6 - u32::from(*p))),
            None => (n.as_f64().unwrap_or(0.0) * 1e6).round() as i64,
        }),
        (J::String(s), Type::Str) => Value::Str(s.as_str().into()),
        (v, Type::Str) => Value::Str(v.to_string().into()),
        // the cast to the column parses it
        (J::String(s), Type::Int(_) | Type::UInt(_) | Type::F32 | Type::F64) if s.trim().parse::<f64>().is_ok() => {
            Value::Str(s.as_str().into())
        }
        (J::Bool(b), _) => Value::Bool(*b),
        (J::Number(n), _) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => Value::Int(i),
            (_, Some(u)) => Value::UInt(u),
            _ => Value::F64(n.as_f64().unwrap_or(0.0)),
        },
        (J::Array(a), Type::Array(inner)) => Value::Array(
            a.iter()
                .map(|v| json_value(v, inner).map(|v| v.cast_into(inner)))
                .collect::<std::result::Result<_, _>>()?,
        ),
        (v, t) => return Err(format!("{v} is not a {t:?}")),
    })
}

/// Failed deliveries: the loop stops on the first one (a restart replays from the last
/// checkpoint, so nothing is lost), saying which message failed and why. Every message is sent
/// with its generation (`Ledger`) as its delivery opaque.
struct Deliveries {
    failed: Arc<AtomicU64>,
    first: std::sync::Mutex<Option<String>>,
    /// What the sinks acked, by generation: a checkpoint takes its sink offsets from these
    /// rather than asking each partition's end.
    ledger: Ledger,
}

impl ClientContext for Deliveries {}

impl ProducerContext for Deliveries {
    type DeliveryOpaque = usize;
    fn delivery(&self, r: &DeliveryResult<'_>, generation: usize) {
        match r {
            Ok(msg) => self.ledger.delivered(generation as u64, Some((msg.topic(), msg.partition(), msg.offset()))),
            Err((e, msg)) => {
                if self.failed.fetch_add(1, Ordering::Relaxed) == 0 {
                    let cause =
                        format!("{}/{}: {e}{}", msg.topic(), msg.partition(), permanence(e.rdkafka_error_code()));
                    *self.first.lock().unwrap_or_else(|p| p.into_inner()) = Some(cause);
                }
                self.ledger.delivered(generation as u64, None);
            }
        }
    }
}

/// Per sink topic, per partition: the offset after the last message acked there (-1: none).
type Acked = HashMap<String, Vec<i64>>;

/// Records message `offset` of `topic`/`partition` as acked in `acked`.
fn ack(acked: &mut Acked, topic: &str, partition: i32, offset: i64) {
    let offsets = match acked.get_mut(topic) {
        Some(o) => o,
        None => acked.entry(topic.to_string()).or_default(),
    };
    let p = partition as usize;
    if offsets.len() <= p {
        offsets.resize(p + 1, -1);
    }
    offsets[p] = offsets[p].max(offset + 1);
}

/// The sinks' deliveries by generation: the messages sent between two checkpoint cuts, the
/// first generation before the first cut. A checkpoint is taken at a cut and written while the
/// data loop goes on producing; its sink offsets must cover every message sent before
/// its cut, and none sent after it, whose offset would leave it unsuppressed when a restart
/// from the checkpoint replays it (ADR-0007). The producer is idempotent, so a partition keeps
/// its messages in the order they were sent: once each message of the cut's generation and the
/// ones before it is acked, the offsets after their highest are the cut's.
#[derive(Default)]
struct Ledger(std::sync::Mutex<Generations>);

#[derive(Default)]
struct Generations {
    /// What the generations settled so far acked.
    settled: Acked,
    /// The generations not settled yet, by number.
    open: BTreeMap<u64, Generation>,
}

#[derive(Default)]
struct Generation {
    sent: u64,
    delivered: u64,
    failed: u64,
    acked: Acked,
}

impl Ledger {
    fn lock(&self) -> std::sync::MutexGuard<'_, Generations> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A message of generation `g` is about to be sent: counted before it is, as its delivery
    /// may be reported (on another thread polling the producer) before `send` returns.
    fn sending(&self, g: u64) {
        self.lock().open.entry(g).or_default().sent += 1;
    }

    /// A message of generation `g` was delivered: acked at `offset` of `topic`/`partition`, or,
    /// `None`, failed.
    fn delivered(&self, g: u64, acked: Option<(&str, i32, i64)>) {
        let mut gens = self.lock();
        let gen = gens.open.entry(g).or_default();
        gen.delivered += 1;
        match acked {
            Some((topic, partition, offset)) => ack(&mut gen.acked, topic, partition, offset),
            None => gen.failed += 1,
        }
    }

    /// Once every message of generation `g` and of those before it is delivered: what they and
    /// every generation settled before acked (`g` and those before it are then settled), or how
    /// many of them failed. `None` while one is still in flight.
    fn settle(&self, g: u64) -> Option<std::result::Result<Acked, u64>> {
        let mut gens = self.lock();
        let upto = gens.open.range(..=g);
        let (mut in_flight, mut failed) = (false, 0);
        for (_, gen) in upto {
            in_flight |= gen.delivered < gen.sent;
            failed += gen.failed;
        }
        if in_flight {
            return None;
        }
        if failed > 0 {
            return Some(Err(failed));
        }
        let later = gens.open.split_off(&(g + 1));
        for (_, gen) in std::mem::replace(&mut gens.open, later) {
            for (topic, offsets) in gen.acked {
                for (p, next) in offsets.into_iter().enumerate() {
                    ack(&mut gens.settled, &topic, p as i32, next - 1);
                }
            }
        }
        Some(Ok(gens.settled.clone()))
    }
}

/// Errors a restart does not fix: replaying produces the same message to the same sink.
fn permanence(code: Option<rdkafka::types::RDKafkaErrorCode>) -> &'static str {
    use rdkafka::types::RDKafkaErrorCode::*;
    match code {
        Some(
            MessageSizeTooLarge
            | InvalidMessage
            | InvalidRecord
            | InvalidTopic
            | TopicAuthorizationFailed
            | ClusterAuthorizationFailed
            | PolicyViolation
            | UnsupportedVersion,
        ) => " (permanent: the restart replays this message and it fails again; change the sink's settings)",
        _ => "",
    }
}

/// `e`, a checkpoint's fatal error, with the first failed delivery if one is why.
fn with_delivery_failure(e: anyhow::Error, producer: &BaseProducer<Deliveries>) -> anyhow::Error {
    match producer.context().failed.load(Ordering::Relaxed) {
        0 => e,
        _ => anyhow!("{e:#}: {}", delivery_failure(producer)),
    }
}

/// The first failed delivery, for the error that stops the loop.
fn delivery_failure(producer: &BaseProducer<Deliveries>) -> String {
    let first = producer.context().first.lock().unwrap_or_else(|p| p.into_inner()).clone();
    producer_failure_detail(first.unwrap_or_else(|| "no detail".into()), producer.client().fatal_error())
}

/// Idempotent producers report a generic local Fatal on send and delivery. The client's
/// fatal error retains the broker's underlying code and reason (e.g. InitProducerId denied).
fn producer_failure_detail(detail: String, fatal: Option<(rdkafka::types::RDKafkaErrorCode, String)>) -> String {
    match fatal {
        Some((code, reason)) => format!("{detail}; producer fatal error {code:?}: {reason}{}", permanence(Some(code))),
        None => detail,
    }
}

/// librdkafka settings brrrrr's exactly-once output rests on, or that have flags of their own.
/// `message.timeout.ms` (`delivery.timeout.ms` is another name for it) is the fence: see
/// `producer_config`.
const RESERVED: [&str; 8] = [
    "bootstrap.servers",
    "group.id",
    "enable.auto.commit",
    "auto.offset.reset",
    "enable.idempotence",
    "transactional.id",
    "message.timeout.ms",
    "delivery.timeout.ms",
];

/// Parses `--*kafka-config` values in order: `key=value`, or `@<file>` of `key=value` lines
/// (blank lines and `#` comments skipped). Errors never quote a value: it may be a secret.
fn kafka_settings<'a>(args: impl IntoIterator<Item = &'a String>) -> Result<Vec<(String, String)>> {
    let mut out = vec![];
    for arg in args {
        let (text, origin) = match arg.strip_prefix('@') {
            Some(path) => (std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?, path),
            None => (arg.clone(), "--kafka-config"),
        };
        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((k, v)) = line.split_once('=') else { bail!("{origin}:{}: expected key=value", i + 1) };
            let k = k.trim();
            if RESERVED.contains(&k) {
                let why = match k {
                    "message.timeout.ms" | "delivery.timeout.ms" => {
                        " (half of --takeover: a message never reaches a sink after another instance may take over; \
                         set --takeover instead)"
                    }
                    _ => " (see --help)",
                };
                bail!("{origin}:{}: {k} is set by brrrrr{why} and cannot be overridden", i + 1);
            }
            out.push((k.to_string(), v.trim().to_string()));
        }
    }
    Ok(out)
}

/// A consumer's tunable defaults. librdkafka stops fetching a partition once 100,000 messages
/// are queued (`queued.min.messages`) and, by default, waits a whole second
/// (`fetch.queue.backoff.ms`) before fetching again, although the queue drains in a fraction
/// of that: a replay then crawls at about 100,000 messages a second per partition, whatever the
/// pipeline (bench/questdb: 42 s for 5M trades that take 6.9 s at 10 ms).
///
/// On a backlog librdkafka prefetches up to 100,000 messages or 64 MB of payload per queue, at
/// ~250 bytes of memory per 90-byte message, and with `--max-drift` each partition is a queue:
/// trade-size restarting over 3 topics x 12 partitions held its whole 3.7M-message backlog,
/// 1.1 GB, which glibc keeps once caught up. 20,000 messages and 4 MB (which also caps
/// `fetch.max.bytes`) bound a queue to a few MB: 530 MB aligned, 197 MB instead of 320 MB
/// unaligned, as fast. At 10,000 an aligned replay of one hot partition waits on Kafka 8% of
/// the time (fetches of `fetch.message.max.bytes`, 1 MB, barely keep up); at 20,000, 2%.
const CONSUMER_DEFAULTS: &[(&str, &str)] =
    &[("fetch.queue.backoff.ms", "10"), ("queued.min.messages", "20000"), ("queued.max.messages.kbytes", "4096")];

/// With `--max-drift`, a partition that is not known to be at its end holds the others back, and
/// librdkafka reports the end when the fetch that finds nothing returns: after
/// `fetch.wait.max.ms`, half a second by default, unless another partition of that broker has
/// records sooner. 50 ms instead, so a partition that goes quiet delays the others no longer.
const ALIGNED_DEFAULTS: &[(&str, &str)] = &[("fetch.wait.max.ms", "50")];

/// A client of the given brokers: brrrrr's tunable defaults, then the user's settings (which may
/// override them). Callers set the `RESERVED` settings, which the user's never contain.
fn client(brokers: &str, defaults: &[(&str, &str)], settings: &[(String, String)]) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", brokers);
    for (k, v) in defaults {
        c.set(*k, *v);
    }
    for (k, v) in settings {
        c.set(k, v);
    }
    c
}

/// The sinks' producer, whose settings the lease rests on: idempotent, and failing every message
/// not delivered `fence` after it was sent (`message.timeout.ms`, which also bounds retries and
/// a request in flight). `produce` sends nothing later than `fence` after the lease was renewed,
/// so nothing this instance sent reaches a sink later than `2 * fence`, `--takeover`, after it:
/// when a standing-by instance may take over. The user's settings never hold these
/// (`RESERVED`); they are set last all the same.
fn producer_config(brokers: &str, settings: &[(String, String)], fence: Duration) -> ClientConfig {
    let mut c = client(brokers, &[("linger.ms", "5")], settings);
    c.set("message.timeout.ms", fence.as_millis().to_string()).set("enable.idempotence", "true");
    c
}

/// Whether external stream `s` is a sink: a view's target. Any other is a source (`Engine::new`
/// checked each side's data_format).
pub fn is_sink(cat: &Catalog, s: &Stream) -> bool {
    cat.views.iter().any(|v| v.target == s.name)
}

fn setting<'a>(s: &'a Stream, k: &str) -> &'a str {
    s.settings.get(k).map_or("", |v| v.as_str())
}

/// The one cluster of `streams` (sources or sinks): --brokers, else their common `brokers`
/// setting. One consumer and one producer serve each direction, so streams on different
/// clusters are an error rather than silently read or written on the first one's.
fn brokers(a: &Args, streams: &[&Stream]) -> Result<String> {
    if let Some(b) = &a.brokers {
        return Ok(b.clone());
    }
    let mut all: Vec<&str> = streams.iter().map(|s| setting(s, "brokers")).collect();
    all.sort_unstable();
    all.dedup();
    match all.as_slice() {
        [one] => Ok(one.to_string()),
        [] => bail!("no streams to take brokers from: set --brokers"),
        more => {
            bail!("streams on different brokers ({}): run one cluster per direction, or set --brokers", more.join(", "))
        }
    }
}

/// At most one log line a second per kind, saying how many were left out: a producer-side
/// schema break or a broker outage would otherwise log at the topic's full rate.
struct Throttle {
    last: Option<Instant>,
    left_out: u64,
}

impl Throttle {
    const fn new() -> Throttle {
        Throttle { last: None, left_out: 0 }
    }

    /// `Some(n)`: log this one, and that `n` were left out since the last.
    fn allow(&mut self, now: Instant) -> Option<u64> {
        if self.last.is_some_and(|t| now.duration_since(t) < Duration::from_secs(1)) {
            self.left_out += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.left_out))
    }
}

fn left_out(n: u64) -> String {
    if n == 0 {
        String::new()
    } else {
        format!(" ({n} more like it not logged)")
    }
}

/// A poll timeout librdkafka can wait out: it waits whole milliseconds, and rdkafka spins through
/// what is left of a partial one instead of sleeping.
fn whole_ms(d: Duration) -> Duration {
    Duration::from_millis(u64::try_from(d.as_micros().div_ceil(1000)).unwrap_or(u64::MAX))
}

fn partitions<C: ClientContext>(c: &rdkafka::client::Client<C>, topic: &str) -> Result<Vec<i32>> {
    let md = c.fetch_metadata(Some(topic), TIMEOUT)?;
    let t = md.topics().first().ok_or(anyhow!("no metadata for {topic}"))?;
    if let Some(e) = t.error() {
        bail!("topic {topic}: {e:?}");
    }
    Ok(t.partitions().iter().map(|p| p.id()).collect())
}

/// The exact ASOF join's lateness in µs, `None` for an arrival join (which has none). Idle close
/// releases a held row once every source has been silent for `--idle-close`, which must outlast
/// how late a right row may be, or it would release rows a quote may still precede.
fn asof_lateness_us(asof: AsofArg, lateness_ms: u64, idle_close: Option<u64>) -> Result<Option<i64>> {
    if asof == AsofArg::Arrival {
        return Ok(None);
    }
    if let Some(idle) = idle_close.filter(|idle| idle.saturating_mul(1000) < lateness_ms) {
        bail!("--idle-close {idle} s is shorter than --asof-lateness-ms {lateness_ms}: it would release rows a quote may still precede");
    }
    Ok(Some(i64::try_from(lateness_ms)?.saturating_mul(1000)))
}

/// The group of each partition of `parts` (`topic`, `partition`) for `align::Merge::grouped`: the group of
/// the source streams reading its topic (`Engine::source_groups`). A topic read by streams of several groups
/// joins them: its rows are ordered against all of theirs. Groups come out as 0, 1, ... in the order of `parts`.
fn partition_groups(
    parts: &[(String, i32)],
    topic_streams: &BTreeMap<String, Vec<String>>,
    groups: &[Vec<String>],
) -> Vec<usize> {
    fn root(parent: &mut [usize], mut i: usize) -> usize {
        while parent[i] != i {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        i
    }
    let of_stream = |s: &String| groups.iter().position(|g| g.contains(s));
    let mut parent: Vec<usize> = (0..groups.len()).collect();
    for streams in topic_streams.values() {
        let mut known = streams.iter().filter_map(of_stream);
        if let Some(first) = known.next() {
            for g in known {
                let (a, b) = (root(&mut parent, first), root(&mut parent, g));
                parent[b] = a;
            }
        }
    }
    // a topic no view reads is a group of its own, after the others
    let mut ids: BTreeMap<Result<usize, &String>, usize> = BTreeMap::new();
    parts
        .iter()
        .map(|(topic, _)| {
            let g = topic_streams.get(topic).and_then(|ss| ss.iter().find_map(of_stream));
            let key = g.map(|g| root(&mut parent, g)).ok_or(topic);
            let next = ids.len();
            *ids.entry(key).or_insert(next)
        })
        .collect()
}

/// `--close-after-ms` as the margin of `Merge::vouched_until`, `None` when it is off.
fn clock_close_margin(
    close_after_ms: Option<u64>,
    max_drift: Option<u64>,
    partition_wait_ms: u64,
) -> Result<Option<i64>> {
    let Some(after) = close_after_ms else { return Ok(None) };
    if max_drift.is_none() {
        bail!("--close-after-ms needs --max-drift: only the aligned merge knows where each source partition ends");
    }
    if after <= partition_wait_ms {
        bail!(
            "--close-after-ms {after} must be more than --partition-wait-ms {partition_wait_ms}: \
             a record is read that long after it is produced"
        );
    }
    Ok(Some(i64::try_from(after)?))
}

/// Whether a replay's suppression of messages already in the sinks may end: its source partitions are read to the
/// ends they had at the start, and a pipeline that closes windows without a row (`--idle-close`,
/// `--close-after-ms`) has done so since, because windows an earlier run closed that way are written again by
/// this run's first such close.
fn suppression_over(closes_without_rows: bool, ends_reached: bool, idle_closed: bool, clock_closed: bool) -> bool {
    ends_reached && (!closes_without_rows || idle_closed || clock_closed)
}

/// Whether a clock close to `t_ms` reaches every window an earlier run closed on the clock. That run was killed
/// before this one caught up (`caught_up_ms`), and closed to at most `margin_ms` before its end: a close that only
/// reaches the replayed data's time would let suppression end before this run closes those windows again.
fn clock_close_covers_earlier_runs(t_ms: i64, caught_up_ms: i64, margin_ms: i64) -> bool {
    t_ms >= caught_up_ms.saturating_sub(margin_ms)
}

/// Idle close uses the source's last broker observation, so a stalled Iggy poll cannot close unseen data.
fn idle_close_target(now: i64, idle_ms: i64, proof: Option<Option<i64>>) -> Option<i64> {
    match proof {
        None => Some(now),
        Some(proof) => proof.map(|at| now.min(at)),
    }
    .map(|at| at.saturating_sub(idle_ms))
}

/// How often the clock close is tried: it only moves forward, and a replay would try it at every batch.
const CLOCK_CLOSE_EVERY: Duration = Duration::from_millis(20);

pub fn run(a: Args) -> Result<()> {
    if a.iggy.is_some() && !cfg!(feature = "iggy") {
        bail!("--iggy needs a binary built with --features iggy");
    }
    let clock_close = clock_close_margin(a.close_after_ms, a.max_drift, a.partition_wait_ms)?;
    let sql = std::fs::read_to_string(&a.sql)?;
    let mut cat: Catalog = brrrrr_core::sql::parse(&sql).map_err(|e| anyhow!("{}: {e}", a.sql.display()))?;
    let sink_topics = name_sinks(&mut cat, &a.sink_topic_prefix, &a.sink_topic_template)?;
    let mut engine = Engine::new(&cat).map_err(|e| anyhow!(e))?;
    engine.set_asof(match a.asof {
        AsofArg::Exact => Asof::Exact,
        AsofArg::Arrival => Asof::Arrival,
    });
    if let Some(us) = asof_lateness_us(a.asof, a.asof_lateness_ms, a.idle_close)? {
        engine.set_asof_lateness(us);
    }
    engine.set_close_threads(a.close_threads as usize);
    engine.set_close_groups(usize::try_from(a.close_groups).unwrap_or(usize::MAX));
    let external: Vec<&Stream> = cat.streams.values().filter(|s| s.kind == Kind::External).collect();
    let (sinks, srcs): (Vec<&Stream>, Vec<&Stream>) = external.iter().partition(|s| is_sink(&cat, s));
    let protos = match &a.proto {
        Some(p) => parse_proto(&std::fs::read_to_string(p)?).map_err(|e| anyhow!(e))?,
        None => Default::default(),
    };
    // topic -> the source streams reading it, with their decoders
    let mut sources: BTreeMap<String, Vec<(String, Decoder)>> = BTreeMap::new();
    for s in &srcs {
        let cols: Vec<_> = s.columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect();
        let decoder = if setting(s, "data_format") == "JSONEachRow" {
            Decoder::Json(cols)
        } else {
            let msg = setting(s, "format_schema").split_once(':').map_or("", |m| m.1);
            let proto = a.proto.as_ref().ok_or(anyhow!("{}: a ProtobufSingle source needs --proto", s.name))?;
            let fields = protos.get(msg).ok_or(anyhow!("{}: no message {msg} in {}", s.name, proto.display()))?;
            Decoder::Proto(Codec::new(fields, &cols))
        };
        sources.entry(setting(s, "topic").to_string()).or_default().push((s.name.clone(), decoder));
    }
    let pipeline = a.sql.file_stem().map_or("pipeline".into(), |s| s.to_string_lossy().into_owned());
    let group = a.group.clone().unwrap_or(format!("brrrrr-{pipeline}"));

    let source_settings =
        if a.iggy.is_some() { vec![] } else { kafka_settings(a.kafka_config.iter().chain(&a.source_kafka_config))? };
    let sink_settings = kafka_settings(a.kafka_config.iter().chain(&a.sink_kafka_config))?;
    let aligned_defaults = if a.max_drift.is_some() { ALIGNED_DEFAULTS } else { &[] };
    let consumer: Option<Arc<BaseConsumer>> = if a.iggy.is_none() {
        Some(Arc::new(
            client(&brokers(&a, &srcs)?, &[CONSUMER_DEFAULTS, aligned_defaults].concat(), &source_settings)
                // aligned consumption needs to know when a partition has nothing more to read
                .set("enable.partition.eof", if a.max_drift.is_some() { "true" } else { "false" })
                .set("group.id", &group)
                .set("enable.auto.commit", "false")
                // an expired checkpointed offset replays what is left rather than skipping to the end
                .set("auto.offset.reset", "earliest")
                .create()?,
        ))
    } else {
        None
    };
    let failed = Arc::new(AtomicU64::new(0));
    let out_brokers = match &a.sink_brokers {
        Some(b) => b.clone(),
        None => brokers(&a, &sinks)?,
    };
    let Timing { interval, takeover, fence } = Timing::of(a.interval, a.takeover)?;
    // shared with each checkpoint's thread, which polls it for the acks it waits for
    let producer: Arc<BaseProducer<Deliveries>> =
        Arc::new(producer_config(&out_brokers, &sink_settings, fence).create_with_context(Deliveries {
            failed: failed.clone(),
            first: std::sync::Mutex::new(None),
            ledger: Ledger::default(),
        })?);

    if !a.sink_topic_prefix.is_empty() || !a.sink_topic_template.is_empty() {
        create_topics(&out_brokers, &sink_settings, &rt_topics(&sink_topics))?;
    }
    let m = Arc::new(Metrics::default());
    m.watermark.store(i64::MIN, Ordering::Relaxed);
    m.event_time.store(i64::MIN, Ordering::Relaxed);
    m.alive_at.store(now_ms(), Ordering::Relaxed);
    metrics::serve(&a.metrics, m.clone()).with_context(|| format!("serving metrics on {}", a.metrics))?;
    // a shadow instance (--sink-topic-prefix or -template) writes other topics from the same
    // SQL: its own state
    let owner = match (a.sink_topic_prefix.as_str(), a.sink_topic_template.as_str()) {
        ("", "") => pipeline.clone(),
        (p, "") => format!("{pipeline}@{p}"),
        (_, t) => format!("{pipeline}@{t}"),
    };
    // an exact join's state (held rows) is another plan's: switching --asof starts from scratch
    // like a changed SQL instead of refusing every checkpoint; arrival keeps its prefixes
    let source_identity = crate::iggy::identity(a.iggy.as_deref())?;
    let checkpoint_sql = format!("{sql}{source_identity}");
    let key = match a.asof {
        AsofArg::Arrival => fnv64(checkpoint_sql.as_bytes()),
        AsofArg::Exact => fnv64(format!("{checkpoint_sql}\n-- asof exact").as_bytes()),
    };
    let stores = Stores::open(&a.checkpoints, &owner, key)?;
    let store = stores.checkpoints.clone();
    eprintln!("checkpoints: {store}");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, stop.clone())?;
    }

    // restore: engine state, source positions, and the dedup keys already written after it
    let start = match (a.fresh, a.restore_epoch) {
        (true, _) => Start::Fresh,
        (false, Some(e)) => Start::Epoch(e),
        (false, None) => Start::Newest,
    };
    // another instance may still be running this pipeline (a rolling deploy, a crashed pod that
    // is not dead yet): wait until it stopped, before reading any state or producing anything
    let Some(newest) = wait_until_free(&stores, takeover, &stop, &m)? else {
        eprintln!("stopped while standing by");
        return Ok(());
    };
    let restored = restore(store.as_ref(), &mut engine, start, &m)?;
    // the restored windows hold the late rows counted so far: brrrrr_late_events_total, a
    // Prometheus counter, counts this process's from 0, or a restart would read as that many new
    let late_before = engine.late();
    let fresh = restored.is_none();
    let (sources_at, mut sinks_at, restored_withhold) = restored.unwrap_or_default();
    // a checkpoint taken on other sink topics (a shadow's, adopted when switching from shadow):
    // those are not this run's to read back, and may be gone
    let before = sinks_at.len();
    sinks_at.retain(|(t, _, _)| sink_topics.contains(t));
    if sinks_at.len() < before {
        eprintln!(
            "the restored checkpoint names {} sink partitions this run does not write: not read back",
            before - sinks_at.len()
        );
    }
    let iggy = a
        .iggy
        .as_ref()
        .map(|connection| {
            let topics = sources
                .iter()
                .map(|(topic, streams)| {
                    (topic.clone(), streams.iter().any(|(s, _)| setting(&cat.streams[s], "seek_to") == "latest"))
                })
                .collect();
            let positions = sources_at.iter().map(|(t, p, o)| ((t.clone(), *p), *o)).collect();
            crate::iggy::Reader::start(connection, topics, positions).map(Arc::new)
        })
        .transpose()?;
    // what a start without a checkpoint must not write, kept in every checkpoint after it so that
    // a restart before the replay caught up withholds the same windows: the windows that began
    // before its sources lost records (partial), and per sink topic, those closed before the
    // topic lost records (unverifiable)
    let withhold = if fresh {
        let mut w = Withhold::none();
        let now = now_ms() as i64 * 1000;
        if !a.partial_windows {
            let parts = match &iggy {
                Some(reader) => reader
                    .origins
                    .iter()
                    .map(|(topic, partition, low, first)| Partition {
                        topic: topic.clone(),
                        partition: *partition,
                        low: *low,
                        first: *first,
                    })
                    .collect(),
                None => first_records(&brokers(&a, &srcs)?, &source_settings, sources.keys(), |_| true)?,
            };
            (w.start, w.awaiting) = source_origin(&parts, now);
        }
        if !a.unverified_history {
            let parts = first_records(&out_brokers, &sink_settings, rt_topics(&sink_topics).iter(), |_| true)?;
            w.topics = sink_bounds(&parts, now);
        }
        w
    } else {
        restored_withhold.unwrap_or_else(Withhold::none)
    };
    let mut withhold = withhold;
    // a sink partition whose records past the restored checkpoint's offset retention deleted:
    // what was written there since cannot be read back, nor told apart from what never was
    if !fresh && !a.unverified_history {
        let lost = |p: &Partition| lost_since(p, &sinks_at);
        let parts = first_records(&out_brokers, &sink_settings, rt_topics(&sink_topics).iter(), lost)?;
        let parts: Vec<_> = parts.into_iter().filter(lost).collect();
        for (topic, bound) in sink_bounds(&parts, now_ms() as i64 * 1000) {
            let at = withhold.topics.entry(topic).or_insert(bound);
            *at = (*at).max(bound);
        }
    }
    if !withhold.is_none() {
        eprintln!("withholding what a start without a checkpoint cannot write whole or once: {withhold}");
    }
    engine.withhold(withhold.start);
    let mut positions: HashMap<(String, i32), i64> = sources_at.iter().map(|(t, p, o)| ((t.clone(), *p), *o)).collect();
    m.alive_at.store(now_ms(), Ordering::Relaxed);
    let from: HashMap<(String, i32), i64> = sinks_at.iter().map(|(t, p, o)| ((t.clone(), *p), *o)).collect();
    // sink partitions the checkpoint does not cover (no checkpoint, or added since) are read
    // back as far as a replay reaches: a cutover from Proton or a crash before the first
    // checkpoint replays everything the sources still hold
    let topics: Vec<String> = sources.keys().cloned().collect();
    let horizon = if iggy.is_some() {
        None
    } else {
        replay_horizon(&brokers(&a, &srcs)?, &source_settings, &topics).unwrap_or_else(|e| {
            eprintln!("reading the sources' retention: {e}; reading the sinks back --lookback {}h", a.lookback);
            Some(hours_ms(a.lookback))
        })
    };
    let lookback = effective_lookback(a.lookback, horizon);
    match lookback {
        None => {
            eprintln!("a source is compacted or keeps its data without limit: the sinks are read back from their start")
        }
        Some(ms) if ms > hours_ms(a.lookback) => eprintln!(
            "the sources keep {}h: reading the sinks back that far (--lookback {}h)",
            (ms as u64).div_ceil(3_600_000),
            a.lookback
        ),
        Some(_) => {}
    }
    let uncovered = read_back_from(lookback, now_ms() as i64);
    let mut from: Vec<_> = from.into_iter().map(|((t, p), o)| (t, p, Offset::Offset(o))).collect();
    for t in &sink_topics {
        for p in partitions(producer.client(), t)? {
            if !from.iter().any(|(ft, fp, _)| ft == t && *fp == p) {
                from.push((t.clone(), p, uncovered));
            }
        }
        m.alive_at.store(now_ms(), Ordering::Relaxed); // one topic at a time: /health stays up
    }
    let Some((mut suppress, sink_ends)) = written_since(&out_brokers, &sink_settings, &from, &m, &stop)? else {
        eprintln!("stopped while reading the sinks back");
        return Ok(());
    };
    m.suppress_keys.store(suppress.len() as u64, Ordering::Relaxed);
    let mut tpl = TopicPartitionList::new();
    for (topic, streams) in &sources {
        let start = if streams.iter().any(|(s, _)| setting(&cat.streams[s], "seek_to") == "latest") {
            Offset::End
        } else {
            Offset::Beginning
        };
        let parts = match &iggy {
            Some(reader) => reader.parts.iter().filter(|(t, _)| t == topic).map(|(_, p)| *p).collect(),
            None => partitions(consumer.as_ref().unwrap().client(), topic)?,
        };
        for p in parts {
            tpl.add_partition_offset(
                topic,
                p,
                iggy.as_ref()
                    .and_then(|reader| reader.starts.get(&(topic.clone(), p)))
                    .or_else(|| positions.get(&(topic.clone(), p)))
                    .map_or(start, |o| Offset::Offset(*o)),
            )?;
        }
        m.alive_at.store(now_ms(), Ordering::Relaxed);
    }
    if let Some(consumer) = &consumer {
        consumer.assign(&tpl)?;
    }
    // aligned: each partition's own queue, split before the first poll
    let mut aligned = match a.max_drift {
        Some(_) => {
            let parts: Vec<(String, i32)> =
                tpl.elements().iter().map(|e| (e.topic().to_string(), e.partition())).collect();
            let queues = parts
                .iter()
                .map(|(t, p)| {
                    if iggy.is_some() {
                        Ok(None)
                    } else {
                        consumer
                            .as_ref()
                            .unwrap()
                            .split_partition_queue(t, *p)
                            .map(Some)
                            .ok_or_else(|| anyhow!("{t}/{p}: no partition queue"))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let wait = a.partition_wait_ms as i64;
            let topic_streams: BTreeMap<String, Vec<String>> =
                sources.iter().map(|(t, ss)| (t.clone(), ss.iter().map(|(s, _)| s.clone()).collect())).collect();
            let groups = partition_groups(&parts, &topic_streams, &engine.source_groups());
            let merge = align::Merge::grouped(&groups, wait, 30_000, now_ms() as i64);
            eprintln!("aligned consumption: {} partitions, one at its end waited for {wait} ms", parts.len());
            eprintln!(
                "aligned consumption: {} independent groups of partitions",
                groups.iter().max().map_or(0, |g| g + 1)
            );
            Some((parts, queues, merge))
        }
        None => None,
    };
    let assigned: HashMap<String, usize> =
        sources.keys().map(|t| (t.clone(), tpl.elements_for_topic(t).len())).collect();
    // the replay has re-produced everything written before this start once every source
    // partition is past the end it has now: no later output can match a suppressed key.
    // With --idle-close, an earlier run may also have closed windows that no row closes: they
    // stay suppressed until this run's first idle close, which (at a later time) closes them all
    let (mut replay_ends, mut replaying, mut idle_closed) = (vec![], true, false);
    // --close-after-ms: when it was last tried, to what time it closed, and whether it has closed since
    // the replay caught up (like an idle close, after which windows no row closes are written again)
    let (mut clock_tried, mut clock_closed_to, mut clock_closed) = (Instant::now(), i64::MIN, false);
    let mut caught_up_ms: Option<i64> = None; // when every source partition was first read to its start-time end
                                              // where each assigned partition starts, for the lag of partitions that deliver nothing
    let mut started_at: HashMap<(String, i32), i64> = HashMap::new();
    for e in tpl.elements() {
        let (low, high) = match &iggy {
            Some(reader) => (0, reader.ends[&(e.topic().to_string(), e.partition())]),
            None => consumer.as_ref().unwrap().fetch_watermarks(e.topic(), e.partition(), TIMEOUT)?,
        };
        let mut start = match e.offset() {
            Offset::Offset(o) => o,
            Offset::Beginning => low,
            _ => high,
        };
        // checked once assigned, as the replay begins: retention may have moved on during recovery.
        // Not fatal: the consumer goes on from the oldest record there (auto.offset.reset), and the
        // gap is said and counted for an alert (brrrrr_source_resume_gap_partitions)
        if positions.contains_key(&(e.topic().to_string(), e.partition())) {
            if let Some(why) = unresumable(start, low, high) {
                eprintln!(
                    "error: {}/{}: the checkpoint resumes at offset {start}, but {why}: going on from offset \
                     {low}, the windows open across the gap are written without the records in between \
                     (--restore-epoch <a newer epoch> resumes from another checkpoint; --fresh starts without \
                     one, withholding the windows its sources no longer hold whole)",
                    e.topic(),
                    e.partition()
                );
                m.source_resume_gaps.fetch_add(1, Ordering::Relaxed);
                m.source_resume_skipped.fetch_add(low.saturating_sub(start).max(0) as u64, Ordering::Relaxed);
                start = low;
            }
        }
        started_at.insert((e.topic().to_string(), e.partition()), start);
        if start < high {
            replay_ends.push(((e.topic().to_string(), e.partition()), high));
        }
        m.alive_at.store(now_ms(), Ordering::Relaxed);
    }
    // bookkeeping only: a failure there must not stop the pipeline, nor a slow broker hold it up
    let (c, c2) = (consumer.clone(), consumer.clone());
    let (iggy_end, iggy_count) = (iggy.clone(), iggy.clone());
    let mut bookkeeper = Bookkeeper::start(
        started_at,
        assigned,
        move |t, p| match &iggy_end {
            Some(reader) => reader.end(t, p),
            None => c.as_ref()?.fetch_watermarks(t, p, BOOKKEEPING).ok().map(|w| w.1),
        },
        move |t| {
            if let Some(reader) = &iggy_count {
                return reader.count(t);
            }
            let md = c2.as_ref()?.client().fetch_metadata(Some(t), BOOKKEEPING).ok()?;
            Some(md.topics().first().map_or(0, |t| t.partitions().len()))
        },
        m.clone(),
    );

    // claim the pipeline once recovery and assignment are done, before producing anything.
    // Claiming before recovery, which can take minutes, would leave the lease without
    // checkpoints meanwhile, for a standing-by instance to take over. What the claim holds is
    // what was restored, so a restart from it replays and suppresses exactly as a restart from
    // the restored checkpoint would
    let mut lease = claim(&stores, newest, |epoch| {
        Checkpoint::encode_of(&engine, epoch, &sources_at, &sinks_at, Some(&withhold)).map_err(|e| anyhow!(e))
    })?;
    m.standby.store(0, Ordering::Relaxed);
    eprintln!("claimed the pipeline with checkpoint {}", lease.epoch);
    eprintln!("epoch {}: {} messages already written will be suppressed", lease.epoch, suppress.values().sum::<u32>());
    let gc = collector(stores.clone());
    let batch_wait = Duration::from_millis(a.batch_wait_ms);
    let loop_started = now_ms();
    let mut out = Vec::new();
    let (mut consumer_log, decode_log) = (Throttle::new(), std::cell::RefCell::new(Throttle::new()));
    let (mut checkpoint_log, mut was_paused) = (Throttle::new(), false);
    let sink_ends = Arc::new(sink_ends);
    // the checkpoint being written, if any, and the generation of what is produced (`Ledger`):
    // one more at each checkpoint's cut
    let (mut pending, mut generation): (Option<Pending>, u64) = (None, 0);
    loop {
        // a checkpoint whose thread is done: applied first, so a lease lost to another instance
        // stops this one here, before it produces anything more. On a stop signal it is waited
        // for, so that the last checkpoint comes after it
        let stopping = stop.load(Ordering::Relaxed);
        if let Some(p) = pending.take_if(|p| stopping || p.thread.is_finished()) {
            let epoch = p.epoch;
            let done = p.thread.join().map_err(|_| anyhow!("checkpoint {epoch}: its thread panicked"))?;
            let done = done.map_err(|e| with_delivery_failure(e, &producer))?;
            let failures = match step(&mut lease, &stores, done.put, p.last, p.started) {
                Step::Written { failures } => failures,
                Step::Failed(e) => {
                    m.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
                    if let Some(n) = checkpoint_log.allow(Instant::now()) {
                        eprintln!("writing checkpoint {epoch}: {e:#}; trying again in {}s{}", a.interval, left_out(n));
                    }
                    continue;
                }
                Step::Stop(e) => return Err(e),
            };
            if failures > 0 {
                eprintln!("checkpoint {epoch} written after {failures} failed attempts");
            }
            m.checkpoints.fetch_add(1, Ordering::Relaxed);
            m.checkpoint_micros
                .store(done.at.saturating_duration_since(p.started).as_micros() as u64, Ordering::Relaxed);
            m.checkpoint_bytes.store(done.bytes as u64, Ordering::Relaxed);
            m.checkpoint_at.store(now_ms() / 1000, Ordering::Relaxed);
            if p.last {
                eprintln!("stopping after checkpoint {epoch}");
                return Ok(());
            }
            let mut commit = TopicPartitionList::new();
            for ((t, partition), o) in &p.positions {
                let _ = commit.add_partition_offset(t, *partition, Offset::Offset(*o));
            }
            if let Some(consumer) = &consumer {
                if let Err(e) = consumer.commit(&commit, CommitMode::Async) {
                    eprintln!("committing offsets for lag visibility: {e}");
                }
            }
            // found after an earlier checkpoint, whose positions this one's are past. Not on a stop
            // signal: the last checkpoint comes next, and the restart consumes them all the same
            if let Some((topic, now)) = bookkeeper.grown().filter(|_| !stopping) {
                bail!("{topic} grew to {now} partitions; restarting from checkpoint {epoch} to consume them");
            }
            bookkeeper.ask(Ask::Checkpointed(p.positions));
        }
        // a checkpoint once it is due and none is being written: its cut copies the state and the
        // source positions between two batches, and its thread waits for the sinks' acks of
        // what was produced before the cut, encodes and writes it while this loop goes on
        if pending.is_none() && lease.attempt(Instant::now(), interval, stopping) {
            let (started, epoch) = (Instant::now(), lease.next());
            let sources: Offsets = positions.iter().map(|((t, p), o)| (t.clone(), *p, *o)).collect();
            // a state too large to copy, and the last checkpoint, which nothing runs beside, are
            // encoded here from the live state once the sinks acked what was sent before the cut
            let cut = if copies_state(m.checkpoint_bytes.load(Ordering::Relaxed)) && !stopping {
                let mut checkpoint = Checkpoint::of(&engine, epoch, sources, vec![]);
                checkpoint.withhold = Some(withhold.clone());
                Cut::Copy { checkpoint, generation }
            } else {
                let ledger = &producer.context().ledger;
                let acked = acked_before(ledger, generation, |d| producer.poll(d), epoch, TIMEOUT)
                    .map_err(|e| with_delivery_failure(e, &producer))?;
                let sinks = sink_offsets(&sink_ends, &acked);
                let bytes = Checkpoint::encode_of(&engine, epoch, &sources, &sinks, Some(&withhold));
                Cut::Encoded { epoch, bytes: bytes.map_err(|e| anyhow!("checkpoint {epoch}: {e}"))? }
            };
            generation += 1;
            let (producer, ends, stores, gc) = (producer.clone(), sink_ends.clone(), stores.clone(), gc.clone());
            let thread = std::thread::Builder::new().name(format!("checkpoint-{epoch}")).spawn(move || {
                let ledger = &producer.context().ledger;
                let done = finish(cut, ledger, |d| producer.poll(d), &ends, &stores, TIMEOUT);
                // the oldest checkpoint goes as soon as this one is there, not once the loop sees it
                if matches!(done, Ok(Finished { put: Ok(()), .. })) {
                    let _ = gc.try_send(()); // a collection already queued covers this one too
                }
                done
            })?;
            let last = stopping;
            pending = Some(Pending { epoch, started, last, positions: positions.clone(), thread });
            m.checkpoint_stall_micros.store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
            if last {
                continue; // waited for at the top: nothing is produced after the last cut
            }
        }
        // past `fence`, what is sent may still be in flight when another instance takes over:
        // nothing more is produced until a checkpoint renews the lease (or its retry finds the
        // epoch taken and stops this one). A batch that would end past it is not started either:
        // `produce` stops the instance rather than send past `fence`
        let paused = lease.paused(Instant::now() + IDLE.max(batch_wait), fence);
        if paused != was_paused {
            was_paused = paused;
            if paused {
                let age = lease.renewed.elapsed().as_secs_f32();
                eprintln!("no checkpoint for {age:.1}s: producing nothing until one is written");
            }
        }
        if paused {
            m.alive_at.store(now_ms(), Ordering::Relaxed);
            producer.poll(Duration::from_millis(100));
            continue;
        }
        // a batch: what is queued, up to 1000 messages, and what arrives within --batch-wait-ms
        // of its first; each partition's consecutive run is one chunk
        let mut batch = Vec::new();
        let until = Instant::now() + IDLE.max(batch_wait);
        // when the batch stops waiting for more: `until` while it is empty
        let mut due = until;
        if let Some((parts, queues, merge)) = aligned.as_mut() {
            while batch.len() < 1000 && Instant::now() < until {
                let (now, clock) = (Instant::now(), now_ms() as i64);
                let (mut heard, mut fatal) = (false, None);
                for (i, q) in queues.iter().enumerate() {
                    heard |= merge.fill(i, clock, || loop {
                        let Some(q) = q else {
                            return match iggy.as_ref().unwrap().poll(i) {
                                Ok(polled) => polled,
                                Err(e) => {
                                    fatal = Some(e);
                                    align::Polled::Empty
                                }
                            };
                        };
                        match q.poll(Duration::ZERO) {
                            Some(Ok(msg)) => {
                                if let Some(payload) = msg.payload() {
                                    let ts = msg.timestamp().to_millis().unwrap_or(i64::MIN);
                                    break align::Polled::Msg(align::Msg {
                                        offset: msg.offset(),
                                        ts,
                                        payload: payload.to_vec(),
                                    });
                                } // a tombstone is no row
                            }
                            Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break align::Polled::Eof,
                            Some(Err(e)) => {
                                if matches!(e, rdkafka::error::KafkaError::MessageConsumptionFatal(_)) {
                                    fatal = Some(e.into());
                                } else {
                                    m.consumer_errors.fetch_add(1, Ordering::Relaxed);
                                    if let Some(n) = consumer_log.allow(now) {
                                        eprintln!("consumer: {e}{}", left_out(n));
                                    }
                                }
                                break align::Polled::Empty;
                            }
                            None => break align::Polled::Empty,
                        }
                    });
                }
                if let Some(e) = fatal {
                    return Err(e);
                }
                // the main queue still serves librdkafka's events; its partitions are all split
                if let Some(Err(e)) = consumer.as_ref().and_then(|c| c.poll(Duration::ZERO)) {
                    m.consumer_errors.fetch_add(1, Ordering::Relaxed);
                    if let Some(n) = consumer_log.allow(now) {
                        eprintln!("consumer: {e}{}", left_out(n));
                    }
                }
                let before = batch.len();
                while batch.len() < 1000 {
                    let Some((i, msg)) = merge.next(clock) else { break };
                    batch.push((parts[i].0.clone(), parts[i].1, msg.offset, msg.payload, msg.ts));
                }
                if before == 0 && !batch.is_empty() {
                    due = due.min(now + batch_wait);
                }
                if !heard && batch.len() == before {
                    let left = due.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    std::thread::sleep(left.min(Duration::from_millis(2)));
                }
            }
        }
        while aligned.is_none() && batch.len() < 1000 && Instant::now() < until {
            if let Some(reader) = &iggy {
                let before = batch.len();
                for (i, (topic, partition)) in reader.parts.iter().enumerate() {
                    if batch.len() == 1000 {
                        break;
                    }
                    if let align::Polled::Msg(msg) = reader.poll(i)? {
                        if batch.is_empty() {
                            due = due.min(Instant::now() + batch_wait);
                        }
                        batch.push((topic.clone(), *partition, msg.offset, msg.payload, msg.ts));
                    }
                }
                if batch.len() == before {
                    if Instant::now() >= due {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                continue;
            }
            match consumer.as_ref().unwrap().poll(whole_ms(due.saturating_duration_since(Instant::now()))) {
                Some(Err(e @ rdkafka::error::KafkaError::MessageConsumptionFatal(_))) => return Err(e.into()),
                // transient (a broker restarting): librdkafka recovers by itself
                Some(Err(e)) => {
                    m.consumer_errors.fetch_add(1, Ordering::Relaxed);
                    if let Some(n) = consumer_log.allow(Instant::now()) {
                        eprintln!("consumer: {e}{}", left_out(n));
                    }
                }
                Some(Ok(msg)) if msg.payload().is_none() => {} // a tombstone is no row
                Some(Ok(msg)) => {
                    if batch.is_empty() {
                        due = due.min(Instant::now() + batch_wait);
                    }
                    batch.push((
                        msg.topic().to_string(),
                        msg.partition(),
                        msg.offset(),
                        msg.payload().unwrap_or_default().to_vec(),
                        msg.timestamp().to_millis().unwrap_or(i64::MIN),
                    ));
                }
                None if !batch.is_empty() => break,
                None => {}
            }
        }
        if a.max_future_skew > 0 {
            let skew = i64::try_from(a.max_future_skew).unwrap_or(i64::MAX).saturating_mul(1_000_000);
            engine.set_time_limit(Some((now_ms() as i64).saturating_mul(1_000).saturating_add(skew)));
        }
        // what each view writes is produced once it has, not once the whole batch has run
        let renewed = lease.renewed;
        // what the batch costs: its time in the engine, producing excluded
        let (batch_started, closed_before, mut producing_for) = (Instant::now(), engine.closed(), Duration::ZERO);
        let mut producing = Producing::new(&mut out, |out: &mut Vec<Emit>| {
            let started = Instant::now();
            let produced =
                produce(&producer, generation, out, &mut suppress, &withhold.topics, &m, renewed + fence, &stop);
            producing_for += started.elapsed();
            produced
        });
        for run in batch.chunk_by(|x, y| (&x.0, x.1) == (&y.0, y.1)) {
            let (topic, partition) = (&run[0].0, run[0].1);
            // a source that had no record when this start had no checkpoint: its feed starts
            // with this one, and no window that began before it has all its input
            if let Some(i) = withhold.awaiting.iter().position(|t| t == topic) {
                withhold.awaiting.remove(i);
                withhold.start = withhold.start.max(run[0].4.saturating_mul(1000));
                engine.withhold(withhold.start);
                eprintln!("{topic} started: withholding {withhold}");
            }
            for (stream, decoder) in &sources[topic] {
                let rows = run.iter().filter_map(|msg| {
                    let r = decoder.decode(&msg.3);
                    if let Err(e) = &r {
                        m.decode_errors.fetch_add(1, Ordering::Relaxed);
                        if let Some(n) = decode_log.borrow_mut().allow(Instant::now()) {
                            eprintln!("{topic}/{partition}@{}: {e}{}", msg.2, left_out(n));
                        }
                    }
                    r.ok()
                });
                engine.insert(stream, rows.collect(), &mut producing);
            }
            producing.result()?;
            positions.insert((topic.clone(), partition), run[run.len() - 1].2 + 1);
        }
        if let Some(reader) = &iggy {
            m.consumer_errors.store(reader.failures(), Ordering::Relaxed);
        }
        m.received.fetch_add(batch.len() as u64, Ordering::Relaxed);
        if !batch.is_empty() {
            m.consumed_at.store(now_ms(), Ordering::Relaxed);
        }
        if let Some(idle) = a.idle_close {
            let idle_ms = i64::try_from(idle).unwrap_or(i64::MAX).saturating_mul(1000);
            let quiet_since = m.consumed_at.load(Ordering::Relaxed).max(loop_started) as i64;
            if !batch.is_empty() {
                bookkeeper.consumed();
            } else if now_ms() as i64 - quiet_since >= idle_ms && bookkeeper.quiet(&positions, Instant::now()) {
                // only a source read to its end is quiet: a lagging or unreachable one is not
                let proof = iggy.as_ref().map(|reader| reader.quiet_at());
                if let Some(at) = idle_close_target(now_ms() as i64, idle_ms, proof) {
                    engine.close_until(at.saturating_mul(1000), &mut producing);
                    idle_closed = true;
                }
            }
        }
        let ends_reached = replay_ends.iter().all(|(tp, end)| positions.get(tp).is_some_and(|p| p >= end));
        if replaying && ends_reached && caught_up_ms.is_none() {
            caught_up_ms = Some(now_ms() as i64);
        }
        if let (Some(margin), Some((_, _, merge))) = (clock_close, aligned.as_ref()) {
            if clock_tried.elapsed() >= CLOCK_CLOSE_EVERY {
                clock_tried = Instant::now();
                if let Some(ms) = merge.vouched_until(now_ms() as i64, margin) {
                    let t = ms.saturating_mul(1000);
                    if t > clock_closed_to {
                        engine.close_until(t, &mut producing);
                        clock_closed_to = t;
                        clock_closed |= caught_up_ms.is_some_and(|c| clock_close_covers_earlier_runs(ms, c, margin));
                    }
                }
            }
        }
        // what no flush sent (a view writes no sink without flushing; this is a safety net)
        producing.flush();
        producing.result()?;
        let pushed = producing.pushed;
        drop(producing);
        let closed = engine.closed();
        if !batch.is_empty() || pushed > 0 || closed > closed_before {
            batch_metrics(&m, batch_started.elapsed(), producing_for, pushed, closed - closed_before);
        }
        m.closed_groups.store(closed, Ordering::Relaxed);
        m.parallel_closes.store(engine.parallel_inserts(), Ordering::Relaxed);
        let closes_without_rows = a.idle_close.is_some() || clock_close.is_some();
        if replaying && suppression_over(closes_without_rows, ends_reached, idle_closed, clock_closed) {
            replaying = false;
            let left: u32 = suppress.values().sum();
            eprintln!("the replay caught up: {left} messages it did not write again are no longer suppressed");
            suppress = Suppress::new();
        }
        m.suppress_keys.store(suppress.len() as u64, Ordering::Relaxed);
        m.late.store(engine.late().saturating_sub(late_before), Ordering::Relaxed);
        m.withheld_partial.store(engine.withheld(), Ordering::Relaxed);
        let books = engine.books();
        m.books.store(books.books as u64, Ordering::Relaxed);
        m.books_awaiting_snapshot.store(books.awaiting_snapshot as u64, Ordering::Relaxed);
        m.book_stale.store(books.stale, Ordering::Relaxed);
        m.book_caught_up.store(books.caught_up, Ordering::Relaxed);
        m.book_malformed.store(books.malformed, Ordering::Relaxed);
        m.future.store(engine.future(), Ordering::Relaxed);
        m.null_time.store(engine.null_time(), Ordering::Relaxed);
        m.unfilled.store(engine.unfilled(), Ordering::Relaxed);
        m.asof_late_right.store(engine.asof_late_right(), Ordering::Relaxed);
        m.watermark.store(engine.min_watermark().unwrap_or(i64::MIN), Ordering::Relaxed);
        m.event_time.store(engine.max_event_time().unwrap_or(i64::MIN), Ordering::Relaxed);
        m.alive_at.store(now_ms(), Ordering::Relaxed);
        producer.poll(Duration::ZERO);
        if failed.load(Ordering::Relaxed) > 0 {
            bail!(
                "a sink delivery failed, {}; restarting from the last checkpoint replays it",
                delivery_failure(&producer)
            );
        }
    }
}

/// The engine's output in the data loop: what a view writes to its sinks is produced as soon as
/// the view has written it (`Output::flush`), not once the whole batch has run, so a 15s bar does
/// not wait for the 1h windows the same row closes. Once `produce` fails it stays stopped:
/// what it had not sent is gone, so nothing pushed after is produced either (it would leave a
/// hole a replay could not tell); the loop takes the error (`result`) and stops on it.
struct Producing<'a, F: FnMut(&mut Vec<Emit>) -> Result<()>> {
    out: &'a mut Vec<Emit>,
    produce: F,
    stopped: bool,
    failed: Option<anyhow::Error>,
    /// Messages the views wrote (`/metrics`).
    pushed: usize,
}

impl<'a, F: FnMut(&mut Vec<Emit>) -> Result<()>> Producing<'a, F> {
    fn new(out: &'a mut Vec<Emit>, produce: F) -> Self {
        Producing { out, produce, stopped: false, failed: None, pushed: 0 }
    }

    /// The error a flush met, once: the loop stops on it.
    fn result(&mut self) -> Result<()> {
        self.failed.take().map_or(Ok(()), Err)
    }
}

impl<F: FnMut(&mut Vec<Emit>) -> Result<()>> Output for Producing<'_, F> {
    fn push(&mut self, e: Emit) {
        self.pushed += 1;
        if !self.stopped {
            self.out.push(e);
        }
    }

    fn flush(&mut self) {
        if self.stopped {
            return;
        }
        if let Err(e) = (self.produce)(self.out) {
            (self.stopped, self.failed) = (true, Some(e));
            self.out.clear();
        }
    }
}

/// Records a batch in `m`: it took `took`, of which `producing` producing; its views wrote
/// `emitted` messages and closed `closed` window groups.
fn batch_metrics(m: &Metrics, took: Duration, producing: Duration, emitted: usize, closed: u64) {
    let engine = took.saturating_sub(producing).as_secs_f64();
    m.batches.engine.observe(engine);
    if closed > 0 {
        m.batches.closing.observe(engine);
    }
    m.batches.produce.observe(producing.as_secs_f64());
    m.batches.emitted.observe(emitted as f64);
    m.batches.closed.observe(closed as f64);
}

/// Messages already in the sinks, by `key_hash` of topic and dedup key (the payload for sinks
/// without the header), with how many times each is there.
type Suppress = HashMap<u128, u32>;

/// A 128-bit hash of a sink message's identity: 32 bytes a message in the suppression map
/// instead of two strings, and the raw bytes (no lossy UTF-8 conversion that could merge
/// distinct keys). Only compared within this process. Collisions: ~n^2 / 2^129.
fn key_hash(topic: &str, key: &[u8]) -> u128 {
    use std::hash::{Hash, Hasher};
    let half = |seed: u8| {
        let mut h = std::hash::DefaultHasher::new();
        (seed, topic, key).hash(&mut h);
        h.finish()
    };
    (u128::from(half(0)) << 64) | u128::from(half(1))
}

/// Whether `e` is one of the messages `suppress` holds (read back from its sink at start):
/// each is suppressed as many times as it was found there.
fn already_written(suppress: &mut Suppress, e: &Emit) -> bool {
    let key = e.headers.iter().find(|h| h.0 == DEDUP).map_or(e.payload.as_bytes(), |h| h.1.as_bytes());
    let std::collections::hash_map::Entry::Occupied(mut n) = suppress.entry(key_hash(&e.topic, key)) else {
        return false;
    };
    *n.get_mut() -= 1;
    if *n.get() == 0 {
        n.remove();
    }
    true
}

/// Produces `out`, except what `suppress` says is already written and the windows that ended
/// before their topic lost its records (`unverifiable`, by `Emit::window_end`: a window closed
/// before then cannot be told apart from one never written), as messages of generation
/// `generation` (`Ledger`). Nothing is sent at or after `until` (the lease may pass to another
/// instance `until - renewed` later, when what is sent then might still be in flight), nor
/// after a stop signal while the queue is full: either stops the instance without a checkpoint,
/// and the restart replays what was not sent.
#[allow(clippy::too_many_arguments)]
fn produce(
    producer: &BaseProducer<Deliveries>,
    generation: u64,
    out: &mut Vec<Emit>,
    suppress: &mut Suppress,
    unverifiable: &BTreeMap<String, i64>,
    m: &Metrics,
    until: Instant,
    stop: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    for e in out.drain(..) {
        if !suppress.is_empty() && already_written(suppress, &e) {
            m.suppressed.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if unverifiable.get(&*e.topic).is_some_and(|lost| e.window_end < *lost) {
            m.withheld_unverified.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let headers =
            e.headers.iter().fold(OwnedHeaders::new(), |h, (k, v)| h.insert(Header { key: k, value: Some(v) }));
        let mut record =
            BaseRecord::<(), _, _>::with_opaque_to(&e.topic, generation as usize).payload(&e.payload).headers(headers);
        let mut full = false; // the queue refused this message
        loop {
            if Instant::now() >= until {
                let why = if full {
                    "the sink queue stayed full past the lease"
                } else {
                    "the lease ran out while producing"
                };
                bail!("{why}: stopping before another instance may take over (the restart replays from the last checkpoint)");
            }
            if !full {
                producer.context().ledger.sending(generation); // offered once, whatever the retries
            }
            match producer.send(record) {
                Ok(()) => {
                    m.sent.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err((
                    rdkafka::error::KafkaError::MessageProduction(rdkafka::types::RDKafkaErrorCode::QueueFull),
                    r,
                )) => {
                    full = true;
                    if stop.load(Ordering::Relaxed) {
                        bail!(
                            "stopped while the sink queue was full: exiting without a checkpoint (the restart replays)"
                        );
                    }
                    producer.poll(Duration::from_millis(10));
                    record = r;
                }
                Err((e, _)) => {
                    return Err(anyhow!(producer_failure_detail(e.to_string(), producer.client().fatal_error())))
                }
            }
        }
    }
    Ok(())
}

/// A checkpoint written, or not: what its thread found.
#[derive(Debug)]
struct Finished {
    /// The encoded checkpoint's size.
    bytes: usize,
    /// When its thread was done.
    at: Instant,
    put: std::result::Result<(), NotWritten>,
}

/// The offsets the sinks acked for every message sent before a cut (generation `generation`
/// and before, `Ledger`), once each of them is acked, `poll`ing the producer for deliveries for
/// at most `timeout`. An error is fatal: a message sent before the cut failed or was not
/// delivered in time (the restart replays it).
fn acked_before(
    ledger: &Ledger,
    generation: u64,
    poll: impl Fn(Duration),
    epoch: u64,
    timeout: Duration,
) -> Result<Acked> {
    let deadline = Instant::now() + timeout;
    loop {
        match (ledger.settle(generation), deadline.checked_duration_since(Instant::now())) {
            (Some(Ok(acked)), _) => return Ok(acked),
            (Some(Err(n)), _) => bail!("{n} sink deliveries failed before checkpoint {epoch}"),
            (None, Some(left)) => poll(left.min(Duration::from_millis(10))),
            (None, None) => bail!(
                "the sinks did not ack what was sent before checkpoint {epoch} within {:.1}s",
                timeout.as_secs_f32()
            ),
        }
    }
}

/// The most the last checkpoint may have encoded to for the next one's cut to copy the state.
/// The copy lives until it is encoded and takes about 0.7 times the state (1.4 times the
/// encoded checkpoint on a trade-size state at a full feed's rate, `benches/memory.rs`): past this, a
/// checkpoint is encoded from the live state on the data thread, as when checkpoints were written on it, rather than
/// risk the pod's memory limit for a shorter stall. Only its write leaves the data thread.
const COPY_CHECKPOINT_UP_TO: u64 = 128 << 20;

/// Whether the next checkpoint's cut copies the state, given how large the last one encoded
/// to (0 before there is one).
fn copies_state(last_checkpoint_bytes: u64) -> bool {
    last_checkpoint_bytes <= COPY_CHECKPOINT_UP_TO
}

/// What a checkpoint's thread is given at its cut.
enum Cut {
    /// A copy of the state with the source positions, its sinks still to fill in: the thread
    /// waits for the acks of what was sent before the cut (generation `generation`), then
    /// encodes it.
    Copy { checkpoint: Checkpoint, generation: u64 },
    /// Checkpoint `epoch`, encoded on the data thread with its sinks (a state too large to copy,
    /// `copies_state`, or the last checkpoint on a stop signal): the thread only writes it.
    Encoded { epoch: u64, bytes: Vec<u8> },
}

/// Finishes a checkpoint off the data thread, which went on consuming and producing after its
/// cut, and creates it in `store`. A copy of the state gets its sinks' end offsets from the acks
/// of what was sent before the cut alone (`acked_before`, `poll`ing for at most `timeout`), so
/// they cover everything produced before the cut and nothing after it, and is encoded, then
/// dropped before the write. An error is fatal: a message sent before the cut failed or was not
/// delivered in time, or the state is too large to be read back. A failed write is not: see
/// `Finished::put` and `step`.
fn finish(
    cut: Cut,
    ledger: &Ledger,
    poll: impl Fn(Duration),
    ends: &SinkEnds,
    stores: &Stores,
    timeout: Duration,
) -> Result<Finished> {
    let (epoch, bytes) = match cut {
        Cut::Copy { mut checkpoint, generation } => {
            let epoch = checkpoint.epoch;
            checkpoint.sinks = sink_offsets(ends, &acked_before(ledger, generation, poll, epoch, timeout)?);
            (epoch, checkpoint.try_encode().map_err(|e| anyhow!("checkpoint {epoch}: {e}"))?)
        }
        Cut::Encoded { epoch, bytes } => (epoch, bytes),
    };
    let put = stores.put(epoch, &bytes);
    Ok(Finished { bytes: bytes.len(), at: Instant::now(), put })
}

/// A checkpoint between its cut, on the data thread, and the end of its thread (`finish`).
struct Pending {
    epoch: u64,
    /// When its cut was started: a written checkpoint renews the lease from then.
    started: Instant,
    /// Taken on a stop signal: the last one.
    last: bool,
    /// The source positions at its cut.
    positions: Positions,
    thread: std::thread::JoinHandle<Result<Finished>>,
}

/// Sink partitions and their end offsets when this instance started.
type SinkEnds = BTreeMap<(String, i32), i64>;

/// A checkpoint's sink offsets: after the last message acked there (of those sent before its
/// cut), else the end at start. Everything before is in the sink, and no offset is past what
/// was sent before the cut, which could leave written messages unsuppressed; one short of it
/// only suppresses a little more.
fn sink_offsets(ends: &SinkEnds, acked: &Acked) -> Vec<(String, i32, i64)> {
    let acked = |t: &str, p: i32| acked.get(t).and_then(|o| o.get(p as usize)).copied().unwrap_or(-1);
    ends.iter().map(|((t, p), end)| (t.clone(), *p, (*end).max(acked(t, *p)))).collect()
}

/// Keeps the newest 5 checkpoints (restore falls back through them) and leases, off the data
/// thread: a collection is a listing and deletes, and a failed one only leaves garbage. Asked
/// after each checkpoint; one already queued covers the next.
fn collector(stores: Stores) -> std::sync::mpsc::SyncSender<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
    std::thread::spawn(move || {
        while rx.recv().is_ok() {
            let collected = collect(stores.checkpoints.as_ref(), ".ckpt").and(collect(stores.owner.as_ref(), ".lease"));
            if let Err(e) = collected {
                eprintln!("deleting old checkpoints: {e:#}");
            }
        }
    });
    tx
}

/// Deletes all but the newest 5 objects named `<epoch><suffix>` (checkpoints, or leases), the
/// release markers of epochs older than those, and what crashed writes left behind. Only the
/// newest epoch's marker is ever read (and a claim consumes it), but a claim that found its
/// epoch taken stops before that: its marker would stay for ever.
fn collect(store: &dyn Store, suffix: &str) -> Result<()> {
    let names = store.list()?;
    let mut all: Vec<_> = names.iter().filter_map(|n| Some((epoch_of(n, suffix)?, n))).collect();
    all.sort_unstable_by_key(|e| std::cmp::Reverse(e.0));
    let (kept, old) = all.split_at(all.len().min(5));
    for (_, name) in old {
        store.delete(name)?;
    }
    if let Some((oldest, _)) = kept.last() {
        for name in names.iter().filter(|n| epoch_of(n, ".released").is_some_and(|e| e < *oldest)) {
            store.delete(name)?;
        }
    }
    store.tidy();
    Ok(())
}

/// The name of checkpoint `epoch`, of its lease (`Stores`), and of the marker saying its
/// instance released it.
fn ckpt(epoch: u64) -> String {
    format!("{epoch:020}.ckpt")
}

fn lease_of(epoch: u64) -> String {
    format!("{epoch:020}.lease")
}

fn released(epoch: u64) -> String {
    format!("{epoch:020}.released")
}

/// One partition as `first_records` found it: its log start, and its oldest retained record's
/// timestamp (µs) if it holds any. Timestamps must be event times: a source's as a feed handler
/// writes them, a sink's when each window was written, near its close.
#[derive(Debug, Clone)]
struct Partition {
    topic: String,
    partition: i32,
    low: i64,
    first: Option<i64>,
}

/// Every partition of `topics`, with the timestamp of its oldest retained record for those
/// `read` picks (`read` sees its log start).
fn first_records<'a>(
    brokers: &str,
    settings: &[(String, String)],
    topics: impl Iterator<Item = &'a String>,
    read: impl Fn(&Partition) -> bool,
) -> Result<Vec<Partition>> {
    let reader: BaseConsumer = client(brokers, CONSUMER_DEFAULTS, settings)
        .set("group.id", "brrrrr-origin")
        .set("enable.auto.commit", "false")
        .create()?;
    let (mut out, mut tpl, mut waiting) = (vec![], TopicPartitionList::new(), HashMap::new());
    for topic in topics {
        for p in partitions(reader.client(), topic)? {
            let (low, high) = reader.fetch_watermarks(topic, p, TIMEOUT)?;
            let part = Partition { topic: topic.clone(), partition: p, low, first: None };
            if high > low && read(&part) {
                tpl.add_partition_offset(topic, p, Offset::Offset(low))?;
                waiting.insert((topic.clone(), p), out.len());
            }
            out.push(part);
        }
    }
    if waiting.is_empty() {
        return Ok(out);
    }
    reader.assign(&tpl)?;
    let deadline = Instant::now() + TIMEOUT;
    while !waiting.is_empty() {
        if Instant::now() > deadline {
            bail!("reading the oldest retained records made no progress for 30 s: {:?}", waiting.keys());
        }
        let Some(msg) = reader.poll(Duration::from_millis(100)) else { continue };
        let msg = msg?;
        if let Some(i) = waiting.remove(&(msg.topic().to_string(), msg.partition())) {
            let ms = msg.timestamp().to_millis().ok_or(anyhow!(
                "{}/{}: a record without a timestamp",
                msg.topic(),
                msg.partition()
            ))?;
            out[i].first = Some(ms * 1000);
        }
    }
    Ok(out)
}

/// The sources' origin (µs): no window that began before it has all its input. A topic's feed
/// covers from the earliest oldest record of its partitions that lost none (its producer ran from
/// then: a partition that starts later had nothing to hold before), and no earlier than the latest
/// oldest retained record of those retention deleted records from (now, if one holds none any
/// more). The origin is the latest topic's. A topic with no record at all yet is returned to be
/// awaited: its first record sets its start.
fn source_origin(parts: &[Partition], now: i64) -> (i64, Vec<String>) {
    let mut by_topic: BTreeMap<&str, (Option<i64>, Option<i64>)> = BTreeMap::new();
    for p in parts {
        let (kept, lost) = by_topic.entry(&p.topic).or_default();
        match (p.low > 0, p.first) {
            (false, Some(t)) => *kept = Some(kept.map_or(t, |k| k.min(t))),
            (false, None) => {}
            (true, first) => *lost = Some(lost.unwrap_or(i64::MIN).max(first.unwrap_or(now))),
        }
    }
    let (mut origin, mut awaiting) = (i64::MIN, vec![]);
    for (topic, (kept, lost)) in by_topic {
        match kept.max(lost) {
            Some(start) => origin = origin.max(start),
            None => awaiting.push(topic.to_string()),
        }
    }
    (origin, awaiting)
}

/// Per sink topic that lost records (a partition's log start past 0), the latest oldest retained
/// record of the partitions that did (now, if one holds none any more): a window closed before it
/// cannot be told apart from one never written. A topic that lost nothing bounds nothing.
fn sink_bounds(parts: &[Partition], now: i64) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for p in parts.iter().filter(|p| p.low > 0) {
        // a partition that holds no record any more has no first one: nothing older than now
        let bound = out.entry(p.topic.clone()).or_insert(i64::MIN);
        *bound = (*bound).max(p.first.unwrap_or(now));
    }
    out
}

/// Whether retention emptied sink partition `p` past where `restored` (a checkpoint's sink
/// offsets) has it, or it lost records at all where it has none (as on a start without one):
/// what was written there is gone, so a replay cannot find it to suppress.
/// `sink_bounds` bounds the topics of those that did.
fn lost_since(p: &Partition, restored: &[(String, i32, i64)]) -> bool {
    restored.iter().find(|(t, n, _)| *t == p.topic && *n == p.partition).map_or(0, |r| r.2) < p.low
}

/// Why a source partition holding offsets `low..high` cannot resume at a checkpoint's `at`:
/// retention deleted records the restored state never read, or
/// the partition ends before it (deleted and created again, or truncated), so its records are
/// not the ones the checkpoint counted.
fn unresumable(at: i64, low: i64, high: i64) -> Option<String> {
    if at < low {
        Some(format!("it starts at {low}: retention deleted offsets {at} to {}", low - 1))
    } else if at > high {
        Some(format!("it ends at {high}: it was deleted and created again, or truncated"))
    } else {
        None
    }
}

/// Hours as milliseconds, saturating: an absurd --lookback means "all of it", not a negative.
fn hours_ms(h: u64) -> i64 {
    i64::try_from(h).unwrap_or(i64::MAX).saturating_mul(3_600_000)
}

/// How far back (ms) a replay can produce windows: the longest retention of the source topics.
/// `None`: unbounded, a topic is compacted, keeps its data forever or does not say.
fn replay_horizon(brokers: &str, settings: &[(String, String)], topics: &[String]) -> Result<Option<i64>> {
    use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
    let admin: AdminClient<_> = client(brokers, &[], settings).create()?;
    let specs: Vec<ResourceSpecifier> = topics.iter().map(|t| ResourceSpecifier::Topic(t)).collect();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let opts = AdminOptions::new().request_timeout(Some(TIMEOUT));
    let mut configs = vec![];
    for (t, r) in topics.iter().zip(rt.block_on(admin.describe_configs(&specs, &opts))?) {
        let config = r.map_err(|e| anyhow!("{t}: {e}"))?;
        let get = |k: &str| config.get(k).and_then(|e| e.value.clone());
        configs.push((get("cleanup.policy"), get("retention.ms")));
    }
    Ok(longest_retention(configs))
}

/// The longest retention (ms) of topics given by their `cleanup.policy` and `retention.ms`:
/// None if one of them is compacted or keeps its records without limit.
fn longest_retention(configs: impl IntoIterator<Item = (Option<String>, Option<String>)>) -> Option<i64> {
    let mut longest = Some(0);
    for (policy, retention) in configs {
        let compacted = policy.is_some_and(|p| p.contains("compact"));
        let ms = retention.and_then(|v| v.parse::<i64>().ok()).filter(|ms| *ms >= 0 && !compacted);
        longest = longest.zip(ms).map(|(l, ms)| l.max(ms));
    }
    longest
}

/// The sink history (ms) to read back without a checkpoint: `--lookback` hours, raised to the
/// replay horizon. `None`: all of it.
fn effective_lookback(lookback_h: u64, horizon: Option<i64>) -> Option<i64> {
    horizon.map(|h| h.max(hours_ms(lookback_h)))
}

/// Where a sink partition no checkpoint covers is read back from, at `now` (ms): `lookback` ms
/// earlier, a time carried as `OffsetTail` and resolved on the sink brokers (`written_since`);
/// its start if there is no bound, or the bound reaches back to 1970 or past it (a saturated
/// --lookback), which is no time a broker could look up.
fn read_back_from(lookback: Option<i64>, now: i64) -> Offset {
    match lookback.map(|ms| now.saturating_sub(ms)) {
        Some(at) if at > 0 => Offset::OffsetTail(at),
        _ => Offset::Beginning,
    }
}

const TAKEN: &str = "another instance is running this pipeline";

/// Why a checkpoint was not written.
#[derive(Debug)]
enum NotWritten {
    /// Its epoch exists (this object, `<store>/<name>`): another instance holds the pipeline
    /// (or an attempt of this one that seemed to fail landed after all). This instance must stop.
    Taken(String),
    /// The store failed. Nothing says the write did not land, so it is tried again under the
    /// same epoch.
    Failed(anyhow::Error),
}

/// A pipeline's checkpoint stores: `checkpoints`, its revision's (`<owner>/<hash of the SQL and
/// --asof>`), and `owner`, the directory above every revision's. The lease is the owner's: each
/// epoch, a claim's or a checkpoint's, is first taken there (`<epoch>.lease`, holding `holder`),
/// so of two instances of any revisions, exactly one takes it. A changed SQL or --asof starts
/// from its own state, never beside the instance it replaces: each would write the windows the
/// other does.
#[derive(Clone)]
struct Stores {
    owner: Arc<dyn Store>,
    checkpoints: Arc<dyn Store>,
    /// This instance, as its leases name it: an attempt of its own that seemed to fail and
    /// landed after all is told apart from another instance's lease.
    holder: Arc<str>,
}

impl Stores {
    fn open(location: &str, owner: &str, revision: u64) -> Result<Stores> {
        // the revision's first: a store refused says where the checkpoints go
        let checkpoints = Arc::from(store::open(location, &[owner, &format!("{revision:016x}")])?);
        Ok(Stores {
            owner: Arc::from(store::open(location, &[owner])?),
            checkpoints,
            holder: format!("{revision:016x} {}", store::unique()).into(),
        })
    }

    /// Creates checkpoint `epoch` once its lease is this instance's.
    fn put(&self, epoch: u64, bytes: &[u8]) -> std::result::Result<(), NotWritten> {
        let taken = |s: &dyn Store, name: String| NotWritten::Taken(format!("{s}/{name}"));
        match self.owner.create(&lease_of(epoch), self.holder.as_bytes()) {
            Ok(true) => {}
            Ok(false) => match self.owner.get(&lease_of(epoch)) {
                Ok(holder) if holder == self.holder.as_bytes() => {} // an attempt of this one's
                Ok(_) => return Err(taken(self.owner.as_ref(), lease_of(epoch))),
                Err(e) => return Err(NotWritten::Failed(e)),
            },
            Err(e) => return Err(NotWritten::Failed(e)),
        }
        match self.checkpoints.create(&ckpt(epoch), bytes) {
            Ok(true) => Ok(()),
            Ok(false) => Err(taken(self.checkpoints.as_ref(), ckpt(epoch))),
            Err(e) => Err(NotWritten::Failed(e)),
        }
    }

    /// The newest epoch taken, of any revision, and whether its instance released it. A build
    /// before the owner's leases wrote this revision's checkpoints and releases only.
    fn newest(&self) -> Result<Option<(u64, bool)>> {
        let (owner, own) = (self.owner.list()?, self.checkpoints.list()?);
        let leases = owner.iter().filter_map(|n| epoch_of(n, ".lease"));
        let Some(newest) = leases.chain(own.iter().filter_map(|n| epoch_of(n, ".ckpt"))).max() else {
            return Ok(None);
        };
        Ok(Some((newest, owner.contains(&released(newest)) || own.contains(&released(newest)))))
    }
}

/// When to checkpoint, under which epoch, and whether producing must wait for a checkpoint.
///
/// A failed write is tried again every interval under the same epoch: it may have landed after
/// all, or a standing-by instance may have claimed that epoch meanwhile, and either way the
/// retry finds it taken and this instance stops. Moving on to the next epoch instead would let
/// this instance run next to one that claimed the failed epoch.
#[derive(Debug, PartialEq)]
struct Lease {
    /// The epoch last written, or last attempted if that failed.
    epoch: u64,
    /// When the cut of the checkpoint last written was taken: what the lease counts from. A
    /// checkpoint being written renews nothing until it is.
    renewed: Instant,
    /// When a checkpoint was last attempted.
    attempted: Instant,
    /// Failed attempts since the last checkpoint written.
    failures: u64,
}

impl Lease {
    fn claimed(epoch: u64, at: Instant) -> Lease {
        Lease { epoch, renewed: at, attempted: at, failures: 0 }
    }

    fn due(&self, now: Instant, interval: Duration) -> bool {
        now.saturating_duration_since(self.attempted) >= interval
    }

    /// Whether to write a checkpoint now: once due, and at once on a stop signal (the last).
    fn attempt(&self, now: Instant, interval: Duration, stopping: bool) -> bool {
        stopping || self.due(now, interval)
    }

    /// The epoch the next attempt creates.
    fn next(&self) -> u64 {
        if self.failures == 0 {
            self.epoch + 1
        } else {
            self.epoch
        }
    }

    /// Checkpoint `next()`, cut at `started`, was written.
    fn written(&mut self, started: Instant) {
        *self = Lease::claimed(self.next(), started);
    }

    /// Checkpoint `next()` failed to be written at `at`.
    fn failed(&mut self, at: Instant) {
        (self.epoch, self.attempted, self.failures) = (self.next(), at, self.failures + 1);
    }

    /// Whether the lease is `fence` old: from then on another instance may take over, so this
    /// one produces nothing more until it writes a checkpoint.
    fn paused(&self, now: Instant, fence: Duration) -> bool {
        now.saturating_duration_since(self.renewed) >= fence
    }
}

/// The lease's timing: a checkpoint every `interval`; a standing-by instance takes over
/// `takeover` (by default three intervals) after the last checkpoint it saw, and a running
/// instance produces only `fence` (half of it) after its last checkpoint: a message still
/// queued by then fails (and the instance restarts from its checkpoint) rather than reach the
/// sink after another instance took over.
#[derive(Debug, PartialEq)]
struct Timing {
    interval: Duration,
    takeover: Duration,
    fence: Duration,
}

impl Timing {
    /// From --interval and --takeover (seconds).
    fn of(interval: u64, takeover: Option<u64>) -> Result<Timing> {
        let takeover = Duration::from_secs(takeover.unwrap_or(3 * interval));
        let interval = Duration::from_secs(interval);
        if takeover <= 2 * interval {
            bail!("--takeover must be more than twice --interval");
        }
        Ok(Timing { interval, takeover, fence: takeover / 2 })
    }
}

/// Claims the pipeline after epoch `newest`, the newest `wait_until_free` found free (whichever
/// checkpoint was restored): creates checkpoint `newest + 1` holding `state(epoch)` (an error
/// from it is returned as is), never overwriting it, and consumes `newest`'s release marker. An
/// instance that claimed meanwhile (one recovering at the same time, of this revision or another,
/// or a paused one whose lease ran out, checkpointing before it produces again) has taken that
/// epoch: this one stops.
fn claim(stores: &Stores, newest: u64, state: impl FnOnce(u64) -> Result<Vec<u8>>) -> Result<Lease> {
    let (epoch, at) = (newest + 1, Instant::now());
    match stores.put(epoch, &state(epoch)?) {
        Ok(()) => {}
        Err(NotWritten::Taken(name)) => bail!("{name} exists: {TAKEN}"),
        Err(NotWritten::Failed(e)) => return Err(e.context(format!("claiming the pipeline with checkpoint {epoch}"))),
    }
    // consumed, if it was there
    let _ = stores.owner.delete(&released(newest));
    let _ = stores.checkpoints.delete(&released(newest));
    Ok(Lease::claimed(epoch, at))
}

/// What an attempt to write a checkpoint leads to.
#[derive(Debug)]
enum Step {
    /// Written, after `failures` failed attempts: the lease is renewed.
    Written { failures: u64 },
    /// The store failed: only the lease ages, the same epoch is tried again once due, and
    /// producing waits once the lease is `fence` old. The store may be slow or away for a while.
    Failed(anyhow::Error),
    /// This instance stops.
    Stop(anyhow::Error),
}

/// What becomes of checkpoint `lease.next()`, started at `started`, whose write was `put`. The
/// last one (`last`, taken on a stop signal) is marked released once written, so that a
/// standing-by instance takes over at once instead of waiting out --takeover; not written, the
/// instance stops without it.
fn step(
    lease: &mut Lease,
    stores: &Stores,
    put: std::result::Result<(), NotWritten>,
    last: bool,
    started: Instant,
) -> Step {
    let epoch = lease.next();
    match put {
        Ok(()) => {}
        Err(NotWritten::Taken(name)) if lease.failures > 0 => {
            return Step::Stop(anyhow!(
                "{name} exists: an attempt that failed landed after all, or {TAKEN}; restarting from it"
            ))
        }
        Err(NotWritten::Taken(name)) => return Step::Stop(anyhow!("{name} exists: {TAKEN}")),
        Err(NotWritten::Failed(e)) if last => {
            return Step::Stop(e.context("stopping without a last checkpoint (the restart replays)"))
        }
        Err(NotWritten::Failed(e)) => {
            lease.failed(Instant::now());
            return Step::Failed(e);
        }
    }
    let failures = lease.failures;
    lease.written(started);
    if last {
        if let Err(e) = stores.owner.create(&released(epoch), &[]) {
            eprintln!("releasing checkpoint {epoch}: {e}");
        }
    }
    Step::Written { failures }
}

/// The consumer lag over the whole assignment: each partition's `end` (None: unknown, counted
/// as no lag) less its position, or where it started for a stuck partition that never
/// delivered.
fn lag(
    started_at: &HashMap<(String, i32), i64>,
    positions: &HashMap<(String, i32), i64>,
    end: impl Fn(&str, i32) -> Option<i64>,
) -> i64 {
    let behind = |(tp, start): (&(String, i32), &i64)| {
        let at = positions.get(tp).unwrap_or(start);
        end(&tp.0, tp.1).map_or(0, |end| (end - at).max(0))
    };
    started_at.iter().map(behind).sum()
}

/// A source topic with more `partitions` (None: unknown) than this run consumes, and how many
/// it has: new partitions are only assigned at start, so the instance restarts to consume them.
fn grown<'a>(
    assigned: &HashMap<&'a String, usize>,
    positions: &HashMap<(String, i32), i64>,
    partitions: impl Fn(&str) -> Option<usize>,
) -> Option<(&'a String, usize)> {
    assigned.iter().find_map(|(topic, n)| {
        let now = partitions(topic)?;
        let known = positions.keys().filter(|(t, _)| t == *topic).count().max(*n);
        (now > known).then_some((*topic, now))
    })
}

/// Whether every assigned partition is read to its `end` (None: unknown, and so not: a lagging
/// or unreachable source is not quiet).
fn caught_up(
    started_at: &HashMap<(String, i32), i64>,
    positions: &HashMap<(String, i32), i64>,
    end: impl Fn(&str, i32) -> Option<i64>,
) -> bool {
    started_at.iter().all(|(tp, start)| end(&tp.0, tp.1).is_some_and(|end| *positions.get(tp).unwrap_or(start) >= end))
}

/// Source positions by topic and partition.
type Positions = HashMap<(String, i32), i64>;

/// What the data thread asks the bookkeeping thread, with the source positions it has.
enum Ask {
    /// A checkpoint was written: the lag, and whether a source topic grew.
    Checkpointed(Positions),
    /// Whether every source partition is read to its end (--idle-close), asked as the `n`th.
    Quiet(u64, Positions),
}

/// The bookkeeping of the data loop, on a thread of its own. Its broker queries (a partition's
/// end, a topic's partitions) each wait up to `BOOKKEEPING` on an unreachable broker: once per
/// source partition, on the data thread, they stalled it for minutes on a large assignment,
/// without a sign of life for `/health`. The data thread only asks, never waits: a checkpoint's
/// ask made while the thread is busy is dropped, and the next checkpoint's covers it; a quiet
/// one is made only once the last is answered.
struct Bookkeeper {
    asks: std::sync::mpsc::SyncSender<Ask>,
    /// A source topic found with more partitions than this run consumes, and how many.
    grown: Arc<std::sync::Mutex<Option<(String, usize)>>>,
    /// The newest `Quiet` ask found read to its end (0: none, or already taken).
    found: Arc<AtomicU64>,
    /// The newest `Quiet` ask answered, either way.
    answered: Arc<AtomicU64>,
    /// The `Quiet` asks made, the first whose answer still counts (none made before rows were
    /// last consumed: the positions they were asked with are behind), and when the last was made.
    asked: u64,
    counts_from: u64,
    asked_at: Instant,
}

impl Bookkeeper {
    /// Starts the thread for the partitions `started_at` (where each started) of the topics
    /// `assigned` (how many partitions each), querying `end` and `partitions`; it keeps
    /// `m.consumer_lag`.
    fn start(
        started_at: Positions,
        assigned: HashMap<String, usize>,
        end: impl Fn(&str, i32) -> Option<i64> + Send + 'static,
        partitions: impl Fn(&str) -> Option<usize> + Send + 'static,
        m: Arc<Metrics>,
    ) -> Bookkeeper {
        let (asks, rx) = std::sync::mpsc::sync_channel::<Ask>(2);
        let book = Bookkeeper {
            asks,
            grown: Arc::default(),
            found: Arc::default(),
            answered: Arc::default(),
            asked: 0,
            counts_from: 1,
            asked_at: Instant::now(),
        };
        let (grown_to, found, answered) = (book.grown.clone(), book.found.clone(), book.answered.clone());
        std::thread::spawn(move || {
            let assigned: HashMap<&String, usize> = assigned.iter().map(|(t, n)| (t, *n)).collect();
            while let Ok(ask) = rx.recv() {
                match ask {
                    Ask::Checkpointed(positions) => {
                        m.consumer_lag.store(lag(&started_at, &positions, &end), Ordering::Relaxed);
                        if let Some((topic, n)) = grown(&assigned, &positions, &partitions) {
                            *grown_to.lock().unwrap_or_else(|p| p.into_inner()) = Some((topic.clone(), n));
                        }
                    }
                    Ask::Quiet(n, positions) => {
                        if caught_up(&started_at, &positions, &end) {
                            found.store(n, Ordering::Relaxed);
                        }
                        answered.store(n, Ordering::Relaxed);
                    }
                }
            }
        });
        book
    }

    /// Asks without waiting; false if the thread is still busy with earlier asks.
    fn ask(&self, ask: Ask) -> bool {
        self.asks.try_send(ask).is_ok()
    }

    fn grown(&self) -> Option<(String, usize)> {
        self.grown.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Rows were consumed: the positions any earlier `Quiet` ask was made with are behind.
    fn consumed(&mut self) {
        self.counts_from = self.asked + 1;
    }

    /// While nothing is consumed: whether the sources were found read to their end at
    /// `positions` (true once per answer). Asks again a second after the last ask, once that
    /// one is answered: a thread slower than that still has its answers heard.
    fn quiet(&mut self, positions: &Positions, now: Instant) -> bool {
        let n = self.asked;
        let quiet =
            n >= self.counts_from && self.found.compare_exchange(n, 0, Ordering::Relaxed, Ordering::Relaxed).is_ok();
        if now.saturating_duration_since(self.asked_at) >= Duration::from_secs(1)
            && self.answered.load(Ordering::Relaxed) >= n
            && self.ask(Ask::Quiet(n + 1, positions.clone()))
        {
            (self.asked, self.asked_at) = (n + 1, now);
        }
        quiet
    }
}

/// Waits until no other instance holds the pipeline, of any revision (`Stores`): its newest
/// epoch was released (its instance stopped on SIGTERM), or none newer has appeared for
/// `takeover`, as timed here. An instance that holds it checkpoints every `--interval` and
/// stops producing `takeover / 2` after its last checkpoint, so it cannot produce once this
/// returns. The newest epoch then (0: none), which the claim comes after: any taken since is
/// another instance's. None: stopped meanwhile.
fn wait_until_free(
    stores: &Stores,
    takeover: Duration,
    stop: &std::sync::atomic::AtomicBool,
    m: &Metrics,
) -> Result<Option<u64>> {
    let mut seen: Option<(u64, Instant)> = None;
    loop {
        let Some((newest, released)) = stores.newest()? else { return Ok(Some(0)) };
        match seen {
            _ if released => {
                eprintln!("checkpoint {newest} was released by its instance: taking over");
                return Ok(Some(newest));
            }
            Some((n, at)) if n == newest && at.elapsed() >= takeover => {
                eprintln!("no checkpoint after {newest} for {}s: taking over", takeover.as_secs());
                return Ok(Some(newest));
            }
            Some((n, _)) if n == newest => {}
            _ => {
                eprintln!(
                    "checkpoint {newest} may be another instance's: standing by until none is newer for {}s",
                    takeover.as_secs()
                );
                seen = Some((newest, Instant::now()));
            }
        }
        if stop.load(Ordering::Relaxed) {
            return Ok(None);
        }
        m.standby.store(1, Ordering::Relaxed);
        m.alive_at.store(now_ms(), Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The checkpoint `run` starts from.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Start {
    /// The newest checkpoint that restores, falling back past those that do not.
    Newest,
    Epoch(u64),
    Fresh,
}

/// Offsets by topic and partition, as a checkpoint lists them.
type Offsets = Vec<(String, i32, i64)>;

/// What `restore` found: the source and sink offsets of the checkpoint restored, if any, and
/// what it withholds (None: none recorded, `Checkpoint::of`).
type Restored = Option<(Offsets, Offsets, Option<Withhold>)>;

/// The epoch of `name` if it is `<epoch:020><suffix>`, as `ckpt` and `released` write them:
/// exactly 20 digits, so that a hand-placed `7.ckpt` or `+7.ckpt` is not taken for epoch 7.
fn epoch_of(name: &str, suffix: &str) -> Option<u64> {
    let digits = name.strip_suffix(suffix)?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.parse().ok())?
}

/// The checkpoints among `names` (`<epoch:020>.ckpt`), newest first.
fn epochs(names: impl IntoIterator<Item = String>) -> Vec<(u64, String)> {
    let mut v: Vec<_> = names.into_iter().filter_map(|n| Some((epoch_of(&n, ".ckpt")?, n))).collect();
    v.sort_unstable_by_key(|p: &(u64, String)| std::cmp::Reverse(p.0));
    v
}

/// Restores `start` into `engine`. With `Start::Newest`, a checkpoint that does not decode or
/// does not fit the plan is skipped for the one before it: an older checkpoint replays more,
/// and its older sink offsets make the replay suppress more, never less. Reading the store
/// failing is an error, not a reason to skip. `Engine::restore` checks a whole snapshot before
/// restoring any of it, so a refused one leaves the engine as it was.
fn restore(store: &dyn Store, engine: &mut Engine, start: Start, m: &Metrics) -> Result<Restored> {
    let all = epochs(store.list()?);
    let newest = all.first().map_or(0, |c| c.0);
    let candidates: Vec<_> = match start {
        Start::Fresh => {
            eprintln!("--fresh: not restoring any checkpoint (the newest is epoch {newest})");
            return Ok(None);
        }
        Start::Epoch(e) => {
            let one: Vec<_> = all.into_iter().filter(|c| c.0 == e).collect();
            if one.is_empty() {
                bail!("--restore-epoch {e}: no such checkpoint under {store}");
            }
            one
        }
        Start::Newest if all.is_empty() => return Ok(None),
        Start::Newest => all,
    };
    let mut refused = vec![];
    let mut stale = vec![]; // (name, version) refused for no reason but an older, unread version
    for (e, name) in candidates {
        // read once: `stale_version` and `decode` both look at these same bytes, which are
        // dropped once decoded, before the state is restored
        let bytes = store.get(&name)?;
        let old_version = Checkpoint::stale_version(&bytes);
        // a panic (a corruption the checks missed) refuses this checkpoint as an error does, and
        // the walk goes on to the one before it, whose restore replaces any state it had set
        let restored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Checkpoint::decode(&bytes).and_then(|mut c| {
                let positions = (std::mem::take(&mut c.sources), std::mem::take(&mut c.sinks), c.withhold.take());
                c.restore(engine).map(|()| positions)
            })
        }))
        .unwrap_or_else(|_| Err("decoding or restoring it panicked (above)".into()));
        match restored {
            Ok(positions) => {
                eprintln!("restored checkpoint {e}");
                return Ok(Some(positions));
            }
            Err(err) => {
                eprintln!("checkpoint {e} ({store}/{name}) does not restore: {err}");
                m.checkpoints_refused.fetch_add(1, Ordering::Relaxed);
                refused.push(e);
                if let Some(v) = old_version {
                    stale.push((name, v));
                }
            }
        }
    }
    // Every retained checkpoint failed for the same reason, and it is one a later build cannot
    // fix by trying harder: each was written by a strictly older version than this build reads
    // (ADR-0008: a deploy of a new format starts every pipeline fresh). That is exactly what an
    // automatic restart on `Start::Newest` hits after such a deploy, so it starts fresh instead
    // of crash-looping, and prunes what will never restore again. A checkpoint refused for any
    // other reason (corruption, a layout drift at the same version, a newer version from a
    // build ahead of this one) is not touched, and still a human's call, as before.
    if start == Start::Newest && !refused.is_empty() && stale.len() == refused.len() {
        let versions: Vec<u16> = stale.iter().map(|(_, v)| *v).collect();
        eprintln!(
            "every retained checkpoint under {store} is an older format this build no longer reads (versions \
             {versions:?}, this build reads versions {}): pruning {} of them and starting fresh",
            brrrrr_core::checkpoint::readable(),
            stale.len()
        );
        for (name, _) in &stale {
            store.delete(name)?;
        }
        return Ok(None);
    }
    bail!(
        "no checkpoint under {store} restores (epochs {refused:?}, reasons above): --restore-epoch <epoch> \
         restores another, --fresh starts without one"
    )
}

/// The topic `template` makes of `topic` (see --sink-topic-template): each `{N}` is the Nth
/// dot-separated part of `topic`, counted from 1; "" leaves `topic` as it is.
/// Sets each sink's `topic` in `cat` to the topic this run writes (`--sink-topic-prefix` or
/// `-template` applied), before the engine is planned from it: every message the engine makes
/// then carries its physical topic, whichever path makes it (a row, an idle close), and is
/// suppressed and withheld under that topic. Returns the sinks' topics.
fn name_sinks(cat: &mut Catalog, prefix: &str, template: &str) -> Result<Vec<String>> {
    if !prefix.is_empty() && !template.is_empty() {
        bail!("--sink-topic-prefix and --sink-topic-template are two ways to name the sinks: pass one");
    }
    let mut topics = vec![];
    for s in cat.streams.values_mut() {
        // `is_sink`, on the views beside the streams borrowed
        if s.kind != Kind::External || !cat.views.iter().any(|v| v.target == s.name) {
            continue;
        }
        // a sink without a topic is the engine's to refuse, not to be given one
        let Some(topic) = s.settings.get_mut("topic").filter(|t| !t.is_empty()) else { continue };
        let named = sink_topic(template, topic).map_err(|e| anyhow!("--sink-topic-template: {e}"))?;
        *topic = format!("{prefix}{named}");
        topics.push(topic.clone());
    }
    Ok(topics)
}

fn sink_topic(template: &str, topic: &str) -> std::result::Result<String, String> {
    if template.is_empty() {
        return Ok(topic.to_string());
    }
    let parts: Vec<&str> = topic.split('.').collect();
    let (mut out, mut rest) = (String::new(), template);
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let close = rest[open..].find('}').ok_or(format!("{template}: an unclosed {{"))? + open;
        let n: usize = rest[open + 1..close].parse().map_err(|_| format!("{template}: {{N}} takes a number"))?;
        let part = n.checked_sub(1).and_then(|i| parts.get(i)).ok_or(format!("{topic} has no part {n}"))?;
        out.push_str(part);
        rest = &rest[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn rt_topics(topics: &[String]) -> Vec<String> {
    let mut t = topics.to_vec();
    t.sort();
    t.dedup();
    t
}

/// Creates the (shadow) sink topics that do not exist yet: one partition, broker defaults.
fn create_topics(brokers: &str, settings: &[(String, String)], topics: &[String]) -> Result<()> {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists;
    let admin: AdminClient<_> = client(brokers, &[], settings).create()?;
    let new: Vec<_> = topics.iter().map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(-1))).collect();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    for r in rt.block_on(admin.create_topics(&new, &AdminOptions::new()))? {
        match r {
            Ok(_) | Err((_, TopicAlreadyExists)) => {}
            Err((t, e)) => bail!("creating {t}: {e}"),
        }
    }
    Ok(())
}

/// Dedup keys (or payloads, for sinks without the header) written after the given offsets.
/// All partitions are assigned at once and read until each reaches the end it had at start, by
/// a message or, when its last offsets hold none, by its end of file; the read fails only if it
/// makes no progress for 30 s. A start given as `OffsetTail(ms)` is
/// a time: the first offset at or after it; `Beginning` is the partition's start.
fn written_since(
    brokers: &str,
    settings: &[(String, String)],
    from: &[(String, i32, Offset)],
    m: &Metrics,
    stop: &std::sync::atomic::AtomicBool,
) -> Result<Option<(Suppress, SinkEnds)>> {
    let reader: Arc<BaseConsumer> = Arc::new(
        client(brokers, CONSUMER_DEFAULTS, settings)
            .set("group.id", "brrrrr-recovery")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest") // retention may delete the start while we read
            .set("enable.partition.eof", "true")
            .create()?,
    );
    let mut by_time = TopicPartitionList::new();
    for (topic, p, start) in from {
        if let Offset::OffsetTail(ms) = start {
            by_time.add_partition_offset(topic, *p, Offset::Offset(*ms))?;
        }
    }
    let mut starts: HashMap<(String, i32), i64> = HashMap::new();
    if by_time.count() > 0 {
        for e in reader.offsets_for_times(by_time, TIMEOUT)?.elements() {
            // a partition's error must not read as "nothing that recent": nothing would be
            // suppressed there
            e.error()
                .map_err(|err| anyhow!("finding where to read {}/{} back from: {err}", e.topic(), e.partition()))?;
            match e.offset() {
                Offset::Offset(o) => drop(starts.insert((e.topic().to_string(), e.partition()), o)),
                Offset::End => {} // nothing that recent
                o => bail!("finding where to read {}/{} back from: {o:?}", e.topic(), e.partition()),
            }
        }
        m.alive_at.store(now_ms(), Ordering::Relaxed);
    }
    let (mut tpl, mut ends, mut all_ends) = (TopicPartitionList::new(), HashMap::new(), SinkEnds::new());
    for (topic, p, start) in from {
        let (low, end) = reader.fetch_watermarks(topic, *p, TIMEOUT)?;
        all_ends.insert((topic.clone(), *p), end);
        m.alive_at.store(now_ms(), Ordering::Relaxed); // one partition at a time: /health stays up
        let start = match start {
            // a checkpoint's offset past the end: the topic was deleted and created again, and all
            // it holds now was written since
            Offset::Offset(o) if *o > end => Some(low),
            Offset::Offset(o) => Some(*o),
            Offset::Beginning => Some(low),
            _ => starts.get(&(topic.clone(), *p)).copied(),
        };
        if let Some(start) = start.filter(|s| end > *s) {
            tpl.add_partition_offset(topic, *p, Offset::Offset(start))?;
            ends.insert((topic.clone(), *p), end);
        }
    }
    let mut keys = Suppress::new();
    if ends.is_empty() {
        return Ok(Some((keys, all_ends)));
    }
    reader.assign(&tpl)?;
    // each partition's own queue, split before the first poll: the end of a partition names its
    // number only, and it is how a partition whose last offsets hold no message (a transaction's
    // commit or abort marker, an aborted message) is known to be read
    let queues = ends
        .keys()
        .map(|(t, p)| {
            let q = reader.split_partition_queue(t, *p).ok_or_else(|| anyhow!("{t}/{p}: no partition queue"))?;
            Ok(((t.clone(), *p), q))
        })
        .collect::<Result<Vec<_>>>()?;
    let (mut progress, mut read) = (Instant::now(), 0u64);
    let mut found = |msg: &rdkafka::message::BorrowedMessage<'_>| {
        let dedup = msg.headers().and_then(|h| h.iter().find(|h| h.key == DEDUP).and_then(|h| h.value));
        *keys.entry(key_hash(msg.topic(), dedup.or(msg.payload()).unwrap_or_default())).or_default() += 1;
        read += 1;
        if read.is_multiple_of(1_000_000) {
            eprintln!("reading the sinks back: {read} messages so far");
        }
    };
    while !ends.is_empty() {
        if stop.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let mut heard = false;
        for (tp, q) in &queues {
            // up to 10,000 messages of a partition at a time
            for _ in 0..10_000 {
                let Some(end) = ends.get(tp).copied() else { break };
                match q.poll(Duration::ZERO) {
                    None => break,
                    Some(Ok(msg)) => {
                        found(&msg);
                        if msg.offset() + 1 >= end {
                            ends.remove(tp);
                        }
                    }
                    // at the partition's end, which is at or past the one it had at start
                    Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => drop(ends.remove(tp)),
                    Some(Err(e @ rdkafka::error::KafkaError::MessageConsumptionFatal(_))) => return Err(e.into()),
                    Some(Err(e)) => {
                        eprintln!("reading the sinks back: {e}");
                        break;
                    }
                }
                heard = true;
            }
        }
        // the main queue serves librdkafka's events (its partitions are all split), and waits a
        // little when no partition had anything
        match reader.poll(if heard { Duration::ZERO } else { Duration::from_millis(10) }) {
            Some(Err(e @ rdkafka::error::KafkaError::MessageConsumptionFatal(_))) => return Err(e.into()),
            Some(Err(e)) => eprintln!("reading the sinks back: {e}"),
            // fetched before its partition was split
            Some(Ok(msg)) => {
                found(&msg);
                let tp = (msg.topic().to_string(), msg.partition());
                if ends.get(&tp).is_some_and(|end| msg.offset() + 1 >= *end) {
                    ends.remove(&tp);
                }
                heard = true;
            }
            None => {}
        }
        if heard {
            progress = Instant::now();
            m.alive_at.store(now_ms(), Ordering::Relaxed);
        } else if progress.elapsed() > TIMEOUT {
            bail!("reading the sinks back made no progress for 30 s: {ends:?}");
        }
    }
    Ok(Some((keys, all_ends)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failed InitProducerId is surfaced as local Fatal on send: keep the broker's cause
    /// so an authorization outage can be fixed without correlating broker logs.
    #[test]
    fn fatal_producer_diagnostic_names_the_underlying_authorization_failure() {
        use rdkafka::types::RDKafkaErrorCode::{ClusterAuthorizationFailed, Fatal};
        let error = rdkafka::error::KafkaError::MessageProduction(Fatal).to_string();
        let reason = "InitProducerId failed: Broker: Cluster authorization failed";
        let detail = producer_failure_detail(error.clone(), Some((ClusterAuthorizationFailed, reason.into())));
        assert!(detail.starts_with(&error), "{detail}");
        assert!(detail.contains("ClusterAuthorizationFailed"), "{detail}");
        assert!(detail.contains(reason), "{detail}");
        assert!(detail.contains("permanent"), "a restart cannot repair authorization: {detail}");
    }

    #[test]
    fn producer_diagnostic_preserves_delivery_location_and_nonfatal_errors() {
        use rdkafka::types::RDKafkaErrorCode::ProducerFenced;
        let delivery = "slippage.v1/0: Message production error: Fatal";
        let detail = producer_failure_detail(delivery.into(), Some((ProducerFenced, "producer fenced".into())));
        assert!(detail.starts_with(delivery), "{detail}");
        assert!(detail.contains("ProducerFenced") && detail.contains("producer fenced"), "{detail}");
        assert!(!detail.contains("permanent"), "{detail}");
        assert_eq!(producer_failure_detail("message timed out".into(), None), "message timed out");
    }

    /// TLS and SASL/SCRAM need OpenSSL inside librdkafka; without it these settings are refused.
    #[test]
    fn clients_accept_tls_and_scram_settings() {
        let cases = [("ssl", None), ("sasl_ssl", Some("SCRAM-SHA-512")), ("sasl_plaintext", Some("SCRAM-SHA-256"))];
        for (protocol, mechanism) in cases {
            let mut settings = vec![("security.protocol".to_string(), protocol.to_string())];
            if let Some(m) = mechanism {
                settings.extend(
                    [("sasl.mechanisms", m), ("sasl.username", "u"), ("sasl.password", "p")]
                        .map(|(k, v)| (k.to_string(), v.to_string())),
                );
            }
            let r: Result<BaseConsumer, _> = client("127.0.0.1:1", &[], &settings).create();
            assert!(r.is_ok(), "{protocol} {mechanism:?}: {:?}", r.err());
        }
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn kafka_settings_come_inline_or_from_files_in_order() {
        let dir = std::env::temp_dir().join(format!("brrrrr-kafka-settings-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("sasl.properties");
        std::fs::write(&file, "# the sinks' cluster\n\nsasl.username = svc \nsasl.password=p=w=d\n").unwrap();
        let args = strings(&["security.protocol=sasl_ssl", &format!("@{}", file.display()), "linger.ms=20"]);
        let got = kafka_settings(&args).unwrap();
        let want = [
            ("security.protocol", "sasl_ssl"),
            ("sasl.username", "svc"),
            ("sasl.password", "p=w=d"), // only the first '=' separates
            ("linger.ms", "20"),
        ];
        assert_eq!(got, want.map(|(k, v)| (k.to_string(), v.to_string())));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn kafka_settings_refuse_what_brrrrr_sets_and_never_echo_values() {
        for k in RESERVED {
            let e = kafka_settings(&strings(&[&format!("{k}=x")])).unwrap_err().to_string();
            assert!(e.contains(k) && e.contains("cannot be overridden"), "{e}");
        }
        let e = kafka_settings(&strings(&["hunter2"])).unwrap_err().to_string();
        assert!(e.contains("expected key=value") && !e.contains("hunter2"), "{e}");
        let e = kafka_settings(&strings(&["@/nonexistent/brrrrr.properties"])).unwrap_err().to_string();
        assert!(e.contains("/nonexistent/brrrrr.properties"), "{e}");
    }

    /// Checkpoints are found by name, newest first by epoch; other objects under the prefix
    /// (a stray upload, a lease) are neither restored nor garbage-collected.
    #[test]
    fn epochs_are_the_checkpoints_newest_first() {
        let names = ["00000000000000000009.ckpt", "notes.txt", "00000000000000000010.ckpt", "x.ckpt"];
        let got = epochs(names.map(String::from));
        let got: Vec<_> = got.iter().map(|(e, n)| (*e, n.as_str())).collect();
        assert_eq!(got, [(10, "00000000000000000010.ckpt"), (9, "00000000000000000009.ckpt")]);
    }

    /// Only names as `ckpt` and `released` write them are epochs: a short, signed, long or
    /// spaced name is not taken for one (two names would parse to the same epoch).
    #[test]
    fn an_epoch_is_exactly_20_digits() {
        assert_eq!(epoch_of("00000000000000000007.ckpt", ".ckpt"), Some(7));
        assert_eq!(epoch_of("18446744073709551615.released", ".released"), Some(u64::MAX));
        for name in [
            "7.ckpt",
            "+0000000000000000007.ckpt",
            "-0000000000000000007.ckpt",
            " 0000000000000000007.ckpt",
            "000000000000000000007.ckpt",
            "0000000000000000007.ckpt",
            "00000000000000000007.released",
            "99999999999999999999.ckpt", // past u64::MAX
            ".ckpt",
        ] {
            assert_eq!(epoch_of(name, ".ckpt"), None, "{name}");
        }
        assert!(epochs(["7.ckpt", "+0000000000000000007.ckpt"].map(String::from)).is_empty());
    }

    /// A sink partition no checkpoint covers is read back from a time `lookback` before now,
    /// or from its start without a bound or with one reaching back to 1970 (a saturated
    /// --lookback): never a time before it, which no broker can look up.
    #[test]
    fn the_sinks_are_read_back_from_a_time_after_1970_or_from_their_start() {
        let now = 1_750_000_000_000;
        assert_eq!(read_back_from(Some(3_600_000), now), Offset::OffsetTail(now - 3_600_000));
        assert_eq!(read_back_from(Some(now - 1), now), Offset::OffsetTail(1));
        assert_eq!(read_back_from(Some(now), now), Offset::Beginning);
        assert_eq!(read_back_from(Some(hours_ms(u64::MAX)), now), Offset::Beginning);
        assert_eq!(read_back_from(None, now), Offset::Beginning);
    }

    /// Without a checkpoint, the sinks are read back as far as a replay reaches: the longest
    /// source retention if longer than --lookback, all of the sinks if a source is unbounded.
    #[test]
    fn the_lookback_reaches_as_far_as_the_sources_keep_data() {
        let h = 3_600_000;
        assert_eq!(effective_lookback(48, Some(24 * h)), Some(48 * h));
        assert_eq!(effective_lookback(48, Some(72 * h)), Some(72 * h));
        assert_eq!(effective_lookback(48, Some(0)), Some(48 * h));
        assert_eq!(effective_lookback(48, None), None);
        assert_eq!(effective_lookback(u64::MAX, Some(h)), Some(i64::MAX));
    }

    /// Suppression keys tell topics and keys apart, and bytes that are not UTF-8 apart (they
    /// used to be converted lossily, which merged them).
    #[test]
    fn a_replay_reaches_the_longest_retention_unless_a_source_keeps_its_data_for_ever() {
        let topic = |policy: &str, ms: &str| (Some(policy.to_string()), Some(ms.to_string()));
        assert_eq!(longest_retention([topic("delete", "3600000"), topic("delete", "7200000")]), Some(7_200_000));
        assert_eq!(longest_retention([topic("delete", "0")]), Some(0));
        assert_eq!(longest_retention([topic("delete", "3600000"), topic("delete", "-1")]), None);
        assert_eq!(longest_retention([topic("compact,delete", "3600000")]), None);
        assert_eq!(longest_retention([topic("delete", "3600000"), (None, None)]), None);
    }

    #[test]
    fn key_hashes_separate_topics_keys_and_raw_bytes() {
        assert_eq!(key_hash("t", b"k"), key_hash("t", b"k"));
        assert_ne!(key_hash("a", b"bc"), key_hash("ab", b"c"));
        assert_ne!(key_hash("t1", b"k"), key_hash("t2", b"k"));
        assert_ne!(key_hash("t", b"\xff"), key_hash("t", b"\xfe"));
        assert_ne!(String::from_utf8_lossy(b"\xff"), "");
        assert_eq!(String::from_utf8_lossy(b"\xff"), String::from_utf8_lossy(b"\xfe"), "what lossy decoding did");
    }

    /// Golden: both halves of the hash, the seed 0 one on top. Only compared within a process
    /// (the sinks are read back on every start), so a toolchain whose `DefaultHasher` changes
    /// only changes these values; a change under the pinned toolchain is a change of the hash.
    #[test]
    fn key_hashes_are_pinned() {
        assert_eq!(key_hash("t", b"k"), 0xc35f8d374143604e_a5329034f526928c);
        assert_eq!(key_hash("m.1m", b"1756684800000000|BTC"), 0x68b1bb075b39b556_f3e7193ba6c62e51);
        assert_eq!(key_hash("", b""), 0x2f439e240465b724_baecc3cc45e998b3);
    }

    #[test]
    fn delivery_errors_a_restart_cannot_fix_are_called_permanent() {
        use rdkafka::types::RDKafkaErrorCode::*;
        for code in [MessageSizeTooLarge, TopicAuthorizationFailed, InvalidRecord, PolicyViolation] {
            assert!(permanence(Some(code)).contains("permanent"), "{code:?}");
        }
        for code in [Some(MessageTimedOut), Some(NotLeaderForPartition), Some(BrokerTransportFailure), None] {
            assert_eq!(permanence(code), "", "{code:?}");
        }
    }

    #[test]
    fn a_throttle_logs_once_a_second_and_counts_what_it_left_out() {
        let (mut t, t0) = (Throttle::new(), Instant::now());
        assert_eq!(t.allow(t0), Some(0));
        assert_eq!(t.allow(t0 + Duration::from_millis(10)), None);
        assert_eq!(t.allow(t0 + Duration::from_millis(999)), None);
        assert_eq!(t.allow(t0 + Duration::from_millis(1000)), Some(2));
        assert_eq!(t.allow(t0 + Duration::from_millis(1500)), None);
        assert_eq!(t.allow(t0 + Duration::from_secs(9)), Some(1));
        assert_eq!(left_out(0), "");
        assert_eq!(left_out(3), " (3 more like it not logged)");
    }

    /// Each direction has one cluster: streams on different brokers are an error, not read or
    /// written on the first one's; --brokers overrides them all.
    #[test]
    fn streams_on_different_brokers_are_an_error_unless_overridden() {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            a: Args,
        }
        let args = |extra: &[&str]| {
            let base = ["run", "p.sql", "--proto", "p.proto", "--checkpoints", "memory:///"];
            <Cli as clap::Parser>::try_parse_from(base.iter().chain(extra)).unwrap().a
        };
        let cat = |b2: &str| {
            brrrrr_core::sql::parse(&format!(
                "CREATE EXTERNAL STREAM a (x float64) SETTINGS type = 'kafka', brokers = 'b1:9092', topic = 'a';
                 CREATE EXTERNAL STREAM b (x float64) SETTINGS type = 'kafka', brokers = '{b2}', topic = 'b';
                 CREATE EXTERNAL STREAM c (x float64) SETTINGS type = 'kafka', brokers = 'b1:9092', topic = 'c'"
            ))
            .unwrap()
        };
        let same = cat("b1:9092");
        let streams: Vec<&Stream> = same.streams.values().collect();
        assert_eq!(brokers(&args(&[]), &streams).unwrap(), "b1:9092");
        let split = cat("b2:9092");
        let streams: Vec<&Stream> = split.streams.values().collect();
        let err = brokers(&args(&[]), &streams).unwrap_err().to_string();
        assert!(err.contains("different brokers (b1:9092, b2:9092)"), "{err}");
        assert_eq!(brokers(&args(&["--brokers", "o:1"]), &streams).unwrap(), "o:1");
        let err = brokers(&args(&[]), &[]).unwrap_err().to_string();
        assert!(err.contains("no streams to take brokers from: set --brokers"), "{err}");
    }

    /// A checkpoint's sink offsets come from the delivery reports: after the last message
    /// acked in a partition, else the partition's end when the instance started.
    fn part(topic: &str, low: i64, first: Option<i64>) -> Partition {
        Partition { topic: topic.into(), partition: 0, low, first }
    }

    /// A restored state resumes only where its source still holds
    /// the records after it; a start without a checkpoint (`--fresh`) is the operator's call.
    #[test]
    fn a_checkpoint_resumes_only_within_what_its_source_holds() {
        assert_eq!(unresumable(5, 5, 9), None);
        assert_eq!(unresumable(9, 0, 9), None, "read to its end");
        assert_eq!(unresumable(4, 5, 9).unwrap(), "it starts at 5: retention deleted offsets 4 to 4");
        assert_eq!(unresumable(10, 0, 9).unwrap(), "it ends at 9: it was deleted and created again, or truncated");
    }

    /// A sink partition is bounded on a restored start only if retention emptied it past the
    /// checkpoint's offset: what was written there since cannot be read back.
    #[test]
    fn a_restored_sink_partition_is_bounded_only_if_it_lost_what_came_after_the_checkpoint() {
        let parts = vec![
            Partition { topic: "a".into(), partition: 0, low: 10, first: Some(100) },
            Partition { topic: "a".into(), partition: 1, low: 10, first: Some(300) },
            Partition { topic: "b".into(), partition: 0, low: 4, first: None },
            Partition { topic: "c".into(), partition: 0, low: 9, first: Some(500) },
        ];
        let restored = [("a".to_string(), 0, 9), ("a".into(), 1, 10), ("b".into(), 0, 3), ("c".into(), 1, 0)];
        let lost: Vec<_> = parts.into_iter().filter(|p| lost_since(p, &restored)).collect();
        let bounds = [("a".to_string(), 100), ("b".to_string(), 1_000), ("c".to_string(), 500)];
        assert_eq!(sink_bounds(&lost, 1_000), BTreeMap::from(bounds));
        // c's partition 0 is not in the checkpoint: read back from --lookback, it lost records
        let lost: Vec<_> = lost.iter().map(|p| (&*p.topic, p.partition)).collect();
        assert_eq!(lost, [("a", 0), ("b", 0), ("c", 0)]);
    }

    #[test]
    fn a_sources_feed_starts_at_its_earliest_record_unless_retention_deleted_older_ones() {
        let now = 1_000;
        // a topic that lost nothing: its producer ran from its earliest record; a partition that
        // starts later had nothing to hold before (a quiet symbol), so it moves nothing
        assert_eq!(source_origin(&[part("t", 0, Some(100)), part("t", 0, Some(400))], now), (100, vec![]));
        // retention deleted records: from the latest partition that lost some, kept ones or not
        assert_eq!(
            source_origin(&[part("t", 5, Some(300)), part("t", 7, Some(200)), part("t", 0, Some(50))], now),
            (300, vec![])
        );
        // everything deleted: nothing it holds is older than now
        assert_eq!(source_origin(&[part("t", 9, None)], now), (now, vec![]));
        // the latest topic's: a pipeline reading two feeds has both from the later one
        assert_eq!(source_origin(&[part("a", 0, Some(100)), part("b", 0, Some(700))], now), (700, vec![]));
        // a topic with no record yet is awaited; an empty partition of a topic with records is not
        assert_eq!(
            source_origin(&[part("a", 0, Some(100)), part("a", 0, None), part("b", 0, None)], now),
            (100, vec!["b".to_string()])
        );
        assert_eq!(source_origin(&[], now), (i64::MIN, vec![]));
    }

    #[test]
    fn a_sink_topic_bounds_only_what_it_lost() {
        let now = 1_000;
        let bounds = sink_bounds(
            &[part("a", 0, Some(100)), part("b", 4, Some(300)), part("b", 6, Some(200)), part("c", 9, None)],
            now,
        );
        assert_eq!(bounds, BTreeMap::from([("b".to_string(), 300), ("c".to_string(), now)]));
    }

    /// Acks arrive per partition in any order across partitions (and, retried, not always in
    /// order within one): each partition keeps the offset after its highest.
    #[test]
    fn acks_keep_the_offset_after_each_partitions_highest() {
        let mut acked = HashMap::new();
        ack(&mut acked, "t", 2, 5);
        assert_eq!(acked["t"], [-1, -1, 6]);
        ack(&mut acked, "t", 2, 3);
        ack(&mut acked, "t", 0, 0);
        ack(&mut acked, "u", 0, 9);
        assert_eq!(acked["t"], [1, -1, 6]);
        assert_eq!(acked["u"], [10]);
    }

    fn acked(pairs: &[(&str, &[i64])]) -> Acked {
        pairs.iter().map(|(t, o)| (t.to_string(), o.to_vec())).collect()
    }

    /// A cut settles only once every message sent before it is delivered, whichever partition
    /// it went to and in whatever order the acks come.
    #[test]
    fn a_cut_settles_once_every_message_sent_before_it_is_acked() {
        let l = Ledger::default();
        assert_eq!(l.settle(0), Some(Ok(Acked::new())), "nothing sent: settled at once");
        for _ in 0..3 {
            l.sending(0);
        }
        assert_eq!(l.settle(0), None);
        l.delivered(0, Some(("t", 1, 7)));
        l.delivered(0, Some(("t", 0, 3)));
        assert_eq!(l.settle(0), None, "one still in flight");
        l.delivered(0, Some(("u", 0, 0)));
        assert_eq!(l.settle(0), Some(Ok(acked(&[("t", &[4, 8]), ("u", &[1])]))));
        assert_eq!(l.settle(0), Some(Ok(acked(&[("t", &[4, 8]), ("u", &[1])]))), "settling again changes nothing");
    }

    /// The loop goes on producing while a checkpoint waits for its acks: what is sent after the
    /// cut (the next generation) is acked at higher offsets, possibly first, and must not move
    /// the cut's sink offsets, or a restart from it would leave those messages unsuppressed.
    #[test]
    fn a_cut_takes_no_offset_of_a_message_sent_after_it() {
        let l = Ledger::default();
        l.sending(0);
        l.sending(0);
        // the cut: what follows is generation 1, sent and acked before the cut's last ack
        l.sending(1);
        l.sending(1);
        l.delivered(1, Some(("t", 0, 12)));
        l.delivered(1, Some(("t", 1, 40)));
        l.delivered(0, Some(("t", 0, 10)));
        assert_eq!(l.settle(0), None, "waits for the cut's own messages, not later ones");
        l.delivered(0, Some(("t", 0, 11)));
        assert_eq!(l.settle(0), Some(Ok(acked(&[("t", &[12])]))), "neither 13 nor partition 1's 41");
        assert_eq!(l.settle(1), Some(Ok(acked(&[("t", &[13, 41])]))));
    }

    /// A later cut keeps what earlier ones settled for the partitions its own messages did not
    /// go to, and settles the generations before it that no cut settled (a cut whose
    /// checkpoint failed to be written is followed by another, a generation later).
    #[test]
    fn a_cut_keeps_what_earlier_cuts_settled_and_settles_every_generation_before_it() {
        let l = Ledger::default();
        l.sending(0);
        l.delivered(0, Some(("t", 2, 5)));
        assert_eq!(l.settle(0), Some(Ok(acked(&[("t", &[-1, -1, 6])]))));
        l.sending(1);
        l.delivered(1, Some(("t", 0, 1)));
        l.sending(2);
        l.delivered(2, Some(("u", 1, 9)));
        l.sending(3);
        assert_eq!(l.settle(2), Some(Ok(acked(&[("t", &[2, -1, 6]), ("u", &[-1, 10])]))));
        assert_eq!(l.settle(4), None, "generation 3 is in flight");
        l.delivered(3, Some(("t", 2, 7)));
        assert_eq!(l.settle(4), Some(Ok(acked(&[("t", &[2, -1, 8]), ("u", &[-1, 10])]))));
        assert_eq!(l.settle(9), Some(Ok(acked(&[("t", &[2, -1, 8]), ("u", &[-1, 10])]))), "no message since");
    }

    /// A message sent before the cut that failed is not in its sink: the checkpoint would skip
    /// it for good, so the cut reports the failure instead of settling. One sent after the cut
    /// is not the cut's to report (the loop stops on it before the next checkpoint).
    #[test]
    fn a_failed_delivery_before_the_cut_keeps_it_from_settling() {
        let l = Ledger::default();
        for g in [0, 0, 0, 1, 1] {
            l.sending(g);
        }
        l.delivered(0, None);
        assert_eq!(l.settle(0), None, "two more still in flight");
        l.delivered(0, None);
        l.delivered(0, Some(("t", 0, 4)));
        l.delivered(1, None);
        assert_eq!(l.settle(0), Some(Err(2)));
        assert_eq!(l.settle(0), Some(Err(2)), "still: the failure is not forgotten");
        l.delivered(1, Some(("t", 0, 5)));
        assert_eq!(l.settle(1), Some(Err(3)));
        let later = Ledger::default();
        later.sending(0);
        later.sending(1);
        later.delivered(1, None);
        later.delivered(0, Some(("t", 0, 0)));
        assert_eq!(later.settle(0), Some(Ok(acked(&[("t", &[1])]))), "a failure after the cut");
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        /// Any interleaving of sends, acks in any order and cuts, one cut settling at a time as
        /// the runtime takes them: a cut settles exactly when every message sent before it is
        /// acked, and its sink offsets are then those after the highest offset of each
        /// partition's messages sent before it, whatever was sent and acked after it. A
        /// partition assigns offsets in the order its messages were sent (the idempotent
        /// producer keeps that order).
        #[test]
        fn a_cut_settles_on_exactly_the_messages_sent_before_it(
            ops in proptest::collection::vec((0u8..4, 0usize..1_000), 0..400),
        ) {
            let l = Ledger::default();
            let (mut generation, mut next, mut in_flight) = (0u64, [0i64; 3], vec![]);
            let mut sent: Vec<(u64, usize, i64)> = vec![]; // generation, partition, offset
            let mut cut: Option<u64> = None;
            for (op, arg) in ops {
                match op {
                    0 => {
                        let p = arg % 3;
                        l.sending(generation);
                        in_flight.push((generation, p, next[p]));
                        sent.push((generation, p, next[p]));
                        next[p] += 1;
                    }
                    1 if !in_flight.is_empty() => {
                        let (g, p, o) = in_flight.swap_remove(arg % in_flight.len());
                        l.delivered(g, Some(("t", p as i32, o)));
                    }
                    2 if cut.is_none() => {
                        cut = Some(generation);
                        generation += 1;
                    }
                    3 => {
                        let Some(c) = cut else { continue };
                        let waiting = in_flight.iter().any(|(g, _, _)| *g <= c);
                        match l.settle(c) {
                            None => prop_assert!(waiting, "a cut with every message acked did not settle"),
                            Some(Err(n)) => prop_assert!(false, "{n} failed, none was"),
                            Some(Ok(acked)) => {
                                prop_assert!(!waiting, "a cut settled with a message before it in flight");
                                let ends: SinkEnds = (0..3).map(|p| (("t".to_string(), p), -1)).collect();
                                let want: Vec<_> = (0..3)
                                    .map(|p| {
                                        let before = sent.iter().filter(|(g, q, _)| *g <= c && *q == p);
                                        ("t".to_string(), p as i32, before.map(|m| m.2 + 1).max().unwrap_or(-1))
                                    })
                                    .collect();
                                prop_assert_eq!(sink_offsets(&ends, &acked), want);
                                cut = None;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn sink_offsets_follow_the_acked_messages() {
        let ends: SinkEnds = [(("a".into(), 0), 10), (("a".into(), 1), 4), (("b".into(), 0), 0)].into();
        let acked: HashMap<String, Vec<i64>> = [("a".to_string(), vec![15, -1]), ("c".to_string(), vec![99])].into();
        assert_eq!(sink_offsets(&ends, &acked), [("a".into(), 0, 15), ("a".into(), 1, 4), ("b".into(), 0, 0)]);
    }

    #[test]
    fn topics_to_create_are_each_named_once_in_order() {
        let topics = ["b", "a", "b"].map(String::from);
        assert_eq!(rt_topics(&topics), ["a", "b"]);
    }

    /// The user's settings override brrrrr's tunable defaults.
    #[test]
    fn user_settings_override_defaults() {
        let c = client("b:1", &[("linger.ms", "5")], &[("linger.ms".into(), "50".into())]);
        assert_eq!(c.get("linger.ms"), Some("50"));
        assert_eq!(c.get("bootstrap.servers"), Some("b:1"));
    }

    /// The fence is the sinks' message timeout, and the producer is idempotent, whatever the
    /// user's settings say: `kafka_settings` refuses both keys, and they are set last anyway.
    /// Batching as long as the fence is refused by librdkafka: every message would time out.
    #[test]
    fn the_producer_times_out_at_the_fence_whatever_the_user_sets() {
        for k in ["message.timeout.ms", "delivery.timeout.ms"] {
            let e = kafka_settings(&strings(&[&format!("{k}=300000")])).unwrap_err().to_string();
            assert!(e.contains("--takeover") && e.contains("cannot be overridden"), "{e}");
        }
        let user = [("linger.ms", "20"), ("message.timeout.ms", "300000"), ("enable.idempotence", "false")];
        let user: Vec<_> = user.map(|(k, v)| (k.to_string(), v.to_string())).into();
        let c = producer_config("b:1", &user, Duration::from_millis(15_500));
        assert_eq!(c.get("message.timeout.ms"), Some("15500"));
        assert_eq!(c.get("enable.idempotence"), Some("true"));
        assert_eq!(c.get("linger.ms"), Some("20"));
        assert_eq!(producer_config("b:1", &[], Duration::from_secs(15)).get("linger.ms"), Some("5"));
        assert!(producer_config("127.0.0.1:1", &user, Duration::from_secs(15)).create::<BaseProducer>().is_ok());
        let long = [("linger.ms".to_string(), "15000".to_string())];
        let e = producer_config("127.0.0.1:1", &long, Duration::from_secs(15)).create::<BaseProducer>().err().unwrap();
        assert!(e.to_string().contains("linger.ms"), "{e}");
    }

    /// A producer without a broker: what it is sent waits in librdkafka's queue.
    fn fenced_producer(settings: &[(&str, &str)]) -> BaseProducer<Deliveries> {
        let settings: Vec<_> = settings.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let deliveries = Deliveries { failed: Arc::default(), first: Default::default(), ledger: Ledger::default() };
        producer_config("127.0.0.1:1", &settings, Duration::from_secs(60)).create_with_context(deliveries).unwrap()
    }

    fn emits(n: usize) -> Vec<Emit> {
        (0..n)
            .map(|i| Emit { topic: "t".into(), payload: i.to_string(), headers: vec![], window_end: i64::MAX })
            .collect()
    }

    /// Nothing is sent once the lease is `fence` old (`until`): what is sent then may reach the
    /// sink after another instance took over. The instance stops instead; the restart replays.
    #[test]
    fn nothing_is_sent_at_or_after_the_deadline() {
        let (producer, m, stop) = (fenced_producer(&[]), Metrics::default(), std::sync::atomic::AtomicBool::new(false));
        let (none, later) = (BTreeMap::new(), Instant::now() + Duration::from_secs(60));
        let sent = |g: u64| producer.context().ledger.lock().open.get(&g).map_or(0, |g| g.sent);
        produce(&producer, 0, &mut emits(3), &mut Suppress::new(), &none, &m, later, &stop).unwrap();
        produce(&producer, 4, &mut emits(2), &mut Suppress::new(), &none, &m, later, &stop).unwrap();
        assert_eq!(m.sent.load(Ordering::Relaxed), 5);
        assert_eq!((sent(0), sent(4)), (3, 2), "each message is in flight in its generation");
        assert_eq!(producer.context().ledger.settle(4), None, "nothing is delivered without a broker");
        let e = produce(&producer, 4, &mut emits(2), &mut Suppress::new(), &none, &m, Instant::now(), &stop);
        let e = e.unwrap_err().to_string();
        assert!(e.contains("the lease ran out while producing") && e.contains("replays"), "{e}");
        assert_eq!((m.sent.load(Ordering::Relaxed), sent(4)), (5, 2), "what is not sent is not in flight");
        // what is suppressed is not sent, so it needs no lease
        let mut suppress: Suppress = [(key_hash("t", b"0"), 1)].into();
        produce(&producer, 4, &mut emits(1), &mut suppress, &none, &m, Instant::now(), &stop).unwrap();
        assert_eq!((m.sent.load(Ordering::Relaxed), m.suppressed.load(Ordering::Relaxed)), (5, 1));
        assert_eq!(sent(4), 2);
    }

    /// A sink topic that lost its records before a bound vouches for no window that ended before
    /// it: those are withheld, each by its own window's end, whatever the batch or the pipeline
    /// has read. Other topics' are sent.
    #[test]
    fn a_window_that_ended_before_its_topic_lost_records_is_withheld() {
        let (producer, m, stop) = (fenced_producer(&[]), Metrics::default(), std::sync::atomic::AtomicBool::new(false));
        let lost: BTreeMap<String, i64> = [("t".to_string(), 3_600_000_000)].into();
        let mut out = emits(4);
        for (e, end) in out.iter_mut().zip([60_000_000, 3_599_999_999, 3_600_000_000, i64::MAX]) {
            e.window_end = end;
        }
        out.push(Emit { topic: "u".into(), window_end: 0, ..emits(1).pop().unwrap() });
        let later = Instant::now() + Duration::from_secs(60);
        produce(&producer, 0, &mut out, &mut Suppress::new(), &lost, &m, later, &stop).unwrap();
        assert_eq!((m.withheld_unverified.load(Ordering::Relaxed), m.sent.load(Ordering::Relaxed)), (2, 3));
    }

    /// librdkafka reports each message's delivery with the generation it was sent in: one that
    /// fails (here: no broker, past its timeout) keeps its generation's cut from settling, and
    /// is the first failure the loop stops on, naming its sink.
    #[test]
    fn a_delivery_is_reported_in_the_generation_it_was_sent_in() {
        let settings: Vec<_> = [("linger.ms", "0")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let deliveries = Deliveries { failed: Arc::default(), first: Default::default(), ledger: Ledger::default() };
        let config = producer_config("127.0.0.1:1", &settings, Duration::from_millis(200));
        let producer: BaseProducer<Deliveries> = config.create_with_context(deliveries).unwrap();
        let (m, none, stop) = (Metrics::default(), BTreeMap::new(), std::sync::atomic::AtomicBool::new(false));
        let later = Instant::now() + Duration::from_secs(60);
        produce(&producer, 3, &mut emits(1), &mut Suppress::new(), &none, &m, later, &stop).unwrap();
        let ledger = &producer.context().ledger;
        assert_eq!((ledger.settle(2), ledger.settle(3)), (Some(Ok(Acked::new())), None));
        let deadline = Instant::now() + Duration::from_secs(10);
        while ledger.settle(3).is_none() {
            assert!(Instant::now() < deadline, "never delivered");
            producer.poll(Duration::from_millis(50));
        }
        assert_eq!(ledger.settle(3), Some(Err(1)));
        assert_eq!(producer.context().failed.load(Ordering::Relaxed), 1);
        let first = delivery_failure(&producer);
        assert!(first.starts_with("t/"), "{first}");
        let e = with_delivery_failure(anyhow!("checkpoint 4"), &producer).to_string();
        assert_eq!(e, format!("checkpoint 4: {first}"));
        let clean = fenced_producer(&[]);
        assert_eq!(with_delivery_failure(anyhow!("checkpoint 4"), &clean).to_string(), "checkpoint 4");
    }

    /// The data loop's output produces what a view wrote once the view has written it:
    /// each flush hands `produce` everything pushed since the last, which it drains.
    #[test]
    fn producing_hands_each_flush_what_was_pushed_since_the_last() {
        let (mut out, mut calls) = (vec![], vec![]);
        let mut p = Producing::new(&mut out, |o: &mut Vec<Emit>| {
            calls.push(o.drain(..).map(|e| e.payload).collect::<Vec<_>>());
            Ok(())
        });
        p.flush();
        for e in emits(2) {
            p.push(e);
        }
        p.flush();
        p.push(emits(3).pop().unwrap());
        p.flush();
        assert!(p.result().is_ok());
        assert_eq!(p.pushed, 3);
        drop(p);
        assert_eq!(calls, [vec![], vec!["0", "1"], vec!["2"]]);
        assert!(out.is_empty());
    }

    /// A batch's cost on `/metrics`: its engine time is its time less producing's (never below
    /// 0), with what it wrote and closed.
    #[test]
    fn a_batch_is_recorded_with_its_engine_time_producing_excluded() {
        let m = Metrics::default();
        batch_metrics(&m, Duration::from_millis(300), Duration::from_millis(40), 1_200, 350);
        batch_metrics(&m, Duration::from_millis(5), Duration::from_millis(9), 0, 0);
        let text = m.render();
        let v = |name: &str| text.lines().find_map(|l| l.strip_prefix(&format!("{name} "))).unwrap().to_string();
        assert_eq!(v("brrrrr_batch_engine_seconds_count"), "2");
        assert!((v("brrrrr_batch_engine_seconds_sum").parse::<f64>().unwrap() - 0.26).abs() < 1e-9);
        assert_eq!(v("brrrrr_batch_engine_seconds_bucket{le=\"0.001\"}"), "1", "5 ms less 9: zero");
        assert!((v("brrrrr_batch_produce_seconds_sum").parse::<f64>().unwrap() - 0.049).abs() < 1e-9);
        assert_eq!(
            (v("brrrrr_batch_emitted_messages_sum"), v("brrrrr_batch_closed_groups_sum")),
            ("1200".into(), "350".into())
        );
        assert_eq!(v("brrrrr_batch_closed_groups_bucket{le=\"0\"}"), "1");
        assert_eq!(v("brrrrr_batch_closing_engine_seconds_count"), "1", "the batch that closed groups only");
        assert!((v("brrrrr_batch_closing_engine_seconds_sum").parse::<f64>().unwrap() - 0.26).abs() < 1e-9);
    }

    /// A flush that fails keeps its error for the loop and stops for good: what it had not sent
    /// is gone, so nothing pushed after it is produced either, even once the loop took the error.
    #[test]
    fn a_failed_flush_keeps_its_error_and_produces_nothing_more() {
        let (mut out, mut calls) = (vec![], 0);
        let mut p = Producing::new(&mut out, |o: &mut Vec<Emit>| {
            calls += 1;
            o.truncate(1); // sent one, then failed
            o.remove(0);
            bail!("the lease ran out while producing")
        });
        for e in emits(3) {
            p.push(e);
        }
        p.flush();
        p.push(emits(1).pop().unwrap());
        p.flush();
        let e = p.result().unwrap_err().to_string();
        assert_eq!(e, "the lease ran out while producing");
        assert!(p.result().is_ok(), "taken once");
        p.push(emits(1).pop().unwrap());
        p.flush();
        assert!(p.result().is_ok());
        assert_eq!(p.pushed, 5, "every message the views wrote is counted, sent or not");
        drop(p);
        assert_eq!(calls, 1, "nothing produced after the failure, before or after the loop took it");
        assert!(out.is_empty(), "nothing pushed after the failure is kept");
    }

    /// A full queue is waited on until the deadline, and on a stop signal not at all.
    #[test]
    fn a_full_queue_is_waited_on_until_the_deadline() {
        let producer = fenced_producer(&[("queue.buffering.max.messages", "1")]);
        let (m, none) = (Metrics::default(), BTreeMap::new());
        let (stop, until) = (std::sync::atomic::AtomicBool::new(false), Instant::now() + Duration::from_millis(300));
        let e = produce(&producer, 0, &mut emits(2), &mut Suppress::new(), &none, &m, until, &stop).unwrap_err();
        assert!(Instant::now() >= until);
        assert!(e.to_string().contains("the sink queue stayed full past the lease"), "{e}");
        assert_eq!(m.sent.load(Ordering::Relaxed), 1);
        // the refused message is counted once, however often it was offered again
        assert_eq!(producer.context().ledger.lock().open[&0].sent, 2);
        stop.store(true, Ordering::Relaxed);
        let later = Instant::now() + Duration::from_secs(60);
        let e = produce(&producer, 0, &mut emits(1), &mut Suppress::new(), &none, &m, later, &stop).unwrap_err();
        assert!(e.to_string().contains("stopped while the sink queue was full"), "{e}");
    }

    /// A batch waits 1 ms for more messages unless told otherwise, and at most a second.
    #[test]
    fn a_batch_waits_1_ms_by_default_and_at_most_a_second() {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            a: Args,
        }
        let wait = |extra: &[&str]| {
            use clap::Parser;
            Cli::try_parse_from([&["run", "p.sql", "--proto", "p.proto"], extra].concat()).map(|c| c.a.batch_wait_ms)
        };
        assert_eq!(wait(&[]).unwrap(), 1);
        assert_eq!(wait(&["--batch-wait-ms", "0"]).unwrap(), 0);
        assert_eq!(wait(&["--batch-wait-ms", "1000"]).unwrap(), 1000);
        assert!(wait(&["--batch-wait-ms", "1001"]).is_err());
    }

    /// An exact ASOF join waits 1 s for its quotes unless told otherwise, and at most the hold.
    #[test]
    fn the_asof_lateness_defaults_to_a_second_and_is_at_most_the_hold() {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            a: Args,
        }
        let ms = |extra: &[&str]| {
            use clap::Parser;
            Cli::try_parse_from([&["run", "p.sql", "--proto", "p.proto"], extra].concat()).map(|c| c.a.asof_lateness_ms)
        };
        assert_eq!(ms(&[]).unwrap(), 1000);
        assert_eq!(ms(&["--asof-lateness-ms", "250"]).unwrap(), 250);
        assert_eq!(ms(&["--asof-lateness-ms", "30000"]).unwrap(), 30_000);
        assert!(ms(&["--asof-lateness-ms", "0"]).is_err());
        assert!(ms(&["--asof-lateness-ms", "30001"]).is_err());
    }

    /// The lateness reaches the join in µs for an exact join only, and idle close may not be shorter
    /// than it.
    #[test]
    fn the_asof_lateness_is_in_microseconds_for_exact_joins_and_idle_close_must_outlast_it() {
        assert_eq!(asof_lateness_us(AsofArg::Exact, 1000, None).unwrap(), Some(1_000_000));
        assert_eq!(asof_lateness_us(AsofArg::Exact, 250, None).unwrap(), Some(250_000));
        assert_eq!(asof_lateness_us(AsofArg::Arrival, 250, None).unwrap(), None);
        // idle close of 1 s outlasts the default 1 s, and 250 ms
        assert_eq!(asof_lateness_us(AsofArg::Exact, 1000, Some(1)).unwrap(), Some(1_000_000));
        // but not 1.5 s: it would release rows a quote may still precede
        assert!(asof_lateness_us(AsofArg::Exact, 1500, Some(1)).is_err());
        // an arrival join has no lateness to outlast
        assert_eq!(asof_lateness_us(AsofArg::Arrival, 1500, Some(1)).unwrap(), None);
    }

    /// Partitions are merged with the ones of the views they reach (`Engine::source_groups`): a topic read by
    /// streams of two groups joins them, and one no view reads stands alone.
    #[test]
    fn partitions_are_grouped_as_their_topics_streams_are() {
        let strs = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let topics = |t: &[(&str, &[&str])]| -> BTreeMap<String, Vec<String>> {
            t.iter().map(|(t, ss)| (t.to_string(), strs(ss))).collect()
        };
        let parts = |p: &[(&str, i32)]| -> Vec<(String, i32)> { p.iter().map(|(t, p)| (t.to_string(), *p)).collect() };
        let groups = [strs(&["a_trades", "a_quotes"]), strs(&["b_trades", "b_quotes"])];
        let parts5 = parts(&[("tb", 0), ("ta", 0), ("ta", 1), ("tq", 0), ("tx", 0)]);
        let per_stream =
            topics(&[("ta", &["a_trades"]), ("tb", &["b_quotes"]), ("tq", &["a_quotes"]), ("tx", &["nobody"])]);
        // numbered as first seen: tb's group 0, ta's and tq's group 1, tx (read by no view) 2
        assert_eq!(partition_groups(&parts5, &per_stream, &groups), [0, 1, 1, 1, 2]);
        // a topic read by one stream of each group makes them one
        let joined = topics(&[
            ("ta", &["a_trades"]),
            ("tb", &["b_quotes"]),
            ("tq", &["a_quotes"]),
            ("tj", &["a_trades", "b_trades"]),
        ]);
        let parts4 = parts(&[("tb", 0), ("ta", 0), ("tq", 0), ("tj", 0)]);
        assert_eq!(partition_groups(&parts4, &joined, &groups), [0, 0, 0, 0]);
        // two topics no view reads are two groups, not one
        let unread = topics(&[("t1", &["x"]), ("t2", &["y"]), ("t3", &["z"])]);
        let parts6 = parts(&[("t1", 0), ("t2", 0), ("t3", 0), ("t1", 1), ("t3", 1), ("t2", 1)]);
        assert_eq!(partition_groups(&parts6, &unread, &groups), [0, 1, 2, 0, 2, 1]);
    }

    /// `--close-after-ms` needs the aligned merge, which alone knows where each partition ends, and must
    /// outlast `--partition-wait-ms`: a record is read that long after it is produced, so a shorter margin
    /// would close a window a partition still owes a row to.
    #[test]
    fn a_clock_close_needs_the_merge_and_a_margin_past_the_partition_wait() {
        assert_eq!(clock_close_margin(None, None, 50).unwrap(), None);
        assert_eq!(clock_close_margin(None, Some(0), 50).unwrap(), None);
        assert_eq!(clock_close_margin(Some(300), Some(0), 50).unwrap(), Some(300));
        assert_eq!(clock_close_margin(Some(51), Some(1), 50).unwrap(), Some(51));
        let off = clock_close_margin(Some(300), None, 50).unwrap_err().to_string();
        assert!(off.contains("--close-after-ms needs --max-drift"), "{off}");
        let short = clock_close_margin(Some(50), Some(0), 50).unwrap_err().to_string();
        assert!(short.contains("must be more than --partition-wait-ms 50"), "{short}");
    }

    /// The flag is a number of milliseconds, 1 to a minute, and off by default.
    #[test]
    fn close_after_ms_is_off_by_default_and_a_bounded_number_of_milliseconds() {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            a: Args,
        }
        let ms = |extra: &[&str]| {
            use clap::Parser;
            Cli::try_parse_from([&["run", "p.sql", "--proto", "p.proto"], extra].concat()).map(|c| c.a.close_after_ms)
        };
        assert_eq!(ms(&[]).unwrap(), None);
        assert_eq!(ms(&["--close-after-ms", "300"]).unwrap(), Some(300));
        assert_eq!(ms(&["--close-after-ms", "60000"]).unwrap(), Some(60_000));
        assert!(ms(&["--close-after-ms", "0"]).is_err());
        assert!(ms(&["--close-after-ms", "60001"]).is_err());
    }

    /// A restart stops suppressing the messages already in the sinks only once the replay has caught up and, for
    /// a pipeline that closes windows without a row, once it has closed since: a window an earlier run closed on
    /// the clock is written again by this run's first clock close, and must still be suppressed.
    #[test]
    fn suppression_ends_when_the_replay_caught_up_and_a_pipeline_that_closes_without_rows_has_done_so() {
        // (closes without rows, ends reached, idle closed, clock closed)
        assert!(suppression_over(false, true, false, false), "nothing closes without a row: the ends are enough");
        assert!(!suppression_over(false, false, false, false), "the replay has not caught up");
        assert!(!suppression_over(true, true, false, false), "caught up, but no close since");
        assert!(suppression_over(true, true, true, false), "an idle close since");
        assert!(suppression_over(true, true, false, true), "a clock close since");
        assert!(!suppression_over(true, false, true, true), "closes before the ends are reached prove nothing");
    }

    /// A clock close counts for the restart only if it reaches every window an earlier run could have closed on
    /// the clock: that run was killed before this one caught up, and closed to at most the margin before then.
    #[test]
    fn a_clock_close_covers_the_earlier_run_only_from_the_time_the_replay_caught_up_less_the_margin() {
        assert!(!clock_close_covers_earlier_runs(699, 1_000, 300), "a close to the replayed data's time");
        assert!(clock_close_covers_earlier_runs(700, 1_000, 300));
        assert!(clock_close_covers_earlier_runs(5_000, 1_000, 300));
    }

    /// A batch's last partial millisecond of waiting is slept, not spun.
    #[test]
    fn poll_timeouts_round_up_to_whole_milliseconds() {
        let us = Duration::from_micros;
        assert_eq!([0, 1, 999, 1000, 1001].map(|n| whole_ms(us(n)).as_millis()), [0, 1, 1, 1, 2]);
    }

    /// A consumer fetches again soon after its queue drains, not a second later.
    #[test]
    fn consumers_fetch_again_within_10_ms() {
        assert_eq!(client("b:1", CONSUMER_DEFAULTS, &[]).get("fetch.queue.backoff.ms"), Some("10"));
    }

    /// With aligned consumption a partition's end is reported within 50 ms, not half a second;
    /// the user's settings still win.
    #[test]
    fn aligned_consumers_learn_of_a_partitions_end_within_50_ms() {
        let defaults = [CONSUMER_DEFAULTS, ALIGNED_DEFAULTS].concat();
        let c = client("b:1", &defaults, &[]);
        assert_eq!(c.get("fetch.wait.max.ms"), Some("50"));
        assert_eq!(c.get("fetch.queue.backoff.ms"), Some("10"));
        assert_eq!(client("b:1", CONSUMER_DEFAULTS, &[]).get("fetch.wait.max.ms"), None);
        let mine = [("fetch.wait.max.ms".to_string(), "500".to_string())];
        assert_eq!(client("b:1", &defaults, &mine).get("fetch.wait.max.ms"), Some("500"));
    }

    /// A backlog is prefetched a few MB per queue, not 100,000 messages per partition; the
    /// user's settings still win.
    #[test]
    fn consumers_prefetch_a_few_mb() {
        let c = client("b:1", CONSUMER_DEFAULTS, &[]);
        assert_eq!(c.get("queued.min.messages"), Some("20000"));
        assert_eq!(c.get("queued.max.messages.kbytes"), Some("4096"));
        let mine = [("queued.max.messages.kbytes".to_string(), "65536".to_string())];
        assert_eq!(client("b:1", CONSUMER_DEFAULTS, &mine).get("queued.max.messages.kbytes"), Some("65536"));
    }
}

#[cfg(test)]
mod sink_topic_tests {
    use super::store_tests::trades;
    use super::*;

    const T0: i64 = 1_700_000_040_000_000; // a minute's start (µs)

    fn catalog(path: &str) -> Catalog {
        brrrrr_core::sql::parse(&std::fs::read_to_string(format!("../../{path}")).unwrap()).unwrap()
    }

    fn topic(cat: &Catalog, stream: &str) -> String {
        setting(&cat.streams[stream], "topic").to_string()
    }

    /// Whatever emits a message, a row or an idle close (the windows closed by
    /// `close_until` once went to the unprefixed topic, a live one), it
    /// carries the topic the run writes.
    #[test]
    fn every_message_the_engine_makes_carries_the_topic_the_run_writes() {
        let mut cat = catalog("tests/acceptance/sql/windows.sql");
        assert_eq!(name_sinks(&mut cat, "shadow.", "").unwrap(), ["shadow.test.1m"]);
        assert_eq!(topic(&cat, "test_1m_kafka_out"), "shadow.test.1m");
        assert_eq!(topic(&cat, "trades_source"), "raw.trades.test", "a source is read where it is");
        let mut engine = Engine::new(&cat).unwrap();
        let (mut rows, mut idle) = (vec![], vec![]);
        engine.insert("trades_source", trades(T0, 300), &mut rows);
        engine.close_until(T0 + 86_400_000_000, &mut idle);
        assert!(!rows.is_empty() && !idle.is_empty());
        assert!(rows.iter().chain(&idle).all(|e| &*e.topic == "shadow.test.1m"));
    }

    #[test]
    fn a_template_names_each_sink_and_leaves_the_sources() {
        let mut cat = catalog("tests/acceptance/sql/two_intervals.sql");
        assert_eq!(name_sinks(&mut cat, "", "shadow.{1}.{2}").unwrap(), ["shadow.test.v1", "shadow.test.v1"]);
        assert_eq!(topic(&cat, "test_5m_kafka_out"), "shadow.test.v1");
        assert_eq!(topic(&cat, "trades_source"), "raw.trades.test");
        let mut idle = vec![];
        let mut engine = Engine::new(&cat).unwrap();
        engine.insert("trades_source", trades(T0, 600), &mut vec![]);
        engine.close_until(T0 + 86_400_000_000, &mut idle);
        assert!(!idle.is_empty() && idle.iter().all(|e| &*e.topic == "shadow.test.v1"));
    }

    #[test]
    fn without_a_prefix_or_template_the_sinks_keep_their_topics() {
        let mut cat = catalog("tests/acceptance/sql/windows.sql");
        assert_eq!(name_sinks(&mut cat, "", "").unwrap(), ["test.1m"]);
        assert_eq!(topic(&cat, "test_1m_kafka_out"), "test.1m");
    }

    #[test]
    fn sinks_are_named_one_way_by_a_template_that_fits_them() {
        let mut cat = catalog("tests/acceptance/sql/windows.sql");
        let both = name_sinks(&mut cat, "shadow.", "{1}").unwrap_err().to_string();
        assert!(both.contains("two ways to name the sinks"), "{both}");
        let misfit = name_sinks(&mut cat, "", "x.{3}").unwrap_err().to_string();
        assert_eq!(misfit, "--sink-topic-template: test.1m has no part 3");
    }

    #[test]
    fn a_sink_without_a_topic_is_not_given_one_but_refused() {
        let sql = std::fs::read_to_string("../../tests/acceptance/sql/windows.sql").unwrap();
        let mut cat = brrrrr_core::sql::parse(&sql.replace("topic = 'test.1m',", "topic = '',")).unwrap();
        assert!(name_sinks(&mut cat, "shadow.", "").unwrap().is_empty());
        assert!(Engine::new(&cat).err().unwrap().contains("no topic"));
    }

    /// A restart suppresses what the sinks hold, by the topic it was read back from: windows
    /// an idle close emits again are suppressed like any other, once per copy found.
    #[test]
    fn idle_closed_windows_already_in_the_prefixed_topic_are_suppressed_once() {
        let mut cat = catalog("tests/acceptance/sql/windows.sql");
        name_sinks(&mut cat, "shadow.", "").unwrap();
        let mut engine = Engine::new(&cat).unwrap();
        let mut idle = vec![];
        engine.insert("trades_source", trades(T0, 300), &mut vec![]);
        engine.close_until(T0 + 86_400_000_000, &mut idle);
        let dedup = |e: &Emit| e.headers.iter().find(|h| h.0 == DEDUP).unwrap().1.clone();
        // as written_since reads them back: the topic and the dedup header
        let mut suppress: Suppress =
            idle.iter().map(|e| (key_hash("shadow.test.1m", dedup(e).as_bytes()), 1)).collect();
        let mut elsewhere: Suppress = idle.iter().map(|e| (key_hash("test.1m", dedup(e).as_bytes()), 1)).collect();
        assert!(idle.iter().all(|e| already_written(&mut suppress, e)));
        assert!(suppress.is_empty());
        assert!(idle.iter().all(|e| !already_written(&mut suppress, e)), "each copy is suppressed once");
        assert!(idle.iter().all(|e| !already_written(&mut elsewhere, e)));
    }

    #[test]
    fn a_message_found_twice_is_suppressed_twice_and_one_without_the_header_by_its_payload() {
        let e = |headers: Vec<(String, String)>| Emit {
            topic: "t".into(),
            payload: "{}".into(),
            headers,
            window_end: i64::MAX,
        };
        let keyed = e(vec![("x".into(), "k2".into()), (DEDUP.into(), "k".into())]);
        let mut suppress = Suppress::from([(key_hash("t", b"k"), 2), (key_hash("t", b"{}"), 1)]);
        assert!(already_written(&mut suppress, &keyed));
        assert!(already_written(&mut suppress, &keyed));
        assert!(!already_written(&mut suppress, &keyed));
        assert!(already_written(&mut suppress, &e(vec![])));
        assert!(suppress.is_empty());
    }

    #[test]
    fn a_template_picks_parts_of_the_topic() {
        assert_eq!(sink_topic("shadow.{1}.{2}", "bars.v1.spot.1m").unwrap(), "shadow.bars.v1");
        assert_eq!(sink_topic("shadow.{1}.{2}", "bars.v1.top-3.5m").unwrap(), "shadow.bars.v1");
        assert_eq!(sink_topic("{4}-{1}", "a.b.c.d").unwrap(), "d-a");
        assert_eq!(sink_topic("", "a.b").unwrap(), "a.b");
    }

    #[test]
    fn a_template_that_does_not_fit_the_topic_is_refused() {
        assert_eq!(sink_topic("x.{3}", "a.b").unwrap_err(), "a.b has no part 3");
        assert!(sink_topic("x.{0}", "a.b").is_err());
        assert!(sink_topic("x.{one}", "a.b").is_err());
        assert!(sink_topic("x.{1", "a.b").is_err());
    }

    #[test]
    fn a_json_source_is_read_where_it_is_and_its_sink_is_named() {
        let mut cat = catalog("tests/acceptance/sql/json.sql");
        assert_eq!(name_sinks(&mut cat, "shadow.", "").unwrap(), ["shadow.test.1m"]);
        assert_eq!(topic(&cat, "trades_source"), "raw.trades.test");
    }
}

/// JSONEachRow sources' messages as rows.
#[cfg(test)]
mod json_tests {
    use super::*;

    fn cols(spec: &[(&str, &str)]) -> Vec<(String, Type)> {
        spec.iter().map(|(n, t)| (n.to_string(), Type::parse(t).unwrap())).collect()
    }

    #[test]
    fn a_json_message_fills_the_columns_by_name_defaults_the_missing_and_ignores_the_rest() {
        let c = cols(&[("symbol", "string"), ("price", "float64"), ("size", "nullable(float64)"), ("n", "uint32")]);
        let row = json_row(&c, br#"{"price": 101.25, "symbol": "BTC", "venue": "x", "n": "7"}"#).unwrap();
        assert_eq!(row, [Value::Str("BTC".into()), Value::F64(101.25), Value::Null, Value::UInt(7)]);
        let row = json_row(&c, br#"{"symbol": null, "price": "42.5", "size": 3, "n": 1.9}"#).unwrap();
        assert_eq!(row, [Value::Str("".into()), Value::F64(42.5), Value::F64(3.0), Value::UInt(1)]);
        // anything but a number in a number's column, or not an object, skips the message
        assert_eq!(json_row(&c, br#"{"price": "n/a"}"#).unwrap_err(), r#"price: "n/a" is not a F64"#);
        assert!(json_row(&c, br#"{"price": [1]}"#).is_err());
        assert!(json_row(&c, b"[1, 2]").unwrap_err().starts_with("not a JSON object"));
        assert!(json_row(&c, b"{\"price\": 1").is_err());
        // a number and an array in a string's column are their JSON text, arrays element by element
        let c = cols(&[("s", "string"), ("a", "array(float64)")]);
        assert_eq!(
            json_row(&c, br#"{"s": [1, "x"], "a": [1, "2.5", null]}"#).unwrap(),
            [Value::Str(r#"[1,"x"]"#.into()), Value::Array([1.0, 2.5, 0.0].map(Value::F64).into())]
        );
    }

    #[test]
    fn a_json_datetime_is_iso_8601_text_or_a_number_in_the_columns_precision_or_seconds() {
        let us = 1_704_187_800_123_000; // 2024-01-02 09:30:00.123 UTC
        let c = cols(&[
            ("ms", "datetime64(3)"),
            ("us", "datetime64(6)"),
            ("s", "datetime"),
            ("n", "nullable(datetime64(3))"),
        ]);
        let at = |json: &str| json_row(&c, json.as_bytes()).map(|r| r.into_iter().take(1).next().unwrap());
        for text in [
            "2024-01-02 09:30:00.123",
            "2024-01-02T09:30:00.123",
            "2024-01-02T09:30:00.123Z",
            "2024-01-02T10:30:00.123+01:00",
            "2024-01-02T10:30:00.123+0100",
            "2024-01-02T04:30:00.123-05",
        ] {
            assert_eq!(at(&format!(r#"{{"ms": "{text}"}}"#)), Ok(Value::Time(us)), "{text}");
        }
        assert_eq!(at(r#"{"ms": 1704187800123}"#), Ok(Value::Time(us)));
        assert_eq!(at(r#"{"ms": 1704187800.123}"#), Ok(Value::Time(us)));
        let row = json_row(&c, br#"{"us": 1704187800123000, "s": 1704187800}"#).unwrap();
        assert_eq!(row, [Value::Time(0), Value::Time(us), Value::Time(us - 123_000), Value::Null]);
        for bad in ["2024-01-02T09:30:00+1", "2024-01-02T09:30:00+01:0x", "yesterday", "1704187800123"] {
            assert!(at(&format!(r#"{{"ms": "{bad}"}}"#)).is_err(), "{bad}");
        }
    }
}

/// Checkpoints, restore and the lease over a real store: a directory, as on the pod's volume.
#[cfg(test)]
mod store_tests {
    use super::*;
    use crate::store::tests::Scratch;
    use brrrrr_core::value::Value;

    fn engine() -> Engine {
        let sql = include_str!("../../../tests/acceptance/sql/windows.sql");
        Engine::new(&brrrrr_core::sql::parse(sql).unwrap()).unwrap()
    }

    /// `n` trades of two symbols, a second apart from `t0` (µs), as the source decodes them.
    pub(super) fn trades(t0: i64, n: i64) -> Vec<brrrrr_core::engine::Row> {
        let trade = |i: i64| {
            let t = t0 + i * 1_000_000;
            let sym = if i % 2 == 0 { "BTC" } else { "ETH" };
            [
                Value::Int(t / 1000),
                Value::Str(i.to_string().into()),
                Value::Int(1),
                Value::Str(sym.into()),
                Value::F64(100.0 + i as f64),
                Value::Int(t),
                Value::Str("buy".into()),
                Value::F64(1.0),
                Value::F64(100.0),
            ]
            .to_vec()
        };
        (0..n).map(trade).collect()
    }

    /// An engine holding open windows.
    fn busy_engine(t0: i64, n: i64) -> Engine {
        let mut e = engine();
        e.insert("trades_source", trades(t0, n), &mut vec![]);
        e
    }

    fn state(e: &Engine) -> Vec<u8> {
        Checkpoint::of(e, 0, vec![], vec![]).encode()
    }

    fn dir_store(dir: &Scratch) -> Box<dyn Store> {
        store::open(dir.path(), &["windows", "00000000000000ff"]).unwrap()
    }

    /// `dir_store` and its owner's, as one instance of the pipeline has them.
    fn dir_stores(dir: &Scratch) -> Stores {
        Stores::open(dir.path(), "windows", 0xff).unwrap()
    }

    fn offsets(o: i64) -> Offsets {
        vec![("raw.trades.test".into(), 0, o)]
    }

    fn write(s: &dyn Store, epoch: u64, e: &Engine, source_at: i64) {
        assert!(s
            .create(&ckpt(epoch), &Checkpoint::of(e, epoch, offsets(source_at), offsets(source_at / 10)).encode())
            .unwrap());
    }

    #[test]
    fn without_checkpoints_nothing_is_restored() {
        let dir = Scratch::new("none");
        let s = dir_store(&dir);
        let mut e = engine();
        for start in [Start::Newest, Start::Fresh] {
            let r = restore(s.as_ref(), &mut e, start, &Metrics::default()).unwrap();
            assert_eq!(r, None, "{start:?}");
        }
        assert_eq!(state(&e), state(&engine()));
    }

    /// A pod restarted on its volume resumes from its newest checkpoint: the engine's state,
    /// and the source and sink positions to replay and suppress from.
    #[test]
    fn the_newest_checkpoint_is_restored_with_its_positions() {
        let dir = Scratch::new("newest");
        let s = dir_store(&dir);
        write(s.as_ref(), 1, &busy_engine(0, 5), 5);
        let running = busy_engine(0, 50);
        write(s.as_ref(), 2, &running, 50);
        drop(s);
        let s = dir_store(&dir); // the restarted pod
        let mut e = engine();
        let m = Metrics::default();
        let r = restore(s.as_ref(), &mut e, Start::Newest, &m).unwrap();
        assert_eq!(r, Some((offsets(50), offsets(5), None)));
        assert_eq!(state(&e), state(&running));
        assert_ne!(state(&e), state(&engine()), "the test proves little with an empty state");
        assert_eq!(m.checkpoints_refused.load(Ordering::Relaxed), 0);
    }

    /// A newest checkpoint cut short, garbled or of another plan is skipped for the one before.
    #[test]
    fn checkpoints_that_do_not_restore_are_skipped_for_older_ones() {
        let dir = Scratch::new("fallback");
        let s = dir_store(&dir);
        let good = busy_engine(0, 20);
        write(s.as_ref(), 3, &good, 20);
        let full = Checkpoint::of(&busy_engine(0, 30), 4, offsets(30), offsets(3)).encode();
        s.create(&ckpt(4), &full[..full.len() / 2]).unwrap(); // cut short
        s.create(&ckpt(5), b"garbage").unwrap();
        // the same SQL planned differently (another window): its state does not fit
        let sql = include_str!("../../../tests/acceptance/sql/windows.sql").replace("1m", "5m");
        let other = Engine::new(&brrrrr_core::sql::parse(&sql).unwrap()).unwrap();
        s.create(&ckpt(6), &Checkpoint::of(&other, 6, offsets(1), offsets(1)).encode()).unwrap();
        let (mut e, m) = (engine(), Metrics::default());
        let r = restore(s.as_ref(), &mut e, Start::Newest, &m).unwrap();
        assert_eq!(r, Some((offsets(20), offsets(2), None)));
        assert_eq!(state(&e), state(&good));
        assert_eq!(m.checkpoints_refused.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn when_no_checkpoint_restores_the_engine_is_left_as_it_was_and_the_error_says_what_to_do() {
        let dir = Scratch::new("allbad");
        let s = dir_store(&dir);
        s.create(&ckpt(1), b"x").unwrap();
        s.create(&ckpt(2), b"y").unwrap();
        let mut e = engine();
        let err = restore(s.as_ref(), &mut e, Start::Newest, &Metrics::default()).err().unwrap().to_string();
        assert!(err.contains("epochs [2, 1]") && err.contains("--fresh") && err.contains(dir.path()), "{err}");
        assert_eq!(state(&e), state(&engine()));
    }

    /// `bytes` with its header's version overwritten, whatever the body: `decode` refuses it on
    /// the version alone, before the body is ever looked at (`checkpoint.rs`).
    fn as_version(mut bytes: Vec<u8>, version: u16) -> Vec<u8> {
        bytes[4..6].copy_from_slice(&version.to_le_bytes());
        bytes
    }

    /// A version this build no longer reads: one before the oldest it reads (`checkpoint::READS`).
    const STALE: u16 = 0;

    /// A deploy of a new checkpoint format finds only checkpoints an older build wrote: rather
    /// than crash-loop, it prunes what will never restore
    /// again and starts fresh, exactly as `--fresh` would, since ADR-0008 already decided that a
    /// deploy of a new version starts every pipeline fresh.
    #[test]
    fn checkpoints_all_of_an_older_version_are_pruned_and_the_pipeline_starts_fresh() {
        let dir = Scratch::new("stale-version");
        let s = dir_store(&dir);
        let running = busy_engine(0, 20);
        let old = Checkpoint::of(&running, 5, offsets(20), offsets(2)).encode();
        s.create(&ckpt(5), &as_version(old, STALE)).unwrap();
        let older = Checkpoint::of(&busy_engine(0, 5), 4, offsets(5), offsets(1)).encode();
        s.create(&ckpt(4), &as_version(older, STALE)).unwrap();
        let (mut e, m) = (engine(), Metrics::default());
        let r = restore(s.as_ref(), &mut e, Start::Newest, &m).unwrap();
        assert_eq!(r, None, "starts fresh, as --fresh would");
        assert_eq!(state(&e), state(&engine()));
        assert_eq!(m.checkpoints_refused.load(Ordering::Relaxed), 2);
        assert!(s.list().unwrap().is_empty(), "the stale checkpoints are pruned, not left to fail again next start");
    }

    /// Only a version *older* than this build's is ever pruned automatically. A newer one (a
    /// downgrade: this build is behind what wrote it) is left for a human, exactly as any other
    /// refusal is: rolling back must never look like it silently agreed to lose that state.
    #[test]
    fn a_newer_version_than_this_build_reads_is_left_for_a_human() {
        let dir = Scratch::new("newer-version");
        let s = dir_store(&dir);
        let bytes = Checkpoint::of(&busy_engine(0, 10), 3, offsets(10), offsets(1)).encode();
        s.create(&ckpt(3), &as_version(bytes, brrrrr_core::checkpoint::VERSION + 1)).unwrap();
        let err = restore(s.as_ref(), &mut engine(), Start::Newest, &Metrics::default()).err().unwrap().to_string();
        assert!(err.contains("epochs [3]") && err.contains("--fresh"), "{err}");
        assert_eq!(s.list().unwrap(), [ckpt(3)], "a newer checkpoint is never pruned automatically");
    }

    /// A stale version is only pruned when every candidate fails for that one reason. Mixed in
    /// with a checkpoint refused for any other reason (here: not a checkpoint at all), the whole
    /// batch stays a human's call, and nothing is deleted -- an unrelated corruption bug must
    /// never be masked by the version-pruning path.
    #[test]
    fn a_stale_version_mixed_with_another_kind_of_failure_prunes_nothing() {
        let dir = Scratch::new("mixed-failure");
        let s = dir_store(&dir);
        let bytes = Checkpoint::of(&busy_engine(0, 10), 2, offsets(10), offsets(1)).encode();
        s.create(&ckpt(2), &as_version(bytes, STALE)).unwrap();
        s.create(&ckpt(3), b"garbage").unwrap();
        let err = restore(s.as_ref(), &mut engine(), Start::Newest, &Metrics::default()).err().unwrap().to_string();
        assert!(err.contains("epochs [3, 2]") && err.contains("--fresh"), "{err}");
        let mut left = s.list().unwrap();
        left.sort();
        assert_eq!(left, [ckpt(2), ckpt(3)], "nothing is pruned when a reason differs");
    }

    /// An explicit `--restore-epoch` that happens to be a stale version is still a plain error,
    /// never an automatic fresh start: the operator asked for one specific epoch.
    #[test]
    fn a_stale_version_at_an_explicit_epoch_is_still_an_error_not_a_fallback() {
        let dir = Scratch::new("stale-epoch");
        let s = dir_store(&dir);
        let bytes = Checkpoint::of(&busy_engine(0, 10), 4, offsets(10), offsets(1)).encode();
        s.create(&ckpt(4), &as_version(bytes, STALE)).unwrap();
        assert!(restore(s.as_ref(), &mut engine(), Start::Epoch(4), &Metrics::default()).is_err());
        assert_eq!(s.list().unwrap(), [ckpt(4)], "an explicit epoch is never pruned automatically");
    }

    #[test]
    fn a_given_epoch_is_restored_even_if_newer_ones_exist() {
        let dir = Scratch::new("epoch");
        let s = dir_store(&dir);
        let old = busy_engine(0, 10);
        write(s.as_ref(), 7, &old, 10);
        write(s.as_ref(), 8, &busy_engine(0, 40), 40);
        let mut e = engine();
        let r = restore(s.as_ref(), &mut e, Start::Epoch(7), &Metrics::default()).unwrap();
        assert_eq!(r, Some((offsets(10), offsets(1), None)));
        assert_eq!(state(&e), state(&old));
        let err = restore(s.as_ref(), &mut engine(), Start::Epoch(6), &Metrics::default()).err().unwrap();
        assert!(err.to_string().contains("--restore-epoch 6: no such checkpoint"), "{err}");
        // a given epoch that does not restore is an error, not a fallback
        s.create(&ckpt(9), b"bad").unwrap();
        assert!(restore(s.as_ref(), &mut engine(), Start::Epoch(9), &Metrics::default()).is_err());
    }

    #[test]
    fn fresh_restores_nothing_but_reports_the_newest_epoch() {
        let dir = Scratch::new("fresh");
        let s = dir_store(&dir);
        write(s.as_ref(), 12, &busy_engine(0, 10), 10);
        let mut e = engine();
        let r = restore(s.as_ref(), &mut e, Start::Fresh, &Metrics::default()).unwrap();
        assert_eq!(r, None);
        assert_eq!(state(&e), state(&engine()));
    }

    /// The lease: an epoch is taken once; the instance that finds it taken stops.
    #[test]
    fn a_taken_epoch_means_another_instance_holds_the_pipeline() {
        let dir = Scratch::new("taken");
        let (mine, theirs) = (dir_stores(&dir), dir_stores(&dir));
        mine.put(3, b"mine").unwrap();
        let Err(NotWritten::Taken(name)) = theirs.put(3, b"theirs") else { panic!("not taken") };
        assert!(name.ends_with(&format!("/windows/{}", lease_of(3))), "{name}");
        assert_eq!(mine.checkpoints.get(&ckpt(3)).unwrap(), b"mine");
        assert_eq!(mine.owner.get(&lease_of(3)).unwrap(), mine.holder.as_bytes());
    }

    /// The lease is the pipeline's, not its revision's. Another SQL
    /// or --asof has checkpoints of its own, but an epoch either one took is taken for the other:
    /// they never both write the pipeline's sinks.
    #[test]
    fn of_two_revisions_of_a_pipeline_only_one_holds_an_epoch() {
        let dir = Scratch::new("revisions");
        let (a, b) = (dir_stores(&dir), Stores::open(dir.path(), "windows", 0xee).unwrap());
        claim(&a, 0, |_| Ok(b"a".to_vec())).unwrap();
        // b, recovering at the same time from nothing of its own, finds a's epoch taken
        let Err(NotWritten::Taken(name)) = b.put(1, b"b") else { panic!("not taken") };
        assert!(name.ends_with(&format!("/windows/{}", lease_of(1))), "{name}");
        assert!(b.checkpoints.list().unwrap().is_empty(), "b wrote no checkpoint");
        // and one starting later stands by: a's epoch is the newest
        assert_eq!(b.newest().unwrap(), Some((1, false)));
        // once a stops on SIGTERM, b takes over at once, after a's epochs
        let mut l = Lease::claimed(1, Instant::now());
        assert!(matches!(checkpoint(&mut l, &a, b"a", true, Instant::now()), Step::Written { .. }));
        assert_eq!(wait_until_free(&b, secs(3600), &not_stopped(), &Metrics::default()).unwrap(), Some(2));
        assert_eq!(claim(&b, 2, |_| Ok(b"b".to_vec())).unwrap().epoch, 3);
        assert_eq!(b.checkpoints.list().unwrap(), [ckpt(3)]);
    }

    /// A lease this instance took is its own: an attempt whose checkpoint failed to be written
    /// after its lease was is tried again under that epoch, as any failed write is.
    #[test]
    fn a_lease_of_this_instances_own_is_not_taken() {
        let dir = Scratch::new("own-lease");
        let s = dir_stores(&dir);
        assert!(s.owner.create(&lease_of(4), s.holder.as_bytes()).unwrap());
        s.put(4, b"x").unwrap();
        assert_eq!(s.checkpoints.get(&ckpt(4)).unwrap(), b"x");
    }

    /// Any other failure to write is `Failed`, which the loop retries, never `Taken`, which
    /// stops the instance: here the volume's directory has become a file.
    #[test]
    fn a_write_the_store_fails_is_failed_not_taken() {
        let dir = Scratch::new("failed");
        let s = dir_stores(&dir);
        std::fs::remove_dir_all(&dir.0).unwrap();
        std::fs::write(&dir.0, b"").unwrap();
        let Err(NotWritten::Failed(e)) = s.put(4, b"x") else { panic!("not Failed") };
        assert!(format!("{e:#}").contains("00000000000000000004.lease"), "{e:#}");
        std::fs::remove_file(&dir.0).unwrap();
        s.put(4, b"x").unwrap(); // the same epoch, once the store is back
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_lease_is_due_an_interval_after_its_last_attempt_and_moves_to_the_next_epoch() {
        let t0 = Instant::now();
        let mut l = Lease::claimed(7, t0);
        assert_eq!(l.next(), 8);
        assert!(!l.due(t0 + secs(9), secs(10)));
        assert!(l.due(t0 + secs(10), secs(10)));
        assert!(!l.due(t0 - secs(1), secs(10)), "a clock read before the claim is not due");
        l.written(t0 + secs(10));
        assert_eq!(l, Lease::claimed(8, t0 + secs(10)));
        assert_eq!(l.next(), 9);
    }

    /// The fencing rule: after a failed write the same epoch is tried again, an interval after
    /// the failure, until one is written; the lease still counts from the last one written.
    #[test]
    fn a_failed_write_is_tried_again_under_the_same_epoch() {
        let t0 = Instant::now();
        let mut l = Lease::claimed(7, t0);
        l.failed(t0 + secs(12)); // epoch 8 failed, after a 2 s timeout
        assert_eq!((l.next(), l.failures, l.renewed), (8, 1, t0));
        assert!(!l.due(t0 + secs(21), secs(10)));
        assert!(l.due(t0 + secs(22), secs(10)));
        l.failed(t0 + secs(22));
        assert_eq!((l.next(), l.failures), (8, 2));
        l.written(t0 + secs(32));
        assert_eq!((l.next(), l.failures, l.renewed), (9, 0, t0 + secs(32)));
    }

    /// Producing stops once the lease is `fence` old, when a standing-by instance may take over,
    /// and starts again with the next checkpoint written.
    #[test]
    fn producing_pauses_once_the_lease_is_fence_old() {
        let t0 = Instant::now();
        let mut l = Lease::claimed(1, t0);
        assert!(!l.paused(t0 + secs(14), secs(15)));
        assert!(l.paused(t0 + secs(15), secs(15)));
        l.failed(t0 + secs(14));
        assert!(!l.paused(t0 + secs(14), secs(15)), "a failure alone does not pause");
        assert!(l.paused(t0 + secs(15), secs(15)));
        l.written(t0 + secs(20));
        assert!(!l.paused(t0 + secs(34), secs(15)));
    }

    /// Over any run of successes and failures, the epochs written are consecutive and every
    /// failed epoch is the one written next: an epoch is never skipped, whatever fails.
    #[test]
    fn written_epochs_are_consecutive_whatever_fails() {
        let t0 = Instant::now();
        for seed in 0u64..200 {
            let (mut l, mut rng, mut written, mut pending) = (Lease::claimed(1, t0), seed, vec![1], None);
            for i in 1..60 {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let (epoch, at) = (l.next(), t0 + secs(i));
                if let Some(p) = pending {
                    assert_eq!(epoch, p, "seed {seed}: a failed epoch is tried again");
                }
                if rng >> 62 == 0 {
                    l.failed(at);
                    pending = Some(epoch);
                } else {
                    l.written(at);
                    (written, pending) = ([written, vec![epoch]].concat(), None);
                }
            }
            assert!(written.windows(2).all(|w| w[1] == w[0] + 1), "seed {seed}: {written:?}");
        }
    }

    #[test]
    fn a_takeover_is_three_intervals_by_default_and_must_be_more_than_two() {
        let t = Timing::of(10, None).unwrap();
        assert_eq!(t, Timing { interval: secs(10), takeover: secs(30), fence: secs(15) });
        assert_eq!(Timing::of(10, Some(21)).unwrap().fence, Duration::from_millis(10_500));
        let e = Timing::of(10, Some(20)).unwrap_err();
        assert_eq!(e.to_string(), "--takeover must be more than twice --interval");
    }

    /// A stop signal checkpoints at once, due or not.
    #[test]
    fn a_checkpoint_is_attempted_once_due_or_at_once_when_stopping() {
        let t0 = Instant::now();
        let l = Lease::claimed(7, t0);
        assert!(!l.attempt(t0 + secs(9), secs(10), false));
        assert!(l.attempt(t0 + secs(9), secs(10), true));
        assert!(l.attempt(t0 + secs(10), secs(10), false));
    }

    /// A store whose directory has become a file: every write fails.
    fn break_store(dir: &Scratch) {
        std::fs::remove_dir_all(&dir.0).unwrap();
        std::fs::write(&dir.0, b"").unwrap();
    }

    fn mend_store(dir: &Scratch) {
        std::fs::remove_file(&dir.0).unwrap();
    }

    #[test]
    fn a_claim_creates_the_next_epoch_and_consumes_the_newest_ones_release() {
        let dir = Scratch::new("claims");
        let (s, theirs) = (dir_stores(&dir), dir_stores(&dir));
        s.checkpoints.create(&released(4), &[]).unwrap();
        let lease = claim(&s, 4, |epoch| Ok(format!("state {epoch}").into_bytes())).unwrap();
        assert_eq!((lease.epoch, lease.next(), lease.failures), (5, 6, 0));
        assert_eq!(s.checkpoints.get(&ckpt(5)).unwrap(), b"state 5");
        assert_eq!(s.checkpoints.list().unwrap(), [ckpt(5)], "the release is consumed");
        assert_eq!(s.owner.list().unwrap(), [lease_of(5)]);
        let taken = claim(&theirs, 4, |_| Ok(b"theirs".to_vec())).unwrap_err().to_string();
        assert!(taken.ends_with(&format!("/{} exists: {TAKEN}", lease_of(5))), "{taken}");
        assert_eq!(s.checkpoints.get(&ckpt(5)).unwrap(), b"state 5");
        break_store(&dir);
        let failed = format!("{:#}", claim(&s, 5, |_| Ok(vec![])).unwrap_err());
        assert!(failed.starts_with("claiming the pipeline with checkpoint 6: "), "{failed}");
        mend_store(&dir);
        let failed = format!("{:#}", claim(&s, 5, |_| bail!("encoding")).unwrap_err());
        assert_eq!(failed, "encoding");
    }

    /// Writes `bytes` as checkpoint `lease.next()`, as its thread does, and applies the outcome
    /// to the lease, as the data thread does.
    fn checkpoint(lease: &mut Lease, stores: &Stores, bytes: &[u8], last: bool, started: Instant) -> Step {
        let written = stores.put(lease.next(), bytes);
        step(lease, stores, written, last, started)
    }

    #[test]
    fn a_written_checkpoint_renews_the_lease_and_on_a_stop_signal_is_released() {
        let dir = Scratch::new("step-written");
        let s = dir_stores(&dir);
        let t0 = Instant::now();
        let mut l = Lease::claimed(1, t0);
        let step = checkpoint(&mut l, &s, b"2", false, t0 + secs(10));
        assert!(matches!(step, Step::Written { failures: 0 }), "{step:?}");
        assert_eq!(l, Lease::claimed(2, t0 + secs(10)));
        assert_eq!(s.checkpoints.list().unwrap(), [ckpt(2)]);
        let step = checkpoint(&mut l, &s, b"3", true, t0 + secs(12));
        assert!(matches!(step, Step::Written { failures: 0 }), "{step:?}");
        assert_eq!(l, Lease::claimed(3, t0 + secs(12)));
        let mut names = s.checkpoints.list().unwrap();
        names.sort();
        assert_eq!(names, [ckpt(2), ckpt(3)]);
        let mut names = s.owner.list().unwrap();
        names.sort();
        assert_eq!(names, [lease_of(2), lease_of(3), released(3)]);
        assert_eq!(wait_until_free(&s, secs(3600), &not_stopped(), &Metrics::default()).unwrap(), Some(3));
    }

    /// A failed write only ages the lease and is tried again under the same epoch; on a stop
    /// signal the instance stops without it (the restart replays).
    #[test]
    fn a_failed_checkpoint_is_tried_again_under_its_epoch_unless_stopping() {
        let dir = Scratch::new("step-failed");
        let s = dir_stores(&dir);
        let t0 = Instant::now();
        let mut l = Lease::claimed(1, t0);
        break_store(&dir);
        let step = checkpoint(&mut l, &s, b"2", false, t0);
        assert!(matches!(&step, Step::Failed(e) if format!("{e:#}").contains(&lease_of(2))), "{step:?}");
        assert_eq!((l.next(), l.failures, l.renewed), (2, 1, t0));
        let Step::Stop(e) = checkpoint(&mut l, &s, b"2", true, t0) else { panic!("not stopped") };
        assert!(format!("{e:#}").starts_with("stopping without a last checkpoint (the restart replays): "), "{e:#}");
        mend_store(&dir);
        let step = checkpoint(&mut l, &s, b"2", false, t0 + secs(30));
        assert!(matches!(step, Step::Written { failures: 1 }), "{step:?}");
        assert_eq!(l, Lease::claimed(2, t0 + secs(30)));
        assert_eq!(s.checkpoints.get(&ckpt(2)).unwrap(), b"2");
    }

    /// Finding the epoch taken stops the instance: another one claimed it, or, after a failed
    /// attempt, that attempt landed after all.
    #[test]
    fn a_taken_checkpoint_stops_the_instance_saying_why() {
        let dir = Scratch::new("step-taken");
        let s = dir_stores(&dir);
        let t0 = Instant::now();
        dir_stores(&dir).put(2, b"theirs").unwrap();
        let mut l = Lease::claimed(1, t0);
        let Step::Stop(e) = checkpoint(&mut l, &s, b"mine", false, t0) else { panic!("not stopped") };
        assert!(e.to_string().ends_with(&format!("/{} exists: {TAKEN}", lease_of(2))), "{e}");
        l.failed(t0);
        let Step::Stop(e) = checkpoint(&mut l, &s, b"mine", true, t0) else { panic!("not stopped") };
        assert!(e.to_string().contains("an attempt that failed landed after all, or"), "{e}");
        assert_eq!(s.checkpoints.list().unwrap(), [ckpt(2)], "neither written nor released");
        assert_eq!(s.owner.list().unwrap(), [lease_of(2)], "neither written nor released");
        // this instance's lease, a checkpoint another instance of a build before leases wrote
        let mut l = Lease::claimed(2, t0);
        assert!(s.checkpoints.create(&ckpt(3), b"theirs").unwrap());
        let Step::Stop(e) = checkpoint(&mut l, &s, b"mine", false, t0) else { panic!("not stopped") };
        assert!(e.to_string().ends_with(&format!("/{} exists: {TAKEN}", ckpt(3))), "{e}");
    }

    /// The sink partitions of `finish`'s tests and their ends when the instance started.
    fn ends() -> SinkEnds {
        [(tp("test.1m", 0), 100), (tp("test.1m", 1), 7), (tp("test.1m", 2), 0)].into()
    }

    /// A checkpoint's cut of `e`: the state copied, the source position, no sinks yet.
    fn cut(e: &Engine, epoch: u64, source_at: i64) -> Cut {
        Cut::Copy { checkpoint: Checkpoint::of(e, epoch, offsets(source_at), vec![]), generation: 0 }
    }

    /// A checkpoint waits for the acks of what was sent before its cut, polling the producer for
    /// them (librdkafka reports deliveries to whoever polls), then writes the cut's state and
    /// source positions with the sink offsets those acks give: past the end at start where a
    /// message was acked, the end at start where none was.
    #[test]
    fn a_checkpoint_waits_for_the_acks_of_what_was_sent_before_its_cut_then_writes_it() {
        let dir = Scratch::new("finish");
        let s = dir_stores(&dir);
        let (l, e) = (Ledger::default(), busy_engine(0, 50));
        for _ in 0..3 {
            l.sending(0);
        }
        l.delivered(0, Some(("test.1m", 2, 3)));
        let polls = std::cell::RefCell::new(vec![]);
        let poll = |d: Duration| {
            polls.borrow_mut().push(d);
            match polls.borrow().len() {
                1 => l.delivered(0, Some(("test.1m", 0, 120))),
                2 => {}
                _ => l.delivered(0, Some(("test.1m", 2, 4))),
            }
        };
        let done = finish(cut(&e, 6, 50), &l, poll, &ends(), &s, secs(5)).unwrap();
        assert!(done.put.is_ok(), "{done:?}");
        assert_eq!(polls.borrow().len(), 3, "polled until the last ack came");
        assert!(polls.borrow().iter().all(|d| *d <= Duration::from_millis(10)), "{polls:?}");
        let bytes = s.checkpoints.get(&ckpt(6)).unwrap();
        assert_eq!(done.bytes, bytes.len());
        assert!(done.at <= Instant::now());
        let c = Checkpoint::decode(&bytes).unwrap();
        assert_eq!((c.epoch, c.sources.clone()), (6, offsets(50)));
        assert_eq!(c.sinks, [("test.1m".into(), 0, 121), ("test.1m".into(), 1, 7), ("test.1m".into(), 2, 5)]);
        let mut restored = engine();
        c.restore(&mut restored).unwrap();
        assert_eq!(state(&restored), state(&e));
    }

    /// The sinks not acking what was sent before the cut within the timeout (librdkafka fails a
    /// message `fence` after it was sent, so this is no broker being slow) stops the instance
    /// without the checkpoint: its sink offsets cannot be known.
    #[test]
    fn a_checkpoint_whose_acks_do_not_come_in_time_is_not_written_and_stops_the_instance() {
        let dir = Scratch::new("finish-late");
        let s = dir_stores(&dir);
        let l = Ledger::default();
        l.sending(0);
        l.sending(1);
        l.delivered(1, Some(("test.1m", 0, 200)));
        let (started, polled) = (Instant::now(), std::cell::Cell::new(Duration::ZERO));
        let poll = |d: Duration| {
            assert!(d <= Duration::from_millis(10), "{d:?}");
            polled.set(polled.get() + d);
            std::thread::sleep(d);
        };
        let e = finish(cut(&engine(), 2, 0), &l, poll, &ends(), &s, Duration::from_millis(200));
        let e = e.unwrap_err().to_string();
        assert_eq!(e, "the sinks did not ack what was sent before checkpoint 2 within 0.2s");
        // it polled until the deadline, never asking for more than was left (a loaded machine
        // oversleeps each poll, so the time asked for may add up to less than the timeout)
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(polled.get() > Duration::ZERO && polled.get() <= Duration::from_millis(200), "{:?}", polled.get());
        assert!(s.checkpoints.list().unwrap().is_empty(), "nothing written");
        // with the acks in, it is written
        l.delivered(0, Some(("test.1m", 1, 7)));
        let done = finish(cut(&engine(), 2, 0), &l, |_| {}, &ends(), &s, Duration::ZERO).unwrap();
        assert!(done.put.is_ok());
        let sinks = Checkpoint::decode(&s.checkpoints.get(&ckpt(2)).unwrap()).unwrap().sinks;
        assert_eq!(sinks, [("test.1m".into(), 0, 100), ("test.1m".into(), 1, 8), ("test.1m".into(), 2, 0)]);
    }

    /// A message sent before the cut that failed is not in its sink; the checkpoint would skip
    /// it for good. It is not written, and the instance stops (the restart replays the message).
    #[test]
    fn a_failed_delivery_before_the_cut_stops_the_instance_without_its_checkpoint() {
        let dir = Scratch::new("finish-failed");
        let s = dir_stores(&dir);
        let l = Ledger::default();
        l.sending(0);
        l.sending(0);
        l.delivered(0, Some(("test.1m", 0, 100)));
        let poll = |_| l.delivered(0, None);
        let e = finish(cut(&engine(), 4, 0), &l, poll, &ends(), &s, secs(5)).unwrap_err();
        assert_eq!(e.to_string(), "1 sink deliveries failed before checkpoint 4");
        assert!(s.checkpoints.list().unwrap().is_empty(), "nothing written");
    }

    /// The write's outcome is the data thread's to act on (`step`): an epoch found taken or a
    /// store that fails is reported, not an error of the thread.
    #[test]
    fn a_checkpoint_found_taken_or_not_written_is_reported_to_the_data_thread() {
        let dir = Scratch::new("finish-put");
        let s = dir_stores(&dir);
        let l = Ledger::default();
        dir_stores(&dir).put(3, b"theirs").unwrap();
        let done = finish(cut(&engine(), 3, 0), &l, |_| {}, &ends(), &s, secs(1)).unwrap();
        assert!(matches!(done.put, Err(NotWritten::Taken(_))), "{done:?}");
        assert_eq!(s.checkpoints.get(&ckpt(3)).unwrap(), b"theirs");
        break_store(&dir);
        let done = finish(cut(&engine(), 4, 0), &l, |_| {}, &ends(), &s, secs(1)).unwrap();
        assert!(matches!(done.put, Err(NotWritten::Failed(_))), "{done:?}");
        mend_store(&dir);
        assert!(s.checkpoints.list().unwrap().is_empty(), "the broken store lost epoch 3, and 4 was not written");
    }

    /// A checkpoint the data thread encoded itself (a state too large to copy, or the last one)
    /// is written as it is: its sinks were taken there, so its thread waits for no ack, even with
    /// later messages still in flight.
    #[test]
    fn a_checkpoint_encoded_on_the_data_thread_is_only_written() {
        let dir = Scratch::new("finish-encoded");
        let s = dir_stores(&dir);
        let l = Ledger::default();
        l.sending(0);
        l.sending(1);
        let bytes = Checkpoint::of(&busy_engine(0, 20), 5, offsets(20), offsets(2)).encode();
        let cut = Cut::Encoded { epoch: 5, bytes: bytes.clone() };
        let done = finish(cut, &l, |_| panic!("polled"), &ends(), &s, Duration::ZERO).unwrap();
        assert!(done.put.is_ok(), "{done:?}");
        assert_eq!((done.bytes, s.checkpoints.get(&ckpt(5)).unwrap()), (bytes.len(), bytes));
        let cut = Cut::Encoded { epoch: 5, bytes: b"again".to_vec() };
        let done = finish(cut, &l, |_| panic!("polled"), &ends(), &s, Duration::ZERO).unwrap();
        assert!(matches!(done.put, Err(NotWritten::Taken(_))), "{done:?}");
    }

    /// A cut copies the state unless the last checkpoint encoded to more than 128 MiB: holding a
    /// copy of such a state risks the pod's memory limit.
    #[test]
    fn a_state_whose_checkpoint_is_past_128_mib_is_not_copied() {
        assert!(copies_state(0), "before the first checkpoint");
        assert!(copies_state(55 << 20), "a trade-size state at a full feed's rate, 55 MB");
        assert!(copies_state(COPY_CHECKPOINT_UP_TO));
        assert!(!copies_state(COPY_CHECKPOINT_UP_TO + 1));
        assert_eq!(COPY_CHECKPOINT_UP_TO, 128 << 20);
    }

    /// A checkpoint written on a thread of its own while the data
    /// thread goes on consuming and producing. The data thread takes the cut, then feeds its
    /// engine and sends and acks messages of the next generation, at higher offsets, the whole
    /// time the checkpoint's thread waits for the cut's own acks (delivered one per poll) and
    /// writes. The checkpoint holds the state and positions of the cut and the sink offsets of
    /// what was sent before it, and a restart from it continues as the data thread did.
    #[test]
    fn a_checkpoint_is_written_while_the_loop_keeps_consuming_and_producing() {
        let dir = Scratch::new("finish-concurrent");
        let s = dir_stores(&dir);
        let l = Arc::new(Ledger::default());
        let mut e = busy_engine(0, 40);
        // before the cut: 20 messages to partition 0 (offsets 100..120), none acked yet
        for _ in 0..20 {
            l.sending(0);
        }
        let at_cut = state(&e);
        let c = cut(&e, 9, 40);
        let thread = {
            let (l, s) = (l.clone(), s.clone());
            let acked = std::sync::atomic::AtomicI64::new(100);
            std::thread::spawn(move || {
                let poll = |_| {
                    std::thread::sleep(Duration::from_millis(2));
                    l.delivered(0, Some(("test.1m", 0, acked.fetch_add(1, Ordering::Relaxed))));
                };
                finish(c, &l, poll, &ends(), &s, secs(5))
            })
        };
        // the loop goes on: rows in, messages of generation 1 out and acked, until it is written
        let (mut offset, mut i, mut out) = (120, 40, vec![]);
        while !thread.is_finished() {
            e.insert("trades_source", trades(i * 1_000_000, 1), &mut out);
            l.sending(1);
            l.delivered(1, Some(("test.1m", (i % 2) as i32, offset)));
            (offset, i) = (offset + 1, i + 1);
        }
        let done = thread.join().unwrap().unwrap();
        assert!(done.put.is_ok());
        assert!(i > 60, "the loop went on while the checkpoint was written: {} rows", i - 40);
        assert_ne!(state(&e), at_cut, "the engine moved on");
        let c = Checkpoint::decode(&s.checkpoints.get(&ckpt(9)).unwrap()).unwrap();
        assert_eq!(c.sources, offsets(40));
        assert_eq!(c.sinks[..2], [("test.1m".into(), 0, 120), ("test.1m".into(), 1, 7)], "the cut's acks only");
        let mut restored = engine();
        c.restore(&mut restored).unwrap();
        assert_eq!(state(&restored), at_cut);
        // replayed from the cut, the restored engine emits what the running one did after it
        let (mut replayed, mut again) = (vec![], vec![]);
        for j in 40..i {
            restored.insert("trades_source", trades(j * 1_000_000, 1), &mut replayed);
        }
        let mut running = busy_engine(0, 40);
        for j in 40..i {
            running.insert("trades_source", trades(j * 1_000_000, 1), &mut again);
        }
        assert_eq!(replayed.len(), again.len());
        assert_eq!(state(&restored), state(&e));
        assert_eq!(state(&running), state(&e));
        assert_eq!(out.len(), again.len());
    }

    fn tp(t: &str, p: i32) -> (String, i32) {
        (t.to_string(), p)
    }

    /// Each partition counts from its position, or from where it started if it delivered
    /// nothing; an end that cannot be read, or one behind the position, counts nothing.
    #[test]
    fn the_lag_sums_every_assigned_partition() {
        let started_at = HashMap::from([(tp("a", 0), 10), (tp("a", 1), 3), (tp("a", 2), 0), (tp("b", 0), 8)]);
        let positions = HashMap::from([(tp("a", 0), 12), (tp("b", 0), 9)]);
        let ends = HashMap::from([(tp("a", 0), 17), (tp("a", 1), 7), (tp("b", 0), 5)]);
        let end = |t: &str, p| ends.get(&tp(t, p)).copied();
        assert_eq!(lag(&started_at, &positions, end), 5 + 4);
    }

    #[test]
    fn a_source_topic_that_gained_partitions_is_found() {
        let (a, b) = ("a".to_string(), "b".to_string());
        let assigned = HashMap::from([(&a, 1), (&b, 2)]);
        // a checkpoint may name more of a topic's partitions than were assigned
        let positions = HashMap::from([(tp("a", 0), 1), (tp("a", 1), 1)]);
        let now = |counts: [Option<usize>; 2]| move |t: &str| counts[usize::from(t == "b")];
        assert_eq!(grown(&assigned, &positions, now([Some(2), Some(2)])), None);
        assert_eq!(grown(&assigned, &positions, now([None, None])), None);
        assert_eq!(grown(&assigned, &positions, now([Some(3), Some(2)])), Some((&a, 3)));
        assert_eq!(grown(&assigned, &positions, now([Some(2), Some(3)])), Some((&b, 3)));
    }

    /// Quiet means every assigned partition is read to its known end: a partition that never
    /// delivered counts from where it started, and an end not known is not quiet.
    #[test]
    fn a_source_is_caught_up_once_every_partition_is_read_to_its_end() {
        let started_at = HashMap::from([(tp("a", 0), 10), (tp("a", 1), 3)]);
        let positions = HashMap::from([(tp("a", 0), 12)]);
        let end = |e0: Option<i64>, e1: Option<i64>| move |_: &str, p: i32| if p == 0 { e0 } else { e1 };
        assert!(caught_up(&started_at, &positions, end(Some(12), Some(3))));
        assert!(caught_up(&started_at, &positions, end(Some(11), Some(0))));
        assert!(!caught_up(&started_at, &positions, end(Some(13), Some(3))));
        assert!(!caught_up(&started_at, &positions, end(Some(12), Some(4))));
        assert!(!caught_up(&started_at, &positions, end(Some(12), None)));
        assert!(caught_up(&HashMap::new(), &positions, end(None, None)));
    }

    /// Waits until `f` holds, for at most 10 s.
    fn eventually(what: &str, f: impl Fn() -> bool) {
        let t0 = Instant::now();
        while !f() {
            assert!(t0.elapsed() < secs(10), "{what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The bookkeeping thread keeps the lag, reports a grown topic and answers whether the
    /// sources are quiet, each from the positions it was asked with.
    #[test]
    fn the_bookkeeper_answers_from_the_positions_it_was_asked_with() {
        let started_at = HashMap::from([(tp("a", 0), 0), (tp("a", 1), 0)]);
        let parts = Arc::new(AtomicU64::new(2));
        let seen = parts.clone();
        let m = Arc::new(Metrics::default());
        let end = |_: &str, p: i32| Some(if p == 0 { 10 } else { 5 });
        let partitions = move |_: &str| Some(seen.load(Ordering::Relaxed) as usize);
        let books = Bookkeeper::start(started_at, HashMap::from([("a".into(), 2)]), end, partitions, m.clone());
        assert!(books.ask(Ask::Checkpointed(HashMap::from([(tp("a", 0), 4)]))));
        eventually("the lag", || m.consumer_lag.load(Ordering::Relaxed) == 6 + 5);
        assert_eq!(books.grown(), None);
        parts.store(3, Ordering::Relaxed);
        assert!(books.ask(Ask::Checkpointed(HashMap::from([(tp("a", 0), 10), (tp("a", 1), 5)]))));
        eventually("growth", || books.grown().is_some());
        assert_eq!(books.grown(), Some(("a".to_string(), 3)));
        assert_eq!(m.consumer_lag.load(Ordering::Relaxed), 0);
    }

    /// While the loop is idle it asks once a second whether the sources are read to their end,
    /// and hears each answer once; an answer to positions it has consumed past does not count.
    #[test]
    fn quiet_is_asked_once_a_second_and_heard_once_for_the_positions_the_loop_has() {
        let started_at = HashMap::from([(tp("a", 0), 0), (tp("a", 1), 0)]);
        let end = |_: &str, p: i32| Some(if p == 0 { 10 } else { 5 });
        let m = Arc::new(Metrics::default());
        let mut books = Bookkeeper::start(started_at, HashMap::new(), end, |_: &str| None, m);
        let answered = |b: &Bookkeeper, n| eventually("an answer", || b.answered.load(Ordering::Relaxed) == n);
        let (behind, read) = (HashMap::from([(tp("a", 0), 10)]), HashMap::from([(tp("a", 0), 10), (tp("a", 1), 5)]));
        let t0 = Instant::now();
        assert!(!books.quiet(&behind, t0)); // not a second since the start: not asked
        assert!(!books.quiet(&behind, t0 + secs(1)));
        answered(&books, 1);
        assert!(!books.quiet(&read, t0 + secs(1))); // not read to its end when asked
        assert!(!books.quiet(&read, t0 + secs(2)));
        answered(&books, 2);
        assert!(books.quiet(&read, t0 + secs(2)));
        assert!(!books.quiet(&read, t0 + secs(2)), "heard once");
        assert!(!books.quiet(&read, t0 + secs(3)));
        answered(&books, 3);
        books.consumed();
        assert!(!books.quiet(&read, t0 + secs(3)), "asked before rows were consumed");
        assert!(!books.quiet(&read, t0 + secs(4)));
        answered(&books, 4);
        assert!(books.quiet(&read, t0 + secs(4)));
    }

    /// A broker that does not answer holds up the bookkeeping thread only: the data thread's
    /// asks return at once, and those made while it is busy are dropped.
    #[test]
    fn a_slow_broker_never_holds_up_the_asker() {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = std::sync::Mutex::new(rx);
        let end = move |_: &str, _: i32| {
            let _ = rx.lock().unwrap().recv(); // until the test ends
            None
        };
        let m = Arc::new(Metrics::default());
        let mut books = Bookkeeper::start(HashMap::from([(tp("a", 0), 0)]), HashMap::new(), end, |_: &str| None, m);
        let t0 = Instant::now();
        let (positions, later) = (HashMap::new(), t0 + secs(1));
        assert!(!books.quiet(&positions, later));
        assert_eq!(books.asked, 1);
        let asked: Vec<bool> = (0..10).map(|_| books.ask(Ask::Checkpointed(HashMap::new()))).collect();
        // an unanswered quiet ask is not made again
        assert!(!books.quiet(&positions, later + secs(1)) && !books.quiet(&positions, later + secs(5)));
        assert!(t0.elapsed() < Duration::from_secs(1));
        assert_eq!(books.asked, 1);
        assert!(!asked[9], "{asked:?}");
        drop(tx);
    }

    /// Two instances on one volume (a rolling update on a ReadWriteMany claim, or both pods on
    /// one node): exactly one claims each epoch.
    #[test]
    fn of_two_instances_claiming_one_epoch_exactly_one_wins() {
        let dir = Scratch::new("claim");
        // of one revision, and of two
        let (a, b, c) = (dir_stores(&dir), dir_stores(&dir), Stores::open(dir.path(), "windows", 0xee).unwrap());
        for epoch in 1..50 {
            let b = if epoch % 2 == 0 { &b } else { &c };
            let (ra, rb) = std::thread::scope(|sc| {
                let ha = sc.spawn(|| a.put(epoch, b"a").is_ok());
                let hb = sc.spawn(|| b.put(epoch, b"b").is_ok());
                (ha.join().unwrap(), hb.join().unwrap())
            });
            assert!(ra ^ rb, "epoch {epoch}: {ra} {rb}");
        }
    }

    /// Release markers go with their checkpoints: a claim that found its epoch taken stops
    /// before it consumes the one it restored, which no one reads after a newer checkpoint.
    #[test]
    fn the_collector_keeps_the_newest_5_checkpoints_their_releases_and_everything_else() {
        let dir = Scratch::new("gc");
        let s = dir_store(&dir);
        for e in 1..=9 {
            s.create(&ckpt(e), b"x").unwrap();
        }
        for e in [2, 4, 5, 9] {
            s.create(&released(e), b"").unwrap();
        }
        s.create("notes.txt", b"").unwrap();
        s.create("4.released", b"").unwrap(); // not a name `released` writes
        collect(s.as_ref(), ".ckpt").unwrap();
        let mut left = s.list().unwrap();
        left.sort();
        let others = [released(5), released(9), "notes.txt".to_string(), "4.released".to_string()];
        let mut want: Vec<String> = (5..=9).map(ckpt).chain(others).collect();
        want.sort();
        assert_eq!(left, want);
        collect(s.as_ref(), ".ckpt").unwrap(); // nothing more to do
        assert_eq!(s.list().unwrap().len(), 9);
    }

    /// Without a checkpoint, no marker is known to be old.
    #[test]
    fn without_checkpoints_the_collector_deletes_no_release() {
        let dir = Scratch::new("gc-empty");
        let s = dir_store(&dir);
        s.create(&released(3), b"").unwrap();
        collect(s.as_ref(), ".ckpt").unwrap();
        assert_eq!(s.list().unwrap(), [released(3)]);
    }

    /// The collector runs on its own thread, asked after each checkpoint.
    #[test]
    fn the_collector_thread_collects_when_asked() {
        let dir = Scratch::new("gcthread");
        let s = dir_stores(&dir);
        for e in 1..=8 {
            s.checkpoints.create(&ckpt(e), b"x").unwrap();
            s.owner.create(&lease_of(e), b"x").unwrap();
        }
        let gc = collector(s.clone());
        gc.send(()).unwrap();
        let t0 = Instant::now();
        while s.checkpoints.list().unwrap().len() > 5 || s.owner.list().unwrap().len() > 5 {
            assert!(t0.elapsed() < Duration::from_secs(10), "not collected: {:?}", s.checkpoints.list().unwrap());
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut left = s.checkpoints.list().unwrap();
        left.sort();
        assert_eq!(left, (4..=8).map(ckpt).collect::<Vec<_>>());
        let mut left = s.owner.list().unwrap();
        left.sort();
        assert_eq!(left, (4..=8).map(lease_of).collect::<Vec<_>>());
    }

    #[test]
    fn names_are_zero_padded_so_they_sort_by_epoch() {
        assert_eq!(ckpt(42), "00000000000000000042.ckpt");
        assert_eq!(released(42), "00000000000000000042.released");
        assert_eq!(ckpt(u64::MAX), "18446744073709551615.ckpt");
        assert_eq!(epochs([ckpt(9), ckpt(10), released(11)]).iter().map(|e| e.0).collect::<Vec<_>>(), [10, 9]);
    }

    fn not_stopped() -> std::sync::atomic::AtomicBool {
        std::sync::atomic::AtomicBool::new(false)
    }

    #[test]
    fn an_empty_store_is_free_at_once() {
        let dir = Scratch::new("free");
        let s = dir_stores(&dir);
        let t0 = Instant::now();
        assert_eq!(wait_until_free(&s, Duration::from_secs(60), &not_stopped(), &Metrics::default()).unwrap(), Some(0));
        assert!(t0.elapsed() < Duration::from_secs(1));
    }

    /// SIGTERM releases the newest checkpoint: the next instance takes over without waiting.
    #[test]
    fn a_released_newest_checkpoint_is_free_at_once() {
        let dir = Scratch::new("released");
        let s = dir_stores(&dir);
        s.checkpoints.create(&ckpt(4), b"x").unwrap();
        s.checkpoints.create(&released(4), b"").unwrap();
        let t0 = Instant::now();
        assert_eq!(wait_until_free(&s, Duration::from_secs(60), &not_stopped(), &Metrics::default()).unwrap(), Some(4));
        assert!(t0.elapsed() < Duration::from_secs(1));
    }

    /// A release of an older epoch says nothing about the newest: its instance may be running.
    #[test]
    fn a_checkpoint_newer_than_the_released_one_is_waited_out() {
        let dir = Scratch::new("stale");
        let s = dir_stores(&dir);
        s.checkpoints.create(&ckpt(4), b"x").unwrap();
        s.checkpoints.create(&released(4), b"").unwrap();
        s.checkpoints.create(&ckpt(5), b"x").unwrap();
        let (t0, m) = (Instant::now(), Metrics::default());
        assert_eq!(wait_until_free(&s, Duration::from_millis(1200), &not_stopped(), &m).unwrap(), Some(5));
        assert!(t0.elapsed() >= Duration::from_millis(1200), "{:?}", t0.elapsed());
        assert_eq!(m.standby.load(Ordering::Relaxed), 1);
    }

    /// While the other instance keeps checkpointing, this one stands by; once it stops, this one
    /// takes over `takeover` after its last checkpoint.
    #[test]
    fn a_running_instance_is_waited_out_until_it_stops_checkpointing() {
        let dir = Scratch::new("running");
        let s = dir_stores(&dir);
        s.checkpoints.create(&ckpt(1), b"x").unwrap();
        let other = {
            let s = s.clone();
            std::thread::spawn(move || {
                // checkpoints far more often than the takeover: a busy runner's stall between
                // two must outlast 1.5 s for the waiter to take over early
                for e in 2..=11 {
                    std::thread::sleep(Duration::from_millis(100));
                    s.checkpoints.create(&ckpt(e), b"x").unwrap();
                }
            })
        };
        let t0 = Instant::now();
        let takeover = Duration::from_millis(1500);
        assert_eq!(wait_until_free(&s, takeover, &not_stopped(), &Metrics::default()).unwrap(), Some(11));
        other.join().unwrap();
        // the other's last checkpoint was at ~1 s: free no earlier than 1.5 s after it
        assert!(t0.elapsed() >= Duration::from_millis(2400), "{:?}", t0.elapsed());
    }

    /// Another revision's lease (its checkpoints are elsewhere) is waited out like a checkpoint
    /// of this one, and its release lets this one take over at once.
    #[test]
    fn another_revisions_lease_is_waited_out_unless_released() {
        let dir = Scratch::new("revision-lease");
        let s = dir_stores(&dir);
        s.checkpoints.create(&ckpt(3), b"x").unwrap();
        s.owner.create(&lease_of(4), b"theirs").unwrap();
        let (t0, m) = (Instant::now(), Metrics::default());
        assert_eq!(wait_until_free(&s, Duration::from_millis(1200), &not_stopped(), &m).unwrap(), Some(4));
        assert!(t0.elapsed() >= Duration::from_millis(1200), "{:?}", t0.elapsed());
        s.owner.create(&released(4), b"").unwrap();
        let t0 = Instant::now();
        assert_eq!(wait_until_free(&s, Duration::from_secs(60), &not_stopped(), &m).unwrap(), Some(4));
        assert!(t0.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn stopping_while_standing_by_gives_up() {
        let dir = Scratch::new("stop");
        let s = dir_stores(&dir);
        s.checkpoints.create(&ckpt(1), b"x").unwrap();
        let stop = std::sync::atomic::AtomicBool::new(true);
        assert_eq!(wait_until_free(&s, Duration::from_secs(60), &stop, &Metrics::default()).unwrap(), None);
    }

    /// A checkpoint written by `checkpoint`'s path (put) restores into the same state: the whole
    /// cycle on a directory, as a pod on a volume runs it.
    #[test]
    fn a_checkpoint_on_a_volume_restores_after_the_pod_is_replaced() {
        let dir = Scratch::new("cycle");
        let mut running = busy_engine(0, 10);
        let s = dir_store(&dir);
        let mut out = vec![];
        for epoch in 1..=12 {
            let t = 10 + epoch as i64 * 7;
            running.insert("trades_source", trades(t * 1_000_000, 3), &mut out);
            write(s.as_ref(), epoch, &running, t);
            collect(s.as_ref(), ".ckpt").unwrap();
        }
        assert_eq!(epochs(s.list().unwrap()).len(), 5);
        let mut replaced = engine();
        let r = restore(dir_store(&dir).as_ref(), &mut replaced, Start::Newest, &Metrics::default()).unwrap();
        assert_eq!(r, Some((offsets(94), offsets(9), None)), "epoch 12's");
        assert_eq!(state(&replaced), state(&running));
    }
}

#[cfg(test)]
mod iggy_idle_tests {
    use super::idle_close_target;
    #[test]
    fn idle_windows_close_only_to_confirmed_iggy_poll_time() {
        assert_eq!(idle_close_target(2000, 100, None), Some(1900), "Kafka keeps its existing clock semantics");
        assert_eq!(
            idle_close_target(2000, 100, Some(Some(1000))),
            Some(900),
            "an old EOF cannot close unseen newer data"
        );
        assert_eq!(
            idle_close_target(2000, 100, Some(None)),
            None,
            "startup, disconnect and stale polls vouch for nothing"
        );
        assert_eq!(
            idle_close_target(2000, 100, Some(Some(3000))),
            Some(1900),
            "proof never moves past the local clock"
        );
    }
}
