//! Window-close-to-emit latency of the example pipelines at the top of the hour.
//!
//! The feed is the memory bench's (`common/feed.rs`: a busy market's rates and symbols). Per pipeline:
//!
//! 1. **Warm-up.** `LAT_WARM` seconds of event time (default 3600: the 1h windows are full)
//!    are fed in 100 ms chunks per source, up to `LAT_PRE` seconds (default 10) before 15:00 UTC,
//!    a top of the hour that closes every interval up to 1h. The engine's state there is kept
//!    as a checkpoint in `LAT_CACHE` (default `target/latency-cache`), reused by later runs
//!    whose engine restores it; `LAT_FRESH=1` feeds it again.
//! 2. **Reference.** From that state, every message up to `LAT_POST` seconds after the top of
//!    the hour (default 20: the next 15s close too) is inserted on its own, in time order. A
//!    message's *due* time is the event time of the row whose insert emitted it: when an engine
//!    that took no time would have emitted it.
//! 3. **Replay**, `LAT_REPS` times (default 5) from the same state. The data loop is simulated
//!    on a virtual clock: a message arrives at its event time, a batch is what has arrived up to
//!    `LAT_BATCH_WAIT_US` (default 1000) after its first, at most 1000 messages, taken at once
//!    when the loop is behind; each run of one source in it is decoded (Protobuf) and inserted,
//!    as `brrrrr run` does. The loop's time is the engine's: the thread's CPU time, which a busy
//!    host does not inflate as it does the wall clock (`LAT_THREADS` > 1 closes on threads and
//!    measures the wall clock instead, as `LAT_WALL=1` does). A message is *emitted* when its
//!    view flushes it. Producing to Kafka is not modeled.
//!
//! A message's latency is its emit time less its due time. Reported per pipeline, the median
//! over the replays of: for the messages due in the 15 s from the top of the hour, their p50,
//! p99 and max latency (the last one out), the max of the 15s and of the 1h bars, the longest
//! batch (the stall); and the max latency of the next 15s close. `digest` hashes every message
//! of the replay (order-insensitive) and must not move; the reference's must equal it.
//!
//! Run: `cargo bench -p brrrrr-core --bench latency [-- <pipeline substring>]`.
//! `LAT_JSON` writes the results there. `LAT_PIPELINES` (default: the pipelines with windows of
//! trades) is a comma-separated list of substrings.
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::{Asof, Emit, Engine, Output};
use brrrrr_core::proto::{parse_proto, Codec};
use brrrrr_core::sql::{parse, Catalog};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[path = "common/feed.rs"]
mod feed;
use feed::{sources, Feed, Source};

/// The binary's allocator (crates/brrrrr/src/main.rs): the close is timed as `brrrrr run` runs it.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// 15:00 UTC: closes every interval's windows up to 1h (not 4h, not 1d).
const BOUNDARY: i64 = 1_790_002_800_000_000;
/// µs of a 100 ms slice.
const SLICE: i64 = 100_000;

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}

fn root(p: &str) -> String {
    format!("{}/../../{p}", env!("CARGO_MANIFEST_DIR"))
}

/// The loop's clock, µs: this thread's CPU time, or the wall clock with close threads.
struct Clock {
    wall: Option<std::time::Instant>,
}

impl Clock {
    fn now(&self) -> f64 {
        match self.wall {
            Some(t) => t.elapsed().as_nanos() as f64 / 1e3,
            None => {
                let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
                t.tv_sec as f64 * 1e6 + t.tv_nsec as f64 / 1e3
            }
        }
    }
}

fn hash(e: &Emit) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (&e.topic, &e.payload, &e.headers).hash(&mut h);
    h.finish()
}

/// The replay's output: each flush's messages, with the clock when the view flushed them.
struct Timed<'a> {
    clock: &'a Clock,
    pending: Vec<Emit>,
    flushed: Vec<(f64, Emit)>,
}

impl Output for Timed<'_> {
    fn push(&mut self, e: Emit) {
        self.pending.push(e);
    }
    fn flush(&mut self) {
        let at = self.clock.now();
        self.flushed.extend(self.pending.drain(..).map(|e| (at, e)));
    }
}

/// A message of the measured span: event time (µs), source, Protobuf bytes.
type Msg = (i64, usize, Vec<u8>);

struct Setup {
    cat: Catalog,
    srcs: Vec<Source>,
    codecs: Vec<Codec>,
}

impl Setup {
    fn engine(&self, threads: usize) -> Engine {
        let mut e = Engine::new(&self.cat).unwrap();
        e.set_asof(Asof::Exact); // the runtime's default
        if threads > 1 {
            e.set_close_threads(threads);
        }
        e
    }
}

/// The engine's state `pre` seconds before the boundary after `warm` seconds of feed, and the
/// generator there; the state from the cache when it restores.
fn warm_up(name: &str, s: &Setup, warm: i64, pre: i64, scale: f64) -> (Vec<u8>, Feed) {
    let start = BOUNDARY - (warm + pre) * 1_000_000;
    let mut feed = Feed::new(s.srcs.len(), scale);
    let dir = std::env::var("LAT_CACHE").unwrap_or_else(|_| root("target/latency-cache"));
    let path = format!("{dir}/{name}-w{warm}-p{pre}-s{scale}.ckpt");
    let fresh = env("LAT_FRESH", 0) == 1;
    let cached = std::fs::read(&path)
        .ok()
        .filter(|b| !fresh && Checkpoint::decode(b).and_then(|c| c.restore(&mut s.engine(1))).is_ok());
    let mut engine = cached.is_none().then(|| s.engine(1));
    let mut out = vec![];
    let t = std::time::Instant::now();
    let mut slice = start;
    while slice < BOUNDARY - pre * 1_000_000 {
        for (si, src) in s.srcs.iter().enumerate() {
            let rows: Vec<_> = feed.slice(si, src, slice).into_iter().map(|(_, r)| r).collect();
            if let (Some(e), false) = (engine.as_mut(), rows.is_empty()) {
                e.insert(&src.stream, rows, &mut out);
                out.clear();
            }
        }
        slice += SLICE;
    }
    let bytes = match (cached, engine) {
        (Some(b), _) => b,
        (None, Some(e)) => {
            let b = Checkpoint::of(&e, 1, vec![], vec![]).encode();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&path, &b).unwrap();
            eprintln!("{name}: warmed up in {:.0} s, state {} KiB", t.elapsed().as_secs_f64(), b.len() / 1024);
            b
        }
        (None, None) => unreachable!(),
    };
    (bytes, feed)
}

/// Every message of the measured span, in event time order (sources in order on ties).
fn measured(s: &Setup, feed: &mut Feed, pre: i64, post: i64) -> Vec<Msg> {
    let mut msgs = vec![];
    let mut slice = BOUNDARY - pre * 1_000_000;
    while slice < BOUNDARY + post * 1_000_000 {
        let at = msgs.len();
        for (si, src) in s.srcs.iter().enumerate() {
            for (t, row) in feed.slice(si, src, slice) {
                let mut buf = vec![];
                s.codecs[si].encode(&row, &mut buf);
                msgs.push((t, si, buf));
            }
        }
        msgs[at..].sort_by_key(|m| (m.0, m.1));
        slice += SLICE;
    }
    msgs
}

fn restore(s: &Setup, state: &[u8], threads: usize) -> Engine {
    let mut e = s.engine(threads);
    Checkpoint::decode(state).unwrap().restore(&mut e).unwrap();
    e
}

/// Each message's due times (several, if it is emitted several times), and the output's digest.
fn reference(s: &Setup, state: &[u8], msgs: &[Msg]) -> (HashMap<u64, Vec<i64>>, u64) {
    let mut e = restore(s, state, 1);
    let (mut due, mut digest, mut out) = (HashMap::<u64, Vec<i64>>::new(), 0u64, vec![]);
    for (t, si, bytes) in msgs {
        e.insert(&s.srcs[*si].stream, vec![s.codecs[*si].decode(bytes).unwrap()], &mut out);
        for m in out.drain(..) {
            let h = hash(&m);
            digest = digest.wrapping_add(h);
            due.entry(h).or_default().push(*t);
        }
    }
    (due, digest)
}

#[derive(Default, Clone)]
struct Run {
    /// Latencies (µs) of the messages due in the 15 s from the boundary, and their topics'
    /// last part (the interval).
    boundary: Vec<(f64, String)>,
    /// Latencies of the messages due after that (the next 15s close).
    next: Vec<f64>,
    /// The longest batch, µs.
    stall: f64,
    digest: u64,
    emitted: usize,
    /// Messages the reference never emitted (or emitted fewer times).
    unmatched: usize,
}

/// One replay of the measured span on the simulated data loop.
fn replay(s: &Setup, state: &[u8], msgs: &[Msg], due: &HashMap<u64, Vec<i64>>, threads: usize) -> Run {
    let (wait, cap) = (env("LAT_BATCH_WAIT_US", 1000i64) as f64, env("LAT_BATCH_MAX", 1000usize));
    let mut e = restore(s, state, threads);
    let wall = threads > 1 || env("LAT_WALL", 0) == 1;
    let clock = Clock { wall: wall.then(std::time::Instant::now) };
    let mut out = Timed { clock: &clock, pending: vec![], flushed: vec![] };
    let mut seen: HashMap<u64, usize> = HashMap::new();
    let mut run = Run::default();
    let (mut now, mut i) = (msgs[0].0 as f64, 0);
    while i < msgs.len() {
        // idle: the batch waits `wait` past its first message; behind: it takes what is queued
        let start = if msgs[i].0 as f64 > now { msgs[i].0 as f64 + wait } else { now };
        let mut j = i;
        while j < msgs.len() && j - i < cap && msgs[j].0 as f64 <= start {
            j += 1;
        }
        let c0 = clock.now();
        for chunk in msgs[i..j].chunk_by(|a, b| a.1 == b.1) {
            let si = chunk[0].1;
            let rows = chunk.iter().map(|m| s.codecs[si].decode(&m.2).unwrap()).collect();
            e.insert(&s.srcs[si].stream, rows, &mut out);
            out.flush(); // the runtime's safety net
        }
        let took = clock.now() - c0;
        run.stall = run.stall.max(took);
        for (at, m) in out.flushed.drain(..) {
            let emitted = start + (at - c0);
            let h = hash(&m);
            run.digest = run.digest.wrapping_add(h);
            run.emitted += 1;
            let k = seen.entry(h).or_default();
            let Some(&t) = due.get(&h).and_then(|d| d.get(*k)) else {
                run.unmatched += 1;
                continue;
            };
            *k += 1;
            let latency = emitted - t as f64;
            if (BOUNDARY..BOUNDARY + 15_000_000).contains(&t) {
                run.boundary.push((latency, m.topic.rsplit('.').next().unwrap_or("").to_string()));
            } else if t >= BOUNDARY + 15_000_000 {
                run.next.push(latency);
            }
        }
        now = start + took;
        i = j;
    }
    run
}

/// The batch that closes the top of the hour's windows, on its own frame for a profiler.
#[inline(never)]
fn boundary_close(s: &Setup, e: &mut Engine, msgs: &[Msg], out: &mut Vec<Emit>) {
    for chunk in msgs.chunk_by(|a, b| a.1 == b.1) {
        let si = chunk[0].1;
        let rows = chunk.iter().map(|m| s.codecs[si].decode(&m.2).unwrap()).collect();
        e.insert(&s.srcs[si].stream, rows, out);
    }
}

/// `LAT_PROFILE=n`: n times from the state, what comes before the top of the hour in one chunk
/// per source, then the first 100 ms past it (`boundary_close`), for `perf record`.
fn profile(s: &Setup, state: &[u8], msgs: &[Msg], n: usize) {
    let split = msgs.partition_point(|m| m.0 < BOUNDARY);
    let end = msgs.partition_point(|m| m.0 < BOUNDARY + 100_000);
    let mut before = msgs[..split].to_vec();
    before.sort_by_key(|m| m.1);
    for _ in 0..n {
        let (mut e, mut out) = (restore(s, state, 1), vec![]);
        for chunk in before.chunk_by(|a, b| a.1 == b.1) {
            let rows = chunk.iter().map(|m| s.codecs[m.1].decode(&m.2).unwrap()).collect();
            e.insert(&s.srcs[chunk[0].1].stream, rows, &mut out);
        }
        out.clear();
        let t = std::time::Instant::now();
        boundary_close(s, &mut e, &msgs[split..end], &mut out);
        eprintln!("boundary close: {:.1} ms, {} messages", t.elapsed().as_secs_f64() * 1e3, out.len());
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn median(mut v: Vec<f64>) -> f64 {
    pct(&mut v, 0.5)
}

fn main() {
    let filter: Option<String> = std::env::args().skip(1).find(|a| !a.starts_with('-'));
    let default = "bars,flow,quotes,returns,stats".to_string();
    let wanted: Vec<String> = std::env::var("LAT_PIPELINES").unwrap_or(default).split(',').map(String::from).collect();
    let (warm, pre, post) = (env("LAT_WARM", 3600i64), env("LAT_PRE", 10i64), env("LAT_POST", 20i64));
    let (reps, scale, threads) = (env("LAT_REPS", 5usize), env("LAT_SCALE", 1.0f64), env("LAT_THREADS", 1usize));
    let protos = parse_proto(&std::fs::read_to_string(root("fixtures/market.proto")).unwrap()).unwrap();
    let mut files: Vec<String> = std::fs::read_dir(root("fixtures/pipelines"))
        .unwrap()
        .map(|e| e.unwrap().path().to_string_lossy().into_owned())
        .filter(|f| f.ends_with(".sql"))
        .collect();
    files.sort();
    println!(
        "{:<14} {:>7} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}  digest",
        "pipeline", "msgs", "stall", "p50", "p99", "max", "max15s", "max1h", "next15s"
    );
    let mut json = serde_json::Map::new();
    for f in files {
        let name = std::path::Path::new(&f).file_stem().unwrap().to_string_lossy().into_owned();
        if !wanted.iter().any(|w| name.contains(w.as_str())) || filter.as_deref().is_some_and(|x| !name.contains(x)) {
            continue;
        }
        let cat = parse(&std::fs::read_to_string(&f).unwrap()).unwrap();
        let srcs = sources(&cat);
        let codecs = srcs
            .iter()
            .map(|s| {
                let msg = cat.streams[&s.stream].settings["format_schema"].split_once(':').unwrap().1;
                Codec::new(&protos[msg], &s.cols)
            })
            .collect();
        let setup = Setup { cat, srcs, codecs };
        let (state, mut feed) = warm_up(&name, &setup, warm, pre, scale);
        let msgs = measured(&setup, &mut feed, pre, post);
        if let Some(n) = std::env::var("LAT_PROFILE").ok().and_then(|n| n.parse().ok()) {
            profile(&setup, &state, &msgs, n);
            continue;
        }
        let (due, ref_digest) = reference(&setup, &state, &msgs);
        let runs: Vec<Run> = (0..reps).map(|_| replay(&setup, &state, &msgs, &due, threads)).collect();
        assert!(runs.windows(2).all(|p| p[0].digest == p[1].digest), "{name}: nondeterministic output");
        let r0 = &runs[0];
        let stat = |f: &dyn Fn(&Run) -> f64| median(runs.iter().map(f).collect()) / 1e3;
        let of = |iv: &'static str| {
            move |r: &Run| r.boundary.iter().filter(|b| b.1 == iv).map(|b| b.0).fold(f64::NAN, f64::max)
        };
        let lat = |r: &Run| r.boundary.iter().map(|b| b.0).collect::<Vec<f64>>();
        let row = serde_json::json!({
            "msgs": msgs.len(),
            "boundary_msgs": r0.boundary.len(),
            "emitted": r0.emitted,
            "unmatched": r0.unmatched,
            "stall_ms": stat(&|r| r.stall),
            "p50_ms": stat(&|r| pct(&mut lat(r), 0.5)),
            "p99_ms": stat(&|r| pct(&mut lat(r), 0.99)),
            "max_ms": stat(&|r| pct(&mut lat(r), 1.0)),
            "max_15s_ms": stat(&of("15s")),
            "max_1h_ms": stat(&of("1h")),
            "next_15s_max_ms": stat(&|r| pct(&mut r.next.clone(), 1.0)),
            "digest": format!("{:016x}", r0.digest),
            "reference_digest_matches": r0.digest == ref_digest,
        });
        let g = |k: &str| row[k].as_f64().unwrap_or(f64::NAN);
        println!(
            "{:<14} {:>7} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1}  {:016x}{}{}",
            name,
            r0.boundary.len(),
            g("stall_ms"),
            g("p50_ms"),
            g("p99_ms"),
            g("max_ms"),
            g("max_15s_ms"),
            g("max_1h_ms"),
            g("next_15s_max_ms"),
            r0.digest,
            if r0.digest == ref_digest { "" } else { "  REFERENCE DIFFERS" },
            if r0.unmatched > 0 { format!("  {} unmatched", r0.unmatched) } else { String::new() },
        );
        json.insert(name, row);
    }
    if let Ok(path) = std::env::var("LAT_JSON") {
        let doc = serde_json::json!({ "warm_s": warm, "pre_s": pre, "post_s": post, "reps": reps, "scale": scale,
            "threads": threads, "pipelines": json });
        std::fs::write(path, serde_json::to_string_pretty(&doc).unwrap() + "\n").unwrap();
    }
}
