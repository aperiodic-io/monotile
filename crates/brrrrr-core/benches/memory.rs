//! Memory of the engine on every example pipeline at a busy market's rates.
//!
//! Rows are encoded to Protobuf and decoded by the runtime's `Codec`, as Kafka messages are.
//! Every Protobuf source of the pipeline is fed a synthetic stream shaped like a busy market's
//! (`common/feed.rs`: `VENUES` for trades and quotes, `rate` for the others), over 500 or 250 symbols with a Zipf-like popularity and prices on a tick grid, in 100 ms
//! chunks per source, for `MEM_SECONDS` of event time (default 1200) at `MEM_SCALE` (default 1).
//!
//! Per pipeline it prints one line of `key=value`:
//! - `state`: live heap the engine keeps after the feed (bytes, output drained);
//! - `peak`: the highest live heap during the feed, input chunks and output included;
//! - `ckpt_peak`: heap on top of `state` while `Checkpoint::encode_of` runs (from the live state);
//! - `cut_cpu`, `cut_peak`: what the runtime's checkpoint costs (a copy taken at the cut, encoded
//!   on another thread): the CPU seconds its cut (`Checkpoint::of`, a copy of the state) holds the
//!   data thread up, and the heap on top of `state` while the copy is held and encoded
//!   (`try_encode`) on the checkpoint's thread;
//! - `ckpt`, `ckpt_hash`: the encoded checkpoint's size and FNV-1a hash (the same hash: the
//!   same bytes, so a change that keeps it writes checkpoints every build reads);
//! - `restore_peak`: heap on top of a fresh engine while a checkpoint is decoded and restored;
//! - `rss`, `hwm`: the process's resident set now and at its highest (kB, /proc/self/status);
//! - `digest`, `emitted`: an order-insensitive hash of the output, which a change must not move.
//!
//! Run: `cargo bench -p brrrrr-core --bench memory -- <pipeline substring>` (one per process,
//! so `hwm` is the pipeline's own).
use brrrrr_core::checkpoint::fnv64;
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::{Asof, Emit, Engine};
use brrrrr_core::proto::{parse_proto, Codec};
use brrrrr_core::sql::{parse, Catalog};
use peak_alloc::PeakAlloc;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

#[path = "common/feed.rs"]
mod feed;
use feed::{sources, Feed};

#[global_allocator]
static GLOBAL: PeakAlloc = PeakAlloc;

fn current() -> isize {
    GLOBAL.current_usage() as isize
}

fn peak() -> isize {
    GLOBAL.peak_usage() as isize
}

/// Starts a new peak at the current heap, which it returns.
fn reset_peak() -> isize {
    GLOBAL.reset_peak_usage();
    current()
}

fn digest(out: &[Emit]) -> u64 {
    out.iter().fold(0u64, |acc, m| {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        (&m.topic, &m.payload, &m.headers).hash(&mut h);
        acc.wrapping_add(h.finish())
    })
}

fn status(key: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

const T0: i64 = 1_790_000_000_000_000;

fn root(p: &str) -> String {
    format!("{}/../../{p}", env!("CARGO_MANIFEST_DIR"))
}

fn main() {
    let filter: Option<String> = std::env::args().skip(1).find(|a| !a.starts_with('-'));
    let seconds: i64 = std::env::var("MEM_SECONDS").ok().and_then(|s| s.parse().ok()).unwrap_or(1200);
    let scale: f64 = std::env::var("MEM_SCALE").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0);
    let mut files: Vec<String> = std::fs::read_dir(root("fixtures/pipelines"))
        .unwrap()
        .map(|e| e.unwrap().path().to_string_lossy().into_owned())
        .filter(|f| f.ends_with(".sql"))
        .collect();
    files.sort();
    for f in files {
        if filter.as_deref().is_some_and(|x| !f.contains(x)) {
            continue;
        }
        let name = std::path::Path::new(&f).file_stem().unwrap().to_string_lossy().into_owned();
        let cat = parse(&std::fs::read_to_string(&f).unwrap()).unwrap();
        if sources(&cat).is_empty() {
            continue; // no Kafka source to feed (the state pipeline)
        }
        measure(&name, &cat, seconds, scale);
    }
}

fn measure(name: &str, cat: &Catalog, seconds: i64, scale: f64) {
    let srcs = sources(cat);
    // rows go through the runtime's decoder: each is encoded to Protobuf and decoded again
    let protos = parse_proto(&std::fs::read_to_string(root("fixtures/market.proto")).unwrap()).unwrap();
    let codecs: Vec<Codec> = srcs
        .iter()
        .map(|s| {
            let msg = cat.streams[&s.stream].settings["format_schema"].split_once(':').unwrap().1;
            Codec::new(&protos[msg], &s.cols)
        })
        .collect();
    let mut buf = vec![];
    let before = current();
    let mut e = Engine::new(cat).unwrap();
    e.set_asof(Asof::Exact); // the runtime's default
    let plan = current() - before;
    let mut feed = Feed::new(srcs.len(), scale);
    let (mut out, mut rows_in, mut emitted, mut dg) = (vec![], 0u64, 0u64, 0u64);
    let base = reset_peak();
    let started = std::time::Instant::now();
    for slice in 0..seconds * 10 {
        let start = T0 + slice * 100_000;
        for (si, src) in srcs.iter().enumerate() {
            let rows: Vec<_> = feed
                .slice(si, src, start)
                .into_iter()
                .map(|(_, row)| {
                    buf.clear();
                    codecs[si].encode(&row, &mut buf);
                    codecs[si].decode(&buf).unwrap()
                })
                .collect();
            if rows.is_empty() {
                continue;
            }
            rows_in += rows.len() as u64;
            e.insert(&src.stream, rows, &mut out);
        }
        emitted += out.len() as u64;
        dg = dg.wrapping_add(digest(&out));
        out.clear();
    }
    let secs = started.elapsed().as_secs_f64();
    out.shrink_to_fit();
    let feed_peak = peak() - base;
    // the generator's books are not the engine's
    let state = current() - before - plan;

    if std::env::var("MEM_BREAKDOWN").is_ok() {
        breakdown(&e);
    }
    let at = reset_peak();
    let cpu = thread_cpu();
    let bytes = Checkpoint::encode_of(&e, 1, &[], &[], None).unwrap();
    let ckpt_cpu = thread_cpu() - cpu;
    let ckpt_peak = peak() - at;

    let at = reset_peak();
    let cpu = thread_cpu();
    let cut = Checkpoint::of(&e, 1, vec![], vec![]);
    let cut_cpu = thread_cpu() - cpu;
    assert!(cut.try_encode().unwrap() == bytes, "the cut's copy encodes to other bytes");
    drop(cut);
    let cut_peak = peak() - at;

    let mut fresh = Engine::new(cat).unwrap();
    fresh.set_asof(Asof::Exact);
    let at = reset_peak();
    let cpu = thread_cpu();
    let decoded = Checkpoint::decode(&bytes).unwrap();
    let decode_cpu = thread_cpu() - cpu;
    decoded.restore(&mut fresh).unwrap();
    let restore_peak = peak() - at;
    let restored = current() - at;
    drop(fresh);
    std::hint::black_box(&e);
    let (rss, hwm) = (status("VmRSS:"), status("VmHWM:"));

    // after rss/hwm: what older builds wrote (a copy of the state, its whole encoding, then
    // `zstd::bulk::compress`), to compare the bytes and the CPU time with
    let cpu = thread_cpu();
    let old = Checkpoint::of(&e, 1, vec![], vec![]);
    let body = postcard::to_allocvec(&old).unwrap();
    let mut reference = bytes[..14].to_vec();
    reference.extend(zstd::bulk::compress(&body, 3).unwrap());
    let ref_cpu = thread_cpu() - cpu;
    drop(old);
    let cpu = thread_cpu();
    let counted = postcard::serialize_with_flavor(&e.state_ref(), postcard::ser_flavors::Size::default()).unwrap();
    let size_cpu = thread_cpu() - cpu;
    std::hint::black_box(counted);
    let inflated = zstd::stream::decode_all(&bytes[14..]).unwrap();
    println!(
        "pipeline={name} rows={rows_in} secs={secs:.1} plan={plan} state={state} peak={feed_peak} ckpt_peak={ckpt_peak} ckpt={} ckpt_hash={:016x} \
         restore_peak={restore_peak} restored={restored} rss={rss} hwm={hwm} emitted={emitted} digest={dg:016x} \
         body={} body_hash={:016x} body_same={} ref_hash={:016x} ckpt_cpu={ckpt_cpu:.3} decode_cpu={decode_cpu:.3} ref_cpu={ref_cpu:.3} size_cpu={size_cpu:.3} \
         cut_cpu={cut_cpu:.4} cut_peak={cut_peak}",
        bytes.len(),
        fnv64(&bytes),
        inflated.len(),
        fnv64(&inflated),
        inflated == body,
        fnv64(&reference),
    );
}

/// CPU time of this thread so far (s), from /proc/thread-self/schedstat.
fn thread_cpu() -> f64 {
    let s = std::fs::read_to_string("/proc/thread-self/schedstat").unwrap_or_default();
    s.split_whitespace().next().and_then(|n| n.parse::<u64>().ok()).unwrap_or(0) as f64 / 1e9
}

/// What the state holds, by operator and accumulator kind: how many, and their elements.
fn breakdown(e: &Engine) {
    let json = serde_json::to_value(e.snapshot()).unwrap();
    let mut kinds: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut add = |k: String, elems: u64| {
        let c = kinds.entry(k).or_default();
        c.0 += 1;
        c.1 += elems;
    };
    fn len(v: &serde_json::Value) -> u64 {
        v.as_array().map_or(0, |a| a.len() as u64)
    }
    fn walk(op: &serde_json::Value, add: &mut dyn FnMut(String, u64)) {
        let Some((kind, body)) = op.as_object().and_then(|o| o.iter().next()) else { return };
        match kind.as_str() {
            "Window" => {
                for g in body["open"].as_array().unwrap() {
                    let (keys, accs) = (&g[1][0], &g[1][1]);
                    add("group".into(), len(keys));
                    add("group key text bytes".into(), g[0][1].as_str().unwrap().len() as u64);
                    for a in accs.as_array().unwrap() {
                        let (k, b) = match a.as_object() {
                            Some(o) => o.iter().next().map(|(k, b)| (k.clone(), b.clone())).unwrap(),
                            None => (a.as_str().unwrap_or("?").to_string(), serde_json::Value::Null),
                        };
                        let elems = match k.as_str() {
                            "Quantile" => len(&b["sampler"]["samples"]),
                            "TDigest" => len(&b["digest"]["merged"]) + len(&b["digest"]["unmerged"]),
                            _ => 0,
                        };
                        add(format!("acc {k}"), elems);
                    }
                }
            }
            "Join" | "JoinHeld" => {
                for side in body["sides"].as_array().unwrap() {
                    for op in side.as_array().unwrap() {
                        walk(op, add);
                    }
                }
                for r in body["versions"].as_array().unwrap() {
                    for (_, v) in r.as_array().unwrap().iter().map(|kv| (&kv[0], &kv[1])) {
                        let width = v[0][1].as_array().map_or(0, |r| r.len() as u64);
                        add("join key".into(), len(v));
                        add(format!("join version (row of {width})"), 0);
                    }
                }
                if let Some(h) = body.get("held") {
                    add("join held rows".into(), len(h));
                }
            }
            k => add(k.to_string(), 0),
        }
    }
    for view in json.as_array().unwrap() {
        for op in view.as_array().unwrap() {
            walk(op, &mut add);
        }
    }
    for (k, (n, elems)) in kinds {
        println!("  {k}: {n} ({elems} elements)");
    }
}
