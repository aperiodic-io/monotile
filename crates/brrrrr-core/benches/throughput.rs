//! Wall-clock throughput of the engine on every example pipeline (fixtures/pipelines).
//!
//! Each pipeline input's source rows (fixtures/pipeline-inputs: synthetic symbols, exchanges
//! and prices) are cycled into a long stream: time columns are rewritten to a steady clock
//! (`STEP_US` per row: 100k rows span about 67 minutes, so 1m..1h windows close), everything
//! else is kept. Chunks of `CHUNK` rows go to one source at a time, round robin, as the
//! runtime's per-partition batches do. Per pipeline it reports:
//!
//! - rows/s: input rows over the fastest of `REPS` runs, each on a fresh engine;
//! - alloc/row, bytes/row: heap allocations (count, bytes) per input row during a run;
//! - state: live heap after the run with the output drained, i.e. what the engine retains;
//! - digest: an order-insensitive hash of every emitted (topic, payload, headers). A refactor
//!   must leave it unchanged.
//!
//! Run: `cargo bench -p brrrrr-core --bench throughput [-- <pipeline substring>]`.
//! `BRRRRR_BENCH_ROWS` overrides the input size per pipeline. `BRRRRR_BENCH_SQL_DIR` reads each
//! pipeline's `<name>.sql` from that directory instead of `fixtures/pipelines` (another version of
//! the same pipelines). `BRRRRR_BENCH_JSON` writes the results there as JSON. A pipeline whose SQL
//! does not load is reported as an error, not a panic.
use brrrrr_core::engine::{Emit, Engine};
use brrrrr_core::sql::{parse, Catalog};
use brrrrr_core::value::{Type, Value};
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
use std::alloc::System;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const BASE_US: i64 = 1_788_220_800_000_000;
const STEP_US: i64 = 40_000;
const CHUNK: usize = 256;
const REPS: usize = 5;

fn root(p: &str) -> String {
    format!("{}/../../{p}", env!("CARGO_MANIFEST_DIR"))
}

fn value(v: &serde_json::Value, t: &Type) -> Value {
    match t.base() {
        Type::F64 => Value::F64(v.as_f64().unwrap()),
        Type::Int(_) => Value::Int(v.as_i64().unwrap()),
        Type::Str => Value::Str(v.as_str().unwrap().into()),
        Type::Bool => Value::Bool(v.as_bool().unwrap()),
        Type::Array(t) => Value::Array(v.as_array().unwrap().iter().map(|x| value(x, t)).collect()),
        t => panic!("source column type {t:?}"),
    }
}

struct Workload {
    name: String,
    cat: Catalog,
    /// (stream, rows) in insertion order.
    chunks: Vec<(String, Vec<Vec<Value>>)>,
    rows: usize,
}

/// The pipeline name a fixture is for, and its SQL: from `BRRRRR_BENCH_SQL_DIR` if set, else
/// `fixtures/pipelines`.
fn pipeline_sql(name: &str) -> std::result::Result<Catalog, String> {
    let dir = std::env::var("BRRRRR_BENCH_SQL_DIR").unwrap_or(root("fixtures/pipelines"));
    let path = &format!("{dir}/{name}.sql");
    let cat = parse(&std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?)
        .map_err(|e| format!("{path}: {e:?}"))?;
    Engine::new(&cat).map_err(|e| format!("{path}: {e:?}"))?;
    Ok(cat)
}

fn workload(fixture: &std::path::Path, total: usize) -> (String, std::result::Result<Workload, String>) {
    let f: serde_json::Value =
        serde_json::from_reader(flate2::read::GzDecoder::new(std::fs::File::open(fixture).unwrap())).unwrap();
    let name = f["pipeline"].as_str().unwrap().to_string();
    match pipeline_sql(&name) {
        Ok(cat) => {
            let w = workload_rows(&f, name.clone(), cat, total);
            (name, Ok(w))
        }
        Err(e) => (name, Err(e)),
    }
}

fn workload_rows(f: &serde_json::Value, name: String, cat: Catalog, total: usize) -> Workload {
    // every input row, per stream
    let mut seen: BTreeMap<String, Vec<Vec<Value>>> = BTreeMap::new();
    for c in f["chunks"].as_array().unwrap() {
        let s = &cat.streams[c["stream"].as_str().unwrap()];
        for r in c["rows"].as_array().unwrap() {
            let row = r.as_array().unwrap().iter().zip(&s.columns).map(|(v, c)| value(v, &c.ty)).collect();
            seen.entry(s.name.clone()).or_default().push(row);
        }
    }
    let times: BTreeMap<&String, Vec<usize>> = seen
        .keys()
        .map(|s| {
            let cols = &cat.streams[s].columns;
            (s, cols.iter().enumerate().filter(|(_, c)| c.name.contains("time")).map(|(i, _)| i).collect())
        })
        .collect();
    let streams: Vec<&String> = seen.keys().collect();
    let (mut chunks, mut i, mut next) = (vec![], 0usize, BTreeMap::<&String, usize>::new());
    while i < total {
        let s = streams[chunks.len() % streams.len()];
        let src = &seen[s];
        let at = next.entry(s).or_default();
        let rows: Vec<Vec<Value>> = (0..CHUNK.min(total - i))
            .map(|k| {
                let mut r = src[(*at + k) % src.len()].clone();
                for &t in &times[s] {
                    r[t] = Value::Int(BASE_US + (i + k) as i64 * STEP_US);
                }
                r
            })
            .collect();
        *at += rows.len();
        i += rows.len();
        chunks.push((s.clone(), rows));
    }
    Workload { name, cat, chunks, rows: total }
}

fn digest(out: &[Emit]) -> u64 {
    // order-insensitive: the sum of per-message hashes
    out.iter().fold(0u64, |acc, m| {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        (&m.topic, &m.payload, &m.headers).hash(&mut h);
        acc.wrapping_add(h.finish())
    })
}

struct Result {
    secs: f64,
    allocs: usize,
    bytes: usize,
    state: isize,
    emitted: usize,
    digest: u64,
}

fn run(w: &Workload) -> Result {
    let mut engine = Engine::new(&w.cat).unwrap();
    // the input's own heap bytes, all handed to the engine and freed by it during the run
    let input = Region::new(GLOBAL);
    let chunks = w.chunks.clone();
    let input = input.change();
    let input = input.bytes_allocated as isize + input.bytes_reallocated;
    let (mut out, mut emitted, mut dg) = (Vec::with_capacity(1 << 16), 0, 0u64);
    let region = Region::new(GLOBAL);
    let t = Instant::now();
    for (s, rows) in chunks {
        engine.insert(&s, rows, &mut out);
        emitted += out.len();
        dg = dg.wrapping_add(digest(&out));
        out.clear();
    }
    let secs = t.elapsed().as_secs_f64();
    let st = region.change();
    let state = st.bytes_allocated as isize - st.bytes_deallocated as isize + st.bytes_reallocated + input;
    std::hint::black_box(&engine);
    Result { secs, allocs: st.allocations + st.reallocations, bytes: st.bytes_allocated, state, emitted, digest: dg }
}

fn main() {
    let filter: Option<String> = std::env::args().skip(1).find(|a| !a.starts_with('-'));
    let total: usize = std::env::var("BRRRRR_BENCH_ROWS").ok().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let mut files: Vec<_> =
        std::fs::read_dir(root("fixtures/pipeline-inputs")).unwrap().map(|e| e.unwrap().path()).collect();
    files.sort();
    println!(
        "{:<26} {:>8} {:>10} {:>9} {:>10} {:>10} {:>8}  digest",
        "pipeline", "rows", "rows/s", "alloc/row", "bytes/row", "state KiB", "emitted"
    );
    let (mut all_rows, mut all_secs) = (0usize, 0f64);
    let mut json = serde_json::Map::new();
    for f in files {
        if filter.as_deref().is_some_and(|x| !f.to_string_lossy().contains(x)) {
            continue;
        }
        let w = match workload(&f, total) {
            (_, Ok(w)) => w,
            (name, Err(e)) => {
                println!("{:<26} ERROR {e}", name);
                json.insert(name, serde_json::json!({ "error": e }));
                continue;
            }
        };
        let runs: Vec<Result> = (0..REPS).map(|_| run(&w)).collect();
        assert!(runs.windows(2).all(|p| p[0].digest == p[1].digest), "{}: nondeterministic output", w.name);
        let best = runs.iter().min_by(|a, b| a.secs.total_cmp(&b.secs)).unwrap();
        all_rows += w.rows;
        all_secs += best.secs;
        json.insert(
            w.name.clone(),
            serde_json::json!({
                "rows": w.rows,
                "rows_per_s": w.rows as f64 / best.secs,
                "alloc_per_row": best.allocs as f64 / w.rows as f64,
                "bytes_per_row": best.bytes as f64 / w.rows as f64,
                "state_bytes": best.state,
                "emitted": best.emitted,
                "digest": format!("{:016x}", best.digest),
            }),
        );
        println!(
            "{:<26} {:>8} {:>10.0} {:>9.1} {:>10.0} {:>10.0} {:>8}  {:016x}",
            w.name,
            w.rows,
            w.rows as f64 / best.secs,
            best.allocs as f64 / w.rows as f64,
            best.bytes as f64 / w.rows as f64,
            best.state as f64 / 1024.0,
            best.emitted,
            best.digest
        );
    }
    println!("{:<26} {:>8} {:>10.0}", "total", all_rows, all_rows as f64 / all_secs);
    if let Ok(path) = std::env::var("BRRRRR_BENCH_JSON") {
        let doc = serde_json::json!({ "step_us": STEP_US, "chunk": CHUNK, "reps": REPS, "pipelines": json });
        std::fs::write(path, serde_json::to_string_pretty(&doc).unwrap() + "\n").unwrap();
    }
}
