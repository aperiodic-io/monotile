//! Replays captured source topics through one pipeline offline, as `brrrrr run` consumes them,
//! and reports what it cost.
//!
//! Each `<topic>.p<partition>.gz` in `<capture>` (lines `partition offset timestamp_ms
//! base64(value)`, as `rpk topic consume -f '%p %o %d %v{base64}\n'` prints them) is one
//! partition. Partitions are merged by record timestamp, as `--max-drift` merges them, and
//! decoded with the pipeline's protobuf codecs in runs of at most 1024 messages of one
//! partition; ASOF joins are exact, as `brrrrr run` makes them.
//!
//! Writes every emitted row to `<out.gz>` (`topic<TAB>payload` lines) and to `<stats.json>`: the
//! CPU time, messages per CPU second, peak RSS, and every 5 minutes of input the RSS and the
//! size and encoding time of a checkpoint.
//!
//! usage: replay <pipeline.sql> <schema.proto> <capture dir> <out.gz> <stats.json>
//! (for example fixtures/pipelines/bars.sql and fixtures/market.proto)
use base64::Engine as _;
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::{Asof, Engine};
use brrrrr_core::proto::{parse_proto, Codec};
use brrrrr_core::sql::{parse, Kind};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::io::{BufRead, BufReader, Write};
use std::time::Instant;

type Reader = BufReader<flate2::read::GzDecoder<std::fs::File>>;

/// A field of /proc/self/status, in kB.
fn status(key: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines().find(|l| l.starts_with(key)).and_then(|l| l.split_whitespace().nth(1)?.parse().ok()).unwrap_or(0)
}

/// This process's user and system CPU time, in seconds.
fn cpu_s() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = s.rsplit(')').next().unwrap_or("").split_whitespace().collect();
    (f[11].parse::<f64>().unwrap_or(0.0) + f[12].parse::<f64>().unwrap_or(0.0)) / 100.0
}

/// The next message of a partition: its record timestamp (ms) and value.
fn next(r: &mut Reader, buf: &mut String) -> Option<(i64, Vec<u8>)> {
    buf.clear();
    if r.read_line(buf).ok()? == 0 {
        return None;
    }
    let mut it = buf.trim_end().splitn(4, ' ');
    let (_partition, _offset, ts, v) = (it.next()?, it.next()?, it.next()?, it.next().unwrap_or(""));
    Some((ts.parse().ok()?, base64::engine::general_purpose::STANDARD.decode(v).ok()?))
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let [_, sql, proto, capture, out_path, stats_path] = &a[..] else {
        panic!("usage: replay <pipeline.sql> <schema.proto> <capture dir> <out.gz> <stats.json>")
    };
    let cat = parse(&std::fs::read_to_string(sql).unwrap()).unwrap();
    let mut engine = Engine::new(&cat).unwrap();
    engine.set_asof(Asof::Exact);
    let protos = parse_proto(&std::fs::read_to_string(proto).unwrap()).unwrap();
    // topic -> its source streams, with their decoders (as `brrrrr run` builds them)
    let mut sources: BTreeMap<String, Vec<(String, Codec)>> = BTreeMap::new();
    for s in cat.streams.values().filter(|s| s.kind == Kind::External) {
        if s.settings.get("data_format").map(String::as_str) != Some("ProtobufSingle") {
            continue;
        }
        let msg = s.settings["format_schema"].split_once(':').unwrap().1;
        let cols: Vec<_> = s.columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect();
        sources.entry(s.settings["topic"].clone()).or_default().push((s.name.clone(), Codec::new(&protos[msg], &cols)));
    }
    let (mut topics, mut readers) = (vec![], vec![]);
    for t in sources.keys() {
        let parts: Vec<_> =
            (0..64).map(|p| format!("{capture}/{t}.p{p}.gz")).filter(|f| std::path::Path::new(f).exists()).collect();
        assert!(!parts.is_empty(), "no capture of {t} in {capture}");
        for f in parts {
            topics.push(t.clone());
            readers
                .push(BufReader::with_capacity(1 << 20, flate2::read::GzDecoder::new(std::fs::File::open(f).unwrap())));
        }
    }
    let mut bufs = vec![String::new(); readers.len()];
    let mut heads: Vec<_> = (0..readers.len()).map(|i| next(&mut readers[i], &mut bufs[i])).collect();
    let mut heap: BinaryHeap<Reverse<(i64, usize)>> =
        heads.iter().enumerate().filter_map(|(i, h)| h.as_ref().map(|h| Reverse((h.0, i)))).collect();
    let mut out_f =
        flate2::write::GzEncoder::new(std::fs::File::create(out_path).unwrap(), flate2::Compression::fast());
    let (mut out, mut emitted, mut msgs, mut decode_errors) = (vec![], 0u64, 0u64, 0u64);
    let (start, cpu0) = (Instant::now(), cpu_s());
    let (mut timeline, mut first, mut mark) = (vec![], None, 0);
    let mut run: Vec<Vec<u8>> = vec![];
    while let Some(Reverse((ts, i))) = heap.pop() {
        let first = *first.get_or_insert(ts);
        if mark == 0 {
            mark = first + 300_000;
        }
        // a run of one partition while it stays the earliest
        run.clear();
        let mut last;
        loop {
            let (t, v) = heads[i].take().expect("a head");
            last = t;
            run.push(v);
            heads[i] = next(&mut readers[i], &mut bufs[i]);
            let more = heads[i]
                .as_ref()
                .is_some_and(|n| run.len() < 1024 && heap.peek().is_none_or(|Reverse((t, _))| n.0 <= *t));
            if !more {
                break;
            }
        }
        if let Some(n) = &heads[i] {
            heap.push(Reverse((n.0, i)));
        }
        msgs += run.len() as u64;
        for (stream, codec) in &sources[&topics[i]] {
            let rows: Vec<_> =
                run.iter().filter_map(|b| codec.decode(b).map_err(|_| decode_errors += 1).ok()).collect();
            engine.insert(stream, rows, &mut out);
        }
        for e in out.drain(..) {
            emitted += 1;
            writeln!(out_f, "{}\t{}", e.topic, e.payload).unwrap();
        }
        if last >= mark {
            let t = Instant::now();
            let bytes = Checkpoint::of(&engine, 1, vec![], vec![]).encode().len();
            timeline.push(serde_json::json!({"input_min": (last - first) / 60_000, "msgs": msgs,
                "rss_mib": status("VmRSS:") / 1024, "checkpoint_mib": bytes as f64 / 1048576.0,
                "checkpoint_encode_s": t.elapsed().as_secs_f64()}));
            eprintln!("{}", timeline.last().unwrap());
            mark += 300_000;
        }
    }
    let cpu = cpu_s() - cpu0;
    let final_checkpoint = Checkpoint::of(&engine, 1, vec![], vec![]).encode().len();
    engine.close_until(i64::MAX / 4, &mut out);
    for e in out.drain(..) {
        emitted += 1;
        writeln!(out_f, "{}\t{}", e.topic, e.payload).unwrap();
    }
    out_f.finish().unwrap();
    let stats = serde_json::json!({"sql": sql, "msgs": msgs, "decode_errors": decode_errors, "emitted": emitted,
        "cpu_s": cpu, "wall_s": start.elapsed().as_secs_f64(), "msgs_per_cpu_s": msgs as f64 / cpu,
        "peak_rss_mib": status("VmHWM:") / 1024, "final_checkpoint_mib": final_checkpoint as f64 / 1048576.0,
        "timeline": timeline});
    std::fs::write(stats_path, serde_json::to_string_pretty(&stats).unwrap()).unwrap();
    println!("{stats}");
}
