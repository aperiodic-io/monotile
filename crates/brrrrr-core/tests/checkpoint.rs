//! The checkpoint format (src/checkpoint.rs) cannot change unnoticed: its traced layout is
//! committed, and so are checkpoints of every example pipeline (fixtures/pipelines, among them
//! state.sql, which holds the state the others do not), which this build must write byte for
//! byte and restore into exactly the output of an uninterrupted run. Older versions are read only
//! where this one only appended to them (`READS`, ADR-0008; none yet).
//!
//! After a deliberate format change (see src/checkpoint.rs first),
//! `BLESS=1 cargo test -p brrrrr-core --test all checkpoint::` rewrites the layout and the current
//! version's checkpoints; then run the tests again without BLESS, as some read what others wrote.
use brrrrr_core::agg::{Acc, Moment};
use brrrrr_core::checkpoint::{fnv64, Checkpoint, LAYOUT, READS, VERSION};
use brrrrr_core::engine::{Asof, Engine, OpState};
use brrrrr_core::sql::parse;
use brrrrr_core::value::Value;
use common::{catalog, feed, fixtures, root};
use serde_reflection::{ContainerFormat, Format, Registry, Tracer, TracerConfig, VariantFormat};

use crate::common;

fn bless() -> bool {
    std::env::var_os("BLESS").is_some()
}

/// Everything a checkpoint serializes, as serde-reflection traces it: every struct's fields
/// and every enum's variants, in order.
fn registry() -> Registry {
    let mut t = Tracer::new(TracerConfig::default());
    t.trace_simple_type::<Value>().unwrap();
    t.trace_simple_type::<Moment>().unwrap();
    t.trace_simple_type::<brrrrr_core::agg::Tie>().unwrap();
    t.trace_simple_type::<brrrrr_core::agg::Cont>().unwrap();
    t.trace_simple_type::<Acc>().unwrap();
    t.trace_simple_type::<brrrrr_core::over::FnState>().unwrap();
    t.trace_simple_type::<OpState>().unwrap();
    t.trace_simple_type::<Checkpoint>().unwrap();
    t.registry().unwrap()
}

fn layout() -> String {
    serde_json::to_string_pretty(&registry()).unwrap() + "\n"
}

#[test]
fn the_serialized_layout_is_the_committed_one() {
    let path = root("crates/brrrrr-core/tests/checkpoint-layout.json");
    let now = layout();
    if bless() {
        std::fs::write(&path, &now).unwrap();
    }
    let committed = std::fs::read_to_string(&path).unwrap();
    assert!(
        now == committed,
        "the checkpoint layout changed: bump checkpoint::VERSION, then BLESS=1 and review the diff \
         of tests/checkpoint-layout.json (see src/checkpoint.rs)"
    );
    let hash = fnv64(committed.as_bytes());
    assert_eq!(LAYOUT, hash, "checkpoint::LAYOUT must be {hash:#018x}, the hash of the committed layout");
}

/// An engine for `fixture` with ASOF joins matched as `asof`.
fn engine_for(cat: &brrrrr_core::sql::Catalog, asof: Asof) -> Engine {
    let mut e = Engine::new(cat).unwrap();
    e.set_asof(asof);
    e
}

/// The join modes a pipeline is checkpointed in: both for one with an ASOF join, whose exact
/// mode keeps held rows (`<pipeline>.exact.ckpt`).
fn modes(fixture: &serde_json::Value) -> Vec<(&'static str, Asof)> {
    let joins = catalog(fixture).views.iter().any(|v| !v.asof.is_empty());
    let mut out = vec![("", Asof::Arrival)];
    if joins {
        out.push((".exact", Asof::Exact));
    }
    out
}

/// The checkpoint a pipeline takes halfway through its input.
fn halfway(fixture: &serde_json::Value, asof: Asof) -> (usize, Vec<u8>) {
    let cat = catalog(fixture);
    let chunks = fixture["chunks"].as_array().unwrap();
    let cut = chunks.len() / 2;
    let mut e = engine_for(&cat, asof);
    feed(&cat, &mut e, &chunks[..cut], &mut vec![]);
    let sources = vec![("raw.source".to_string(), 0, cut as i64)];
    let sinks = vec![("metrics.sink".to_string(), 2, 7)];
    let bytes = Checkpoint::encode_of(&e, cut as u64, &sources, &sinks, None).unwrap();
    // the borrowed state serializes as its copy does
    let copy = Checkpoint::of(&e, cut as u64, sources, sinks);
    assert!(bytes == copy.encode(), "{}: encode_of writes other bytes than encode", pipeline(fixture));
    assert!(copy.try_encode().as_ref() == Ok(&bytes), "{}: try_encode writes other bytes", pipeline(fixture));
    (cut, bytes)
}

/// The runtime copies the state at a checkpoint's cut and encodes the copy on another thread,
/// while the data thread feeds the engine on: the copy shares nothing the engine goes on
/// changing. Taken halfway through every pipeline's input, its sinks filled in only once the
/// engine has taken the rest and emitted from it, it is still the checkpoint of the halfway
/// state, byte for byte, and restores into exactly the output of an uninterrupted run.
#[test]
fn a_copy_taken_at_the_cut_is_the_cuts_checkpoint_whatever_the_engine_takes_after_it() {
    for fixture in fixtures() {
        for (_, asof) in modes(&fixture) {
            let name = pipeline(&fixture);
            let (cut, bytes) = halfway(&fixture, asof);
            let cat = catalog(&fixture);
            let chunks = fixture["chunks"].as_array().unwrap();
            let mut e = engine_for(&cat, asof);
            feed(&cat, &mut e, &chunks[..cut], &mut vec![]);
            let mut copy = Checkpoint::of(&e, cut as u64, vec![("raw.source".to_string(), 0, cut as i64)], vec![]);
            let mut after = vec![];
            feed(&cat, &mut e, &chunks[cut..], &mut after);
            assert!(!after.is_empty(), "{name}: nothing after the cut, the test proves little");
            assert!(
                Checkpoint::encode_of(&e, cut as u64, &copy.sources, &[], None).unwrap() != copy.try_encode().unwrap(),
                "{name}: the engine's state did not change after the cut, the test proves little"
            );
            copy.sinks = vec![("metrics.sink".to_string(), 2, 7)];
            assert!(copy.try_encode().unwrap() == bytes, "{name}: the copy changed with the engine");
            let (mut restored, mut got) = (engine_for(&cat, asof), vec![]);
            Checkpoint::decode(&bytes).unwrap().restore(&mut restored).unwrap();
            feed(&cat, &mut restored, &chunks[cut..], &mut got);
            assert!(got == after, "{name}: restored from the copy, the pipeline continues differently");
        }
    }
}

fn pipeline(fixture: &serde_json::Value) -> &str {
    fixture["pipeline"].as_str().unwrap()
}

#[test]
fn this_build_writes_the_committed_checkpoints() {
    let dir = root(&format!("fixtures/checkpoints/v{VERSION}"));
    std::fs::create_dir_all(&dir).unwrap();
    for fixture in fixtures() {
        for (suffix, asof) in modes(&fixture) {
            let path = format!("{dir}/{}{suffix}.ckpt", pipeline(&fixture));
            let (_, bytes) = halfway(&fixture, asof);
            if bless() {
                std::fs::write(&path, &bytes).unwrap();
            }
            let committed = std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
            assert!(
                bytes == committed,
                "{path}: this build writes other bytes. A format change needs a new VERSION (see src/checkpoint.rs); \
                 a change in what a pipeline keeps needs a review of why"
            );
        }
    }
}

/// Every committed checkpoint, of every version: (path, pipeline, bytes).
fn committed() -> Vec<(String, String, Vec<u8>)> {
    let mut out = vec![];
    for version in std::fs::read_dir(root("fixtures/checkpoints")).unwrap() {
        for f in std::fs::read_dir(version.unwrap().path()).unwrap() {
            let path = f.unwrap().path();
            if path.extension().is_none_or(|e| e != "ckpt") {
                continue;
            }
            let name = path.file_stem().unwrap().to_string_lossy().into_owned();
            out.push((path.display().to_string(), name, std::fs::read(&path).unwrap()));
        }
    }
    out.sort();
    out
}

#[test]
fn every_committed_checkpoint_restores_and_continues_exactly() {
    let fixtures = fixtures();
    let committed = committed();
    assert!(committed.len() >= fixtures.len(), "{} committed checkpoints", committed.len());
    for (path, name, bytes) in committed {
        let (name, asof) = match name.strip_suffix(".exact") {
            Some(name) => (name.to_string(), Asof::Exact),
            None => (name, Asof::Arrival),
        };
        let fixture = fixtures.iter().find(|f| pipeline(f) == name).unwrap_or_else(|| panic!("{path}: no fixture"));
        let cat = catalog(fixture);
        let chunks = fixture["chunks"].as_array().unwrap();
        let c = Checkpoint::decode(&bytes).unwrap_or_else(|e| panic!("{path}: {e}"));
        let cut = c.epoch as usize;
        assert_eq!((c.sources.len(), c.sinks.len()), (1, 1), "{path}");
        // the reference: the same input without the interruption
        let (mut whole, mut want) = (engine_for(&cat, asof), vec![]);
        feed(&cat, &mut whole, &chunks[..cut], &mut vec![]);
        feed(&cat, &mut whole, &chunks[cut..], &mut want);
        let (mut restored, mut got) = (engine_for(&cat, asof), vec![]);
        c.restore(&mut restored).unwrap_or_else(|e| panic!("{path}: {e}"));
        feed(&cat, &mut restored, &chunks[cut..], &mut got);
        assert!(!want.is_empty(), "{path}: nothing after the cut, the checkpoint proves little");
        assert!(got == want, "{path}: the restored pipeline continues differently");
    }
}

type Variants = std::collections::BTreeSet<(String, String)>;

/// Every (enum, variant) that `j`, a value of `format` as serde_json writes it, holds: the
/// traced layout says which type each part of the JSON is, so `Value::Int` and `Tie::Int` are
/// told apart.
fn variants(reg: &Registry, format: &Format, j: &serde_json::Value, out: &mut Variants) {
    let each = |fs: &[Format], j: &serde_json::Value, out: &mut Variants| {
        let a = j.as_array().unwrap_or_else(|| panic!("a tuple, got {j}"));
        assert_eq!(a.len(), fs.len(), "{j}");
        fs.iter().zip(a).for_each(|(f, j)| variants(reg, f, j, out));
    };
    let named = |fs: &[serde_reflection::Named<Format>], j: &serde_json::Value, out: &mut Variants| {
        fs.iter().for_each(|f| variants(reg, &f.value, &j[&f.name], out));
    };
    match format {
        Format::TypeName(name) => match &reg[name] {
            ContainerFormat::UnitStruct => {}
            ContainerFormat::NewTypeStruct(f) => variants(reg, f, j, out),
            ContainerFormat::TupleStruct(fs) => each(fs, j, out),
            ContainerFormat::Struct(fs) => named(fs, j, out),
            ContainerFormat::Enum(vs) => {
                // externally tagged: "Unit", or {"Variant": its content}
                let (tag, content) = match j {
                    serde_json::Value::String(tag) => (tag, &serde_json::Value::Null),
                    serde_json::Value::Object(m) if m.len() == 1 => m.iter().next().unwrap(),
                    _ => panic!("{name}: not a variant: {j}"),
                };
                let v = vs.values().find(|v| v.name == *tag).unwrap_or_else(|| panic!("{name}::{tag}"));
                out.insert((name.clone(), tag.clone()));
                match &v.value {
                    VariantFormat::Unit => {}
                    VariantFormat::NewType(f) => variants(reg, f, content, out),
                    VariantFormat::Tuple(fs) => each(fs, content, out),
                    VariantFormat::Struct(fs) => named(fs, content, out),
                    VariantFormat::Variable(_) => panic!("{name}::{tag} was not traced"),
                }
            }
        },
        Format::Option(f) if !j.is_null() => variants(reg, f, j, out),
        Format::Seq(f) | Format::TupleArray { content: f, .. } => {
            let a = j.as_array().unwrap_or_else(|| panic!("a sequence, got {j}"));
            a.iter().for_each(|j| variants(reg, f, j, out));
        }
        Format::Tuple(fs) => each(fs, j, out),
        Format::Map { .. } | Format::Variable(_) => panic!("no checkpoint type holds {format:?}"),
        _ => {} // a number, text, bool, unit or `None`
    }
}

/// The committed checkpoints are only as good as what they hold: a same-shape change to what a
/// variant means (µs for ms, two fields of one type swapped, another seed) passes the layout
/// guard and is caught only by a committed checkpoint that holds that variant. So this
/// version's committed checkpoints hold every variant of every enum in the traced layout, the
/// ones a variant added later included: a new operator, accumulator, window function state,
/// moment or value kind needs a pipeline that keeps it, in an example pipeline or in
/// fixtures/pipelines/state.sql, and its checkpoint blessed.
#[test]
fn the_committed_checkpoints_hold_every_kind_of_state() {
    let reg = registry();
    let checkpoint = Format::TypeName("Checkpoint".into());
    let mut seen = Variants::new();
    for (path, _, bytes) in committed().into_iter().filter(|c| c.0.contains(&format!("/v{VERSION}/"))) {
        let c = Checkpoint::decode(&bytes).unwrap_or_else(|e| panic!("{path}: {e}"));
        variants(&reg, &checkpoint, &serde_json::to_value(&c).unwrap(), &mut seen);
    }
    let mut want = Variants::new();
    for (name, container) in &reg {
        if let ContainerFormat::Enum(vs) = container {
            want.extend(vs.values().map(|v| (name.clone(), v.name.clone())));
        }
    }
    // a check on the derivation: it requires the variants only some pipelines keep
    let rare =
        ["OpState::Over", "Acc::If", "FnState::Frame", "Moment::VarSamp", "Tie::None", "Value::Array", "Cont::Digest"];
    for v in rare {
        let (e, v) = v.split_once("::").unwrap();
        assert!(want.contains(&(e.into(), v.into())), "{e}::{v} is not in the traced layout");
    }
    let missing: Vec<_> = want.difference(&seen).map(|(e, v)| format!("{e}::{v}")).collect();
    assert!(missing.is_empty(), "no committed version-{VERSION} checkpoint holds {missing:?}");
}

const SQL: &str = "
CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, price float64, qty float64);
CREATE STREAM IF NOT EXISTS out (symbol string, a float64, b float64);
CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO out AS
SELECT symbol, sum(price) AS a, sum(qty) AS b
FROM tumble(trades, local_event_time, 1m)
GROUP BY window_start, symbol
EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;
";

fn engine(sql: &str) -> Engine {
    Engine::new(&parse(sql).unwrap()).unwrap()
}

fn trade(sec: i64, price: f64, qty: f64) -> Vec<Value> {
    vec![Value::Time(sec * 1_000_000), Value::Str("A".into()), Value::F64(price), Value::F64(qty)]
}

fn sample() -> Checkpoint {
    let mut e = engine(SQL);
    e.insert("trades", vec![trade(1, 10.0, 1.0), trade(2, 20.0, 2.0)], &mut vec![]);
    Checkpoint::of(&e, 3, vec![("t".into(), 0, 5)], vec![("s".into(), 1, 9)])
}

#[test]
fn a_checkpoint_round_trips_through_its_bytes() {
    let bytes = sample().encode();
    assert_eq!(&bytes[..4], b"BRRR");
    assert_eq!(bytes[4..6], VERSION.to_le_bytes());
    assert_eq!(bytes[6..14], LAYOUT.to_le_bytes());
    let c = Checkpoint::decode(&bytes).unwrap();
    assert_eq!((c.epoch, c.sources.clone(), c.sinks.clone()), (3, vec![("t".into(), 0, 5)], vec![("s".into(), 1, 9)]));
    assert_eq!(c.encode(), bytes);
}

/// What a start without a checkpoint withholds is in every checkpoint after it (kept beside them,
/// it would be deleted and created again, and a crash between the two would lose it), streamed
/// from the live state as from a copy.
#[test]
fn a_checkpoint_carries_what_its_run_withholds() {
    let mut w = brrrrr_core::checkpoint::Withhold::none();
    assert!(w.is_none());
    w.start = 60_000_000;
    w.topics.insert("m.1m".into(), 120_000_000);
    w.awaiting.push("raw.trades.b".into());
    assert!(!w.is_none());
    let mut c = sample();
    c.withhold = Some(w.clone());
    let bytes = c.encode();
    assert_eq!(Checkpoint::decode(&bytes).unwrap().withhold, Some(w.clone()));
    let e = engine(SQL);
    let streamed = Checkpoint::encode_of(&e, c.epoch, &c.sources, &c.sinks, Some(&w)).unwrap();
    let copied = Checkpoint { withhold: Some(w.clone()), ..Checkpoint::of(&e, c.epoch, c.sources.clone(), c.sinks) };
    assert_eq!(streamed, copied.encode());
    assert_eq!(
        w.to_string(),
        "the windows that began before 60 (unix s, partial); on m.1m, the windows closed before 120 (unix s, which \
         it no longer holds); the windows that began before the first record of raw.trades.b"
    );
    assert_eq!(Checkpoint::decode(&sample().encode()).unwrap().withhold, None);
}

#[test]
fn bytes_without_the_header_are_not_a_checkpoint() {
    let mut other = sample().encode();
    other[0] = b'b';
    for bytes in [&b""[..], b"BRRR", &sample().encode()[..13], &other] {
        assert_eq!(Checkpoint::decode(bytes).unwrap_err(), "not a brrrrr checkpoint");
    }
}

#[test]
fn another_version_is_refused_before_anything_is_decoded() {
    let mut bytes = sample().encode();
    bytes[4..6].copy_from_slice(&(VERSION + 1).to_le_bytes());
    bytes.truncate(14); // nothing after the header: the version alone decides
    let err = Checkpoint::decode(&bytes).unwrap_err();
    assert_eq!(err, format!("checkpoint format version {}; this build reads versions {VERSION}", VERSION + 1));
}

/// A version before the oldest one read is refused: its bytes would decode into this build's
/// types as something else, or not at all.
#[test]
fn versions_before_the_oldest_one_read_are_refused() {
    let mut bytes = sample().encode();
    for v in 0u16..VERSION {
        bytes[4..6].copy_from_slice(&v.to_le_bytes());
        let err = Checkpoint::decode(&bytes).unwrap_err();
        assert_eq!(err, format!("checkpoint format version {v}; this build reads versions {VERSION}"));
    }
}

/// `stale_version` (`run.rs::restore` prunes what it reports, ADR-0008): a version this build
/// does not read and never will (older than every version it reads) is the only one it reports.
/// A version in `READS` is not: pruning it would throw away every pipeline's state on the deploy
/// that version is kept readable for. Its own version and a newer one (a downgrade) are `None`,
/// same as anything that is not a checkpoint at all: only a human decides those.
#[test]
fn stale_version_is_only_one_this_build_does_not_read() {
    let bytes = sample().encode();
    let of_version = |v: u16| {
        let mut b = bytes.clone();
        b[4..6].copy_from_slice(&v.to_le_bytes());
        b
    };
    let oldest = READS.iter().map(|r| r.0).fold(VERSION, u16::min);
    for v in 0..oldest {
        assert_eq!(Checkpoint::stale_version(&of_version(v)), Some(v), "version {v}");
    }
    for v in READS.map(|r| r.0).into_iter().chain([VERSION, VERSION + 1, VERSION + 5]) {
        assert_eq!(Checkpoint::stale_version(&of_version(v)), None, "version {v}");
    }
}

// ---- older versions this one reads (`READS`) ----------------------------------------------------
//
// None yet. A version that only appends to the one before it keeps that version's committed
// checkpoints (`fixtures/checkpoints/v<version>/`) and traced layout
// (`tests/checkpoint-layout-v<version>.json`), and lists it in `READS`: these tests then prove
// the appends and restore its checkpoints.

/// An older version's layout as its build traced it: `tests/checkpoint-layout.json` of the build
/// that wrote `fixtures/checkpoints/v<version>/`, committed unchanged.
fn layout_of(version: u16) -> String {
    std::fs::read_to_string(root(&format!("crates/brrrrr-core/tests/checkpoint-layout-v{version}.json"))).unwrap()
}

/// `READS` names each older version by the hash of that committed layout, the one its
/// checkpoints carry.
#[test]
fn older_versions_are_read_under_the_layouts_their_builds_traced() {
    for (version, layout) in READS {
        assert_eq!(layout, fnv64(layout_of(version).as_bytes()), "version {version}");
        let of_version: Vec<_> = committed().into_iter().filter(|c| c.0.contains(&format!("/v{version}/"))).collect();
        assert!(of_version.len() >= fixtures().len(), "{} committed version-{version} checkpoints", of_version.len());
        for (path, _, bytes) in of_version {
            assert_eq!(bytes[4..6], version.to_le_bytes(), "{path}");
            assert_eq!(bytes[6..14], layout.to_le_bytes(), "{path}");
        }
    }
}

/// Why an older version's bytes decode into this build's types unchanged: postcard writes no
/// names, so the bytes only decode the same if every type that version serializes is serialized
/// the same way now. A version that only adds keeps every type the older version had the same,
/// except that an enum may gain variants after its last one (the older bytes never hold them),
/// and there may be new types only the new variants reach. A field or variant changed, removed,
/// reordered or inserted before others fails this: bump `VERSION` and drop the older versions
/// from `READS`.
#[test]
fn this_version_only_appends_to_the_layouts_it_reads() {
    let now: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&layout()).unwrap();
    for (version, _) in READS {
        let old: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&layout_of(version)).unwrap();
        for (name, was) in &old {
            let now = now.get(name).unwrap_or_else(|| panic!("{name}: in version {version}, gone from this one"));
            if now == was {
                continue;
            }
            let (Some(was), Some(now)) = (was["ENUM"].as_object(), now["ENUM"].as_object()) else {
                panic!("{name} changed from version {version}:\n{was}\n{now}");
            };
            for (index, variant) in was {
                assert_eq!(now.get(index), Some(variant), "{name}: variant {index} changed from version {version}");
            }
            let added = now.keys().filter(|i| i.parse::<usize>().unwrap() >= was.len()).count();
            assert_eq!(added, now.len() - was.len(), "{name}: a variant inserted before version {version}'s last");
        }
    }
}

/// An older version's header is read only with that version's own layout: bytes of any other
/// layout under it (a build that changed the format without bumping it, this version's, or
/// the other older version's) are refused before anything is decoded.
#[test]
fn an_older_version_is_read_only_with_its_own_layout() {
    for (version, layout) in READS {
        let old = std::fs::read(root(&format!("fixtures/checkpoints/v{version}/flow.ckpt"))).unwrap();
        assert!(Checkpoint::decode(&old).is_ok());
        let others = READS.iter().filter(|r| r.0 != version).map(|r| r.1);
        for other in others.chain([LAYOUT, layout ^ 1, 0]) {
            let mut b = old.clone();
            b[6..14].copy_from_slice(&other.to_le_bytes());
            let err = Checkpoint::decode(&b).unwrap_err();
            assert!(err.contains("changed the format without bumping"), "version {version}, {other:016x}: {err}");
        }
    }
}

/// Every committed checkpoint of an older version, restored by this build and checkpointed
/// again, is written as this version, and that checkpoint restores and continues exactly as the
/// older one does: the first checkpoint after the deploy moves the pipeline to this version.
#[test]
fn a_restored_older_checkpoint_is_written_back_as_this_version() {
    let fixtures = fixtures();
    let mut seen = std::collections::BTreeSet::new();
    for (path, name, bytes) in committed().into_iter().filter(|c| !c.0.contains(&format!("/v{VERSION}/"))) {
        let (name, asof) = match name.strip_suffix(".exact") {
            Some(name) => (name.to_string(), Asof::Exact),
            None => (name, Asof::Arrival),
        };
        let fixture = fixtures.iter().find(|f| pipeline(f) == name).unwrap_or_else(|| panic!("{path}: no fixture"));
        let cat = catalog(fixture);
        let chunks = fixture["chunks"].as_array().unwrap();
        let c = Checkpoint::decode(&bytes).unwrap_or_else(|e| panic!("{path}: {e}"));
        let (epoch, sources, sinks) = (c.epoch, c.sources.clone(), c.sinks.clone());
        let mut restored = engine_for(&cat, asof);
        c.restore(&mut restored).unwrap_or_else(|e| panic!("{path}: {e}"));
        let again = Checkpoint::encode_of(&restored, epoch, &sources, &sinks, None).unwrap();
        assert_ne!(bytes[4..6], VERSION.to_le_bytes(), "{path}");
        assert_eq!(again[4..6], VERSION.to_le_bytes(), "{path}");
        assert_eq!(again[6..14], LAYOUT.to_le_bytes(), "{path}");
        let (mut from_old, mut want) = (engine_for(&cat, asof), vec![]);
        Checkpoint::decode(&bytes).unwrap().restore(&mut from_old).unwrap();
        feed(&cat, &mut from_old, &chunks[epoch as usize..], &mut want);
        let now = Checkpoint::decode(&again).unwrap_or_else(|e| panic!("{path} written back: {e}"));
        assert_eq!((now.epoch, now.sources.clone(), now.sinks.clone()), (epoch, sources, sinks), "{path}");
        let (mut from_now, mut got) = (engine_for(&cat, asof), vec![]);
        now.restore(&mut from_now).unwrap();
        feed(&cat, &mut from_now, &chunks[epoch as usize..], &mut got);
        assert!(!want.is_empty() && got == want, "{path}: written back as this version, it continues differently");
        seen.insert(u16::from_le_bytes([bytes[4], bytes[5]]));
    }
    let read: std::collections::BTreeSet<_> = READS.iter().map(|r| r.0).collect();
    assert_eq!(seen, read, "the older versions' committed checkpoints");
}

/// Bytes that are not a whole checkpoint are never reported as a stale version, however old a
/// version happens to sit at their header's offset: the header is not there to read at all.
#[test]
fn stale_version_of_bytes_that_are_not_a_checkpoint_is_none() {
    let old = sample().encode();
    let mut wrong_magic = old.clone();
    wrong_magic[0] = b'b'; // full length, an old version at the header's offset, wrong magic
    wrong_magic[4..6].copy_from_slice(&1u16.to_le_bytes());
    let mut too_short = old.clone();
    too_short[4..6].copy_from_slice(&1u16.to_le_bytes());
    too_short.truncate(13); // magic and an old version present, but one byte short of the header
    for bytes in [&b""[..], b"BRRR", &wrong_magic, &too_short] {
        assert_eq!(Checkpoint::stale_version(bytes), None, "{bytes:?}");
    }
}

/// A busy feed's checkpoints reach hundreds of MB (slippage's reservoirs), written every
/// interval: the state is compressed with zstd. What pipelines keep compresses well: prices on a tick
/// grid, slippage that is mostly zero, repeated keys.
#[test]
fn the_state_is_compressed_with_zstd() {
    let mut e = engine(
        "CREATE STREAM IF NOT EXISTS trades (local_event_time datetime64(6), symbol string, price float64, qty float64);
         CREATE STREAM IF NOT EXISTS out (symbol string, a float64);
         CREATE MATERIALIZED VIEW IF NOT EXISTS w INTO out AS
         SELECT symbol, quantile(0.5)(price) AS a FROM tumble(trades, local_event_time, 1h)
         GROUP BY window_start, symbol EMIT AFTER WINDOW CLOSE WITH DELAY INTERVAL '50' MILLISECOND;",
    );
    let rows = (0..20_000).map(|i| trade(i / 10, [0.0, 0.0, 0.0, 0.25, -0.5][(i % 5) as usize], 1.0)).collect();
    e.insert("trades", rows, &mut vec![]);
    let c = Checkpoint::of(&e, 1, vec![], vec![]);
    let bytes = c.encode();
    assert_eq!(bytes[14..18], [0x28, 0xb5, 0x2f, 0xfd], "a zstd frame after the header");
    let raw = postcard::to_allocvec(&c).unwrap().len();
    assert!(raw > 8 * 8_192, "a full reservoir");
    assert!(bytes.len() * 10 < raw, "{} bytes, {raw} uncompressed", bytes.len());
    assert_eq!(Checkpoint::decode(&bytes).unwrap().encode(), bytes);
}

#[test]
fn another_layout_under_the_same_version_is_refused() {
    let mut bytes = sample().encode();
    bytes[13] ^= 0x80;
    let err = Checkpoint::decode(&bytes).unwrap_err();
    assert!(err.contains("changed the format without bumping its version"), "{err}");
}

#[test]
fn bytes_after_the_checkpoint_are_refused() {
    let mut bytes = sample().encode();
    bytes.extend([0, 0]);
    assert_eq!(Checkpoint::decode(&bytes).unwrap_err(), "2 bytes after the checkpoint");
}

/// The body is decoded as it is inflated: bytes after the checkpoint inside the zstd frame are
/// refused as those after the frame are.
#[test]
fn bytes_after_the_checkpoint_in_its_frame_are_refused() {
    let bytes = sample().encode();
    let mut body = zstd::stream::decode_all(&bytes[14..]).unwrap();
    body.extend([0, 0, 0]);
    let mut other = bytes[..14].to_vec();
    other.extend(zstd::bulk::compress(&body, 3).unwrap());
    assert_eq!(Checkpoint::decode(&other).unwrap_err(), "3 bytes after the checkpoint");
    body.truncate(body.len() - 4);
    let mut short = bytes[..14].to_vec();
    short.extend(zstd::bulk::compress(&body, 3).unwrap());
    assert!(Checkpoint::decode(&short).is_err(), "a frame that ends inside the checkpoint");
}

#[test]
fn a_truncated_checkpoint_is_refused() {
    let bytes = sample().encode();
    assert!(Checkpoint::decode(&bytes[..bytes.len() - 1]).is_err());
}

/// The same aggregates in another order have the same shape, so restore's own checks accept
/// them; only the plan fingerprint tells the sums apart.
#[test]
fn state_of_the_same_sql_planned_differently_is_refused() {
    let swapped = SQL.replace("sum(price) AS a, sum(qty) AS b", "sum(qty) AS b, sum(price) AS a");
    let mut e = engine(&swapped);
    let err = sample().restore(&mut e).unwrap_err();
    assert!(err.contains("a planner change"), "{err}");
    // without the fingerprint, the state restores, into the wrong sums
    assert!(e.restore(sample().state).is_ok());
}

#[test]
fn the_plan_fingerprint_is_the_same_in_every_build() {
    // FNV-1a's published test vectors
    assert_eq!(fnv64(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv64(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv64(b"foobar"), 0x8594_4171_f739_67e8);
    // what a window's fingerprint covers: its width, delay, keys and aggregates
    let fp = |sql: &str| engine(sql).fingerprint();
    let base = fp(SQL);
    for other in [
        SQL.replace("1m)", "2m)"),
        SQL.replace("'50'", "'60'"),
        SQL.replace("GROUP BY window_start, symbol", "GROUP BY window_start, symbol, price"),
        SQL.replace("sum(qty)", "avg(qty)"),
    ] {
        assert_ne!(fp(&other), base, "{other}");
    }
    // and not the names of its outputs
    assert_eq!(fp(&SQL.replace("AS a", "AS x")), base);
}

/// A checkpoint of this version around `body`, the postcard encoding of a `Checkpoint`.
fn envelope(body: &[u8]) -> Vec<u8> {
    let mut bytes = sample().encode()[..14].to_vec();
    // and no `withhold` (None)
    bytes.extend(zstd::bulk::compress(&[body, &[0]].concat(), 3).unwrap());
    bytes
}

/// A checkpoint of one view whose one window holds one group whose one accumulator is `acc`.
fn one_accumulator(acc: &[u8]) -> Vec<u8> {
    // epoch, plan, no sources, no sinks; one view of one op: a Window (tag 1) at 0, 0, 0 late,
    // with one group, (0, ""), of no keys and one accumulator
    envelope(&[&[0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1], acc].concat())
}

/// Postcard has no depth limit: a checkpoint whose body is the `Acc::If` tag repeated (a few
/// hundred bytes compressed) overflowed the stack in `decode`, which no fallback
/// survives. It nests as deep as a plan can, and no deeper, counted with arrays and joins.
#[test]
fn a_checkpoint_nested_past_the_bound_is_refused_not_a_stack_overflow() {
    use brrrrr_core::checkpoint::MAX_NESTING;
    let refused = format!("a state nested more than {MAX_NESTING} deep");
    let nest = |what: &str, n: usize| match what {
        // Acc::If (tag 17) around Count { n: 0, rows: false } (tag 6)
        "ifs" => one_accumulator(&[vec![17; n], vec![6, 0, 0]].concat()),
        // Acc::Latest (tag 0) of Value::Array (tag 8) of one ... around Value::Null (tag 0)
        "arrays" => one_accumulator(&[vec![0], [8, 1].repeat(n), vec![0]].concat()),
        // OpState::Join (tag 2) whose one side is one Join ... around Stateless, none with versions
        _ => envelope(&[vec![0, 0, 0, 0, 1, 1], [2, 1, 1].repeat(n), vec![0], vec![0; n]].concat()),
    };
    for what in ["ifs", "arrays", "joins"] {
        Checkpoint::decode(&nest(what, MAX_NESTING)).unwrap_or_else(|e| panic!("{what}: {e}"));
        for n in [MAX_NESTING + 1, 100_000] {
            assert_eq!(Checkpoint::decode(&nest(what, n)).unwrap_err(), refused, "{what} {n}");
        }
    }
    // the levels count together, and each is left again: a second value nests as deep
    let half = MAX_NESTING / 2;
    let together = |extra: usize| [vec![17; half], vec![0], [8, 1].repeat(MAX_NESTING - half + extra), vec![0]];
    assert!(Checkpoint::decode(&one_accumulator(&together(0).concat())).is_ok());
    assert_eq!(Checkpoint::decode(&one_accumulator(&together(1).concat())).unwrap_err(), refused);
    let deep = [[8, 1].repeat(MAX_NESTING - 1), vec![0]].concat();
    assert!(Checkpoint::decode(&one_accumulator(&[&[0, 8, 2], &deep[..], &deep[..]].concat())).is_ok());
}

/// A plan nests `_if` at most `MAX_IF` times, well inside what a checkpoint may hold.
#[test]
fn an_aggregate_takes_at_most_max_if_combinators() {
    use brrrrr_core::agg::MAX_IF;
    use brrrrr_core::checkpoint::MAX_NESTING;
    const { assert!(MAX_IF < MAX_NESTING / 4) };
    let name = |n: usize| format!("count{}", "_if".repeat(n));
    assert!(Acc::new(&name(MAX_IF), &[], MAX_IF).is_ok());
    let err = Acc::new(&name(MAX_IF + 1), &[], MAX_IF + 1).unwrap_err();
    assert_eq!(err, format!("{}: more than {MAX_IF} _if combinators", name(MAX_IF + 1)));
}
