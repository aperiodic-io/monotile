//! Shared by the tests that run the example pipelines (fixtures/pipelines) on their committed
//! synthetic inputs (fixtures/pipeline-inputs, scripts/record-pipeline-inputs.py): each holds
//! rows for one pipeline's sources, as chunks.
use brrrrr_core::engine::{Emit, Engine};
use brrrrr_core::sql::{parse, Catalog};
use brrrrr_core::value::{Type, Value};

pub fn root(p: &str) -> String {
    format!("{}/../../{p}", env!("CARGO_MANIFEST_DIR"))
}

pub fn value(v: &serde_json::Value, t: &Type) -> Value {
    match t.base() {
        Type::F64 => Value::F64(v.as_f64().unwrap()),
        Type::Int(_) => Value::Int(v.as_i64().unwrap()),
        Type::Str => Value::Str(v.as_str().unwrap().into()),
        Type::Bool => Value::Bool(v.as_bool().unwrap()),
        Type::Array(t) => Value::Array(v.as_array().unwrap().iter().map(|x| value(x, t)).collect()),
        t => panic!("source column type {t:?}"),
    }
}

/// The pipeline's SQL.
fn sql(fixture: &serde_json::Value) -> String {
    let pipeline = fixture["pipeline"].as_str().unwrap();
    std::fs::read_to_string(root(&format!("fixtures/pipelines/{pipeline}.sql"))).unwrap()
}

/// The pipeline's SQL, parsed.
pub fn catalog(fixture: &serde_json::Value) -> Catalog {
    parse(&sql(fixture)).unwrap()
}

pub fn feed(cat: &Catalog, engine: &mut Engine, chunks: &[serde_json::Value], out: &mut Vec<Emit>) {
    for chunk in chunks {
        let stream = &cat.streams[chunk["stream"].as_str().unwrap()];
        let rows = chunk["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r.as_array().unwrap().iter().zip(&stream.columns).map(|(v, c)| value(v, &c.ty)).collect());
        engine.insert(&stream.name, rows.collect(), out);
    }
}

/// Every example pipeline with its input. The state one (fixtures/pipelines/state.sql) is fed
/// trades with the symbols interleaved in time order, so that its checkpoints halfway hold every
/// symbol's window open and many rows into it.
pub fn fixtures() -> Vec<serde_json::Value> {
    let mut files: Vec<_> =
        std::fs::read_dir(root("fixtures/pipeline-inputs")).unwrap().map(|e| e.unwrap().path()).collect();
    files.sort();
    assert!(files.len() > 5, "no pipeline inputs");
    files
        .iter()
        .map(|f| serde_json::from_reader(flate2::read::GzDecoder::new(std::fs::File::open(f).unwrap())).unwrap())
        .collect()
}

/// Runs `body` in a process of its own, for a test that reads a process-wide counter (a runtime
/// metric such as `SEQUENCE_OUT_OF_ORDER`) which the tests running beside it under `cargo test`
/// move too. `name` is the test's path in this binary (`sequence::a_test`). nextest runs every
/// test in a process of its own already: there `body` runs as it is.
pub fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var_os("BRRRRR_TEST_ISOLATED").is_some() || std::env::var_os("NEXTEST_RUN_ID").is_some() {
        return body();
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([name, "--exact", "--test-threads=1", "--nocapture"])
        .env("BRRRRR_TEST_ISOLATED", "1")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success() && text.contains("1 passed"), "{name}, in a process of its own:\n{text}");
}
