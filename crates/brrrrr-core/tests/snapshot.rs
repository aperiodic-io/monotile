//! In-memory snapshots of every example pipeline (fixtures/pipeline-inputs): what
//! `Engine::snapshot` takes restores into output identical to an uninterrupted run, and only
//! into the SQL it was taken of.
use brrrrr_core::engine::Engine;
use brrrrr_core::sql::parse;
use common::{catalog, feed, fixtures};

use crate::common;

/// A checkpoint taken after a chunk (about six cut points per pipeline), encoded as the runtime stores it (postcard) and restored
/// into a fresh engine, must continue exactly as if nothing had happened: same messages, same
/// order. Every stateful operator of every example pipeline is on this path.
#[test]
fn a_checkpoint_after_any_chunk_restores_to_identical_output() {
    for fixture in fixtures() {
        let cat = catalog(&fixture);
        let chunks = fixture["chunks"].as_array().unwrap();
        let mut whole = vec![];
        feed(&cat, &mut Engine::new(&cat).unwrap(), chunks, &mut whole);
        assert!(!whole.is_empty(), "{}: the input produces nothing, the test proves nothing", fixture["pipeline"]);
        for cut in (1..chunks.len()).step_by(chunks.len() / 6 + 1) {
            let (mut first, mut out) = (Engine::new(&cat).unwrap(), vec![]);
            feed(&cat, &mut first, &chunks[..cut], &mut out);
            let bytes = postcard::to_allocvec(&first.snapshot()).unwrap();
            let mut second = Engine::new(&cat).unwrap();
            second.restore(postcard::from_bytes(&bytes).unwrap()).unwrap();
            feed(&cat, &mut second, &chunks[cut..], &mut out);
            assert!(out == whole, "{}: restoring after chunk {cut} changed the output", fixture["pipeline"]);
        }
    }
}

#[test]
fn a_snapshot_from_other_sql_is_refused() {
    let fixtures = fixtures();
    let cat = catalog(&fixtures[0]);
    let other = parse("CREATE STREAM IF NOT EXISTS s (x int64);").unwrap();
    let mut e = Engine::new(&other).unwrap();
    let err = e.restore(Engine::new(&cat).unwrap().snapshot()).unwrap_err();
    assert!(err.contains("views"), "{err}");
}
