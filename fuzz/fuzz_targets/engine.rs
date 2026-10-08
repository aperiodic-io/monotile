//! Fuzzer-chosen rows (extreme and out-of-order times, NaN/inf, odd symbols) through the example
//! pipelines, cut into fuzzer-chosen chunks: nothing panics, and a checkpoint taken after any
//! chunk (postcard round trip, fresh engine) continues with exactly the same messages.
#![no_main]
use arbitrary::Unstructured;
use brrrrr_core::engine::{Emit, Engine, State};
use brrrrr_core::sql::Catalog;
use brrrrr_core::value::Value;
use common::{chunks, pipelines};
use std::cell::RefCell;

mod common;

thread_local! {
    /// Per pipeline: three engines (whole run, before and after the cut) and a pristine
    /// snapshot that resets them; planning the SQL every iteration is too slow.
    static ENGINES: RefCell<Vec<Option<([Engine; 3], State)>>> = RefCell::new(vec![]);
}

fn fresh(pick: usize, cat: &Catalog, f: impl FnOnce(&mut [Engine; 3])) {
    ENGINES.with(|e| {
        let mut e = e.borrow_mut();
        if e.len() <= pick {
            e.resize_with(pick + 1, || None);
        }
        let (engines, pristine) = e[pick].get_or_insert_with(|| {
            let fresh = Engine::new(cat).unwrap();
            let pristine = postcard::from_bytes(&postcard::to_allocvec(&fresh.snapshot()).unwrap()).unwrap();
            ([fresh, Engine::new(cat).unwrap(), Engine::new(cat).unwrap()], pristine)
        });
        for engine in engines.iter_mut() {
            engine.restore(pristine.clone()).unwrap();
        }
        f(engines)
    })
}

fn feed(e: &mut Engine, chunks: &[(String, Vec<Vec<Value>>)], out: &mut Vec<Emit>) {
    for (s, rows) in chunks {
        e.insert(s, rows.clone(), out);
    }
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(pick) = u.choose_index(pipelines().len()) else { return };
    let (cat, sources) = &pipelines()[pick];
    let chunks = chunks(&mut u, cat, sources);
    let cut = if chunks.is_empty() { 0 } else { u.choose_index(chunks.len()).unwrap_or(0) };
    fresh(pick, cat, |[whole_engine, first, second]| {
        let (mut whole, mut out) = (vec![], vec![]);
        feed(whole_engine, &chunks, &mut whole);
        feed(first, &chunks[..cut], &mut out);
        let bytes = postcard::to_allocvec(&first.snapshot()).unwrap();
        second.restore(postcard::from_bytes(&bytes).unwrap()).unwrap();
        feed(second, &chunks[cut..], &mut out);
        assert!(out == whole, "restoring after chunk {cut} changed the output");
    });
});
