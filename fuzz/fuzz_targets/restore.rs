//! A checkpoint corrupted at fuzzer-chosen bytes: decoding may fail and restoring may refuse it,
//! but nothing panics, and an engine that accepted it keeps running without panicking.
#![no_main]
use arbitrary::Unstructured;
use brrrrr_core::engine::{Engine, State};
use common::{chunks, pipelines};
use std::cell::RefCell;

mod common;

thread_local! {
    /// Per pipeline: two engines and a pristine snapshot that resets them (planning is slow).
    static ENGINES: RefCell<Vec<Option<([Engine; 2], State)>>> = RefCell::new(vec![]);
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(pick) = u.choose_index(pipelines().len()) else { return };
    let (cat, sources) = &pipelines()[pick];
    let Ok(flips) = u.int_in_range(1..=4usize) else { return };
    let flips: Vec<(u16, u8)> = (0..flips).filter_map(|_| Some((u.arbitrary().ok()?, u.arbitrary().ok()?))).collect();
    let chunks = chunks(&mut u, cat, sources);
    let cut = chunks.len() / 2;
    ENGINES.with(|e| {
        let mut e = e.borrow_mut();
        if e.len() <= pick {
            e.resize_with(pick + 1, || None);
        }
        let ([first, second], pristine) = e[pick].get_or_insert_with(|| {
            let fresh = Engine::new(cat).unwrap();
            let pristine = fresh.snapshot();
            ([fresh, Engine::new(cat).unwrap()], pristine)
        });
        first.restore(pristine.clone()).unwrap();
        second.restore(pristine.clone()).unwrap();
        let mut out = vec![];
        for (s, rows) in &chunks[..cut] {
            first.insert(s, rows.clone(), &mut out);
        }
        let mut bytes = postcard::to_allocvec(&first.snapshot()).unwrap();
        // corrupt the end, where the open groups and join versions are, more often than the rest
        let n = bytes.len();
        for (at, x) in flips {
            let at = if at % 2 == 0 { n - 1 - (at as usize / 2) % n.min(4096) } else { at as usize % n };
            bytes[at] ^= x.max(1);
        }
        let Ok(state) = postcard::from_bytes::<State>(&bytes) else { return };
        if second.restore(state).is_err() {
            return;
        }
        for (s, rows) in &chunks[cut..] {
            second.insert(s, rows.clone(), &mut out);
        }
        let _ = postcard::to_allocvec(&second.snapshot()).unwrap();
    })
});
