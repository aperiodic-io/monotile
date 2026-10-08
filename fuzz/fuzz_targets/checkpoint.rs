//! Whole checkpoints, as the runtime reads them (`Checkpoint::decode`, audit C6): this build's
//! header, then the fuzzer's bytes either as the zstd frame or compressed into one as the
//! postcard body, which reaches every type of the state (a body nested without end overflowed the
//! stack). Decoding may fail and restoring may refuse, but nothing panics or aborts.
#![no_main]
use brrrrr_core::checkpoint::{Checkpoint, LAYOUT, MAGIC, VERSION};
use brrrrr_core::engine::{Engine, State};
use common::pipelines;
use std::cell::RefCell;

mod common;

thread_local! {
    /// Per pipeline: an engine and a pristine snapshot that resets it (planning is slow).
    static ENGINES: RefCell<Vec<Option<(Engine, State)>>> = RefCell::new(vec![]);
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Some((&how, rest)) = data.split_first() else { return };
    let mut bytes = [&MAGIC[..], &VERSION.to_le_bytes(), &LAYOUT.to_le_bytes()].concat();
    if how & 1 == 0 {
        bytes.extend(rest);
    } else {
        bytes.extend(zstd::bulk::compress(rest, 1).unwrap());
    }
    // on a small stack: an input of a few kB nests a few thousand levels deep, which would not
    // overflow the main thread's 8 MiB, but does 256 KiB (unless the depth is bounded)
    let decode = std::thread::Builder::new().stack_size(256 << 10).spawn(move || Checkpoint::decode(&bytes));
    let Ok(c) = decode.unwrap().join().unwrap() else { return };
    let pick = usize::from(how >> 1) % pipelines().len();
    ENGINES.with(|e| {
        let mut e = e.borrow_mut();
        if e.len() <= pick {
            e.resize_with(pick + 1, || None);
        }
        let (engine, pristine) = e[pick].get_or_insert_with(|| {
            let fresh = Engine::new(&pipelines()[pick].0).unwrap();
            let pristine = fresh.snapshot();
            (fresh, pristine)
        });
        engine.restore(pristine.clone()).unwrap();
        // the state alone, as if its plan fingerprint matched
        let _ = engine.restore(c.state);
    })
});
