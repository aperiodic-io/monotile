//! Any text: the parser and the planner return errors, never panic.
#![no_main]
libfuzzer_sys::fuzz_target!(|sql: &str| {
    if let Ok(cat) = brrrrr_core::sql::parse(sql) {
        let _ = brrrrr_core::engine::Engine::new(&cat);
    }
});
