//! Parse and plan time of the historical executor (`Historical::new`) for some example pipelines.
//! Run from the repository's root: `cargo run --release -p brrrrr-core --example plan_time`.
fn main() {
    for f in ["bars", "flow", "quotes", "derivatives"] {
        let sql = std::fs::read_to_string(format!("fixtures/pipelines/{f}.sql")).unwrap();
        let t = std::time::Instant::now();
        let cat = brrrrr_core::sql::parse(&sql).unwrap();
        let parse = t.elapsed();
        let t = std::time::Instant::now();
        for _ in 0..20 {
            std::hint::black_box(brrrrr_core::engine::Historical::new(&cat).unwrap());
        }
        println!("{f}: parse {parse:?}, plan {:?}", t.elapsed() / 20);
    }
}
