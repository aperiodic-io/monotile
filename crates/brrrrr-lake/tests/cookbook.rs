//! Every cookbook recipe (`fixtures/cookbook/cookbook.sql`) over the cookbook's files, held to
//! DuckDB's answer for the same files (`fixtures/cookbook/expected/`): the same rows in the same
//! order, numbers within 1e-9 relative (1e-6 for a t-digest's), times to the microsecond.
use brrrrr_core::query::text;
use brrrrr_lake::Lake;
use std::path::PathBuf;

fn dir() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/cookbook"))
}

/// (name, brrrrr's SQL) of each recipe.
pub fn recipes() -> Vec<(String, String)> {
    let text = std::fs::read_to_string(dir().join("cookbook.sql")).unwrap();
    text.split("-- name: ")
        .skip(1)
        .map(|block| {
            let mut lines = block.lines();
            let name = lines.next().unwrap().trim().to_string();
            let sql: Vec<&str> = lines.filter(|l| !l.starts_with("--")).collect();
            (name, sql.join("\n").trim().trim_end_matches(';').to_string())
        })
        .collect()
}

/// Whether two cells agree: equal text, numbers close enough, or the same time.
fn same(a: &str, b: &str, rel: f64) -> bool {
    if a == b {
        return true;
    }
    if let (Ok(x), Ok(y)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return (x - y).abs() <= rel * x.abs().max(y.abs()).max(1e-12) || (x.is_nan() && y.is_nan());
    }
    let t = |s: &str| brrrrr_core::value::parse_datetime(s.trim_end_matches("+00:00"));
    matches!((t(a), t(b)), (Some(x), Some(y)) if x == y)
}

#[test]
fn every_recipe_gives_duckdb_s_answer() {
    std::env::set_var("BRRRRR_CACHE", std::env::temp_dir().join("brrrrr-cookbook-cache"));
    let d = dir();
    let mut failed = vec![];
    for (name, sql) in recipes() {
        // the recipes name the files as a user in the directory would
        let sql = ["trades.csv", "quotes.csv", "instruments.csv", "readings.csv", "events.csv"]
            .iter()
            .fold(sql, |s, f| s.replace(&format!("'{f}'"), &format!("'{}'", d.join(f).display())));
        let expected = std::fs::read_to_string(d.join(format!("expected/{name}.csv"))).unwrap();
        let mut lines = expected.lines();
        let header: Vec<String> = lines.next().unwrap().split(',').map(str::to_string).collect();
        let want: Vec<Vec<String>> = lines.map(|l| l.split(',').map(str::to_string).collect()).collect();
        for threads in [1, 4] {
            let mut lake = Lake::new();
            lake.threads = threads;
            let got = match lake.execute(&sql) {
                Ok(a) => a,
                Err(e) => {
                    failed.push(format!("{name} ({threads} threads): {e:#}"));
                    continue;
                }
            };
            let rows: Vec<Vec<String>> = got.rows.iter().map(|r| r.iter().map(text).collect()).collect();
            let rel = if sql.contains("quantile") || sql.contains("median") { 1e-6 } else { 1e-9 };
            let ok = got.columns.len() == header.len()
                && rows.len() == want.len()
                && rows
                    .iter()
                    .zip(&want)
                    .all(|(r, w)| r.len() == w.len() && r.iter().zip(w).all(|(a, b)| same(a, b, rel)));
            if !ok {
                let first = rows.iter().zip(&want).position(|(r, w)| !r.iter().zip(w).all(|(a, b)| same(a, b, rel)));
                failed.push(format!(
                    "{name} ({threads} threads): {} rows, DuckDB {}; columns {:?} vs {header:?}; first difference at {first:?}: {:?} vs {:?}",
                    rows.len(),
                    want.len(),
                    got.columns,
                    first.and_then(|i| rows.get(i)),
                    first.and_then(|i| want.get(i)),
                ));
            }
        }
    }
    assert!(failed.is_empty(), "{} recipes differ:\n{}", failed.len(), failed.join("\n"));
}
