//! `skewness`, `kurtosis` and `kurtosis_pop`: DuckDB's definitions, held to the exact values
//! of fixtures/duckdb-vectors (scripts/record-duckdb-vectors.py), which also records what DuckDB
//! 1.5.5 itself returns. DuckDB sums raw powers of x and loses every digit on price-like data
//! (kurtosis 2037.8 where the exact value is 0.0048); brrrrr's shifted power sums must stay within
//! `TOL` of the exact value everywhere, and so agree with DuckDB wherever DuckDB is accurate.
use brrrrr_core::agg::Acc;
use brrrrr_core::value::Value;
use std::io::BufRead;

/// Relative error allowed (absolute below 1): a few thousand ULPs, for sums over hundreds of rows.
const TOL: f64 = 1e-10;

fn f64_of(hex: &str) -> f64 {
    f64::from_bits(u64::from_str_radix(hex, 16).unwrap())
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= TOL * b.abs().max(1.0)
}

#[test]
fn moments_have_duckdbs_definitions_and_the_exact_values() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/duckdb-vectors/moments.jsonl.gz");
    let file = flate2::read::GzDecoder::new(std::fs::File::open(path).unwrap());
    let (mut cases, mut duckdb_accurate, mut worst, mut wrong) = (0, 0, 0f64, vec![]);
    for line in std::io::BufReader::new(file).lines() {
        let case: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let xs: Vec<f64> = case["xs"].as_array().unwrap().iter().map(|x| f64_of(x.as_str().unwrap())).collect();
        for f in ["skewness", "kurtosis", "kurtosis_pop"] {
            let mut acc = Acc::new(f, &[], 1).unwrap();
            xs.iter().for_each(|x| acc.add(Value::F64(*x)));
            let got = acc.result();
            let duckdb = case[f].as_str().unwrap();
            let ok = match case["exact"][f].as_str().unwrap() {
                // where DuckDB raises (an infinite input, or moments that overflow), brrrrr has NULL
                _ if duckdb == "error" => got == Value::Null,
                "-" => got == Value::Null,
                // no variance (a constant window) or too few rows; DuckDB's rounding can leave a
                // variance on a constant window, and a number where there is none
                "null" => got == Value::Null,
                "nan" => matches!(got, Value::F64(v) if v.is_nan()),
                exact => {
                    let exact = f64_of(exact);
                    let Value::F64(v) = got else { panic!("{f}: {got:?} where the exact value is {exact}") };
                    worst = worst.max((v - exact).abs() / exact.abs().max(1.0));
                    // where DuckDB is accurate, brrrrr agrees with it too
                    if let Some(d) =
                        u64::from_str_radix(duckdb, 16).ok().map(f64::from_bits).filter(|d| close(*d, exact))
                    {
                        duckdb_accurate += 1;
                        assert!(close(v, d), "{f}: brrrrr {v}, DuckDB {d}");
                    }
                    close(v, exact)
                }
            };
            if !ok {
                wrong.push(format!(
                    "{f}({} values): exact {}, DuckDB {duckdb}, brrrrr {got:?}",
                    xs.len(),
                    case["exact"][f]
                ));
            }
            cases += 1;
        }
    }
    println!("{cases} cases, worst relative error {worst:e}, DuckDB accurate in {duckdb_accurate}");
    assert!(cases > 1000 && duckdb_accurate > 500, "{cases} cases, {duckdb_accurate} where DuckDB is accurate");
    assert!(wrong.is_empty(), "{} of {cases} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
