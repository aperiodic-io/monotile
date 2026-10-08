//! Shared by the engine fuzz targets: the example pipelines and fuzzer-chosen source rows.
use arbitrary::{Arbitrary, Unstructured};
use brrrrr_core::sql::{parse, Catalog, Kind};
use brrrrr_core::value::{Type, Value};
use std::sync::OnceLock;

/// Per pipeline: its catalog and the source streams rows can be inserted into.
pub fn pipelines() -> &'static Vec<(Catalog, Vec<String>)> {
    static P: OnceLock<Vec<(Catalog, Vec<String>)>> = OnceLock::new();
    P.get_or_init(|| {
        [
            include_str!("../../../fixtures/pipelines/bars.sql"),
            include_str!("../../../fixtures/pipelines/book.sql"),
            include_str!("../../../fixtures/pipelines/derivatives.sql"),
            include_str!("../../../fixtures/pipelines/flow.sql"),
            include_str!("../../../fixtures/pipelines/quotes.sql"),
            include_str!("../../../fixtures/pipelines/returns.sql"),
            include_str!("../../../fixtures/pipelines/stats.sql"),
            include_str!("../../../fixtures/pipelines/state.sql"),
        ]
        .iter()
        .map(|sql| {
            let cat = parse(sql).unwrap();
            // the streams no view writes into
            let mut sources: Vec<String> = (cat.streams.values())
                .filter(|s| !matches!(s.kind, Kind::Table(_)) && !cat.views.iter().any(|v| v.target == s.name))
                .map(|s| s.name.clone())
                .collect();
            sources.sort();
            (cat, sources)
        })
        .collect()
    })
}

pub const BASE: i64 = 1_788_220_800_000_000;

pub fn value(u: &mut Unstructured, name: &str, t: &Type) -> arbitrary::Result<Value> {
    Ok(match t.base() {
        Type::Int(_) if name.contains("time") || name.contains("timestamp") => {
            Value::Int(match u.int_in_range(0..=15)? {
                0 => i64::MIN,
                1 => i64::MAX,
                2 => i64::arbitrary(u)?,
                // mostly near the base, seconds to hours apart, in any order
                _ => BASE + i64::from(u.int_in_range(-7_200_000..=7_200_000i32)?) * 1_000,
            })
        }
        Type::Int(_) => Value::Int(i64::from(u.int_in_range(0..=3u8)?)),
        Type::F64 => Value::F64(match u.int_in_range(0..=9)? {
            0 => f64::NAN,
            1 => f64::INFINITY,
            2 => f64::from_bits(u64::arbitrary(u)?),
            3 => 0.0,
            _ => f64::from(u.int_in_range(1..=100_000u32)?) / 100.0,
        }),
        Type::Bool => Value::Bool(bool::arbitrary(u)?),
        Type::Array(t) => {
            let n = u.int_in_range(0..=4usize)?;
            Value::Array((0..n).map(|_| value(u, name, t)).collect::<arbitrary::Result<_>>()?)
        }
        Type::Str if name == "side" => Value::Str(["buy", "sell", "", "BUY"][u.choose_index(4)?].into()),
        Type::Str => Value::Str(
            ["BTC-USDT", "ETH-USDT", "spot-SOL-USDC", "perpetual-X-DAI:DAI", "", "ÄÖ"][u.choose_index(6)?].into(),
        ),
        t => panic!("source column type {t:?}"),
    })
}

/// Up to 40 fuzzer-chosen chunks of 1-8 rows, each into one of the pipeline's sources.
pub fn chunks(u: &mut Unstructured, cat: &Catalog, sources: &[String]) -> Vec<(String, Vec<Vec<Value>>)> {
    let mut chunks = vec![];
    while chunks.len() < 40 && !u.is_empty() {
        let Ok(s) = u.choose(sources) else { break };
        let cols = &cat.streams[s].columns;
        let n = u.int_in_range(1..=8usize).unwrap_or(1);
        let rows: arbitrary::Result<Vec<Vec<Value>>> =
            (0..n).map(|_| cols.iter().map(|c| value(u, &c.name, &c.ty)).collect()).collect();
        let Ok(rows) = rows else { break };
        chunks.push((s.clone(), rows));
    }
    chunks
}
