//! Any text as an ad-hoc query (`query::compile`, ADR-0018) over small tables of every column
//! type, and a query that compiles run on their rows: an error or an answer, never a panic, a
//! hang or a blow-up of memory.
#![no_main]
use brrrrr_core::column::Batch;
use brrrrr_core::engine::{Row, Serial, Source};
use brrrrr_core::query::{compile, execute, Input, Table};
use brrrrr_core::value::{Type, Value};

struct Rows(Vec<Batch>);

impl Source for Rows {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        (!self.0.is_empty()).then(|| Ok(self.0.remove(0)))
    }
}

/// A table's columns and rows: `trades` and `quotes` as the cookbook's, `t` of every type, with
/// NULLs, NaN, infinities and the extremes; any other name or path is `t`.
fn table(t: &Table) -> (Vec<(String, Type)>, Vec<Row>) {
    let name = match t {
        Table::Named(n) => n.as_str(),
        Table::Path { path, .. } => path.as_str(),
    };
    let s = |x: &str| Value::Str(x.into());
    let ts = |sec: i64| Value::Time(1_704_067_200_000_000 + sec * 1_000_000);
    let cols = |c: &[(&str, Type)]| c.iter().map(|(n, t)| (n.to_string(), t.clone())).collect::<Vec<_>>();
    match name.trim_end_matches(".csv") {
        "trades" => (
            cols(&[("ts", Type::Time(6)), ("symbol", Type::Str), ("price", Type::F64), ("size", Type::F64)]),
            vec![
                vec![ts(10), s("A"), Value::F64(100.0), Value::F64(1.0)],
                vec![ts(20), s("B"), Value::F64(50.0), Value::F64(2.0)],
                vec![ts(30), s("A"), Value::F64(101.0), Value::F64(2.0)],
                vec![ts(65), s("A"), Value::F64(99.0), Value::Null],
                vec![ts(70), s("B"), Value::F64(f64::NAN), Value::F64(1.0)],
            ],
        ),
        "quotes" => (
            cols(&[("ts", Type::Time(6)), ("symbol", Type::Str), ("bid", Type::F64), ("ask", Type::F64)]),
            vec![
                vec![ts(5), s("A"), Value::F64(99.5), Value::F64(100.5)],
                vec![ts(15), s("B"), Value::F64(49.5), Value::F64(50.5)],
                vec![ts(60), s("A"), Value::F64(98.5), Value::F64(99.5)],
            ],
        ),
        _ => (
            cols(&[
                ("ts", Type::Time(6)),
                ("s", Type::Str),
                ("f", Type::F64),
                ("i", Type::Int(64)),
                ("u", Type::UInt(64)),
                ("b", Type::Bool),
            ]),
            vec![
                vec![ts(0), s("a"), Value::F64(1.5), Value::Int(1), Value::UInt(1), Value::Bool(true)],
                vec![ts(1), s(""), Value::F64(f64::INFINITY), Value::Int(i64::MAX), Value::UInt(u64::MAX), Value::Null],
                vec![ts(1), Value::Null, Value::F64(-0.0), Value::Int(i64::MIN), Value::UInt(0), Value::Bool(false)],
                vec![Value::Null, s("é"), Value::Null, Value::Null, Value::Null, Value::Bool(true)],
                vec![ts(-3600), s("a"), Value::F64(f64::NAN), Value::Int(-1), Value::UInt(7), Value::Bool(false)],
            ],
        ),
    }
}

libfuzzer_sys::fuzz_target!(|sql: &str| {
    let Ok(c) = compile(sql, &mut |t| Ok(table(t).0)) else { return };
    let mut open = |src: &brrrrr_core::query::Source, _: &[bool], parts: Option<(usize, usize)>| {
        let (cols, rows) = table(&src.table);
        let s: Input = Box::new(Rows(rows.chunks(2).map(|r| Batch::from_rows(r, cols.len())).collect()));
        Ok(match parts {
            Some((key, n)) => brrrrr_core::query::split(s, key, n),
            None => vec![s],
        })
    };
    let _ = execute(&c, &mut open, &Serial);
});
