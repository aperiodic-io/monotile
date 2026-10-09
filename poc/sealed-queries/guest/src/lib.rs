//! A query as a module. `QUERY_FILE` (at build time) is baked in; the host hands the module its
//! tables as text and gets the result back as CSV. The ABI is three functions, the same in wasm32
//! and in a native shared library: `alloc(n)` for the input, `run(ptr, len)` gives the output's
//! pointer, `out_len()` its length.
//!
//! The input, per table: `#name col:type,col:type,...` then its CSV rows (no header), types
//! `time`, `str`, `f64`, `i64`.
use brrrrr_core::column::Batch;
use brrrrr_core::engine::{Serial, Source};
use brrrrr_core::query::{compile, execute, split, text, Input, Table};
use brrrrr_core::value::{parse_datetime, Type, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

const QUERY: &str = include_str!(env!("QUERY_FILE"));

static OUT_LEN: AtomicUsize = AtomicUsize::new(0);

#[no_mangle]
pub extern "C" fn alloc(n: usize) -> *mut u8 {
    Box::into_raw(vec![0u8; n].into_boxed_slice()) as *mut u8
}

/// # Safety
/// `ptr` is `len` bytes from `alloc`.
#[no_mangle]
pub unsafe extern "C" fn run(ptr: *mut u8, len: usize) -> *mut u8 {
    let input = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len));
    #[allow(unused_mut)]
    let mut out = query(&String::from_utf8_lossy(&input)).unwrap_or_else(|e| format!("error: {e}\n"));
    #[cfg(feature = "exfil")]
    out.push_str(&std::fs::read_to_string("/etc/hostname").unwrap_or_else(|e| format!("exfil blocked: {e}\n")));
    let out = out.into_bytes().into_boxed_slice();
    OUT_LEN.store(out.len(), Ordering::Relaxed);
    Box::into_raw(out) as *mut u8
}

#[no_mangle]
pub extern "C" fn out_len() -> usize {
    OUT_LEN.load(Ordering::Relaxed)
}

struct Rows(Vec<Batch>);

impl Source for Rows {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        (!self.0.is_empty()).then(|| Ok(self.0.remove(0)))
    }
}

type Tables = Vec<(String, Vec<(String, Type)>, Vec<Vec<Value>>)>;

// ponytail: CSV split on commas, no quoting; the host's real format (Arrow IPC) when it matters
fn tables(input: &str) -> Result<Tables, String> {
    let mut out: Tables = vec![];
    for line in input.lines().filter(|l| !l.is_empty()) {
        if let Some(head) = line.strip_prefix('#') {
            let (name, cols) = head.split_once(' ').ok_or("#name col:type,...")?;
            let cols = cols.split(',').map(|c| {
                let (n, t) = c.split_once(':').ok_or(format!("{c}: col:type"))?;
                let t = match t {
                    "time" => Type::Time(6),
                    "str" => Type::Str,
                    "f64" => Type::F64,
                    "i64" => Type::Int(64),
                    t => return Err(format!("{t}: time, str, f64 or i64")),
                };
                Ok((n.to_string(), t))
            });
            out.push((name.to_string(), cols.collect::<Result<_, String>>()?, vec![]));
            continue;
        }
        let (_, cols, rows) = out.last_mut().ok_or("rows before a #table line")?;
        let row = line.split(',').zip(cols.iter()).map(|(v, (_, t))| match (v, t) {
            ("", _) => Ok(Value::Null),
            (v, Type::Time(_)) => parse_datetime(v).map(Value::Time).ok_or(format!("{v}: not a time")),
            (v, Type::F64) => v.parse().map(Value::F64).map_err(|_| format!("{v}: not a number")),
            (v, Type::Int(_)) => v.parse().map(Value::Int).map_err(|_| format!("{v}: not an integer")),
            (v, _) => Ok(Value::Str(v.into())),
        });
        rows.push(row.collect::<Result<_, String>>()?);
    }
    Ok(out)
}

fn query(input: &str) -> Result<String, String> {
    let tables = tables(input)?;
    let find = |t: &Table| match t {
        Table::Named(n) => tables.iter().find(|x| x.0 == *n).ok_or(format!("no table {n}")),
        t => Err(format!("{t}: a module reads only the tables it is given")),
    };
    let c = compile(QUERY, &mut |t| find(t).map(|x| x.1.clone()))?;
    let mut open = |s: &brrrrr_core::query::Source, _: &[bool], parts: Option<(usize, usize)>| {
        let (_, cols, rows) = find(&s.table)?;
        let src: Input = Box::new(Rows(rows.chunks(4096).map(|r| Batch::from_rows(r, cols.len())).collect()));
        Ok(match parts {
            Some((key, n)) => split(src, key, n),
            None => vec![src],
        })
    };
    let mut out = c.columns.join(",") + "\n";
    for row in execute(&c, &mut open, &Serial)? {
        out += &row.iter().map(text).collect::<Vec<_>>().join(",");
        out.push('\n');
    }
    Ok(out)
}
