//! What every POC's runner shares: running a query that only reads the tables it is given, and
//! age keys and envelopes.
use anyhow::{anyhow, Result};
use brrrrr_core::query::{Table, ONE_ROW};
use brrrrr_lake::{write, Lake};

/// Runs `sql` over `tables` (name, location) and gives its result as CSV. A query nobody on this
/// side may read cannot be reviewed either, so what it reads is checked before any of it runs: a
/// path, a URL or a name that is not one of `tables` is refused (`FROM 'https://host/?'...` is a
/// read that sends data out; `'/other/tenant/*.parquet'` reads what it may not).
pub fn run(sql: &str, tables: &[(String, String)]) -> Result<Vec<u8>> {
    let mut lake = Lake::new();
    for (n, p) in tables {
        lake.register(n, p);
    }
    let mut columns = |t: &Table| match t {
        Table::Named(n) if n == ONE_ROW => Ok(vec![]),
        Table::Named(n) if tables.iter().any(|t| t.0 == *n) => {
            let c = lake.compile(&format!("SELECT * FROM {n}")).map_err(|e| e.to_string())?;
            Ok(c.sources[0].columns.clone())
        }
        t => Err(format!("{t}: not a table this query may read")),
    };
    brrrrr_core::query::compile(sql, &mut columns).map_err(|e| anyhow!(e))?;
    let a = lake.query(sql)?;
    let mut out = vec![];
    write::write(&mut out, write::Out::Csv, &a.columns, &a.rows, 0)?;
    Ok(out)
}

/// `name=location` arguments.
pub fn tables(args: &[String]) -> Result<Vec<(String, String)>> {
    let t = args.iter().map(|a| a.split_once('=').map(|(n, p)| (n.into(), p.into())));
    t.collect::<Option<_>>().ok_or(anyhow!("tables are name=location"))
}

/// The identity of an `age-keygen` file.
pub fn identity(path: &str) -> Result<age::x25519::Identity> {
    let text = std::fs::read_to_string(path)?;
    let key = text.lines().find(|l| l.starts_with("AGE-SECRET-KEY-")).ok_or(anyhow!("{path}: no age key"))?;
    key.parse().map_err(|e: &str| anyhow!("{path}: {e}"))
}

pub fn recipient(s: &str) -> Result<age::x25519::Recipient> {
    s.parse().map_err(|e: &str| anyhow!("{s}: {e}"))
}

pub fn seal(to: &age::x25519::Recipient, plain: &[u8]) -> Result<Vec<u8>> {
    Ok(age::encrypt(to, plain)?)
}

pub fn open(with: &age::x25519::Identity, sealed: &[u8]) -> Result<Vec<u8>> {
    Ok(age::decrypt(with, sealed)?)
}

/// The arguments after the program's name.
pub fn args() -> Vec<String> {
    std::env::args().skip(1).collect()
}

/// The value after `--name` in `args`.
pub fn flag(args: &[String], name: &str) -> Result<String> {
    let i = args.iter().position(|a| a == name).ok_or(anyhow!("missing {name}"))?;
    args.get(i + 1).cloned().ok_or(anyhow!("{name} needs a value"))
}

/// A query module's input (guest/src/lib.rs): each `name=path=schema` table's CSV, its header
/// line replaced by `#name schema`.
pub fn module_input(tables: &[String]) -> Result<Vec<u8>> {
    let mut out = String::new();
    for t in tables {
        let [name, path, schema] = t.splitn(3, '=').collect::<Vec<_>>()[..] else {
            return Err(anyhow!("{t}: name=path=col:type,..."));
        };
        let csv = std::fs::read_to_string(path)?;
        out += &format!("#{name} {schema}\n{}\n", csv.split_once('\n').map_or("", |c| c.1));
    }
    Ok(out.into_bytes())
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn unhex(s: &str) -> Result<Vec<u8>> {
    let byte = |i| u8::from_str_radix(s.get(i..i + 2).ok_or(anyhow!("odd hex"))?, 16).map_err(|e| anyhow!("{e}"));
    (0..s.len()).step_by(2).map(byte).collect()
}
