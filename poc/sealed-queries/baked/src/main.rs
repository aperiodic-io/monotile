//! POC 3: a query compiled into its own binary ahead of time (`QUERY_FILE=q.sql cargo build -p
//! baked --release`), its text encrypted, the binary stripped. `strings` finds no SQL in it. But
//! the key is in the binary too, as it must be for the binary to run the query: any cipher here
//! is obfuscation, as good as this XOR. The text is in memory while it runs (README.md, attack).
//!
//! baked name=location...
use anyhow::Result;
use std::io::Write;

// ponytail: XOR under a key in the same binary; a real cipher would hide the key no better
const SEALED: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/query.bin"));
const KEY: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/key.bin"));

fn main() -> Result<()> {
    let sql: Vec<u8> = SEALED.iter().zip(KEY.iter().cycle()).map(|(b, k)| b ^ k).collect();
    let out = sealed::run(std::str::from_utf8(&sql)?, &sealed::tables(&sealed::args())?)?;
    std::io::stdout().write_all(&out)?;
    Ok(())
}
