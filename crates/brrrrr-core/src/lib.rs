//! brrrrr-core: everything that is deterministic and I/O free.
//! See docs/adr/0002 and docs/adr/0012.
pub mod agg;
pub mod book;
pub mod checkpoint;
pub mod column;
pub mod engine;
pub mod expr;
pub mod format;
pub mod over;
pub mod pow;
#[rustfmt::skip] // generated tables, kept four per line
mod pow_tables;
pub mod proto;
pub mod query;
pub mod sql;
pub mod value;
