//! A build without the `sql` feature (the live images) reads and writes no files: a pipeline
//! with file streams (`type = 'file'`) is refused (`parquet.rs` is the real one).
use anyhow::{bail, Result};
use brrrrr_core::sql::{Catalog, Kind};
use brrrrr_core::value::Value;
use std::collections::HashMap;
use std::time::Instant;

pub enum Sources {}
pub enum Sinks {}
pub enum Cut {}

pub struct Chunk {
    pub stream: String,
    pub file: String,
    pub topic: String,
    pub offset: i64,
    pub rows: Vec<Vec<Value>>,
}

/// A pipeline with file streams is refused.
pub fn supported(cat: &Catalog) -> Result<()> {
    let file =
        cat.streams.values().find(|s| s.kind == Kind::External && s.settings.get("type").is_some_and(|t| t == "file"));
    if let Some(s) = file {
        bail!(
            "{}: file streams (type = 'file') need a build with the sql feature, as the release binary and image",
            s.name
        );
    }
    Ok(())
}

pub fn prune(_: &mut HashMap<(String, i32), i64>, _: &str, _: &str) {}

impl Sources {
    pub fn open(_: &Catalog, _: &[(String, i32, i64)]) -> Result<Option<Sources>> {
        Ok(None)
    }
    pub fn next(&mut self, _: usize, _: Instant) -> Option<Chunk> {
        match *self {}
    }
    pub fn caught_up(&self) -> bool {
        match *self {}
    }
    pub fn past_start(&self) -> bool {
        match *self {}
    }
    pub fn errors(&mut self) -> u64 {
        match *self {}
    }
}

impl Sinks {
    pub fn open(_: &Catalog, _: &str) -> Option<Sinks> {
        None
    }
    pub fn row(&mut self, _: &str, _: &[Value]) -> bool {
        match *self {}
    }
    pub fn result(&mut self) -> Result<()> {
        match *self {}
    }
    pub fn cut(&mut self) -> Result<Cut> {
        match *self {}
    }
    pub fn clean(&self, _: u64) -> Result<Vec<String>> {
        match *self {}
    }
}

impl Cut {
    pub fn commit(self, _: u64, _: Instant) -> Result<Vec<String>> {
        match self {}
    }
}
