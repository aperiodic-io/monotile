//! Remote Parquet files read by byte ranges: the footer once, then, for each row group a query
//! reads, only the column chunks of the columns it reads, fetched together (object_store's
//! `get_ranges`: nearby ranges coalesced, several requests in flight) and decoded from memory.
//! The slices of a row group decoded side by side share its fetch, freed when they are done.
//! Ranges fetched are kept in the download cache, by object version and byte range, under its
//! size and its leases (`files::Files::entry`), so a query asked again reads them from disk.
use crate::files::Lease;
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::file::reader::{ChunkReader, Length};
use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// What the tail of a file is fetched as at first: its footer, most often whole.
const TAIL: u64 = 64 << 10;

/// Fetched byte ranges of a file: (offset, bytes).
pub type Chunks = Vec<(u64, Bytes)>;

/// A row group's chunks, fetched once for every slice of it that needs them.
pub type Fetch = OnceLock<std::result::Result<Chunks, String>>;

/// The row groups being read, by (row group, leaves): their fetch, while a slice holds it.
type Groups = HashMap<(usize, Vec<usize>), Weak<Fetch>>;

/// Where a remote file's ranges come from: the cache, else the store.
struct Remote {
    name: String,
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    rt: tokio::runtime::Handle,
    /// The cache: the file's entry and its lease, the cache's directory and size; none without one.
    cache: Option<(PathBuf, Arc<Lease>, PathBuf, u64)>,
    /// Bytes fetched from the store (not the cache).
    fetched: AtomicU64,
}

impl Remote {
    /// A byte range of the file (`ranges`).
    fn range(&self, r: Range<u64>) -> Result<Bytes> {
        Ok(self.ranges(std::slice::from_ref(&r))?.remove(0))
    }

    /// Byte ranges of the file: from the cache, else fetched together and kept in it.
    fn ranges(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let file = |r: &Range<u64>| self.cache.as_ref().map(|c| c.0.join(format!(".range-{}-{}", r.start, r.end)));
        let mut out: Vec<Option<Bytes>> = ranges
            .iter()
            .map(|r| {
                let b = file(r).and_then(|p| std::fs::read(p).ok());
                b.filter(|b| b.len() as u64 == r.end - r.start).map(Bytes::from)
            })
            .collect();
        let missing: Vec<Range<u64>> =
            ranges.iter().zip(&out).filter(|(_, b)| b.is_none()).map(|(r, _)| r.clone()).collect();
        if missing.is_empty() {
            return Ok(out.into_iter().flatten().collect());
        }
        let got = self
            .rt
            .block_on(self.store.get_ranges(&self.path, &missing))
            .with_context(|| format!("reading {}", self.name))?;
        self.fetched.fetch_add(got.iter().map(|b| b.len() as u64).sum(), Ordering::Relaxed);
        let mut got = got.into_iter();
        for (r, slot) in ranges.iter().zip(out.iter_mut()) {
            if slot.is_none() {
                let b = got.next().ok_or(anyhow!("{}: a range not fetched", self.name))?;
                if let Some(p) = file(r) {
                    // written whole or not at all: another process may read it at once
                    let tmp = p.with_extension(format!("{}.part", std::process::id()));
                    if std::fs::write(&tmp, &b).is_ok() {
                        let _ = std::fs::rename(&tmp, &p);
                    }
                }
                *slot = Some(b);
            }
        }
        if let Some((_, _, root, cap)) = &self.cache {
            crate::files::evict(root, *cap);
        }
        Ok(out.into_iter().flatten().collect())
    }
}

/// A remote Parquet file read by ranges.
pub struct Ranged {
    pub name: String,
    pub size: u64,
    pub meta: ArrowReaderMetadata,
    remote: Remote,
    /// The row groups being read, by (row group, leaves): their fetch, while a slice holds it.
    groups: Mutex<Groups>,
}

impl std::fmt::Debug for Ranged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ranged({}, {} bytes)", self.name, self.size)
    }
}

impl Ranged {
    /// A remote file, its footer read (from the cache when it is there). `cache`: the file's
    /// entry and its lease, the cache's directory and size.
    pub fn open(
        name: &str,
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
        size: u64,
        rt: tokio::runtime::Handle,
        cache: Option<(PathBuf, Arc<Lease>, PathBuf, u64)>,
    ) -> Result<Ranged> {
        let remote = Remote { name: name.to_string(), store, path, rt, cache, fetched: AtomicU64::new(0) };
        let not = || format!("{name}: not a Parquet file");
        let tail = size.min(TAIL);
        let mut bytes = remote.range(size - tail..size)?;
        if bytes.len() < 8 || &bytes[bytes.len() - 4..] != b"PAR1" {
            bail!("{}", not());
        }
        let len = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into()?) as u64;
        if len + 8 > size {
            bail!("{}", not());
        }
        if len + 8 > tail {
            bytes = remote.range(size - len - 8..size)?;
        }
        let footer = &bytes[bytes.len() - 8 - len as usize..bytes.len() - 8];
        let meta = parquet::file::metadata::ParquetMetaDataReader::decode_metadata(footer).with_context(not)?;
        let meta = ArrowReaderMetadata::try_new(Arc::new(meta), Default::default()).with_context(not)?;
        Ok(Ranged { name: name.to_string(), size, meta, remote, groups: Mutex::default() })
    }

    /// Bytes fetched from the store so far (not those read from the cache).
    pub fn fetched(&self) -> u64 {
        self.remote.fetched.load(Ordering::Relaxed)
    }

    /// The column chunks of row group `g`'s `leaves`, fetched once for all the slices of it
    /// read while one holds them.
    fn chunks(&self, g: usize, leaves: &[usize]) -> Result<Arc<Fetch>> {
        let fetch = {
            let mut groups = self.groups.lock().expect("groups");
            groups.retain(|_, w| w.strong_count() > 0);
            let key = (g, leaves.to_vec());
            match groups.get(&key).and_then(Weak::upgrade) {
                Some(f) => f,
                None => {
                    let f = Arc::new(Fetch::new());
                    groups.insert(key, Arc::downgrade(&f));
                    f
                }
            }
        };
        let got = fetch.get_or_init(|| {
            let rg = self.meta.metadata().row_group(g);
            let ranges: Vec<Range<u64>> = leaves
                .iter()
                .map(|&c| {
                    let (start, len) = rg.column(c).byte_range();
                    start..start + len
                })
                .collect();
            let fetched = self.remote.ranges(&ranges).map_err(|e| format!("{e:#}"))?;
            Ok(ranges.iter().map(|r| r.start).zip(fetched).collect())
        });
        match got {
            Ok(_) => Ok(fetch),
            Err(e) => Err(anyhow!("{e}")),
        }
    }
}

/// A Parquet file's bytes as the decoders read them: a local file, or a remote one by ranges
/// (with the chunks of the row group being read fetched ahead).
pub enum Input {
    Local(std::fs::File),
    Ranged(Arc<Ranged>, Option<Arc<Fetch>>),
}

impl Input {
    /// A file's input; for a remote one and `unit` (a row group, the leaves read), the chunks
    /// of that row group's leaves fetched.
    pub fn of(f: &crate::files::File, unit: Option<(usize, &[usize])>) -> Result<Input> {
        Ok(match &f.ranged {
            Some(r) => Input::Ranged(r.clone(), unit.map(|(g, leaves)| r.chunks(g, leaves)).transpose()?),
            None => Input::Local(std::fs::File::open(&f.local).with_context(|| f.name.clone())?),
        })
    }

    /// The chunk fetched that holds `start`: its offset and bytes.
    fn chunk(&self, start: u64) -> Option<(u64, &Bytes)> {
        let Input::Ranged(_, Some(fetch)) = self else { return None };
        let chunks = fetch.get()?.as_ref().ok()?;
        chunks.iter().find(|(at, b)| start >= *at && start < *at + b.len() as u64).map(|(at, b)| (*at, b))
    }
}

impl Length for Input {
    fn len(&self) -> u64 {
        match self {
            Input::Local(f) => f.len(),
            Input::Ranged(r, _) => r.size,
        }
    }
}

fn external(e: anyhow::Error) -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::External(e.into())
}

impl ChunkReader for Input {
    type T = Box<dyn std::io::Read + Send>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        match self {
            Input::Local(f) => Ok(Box::new(f.get_read(start)?)),
            Input::Ranged(r, _) => {
                // within a chunk: the rest of it; else what follows, fetched
                let b = match self.chunk(start) {
                    Some((at, b)) => b.slice((start - at) as usize..),
                    None => {
                        let end = r.size.min(start + (1 << 20));
                        r.remote.range(start..end).map_err(external)?
                    }
                };
                Ok(Box::new(std::io::Cursor::new(b)))
            }
        }
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        match self {
            Input::Local(f) => f.get_bytes(start, length),
            Input::Ranged(r, _) => match self.chunk(start) {
                Some((at, b)) if start - at + length as u64 <= b.len() as u64 => {
                    Ok(b.slice((start - at) as usize..(start - at) as usize + length))
                }
                _ => Ok(r.remote.range(start..start + length as u64).map_err(external)?),
            },
        }
    }
}
