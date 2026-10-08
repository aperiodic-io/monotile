//! Where a table's files are: a path, a glob, a directory, or a URL of an object store
//! (`s3://`, `gs://`, `az://`/`abfss://`, `http(s)://`). Remote files are downloaded once into a
//! cache (re-fetched when the object changes) and read as local ones; results are uploaded.
//!
//! Object stores take their settings from the environment, as each cloud's own tools do:
//! - S3 and compatible stores (MinIO and the like): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
//!   `AWS_REGION`, `AWS_ENDPOINT_URL` (or `AWS_ENDPOINT`), `AWS_SKIP_SIGNATURE=true` for public
//!   buckets;
//! - GCS: `GOOGLE_SERVICE_ACCOUNT` (a key file) or `GOOGLE_SERVICE_ACCOUNT_KEY` (its JSON);
//! - Azure: `AZURE_STORAGE_ACCOUNT_NAME` with `AZURE_STORAGE_ACCOUNT_KEY` or
//!   `AZURE_STORAGE_SAS_KEY`; `AZURE_STORAGE_USE_EMULATOR=true` for Azurite.
use anyhow::{anyhow, bail, Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ClientOptions, ObjectStore, RetryConfig, WriteMultipart};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use url::Url;

/// Results up to this size are uploaded in one request; larger ones in parts.
const SINGLE_PUT: u64 = 64 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Parquet,
    Csv {
        delimiter: u8,
    },
    Json,
    /// Databento's binary encoding, zstd-compressed or not (`dbn`).
    Dbn,
}

/// A file of a table.
#[derive(Clone, Debug)]
pub struct File {
    /// As the user named it: a path or a URL.
    pub name: String,
    /// Where it is read from (a remote file's copy in the cache).
    pub local: PathBuf,
    pub format: Format,
    pub gzip: bool,
    /// Hive-style `key=value` directories on its path, outermost first.
    pub partitions: Vec<(String, String)>,
    /// A remote file's object, until `Files::fetch` copies it into the cache (`local`).
    pub object: Option<object_store::ObjectMeta>,
    /// Its reader's options (`read_csv('x.csv', header = false)`).
    pub options: Arc<crate::read::Options>,
    /// What keeps a remote file's copy there while it is read (`Lease`).
    pub lease: Option<Arc<Lease>>,
    /// A remote Parquet file read by byte ranges, not downloaded (`Files::fetch`).
    pub ranged: Option<Arc<crate::ranged::Ranged>>,
}

/// What keeps a remote file's copy readable while a query reads it, until the last of the
/// query's `File`s is dropped: a shared lock on its cache entry, which no process evicts while
/// one is held, or, without a cache, the temporary copy itself, deleted then.
#[derive(Debug)]
pub enum Lease {
    Entry(std::fs::File),
    Temp(PathBuf),
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Lease::Temp(p) = self {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Object stores by bucket, and the cache remote files are read from.
pub struct Files {
    cache: PathBuf,
    /// The cache's size at most (bytes), the least recently used files evicted past it; 0: no
    /// cache (`cache_size`).
    cap: u64,
    /// Remote Parquet files of this size or more are read by ranges (`RANGED_FROM`).
    ranged_from: u64,
    /// Without a cache, the temporary copies read now, by object and version: a query that
    /// fetches a file twice (its columns, then its rows) downloads it once.
    temps: Mutex<HashMap<String, std::sync::Weak<Lease>>>,
    /// The remote Parquet files read by ranges now, by object and version: their footer read once.
    ranged: Mutex<HashMap<String, std::sync::Weak<crate::ranged::Ranged>>>,
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
    rt: tokio::runtime::Runtime,
}

/// Remote Parquet files of this size or more are read by ranges (`ranged`), smaller ones
/// downloaded whole: one request then costs less than the several ranges would save.
/// `BRRRRR_RANGED_FROM` sets another (bytes; 0: every one by ranges).
pub const RANGED_FROM: u64 = 16 << 20;

/// The cache's size by default: a few weeks of a market's files, and a small part of a disk.
pub const CACHE_SIZE: u64 = 10 << 30;

/// `BRRRRR_CACHE_SIZE` (`10G`, `500MB`, `0` for no cache, or bytes), else `CACHE_SIZE`.
pub fn cache_size() -> Result<u64> {
    let Ok(v) = std::env::var("BRRRRR_CACHE_SIZE") else { return Ok(CACHE_SIZE) };
    let t = v.trim().to_ascii_uppercase();
    let t = t.trim_end_matches("IB").trim_end_matches('B');
    let (n, unit) = match t.find(|c: char| !c.is_ascii_digit() && c != '.') {
        Some(i) => (&t[..i], &t[i..]),
        None => (t, ""),
    };
    let k: f64 = match unit.trim() {
        "" => 1.0,
        "K" => 1024.0,
        "M" => 1024.0 * 1024.0,
        "G" => 1024.0 * 1024.0 * 1024.0,
        "T" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => bail!("BRRRRR_CACHE_SIZE={v}: a size (10G, 500M, 0 for no cache)"),
    };
    let n: f64 = n.parse().map_err(|_| anyhow!("BRRRRR_CACHE_SIZE={v}: a size (10G, 500M, 0 for no cache)"))?;
    Ok((n * k) as u64)
}

impl Default for Files {
    fn default() -> Self {
        Files::new(default_cache())
    }
}

/// `BRRRRR_CACHE`, or `brrrrr` in the user's cache directory.
pub fn default_cache() -> PathBuf {
    if let Ok(c) = std::env::var("BRRRRR_CACHE") {
        return c.into();
    }
    let home = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .or_else(|_| std::env::var("LOCALAPPDATA").map(PathBuf::from))
        .unwrap_or_else(|_| std::env::temp_dir());
    home.join("brrrrr")
}

pub fn has_glob(s: &str) -> bool {
    s.contains(['*', '?', '['])
}

/// A file's format and whether it is gzipped, from its name (`None`: not a table file).
pub fn format_of(name: &str, wanted: Option<&str>) -> Option<(Format, bool)> {
    let lower = name.to_ascii_lowercase();
    if wanted == Some("dbn") || wanted.is_none() && (lower.ends_with(".dbn") || lower.ends_with(".dbn.zst")) {
        return Some((Format::Dbn, false));
    }
    let (stem, gzip) = match lower.strip_suffix(".gz") {
        Some(s) => (s.to_string(), true),
        None => (lower.clone(), false),
    };
    let ext = stem.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
    let by_ext = match ext {
        "parquet" | "pq" | "parq" => Some(Format::Parquet),
        "csv" | "txt" => Some(Format::Csv { delimiter: b',' }),
        "tsv" | "tab" => Some(Format::Csv { delimiter: b'\t' }),
        "json" | "jsonl" | "ndjson" => Some(Format::Json),
        _ => None,
    };
    let format = match wanted {
        Some("parquet") => Format::Parquet,
        Some("csv") => by_ext.filter(|f| matches!(f, Format::Csv { .. })).unwrap_or(Format::Csv { delimiter: b',' }),
        Some("json") => Format::Json,
        _ => by_ext?,
    };
    Some((format, gzip))
}

/// The value of a partition of NULLs (Hive's, Spark's and DuckDB's name for it).
pub const NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// The `key=value` directories of a path, their values unescaped (`%20` is a space).
fn partitions(path: &str) -> Vec<(String, String)> {
    let dirs: Vec<&str> = path.split('/').collect();
    dirs[..dirs.len().saturating_sub(1)]
        .iter()
        .filter_map(|d| d.split_once('=').filter(|(k, _)| !k.is_empty()))
        .map(|(k, v)| (k.to_string(), urlencoding_decode(v)))
        .collect()
}

/// A partition's directory, `key=value`, as DuckDB writes it: the value's text with what is
/// not a letter, digit, `-`, `_`, `.` or `~` escaped (`%2F`), NULL as `NULL_PARTITION`.
pub fn partition_dir(key: &str, value: Option<&str>) -> String {
    let Some(v) = value else { return format!("{key}={NULL_PARTITION}") };
    let mut out = format!("{key}=");
    for b in v.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The candidate closest to `name`, if it is close: as a typo is (an edit distance, a swap of
/// two letters one edit, of at most a quarter of the name's length, case aside).
pub fn closest<'a>(name: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    let a: Vec<char> = name.to_lowercase().chars().collect();
    let distance = |b: &str| {
        let b: Vec<char> = b.to_lowercase().chars().collect();
        let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
        (0..=a.len()).for_each(|i| d[i][0] = i);
        (0..=b.len()).for_each(|j| d[0][j] = j);
        for i in 1..=a.len() {
            for j in 1..=b.len() {
                let cost = usize::from(a[i - 1] != b[j - 1]);
                d[i][j] = (d[i - 1][j - 1] + cost).min(d[i - 1][j] + 1).min(d[i][j - 1] + 1);
                if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                    d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
                }
            }
        }
        d[a.len()][b.len()]
    };
    let max = (a.len() / 4).max(1);
    candidates
        .into_iter()
        .filter(|c| *c != name)
        .map(|c| (distance(c), c))
        .filter(|(d, _)| *d <= max)
        .min_by_key(|(d, c)| (*d, c.len()))
        .map(|(_, c)| c)
}

/// `: did you mean ...?`, or nothing.
pub fn hint(close: Option<String>) -> String {
    close.map_or(String::new(), |c| format!(": did you mean {c}?"))
}

/// A path that is not there: the closest of its directory's entries.
fn closest_path(location: &str) -> Option<String> {
    let path = Path::new(location);
    let name = path.file_name()?.to_str()?;
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    let names: Vec<String> = std::fs::read_dir(dir.unwrap_or(Path::new(".")))
        .ok()?
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    let close = closest(name, names.iter().map(String::as_str))?;
    Some(dir.map_or(close.to_string(), |d| d.join(close).display().to_string()))
}

pub fn is_url(s: &str) -> bool {
    s.split_once("://").is_some_and(|(scheme, _)| scheme.len() > 1 && scheme.chars().all(|c| c.is_ascii_alphanumeric()))
}

impl Files {
    pub fn new(cache: PathBuf) -> Files {
        // threads of its own: the decoders' ranged reads are several at a time
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("brrrrr-io")
            .enable_all()
            .build()
            .expect("a tokio runtime");
        // a size that does not parse: the default (the query says nothing of it)
        let cap = cache_size().unwrap_or(CACHE_SIZE);
        let ranged_from =
            std::env::var("BRRRRR_RANGED_FROM").ok().and_then(|v| v.trim().parse().ok()).unwrap_or(RANGED_FROM);
        Files {
            cache,
            cap,
            ranged_from,
            temps: Mutex::default(),
            ranged: Mutex::default(),
            stores: Mutex::new(HashMap::new()),
            rt,
        }
    }

    /// The same, of `cap` bytes at most (0: no cache).
    pub fn with_cap(cache: PathBuf, cap: u64) -> Files {
        Files { cap, ..Files::new(cache) }
    }

    /// The files of a location, in path order; `format` is a reader's choice (`read_csv`).
    /// Remote ones are listed, not fetched: `fetch` them before reading.
    pub fn list(&self, location: &str, format: Option<&str>) -> Result<Vec<File>> {
        let files = self.listing(location, format, false)?;
        if files.is_empty() {
            bail!("no files at {location}{}", hint(self.closest_object(location)));
        }
        Ok(files)
    }

    /// The first file of a location, in path order: in a store, of its first page of objects
    /// (stores list in key order), however many follow.
    pub fn first(&self, location: &str, format: Option<&str>) -> Result<File> {
        self.listing(location, format, true)?.into_iter().next().ok_or(anyhow!("no files at {location}"))
    }

    /// The files of a directory's subdirectories `dirs` (`date=2024-01-02/`), in path order,
    /// with the partitions of their paths from the directory; none for a subdirectory not there.
    pub fn list_under(&self, location: &str, format: Option<&str>, dirs: &[String]) -> Result<Vec<File>> {
        // a table's files are its log's, wherever they are: all of them (pruned when read)
        if let Some(files) = crate::tables::files(self, location)? {
            return Ok(files);
        }
        let root = location.trim_end_matches('/');
        let mut files = vec![];
        for d in dirs {
            let sub = format!("{root}/{d}");
            let remote = is_url(&sub) && !sub.starts_with("file://");
            if !remote && !Path::new(sub.strip_prefix("file://").unwrap_or(&sub)).is_dir() {
                continue;
            }
            for mut f in self.listing(&sub, format, false)? {
                if !remote {
                    let rel = f.name.strip_prefix(root.strip_prefix("file://").unwrap_or(root)).unwrap_or(&f.name);
                    f.partitions = partitions(rel);
                }
                files.push(f);
            }
        }
        files.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(files)
    }

    fn listing(&self, location: &str, format: Option<&str>, first: bool) -> Result<Vec<File>> {
        // a Delta or Iceberg table: the files its log or metadata says
        if let Some(files) = crate::tables::files(self, location)? {
            return Ok(if first { files.into_iter().take(1).collect() } else { files });
        }
        if is_url(location) && !location.starts_with("file://") {
            return self.remote(location, format, first);
        }
        let mut files = local(location.strip_prefix("file://").unwrap_or(location), format)?;
        if first {
            files.truncate(1);
        }
        Ok(files)
    }

    /// The store of a URL's bucket, built once from the environment.
    pub fn store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let key = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
        if let Some(s) = self.stores.lock().expect("stores").get(&key) {
            return Ok(s.clone());
        }
        let opts = ClientOptions::new().with_allow_http(true).with_connect_timeout(std::time::Duration::from_secs(5));
        // a wrong endpoint or credentials say so in seconds, not after minutes of retries
        let retry =
            RetryConfig { max_retries: 3, retry_timeout: std::time::Duration::from_secs(20), ..Default::default() };
        let s: Arc<dyn ObjectStore> = match url.scheme() {
            "s3" | "s3a" => {
                let mut b =
                    AmazonS3Builder::from_env().with_url(url.as_str()).with_client_options(opts).with_retry(retry);
                // the AWS CLI's name for it; object_store reads AWS_ENDPOINT
                if let (Ok(e), Err(_)) = (std::env::var("AWS_ENDPOINT_URL"), std::env::var("AWS_ENDPOINT")) {
                    b = b.with_endpoint(e);
                }
                Arc::new(b.build()?)
            }
            "gs" | "gcs" => Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_url(url.as_str())
                    .with_client_options(opts)
                    .with_retry(retry)
                    .build()?,
            ),
            "az" | "azure" | "abfs" | "abfss" | "adl" => Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_url(url.as_str())
                    .with_client_options(opts)
                    .with_retry(retry)
                    .build()?,
            ),
            "http" | "https" => Arc::new(
                HttpBuilder::new()
                    .with_url(&url[..url::Position::BeforePath])
                    .with_client_options(opts)
                    .with_retry(retry)
                    .build()?,
            ),
            s => bail!("{s}:// is not a store brrrrr reads: paths, s3://, gs://, az://, abfss:// and http(s)://"),
        };
        self.stores.lock().expect("stores").insert(key, s.clone());
        Ok(s)
    }

    fn remote(&self, location: &str, format: Option<&str>, first: bool) -> Result<Vec<File>> {
        let url = Url::parse(location).with_context(|| format!("{location}: not a URL"))?;
        let store = self.store(&url)?;
        let path = url.path().trim_start_matches('/').to_string();
        let path = urlencoding_decode(&path);
        let wanted = |m: &object_store::ObjectMeta| {
            format_of(m.location.as_ref(), format).is_some() || !has_glob(&path) && format.is_some()
        };
        let objects: Vec<object_store::ObjectMeta> = self.rt.block_on(async {
            use futures::StreamExt;
            if has_glob(&path) {
                let pattern = glob::Pattern::new(&path).map_err(|e| anyhow!("{location}: {e}"))?;
                let prefix =
                    path[..path.find(['*', '?', '[']).unwrap_or(path.len())].rsplit_once('/').map_or("", |p| p.0);
                let mut out = vec![];
                let mut s = store.list(Some(&key(prefix)));
                while let Some(m) = s.next().await {
                    let m = m?;
                    if pattern.matches(m.location.as_ref()) && wanted(&m) {
                        out.push(m);
                        if first {
                            break;
                        }
                    }
                }
                Ok::<_, anyhow::Error>(out)
            } else if url.scheme().starts_with("http") {
                // over HTTP, a GET's headers (its body left unread): some servers refuse HEAD,
                // some answer it with a 404 after seconds
                Ok(vec![store.get(&key(&path)).await?.meta])
            } else {
                match store.head(&key(&path)).await {
                    Ok(m) => Ok(vec![m]),
                    // a directory: everything under it
                    Err(object_store::Error::NotFound { .. }) => {
                        let mut out = vec![];
                        let mut s = store.list(Some(&key(&path)));
                        while let Some(m) = s.next().await {
                            let m = m?;
                            if wanted(&m) {
                                out.push(m);
                                if first {
                                    break;
                                }
                            }
                        }
                        Ok(out)
                    }
                    Err(e) => Err(e.into()),
                }
            }
        })?;
        let mut objects: Vec<_> = objects.into_iter().filter(wanted).collect();
        objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
        let mut files = vec![];
        for m in objects {
            let key = m.location.to_string();
            let name = format!("{}://{}/{key}", url.scheme(), url.host_str().unwrap_or(""));
            let (format, gzip) = format_of(&key, format).or_else(|| format_of(&path, format)).ok_or(anyhow!("{name}: unknown format: name it .parquet, .csv or .json, or read it with read_parquet/read_csv/read_json"))?;
            files.push(File {
                name,
                local: PathBuf::new(),
                format,
                gzip,
                partitions: partitions(&key),
                object: Some(m),
                options: Default::default(),
                lease: None,
                ranged: None,
            });
        }
        Ok(files)
    }

    /// Copies the remote files into the cache (those not there yet), to be read as local ones.
    pub fn fetch(&self, files: &mut [File]) -> Result<()> {
        for f in files {
            if let Some(m) = f.object.take() {
                let store = self.store(&Url::parse(&f.name)?)?;
                // a large Parquet file: by ranges, what a query reads of it alone
                if f.format == Format::Parquet && m.size >= self.ranged_from {
                    f.ranged = Some(self.ranged(&store, &m, &f.name)?);
                    continue;
                }
                let (local, lease) = self.download(&store, &m, &f.name)?;
                (f.local, f.lease) = (local, Some(lease));
            }
        }
        Ok(())
    }

    /// An object's copy in the cache, downloaded unless the copy there is of its version, and
    /// the lease that keeps it there while it is read. Without a cache, a temporary copy.
    fn download(
        &self,
        store: &Arc<dyn ObjectStore>,
        m: &object_store::ObjectMeta,
        name: &str,
    ) -> Result<(PathBuf, Arc<Lease>)> {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pid = std::process::id();
        let base = name.rsplit('/').next().unwrap_or("file");
        let version = format!("{}-{}-{}", m.size, m.e_tag.as_deref().unwrap_or(""), m.last_modified.timestamp_millis());
        if self.cap == 0 {
            let key = format!("{name}\n{version}");
            if let Some(lease) = self.temps.lock().expect("temps").get(&key).and_then(|w| w.upgrade()) {
                if let Lease::Temp(p) = lease.as_ref() {
                    return Ok((p.clone(), lease.clone()));
                }
            }
            let tmp = std::env::temp_dir().join(format!(".brrrrr-{pid}-{n}-{base}"));
            let lease = Arc::new(Lease::Temp(tmp.clone()));
            self.get_into(store, m, name, &tmp)?;
            let mut temps = self.temps.lock().expect("temps");
            temps.retain(|_, w| w.strong_count() > 0);
            temps.insert(key, Arc::downgrade(&lease));
            return Ok((tmp, lease));
        }
        let (dir, lease) = self.entry(name, &version)?;
        let local = dir.join(base);
        if !local.exists() {
            let tmp = dir.join(format!(".{base}.{pid}-{n}.part"));
            self.get_into(store, m, name, &tmp)?;
            std::fs::rename(&tmp, &local)?;
            evict(&self.cache, self.cap);
        }
        Ok((local, lease))
    }

    /// An object's entry in the cache (a directory), of `version`, leased: one of another
    /// version is emptied first; one evicted while we waited for the lease is made again.
    fn entry(&self, name: &str, version: &str) -> Result<(PathBuf, Arc<Lease>)> {
        use std::hash::{Hash, Hasher};
        use std::io::{Read, Seek, Write};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        name.hash(&mut h);
        let dir = self.cache.join(format!("{:016x}", h.finish()));
        let stamp_path = dir.join(".version");
        for _ in 0..3 {
            std::fs::create_dir_all(&dir).with_context(|| format!("the cache {}", dir.display()))?;
            let mut stamp = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&stamp_path)
                .with_context(|| format!("the cache {}", dir.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                stamp.lock_shared()?;
                let (held, there) = (stamp.metadata()?, std::fs::metadata(&stamp_path));
                if !there.is_ok_and(|t| (t.dev(), t.ino()) == (held.dev(), held.ino())) {
                    continue;
                }
            }
            let mut have = String::new();
            stamp.read_to_string(&mut have)?;
            if have != version {
                for e in std::fs::read_dir(&dir)?.flatten() {
                    if e.file_name() != ".version" {
                        let _ = std::fs::remove_file(e.path());
                    }
                }
                stamp.set_len(0)?;
                stamp.rewind()?;
                stamp.write_all(version.as_bytes())?;
            }
            // its last use, for eviction
            let _ = stamp.set_modified(std::time::SystemTime::now());
            return Ok((dir, Arc::new(Lease::Entry(stamp))));
        }
        bail!("the cache {}: its entry for {name} was evicted again and again", dir.display())
    }

    /// A remote Parquet file read by ranges (the one open now, if one is: its footer read once).
    fn ranged(
        &self,
        store: &Arc<dyn ObjectStore>,
        m: &object_store::ObjectMeta,
        name: &str,
    ) -> Result<Arc<crate::ranged::Ranged>> {
        let version = format!("{}-{}-{}", m.size, m.e_tag.as_deref().unwrap_or(""), m.last_modified.timestamp_millis());
        let key = format!("{name}\n{version}");
        if let Some(r) = self.ranged.lock().expect("ranged").get(&key).and_then(|w| w.upgrade()) {
            return Ok(r);
        }
        let cache = match self.cap {
            0 => None,
            cap => {
                let (dir, lease) = self.entry(name, &version)?;
                Some((dir, lease, self.cache.clone(), cap))
            }
        };
        let r = Arc::new(crate::ranged::Ranged::open(
            name,
            store.clone(),
            m.location.clone(),
            m.size,
            self.rt.handle().clone(),
            cache,
        )?);
        let mut all = self.ranged.lock().expect("ranged");
        all.retain(|_, w| w.strong_count() > 0);
        all.insert(key, Arc::downgrade(&r));
        Ok(r)
    }

    /// An object written to `to`.
    fn get_into(
        &self,
        store: &Arc<dyn ObjectStore>,
        m: &object_store::ObjectMeta,
        name: &str,
        to: &Path,
    ) -> Result<()> {
        self.rt
            .block_on(async {
                use futures::StreamExt;
                use std::io::Write;
                let mut out = std::fs::File::create(to)?;
                let mut s = store.get(&m.location).await?.into_stream();
                while let Some(chunk) = s.next().await {
                    out.write_all(&chunk?)?;
                }
                out.sync_all()?;
                Ok::<_, anyhow::Error>(())
            })
            .with_context(|| format!("downloading {name}"))
    }

    /// Everything under a directory (a store's prefix, recursively): each one's name (a path, or
    /// a URL) and its object (a store's); with `first`, the first one alone.
    pub fn names(&self, dir: &str, first: bool) -> Result<Vec<(String, Option<object_store::ObjectMeta>)>> {
        if is_url(dir) && !dir.starts_with("file://") {
            let url = Url::parse(dir).with_context(|| format!("{dir}: not a URL"))?;
            let store = self.store(&url)?;
            let prefix = key(&urlencoding_decode(url.path().trim_matches('/')));
            let base = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
            return self.rt.block_on(async {
                use futures::StreamExt;
                let mut out = vec![];
                let mut s = store.list(Some(&prefix));
                while let Some(m) = s.next().await {
                    let m = m?;
                    out.push((format!("{base}/{}", m.location), Some(m)));
                    if first {
                        break;
                    }
                }
                Ok(out)
            });
        }
        fn all(p: &Path, out: &mut Vec<PathBuf>, first: bool) {
            for e in std::fs::read_dir(p).into_iter().flatten().flatten() {
                if first && !out.is_empty() {
                    return;
                }
                let p = e.path();
                if p.is_dir() {
                    all(&p, out, first);
                } else {
                    out.push(p);
                }
            }
        }
        let mut paths = vec![];
        all(Path::new(dir.strip_prefix("file://").unwrap_or(dir)), &mut paths, first);
        paths.sort();
        Ok(paths.into_iter().map(|p| (p.to_string_lossy().into_owned(), None)).collect())
    }

    /// A file's bytes, local or in a store (a table's log or metadata).
    pub fn bytes(&self, name: &str) -> Result<Vec<u8>> {
        if !is_url(name) || name.starts_with("file://") {
            let p = name.strip_prefix("file://").unwrap_or(name);
            return std::fs::read(p).with_context(|| p.to_string());
        }
        let url = Url::parse(name).with_context(|| format!("{name}: not a URL"))?;
        let store = self.store(&url)?;
        let path = key(&urlencoding_decode(url.path().trim_start_matches('/')));
        self.rt
            .block_on(async { Ok::<_, anyhow::Error>(store.get(&path).await?.bytes().await?.to_vec()) })
            .with_context(|| name.to_string())
    }

    /// A table's Parquet file `name` (a path, or a URL: its object, asked for if not given).
    pub fn data_file(
        &self,
        name: &str,
        object: Option<object_store::ObjectMeta>,
        partitions: Vec<(String, String)>,
    ) -> Result<File> {
        let remote = is_url(name) && !name.starts_with("file://");
        let object = match (remote, object) {
            (false, _) => None,
            (true, Some(m)) => Some(m),
            (true, None) => {
                let url = Url::parse(name).with_context(|| format!("{name}: not a URL"))?;
                let store = self.store(&url)?;
                let path = key(&urlencoding_decode(url.path().trim_start_matches('/')));
                Some(self.rt.block_on(store.head(&path)).with_context(|| name.to_string())?)
            }
        };
        let local = if remote { PathBuf::new() } else { PathBuf::from(name.strip_prefix("file://").unwrap_or(name)) };
        let options = Default::default();
        Ok(File {
            name: name.to_string(),
            local,
            format: Format::Parquet,
            gzip: false,
            partitions,
            object,
            options,
            lease: None,
            ranged: None,
        })
    }

    /// A location there is nothing at: the closest of what its parent holds (one listing of a
    /// store's prefix), for an error to suggest.
    fn closest_object(&self, location: &str) -> Option<String> {
        if !is_url(location) || location.starts_with("file://") || has_glob(location) {
            return None;
        }
        let url = Url::parse(location).ok()?;
        let path = urlencoding_decode(url.path().trim_matches('/'));
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", &path));
        let store = self.store(&url).ok()?;
        let listed =
            self.rt.block_on(store.list_with_delimiter((!parent.is_empty()).then(|| key(parent)).as_ref())).ok()?;
        let names: Vec<String> = listed
            .objects
            .iter()
            .map(|o| o.location.as_ref())
            .chain(listed.common_prefixes.iter().map(|p| p.as_ref()))
            .map(|k| k.rsplit('/').next().unwrap_or(k).to_string())
            .collect();
        let close = closest(name, names.iter().map(String::as_str))?;
        let base = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
        Some(if parent.is_empty() { format!("{base}/{close}") } else { format!("{base}/{parent}/{close}") })
    }

    /// Whether a directory, or a store's prefix, holds a file; an error if it is a file.
    pub fn has_files(&self, location: &str) -> Result<bool> {
        if is_url(location) && !location.starts_with("file://") {
            let url = Url::parse(location).with_context(|| format!("{location}: not a URL"))?;
            let store = self.store(&url)?;
            let prefix = key(&urlencoding_decode(url.path().trim_matches('/')));
            return self.rt.block_on(async {
                use futures::StreamExt;
                Ok(store.list(Some(&prefix)).next().await.transpose()?.is_some())
            });
        }
        let p = Path::new(location.strip_prefix("file://").unwrap_or(location));
        if p.is_file() {
            bail!("{location} is a file, not a directory");
        }
        fn any(p: &Path) -> bool {
            std::fs::read_dir(p).into_iter().flatten().flatten().any(|e| !e.path().is_dir() || any(&e.path()))
        }
        Ok(any(p))
    }

    /// Writes `local` to `url` (an object store's or a file's).
    pub fn upload(&self, local: &Path, to: &str) -> Result<()> {
        if !is_url(to) || to.starts_with("file://") {
            let to = Path::new(to.strip_prefix("file://").unwrap_or(to));
            if let Some(dir) = to.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::copy(local, to).with_context(|| to.display().to_string())?;
            return Ok(());
        }
        let url = Url::parse(to)?;
        let store = self.store(&url)?;
        let path = key(&urlencoding_decode(url.path().trim_start_matches('/')));
        let size = std::fs::metadata(local)?.len();
        self.rt
            .block_on(async {
                use std::io::Read;
                // one request up to 64 MiB (every store and emulator takes it); in parts past it
                if size <= SINGLE_PUT {
                    store.put(&path, std::fs::read(local)?.into()).await?;
                    return Ok(());
                }
                let mut w = WriteMultipart::new(store.put_multipart(&path).await?);
                let mut f = std::fs::File::open(local)?;
                let mut buf = vec![0; 8 << 20];
                loop {
                    let n = f.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    w.write(&buf[..n]);
                }
                w.finish().await?;
                Ok::<_, anyhow::Error>(())
            })
            .with_context(|| format!("uploading to {to}"))
    }
}

/// An object's key as it is (`ObjPath::from` would escape a `%` in it, which DuckDB's partition
/// directories have: `lot=big%20lots`).
fn key(k: &str) -> ObjPath {
    ObjPath::parse(k).unwrap_or_else(|_| ObjPath::from(k))
}

/// The cache within its size: the entries used least recently evicted first, but none a
/// query reads (a process holds its lease: `download`). Entries are renamed away before they
/// are deleted, so that no process finds one half gone.
pub fn evict(cache: &Path, cap: u64) {
    let Ok(dir) = std::fs::read_dir(cache) else { return };
    let mut entries = vec![];
    let mut total = 0;
    for e in dir.flatten() {
        let p = e.path();
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with(".evicted-") {
            let _ = std::fs::remove_dir_all(&p); // an eviction cut short
            continue;
        }
        if !p.is_dir() || name.starts_with('.') {
            continue;
        }
        let size: u64 = std::fs::read_dir(&p)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|f| f.metadata().ok())
            .map(|m| m.len())
            .sum();
        let used = std::fs::metadata(p.join(".version")).and_then(|m| m.modified()).unwrap_or(std::time::UNIX_EPOCH);
        total += size;
        entries.push((used, size, p));
    }
    if total <= cap {
        return;
    }
    entries.sort();
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    for (used, size, p) in entries {
        if total <= cap {
            break;
        }
        let Ok(stamp) = std::fs::OpenOptions::new().read(true).write(true).open(p.join(".version")) else {
            continue;
        };
        // read by a query: kept (where locks are not taken, any used in the last hour)
        #[cfg(unix)]
        if stamp.try_lock().is_err() {
            continue;
        }
        #[cfg(not(unix))]
        if used.elapsed().map_or(true, |a| a.as_secs() < 3600) {
            continue;
        }
        let _ = used;
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let away = cache.join(format!(".evicted-{}-{n}", std::process::id()));
        if std::fs::rename(&p, &away).is_ok() {
            let _ = std::fs::remove_dir_all(&away);
            total = total.saturating_sub(size);
        }
        drop(stamp);
    }
}

fn urlencoding_decode(s: &str) -> String {
    url::form_urlencoded::parse(format!("x={}", s.replace('+', "%2B")).as_bytes())
        .next()
        .map(|(_, v)| v.into_owned())
        .unwrap_or_else(|| s.to_string())
}

fn local(location: &str, format: Option<&str>) -> Result<Vec<File>> {
    let mut paths: Vec<PathBuf> = if has_glob(location) {
        glob::glob(location)
            .map_err(|e| anyhow!("{location}: {e}"))?
            .filter_map(|p| p.ok())
            .filter(|p| p.is_file())
            .collect()
    } else {
        let p = PathBuf::from(location);
        if p.is_dir() {
            let mut out = vec![];
            walk(&p, &mut out)?;
            out.retain(|f| format_of(&f.to_string_lossy(), format).is_some());
            out
        } else if p.is_file() {
            vec![p]
        } else {
            bail!("no file {location}{}", hint(closest_path(location)));
        }
    };
    paths.sort();
    paths
        .into_iter()
        .map(|p| {
            let name = p.to_string_lossy().to_string();
            let (format, gzip) = format_of(&name, format)
                .or_else(|| sniff(&p))
                .ok_or(anyhow!("{name}: unknown format: name it .parquet, .csv or .json, or read it with read_parquet/read_csv/read_json"))?;
            let rel = name.strip_prefix(location.trim_end_matches('/')).unwrap_or(&name).to_string();
            Ok(File {
                partitions: partitions(&rel),
                name,
                local: p,
                format,
                gzip,
                object: None,
                options: Default::default(),
                lease: None,
                ranged: None,
            })
        })
        .collect()
}

/// A file's format from its first bytes.
fn sniff(p: &Path) -> Option<(Format, bool)> {
    use std::io::Read;
    let mut head = [0u8; 4];
    std::fs::File::open(p).ok()?.read_exact(&mut head).ok()?;
    match &head {
        b"PAR1" => Some((Format::Parquet, false)),
        [0x1f, 0x8b, ..] => Some((Format::Csv { delimiter: b',' }, true)),
        [b'{', ..] => Some((Format::Json, false)),
        _ => Some((Format::Csv { delimiter: b',' }, false)),
    }
}

/// The Parquet files under `dir` (hidden and `_` entries left out), in no order.
pub fn walk_parquet(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut all = vec![];
    if walk(dir, &mut all).is_ok() {
        out.extend(all.into_iter().filter(|p| p.extension().is_some_and(|e| e == "parquet")));
    }
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for e in std::fs::read_dir(dir).with_context(|| dir.display().to_string())? {
        let p = e?.path();
        let hidden = p.file_name().is_some_and(|n| n.to_string_lossy().starts_with(['.', '_']));
        if hidden {
            continue;
        }
        if p.is_dir() {
            walk(&p, out)?;
        } else {
            out.push(p);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_and_partitions() {
        assert_eq!(format_of("a/b.parquet", None), Some((Format::Parquet, false)));
        assert_eq!(format_of("x.csv.gz", None), Some((Format::Csv { delimiter: b',' }, true)));
        assert_eq!(format_of("x.tsv", None), Some((Format::Csv { delimiter: b'\t' }, false)));
        assert_eq!(format_of("x.ndjson", None), Some((Format::Json, false)));
        assert_eq!(format_of("x.data", None), None);
        assert_eq!(format_of("x.data", Some("csv")), Some((Format::Csv { delimiter: b',' }, false)));
        assert_eq!(
            partitions("trades/date=2024-01-01/symbol=BTC/part-0.parquet"),
            [("date".to_string(), "2024-01-01".to_string()), ("symbol".to_string(), "BTC".to_string())]
        );
        // DuckDB's escapes, read back
        let dir = partition_dir("t", Some("2024-01-02 00:00:00 a/b+é~"));
        assert_eq!(dir, "t=2024-01-02%2000%3A00%3A00%20a%2Fb%2B%C3%A9~");
        assert_eq!(
            partitions(&format!("{dir}/x=/f")),
            [("t".into(), "2024-01-02 00:00:00 a/b+é~".into()), ("x".into(), String::new())]
        );
        assert_eq!(partition_dir("k", None), "k=__HIVE_DEFAULT_PARTITION__");
        // a typo's correction: close, the closest
        let names = ["trades.csv", "quotes.csv", "trades.parquet", "instruments.csv"];
        assert_eq!(closest("trade.csv", names), Some("trades.csv"));
        assert_eq!(closest("TRADES.CSV", names), Some("trades.csv"));
        assert_eq!(closest("qoutes.csv", names), Some("quotes.csv"));
        assert_eq!(closest("orders.csv", names), None);
        assert!(is_url("s3://b/k") && is_url("https://x/y") && !is_url("C:/x") && !is_url("data/x.csv"));
    }

    /// A directory served over HTTP (GET and HEAD, as object stores answer them), on a port of
    /// its own.
    fn serve(root: PathBuf) -> String {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for mut s in listener.incoming().flatten() {
                let mut buf = [0u8; 8192];
                let n = s.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let mut first = req.split_whitespace();
                let (method, path) = (first.next().unwrap_or(""), first.next().unwrap_or("/"));
                match std::fs::read(root.join(path.trim_start_matches('/'))) {
                    Ok(body) => {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nLast-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\n\
                             ETag: \"{}\"\r\nConnection: close\r\n\r\n",
                            body.len(),
                            body.len()
                        );
                        let _ = s.write_all(head.as_bytes());
                        if method == "GET" {
                            let _ = s.write_all(&body);
                        }
                    }
                    Err(_) => {
                        let _ =
                            s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    }
                }
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    #[test]
    fn the_cache_keeps_to_its_size_and_never_evicts_a_file_being_read() {
        let d = tempfile::tempdir().unwrap();
        let served = d.path().join("served");
        std::fs::create_dir_all(&served).unwrap();
        for (f, n) in [("a.csv", 600), ("b.csv", 600), ("c.csv", 600)] {
            std::fs::write(served.join(f), format!("x\n{}\n", "1".repeat(n))).unwrap();
        }
        let url = serve(served);
        let cache = d.path().join("cache");
        let files = Files::with_cap(cache.clone(), 1500);
        let get = |f: &str| {
            let mut l = files.list(&format!("{url}/{f}"), None).unwrap();
            files.fetch(&mut l).unwrap();
            l.remove(0)
        };
        let entries = || std::fs::read_dir(&cache).unwrap().flatten().filter(|e| e.path().is_dir()).count();
        // a and b fit; c does not: the least recently used goes, unless it is being read: a is
        // (its lease held), so b goes
        let a = get("a.csv");
        drop(get("b.csv"));
        std::thread::sleep(std::time::Duration::from_millis(20));
        let c = get("c.csv");
        assert!(a.local.exists() && c.local.exists(), "a is leased, c just used: kept");
        assert_eq!(entries(), 2);
        let a_copy = a.local.clone();
        drop((a, c));
        // b again: downloaded, and now a, the least recently used, goes
        std::thread::sleep(std::time::Duration::from_millis(20));
        let b = get("b.csv");
        assert!(b.local.exists() && !a_copy.exists());
        assert_eq!(entries(), 2);
        // a file changed is downloaded again
        std::fs::write(d.path().join("served/a.csv"), "x\n2\n").unwrap();
        let a = get("a.csv");
        assert_eq!(std::fs::read_to_string(&a.local).unwrap(), "x\n2\n");
        // no cache: a temporary copy, gone when its query is done
        let none = Files::with_cap(d.path().join("none"), 0);
        let mut l = none.list(&format!("{url}/b.csv"), None).unwrap();
        none.fetch(&mut l).unwrap();
        let copy = l[0].local.clone();
        assert!(copy.exists() && !d.path().join("none").exists());
        drop(l);
        assert!(!copy.exists());
        // a name close to one there
        let err = format!("{:#}", files.list(&format!("{url}/x/b.csv"), None).unwrap_err());
        assert!(err.contains("x/b.csv"), "{err}");
    }
}
