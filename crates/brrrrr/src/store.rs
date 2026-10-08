//! Where checkpoints are kept (ADR-0008): a directory, by default on the volume mounted at
//! `/var/lib/brrrrr` (a Kubernetes volume), or, in a build with the `object-store` feature (off by
//! default), an object store: `s3://bucket/prefix` (S3, MinIO, other S3-compatible stores),
//! `memory:///`.
//!
//! A store holds named objects under one prefix (`<pipeline>/<hash of the SQL file>/`). The
//! runtime needs four things of it, all blocking (the data thread is not async, ADR-0005): list
//! the names, read one, create one that must not exist yet (the lease rests on that: two
//! instances creating one epoch, exactly one succeeds) and delete one.
use anyhow::{bail, Context, Result};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Where checkpoints go without `--checkpoints`: mount the pod's volume at `/var/lib/brrrrr`.
pub const DEFAULT: &str = "/var/lib/brrrrr/checkpoints";

/// Objects under one prefix. `Display` says where, for messages.
pub trait Store: Send + Sync + std::fmt::Display {
    /// The names of the objects under the prefix, in no particular order.
    fn list(&self) -> Result<Vec<String>>;
    fn get(&self, name: &str) -> Result<Vec<u8>>;
    /// Creates `name` with `bytes`, whole or not at all. `false`: it existed already, and is
    /// left as it was.
    fn create(&self, name: &str, bytes: &[u8]) -> Result<bool>;
    /// Deletes `name`; one that is not there is not an error.
    fn delete(&self, name: &str) -> Result<()>;
    /// Removes what a crash may have left behind (not objects). Best effort.
    fn tidy(&self) {}
}

/// Pods sharing a volume or a bucket may all be pid 1: the time and a counter tell them apart.
pub(crate) fn unique() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("{}-{nanos}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed))
}

/// Proves that `store` creates an object only if its name is free, which the lease rests on
/// (of two instances creating one epoch, exactly one succeeds): creates an object of this
/// process's own, then again, which must be refused, and deletes it. Its name is hidden, so
/// never listed, and ends in `.tmp`, so a directory's `tidy` removes it if a crash left it.
fn check_exclusive(store: &dyn Store) -> Result<()> {
    let name = format!(".probe-{}.tmp", unique());
    if !store.create(&name, b"")? {
        bail!("{store}/{name}, a new name, was found taken");
    }
    let again = store.create(&name, b"again");
    store.delete(&name)?;
    if again? {
        bail!("{store} created {name} a second time, over the first");
    }
    Ok(())
}

/// The store at `location` (a directory, `file:///dir`, or an object store URL), under `parts`
/// (each a path segment). A directory is created if missing and must be writable.
pub fn open(location: &str, parts: &[&str]) -> Result<Box<dyn Store>> {
    let dir = match location.split_once("://") {
        None => Some(location),
        Some(("file", path)) => Some(path),
        Some(_) => None,
    };
    match dir {
        Some("") => bail!("--checkpoints: an empty directory"),
        Some(dir) => Ok(Box::new(Dir::open(parts.iter().fold(PathBuf::from(dir), |d, p| d.join(p)))?)),
        #[cfg(feature = "object-store")]
        None => Ok(Box::new(Remote::open(location, parts)?)),
        #[cfg(not(feature = "object-store"))]
        None => bail!(
            "--checkpoints {location}: this brrrrr is built without the object-store feature; give a directory, \
             or run the -extended image"
        ),
    }
}

/// Objects as files in a directory. A file is written under a hidden temporary name, synced,
/// then hard-linked to its name, which fails if the name exists: whole or not at all, and
/// never over another instance's (on any POSIX file system, NFS included). An instance killed
/// mid-write leaves only its temporary file, which `tidy` removes.
pub struct Dir {
    dir: PathBuf,
    /// `std::fs::hard_link`, but in tests of file systems that refuse it.
    link: Link,
}

type Link = fn(&Path, &Path) -> std::io::Result<()>;

/// Temporary files older than this are a crashed write's: no write takes this long.
const STALE: Duration = Duration::from_secs(600);

fn not_found(e: &std::io::Error) -> bool {
    e.kind() == ErrorKind::NotFound
}

impl Dir {
    pub fn open(dir: PathBuf) -> Result<Dir> {
        Dir::with_link(dir, |from, to| std::fs::hard_link(from, to))
    }

    fn with_link(dir: PathBuf, link: Link) -> Result<Dir> {
        let d = Dir { dir, link };
        // fail at start, not at the first checkpoint minutes later (after recovery)
        let probe = d.write_temp(b"").and_then(std::fs::remove_file);
        probe.with_context(|| {
            format!(
                "checkpoint directory {d} is not writable (on Kubernetes: mount a PersistentVolumeClaim at \
                 /var/lib/brrrrr and set the pod's securityContext.fsGroup to its user's group, 65534 in the image)"
            )
        })?;
        // a claim would fail the same way, after recovery, and be tried again for ever
        check_exclusive(&d).with_context(|| {
            format!(
                "checkpoint directory {d} cannot create a file only if its name is free, which the lease between \
                 instances rests on (with a hard link, which CIFS/SMB volumes such as Azure Files, some FUSE-based \
                 CSI drivers and 9p shares refuse: use a POSIX file system, a block volume or NFS, or an object \
                 store)"
            )
        })?;
        Ok(d)
    }

    /// A new hidden file holding `bytes`, synced to disk. The directory is created if missing
    /// (the volume may have been emptied while running).
    fn write_temp(&self, bytes: &[u8]) -> std::io::Result<PathBuf> {
        std::fs::create_dir_all(&self.dir)?;
        // create_new refuses a name taken all the same
        let tmp = self.dir.join(format!(".{}.tmp", unique()));
        let mut f = std::fs::File::options().write(true).create_new(true).open(&tmp)?;
        if let Err(e) = f.write_all(bytes).and_then(|()| f.sync_all()) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(tmp)
    }
}

impl std::fmt::Display for Dir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.dir.display())
    }
}

impl Store for Dir {
    fn list(&self) -> Result<Vec<String>> {
        let entries = match std::fs::read_dir(&self.dir) {
            Err(e) if not_found(&e) => return Ok(vec![]),
            r => r.with_context(|| format!("listing {self}"))?,
        };
        let mut names = vec![];
        for e in entries {
            let e = e.with_context(|| format!("listing {self}"))?;
            let Ok(name) = e.file_name().into_string() else { continue };
            if !name.starts_with('.') && e.file_type().is_ok_and(|t| t.is_file()) {
                names.push(name);
            }
        }
        Ok(names)
    }

    fn get(&self, name: &str) -> Result<Vec<u8>> {
        std::fs::read(self.dir.join(name)).with_context(|| format!("reading {self}/{name}"))
    }

    fn create(&self, name: &str, bytes: &[u8]) -> Result<bool> {
        let tmp = self.write_temp(bytes).with_context(|| format!("writing {self}/{name}"))?;
        let linked = (self.link)(&tmp, &self.dir.join(name));
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(e).with_context(|| format!("creating {self}/{name}")),
            // the new name is durable once the directory is
            Ok(()) => sync_dir(&self.dir).with_context(|| format!("syncing {self}")).map(|()| true),
        }
    }

    fn delete(&self, name: &str) -> Result<()> {
        match std::fs::remove_file(self.dir.join(name)) {
            Err(e) if !not_found(&e) => Err(e).with_context(|| format!("deleting {self}/{name}")),
            _ => Ok(()),
        }
    }

    fn tidy(&self) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else { return };
        let now = SystemTime::now();
        for e in entries.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            let old =
                |m: std::fs::Metadata| m.modified().is_ok_and(|t| now.duration_since(t).unwrap_or_default() > STALE);
            if name.starts_with('.') && name.ends_with(".tmp") && e.metadata().is_ok_and(old) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Objects in an object store, created with `PutMode::Create`: on S3, `If-None-Match: *`
/// (`AWS_CONDITIONAL_PUT=etag`, which `open` sets itself and refuses to be set otherwise).
/// `open` proves that the store refuses to create a name twice; it cannot prove the refusal
/// atomic between concurrent creates, which S3 and MinIO guarantee and adobe/s3mock does
/// not. Credentials and settings come from the `AWS_*` environment.
///
/// One runtime with one worker thread serves every call, from the data thread and the
/// collector alike, and drives the pooled connections between calls. With a current-thread
/// runtime per calling thread, a checkpoint that picked up a connection the collector had
/// pooled waited for the collector's runtime to run again, which it only did after that
/// checkpoint: it timed out
/// (`tests::the_data_thread_and_the_collector_share_connections` reproduces it against S3).
#[cfg(feature = "object-store")]
pub struct Remote {
    store: Box<dyn object_store::ObjectStore>,
    prefix: object_store::path::Path,
    rt: tokio::runtime::Runtime,
    at: String,
    /// The deadline of a call that moves little data (see `deadline`).
    min: Duration,
}

/// A call's deadline: `min`, or as long as moving `bytes` takes at 4 MiB/s. A flat 30 s timed
/// out 190 MB checkpoints on a MinIO.
#[cfg(feature = "object-store")]
fn deadline(min: Duration, bytes: u64) -> Duration {
    min.max(Duration::from_secs(bytes / (4 << 20)))
}

/// The object store's settings: the `AWS_*` environment, as object_store names them, with
/// conditional creates set rather than left to object_store's default, and the client's own
/// timeout (30 s for any request, which would cut large uploads short) out of the way: each
/// call has a deadline of its own instead.
#[cfg(feature = "object-store")]
fn options(env: impl IntoIterator<Item = (String, String)>) -> Result<Vec<(String, String)>> {
    let aws = env.into_iter().filter(|(k, _)| k.starts_with("AWS_"));
    let mut opts: Vec<(String, String)> = aws.map(|(k, v)| (k.to_ascii_lowercase(), v)).collect();
    if let Some((_, v)) = opts.iter().find(|(k, v)| k == "aws_conditional_put" && v.trim() != "etag") {
        bail!("AWS_CONDITIONAL_PUT={v}: the lease between instances needs conditional creates; unset it or set etag");
    }
    opts.extend([("aws_conditional_put", "etag"), ("timeout", "1h")].map(|(k, v)| (k.to_string(), v.to_string())));
    Ok(opts)
}

#[cfg(feature = "object-store")]
impl Remote {
    pub fn open(location: &str, parts: &[&str]) -> Result<Remote> {
        let url = url::Url::parse(location).with_context(|| format!("--checkpoints {location}"))?;
        let opts = options(std::env::vars()).with_context(|| format!("--checkpoints {location}"))?;
        let (store, prefix) =
            object_store::parse_url_opts(&url, opts).with_context(|| format!("--checkpoints {location}"))?;
        let at = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
        Remote::new(store, prefix, parts, at, Duration::from_secs(30))?.checked()
    }

    /// `self`, once it proved that it refuses to create a name twice: a store that ignores
    /// `If-None-Match` would let two instances both claim an epoch.
    fn checked(self) -> Result<Remote> {
        check_exclusive(&self).with_context(|| {
            format!(
                "checking that {self} refuses to create an object that exists (S3's If-None-Match), which the \
                 lease between instances rests on (S3 and MinIO do)"
            )
        })?;
        Ok(self)
    }

    fn new(
        store: Box<dyn object_store::ObjectStore>,
        prefix: object_store::path::Path,
        parts: &[&str],
        at: String,
        min: Duration,
    ) -> Result<Remote> {
        let prefix = parts.iter().fold(prefix, |p, part| p.child(*part));
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build()?;
        Ok(Remote { store, at: format!("{at}/{prefix}"), prefix, rt, min })
    }

    /// Runs `call`, which moves about `bytes`, on the store's runtime: the store's answer, or an
    /// error once the deadline passed (the call is dropped).
    fn call<T>(&self, bytes: u64, call: impl std::future::Future<Output = T>) -> Result<T> {
        let limit = deadline(self.min, bytes);
        let timed = self.rt.block_on(async { tokio::time::timeout(limit, call).await });
        timed.map_err(|_| anyhow::anyhow!("no answer within {:.1}s", limit.as_secs_f32()))
    }
}

#[cfg(feature = "object-store")]
impl std::fmt::Display for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.at)
    }
}

#[cfg(feature = "object-store")]
impl Store for Remote {
    fn list(&self) -> Result<Vec<String>> {
        let listed = self.call(0, self.store.list_with_delimiter(Some(&self.prefix)));
        let objects = listed.and_then(|r| Ok(r?)).with_context(|| format!("listing {self}"))?.objects;
        // hidden names are not objects, as in a directory (a probe a crash left behind)
        let names = objects.into_iter().filter_map(|o| o.location.filename().map(str::to_string));
        Ok(names.filter(|n| !n.starts_with('.')).collect())
    }

    fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = self.prefix.child(name);
        let read = self.call(0, self.store.head(&path)).and_then(|head| {
            let size = head?.size;
            Ok(self.call(size, async { self.store.get(&path).await?.bytes().await })??)
        });
        // the body's own allocation when it is one buffer (a copy only when it is not)
        Ok(read.with_context(|| format!("reading {self}/{name}"))?.into())
    }

    fn create(&self, name: &str, bytes: &[u8]) -> Result<bool> {
        use object_store::{Error, PutMode, PutOptions, PutPayload};
        let create = PutOptions { mode: PutMode::Create, ..Default::default() };
        let path = self.prefix.child(name);
        let put = self.store.put_opts(&path, PutPayload::from(bytes.to_vec()), create);
        let what = || format!("creating {self}/{name}");
        match self.call(bytes.len() as u64, put).with_context(what)? {
            Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => Ok(false),
            r => r.map(|_| true).with_context(what),
        }
    }

    fn delete(&self, name: &str) -> Result<()> {
        let what = || format!("deleting {self}/{name}");
        match self.call(0, self.store.delete(&self.prefix.child(name))).with_context(what)? {
            Err(object_store::Error::NotFound { .. }) | Ok(()) => Ok(()),
            r => r.with_context(what),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Arc;

    /// A directory of its own, removed afterwards.
    pub struct Scratch(pub PathBuf);

    impl Scratch {
        pub fn new(name: &str) -> Scratch {
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("brrrrr-store-{name}-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            Scratch(dir)
        }

        pub fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    /// Every kind of store this build has, each empty: a directory, and `memory:///`.
    fn stores(name: &str) -> Vec<(Box<dyn Store>, Option<Scratch>)> {
        let dir = Scratch::new(name);
        let mut all: Vec<(Box<dyn Store>, Option<Scratch>)> = vec![(open(dir.path(), &["p", "k"]).unwrap(), Some(dir))];
        if cfg!(feature = "object-store") {
            all.push((open("memory:///", &["p", "k"]).unwrap(), None));
        }
        all
    }

    #[test]
    fn a_created_object_is_listed_and_read_back() {
        for (s, _dir) in stores("roundtrip") {
            assert_eq!(s.list().unwrap(), Vec::<String>::new(), "{s}");
            assert!(s.create("00000000000000000001.ckpt", b"one").unwrap(), "{s}");
            assert!(s.create("00000000000000000001.released", b"").unwrap(), "{s}");
            assert_eq!(
                sorted(s.list().unwrap()),
                ["00000000000000000001.ckpt", "00000000000000000001.released"],
                "{s}"
            );
            assert_eq!(s.get("00000000000000000001.ckpt").unwrap(), b"one", "{s}");
            assert_eq!(s.get("00000000000000000001.released").unwrap(), b"", "{s}");
        }
    }

    /// The lease: creating what exists fails, and leaves the existing object as it was.
    #[test]
    fn creating_an_existing_object_fails_and_keeps_it() {
        for (s, _dir) in stores("exists") {
            assert!(s.create("a", b"first").unwrap(), "{s}");
            assert!(!s.create("a", b"second").unwrap(), "{s}");
            assert_eq!(s.get("a").unwrap(), b"first", "{s}");
            assert_eq!(s.list().unwrap(), ["a"], "{s}");
        }
    }

    #[test]
    fn a_deleted_object_is_gone_and_deleting_a_missing_one_is_fine() {
        for (s, _dir) in stores("delete") {
            s.create("a", b"x").unwrap();
            s.create("b", b"y").unwrap();
            s.delete("a").unwrap();
            assert_eq!(s.list().unwrap(), ["b"], "{s}");
            s.delete("a").unwrap();
            s.delete("never").unwrap();
            assert!(s.get("a").is_err(), "{s}");
            assert!(s.create("a", b"again").unwrap(), "{s}: a deleted name can be created anew");
            assert_eq!(s.get("a").unwrap(), b"again");
        }
    }

    #[test]
    fn reading_a_missing_object_says_which() {
        for (s, _dir) in stores("missing") {
            let e = format!("{:#}", s.get("00000000000000000007.ckpt").unwrap_err());
            assert!(e.contains("00000000000000000007.ckpt"), "{e}");
        }
    }

    /// Stores under other parts (another pipeline, another SQL) do not see each other's objects.
    #[test]
    fn stores_under_other_parts_are_apart() {
        let dir = Scratch::new("apart");
        let mut pairs: Vec<(Box<dyn Store>, Box<dyn Store>)> =
            vec![(open(dir.path(), &["p", "k1"]).unwrap(), open(dir.path(), &["p", "k2"]).unwrap())];
        if cfg!(feature = "object-store") {
            // one memory store per open: apart anyway, but the prefix must not leak either
            pairs.push((open("memory:///", &["p", "k1"]).unwrap(), open("memory:///", &["q"]).unwrap()));
        }
        for (a, b) in pairs {
            a.create("x", b"a").unwrap();
            assert!(b.list().unwrap().is_empty(), "{b}");
            assert!(b.create("x", b"b").unwrap(), "{b}");
            assert_eq!(a.get("x").unwrap(), b"a");
            assert_eq!(b.get("x").unwrap(), b"b");
        }
        // a store deeper in the tree lists only its own level
        let parent = open(dir.path(), &["p"]).unwrap();
        assert!(parent.list().unwrap().is_empty(), "directories are not objects");
    }

    /// Of many instances creating one epoch at once, exactly one wins, and the object is whole.
    #[test]
    fn of_concurrent_creates_of_one_name_exactly_one_wins() {
        for round in 0..20 {
            let dir = Scratch::new("race");
            let stores: Vec<Arc<dyn Store>> =
                (0..8).map(|_| Arc::from(open(dir.path(), &["p", "k"]).unwrap())).collect();
            let won: Vec<(usize, bool)> = std::thread::scope(|sc| {
                let hs: Vec<_> = stores
                    .iter()
                    .enumerate()
                    .map(|(i, s)| sc.spawn(move || (i, s.create("claim", &vec![i as u8; 100_000]).unwrap())))
                    .collect();
                hs.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let winners: Vec<usize> = won.iter().filter(|w| w.1).map(|w| w.0).collect();
            assert_eq!(winners.len(), 1, "round {round}: {won:?}");
            assert_eq!(stores[0].get("claim").unwrap(), vec![winners[0] as u8; 100_000], "round {round}");
            assert_eq!(stores[0].list().unwrap(), ["claim"], "round {round}: no temporary file is left");
        }
    }

    /// A directory takes plain paths (a volume's mount) and file:// URLs alike, and nests the
    /// parts under it.
    #[test]
    fn a_directory_is_a_path_or_a_file_url() {
        let dir = Scratch::new("forms");
        let plain = open(dir.path(), &["pipe@shadow.", "00ff"]).unwrap();
        let url = open(&format!("file://{}", dir.path()), &["pipe@shadow.", "00ff"]).unwrap();
        assert_eq!(plain.to_string(), format!("{}/pipe@shadow./00ff", dir.path()));
        assert_eq!(url.to_string(), plain.to_string());
        plain.create("x", b"1").unwrap();
        assert_eq!(url.get("x").unwrap(), b"1");
        assert_eq!(std::fs::read(dir.0.join("pipe@shadow.").join("00ff").join("x")).unwrap(), b"1");
    }

    #[test]
    fn opening_a_directory_creates_it() {
        let dir = Scratch::new("create");
        open(dir.path(), &["a", "b"]).unwrap();
        assert!(dir.0.join("a").join("b").is_dir());
    }

    #[test]
    fn an_empty_directory_is_refused() {
        for location in ["", "file://"] {
            let e = open(location, &["p"]).err().unwrap().to_string();
            assert!(e.contains("empty directory"), "{location}: {e}");
        }
    }

    /// Fails at start, saying how to fix it on Kubernetes, rather than at the first checkpoint.
    #[test]
    fn a_directory_that_cannot_be_written_is_refused_at_open() {
        let dir = Scratch::new("unwritable");
        std::fs::write(&dir.0, b"a file, not a directory").unwrap();
        let e = format!("{:#}", open(dir.path(), &["p"]).err().unwrap());
        assert!(e.contains("is not writable") && e.contains("fsGroup") && e.contains(dir.path()), "{e}");
        std::fs::remove_file(&dir.0).unwrap();
    }

    #[test]
    fn opening_leaves_no_file_behind() {
        let dir = Scratch::new("probe");
        open(dir.path(), &[]).unwrap();
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0);
    }

    /// A file system that cannot create a file only if its name is free (CIFS/SMB, some FUSE
    /// drivers and 9p refuse hard links) fails at start, saying why, rather than at the claim
    /// after recovery, which would then be tried again for ever.
    #[test]
    fn a_directory_whose_file_system_cannot_link_is_refused_at_open() {
        let cases: [(Link, &str); 3] = [
            (|_, _| Err(ErrorKind::Unsupported.into()), "creating"),
            // a link over an existing name: not create-if-absent
            (|from, to| std::fs::copy(from, to).map(drop), "a second time, over the first"),
            (|_, _| Err(ErrorKind::AlreadyExists.into()), "a new name, was found taken"),
        ];
        for (link, cause) in cases {
            let dir = Scratch::new("nolink");
            let e = format!("{:#}", Dir::with_link(dir.0.clone(), link).err().unwrap());
            assert!(
                e.contains(&format!(
                    "checkpoint directory {} cannot create a file only if its name is free",
                    dir.path()
                )) && e.contains("hard link")
                    && e.contains("Azure Files")
                    && e.contains(cause)
                    && !e.contains("fsGroup"),
                "{e}"
            );
            assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0, "{cause}: nothing is left behind");
        }
    }

    /// A probe a crash cut short is never listed, and is tidied away like any temporary file.
    #[test]
    fn a_probe_left_behind_is_not_listed_and_is_tidied() {
        let dir = Scratch::new("probecrash");
        // the link is made, then the process "dies" before removing it
        let crash: Link = |from, to| std::fs::hard_link(from, to).and(Err(ErrorKind::Interrupted.into()));
        assert!(Dir::with_link(dir.0.clone(), crash).is_err());
        let left: Vec<_> = std::fs::read_dir(&dir.0).unwrap().map(|e| e.unwrap().path()).collect();
        assert_eq!(left.len(), 1, "{left:?}");
        let s = open(dir.path(), &[]).unwrap();
        assert_eq!(s.list().unwrap(), Vec::<String>::new());
        s.tidy();
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 1, "a recent one may be another's probe");
        let old = SystemTime::now() - STALE - Duration::from_secs(5);
        std::fs::File::options().write(true).open(&left[0]).unwrap().set_modified(old).unwrap();
        s.tidy();
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0);
    }

    /// Hidden names are not objects, in any store: a probe a crash left is never listed.
    #[test]
    fn hidden_names_are_not_listed() {
        for (s, _dir) in stores("hiddenall") {
            assert!(s.create(".probe-1-2-3.tmp", b"").unwrap(), "{s}");
            assert!(s.create("a", b"").unwrap(), "{s}");
            assert_eq!(s.list().unwrap(), ["a"], "{s}");
        }
    }

    /// A pod restarted on the same volume finds what the last one wrote.
    #[test]
    fn objects_outlive_the_store_that_wrote_them() {
        let dir = Scratch::new("reopen");
        open(dir.path(), &["p"]).unwrap().create("00000000000000000003.ckpt", b"state").unwrap();
        let again = open(dir.path(), &["p"]).unwrap();
        assert_eq!(again.list().unwrap(), ["00000000000000000003.ckpt"]);
        assert_eq!(again.get("00000000000000000003.ckpt").unwrap(), b"state");
    }

    /// Hidden files (a write in progress, or one a crash cut short) and subdirectories are not
    /// objects: a half-written checkpoint is never listed, so never restored.
    #[test]
    fn a_directory_lists_only_its_files_that_are_not_hidden() {
        let dir = Scratch::new("hidden");
        let s = open(dir.path(), &[]).unwrap();
        std::fs::write(dir.0.join(".1-2-3.tmp"), b"half a checkpoint").unwrap();
        std::fs::write(dir.0.join(".other"), b"").unwrap();
        std::fs::create_dir(dir.0.join("00000000000000000009.ckpt")).unwrap();
        s.create("00000000000000000001.ckpt", b"x").unwrap();
        assert_eq!(s.list().unwrap(), ["00000000000000000001.ckpt"]);
    }

    /// The volume emptied under a running instance (`rm -rf`, a restored snapshot): listing
    /// finds nothing, and the next checkpoint recreates the directory.
    #[test]
    fn a_directory_removed_while_open_lists_empty_and_is_recreated() {
        let dir = Scratch::new("removed");
        let s = open(dir.path(), &["p", "k"]).unwrap();
        s.create("a", b"1").unwrap();
        std::fs::remove_dir_all(&dir.0).unwrap();
        assert_eq!(s.list().unwrap(), Vec::<String>::new());
        s.delete("a").unwrap();
        assert!(s.create("a", b"2").unwrap());
        assert_eq!(s.get("a").unwrap(), b"2");
    }

    #[test]
    fn listing_a_directory_that_cannot_be_read_is_an_error() {
        let dir = Scratch::new("notdir");
        let s = open(dir.path(), &["k"]).unwrap();
        std::fs::remove_dir_all(&dir.0).unwrap();
        std::fs::write(&dir.0, b"").unwrap(); // the parent is now a file
        let e = format!("{:#}", s.list().unwrap_err());
        assert!(e.contains("listing") && e.contains(dir.path()), "{e}");
        let e = format!("{:#}", s.create("a", b"").unwrap_err());
        assert!(e.contains("writing") && e.contains("/k/a"), "{e}");
        std::fs::remove_file(&dir.0).unwrap();
    }

    /// Only an existing name means "taken": any other failure to create is an error, never
    /// read as another instance holding the pipeline.
    #[test]
    fn a_create_that_fails_otherwise_is_an_error_not_a_taken_name() {
        let dir = Scratch::new("linkfail");
        let s = open(dir.path(), &[]).unwrap();
        let e = format!("{:#}", s.create("no-such-dir/a", b"x").unwrap_err());
        assert!(e.contains("creating") && e.contains("no-such-dir/a"), "{e}");
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0, "the temporary file is gone");
    }

    #[test]
    fn deleting_what_cannot_be_deleted_is_an_error() {
        let dir = Scratch::new("undeletable");
        let s = open(dir.path(), &[]).unwrap();
        std::fs::create_dir(dir.0.join("sub")).unwrap(); // remove_file refuses a directory
        let e = format!("{:#}", s.delete("sub").unwrap_err());
        assert!(e.contains("deleting") && e.contains("sub"), "{e}");
    }

    /// `tidy` removes a crashed write's temporary files once they are old, and nothing else.
    #[test]
    fn tidying_removes_only_stale_temporary_files() {
        let dir = Scratch::new("tidy");
        let s = open(dir.path(), &[]).unwrap();
        let old = SystemTime::now() - STALE - Duration::from_secs(5);
        let recent = SystemTime::now() - STALE + Duration::from_secs(60);
        let file = |name: &str, at: SystemTime| {
            std::fs::write(dir.0.join(name), b"x").unwrap();
            std::fs::File::options().write(true).open(dir.0.join(name)).unwrap().set_modified(at).unwrap();
        };
        file(".1-1-1.tmp", old); // a crashed write
        file(".1-1-2.tmp", recent); // a write that may still be going on
        file(".keep", old); // hidden, but no temporary file of ours
        file("old.tmp", old); // an object, whatever its name
        file("00000000000000000001.ckpt", old);
        s.tidy();
        let mut left: Vec<String> =
            std::fs::read_dir(&dir.0).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        left.sort();
        assert_eq!(left, [".1-1-2.tmp", ".keep", "00000000000000000001.ckpt", "old.tmp"]);
        // a directory gone is nothing to tidy
        std::fs::remove_dir_all(&dir.0).unwrap();
        s.tidy();
    }

    /// A created file is complete on disk, and so is its name: synced before it is linked, the
    /// directory synced after (not observable here, but the file is there under its name).
    #[test]
    fn a_created_file_holds_exactly_its_bytes() {
        let dir = Scratch::new("bytes");
        let s = open(dir.path(), &[]).unwrap();
        let big: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        assert!(s.create("big", &big).unwrap());
        assert_eq!(std::fs::read(dir.0.join("big")).unwrap(), big);
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 1, "the temporary file is gone");
    }

    #[cfg(feature = "object-store")]
    #[test]
    fn object_store_urls_open_a_remote_store() {
        let s = open("memory:///", &["pipe", "00ff"]).unwrap();
        assert_eq!(s.to_string(), "memory:///pipe/00ff");
        let e = format!("{:#}", open("nope://bucket/x", &["p"]).err().unwrap());
        assert!(e.contains("--checkpoints nope://bucket/x"), "{e}");
    }

    /// An object store that ignores `PutMode::Create`, as an S3 server that ignores
    /// `If-None-Match` does: every create overwrites.
    #[cfg(feature = "object-store")]
    #[derive(Debug)]
    struct Careless(object_store::memory::InMemory);

    #[cfg(feature = "object-store")]
    impl std::fmt::Display for Careless {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("careless")
        }
    }

    #[cfg(feature = "object-store")]
    #[async_trait::async_trait]
    impl object_store::ObjectStore for Careless {
        async fn put_opts(
            &self,
            at: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let opts = object_store::PutOptions { mode: object_store::PutMode::Overwrite, ..opts };
            self.0.put_opts(at, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            at: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(at, opts).await
        }
        async fn get_opts(
            &self,
            at: &object_store::path::Path,
            opts: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(at, opts).await
        }
        async fn delete(&self, at: &object_store::path::Path) -> object_store::Result<()> {
            self.0.delete(at).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.0.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.0.copy(from, to).await
        }
        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.0.copy_if_not_exists(from, to).await
        }
    }

    /// An object store that does not refuse to create what exists would let two instances
    /// both claim an epoch: it is refused at open, and one that refuses is taken.
    #[cfg(feature = "object-store")]
    #[test]
    fn an_object_store_that_ignores_create_if_absent_is_refused_at_open() {
        use object_store::memory::InMemory;
        let remote = |store: Box<dyn object_store::ObjectStore>| {
            Remote::new(store, "p".into(), &["k"], "test://".into(), Duration::from_secs(30)).unwrap()
        };
        let e = format!("{:#}", remote(Box::new(Careless(InMemory::new()))).checked().err().unwrap());
        assert!(
            e.contains("checking that test:///p/k refuses to create an object that exists (S3's If-None-Match)")
                && e.contains("a second time, over the first"),
            "{e}"
        );
        let s = remote(Box::new(InMemory::new())).checked().unwrap();
        assert_eq!(s.list().unwrap(), Vec::<String>::new());
        assert!(s.create("a", b"").unwrap() && !s.create("a", b"").unwrap());
    }

    /// Conditional creates are set explicitly (not left to object_store's default); a setting
    /// that turns them off is refused rather than overridden in silence.
    #[cfg(feature = "object-store")]
    #[test]
    fn the_object_store_settings_keep_conditional_creates() {
        let env = |vars: &[(&str, &str)]| vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>();
        let opts = options(env(&[("AWS_REGION", "auto"), ("HOME", "/root")])).unwrap();
        let want = env(&[("aws_region", "auto"), ("aws_conditional_put", "etag"), ("timeout", "1h")]);
        assert_eq!(opts, want);
        assert!(options(env(&[("AWS_CONDITIONAL_PUT", " etag ")])).is_ok());
        for off in ["disabled", "dynamo:locks"] {
            let e = options(env(&[("AWS_CONDITIONAL_PUT", off)])).unwrap_err().to_string();
            assert!(e.contains(&format!("AWS_CONDITIONAL_PUT={off}")) && e.contains("unset it or set etag"), "{e}");
        }
    }

    /// A call that hangs fails at its deadline instead of stalling the data thread: a
    /// checkpoint write that fails this way is tried again (run.rs), never mistaken for a
    /// taken epoch.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_call_that_hangs_fails_at_its_deadline() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let hang = Duration::from_secs(3600);
        let config = ThrottleConfig {
            wait_put_per_call: hang,
            wait_get_per_call: hang,
            wait_list_with_delimiter_per_call: hang,
            wait_delete_per_call: hang,
            ..Default::default()
        };
        let inner = object_store::memory::InMemory::new();
        let throttled = ThrottledStore::new(inner, config);
        let s =
            Remote::new(Box::new(throttled), "p".into(), &["k"], "hang://".into(), Duration::from_millis(300)).unwrap();
        let t0 = std::time::Instant::now();
        let e = format!("{:#}", s.create("00000000000000000001.ckpt", b"x").unwrap_err());
        assert!(
            e.contains("creating hang:///p/k/00000000000000000001.ckpt") && e.contains("no answer within 0.3s"),
            "{e}"
        );
        assert!(s.list().is_err() && s.delete("a").is_err() && s.get("a").is_err());
        assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
    }

    /// The deadline grows with what a call moves: a flat 30 s cut 190 MB checkpoints short.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_calls_deadline_is_its_minimum_or_4_mib_per_second() {
        let min = Duration::from_secs(30);
        assert_eq!(deadline(min, 0), min);
        assert_eq!(deadline(min, 120 << 20), min);
        assert_eq!(deadline(min, (124 << 20) - 1), min);
        assert_eq!(deadline(min, 124 << 20), Duration::from_secs(31));
        assert_eq!(deadline(min, 400 << 20), Duration::from_secs(100));
    }

    /// A store in the S3 at `BRRRRR_IT_S3` (bucket `checkpoints`; CI runs adobe/s3mock), under a
    /// prefix of its own, set up and checked as `open` does.
    #[cfg(feature = "object-store")]
    fn s3(min: Duration) -> Remote {
        static N: AtomicU64 = AtomicU64::new(0);
        let endpoint = std::env::var("BRRRRR_IT_S3")
            .expect("BRRRRR_IT_S3: an S3 endpoint with a bucket `checkpoints`, e.g. adobe/s3mock");
        let env = [
            ("AWS_ENDPOINT", endpoint.as_str()),
            ("AWS_ALLOW_HTTP", "true"),
            ("AWS_ACCESS_KEY_ID", "k"),
            ("AWS_SECRET_ACCESS_KEY", "s"),
            ("AWS_REGION", "us-east-1"),
        ];
        let opts = options(env.map(|(k, v)| (k.to_string(), v.to_string()))).unwrap();
        let url = url::Url::parse("s3://checkpoints/it").unwrap();
        let (store, prefix) = object_store::parse_url_opts(&url, opts).unwrap();
        let unique = format!("{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed));
        Remote::new(store, prefix, &[&unique], endpoint, min).unwrap().checked().unwrap()
    }

    /// The contract every store keeps, on an S3 server: conditional creates, listing under a
    /// prefix, reads (by their size's deadline) and deletes. Not one winner among concurrent
    /// creates: that is the server's to guarantee (S3 and MinIO do; s3mock, which CI runs,
    /// lets several of 8 simultaneous creates of one name succeed).
    #[cfg(feature = "object-store")]
    #[test]
    #[ignore = "needs an S3 endpoint in BRRRRR_IT_S3"]
    fn the_store_contract_holds_against_s3() {
        let s = s3(Duration::from_secs(30));
        assert!(s.list().unwrap().is_empty());
        assert!(s.create("00000000000000000001.ckpt", b"one").unwrap());
        assert!(!s.create("00000000000000000001.ckpt", b"other").unwrap(), "{s}: S3 must honour If-None-Match");
        assert!(s.create("00000000000000000001.released", b"").unwrap());
        assert_eq!(sorted(s.list().unwrap()), ["00000000000000000001.ckpt", "00000000000000000001.released"]);
        assert_eq!(s.get("00000000000000000001.ckpt").unwrap(), b"one");
        let big: Vec<u8> = (0..5_000_000u32).map(|i| (i % 251) as u8).collect();
        assert!(s.create("big", &big).unwrap());
        assert_eq!(s.get("big").unwrap(), big);
        s.delete("big").unwrap();
        s.delete("never").unwrap();
        assert!(s.get("big").is_err());
    }

    /// The data thread checkpoints while the collector lists and deletes
    /// on its own thread, their calls sharing pooled connections. Every checkpoint must be
    /// written well within the deadline (2 s here), however the connections mix. With a
    /// current-thread runtime per thread this failed at about the fifth checkpoint.
    #[cfg(feature = "object-store")]
    #[test]
    #[ignore = "needs an S3 endpoint in BRRRRR_IT_S3"]
    fn the_data_thread_and_the_collector_share_connections_against_s3() {
        let s: Arc<dyn Store> = Arc::new(s3(Duration::from_secs(2)));
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
        let collector = {
            let s = s.clone();
            std::thread::spawn(move || {
                while rx.recv().is_ok() {
                    let mut names = s.list().unwrap();
                    names.sort();
                    let old = names.len().saturating_sub(5);
                    names[..old].iter().for_each(|n| s.delete(n).unwrap());
                }
            })
        };
        for epoch in 1..=40u64 {
            let name = format!("{epoch:020}.ckpt");
            assert!(s.create(&name, &[1; 1000]).unwrap_or_else(|e| panic!("checkpoint {epoch}: {e:#}")));
            let _ = tx.try_send(());
            std::thread::sleep(Duration::from_millis(100));
        }
        drop(tx);
        collector.join().unwrap();
        assert_eq!(s.list().unwrap().len(), 5);
    }

    #[cfg(not(feature = "object-store"))]
    #[test]
    fn without_the_feature_object_store_urls_are_refused() {
        for url in ["s3://bucket/prefix", "memory:///"] {
            let e = open(url, &["p"]).err().unwrap().to_string();
            assert!(e.contains("without the object-store feature") && e.contains(url), "{e}");
        }
    }

    #[test]
    fn the_default_is_under_the_volume_mount() {
        assert_eq!(DEFAULT, "/var/lib/brrrrr/checkpoints");
    }
}
