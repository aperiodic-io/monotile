//! The checkpoint format: what `brrrrr run` writes to the object store every interval and
//! restores on start (ADR-0008).
//!
//! A checkpoint is a fixed header (`MAGIC`, `VERSION` as u16 LE, `LAYOUT` as u64 LE), then one
//! zstd frame of the postcard encoding of a `Checkpoint`, and nothing after either. The header is
//! checked before anything else is decoded, so a checkpoint of another format is refused for what
//! it is. This version is read, and the older versions in `READS`, each under its own layout:
//! older checkpoints are refused (ADR-0008: a deploy of a new version starts every pipeline
//! fresh, unless the new version only appended to the old one's layout).
//!
//! Postcard writes no field or variant names: bytes of one layout decode "successfully" into
//! another whenever the shapes happen to line up (two fields of one type swapped, a variant
//! inserted before others of the same shape, a trailing field removed). Hence the rules,
//! enforced by `tests/checkpoint.rs`:
//! - Any change to what is serialized (a field or variant added, removed or reordered in
//!   `Checkpoint`, `engine::State` or anything in it: `OpState`, `agg::Acc`, `value::Value`, ...)
//!   changes the traced layout, which no longer matches `tests/checkpoint-layout.json`.
//!   Renaming alone changes nothing on the wire and is free.
//! - Such a change bumps `VERSION`, updates `LAYOUT` and replaces the committed checkpoints
//!   under `fixtures/checkpoints/v<VERSION>/`. Where it only appends (variants after an enum's
//!   last, types only they reach), the previous version stays readable: it keeps its
//!   checkpoints and traced layout (`tests/checkpoint-layout-v<version>.json`) and is listed in
//!   `READS`. Any other change drops every older version from `READS`.
//! - A change in how a plan keeps its state (its operators, the order of a window's
//!   aggregates) changes `Engine::fingerprint`, and those checkpoints are refused at restore.
use crate::engine::{Engine, State, StateRef};
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use zstd::stream::write::Encoder;

/// The first bytes of every checkpoint.
pub const MAGIC: [u8; 4] = *b"BRRR";
/// The format version this build writes.
pub const VERSION: u16 = 10;
/// `fnv64` of `tests/checkpoint-layout.json`, the traced layout of this version.
pub const LAYOUT: u64 = 0x38c2_fd38_91ea_215f;
/// The older versions this build also reads, each under the one layout its build traced
/// (`tests/checkpoint-layout-v<version>.json`): those this version only appended to (variants
/// after an enum's last, a field after a struct's last that they are decoded without), so their
/// bytes decode into this build's types. `tests/checkpoint.rs` proves the appends
/// (`this_version_only_appends_to_the_layouts_it_reads`) and restores the committed checkpoints
/// of each. Anything older is stale (`stale_version`).
pub const READS: [(u16, u64); 1] = [
    // format 9, which this one appended count(DISTINCT), exact quantiles, gap fill and lead to.
    // Remove once every pipeline has checkpointed at 10 (its first checkpoint after the deploy).
    (9, 0xed67_2738_89af_ffd0),
];
const HEADER: usize = 4 + 2 + 8;
/// zstd's default: a 190 MB state compresses in about a second and to a tenth or less.
const ZSTD_LEVEL: i32 = 3;

/// One checkpoint: the engine state with the positions it belongs to. One object holds it
/// all, so a checkpoint exists iff it is complete.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub epoch: u64,
    /// `Engine::fingerprint` of the plan whose state this is.
    pub plan: u64,
    /// Next offset to read per source topic-partition.
    pub sources: Vec<(String, i32, i64)>,
    /// End offset per sink topic-partition once everything before the checkpoint was acked.
    pub sinks: Vec<(String, i32, i64)>,
    pub state: State,
    /// What the run that wrote it withholds, since a start without a checkpoint. `None`: none
    /// recorded (`Checkpoint::of`).
    pub withhold: Option<Withhold>,
}

/// What a start without a checkpoint cannot write whole or once (µs), which every checkpoint
/// after it carries: the windows that began before `start` (partial: its sources had lost
/// older records), and per sink topic those that ended before its bound (it had lost the
/// records that would say whether they were written). A source in `awaiting` had no record
/// yet: its first raises `start` to its time, and a restart from a checkpoint taken before
/// that reads that record again.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Withhold {
    pub start: i64,
    pub topics: BTreeMap<String, i64>,
    pub awaiting: Vec<String>,
}

impl Withhold {
    pub fn none() -> Withhold {
        Withhold { start: i64::MIN, topics: BTreeMap::new(), awaiting: vec![] }
    }

    pub fn is_none(&self) -> bool {
        *self == Withhold::none()
    }
}

impl std::fmt::Display for Withhold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let at = |us: i64| us / 1_000_000;
        let mut parts = vec![];
        if self.start != i64::MIN {
            parts.push(format!("the windows that began before {} (unix s, partial)", at(self.start)));
        }
        for (t, us) in &self.topics {
            parts.push(format!("on {t}, the windows closed before {} (unix s, which it no longer holds)", at(*us)));
        }
        if !self.awaiting.is_empty() {
            parts.push(format!("the windows that began before the first record of {}", self.awaiting.join(", ")));
        }
        write!(f, "{}", parts.join("; "))
    }
}

impl Checkpoint {
    pub fn of(engine: &Engine, epoch: u64, sources: Vec<(String, i32, i64)>, sinks: Vec<(String, i32, i64)>) -> Self {
        Checkpoint { epoch, plan: engine.fingerprint(), sources, sinks, state: engine.snapshot(), withhold: None }
    }

    /// # Panics
    ///
    /// If the checkpoint would inflate past `MAX_INFLATED`, which `decode` refuses (the runtime
    /// writes with `try_encode` and `encode_of`, which return that as an error).
    pub fn encode(&self) -> Vec<u8> {
        self.try_encode().unwrap_or_else(|e| panic!("{e}"))
    }

    /// `encode`, with a state that would inflate past `MAX_INFLATED` an error: written, it could
    /// never be read back. The runtime encodes the copy of the state it took at a checkpoint's
    /// cut this way, off the data thread.
    pub fn try_encode(&self) -> Result<Vec<u8>, String> {
        encode(self, MAX_INFLATED)
    }

    /// `Checkpoint::of(engine, ..).encode()`, serialized from the engine's live state: neither
    /// a copy of the state nor its uncompressed encoding is ever in memory, only the compressed
    /// checkpoint. The two took about twice the state on top of it, the pods' high-water mark.
    /// A state that would inflate past `MAX_INFLATED` is an error: written, it could never be
    /// read back.
    pub fn encode_of(
        engine: &Engine,
        epoch: u64,
        sources: &[(String, i32, i64)],
        sinks: &[(String, i32, i64)],
        withhold: Option<&Withhold>,
    ) -> Result<Vec<u8>, String> {
        let (plan, state) = (engine.fingerprint(), engine.state_ref());
        let c = CheckpointRef { epoch, plan, sources, sinks, state, withhold };
        encode(&c, MAX_INFLATED)
    }

    /// The version of a checkpoint this build will never read, because it is older than every
    /// version this build reads (its own and `READS`): versions only move forward, so it will
    /// not restore under this build or a later one either, and pruning it is safe. `None` for
    /// anything else -- not a checkpoint, a version this build reads, or (a downgrade) a newer
    /// one, which is a human's call, not an automatic one (`restore` in `run.rs`).
    pub fn stale_version(bytes: &[u8]) -> Option<u16> {
        let ours = bytes.len() >= HEADER && bytes[..4] == MAGIC;
        let version = ours.then(|| u16::from_le_bytes([bytes[4], bytes[5]]))?;
        (version < oldest_read()).then_some(version)
    }

    pub fn decode(bytes: &[u8]) -> Result<Checkpoint, String> {
        decode(bytes, MAX_INFLATED)
    }

    /// Restores the state into `engine`, which must have the plan the state was taken of.
    pub fn restore(self, engine: &mut Engine) -> Result<(), String> {
        let plan = engine.fingerprint();
        if self.plan != plan {
            return Err(format!(
                "the state is of plan {:016x}, this build plans the SQL as {plan:016x} \
                 (a planner change: start the pipeline fresh)",
                self.plan
            ));
        }
        engine.restore(self.state)
    }
}

/// `Checkpoint::decode`, inflating the body to at most `most` bytes.
fn decode(bytes: &[u8], most: u64) -> Result<Checkpoint, String> {
    if bytes.len() < HEADER || bytes[..4] != MAGIC {
        return Err("not a brrrrr checkpoint".into());
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    let Some(want) = layout_of(version) else {
        return Err(format!("checkpoint format version {version}; this build reads versions {}", readable()));
    };
    let layout = u64::from_le_bytes(bytes[6..HEADER].try_into().expect("8 bytes"));
    if layout != want {
        return Err(format!(
            "checkpoint layout {layout:016x}, this build's for version {version} is {want:016x}: \
             one of the two builds changed the format without bumping its version"
        ));
    }
    let body = &bytes[HEADER..];
    let frame = zstd::zstd_safe::find_frame_compressed_size(body).map_err(|_| "a truncated zstd frame")?;
    if frame < body.len() {
        return Err(format!("{} bytes after the checkpoint", body.len() - frame));
    }
    // decoded as it is inflated: the inflated body is about as large as the state it holds
    let inflate = zstd::stream::Decoder::with_buffer(body).map_err(|e| e.to_string())?.single_frame();
    let body = AtMost { r: inflate, left: most, most };
    take(body)
}

/// The most a checkpoint's body inflates to: 4 GiB. zstd inflates a crafted frame some 4·10⁴
/// times, so a crafted checkpoint of 1 MB could make the decoder read 40 GB (and hold what it
/// decodes). A slippage pipeline's state over hundreds of symbols, with a reservoir per
/// median, has reached about 200 MB; the bound is twenty times that.
const MAX_INFLATED: u64 = 4 << 30;

/// A reader of at most `most` bytes, `left` of them still to come: one more is an error, not the
/// end of the input.
struct AtMost<R> {
    r: R,
    left: u64,
    most: u64,
}

impl<R: Read> Read for AtMost<R> {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        let n = self.r.read(b)?;
        let most = self.most;
        self.left = (self.left.checked_sub(n as u64))
            .ok_or_else(|| std::io::Error::other(format!("the checkpoint inflates to more than {most} bytes")))?;
        Ok(n)
    }
}

/// The layout this build reads `version` under: its own, or an older version's (`READS`).
fn layout_of(version: u16) -> Option<u64> {
    if version == VERSION {
        return Some(LAYOUT);
    }
    READS.iter().find(|r| r.0 == version).map(|r| r.1)
}

/// The oldest version this build reads: anything before it is stale.
fn oldest_read() -> u16 {
    READS.iter().map(|r| r.0).fold(VERSION, u16::min)
}

/// The versions this build reads, oldest first: "5 and 6".
pub fn readable() -> String {
    let mut v: Vec<String> = READS.iter().map(|r| r.0.to_string()).collect();
    let last = VERSION.to_string();
    if v.is_empty() {
        return last;
    }
    v.push(last);
    let last = v.pop().expect("two or more");
    format!("{} and {last}", v.join(", "))
}

/// A `Checkpoint` borrowed: the same bytes.
#[derive(Serialize)]
#[serde(rename = "Checkpoint")]
struct CheckpointRef<'a> {
    epoch: u64,
    plan: u64,
    sources: &'a [(String, i32, i64)],
    sinks: &'a [(String, i32, i64)],
    state: StateRef<'a>,
    withhold: Option<&'a Withhold>,
}

/// The header, then the zstd frame of the postcard encoding of `c`, streamed through the
/// compressor. The size is counted first (a serialization without output, ~1% of the encode)
/// and pledged: the frame then records its content size, and zstd sizes its window to it. An
/// encoding longer than `most`, the most `decode` inflates, is refused before anything is
/// compressed.
fn encode<T: Serialize>(c: &T, most: u64) -> Result<Vec<u8>, String> {
    let size = postcard::serialize_with_flavor(c, postcard::ser_flavors::Size::default())
        .expect("postcard serializes every checkpoint type");
    if size as u64 > most {
        return Err(format!("the state encodes to {size} bytes, more than the {most} a checkpoint may inflate to"));
    }
    let mut out = Vec::with_capacity(HEADER);
    out.extend(MAGIC);
    out.extend(VERSION.to_le_bytes());
    out.extend(LAYOUT.to_le_bytes());
    let mut z = Encoder::new(Out(out), ZSTD_LEVEL).expect("a zstd context");
    z.set_pledged_src_size(Some(size as u64)).expect("before any input");
    let zstd = Zstd { buf: Vec::with_capacity(CHUNK), z };
    let z = postcard::serialize_with_flavor(c, zstd).expect("postcard serializes every checkpoint type");
    Ok(z.finish().expect("zstd compresses any bytes").0)
}

/// The checkpoint's bytes, grown by an eighth at a time rather than doubled: growing copies,
/// so the peak is about twice the checkpoint, where doubling could make it three times.
struct Out(Vec<u8>);

impl Write for Out {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.0.capacity() - self.0.len() < b.len() {
            self.0.reserve_exact(b.len().max(self.0.len() / 8).max(1 << 20));
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Bytes handed to the compressor at a time.
const CHUNK: usize = 64 * 1024;

/// A postcard flavor that hands what it is given to `z` (the compressor) in pieces of at most
/// `CHUNK`, filled before they go; only a value longer than that goes on its own.
struct Zstd<W> {
    buf: Vec<u8>,
    z: W,
}

impl<W: Write> Zstd<W> {
    fn flush(&mut self) -> postcard::Result<()> {
        self.z.write_all(&self.buf).map_err(|_| postcard::Error::SerializeBufferFull)?;
        self.buf.clear();
        Ok(())
    }
}

impl<W: Write> postcard::ser_flavors::Flavor for Zstd<W> {
    type Output = W;

    fn try_push(&mut self, b: u8) -> postcard::Result<()> {
        if self.buf.len() == CHUNK {
            self.flush()?;
        }
        self.buf.push(b);
        Ok(())
    }

    fn try_extend(&mut self, b: &[u8]) -> postcard::Result<()> {
        if self.buf.len() + b.len() > CHUNK {
            self.flush()?;
        }
        if b.len() > CHUNK {
            return self.z.write_all(b).map_err(|_| postcard::Error::SerializeBufferFull);
        }
        self.buf.extend_from_slice(b);
        Ok(())
    }

    fn finalize(mut self) -> postcard::Result<Self::Output> {
        self.flush()?;
        Ok(self.z)
    }
}

/// Decodes the postcard encoding of a `Checkpoint` from `r`, which must hold
/// nothing after it.
fn take<C: serde::de::DeserializeOwned>(r: impl Read) -> Result<C, String> {
    let mut de = postcard::Deserializer::from_flavor(Stream { r, buf: vec![0; 1 << 16], at: 0, end: 0, failed: None });
    TOO_DEEP.with(|t| t.set(false));
    let c = C::deserialize(&mut de);
    let Stream { mut r, at, end, failed, .. } = de.finalize().expect("a stream always finalizes");
    let c = match (c, failed) {
        (Ok(c), _) => c,
        (Err(_), _) if TOO_DEEP.with(Cell::take) => return Err(format!("a state nested more than {MAX_NESTING} deep")),
        // a read error (a corrupt zstd frame) says more than postcard's "unexpected end"
        (Err(_), Some(e)) => return Err(e.to_string()),
        (Err(e), None) => return Err(e.to_string()),
    };
    match std::io::copy(&mut r, &mut std::io::sink()) {
        Ok(0) if at == end => Ok(c),
        Ok(n) => Err(format!("{} bytes after the checkpoint", n as usize + end - at)),
        Err(e) => Err(e.to_string()),
    }
}

/// A postcard flavor over a reader, through `buf[at..end]`. Nothing in a checkpoint borrows
/// from its bytes, so `buf` only has to hold the longest string.
struct Stream<R> {
    r: R,
    buf: Vec<u8>,
    at: usize,
    end: usize,
    failed: Option<std::io::Error>,
}

impl<R: Read> Stream<R> {
    /// Makes `buf[at..at + n]` the next `n` bytes. `buf` grows as they arrive: a corrupt length
    /// is not allocated up front.
    #[cold]
    fn fill(&mut self, n: usize) -> postcard::Result<()> {
        self.buf.copy_within(self.at..self.end, 0);
        (self.end, self.at) = (self.end - self.at, 0);
        while self.end < n {
            if self.end == self.buf.len() {
                self.buf.resize(self.buf.len() * 2, 0);
            }
            match self.r.read(&mut self.buf[self.end..]) {
                Ok(0) => return Err(postcard::Error::DeserializeUnexpectedEnd),
                Ok(got) => self.end += got,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    self.failed = Some(e);
                    return Err(postcard::Error::DeserializeUnexpectedEnd);
                }
            }
        }
        Ok(())
    }
}

impl<'de, R: Read + 'de> postcard::de_flavors::Flavor<'de> for Stream<R> {
    type Remainder = Self;
    type Source = R;

    #[inline]
    fn pop(&mut self) -> postcard::Result<u8> {
        if self.at == self.end {
            self.fill(1)?;
        }
        self.at += 1;
        Ok(self.buf[self.at - 1])
    }

    fn try_take_n(&mut self, _: usize) -> postcard::Result<&'de [u8]> {
        Err(postcard::Error::DeserializeBadEncoding)
    }

    #[inline]
    fn try_take_n_temp<'a>(&'a mut self, n: usize) -> postcard::Result<&'a [u8]>
    where
        'de: 'a,
    {
        if self.end - self.at < n {
            self.fill(n)?;
        }
        self.at += n;
        Ok(&self.buf[self.at - n..self.at])
    }

    fn finalize(self) -> postcard::Result<Self> {
        Ok(self)
    }
}

/// How deep the recursive parts of a state (`Acc::If`, `Value::Array`, a join's sides) may nest
/// in a checkpoint, together. Postcard has no depth limit: a crafted checkpoint of a few hundred
/// bytes whose body is the `If` tag repeated overflowed the stack, which no fallback survives. SQL
/// nests them a few times (`agg::MAX_IF` combinators, arrays of arrays, a join in a join).
pub const MAX_NESTING: usize = 64;

thread_local! {
    /// The recursive values being deserialized on this thread, outermost first (`nested`).
    static NESTING: Cell<usize> = const { Cell::new(0) };
    /// Whether `nested` refused a value since `take` began (postcard drops an error's message).
    static TOO_DEEP: Cell<bool> = const { Cell::new(false) };
}

/// `T::deserialize` one level deeper, refused past `MAX_NESTING`: the `deserialize_with` of each
/// recursive field, so the bound needs no change to the format.
pub(crate) fn nested<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<T, D::Error> {
    /// One level, left however its deserialization ends.
    struct Level;
    impl Drop for Level {
        fn drop(&mut self) {
            NESTING.with(|n| n.set(n.get() - 1));
        }
    }
    let depth = NESTING.with(|n| {
        n.set(n.get() + 1);
        n.get()
    });
    let _level = Level;
    if depth <= MAX_NESTING {
        return T::deserialize(d);
    }
    TOO_DEEP.with(|t| t.set(true));
    Err(serde::de::Error::custom(format!("a state nested more than {MAX_NESTING} deep")))
}

/// FNV-1a, 64 bits: a hash that is the same in every build.
pub fn fnv64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3))
}

#[cfg(test)]
mod tests {
    use super::*;
    use postcard::ser_flavors::Flavor;

    /// A writer that records the size of each write.
    #[derive(Default)]
    struct Pieces(Vec<usize>);

    impl Write for Pieces {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            if !b.is_empty() {
                self.0.push(b.len());
            }
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn pieces(ops: &[(bool, usize)]) -> Vec<usize> {
        let mut z = Zstd { buf: Vec::with_capacity(CHUNK), z: Pieces::default() };
        for &(push, n) in ops {
            match push {
                true => (0..n).for_each(|_| z.try_push(7).unwrap()),
                false => z.try_extend(&vec![7; n]).unwrap(),
            }
            assert!(z.buf.capacity() <= CHUNK, "the buffer grew past a chunk");
        }
        z.finalize().unwrap().0
    }

    /// The compressor gets full chunks: a value that fits is buffered, one that does not flushes
    /// the buffer first, and only a value longer than a chunk is handed over on its own.
    #[test]
    fn the_compressor_gets_the_encoding_in_full_chunks() {
        assert_eq!(pieces(&[(false, CHUNK - 10), (false, 10)]), [CHUNK]);
        assert_eq!(pieces(&[(false, CHUNK - 10), (false, 20)]), [CHUNK - 10, 20]);
        assert_eq!(pieces(&[(true, CHUNK + 1)]), [CHUNK, 1]);
        assert_eq!(pieces(&[(true, 5), (false, CHUNK + 1), (true, 3)]), [5, CHUNK + 1, 3]);
        assert_eq!(pieces(&[(false, 3 * CHUNK)]), [3 * CHUNK]);
    }

    /// The output grows only when a write does not fit, by an eighth of what it holds and at
    /// least a MiB: few copies of a large checkpoint, and little room left over.
    #[test]
    fn the_output_grows_by_an_eighth_when_full() {
        let mut out = Out(Vec::with_capacity(10));
        out.write_all(&[1; 10]).unwrap();
        assert_eq!(out.0.capacity(), 10, "a write that fits does not grow it");
        let mut grown = 0;
        for _ in 0..(40 << 20) / 60_000 {
            let (len, cap) = (out.0.len(), out.0.capacity());
            out.write_all(&[2; 60_000]).unwrap();
            if out.0.capacity() != cap {
                assert!(cap - len < 60_000, "grown with room for the write");
                assert_eq!(out.0.capacity(), len + 60_000usize.max(len / 8).max(1 << 20));
                grown += 1;
            }
        }
        assert!(grown < 25, "grown {grown} times");
    }

    /// A reader that hands out one byte per read, failing once with `Interrupted` first.
    struct Trickle<'a>(&'a [u8], bool);

    impl Read for Trickle<'_> {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            if !self.1 {
                self.1 = true;
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            let n = self.0.len().min(b.len()).min(1);
            b[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    fn stream(bytes: &[u8]) -> Stream<Trickle<'_>> {
        Stream { r: Trickle(bytes, false), buf: vec![0; 1 << 16], at: 0, end: 0, failed: None }
    }

    /// Values decode from a reader however it splits them (retrying an interrupted read), up to
    /// the last byte, and the window grows by doubling only for a value longer than it.
    #[test]
    fn values_decode_through_the_window_one_byte_at_a_time() {
        let values = vec!["short".to_string(), "x".repeat(100_000), "end".to_string()];
        let bytes = postcard::to_allocvec(&values).unwrap();
        let mut de = postcard::Deserializer::from_flavor(stream(&bytes));
        assert_eq!(Vec::<String>::deserialize(&mut de).unwrap(), values);
        let s = de.finalize().unwrap();
        assert_eq!((s.at, s.end, s.buf.len()), (s.end, s.end, 1 << 17));
        let small = postcard::to_allocvec(&vec!["a".repeat(1000); 3]).unwrap();
        let mut de = postcard::Deserializer::from_flavor(stream(&small));
        assert_eq!(Vec::<String>::deserialize(&mut de).unwrap().len(), 3);
        assert_eq!(de.finalize().unwrap().buf.len(), 1 << 16, "the window grew for values that fit");
    }

    /// A read error is reported, not retried; a value that borrows from the bytes is refused.
    #[test]
    fn a_failed_read_and_a_borrowed_value_are_errors() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("the frame is corrupt"))
            }
        }
        assert_eq!(take::<Checkpoint>(Broken).unwrap_err(), "the frame is corrupt");
        let bytes = postcard::to_allocvec("borrowed").unwrap();
        let mut de = postcard::Deserializer::from_flavor(stream(&bytes));
        assert!(<&str>::deserialize(&mut de).is_err());
    }

    /// A frame is inflated up to the bound and refused past it: a MiB of zeros, an
    /// empty checkpoint and then zeros, compresses to a few dozen bytes.
    #[test]
    fn a_frame_that_inflates_past_the_bound_is_refused() {
        let body = vec![0; 1 << 20];
        let mut bytes = [&MAGIC[..], &VERSION.to_le_bytes(), &LAYOUT.to_le_bytes()].concat();
        bytes.extend(zstd::bulk::compress(&body, 3).unwrap());
        assert!(bytes.len() < 100, "{} bytes", bytes.len());
        let past = format!("the checkpoint inflates to more than {} bytes", body.len() - 1);
        assert_eq!(decode(&bytes, body.len() as u64 - 1).unwrap_err(), past);
        assert_eq!(decode(&bytes, 1000).unwrap_err(), "the checkpoint inflates to more than 1000 bytes");
        // up to the bound, it is decoded (and refused for what it holds)
        let after = format!("{} bytes after the checkpoint", body.len() - 6); // 6 fields, 0 each
        assert_eq!(decode(&bytes, body.len() as u64).unwrap_err(), after);
        assert_eq!(Checkpoint::decode(&bytes).unwrap_err(), after);
    }

    /// What `encode` writes up to a bound, `decode` reads up to the same bound: a checkpoint
    /// that inflates past it is refused when written, not written and then refused on restore.
    #[test]
    fn a_checkpoint_past_the_bound_is_refused_when_written() {
        let cat = crate::sql::parse("CREATE STREAM IF NOT EXISTS s (x int64);").unwrap();
        let c = Checkpoint::of(&Engine::new(&cat).unwrap(), 1, vec![("t".into(), 0, 5)], vec![]);
        let size = postcard::to_allocvec(&c).unwrap().len() as u64;
        let bytes = encode(&c, size).unwrap();
        assert_eq!(decode(&bytes, size).unwrap().sources, c.sources);
        let past = format!("the state encodes to {size} bytes, more than the {} a checkpoint may inflate to", size - 1);
        assert_eq!(encode(&c, size - 1).unwrap_err(), past);
        assert_eq!(Checkpoint::encode_of(&Engine::new(&cat).unwrap(), 1, &c.sources, &[], None), Ok(bytes));
    }
}
