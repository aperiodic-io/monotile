//! Live tables: tables that grow. A live table is its history, Parquet files partitioned by day
//! (`<dir>/date=YYYY-MM-DD/part-<n>.parquet`), and the rows written since its last flush, held in
//! memory and in a write-ahead log (`<dir>/_wal/<n>.arrows`, Arrow IPC streams) so that a
//! restart loses none of them. A query reads both, files first: one table, today and history.
//!
//! A flush writes the rows held into Parquet, then, at once for every query, makes the files
//! visible, drops the rows from memory and deletes the log they were in: a query sees each row
//! once, before and after.
use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{Array, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, SchemaRef};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// A live table's state behind one lock: what a query reads, and what a flush changes.
#[derive(Default)]
pub struct State {
    pub schema: Option<SchemaRef>,
    /// The rows since the last flush, in the order written.
    pub buffer: Vec<RecordBatch>,
    pub rows: usize,
    /// The open log segment and its number.
    wal: Option<(u64, arrow_ipc::writer::StreamWriter<std::fs::File>)>,
    /// Log segments whose rows the buffer holds.
    segments: Vec<u64>,
    next_segment: u64,
    next_part: u64,
}

impl std::fmt::Debug for Live {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Live({})", self.dir.display())
    }
}

/// A table that grows.
pub struct Live {
    pub name: String,
    pub dir: PathBuf,
    /// The column a row's day (its partition) is read from; `None`: the day it is flushed.
    pub time: Option<String>,
    /// fsync the log after every write.
    pub sync: bool,
    pub state: RwLock<State>,
}

fn segment_path(dir: &Path, n: u64) -> PathBuf {
    dir.join("_wal").join(format!("{n:012}.arrows"))
}

impl Live {
    /// Opens a live table in `dir`, replaying the rows its log holds.
    pub fn open(name: &str, dir: &Path, time: Option<String>, sync: bool) -> Result<Live> {
        std::fs::create_dir_all(dir.join("_wal")).with_context(|| dir.display().to_string())?;
        let mut st = State::default();
        let mut segs: Vec<u64> = std::fs::read_dir(dir.join("_wal"))?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str()?.strip_suffix(".arrows")?.parse().ok())
            .collect();
        segs.sort();
        for n in &segs {
            let f = std::fs::File::open(segment_path(dir, *n))?;
            // a segment cut short by a crash: the batches before its end
            if let Ok(r) = arrow_ipc::reader::StreamReader::try_new(f, None) {
                st.schema.get_or_insert(r.schema());
                for b in r.flatten() {
                    st.rows += b.num_rows();
                    st.buffer.push(b);
                }
            }
        }
        st.segments = segs.clone();
        st.next_segment = segs.last().map_or(0, |n| n + 1);
        // the parts' numbers go on from the last on disk
        let mut parts = vec![];
        crate::files::walk_parquet(dir, &mut parts);
        st.next_part = parts
            .iter()
            .filter_map(|p| p.file_stem()?.to_str()?.strip_prefix("part-")?.parse::<u64>().ok())
            .max()
            .map_or(0, |n| n + 1);
        if st.schema.is_none() {
            if let Some(p) = parts.first() {
                let f = std::fs::File::open(p)?;
                let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)?;
                st.schema = Some(b.schema().clone());
            }
        }
        Ok(Live { name: name.into(), dir: dir.into(), time, sync, state: RwLock::new(st) })
    }

    /// Appends rows: to the log, then to memory. The first rows written set the table's columns;
    /// later ones must have them (in any order; a missing one is NULL).
    pub fn append(&self, batch: RecordBatch) -> Result<usize> {
        let mut st = self.state.write().unwrap_or_else(|p| p.into_inner());
        let batch = match &st.schema {
            None => {
                st.schema = Some(batch.schema());
                batch
            }
            Some(s) => conform(&batch, s).with_context(|| format!("writing to {}", self.name))?,
        };
        if st.wal.is_none() {
            let n = st.next_segment;
            st.next_segment += 1;
            let f = std::fs::File::create(segment_path(&self.dir, n))?;
            let w = arrow_ipc::writer::StreamWriter::try_new(f, &batch.schema())?;
            st.wal = Some((n, w));
            st.segments.push(n);
        }
        let (_, w) = st.wal.as_mut().expect("opened");
        w.write(&batch)?;
        w.flush()?;
        if self.sync {
            w.get_ref().sync_data()?;
        }
        let n = batch.num_rows();
        st.rows += n;
        st.buffer.push(batch);
        Ok(n)
    }

    /// Writes the rows held into Parquet, one file per day, then makes them the table's history.
    /// Returns how many rows it wrote.
    pub fn flush(&self) -> Result<usize> {
        // what to write, and a new log segment for the rows written meanwhile
        let (batches, sealed, first_part) = {
            let mut st = self.state.write().unwrap_or_else(|p| p.into_inner());
            if st.buffer.is_empty() {
                return Ok(0);
            }
            if let Some((_, mut w)) = st.wal.take() {
                w.finish()?;
            }
            let sealed = std::mem::take(&mut st.segments);
            let first = st.next_part;
            st.next_part += 1;
            (st.buffer.clone(), sealed, first)
        };
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // by day, each day's rows in their order
        let mut days: std::collections::BTreeMap<String, Vec<RecordBatch>> = Default::default();
        for b in &batches {
            for (day, part) in split_by_day(b, self.time.as_deref())? {
                days.entry(day).or_default().push(part);
            }
        }
        let mut written = vec![];
        for (day, parts) in &days {
            let dir = self.dir.join(format!("date={day}"));
            std::fs::create_dir_all(&dir)?;
            let tmp = dir.join(format!(".part-{first_part:012}.parquet"));
            let f = std::fs::File::create(&tmp)?;
            let props = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
                .build();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, parts[0].schema(), Some(props))?;
            for p in parts {
                w.write(p)?;
            }
            w.into_inner()?.sync_all()?;
            written.push((tmp, dir.join(format!("part-{first_part:012}.parquet"))));
        }
        // at once for every query: the files in, the rows and their log out
        let mut st = self.state.write().unwrap_or_else(|p| p.into_inner());
        for (tmp, to) in &written {
            std::fs::rename(tmp, to)?;
        }
        st.buffer.drain(..batches.len());
        st.rows -= rows;
        for n in sealed {
            let _ = std::fs::remove_file(segment_path(&self.dir, n));
        }
        Ok(rows)
    }

    /// The Parquet files of the table's history, in order.
    pub fn files(&self) -> Vec<PathBuf> {
        let mut out = vec![];
        crate::files::walk_parquet(&self.dir, &mut out);
        out.sort();
        out
    }
}

/// `batch` in the columns of `schema`, by name: a missing column NULL, its own extra columns
/// refused, a column of another type cast if it can be.
fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let have = batch.schema();
    if let Some(extra) = have.fields().iter().find(|f| schema.index_of(f.name()).is_err()) {
        bail!(
            "{} is not a column of the table (it has {})",
            extra.name(),
            schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ")
        );
    }
    let cols = schema
        .fields()
        .iter()
        .map(|f| match have.index_of(f.name()) {
            Ok(i) => {
                let c = batch.column(i);
                if c.data_type() == f.data_type() {
                    Ok(c.clone())
                } else {
                    arrow_cast::cast(c, f.data_type())
                        .map_err(|e| anyhow!("column {}: {} into {}: {e}", f.name(), c.data_type(), f.data_type()))
                }
            }
            Err(_) => Ok(arrow_array::new_null_array(f.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

/// A batch's rows by the UTC day of their `time` column (or today's, without one), in order.
fn split_by_day(b: &RecordBatch, time: Option<&str>) -> Result<Vec<(String, RecordBatch)>> {
    let day = |us: i64| {
        let (y, m, d) = brrrrr_core::value::civil(us.div_euclid(86_400_000_000));
        format!("{y:04}-{m:02}-{d:02}")
    };
    let Some(col) = time.and_then(|t| b.schema().index_of(t).ok()) else {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as i64);
        return Ok(vec![(day(now), b.clone())]);
    };
    let t = arrow_cast::cast(b.column(col), &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None))?;
    let t = t.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or(anyhow!("not a time"))?;
    let mut out: Vec<(String, Vec<u32>)> = vec![];
    for i in 0..b.num_rows() {
        let d = if t.is_null(i) { "unknown".to_string() } else { day(t.value(i)) };
        match out.last_mut() {
            Some((x, rows)) if *x == d => rows.push(i as u32),
            _ => out.push((d, vec![i as u32])),
        }
    }
    out.into_iter()
        .map(|(d, rows)| {
            let idx = arrow_array::UInt32Array::from(rows);
            let cols =
                b.columns().iter().map(|c| arrow_select::take::take(c, &idx, None)).collect::<Result<Vec<_>, _>>()?;
            Ok((d, RecordBatch::try_new(b.schema(), cols)?))
        })
        .collect()
}
