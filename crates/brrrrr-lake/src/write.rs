//! A result's rows out: as a table to read, CSV, JSON lines, or a Parquet file.
use anyhow::Result;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use brrrrr_core::column::{Batch, Col, Data};
use brrrrr_core::engine::Row;
use brrrrr_core::query::text;
use brrrrr_core::value::Value;
use std::io::Write;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Out {
    Table,
    Csv,
    Json,
    Parquet,
}

impl Out {
    /// The format of a file name (`.csv`, `.json`/`.jsonl`/`.ndjson`, `.parquet`).
    pub fn of_path(p: &str) -> Option<Out> {
        let p = p.to_ascii_lowercase();
        let p = p.strip_suffix(".gz").unwrap_or(&p);
        Some(match p.rsplit_once('.')?.1 {
            "csv" | "tsv" => Out::Csv,
            "json" | "jsonl" | "ndjson" => Out::Json,
            "parquet" | "pq" => Out::Parquet,
            _ => return None,
        })
    }

    pub fn parse(s: &str) -> Option<Out> {
        Some(match s.to_ascii_lowercase().as_str() {
            "table" | "box" => Out::Table,
            "csv" => Out::Csv,
            "json" | "jsonl" | "ndjson" => Out::Json,
            "parquet" => Out::Parquet,
            _ => return None,
        })
    }
}

/// A result's rows: the batches of columns the query made, and the rows of them, made only
/// when a caller asks for rows (a table to read, a row at a time to a client).
#[derive(Clone, Default)]
pub struct Rows {
    batches: Vec<Batch>,
    rows: std::sync::OnceLock<Vec<Row>>,
}

impl Rows {
    pub fn of_batches(batches: Vec<Batch>) -> Rows {
        Rows { batches, rows: Default::default() }
    }

    pub fn batches(&self) -> &[Batch] {
        &self.batches
    }

    /// The rows' count (no row made).
    pub fn len(&self) -> usize {
        self.batches.iter().map(|b| b.len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Row `i` alone (no other row made).
    pub fn row(&self, mut i: usize) -> Option<Row> {
        for b in &self.batches {
            if i < b.len {
                return Some(b.row(i));
            }
            i -= b.len;
        }
        None
    }

    /// Each row, made as it is reached.
    pub fn iter_rows(&self) -> impl Iterator<Item = Row> + '_ {
        self.batches.iter().flat_map(|b| (0..b.len).map(move |i| b.row(i)))
    }
}

impl Rows {
    /// The first `n` rows alone.
    pub fn truncate(&mut self, n: usize) {
        let mut left = n;
        let batches = std::mem::take(&mut self.batches);
        for b in batches {
            if left == 0 {
                break;
            }
            let k = b.len.min(left);
            self.batches.push(if k == b.len { b } else { b.slice(0..k) });
            left -= k;
        }
        if let Some(rows) = self.rows.get_mut() {
            rows.truncate(n);
        }
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;
    fn into_iter(mut self) -> Self::IntoIter {
        match self.rows.take() {
            Some(rows) => rows.into_iter(),
            None => self.iter_rows().collect::<Vec<_>>().into_iter(),
        }
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = &'a Row;
    type IntoIter = std::slice::Iter<'a, Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl From<Vec<Row>> for Rows {
    fn from(rows: Vec<Row>) -> Rows {
        let width = rows.first().map_or(0, Vec::len);
        let batches = if rows.is_empty() { vec![] } else { vec![Batch::from_rows(&rows, width)] };
        Rows { batches, rows: rows.into() }
    }
}

impl std::ops::Deref for Rows {
    type Target = [Row];
    fn deref(&self) -> &[Row] {
        self.rows.get_or_init(|| self.iter_rows().collect())
    }
}

impl std::fmt::Debug for Rows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Rows({} in {} batches)", self.len(), self.batches.len())
    }
}

/// The kind of a result column, from its values: what a Parquet column of it is.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Null,
    Bool,
    Int,
    UInt,
    Float,
    Time,
    Text,
}

fn kind_of_value(v: &Value) -> Kind {
    match v {
        Value::Null => Kind::Null,
        Value::Bool(_) => Kind::Bool,
        Value::Int(_) => Kind::Int,
        Value::UInt(_) => Kind::UInt,
        Value::F32(_) | Value::F64(_) => Kind::Float,
        Value::Time(_) => Kind::Time,
        _ => Kind::Text,
    }
}

/// Two kinds as one column's: NULL is either, numbers of two kinds a float, others text.
fn unify(a: Kind, b: Kind) -> Kind {
    match (a, b) {
        (Kind::Null, v) | (v, Kind::Null) => v,
        (a, b) if a == b => a,
        (Kind::Int | Kind::UInt | Kind::Float, Kind::Int | Kind::UInt | Kind::Float) => Kind::Float,
        _ => Kind::Text,
    }
}

/// A column's kind, from its data (of its values, where they are of several).
fn kind_of_col(c: &Col) -> Kind {
    if c.nulls.as_ref().is_some_and(|n| n.iter().all(|x| *x)) {
        return Kind::Null;
    }
    match &c.data {
        Data::Bool(_) => Kind::Bool,
        Data::Int(_) => Kind::Int,
        Data::UInt(_) => Kind::UInt,
        Data::F32(_) | Data::F64(_) => Kind::Float,
        Data::Time(_) => Kind::Time,
        Data::Str(_) => Kind::Text,
        Data::Const(v, _) => kind_of_value(v),
        _ => (0..c.len()).map(|i| kind_of_value(&c.get(i))).fold(Kind::Null, unify),
    }
}

/// Column `i`'s kind over every batch.
fn kind_of(batches: &[Batch], i: usize) -> Kind {
    batches.iter().filter(|b| b.len > 0).map(|b| kind_of_col(&b.cols[i])).fold(Kind::Null, unify)
}

/// Each result column's type as a client reads it: bool, int, uint, float, time, text, or
/// null when it holds no value.
pub fn types(columns: &[String], rows: &Rows) -> Vec<&'static str> {
    (0..columns.len())
        .map(|i| match kind_of(rows.batches(), i) {
            Kind::Null => "null",
            Kind::Bool => "bool",
            Kind::Int => "int",
            Kind::UInt => "uint",
            Kind::Float => "float",
            Kind::Time => "time",
            Kind::Text => "text",
        })
        .collect()
}

/// A row as a JSON object (times as ISO 8601 text in UTC).
pub fn json_row(out: &mut String, columns: &[String], row: &[Value]) {
    out.push('{');
    for (i, (c, v)) in columns.iter().zip(row).enumerate() {
        if i > 0 {
            out.push(',');
        }
        brrrrr_core::format::json(out, &Value::Str(c.as_str().into()), &brrrrr_core::value::Type::Str);
        out.push(':');
        json_value(out, v);
    }
    out.push('}');
}

/// A value as JSON (a time as ISO 8601 text in UTC).
pub fn json_value(out: &mut String, v: &Value) {
    match v {
        Value::Time(_) => {
            out.push('"');
            out.push_str(&iso(v));
            out.push('"');
        }
        Value::F64(f) if !f.is_finite() => out.push_str("null"),
        v => brrrrr_core::format::json(out, v, &brrrrr_core::value::Type::Any),
    }
}

/// The result as an Arrow batch: each column typed by its values.
pub fn record_batch(columns: &[String], rows: &[Row]) -> Result<RecordBatch> {
    let rows = Rows::from(rows.to_vec());
    let schema = schema(columns, rows.batches());
    match record_batches(columns, rows.batches())?.into_iter().next() {
        Some(b) => Ok(b),
        None => Ok(RecordBatch::new_empty(schema)),
    }
}

/// The Arrow schema of a result: each column typed by its values in every batch.
pub fn schema(columns: &[String], batches: &[Batch]) -> Arc<Schema> {
    let kinds: Vec<Kind> = (0..columns.len()).map(|i| kind_of(batches, i)).collect();
    schema_of(columns, &kinds)
}

fn schema_of(columns: &[String], kinds: &[Kind]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .zip(kinds)
        .map(|(name, k)| {
            let t = match k {
                Kind::Bool => DataType::Boolean,
                Kind::Int => DataType::Int64,
                Kind::UInt => DataType::UInt64,
                Kind::Float => DataType::Float64,
                Kind::Time => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                Kind::Text | Kind::Null => DataType::Utf8,
            };
            Field::new(name, t, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// The result as Arrow batches, one of each of its batches, all of one schema (`schema`):
/// columns of a kind made arrays at once, others value by value.
pub fn record_batches(columns: &[String], batches: &[Batch]) -> Result<Vec<RecordBatch>> {
    let schema = schema(columns, batches);
    let kinds: Vec<Kind> = (0..columns.len()).map(|i| kind_of(batches, i)).collect();
    let mut out = vec![];
    for b in batches.iter().filter(|b| b.len > 0) {
        let arrays: Vec<ArrayRef> = kinds.iter().enumerate().map(|(i, k)| array(&b.cols[i], *k)).collect();
        out.push(RecordBatch::try_new_with_options(
            schema.clone(),
            arrays,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(b.len)),
        )?);
    }
    Ok(out)
}

/// A column as an Arrow array of `kind`.
fn array(c: &Col, kind: Kind) -> ArrayRef {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, TimestampMicrosecondArray, UInt64Array};
    let nulls =
        || c.nulls.as_ref().map(|n| arrow_buffer::NullBuffer::from(n.iter().map(|x| !x).collect::<Vec<bool>>()));
    let n = c.len();
    match (kind, &c.data) {
        (Kind::Int, Data::Int(v)) => Arc::new(Int64Array::new(v.to_vec().into(), nulls())),
        (Kind::UInt, Data::UInt(v)) => Arc::new(UInt64Array::new(v.to_vec().into(), nulls())),
        (Kind::Float, Data::F64(v)) => Arc::new(Float64Array::new(v.to_vec().into(), nulls())),
        (Kind::Time, Data::Time(v)) => {
            Arc::new(TimestampMicrosecondArray::new(v.to_vec().into(), nulls()).with_timezone("UTC"))
        }
        (Kind::Bool, Data::Bool(v)) => Arc::new(BooleanArray::new(v.iter().copied().collect(), nulls())),
        (Kind::Text, Data::Str(s)) => {
            let mut b = StringBuilder::with_capacity(n, s.iter().map(str::len).sum());
            (0..n).for_each(|i| b.append_option((!c.is_null(i)).then(|| s.get(i))));
            Arc::new(b.finish())
        }
        // value by value: a constant, values of several kinds, a column of another kind
        (Kind::Bool, _) => {
            let mut b = BooleanBuilder::with_capacity(n);
            (0..n).for_each(|i| {
                b.append_option(match c.get(i) {
                    Value::Bool(x) => Some(x),
                    _ => None,
                })
            });
            Arc::new(b.finish())
        }
        (Kind::Int, _) => {
            let mut b = Int64Builder::with_capacity(n);
            (0..n).for_each(|i| {
                let v = c.get(i);
                b.append_option(v.i64().filter(|_| !v.is_null()))
            });
            Arc::new(b.finish())
        }
        (Kind::UInt, _) => {
            let mut b = UInt64Builder::with_capacity(n);
            (0..n).for_each(|i| {
                b.append_option(match c.get(i) {
                    Value::UInt(x) => Some(x),
                    _ => None,
                })
            });
            Arc::new(b.finish())
        }
        (Kind::Float, _) => {
            let mut b = Float64Builder::with_capacity(n);
            (0..n).for_each(|i| b.append_option(c.get(i).f64()));
            Arc::new(b.finish())
        }
        (Kind::Time, _) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
            (0..n).for_each(|i| {
                b.append_option(match c.get(i) {
                    Value::Time(x) => Some(x),
                    _ => None,
                })
            });
            Arc::new(b.finish())
        }
        (Kind::Text | Kind::Null, _) => {
            let mut b = StringBuilder::with_capacity(n, n * 8);
            (0..n).for_each(|i| {
                let v = c.get(i);
                b.append_option((!v.is_null()).then(|| text(&v)))
            });
            Arc::new(b.finish())
        }
    }
}

/// Writes the result in `format` (`Table` for a terminal: `max_rows` rows at most, the first
/// and last halves).
pub fn write(out: &mut dyn Write, format: Out, columns: &[String], rows: &Rows, max_rows: usize) -> Result<()> {
    if format == Out::Table {
        out.write_all(table(columns, rows, max_rows).as_bytes())?;
        return Ok(());
    }
    // into a buffer of its own (the writer's threads need one), given on as it fills
    let mut w = ResultWriter::new(vec![], format, columns, kinds(columns, rows.batches()))?;
    for b in rows.batches() {
        w.push(b)?;
        out.write_all(w.out_mut())?;
        w.out_mut().clear();
    }
    out.write_all(&w.finish()?)?;
    Ok(())
}

/// Each column's kind over a result's batches (`schema`).
fn kinds(columns: &[String], batches: &[Batch]) -> Option<Vec<Kind>> {
    (!batches.is_empty()).then(|| (0..columns.len()).map(|i| kind_of(batches, i)).collect())
}

/// Rows of a Parquet row group.
const ROW_GROUP: usize = 1 << 20;

/// A result written to `W` as its batches come (`push`), then `finish`ed: CSV and JSON lines a
/// row at a time, Parquet a row group at a time, the columns of its row groups encoded side by
/// side on the threads of `encoders` and appended in order (a few row groups at most held). The
/// columns' types are those of `kinds` if given, else of the first row group's values.
pub struct ResultWriter<W: Write + Send> {
    format: Out,
    columns: Vec<String>,
    kinds: Option<Vec<Kind>>,
    out: Option<W>,
    parquet: Option<Parquet<W>>,
    pending: Vec<Batch>,
    pending_rows: usize,
    pub rows: usize,
}

struct Parquet<W: Write + Send> {
    file: parquet::file::writer::SerializedFileWriter<W>,
    factory: parquet::arrow::arrow_writer::ArrowRowGroupWriterFactory,
    schema: Arc<Schema>,
    /// Row groups sent to the encoders, those appended, and the columns encoded of the others.
    sent: usize,
    appended: usize,
    encoded: std::collections::BTreeMap<usize, Vec<Option<parquet::arrow::arrow_writer::ArrowColumnChunk>>>,
    done: std::sync::mpsc::Receiver<Encoded>,
    to_done: std::sync::mpsc::Sender<Encoded>,
}

/// A column chunk encoded: (row group, column, chunk).
type Encoded = (usize, usize, parquet::errors::Result<parquet::arrow::arrow_writer::ArrowColumnChunk>);

/// A column of a row group to encode (its parts, as Arrow arrays of `kind`), and where to say it is.
struct Job {
    group: usize,
    column: usize,
    writer: parquet::arrow::arrow_writer::ArrowColumnWriter,
    parts: Vec<Arc<Col>>,
    kind: Kind,
    field: arrow_schema::FieldRef,
    done: std::sync::mpsc::Sender<Encoded>,
}

impl Job {
    fn encode(mut self) {
        let r = || {
            for c in &self.parts {
                let a = array(c, self.kind);
                for leaf in parquet::arrow::arrow_writer::compute_leaves(&self.field, &a)? {
                    self.writer.write(&leaf)?;
                }
            }
            self.writer.close()
        };
        let r = r();
        let _ = self.done.send((self.group, self.column, r));
    }
}

/// The encoders: a thread a CPU, shared by every writer.
fn encoders() -> &'static std::sync::Mutex<std::sync::mpsc::Sender<Job>> {
    static POOL: std::sync::OnceLock<std::sync::Mutex<std::sync::mpsc::Sender<Job>>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let n = std::thread::available_parallelism().map_or(2, |n| n.get());
        for i in 0..n {
            let rx = rx.clone();
            let _ = std::thread::Builder::new().name(format!("parquet-{i}")).spawn(move || loop {
                let job = rx.lock().expect("jobs").recv();
                let Ok(j) = job else { return };
                j.encode();
            });
        }
        std::sync::Mutex::new(tx)
    })
}

/// Row groups encoded at most at a time by one writer, its columns side by side: more is no
/// faster (10M rows), only holds more.
const IN_FLIGHT: usize = 2;

impl<W: Write + Send> ResultWriter<W> {
    pub fn new(out: W, format: Out, columns: &[String], kinds: Option<Vec<Kind>>) -> Result<ResultWriter<W>> {
        let mut w = ResultWriter {
            format,
            columns: columns.to_vec(),
            kinds,
            out: Some(out),
            parquet: None,
            pending: vec![],
            pending_rows: 0,
            rows: 0,
        };
        if format == Out::Csv {
            let head: Vec<String> = columns.iter().map(|c| csv_field(c)).collect();
            writeln!(w.out.as_mut().expect("out"), "{}", head.join(","))?;
        }
        Ok(w)
    }

    /// The output (CSV and JSON: what is written so far; Parquet: until its first row group).
    pub fn out_mut(&mut self) -> &mut W {
        match &mut self.parquet {
            Some(p) => p.file.inner_mut(),
            None => self.out.as_mut().expect("out"),
        }
    }

    pub fn push(&mut self, b: &Batch) -> Result<()> {
        self.rows += b.len;
        match self.format {
            Out::Csv | Out::Json | Out::Table => {
                let out = self.out.as_mut().expect("out");
                write_rows(out, self.format, &self.columns, b)
            }
            Out::Parquet => {
                // the columns' types are set: a value of another kind would be lost (a column NULL
                // until then: its values as text)
                for (i, &k) in self.kinds.iter().flatten().enumerate() {
                    let now = kind_of(std::slice::from_ref(b), i);
                    if k != Kind::Null && unify(k, now) != k {
                        anyhow::bail!(
                            "the column {} is {k:?} in the first {ROW_GROUP} rows, then {now:?}: CAST it to one type",
                            self.columns[i]
                        );
                    }
                }
                if b.len > 0 {
                    self.pending_rows += b.len;
                    self.pending.push(b.clone());
                }
                if self.pending_rows >= ROW_GROUP {
                    self.row_group()?;
                }
                Ok(())
            }
        }
    }

    /// Rows held for the next row group.
    pub fn pending(&self) -> usize {
        self.pending_rows
    }

    /// The batches held as a row group, sent to the encoders; those encoded appended.
    pub fn row_group(&mut self) -> Result<()> {
        let kinds =
            self.kinds.get_or_insert_with(|| (0..self.columns.len()).map(|i| kind_of(&self.pending, i)).collect());
        if self.parquet.is_none() {
            let schema = schema_of(&self.columns, kinds);
            let props = parquet::file::properties::WriterProperties::builder()
                // zstd(3): as fast as zstd(1) here (encoding is not what COPY waits for), and files 15%
                // smaller; DuckDB's zstd size
                .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
                .build();
            let out = self.out.take().expect("out");
            let (file, factory) =
                parquet::arrow::ArrowWriter::try_new(out, schema.clone(), Some(props))?.into_serialized_writer()?;
            let (to_done, done) = std::sync::mpsc::channel();
            self.parquet = Some(Parquet {
                file,
                factory,
                schema,
                sent: 0,
                appended: 0,
                encoded: Default::default(),
                done,
                to_done,
            });
        }
        let p = self.parquet.as_mut().expect("parquet");
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        let writers = p.factory.create_column_writers(p.sent)?;
        let tx = encoders().lock().expect("encoders").clone();
        for (column, writer) in writers.into_iter().enumerate() {
            let parts = pending.iter().map(|b| b.cols[column].clone()).collect();
            let field = p.schema.fields()[column].clone();
            tx.send(Job { group: p.sent, column, writer, parts, kind: kinds[column], field, done: p.to_done.clone() })
                .map_err(|_| anyhow::anyhow!("the Parquet encoders stopped"))?;
        }
        p.encoded.insert(p.sent, (0..self.columns.len()).map(|_| None).collect());
        p.sent += 1;
        // a few row groups held at most: wait for the oldest
        while p.sent - p.appended > IN_FLIGHT {
            p.append(true)?;
        }
        p.append(false)
    }

    /// The rest written, the file closed: its output.
    pub fn finish(mut self) -> Result<W> {
        if self.format != Out::Parquet {
            return Ok(self.out.take().expect("out"));
        }
        self.row_group()?;
        let mut p = self.parquet.take().expect("parquet");
        while p.appended < p.sent {
            p.append(true)?;
        }
        Ok(p.file.into_inner()?)
    }
}

impl<W: Write + Send> Parquet<W> {
    /// The row groups whose columns are all encoded, appended in order; with `wait`, after
    /// waiting for one column at least.
    fn append(&mut self, wait: bool) -> Result<()> {
        let mut take = |(g, c, r): Encoded| -> Result<()> {
            if let Some(cols) = self.encoded.get_mut(&g) {
                cols[c] = Some(r?);
            }
            Ok(())
        };
        if wait {
            take(self.done.recv().map_err(|_| anyhow::anyhow!("the Parquet encoders stopped"))?)?;
        }
        while let Ok(e) = self.done.try_recv() {
            take(e)?;
        }
        while self.encoded.get(&self.appended).is_some_and(|c| c.iter().all(Option::is_some)) {
            let cols = self.encoded.remove(&self.appended).expect("checked");
            let mut rg = self.file.next_row_group()?;
            for c in cols.into_iter().flatten() {
                c.append_to_row_group(&mut rg)?;
            }
            rg.close()?;
            self.appended += 1;
        }
        Ok(())
    }
}

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// A batch's rows as CSV or JSON lines.
fn write_rows(out: &mut dyn Write, format: Out, columns: &[String], b: &Batch) -> Result<()> {
    let mut line = String::new();
    for i in 0..b.len {
        let r = b.row(i);
        line.clear();
        if format == Out::Csv {
            let cells: Vec<String> =
                r.iter().map(|v| if v.is_null() { String::new() } else { csv_field(&text(v)) }).collect();
            line.push_str(&cells.join(","));
            line.push('\n');
        } else {
            line.push('{');
            for (i, (c, v)) in columns.iter().zip(&r).enumerate() {
                if i > 0 {
                    line.push(',');
                }
                brrrrr_core::format::json(&mut line, &Value::Str(c.as_str().into()), &brrrrr_core::value::Type::Str);
                line.push(':');
                match v {
                    Value::Time(_) => {
                        line.push('"');
                        line.push_str(&iso(v));
                        line.push('"');
                    }
                    v => brrrrr_core::format::json(&mut line, v, &brrrrr_core::value::Type::Any),
                }
            }
            line.push_str("}\n");
        }
        out.write_all(line.as_bytes())?;
    }
    Ok(())
}

/// A time as ISO 8601 in UTC (`2024-01-01T00:00:00Z`, the fraction only when there is one).
pub fn iso(v: &Value) -> String {
    let t = text(v);
    format!("{}Z", t.replacen(' ', "T", 1))
}

/// Rows as a box-drawn table: numbers to the right, fractions to 10 significant digits (to read,
/// not `18.19910000000001`; the other formats keep every digit), at most `max` rows (the first
/// and last halves, with a count of those left out), long values cut.
pub fn table(columns: &[String], rows: &Rows, max: usize) -> String {
    const WIDTH: usize = 40;
    let cut = |s: String| {
        if s.chars().count() > WIDTH {
            format!("{}…", s.chars().take(WIDTH - 1).collect::<String>())
        } else {
            s
        }
    };
    // the rows shown alone made rows
    let n = rows.len();
    let at: Vec<usize> = if n > max { (0..max / 2).chain(n - max / 2..n).collect() } else { (0..n).collect() };
    let shown: Vec<Row> = at.into_iter().filter_map(|i| rows.row(i)).collect();
    let shown_text = |v: &Value| match v {
        Value::F64(x) if x.is_finite() => text(&Value::F64(format!("{x:.9e}").parse().unwrap_or(*x))),
        Value::F32(x) if x.is_finite() => text(&Value::F64(format!("{x:.6e}").parse().unwrap_or(*x as f64))),
        v => text(v),
    };
    let cells: Vec<Vec<String>> = shown.iter().map(|r| r.iter().map(|v| cut(shown_text(v))).collect()).collect();
    let numeric: Vec<bool> = (0..columns.len())
        .map(|i| {
            shown
                .iter()
                .all(|r| matches!(r[i], Value::Int(_) | Value::UInt(_) | Value::F32(_) | Value::F64(_) | Value::Null))
        })
        .collect();
    let widths: Vec<usize> = (0..columns.len())
        .map(|i| cells.iter().map(|r| r[i].chars().count()).chain([columns[i].chars().count()]).max().unwrap_or(1))
        .collect();
    let line = |l: &str, m: &str, r: &str| {
        let parts: Vec<String> = widths.iter().map(|w| "─".repeat(w + 2)).collect();
        format!("{l}{}{r}\n", parts.join(m))
    };
    let row = |vals: &[String], align: bool| {
        let parts: Vec<String> = vals
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let pad = widths[i] - v.chars().count();
                if align && numeric[i] {
                    format!(" {}{v} ", " ".repeat(pad))
                } else {
                    format!(" {v}{} ", " ".repeat(pad))
                }
            })
            .collect();
        format!("│{}│\n", parts.join("│"))
    };
    let mut out = line("┌", "┬", "┐");
    out.push_str(&row(columns, false));
    out.push_str(&line("├", "┼", "┤"));
    for (k, r) in cells.iter().enumerate() {
        if rows.len() > max && k == max / 2 {
            let dots: Vec<String> = widths.iter().map(|_| "·".into()).collect();
            out.push_str(&row(&dots, false));
        }
        out.push_str(&row(r, true));
    }
    out.push_str(&line("└", "┴", "┘"));
    let n = rows.len();
    out.push_str(&format!(
        "{n} row{}{}\n",
        if n == 1 { "" } else { "s" },
        if n > max { format!(" ({} shown)", shown.len()) } else { String::new() }
    ));
    out
}
