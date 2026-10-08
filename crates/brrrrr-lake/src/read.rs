//! Files as a source's batches: Parquet, CSV and JSON lines (each optionally gzipped), their
//! columns as the engine holds them.
use crate::files::{File, Format};
use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{cast::AsArray, types::*, Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType as D, SchemaRef, TimeUnit};
use brrrrr_core::column::{Batch, Col, Data, Strs};
use brrrrr_core::engine::Source;
use brrrrr_core::value::{Type, Value};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::arrow::ProjectionMask;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

/// Rows per batch read.
pub const BATCH_ROWS: usize = 65_536;

/// The engine's type of an Arrow column; `None` for what brrrrr does not read.
pub fn engine_type(t: &D) -> Option<Type> {
    Some(match t {
        D::Boolean => Type::Bool,
        D::Int8 | D::Int16 | D::Int32 | D::Int64 => Type::Int(64),
        D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64 => Type::UInt(64),
        D::Float16 | D::Float32 | D::Float64 | D::Decimal32(..) | D::Decimal64(..) | D::Decimal128(..) => Type::F64,
        D::Utf8 | D::LargeUtf8 | D::Utf8View => Type::Str,
        D::Dictionary(_, v) => return engine_type(v),
        D::Timestamp(..) | D::Date32 | D::Date64 => Type::Time(6),
        D::Null => Type::Str,
        _ => return None,
    })
}

/// A table's columns, from its first file (every file's, `union_by_name`): their names and
/// types (columns of other types are left out), then its partitions' (`key=value` directories
/// the file has no column for). A
/// partition is a number when every file's value of it is a whole number (`hour=9`), text
/// otherwise (`date=2024-01-02`, `symbol=BTC`).
pub fn schema(files: &[File]) -> Result<Vec<(String, Type)>> {
    let f = &files[0];
    let columns = |f: &File| -> Result<Vec<(String, Type)>> {
        let s = arrow_schema(f)?;
        Ok(s.fields().iter().filter_map(|fd| field_type(fd).map(|t| (fd.name().clone(), t))).collect())
    };
    let mut cols = columns(f)?;
    // every file's columns, in the order they first come, a number of both kinds a double and
    // of a number and anything else text (DuckDB's union_by_name)
    if f.options.union_by_name {
        for other in &files[1..] {
            for (n, t) in columns(other)? {
                match cols.iter_mut().find(|c| c.0 == n) {
                    Some(c) if c.1 == t => {}
                    Some(c) => {
                        let num = |t: &Type| matches!(t, Type::Int(_) | Type::UInt(_) | Type::F64);
                        c.1 = if num(&c.1) && num(&t) { Type::F64 } else { Type::Str };
                    }
                    None => cols.push((n, t)),
                }
            }
        }
    }
    for (k, _) in &f.partitions {
        if !cols.iter().any(|c| c.0 == *k) {
            let whole = files.iter().all(|f| {
                partition(f, k).is_some_and(|v| v == crate::files::NULL_PARTITION || v.parse::<i64>().is_ok())
            });
            cols.push((k.clone(), if whole { Type::Int(64) } else { Type::Str }));
        }
    }
    if cols.is_empty() {
        bail!("{}: no columns brrrrr reads (numbers, text, booleans, times)", f.name);
    }
    Ok(cols)
}

/// A file's value of a partition, as its path has it.
pub fn partition<'a>(f: &'a File, key: &str) -> Option<&'a str> {
    f.partitions.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// A partition's value as its column's type (`schema`) has it.
pub fn partition_value(v: &str, ty: &Type) -> Value {
    if v == crate::files::NULL_PARTITION {
        return Value::Null;
    }
    match ty.base() {
        Type::Int(_) => v.parse().map_or(Value::Null, Value::Int),
        _ => Value::Str(v.into()),
    }
}

/// A file's bytes, gunzipped if it is gzipped, without a UTF-8 byte order mark.
pub(crate) fn open(f: &File) -> Result<Box<dyn BufRead + Send>> {
    let file = std::fs::File::open(&f.local).with_context(|| f.name.clone())?;
    let mut r: Box<dyn BufRead + Send> = if f.gzip {
        Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(BufReader::new(file))))
    } else {
        Box::new(BufReader::new(file))
    };
    if r.fill_buf()?.starts_with(b"\xEF\xBB\xBF") {
        r.consume(3);
    }
    Ok(r)
}

/// A reader's options, DuckDB's (`read_csv('x.csv', header = false, delim = ';')`): written
/// after the path in the call, which the compiled query's table carries (`Table::Path`).
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub header: Option<bool>,
    pub delim: Option<u8>,
    pub quote: Option<u8>,
    pub escape: Option<u8>,
    /// Lines skipped before the header.
    pub skip: usize,
    /// Every column's name and type, in order.
    pub columns: Option<Vec<(String, String)>>,
    /// Some columns' types, by name.
    pub types: Vec<(String, String)>,
    /// How times are written (`%Y.%m.%dD%H:%M:%S.%n`).
    pub timestampformat: Option<String>,
    /// The texts that are NULL (by default, an empty field).
    pub nullstr: Vec<String>,
    pub auto_detect: Option<bool>,
    /// Every file's columns, by name; types widened where they differ (`schema`).
    pub union_by_name: bool,
}

impl Options {
    /// `name = value, ...`: values strings, numbers, booleans, `{'k': 'v', ...}` and `['a', ...]`.
    pub fn parse(text: &str) -> Result<Options> {
        use sqlparser::tokenizer::{Token, Tokenizer};
        let toks: Vec<Token> = Tokenizer::new(&sqlparser::dialect::GenericDialect {}, text)
            .tokenize()
            .map_err(|e| anyhow!("{text}: {e}"))?
            .into_iter()
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .collect();
        enum V {
            One(String),
            Map(Vec<(String, String)>),
            List(Vec<String>),
        }
        let lit = |t: &Token| match t {
            Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Some(s.clone()),
            Token::Number(n, _) => Some(n.clone()),
            Token::Word(w) => Some(w.value.clone()),
            _ => None,
        };
        let mut o = Options::default();
        let mut i = 0;
        while i < toks.len() {
            let name = match &toks[i] {
                Token::Word(w) => w.value.to_ascii_lowercase(),
                t => bail!("reader options: {t} where an option's name goes (name = value, ...)"),
            };
            if toks.get(i + 1) != Some(&Token::Eq) {
                bail!("reader option {name}: name = value");
            }
            i += 2;
            let v = match toks.get(i) {
                Some(Token::LBrace) => {
                    let mut m = vec![];
                    i += 1;
                    while toks.get(i) != Some(&Token::RBrace) {
                        match (toks.get(i).and_then(lit), toks.get(i + 1), toks.get(i + 2).and_then(lit)) {
                            (Some(k), Some(Token::Colon), Some(v)) => m.push((k, v)),
                            _ => bail!("reader option {name}: {{'column': 'TYPE', ...}}"),
                        }
                        i += 3;
                        if toks.get(i) == Some(&Token::Comma) {
                            i += 1;
                        }
                    }
                    V::Map(m)
                }
                Some(Token::LBracket) => {
                    let mut l = vec![];
                    i += 1;
                    while toks.get(i) != Some(&Token::RBracket) {
                        l.push(toks.get(i).and_then(lit).ok_or(anyhow!("reader option {name}: ['a', 'b', ...]"))?);
                        i += 1;
                        if toks.get(i) == Some(&Token::Comma) {
                            i += 1;
                        }
                    }
                    V::List(l)
                }
                Some(t) => V::One(lit(t).ok_or(anyhow!("reader option {name}: {t} is not a value"))?),
                None => bail!("reader option {name}: no value"),
            };
            i += 1;
            if toks.get(i) == Some(&Token::Comma) {
                i += 1;
            }
            let one = |v: &V| match v {
                V::One(s) => Ok(s.clone()),
                _ => Err(anyhow!("reader option {name}: one value")),
            };
            let flag = |v: &V| -> Result<bool> {
                match one(v)?.to_ascii_lowercase().as_str() {
                    "true" | "1" => Ok(true),
                    "false" | "0" => Ok(false),
                    x => bail!("reader option {name}: {x} is not true or false"),
                }
            };
            let byte = |v: &V| -> Result<u8> {
                let s = one(v)?;
                let s = if s == "\\t" { "\t".to_string() } else { s };
                match s.as_bytes() {
                    [b] => Ok(*b),
                    _ => bail!("reader option {name}: one character ('{s}' is not)"),
                }
            };
            match name.as_str() {
                "header" => o.header = Some(flag(&v)?),
                "delim" | "sep" | "delimiter" => o.delim = Some(byte(&v)?),
                "quote" => o.quote = Some(byte(&v)?),
                "escape" => o.escape = Some(byte(&v)?),
                "skip" => o.skip = one(&v)?.parse().map_err(|_| anyhow!("reader option skip: a number of lines"))?,
                "columns" => match v {
                    V::Map(m) => o.columns = Some(m),
                    _ => bail!("reader option columns: {{'name': 'TYPE', ...}}, every column in order"),
                },
                "types" | "dtypes" | "column_types" => match v {
                    V::Map(m) => o.types = m,
                    _ => bail!("reader option types: {{'column': 'TYPE', ...}}"),
                },
                "timestampformat" | "timestamp_format" => o.timestampformat = Some(one(&v)?),
                "nullstr" | "null_str" | "na_values" => {
                    o.nullstr = match v {
                        V::List(l) => l,
                        v => vec![one(&v)?],
                    }
                }
                "auto_detect" => o.auto_detect = Some(flag(&v)?),
                "union_by_name" => o.union_by_name = flag(&v)?,
                // what brrrrr does anyway: Hive partitions read, compression from the name
                "hive_partitioning" | "compression" | "sample_size" => {}
                n => bail!(
                    "reader option {n} is not one brrrrr reads: header, delim, quote, escape, skip, columns, types, \
                     timestampformat, nullstr, auto_detect, union_by_name"
                ),
            }
        }
        if o.auto_detect == Some(false) && o.columns.is_none() {
            bail!("auto_detect = false: name the columns and their types with columns = {{'name': 'TYPE', ...}}");
        }
        Ok(o)
    }

    /// The regex of the texts that are NULL, if they are not just the empty one.
    fn nulls(&self) -> Result<Option<regex::Regex>> {
        if self.nullstr.is_empty() {
            return Ok(None);
        }
        let alt: Vec<String> = self.nullstr.iter().map(|s| regex::escape(s)).collect();
        Ok(Some(regex::Regex::new(&format!("^(?:{})$", alt.join("|")))?))
    }
}

/// The column a Field holds as the engine reads it: its type, or a time from text or counts
/// (`TIME`).
fn field_type(f: &arrow_schema::Field) -> Option<Type> {
    if f.metadata().contains_key(TIME) {
        return Some(Type::Time(6));
    }
    engine_type(f.data_type())
}

/// A Field's metadata key: its text (or numbers) are times, read as its value says: `auto`
/// (`parse_time`), `s`/`ms`/`us`/`ns` (counts since the epoch, or text), or a format.
const TIME: &str = "brrrrr.time";

/// A DuckDB type name as the Arrow type a CSV column is read in, and how a time is read.
fn duck_type(name: &str, format: Option<&str>) -> Result<(D, Option<String>)> {
    let up = name.trim().to_ascii_uppercase();
    let base = up.split('(').next().unwrap_or("").trim();
    Ok(match base {
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" | "UUID" => (D::Utf8, None),
        "BIGINT" | "INTEGER" | "INT" | "INT8" | "INT4" | "INT2" | "SMALLINT" | "TINYINT" | "LONG" | "HUGEINT" => {
            (D::Int64, None)
        }
        "UBIGINT" | "UINTEGER" | "USMALLINT" | "UTINYINT" => (D::UInt64, None),
        "DOUBLE" | "FLOAT" | "FLOAT8" | "FLOAT4" | "REAL" | "DECIMAL" | "NUMERIC" => (D::Float64, None),
        "BOOLEAN" | "BOOL" => (D::Boolean, None),
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" | "DATE" => {
            (D::Utf8, Some(format.unwrap_or("auto").to_string()))
        }
        "TIMESTAMP_S" => (D::Utf8, Some("s".into())),
        "TIMESTAMP_MS" => (D::Utf8, Some("ms".into())),
        "TIMESTAMP_US" => (D::Utf8, Some("us".into())),
        "TIMESTAMP_NS" => (D::Utf8, Some("ns".into())),
        _ => bail!(
            "type {name}: brrrrr reads VARCHAR, BIGINT, UBIGINT, DOUBLE, BOOLEAN, TIMESTAMP (text) and \
             TIMESTAMP_S, _MS, _US and _NS (counts since the epoch)"
        ),
    })
}

/// A time as `format` writes it (strftime's `%Y %m %d %H %M %S`, `%f` microseconds, `%g`
/// milliseconds, `%n` nanoseconds, kept to the microsecond; `%%`), in µs.
pub fn parse_format(s: &str, format: &str) -> Option<i64> {
    let (mut y, mut mo, mut d, mut h, mut mi, mut sec, mut frac) = (1970, 1, 1, 0, 0, 0, String::new());
    let mut s = s.trim();
    let mut f = format.chars();
    let digits = |s: &mut &str, max: usize| -> Option<String> {
        let n = s.bytes().take(max).take_while(u8::is_ascii_digit).count();
        (n > 0).then(|| {
            let (a, b) = s.split_at(n);
            *s = b;
            a.to_string()
        })
    };
    while let Some(c) = f.next() {
        if c != '%' {
            s = s.strip_prefix(c)?;
            continue;
        }
        match f.next()? {
            'Y' => y = digits(&mut s, 4)?.parse().ok()?,
            'm' => mo = digits(&mut s, 2)?.parse().ok()?,
            'd' => d = digits(&mut s, 2)?.parse().ok()?,
            'H' => h = digits(&mut s, 2)?.parse().ok()?,
            'M' => mi = digits(&mut s, 2)?.parse().ok()?,
            'S' => sec = digits(&mut s, 2)?.parse().ok()?,
            'g' => frac = digits(&mut s, 3)?,
            'f' => frac = digits(&mut s, 6)?,
            'n' => frac = digits(&mut s, 9)?,
            '%' => s = s.strip_prefix('%')?,
            _ => return None,
        }
    }
    if !s.is_empty() {
        return None;
    }
    let frac = if frac.is_empty() { String::new() } else { format!(".{frac}") };
    brrrrr_core::value::parse_datetime(&format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{sec:02}{frac}"))
}

/// A text column (or numbers) as times (µs), as `spec` (`TIME`) says; an error names the
/// value, its line (`line` is the first row's) and the column.
fn to_times(a: &ArrayRef, spec: &str, line: usize, name: &str) -> Result<ArrayRef> {
    let unit: Option<i64> = match spec {
        "s" => Some(1_000_000),
        "ms" => Some(1_000),
        "us" => Some(1),
        "ns" => Some(-1_000),
        _ => None,
    };
    let text = arrow_cast::cast(a, &D::Utf8)?;
    let text = text.as_string::<i32>();
    let mut out = arrow_array::builder::TimestampMicrosecondBuilder::with_capacity(text.len());
    for (i, v) in text.iter().enumerate() {
        let Some(v) = v else {
            out.append_null();
            continue;
        };
        let count = v.trim().parse::<i64>().ok();
        let t = match (count, unit) {
            (Some(n), Some(k)) if k > 0 => n.checked_mul(k),
            (Some(n), Some(k)) => Some(n.div_euclid(-k)),
            (Some(_), None) if spec == "auto" => bail!(
                "line {}, column {name}: {v} is a number: TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_US or TIMESTAMP_NS \
                 reads it as a count since the epoch (types = {{'{name}': 'TIMESTAMP_US'}})",
                line + i
            ),
            _ if unit.is_some() || spec == "auto" => parse_time(v),
            _ => parse_format(v, spec),
        };
        match t {
            Some(t) => out.append_value(t),
            None => bail!(
                "line {}, column {name}: '{v}' is not a time{}",
                line + i,
                if unit.is_none() && spec != "auto" { format!(" as {spec} writes it") } else { String::new() }
            ),
        }
    }
    Ok(Arc::new(out.finish()))
}

/// The Arrow type the engine's `ty` is read from.
fn arrow_type(ty: &Type) -> Option<D> {
    Some(match ty.base() {
        Type::Bool => D::Boolean,
        Type::Int(_) => D::Int64,
        Type::UInt(_) => D::UInt64,
        Type::F32 | Type::F64 => D::Float64,
        Type::Str => D::Utf8,
        Type::Time(_) => D::Timestamp(TimeUnit::Microsecond, None),
        _ => return None,
    })
}

/// A file's CSV, after the lines `skip`s.
fn csv_input(f: &File) -> Result<Box<dyn BufRead + Send>> {
    let mut r = open(f)?;
    for _ in 0..f.options.skip {
        if r.read_until(b'\n', &mut vec![])? == 0 {
            break;
        }
    }
    Ok(r)
}

/// A CSV file's format: its delimiter (sniffed, or the options'), quote, escape and NULLs.
fn csv_format(f: &File, delimiter: u8, header: bool) -> Result<arrow_csv::reader::Format> {
    let o = &f.options;
    let mut fmt = arrow_csv::reader::Format::default().with_header(header).with_delimiter(o.delim.unwrap_or(delimiter));
    if let Some(q) = o.quote {
        fmt = fmt.with_quote(q);
    }
    if let Some(e) = o.escape {
        fmt = fmt.with_escape(e);
    }
    if let Some(r) = o.nulls()? {
        fmt = fmt.with_null_regex(r);
    }
    Ok(fmt)
}

/// Whether a CSV file has a header line: as its options say; with `columns` and no `header`,
/// whether its first line names them.
fn csv_header(f: &File, delimiter: u8) -> Result<bool> {
    let o = &f.options;
    match (o.header, &o.columns) {
        (Some(h), _) => Ok(h),
        (None, Some(cols)) => {
            let mut line = String::new();
            csv_input(f)?.read_line(&mut line)?;
            let d = o.delim.unwrap_or(delimiter) as char;
            let names: Vec<&str> = line.trim_end().split(d).map(|n| n.trim().trim_matches('"')).collect();
            Ok(names.len() == cols.len() && names.iter().zip(cols).all(|(n, c)| n.eq_ignore_ascii_case(&c.0)))
        }
        (None, None) => Ok(o.auto_detect != Some(false)),
    }
}

/// A CSV file's columns as they are read: inferred from its first rows (or its `columns`), its
/// `types` applied, and text that is all times (`timestampformat`'s, or `parse_time`'s) a time.
fn csv_schema(f: &File, delimiter: u8) -> Result<SchemaRef> {
    let o = &f.options;
    let header = csv_header(f, delimiter)?;
    let fmt = csv_format(f, delimiter, header)?;
    let mut fields: Vec<arrow_schema::Field> = match &o.columns {
        Some(cols) => cols
            .iter()
            .map(|(n, t)| {
                let (dt, time) = duck_type(t, o.timestampformat.as_deref())?;
                Ok(timed(arrow_schema::Field::new(n, dt, true), time))
            })
            .collect::<Result<_>>()?,
        None => {
            let (s, _) =
                fmt.infer_schema(csv_input(f)?, Some(10_000)).with_context(|| format!("{}: not CSV", f.name))?;
            s.fields()
                .iter()
                .enumerate()
                .map(|(i, fd)| {
                    // without a header, DuckDB's names
                    let name = if header { fd.name().clone() } else { format!("column{i}") };
                    fd.as_ref().clone().with_name(name)
                })
                .collect()
        }
    };
    for (n, t) in &o.types {
        let fd = fields.iter_mut().find(|fd| fd.name() == n).ok_or(anyhow!("{}: types: no column {n}", f.name))?;
        let (dt, time) = duck_type(t, o.timestampformat.as_deref())?;
        *fd = timed(fd.clone().with_data_type(dt), time);
    }
    // text columns that are all times: read as text, made times (`to_times`)
    let typed: Vec<&str> = o.types.iter().map(|t| t.0.as_str()).collect();
    let text: Vec<usize> = (0..fields.len())
        .filter(|&i| {
            o.columns.is_none() && fields[i].data_type() == &D::Utf8 && !typed.contains(&fields[i].name().as_str())
        })
        .collect();
    if !text.is_empty() {
        let all_text = arrow_schema::Schema::new(
            fields.iter().map(|fd| arrow_schema::Field::new(fd.name(), D::Utf8, true)).collect::<Vec<_>>(),
        );
        let mut r = arrow_csv::ReaderBuilder::new(Arc::new(all_text))
            .with_format(fmt)
            .with_batch_size(10_000)
            .build(csv_input(f)?)?;
        if let Some(b) = r.next().transpose().with_context(|| format!("{}: not CSV", f.name))? {
            for i in text {
                let col = b.column(i).as_string::<i32>();
                let parse = |v: &str| match &o.timestampformat {
                    Some(fmt) => parse_format(v, fmt).or_else(|| parse_time(v)),
                    None => parse_time(v),
                };
                let mut seen = false;
                if col.iter().flatten().all(|v| {
                    seen = true;
                    parse(v).is_some()
                }) && seen
                {
                    let spec = o.timestampformat.clone().unwrap_or("auto".into());
                    fields[i] = timed(fields[i].clone(), Some(spec));
                }
            }
        }
    }
    Ok(Arc::new(arrow_schema::Schema::new(fields)))
}

/// A CSV reader's error with the column's name for its number, and the file's line (from 1,
/// counting the header and the lines skipped) for the reader's.
fn csv_error(f: &File, e: &str) -> String {
    let Ok(re) = regex::Regex::new(r"for column (\d+) at line (\d+)") else { return e.to_string() };
    let names = arrow_schema(f).map(|s| s.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>());
    re.replace(e, |c: &regex::Captures| {
        let col: usize = c[1].parse().unwrap_or(0);
        let line: usize = c[2].parse().unwrap_or(0) + 1 + f.options.skip;
        let name = names.as_ref().ok().and_then(|n| n.get(col).cloned()).unwrap_or_else(|| c[1].to_string());
        format!("for column {name} at line {line}")
    })
    .into_owned()
}

fn timed(f: arrow_schema::Field, time: Option<String>) -> arrow_schema::Field {
    match time {
        Some(spec) => {
            f.with_data_type(D::Utf8).with_metadata(std::collections::HashMap::from([(TIME.to_string(), spec)]))
        }
        None => f,
    }
}

/// The Arrow schema of a file: Parquet's own, CSV's and JSON's inferred from their first rows.
pub fn arrow_schema(f: &File) -> Result<SchemaRef> {
    match f.format {
        Format::Parquet => {
            if let Some(r) = &f.ranged {
                return Ok(r.meta.schema().clone());
            }
            let file = std::fs::File::open(&f.local).with_context(|| f.name.clone())?;
            let b = ParquetRecordBatchReaderBuilder::try_new(file)
                .with_context(|| format!("{}: not a Parquet file", f.name))?;
            Ok(b.schema().clone())
        }
        Format::Csv { delimiter } => csv_schema(f, delimiter),
        Format::Dbn => crate::dbn::schema(f),
        Format::Json => {
            let mut r = BufReader::new(open(f)?);
            let (s, _) = arrow_json::reader::infer_json_schema(&mut r, Some(10_000))
                .with_context(|| format!("{}: not JSON lines", f.name))?;
            // the keys in the order the first rows have them, and text that is all times a time
            let mut order: Vec<String> = vec![];
            let mut times: std::collections::HashMap<String, bool> = Default::default();
            for line in BufReader::new(open(f)?).lines().take(1000) {
                let Ok(serde_json::Value::Object(o)) = serde_json::from_str::<serde_json::Value>(&line?) else {
                    continue;
                };
                for (k, v) in o {
                    if !order.contains(&k) {
                        order.push(k.clone());
                    }
                    if let serde_json::Value::String(t) = v {
                        let ok = parse_time(&t).is_some();
                        times.entry(k).and_modify(|x| *x &= ok).or_insert(ok);
                    }
                }
            }
            let mut fields: Vec<arrow_schema::FieldRef> = s.fields().iter().cloned().collect();
            fields.sort_by_key(|f| order.iter().position(|k| k == f.name()).unwrap_or(usize::MAX));
            let fields: Vec<arrow_schema::FieldRef> = fields
                .into_iter()
                .map(|f| match (f.data_type(), times.get(f.name())) {
                    (D::Utf8, Some(true)) => Arc::new(timed(f.as_ref().clone(), Some("auto".into()))),
                    _ => f,
                })
                .collect();
            Ok(Arc::new(arrow_schema::Schema::new(fields)))
        }
    }
}

/// Rows written to a live table: JSON lines or CSV (with a header), in `schema`'s columns when
/// the table has them, else in the columns they infer (text that is all times a time).
pub fn parse_rows(body: &[u8], csv: bool, schema: Option<&SchemaRef>) -> Result<RecordBatch> {
    let schema = match schema {
        Some(s) => s.clone(),
        None if csv => {
            let (s, _) = arrow_csv::reader::Format::default().with_header(true).infer_schema(body, Some(10_000))?;
            Arc::new(s)
        }
        None => json_schema(body)?,
    };
    let mut batches = vec![];
    if csv {
        let r =
            arrow_csv::ReaderBuilder::new(schema.clone()).with_header(true).with_batch_size(BATCH_ROWS).build(body)?;
        for b in r {
            batches.push(b?);
        }
    } else {
        // a key the table does not have is an error (live::conform says which); the decoder
        // would drop it silently
        if let Ok(serde_json::Value::Object(o)) = body
            .split(|b| *b == b'\n')
            .find(|l| !l.is_empty())
            .map(serde_json::from_slice)
            .unwrap_or(Ok(serde_json::Value::Null))
        {
            if let Some(k) = o.keys().find(|k| schema.index_of(k).is_err()) {
                bail!(
                    "{k} is not a column of the table (it has {})",
                    schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ")
                );
            }
        }
        let r = arrow_json::ReaderBuilder::new(schema.clone()).with_batch_size(BATCH_ROWS).build(body)?;
        for b in r {
            batches.push(b?);
        }
    }
    Ok(arrow_select::concat::concat_batches(&schema, &batches)?)
}

/// The columns of JSON lines, in the order of their first appearance, text that is all times
/// a time.
pub fn json_schema(body: &[u8]) -> Result<SchemaRef> {
    let (s, _) = arrow_json::reader::infer_json_schema(&mut BufReader::new(body), Some(10_000))?;
    let mut order: Vec<String> = vec![];
    let mut times: std::collections::HashMap<String, bool> = Default::default();
    for line in body.split(|b| *b == b'\n').take(1000) {
        let Ok(serde_json::Value::Object(o)) = serde_json::from_slice::<serde_json::Value>(line) else { continue };
        for (k, v) in o {
            if !order.contains(&k) {
                order.push(k.clone());
            }
            if let serde_json::Value::String(t) = v {
                let ok = parse_time(&t).is_some();
                times.entry(k).and_modify(|x| *x &= ok).or_insert(ok);
            }
        }
    }
    let mut fields: Vec<arrow_schema::FieldRef> = s.fields().iter().cloned().collect();
    fields.sort_by_key(|f| order.iter().position(|k| k == f.name()).unwrap_or(usize::MAX));
    let fields: Vec<arrow_schema::FieldRef> = fields
        .into_iter()
        .map(|f| match (f.data_type(), times.get(f.name())) {
            (D::Utf8, Some(true)) => {
                Arc::new(f.as_ref().clone().with_data_type(D::Timestamp(TimeUnit::Microsecond, None)))
            }
            _ => f,
        })
        .collect();
    Ok(Arc::new(arrow_schema::Schema::new(fields)))
}

/// A time as text (`2024-01-01 00:00:10`, `2024-01-01T00:00:10.5Z`, `2024-01-01`, kdb+'s
/// `2024.01.02D09:30:00.123456789` and `2024.01.02T09:30:00.123`), in µs (a finer fraction
/// kept to the microsecond).
pub fn parse_time(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    let kdb;
    let s = if b.len() >= 10 && b[4] == b'.' && b[7] == b'.' && b[..4].iter().all(u8::is_ascii_digit) {
        kdb = format!("{}-{}-{}{}", &s[..4], s.get(5..7)?, s.get(8..10)?, s.get(10..)?.replacen('D', "T", 1));
        kdb.as_str()
    } else {
        s
    };
    let s = s.strip_suffix('Z').or_else(|| s.strip_suffix("+00:00")).unwrap_or(s);
    brrrrr_core::value::parse_datetime(s)
}

/// The rows decoded ahead that a source holds at most while every part still has some.
pub const AHEAD_BYTES: usize = 64 << 20;

/// What the decoding threads of one source and its parts share: per part, the units' rows
/// decoded so far (units finish out of order; each part reads them in order), and how much
/// they hold.
struct Shared {
    parts: Vec<PartQueue>,
    bytes: usize,
    /// Parts waiting for rows: while one does, the decoders never pause.
    waiting: usize,
    failed: Option<String>,
    /// How many units there are; `None`: until the decoders say.
    units: Option<usize>,
    /// The units decoded at most ahead of the part furthest behind (`Parallel::open`).
    window: Option<usize>,
}

#[derive(Default)]
struct PartQueue {
    next: usize,
    pending: BTreeMap<usize, (std::collections::VecDeque<Batch>, bool)>,
}

type Sync = Arc<(std::sync::Mutex<Shared>, std::sync::Condvar)>;

fn size_of_batch(b: &Batch) -> usize {
    b.len * b.cols.len().max(1) * 16
}

/// Puts rows of `unit` for each part (split as `parts` says), then waits while the source
/// holds more than `AHEAD_BYTES` and no part waits for rows. `false` once the source failed.
fn put(sync: &Sync, unit: usize, batches: Vec<Batch>, parts: Option<(usize, usize)>) -> bool {
    let (m, cv) = &**sync;
    let bytes: usize = batches.iter().map(size_of_batch).sum();
    let per: Vec<Vec<Batch>> = match parts {
        None => vec![batches],
        Some((key, n)) => {
            let mut per: Vec<Vec<Batch>> = (0..n).map(|_| vec![]).collect();
            for b in &batches {
                for (p, part) in brrrrr_core::query::split_batch(b, key, n).into_iter().enumerate() {
                    per[p].extend(part);
                }
            }
            per
        }
    };
    let mut st = m.lock().unwrap_or_else(|p| p.into_inner());
    for (q, rows) in st.parts.iter_mut().zip(per) {
        q.pending.entry(unit).or_default().0.extend(rows);
    }
    st.bytes += bytes;
    cv.notify_all();
    while st.window.is_none() && st.bytes > AHEAD_BYTES && st.waiting == 0 && st.failed.is_none() {
        st = cv.wait(st).unwrap_or_else(|p| p.into_inner());
    }
    st.failed.is_none()
}

/// Waits until `unit` is within the window of the part furthest behind (`Parallel::open`).
/// `false` once the source failed.
fn turn(sync: &Sync, unit: usize) -> bool {
    let (m, cv) = &**sync;
    let mut st = m.lock().unwrap_or_else(|p| p.into_inner());
    while st.failed.is_none()
        && st.window.is_some_and(|w| unit >= st.parts.iter().map(|q| q.next).min().unwrap_or(0).saturating_add(w))
    {
        st = cv.wait(st).unwrap_or_else(|p| p.into_inner());
    }
    st.failed.is_none()
}

/// Marks `unit` read whole, or the source failed.
fn end(sync: &Sync, unit: usize, failed: Option<String>) {
    let (m, cv) = &**sync;
    let mut st = m.lock().unwrap_or_else(|p| p.into_inner());
    match failed {
        Some(e) => st.failed = Some(e),
        None => st.parts.iter_mut().for_each(|q| q.pending.entry(unit).or_default().1 = true),
    }
    cv.notify_all();
}

/// Rows of a row group decoded as one unit: a larger group is read in slices of these (the
/// pages before a slice are skipped, not decoded), so that the units decoded side by side, ahead
/// of the query, hold few rows.
const SLICE_ROWS: usize = 4 * BATCH_ROWS;

/// Rows `rows` of row group `group` of a Parquet file: what a decoder decodes at a time.
struct Unit {
    file: File,
    meta: ArrowReaderMetadata,
    group: usize,
    rows: std::ops::Range<usize>,
}

/// A table's Parquet row groups decoded on `threads` threads (thread `t` of `k` decodes units
/// `t`, `t + k`, ...), given in order. With `parts` (a key column and a count), the rows are
/// split among that many parts (`query::parts_of`) as they are decoded, each part a source of
/// its own. A unit is a slice of a row group (`SLICE_ROWS`). How far ahead of the query the
/// decoders run is bounded as `open` says.
pub struct Parallel {
    sync: Sync,
    me: usize,
}

impl Parallel {
    fn shared(n: usize, units: Option<usize>, window: Option<usize>) -> Sync {
        let st = Shared {
            parts: (0..n).map(|_| PartQueue::default()).collect(),
            bytes: 0,
            waiting: 0,
            failed: None,
            units,
            window,
        };
        Arc::new((std::sync::Mutex::new(st), std::sync::Condvar::new()))
    }

    ///
    /// `alone`: the query reads no other source. Its decoders then stay within `threads` units
    /// of the part furthest behind, which bounds what they hold to that many units' rows
    /// however much faster than the query they decode. A part reading two sources (a join) may
    /// wait on one while it holds the other's rows: those keep to `AHEAD_BYTES`, passed while
    /// any part waits, which never stalls one part on another.
    pub fn open(
        files: Vec<File>,
        columns: Vec<(String, Type)>,
        read: Vec<bool>,
        threads: usize,
        parts: Option<(usize, usize)>,
        alone: bool,
    ) -> Result<Vec<Parallel>> {
        let mut units = vec![];
        for f in &files {
            let meta = match &f.ranged {
                Some(r) => r.meta.clone(),
                None => {
                    let file = std::fs::File::open(&f.local).with_context(|| f.name.clone())?;
                    ArrowReaderMetadata::load(&file, Default::default())
                        .with_context(|| format!("{}: not a Parquet file", f.name))?
                }
            };
            for (g, rg) in meta.metadata().row_groups().iter().enumerate() {
                let n = rg.num_rows() as usize;
                for at in (0..n).step_by(SLICE_ROWS) {
                    units.push(Unit {
                        file: f.clone(),
                        meta: meta.clone(),
                        group: g,
                        rows: at..n.min(at + SLICE_ROWS),
                    });
                }
            }
        }
        let n = parts.map_or(1, |p| p.1);
        let threads = threads.clamp(1, units.len().max(1));
        let sync = Parallel::shared(n, Some(units.len()), alone.then_some(threads));
        let units = Arc::new(units);
        let layout = Arc::new(Layout { columns, read });
        for t in 0..threads {
            let (units, layout, sync) = (units.clone(), layout.clone(), sync.clone());
            std::thread::Builder::new().name(format!("decode-{t}")).spawn(move || {
                for (u, unit) in units.iter().enumerate().skip(t).step_by(threads) {
                    if !turn(&sync, u) {
                        return;
                    }
                    match layout.rows(unit, &mut |b| put(&sync, u, vec![b], parts)) {
                        Ok(true) => end(&sync, u, None),
                        Ok(false) => return,
                        Err(e) => return end(&sync, u, Some(format!("{e:#}"))),
                    }
                }
            })?;
        }
        Ok((0..n).map(|me| Parallel { sync: sync.clone(), me }).collect())
    }

    /// `src` read on a thread of its own and split among `parts` (key column, count) as it is.
    pub fn fan_out(mut src: Box<dyn Source + Send>, parts: (usize, usize)) -> Result<Vec<Parallel>> {
        let sync = Parallel::shared(parts.1, None, None);
        let s = sync.clone();
        std::thread::Builder::new().name("split".into()).spawn(move || {
            // each batch a unit, in order
            let mut u = 0;
            while let Some(b) = src.next() {
                match b {
                    Ok(b) => {
                        if !put(&s, u, vec![b], Some(parts)) {
                            return;
                        }
                        end(&s, u, None);
                        u += 1;
                    }
                    Err(e) => return end(&s, u, Some(e)),
                }
            }
            let (m, cv) = &*s;
            m.lock().unwrap_or_else(|p| p.into_inner()).units = Some(u);
            cv.notify_all();
        })?;
        Ok((0..parts.1).map(|me| Parallel { sync: sync.clone(), me }).collect())
    }
}

/// A part no longer read (its query failed, or it ended) holds no decoder back.
impl Drop for Parallel {
    fn drop(&mut self) {
        let (m, cv) = &*self.sync;
        let mut st = m.lock().unwrap_or_else(|p| p.into_inner());
        st.parts[self.me].next = usize::MAX;
        st.parts[self.me].pending.clear();
        cv.notify_all();
    }
}

impl Source for Parallel {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        let (m, cv) = &*self.sync;
        let mut st = m.lock().unwrap_or_else(|p| p.into_inner());
        let me = self.me;
        loop {
            if let Some(e) = &st.failed {
                return Some(Err(e.clone()));
            }
            let next = st.parts[me].next;
            if st.units.is_some_and(|u| next >= u) {
                return None;
            }
            let mut got = None;
            let q = &mut st.parts[me];
            if let Some((rows, done)) = q.pending.get_mut(&next) {
                match rows.pop_front() {
                    Some(b) => got = Some(b),
                    None if *done => {
                        q.pending.remove(&next);
                        q.next += 1;
                        // a decoder may wait for the parts to move on
                        cv.notify_all();
                        continue;
                    }
                    None => {}
                }
            }
            if let Some(b) = got {
                st.bytes = st.bytes.saturating_sub(size_of_batch(&b));
                cv.notify_all();
                return Some(Ok(b));
            }
            st.waiting += 1;
            cv.notify_all();
            st = cv.wait(st).unwrap_or_else(|p| p.into_inner());
            st.waiting -= 1;
        }
    }
}

/// One table's files read in turn as a stream's batches: its `columns` (the stream's, in
/// order), those not `read` left NULL.
pub struct Reader {
    files: std::vec::IntoIter<File>,
    layout: Layout,
    /// The file read, its batches, and the rows read of it.
    current: Option<(File, Batches, usize)>,
}

/// A file's Arrow batches as its reader gives them.
type Batches = Box<dyn Iterator<Item = Result<RecordBatch, arrow_schema::ArrowError>> + Send>;

/// A stream's columns, and which of them are read: how a file's rows become its batches.
struct Layout {
    columns: Vec<(String, Type)>,
    read: Vec<bool>,
}

impl Reader {
    pub fn new(files: Vec<File>, columns: Vec<(String, Type)>, read: Vec<bool>) -> Reader {
        Reader { files: files.into_iter(), layout: Layout { columns, read }, current: None }
    }

    fn open_next(&mut self) -> Result<bool> {
        let Some(f) = self.files.next() else { return Ok(false) };
        let wanted: Vec<&str> =
            self.layout.columns.iter().zip(&self.layout.read).filter(|(_, r)| **r).map(|(c, _)| c.0.as_str()).collect();
        let it: Batches = match f.format {
            Format::Parquet => {
                let b = ParquetRecordBatchReaderBuilder::try_new(crate::ranged::Input::of(&f, None)?)
                    .with_context(|| format!("{}: not a Parquet file", f.name))?;
                let schema = b.parquet_schema();
                let leaves: Vec<usize> =
                    (0..schema.num_columns()).filter(|&j| wanted.contains(&schema.column(j).name())).collect();
                let mask = ProjectionMask::leaves(schema, leaves);
                Box::new(b.with_projection(mask).with_batch_size(BATCH_ROWS).build()?)
            }
            Format::Csv { delimiter } => {
                let s = arrow_schema(&f)?;
                let fmt = csv_format(&f, delimiter, csv_header(&f, delimiter)?)?;
                Box::new(
                    arrow_csv::ReaderBuilder::new(s)
                        .with_format(fmt)
                        .with_batch_size(BATCH_ROWS)
                        .build(csv_input(&f)?)?,
                )
            }
            Format::Json => {
                let s = arrow_schema(&f)?;
                Box::new(
                    arrow_json::ReaderBuilder::new(s).with_batch_size(BATCH_ROWS).build(BufReader::new(open(&f)?))?,
                )
            }
            Format::Dbn => Box::new(crate::dbn::batches(&f, BATCH_ROWS)?),
        };
        self.current = Some((f, it, 0));
        Ok(true)
    }
}

impl Layout {
    /// A unit's rows of a Parquet file, decoded, each batch to `send` as it is; `false` if
    /// `send` stopped.
    fn rows(&self, u: &Unit, send: &mut dyn FnMut(Batch) -> bool) -> Result<bool> {
        let f = &u.file;
        let wanted: Vec<&str> =
            self.columns.iter().zip(&self.read).filter(|(_, r)| **r).map(|(c, _)| c.0.as_str()).collect();
        let schema = u.meta.parquet_schema();
        let leaves: Vec<usize> =
            (0..schema.num_columns()).filter(|&j| wanted.contains(&schema.column(j).name())).collect();
        // a remote file's: the chunks of these leaves of this row group, fetched
        let input = crate::ranged::Input::of(f, Some((u.group, &leaves)))?;
        let b = ParquetRecordBatchReaderBuilder::new_with_metadata(input, u.meta.clone());
        let schema = b.parquet_schema();
        let mask = ProjectionMask::leaves(schema, leaves);
        let rows = RowSelection::from(vec![RowSelector::skip(u.rows.start), RowSelector::select(u.rows.len())]);
        let b = b.with_projection(mask).with_row_groups(vec![u.group]).with_row_selection(rows);
        let mut at = u.rows.start;
        for rb in b.with_batch_size(BATCH_ROWS).build()? {
            let rb = rb?;
            let n = rb.num_rows();
            if !send(self.batch(f, &rb, at)?) {
                return Ok(false);
            }
            at += n;
        }
        Ok(true)
    }

    /// A file's rows as the stream's columns: each by name (NULL where the file has none), of
    /// its type (a time from text, as `TIME` says; another type cast, or an error that names
    /// the file and column). `at`: the rows of the file before these.
    fn batch(&self, f: &File, rb: &RecordBatch, at: usize) -> Result<Batch> {
        let n = rb.num_rows();
        let schema = rb.schema();
        let cols = self
            .columns
            .iter()
            .zip(&self.read)
            .map(|((name, ty), read)| {
                if !read {
                    return Ok(Arc::new(Col::new(Data::Const(Value::Null, n))));
                }
                if let Some(v) = partition(f, name) {
                    if schema.index_of(name).is_err() {
                        return Ok(Arc::new(Col::new(Data::Const(partition_value(v, ty), n))));
                    }
                }
                match schema.index_of(name) {
                    Ok(i) => {
                        let field = schema.field(i);
                        let mut a = rb.column(i).clone();
                        if let Some(spec) = field.metadata().get(TIME) {
                            // its line: the header's and the skipped ones before the rows
                            let line = at + 1 + f.options.skip + usize::from(matches!(f.format, Format::Csv { .. }));
                            a = to_times(&a, spec, line, name).with_context(|| f.name.clone())?;
                        } else if let (Some(have), Some(want)) = (engine_type(a.data_type()), arrow_type(ty)) {
                            if have != *ty.base() {
                                // a number widened, or anything as text; not a value lost
                                let num = |t: &Type| matches!(t, Type::Int(_) | Type::UInt(_));
                                let widens = matches!(ty.base(), Type::Str | Type::F64)
                                    && (num(&have) || *ty.base() == Type::Str)
                                    || num(&have) && num(ty.base());
                                let lost = || {
                                    anyhow!(
                                        "{}: column {name} is {} here and {} in the table's first file: read the \
                                         files with union_by_name = true (read_parquet('...', union_by_name = true))",
                                        f.name,
                                        crate::type_name(&have),
                                        crate::type_name(ty)
                                    )
                                };
                                if !widens {
                                    return Err(lost());
                                }
                                let safe = arrow_cast::CastOptions { safe: false, ..Default::default() };
                                a = arrow_cast::cast_with_options(&a, &want, &safe).map_err(|e| lost().context(e))?;
                            }
                        }
                        column(&a, ty).with_context(|| format!("{}: column {name}", f.name)).map(Arc::new)
                    }
                    // a file of the table without the column: NULL
                    Err(_) => Ok(Arc::new(Col::new(Data::Const(Value::Null, n)))),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Batch::new(n, cols))
    }
}

impl Source for Reader {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        loop {
            if self.current.is_none() {
                match self.open_next() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(e) => return Some(Err(format!("{e:#}"))),
                }
            }
            let (f, it, at) = self.current.as_mut().expect("opened");
            match it.next() {
                Some(Ok(rb)) => {
                    let (f, from) = (f.clone(), *at);
                    *at += rb.num_rows();
                    return Some(self.layout.batch(&f, &rb, from).map_err(|e| format!("{e:#}")));
                }
                Some(Err(e)) => return Some(Err(format!("{}: {}", f.name, csv_error(f, &e.to_string())))),
                None => self.current = None,
            }
        }
    }
}

/// A table held in memory (a DataFrame registered from Python): its Arrow batches as a stream's.
pub struct Memory {
    batches: std::vec::IntoIter<RecordBatch>,
    layout: Layout,
    name: String,
}

impl Memory {
    pub fn new(name: &str, batches: Vec<RecordBatch>, columns: Vec<(String, Type)>, read: Vec<bool>) -> Memory {
        Memory { batches: batches.into_iter(), layout: Layout { columns, read }, name: name.into() }
    }
}

impl Source for Memory {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        let rb = self.batches.next()?;
        let f = File {
            name: self.name.clone(),
            local: Default::default(),
            format: crate::files::Format::Parquet,
            gzip: false,
            partitions: vec![],
            object: None,
            options: Default::default(),
            lease: None,
            ranged: None,
        };
        Some(self.layout.batch(&f, &rb, 0).map_err(|e| format!("{e:#}")))
    }
}

/// One source, then another (a live table's history, then its newest rows).
pub struct Chain<'a>(pub Option<Box<dyn Source + Send + 'a>>, pub Box<dyn Source + Send + 'a>);

impl Source for Chain<'_> {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        if let Some(first) = self.0.as_mut() {
            match first.next() {
                Some(b) => return Some(b),
                None => self.0 = None,
            }
        }
        self.1.next()
    }
}

/// An Arrow batch as the engine's rows, in `columns` (by name; a missing one NULL).
pub fn rows_of(batch: &RecordBatch, columns: &[(String, Type)]) -> Result<Vec<Vec<Value>>> {
    let schema = batch.schema();
    let cols: Vec<Col> = columns
        .iter()
        .map(|(n, t)| match schema.index_of(n) {
            Ok(i) => column(batch.column(i), t),
            Err(_) => Ok(Col::new(Data::Const(Value::Null, batch.num_rows()))),
        })
        .collect::<Result<_>>()?;
    Ok((0..batch.num_rows()).map(|r| cols.iter().map(|c| c.get(r)).collect()).collect())
}

/// The columns brrrrr reads of an Arrow schema, and their types.
pub fn columns_of(s: &arrow_schema::Schema) -> Vec<(String, Type)> {
    s.fields().iter().filter_map(|f| engine_type(f.data_type()).map(|t| (f.name().clone(), t))).collect()
}

/// Rows of a source held in memory and put in the order of a column (a table whose files are
/// not in time order).
pub struct Sorted(std::vec::IntoIter<Batch>);

impl Sorted {
    pub fn new(mut src: Box<dyn Source + '_>, width: usize, clock: usize) -> Result<Sorted> {
        let mut rows = vec![];
        while let Some(b) = src.next() {
            rows.extend(b.map_err(|e| anyhow!(e))?.rows());
        }
        // NULL times last; equal times keep their order
        rows.sort_by_key(|r: &Vec<Value>| r[clock].i64().unwrap_or(i64::MAX));
        let batches: Vec<Batch> = rows.chunks(BATCH_ROWS).map(|c| Batch::from_rows(c, width)).collect();
        Ok(Sorted(batches.into_iter()))
    }
}

impl Source for Sorted {
    fn next(&mut self) -> Option<Result<Batch, String>> {
        self.0.next().map(Ok)
    }
}

/// An Arrow array as an engine column of `ty`.
pub fn column(a: &ArrayRef, ty: &Type) -> Result<Col> {
    // dictionary text (Parquet's usual symbols): each row's string copied once, from its entry
    if let (D::Dictionary(k, v), Type::Str) = (a.data_type(), ty.base()) {
        if matches!(v.as_ref(), D::Utf8)
            && matches!(k.as_ref(), D::Int32 | D::Int16 | D::Int8 | D::UInt32 | D::UInt16 | D::UInt8)
        {
            let keys = arrow_cast::cast(a.as_any_dictionary().keys(), &D::Int64)?;
            let keys = keys.as_primitive::<Int64Type>();
            let values = a.as_any_dictionary().values().clone();
            let values = values.as_string::<i32>();
            let nulls: Option<Vec<bool>> =
                a.nulls().filter(|n| n.null_count() != 0).map(|n| (0..a.len()).map(|i| n.is_null(i)).collect());
            // the rows as the dictionary's codes: its strings once (a NULL row's code is any)
            let mut dictionary: Strs = values.iter().map(|v| v.unwrap_or("")).collect();
            if dictionary.is_empty() {
                dictionary = [""].into_iter().collect();
            }
            let null = |i: usize| nulls.as_ref().is_some_and(|n| n[i]);
            let codes = (keys.values().iter().enumerate())
                .map(|(i, k)| if null(i) { 0 } else { u32::try_from(*k).unwrap_or(u32::MAX) })
                .collect();
            let strs = Strs::from_dictionary(&dictionary, codes).map_err(|e| anyhow!(e))?;
            return Ok(Col { data: Data::Str(strs), nulls: nulls.map(Into::into) });
        }
    }
    let a = match a.data_type() {
        D::Dictionary(..) | D::Utf8View | D::Float16 | D::Decimal32(..) | D::Decimal64(..) | D::Decimal128(..) => {
            let to = match ty.base() {
                Type::F64 => D::Float64,
                _ => D::Utf8,
            };
            arrow_cast::cast(a, &to)?
        }
        _ => a.clone(),
    };
    let nulls = a.nulls().filter(|n| n.null_count() != 0).map(|n| (0..a.len()).map(|i| n.is_null(i)).collect());
    let ints = |v: Vec<i64>| Data::Int(v.into());
    let data = match a.data_type() {
        D::Boolean => Data::Bool(a.as_boolean().iter().map(|x| x.unwrap_or(false)).collect()),
        D::Int8 => ints(a.as_primitive::<Int8Type>().values().iter().map(|x| *x as i64).collect()),
        D::Int16 => ints(a.as_primitive::<Int16Type>().values().iter().map(|x| *x as i64).collect()),
        D::Int32 => ints(a.as_primitive::<Int32Type>().values().iter().map(|x| *x as i64).collect()),
        D::Int64 => ints(a.as_primitive::<Int64Type>().values().to_vec()),
        D::UInt8 => Data::UInt(a.as_primitive::<UInt8Type>().values().iter().map(|x| *x as u64).collect()),
        D::UInt16 => Data::UInt(a.as_primitive::<UInt16Type>().values().iter().map(|x| *x as u64).collect()),
        D::UInt32 => Data::UInt(a.as_primitive::<UInt32Type>().values().iter().map(|x| *x as u64).collect()),
        D::UInt64 => Data::UInt(a.as_primitive::<UInt64Type>().values().to_vec().into()),
        D::Float32 => Data::F64(a.as_primitive::<Float32Type>().values().iter().map(|x| *x as f64).collect()),
        D::Float64 => Data::F64(a.as_primitive::<Float64Type>().values().to_vec().into()),
        D::Utf8 => {
            let s = a.as_string::<i32>();
            let o = s.value_offsets();
            let (lo, hi) = (o[0] as usize, o[o.len() - 1] as usize);
            let offsets = o.iter().map(|x| (*x - o[0]) as u32).collect();
            Data::Str(Strs::from_parts(offsets, s.values()[lo..hi].to_vec()).map_err(|e| anyhow!(e))?)
        }
        D::LargeUtf8 => Data::Str(a.as_string::<i64>().iter().map(|x| x.unwrap_or("")).collect()),
        D::Timestamp(unit, _) => {
            let raw = arrow_cast::cast(&a, &D::Int64)?;
            let v = raw.as_primitive::<Int64Type>().values();
            let v: Vec<i64> = match unit {
                TimeUnit::Second => v.iter().map(|x| x.saturating_mul(1_000_000)).collect(),
                TimeUnit::Millisecond => v.iter().map(|x| x.saturating_mul(1_000)).collect(),
                TimeUnit::Microsecond => v.to_vec(),
                // ponytail: nanoseconds kept to the microsecond, the engine's unit
                TimeUnit::Nanosecond => v.iter().map(|x| x.div_euclid(1_000)).collect(),
            };
            Data::Time(v.into())
        }
        D::Date32 => {
            Data::Time(a.as_primitive::<Date32Type>().values().iter().map(|d| *d as i64 * 86_400_000_000).collect())
        }
        D::Date64 => Data::Time(a.as_primitive::<Date64Type>().values().iter().map(|d| d * 1_000).collect()),
        D::Null => Data::Const(Value::Null, a.len()),
        t => bail!("{t} is not a type brrrrr reads"),
    };
    // the column as its table types it (an Int64 file column of a table whose first file has
    // floats)
    let col = Col { data, nulls };
    Ok(match (ty.base(), &col.data) {
        (Type::F64, Data::Int(_) | Data::UInt(_)) | (Type::Str, Data::Int(_) | Data::F64(_) | Data::Bool(_)) => {
            Col::from_values((0..col.len()).map(|i| col.get(i).cast(ty)).collect())
        }
        _ => col,
    })
}

/// A CSV file's delimiter: the most frequent of `,`, tab, `;` and `|` in its first line.
pub fn sniff_delimiter(mut r: impl BufRead) -> u8 {
    let mut line = String::new();
    let _ = r.read_line(&mut line);
    [b',', b'\t', b';', b'|'].into_iter().max_by_key(|d| line.bytes().filter(|b| b == d).count()).unwrap_or(b',')
}

/// A file's first `n` bytes, gunzipped if it is gzipped (to sniff its format).
pub fn peek(f: &File, n: usize) -> Result<Vec<u8>> {
    let mut r = open(f)?;
    let mut buf = vec![0; n];
    let mut got = 0;
    while got < n {
        let k = r.read(&mut buf[got..])?;
        if k == 0 {
            break;
        }
        got += k;
    }
    buf.truncate(got);
    Ok(buf)
}
