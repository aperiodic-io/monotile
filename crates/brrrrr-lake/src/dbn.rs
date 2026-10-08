//! Databento's binary encoding (DBN, versions 1 to 3), zstd-compressed or not: a metadata
//! header, then fixed little-endian records. The market data schemas are read as Databento's own
//! CSV and Parquet exports have them: their columns in its order and names, times read to the
//! microsecond, prices as numbers (fixed-point 1e-9, as its `pretty_px`), and `symbol`, each
//! record's instrument as the header's symbology maps it.
use crate::files::File;
use anyhow::{anyhow, bail, Context, Result};
use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType as D, Field, Schema, SchemaRef, TimeUnit};
use std::collections::HashMap;
use std::io::{BufRead, Read};
use std::sync::Arc;

/// A column of a record: where it is, and what it is.
#[derive(Clone, Copy)]
enum Kind {
    /// Nanoseconds since the epoch (u64; `u64::MAX` is none).
    Ts,
    /// A price in units of 1e-9 (i64; `i64::MAX` is none).
    Px,
    U8,
    U16,
    U32,
    U64,
    I32,
    /// An ASCII character (an action, a side).
    Char,
}

/// A schema's records: their columns (name, kind, offset), and whether a record's day for its
/// symbol is that of `ts_recv` (its index time) rather than `ts_event`.
struct Layout {
    columns: Vec<(String, Kind, usize)>,
    recv: bool,
}

fn layout(rtype: u8) -> Result<Layout> {
    use Kind::*;
    let col = |n: &str, k: Kind, at: usize| (n.to_string(), k, at);
    let head = |ts_recv: Option<usize>| {
        let mut c = vec![];
        if let Some(at) = ts_recv {
            c.push(col("ts_recv", Ts, at));
        }
        c.extend([
            col("ts_event", Ts, 8),
            col("rtype", U8, 1),
            col("publisher_id", U16, 2),
            col("instrument_id", U32, 4),
        ]);
        c
    };
    Ok(match rtype {
        // trades (mbp-0), mbp-1 and tbbo, mbp-10
        0x00 | 0x01 | 0x0A => {
            let mut c = head(Some(32));
            c.extend([
                col("action", Char, 28),
                col("side", Char, 29),
                col("depth", U8, 31),
                col("price", Px, 16),
                col("size", U32, 24),
                col("flags", U8, 30),
                col("ts_in_delta", I32, 40),
                col("sequence", U32, 44),
            ]);
            let levels = match rtype {
                0x01 => 1,
                0x0A => 10,
                _ => 0,
            };
            for i in 0..levels {
                let at = 48 + 32 * i;
                c.extend([
                    col(&format!("bid_px_{i:02}"), Px, at),
                    col(&format!("ask_px_{i:02}"), Px, at + 8),
                    col(&format!("bid_sz_{i:02}"), U32, at + 16),
                    col(&format!("ask_sz_{i:02}"), U32, at + 20),
                    col(&format!("bid_ct_{i:02}"), U32, at + 24),
                    col(&format!("ask_ct_{i:02}"), U32, at + 28),
                ]);
            }
            Layout { columns: c, recv: true }
        }
        0xA0 => {
            let mut c = head(Some(40));
            c.extend([
                col("action", Char, 38),
                col("side", Char, 39),
                col("price", Px, 24),
                col("size", U32, 32),
                col("channel_id", U8, 37),
                col("order_id", U64, 16),
                col("flags", U8, 36),
                col("ts_in_delta", I32, 48),
                col("sequence", U32, 52),
            ]);
            Layout { columns: c, recv: true }
        }
        // ohlcv-1s, -1m, -1h, -1d, -eod (and the deprecated one)
        0x11 | 0x20..=0x24 => {
            let mut c = head(None);
            c.extend([
                col("open", Px, 16),
                col("high", Px, 24),
                col("low", Px, 32),
                col("close", Px, 40),
                col("volume", U64, 48),
            ]);
            Layout { columns: c, recv: false }
        }
        r => bail!(
            "records of type {r:#04x}: brrrrr reads DBN's trades, tbbo, mbp-1, mbp-10, mbo and ohlcv schemas \
             (not definitions, statistics, status or imbalance yet)"
        ),
    })
}

/// The record type of a schema in the header (`None`: mixed, the first record's).
fn rtype_of(schema: u16) -> Option<u8> {
    Some(match schema {
        0 => 0xA0,        // mbo
        1 | 3 => 0x01,    // mbp-1, tbbo
        2 => 0x0A,        // mbp-10
        4 => 0x00,        // trades
        5 => 0x20,        // ohlcv-1s
        6 => 0x21,        // ohlcv-1m
        7 => 0x22,        // ohlcv-1h
        8 => 0x23,        // ohlcv-1d
        13 => 0x24,       // ohlcv-eod
        _ => return None, // mixed, or another
    })
}

/// What the header says: the schema's record type, and each instrument's raw symbols by date
/// (YYYYMMDD, from inclusive, to exclusive).
struct Header {
    rtype: Option<u8>,
    symbols: HashMap<u32, Vec<(u32, u32, String)>>,
}

/// A DBN file's bytes: zstd-compressed (by its magic number) or not.
fn input(f: &File) -> Result<Box<dyn BufRead + Send>> {
    let mut r = crate::read::open(f)?;
    if r.fill_buf()?.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        return Ok(Box::new(std::io::BufReader::new(zstd::stream::read::Decoder::with_buffer(r)?)));
    }
    Ok(r)
}

fn header(r: &mut impl Read, name: &str) -> Result<Header> {
    let mut pre = [0u8; 8];
    r.read_exact(&mut pre).with_context(|| format!("{name}: not a DBN file"))?;
    if &pre[..3] != b"DBN" {
        bail!("{name}: not a DBN file (it does not begin with DBN)");
    }
    let version = pre[3];
    if !(1..=3).contains(&version) {
        bail!("{name}: DBN version {version}; brrrrr reads versions 1 to 3");
    }
    let mut m = vec![0; u32::from_le_bytes(pre[4..8].try_into()?) as usize];
    r.read_exact(&mut m).with_context(|| format!("{name}: its DBN header cut short"))?;
    let short = || anyhow!("{name}: its DBN header cut short");
    let u16_at = |at: usize| m.get(at..at + 2).map(|b| u16::from_le_bytes([b[0], b[1]]));
    let u32_at = |at: usize| m.get(at..at + 4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));
    let schema = u16_at(16).ok_or_else(short)?;
    // dataset (16), schema (2), start, end and limit (8 each), version 1's record count (8)
    let mut at = 42 + if version == 1 { 8 } else { 0 };
    let stype_out = *m.get(at + 1).ok_or_else(short)?;
    at += 3;
    let cstr = if version == 1 {
        22
    } else {
        at += 2;
        u16_at(at - 2).ok_or_else(short)? as usize
    };
    at += if version == 1 { 47 } else { 53 }; // reserved
    at += 4 + u32_at(at).ok_or_else(short)? as usize; // the schema definition: none
    let text = |at: usize| -> Result<String> {
        let b = m.get(at..at + cstr).ok_or_else(short)?;
        Ok(String::from_utf8_lossy(b).trim_end_matches('\0').to_string())
    };
    // symbols, partial, not found: lists of symbols
    for _ in 0..3 {
        at += 4 + u32_at(at).ok_or_else(short)? as usize * cstr;
    }
    let mut symbols: HashMap<u32, Vec<(u32, u32, String)>> = HashMap::new();
    let mappings = u32_at(at).ok_or_else(short)?;
    at += 4;
    for _ in 0..mappings {
        let raw = text(at)?;
        let n = u32_at(at + cstr).ok_or_else(short)?;
        at += cstr + 4;
        for _ in 0..n {
            let (from, to) = (u32_at(at).ok_or_else(short)?, u32_at(at + 4).ok_or_else(short)?);
            let symbol = text(at + 8)?;
            at += 8 + cstr;
            // instrument IDs out (stype_out 0): an interval's symbol is the record's ID
            if let (0, Ok(id)) = (stype_out, symbol.parse::<u32>()) {
                symbols.entry(id).or_default().push((from, to, raw.clone()));
            }
        }
    }
    Ok(Header { rtype: rtype_of(schema), symbols })
}

/// A DBN file's columns: its schema's (that of its first record, for a file of several).
pub fn schema(f: &File) -> Result<SchemaRef> {
    let mut r = input(f)?;
    let h = header(&mut r, &f.name)?;
    let rtype = match h.rtype {
        Some(t) => t,
        None => {
            let mut hd = [0u8; 2];
            r.read_exact(&mut hd)
                .with_context(|| format!("{}: a DBN file of several schemas, and no record", f.name))?;
            hd[1]
        }
    };
    Ok(arrow(&layout(rtype)?, !h.symbols.is_empty()))
}

fn arrow(l: &Layout, symbol: bool) -> SchemaRef {
    let mut fields: Vec<Field> = l
        .columns
        .iter()
        .map(|(n, k, _)| {
            let ty = match k {
                Kind::Ts => D::Timestamp(TimeUnit::Nanosecond, None),
                Kind::Px => D::Float64,
                Kind::U64 => D::UInt64,
                Kind::Char => D::Utf8,
                _ => D::Int64,
            };
            Field::new(n, ty, true)
        })
        .collect();
    if symbol {
        fields.push(Field::new("symbol", D::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

/// A DBN file's records as Arrow batches of `rows` rows. Records of another type than its
/// schema's (a stream's system or error messages) are left out.
pub fn batches(f: &File, rows: usize) -> Result<Batches> {
    let mut r = input(f)?;
    let h = header(&mut r, &f.name)?;
    let schema = schema(f)?;
    let rtype = h.rtype;
    Ok(Batches { r, h, rtype, schema, rows, name: f.name.clone(), done: false })
}

pub struct Batches {
    r: Box<dyn BufRead + Send>,
    h: Header,
    /// The schema's record type (the first record's, for a file of several).
    rtype: Option<u8>,
    schema: SchemaRef,
    rows: usize,
    name: String,
    done: bool,
}

/// A day's YYYYMMDD of nanoseconds since the epoch.
fn yyyymmdd(ns: u64) -> u32 {
    // Howard Hinnant's civil_from_days
    let z = (ns / 86_400_000_000_000) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y * 10_000 + m * 100 + d) as u32
}

impl Batches {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        let mut rec = vec![0u8; 512];
        let mut got: Vec<Vec<u8>> = Vec::with_capacity(self.rows.min(1 << 16));
        while got.len() < self.rows {
            let mut len = [0u8; 1];
            if self.r.read(&mut len)? == 0 {
                self.done = true;
                break;
            }
            let n = len[0] as usize * 4;
            if n < 16 {
                bail!("{}: a DBN record of {n} bytes", self.name);
            }
            rec.resize(n, 0);
            rec[0] = len[0];
            self.r
                .read_exact(&mut rec[1..n])
                .with_context(|| format!("{}: its last DBN record cut short", self.name))?;
            let rtype = *self.rtype.get_or_insert(rec[1]);
            if rec[1] == rtype {
                got.push(rec[..n].to_vec());
            }
        }
        if got.is_empty() {
            return Ok(None);
        }
        let l = layout(self.rtype.unwrap_or(0))?;
        let le = |r: &[u8], at: usize, w: usize| -> u64 {
            r.get(at..at + w).map_or(0, |b| b.iter().rev().fold(0u64, |a, x| (a << 8) | u64::from(*x)))
        };
        let mut cols: Vec<ArrayRef> = vec![];
        for (_, k, at) in &l.columns {
            let (at, k) = (*at, *k);
            cols.push(match k {
                Kind::Ts => {
                    let mut b = TimestampNanosecondBuilder::with_capacity(got.len());
                    got.iter().for_each(|r| match le(r, at, 8) {
                        u64::MAX => b.append_null(),
                        t => b.append_value(t as i64),
                    });
                    Arc::new(b.finish())
                }
                Kind::Px => {
                    let mut b = Float64Builder::with_capacity(got.len());
                    got.iter().for_each(|r| match le(r, at, 8) as i64 {
                        i64::MAX => b.append_null(),
                        p => b.append_value(p as f64 / 1e9),
                    });
                    Arc::new(b.finish())
                }
                Kind::U64 => {
                    let mut b = UInt64Builder::with_capacity(got.len());
                    got.iter().for_each(|r| b.append_value(le(r, at, 8)));
                    Arc::new(b.finish())
                }
                Kind::Char => {
                    let mut b = StringBuilder::with_capacity(got.len(), got.len());
                    got.iter().for_each(|r| match r.get(at).copied().unwrap_or(0) {
                        0 => b.append_null(),
                        c => b.append_value((c as char).to_string()),
                    });
                    Arc::new(b.finish())
                }
                k => {
                    let mut b = Int64Builder::with_capacity(got.len());
                    got.iter().for_each(|r| {
                        b.append_value(match k {
                            Kind::U8 => le(r, at, 1) as i64,
                            Kind::U16 => le(r, at, 2) as i64,
                            Kind::I32 => le(r, at, 4) as u32 as i32 as i64,
                            _ => le(r, at, 4) as i64,
                        })
                    });
                    Arc::new(b.finish())
                }
            });
        }
        if !self.h.symbols.is_empty() {
            let mut b = StringBuilder::with_capacity(got.len(), got.len() * 4);
            for r in &got {
                let id = le(r, 4, 4) as u32;
                let day = || yyyymmdd(le(r, if l.recv { l.columns[0].2 } else { 8 }, 8));
                match self.h.symbols.get(&id).map(|v| v.as_slice()) {
                    Some([(_, _, raw)]) => b.append_value(raw),
                    Some(all) => match all.iter().find(|(from, to, _)| (*from..*to).contains(&day())) {
                        Some((_, _, raw)) => b.append_value(raw),
                        None => b.append_null(),
                    },
                    None => b.append_null(),
                }
            }
            cols.push(Arc::new(b.finish()));
        }
        Ok(Some(RecordBatch::try_new(self.schema.clone(), cols)?))
    }
}

impl Iterator for Batches {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().map_err(|e| arrow_schema::ArrowError::ExternalError(e.into())).transpose()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn days() {
        assert_eq!(super::yyyymmdd(1_704_205_800_000_000_000), 20240102);
        assert_eq!(super::yyyymmdd(0), 19700101);
        assert_eq!(super::yyyymmdd(951_782_400_000_000_000), 20000229);
    }
}
