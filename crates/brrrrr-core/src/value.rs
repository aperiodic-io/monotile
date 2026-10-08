//! Values and SQL types with ClickHouse/Proton conversion semantics.
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A single SQL value. `Time` is a `datetime64` in microseconds since the epoch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    F32(f32),
    F64(f64),
    Str(Arc<str>),
    Time(i64),
    Array(#[serde(deserialize_with = "crate::checkpoint::nested")] Arc<[Value]>),
}

/// SQL types as they appear in Proton DDL (`low_cardinality` is only a storage hint).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    Bool,
    Int(u8),
    UInt(u8),
    F32,
    F64,
    Str,
    Time(u8),
    Array(Box<Type>),
    Map(Box<Type>, Box<Type>),
    Nullable(Box<Type>),
    /// Whatever the values are: no cast (the columns of an ad-hoc query's intermediate streams,
    /// whose types are its expressions', `query`).
    Any,
}

impl Type {
    /// Parses `nullable(float32)`, `low_cardinality(string)`, `datetime64(6)`, `map(string, string)`, ...
    pub fn parse(s: &str) -> Result<Type, String> {
        let s = s.trim();
        let (name, arg) = match s.find('(') {
            Some(i) if s.ends_with(')') => (&s[..i], Some(&s[i + 1..s.len() - 1])),
            _ => (s, None),
        };
        let inner = || Type::parse(arg.ok_or(format!("{name} needs an argument"))?);
        Ok(match (name.to_ascii_lowercase().as_str(), arg) {
            ("bool" | "boolean", None) => Type::Bool,
            ("any", None) => Type::Any,
            ("float32", None) => Type::F32,
            ("float64", None) => Type::F64,
            ("string", None) => Type::Str,
            ("datetime", None) => Type::Time(0),
            ("datetime64", a) => {
                let p = a.map_or(Ok(3), |a| a.trim().parse().map_err(|_| format!("bad precision {a}")))?;
                if p > 6 {
                    return Err(format!("{s}: brrrrr keeps datetimes in microseconds (precision up to 6)"));
                }
                Type::Time(p)
            }
            ("nullable", _) => Type::Nullable(Box::new(inner()?)),
            ("low_cardinality", _) => inner()?,
            ("array", _) => Type::Array(Box::new(inner()?)),
            ("map", Some(a)) => {
                let (k, v) = a.split_once(',').ok_or("map needs two types")?;
                Type::Map(Box::new(Type::parse(k)?), Box::new(Type::parse(v)?))
            }
            (n, None) if n.starts_with("uint") => Type::UInt(int_bits(&n[4..], s)?),
            (n, None) if n.starts_with("int") => Type::Int(int_bits(&n[3..], s)?),
            _ => return Err(format!("unknown type {s}")),
        })
    }

    /// The type without `Nullable`.
    pub fn base(&self) -> &Type {
        match self {
            Type::Nullable(t) => t.base(),
            t => t,
        }
    }

    /// The value a non-nullable column takes when given NULL (ClickHouse inserts defaults).
    pub fn default_value(&self) -> Value {
        match self {
            Type::Nullable(_) | Type::Any => Value::Null,
            Type::Bool => Value::Bool(false),
            Type::Int(_) => Value::Int(0),
            Type::UInt(_) => Value::UInt(0),
            Type::F32 => Value::F32(0.0),
            Type::F64 => Value::F64(0.0),
            Type::Str => Value::Str("".into()),
            Type::Time(_) => Value::Time(0),
            Type::Array(_) | Type::Map(..) => Value::Array(Arc::new([])),
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Numeric view used by arithmetic and aggregates (`None` for NULL and non-numbers).
    pub fn f64(&self) -> Option<f64> {
        Some(match *self {
            Value::Int(i) | Value::Time(i) => i as f64,
            Value::UInt(u) => u as f64,
            Value::F32(f) => f as f64,
            Value::F64(f) => f,
            Value::Bool(b) => b as u8 as f64,
            _ => return None,
        })
    }

    pub fn i64(&self) -> Option<i64> {
        Some(match *self {
            Value::Int(i) | Value::Time(i) => i,
            Value::UInt(u) => u as i64,
            Value::Bool(b) => b as i64,
            Value::F32(f) => f as i64,
            Value::F64(f) => f as i64,
            _ => return None,
        })
    }

    pub fn str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Whether `cast(t)` would return this value unchanged, so an insert can keep it as is.
    /// Conservative: `false` only means the cast must run.
    pub fn conforms(&self, t: &Type) -> bool {
        match (self, t) {
            (_, Type::Any) | (Value::Null, Type::Nullable(_)) => true,
            (v, Type::Nullable(t)) => v.conforms(t),
            (Value::Bool(_), Type::Bool)
            | (Value::F32(_), Type::F32)
            | (Value::F64(_), Type::F64)
            | (Value::Str(_), Type::Str)
            | (Value::Time(_), Type::Time(_)) => true,
            (Value::Int(i), Type::Int(bits)) => wrap_signed(*i, *bits) == *i,
            (Value::UInt(u), Type::UInt(bits)) => wrap_unsigned(*u, *bits) == *u,
            (Value::Array(a), Type::Array(t)) => a.iter().all(|v| v.conforms(t)),
            _ => false,
        }
    }

    /// [`Value::cast`] that keeps the value when it already conforms.
    pub fn cast_into(self, t: &Type) -> Value {
        if self.conforms(t) {
            self
        } else {
            self.cast(t)
        }
    }

    /// Converts to `t` with ClickHouse insert/cast semantics: floats truncate toward zero into
    /// integers, integers wrap to the target width, NULL becomes the default of non-nullable types.
    pub fn cast(&self, t: &Type) -> Value {
        match (self, t) {
            (v, Type::Any) => v.clone(),
            (Value::Null, t) => t.default_value(),
            (v, Type::Nullable(t)) => v.cast(t),
            (Value::Str(s), Type::Str) => Value::Str(s.clone()),
            (v, Type::Str) => Value::Str(crate::format::text_value(v)),
            (Value::Str(s), t) => parse_str(s, t),
            (v, Type::F64) => Value::F64(v.f64().unwrap_or(0.0)),
            (v, Type::F32) => Value::F32(v.f64().unwrap_or(0.0) as f32),
            (v, Type::Bool) => Value::Bool(v.f64().is_some_and(|f| f != 0.0)),
            // what C++ casts compile to on x86-64: see cvttsd2si
            (Value::F32(_) | Value::F64(_), Type::Int(bits)) => {
                let f = self.f64().unwrap_or(0.0);
                Value::Int(wrap_signed(cvttsd2si(f, if *bits > 32 { 64 } else { 32 }), *bits))
            }
            (Value::F32(_) | Value::F64(_), Type::UInt(bits)) => {
                let f = self.f64().unwrap_or(0.0);
                Value::UInt(wrap_unsigned(if *bits > 32 { f64_to_u64(f) } else { cvttsd2si(f, 64) as u64 }, *bits))
            }
            (Value::F32(_) | Value::F64(_), Type::Time(_)) => Value::Time(cvttsd2si(self.f64().unwrap_or(0.0), 64)),
            (v, Type::Int(bits)) => Value::Int(wrap_signed(v.i64().unwrap_or(0), *bits)),
            (v, Type::UInt(bits)) => Value::UInt(wrap_unsigned(v.i64().unwrap_or(0) as u64, *bits)),
            (v, Type::Time(_)) => Value::Time(v.i64().unwrap_or(0)),
            (Value::Array(a), Type::Array(t)) => Value::Array(a.iter().map(|v| v.cast(t)).collect()),
            (v, _) => v.clone(),
        }
    }
}

/// `Int<bits>` widths: 8, 16, 32 or 64. Values are 64-bit, so wider ones would silently lose.
fn int_bits(bits: &str, ty: &str) -> Result<u8, String> {
    match bits.parse::<u16>() {
        Ok(b @ (8 | 16 | 32 | 64)) => Ok(b as u8),
        Ok(128 | 256) => Err(format!("{ty}: brrrrr's integers are 64-bit")),
        _ => Err(format!("unknown type {ty}")),
    }
}

/// x86 cvttsd2si of a double into a `w`-bit integer (32 or 64): truncation toward zero, and
/// the most negative integer for NaN and anything out of range. ClickHouse's float to integer
/// casts are C++ casts, which compile to this.
fn cvttsd2si(f: f64, w: u32) -> i64 {
    let (lo, hi) = if w == 64 { (i64::MIN as f64, -(i64::MIN as f64)) } else { (i32::MIN as f64, -(i32::MIN as f64)) };
    if f > lo - 1.0 && f < hi {
        f as i64
    } else if w == 64 {
        i64::MIN
    } else {
        i32::MIN as i64
    }
}

/// A C++ double to uint64_t cast as compiled for x86-64 without AVX-512: cvttsd2si below 2^63,
/// otherwise of `f - 2^63` with the top bit put back. So [2^63, 2^64) converts exactly, NaN and
/// 2^64 or more give 0, and negative numbers wrap.
fn f64_to_u64(f: f64) -> u64 {
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    if f < TWO_63 {
        cvttsd2si(f, 64) as u64
    } else {
        cvttsd2si(f - TWO_63, 64) as u64 ^ (1 << 63)
    }
}

fn wrap_signed(v: i64, bits: u8) -> i64 {
    if bits >= 64 {
        v
    } else {
        let sh = 64 - bits as u32;
        (v << sh) >> sh
    }
}

fn wrap_unsigned(v: u64, bits: u8) -> u64 {
    if bits >= 64 {
        v
    } else {
        v & ((1u64 << bits) - 1)
    }
}

pub(crate) fn parse_str(s: &str, t: &Type) -> Value {
    let f = s.trim().parse::<f64>().ok();
    match t {
        Type::F64 => Value::F64(f.unwrap_or(0.0)),
        Type::F32 => Value::F32(s.trim().parse::<f32>().unwrap_or(0.0)),
        Type::Int(b) => Value::Int(wrap_signed(s.trim().parse::<i64>().unwrap_or(f.unwrap_or(0.0) as i64), *b)),
        Type::UInt(b) => Value::UInt(wrap_unsigned(s.trim().parse::<u64>().unwrap_or(0), *b)),
        Type::Bool => Value::Bool(matches!(s.trim(), "true" | "1")),
        Type::Time(_) => Value::Time(parse_datetime(s).unwrap_or(0)),
        _ => Value::Str(s.into()),
    }
}

/// Days since 1970-01-01 to (year, month, day) in the proleptic Gregorian calendar.
pub fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + (m <= 2) as i64, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - (m <= 2) as i64;
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// Parses `YYYY-MM-DD[ hh[:mm[:ss[.ffffff]]]]` (`T` or a space between), UTC or followed by
/// ISO 8601's `Z` or a UTC offset (`+01:00`, `+0100`, `-05`), into microseconds. Anything else (a
/// day the month does not have, 24:00, an offset of other digits) is not a datetime: `None`,
/// never a datetime off by what could not be read.
pub fn parse_datetime(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Some(utc) = s.strip_suffix(['Z', 'z']) {
        return local_datetime(utc);
    }
    // an offset after the time: past the date's own dashes (`YYYY-MM-DD` is 10 bytes)
    let Some(at) = s.rfind(['+', '-']).filter(|at| *at > 10) else { return local_datetime(s) };
    let (local, offset) = s.split_at(at);
    let d = offset[1..].replace(':', "");
    if !matches!(d.len(), 2 | 4) || !d.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let minutes = d[..2].parse::<i64>().ok()? * 60 + if d.len() == 4 { d[2..].parse::<i64>().ok()? } else { 0 };
    // the local time is UTC plus the offset
    let sign = if offset.starts_with('+') { -1 } else { 1 };
    Some(local_datetime(local)? + sign * minutes * 60_000_000)
}

/// `parse_datetime` without a zone.
fn local_datetime(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = s.split_once([' ', 'T']).unwrap_or((s, "00"));
    let mut d = date.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (y, m, dd) = (d.next()??, d.next()??, d.next()??);
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut t = hms.splitn(3, ':');
    // an absent minute or second is 0; one that is there must be a number
    let mut part = || t.next().map_or(Some(0), |p| p.parse::<i64>().ok());
    let (h, mi, sec) = (part()?, part()?, part()?);
    if !(1..=12).contains(&m) {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let month_days = match m {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if !(1..=month_days).contains(&dd) {
        return None; // 2025-02-30
    }
    if !(0..24).contains(&h) || !(0..60).contains(&mi) || !(0..60).contains(&sec) {
        return None;
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None; // the digit check also keeps the byte slice below on a char boundary
    }
    let days = days_from_civil(y, m as u32, dd as u32);
    let us = format!("{frac:0<6}")[..6].parse::<i64>().ok()?;
    Some((days * 86_400 + h * 3600 + mi * 60 + sec) * 1_000_000 + us)
}

/// `15s`, `1m`, `100ms`, `250us`, `1h`, `1d`, `1w`, `5 minutes`, `1 hour` in µs (positive).
pub fn duration_us(s: &str) -> Option<i64> {
    let s = s.trim();
    let at = s.find(|c: char| !c.is_ascii_digit())?;
    let n: i64 = s[..at].parse().ok()?;
    let unit = s[at..].trim().to_ascii_lowercase();
    let us = match unit.trim_end_matches('s') {
        "u" | "micro" | "microsecond" => 1,
        "m" if unit == "ms" => 1_000,
        "milli" | "millisecond" => 1_000,
        "" if unit == "s" => 1_000_000,
        "sec" | "second" => 1_000_000,
        "m" | "min" | "minute" => 60_000_000,
        "h" | "hour" => 3_600_000_000,
        "d" | "day" => 86_400_000_000,
        "w" | "week" => 7 * 86_400_000_000,
        _ => return None,
    };
    n.checked_mul(us).filter(|w| *w > 0)
}
