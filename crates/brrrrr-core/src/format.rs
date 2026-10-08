//! Proton-exact text: float layout (WriteHelpers.h writeFloatText), JSONEachRow values, datetimes.
use crate::value::{civil, Type, Value};
use std::cell::RefCell;
use std::sync::Arc;

/// Appends `x` as Proton writes it: an integral value below 2^53 (f64) / 2^24 (f32) as an integer
/// (`-0` for negative zero); otherwise the shortest round-trip digits (ties to even, via ryu) in
/// fixed notation when the leading digit's decimal exponent is in -6..=20, compact scientific
/// (`1.5e21`, `1e-7`) otherwise.
/// NaN and infinities are written as `null`, as in JSONEachRow; `to_text` spells them out.
pub fn float(out: &mut String, x: f64, f32: bool) {
    if !x.is_finite() {
        return out.push_str("null");
    }
    let limit = if f32 { 16_777_216.0 } else { 9_007_199_254_740_992.0 };
    if x == x.trunc() && x.abs() < limit {
        let mut b = itoa::Buffer::new();
        return out.push_str(if x == 0.0 && x.is_sign_negative() { "-0" } else { b.format(x as i64) });
    }
    let mut buf = ryu::Buffer::new();
    let s = if f32 { buf.format_finite(x as f32) } else { buf.format_finite(x) };
    // ryu writes fixed notation only for decimal exponents -5 to 15 (f64) or -6 to 12 (f32), within
    // ours below (-6 to 20), with the same shortest digits, no leading or trailing zeros and a
    // "0.000" before small ones: its text is ours, but for the ".0" it puts after an integral
    // value; anything with an exponent takes the way below. Most of a sink's values (prices,
    // sizes) take this one, without taking the digits apart
    if !s.as_bytes().contains(&b'e') && !s.ends_with(".0") {
        return out.push_str(s);
    }
    let (neg, s) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
    let (mant, exp) = s.split_once('e').map_or((s, 0), |(m, e)| (m, e.parse::<i32>().unwrap_or(0)));
    let int = mant.find('.').unwrap_or(mant.len());
    // the significant digits, without the point and the leading and trailing zeros, on the
    // stack: this runs for every non-integral value a sink writes. ryu writes at most 17
    // significant digits, in a mantissa of at most 24 characters
    let (mut d, mut n, mut lead) = ([0u8; 24], 0, 0);
    for b in mant.bytes().filter(|b| *b != b'.') {
        if n == 0 && b == b'0' {
            lead += 1;
        } else {
            d[n] = b;
            n += 1;
        }
    }
    let n = d[..n].iter().rposition(|b| *b != b'0').map_or(0, |last| last + 1);
    let digits = std::str::from_utf8(&d[..n]).expect("ryu writes ASCII digits");
    let point = int as i32 + exp - lead; // decimal point position within `digits`
    if neg {
        out.push('-');
    }
    // The notation is chosen by the decimal exponent of the shortest digits, not by the binary
    // value: the f32 closest to 1e-6 (9.99999997e-7) prints as "0.000001".
    if (-6..=20).contains(&(point - 1)) {
        if point <= 0 {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', (-point) as usize));
            out.push_str(digits);
        } else if point as usize >= digits.len() {
            out.push_str(digits);
            out.extend(std::iter::repeat_n('0', point as usize - digits.len()));
        } else {
            out.push_str(&digits[..point as usize]);
            out.push('.');
            out.push_str(&digits[point as usize..]);
        }
    } else {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push_str(itoa::Buffer::new().format(point - 1));
    }
}

/// `YYYY-MM-DD hh:mm:ss[.ffffff]` in UTC with `precision` fractional digits.
pub fn datetime(out: &mut String, us: i64, precision: u8) {
    let (secs, frac) = (us.div_euclid(1_000_000), us.rem_euclid(1_000_000));
    let (y, m, d) = civil(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    if (0..=9999).contains(&y) {
        padded(out, y, 4);
    } else {
        out.push_str(&format!("{y:04}")); // outside 0000-9999: rare, as `{:04}` writes it
    }
    for (sep, n) in [('-', m as i64), ('-', d as i64), (' ', sod / 3600), (':', sod / 60 % 60), (':', sod % 60)] {
        out.push(sep);
        padded(out, n, 2);
    }
    if precision > 0 {
        let (mut f, mut b) = ([b'0'; 6], itoa::Buffer::new());
        let digits = b.format(frac).as_bytes();
        f[6 - digits.len()..].copy_from_slice(digits);
        out.push('.');
        f[..precision.min(6) as usize].iter().for_each(|b| out.push(*b as char));
    }
}

/// `n` (0 or more, below 10^width) in `width` digits, zero-padded.
pub(crate) fn padded(out: &mut String, n: i64, width: usize) {
    let mut b = itoa::Buffer::new();
    let s = b.format(n);
    out.extend(std::iter::repeat_n('0', width.saturating_sub(s.len())));
    out.push_str(s);
}

thread_local! {
    /// The string [`scratch`] lends: grown once per thread rather than once per text.
    static SCRATCH: RefCell<String> = const { RefCell::new(String::new()) };
}

/// `f` of an empty string the thread keeps, for a text built there and then copied out at its
/// length (`Arc::from(s.as_str())`, `s.to_owned()`): one allocation, where a `String` grown as
/// it is written and then made an `Arc<str>` takes two to four. The texts of every sink row
/// (its headers, its `to_string`s) are built this way. A call within `f` (a `concat` of
/// a `concat`) gets a string of its own.
pub(crate) fn scratch<T>(f: impl FnOnce(&mut String) -> T) -> T {
    SCRATCH.with(|s| match s.try_borrow_mut() {
        Ok(mut s) => {
            s.clear();
            let out = f(&mut s);
            if s.capacity() > 1 << 16 {
                *s = String::new(); // a rare long text is not kept for every short one after it
            }
            out
        }
        Err(_) => f(&mut String::new()),
    })
}

/// [`to_text`] as a string value: a string is shared, anything else allocated once.
pub(crate) fn text_value(v: &Value) -> Arc<str> {
    match v {
        Value::Str(s) => s.clone(),
        v => scratch(|s| {
            write_text(s, v);
            s.as_str().into()
        }),
    }
}

/// Text of a value as Proton's `to_string` renders it.
pub fn to_text(v: &Value) -> String {
    let mut s = String::new();
    write_text(&mut s, v);
    s
}

/// Appends [`to_text`] of `v` to `s`.
pub fn write_text(s: &mut String, v: &Value) {
    match v {
        Value::Null => s.push_str("NULL"),
        Value::Bool(b) => s.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => s.push_str(itoa::Buffer::new().format(*i)),
        Value::UInt(u) => s.push_str(itoa::Buffer::new().format(*u)),
        Value::F32(f) if !f.is_finite() => s.push_str(nonfinite(*f as f64)),
        Value::F64(f) if !f.is_finite() => s.push_str(nonfinite(*f)),
        Value::F32(f) => float(s, *f as f64, true),
        Value::F64(f) => float(s, *f, false),
        Value::Str(x) => s.push_str(x),
        Value::Time(t) => datetime(s, *t, 6),
        Value::Array(a) => {
            s.push('[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                json(s, v, &Type::Str);
            }
            s.push(']');
        }
    }
}

fn nonfinite(x: f64) -> &'static str {
    if x.is_nan() {
        "nan"
    } else if x.is_sign_positive() {
        "inf"
    } else {
        "-inf"
    }
}

/// Appends one JSONEachRow value of declared type `t` (Proton: 64-bit ints unquoted, `/` escaped).
pub fn json(out: &mut String, v: &Value, t: &Type) {
    match (v, t.base()) {
        (Value::Null, _) => out.push_str("null"),
        (Value::F32(f), _) => float(out, *f as f64, true),
        (Value::F64(f), _) => float(out, *f, false),
        (Value::Str(s), _) => string(out, s),
        (Value::Time(us), Type::Time(p)) => {
            out.push('"');
            datetime(out, *us, *p);
            out.push('"');
        }
        (Value::Time(us), _) => {
            out.push('"');
            datetime(out, *us, 6);
            out.push('"');
        }
        (Value::Array(a), t) => {
            let inner = if let Type::Array(i) = t { i.as_ref() } else { t };
            out.push('[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json(out, v, inner);
            }
            out.push(']');
        }
        (v, _) => write_text(out, v),
    }
}

fn string(out: &mut String, s: &str) {
    out.push('"');
    // most text needs no escape: what does has a byte below 0x20, `"`, `\` or `/`, or is
    // U+2028 or U+2029, which both start with 0xE2
    if !s.bytes().any(|b| matches!(b, 0..=0x1F | b'"' | b'\\' | b'/' | 0xE2)) {
        out.push_str(s);
        out.push('"');
        return;
    }
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '/' => out.push_str("\\/"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            // control characters and the JavaScript line separators, upper-case hex like Proton
            c if (c as u32) < 0x20 || c == '\u{2028}' || c == '\u{2029}' => {
                out.push_str(&format!("\\u{:04X}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// A sink's JSONEachRow line, laid out once: for each column it writes, its position in the
/// row, its declared type, and its key with what goes before it (`{"name":`, then `,"name":`).
/// Writing a row then copies keys and formats values, allocating nothing but the line it grows
/// (`json_row`'s output, byte for byte).
#[derive(Clone, Debug, PartialEq)]
pub struct RowFormat {
    cols: Vec<(usize, String, Type)>,
}

impl RowFormat {
    /// The columns of a row to write, as (position in the row, name, declared type), in order.
    pub fn new<'a>(cols: impl IntoIterator<Item = (usize, &'a str, &'a Type)>) -> RowFormat {
        let cols = cols.into_iter().enumerate().map(|(i, (at, name, t))| {
            let mut key = String::from(if i == 0 { "{" } else { "," });
            string(&mut key, name);
            key.push(':');
            (at, key, t.clone())
        });
        RowFormat { cols: cols.collect() }
    }

    /// Appends `row`'s line (`{...}` and a newline). `row` holds every column laid out: a sink's
    /// rows are built to its columns.
    pub fn write(&self, out: &mut String, row: &[Value]) {
        debug_assert!(self.cols.iter().all(|(at, _, _)| *at < row.len()), "a row narrower than its sink");
        if self.cols.is_empty() {
            out.push('{');
        }
        for (at, key, t) in &self.cols {
            out.push_str(key);
            json(out, &row[*at], t);
        }
        out.push_str("}\n");
    }
}

/// One JSONEachRow line: `{"col":value,...}\n` in the declared column order.
pub fn json_row(out: &mut String, cols: &[(String, Type)], row: &[Value]) {
    out.push('{');
    for (i, ((name, t), v)) in cols.iter().zip(row).enumerate() {
        if i > 0 {
            out.push(',');
        }
        string(out, name);
        out.push(':');
        json(out, v, t);
    }
    out.push_str("}\n");
}
