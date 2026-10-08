//! Columns: a stream's rows held column by column, for the historical executor
//! (`engine::batch`, ADR-0016). A column is typed where every value is of one kind, with a NULL
//! mask, and falls back to `Value`s where it is not; a value read back is the `Value` the row
//! engine would hold, so every typed path is a shortcut of its row semantics. Values are shared:
//! a slice of a column, or a column passed on by a view, copies nothing.
use crate::value::Value;
use std::ops::{Deref, Range};
use std::sync::Arc;

/// A run of a shared buffer: what a column holds.
#[derive(Clone)]
pub struct Buf<T> {
    all: Arc<Vec<T>>,
    at: usize,
    len: usize,
}

impl<T> Buf<T> {
    /// Rows `r` of this one, sharing its values.
    pub fn slice(&self, r: Range<usize>) -> Buf<T> {
        assert!(r.start <= r.end && r.end <= self.len, "slice {r:?} of {} values", self.len);
        Buf { all: self.all.clone(), at: self.at + r.start, len: r.len() }
    }
}

impl<T> Deref for Buf<T> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        &self.all[self.at..self.at + self.len]
    }
}

impl<'a, T> IntoIterator for &'a Buf<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> From<Vec<T>> for Buf<T> {
    fn from(v: Vec<T>) -> Buf<T> {
        let len = v.len();
        Buf { all: Arc::new(v), at: 0, len }
    }
}

impl<T> FromIterator<T> for Buf<T> {
    fn from_iter<I: IntoIterator<Item = T>>(i: I) -> Buf<T> {
        i.into_iter().collect::<Vec<T>>().into()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Buf<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

/// One column of a batch: its values, and which of them are NULL.
#[derive(Clone, Debug)]
pub struct Col {
    pub data: Data,
    /// `true` where the value is NULL (the data holds a placeholder there); `None`: no NULL.
    /// `Data::Vals` holds its NULLs as values and has none.
    pub nulls: Option<Buf<bool>>,
}

/// A column's values, all of one `Value` kind (`Vals`: any).
#[derive(Clone, Debug)]
pub enum Data {
    Bool(Buf<bool>),
    Int(Buf<i64>),
    UInt(Buf<u64>),
    F32(Buf<f32>),
    F64(Buf<f64>),
    Time(Buf<i64>),
    Str(Strs),
    /// One value in every row (`Value::Null`: every row NULL), `len` rows.
    Const(Value, usize),
    Vals(Buf<Value>),
    /// Row by row, `then`'s value where `pick` is true, else `other`'s: `if(c, x, 0)` of a
    /// Float64 `x`, whose rows are of two kinds.
    Choose(Buf<bool>, Arc<Col>, Arc<Col>),
}

/// Strings back to back in one text: entry `e` is `text[offsets[e]..offsets[e + 1]]`, and row
/// `i` is entry `i`, or entry `codes[i]` when the strings are a dictionary's (Parquet's usual
/// text): rows of one code hold one string, its entry's, which is hashed once (`codes`), and a
/// part of the rows copies its codes, not its strings (`take`). A dictionary may hold a string
/// twice: rows of two codes may still hold one string.
#[derive(Clone, Debug)]
pub struct Strs {
    offsets: Buf<u32>,
    text: Arc<String>,
    codes: Option<Buf<u32>>,
}

impl Default for Strs {
    fn default() -> Strs {
        Strs { offsets: vec![0].into(), text: Arc::default(), codes: None }
    }
}

impl Strs {
    pub fn len(&self) -> usize {
        match &self.codes {
            Some(c) => c.len(),
            None => self.offsets.len() - 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Row `i`'s entry.
    #[inline]
    fn entry(&self, i: usize) -> usize {
        match &self.codes {
            Some(c) => c[i] as usize,
            None => i,
        }
    }

    #[inline]
    pub fn get(&self, i: usize) -> &str {
        let e = self.entry(i);
        &self.text[self.offsets[e] as usize..self.offsets[e + 1] as usize]
    }

    /// Strings from Arrow's layout: offsets into `text`, which must hold UTF-8 and every
    /// boundary on a character.
    pub fn from_parts(offsets: Vec<u32>, text: Vec<u8>) -> Result<Strs, String> {
        let text = String::from_utf8(text).map_err(|_| "strings that are not UTF-8")?;
        let ok = !offsets.is_empty()
            && offsets.windows(2).all(|w| w[0] <= w[1])
            && offsets.last().is_some_and(|l| *l as usize <= text.len())
            && offsets.iter().all(|o| text.is_char_boundary(*o as usize));
        if !ok {
            return Err("malformed string offsets".into());
        }
        Ok(Strs { offsets: offsets.into(), text: Arc::new(text), codes: None })
    }

    /// Row `i` the entry `codes[i]` of `dictionary`; an error for a code past its entries.
    pub fn from_dictionary(dictionary: &Strs, codes: Vec<u32>) -> Result<Strs, String> {
        // the dictionary's entries, one a row
        let entries: Strs = if dictionary.codes.is_some() { dictionary.iter().collect() } else { dictionary.clone() };
        if codes.iter().any(|c| *c as usize >= entries.len()) {
            return Err("a dictionary code past its entries".into());
        }
        Ok(Strs { offsets: entries.offsets, text: entries.text, codes: Some(codes.into()) })
    }

    /// Each row's dictionary entry, if the strings are a dictionary's (`from_dictionary`).
    pub fn codes(&self) -> Option<&[u32]> {
        self.codes.as_deref()
    }

    /// The strings at `rows`, in their order: a dictionary's codes, else their bytes copied,
    /// sized up front, the text checked once.
    pub fn take(&self, rows: &[usize]) -> Strs {
        if let Some(c) = &self.codes {
            let codes = Some(rows.iter().map(|&i| c[i]).collect());
            return Strs { offsets: self.offsets.clone(), text: self.text.clone(), codes };
        }
        let (o, t) = (&self.offsets[..], self.text.as_bytes());
        let bytes: usize = rows.iter().map(|&i| (o[i + 1] - o[i]) as usize).sum();
        let (mut offsets, mut text) = (Vec::with_capacity(rows.len() + 1), Vec::with_capacity(bytes));
        offsets.push(0u32);
        for &i in rows {
            text.extend_from_slice(&t[o[i] as usize..o[i + 1] as usize]);
            offsets.push(u32::try_from(text.len()).expect("a batch's strings fit 4 GiB"));
        }
        let text = String::from_utf8(text).expect("strings cut at their offsets, on characters");
        Strs { offsets: offsets.into(), text: Arc::new(text), codes: None }
    }

    /// `parts` one after the other: of one dictionary, their codes; else each one's text
    /// copied whole, its offsets shifted.
    pub fn concat(parts: &[&Strs]) -> Strs {
        let shared = parts.first().is_some_and(|f| {
            f.codes.is_some() && parts.iter().all(|s| s.codes.is_some() && Arc::ptr_eq(&s.text, &f.text))
        });
        if shared {
            let codes = parts.iter().flat_map(|s| s.codes.as_deref().expect("checked").iter().copied()).collect();
            return Strs { offsets: parts[0].offsets.clone(), text: parts[0].text.clone(), codes: Some(codes) };
        }
        let n: usize = parts.iter().map(|s| s.len()).sum();
        let (mut offsets, mut text) = (Vec::with_capacity(n + 1), String::new());
        offsets.push(0u32);
        let at = |text: &String| u32::try_from(text.len()).expect("a batch's strings fit 4 GiB");
        for s in parts {
            match &s.codes {
                // a dictionary's: row by row
                Some(_) => (0..s.len()).for_each(|i| {
                    text.push_str(s.get(i));
                    offsets.push(at(&text));
                }),
                None => {
                    let (first, last) = (s.offsets[0], s.offsets[s.len()]);
                    let base = at(&text);
                    text.push_str(&s.text[first as usize..last as usize]);
                    offsets.extend(s.offsets[1..].iter().map(|o| o - first + base));
                }
            }
        }
        Strs { offsets: offsets.into(), text: Arc::new(text), codes: None }
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> + '_ {
        (0..self.len()).map(|i| self.get(i))
    }

    /// String `i`'s bytes.
    #[inline]
    pub fn bytes(&self, i: usize) -> &[u8] {
        let e = self.entry(i);
        &self.text.as_bytes()[self.offsets[e] as usize..self.offsets[e + 1] as usize]
    }

    /// Each string's bytes.
    pub fn iter_bytes(&self) -> impl Iterator<Item = &[u8]> + '_ {
        (0..self.len()).map(|i| self.bytes(i))
    }

    fn slice(&self, r: Range<usize>) -> Strs {
        match &self.codes {
            Some(c) => Strs { offsets: self.offsets.clone(), text: self.text.clone(), codes: Some(c.slice(r)) },
            None => Strs { offsets: self.offsets.slice(r.start..r.end + 1), text: self.text.clone(), codes: None },
        }
    }
}

impl<'a> FromIterator<&'a str> for Strs {
    fn from_iter<I: IntoIterator<Item = &'a str>>(i: I) -> Strs {
        let i = i.into_iter();
        let mut offsets = Vec::with_capacity(i.size_hint().0 + 1);
        offsets.push(0u32);
        let mut text = String::new();
        for s in i {
            text.push_str(s);
            offsets.push(u32::try_from(text.len()).expect("a batch's strings fit 4 GiB"));
        }
        // held while the batch is: no room grown and left unused
        text.shrink_to_fit();
        Strs { offsets: offsets.into(), text: Arc::new(text), codes: None }
    }
}

/// Whether any of `m` is true: every value read, so that it runs as a vector loop (`contains`
/// stops at the first, a value at a time).
pub(crate) fn any_true(m: &[bool]) -> bool {
    m.iter().fold(false, |a, b| a | b)
}

impl Col {
    pub fn new(data: Data) -> Col {
        Col { data, nulls: None }
    }

    pub fn len(&self) -> usize {
        match &self.data {
            Data::Bool(v) => v.len(),
            Data::Int(v) | Data::Time(v) => v.len(),
            Data::UInt(v) => v.len(),
            Data::F32(v) => v.len(),
            Data::F64(v) => v.len(),
            Data::Str(s) => s.len(),
            Data::Const(_, n) => *n,
            Data::Vals(v) => v.len(),
            Data::Choose(p, ..) => p.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        match (&self.nulls, &self.data) {
            (Some(n), _) => n[i],
            (None, Data::Const(v, _)) => v.is_null(),
            (None, Data::Vals(v)) => v[i].is_null(),
            (None, Data::Choose(p, a, b)) => (if p[i] { a } else { b }).is_null(i),
            _ => false,
        }
    }

    /// Row `i` as the row engine holds it.
    pub fn get(&self, i: usize) -> Value {
        if self.nulls.as_ref().is_some_and(|n| n[i]) {
            return Value::Null;
        }
        match &self.data {
            Data::Bool(v) => Value::Bool(v[i]),
            Data::Int(v) => Value::Int(v[i]),
            Data::UInt(v) => Value::UInt(v[i]),
            Data::F32(v) => Value::F32(v[i]),
            Data::F64(v) => Value::F64(v[i]),
            Data::Time(v) => Value::Time(v[i]),
            Data::Str(s) => Value::Str(s.get(i).into()),
            Data::Const(v, _) => v.clone(),
            Data::Vals(v) => v[i].clone(),
            Data::Choose(p, a, b) => (if p[i] { a } else { b }).get(i),
        }
    }

    /// A column of `values`, typed where they are all of one kind (NULLs aside).
    pub fn from_values(values: Vec<Value>) -> Col {
        let n = values.len();
        let Some(first) = values.iter().find(|v| !v.is_null()) else {
            return Col::new(Data::Const(Value::Null, n));
        };
        let nulls: Vec<bool> = values.iter().map(Value::is_null).collect();
        let nulls = any_true(&nulls).then(|| nulls.into());
        macro_rules! typed {
            ($variant:ident, $default:expr) => {{
                let mut out = Vec::with_capacity(n);
                for v in &values {
                    match v {
                        Value::$variant(x) => out.push(x.clone()),
                        Value::Null => out.push($default),
                        _ => return Col::new(Data::Vals(values.into())),
                    }
                }
                Col { data: Data::$variant(out.into()), nulls }
            }};
        }
        match first {
            Value::Bool(_) => typed!(Bool, false),
            Value::Int(_) => typed!(Int, 0),
            Value::UInt(_) => typed!(UInt, 0),
            Value::F32(_) => typed!(F32, 0.0),
            Value::F64(_) => typed!(F64, 0.0),
            Value::Time(_) => typed!(Time, 0),
            Value::Str(_) => {
                if values.iter().any(|v| !matches!(v, Value::Str(_) | Value::Null)) {
                    return Col::new(Data::Vals(values.into()));
                }
                let s = values.iter().map(|v| v.str().unwrap_or("")).collect();
                Col { data: Data::Str(s), nulls }
            }
            Value::Array(_) | Value::Null => Col::new(Data::Vals(values.into())),
        }
    }

    /// The rows at `rows`, in that order.
    pub fn take(&self, rows: &[usize]) -> Col {
        let data = match &self.data {
            Data::Bool(v) => Data::Bool(rows.iter().map(|&i| v[i]).collect()),
            Data::Int(v) => Data::Int(rows.iter().map(|&i| v[i]).collect()),
            Data::UInt(v) => Data::UInt(rows.iter().map(|&i| v[i]).collect()),
            Data::F32(v) => Data::F32(rows.iter().map(|&i| v[i]).collect()),
            Data::F64(v) => Data::F64(rows.iter().map(|&i| v[i]).collect()),
            Data::Time(v) => Data::Time(rows.iter().map(|&i| v[i]).collect()),
            Data::Str(s) => Data::Str(s.take(rows)),
            Data::Const(v, _) => Data::Const(v.clone(), rows.len()),
            Data::Vals(v) => Data::Vals(rows.iter().map(|&i| v[i].clone()).collect()),
            Data::Choose(p, a, b) => {
                Data::Choose(rows.iter().map(|&i| p[i]).collect(), Arc::new(a.take(rows)), Arc::new(b.take(rows)))
            }
        };
        let nulls: Option<Buf<bool>> = self.nulls.as_ref().map(|n| rows.iter().map(|&i| n[i]).collect());
        Col { data, nulls: nulls.filter(|n| any_true(n)) }
    }

    /// Row `i` of this column at each `Some(i)`, `fill` at each `None`: typed where `fill` is
    /// of the column's kind or NULL.
    pub fn gather(&self, rows: &[Option<usize>], fill: &Value) -> Col {
        let nulls = |own: bool| -> Option<Buf<bool>> {
            let n: Buf<bool> = rows.iter().map(|r| r.map_or(fill.is_null(), |i| own && self.is_null(i))).collect();
            any_true(&n).then_some(n)
        };
        macro_rules! typed {
            ($variant:ident, $v:expr, $default:expr) => {{
                let fv = match fill {
                    Value::$variant(x) => x.clone(),
                    _ => $default,
                };
                Col {
                    data: Data::$variant(rows.iter().map(|r| r.map_or(fv.clone(), |i| $v[i].clone())).collect()),
                    nulls: nulls(self.nulls.is_some()),
                }
            }};
        }
        let fits = |f: fn(&Value) -> bool| fill.is_null() || f(fill);
        match &self.data {
            Data::F64(v) if fits(|f| matches!(f, Value::F64(_))) => typed!(F64, v, 0.0),
            Data::Int(v) if fits(|f| matches!(f, Value::Int(_))) => typed!(Int, v, 0),
            Data::Time(v) if fits(|f| matches!(f, Value::Time(_))) => typed!(Time, v, 0),
            Data::Str(s) if fits(|f| matches!(f, Value::Str(_))) => {
                let fv = fill.str().unwrap_or("");
                Col {
                    data: Data::Str(rows.iter().map(|r| r.map_or(fv, |i| s.get(i))).collect()),
                    nulls: nulls(self.nulls.is_some()),
                }
            }
            Data::Const(v, _) if fill == v => Col::new(Data::Const(v.clone(), rows.len())),
            _ => Col::from_values(rows.iter().map(|r| r.map_or_else(|| fill.clone(), |i| self.get(i))).collect()),
        }
    }

    /// Rows `r`, sharing the values.
    pub fn slice(&self, r: Range<usize>) -> Col {
        let data = match &self.data {
            Data::Bool(v) => Data::Bool(v.slice(r.clone())),
            Data::Int(v) => Data::Int(v.slice(r.clone())),
            Data::UInt(v) => Data::UInt(v.slice(r.clone())),
            Data::F32(v) => Data::F32(v.slice(r.clone())),
            Data::F64(v) => Data::F64(v.slice(r.clone())),
            Data::Time(v) => Data::Time(v.slice(r.clone())),
            Data::Str(s) => Data::Str(s.slice(r.clone())),
            Data::Const(v, _) => Data::Const(v.clone(), r.len()),
            Data::Vals(v) => Data::Vals(v.slice(r.clone())),
            Data::Choose(p, a, b) => {
                Data::Choose(p.slice(r.clone()), Arc::new(a.slice(r.clone())), Arc::new(b.slice(r.clone())))
            }
        };
        let nulls = self.nulls.as_ref().map(|n| n.slice(r));
        Col { data, nulls: nulls.filter(|n| any_true(n)) }
    }

    /// The columns one after another, of the same kind where all are.
    pub fn concat(cols: &[&Col]) -> Col {
        if let [c] = cols {
            return (*c).clone();
        }
        let n: usize = cols.iter().map(|c| c.len()).sum();
        let nulls: Option<Buf<bool>> = cols
            .iter()
            .any(|c| c.nulls.is_some() || matches!(c.data, Data::Const(Value::Null, _)))
            .then(|| cols.iter().flat_map(|c| (0..c.len()).map(|i| c.is_null(i))).collect());
        macro_rules! join {
            ($variant:ident) => {{
                let mut out = Vec::with_capacity(n);
                for c in cols {
                    let Data::$variant(v) = &c.data else { unreachable!("checked") };
                    out.extend_from_slice(v);
                }
                Col { data: Data::$variant(out.into()), nulls }
            }};
        }
        let all = |f: fn(&Data) -> bool| cols.iter().all(|c| f(&c.data));
        match cols.first().map(|c| &c.data) {
            None => Col::new(Data::Vals(Vec::new().into())),
            Some(Data::Bool(_)) if all(|d| matches!(d, Data::Bool(_))) => join!(Bool),
            Some(Data::Int(_)) if all(|d| matches!(d, Data::Int(_))) => join!(Int),
            Some(Data::UInt(_)) if all(|d| matches!(d, Data::UInt(_))) => join!(UInt),
            Some(Data::F32(_)) if all(|d| matches!(d, Data::F32(_))) => join!(F32),
            Some(Data::F64(_)) if all(|d| matches!(d, Data::F64(_))) => join!(F64),
            Some(Data::Time(_)) if all(|d| matches!(d, Data::Time(_))) => join!(Time),
            Some(Data::Str(_)) if all(|d| matches!(d, Data::Str(_))) => {
                let parts: Vec<&Strs> = cols
                    .iter()
                    .map(|c| match &c.data {
                        Data::Str(s) => s,
                        _ => unreachable!("checked"),
                    })
                    .collect();
                Col { data: Data::Str(Strs::concat(&parts)), nulls }
            }
            Some(Data::Const(v, _)) if cols.iter().all(|c| matches!(&c.data, Data::Const(w, _) if w == v)) => {
                Col::new(Data::Const(v.clone(), n))
            }
            _ => Col::new(Data::Vals(cols.iter().flat_map(|c| (0..c.len()).map(|i| c.get(i))).collect())),
        }
    }

    /// Whether no row is NULL, for certain (a column of values: not known).
    pub fn no_nulls(&self) -> bool {
        match &self.data {
            Data::Vals(_) => false,
            Data::Const(v, _) => self.nulls.is_none() && !v.is_null(),
            Data::Choose(_, a, b) => a.no_nulls() && b.no_nulls(),
            _ => self.nulls.is_none(),
        }
    }

    /// Float64 values without NULLs, if this is such a column.
    pub fn f64s(&self) -> Option<&[f64]> {
        match (&self.data, &self.nulls) {
            (Data::F64(v), None) => Some(v),
            _ => None,
        }
    }

    /// Integers (or times) without NULLs, if this is such a column.
    pub fn i64s(&self) -> Option<&[i64]> {
        match (&self.data, &self.nulls) {
            (Data::Int(v) | Data::Time(v), None) => Some(v),
            _ => None,
        }
    }

    /// Strings without NULLs, if this is such a column.
    pub fn strs(&self) -> Option<&Strs> {
        match (&self.data, &self.nulls) {
            (Data::Str(s), None) => Some(s),
            _ => None,
        }
    }

    /// The one value of every row, if the column has one (checked row by row for the others).
    pub fn constant(&self) -> Option<Value> {
        if let Data::Const(v, _) = &self.data {
            return Some(v.clone());
        }
        let n = self.len();
        if n == 0 {
            return None;
        }
        let same = match (&self.data, &self.nulls) {
            (Data::Str(s), None) => s.iter().all(|x| x == s.get(0)),
            (Data::Int(v) | Data::Time(v), None) => v.iter().all(|x| *x == v[0]),
            _ => (1..n).all(|i| self.get(i) == self.get(0)),
        };
        same.then(|| self.get(0))
    }
}

/// Rows of a stream, column by column in its column order. Columns are shared: a view that
/// passes a column on (a rename) passes the same column.
#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub len: usize,
    pub cols: Vec<Arc<Col>>,
}

impl Batch {
    pub fn new(len: usize, cols: Vec<Arc<Col>>) -> Batch {
        debug_assert!(cols.iter().all(|c| c.len() == len));
        Batch { len, cols }
    }

    /// The rows of `rows` (each of `width` values) as a batch.
    pub fn from_rows(rows: &[Vec<Value>], width: usize) -> Batch {
        let cols = (0..width).map(|j| Arc::new(Col::from_values(rows.iter().map(|r| r[j].clone()).collect())));
        Batch { len: rows.len(), cols: cols.collect() }
    }

    /// Row `i` as the row engine holds it.
    pub fn row(&self, i: usize) -> Vec<Value> {
        self.cols.iter().map(|c| c.get(i)).collect()
    }

    pub fn rows(&self) -> Vec<Vec<Value>> {
        (0..self.len).map(|i| self.row(i)).collect()
    }

    /// Rows `r`, sharing the values.
    pub fn slice(&self, r: Range<usize>) -> Batch {
        Batch { len: r.len(), cols: self.cols.iter().map(|c| Arc::new(c.slice(r.clone()))).collect() }
    }

    pub fn take(&self, rows: &[usize]) -> Batch {
        Batch { len: rows.len(), cols: self.cols.iter().map(|c| Arc::new(c.take(rows))).collect() }
    }

    /// `batches` one after another (all of the same width).
    pub fn concat(batches: &[Batch]) -> Batch {
        match batches {
            [] => Batch::default(),
            [b] => b.clone(),
            bs => {
                let width = bs[0].cols.len();
                let cols =
                    (0..width).map(|j| Arc::new(Col::concat(&bs.iter().map(|b| &*b.cols[j]).collect::<Vec<_>>())));
                Batch { len: bs.iter().map(|b| b.len).sum(), cols: cols.collect() }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values() -> Vec<Vec<Value>> {
        vec![
            vec![Value::Int(1), Value::F64(1.5), Value::Str("a".into()), Value::Null, Value::Str("x".into())],
            vec![Value::Int(2), Value::Null, Value::Str("bc".into()), Value::Null, Value::Int(3)],
            vec![Value::Int(3), Value::F64(-0.0), Value::Str("é".into()), Value::Null, Value::Null],
        ]
    }

    #[test]
    fn rows_come_back_as_they_went_in() {
        let rows = values();
        let b = Batch::from_rows(&rows, 5);
        assert_eq!(b.rows(), rows);
        assert!(matches!(b.cols[0].data, Data::Int(_)));
        assert!(matches!(b.cols[1].data, Data::F64(_)));
        assert_eq!(b.cols[1].nulls.as_deref(), Some(&[false, true, false][..]));
        assert!(matches!(b.cols[2].data, Data::Str(_)));
        assert!(matches!(b.cols[3].data, Data::Const(Value::Null, 3)));
        assert!(matches!(b.cols[4].data, Data::Vals(_)), "a string and an integer in one column");
    }

    /// A dictionary's strings: its codes taken, sliced and concatenated, its rows' text as any.
    #[test]
    fn a_dictionarys_strings_keep_their_codes() {
        let dictionary: Strs = ["b", "a", "é"].into_iter().collect();
        assert!(Strs::from_dictionary(&dictionary, vec![3]).is_err());
        let s = Strs::from_dictionary(&dictionary, vec![1, 0, 2, 1]).unwrap();
        assert_eq!(s.iter().collect::<Vec<_>>(), ["a", "b", "é", "a"]);
        assert_eq!(s.bytes(2), "é".as_bytes());
        let t = s.take(&[3, 2]);
        assert_eq!((t.codes(), t.iter().collect::<Vec<_>>()), (Some(&[1, 2][..]), vec!["a", "é"]));
        let u = s.slice(1..3);
        assert_eq!((u.len(), u.get(0)), (2, "b"));
        // one dictionary: codes; another's or plain text: the text
        let same = Strs::concat(&[&t, &u]);
        assert_eq!(
            (same.codes().map(<[u32]>::len), same.iter().collect::<Vec<_>>()),
            (Some(4), vec!["a", "é", "b", "é"])
        );
        let other = Strs::from_dictionary(&s, vec![0, 3]).unwrap();
        let plain: Strs = ["x"].into_iter().collect();
        let mixed = Strs::concat(&[&t, &other, &plain]);
        assert_eq!((mixed.codes(), mixed.iter().collect::<Vec<_>>()), (None, vec!["a", "é", "a", "a", "x"]));
    }

    #[test]
    fn slices_takes_and_concatenations_keep_the_values() {
        let rows = values();
        let b = Batch::from_rows(&rows, 5);
        assert_eq!(b.slice(1..3).rows(), rows[1..3].to_vec());
        assert_eq!(b.slice(1..3).slice(1..2).rows(), rows[2..3].to_vec(), "a slice of a slice");
        assert_eq!(b.take(&[2, 0, 2]).rows(), vec![rows[2].clone(), rows[0].clone(), rows[2].clone()]);
        let joined = Batch::concat(&[b.slice(0..1), b.slice(1..3)]);
        assert_eq!(joined.rows(), rows);
        assert!(matches!(joined.cols[2].data, Data::Str(_)));
        assert_eq!(joined.cols[1].nulls.as_deref(), Some(&[false, true, false][..]));
    }

    #[test]
    fn a_column_knows_its_one_value() {
        let c = Col::from_values(vec![Value::Str("BTC".into()), Value::Str("BTC".into())]);
        assert_eq!(c.constant(), Some(Value::Str("BTC".into())));
        assert_eq!(Col::from_values(vec![Value::Int(1), Value::Int(2)]).constant(), None);
        assert_eq!(Col::from_values(vec![Value::Null, Value::Null]).constant(), Some(Value::Null));
    }

    /// A column of every kind, of 6 rows, and its values.
    fn kinds() -> Vec<(Col, Vec<Value>)> {
        let typed = |v: Vec<Value>| (Col::from_values(v.clone()), v);
        let mut out = vec![
            typed(vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Null,
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
            ]),
            typed((0..6).map(|i| Value::UInt(i * 3)).collect()),
            typed((0..6).map(|i| if i == 2 { Value::Null } else { Value::F32(i as f32 / 2.0) }).collect()),
            typed((0..6).map(|i| Value::Time(i * 10)).collect()),
            typed((0..6).map(|i| Value::Str(format!("s{i}").into())).collect()),
            typed(vec![
                Value::Int(1),
                Value::Str("a".into()),
                Value::Null,
                Value::Int(4),
                Value::Array(Arc::new([Value::Int(1)])),
                Value::F64(0.5),
            ]),
            (Col::new(Data::Const(Value::Int(7), 6)), vec![Value::Int(7); 6]),
        ];
        let pick: Vec<bool> = (0..6).map(|i| i % 2 == 0).collect();
        let (a, b) =
            (Col::from_values((0..6).map(|i| Value::F64(i as f64)).collect()), Col::new(Data::Const(Value::Int(0), 6)));
        let v = (0..6).map(|i| if i % 2 == 0 { Value::F64(i as f64) } else { Value::Int(0) }).collect();
        out.push((Col::new(Data::Choose(pick.into(), Arc::new(a), Arc::new(b))), v));
        out
    }

    #[test]
    fn every_kind_of_column_takes_slices_concatenates_and_gathers_its_values() {
        for (c, v) in kinds() {
            assert_eq!(c.len(), 6);
            assert!(!c.is_empty());
            assert_eq!((0..6).map(|i| c.get(i)).collect::<Vec<_>>(), v, "{c:?}");
            assert_eq!(
                (0..6).map(|i| c.is_null(i)).collect::<Vec<_>>(),
                v.iter().map(Value::is_null).collect::<Vec<_>>()
            );
            let t = c.take(&[5, 0, 2]);
            assert_eq!((0..3).map(|i| t.get(i)).collect::<Vec<_>>(), [v[5].clone(), v[0].clone(), v[2].clone()]);
            let s = c.slice(2..5);
            assert_eq!((0..3).map(|i| s.get(i)).collect::<Vec<_>>(), v[2..5].to_vec());
            let j = Col::concat(&[&c.slice(0..2), &c.slice(2..6)]);
            assert_eq!((0..6).map(|i| j.get(i)).collect::<Vec<_>>(), v);
            for fill in [Value::Null, v[1].clone(), Value::Str("z".into())] {
                let g = c.gather(&[Some(4), None, Some(0)], &fill);
                assert_eq!(
                    (0..3).map(|i| g.get(i)).collect::<Vec<_>>(),
                    [v[4].clone(), fill.clone(), v[0].clone()],
                    "{c:?} {fill:?}"
                );
            }
        }
        // columns of two kinds, one after the other: values
        let mixed = Col::concat(&[&Col::from_values(vec![Value::Int(1)]), &Col::from_values(vec![Value::F64(2.0)])]);
        assert_eq!((mixed.get(0), mixed.get(1)), (Value::Int(1), Value::F64(2.0)));
        assert_eq!(Col::concat(&[]).len(), 0);
        let consts = Col::concat(&[&Col::new(Data::Const(Value::Null, 2)), &Col::from_values(vec![Value::Int(3)])]);
        assert_eq!((consts.get(0), consts.get(2)), (Value::Null, Value::Int(3)));
    }

    #[test]
    fn a_columns_one_value_is_found_of_every_kind() {
        let c = Col::from_values(vec![Value::Int(1), Value::Int(1)]);
        assert_eq!(c.constant(), Some(Value::Int(1)));
        let v = Col::from_values(vec![Value::Array(Arc::new([])), Value::Array(Arc::new([]))]);
        assert_eq!(v.constant(), Some(Value::Array(Arc::new([]))));
        assert_eq!(Col::from_values(vec![]).constant(), Some(Value::Null));
        assert_eq!(Col::new(Data::Int(Buf::from(vec![]))).constant(), None);
        assert!(Col::from_values(vec![]).no_nulls() || Col::from_values(vec![]).is_empty());
        assert!(Strs::default().is_empty());
        assert_eq!(format!("{:?}", Buf::from(vec![1, 2])), "[1, 2]");
    }

    #[test]
    fn malformed_strings_are_refused() {
        assert_eq!(Strs::from_parts(vec![0, 1, 3], b"abc".to_vec()).unwrap().iter().collect::<Vec<_>>(), ["a", "bc"]);
        assert!(Strs::from_parts(vec![0, 4], b"abc".to_vec()).is_err());
        assert!(Strs::from_parts(vec![0, 1], "é".as_bytes().to_vec()).is_err(), "a boundary inside a character");
        assert!(Strs::from_parts(vec![0, 1], vec![0xff]).is_err(), "not UTF-8");
    }

    /// What callers count on beyond the values: concatenations of one kind stay of that kind,
    /// a gather fills with its fill, the typed views of a column without NULLs.
    #[test]
    fn columns_keep_their_kind_and_say_what_they_hold() {
        let f = Col::new(Data::F64(vec![1.5, 2.5].into()));
        let i = Col::new(Data::Int(vec![1, 2].into()));
        let t = Col::new(Data::Time(vec![1, 2].into()));
        let s = Col::new(Data::Str(["ab", "c"].into_iter().collect()));
        let u = Col::new(Data::UInt(vec![1, 2].into()));
        let g = Col::new(Data::F32(vec![1.0, 2.0].into()));
        let b = Col::new(Data::Bool(vec![true, false].into()));
        let k = Col::new(Data::Const(Value::Int(7), 2));
        let kind = |c: &Col| std::mem::discriminant(&c.data);
        for c in [&f, &i, &t, &s, &u, &g, &b, &k] {
            assert_eq!(kind(&Col::concat(&[c, c])), kind(c), "{c:?}");
        }
        // of two kinds, whichever comes first: values
        for c in [&f, &i, &t, &s, &u, &g, &b, &k] {
            let other = if std::ptr::eq(c, &f) { &i } else { &f };
            let m = Col::concat(&[c, other]);
            assert!(matches!(m.data, Data::Vals(_)), "{c:?}");
            assert_eq!(m.get(3), other.get(1));
        }
        let mixed = Col::concat(&[&f, &i]);
        assert!(matches!(mixed.data, Data::Vals(_)));
        assert_eq!((0..4).map(|r| mixed.get(r)).collect::<Vec<_>>()[1..3], [Value::F64(2.5), Value::Int(1)]);
        // a fill of another kind is the fill, not the kind's default
        for (c, fill) in [(&f, Value::Int(9)), (&i, Value::F64(0.5)), (&t, Value::Int(9)), (&s, Value::Int(9))] {
            assert_eq!(c.gather(&[None, Some(1)], &fill).get(0), fill, "{c:?}");
        }
        let n = Col { data: Data::F64(vec![1.0, 0.0].into()), nulls: Some(vec![false, true].into()) };
        assert_eq!(n.gather(&[Some(0), Some(1)], &Value::Null).get(0), Value::F64(1.0));
        assert_eq!(n.gather(&[Some(0), Some(1)], &Value::Null).get(1), Value::Null);
        // the typed views, only without NULLs
        assert_eq!(f.f64s(), Some(&[1.5, 2.5][..]));
        assert_eq!(i.i64s(), Some(&[1, 2][..]));
        assert_eq!(t.i64s(), Some(&[1, 2][..]));
        assert_eq!(s.strs().map(|s| s.iter().collect::<Vec<_>>()), Some(vec!["ab", "c"]));
        assert!(n.f64s().is_none() && i.f64s().is_none() && f.i64s().is_none() && f.strs().is_none());
        assert!(f.no_nulls() && !n.no_nulls());
        assert!(!Col::new(Data::Vals(vec![Value::Int(1), Value::Null].into())).no_nulls());
        let st = s.strs().unwrap();
        assert!(!st.is_empty() && Strs::from_parts(vec![0], vec![]).unwrap().is_empty());
        assert_eq!((st.bytes(0), st.bytes(1)), (&b"ab"[..], &b"c"[..]));
        assert!(!any_true(&[false, false]) && any_true(&[false, true]));
    }
}
