//! Protobuf (`ProtobufSingle`) rows: a tiny `.proto` parser plus a table-driven wire codec that
//! maps message fields to an external stream's declared columns by name.
use crate::value::{Type, Value};
use std::collections::HashMap;

/// Scalar protobuf field types (fixtures/market.proto's, among others).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// int32/int64: two's complement varints.
    Int,
    /// uint32/uint64.
    UInt,
    /// sint32/sint64: ZigZag varints.
    SInt,
    Bool,
    Double,
    Float,
    Str,
}

#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub number: u32,
    pub kind: Kind,
    pub repeated: bool,
}

/// The largest field number protobuf allows (2^29 - 1).
const MAX_FIELD: u64 = (1 << 29) - 1;

/// The tokens of a `.proto` file: words (identifiers, numbers, dotted names), string literals
/// and single punctuation characters. Comments are dropped, wherever they are.
fn tokens(src: &str) -> Result<Vec<&str>, String> {
    let word = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-' | '+');
    let mut out = vec![];
    let mut rest = src;
    while let Some(c) = rest.chars().next() {
        let n = if c.is_whitespace() {
            c.len_utf8()
        } else if rest.starts_with("//") {
            rest.find('\n').unwrap_or(rest.len())
        } else if let Some(body) = rest.strip_prefix("/*") {
            2 + body.find("*/").ok_or("unterminated /* comment")? + 2
        } else {
            let n = if c == '"' || c == '\'' {
                1 + rest[1..].find(c).ok_or("unterminated string")? + 1
            } else if word(c) {
                rest.find(|c| !word(c)).unwrap_or(rest.len())
            } else {
                c.len_utf8()
            };
            out.push(&rest[..n]);
            n
        };
        // every branch consumes at least `c`: the loop always ends, whatever a later edit (or a
        // mutant) does to the lengths above
        rest = &rest[n.max(c.len_utf8())..];
    }
    Ok(out)
}

/// All messages of a `.proto` file, by simple name (`Trade`): flat messages of scalar
/// fields, as fixtures/market.proto has. Anything else (nested messages, enums, oneofs, maps)
/// is an error rather than a field silently missing or misread.
pub fn parse_proto(src: &str) -> Result<HashMap<String, Vec<Field>>, String> {
    let t = tokens(src)?;
    let mut i = 0;
    let at = |i: usize| t.get(i).copied().unwrap_or("end of file");
    // skips to the next `;`, which the loops step over
    let statement = |i: &mut usize| -> Result<(), String> {
        *i += t[*i..].iter().position(|x| *x == ";").ok_or(format!("`{}` without `;`", t[*i]))?;
        Ok(())
    };
    let mut out = HashMap::new();
    while i < t.len() {
        match t[i] {
            "syntax" | "package" | "import" | "option" => statement(&mut i)?,
            ";" => i += 1,
            "message" => {
                let name = at(i + 1).to_string();
                if at(i + 2) != "{" {
                    return Err(format!("message {name}: expected `{{`, found `{}`", at(i + 2)));
                }
                i += 3;
                let mut fields: Vec<Field> = vec![];
                loop {
                    match at(i) {
                        "end of file" => return Err(format!("message {name}: unterminated")),
                        "}" => break i += 1,
                        ";" => i += 1,
                        "option" | "reserved" => statement(&mut i)?,
                        w @ ("message" | "enum" | "oneof" | "map" | "extend" | "extensions" | "group") => {
                            return Err(format!("message {name}: {w} is not supported"))
                        }
                        _ => {
                            let f = field(&t, &mut i).map_err(|e| format!("message {name}: {e}"))?;
                            if fields.iter().any(|g| g.name == f.name || g.number == f.number) {
                                return Err(format!("message {name}: field {} or number {} twice", f.name, f.number));
                            }
                            fields.push(f);
                        }
                    }
                }
                if out.insert(name.clone(), fields).is_some() {
                    return Err(format!("message {name} defined twice"));
                }
            }
            w => return Err(format!("`{w}` is not supported here")),
        }
    }
    Ok(out)
}

/// `[repeated|optional] <type> <name> = <number> [ [options] ] ;` at `t[*i..]`, up to the `;`.
fn field(t: &[&str], i: &mut usize) -> Result<Field, String> {
    let at = |i: usize| t.get(i).copied().unwrap_or("end of file");
    let repeated = at(*i) == "repeated";
    if repeated || at(*i) == "optional" {
        *i += 1;
    }
    let (ty, name, eq, num) = (at(*i), at(*i + 1), at(*i + 2), at(*i + 3));
    let kind = match ty {
        "int64" | "int32" => Kind::Int,
        "uint64" | "uint32" => Kind::UInt,
        "sint64" | "sint32" => Kind::SInt,
        "bool" => Kind::Bool,
        "double" => Kind::Double,
        "float" => Kind::Float,
        "string" | "bytes" => Kind::Str,
        t => return Err(format!("unsupported proto type {t}")),
    };
    if eq != "=" || !name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
        return Err(format!("bad field `{ty} {name} {eq}`"));
    }
    let number: u64 = num.parse().map_err(|_| format!("bad field number `{num}` of {name}"))?;
    if !(1..=MAX_FIELD).contains(&number) || (19_000..=19_999).contains(&number) {
        return Err(format!("field number {number} of {name} is outside protobuf's range"));
    }
    *i += 4;
    if at(*i) == "[" {
        let n = t[*i..].iter().position(|x| *x == "]").ok_or(format!("unterminated options of {name}"))?;
        *i += n + 1;
    }
    // the `;` is left for the message loop to step over
    if at(*i) != ";" {
        return Err(format!("expected `;` after field {name}, found `{}`", at(*i)));
    }
    Ok(Field { name: name.to_string(), number: number as u32, kind, repeated })
}

/// Decodes one message into a row with the given columns (name, type); absent fields are NULL.
#[derive(Debug)]
pub struct Codec {
    by_number: rustc_hash::FxHashMap<u32, (usize, Kind, bool)>,
    columns: Vec<(String, Type)>,
    fields: Vec<Option<Field>>, // per column
}

impl Codec {
    pub fn new(fields: &[Field], columns: &[(String, Type)]) -> Codec {
        let col_fields: Vec<Option<Field>> =
            columns.iter().map(|(n, _)| fields.iter().find(|f| &f.name == n).cloned()).collect();
        let by_number = col_fields
            .iter()
            .enumerate()
            .filter_map(|(i, f)| f.as_ref().map(|f| (f.number, (i, f.kind, f.repeated))))
            .collect();
        Codec { by_number, columns: columns.to_vec(), fields: col_fields }
    }

    pub fn decode(&self, mut b: &[u8]) -> Result<Vec<Value>, String> {
        let mut row = vec![Value::Null; self.columns.len()];
        // repeated fields' values by column; a message has a few, found by a linear scan
        let mut arrays: Vec<(usize, Vec<Value>)> = vec![];
        while !b.is_empty() {
            let tag = varint(&mut b)?;
            let (num, wire) = (tag >> 3, tag & 7);
            if num > MAX_FIELD {
                return Err(format!("field number {num} is outside protobuf's range"));
            }
            let target = self.by_number.get(&(num as u32)).copied();
            let mut put = |v: Value| match target {
                Some((i, _, true)) => match arrays.iter_mut().find(|(c, _)| *c == i) {
                    Some((_, a)) => a.push(v),
                    None => arrays.push((i, vec![v])),
                },
                Some((i, _, false)) => row[i] = v,
                None => {}
            };
            match (wire, target) {
                // bool and unsigned varints come out of the final cast to the column type
                (0, Some((_, Kind::SInt, _))) => put(Value::Int(unzigzag(varint(&mut b)?))),
                (0, _) => put(Value::Int(varint(&mut b)? as i64)),
                (1, _) => put(Value::F64(f64::from_le_bytes(take(&mut b, 8)?.try_into().unwrap()))),
                (5, _) => put(Value::F32(f32::from_le_bytes(take(&mut b, 4)?.try_into().unwrap()))),
                (2, Some((_, Kind::Str, _)) | None) => {
                    let n = varint(&mut b)? as usize;
                    put(Value::Str(String::from_utf8_lossy(take(&mut b, n)?).into()));
                }
                (2, Some((i, k, repeated))) => {
                    // packed repeated scalars, decoded into a vector sized from the byte length
                    let n = varint(&mut b)? as usize;
                    let mut p = take(&mut b, n)?;
                    let mut vals = Vec::with_capacity(match k {
                        Kind::Double => n / 8,
                        Kind::Float => n / 4,
                        _ => n, // a varint takes at least one byte
                    });
                    while !p.is_empty() {
                        vals.push(match k {
                            Kind::Double => Value::F64(f64::from_le_bytes(take(&mut p, 8)?.try_into().unwrap())),
                            Kind::Float => Value::F32(f32::from_le_bytes(take(&mut p, 4)?.try_into().unwrap())),
                            Kind::SInt => Value::Int(unzigzag(varint(&mut p)?)),
                            _ => Value::Int(varint(&mut p)? as i64), // bool/uint: the final cast
                        });
                    }
                    if repeated {
                        match arrays.iter_mut().find(|(c, _)| *c == i) {
                            Some((_, a)) => a.extend(vals),
                            None => arrays.push((i, vals)),
                        }
                    } else if let Some(v) = vals.pop() {
                        // a packed encoding of a singular field: the last value wins, as for put
                        row[i] = v;
                    }
                }
                (w, _) => return Err(format!("unsupported wire type {w}")),
            }
        }
        for (i, a) in arrays {
            row[i] = Value::Array(a.into());
        }
        // values already of their column's type (the arrays of a book, mostly) are moved, not copied
        Ok(row.into_iter().zip(&self.columns).map(|(v, (_, t))| v.cast_into(t)).collect())
    }

    /// Encodes a row (in column order) as the message; NULL and empty arrays are omitted.
    pub fn encode(&self, row: &[Value], out: &mut Vec<u8>) {
        for (f, v) in self.fields.iter().zip(row) {
            let Some(f) = f else { continue };
            let items: &[Value] = match v {
                Value::Array(a) => a,
                Value::Null => &[],
                v => std::slice::from_ref(v),
            };
            if items.is_empty() {
                continue;
            }
            if f.repeated && f.kind != Kind::Str {
                let mut p = vec![];
                items.iter().for_each(|v| scalar(f.kind, v, &mut p));
                put_varint(out, (f.number as u64) << 3 | 2);
                put_varint(out, p.len() as u64);
                out.extend(p);
                continue;
            }
            for v in items {
                let wire = match f.kind {
                    Kind::Double => 1,
                    Kind::Float => 5,
                    Kind::Str => 2,
                    _ => 0,
                };
                put_varint(out, (f.number as u64) << 3 | wire);
                if f.kind == Kind::Str {
                    let s = crate::format::to_text(v);
                    put_varint(out, s.len() as u64);
                    out.extend(s.as_bytes());
                } else {
                    scalar(f.kind, v, out);
                }
            }
        }
    }
}

fn scalar(k: Kind, v: &Value, out: &mut Vec<u8>) {
    match k {
        Kind::Double => out.extend(v.f64().unwrap_or(0.0).to_le_bytes()),
        Kind::Float => out.extend((v.f64().unwrap_or(0.0) as f32).to_le_bytes()),
        Kind::SInt => put_varint(out, zigzag(v.i64().unwrap_or(0))),
        _ => put_varint(out, v.i64().unwrap_or(0) as u64),
    }
}

/// ZigZag: 0, -1, 1, -2, ... as 0, 1, 2, 3, ...
fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
    if b.len() < n {
        return Err("truncated message".into());
    }
    let (h, t) = b.split_at(n);
    *b = t;
    Ok(h)
}

fn varint(b: &mut &[u8]) -> Result<u64, String> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = b.split_first().ok_or("truncated varint")?;
        *b = rest;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return Ok(v);
        }
    }
    Err("varint too long".into())
}

pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}
