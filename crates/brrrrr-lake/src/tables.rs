//! Open table formats as lists of files, as a directory of Hive partitions is one: a Delta Lake
//! table (its `_delta_log`: commits in JSON lines, checkpoints in Parquet) and an Apache Iceberg
//! table (its metadata JSON, the current snapshot's manifest list and manifests, in Avro). Each
//! data file comes with its partitions' values, so that a `WHERE` on them skips it. A table is
//! read as it is now; what changes rows beyond their files (deletion vectors, delete files) is
//! refused rather than misread.
use crate::files::{File, Files, NULL_PARTITION};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value as J;
use std::collections::BTreeMap;

/// Whether `location` is a table of `kind` (`delta`, `iceberg`), as `files` finds one.
pub fn is(fs: &Files, location: &str, kind: &str) -> Result<bool> {
    let root = location.trim_end_matches('/');
    Ok(match kind {
        "delta" => !fs.names(&format!("{root}/_delta_log"), true)?.is_empty(),
        _ => {
            root.ends_with(".metadata.json")
                || fs.names(&format!("{root}/metadata"), false)?.iter().any(|n| n.0.ends_with(".metadata.json"))
        }
    })
}

/// The data files of the Delta or Iceberg table at `location`, if it is one: a directory with a
/// `_delta_log`, a directory with `metadata/*.metadata.json`, or an Iceberg metadata file.
pub fn files(fs: &Files, location: &str) -> Result<Option<Vec<File>>> {
    let root = location.trim_end_matches('/');
    if root.ends_with(".metadata.json") {
        let dir = root.rsplit_once("/metadata/").map_or(root, |r| r.0);
        return iceberg(fs, dir, root).map(Some);
    }
    // a file (by its name) is no table
    if crate::files::has_glob(root) || crate::files::format_of(root, None).is_some() {
        return Ok(None);
    }
    if !fs.names(&format!("{root}/_delta_log"), true)?.is_empty() {
        return delta(fs, root).with_context(|| format!("the Delta table {root}")).map(Some);
    }
    let metadata: Vec<String> = fs
        .names(&format!("{root}/metadata"), false)?
        .into_iter()
        .map(|n| n.0)
        .filter(|n| n.ends_with(".metadata.json"))
        .collect();
    // the newest: `v3.metadata.json`, `00003-<uuid>.metadata.json`
    let version = |n: &str| -> u64 {
        let base = n.rsplit('/').next().unwrap_or(n).trim_start_matches('v');
        base.split(['-', '.']).next().and_then(|v| v.parse().ok()).unwrap_or(0)
    };
    match metadata.iter().max_by_key(|n| version(n)) {
        Some(m) => iceberg(fs, root, m).map(Some),
        None => Ok(None),
    }
}

/// A path written percent-encoded (Delta's, a URI's), as it is on the store.
fn decoded(p: &str) -> String {
    let b = p.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match (b[i], b.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok())) {
            (b'%', Some(x)) => {
                out.push(x);
                i += 3;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Delta reader features a table's files are read the same with.
const DELTA_FEATURES: &[&str] = &["timestampNtz", "deletionVectors", "columnMapping", "vacuumProtocolCheck"];

/// A Delta table's files: its latest checkpoint's actions, then the commits after it, replayed.
fn delta(fs: &Files, root: &str) -> Result<Vec<File>> {
    let log = format!("{root}/_delta_log");
    let (mut commits, mut checkpoints) = (BTreeMap::new(), BTreeMap::<u64, Vec<String>>::new());
    for (name, _) in fs.names(&log, false)? {
        let base = name.rsplit('/').next().unwrap_or(&name).to_string();
        let mut parts = base.split('.');
        let Some(v) = parts.next().and_then(|v| v.parse::<u64>().ok()) else { continue };
        match parts.collect::<Vec<_>>().as_slice() {
            ["json"] => {
                commits.insert(v, name);
            }
            ["checkpoint", "parquet"] | ["checkpoint", _, _, "parquet"] => checkpoints.entry(v).or_default().push(name),
            ["checkpoint", _, "json" | "parquet"] => {
                bail!("its checkpoint {base} is a V2 checkpoint, which brrrrr does not read yet")
            }
            _ => {}
        }
    }
    // the newest whole checkpoint (a multi-part one has every part)
    let whole = |names: &Vec<String>| {
        names.len() == 1 && names[0].ends_with(".checkpoint.parquet")
            || names.iter().all(|n| n.split('.').nth(3).and_then(|p| p.parse::<usize>().ok()) == Some(names.len()))
    };
    let start = checkpoints.iter().rev().find(|(_, n)| whole(n));
    let mut actions: Vec<J> = vec![];
    if let Some((_, parts)) = start {
        let mut parts = parts.clone();
        parts.sort();
        for p in parts {
            actions.extend(checkpoint_actions(&fs.bytes(&p)?).with_context(|| p.clone())?);
        }
    }
    let from = start.map_or(0, |(v, _)| v + 1);
    for (i, (v, name)) in commits.range(from..).enumerate() {
        if *v != from + i as u64 {
            bail!("its log has no commit {} (between {from} and {v})", from + i as u64);
        }
        let body = fs.bytes(name)?;
        for line in body.split(|b| *b == b'\n').filter(|l| !l.iter().all(u8::is_ascii_whitespace)) {
            actions.push(serde_json::from_slice(line).with_context(|| name.clone())?);
        }
    }
    if start.is_none() && !commits.contains_key(&0) {
        bail!("its log has neither a checkpoint nor its first commit");
    }
    let (mut files, mut protocol, mut meta) = (BTreeMap::<String, J>::new(), J::Null, J::Null);
    for a in actions {
        if let Some(add) = a.get("add").filter(|x| !x.is_null()) {
            files.insert(add["path"].as_str().unwrap_or_default().to_string(), add.clone());
        } else if let Some(rm) = a.get("remove").filter(|x| !x.is_null()) {
            files.remove(rm["path"].as_str().unwrap_or_default());
        } else if let Some(p) = a.get("protocol").filter(|x| !x.is_null()) {
            protocol = p.clone();
        } else if let Some(m) = a.get("metaData").filter(|x| !x.is_null()) {
            meta = m.clone();
        }
    }
    // what changes how its files are read
    let features: Vec<&str> =
        protocol["readerFeatures"].as_array().into_iter().flatten().filter_map(J::as_str).collect();
    if let Some(f) = features.iter().find(|f| !DELTA_FEATURES.contains(f)) {
        bail!("it needs the reader feature {f}, which brrrrr does not have (it reads {})", DELTA_FEATURES.join(", "));
    }
    let mapping = meta["configuration"]["delta.columnMapping.mode"].as_str().unwrap_or("none");
    if mapping != "none" {
        bail!("its columns are mapped (delta.columnMapping.mode = {mapping}), which brrrrr does not read yet");
    }
    let partition_columns: Vec<&str> =
        meta["partitionColumns"].as_array().into_iter().flatten().filter_map(J::as_str).collect();
    let listed = if files.is_empty() { BTreeMap::new() } else { fs.names(root, false)?.into_iter().collect() };
    let mut out = vec![];
    for (path, add) in files {
        if add.get("deletionVector").is_some_and(|d| !d.is_null()) {
            bail!("{path} has deleted rows (a deletion vector), which brrrrr does not read yet");
        }
        let name = if path.contains("://") { decoded(&path) } else { format!("{root}/{}", decoded(&path)) };
        let partitions = partition_columns
            .iter()
            .map(|c| {
                let v = add["partitionValues"].get(*c).and_then(J::as_str).unwrap_or(NULL_PARTITION);
                (c.to_string(), v.to_string())
            })
            .collect();
        let meta = listed.get(&name).cloned().flatten();
        out.push(fs.data_file(&name, meta, partitions)?);
    }
    Ok(out)
}

/// A Delta checkpoint's actions, each as the JSON a commit writes it.
fn checkpoint_actions(parquet: &[u8]) -> Result<Vec<J>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ProjectionMask;
    let b = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(parquet))?;
    let wanted = ["add", "remove", "metaData", "protocol"];
    let roots: Vec<usize> = b
        .parquet_schema()
        .root_schema()
        .get_fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| wanted.contains(&f.name()))
        .map(|(i, _)| i)
        .collect();
    let mask = ProjectionMask::roots(b.parquet_schema(), roots);
    let mut lines = vec![];
    let mut w = arrow_json::LineDelimitedWriter::new(&mut lines);
    for batch in b.with_projection(mask).build()? {
        w.write(&batch?)?;
    }
    w.finish()?;
    lines.split(|b| *b == b'\n').filter(|l| !l.is_empty()).map(|l| Ok(serde_json::from_slice(l)?)).collect()
}

/// An Iceberg table's files: its current snapshot's manifests' live data files. Paths under the
/// location the metadata names are read under `root` instead (a table copied or moved).
fn iceberg(fs: &Files, root: &str, metadata: &str) -> Result<Vec<File>> {
    let ctx = || format!("the Iceberg table {metadata}");
    let m: J = serde_json::from_slice(&fs.bytes(metadata)?).with_context(ctx)?;
    let location = m["location"].as_str().unwrap_or(root).trim_end_matches('/').to_string();
    let here = |p: &str| -> String {
        let p = match p.strip_prefix(location.as_str()) {
            Some(rest) => format!("{root}{rest}"),
            None => p.to_string(),
        };
        p.strip_prefix("file://").map(str::to_string).unwrap_or(p)
    };
    let current = m["current-snapshot-id"].as_i64().filter(|s| *s != -1);
    let Some(snapshot) = current
        .and_then(|id| m["snapshots"].as_array()?.iter().find(|s| s["snapshot-id"].as_i64() == Some(id)).cloned())
    else {
        return Ok(vec![]); // a table with no rows yet
    };
    // each partition spec's identity fields: (partition field, source column, source type)
    let schema = match m["schemas"].as_array() {
        Some(all) => all.iter().find(|s| s["schema-id"] == m["current-schema-id"]).cloned().unwrap_or(J::Null),
        None => m["schema"].clone(),
    };
    let column = |id: &J| {
        schema["fields"].as_array().into_iter().flatten().find(|f| f["id"] == *id).map(|f| {
            (f["name"].as_str().unwrap_or_default().to_string(), f["type"].as_str().unwrap_or_default().to_string())
        })
    };
    let specs: BTreeMap<i64, Vec<(String, String, String)>> = m["partition-specs"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|s| {
            let fields = s["fields"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|f| f["transform"] == "identity")
                .filter_map(|f| {
                    let (col, ty) = column(&f["source-id"])?;
                    Some((f["name"].as_str()?.to_string(), col, ty))
                })
                .collect();
            (s["spec-id"].as_i64().unwrap_or(0), fields)
        })
        .collect();
    // the manifests: (path, content, spec)
    let manifests: Vec<(String, i64, i64)> = match snapshot["manifest-list"].as_str() {
        Some(list) => avro::records(&fs.bytes(&here(list))?)
            .with_context(|| format!("the manifest list {list}"))?
            .into_iter()
            .map(|r| {
                let path = r["manifest_path"].as_str().unwrap_or_default().to_string();
                (path, r["content"].as_i64().unwrap_or(0), r["partition_spec_id"].as_i64().unwrap_or(0))
            })
            .collect(),
        // format 1 may list them in the snapshot
        None => snapshot["manifests"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| Some((p.as_str()?.to_string(), 0, m["default-spec-id"].as_i64().unwrap_or(0))))
            .collect(),
    };
    let mut out = vec![];
    for (path, _, spec) in manifests {
        let entries = avro::records(&fs.bytes(&here(&path))?).with_context(|| format!("the manifest {path}"))?;
        for e in entries {
            // 2: deleted in this snapshot
            if e["status"].as_i64() == Some(2) {
                continue;
            }
            let f = &e["data_file"];
            let file = f["file_path"].as_str().unwrap_or_default();
            if f["content"].as_i64().unwrap_or(0) != 0 {
                bail!(
                    "{}: {file} deletes rows (a delete file), which brrrrr does not read yet: compact the table \
                     (rewrite_data_files) to read it",
                    ctx()
                );
            }
            let format = f["file_format"].as_str().unwrap_or("PARQUET");
            if !format.eq_ignore_ascii_case("parquet") {
                bail!("{}: {file} is {format}; brrrrr reads Iceberg tables of Parquet files", ctx());
            }
            let partitions = specs
                .get(&spec)
                .into_iter()
                .flatten()
                .filter_map(|(field, col, ty)| Some((col.clone(), identity_text(&f["partition"][field.as_str()], ty)?)))
                .collect();
            out.push(fs.data_file(&here(file), None, partitions)?);
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// An identity partition's value as text, as a Hive directory would have it; `None` for one
/// brrrrr does not compare (its files are then read whatever the `WHERE`).
fn identity_text(v: &J, ty: &str) -> Option<String> {
    Some(match v {
        J::Null => NULL_PARTITION.to_string(),
        J::String(s) => s.clone(),
        J::Bool(b) => b.to_string(),
        J::Number(n) if ty == "date" => {
            let days = n.as_i64()?;
            brrrrr_core::query::text(&brrrrr_core::value::Value::Time(days * 86_400_000_000))[..10].to_string()
        }
        J::Number(n) if ty.starts_with("timestamp") => {
            brrrrr_core::query::text(&brrrrr_core::value::Value::Time(n.as_i64()?))
        }
        J::Number(n) => n.to_string(),
        _ => return None,
    })
}

/// Avro object container files as JSON values (Iceberg's manifests): the writer's schema from
/// the header, blocks uncompressed (`null`, `deflate`, `snappy`, `zstandard`). Bytes and fixed
/// values are given as arrays of their bytes.
pub mod avro {
    use super::*;

    struct Cursor<'a> {
        b: &'a [u8],
        at: usize,
    }

    impl<'a> Cursor<'a> {
        fn take(&mut self, n: usize) -> Result<&'a [u8]> {
            let s = self.b.get(self.at..self.at + n).ok_or(anyhow!("an Avro file cut short"))?;
            self.at += n;
            Ok(s)
        }
        /// A zigzag varint.
        fn long(&mut self) -> Result<i64> {
            let (mut v, mut shift) = (0u64, 0);
            loop {
                let b = self.take(1)?[0];
                v |= u64::from(b & 0x7f) << shift;
                if b & 0x80 == 0 {
                    return Ok((v >> 1) as i64 ^ -((v & 1) as i64));
                }
                shift += 7;
                if shift > 63 {
                    bail!("an Avro number too long");
                }
            }
        }
        fn bytes(&mut self) -> Result<&'a [u8]> {
            let n = usize::try_from(self.long()?).map_err(|_| anyhow!("an Avro length below 0"))?;
            self.take(n)
        }
    }

    /// The records of an Avro object container file.
    pub fn records(data: &[u8]) -> Result<Vec<J>> {
        let mut c = Cursor { b: data, at: 0 };
        if c.take(4)? != b"Obj\x01" {
            bail!("not an Avro file");
        }
        let mut meta = BTreeMap::new();
        blocks(&mut c, |c| {
            let k = String::from_utf8_lossy(c.bytes()?).into_owned();
            meta.insert(k, c.bytes()?.to_vec());
            Ok(())
        })?;
        let schema: J =
            serde_json::from_slice(meta.get("avro.schema").ok_or(anyhow!("an Avro file without a schema"))?)?;
        let codec = meta.get("avro.codec").map_or("null".into(), |c| String::from_utf8_lossy(c).into_owned());
        let sync = c.take(16)?;
        let mut named = BTreeMap::new();
        let mut out = vec![];
        while c.at < data.len() {
            let n = c.long()?;
            let block = c.bytes()?;
            let block = match codec.as_str() {
                "null" => block.to_vec(),
                "deflate" => {
                    let mut v = vec![];
                    std::io::Read::read_to_end(&mut flate2::read::DeflateDecoder::new(block), &mut v)?;
                    v
                }
                // a snappy block, then its CRC-32
                "snappy" => snap::raw::Decoder::new().decompress_vec(&block[..block.len().saturating_sub(4)])?,
                "zstandard" => zstd::decode_all(block)?,
                c => bail!("Avro blocks compressed with {c}, which brrrrr does not read"),
            };
            let mut b = Cursor { b: &block, at: 0 };
            for _ in 0..n {
                out.push(value(&mut b, &schema, &mut named)?);
            }
            if c.take(16)? != sync {
                bail!("an Avro file whose blocks do not end in its sync marker");
            }
        }
        Ok(out)
    }

    /// Avro's blocks of items (arrays and maps): counts, a negative one followed by a size.
    fn blocks(c: &mut Cursor, mut item: impl FnMut(&mut Cursor) -> Result<()>) -> Result<()> {
        loop {
            let mut n = c.long()?;
            if n == 0 {
                return Ok(());
            }
            if n < 0 {
                n = -n;
                c.long()?;
            }
            for _ in 0..n {
                item(c)?;
            }
        }
    }

    fn value(c: &mut Cursor, schema: &J, named: &mut BTreeMap<String, J>) -> Result<J> {
        match schema {
            J::Array(union) => {
                let i = usize::try_from(c.long()?).ok().filter(|i| *i < union.len());
                value(c, &union[i.ok_or(anyhow!("an Avro union's branch out of range"))?], named)
            }
            J::String(t) => match t.as_str() {
                "null" => Ok(J::Null),
                "boolean" => Ok(J::Bool(c.take(1)?[0] != 0)),
                "int" | "long" => Ok(J::from(c.long()?)),
                "float" => Ok(J::from(f32::from_le_bytes(c.take(4)?.try_into()?) as f64)),
                "double" => Ok(J::from(f64::from_le_bytes(c.take(8)?.try_into()?))),
                "bytes" => Ok(J::from(c.bytes()?.to_vec())),
                "string" => Ok(J::from(String::from_utf8_lossy(c.bytes()?).into_owned())),
                name => {
                    let s = named.get(name).cloned().ok_or(anyhow!("an Avro type {name} not defined"))?;
                    value(c, &s, named)
                }
            },
            J::Object(o) => {
                let ty = o.get("type").ok_or(anyhow!("an Avro schema without a type"))?;
                if let Some(name) = o.get("name").and_then(J::as_str) {
                    named.insert(name.to_string(), schema.clone());
                    if let Some(ns) = o.get("namespace").and_then(J::as_str) {
                        named.insert(format!("{ns}.{name}"), schema.clone());
                    }
                }
                match ty.as_str() {
                    Some("record" | "error") => {
                        let mut out = serde_json::Map::new();
                        for f in o.get("fields").and_then(J::as_array).into_iter().flatten() {
                            let name = f["name"].as_str().unwrap_or_default().to_string();
                            out.insert(name, value(c, &f["type"], named)?);
                        }
                        Ok(J::Object(out))
                    }
                    Some("enum") => {
                        let i = c.long()?;
                        Ok(o["symbols"].get(i as usize).cloned().unwrap_or(J::Null))
                    }
                    Some("array") => {
                        let mut out = vec![];
                        blocks(c, |c| {
                            out.push(value(c, &o["items"], named)?);
                            Ok(())
                        })?;
                        Ok(J::Array(out))
                    }
                    Some("map") => {
                        let mut out = serde_json::Map::new();
                        blocks(c, |c| {
                            let k = String::from_utf8_lossy(c.bytes()?).into_owned();
                            out.insert(k, value(c, &o["values"], named)?);
                            Ok(())
                        })?;
                        Ok(J::Object(out))
                    }
                    Some("fixed") => {
                        let n = o["size"].as_u64().ok_or(anyhow!("an Avro fixed without a size"))? as usize;
                        Ok(J::from(c.take(n)?.to_vec()))
                    }
                    // a primitive with a logical type (`{"type": "int", "logicalType": "date"}`)
                    _ => value(c, ty, named),
                }
            }
            s => bail!("an Avro schema {s} brrrrr does not read"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Avro's zigzag varint.
    fn long(v: i64, out: &mut Vec<u8>) {
        let mut z = ((v << 1) ^ (v >> 63)) as u64;
        loop {
            let b = (z & 0x7f) as u8;
            z >>= 7;
            if z == 0 {
                return out.push(b);
            }
            out.push(b | 0x80);
        }
    }

    #[test]
    fn avro_values_and_paths() {
        // a file of one record, uncompressed
        let schema = r#"{"type":"record","name":"r","fields":[{"name":"a","type":"long"},{"name":"s","type":"string"},
            {"name":"u","type":["null","int"]},{"name":"l","type":{"type":"array","items":"int"}}]}"#;
        let mut f = b"Obj\x01".to_vec();
        long(1, &mut f); // a map of one entry
        long(11, &mut f);
        f.extend(b"avro.schema");
        long(schema.len() as i64, &mut f);
        f.extend(schema.as_bytes());
        long(0, &mut f);
        f.extend([7u8; 16]);
        let mut record = vec![];
        for v in [300, 1] {
            long(v, &mut record);
        }
        record.push(b'x');
        for v in [0, 2, 1, -2, 0] {
            long(v, &mut record);
        }
        long(1, &mut f);
        long(record.len() as i64, &mut f);
        f.extend(record);
        f.extend([7u8; 16]);
        assert_eq!(avro::records(&f).unwrap(), [serde_json::json!({"a": 300, "s": "x", "u": null, "l": [1, -2]})]);
        assert_eq!(decoded("date=2024-01-01/a%20b%3D.parquet"), "date=2024-01-01/a b=.parquet");
        assert_eq!(identity_text(&J::from(19724), "date").as_deref(), Some("2024-01-02"));
        assert_eq!(identity_text(&J::Null, "string").as_deref(), Some(NULL_PARTITION));
    }
}
