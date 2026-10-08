//! brrrrr for Python (`import brrrrr`): a session that runs SQL over files, object stores and
//! DataFrames (`brrrrr_lake`), and results any Arrow-speaking library takes without a copy
//! (the Arrow PyCapsule interface: pyarrow, Polars, pandas, DuckDB).
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use brrrrr_core::value::Value;
use brrrrr_lake::write::{self, Out};
use brrrrr_lake::Lake;
use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PyTuple};
use std::sync::Mutex;

pyo3::create_exception!(
    _brrrrr,
    Error,
    PyException,
    "A statement brrrrr could not run: the reason, and what to write instead."
);

fn err(e: anyhow::Error) -> PyErr {
    Error::new_err(brrrrr_lake::message(&e))
}

/// A session: its named tables (files, object stores, DataFrames) and views.
#[pyclass(module = "brrrrr._brrrrr")]
struct Connection {
    lake: Mutex<Lake>,
}

#[pymethods]
impl Connection {
    #[new]
    #[pyo3(signature = (threads=None))]
    fn new(threads: Option<usize>) -> Connection {
        let mut lake = Lake::new();
        if let Some(t) = threads {
            lake.threads = t.max(1);
        }
        Connection { lake: Mutex::new(lake) }
    }

    /// Runs one statement (or several, `;`-separated: the last one's result).
    fn execute(&self, py: Python<'_>, sql: &str) -> PyResult<Result> {
        let statements = split(sql);
        let lake = &self.lake;
        // the engine runs without the GIL: other Python threads go on
        py.detach(|| {
            let mut lake = lake.lock().unwrap_or_else(|p| p.into_inner());
            let mut last = None;
            for s in &statements {
                last = Some(lake.execute(s).map_err(err)?);
            }
            let a = last.unwrap_or_default();
            Ok(Result { columns: a.columns, rows: a.rows, message: a.message })
        })
    }

    /// Names a location (a path, glob, directory or URL) as a table.
    fn register_location(&self, name: &str, location: &str) {
        self.lake.lock().unwrap_or_else(|p| p.into_inner()).register(name, location);
    }

    /// Names an Arrow stream (a pyarrow Table, a Polars or pandas DataFrame: anything with
    /// `__arrow_c_stream__`) as a table, held in memory.
    fn register_arrow(&self, name: &str, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let batches = import(data)?;
        self.lake.lock().unwrap_or_else(|p| p.into_inner()).register_batches(name, batches).map_err(err)
    }

    fn unregister(&self, name: &str) {
        self.lake.lock().unwrap_or_else(|p| p.into_inner()).unregister(name);
    }
}

/// `;`-separated statements, quotes and comments respected.
fn split(script: &str) -> Vec<String> {
    let (mut out, mut cur, mut quote) = (vec![], String::new(), None);
    let mut chars = script.chars().peekable();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => {
                quote = None;
                cur.push(c);
            }
            (Some(_), c) => cur.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                cur.push(c);
            }
            (None, '-') if chars.peek() == Some(&'-') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
                cur.push('\n');
            }
            (None, ';') => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            (None, c) => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// An Arrow stream's batches, from any object with `__arrow_c_stream__`.
fn import(data: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    if !data.hasattr("__arrow_c_stream__")? {
        return Err(PyTypeError::new_err(format!(
            "{} has no __arrow_c_stream__: register a pyarrow Table, or a Polars or pandas (>= 2.2) DataFrame",
            data.get_type().name()?
        )));
    }
    let capsule = data.call_method0("__arrow_c_stream__")?.cast_into::<PyCapsule>()?;
    let name = capsule.name()?.map(|n| unsafe { n.as_cstr() }.to_string_lossy().to_string());
    if name.as_deref() != Some("arrow_array_stream") {
        return Err(PyValueError::new_err("__arrow_c_stream__ gave a capsule that is not an arrow_array_stream"));
    }
    let ptr = capsule.pointer_checked(Some(c"arrow_array_stream"))?.as_ptr() as *mut FFI_ArrowArrayStream;
    // SAFETY: the capsule holds an FFI_ArrowArrayStream (its name says so, per the Arrow
    // PyCapsule interface); `from_raw` moves the stream out, leaving the capsule's released.
    let reader = unsafe { ArrowArrayStreamReader::from_raw(ptr) }.map_err(|e| PyValueError::new_err(e.to_string()))?;
    reader.collect::<std::result::Result<Vec<_>, _>>().map_err(|e| PyValueError::new_err(e.to_string()))
}

/// A statement's result: its columns and rows, or a message (`COPY`'s, a view's creation).
#[pyclass(module = "brrrrr._brrrrr")]
struct Result {
    columns: Vec<String>,
    /// In the batches of columns the query made: rows are made only for `fetchall`.
    rows: write::Rows,
    message: Option<String>,
}

fn py_value(py: Python<'_>, v: &Value) -> PyResult<Py<PyAny>> {
    Ok(match v {
        Value::Null => py.None(),
        Value::Bool(b) => b.into_pyobject(py)?.to_owned().into_any().unbind(),
        Value::Int(i) => i.into_pyobject(py)?.into_any().unbind(),
        Value::UInt(u) => u.into_pyobject(py)?.into_any().unbind(),
        Value::F32(f) => (*f as f64).into_pyobject(py)?.into_any().unbind(),
        Value::F64(f) => f.into_pyobject(py)?.into_any().unbind(),
        Value::Str(s) => s.as_ref().into_pyobject(py)?.into_any().unbind(),
        Value::Time(us) => {
            // an aware datetime in UTC
            let dt = py.import("datetime")?;
            let utc = dt.getattr("timezone")?.getattr("utc")?;
            let epoch = dt.getattr("datetime")?.call1((1970, 1, 1, 0, 0, 0, 0, utc))?;
            let delta = dt.getattr("timedelta")?.call1((0, 0, *us))?;
            epoch.call_method1("__add__", (delta,))?.unbind()
        }
        Value::Array(a) => {
            let items = a.iter().map(|x| py_value(py, x)).collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, items)?.into_any().unbind()
        }
    })
}

#[pymethods]
impl Result {
    /// The result's column names.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// What a statement without rows said (`COPY`: the rows written), else None.
    #[getter]
    fn message(&self) -> Option<String> {
        self.message.clone()
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    /// Every row, as a tuple.
    fn fetchall(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.rows
            .iter()
            .map(|r| {
                let items = r.iter().map(|v| py_value(py, v)).collect::<PyResult<Vec<_>>>()?;
                Ok(PyTuple::new(py, items)?.into_any().unbind())
            })
            .collect()
    }

    /// The first row, or None.
    fn fetchone(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        Ok(self.fetchall(py)?.into_iter().next())
    }

    /// The Arrow PyCapsule interface: the rows as an Arrow stream any library takes.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema; // the batches' own types are given (the consumer may cast)
                                  // the query's batches as they are, no row made
        let schema = write::schema(&self.columns, self.rows.batches());
        let batches = write::record_batches(&self.columns, self.rows.batches()).map_err(err)?;
        let reader: Box<dyn RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(batches.into_iter().map(Ok), schema));
        let stream = FFI_ArrowArrayStream::new(reader);
        PyCapsule::new_with_value(py, stream, c"arrow_array_stream")
    }

    /// Writes the result to a file or object store URL (its format from its name: .parquet,
    /// .csv, .json).
    fn write(&self, path: &str) -> PyResult<()> {
        let format = Out::of_path(path).unwrap_or(Out::Parquet);
        let tmp = std::env::temp_dir().join(format!(
            ".brrrrr-py-{}-{}",
            std::process::id(),
            path.rsplit('/').next().unwrap_or("out")
        ));
        let r = (|| -> anyhow::Result<()> {
            let mut f = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
            write::write(&mut f, format, &self.columns, &self.rows, usize::MAX)?;
            std::io::Write::flush(&mut f)?;
            drop(f);
            brrrrr_lake::files::Files::default().upload(&tmp, path)
        })();
        let _ = std::fs::remove_file(&tmp);
        r.map_err(err)
    }

    /// The rows as a table to read (at most `max_rows`, the first and last halves).
    #[pyo3(signature = (max_rows=40))]
    fn table(&self, max_rows: usize) -> String {
        match &self.message {
            Some(m) if self.columns.is_empty() => m.clone(),
            _ => write::table(&self.columns, &self.rows, max_rows),
        }
    }

    fn __repr__(&self) -> String {
        self.table(40)
    }
}

#[pymodule]
fn _brrrrr(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Connection>()?;
    m.add_class::<Result>()?;
    m.add("Error", m.py().get_type::<Error>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
