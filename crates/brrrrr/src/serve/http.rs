//! The HTTP API and console of `brrrrr serve`, on plain std threads (a thread per connection,
//! `Connection: close`): small, and enough for a console, scripts and dashboards.
//!
//! | route | what |
//! | --- | --- |
//! | `GET /` | the console |
//! | `POST /query` (SQL in the body), `GET /query?q=` | a statement's answer: JSON (`{columns, types, rows, elapsed_ms}`), or `format=csv`, `jsonl`, `parquet` |
//! | `POST /write/<table>` | rows: JSON lines, or CSV (`Content-Type: text/csv` or `format=csv`); `time=<column>` on the first write names the time column |
//! | `GET /tables` | the tables and views, their columns and how many rows they hold |
//! | `GET /subscribe/<table>` | server-sent events: each new row, as JSON |
//! | `GET /watch?q=&every=1s` | server-sent events: the query's answer, each time it changes |
//! | `POST /flush` | the rows in memory to Parquet now |
//! | `GET /health`, `GET /metrics` | liveness, Prometheus counters |
use super::{Server, Stop};
use anyhow::Result;
use brrrrr_lake::write::{self, Out};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CONSOLE: &str = include_str!("console.html");
/// The largest body a request may send (rows written in one request).
const MAX_BODY: usize = 256 << 20;

pub fn serve(addr: &str, server: Arc<Server>) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    for stream in listener.incoming().flatten() {
        let s = server.clone();
        std::thread::Builder::new().name("http".into()).spawn(move || {
            let _ = handle(stream, &s);
        })?;
    }
    Ok(())
}

struct Request {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &TcpStream) -> std::io::Result<Request> {
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    r.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let (method, target) = (parts.next().unwrap_or("").to_string(), parts.next().unwrap_or("/").to_string());
    let mut headers = BTreeMap::new();
    loop {
        let mut h = String::new();
        if r.read_line(&mut h)? == 0 || h.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let n: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    if n > MAX_BODY {
        return Err(std::io::Error::other("body too large"));
    }
    let mut body = vec![0; n];
    r.read_exact(&mut body)?;
    let (path, q) = target.split_once('?').unwrap_or((&target, ""));
    let query = url::form_urlencoded::parse(q.as_bytes()).into_owned().collect();
    Ok(Request { method, path: path.to_string(), query, headers, body })
}

fn respond(s: &mut TcpStream, status: &str, kind: &str, body: &[u8]) -> std::io::Result<()> {
    write!(
        s,
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    s.write_all(body)
}

fn json_error(s: &mut TcpStream, status: &str, msg: &str) -> std::io::Result<()> {
    let mut b = String::from("{\"error\":");
    brrrrr_core::format::json(&mut b, &brrrrr_core::value::Value::Str(msg.into()), &brrrrr_core::value::Type::Str);
    b.push('}');
    respond(s, status, "application/json", b.as_bytes())
}

/// Whether the request carries the server's token (a header, or `token=` for event streams,
/// which browsers open without headers).
fn authorized(server: &Server, r: &Request) -> bool {
    let Some(token) = &server.token else { return true };
    let given = r
        .headers
        .get("authorization")
        .and_then(|h| h.strip_prefix("Bearer "))
        .or(r.query.get("token").map(String::as_str));
    // compared in constant time: the token is a secret
    given
        .is_some_and(|g| g.len() == token.len() && g.bytes().zip(token.bytes()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0)
}

fn handle(mut stream: TcpStream, server: &Server) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    let r = match read_request(&stream) {
        Ok(r) => r,
        Err(e) => return json_error(&mut stream, "400 Bad Request", &e.to_string()),
    };
    let route = (r.method.as_str(), r.path.as_str());
    if r.method == "OPTIONS" {
        return write!(
            stream,
            "HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: Authorization, Content-Type\r\nAccess-Control-Allow-Methods: GET, POST\r\nConnection: close\r\n\r\n"
        );
    }
    match route {
        ("GET", "/" | "/index.html") => {
            return respond(&mut stream, "200 OK", "text/html; charset=utf-8", CONSOLE.as_bytes())
        }
        ("GET", "/health") => return respond(&mut stream, "200 OK", "text/plain", b"ok\n"),
        _ => {}
    }
    if !authorized(server, &r) {
        return json_error(&mut stream, "401 Unauthorized", "a token is needed: Authorization: Bearer <token>");
    }
    match route {
        ("GET", "/metrics") => {
            let c = &server.counters;
            let (running, waiting) = server.running();
            let mut body = String::new();
            for (name, kind, v) in [
                ("brrrrr_queries_total", "counter", c.queries.load(Ordering::Relaxed)),
                ("brrrrr_query_errors_total", "counter", c.query_errors.load(Ordering::Relaxed)),
                ("brrrrr_statements_timed_out_total", "counter", c.timed_out.load(Ordering::Relaxed)),
                ("brrrrr_statements_canceled_total", "counter", c.canceled.load(Ordering::Relaxed)),
                ("brrrrr_statements_abandoned_total", "counter", c.abandoned.load(Ordering::Relaxed)),
                ("brrrrr_statements_refused_busy_total", "counter", c.busy.load(Ordering::Relaxed)),
                ("brrrrr_statements_running", "gauge", u64::from(running)),
                ("brrrrr_statements_waiting", "gauge", u64::from(waiting)),
                ("brrrrr_rows_written_total", "counter", c.rows_written.load(Ordering::Relaxed)),
                ("brrrrr_rows_flushed_total", "counter", c.rows_flushed.load(Ordering::Relaxed)),
                ("brrrrr_view_rows_total", "counter", c.view_rows.load(Ordering::Relaxed)),
                ("brrrrr_uptime_seconds", "gauge", server.started.elapsed().as_secs()),
            ] {
                body.push_str(&format!("# TYPE {name} {kind}\n{name} {v}\n"));
            }
            respond(&mut stream, "200 OK", "text/plain; version=0.0.4", body.as_bytes())
        }
        ("GET" | "POST", "/query") => {
            let sql = match r.query.get("q") {
                Some(q) => q.clone(),
                None => String::from_utf8_lossy(&r.body).to_string(),
            };
            query(&mut stream, server, &sql, &r)
        }
        ("GET", "/tables") => {
            let mut b = String::from("[");
            for (i, t) in server.tables().iter().enumerate() {
                let (name, kind, cols, rows, files) = (&t.name, t.kind, &t.columns, t.rows, t.files);
                if i > 0 {
                    b.push(',');
                }
                let s = |v: &str| {
                    let mut o = String::new();
                    brrrrr_core::format::json(
                        &mut o,
                        &brrrrr_core::value::Value::Str(v.into()),
                        &brrrrr_core::value::Type::Str,
                    );
                    o
                };
                let cols: Vec<String> =
                    cols.iter().map(|(c, t)| format!("{{\"name\":{},\"type\":{}}}", s(c), s(t))).collect();
                b.push_str(&format!(
                    "{{\"name\":{},\"kind\":{},\"columns\":[{}],\"rows_in_memory\":{rows},\"files\":{files}}}",
                    s(name),
                    s(kind),
                    cols.join(",")
                ));
            }
            b.push(']');
            respond(&mut stream, "200 OK", "application/json", b.as_bytes())
        }
        ("POST", "/flush") => match server.flush() {
            Ok(n) => respond(&mut stream, "200 OK", "application/json", format!("{{\"rows_flushed\":{n}}}").as_bytes()),
            Err(e) => json_error(&mut stream, "500 Internal Server Error", &brrrrr_lake::message(&e)),
        },
        ("POST", p) if p.starts_with("/write/") => {
            let table = &p["/write/".len()..];
            let csv = r.query.get("format").is_some_and(|f| f == "csv")
                || r.headers.get("content-type").is_some_and(|c| c.contains("csv"));
            match server.write(table, &r.body, csv, r.query.get("time").map(String::as_str)) {
                Ok(n) => respond(
                    &mut stream,
                    "200 OK",
                    "application/json",
                    format!("{{\"table\":\"{table}\",\"rows\":{n}}}").as_bytes(),
                ),
                Err(e) => json_error(&mut stream, "400 Bad Request", &brrrrr_lake::message(&e)),
            }
        }
        ("GET", p) if p.starts_with("/subscribe/") => {
            let rx = server.subscribe(&p["/subscribe/".len()..]);
            events(&mut stream)?;
            loop {
                match rx.recv_timeout(Duration::from_secs(15)) {
                    Ok(lines) => {
                        let mut out = String::new();
                        for l in lines.lines() {
                            out.push_str("data: ");
                            out.push_str(l);
                            out.push_str("\n\n");
                        }
                        stream.write_all(out.as_bytes())?;
                    }
                    // a comment keeps the connection open, and finds one that is gone
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => stream.write_all(b": keepalive\n\n")?,
                    Err(_) => return Ok(()),
                }
                stream.flush()?;
            }
        }
        ("GET", "/watch") => {
            let Some(sql) = r.query.get("q").cloned() else {
                return json_error(&mut stream, "400 Bad Request", "/watch?q=<query>");
            };
            let every = r
                .query
                .get("every")
                .and_then(|e| brrrrr_core::value::duration_us(e))
                .map_or(Duration::from_secs(1), |us| Duration::from_micros(us as u64).max(Duration::from_millis(100)));
            events(&mut stream)?;
            let mut last = String::new();
            loop {
                let start = Instant::now();
                let body = match server.execute(&sql, &Stop::with_limit(None)) {
                    Ok(a) => answer_json(&a, start.elapsed()),
                    Err(e) => {
                        format!("{{\"error\":\"{}\"}}", brrrrr_lake::message(&e).replace('"', "'").replace('\n', " "))
                    }
                };
                // only an answer that changed (its rows, not its timing)
                let key = body.split(",\"elapsed_ms\"").next().unwrap_or("").to_string();
                if key != last {
                    stream.write_all(format!("data: {body}\n\n").as_bytes())?;
                    last = key;
                } else {
                    stream.write_all(b": unchanged\n\n")?;
                }
                stream.flush()?;
                std::thread::sleep(every);
            }
        }
        _ => json_error(&mut stream, "404 Not Found", &format!("no route {} {}", r.method, r.path)),
    }
}

fn events(stream: &mut TcpStream) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()
}

/// An answer as JSON: `{columns, types, rows, elapsed_ms}` or `{message, elapsed_ms}`.
fn answer_json(a: &brrrrr_lake::Answer, took: Duration) -> String {
    let ms = took.as_secs_f64() * 1000.0;
    if let Some(m) = &a.message {
        if a.columns.is_empty() {
            let mut s = String::from("{\"message\":");
            brrrrr_core::format::json(
                &mut s,
                &brrrrr_core::value::Value::Str(m.as_str().into()),
                &brrrrr_core::value::Type::Str,
            );
            s.push_str(&format!(",\"elapsed_ms\":{ms:.3}}}"));
            return s;
        }
    }
    let mut s = String::from("{\"columns\":[");
    for (i, c) in a.columns.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        brrrrr_core::format::json(
            &mut s,
            &brrrrr_core::value::Value::Str(c.as_str().into()),
            &brrrrr_core::value::Type::Str,
        );
    }
    s.push_str("],\"types\":[");
    let types = write::types(&a.columns, &a.rows);
    s.push_str(&types.iter().map(|t| format!("\"{t}\"")).collect::<Vec<_>>().join(","));
    s.push_str("],\"rows\":[");
    for (i, r) in a.rows.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('[');
        for (j, v) in r.iter().enumerate() {
            if j > 0 {
                s.push(',');
            }
            write::json_value(&mut s, v);
        }
        s.push(']');
    }
    s.push_str(&format!("],\"elapsed_ms\":{ms:.3}}}"));
    s
}

/// Stops `stop` if the client hangs up before its answer (a closed tab, an interrupted curl), until
/// `done` is set.
fn on_hang_up(stream: &TcpStream, stop: Arc<Stop>, done: Arc<AtomicBool>) -> std::io::Result<()> {
    let probe = stream.try_clone()?;
    // the request is read: all the connection does now is write, so a short timeout harms nothing
    probe.set_read_timeout(Some(Duration::from_millis(50)))?;
    std::thread::Builder::new().name("http-hang-up".into()).spawn(move || {
        let mut b = [0u8; 1];
        while !done.load(Ordering::Relaxed) {
            match probe.peek(&mut b) {
                Ok(0) => {
                    stop.stop(super::HUNG_UP);
                    break;
                }
                Ok(_) => break, // it sent more: still there
                Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
                Err(_) => {
                    stop.stop(super::HUNG_UP);
                    break;
                }
            }
        }
    })?;
    Ok(())
}

fn query(stream: &mut TcpStream, server: &Server, sql: &str, r: &Request) -> std::io::Result<()> {
    let start = Instant::now();
    let (stop, done) = (Stop::with_limit(None), Arc::new(AtomicBool::new(false)));
    on_hang_up(stream, stop.clone(), done.clone())?;
    let answer = server.execute(sql, &stop);
    done.store(true, Ordering::Relaxed);
    let mut answer = match answer {
        Ok(a) => a,
        Err(e) => return json_error(stream, "400 Bad Request", &brrrrr_lake::message(&e)),
    };
    // a client's page of rows: `limit` (the console asks for a screenful)
    if let Some(l) = r.query.get("limit").and_then(|l| l.parse::<usize>().ok()) {
        answer.rows.truncate(l);
    }
    if let (Some(m), true) = (&answer.message, answer.columns.is_empty()) {
        if r.query.get("format").is_some_and(|f| f != "json") {
            return respond(stream, "200 OK", "text/plain; charset=utf-8", format!("{m}\n").as_bytes());
        }
    }
    match r.query.get("format").map(String::as_str) {
        None | Some("json") => {
            respond(stream, "200 OK", "application/json", answer_json(&answer, start.elapsed()).as_bytes())
        }
        Some(f) => {
            let (out, kind) = match f {
                "csv" => (Out::Csv, "text/csv"),
                "jsonl" | "ndjson" => (Out::Json, "application/x-ndjson"),
                "parquet" => (Out::Parquet, "application/vnd.apache.parquet"),
                "table" => (Out::Table, "text/plain; charset=utf-8"),
                f => {
                    return json_error(
                        stream,
                        "400 Bad Request",
                        &format!("format {f}: json, csv, jsonl, parquet or table"),
                    )
                }
            };
            let mut body = vec![];
            if let Err(e) = write::write(&mut body, out, &answer.columns, &answer.rows, usize::MAX) {
                return json_error(stream, "500 Internal Server Error", &format!("{e:#}"));
            }
            respond(stream, "200 OK", kind, &body)
        }
    }
}
