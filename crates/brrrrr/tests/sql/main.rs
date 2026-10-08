//! Acceptance tests of `brrrrr sql`: the Gherkin features under `tests/acceptance/sql` run the
//! built binary as a user would, in a directory of their own, against real object stores in
//! Docker: S3 (versitygw, with credentials), GCS (fake-gcs-server), Azure Blob (Azurite) and an
//! HTTP server of the test's own. Each emulator starts on first use and is removed at the end.
//!
//! Placeholders in steps: `{bucket}` is the scenario's bucket (or container), `{http}` the URL of
//! the scenario's directory served over HTTP.
use cucumber::{given, then, when, StatsWriter, World};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const S3_PORT: u16 = 27070;
const GCS_PORT: u16 = 24443;
const AZURE_PORT: u16 = 30000;
const S3_KEY: (&str, &str) = ("brrrrr", "brrrrr-secret-key");
const AZURE_ACCOUNT: &str = "devstoreaccount1";
const AZURE_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

static NEXT: AtomicUsize = AtomicUsize::new(0);
static STARTED: Mutex<Vec<String>> = Mutex::new(vec![]);

#[derive(Debug, World)]
#[world(init = Self::new)]
pub struct Sql {
    dir: tempfile::TempDir,
    id: usize,
    env: Vec<(String, String)>,
    out: Option<Output>,
    http: Option<String>,
    /// A `brrrrr serve` of the scenario: the process, its HTTP and PostgreSQL ports, its token.
    server: Option<Served>,
    /// The last HTTP answer: its status and body.
    answer: Option<(u16, String)>,
    /// A subscription's events, as they come.
    events: Option<std::sync::mpsc::Receiver<String>>,
    /// The next extended query's parameters: (PostgreSQL type, value as text).
    params: Vec<(String, String)>,
}

#[derive(Debug)]
struct Served {
    child: std::process::Child,
    args: Vec<String>,
    http: u16,
    pg: u16,
    token: Option<String>,
}

impl Drop for Served {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Sql {
    fn new() -> Sql {
        let id = NEXT.fetch_add(1, Ordering::SeqCst);
        let dir = tempfile::tempdir().expect("a directory");
        let cache = dir.path().join(".cache").display().to_string();
        Sql {
            dir,
            id,
            env: vec![("BRRRRR_CACHE".into(), cache)],
            out: None,
            http: None,
            server: None,
            answer: None,
            events: None,
            params: vec![],
        }
    }

    fn bucket(&self) -> String {
        format!("brrrrr-test-{}-{}", std::process::id(), self.id)
    }

    fn expand(&self, s: &str) -> String {
        let s = s.replace("{bucket}", &self.bucket());
        // a query that works for a while on any machine: big.csv 64 times over, a pattern matched
        // on every row
        let s = s.replace(
            "{slow}",
            &format!(
                "SELECT count(*) AS n FROM ({}) WHERE regexp_matches(symbol || CAST(price AS VARCHAR) || symbol, '^(A|B)+[0-9.]+x?(A|B)+$')",
                ["SELECT * FROM 'big.csv'"; 64].join(" UNION ALL ")
            ),
        );
        match &self.http {
            Some(h) => s.replace("{http}", h),
            None => s,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn run(&mut self, args: &[String], stdin: Option<&str>) {
        let mut c = Command::new(env!("CARGO_BIN_EXE_brrrrr"));
        c.args(args).current_dir(self.dir.path()).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        // nothing of the outer environment's stores leaks in
        for k in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT",
            "AWS_REGION",
            "AWS_PROFILE",
        ] {
            c.env_remove(k);
        }
        c.envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let mut child = c.spawn().expect("brrrrr runs");
        if let Some(input) = stdin {
            child.stdin.take().expect("stdin").write_all(input.as_bytes()).expect("written");
        }
        drop(child.stdin.take());
        self.out = Some(child.wait_with_output().expect("brrrrr ends"));
    }

    fn output(&self) -> &Output {
        self.out.as_ref().expect("a command ran")
    }

    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output().stdout).to_string()
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output().stderr).to_string()
    }
}

/// Splits a command line into words: blanks separate, quotes group (and are kept off), a
/// backslash keeps the next character as it is.
fn words(s: &str) -> Vec<String> {
    let (mut out, mut cur, mut quote, mut any) = (vec![], String::new(), None, false);
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            cur.extend(chars.next());
            continue;
        }
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => cur.push(c),
            (None, '"' | '\'') => {
                quote = Some(c);
                any = true;
            }
            (None, c) if c.is_whitespace() => {
                if any || !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                any = false;
            }
            (None, c) => cur.push(c),
        }
    }
    if any || !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker").args(args).output().expect("docker runs");
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// Starts a container once per run (removing one of its name first); waits for its port.
fn emulator(name: &str, port: u16, args: &[&str]) {
    static LOCK: Mutex<()> = Mutex::new(());
    let _g = LOCK.lock().unwrap();
    let mut started = STARTED.lock().unwrap();
    if started.iter().any(|s| s == name) {
        return;
    }
    docker(&["rm", "-f", name]);
    let mut all = vec!["run", "-d", "--rm", "--name", name];
    all.extend(args);
    let id = docker(&all);
    assert!(!id.trim().is_empty(), "{name} did not start");
    started.push(name.to_string());
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            std::thread::sleep(std::time::Duration::from_millis(500));
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    panic!("{name} does not listen on {port}: {}", docker(&["logs", name]));
}

fn s3() {
    let port = format!("{S3_PORT}:7070");
    let (k, s) = (format!("ROOT_ACCESS_KEY={}", S3_KEY.0), format!("ROOT_SECRET_KEY={}", S3_KEY.1));
    emulator(
        "brrrrr-sql-it-s3",
        S3_PORT,
        &["-p", &port, "--tmpfs", "/data", "-e", &k, "-e", &s, "versity/versitygw:latest", "posix", "/data"],
    );
}

fn gcs() {
    let port = format!("{GCS_PORT}:4443");
    let host = format!("127.0.0.1:{GCS_PORT}");
    let url = format!("http://127.0.0.1:{GCS_PORT}");
    emulator(
        "brrrrr-sql-it-gcs",
        GCS_PORT,
        &[
            "-p",
            &port,
            "fsouza/fake-gcs-server:latest",
            "-scheme",
            "http",
            "-port",
            "4443",
            "-public-host",
            &host,
            "-external-url",
            &url,
        ],
    );
}

fn azure() {
    let port = format!("{AZURE_PORT}:10000");
    emulator(
        "brrrrr-sql-it-azure",
        AZURE_PORT,
        &[
            "-p",
            &port,
            "mcr.microsoft.com/azure-storage/azurite:latest",
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--blobPort",
            "10000",
            "--loose",
            "--skipApiVersionCheck",
        ],
    );
}

/// An HTTP/1.0 request to 127.0.0.1:`port`: its status and body.
fn http(port: u16, method: &str, path: &str, headers: &[(String, String)], body: &[u8]) -> (u16, String) {
    // an emulator that has just started may take its port before it answers on it
    for _ in 0..50 {
        if let Ok(r) = try_http(port, method, path, headers, body) {
            return r;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    panic!("{method} {path}: no answer on {port}");
}

fn try_http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> std::io::Result<(u16, String)> {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
    let mut req = format!("{method} {path} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {}\r\n", body.len());
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes())?;
    s.write_all(body)?;
    let mut out = vec![];
    s.read_to_end(&mut out)?;
    let out = String::from_utf8_lossy(&out).to_string();
    let status = out.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    if status == 0 {
        return Err(std::io::Error::other("no status"));
    }
    Ok((status, out))
}

/// UTC now: (yyyymmdd'T'hhmmss'Z', RFC 1123).
fn now() -> (String, String) {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
    let (y, m, d) = brrrrr_core::value::civil(secs.div_euclid(86_400));
    let t = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (t / 3600, t / 60 % 60, t % 60);
    const WD: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MON: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let wd = WD[secs.div_euclid(86_400).rem_euclid(7) as usize];
    (
        format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z"),
        format!("{wd}, {d:02} {} {y:04} {hh:02}:{mm:02}:{ss:02} GMT", MON[m as usize - 1]),
    )
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
    m.update(data.as_bytes());
    m.finalize().into_bytes().to_vec()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// An S3 request signed with AWS Signature Version 4 (path style, region us-east-1).
fn s3_request(method: &str, path: &str, body: &[u8]) -> (u16, String) {
    // SigV4's canonical URI: each byte but unreserved ones and '/' percent-encoded
    let path: String = path
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => (b as char).to_string(),
            b => format!("%{b:02X}"),
        })
        .collect();
    let path = path.as_str();
    let (amz, _) = now();
    let date = &amz[..8];
    let hash = hex(&Sha256::digest(body));
    let host = format!("127.0.0.1:{S3_PORT}");
    let canonical = format!("{method}\n{path}\n\nhost:{host}\nx-amz-content-sha256:{hash}\nx-amz-date:{amz}\n\nhost;x-amz-content-sha256;x-amz-date\n{hash}");
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let to_sign = format!("AWS4-HMAC-SHA256\n{amz}\n{scope}\n{}", hex(&Sha256::digest(canonical.as_bytes())));
    let mut k = hmac(format!("AWS4{}", S3_KEY.1).as_bytes(), date);
    for part in ["us-east-1", "s3", "aws4_request"] {
        k = hmac(&k, part);
    }
    let sig = hex(&hmac(&k, &to_sign));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}",
        S3_KEY.0
    );
    http(
        S3_PORT,
        method,
        path,
        &[("x-amz-content-sha256".into(), hash), ("x-amz-date".into(), amz), ("Authorization".into(), auth)],
        body,
    )
}

/// An Azure Blob request signed with the emulator's shared key.
fn azure_request(
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    extra: &[(&str, &str)],
    body: &[u8],
) -> (u16, String) {
    use base64::Engine;
    let (_, date) = now();
    let mut ms: Vec<(String, String)> = vec![("x-ms-date".into(), date), ("x-ms-version".into(), "2021-08-06".into())];
    ms.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    ms.sort();
    let length = if body.is_empty() { String::new() } else { body.len().to_string() };
    let headers: String = ms.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let resource: String = query.iter().map(|(k, v)| format!("\n{k}:{v}")).collect();
    let to_sign =
        format!("{method}\n\n\n{length}\n\n\n\n\n\n\n\n\n{headers}/{AZURE_ACCOUNT}/{AZURE_ACCOUNT}{path}{resource}");
    let key = base64::engine::general_purpose::STANDARD.decode(AZURE_KEY).unwrap();
    let sig = base64::engine::general_purpose::STANDARD.encode(hmac(&key, &to_sign));
    let mut h: Vec<(String, String)> = ms;
    h.push(("Authorization".into(), format!("SharedKey {AZURE_ACCOUNT}:{sig}")));
    let q: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let url = if q.is_empty() {
        format!("/{AZURE_ACCOUNT}{path}")
    } else {
        format!("/{AZURE_ACCOUNT}{path}?{}", q.join("&"))
    };
    http(AZURE_PORT, method, &url, &h, body)
}

// ---- files

#[given(expr = "a file {string} with:")]
fn a_file(w: &mut Sql, name: String, step: &cucumber::gherkin::Step) {
    let p = w.path(&name);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, format!("{}\n", step.docstring().expect("a docstring").trim())).unwrap();
}

#[given(expr = "a gzipped file {string} with:")]
fn a_gzipped_file(w: &mut Sql, name: String, step: &cucumber::gherkin::Step) {
    let p = w.path(&name);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    let mut gz = flate2::write::GzEncoder::new(std::fs::File::create(p).unwrap(), Default::default());
    gz.write_all(format!("{}\n", step.docstring().unwrap().trim()).as_bytes()).unwrap();
    gz.finish().unwrap();
}

#[given("the cookbook's files")]
fn cookbook_files(w: &mut Sql) {
    for f in ["trades.csv", "quotes.csv", "instruments.csv", "readings.csv", "events.csv"] {
        let from = format!("{}/../../fixtures/cookbook/{f}", env!("CARGO_MANIFEST_DIR"));
        std::fs::copy(from, w.path(f)).unwrap();
    }
}

#[given(expr = "the table {string} from the fixtures")]
fn table_fixture(w: &mut Sql, name: String) {
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for e in std::fs::read_dir(from).unwrap().flatten() {
            if e.path().is_dir() {
                copy(&e.path(), &to.join(e.file_name()));
            } else {
                std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
            }
        }
    }
    let from = format!("{}/../../fixtures/tables/{name}", env!("CARGO_MANIFEST_DIR"));
    copy(std::path::Path::new(&from), &w.path(&name));
}

#[given(expr = "the fixture {string}")]
fn fixture_file(w: &mut Sql, path: String) {
    let from = format!("{}/../../fixtures/{path}", env!("CARGO_MANIFEST_DIR"));
    let name = path.rsplit('/').next().unwrap_or(&path);
    std::fs::copy(from, w.path(name)).unwrap();
}

#[given(expr = "the fixture {string} as {string}")]
fn fixture_file_as(w: &mut Sql, path: String, name: String) {
    let from = format!("{}/../../fixtures/{path}", env!("CARGO_MANIFEST_DIR"));
    std::fs::copy(from, w.path(&name)).unwrap();
}

#[given(expr = "{string} written as {string}")]
fn converted(w: &mut Sql, from: String, to: String) {
    w.run(&["sql".into(), format!("COPY (FROM '{from}') TO '{to}'")], None);
    assert!(w.output().status.success(), "{}", w.stderr());
}

#[given("the directory is served over HTTP")]
fn served(w: &mut Sql) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let root = w.dir.path().to_path_buf();
    std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            let root = root.clone();
            std::thread::spawn(move || serve(s, &root));
        }
    });
    w.http = Some(format!("http://127.0.0.1:{port}"));
}

/// GET and HEAD of the files under `root`.
fn serve(mut s: std::net::TcpStream, root: &std::path::Path) {
    let mut buf = [0u8; 8192];
    let n = s.read(&mut buf).unwrap_or(0);
    let req = String::from_utf8_lossy(&buf[..n]);
    let mut first = req.split_whitespace();
    let (method, path) = (first.next().unwrap_or(""), first.next().unwrap_or("/"));
    let file = root.join(path.trim_start_matches('/').split('?').next().unwrap_or(""));
    match std::fs::read(&file) {
        Ok(body) if file.is_file() => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nLast-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\nETag: \"{}\"\r\nConnection: close\r\n\r\n",
                body.len(),
                body.len()
            );
            let _ = s.write_all(head.as_bytes());
            if method == "GET" {
                let _ = s.write_all(&body);
            }
        }
        _ => {
            let _ = s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
    }
}

// ---- object stores

#[given("an S3 bucket")]
fn s3_bucket(w: &mut Sql) {
    s3();
    let (status, body) = s3_request("PUT", &format!("/{}", w.bucket()), b"");
    assert!(status == 200, "S3 bucket: {status} {body}");
}

#[given(expr = "the file {string} in S3 at {string}")]
fn s3_object(w: &mut Sql, file: String, key: String) {
    let body = std::fs::read(w.path(&file)).unwrap();
    let (status, out) = s3_request("PUT", &format!("/{}/{}", w.bucket(), w.expand(&key)), &body);
    assert!(status == 200, "S3 object: {status} {out}");
}

#[given(expr = "the directory {string} in S3 at {string}")]
fn s3_directory(w: &mut Sql, dir: String, prefix: String) {
    fn files(d: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(d).unwrap().flatten() {
            if e.path().is_dir() {
                files(&e.path(), out);
            } else {
                out.push(e.path());
            }
        }
    }
    let root = w.path(&dir);
    let mut all = vec![];
    files(&root, &mut all);
    for f in all {
        let rel = f.strip_prefix(&root).unwrap().to_string_lossy().replace('\\', "/");
        let key = format!("{}/{rel}", w.expand(prefix.trim_end_matches('/')));
        let (status, out) = s3_request("PUT", &format!("/{}/{key}", w.bucket()), &std::fs::read(&f).unwrap());
        assert!(status == 200, "S3 object {key}: {status} {out}");
    }
}

#[given(expr = "the environment variable {word} is {string}")]
fn environment(w: &mut Sql, name: String, value: String) {
    w.env.retain(|(k, _)| *k != name);
    w.env.push((name, value));
}

#[given("S3 credentials")]
fn s3_credentials(w: &mut Sql) {
    s3();
    w.env.extend(
        [
            ("AWS_ACCESS_KEY_ID", S3_KEY.0.to_string()),
            ("AWS_SECRET_ACCESS_KEY", S3_KEY.1.to_string()),
            ("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{S3_PORT}")),
            ("AWS_REGION", "us-east-1".to_string()),
        ]
        .map(|(k, v)| (k.to_string(), v)),
    );
}

#[given(expr = "the S3 secret is {string}")]
fn s3_secret(w: &mut Sql, secret: String) {
    w.env.retain(|(k, _)| k != "AWS_SECRET_ACCESS_KEY");
    w.env.push(("AWS_SECRET_ACCESS_KEY".into(), secret));
}

#[given(expr = "the S3 endpoint is {string}")]
fn s3_endpoint(w: &mut Sql, url: String) {
    w.env.retain(|(k, _)| k != "AWS_ENDPOINT_URL");
    w.env.push(("AWS_ENDPOINT_URL".into(), url));
}

#[given("a GCS bucket")]
fn gcs_bucket(w: &mut Sql) {
    gcs();
    let body = format!("{{\"name\":\"{}\"}}", w.bucket());
    let (status, out) =
        http(GCS_PORT, "POST", "/storage/v1/b", &[("Content-Type".into(), "application/json".into())], body.as_bytes());
    assert!(status == 200, "GCS bucket: {status} {out}");
    let key = format!(
        "{{\"gcs_base_url\":\"http://127.0.0.1:{GCS_PORT}\",\"disable_oauth\":true,\"client_email\":\"\",\"private_key\":\"\",\"private_key_id\":\"\"}}"
    );
    w.env.push(("GOOGLE_SERVICE_ACCOUNT_KEY".into(), key));
}

#[given(expr = "the file {string} in GCS at {string}")]
fn gcs_object(w: &mut Sql, file: String, key: String) {
    let body = std::fs::read(w.path(&file)).unwrap();
    let path = format!("/upload/storage/v1/b/{}/o?uploadType=media&name={}", w.bucket(), w.expand(&key));
    let (status, out) = http(GCS_PORT, "POST", &path, &[], &body);
    assert!(status == 200, "GCS object: {status} {out}");
}

#[given("an Azure container")]
fn azure_container(w: &mut Sql) {
    azure();
    let (status, out) = azure_request("PUT", &format!("/{}", w.bucket()), &[("restype", "container")], &[], b"");
    assert!(status == 201, "Azure container: {status} {out}");
    w.env.push(("AZURE_STORAGE_USE_EMULATOR".into(), "true".into()));
    w.env.push(("AZURITE_BLOB_STORAGE_URL".into(), format!("http://127.0.0.1:{AZURE_PORT}")));
}

#[given(expr = "the file {string} in Azure at {string}")]
fn azure_blob(w: &mut Sql, file: String, key: String) {
    let body = std::fs::read(w.path(&file)).unwrap();
    let path = format!("/{}/{}", w.bucket(), w.expand(&key));
    let (status, out) = azure_request("PUT", &path, &[], &[("x-ms-blob-type", "BlockBlob")], &body);
    assert!(status == 201, "Azure blob: {status} {out}");
}

// ---- running brrrrr

#[when("I run brrrrr sql:")]
fn run_sql(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let sql = w.expand(step.docstring().expect("the SQL").trim());
    w.run(&["sql".into(), "--format".into(), "csv".into(), sql], None);
}

#[when(expr = "I run {string}")]
fn run_command(w: &mut Sql, line: String) {
    let mut args = words(&w.expand(&line));
    assert_eq!(args.remove(0), "brrrrr", "a brrrrr command");
    w.run(&args, None);
}

#[when(expr = "I run {string} on:")]
fn run_on_sql(w: &mut Sql, line: String, step: &cucumber::gherkin::Step) {
    let mut args = words(&w.expand(&line));
    assert_eq!(args.remove(0), "brrrrr", "a brrrrr command");
    args.push(w.expand(step.docstring().expect("the SQL").trim()));
    w.run(&args, None);
}

#[when(expr = "I run {string} with the input:")]
fn run_with_input(w: &mut Sql, line: String, step: &cucumber::gherkin::Step) {
    let mut args = words(&w.expand(&line));
    assert_eq!(args.remove(0), "brrrrr", "a brrrrr command");
    let input = w.expand(step.docstring().expect("the input"));
    w.run(&args, Some(&input));
}

#[when(expr = "I run {string} and read only its first line")]
fn run_head(w: &mut Sql, line: String) {
    let mut args = words(&w.expand(&line));
    assert_eq!(args.remove(0), "brrrrr", "a brrrrr command");
    let mut child = Command::new(env!("CARGO_BIN_EXE_brrrrr"))
        .args(&args)
        .current_dir(w.dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("brrrrr runs");
    let mut first = String::new();
    // the reader goes after one line, as `head -n 1` goes
    std::io::BufRead::read_line(&mut std::io::BufReader::new(child.stdout.take().expect("stdout")), &mut first)
        .expect("a line");
    let mut out = child.wait_with_output().expect("brrrrr ends");
    out.stdout = first.into_bytes();
    w.out = Some(out);
}

#[then("it says nothing")]
fn says_nothing(w: &mut Sql) {
    assert!(w.output().status.success(), "brrrrr failed: {}", w.stderr());
    assert_eq!(w.stderr(), "");
}

fn lines(s: &str) -> Vec<String> {
    s.trim().lines().map(|l| l.trim_end().to_string()).collect()
}

#[then("the output is:")]
fn output_is(w: &mut Sql, step: &cucumber::gherkin::Step) {
    assert!(w.output().status.success(), "brrrrr failed: {}", w.stderr());
    let want = w.expand(step.docstring().unwrap());
    assert_eq!(lines(&w.stdout()), lines(&want), "stderr: {}", w.stderr());
}

#[then(expr = "the output contains {string}")]
fn output_contains(w: &mut Sql, text: String) {
    assert!(w.output().status.success(), "brrrrr failed: {}", w.stderr());
    let text = w.expand(&text);
    assert!(w.stdout().contains(&text), "{:?} not in:\n{}", text, w.stdout());
}

#[then(expr = "the output has {int} lines")]
fn output_lines(w: &mut Sql, n: usize) {
    assert!(w.output().status.success(), "brrrrr failed: {}", w.stderr());
    assert_eq!(lines(&w.stdout()).len(), n, "{}", w.stdout());
}

#[then(expr = "it says {string}")]
fn it_says(w: &mut Sql, text: String) {
    assert!(w.output().status.success(), "brrrrr failed: {}", w.stderr());
    let text = w.expand(&text);
    assert!(w.stderr().contains(&text), "{:?} not in:\n{}", text, w.stderr());
}

#[then(expr = "it fails with {string}")]
fn fails_with(w: &mut Sql, text: String) {
    assert!(!w.output().status.success(), "brrrrr succeeded: {}", w.stdout());
    let text = w.expand(&text);
    assert!(w.stderr().contains(&text), "{:?} not in:\n{}", text, w.stderr());
}

#[then(expr = "the file {string} exists")]
fn file_exists(w: &mut Sql, name: String) {
    assert!(w.path(&name).is_file(), "no file {name}");
}

#[then(expr = "the file {string} reads:")]
fn file_reads(w: &mut Sql, name: String, step: &cucumber::gherkin::Step) {
    assert_eq!(lines(&std::fs::read_to_string(w.path(&name)).unwrap()), lines(step.docstring().unwrap()));
}

// ---- brrrrr serve

const SERVE_PORTS: u16 = 31000;

fn start_server(w: &mut Sql, args: Vec<String>, token: Option<String>) {
    let (http, pg) = (SERVE_PORTS + 2 * w.id as u16, SERVE_PORTS + 2 * w.id as u16 + 1);
    let mut all = vec!["serve".to_string(), "--data".into(), w.path("data").display().to_string()];
    all.extend(["--http".into(), format!("127.0.0.1:{http}"), "--pg".into(), format!("127.0.0.1:{pg}")]);
    all.extend(args.iter().cloned());
    if let Some(t) = &token {
        all.extend(["--token".into(), t.clone()]);
    }
    let mut c = Command::new(env!("CARGO_BIN_EXE_brrrrr"));
    c.args(&all).current_dir(w.dir.path()).stdout(Stdio::null()).stderr(Stdio::piped());
    c.envs(w.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    let child = c.spawn().expect("brrrrr serve runs");
    w.server = Some(Served { child, args, http, pg, token });
    for _ in 0..100 {
        if try_http(http, "GET", "/health", &[], b"").is_ok_and(|r| r.0 == 200) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("brrrrr serve did not start");
}

fn server(w: &Sql) -> &Served {
    w.server.as_ref().expect("a server")
}

fn auth(w: &Sql) -> Vec<(String, String)> {
    server(w).token.iter().map(|t| ("Authorization".to_string(), format!("Bearer {t}"))).collect()
}

/// An HTTP answer's body (after its headers).
fn body(raw: &str) -> String {
    raw.split_once("\r\n\r\n").map_or(String::new(), |b| b.1.to_string())
}

#[given("a brrrrr server")]
fn a_server(w: &mut Sql) {
    start_server(w, vec!["--flush".into(), "1h".into()], None);
}

#[given(expr = "a brrrrr server with the token {string}")]
fn a_server_with_token(w: &mut Sql, token: String) {
    start_server(w, vec!["--flush".into(), "1h".into()], Some(token));
}

#[given(expr = "a brrrrr server ingesting {string} from the Kafka topic {string}")]
fn a_server_on_kafka(w: &mut Sql, table: String, topic: String) {
    kafka();
    start_server(
        w,
        vec![
            "--flush".into(),
            "1h".into(),
            "--kafka".into(),
            format!("127.0.0.1:{KAFKA_PORT}"),
            "--ingest".into(),
            format!("{table}={}", w.expand(&topic)),
        ],
        None,
    );
}

#[when(expr = "I write to {string}:")]
#[given(expr = "rows written to {string}:")]
fn write_rows(w: &mut Sql, table: String, step: &cucumber::gherkin::Step) {
    let body = format!("{}\n", step.docstring().unwrap().trim());
    let r = try_http(server(w).http, "POST", &format!("/write/{table}"), &auth(w), body.as_bytes()).unwrap();
    w.answer = Some((r.0, body_of(&r.1)));
}

#[when(expr = "I write CSV to {string}:")]
fn write_csv(w: &mut Sql, table: String, step: &cucumber::gherkin::Step) {
    let body = format!("{}\n", step.docstring().unwrap().trim());
    let mut h = auth(w);
    h.push(("Content-Type".into(), "text/csv".into()));
    let r = try_http(server(w).http, "POST", &format!("/write/{table}"), &h, body.as_bytes()).unwrap();
    w.answer = Some((r.0, body_of(&r.1)));
}

fn body_of(raw: &str) -> String {
    body(raw)
}

#[given(expr = "a brrrrr server with {string}")]
fn a_server_with(w: &mut Sql, args: String) {
    let mut all: Vec<String> = vec!["--flush".into(), "1h".into()];
    all.extend(args.split_whitespace().map(String::from));
    start_server(w, all, None);
}

#[given(expr = "a CSV file {string} of {int} trades")]
fn big_csv(w: &mut Sql, name: String, n: usize) {
    let mut out = String::from("ts,symbol,price\n");
    for i in 0..n {
        out.push_str(&format!("2024-01-02 09:{:02}:{:02},S{},{}\n", (i / 60) % 60, i % 60, i % 97, 100 + i % 1013));
    }
    std::fs::write(w.path(&name), out).unwrap();
}

#[when(expr = "I query over HTTP, hanging up after {int} ms:")]
fn query_http_hang_up(w: &mut Sql, ms: u64, step: &cucumber::gherkin::Step) {
    let sql = w.expand(step.docstring().unwrap().trim());
    let mut s = std::net::TcpStream::connect(("127.0.0.1", server(w).http)).unwrap();
    let req = format!("POST /query HTTP/1.0\r\nContent-Length: {}\r\n\r\n{sql}", sql.len());
    s.write_all(req.as_bytes()).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(ms));
    drop(s);
}

#[when(expr = "I query over PostgreSQL, canceling after {int} ms:")]
async fn query_pg_cancel(w: &mut Sql, ms: u64, step: &cucumber::gherkin::Step) {
    let sql = w.expand(step.docstring().unwrap().trim());
    let conf = format!("host=127.0.0.1 port={} user=me dbname=brrrrr", server(w).pg);
    let (client, conn) = tokio_postgres::connect(&conf, tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);
    let cancel = client.cancel_token();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        cancel.cancel_query(tokio_postgres::NoTls).await.unwrap();
    });
    w.answer = Some(match client.simple_query(&sql).await {
        Ok(_) => (200, "answered".into()),
        Err(e) => (500, format!("{e:?}")),
    });
}

#[then(expr = "within {int} seconds the server's metrics say {string}")]
fn metrics_say(w: &mut Sql, secs: u64, line: String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    loop {
        let r = try_http(server(w).http, "GET", "/metrics", &auth(w), b"").unwrap();
        if r.1.lines().any(|l| l == line) {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{line:?} not in {}", r.1);
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[when("I query over HTTP:")]
fn query_http(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let sql = w.expand(step.docstring().unwrap().trim());
    let r = try_http(server(w).http, "POST", "/query?format=csv", &auth(w), sql.as_bytes()).unwrap();
    w.answer = Some((r.0, body_of(&r.1)));
}

#[when(expr = "I query over HTTP for the count of {int} queries' UNION ALL")]
fn query_http_union(w: &mut Sql, n: usize) {
    let sql = format!("SELECT count(*) AS n FROM ({})", vec!["SELECT 1 AS x"; n].join(" UNION ALL "));
    let r = try_http(server(w).http, "POST", "/query?format=csv", &auth(w), sql.as_bytes()).unwrap();
    w.answer = Some((r.0, body_of(&r.1)));
}

#[when("I query for JSON:")]
fn query_json(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let sql = step.docstring().unwrap().trim().to_string();
    let r = try_http(server(w).http, "POST", "/query", &auth(w), sql.as_bytes()).unwrap();
    // the timing changes from run to run
    let b = body_of(&r.1);
    let b = b.split(",\"elapsed_ms\"").next().unwrap_or("").to_string();
    w.answer = Some((r.0, b));
}

#[when(expr = "I send {string} without the token")]
fn without_token(w: &mut Sql, line: String) {
    let (method, path) = line.split_once(' ').unwrap();
    let r = try_http(server(w).http, method, path, &[], b"SELECT 1").unwrap();
    w.answer = Some((r.0, body_of(&r.1)));
}

#[when("I flush")]
fn flush(w: &mut Sql) {
    let r = try_http(server(w).http, "POST", "/flush", &auth(w), b"").unwrap();
    assert_eq!(r.0, 200, "{}", r.1);
}

#[when("the server stops and starts again")]
fn restart(w: &mut Sql) {
    let s = w.server.take().expect("a server");
    let (args, token) = (s.args.clone(), s.token.clone());
    // SIGTERM: a stop that flushes
    let _ = Command::new("kill").arg(s.child.id().to_string()).status();
    let mut s = s;
    let _ = s.child.wait();
    start_server(w, args, token);
}

#[when("the server is killed and starts again")]
fn crash(w: &mut Sql) {
    let s = w.server.take().expect("a server");
    let (args, token) = (s.args.clone(), s.token.clone());
    drop(s); // SIGKILL: nothing flushed, the log replayed
    start_server(w, args, token);
}

#[when(expr = "I subscribe to {string}")]
fn subscribe(w: &mut Sql, table: String) {
    let port = server(w).http;
    let path =
        format!("/subscribe/{table}{}", server(w).token.as_ref().map_or(String::new(), |t| format!("?token={t}")));
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        write!(s, "GET {path} HTTP/1.0\r\n\r\n").unwrap();
        let mut r = std::io::BufReader::new(s);
        let mut line = String::new();
        while std::io::BufRead::read_line(&mut r, &mut line).unwrap_or(0) > 0 {
            if let Some(d) = line.trim().strip_prefix("data: ") {
                if tx.send(d.to_string()).is_err() {
                    return;
                }
            }
            line.clear();
        }
    });
    // the subscription is in place before the next step writes
    std::thread::sleep(std::time::Duration::from_millis(300));
    w.events = Some(rx);
}

#[then("the subscription receives:")]
fn received(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let want = lines(step.docstring().unwrap());
    let rx = w.events.as_ref().expect("a subscription");
    let mut got = vec![];
    while got.len() < want.len() {
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(e) => got.push(e),
            Err(_) => break,
        }
    }
    assert_eq!(got, want);
}

#[then(expr = "the answer is {int}")]
fn status_is(w: &mut Sql, status: u16) {
    let (s, b) = w.answer.as_ref().expect("an answer");
    assert_eq!(*s, status, "{b}");
}

#[then("the answer is:")]
fn answer_is(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let (s, b) = w.answer.as_ref().expect("an answer");
    assert_eq!(*s, 200, "{b}");
    assert_eq!(lines(b), lines(step.docstring().unwrap()));
}

#[then(expr = "the answer says {string}")]
fn answer_says(w: &mut Sql, text: String) {
    let (_, b) = w.answer.as_ref().expect("an answer");
    assert!(b.contains(&text), "{text:?} not in {b}");
}

#[then(expr = "within {int} seconds the query answers:")]
fn eventually(w: &mut Sql, secs: u64, step: &cucumber::gherkin::Step) {
    let (sql, want) = step.docstring().unwrap().split_once("\n---\n").expect("the query, ---, the answer");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    loop {
        let r = try_http(server(w).http, "POST", "/query?format=csv", &auth(w), sql.trim().as_bytes()).unwrap();
        let got = lines(&body_of(&r.1));
        if got == lines(want) {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "after {secs} s: {got:?}");
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

/// A PostgreSQL client's view of a query: a header, then each row's values as text, `,`-joined.
async fn pg_query(w: &Sql, sql: &str, extended: bool) -> Result<Vec<String>, String> {
    let s = server(w);
    let password = s.token.clone().unwrap_or_default();
    let conf = format!("host=127.0.0.1 port={} user=me password='{password}' dbname=brrrrr", s.pg);
    let (client, conn) = tokio_postgres::connect(&conf, tokio_postgres::NoTls).await.map_err(|e| e.to_string())?;
    tokio::spawn(conn);
    if extended {
        use tokio_postgres::types::{ToSql, Type};
        // each parameter of the type the client says, sent as that type's binary, as drivers do
        let params: Vec<(Box<dyn ToSql + Sync + Send>, Type)> = w
            .params
            .iter()
            .map(|(t, v)| -> (Box<dyn ToSql + Sync + Send>, Type) {
                match t.as_str() {
                    "int4" => (Box::new(v.parse::<i32>().unwrap()), Type::INT4),
                    "int8" => (Box::new(v.parse::<i64>().unwrap()), Type::INT8),
                    "float8" => (Box::new(v.parse::<f64>().unwrap()), Type::FLOAT8),
                    "bool" => (Box::new(v.parse::<bool>().unwrap()), Type::BOOL),
                    "timestamptz" => {
                        (Box::new(chrono::DateTime::parse_from_rfc3339(v).unwrap().to_utc()), Type::TIMESTAMPTZ)
                    }
                    "timestamp" => (
                        Box::new(chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S").unwrap()),
                        Type::TIMESTAMP,
                    ),
                    _ => (Box::new(v.clone()), Type::TEXT),
                }
            })
            .collect();
        let typed: Vec<(&(dyn ToSql + Sync), Type)> =
            params.iter().map(|(v, t)| (v.as_ref() as &(dyn ToSql + Sync), t.clone())).collect();
        let rows = client.query_typed(sql, &typed).await.map_err(|e| format!("{e:?}"))?;
        let Some(first) = rows.first() else { return Ok(vec![]) };
        let mut out =
            vec![first.columns().iter().map(|c| format!("{}:{}", c.name(), c.type_())).collect::<Vec<_>>().join(",")];
        for r in &rows {
            let cells: Vec<String> = (0..r.len())
                .map(|i| match r.columns()[i].type_().name() {
                    "int8" => r.get::<_, Option<i64>>(i).map_or("NULL".into(), |v| v.to_string()),
                    "float8" => r.get::<_, Option<f64>>(i).map_or("NULL".into(), |v| v.to_string()),
                    "bool" => r.get::<_, Option<bool>>(i).map_or("NULL".into(), |v| v.to_string()),
                    "timestamptz" => r
                        .get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
                        .map_or("NULL".into(), |v| v.format("%Y-%m-%d %H:%M:%S").to_string()),
                    _ => r.get::<_, Option<String>>(i).unwrap_or_else(|| "NULL".into()),
                })
                .collect();
            out.push(cells.join(","));
        }
        Ok(out)
    } else {
        let msgs = client.simple_query(sql).await.map_err(|e| format!("{e:?}"))?;
        let mut out = vec![];
        for m in msgs {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                if out.is_empty() {
                    out.push(r.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>().join(","));
                }
                out.push((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join(","));
            }
        }
        Ok(out)
    }
}

#[when("I query over PostgreSQL:")]
async fn query_pg(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let r = pg_query(w, &w.expand(step.docstring().unwrap().trim()), false).await;
    w.answer = Some(match r {
        Ok(lines) => (200, lines.join("\n")),
        Err(e) => (500, e),
    });
}

#[when("I query over PostgreSQL's extended protocol:")]
async fn query_pg_extended(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let r = pg_query(w, step.docstring().unwrap().trim(), true).await;
    w.answer = Some(match r {
        Ok(lines) => (200, lines.join("\n")),
        Err(e) => (500, e),
    });
}

#[given("the PostgreSQL parameters:")]
async fn pg_params(w: &mut Sql, step: &cucumber::gherkin::Step) {
    let table = step.table.as_ref().expect("a table of type and value");
    w.params = table.rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect();
}

#[when(expr = "I connect over PostgreSQL with the password {string}")]
async fn pg_password(w: &mut Sql, password: String) {
    let conf = format!("host=127.0.0.1 port={} user=me password='{password}'", server(w).pg);
    w.answer = Some(match tokio_postgres::connect(&conf, tokio_postgres::NoTls).await {
        Ok(_) => (200, "connected".into()),
        Err(e) => (401, format!("{e:?}")),
    });
}

const KAFKA_PORT: u16 = 29192;

fn kafka() {
    let port = format!("{KAFKA_PORT}:{KAFKA_PORT}");
    let listen = format!("0.0.0.0:{KAFKA_PORT}");
    let advertise = format!("127.0.0.1:{KAFKA_PORT}");
    emulator(
        "brrrrr-sql-it-kafka",
        KAFKA_PORT,
        &[
            "-p",
            &port,
            "redpandadata/redpanda:v24.2.7",
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--memory",
            "512M",
            "--reactor-backend=epoll",
            "--kafka-addr",
            &listen,
            "--advertise-kafka-addr",
            &advertise,
        ],
    );
}

#[when(expr = "the Kafka topic {string} gets:")]
fn produce(w: &mut Sql, topic: String, step: &cucumber::gherkin::Step) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let topic = w.expand(&topic);
    let p: BaseProducer =
        rdkafka::ClientConfig::new().set("bootstrap.servers", format!("127.0.0.1:{KAFKA_PORT}")).create().unwrap();
    for line in step.docstring().unwrap().trim().lines() {
        p.send(BaseRecord::<(), str>::to(&topic).payload(line.trim())).unwrap();
    }
    p.flush(std::time::Duration::from_secs(10)).unwrap();
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|a| a == "--list") {
        return; // a cucumber binary lists no libtest tests (nextest)
    }
    let features = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/acceptance/sql");
    let w = Sql::cucumber().max_concurrent_scenarios(8).fail_on_skipped().run(features).await;
    for name in STARTED.lock().unwrap().iter() {
        docker(&["rm", "-f", name]);
    }
    if w.execution_has_failed() {
        std::process::exit(1);
    }
}
