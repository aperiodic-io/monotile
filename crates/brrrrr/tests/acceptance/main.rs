//! Acceptance tests: the Gherkin features under `tests/acceptance/features` are the executable
//! specification of brrrrr (docs/plan/06-testing.md).
//!
//! Scenarios run concurrently (`BRRRRR_IT_CONCURRENCY`, default 8), each on brokers, ports and
//! checkpoints of its own. Kafka scenarios start their own Redpanda container (1 core, 1 GiB). By
//! default it joins the docker network `brrrrr-it` as `redpanda-<scenario>:9092` and the tests
//! run on that network (`NETWORK=brrrrr-it ./run.sh`); with `BRRRRR_IT_BROKER=127.0.0.1:<port>`
//! (CI) scenario `n` publishes `<port> + 2n` on the host instead (and `+ 1` for a separate sinks'
//! broker). The containers and their volumes are removed afterwards.
use brrrrr_core::checkpoint::Checkpoint;
use brrrrr_core::engine::Engine;
use brrrrr_core::proto::{parse_proto, Codec};
use brrrrr_core::value::{Type, Value};
use cucumber::{given, then, when, World};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Output;
use std::time::{Duration, Instant};

/// The `/metrics` listener of a scenario's first process, or its second (`second = 1`).
fn metrics_addr(w: &Brrrrr, second: usize) -> String {
    format!("127.0.0.1:{}", 19464 + 2 * w.id.0 + second)
}

fn get(w: &Brrrrr, path: &str) -> String {
    use std::io::{Read, Write};
    let addr = metrics_addr(w, 0);
    let fetch = || -> std::io::Result<String> {
        let mut s = std::net::TcpStream::connect(&addr)?;
        write!(s, "GET {path} HTTP/1.0\r\n\r\n")?;
        let mut out = String::new();
        s.read_to_string(&mut out)?;
        Ok(out)
    };
    // ~10 s: a busy runner may take that long to answer
    (0..30)
        .find_map(|_| fetch().map_err(|_| std::thread::sleep(Duration::from_millis(300))).ok())
        .unwrap_or_else(|| panic!("no metrics endpoint at {addr}: {}", w.log.lock().unwrap()))
}

/// What a failing step shows of the run: its sources' and closes' metrics, if it still answers,
/// and the end of what it logged.
fn diagnosis(w: &Brrrrr) -> String {
    use std::io::{Read, Write};
    let metrics = std::net::TcpStream::connect(metrics_addr(w, 0))
        .and_then(|mut s| {
            write!(s, "GET /metrics HTTP/1.0\r\n\r\n")?;
            let mut out = String::new();
            s.read_to_string(&mut out).map(|_| out)
        })
        .map(|m| {
            m.lines()
                .filter(|l| !l.starts_with('#') && l.starts_with("brrrrr_") && !l.contains("_bucket"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_else(|e| format!("(no metrics: {e})"));
    let log = w.log.lock().unwrap();
    let tail = &log[log.len().saturating_sub(4000)..];
    format!("\n--- its metrics:\n{metrics}\n--- the end of its log:\n{tail}")
}

/// A scenario's Redpanda: its container and the address brrrrr and the steps reach it on. `n` is
/// 0 for the sources' broker and 1 for a separate sinks' broker.
fn redpanda_of(w: &Brrrrr, n: usize) -> (String, String) {
    let id = w.id.0;
    let name = format!("brrrrr-it-redpanda-{id}{}", ["", "-out"][n]);
    let addr = match std::env::var("BRRRRR_IT_BROKER") {
        Ok(base) => {
            let (host, port) = base.rsplit_once(':').expect("BRRRRR_IT_BROKER is host:port");
            format!("{host}:{}", port.parse::<usize>().expect("a port") + 2 * id + n)
        }
        Err(_) => format!("redpanda-{id}{}:9092", ["", "-out"][n]),
    };
    (name, addr)
}

fn broker(w: &Brrrrr) -> String {
    redpanda_of(w, 0).1
}

fn sink_broker(w: &Brrrrr) -> String {
    w.sink_broker.clone().unwrap_or_else(|| broker(w))
}

/// A scenario's number, unique in this run: its brokers, ports and files are its own.
#[derive(Debug)]
struct Id(usize);

impl Default for Id {
    fn default() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        Id(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

#[derive(Debug, Default, World)]
pub struct Brrrrr {
    id: Id,
    /// The Redpanda containers this scenario started, removed with it.
    containers: Vec<String>,
    /// The sinks' broker when the scenario runs them on a second Redpanda.
    sink_broker: Option<String>,
    /// librdkafka settings every client of the sinks' broker needs (SASL credentials).
    sink_settings: Vec<(String, String)>,
    output: Option<Output>,
    /// The pipeline and trades of the last run, to compute the expected messages.
    sql: Option<String>,
    trades: usize,
    checkpoints: Option<PathBuf>,
    /// Where checkpoints go instead of `checkpoints`: an S3 URL (`@object-store` scenarios).
    s3: Option<String>,
    /// `/metrics` and `/health` of the running process, scraped before it stops.
    metrics: String,
    health: String,
    /// Step-by-step scenarios: the running process(es), their stderr, trades produced so far.
    child: Option<tokio::process::Child>,
    second: Option<tokio::process::Child>,
    log: std::sync::Arc<std::sync::Mutex<String>>,
    produced: usize,
    stopped: Option<std::process::ExitStatus>,
    /// `--sink-topic-prefix` for the processes this scenario starts.
    prefix: String,
    /// More arguments for every `brrrrr run` this scenario starts.
    extra: Vec<String>,
    /// Trades timed up to now, their records stamped with their times (`recent_trades`).
    recent: Vec<Vec<Value>>,
    /// Unix µs the scenario's first brrrrr was started.
    started_at: i64,
    /// What the scenario's first process reaches the S3 store through (`@object-store`).
    proxy: Option<Proxy>,
    /// `brrrrr_checkpoints_total` once the proxy held every request.
    checkpoints_when_held: Option<u64>,
    /// `brrrrr_checkpoints_total` before a source grew: `brrrrr writes a checkpoint` waits for one
    /// after it, not after the step began.
    checkpoints_before: Option<f64>,
    /// The end (Unix µs) of the minute `live_trades` left quiet: its last trade is just before it.
    live_minute_end: i64,
    /// Every this many trades, one without a price (`trades`); 0: none.
    priceless: usize,
}

/// A TCP proxy in front of the S3 store that can hold every request: what a process sends the
/// store is not passed on until it is released, as a store that hangs (an object store slow to
/// answer, a volume stuck on its storage). What the store answers is passed on at once.
#[derive(Debug)]
struct Proxy {
    /// `http://<address>`, for `AWS_ENDPOINT`.
    endpoint: String,
    hold: tokio::sync::watch::Sender<bool>,
}

impl Proxy {
    async fn start(upstream: String) -> Proxy {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (hold, held) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let (upstream, mut held) = (upstream.clone(), held.clone());
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(&upstream).await else { return };
                    let ((mut from_client, mut to_client), (mut from_server, mut to_server)) =
                        (client.into_split(), server.into_split());
                    let requests = async move {
                        let mut buf = vec![0; 64 * 1024];
                        while let Ok(n @ 1..) = from_client.read(&mut buf).await {
                            // held: passed on once released (also if the client is gone by then)
                            if held.wait_for(|held| !*held).await.is_err()
                                || to_server.write_all(&buf[..n]).await.is_err()
                            {
                                break;
                            }
                        }
                        let _ = to_server.shutdown().await;
                    };
                    let answers = async move {
                        let _ = tokio::io::copy(&mut from_server, &mut to_client).await;
                        let _ = to_client.shutdown().await;
                    };
                    tokio::join!(requests, answers);
                });
            }
        });
        Proxy { endpoint, hold }
    }
}

impl Drop for Brrrrr {
    fn drop(&mut self) {
        if !self.containers.is_empty() {
            let _ = std::process::Command::new("docker").args(["rm", "-f", "-v"]).args(&self.containers).output();
        }
        if let Some(d) = &self.checkpoints {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

fn root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

fn brrrrr() -> tokio::process::Command {
    let mut c = tokio::process::Command::new(env!("CARGO_BIN_EXE_brrrrr"));
    c.current_dir(root());
    c
}

#[when(expr = "I run brrrrr with {string}")]
async fn run_with(w: &mut Brrrrr, args: String) {
    let out = brrrrr().args(args.split_whitespace()).output().await.expect("spawn brrrrr");
    w.output = Some(out);
}

#[when(expr = "I run brrrrr with {string} and BRRRRR_CHECKPOINTS={string}")]
async fn run_with_env(w: &mut Brrrrr, args: String, checkpoints: String) {
    let out = brrrrr().args(args.split_whitespace()).env("BRRRRR_CHECKPOINTS", checkpoints).output().await;
    w.output = Some(out.expect("spawn brrrrr"));
}

/// This build (the default image's) refuses object store URLs; one with the object-store
/// feature (the -extended image's) goes to the store instead.
#[then("its output says whether this build has the object store")]
async fn output_object_store(w: &mut Brrrrr) {
    let o = w.output.as_ref().expect("ran");
    let text = String::from_utf8_lossy(&o.stderr);
    let refused = text.contains("without the object-store feature; give a directory, or run the -extended image");
    assert_eq!(refused, cfg!(not(feature = "object-store")), "{text}");
    assert_eq!(o.status.code(), Some(1), "{text}");
}

#[then("it exits successfully")]
async fn exits_ok(w: &mut Brrrrr) {
    let o = w.output.as_ref().expect("ran");
    assert!(o.status.success(), "status {:?}, stderr {}", o.status, String::from_utf8_lossy(&o.stderr));
}

#[then("it exits with an error")]
async fn exits_err(w: &mut Brrrrr) {
    let o = w.output.as_ref().expect("ran");
    assert_eq!(o.status.code(), Some(1), "stdout {}", String::from_utf8_lossy(&o.stdout));
}

#[then(expr = "its output contains {string}")]
async fn output_contains(w: &mut Brrrrr, needle: String) {
    let o = w.output.as_ref().expect("ran");
    let text = format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr));
    assert!(text.contains(&needle), "{needle:?} not in {text:?}");
}

fn client(w: &Brrrrr, extra: &[(&str, &str)]) -> ClientConfig {
    client_on(&broker(w), extra)
}

fn sink_client(w: &Brrrrr, extra: &[(&str, &str)]) -> ClientConfig {
    let mut c = client_on(&sink_broker(w), extra);
    for (k, v) in &w.sink_settings {
        c.set(k, v);
    }
    c
}

fn client_on(broker: &str, extra: &[(&str, &str)]) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", broker);
    for (k, v) in extra {
        c.set(*k, *v);
    }
    c
}

#[given("a Redpanda broker")]
async fn redpanda(w: &mut Brrrrr) {
    start_redpanda(w, 0).await;
}

#[given("a separate Redpanda broker for the sinks")]
async fn sink_redpanda(w: &mut Brrrrr) {
    w.sink_broker = Some(start_redpanda(w, 1).await);
}

#[given(expr = "a separate Redpanda broker at version {string} for the sinks")]
async fn sink_redpanda_version(w: &mut Brrrrr, version: String) {
    w.sink_broker = Some(start_redpanda_version(w, 1, &version).await);
}

/// A sinks' cluster that requires SASL: a SCRAM user, then SASL on its listener.
/// brrrrr gets the credentials as a file (`--sink-kafka-config @file`).
#[given("the sinks' broker requires SASL/SCRAM")]
async fn sink_sasl(w: &mut Brrrrr) {
    let container = redpanda_of(w, 1).0;
    let rpk = |args: &[&str]| {
        let out = std::process::Command::new("docker").args(["exec", &container, "rpk"]).args(args).output();
        let out = out.expect("docker exec");
        assert!(out.status.success(), "rpk {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    };
    rpk(&["security", "user", "create", "brrrrr", "-p", "s3cret", "--mechanism", "SCRAM-SHA-256"]);
    rpk(&["cluster", "config", "set", "superusers", r#"["brrrrr"]"#]);
    rpk(&["cluster", "config", "set", "enable_sasl", "true"]);
    let settings = [
        ("security.protocol", "sasl_plaintext"),
        ("sasl.mechanisms", "SCRAM-SHA-256"),
        ("sasl.username", "brrrrr"),
        ("sasl.password", "s3cret"),
    ];
    w.sink_settings = settings.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    // SASL is enforced: without credentials the broker refuses, with them it answers
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let plain: BaseConsumer = client_on(&sink_broker(w), &[]).create().unwrap();
        let authed: BaseConsumer = sink_client(w, &[]).create().unwrap();
        let t = Duration::from_secs(2);
        if plain.fetch_metadata(None, t).is_err() && authed.fetch_metadata(None, t).is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "SASL was not enforced");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let file = std::env::temp_dir().join(format!("brrrrr-it-sasl-{}-{}.properties", std::process::id(), w.id.0));
    let lines: Vec<String> = settings.iter().map(|(k, v)| format!("{k}={v}")).collect();
    std::fs::write(&file, format!("# the sinks' cluster\n{}\n", lines.join("\n"))).unwrap();
    w.extra = vec!["--sink-kafka-config".into(), format!("@{}", file.display())];
}

/// Keep discovery and replay authorized, but reproduce an anonymous writer denied InitProducerId.
#[given("the sinks' broker permits anonymous topic reads but denies idempotent writes")]
async fn sink_denies_idempotence(w: &mut Brrrrr) {
    let container = redpanda_of(w, 1).0;
    for args in [
        vec![
            "security",
            "acl",
            "create",
            "--allow-principal",
            "User:*",
            "--operation",
            "read,describe,describe-configs",
            "--topic",
            "*",
        ],
        vec!["security", "acl", "create", "--allow-principal", "User:*", "--operation", "read", "--group", "*"],
        vec!["security", "acl", "create", "--deny-principal", "User:*", "--operation", "idempotent-write", "--cluster"],
        vec!["cluster", "config", "set", "kafka_enable_authorization", "true"],
    ] {
        let out = std::process::Command::new("docker").args(["exec", &container, "rpk"]).args(&args).output().unwrap();
        assert!(out.status.success(), "rpk {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    }
    // Config writes return before the broker applies them. Do not let the producer acquire
    // its id while authorization is still off: an acquired id would not test InitProducerId.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let out = std::process::Command::new("docker")
            .args(["exec", &container, "rpk", "security", "acl", "list"])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&out.stdout).contains("CLUSTER_AUTHORIZATION_FAILED") {
            break;
        }
        assert!(Instant::now() < deadline, "authorization was not enforced: {out:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Starts the scenario's Redpanda `n` (see `redpanda_of`) and returns its address once it answers.
async fn start_redpanda(w: &mut Brrrrr, n: usize) -> String {
    start_redpanda_version(w, n, "v24.2.7").await
}

async fn start_redpanda_version(w: &mut Brrrrr, n: usize, version: &str) -> String {
    let (container, broker) = redpanda_of(w, n);
    let _ = std::process::Command::new("docker").args(["rm", "-f", "-v", &container]).output();
    w.containers.push(container.clone());
    let (host, port) = broker.rsplit_once(':').unwrap();
    let net = match std::env::var("BRRRRR_IT_BROKER") {
        Err(_) => vec!["--network".to_string(), "brrrrr-it".into(), "--network-alias".into(), host.to_string()],
        Ok(_) => vec!["-p".to_string(), format!("{port}:{port}")],
    };
    let image = format!("redpandadata/redpanda:{version}");
    let args = [
        "--cpus=1",
        "--memory=1g",
        &image,
        "redpanda",
        "start",
        "--mode",
        "dev-container",
        "--smp",
        "1",
        "--memory",
        "512M",
        // linux-aio reserves ~11,000 of the host's fs.aio-max-nr (65,536 by default) per broker:
        // with more than five scenarios' brokers at once, the next would not start
        "--reactor-backend=epoll",
        "--kafka-addr",
    ];
    let out = std::process::Command::new("docker")
        .args(["run", "-d", "--name", &container])
        .args(net)
        .args(args)
        .arg(format!("0.0.0.0:{port}"))
        .args(["--advertise-kafka-addr", &broker])
        .output()
        .expect("docker");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let consumer: BaseConsumer = client_on(&broker, &[]).create().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while consumer.fetch_metadata(None, Duration::from_secs(1)).is_err() {
        assert!(Instant::now() < deadline, "redpanda did not start");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    broker
}

#[given(expr = "the sink topic {string} with {int} partition(s)")]
async fn sink_topic(w: &mut Brrrrr, name: String, partitions: i32) {
    let admin: AdminClient<_> = sink_client(w, &[]).create().unwrap();
    let r = admin
        .create_topics(&[NewTopic::new(&name, partitions, TopicReplication::Fixed(1))], &AdminOptions::new())
        .await;
    assert!(r.unwrap().iter().all(|t| t.is_ok()), "creating {name}");
    visible(w, &name, partitions as usize).await;
}

#[given(expr = "the topic {string} with {int} partition(s) and {string}")]
async fn topic_with(w: &mut Brrrrr, name: String, partitions: i32, setting: String) {
    let (k, v) = setting.split_once('=').expect("key=value");
    let admin: AdminClient<_> = client(w, &[]).create().unwrap();
    let t = NewTopic::new(&name, partitions, TopicReplication::Fixed(1)).set(k, v);
    let r = admin.create_topics(&[t], &AdminOptions::new()).await;
    assert!(r.unwrap().iter().all(|t| t.is_ok()), "creating {name}");
    visible(w, &name, partitions as usize).await;
}

/// Waits (up to 30 s) until the broker's metadata has `topic` with `partitions` partitions: one
/// just created or grown may be unknown to a client for a moment, and a brrrrr started then
/// would stop on it (`UnknownTopicOrPartition`).
async fn visible(w: &Brrrrr, topic: &str, partitions: usize) {
    let c: BaseConsumer = client(w, &[]).create().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seen = c.fetch_metadata(Some(topic), Duration::from_secs(5)).ok().and_then(|md| {
            let t = md.topics().first()?;
            t.error().is_none().then(|| t.partitions().len())
        });
        if seen == Some(partitions) {
            return;
        }
        assert!(Instant::now() < deadline, "{topic}: {seen:?} partitions, {partitions} wanted");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Windows written by an earlier run, long ago: their messages carry that time.
#[given(expr = "{int} trades are waiting in the source")]
async fn waiting(w: &mut Brrrrr, n: usize) {
    let sql = std::fs::read_to_string(root().join("tests/acceptance/sql/windows.sql")).unwrap();
    produce(w, &codec(&sql), &(0..n).map(trade).collect::<Vec<_>>());
    (w.sql, w.produced, w.trades) = (Some(sql), n, n);
}

/// The windows the source's trades close by themselves, as an earlier run wrote them.
#[given(expr = "{string} already holds the windows those trades close, written {int} hours ago")]
async fn written_long_ago(w: &mut Brrrrr, topic: String, hours: i64) {
    let sql = w.sql.clone().unwrap();
    let at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
        - hours * 3_600_000;
    let producer: BaseProducer = sink_client(w, &[]).create().unwrap();
    let mut engine = Engine::new(&brrrrr_core::sql::parse(&sql).unwrap()).unwrap();
    let mut out = vec![];
    engine.insert("trades_source", (0..w.produced).map(trade).collect(), &mut out);
    let closed =
        out.into_iter().map(|e| (e.headers.into_iter().find(|h| h.0 == "redpanda-dedup-key").unwrap().1, e.payload));
    for (key, payload) in closed {
        let headers = rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header { key: "redpanda-dedup-key", value: Some(&key) });
        let rec = BaseRecord::<(), _>::to(&topic).payload(&payload).headers(headers).timestamp(at);
        producer.send(rec).map_err(|(e, _)| e).unwrap();
    }
    producer.flush(Duration::from_secs(30)).unwrap();
}

#[when("brrrrr has read every trade")]
async fn read_everything(w: &mut Brrrrr) {
    let want = format!("\nbrrrrr_received_events_total {}\n", w.produced + 1); // and the closing trade
    let deadline = Instant::now() + Duration::from_secs(60);
    while !get(w, "/metrics").contains(&want) {
        assert!(Instant::now() < deadline, "{want:?} not in {}", get(w, "/metrics"));
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await; // its output reaches the sink
}

#[given(expr = "the topic {string} takes messages of at most {int} bytes")]
async fn max_bytes(w: &mut Brrrrr, topic: String, bytes: u64) {
    use rdkafka::admin::{AlterConfig, ResourceSpecifier};
    let admin: AdminClient<_> = sink_client(w, &[]).create().unwrap();
    let limit = bytes.to_string();
    let config = AlterConfig::new(ResourceSpecifier::Topic(&topic)).set("max.message.bytes", &limit);
    let r = admin.alter_configs(&[config], &AdminOptions::new()).await.unwrap();
    assert!(r.iter().all(|c| c.is_ok()), "{r:?}");
}

#[given(expr = "the topic {string} with {int} partition(s)")]
async fn topic(w: &mut Brrrrr, name: String, partitions: i32) {
    let admin: AdminClient<_> = client(w, &[]).create().unwrap();
    let r = admin
        .create_topics(&[NewTopic::new(&name, partitions, TopicReplication::Fixed(1))], &AdminOptions::new())
        .await;
    assert!(r.unwrap().iter().all(|t| t.is_ok()), "creating {name}");
    visible(w, &name, partitions as usize).await;
}

/// Deterministic trades: 7 symbols, one per second of local time, then one far later that
/// closes every window. In order, so any chunking gives the same windows. With `priceless`,
/// the last of every `priceless` trades has no price: its message has no such field, which a
/// nullable column reads as NULL.
fn trades(n: usize, priceless: usize) -> Vec<Vec<Value>> {
    // the closing trade must come after every other: a trade before one already read is late,
    // and the pipeline drops it where `expected`, one batch, would still count it
    assert!((n as i64 + 60) * 1_000_000 < 2 * DAY, "{n} trades reach past the closing trade");
    let mut v: Vec<_> = (0..n).map(trade).collect();
    if priceless > 0 {
        v.iter_mut().skip(priceless - 1).step_by(priceless).for_each(|t| t[4] = Value::Null);
    }
    v.push(closing(n));
    v
}

/// A day in µs.
const DAY: i64 = 86_400_000_000;

/// The first trade's time (µs): midnight UTC two days ago. Records are stamped with their trades'
/// times, as the raw producers stamp them (a source's feed starts at its oldest record), so the
/// trades are recent enough for the topics' retention; midnight keeps every window aligned as
/// before. Two days, not one: the @stress scenario's 100 000 trades, a second apart, span 28
/// hours, and must all be in the past and before the closing trade.
fn base() -> i64 {
    static BASE: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *BASE.get_or_init(|| {
        let now = now_us();
        let today = now - now.rem_euclid(DAY);
        today - 2 * DAY
    })
}

/// Trade `i`, one second after trade `i - 1`.
fn trade(i: usize) -> Vec<Value> {
    trade_at(i, base() + i as i64 * 1_000_000)
}

/// A trade two days after the first (today's midnight: never in the future), closing every window.
fn closing(i: usize) -> Vec<Value> {
    trade_at(i, base() + 2 * DAY)
}

fn trade_at(i: usize, local: i64) -> Vec<Value> {
    {
        let price = 100.0 + ((i * 37) % 1000) as f64 / 100.0;
        vec![
            Value::Int(local - 5_000),
            Value::Str(i.to_string().into()),
            Value::Int(1),
            Value::Str(["A", "B", "C", "D", "E", "F", "G"][i % 7].into()),
            Value::F64(price),
            Value::Int(local),
            Value::Str(["buy", "sell"][i % 2].into()),
            Value::F64(1.0),
            Value::F64(price),
        ]
    }
}

/// How the trades are written to their topic: a protobuf Trade (fixtures/market.proto), or for
/// a JSONEachRow source a JSON object of its columns, each value as its column's type writes it (a datetime as text).
enum Encoder {
    Proto(Codec),
    Json(Vec<(String, Type)>),
}

impl Encoder {
    fn encode(&self, row: &[Value], out: &mut Vec<u8>) {
        match self {
            Encoder::Proto(c) => c.encode(row, out),
            Encoder::Json(cols) => {
                let (row, mut line): (Vec<_>, _) =
                    (row.iter().zip(cols).map(|(v, c)| v.cast(&c.1)).collect(), String::new());
                brrrrr_core::format::json_row(&mut line, cols, &row);
                out.extend_from_slice(line.trim_end().as_bytes());
            }
        }
    }
}

fn codec(sql: &str) -> Encoder {
    let cat = brrrrr_core::sql::parse(sql).unwrap();
    let source = &cat.streams["trades_source"];
    let cols: Vec<_> = source.columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect();
    if source.settings["data_format"] == "JSONEachRow" {
        return Encoder::Json(cols);
    }
    let protos = parse_proto(&std::fs::read_to_string(root().join("fixtures/market.proto")).unwrap()).unwrap();
    Encoder::Proto(Codec::new(&protos["Trade"], &cols))
}

/// A record's timestamp is its trade's time (column 5, µs), as a producer's is.
fn record_ms(row: &[Value]) -> Option<i64> {
    Some(row.get(5)?.i64()? / 1000)
}

/// Produces rows round-robin over the source topic's partitions, in order within each.
fn produce(w: &Brrrrr, codec: &Encoder, rows: &[Vec<Value>]) {
    let producer: BaseProducer = client(w, &[("linger.ms", "5")]).create().unwrap();
    let parts = producer.client().fetch_metadata(Some("raw.trades.test"), Duration::from_secs(10)).unwrap().topics()[0]
        .partitions()
        .len() as i32;
    for (i, r) in rows.iter().enumerate() {
        let mut payload = vec![];
        codec.encode(r, &mut payload);
        let rec = || {
            let record = BaseRecord::<(), _>::to("raw.trades.test").payload(&payload).partition(i as i32 % parts);
            match record_ms(r) {
                Some(ts) => record.timestamp(ts),
                None => record,
            }
        };
        while let Err((e, _)) = producer.send(rec()) {
            assert!(format!("{e}").contains("full"), "{e}");
            producer.poll(Duration::from_millis(10));
        }
    }
    producer.flush(Duration::from_secs(30)).unwrap();
}

fn start(w: &mut Brrrrr, sql: &str, interval: u64) -> tokio::process::Child {
    start_on(w, sql, interval, 0)
}

/// Starts the scenario's first process (`second = 0`) or its second, on a `/metrics` of its own.
fn start_on(w: &mut Brrrrr, sql: &str, interval: u64, second: usize) -> tokio::process::Child {
    let id = w.id.0;
    let dir = w
        .checkpoints
        .get_or_insert_with(|| std::env::temp_dir().join(format!("brrrrr-it-{}-{id}", std::process::id())))
        .clone();
    std::fs::create_dir_all(&dir).unwrap();
    let mut run = brrrrr();
    match &w.s3 {
        // as a pod is configured: a directory on its volume, from the environment
        None => run.env("BRRRRR_CHECKPOINTS", &dir),
        Some(url) => run.env("BRRRRR_CHECKPOINTS", url).envs(s3_env()),
    };
    // the first process reaches the store through the proxy, a second one directly
    if let (Some(proxy), 0) = (&w.proxy, second) {
        run.env("AWS_ENDPOINT", &proxy.endpoint);
    }
    run.args(["run", sql]);
    // a JSONEachRow pipeline runs without one, as users run it
    if std::fs::read_to_string(root().join(sql)).unwrap().contains("ProtobufSingle") {
        run.args(["--proto", "fixtures/market.proto"]);
    }
    // a scenario's own --interval (in `extra`) replaces the step's
    if !w.extra.iter().any(|a| a == "--interval") {
        run.args(["--interval", &interval.to_string()]);
    }
    let mut child = run
        .arg("--brokers")
        .arg(broker(w))
        .args(["--metrics", &metrics_addr(w, second), "--sink-topic-prefix", &w.prefix, "--sink-brokers"])
        .arg(sink_broker(w))
        .args(&w.extra)
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn brrrrr run");
    // keep every process's stderr, for steps that check what it said
    let (log, mut err) = (w.log.clone(), child.stderr.take().unwrap());
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0; 4096];
        while let Ok(n @ 1..) = err.read(&mut buf).await {
            log.lock().unwrap().push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    });
    child
}

/// A message's dedup key (its payload, without the header) and payload, as `read_all` reads it.
fn key_and_payload(e: brrrrr_core::engine::Emit) -> (String, String) {
    let key = e.headers.into_iter().find(|h| h.0 == "redpanda-dedup-key").map_or_else(|| e.payload.clone(), |h| h.1);
    (key, e.payload)
}

/// Every message of a topic: (dedup key, or its payload without one; payload), in no particular order. Each partition is
/// read from its log start (retention may have deleted older records) to its end of file:
/// a transaction's markers and aborted messages take offsets but are never delivered.
fn read_all(w: &Brrrrr, topic: &str) -> Vec<(String, String)> {
    let reader = [("group.id", "brrrrr-it-reader"), ("enable.partition.eof", "true")];
    let consumer: BaseConsumer = sink_client(w, &reader).create().unwrap();
    let md = consumer.fetch_metadata(Some(topic), Duration::from_secs(10)).unwrap();
    let (mut tpl, mut open) = (TopicPartitionList::new(), std::collections::HashSet::new());
    for p in md.topics()[0].partitions() {
        tpl.add_partition_offset(topic, p.id(), Offset::Beginning).unwrap();
        open.insert(p.id());
    }
    consumer.assign(&tpl).unwrap();
    let mut out = vec![];
    while !open.is_empty() {
        let m = match consumer.poll(Duration::from_secs(10)) {
            None => panic!("{topic}: partitions {open:?} never reached their end"),
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(p))) => {
                open.remove(&p);
                continue;
            }
            Some(m) => m.unwrap(),
        };
        let key = m.headers().and_then(|h| h.iter().find(|h| h.key == "redpanda-dedup-key").and_then(|h| h.value));
        let text = |b: Option<&[u8]>| String::from_utf8_lossy(b.unwrap_or_default()).into_owned();
        out.push((text(key.or(m.payload())), text(m.payload())));
    }
    out
}

fn expected(sql: &str, n: usize, priceless: usize) -> Vec<(String, String)> {
    let mut engine = Engine::new(&brrrrr_core::sql::parse(sql).unwrap()).unwrap();
    let mut out = vec![];
    engine.insert("trades_source", trades(n, priceless), &mut out);
    out.into_iter().map(key_and_payload).collect()
}

/// What the engine emits to `topic` for the first `n` trades and the closing one.
fn expected_on(sql: &str, n: usize, priceless: usize, topic: &str) -> Vec<(String, String)> {
    let mut engine = Engine::new(&brrrrr_core::sql::parse(sql).unwrap()).unwrap();
    let mut out = vec![];
    engine.insert("trades_source", trades(n, priceless), &mut out);
    out.retain(|e| &*e.topic == topic);
    out.into_iter().map(key_and_payload).collect()
}

/// Every message of a topic's partitions, with its record timestamp (ms): when brrrrr's
/// producer took it (CreateTime).
fn read_timed(w: &Brrrrr, topic: &str) -> Vec<(String, i64)> {
    let reader = [("group.id", "brrrrr-it-reader"), ("enable.partition.eof", "true")];
    let consumer: BaseConsumer = sink_client(w, &reader).create().unwrap();
    let md = consumer.fetch_metadata(Some(topic), Duration::from_secs(10)).unwrap();
    let (mut tpl, mut open) = (TopicPartitionList::new(), std::collections::HashSet::new());
    for p in md.topics()[0].partitions() {
        tpl.add_partition_offset(topic, p.id(), Offset::Beginning).unwrap();
        open.insert(p.id());
    }
    consumer.assign(&tpl).unwrap();
    let mut out = vec![];
    while !open.is_empty() {
        match consumer.poll(Duration::from_secs(10)) {
            None => panic!("{topic}: partitions {open:?} never reached their end"),
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(p))) => drop(open.remove(&p)),
            Some(m) => {
                let m = m.unwrap();
                let key =
                    m.headers().and_then(|h| h.iter().find(|h| h.key == "redpanda-dedup-key").and_then(|h| h.value));
                let ts = m.timestamp().to_millis().expect("a record timestamp");
                out.push((String::from_utf8_lossy(key.unwrap_or_default()).into_owned(), ts));
            }
        }
    }
    out
}

/// Waits until `topic` holds every message the engine emits to it for the trades so far.
async fn settle_on(w: &Brrrrr, topic: &str) -> usize {
    let want = expected_on(w.sql.as_ref().unwrap(), w.trades, w.priceless, topic).len();
    let deadline = Instant::now() + Duration::from_secs(90);
    while count(w, topic) < want {
        assert!(
            Instant::now() < deadline,
            "{topic}: {} of {want} messages: {}",
            count(w, topic),
            w.log.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    committed(w, topic, want, deadline).await;
    want
}

/// Waits (until `deadline`) for `want` messages of `topic` to be readable. A topic's high
/// watermark counts a transaction's messages before it commits, and a reader sees them only once
/// it has: on a slow runner, after the step's next look.
async fn committed(w: &Brrrrr, topic: &str, want: usize, deadline: Instant) {
    while read_all(w, topic).len() < want && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[then(regex = r#"^"([^"]+)" holds exactly the engine's messages to it for those trades$"#)]
async fn holds_exactly_its_own(w: &mut Brrrrr, topic: String) {
    settle_on(w, &topic).await;
    tokio::time::sleep(Duration::from_secs(3)).await; // a duplicate would arrive meanwhile
    let mut want = expected_on(w.sql.as_ref().unwrap(), w.trades, w.priceless, &topic);
    let mut got = read_all(w, &topic);
    assert!(want.len() > 100, "the scenario proves little with {} messages", want.len());
    want.sort();
    got.sort();
    let dups = got.windows(2).filter(|p| p[0].0 == p[1].0).count();
    assert_eq!(dups, 0, "{dups} duplicated messages on {topic}");
    assert_eq!(got.len(), want.len(), "{topic}");
    assert!(got == want, "{topic} holds other messages than the engine emits");
}

/// A view's messages are produced once the view has written them, before the views after it
/// run: the last messages of `first` (those the closing trade closed) were taken by the
/// producer at least `ms` before the first of `then`, whose view runs after and closes tens of
/// thousands of groups. Produced together after the batch, they would be a few ms apart.
#[then(expr = "the last messages on {string} were produced at least {int} ms before the first on {string}")]
async fn produced_before(w: &mut Brrrrr, first: String, ms: i64, then: String) {
    settle_on(w, &first).await;
    let n = settle_on(w, &then).await;
    let last = read_timed(w, &first).into_iter().map(|m| m.1).max().unwrap();
    let later = read_timed(w, &then);
    assert_eq!(later.len(), n);
    let earliest = later.iter().map(|m| m.1).min().unwrap();
    let span = later.iter().map(|m| m.1).max().unwrap() - earliest;
    assert!(
        earliest - last >= ms,
        "{first}'s last message at {last}, {then}'s first at {earliest} ({} ms apart; {then}'s span {span} ms)",
        earliest - last
    );
}

/// Waits until `n` processes of this scenario have claimed the pipeline.
async fn claimed(w: &Brrrrr, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while w.log.lock().unwrap().matches("claimed the pipeline").count() < n {
        assert!(Instant::now() < deadline, "no takeover: {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits until the sink topic holds as many messages as expected (or 90 s pass); with no topic,
/// until the sinks' topics (every topic but the source's) hold that many together.
async fn settle(w: &Brrrrr, topic: Option<&str>, want: usize) {
    let consumer: BaseConsumer = sink_client(w, &[]).create().unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let md = consumer.fetch_metadata(topic, Duration::from_secs(5));
        let parts: Vec<(String, i32)> = md
            .iter()
            .flat_map(|m| m.topics())
            .filter(|t| topic.is_some() || !(t.name().starts_with('_') || t.name() == "raw.trades.test"))
            .flat_map(|t| t.partitions().iter().map(|p| (t.name().to_string(), p.id())))
            .collect();
        let total: i64 = parts
            .iter()
            .map(|(t, p)| consumer.fetch_watermarks(t, *p, Duration::from_secs(5)).map_or(0, |w| w.1))
            .sum();
        if total as usize >= want || Instant::now() > deadline {
            if let Some(topic) = topic {
                committed(w, topic, want, deadline).await;
            }
            // a little longer: anything beyond `want` would be a duplicate the test must see
            tokio::time::sleep(Duration::from_secs(3)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[given(expr = "brrrrr writes its sinks with the prefix {string}")]
async fn with_prefix(w: &mut Brrrrr, prefix: String) {
    w.prefix = prefix;
}

#[given(expr = "brrrrr writes its sinks to {string}")]
async fn with_template(w: &mut Brrrrr, template: String) {
    w.extra.extend(["--sink-topic-template".into(), template]);
}

#[then(expr = "{string} holds no message")]
async fn holds_nothing(w: &mut Brrrrr, topic: String) {
    assert!(read_all(w, &topic).is_empty(), "{topic} was written");
}

#[when(
    regex = r#"^brrrrr runs "([^"]+)" over (\d+) trades(?:, killed (\d+) times)?(?:, every (\d+)th without a price)?$"#
)]
async fn runs(w: &mut Brrrrr, path: String, n: usize, kills: String, priceless: String) {
    // A fixture before midnight creates a partial prior-minute window that the runtime must
    // withhold, while the exact-once oracle expects complete windows from midnight onward.
    assert_eq!(base().rem_euclid(DAY), 0, "trade fixtures must start exactly at UTC midnight");
    let kills: usize = kills.parse().unwrap_or(0);
    w.priceless = priceless.parse().unwrap_or(0);
    let sql = std::fs::read_to_string(root().join(&path)).unwrap();
    let (codec, rows) = (codec(&sql), trades(n, w.priceless));
    let want = expected(&sql, n, w.priceless).len();
    // with kills, checkpoints are 2 s apart and each kill lands 0.1-0.9 s after a slice was
    // processed: its windows are mostly in the topic but not covered by a checkpoint, so the
    // restart replays them and only the suppression keeps them from being written twice. The
    // replacement stands by for --takeover (3 * interval): a longer interval would only add
    // waiting to every kill.
    let interval = if kills > 0 { 2 } else { 1 };
    let mut child = start(w, &path, interval);
    // a kill every 8 slices (~7 s): past at least one 2 s checkpoint, so restores are exercised
    let slices = (kills + 1) * 8;
    let mut rng = 0x2545_f491_4f6c_dd1du64;
    for (i, part) in rows.chunks(rows.len().div_ceil(slices)).enumerate() {
        produce(w, &codec, part);
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        if kills > 0 && i % 8 == 7 && i / 8 < kills {
            tokio::time::sleep(Duration::from_millis(100 + rng % 800)).await;
            child.kill().await.unwrap();
            child = start(w, &path, interval);
            // the replacement stands by until the killed one's lease runs out: let it take over
            // before the next kill, so every kill interrupts a running pipeline
            claimed(w, i / 8 + 2).await;
        } else {
            tokio::time::sleep(Duration::from_millis(200 + rng % 1_300)).await;
        }
    }
    settle(w, None, want).await;
    (w.metrics, w.health) = (get(w, "/metrics"), get(w, "/health"));
    child.kill().await.unwrap();
    (w.sql, w.trades) = (Some(sql), n);
}

/// Order book messages of three symbols as two producer replicas publish them, each with its
/// venue sequence and dedup key: a snapshot per symbol, diffs with rising venue seqs, then
/// replica B's REST snapshot of A with a seq older than A's own snapshot but a newer time (bids
/// at 50, which A's book never has), so neither applicable nor to be brought up to date, then
/// one more diff of A.
fn book_messages() -> Vec<(Vec<Value>, String)> {
    let arr = |v: &[f64]| Value::Array(v.iter().map(|x| Value::F64(*x)).collect());
    let msg = |t: i64, sym: &str, snap: bool, seq: i64, bids: &[f64], asks: &[f64]| {
        let row = vec![
            Value::Int(t),
            Value::Int(1),
            Value::Str(sym.into()),
            Value::Bool(snap),
            arr(bids),
            arr(&vec![1.0; bids.len()]),
            arr(asks),
            arr(&vec![1.0; asks.len()]),
            Value::Int(t + 3),
            Value::Int(seq),
        ];
        (row, format!("{t}-test-perpetual-{sym}-USDT:USD-{seq}"))
    };
    let mut out = vec![];
    for (i, sym) in ["A", "B", "C"].iter().enumerate() {
        out.push(msg(base() + i as i64, sym, true, 100, &[99.0, 98.0, 97.0], &[101.0, 102.0, 103.0]));
    }
    for i in 0..60 {
        let sym = ["A", "B", "C"][i % 3];
        // a new best bid each time: every diff changes the top 5
        out.push(msg(base() + 10 + i as i64, sym, false, 101 + i as i64 / 3, &[99.0 + 0.01 * (i + 1) as f64], &[]));
    }
    out.push(msg(base() + 100, "A", true, 99, &[50.0], &[51.0]));
    out.push(msg(base() + 101, "A", false, 121, &[99.9], &[]));
    out
}

#[when(expr = "brrrrr runs {string} over order books with a replica's late snapshot")]
async fn runs_books(w: &mut Brrrrr, path: String) {
    let sql = std::fs::read_to_string(root().join(&path)).unwrap();
    let cat = brrrrr_core::sql::parse(&sql).unwrap();
    let protos = parse_proto(&std::fs::read_to_string(root().join("fixtures/market.proto")).unwrap()).unwrap();
    let cols: Vec<_> = cat.streams["ob"].columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect();
    let codec = Codec::new(&protos["BookUpdate"], &cols);
    let producer: BaseProducer = client(w, &[]).create().unwrap();
    for (row, key) in book_messages() {
        let mut payload = vec![];
        codec.encode(&row, &mut payload);
        let headers = OwnedHeaders::new().insert(Header { key: "redpanda-dedup-key", value: Some(&key) });
        // stamped with the message's venue time (ms), as the producers stamp it
        let Value::Int(t) = row[0] else { unreachable!() };
        let record =
            BaseRecord::<(), _>::to("raw.orderbook.test").payload(&payload).headers(headers).timestamp(t / 1000);
        producer.send(record).unwrap();
    }
    producer.flush(Duration::from_secs(30)).unwrap();
    let mut child = start(w, &path, 1);
    settle(w, Some("book.top"), book_rows(&sql).len()).await;
    child.kill().await.unwrap();
    w.sql = Some(sql);
}

/// What the engine emits for `book_messages`.
fn book_rows(sql: &str) -> Vec<(String, String)> {
    let mut engine = Engine::new(&brrrrr_core::sql::parse(sql).unwrap()).unwrap();
    let rows = book_messages().into_iter().map(|(row, _)| row).collect();
    let mut out = vec![];
    engine.insert("ob", rows, &mut out);
    out.into_iter().map(|e| (e.headers[0].1.clone(), e.payload)).collect()
}

#[then(expr = "{string} holds exactly the engine's book rows, none of them the late snapshot's")]
async fn holds_books(w: &mut Brrrrr, topic: String) {
    let (mut want, mut got) = (book_rows(w.sql.as_ref().unwrap()), read_all(w, &topic));
    want.sort();
    got.sort();
    assert_eq!(want.len(), 3 + 60 + 1, "a row per snapshot and diff, but none for the late snapshot");
    assert!(got.iter().all(|(_, p)| !p.contains("[50]")), "the late snapshot was applied");
    assert_eq!(got, want);
}

#[when("its checkpoints are lost and it runs again")]
async fn rerun_without_checkpoints(w: &mut Brrrrr) {
    std::fs::remove_dir_all(w.checkpoints.as_ref().unwrap()).unwrap();
    let mut child = start(w, "tests/acceptance/sql/windows.sql", 1);
    // it replays the whole input and must suppress every window already in the topic
    tokio::time::sleep(Duration::from_secs(8)).await;
    child.kill().await.unwrap();
}

#[when("its checkpoints are lost")]
async fn checkpoints_lost(w: &mut Brrrrr) {
    std::fs::remove_dir_all(w.checkpoints.as_ref().unwrap()).unwrap();
}

#[when(expr = "{int} seconds pass")]
async fn seconds_pass(_: &mut Brrrrr, secs: u64) {
    tokio::time::sleep(Duration::from_secs(secs)).await;
}

#[then(regex = r#"^"([^"]+)" holds exactly the engine's messages for those trades$"#)]
async fn holds_exactly(w: &mut Brrrrr, topic: String) {
    let mut want = expected(w.sql.as_ref().unwrap(), w.trades, w.priceless);
    settle(w, Some(&topic), want.len()).await;
    let mut got = read_all(w, &topic);
    assert!(want.len() > 100, "the scenario proves little with {} windows", want.len());
    want.sort();
    got.sort();
    let dups = got.windows(2).filter(|p| p[0].0 == p[1].0).count();
    let missing: Vec<_> = want.iter().filter(|x| got.binary_search(x).is_err()).take(3).collect();
    assert_eq!(dups, 0, "{dups} duplicated windows");
    assert!(
        missing.is_empty(),
        "{} of {} windows missing, e.g. {missing:?}{}",
        want.len() - got.len().min(want.len()),
        want.len(),
        diagnosis(w)
    );
    assert_eq!(got, want);
}

/// A message or answer row as JSON, its datetimes (`2024-01-02 09:30:00.000`, `…T09:30:00Z`) as µs.
fn normalized(line: &str) -> String {
    let mut v: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line).unwrap();
    for x in v.values_mut() {
        let us =
            x.as_str().and_then(|s| brrrrr_core::value::parse_datetime(&s.trim_end_matches('Z').replace('T', " ")));
        if let Some(us) = us {
            *x = us.into();
        }
    }
    serde_json::Value::Object(v).to_string()
}

/// "Backfill = live": the run's sink holds exactly what `brrrrr sql` answers for its view's
/// query over the same trades in a JSON file, the closing trade's (whose window is not closed) aside.
#[then(regex = r#"^"([^"]+)" holds what brrrrr sql answers for its view over those trades in a file$"#)]
async fn holds_the_backfill(w: &mut Brrrrr, topic: String) {
    let sql = w.sql.clone().unwrap();
    let query = sql[sql.find("AS\nSELECT").expect("a view AS SELECT ...") + 3..].trim().trim_end_matches(';');
    let (encoder, path) =
        (codec(&sql), std::env::temp_dir().join(format!("brrrrr-it-{}-{}.jsonl", std::process::id(), w.id.0)));
    let mut file = vec![];
    for r in &trades(w.trades, 0)[..w.trades] {
        encoder.encode(r, &mut file);
        file.push(b'\n');
    }
    std::fs::write(&path, file).unwrap();
    let table = format!("trades_source={}", path.display());
    let out = brrrrr().args(["sql", "--format", "json", "-t", &table, query]).output().await.unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let mut want: Vec<String> = String::from_utf8(out.stdout).unwrap().lines().map(normalized).collect();
    settle(w, Some(&topic), want.len()).await;
    let mut got: Vec<String> = read_all(w, &topic).iter().map(|(_, payload)| normalized(payload)).collect();
    assert!(want.len() > 100, "the scenario proves little with {} bars", want.len());
    want.sort();
    got.sort();
    assert_eq!(got, want);
}

#[then(expr = "its metrics count {int} received and {int} sent messages")]
async fn metrics_count(w: &mut Brrrrr, received: u64, sent: u64) {
    for (name, want) in [("brrrrr_received_events_total", received), ("brrrrr_sent_events_total", sent)] {
        let line = format!("\n{name} {want}\n");
        assert!(w.metrics.contains(&line), "{line:?} not in {}", w.metrics);
    }
    for name in [
        "brrrrr_checkpoints_total",
        "brrrrr_watermark_seconds",
        "brrrrr_event_time_seconds",
        "brrrrr_checkpoint_age_seconds",
        "brrrrr_checkpoint_duration_seconds",
        "brrrrr_checkpoint_stall_seconds",
        "brrrrr_consumer_lag",
        "brrrrr_suppression_keys",
        "brrrrr_source_idle_seconds",
    ] {
        assert!(w.metrics.contains(&format!("# TYPE {name} ")), "{name} missing");
    }
}

/// A line of the metrics scraped before the run's process stopped (`brrrrr runs ...`).
#[then(expr = "its last metrics say {string}")]
async fn last_metrics_say(w: &mut Brrrrr, line: String) {
    assert!(w.metrics.contains(&format!("\n{line}\n")), "{line:?} not in {}", w.metrics);
}

/// The batch histograms counted the run's batches: each as many as the others, the
/// engine's time spent on them, and among them the batches that closed groups.
#[then("its last metrics count its batches")]
async fn last_metrics_count_batches(w: &mut Brrrrr) {
    let v = |name: &str| -> f64 {
        let line = w.metrics.lines().find_map(|l| l.strip_prefix(&format!("{name} ")));
        line.and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("no {name} in {}", w.metrics))
    };
    let batches = v("brrrrr_batch_engine_seconds_count");
    assert!(batches > 0.0, "{}", w.metrics);
    for h in ["brrrrr_batch_produce_seconds", "brrrrr_batch_emitted_messages", "brrrrr_batch_closed_groups"] {
        assert_eq!(v(&format!("{h}_count")), batches, "{h}");
    }
    assert!(v("brrrrr_batch_engine_seconds_sum") > 0.0);
    // the batches that closed groups: some, and no more than there were batches (here, slices of
    // trades minutes long, each of them may)
    let closing = v("brrrrr_batch_closing_engine_seconds_count");
    assert!(closing > 0.0 && closing <= batches, "{closing} closing batches of {batches}");
    assert_eq!(closing, batches - v("brrrrr_batch_closed_groups_bucket{le=\"0\"}"), "those that closed none");
}

/// Some inserts closed their views' windows on several threads (`brrrrr_parallel_closes_total`).
#[then("its last metrics count closes on several threads")]
async fn last_metrics_count_parallel_closes(w: &mut Brrrrr) {
    let line = w.metrics.lines().find_map(|l| l.strip_prefix("brrrrr_parallel_closes_total "));
    let n: f64 = line.and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("{}", w.metrics));
    assert!(n > 0.0, "no close on several threads: {}", w.metrics);
}

#[then("its health check passes")]
async fn healthy(w: &mut Brrrrr) {
    assert!(w.health.starts_with("HTTP/1.1 200 OK") && w.health.ends_with("ok\n"), "{}", w.health);
}

#[when(expr = "brrrrr is started on {string}")]
async fn started(w: &mut Brrrrr, path: String) {
    w.sql = Some(std::fs::read_to_string(root().join(&path)).unwrap());
    w.child = Some(start(w, &path, 1));
}

#[when(expr = "{int} more trades are produced")]
async fn more_trades(w: &mut Brrrrr, n: usize) {
    let rows: Vec<_> = (w.produced..w.produced + n).map(trade).collect();
    produce(w, &codec(w.sql.as_ref().unwrap()), &rows);
    w.produced += n;
    w.trades = w.produced;
    tokio::time::sleep(Duration::from_secs(2)).await;
}

/// A trade whose time is in the year 3000: a corrupt feed. It is not one of the trades whose
/// windows the engine emits.
#[when("a trade from the year 3000 is produced")]
async fn far_future(w: &mut Brrrrr) {
    produce(w, &codec(w.sql.as_ref().unwrap()), &[trade_at(w.produced, 32_503_680_000_000_000)]);
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[then(expr = "its metrics say {string}")]
async fn metrics_say(w: &mut Brrrrr, line: String) {
    let metrics = get(w, "/metrics");
    assert!(metrics.contains(&format!("\n{line}\n")), "{line:?} not in {metrics}");
}

#[then("its metrics count failed checkpoint writes")]
async fn metrics_count_failures(w: &mut Brrrrr) {
    let metrics = get(w, "/metrics");
    let n = metrics.lines().find_map(|l| l.strip_prefix("brrrrr_checkpoint_failures_total "));
    assert!(n.and_then(|n| n.parse::<f64>().ok()).is_some_and(|n| n > 0.0), "{metrics}");
}

#[when("the closing trade is produced")]
async fn closing_trade(w: &mut Brrrrr) {
    produce(w, &codec(w.sql.as_ref().unwrap()), &[closing(w.produced)]);
    settle(w, Some(&format!("{}test.1m", w.prefix)), expected(w.sql.as_ref().unwrap(), w.produced, w.priceless).len())
        .await;
}

#[when("brrrrr is stopped with SIGTERM")]
async fn sigterm(w: &mut Brrrrr) {
    let mut child = w.child.take().unwrap();
    let pid = child.id().unwrap().to_string();
    assert!(std::process::Command::new("kill").args(["-TERM", &pid]).status().unwrap().success());
    w.stopped = Some(tokio::time::timeout(Duration::from_secs(30), child.wait()).await.expect("stops").unwrap());
}

#[when("brrrrr is killed")]
async fn killed(w: &mut Brrrrr) {
    w.child.take().unwrap().kill().await.unwrap();
}

/// A transactional writer of the sink topic (a Connect replicator, another transactional producer)
/// aborts a transaction in each partition: their last offsets hold its message and its abort
/// marker, neither of which a reader is ever given.
#[when(expr = "another writer aborts a transaction on {string}")]
async fn aborted(w: &mut Brrrrr, topic: String) {
    let producer: BaseProducer = sink_client(w, &[("transactional.id", "brrrrr-it-foreign")]).create().unwrap();
    let parts = producer.client().fetch_metadata(Some(&topic), Duration::from_secs(10)).unwrap().topics()[0]
        .partitions()
        .len() as i32;
    producer.init_transactions(Duration::from_secs(30)).unwrap();
    producer.begin_transaction().unwrap();
    for p in 0..parts {
        producer.send(BaseRecord::<(), _>::to(&topic).payload("aborted").partition(p)).map_err(|(e, _)| e).unwrap();
    }
    producer.flush(Duration::from_secs(30)).unwrap();
    producer.abort_transaction(Duration::from_secs(30)).unwrap();
}

#[then(expr = "its log does not say {string}")]
async fn log_does_not_say(w: &mut Brrrrr, text: String) {
    let log = w.log.lock().unwrap().clone();
    assert!(!log.contains(&text), "the log says {text:?}:\n{log}");
}

#[when(expr = "brrrrr is started again on {string}")]
async fn restarted(w: &mut Brrrrr, path: String) {
    w.log.lock().unwrap().clear();
    w.child = Some(start(w, &path, 1));
}

#[when(expr = "brrrrr is started again on {string} with {string}")]
async fn restarted_with(w: &mut Brrrrr, path: String, args: String) {
    w.log.lock().unwrap().clear();
    let n = w.extra.len();
    w.extra.extend(args.split_whitespace().map(String::from));
    w.child = Some(start(w, &path, 1));
    w.extra.truncate(n);
}

/// The checkpoint files of this scenario, oldest first (their names are zero-padded epochs).
fn checkpoint_files(w: &Brrrrr) -> Vec<PathBuf> {
    fn walk(d: &std::path::Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "ckpt") {
                out.push(p);
            }
        }
    }
    let mut files = vec![];
    walk(w.checkpoints.as_ref().unwrap(), &mut files);
    files.sort_by_key(|p| p.file_name().unwrap().to_owned());
    files
}

/// Every file on the scenario's volume, relative to it.
fn volume_files(w: &Brrrrr) -> Vec<String> {
    fn walk(root: &std::path::Path, d: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(root, &p, out);
            } else {
                out.push(p.strip_prefix(root).unwrap().display().to_string());
            }
        }
    }
    let (mut files, root) = (vec![], w.checkpoints.as_ref().unwrap());
    walk(root, root, &mut files);
    files.sort();
    files
}

/// Checkpoints are files `<pipeline>/<hash of the SQL>/<epoch>.ckpt` on the volume, at most 5 of
/// them, their leases `<pipeline>/<epoch>.lease`, and nothing else is there but a checkpoint being
/// written (a hidden temporary file).
#[then(expr = "its volume holds the checkpoints of {string} and nothing else")]
async fn volume_holds(w: &mut Brrrrr, pipeline: String) {
    let mut files = volume_files(w);
    files.retain(|f| !(f.rsplit('/').next().unwrap().starts_with('.') && f.ends_with(".tmp")));
    let ckpts = files.iter().filter(|f| f.ends_with(".ckpt")).count();
    assert!((1..=5).contains(&ckpts), "{files:?}");
    for f in &files {
        let parts: Vec<&str> = f.split('/').collect();
        if let [p, name] = parts[..] {
            assert_eq!(p, pipeline, "{files:?}");
            let epoch = name.strip_suffix(".lease").or(name.strip_suffix(".released"));
            assert!(epoch.is_some_and(|e| e.len() == 20 && e.parse::<u64>().is_ok()), "{f} in {files:?}");
            continue;
        }
        let [p, key, name] = parts[..] else { panic!("{f} is not <pipeline>/<key>/<name>: {files:?}") };
        assert_eq!(p, pipeline, "{files:?}");
        assert!(key.len() == 16 && key.chars().all(|c| c.is_ascii_hexdigit()), "{files:?}");
        let epoch = name.strip_suffix(".ckpt").or(name.strip_suffix(".released"));
        assert!(epoch.is_some_and(|e| e.len() == 20 && e.parse::<u64>().is_ok()), "{f} in {files:?}");
    }
}

/// The pod is rescheduled with its volume: the volume moves (here: is copied elsewhere and the
/// original removed), and the pod finds it at the same mount.
#[when("its volume moves to another place")]
async fn volume_moves(w: &mut Brrrrr) {
    let from = w.checkpoints.take().unwrap();
    let to = from.with_extension("moved");
    let _ = std::fs::remove_dir_all(&to);
    let status = std::process::Command::new("cp").arg("-a").arg(&from).arg(&to).status().unwrap();
    assert!(status.success());
    std::fs::remove_dir_all(&from).unwrap();
    w.checkpoints = Some(to);
}

#[then("it is still running")]
async fn still_running(w: &mut Brrrrr) {
    let exited = w.child.as_mut().unwrap().try_wait().unwrap();
    assert!(exited.is_none(), "exited {exited:?}: {}", w.log.lock().unwrap());
}

/// The S3 at `BRRRRR_IT_S3` (CI runs adobe/s3mock with a bucket `checkpoints`), as the
/// `-extended` image is configured for one: `AWS_*` settings.
fn s3_env() -> Vec<(&'static str, String)> {
    let endpoint = std::env::var("BRRRRR_IT_S3").expect("@object-store scenarios need an S3 endpoint in BRRRRR_IT_S3");
    let fixed = [
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_CONDITIONAL_PUT", "etag"),
        ("AWS_ACCESS_KEY_ID", "k"),
        ("AWS_SECRET_ACCESS_KEY", "s"),
        ("AWS_REGION", "us-east-1"),
    ];
    fixed.iter().map(|(k, v)| (*k, v.to_string())).chain([("AWS_ENDPOINT", endpoint)]).collect()
}

#[given("its checkpoints go to the S3 store")]
async fn checkpoints_in_s3(w: &mut Brrrrr) {
    w.s3 = Some(format!("s3://checkpoints/it-{}-{}", std::process::id(), w.id.0));
}

#[given("its checkpoints go to the S3 store through a proxy")]
async fn checkpoints_in_s3_through_a_proxy(w: &mut Brrrrr) {
    checkpoints_in_s3(w).await;
    let s3 = std::env::var("BRRRRR_IT_S3").expect("@object-store scenarios need an S3 endpoint in BRRRRR_IT_S3");
    w.proxy = Some(Proxy::start(s3.trim_start_matches("http://").to_string()).await);
}

/// A metric of the scenario's first process, as it says it now.
fn metric(w: &Brrrrr, name: &str) -> f64 {
    let text = get(w, "/metrics");
    let value = text.lines().find_map(|l| l.strip_prefix(name)?.strip_prefix(' '));
    value.and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("no {name} in {text}"))
}

/// From now on the store gets no request of the scenario's first process: its checkpoint writes
/// hang. Once it has read every trade produced: a source's first record writes what a start
/// without a checkpoint withholds to the store, on the data thread, and held it would stop the
/// instance. What the store already had in full is answered first.
#[when("the S3 store holds every request")]
async fn store_holds(w: &mut Brrrrr) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while metric(w, "brrrrr_received_events_total") < w.produced as f64 {
        assert!(Instant::now() < deadline, "{} trades not read: {}", w.produced, w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    w.proxy.as_ref().expect("a proxy").hold.send_replace(true);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    w.checkpoints_when_held = Some(metric(w, "brrrrr_checkpoints_total") as u64);
}

#[when("the S3 store answers again")]
async fn store_answers(w: &mut Brrrrr) {
    w.proxy.as_ref().expect("a proxy").hold.send_replace(false);
}

#[then("no checkpoint was written while the S3 store held its requests")]
async fn none_written_while_held(w: &mut Brrrrr) {
    let held = w.checkpoints_when_held.expect("the store held its requests");
    assert_eq!(metric(w, "brrrrr_checkpoints_total") as u64, held, "{}", w.log.lock().unwrap());
}

#[then(expr = "within {int} seconds it writes a checkpoint again")]
async fn checkpoints_again(w: &mut Brrrrr, secs: u64) {
    let held = w.checkpoints_when_held.expect("the store held its requests");
    let deadline = Instant::now() + Duration::from_secs(secs);
    while metric(w, "brrrrr_checkpoints_total") as u64 <= held {
        assert!(Instant::now() < deadline, "no checkpoint: {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// How many messages `topic` holds.
fn count(w: &Brrrrr, topic: &str) -> usize {
    let consumer: BaseConsumer = sink_client(w, &[]).create().unwrap();
    let md = consumer.fetch_metadata(Some(topic), Duration::from_secs(5)).unwrap();
    let parts = md.topics()[0].partitions().iter().map(|p| p.id());
    let ends = parts.map(|p| consumer.fetch_watermarks(topic, p, Duration::from_secs(5)).unwrap());
    ends.map(|(low, high)| high - low).sum::<i64>() as usize
}

/// Every window the trades produced so far close, the closing trade among them, is in the topic
/// already, without waiting for anything more.
#[then(regex = r#"^"([^"]+)" already holds every window those trades close$"#)]
async fn already_holds(w: &mut Brrrrr, topic: String) {
    let want = expected(w.sql.as_ref().unwrap(), w.produced, w.priceless).len();
    let got = count(w, &topic);
    assert!(got >= want, "{got} of {want} windows: {}", w.log.lock().unwrap());
}

#[then(regex = r#"^"([^"]+)" holds fewer windows than those trades close$"#)]
async fn holds_fewer(w: &mut Brrrrr, topic: String) {
    let want = expected(w.sql.as_ref().unwrap(), w.produced, w.priceless).len();
    let got = count(w, &topic);
    assert!(got < want, "all {want} windows are there: {}", w.log.lock().unwrap());
}

#[when("the closing trade is produced, its windows not waited for")]
async fn closing_trade_unawaited(w: &mut Brrrrr) {
    produce(w, &codec(w.sql.as_ref().unwrap()), &[closing(w.produced)]);
    w.trades = w.produced;
    tokio::time::sleep(Duration::from_secs(3)).await;
}

#[when("brrrrr is sent SIGTERM")]
async fn sent_sigterm(w: &mut Brrrrr) {
    let pid = w.child.as_ref().unwrap().id().unwrap().to_string();
    assert!(std::process::Command::new("kill").args(["-TERM", &pid]).status().unwrap().success());
}

#[then(expr = "within {int} seconds it stops cleanly after a last checkpoint")]
async fn stops_cleanly_within(w: &mut Brrrrr, secs: u64) {
    let mut child = w.child.take().unwrap();
    let status = tokio::time::timeout(Duration::from_secs(secs), child.wait()).await;
    w.stopped = Some(status.unwrap_or_else(|_| panic!("still running: {}", w.log.lock().unwrap())).unwrap());
    stopped_cleanly(w).await;
}

#[given(expr = "brrrrr runs with {string}")]
async fn runs_with(w: &mut Brrrrr, args: String) {
    w.extra.extend(args.split_whitespace().map(String::from));
}

/// What a checkpoint costs the data loop: its cut copies the state, and the rest (the
/// sinks' acks, encoding, the write) happens on a thread of its own while the loop goes on. The
/// metrics of at least 3 checkpoints of a large state (`brrrrr_checkpoint_bytes`), each read
/// once it is written: the time it held the loop up is less than a quarter of its duration.
#[then(expr = "its checkpoints of over {int} MB held the data loop up for less than a quarter of their duration")]
async fn stalls_are_a_fraction(w: &mut Brrrrr, mb: u64) {
    let (deadline, mut seen) = (Instant::now() + Duration::from_secs(60), std::collections::BTreeMap::new());
    while seen.len() < 3 {
        assert!(Instant::now() < deadline, "{} checkpoints of over {mb} MB: {seen:?}", seen.len());
        let n = metric(w, "brrrrr_checkpoints_total") as u64;
        let bytes = metric(w, "brrrrr_checkpoint_bytes");
        let (stall, total) =
            (metric(w, "brrrrr_checkpoint_stall_seconds"), metric(w, "brrrrr_checkpoint_duration_seconds"));
        if bytes > (mb << 20) as f64 && metric(w, "brrrrr_checkpoints_total") as u64 == n {
            seen.insert(n, (stall, total, bytes));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for (n, (stall, total, bytes)) in &seen {
        assert!(
            stall * 4.0 < *total,
            "checkpoint {n} of {bytes} bytes held the loop up {stall}s of {total}s: {seen:?}"
        );
    }
}

#[when(expr = "the topic {string} grows to {int} partitions")]
async fn grows(w: &mut Brrrrr, topic: String, n: usize) {
    // a checkpoint may land while the partitions are created: the one after the growth counts
    w.checkpoints_before = Some(metric(w, "brrrrr_checkpoints_total"));
    let admin: AdminClient<_> = client(w, &[]).create().unwrap();
    let r = admin.create_partitions(&[rdkafka::admin::NewPartitions::new(&topic, n)], &AdminOptions::new()).await;
    assert!(r.unwrap().iter().all(|t| t.is_ok()), "growing {topic}");
    visible(w, &topic, n).await;
}

/// Waits until the scenario's first process has written another checkpoint (within 30 s): one
/// after the step began, or after a source grew if one did just before.
#[when("brrrrr writes a checkpoint")]
async fn writes_a_checkpoint(w: &mut Brrrrr) {
    let n = w.checkpoints_before.take().unwrap_or_else(|| metric(w, "brrrrr_checkpoints_total"));
    let deadline = Instant::now() + Duration::from_secs(30);
    while metric(w, "brrrrr_checkpoints_total") <= n {
        assert!(Instant::now() < deadline, "no checkpoint: {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The volume's directory becomes a file: every checkpoint write fails, as when the volume is
/// full or its storage away (permissions would not stop root). Its checkpoints are kept aside.
#[when("its checkpoints cannot be written")]
async fn checkpoints_unwritable(w: &mut Brrrrr) {
    let dir = w.checkpoints.clone().unwrap();
    std::fs::rename(&dir, dir.with_extension("aside")).unwrap();
    std::fs::write(&dir, b"").unwrap();
}

#[when("its checkpoints can be written again")]
async fn checkpoints_writable(w: &mut Brrrrr) {
    let dir = w.checkpoints.clone().unwrap();
    std::fs::remove_file(&dir).unwrap();
    std::fs::rename(dir.with_extension("aside"), &dir).unwrap();
}

#[when("its volume is emptied")]
async fn volume_emptied(w: &mut Brrrrr) {
    let dir = w.checkpoints.as_ref().unwrap();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        std::fs::remove_dir_all(e.path()).unwrap();
    }
}

#[when("its newest checkpoint is corrupted")]
async fn corrupt_newest(w: &mut Brrrrr) {
    let files = checkpoint_files(w);
    let newest = files.last().expect("a checkpoint");
    let mut bytes = std::fs::read(newest).unwrap();
    bytes.truncate(bytes.len() / 2);
    std::fs::write(newest, bytes).unwrap();
}

#[when(expr = "brrrrr is started again on {string} from its oldest checkpoint")]
async fn restarted_oldest(w: &mut Brrrrr, path: String) {
    let files = checkpoint_files(w);
    assert!(files.len() >= 2, "only {} checkpoints", files.len());
    let oldest = files[0].file_stem().unwrap().to_str().unwrap().parse::<u64>().unwrap();
    restarted_with(w, path, format!("--restore-epoch {oldest}")).await;
    let want = format!("restored checkpoint {oldest}\n");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !w.log.lock().unwrap().contains(&want) {
        assert!(Instant::now() < deadline, "{want:?} not in {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[when(expr = "a shadow brrrrr is started on {string} with the sink prefix {string}")]
async fn shadow(w: &mut Brrrrr, path: String, prefix: String) {
    let own = std::mem::replace(&mut w.prefix, prefix);
    w.second = Some(start_on(w, &path, 1, 1));
    w.prefix = own;
}

#[when(expr = "two brrrrr are started together on {string}")]
async fn together(w: &mut Brrrrr, path: String) {
    w.sql = Some(std::fs::read_to_string(root().join(&path)).unwrap());
    w.child = Some(start(w, &path, 1));
    w.second = Some(start_on(w, &path, 1, 1));
}

#[when(expr = "two brrrrr are started together on {string}, one of them changed")]
async fn together_changed(w: &mut Brrrrr, path: String) {
    w.sql = Some(std::fs::read_to_string(root().join(&path)).unwrap());
    let changed = changed_sql(w, &path);
    w.child = Some(start(w, &path, 1));
    w.second = Some(start_on(w, &changed, 1, 1));
}

#[when(expr = "brrrrr is started on {string} with {string}")]
async fn started_with(w: &mut Brrrrr, path: String, args: String) {
    w.sql = Some(std::fs::read_to_string(root().join(&path)).unwrap());
    w.started_at = now_us();
    let n = w.extra.len();
    w.extra.extend(args.split_whitespace().map(String::from));
    w.child = Some(start(w, &path, 1));
    w.extra.truncate(n);
}

#[when(expr = "a second brrrrr is started on {string}")]
async fn second(w: &mut Brrrrr, path: String) {
    // Standby scenarios need an existing owner; simultaneous claims are tested by `together`.
    log_says(w, "claimed the pipeline".into()).await;
    w.second = Some(start_on(w, &path, 1, 1));
}

/// The scenario's SQL with a comment added, under the same file name (the same pipeline): its
/// checkpoints are keyed by its text, its lease is the pipeline's.
fn changed_sql(w: &Brrrrr, path: &str) -> String {
    let dir = std::env::temp_dir().join(format!("brrrrr-it-sql-{}-{}", std::process::id(), w.id.0));
    std::fs::create_dir_all(&dir).unwrap();
    let changed = dir.join(std::path::Path::new(path).file_name().unwrap());
    std::fs::write(&changed, format!("{}\n-- changed\n", w.sql.as_ref().unwrap())).unwrap();
    changed.to_str().unwrap().to_string()
}

#[when(expr = "the SQL file {string} changes and brrrrr restarts on it")]
async fn sql_changes(w: &mut Brrrrr, path: String) {
    let changed = changed_sql(w, &path);
    w.log.lock().unwrap().clear();
    w.child = Some(start(w, &changed, 1));
    tokio::time::sleep(Duration::from_secs(8)).await;
}

#[when(expr = "a second brrrrr is started on a changed {string}")]
async fn second_changed(w: &mut Brrrrr, path: String) {
    log_says(w, "claimed the pipeline".into()).await;
    let changed = changed_sql(w, &path);
    w.second = Some(start_on(w, &changed, 1, 1));
}

#[when(expr = "a second brrrrr is started on {string} with {string}")]
async fn second_with(w: &mut Brrrrr, path: String, args: String) {
    log_says(w, "claimed the pipeline".into()).await;
    let n = w.extra.len();
    w.extra.extend(args.split_whitespace().map(String::from));
    w.second = Some(start_on(w, &path, 1, 1));
    w.extra.truncate(n);
}

#[then("it stopped cleanly after a last checkpoint")]
async fn stopped_cleanly(w: &mut Brrrrr) {
    assert!(w.stopped.unwrap().success(), "exit {:?}: {}", w.stopped, w.log.lock().unwrap());
    assert!(w.log.lock().unwrap().contains("stopping after checkpoint"), "{}", w.log.lock().unwrap());
}

#[then(expr = "its log says {string} never")]
async fn log_never(w: &mut Brrrrr, needle: String) {
    assert!(!w.log.lock().unwrap().contains(&needle), "{needle:?} in {}", w.log.lock().unwrap());
}

#[then(expr = "its log says {string}")]
async fn log_says(w: &mut Brrrrr, needle: String) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !w.log.lock().unwrap().contains(&needle) {
        assert!(Instant::now() < deadline, "{needle:?} not in {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The scenario's processes, together, have said `needle` at least `n` times (within 20 s).
#[then(expr = "its log says {string} {int} times")]
async fn log_says_times(w: &mut Brrrrr, needle: String, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while w.log.lock().unwrap().matches(&needle).count() < n {
        assert!(Instant::now() < deadline, "{needle:?} not {n} times in {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Whichever lists the checkpoints first claims; the other either finds the claim and stands by
/// or loses the race to create the same epoch and stops.
#[then("exactly one of them claimed the pipeline")]
async fn exactly_one_claimed(w: &mut Brrrrr) {
    tokio::time::sleep(Duration::from_secs(3)).await;
    let log = w.log.lock().unwrap().clone();
    assert_eq!(log.matches("claimed the pipeline").count(), 1, "{log}");
    assert!(log.contains("standing by") || log.contains("another instance is running this pipeline"), "{log}");
}

#[then(expr = "within {int} seconds it stops with {string}")]
async fn it_stops(w: &mut Brrrrr, secs: u64, needle: String) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(status) = w.child.as_mut().unwrap().try_wait().unwrap() {
            assert!(!status.success(), "it should fail: {}", w.log.lock().unwrap());
            assert!(w.log.lock().unwrap().contains(&needle), "{needle:?} not in {}", w.log.lock().unwrap());
            return;
        }
        assert!(Instant::now() < deadline, "it kept running: {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[then(expr = "within {int} seconds one of them stops with {string}")]
async fn one_stops(w: &mut Brrrrr, secs: u64, needle: String) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let a = w.child.as_mut().unwrap().try_wait().unwrap();
        let b = w.second.as_mut().unwrap().try_wait().unwrap();
        if let Some(status) = a.or(b) {
            assert!(!status.success(), "it should fail: {}", w.log.lock().unwrap());
            assert!(w.log.lock().unwrap().contains(&needle), "{needle:?} not in {}", w.log.lock().unwrap());
            return;
        }
        assert!(Instant::now() < deadline, "both kept running: {}", w.log.lock().unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// With several source partitions, which rows are late depends on arrival order, so the windows are not fixed; exactly-once still is.
#[then(regex = r#"^"([^"]+)" holds each window at most once, all of them windows the engine emits$"#)]
async fn at_most_once(w: &mut Brrrrr, topic: String) {
    let want: std::collections::HashSet<String> =
        expected(w.sql.as_ref().unwrap(), w.trades, w.priceless).into_iter().map(|(k, _)| k).collect();
    let got = read_all(w, &topic);
    let mut keys: Vec<&String> = got.iter().map(|(k, _)| k).collect();
    keys.sort();
    assert!(keys.windows(2).all(|p| p[0] != p[1]), "a window was written twice");
    assert!(got.iter().all(|(k, _)| want.contains(k)), "a window the engine never emits");
    assert!(got.len() * 2 > want.len(), "only {} of {} windows: the scenario proves little", got.len(), want.len());
}

fn now_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64
}

/// Produces rows with their records stamped with the trades' own times (column 5, µs), as the
/// raw producers stamp them, so a source's oldest record says when its oldest trade was.
fn produce_stamped(w: &Brrrrr, codec: &Encoder, rows: &[Vec<Value>]) {
    let producer: BaseProducer = client(w, &[("linger.ms", "5")]).create().unwrap();
    for r in rows {
        let mut payload = vec![];
        codec.encode(r, &mut payload);
        let ms = r[5].i64().unwrap() / 1000;
        while let Err((e, _)) =
            producer.send(BaseRecord::<(), _>::to("raw.trades.test").payload(&payload).timestamp(ms))
        {
            assert!(format!("{e}").contains("full"), "{e}");
            producer.poll(Duration::from_millis(10));
        }
    }
    producer.flush(Duration::from_secs(30)).unwrap();
}

/// Trades timed from `minutes` ago until now, as a source that has been receiving them that
/// long holds them: what a start without a checkpoint replays.
#[given(expr = "trades every {int} seconds from {int} minutes ago until now are waiting in the source")]
async fn recent_trades(w: &mut Brrrrr, every: i64, minutes: i64) {
    recent_trades_of(w, every, minutes, "tests/acceptance/sql/windows.sql".into()).await;
}

#[given(expr = "trades every {int} seconds from {int} minutes ago until now are waiting in the source of {string}")]
async fn recent_trades_of(w: &mut Brrrrr, every: i64, minutes: i64, path: String) {
    let sql = std::fs::read_to_string(root().join(path)).unwrap();
    let (now, from) = (now_us(), now_us() - minutes * 60_000_000);
    // mid-second, so the first trade is not at a window's start
    let rows: Vec<_> = (0..)
        .map(|i| from + 500_000 + i * every * 1_000_000)
        .take_while(|t| *t < now)
        .enumerate()
        .map(|(i, t)| trade_at(i, t))
        .collect();
    produce_stamped(w, &codec(&sql), &rows);
    (w.sql, w.produced, w.recent) = (Some(sql), rows.len(), rows);
}

/// Retention deleted the source's first `n` trades: its log starts after them.
#[given(expr = "the source's first {int} trades are gone with its retention")]
async fn retention_deleted(w: &mut Brrrrr, n: usize) {
    let admin: AdminClient<_> = client(w, &[]).create().unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset("raw.trades.test", 0, Offset::Offset(n as i64)).unwrap();
    let r = admin.delete_records(&tpl, &AdminOptions::new()).await.unwrap();
    assert!(r.elements().iter().all(|e| e.error().is_ok()), "deleting records: {r:?}");
    w.recent.drain(..n);
}

/// Retention deleted the source's records before offset `n`, after brrrrr read them or not.
#[when(expr = "the source lost its records before offset {int} to retention")]
async fn source_retention(w: &mut Brrrrr, n: i64) {
    let admin: AdminClient<_> = client(w, &[]).create().unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset("raw.trades.test", 0, Offset::Offset(n)).unwrap();
    let r = admin.delete_records(&tpl, &AdminOptions::new()).await.unwrap();
    assert!(r.elements().iter().all(|e| e.error().is_ok()), "deleting records: {r:?}");
}

/// Retention deleted every record each partition of a sink topic holds.
#[when(expr = "{string} lost every record it holds to retention")]
async fn sink_emptied(w: &mut Brrrrr, topic: String) {
    let consumer: BaseConsumer = sink_client(w, &[]).create().unwrap();
    let md = consumer.fetch_metadata(Some(&topic), Duration::from_secs(10)).unwrap();
    let mut tpl = TopicPartitionList::new();
    for p in md.topics()[0].partitions() {
        let (_, high) = consumer.fetch_watermarks(&topic, p.id(), Duration::from_secs(10)).unwrap();
        tpl.add_partition_offset(&topic, p.id(), Offset::Offset(high)).unwrap();
    }
    let admin: AdminClient<_> = sink_client(w, &[]).create().unwrap();
    let r = admin.delete_records(&tpl, &AdminOptions::new()).await.unwrap();
    assert!(r.elements().iter().all(|e| e.error().is_ok()), "deleting records: {r:?}");
}

#[then(regex = r#"^"([^"]+)" holds exactly the engine's messages for the recent trades$"#)]
async fn holds_recent_whole(w: &mut Brrrrr, topic: String) {
    let mut engine = Engine::new(&brrrrr_core::sql::parse(w.sql.as_ref().unwrap()).unwrap()).unwrap();
    let mut out = vec![];
    engine.insert("trades_source", w.recent.clone(), &mut out);
    let mut want: Vec<(String, String)> = out
        .into_iter()
        .map(|e| (e.headers.into_iter().find(|h| h.0 == "redpanda-dedup-key").unwrap().1, e.payload))
        .collect();
    assert!(want.len() > 20, "the scenario proves little with {} windows", want.len());
    // the last windows close on the clock (--close-after-ms): written seconds after the trades
    settle(w, Some(&topic), want.len()).await;
    let mut got = read_all(w, &topic);
    want.sort();
    got.sort();
    let missing: Vec<_> = want.iter().filter(|m| got.binary_search(m).is_err()).take(3).collect();
    let extra: Vec<_> = got.iter().filter(|m| want.binary_search(m).is_err()).take(3).collect();
    assert!(
        got == want,
        "{} messages, {} wanted; missing {missing:?}, extra {extra:?}{}",
        got.len(),
        want.len(),
        diagnosis(w)
    );
}

/// Trades timed from `from` minutes ago until `until` minutes ago, stamped with their times.
fn trades_between(w: &mut Brrrrr, every: i64, from: i64, until: i64) {
    if w.sql.is_none() {
        w.sql = Some(std::fs::read_to_string(root().join("tests/acceptance/sql/windows.sql")).unwrap());
    }
    let (start, end) = (now_us() - from * 60_000_000, now_us() - until * 60_000_000);
    let first = w.recent.len();
    let rows: Vec<_> = (0..)
        .map(|i| start + 500_000 + i * every * 1_000_000)
        .take_while(|t| *t < end)
        .enumerate()
        .map(|(i, t)| trade_at(first + i, t))
        .collect();
    produce_stamped(w, &codec(w.sql.as_ref().unwrap()), &rows);
    w.produced += rows.len();
    w.recent.extend(rows);
}

/// Live consumption of two partitions once the source is read. Partition 0 delivers a
/// trade whose time closes every window so far. Partition 1, at its end, delivers a trade of the
/// last of those windows only `secs` later, as a fetch that is late does: its record is the older
/// one. Then the trade that closes every window. The records are stamped now, as a producer's.
#[when(expr = "partition 1 delivers a trade {int} seconds after partition 0 delivered the one that closes its window")]
async fn late_partition(w: &mut Brrrrr, secs: u64) {
    let read = format!("\nbrrrrr_received_events_total {}\n", w.produced);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !get(w, "/metrics").contains(&read) {
        assert!(Instant::now() < deadline, "{read:?} not in {}", get(w, "/metrics"));
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await; // both partitions' ends are reported
    let codec = codec(w.sql.as_ref().unwrap());
    let producer: BaseProducer = client(w, &[]).create().unwrap();
    let send = |row: &[Value], partition: i32, ms: i64| {
        let mut payload = vec![];
        codec.encode(row, &mut payload);
        let record = BaseRecord::<(), _>::to("raw.trades.test").payload(&payload).partition(partition).timestamp(ms);
        producer.send(record).map_err(|(e, _)| e).unwrap();
        producer.flush(Duration::from_secs(30)).unwrap();
    };
    let (n, now) = (w.recent.len(), now_us());
    let (older, newer) = (trade_at(n, now - 1_000_000), trade_at(n + 1, now + 61_000_000));
    send(&newer, 0, now / 1000);
    tokio::time::sleep(Duration::from_secs(secs)).await;
    send(&older, 1, now / 1000 - 1000);
    let closing = trade_at(n + 2, now + 240_000_000);
    send(&closing, 0, now_us() / 1000);
    w.recent.extend([older, newer, closing]);
    w.produced += 3;
    let read = format!("\nbrrrrr_received_events_total {}\n", w.produced);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !get(w, "/metrics").contains(&read) {
        assert!(Instant::now() < deadline, "{read:?} not in {}", get(w, "/metrics"));
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await; // its output reaches the sink
}

/// brrrrr has read what it needs at start (the sources' and sinks' oldest records) and claimed
/// the pipeline: what it found then is what it went by.
#[when("brrrrr has claimed the pipeline")]
async fn has_claimed(w: &mut Brrrrr) {
    claimed(w, 1).await;
}

#[when(expr = "trades every {int} seconds from {int} minutes ago until now are produced")]
async fn trades_produced(w: &mut Brrrrr, every: i64, from: i64) {
    trades_between(w, every, from, 0);
}

/// Trades stamped with the clock, as a live feed stamps them (capture time), round-robin over the topic's two
/// partitions every `every` ms, until `before` seconds before the end of a minute at least 20 s away, then no
/// more: the minute is left quiet. A back-dated trade proves nothing about a clock close, which closes windows by
/// the clock: only a row stamped now can be late for it.
#[when(expr = "live trades are produced every {int} ms on both partitions until {int} second before a minute ends")]
async fn live_trades(w: &mut Brrrrr, every: u64, before: i64) {
    let sql = w.sql.clone().expect("brrrrr is started on a SQL");
    let codec = codec(&sql);
    let now = now_us();
    let minute = 60_000_000;
    let mut end = (now / minute + 1) * minute;
    if end - now < 20_000_000 {
        end += minute;
    }
    let stop = end - before * 1_000_000;
    let producer: BaseProducer = client(w, &[("linger.ms", "1")]).create().unwrap();
    let mut i = w.recent.len();
    while now_us() < stop {
        let row = trade_at(i, now_us());
        let mut payload = vec![];
        codec.encode(&row, &mut payload);
        let ms = row[5].i64().unwrap() / 1000;
        let record =
            BaseRecord::<(), _>::to("raw.trades.test").payload(&payload).partition((i % 2) as i32).timestamp(ms);
        producer.send(record).map_err(|(e, _)| e).unwrap();
        producer.poll(Duration::ZERO);
        w.recent.push(row);
        i += 1;
        tokio::time::sleep(Duration::from_millis(every)).await;
    }
    producer.flush(Duration::from_secs(30)).unwrap();
    (w.produced, w.live_minute_end) = (w.recent.len(), end);
}

/// The windows of the minute that ended at `w.live_minute_end`, as the engine writes them once it closes: the
/// trades inserted, then the clock past the minute.
fn live_minute_expected(w: &Brrrrr) -> Vec<(String, String)> {
    let mut engine = Engine::new(&brrrrr_core::sql::parse(w.sql.as_ref().unwrap()).unwrap()).unwrap();
    let mut out = vec![];
    engine.insert("trades_source", w.recent.clone(), &mut out);
    engine.close_until(w.live_minute_end + 5_000_000, &mut out);
    let started = (w.live_minute_end - 60_000_000).to_string();
    out.into_iter()
        .map(|e| (e.headers.into_iter().find(|h| h.0 == "redpanda-dedup-key").unwrap().1, e.payload))
        .filter(|(key, _)| key.split('|').next() == Some(started.as_str()))
        .collect()
}

fn live_minute_in(w: &Brrrrr, topic: &str) -> Vec<(String, String)> {
    let started = (w.live_minute_end - 60_000_000).to_string();
    let mut got: Vec<_> =
        read_all(w, topic).into_iter().filter(|(key, _)| key.split('|').next() == Some(started.as_str())).collect();
    got.sort();
    got
}

/// The quiet minute's windows are in the sink within `secs` seconds of its end, and are the engine's.
#[then(regex = r#"^the quiet minute reaches "([^"]+)" within (\d+) seconds of its end, as the engine writes it$"#)]
async fn quiet_minute_arrives(w: &mut Brrrrr, topic: String, secs: u64) {
    let mut want = live_minute_expected(w);
    want.sort();
    assert!(want.len() >= 5, "the scenario proves little with {} windows", want.len());
    let deadline = std::time::UNIX_EPOCH + Duration::from_micros(w.live_minute_end as u64) + Duration::from_secs(secs);
    let mut got = live_minute_in(w, &topic);
    while got.len() < want.len() && std::time::SystemTime::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        got = live_minute_in(w, &topic);
    }
    assert_eq!(got.len(), want.len(), "{} of {} windows within {secs} s of the minute's end", got.len(), want.len());
    assert_eq!(got, want);
}

/// The control: without a clock close the quiet minute stays open.
#[then(regex = r#"^the quiet minute is not in "([^"]+)" (\d+) seconds after its end$"#)]
async fn quiet_minute_stays_open(w: &mut Brrrrr, topic: String, secs: u64) {
    let at = std::time::UNIX_EPOCH + Duration::from_micros(w.live_minute_end as u64) + Duration::from_secs(secs);
    if let Ok(left) = at.duration_since(std::time::SystemTime::now()) {
        tokio::time::sleep(left).await;
    }
    let got = live_minute_in(w, &topic);
    assert!(got.is_empty(), "{} windows of the quiet minute without a clock close, e.g. {:?}", got.len(), got.first());
}

/// A trade two minutes from now closes every window of the recent trades.
#[when("the recent trades close their windows")]
async fn recent_closing(w: &mut Brrrrr) {
    let closing = trade_at(w.recent.len(), now_us() + 120_000_000);
    produce_stamped(w, &codec(w.sql.as_ref().unwrap()), std::slice::from_ref(&closing));
    w.recent.push(closing);
    // what it waits for does not matter much: a shortfall only makes settle wait its 90 s out
    tokio::time::sleep(Duration::from_secs(5)).await;
    settle(w, None, 20).await;
}

/// What brrrrr withheld, as its newest checkpoint holds it once no source is awaited any more
/// (µs): the windows that began before the first value, and per sink topic those closed before
/// its bound.
fn withhold_bounds(w: &Brrrrr) -> (i64, HashMap<String, i64>) {
    // the claim's checkpoint holds them, and the first after a source's first record its start
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let newest = checkpoint_files(w).pop().and_then(|f| Checkpoint::decode(&std::fs::read(f).ok()?).ok());
        match newest.and_then(|c| c.withhold) {
            Some(h) if h.awaiting.is_empty() => return (h.start, h.topics.into_iter().collect()),
            _ if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(200)),
            h => panic!("no withholding bounds in a checkpoint: {h:?}"),
        }
    }
}

/// The engine's messages for the recent trades, withholding what brrrrr withheld; and how many
/// window groups that withheld.
fn recent_expected(w: &Brrrrr) -> (Vec<(String, String)>, u64) {
    let mut engine = Engine::new(&brrrrr_core::sql::parse(w.sql.as_ref().unwrap()).unwrap()).unwrap();
    let (start, _) = withhold_bounds(w);
    engine.withhold(start);
    let mut out = vec![];
    engine.insert("trades_source", w.recent.clone(), &mut out);
    let messages = out
        .into_iter()
        .map(|e| (e.headers.into_iter().find(|h| h.0 == "redpanda-dedup-key").unwrap().1, e.payload))
        .collect();
    (messages, engine.withheld())
}

#[then(
    regex = r#"^"([^"]+)" holds exactly the engine's messages for the recent trades, withholding what brrrrr withheld$"#
)]
async fn holds_recent(w: &mut Brrrrr, topic: String) {
    let (mut want, withheld) = recent_expected(w);
    assert!(withheld > 0 && want.len() > 20, "the scenario proves little: {withheld} withheld, {} written", want.len());
    let mut got = read_all(w, &topic);
    want.sort();
    got.sort();
    assert_eq!(got.windows(2).filter(|p| p[0].0 == p[1].0).count(), 0, "a window was written twice");
    let stale: Vec<_> = got.iter().filter(|x| want.binary_search(x).is_err()).take(3).collect();
    assert!(
        stale.is_empty(),
        "{} windows written that should have been withheld, e.g. {stale:?}",
        got.len().saturating_sub(want.len())
    );
    assert_eq!(got, want);
}

#[then(expr = "its metrics count withheld {word} windows")]
async fn metrics_count_withheld(w: &mut Brrrrr, reason: String) {
    let metrics = get(w, "/metrics");
    let n = metrics.lines().find_map(|l| l.strip_prefix(&format!("brrrrr_withheld_{reason}_windows_total ")));
    assert!(n.and_then(|n| n.parse::<f64>().ok()).is_some_and(|n| n > 0.0), "{metrics}");
}

#[then("brrrrr withheld the windows that began before the first trade")]
async fn withheld_partial(w: &mut Brrrrr) {
    let (start, _) = withhold_bounds(w);
    // the first record's timestamp: its trade's time, to the millisecond
    let first = w.recent[0][5].i64().unwrap() / 1000 * 1000;
    assert_eq!(start, first, "withheld the windows that began before {start}, the first trade was at {first}");
}

/// The width of the windows a sink topic holds, from its interval suffix (`test.v1.test.5m`).
fn topic_width_us(topic: &str) -> i64 {
    match topic.rsplit('.').next().unwrap() {
        "1m" => 60_000_000,
        "5m" => 300_000_000,
        other => panic!("{topic}: no width for {other:?}"),
    }
}

/// A window message's end (µs): its `time` (the window's start) plus its topic's width.
fn window_end(topic: &str, payload: &str) -> i64 {
    let at = payload.find("\"time\":").unwrap_or_else(|| panic!("no time in {payload}")) + 7;
    let digits: String = payload[at..].chars().take_while(|c| c.is_ascii_digit() || *c == '-').collect();
    digits.parse::<i64>().unwrap() + topic_width_us(topic)
}

/// The engine's windows of the recent trades (the closing one included, once produced) on `topic`.
fn recent_windows(w: &Brrrrr, topic: &str) -> Vec<(String, String)> {
    let mut engine = Engine::new(&brrrrr_core::sql::parse(w.sql.as_ref().unwrap()).unwrap()).unwrap();
    let mut out = vec![];
    engine.insert("trades_source", w.recent.clone(), &mut out);
    out.into_iter()
        .filter(|e| &*e.topic == topic)
        .map(|e| (e.headers.into_iter().find(|h| h.0 == "redpanda-dedup-key").unwrap().1, e.payload))
        .collect()
}

/// What an earlier run wrote live: each of the recent trades' windows, stamped when it closed,
/// but for the one that began `skip` minutes ago, if any, which it never wrote.
fn prefill(w: &Brrrrr, topic: &str, skip: Option<i64>) {
    let width = topic_width_us(topic);
    let skipped = skip.map(|m| {
        let at = now_us() - m * 60_000_000;
        at - at.rem_euclid(width)
    });
    let producer: BaseProducer = sink_client(w, &[]).create().unwrap();
    let mut written = 0;
    for (key, payload) in recent_windows(w, topic) {
        let end = window_end(topic, &payload);
        if Some(end - width) == skipped {
            continue;
        }
        let headers = rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header { key: "redpanda-dedup-key", value: Some(&key) });
        let rec = BaseRecord::<(), _>::to(topic).payload(&payload).headers(headers).timestamp(end / 1000);
        producer.send(rec).map_err(|(e, _)| e).unwrap();
        written += 1;
    }
    producer.flush(Duration::from_secs(30)).unwrap();
    assert!(written > 20, "{topic}: only {written} windows");
}

#[given(expr = "{string} already holds the windows the recent trades close, written as they closed")]
async fn prefilled(w: &mut Brrrrr, topic: String) {
    prefill(w, &topic, None);
}

#[given(
    expr = "{string} already holds the windows the recent trades close but the one from {int} minutes ago, written as they closed"
)]
async fn prefilled_but_one(w: &mut Brrrrr, topic: String, minutes: i64) {
    prefill(w, &topic, Some(minutes));
}

/// Retention deleted the sink topic's records written more than `minutes` ago.
#[given(expr = "{string} lost its records older than {int} minutes to retention")]
async fn sink_retention(w: &mut Brrrrr, topic: String, minutes: i64) {
    let cut_ms = (now_us() - minutes * 60_000_000) / 1000;
    let consumer: BaseConsumer = sink_client(w, &[("group.id", "brrrrr-it-cut")]).create().unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::Offset(cut_ms)).unwrap();
    let at = consumer.offsets_for_times(tpl, Duration::from_secs(10)).unwrap();
    let Offset::Offset(offset) = at.elements()[0].offset() else { panic!("{topic}: nothing after the cut") };
    assert!(offset > 0, "{topic}: nothing to delete before the cut");
    let admin: AdminClient<_> = sink_client(w, &[]).create().unwrap();
    let mut del = TopicPartitionList::new();
    del.add_partition_offset(&topic, 0, Offset::Offset(offset)).unwrap();
    let r = admin.delete_records(&del, &AdminOptions::new()).await.unwrap();
    assert!(r.elements().iter().all(|e| e.error().is_ok()), "deleting records: {r:?}");
}

#[then(regex = r#"^"([^"]+)" holds each window once, none that closed before it lost its records$"#)]
async fn holds_verified(w: &mut Brrrrr, topic: String) {
    let (_, topics) = withhold_bounds(w);
    let bound = *topics.get(&topic).unwrap_or_else(|| panic!("{topic}: brrrrr bounded nothing on it: {topics:?}"));
    let mut want: Vec<_> =
        recent_windows(w, &topic).into_iter().filter(|(_, p)| window_end(&topic, p) >= bound).collect();
    let mut got = read_all(w, &topic);
    let old: Vec<_> = got.iter().filter(|(_, p)| window_end(&topic, p) < bound).take(3).collect();
    assert!(old.is_empty(), "{topic}: windows closed before it lost its records were written again: {old:?}");
    want.sort();
    got.sort();
    assert_eq!(got, want);
}

#[then(regex = r#"^"([^"]+)" holds each of the engine's windows for the recent trades once$"#)]
async fn holds_every_recent(w: &mut Brrrrr, topic: String) {
    let (_, topics) = withhold_bounds(w);
    assert!(!topics.contains_key(&topic), "{topic} lost no record, yet brrrrr bounded it");
    let mut want = recent_windows(w, &topic);
    let mut got = read_all(w, &topic);
    want.sort();
    got.sort();
    assert_eq!(got, want);
}

/// A shadow (`--sink-topic-prefix`) runs `path` over `n` trades and stops with SIGTERM, leaving
/// its checkpoints as a pod's volume holds them when its instance is switched from shadow.
#[when(expr = "a shadow brrrrr with the sink prefix {string} runs {string} over {int} trades and stops")]
async fn shadow_run(w: &mut Brrrrr, prefix: String, path: String, n: usize) {
    w.sql = Some(std::fs::read_to_string(root().join(&path)).unwrap());
    let own = std::mem::replace(&mut w.prefix, prefix);
    w.child = Some(start(w, &path, 1));
    w.prefix = own;
    more_trades(w, n).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    sigterm(w).await;
}

/// Adopting a shadow's checkpoints: they are copied where the live instance of the pipeline
/// keeps its own.
#[when("its shadow's checkpoints are adopted")]
async fn adopt(w: &mut Brrrrr) {
    let dir = w.checkpoints.clone().unwrap();
    let shadow =
        std::fs::read_dir(&dir).unwrap().flatten().map(|e| e.path()).find(|p| p.to_string_lossy().contains('@'));
    let shadow = shadow.expect("the shadow's checkpoints");
    let own = dir.join(shadow.file_name().unwrap().to_string_lossy().split('@').next().unwrap());
    let status = std::process::Command::new("cp").arg("-a").arg(&shadow).arg(&own).status().unwrap();
    assert!(status.success());
}

#[when(expr = "the sink topic {string} is deleted")]
async fn delete_sink_topic(w: &mut Brrrrr, name: String) {
    let admin: AdminClient<_> = sink_client(w, &[]).create().unwrap();
    let r = admin.delete_topics(&[&name], &AdminOptions::new()).await;
    assert!(r.unwrap().iter().all(|t| t.is_ok()), "deleting {name}");
}

/// None of the windows the first `n` trades closed: those the shadow wrote to its own topic.
#[then(regex = r#"^"([^"]+)" holds none of the windows the shadow wrote over (\d+) trades$"#)]
async fn none_of_the_shadows(w: &mut Brrrrr, topic: String, n: usize) {
    // the windows the first n trades close by themselves, without the closing trade
    let closed_by_trades: std::collections::HashSet<String> = {
        let mut engine = Engine::new(&brrrrr_core::sql::parse(w.sql.as_ref().unwrap()).unwrap()).unwrap();
        let mut out = vec![];
        engine.insert("trades_source", (0..n).map(trade).collect(), &mut out);
        out.into_iter().map(|e| e.headers.into_iter().find(|h| h.0 == "redpanda-dedup-key").unwrap().1).collect()
    };
    let again: Vec<_> = read_all(w, &topic).into_iter().filter(|(k, _)| closed_by_trades.contains(k)).take(3).collect();
    assert!(closed_by_trades.len() > 100, "the scenario proves little with {} windows", closed_by_trades.len());
    assert!(again.is_empty(), "windows the shadow wrote before the switch were written again: {again:?}");
}

// steps block on librdkafka and std::net: enough threads that concurrent scenarios keep time
#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() {
    // cargo-nextest (the mutants and coverage jobs) lists every test binary with `--list`: this
    // one is cucumber, not a libtest harness, and lists no tests of its own
    if std::env::args().any(|a| a == "--list") {
        return;
    }
    let features = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/acceptance/features");
    let concurrency = std::env::var("BRRRRR_IT_CONCURRENCY").map_or(8, |n| n.parse().expect("a number"));
    Brrrrr::cucumber().max_concurrent_scenarios(concurrency).fail_on_skipped().run_and_exit(features).await;
}
