//! Broker cursors and brrrrr checkpoints have different commit boundaries.
#![cfg(feature = "iggy")]
use brrrrr_core::{
    checkpoint::Checkpoint,
    proto::{parse_proto, Codec},
    value::Value,
};
use iggy::prelude::*;
use rdkafka::{
    consumer::{BaseConsumer, Consumer as KafkaConsumer},
    message::Message,
    ClientConfig, Offset, TopicPartitionList,
};
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Broker {
    rt: tokio::runtime::Runtime,
    client: IggyClient,
    connection: String,
    name: String,
    stream: Identifier,
    topic: Identifier,
    stream_cleaned: bool,
}
impl Broker {
    fn request<F: std::future::Future>(&self, future: F) -> F::Output {
        self.rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(15), future).await.expect("Iggy fixture request timed out")
        })
    }
    fn new(label: &str, partitions: u32) -> Self {
        let connection = std::env::var("BRRRRR_IT_IGGY").expect("set BRRRRR_IT_IGGY");
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let client = IggyClient::from_connection_string(&connection).unwrap();
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let name = format!("brrrrr-durable-{label}-{}-{nonce}", std::process::id());
        let (stream, topic) = rt.block_on(async {
            client.connect().await.unwrap();
            let stream = Identifier::numeric(client.create_stream(&name).await.unwrap().id).unwrap();
            let topic = client
                .create_topic(
                    &stream,
                    "trades.exchange",
                    &TopicCreateOptions {
                        partitions_count: Some(partitions),
                        segment_size: Some(8_388_608.into()),
                        durability: Durability::Persisted,
                        consumer_offset_durability: Durability::Persisted,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            (stream, Identifier::numeric(topic.id).unwrap())
        });
        Self { rt, client, connection, name, stream, topic, stream_cleaned: false }
    }
    fn poll(&self, consumer: &iggy::prelude::Consumer, strategy: PollingStrategy, count: u32) -> PolledMessages {
        self.request(self.client.poll_messages(&self.stream, &self.topic, Some(0), consumer, &strategy, count, false))
            .unwrap()
    }
    fn stored(&self, consumer: &iggy::prelude::Consumer) -> Option<u64> {
        self.request(self.client.get_consumer_offset(consumer, &self.stream, &self.topic, Some(0)))
            .unwrap()
            .map(|o| o.stored_offset)
    }
    fn store(&self, consumer: &iggy::prelude::Consumer, offset: u64) {
        self.request(self.client.store_consumer_offset(consumer, &self.stream, &self.topic, Some(0), offset)).unwrap();
    }
    fn send(&self, partition: u32, mut messages: Vec<IggyMessage>) {
        self.request(self.client.send_messages(
            &self.stream,
            &self.topic,
            &Partitioning::partition_id(partition),
            &mut messages,
        ))
        .unwrap();
    }
}
impl Drop for Broker {
    fn drop(&mut self) {
        self.rt.block_on(async {
            if !self.stream_cleaned {
                let deleted =
                    tokio::time::timeout(Duration::from_secs(5), self.client.delete_stream(&self.stream)).await;
                if !matches!(deleted, Ok(Ok(()))) {
                    eprintln!("failed to remove isolated fixture stream {}: {deleted:?}", self.name);
                }
            }
            let _ = tokio::time::timeout(Duration::from_secs(2), self.client.shutdown()).await;
        });
    }
}
fn named(name: &str) -> iggy::prelude::Consumer {
    iggy::prelude::Consumer::new(Identifier::named(name).unwrap())
}
fn numeric(id: u32) -> iggy::prelude::Consumer {
    iggy::prelude::Consumer::new(Identifier::numeric(id).unwrap())
}
fn offsets(reply: &PolledMessages) -> Vec<u64> {
    reply.messages.iter().map(|m| m.header.offset).collect()
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY, writable Iggy TCP broker"]
fn persisted_named_offsets_survive_client_identity_changes_and_do_not_commit_on_fetch() {
    let b = Broker::new("cursor", 1);
    b.send(0, (0..8).map(|n| IggyMessage::from(format!("record-{n}"))).collect());
    let info = b.request(b.client.get_topic(&b.stream, &b.topic)).unwrap().unwrap();
    for key in ["durability", "consumer_offset_durability"] {
        let value = &info.options[&HeaderKey::try_from(key).unwrap()];
        assert_eq!(value.value.as_str().unwrap(), "persisted");
        assert!(value.explicit, "{key} is an explicitly selected creation policy");
    }
    let message_only = b
        .request(b.client.create_topic(
            &b.stream,
            "messages-only",
            &TopicCreateOptions {
                partitions_count: Some(1),
                segment_size: Some(8_388_608.into()),
                durability: Durability::Persisted,
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        message_only.options[&HeaderKey::try_from("consumer_offset_durability").unwrap()].value.as_str().unwrap(),
        "replicated",
        "message persistence does not imply offset persistence"
    );
    for key in ["durability", "consumer_offset_durability"] {
        let update = TopicUpdateOptions {
            raw: [(key.to_string(), "replicated".to_string())].into_iter().collect(),
            ..Default::default()
        };
        assert!(
            b.request(b.client.update_topic(&b.stream, &b.topic, "trades.exchange", &update)).is_err(),
            "{key} cannot be weakened in place under existing data/cursors"
        );
    }
    let a = named("worker-a");
    let other = named("worker-b");
    assert_eq!(offsets(&b.poll(&a, PollingStrategy::offset(0), 8)), (0..8).collect::<Vec<_>>());
    assert_eq!(b.stored(&a), None, "fetching every record does not persist unprocessed work");
    b.store(&a, 2);
    b.store(&other, 5);
    assert_eq!(offsets(&b.poll(&a, PollingStrategy::next(), 8)), (3..8).collect::<Vec<_>>());
    assert_eq!(b.stored(&a), Some(2), "poll(false) does not move the durable cursor");
    assert_eq!(offsets(&b.poll(&other, PollingStrategy::next(), 8)), vec![6, 7]);
    assert_eq!(b.stored(&other), Some(5), "independent named consumers have independent cursors");
    let fresh = IggyClient::from_connection_string(&b.connection).unwrap();
    b.request(async {
        fresh.connect().await.unwrap();
        assert_eq!(
            fresh
                .get_consumer_offset(&named("worker-a"), &b.stream, &b.topic, Some(0))
                .await
                .unwrap()
                .unwrap()
                .stored_offset,
            2
        );
        let reply = fresh
            .poll_messages(&b.stream, &b.topic, Some(0), &named("worker-a"), &PollingStrategy::next(), 8, false)
            .await
            .unwrap();
        assert_eq!(
            offsets(&reply),
            (3..8).collect::<Vec<_>>(),
            "a new connection resumes explicit stored work, not the last fetch"
        );
        fresh.delete_consumer_offset(&a, &b.stream, &b.topic, Some(0)).await.unwrap();
        assert!(fresh.get_consumer_offset(&a, &b.stream, &b.topic, Some(0)).await.unwrap().is_none());
        fresh.shutdown().await.unwrap();
    });
    assert_eq!(b.stored(&other), Some(5), "deleting one cursor cannot delete another consumer's position");
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Pipeline {
    path: PathBuf,
    state: PathBuf,
    log: PathBuf,
    address: String,
    sink: String,
    broker: String,
    codec: Codec,
    base: i64,
}
impl Pipeline {
    fn new(b: &Broker, latest: bool) -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let path = std::env::temp_dir().join(&b.name);
        fs::create_dir_all(&path).unwrap();
        let sink = format!("{}.1m", b.name);
        let broker = std::env::var("BRRRRR_IT_BROKER").expect("set BRRRRR_IT_BROKER");
        let mut sql = fs::read_to_string(root.join("tests/acceptance/sql/windows.sql"))
            .unwrap()
            .replace("raw.trades.test", &format!("{}.trades.exchange", b.name))
            .replace("test.1m", &sink);
        if latest {
            sql = sql.replace("seek_to = 'earliest'", "seek_to = 'latest'");
        }
        fs::write(path.join("runtime.sql"), &sql).unwrap();
        let cat = brrrrr_core::sql::parse(&sql).unwrap();
        let schema = parse_proto(&fs::read_to_string(root.join("fixtures/market.proto")).unwrap()).unwrap();
        let cols =
            cat.streams["trades_source"].columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect::<Vec<_>>();
        let codec = Codec::new(&schema["Trade"], &cols);
        let admin: rdkafka::admin::AdminClient<_> =
            ClientConfig::new().set("bootstrap.servers", &broker).create().unwrap();
        b.request(async {
            let result = admin
                .create_topics(
                    &[rdkafka::admin::NewTopic::new(&sink, 1, rdkafka::admin::TopicReplication::Fixed(1))],
                    &rdkafka::admin::AdminOptions::new(),
                )
                .await
                .unwrap();
            assert!(result.iter().all(Result::is_ok), "{result:?}");
        });
        let address = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        let base = (now - 300_000_000) / 60_000_000 * 60_000_000;
        Self { state: path.join("checkpoints"), log: path.join("stderr"), path, address, sink, broker, codec, base }
    }
    fn send(&self, b: &Broker, partition: u32, start: usize, end: usize) {
        b.send(
            partition,
            (start..end)
                .map(|i| {
                    let t = self.base + i as i64 * 30_000_000;
                    let row = vec![
                        Value::Int(t),
                        Value::Str(i.to_string().into()),
                        Value::Int(1),
                        Value::Str("BTCUSDT".into()),
                        Value::F64(100.0 + i as f64),
                        Value::Int(t),
                        Value::Str("buy".into()),
                        Value::F64(1.0),
                        Value::F64(100.0 + i as f64),
                    ];
                    let mut bytes = vec![];
                    self.codec.encode(&row, &mut bytes);
                    let mut m = IggyMessage::from(bytes);
                    m.header.origin_timestamp = t as u64;
                    m
                })
                .collect(),
        );
    }
    fn start(&self, b: &Broker) -> Process {
        self.start_with_source_kafka_config(b, None)
    }
    fn start_with_source_kafka_config(&self, b: &Broker, source_config: Option<&str>) -> Process {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_brrrrr"));
        cmd.arg("run")
            .arg(self.path.join("runtime.sql"))
            .arg("--proto")
            .arg(root.join("fixtures/market.proto"))
            .args([
                "--brokers",
                &self.broker,
                "--metrics",
                &self.address,
                "--interval",
                "1",
                "--takeover",
                "3",
                "--partial-windows",
                "--checkpoints",
            ])
            .arg(&self.state)
            .env("BRRRRR_IGGY", &b.connection)
            .stdout(Stdio::null())
            .stderr(fs::File::create(&self.log).unwrap());
        if let Some(source_config) = source_config {
            cmd.args(["--source-kafka-config", source_config]);
        }
        Process(cmd.spawn().unwrap())
    }
    fn wait_for(&self, child: &mut Process, predicate: impl Fn(&str) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let metrics = self.metrics();
            if predicate(&metrics) {
                return;
            }
            assert!(child.0.try_wait().unwrap().is_none(), "{}", fs::read_to_string(&self.log).unwrap());
            assert!(Instant::now() < deadline, "{metrics}\n{}", fs::read_to_string(&self.log).unwrap());
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn metrics(&self) -> String {
        let Ok(mut stream) = TcpStream::connect_timeout(&self.address.parse().unwrap(), Duration::from_millis(100))
        else {
            return String::new();
        };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
        stream.write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n").unwrap();
        let mut result = String::new();
        let _ = stream.read_to_string(&mut result);
        result
    }
    fn wait_received(&self, child: &mut Process, n: usize) {
        self.wait_for(child, |m| m.contains(&format!("brrrrr_received_events_total {n}\n")));
    }
    fn wait_checkpoint(&self, child: &mut Process, total: i64) -> (PathBuf, Checkpoint) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(c) = checkpoints(&self.state)
                .into_iter()
                .rev()
                .find(|(_, c)| c.sources.iter().map(|(_, _, o)| *o).sum::<i64>() == total)
            {
                return c;
            }
            assert!(child.0.try_wait().unwrap().is_none(), "{}", fs::read_to_string(&self.log).unwrap());
            assert!(Instant::now() < deadline, "no processed checkpoint at {total}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn stop(&self, child: &mut Process, success: bool) {
        if child.0.try_wait().unwrap().is_none() {
            assert!(Command::new("kill").args(["-TERM", &child.0.id().to_string()]).status().unwrap().success());
        }
        self.exit(child, success);
    }
    fn exit(&self, child: &mut Process, success: bool) {
        let started = Instant::now();
        let deadline = started + Duration::from_secs(20);
        let mut captured = false;
        loop {
            if !captured && started.elapsed() > Duration::from_secs(3) {
                captured = true;
                let mut report = String::new();
                if let Ok(tasks) = fs::read_dir(format!("/proc/{}/task", child.0.id())) {
                    for task in tasks.flatten() {
                        for file in ["comm", "wchan", "stack"] {
                            report.push_str(&format!(
                                "{}: {}\n",
                                task.path().join(file).display(),
                                fs::read_to_string(task.path().join(file)).unwrap_or_default()
                            ));
                        }
                    }
                }
                let path = format!("/tmp/brrrrr-iggy-durability-shutdown-{}.txt", child.0.id());
                fs::write(&path, report).unwrap();
                eprintln!("slow teardown evidence: {path}");
            }
            if let Some(status) = child.0.try_wait().unwrap() {
                assert_eq!(status.success(), success, "{}", fs::read_to_string(&self.log).unwrap());
                return;
            }
            assert!(Instant::now() < deadline, "process did not stop: {}", fs::read_to_string(&self.log).unwrap());
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn output(&self) -> Vec<Vec<u8>> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.broker)
            .set("group.id", &self.sink)
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        let (_, end) = consumer.fetch_watermarks(&self.sink, 0, Duration::from_secs(10)).unwrap();
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.sink, 0, Offset::Beginning).unwrap();
        consumer.assign(&tpl).unwrap();
        let mut out = vec![];
        let deadline = Instant::now() + Duration::from_secs(10);
        while out.len() < end as usize {
            assert!(Instant::now() < deadline);
            if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(50)) {
                out.push(msg.payload().unwrap().to_vec());
            }
        }
        out
    }
}
impl Drop for Pipeline {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
fn checkpoints(dir: &Path) -> Vec<(PathBuf, Checkpoint)> {
    let mut out = vec![];
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(checkpoints(&path));
            } else if path.extension().is_some_and(|e| e == "ckpt") {
                if let Ok(bytes) = fs::read(&path) {
                    if let Ok(c) = Checkpoint::decode(&bytes) {
                        out.push((path, c));
                    }
                }
            }
        }
    }
    out.sort_by_key(|(_, c)| c.epoch);
    out
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY and BRRRRR_IT_BROKER"]
fn pipeline_checkpoint_overrides_latest_and_an_ahead_broker_cursor() {
    let b = Broker::new("latest", 1);
    let p = Pipeline::new(&b, true);
    let mut child = p.start(&b);
    p.wait_for(&mut child, |_| {
        fs::read_to_string(&p.log).unwrap_or_default().contains("claimed the pipeline with checkpoint")
    });
    p.send(&b, 0, 0, 1);
    p.wait_received(&mut child, 1);
    p.wait_checkpoint(&mut child, 1);
    p.stop(&mut child, true);
    drop(child);
    assert_eq!(b.stored(&numeric(1)), None, "brrrrr never auto-commits fetched rows on the broker");
    p.send(&b, 0, 1, 4);
    b.store(&numeric(1), 3);
    let mut child = p.start(&b);
    p.wait_received(&mut child, 3);
    p.stop(&mut child, true);
    drop(child);
    assert_eq!(b.stored(&numeric(1)), Some(3), "brrrrr leaves externally managed broker cursors untouched");
    let checkpoint = checkpoints(&p.state).pop().unwrap().1;
    assert_eq!(checkpoint.sources[0].2, 4, "processed next offset is paired with engine state");
    let out = p.output();
    assert_eq!(out.len(), 1, "exactly one closed bar, no missing or duplicate window");
    let expected = format!("{{\"symbol\":\"BTCUSDT\",\"time\":{},\"n\":2,\"last\":101,\"mean\":100.5}}\n", p.base);
    assert_eq!(
        out[0],
        expected.as_bytes(),
        "the saved open-window row combines with replay rather than being skipped by latest/broker offset"
    );
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY and BRRRRR_IT_BROKER"]
fn a_removed_checkpoint_partition_refuses_recovery() {
    let b = Broker::new("shrink", 2);
    let p = Pipeline::new(&b, false);
    p.send(&b, 0, 0, 1);
    p.send(&b, 1, 0, 1);
    let mut child = p.start(&b);
    p.wait_received(&mut child, 2);
    p.wait_checkpoint(&mut child, 2);
    p.stop(&mut child, true);
    drop(child);
    assert_eq!(checkpoints(&p.state).last().unwrap().1.sources.len(), 2);
    b.request(b.client.delete_partitions(&b.stream, &b.topic, 1)).unwrap();
    let before = checkpoints(&p.state).last().unwrap().1.epoch;
    let mut child = p.start(&b);
    p.exit(&mut child, false);
    let log = fs::read_to_string(&p.log).unwrap();
    assert!(log.contains("checkpoint") && log.contains("partition"), "{log}");
    assert_eq!(
        checkpoints(&p.state).last().unwrap().1.epoch,
        before,
        "a deleted source cannot claim replacement state with missing history"
    );
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY and BRRRRR_IT_BROKER"]
fn live_purge_and_shorter_replacement_history_stop_the_pipeline() {
    let b = Broker::new("purge", 1);
    let p = Pipeline::new(&b, false);
    p.send(&b, 0, 0, 4);
    let mut child = p.start(&b);
    p.wait_received(&mut child, 4);
    p.wait_checkpoint(&mut child, 4);
    b.request(b.client.purge_topic(&b.stream, &b.topic)).unwrap();
    let empty = b.request(b.client.get_topic(&b.stream, &b.topic)).unwrap().unwrap();
    assert_eq!((empty.partitions[0].current_offset, empty.partitions[0].messages_count), (0, 0));
    p.send(&b, 0, 0, 1);
    let replaced = b.request(b.client.get_topic(&b.stream, &b.topic)).unwrap().unwrap();
    assert_eq!(
        (replaced.partitions[0].current_offset, replaced.partitions[0].messages_count),
        (0, 1),
        "current_offset zero is ambiguous without message count"
    );
    p.exit(&mut child, false);
    let log = fs::read_to_string(&p.log).unwrap();
    assert!(
        log.contains("Iggy") && (log.contains("truncat") || log.contains("end") || log.contains("retained")),
        "{log}"
    );
    assert_eq!(b.stored(&numeric(1)), None, "no cursor commit can hide lost unprocessed history");
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY and BRRRRR_IT_BROKER"]
fn a_reader_without_poll_permission_fails_without_consuming_or_claiming_a_checkpoint() {
    let mut b = Broker::new("auth", 1);
    let p = Pipeline::new(&b, false);
    p.send(&b, 0, 0, 1);
    let username = format!("denied-reader-{}", std::process::id());
    let user = b
        .request(b.client.create_user(
            &username,
            "source-test-password",
            UserStatus::Active,
            Some(Permissions {
                global: GlobalPermissions { read_servers: true, ..Default::default() },
                ..Default::default()
            }),
        ))
        .unwrap();
    let denied = format!("iggy+tcp://{username}:source-test-password@{}", b.connection.rsplit('@').next().unwrap());
    let client = IggyClient::from_connection_string(&denied).unwrap();
    b.request(async {
        client.connect().await.unwrap();
        let err = client
            .poll_messages(&b.stream, &b.topic, Some(0), &numeric(1), &PollingStrategy::offset(0), 1, false)
            .await
            .unwrap_err();
        assert_eq!(err, IggyError::Unauthorized);
        client.shutdown().await.unwrap();
    });
    b.connection = denied;
    let mut child = p.start(&b);
    p.exit(&mut child, false);
    assert!(checkpoints(&p.state).is_empty(), "unauthorized reads cannot claim recoverable progress");
    assert!(p.output().is_empty(), "unauthorized source data produces no Kafka windows");
    let log = fs::read_to_string(&p.log).unwrap();
    assert!(log.contains("Unauthorized") || log.contains("unauthorized"), "{log}");
    b.request(b.client.delete_user(&Identifier::numeric(user.id).unwrap())).unwrap();
}

/// Restore only the explicitly supplied disposable broker, even if a crash assertion fails.
struct RestartBroker(String);
impl Drop for RestartBroker {
    fn drop(&mut self) {
        match Command::new("docker").args(["start", &self.0]).output() {
            Ok(out) if out.status.success() => {}
            other => eprintln!("failed to restore owned broker {}: {other:?}", self.0),
        }
    }
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY and BRRRRR_IT_IGGY_CONTAINER for an owned disposable broker"]
fn persisted_messages_and_named_cursor_survive_broker_sigkill_without_graceful_close() {
    let container = std::env::var("BRRRRR_IT_IGGY_CONTAINER")
        .expect("set BRRRRR_IT_IGGY_CONTAINER to the disposable broker created for this test job");
    assert!(!container.starts_with("iggy-nightly-"), "shared root regression brokers must not be stopped by this test");
    let mut b = Broker::new("crash", 1);
    let expected: Vec<Vec<u8>> = (0..8).map(|i| vec![0, 0xfe, 0xff, i]).collect();
    b.send(0, expected.iter().cloned().map(IggyMessage::from).collect());
    let worker = named("durable-restarting-worker");
    b.store(&worker, 2);
    let restore = RestartBroker(container.clone());
    // Both send/store completed under their own persisted policies. Neither the old client nor
    // the server is gracefully shut down before this signal, so a final flush cannot rescue them.
    let killed = Command::new("docker").args(["kill", "--signal", "KILL", &container]).output().unwrap();
    assert!(killed.status.success(), "broker SIGKILL setup failed: {}", String::from_utf8_lossy(&killed.stderr));
    let started = Command::new("docker").args(["start", &container]).output().unwrap();
    assert!(started.status.success(), "broker restart setup failed: {}", String::from_utf8_lossy(&started.stderr));
    let fresh = IggyClient::from_connection_string(&b.connection).unwrap();
    b.request(async {
        fresh.connect().await.unwrap();
        assert_eq!(
            fresh
                .get_consumer_offset(&named("durable-restarting-worker"), &b.stream, &b.topic, Some(0))
                .await
                .unwrap()
                .unwrap()
                .stored_offset,
            2,
            "acknowledged explicit stored cursor survives a process crash"
        );
        let reply = fresh
            .poll_messages(&b.stream, &b.topic, Some(0), &worker, &PollingStrategy::next(), 8, false)
            .await
            .unwrap();
        assert_eq!(offsets(&reply), (3..8).collect::<Vec<_>>());
        assert_eq!(
            reply.messages.into_iter().map(|m| m.payload.to_vec()).collect::<Vec<_>>(),
            expected[3..],
            "persisted acknowledged payloads, not merely offsets, survive without graceful flush"
        );
        let all = fresh
            .poll_messages(
                &b.stream,
                &b.topic,
                Some(0),
                &named("independent-after-crash"),
                &PollingStrategy::offset(0),
                8,
                false,
            )
            .await
            .unwrap();
        assert_eq!(all.messages.into_iter().map(|m| m.payload.to_vec()).collect::<Vec<_>>(), expected);
        fresh.delete_stream(&b.stream).await.unwrap();
        fresh.shutdown().await.unwrap();
    });
    // Cleanup uses the new live transport, never the old connection's stale socket.
    b.stream_cleaned = true;
    drop(restore);
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY and BRRRRR_IT_BROKER"]
fn iggy_sources_ignore_invalid_kafka_source_configuration_while_kafka_sinks_work() {
    let b = Broker::new("source-config", 1);
    let p = Pipeline::new(&b, false);
    p.send(&b, 0, 0, 4);
    let mut child = p.start_with_source_kafka_config(&b, Some("security.protocol=not-a-kafka-protocol"));
    p.wait_received(&mut child, 4);
    p.stop(&mut child, true);
    drop(child);
    let ignored_file = format!("@{}", p.path.join("nonexistent-source-kafka.conf").display());
    let mut child = p.start_with_source_kafka_config(&b, Some(&ignored_file));
    p.wait_for(&mut child, |_| {
        fs::read_to_string(&p.log).unwrap_or_default().contains("claimed the pipeline with checkpoint")
    });
    p.wait_received(&mut child, 0);
    p.stop(&mut child, true);
    let checkpoint = checkpoints(&p.state).pop().unwrap().1;
    assert_eq!(
        checkpoint.sources[0].2, 4,
        "Iggy never creates/configures a Kafka source client or reads source-only config files"
    );
    let expected = format!("{{\"symbol\":\"BTCUSDT\",\"time\":{},\"n\":2,\"last\":101,\"mean\":100.5}}\n", p.base);
    assert_eq!(
        p.output(),
        vec![expected.into_bytes()],
        "valid Kafka sink configuration still delivers the complete window"
    );
}
