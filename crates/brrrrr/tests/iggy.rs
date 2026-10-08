//! End-to-end Iggy input → protobuf decoder (fixtures/market.proto) → engine → Kafka output → checkpoint recovery.
#![cfg(feature = "iggy")]
use brrrrr_core::{
    checkpoint::Checkpoint,
    engine::Engine,
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
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn metrics(address: &str) -> String {
    let Ok(mut stream) = TcpStream::connect_timeout(&address.parse().unwrap(), Duration::from_secs(1)) else {
        return String::new();
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    let _ = stream.write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n");
    let mut out = String::new();
    let _ = stream.read_to_string(&mut out);
    out
}

fn await_received(child: &mut Process, address: &str, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(45);
    let needle = format!("brrrrr_received_events_total {n}\n");
    loop {
        let text = metrics(address);
        if text.contains(&needle) {
            return;
        }
        assert!(child.0.try_wait().unwrap().is_none(), "brrrrr exited before {n} events");
        assert!(Instant::now() < deadline, "did not receive {n}: {text}");
        std::thread::sleep(Duration::from_millis(50));
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
                    if let Ok(checkpoint) = Checkpoint::decode(&bytes) {
                        out.push((path, checkpoint));
                    }
                }
            }
        }
    }
    out.sort_by_key(|(_, c)| c.epoch);
    out
}

fn row(i: usize, base: i64) -> Vec<Value> {
    let t = base + (i / 3) as i64 * 15_000_000;
    vec![
        Value::Int(t - 5_000),
        Value::Str(i.to_string().into()),
        Value::Int(1),
        Value::Str("BTCUSDT".into()),
        Value::F64(100.0 + (i / 3) as f64),
        Value::Int(t),
        Value::Str("buy".into()),
        Value::F64(1.0),
        Value::F64(100.0 + (i / 3) as f64),
    ]
}

#[test]
#[ignore = "requires BRRRRR_IT_IGGY and BRRRRR_IT_BROKER, writable Iggy and Kafka brokers"]
fn iggy_source_engine_output_survives_crash_restore_and_graceful_restart() {
    let connection = std::env::var("BRRRRR_IT_IGGY").expect("set BRRRRR_IT_IGGY");
    let broker = std::env::var("BRRRRR_IT_BROKER").expect("set BRRRRR_IT_BROKER");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let temp = std::env::temp_dir().join(format!("brrrrr-iggy-runtime-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&temp).unwrap();
    let name = format!("brrrrr-runtime-{}-{nonce}", std::process::id());
    let source = format!("{name}.trades.exchange");
    let sink = format!("{name}.1m");
    let sql = fs::read_to_string(root.join("tests/acceptance/sql/windows.sql"))
        .unwrap()
        .replace("raw.trades.test", &source)
        .replace("test.1m", &sink);
    let sql_path = temp.join("runtime.sql");
    fs::write(&sql_path, &sql).unwrap();
    let cat = brrrrr_core::sql::parse(&sql).unwrap();
    let proto = fs::read_to_string(root.join("fixtures/market.proto")).unwrap();
    let fields = parse_proto(&proto).unwrap();
    let cols: Vec<_> = cat.streams["trades_source"].columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect();
    let codec = Codec::new(&fields["Trade"], &cols);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64;
    let base = (now + 120_000_000) / 60_000_000 * 60_000_000;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let client = IggyClient::from_connection_string(&connection).unwrap();
    let (stream, topic) = rt.block_on(async {
        client.connect().await.unwrap();
        let stream = client.create_stream(&name).await.unwrap();
        let stream_id = Identifier::numeric(stream.id).unwrap();
        let topic = client
            .create_topic(
                &stream_id,
                "trades.exchange",
                &TopicCreateOptions {
                    partitions_count: Some(3),
                    segment_size: Some(8_388_608.into()),
                    durability: Durability::Persisted,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let admin: rdkafka::admin::AdminClient<_> =
            ClientConfig::new().set("bootstrap.servers", &broker).create().unwrap();
        let result = admin
            .create_topics(
                &[rdkafka::admin::NewTopic::new(&sink, 1, rdkafka::admin::TopicReplication::Fixed(1))],
                &rdkafka::admin::AdminOptions::new(),
            )
            .await
            .unwrap();
        assert!(result.iter().all(Result::is_ok), "creating test sink: {result:?}");
        (stream_id, Identifier::numeric(topic.id).unwrap())
    });
    let publish = |from: usize, to: usize| {
        rt.block_on(async {
            for p in 0..3 {
                let mut messages = vec![];
                for i in (from..to).filter(|i| i % 3 == p as usize) {
                    let r = row(i, base);
                    let mut bytes = vec![];
                    codec.encode(&r, &mut bytes);
                    let mut m = IggyMessage::from(bytes);
                    m.header.origin_timestamp = r[5].i64().unwrap() as u64;
                    messages.push(m);
                }
                client.send_messages(&stream, &topic, &Partitioning::partition_id(p), &mut messages).await.unwrap();
            }
        })
    };
    let addr = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
    let state = temp.join("checkpoints");
    let start = |restore: Option<u64>| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_brrrrr"));
        cmd.args(["run"])
            .arg(&sql_path)
            .args(["--proto"])
            .arg(root.join("fixtures/market.proto"))
            .args(["--brokers", &broker, "--checkpoints"])
            .arg(&state)
            .args([
                "--metrics",
                &addr,
                "--interval",
                "1",
                "--takeover",
                "3",
                "--max-drift",
                "0",
                "--partition-wait-ms",
                "0",
                "--partial-windows",
            ])
            .env("BRRRRR_IGGY", &connection)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if let Some(epoch) = restore {
            cmd.args(["--restore-epoch", &epoch.to_string()]);
        }
        Process(cmd.spawn().unwrap())
    };
    publish(0, 39);
    let mut child = start(None);
    await_received(&mut child, &addr, 39);
    let deadline = Instant::now() + Duration::from_secs(10);
    let checkpoint = loop {
        if let Some((_, c)) =
            checkpoints(&state).into_iter().find(|(_, c)| c.sources.iter().map(|(_, _, o)| *o).sum::<i64>() == 39)
        {
            break c.epoch;
        }
        assert!(Instant::now() < deadline, "no checkpoint for 39 inputs");
        std::thread::sleep(Duration::from_millis(50));
    };
    publish(39, 78);
    await_received(&mut child, &addr, 78);
    // Kill with sink messages after the selected checkpoint. Restore must suppress their replay.
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    drop(child);
    let mut child = start(Some(checkpoint));
    await_received(&mut child, &addr, 39);
    let status = Command::new("kill").args(["-TERM", &child.0.id().to_string()]).status().unwrap();
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(child);
    let positions: HashMap<_, _> =
        checkpoints(&state).last().unwrap().1.sources.iter().map(|(t, p, o)| ((t.clone(), *p), *o)).collect();
    assert_eq!(positions.values().sum::<i64>(), 78);
    assert_eq!(positions.len(), 3);
    publish(78, 117);
    let mut child = start(None);
    await_received(&mut child, &addr, 39);
    Command::new("kill").args(["-TERM", &child.0.id().to_string()]).status().unwrap();
    assert!(child.0.wait().unwrap().success());
    drop(child);
    let mut expected = vec![];
    let mut engine = Engine::new(&cat).unwrap();
    engine.insert("trades_source", (0..117).map(|i| row(i, base)).collect(), &mut expected);
    let mut expected: Vec<_> = expected.into_iter().map(|e| e.payload.into_bytes()).collect();
    expected.sort();
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("group.id", &name)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let (_, end) = consumer.fetch_watermarks(&sink, 0, Duration::from_secs(10)).unwrap();
    assert_eq!(end as usize, expected.len(), "each closed window appears exactly once after replay");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&sink, 0, Offset::Beginning).unwrap();
    consumer.assign(&tpl).unwrap();
    let mut actual = vec![];
    let deadline = Instant::now() + Duration::from_secs(10);
    while actual.len() < expected.len() {
        assert!(Instant::now() < deadline);
        if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(50)) {
            actual.push(msg.payload().unwrap().to_vec());
        }
    }
    actual.sort();
    assert_eq!(actual, expected, "source decode, alignment, checkpoint state and Kafka output match pure engine");
    rt.block_on(async {
        client.delete_stream(&stream).await.unwrap();
        client.shutdown().await.unwrap();
    });
    fs::remove_dir_all(temp).unwrap();
}
