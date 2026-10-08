//! Iggy's transport runs on one I/O thread; bounded partition queues feed the existing data loop.
//! Offsets are explicit and never committed on poll: only the runtime's checkpoints advance recovery.
#[cfg(feature = "iggy")]
use crate::align::Msg;
use crate::align::Polled;
use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Positions = HashMap<(String, i32), i64>;
type Status = HashMap<(String, i32), (i64, Instant, Option<i64>)>;

pub fn identity(connection: Option<&str>) -> Result<String> {
    let Some(connection) = connection else { return Ok(String::new()) };
    let address = connection.strip_prefix("iggy+tcp://").ok_or_else(|| anyhow!("--iggy requires iggy+tcp://"))?;
    let address = address.split('?').next().unwrap_or(address).rsplit('@').next().unwrap_or(address);
    if address.is_empty() {
        bail!("--iggy requires a server address")
    }
    Ok(format!("\n-- iggy {address}"))
}

#[cfg(any(feature = "iggy", test))]
fn split_topic(topic: &str) -> Result<(&str, &str)> {
    topic
        .split_once('.')
        .filter(|(s, t)| !s.is_empty() && !t.is_empty())
        .ok_or_else(|| anyhow!("Iggy source topic {topic:?} must be <stream>.<topic>"))
}

#[cfg(any(feature = "iggy", test))]
fn start_offset(checkpoint: Option<i64>, latest: bool, low: i64, end: i64) -> Result<u64> {
    let offset = checkpoint.unwrap_or(if latest { end } else { low });
    if offset < low || offset > end {
        bail!("Iggy checkpoint offset {offset} is outside retained range {low}..={end}");
    }
    Ok(u64::try_from(offset)?)
}

#[cfg(any(feature = "iggy", test))]
fn next_offset(from: u64, offsets: &[u64]) -> Result<u64> {
    let mut next = from;
    for &offset in offsets {
        if offset < from && next == from {
            continue;
        } // replayed batch prefix, never a row twice
        if offset != next {
            bail!("Iggy partition offset gap or reordering: expected {next}, got {offset}");
        }
        next = offset
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| anyhow!("Iggy offset exceeds checkpoint range"))?;
    }
    if !offsets.is_empty() && next == from {
        bail!("Iggy poll returned only records before requested offset {from}");
    }
    Ok(next)
}

#[cfg(any(feature = "iggy", test))]
fn empty_at_end(next: u64, current: u64, known: i64) -> bool {
    known as u64 <= next && (current.checked_add(1) == Some(next) || next == 0 && current == 0 && known == 0)
}

#[cfg(any(feature = "iggy", test))]
fn checkpoint_partitions(positions: &Positions, assignment: &[(String, i32)]) -> Result<()> {
    for (topic, partition) in positions.keys() {
        if !assignment.iter().any(|(t, p)| t == topic && p == partition) {
            bail!("Iggy checkpoint partition {topic}/{partition} no longer exists; refusing to skip its input");
        }
    }
    Ok(())
}

#[cfg(any(feature = "iggy", test))]
fn observed_end(current: u64, after: u64, known: i64) -> Result<i64> {
    let high = i64::try_from(current)?.checked_add(1).ok_or_else(|| anyhow!("Iggy offset exceeds checkpoint range"))?;
    if high < known {
        bail!("Iggy partition end regressed from {known} to {high}; refusing to resume a reset or stale log");
    }
    if (high as u64) < after {
        bail!("Iggy poll delivered through {after} beyond its advertised end {high}");
    }
    Ok(high)
}

pub struct Reader {
    pub parts: Vec<(String, i32)>,
    pub starts: Positions,
    pub ends: Positions,
    pub origins: Vec<(String, i32, i64, Option<i64>)>,
    queues: Vec<Mutex<tokio::sync::mpsc::Receiver<Polled>>>,
    status: Arc<Mutex<Status>>,
    counts: Arc<Mutex<HashMap<String, usize>>>,
    error: Arc<Mutex<Option<String>>>,
    errors: Arc<std::sync::atomic::AtomicU64>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Reader {
    #[cfg(not(feature = "iggy"))]
    pub fn start(_: &str, _: Vec<(String, bool)>, _: Positions) -> Result<Self> {
        bail!("--iggy needs a binary built with --features iggy")
    }

    pub fn poll(&self, i: usize) -> Result<Polled> {
        if let Some(e) = self.error.lock().unwrap().as_ref() {
            bail!("Iggy source: {e}");
        }
        match self.queues[i].lock().unwrap().try_recv() {
            Ok(m) => Ok(m),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                let status = self.status.lock().unwrap();
                Ok(status
                    .get(&self.parts[i])
                    .and_then(|(_, at, proof)| (at.elapsed() < Duration::from_secs(10)).then_some(*proof).flatten())
                    .map_or(Polled::Unavailable, Polled::EofAt))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => bail!("Iggy source I/O stopped"),
        }
    }

    pub fn end(&self, topic: &str, partition: i32) -> Option<i64> {
        self.status
            .lock()
            .unwrap()
            .get(&(topic.into(), partition))
            .filter(|(_, at, _)| at.elapsed() < Duration::from_secs(10))
            .map(|(end, _, _)| *end)
    }

    /// The oldest confirmed EOF observation, absent when any partition's proof is missing or stale.
    pub fn quiet_at(&self) -> Option<i64> {
        let status = self.status.lock().unwrap();
        self.parts
            .iter()
            .map(|tp| {
                status
                    .get(tp)
                    .and_then(|(_, at, proof)| (at.elapsed() < Duration::from_secs(10)).then_some(*proof).flatten())
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .min()
    }

    pub fn failures(&self) -> u64 {
        self.errors.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn count(&self, topic: &str) -> Option<usize> {
        self.counts.lock().unwrap().get(topic).copied()
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(feature = "iggy")]
mod sdk {
    use super::*;
    use anyhow::Context;
    use iggy::prelude::*;
    const TIMEOUT: Duration = Duration::from_secs(30);

    fn transient(e: &IggyError) -> bool {
        matches!(
            e,
            IggyError::Disconnected
                | IggyError::NotConnected
                | IggyError::CannotEstablishConnection
                | IggyError::TcpError
                | IggyError::IoError(_)
                | IggyError::StaleClient
                | IggyError::TransientNotAccepted
                | IggyError::TransientNotCommitted
                | IggyError::TaskTimeout
        )
    }

    async fn metadata(client: &IggyClient, name: &str) -> Result<TopicDetails> {
        let (stream, topic) = split_topic(name)?;
        tokio::time::timeout(TIMEOUT, client.get_topic(&stream.try_into()?, &topic.try_into()?))
            .await??
            .ok_or_else(|| anyhow!("Iggy topic {name} does not exist"))
    }

    fn end(p: &Partition) -> Result<i64> {
        Ok(if p.messages_count == 0 && p.current_offset == 0 {
            0
        } else {
            i64::try_from(p.current_offset)?
                .checked_add(1)
                .ok_or_else(|| anyhow!("Iggy offset exceeds checkpoint range"))?
        })
    }

    async fn supervise(
        mut tasks: tokio::task::JoinSet<()>,
        mut stop: tokio::sync::oneshot::Receiver<()>,
        error: Arc<Mutex<Option<String>>>,
    ) {
        tokio::select! {
            _ = &mut stop => {},
            done = tasks.join_next() => {
                let cause = match done {
                    Some(Err(e)) if e.is_panic() => "Iggy partition worker panicked",
                    Some(Err(_)) => "Iggy partition worker was cancelled unexpectedly",
                    Some(Ok(())) | None => "Iggy partition worker stopped unexpectedly",
                };
                error.lock().unwrap().get_or_insert_with(|| cause.into());
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    impl Reader {
        pub fn start(connection: &str, topics: Vec<(String, bool)>, positions: Positions) -> Result<Self> {
            identity(Some(connection))?;
            // Never include a connection string (credentials) in an error or a log.
            let client = Arc::new(
                IggyClient::from_connection_string(connection)
                    .map_err(|_| anyhow!("invalid Iggy TCP connection configuration"))?,
            );
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
            let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
            let thread = std::thread::Builder::new().name("iggy-source".into()).spawn(move || {
                let result = (|| -> Result<()> {
                    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
                    rt.block_on(async {
                        tokio::time::timeout(TIMEOUT, client.connect()).await??;
                        let mut reader = Reader {
                            parts: vec![],
                            starts: HashMap::new(),
                            ends: HashMap::new(),
                            origins: vec![],
                            queues: vec![],
                            status: Arc::default(),
                            counts: Arc::default(),
                            error: Arc::default(),
                            errors: Arc::default(),
                            stop: None,
                            thread: None,
                        };
                        let mut tasks = tokio::task::JoinSet::new();
                        for (name, latest) in topics {
                            let info = metadata(&client, &name).await?;
                            reader.counts.lock().unwrap().insert(name.clone(), info.partitions.len());
                            let (stream, topic) = split_topic(&name)?;
                            let (stream, topic): (Identifier, Identifier) = (stream.try_into()?, topic.try_into()?);
                            for p in info.partitions {
                                let partition = i32::try_from(p.id)?;
                                let tp = (name.clone(), partition);
                                let mut high = end(&p)?;
                                // First record proves whether retained history begins at zero.
                                let first = tokio::time::timeout(
                                    TIMEOUT,
                                    client.poll_messages(
                                        &stream,
                                        &topic,
                                        Some(p.id),
                                        &iggy::prelude::Consumer::new(Identifier::numeric(1)?),
                                        &PollingStrategy::first(),
                                        1,
                                        false,
                                    ),
                                )
                                .await??;
                                if first.partition_id != p.id {
                                    bail!(
                                        "{name}/{partition}: first poll replied for partition {}",
                                        first.partition_id
                                    );
                                }
                                if !first.messages.is_empty() {
                                    high = high.max(observed_end(
                                        first.current_offset,
                                        first
                                            .messages
                                            .last()
                                            .unwrap()
                                            .header
                                            .offset
                                            .checked_add(1)
                                            .ok_or_else(|| anyhow!("Iggy offset overflow"))?,
                                        0,
                                    )?);
                                }
                                let origin = first.messages.first().map(|m| {
                                    (
                                        m.header.offset as i64,
                                        i64::try_from(m.header.origin_timestamp).unwrap_or(i64::MAX),
                                    )
                                });
                                let low = origin.map_or(if p.messages_count == 0 { high } else { 0 }, |x| x.0);
                                let from = start_offset(positions.get(&tp).copied(), latest, low, high)
                                    .with_context(|| format!("{name}/{partition}"))?;
                                reader.origins.push((name.clone(), partition, low, origin.map(|x| x.1)));
                                reader.parts.push(tp.clone());
                                reader.starts.insert(tp.clone(), from as i64);
                                reader.ends.insert(tp.clone(), high);
                                reader.status.lock().unwrap().insert(tp.clone(), (high, Instant::now(), None));
                                let (tx, rx) = tokio::sync::mpsc::channel(1000);
                                reader.queues.push(Mutex::new(rx));
                                let (client, status, error, counts, errors) = (
                                    client.clone(),
                                    reader.status.clone(),
                                    reader.error.clone(),
                                    reader.counts.clone(),
                                    reader.errors.clone(),
                                );
                                let (stream, topic) = (stream.clone(), topic.clone());
                                tasks.spawn(async move {
                                    let result = consume(
                                        &client, &stream, &topic, p.id, tp, from, high, tx, status, counts, errors,
                                    )
                                    .await;
                                    if let Err(e) = result {
                                        *error.lock().unwrap() = Some(format!("{e:#}"));
                                    }
                                });
                            }
                        }
                        if reader.parts.is_empty() {
                            bail!("Iggy source has no partitions");
                        }
                        checkpoint_partitions(&positions, &reader.parts)?;
                        let error = reader.error.clone();
                        ready_tx.send(Ok(reader)).map_err(|_| anyhow!("Iggy source startup cancelled"))?;
                        supervise(tasks, stop_rx, error).await;
                        let _ = tokio::time::timeout(Duration::from_secs(2), client.shutdown()).await;
                        Ok(())
                    })
                })();
                if let Err(e) = result {
                    let _ = ready_tx.send(Err(e));
                }
            })?;
            // Each metadata call has its own deadline; the thread remains joinable on failure.
            let mut reader = match ready_rx.recv().context("Iggy source startup stopped")? {
                Ok(reader) => reader,
                Err(e) => {
                    let _ = thread.join();
                    return Err(e);
                }
            };
            reader.stop = Some(stop_tx);
            reader.thread = Some(thread);
            Ok(reader)
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume(
        client: &IggyClient,
        stream: &Identifier,
        topic: &Identifier,
        partition: u32,
        tp: (String, i32),
        mut next: u64,
        mut known_end: i64,
        tx: tokio::sync::mpsc::Sender<Polled>,
        status: Arc<Mutex<Status>>,
        counts: Arc<Mutex<HashMap<String, usize>>>,
        errors: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<()> {
        let consumer = iggy::prelude::Consumer::new(Identifier::numeric(1)?);
        let mut refreshed = Instant::now();
        loop {
            let polled_at = (Instant::now(), crate::metrics::now_ms() as i64);
            let reply = tokio::time::timeout(
                TIMEOUT,
                client.poll_messages(
                    stream,
                    topic,
                    Some(partition),
                    &consumer,
                    &PollingStrategy::offset(next),
                    1000,
                    false,
                ),
            )
            .await;
            let reply = match reply {
                Ok(Ok(reply)) => reply,
                Ok(Err(e)) if !transient(&e) => return Err(anyhow!("{}/{partition}@{next}: {e}", tp.0)),
                _ => {
                    if let Some((_, _, proof)) = status.lock().unwrap().get_mut(&tp) {
                        *proof = None;
                    }
                    let n = errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if n.is_power_of_two() {
                        eprintln!("Iggy source {}/{partition}@{next}: transient poll failure ({n}); retrying", tp.0);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if reply.partition_id != partition {
                bail!("{}/{partition}: broker replied for partition {}", tp.0, reply.partition_id);
            }
            let offsets: Vec<_> = reply.messages.iter().map(|m| m.header.offset).collect();
            let after = next_offset(next, &offsets).with_context(|| format!("{}/{partition}", tp.0))?;
            let empty = reply.messages.is_empty();
            if !empty {
                observed_end(reply.current_offset, after, known_end)
                    .with_context(|| format!("{}/{partition}@{next}", tp.0))?;
            } else if observed_end(reply.current_offset, 0, 0)? < known_end {
                // A catching-up shard may answer behind the committed metadata.
                // It cannot prove EOF. A decrease in metadata itself fences a
                // reset log; unavailable metadata leaves the proof invalid.
                status.lock().unwrap().insert(tp.clone(), (known_end, Instant::now(), None));
                match metadata(client, &tp.0).await {
                    Ok(info) => {
                        let p =
                            info.partitions.iter().find(|p| p.id == partition).ok_or_else(|| {
                                anyhow!("{}/{partition}: Iggy source partition no longer exists", tp.0)
                            })?;
                        let high = end(p)?;
                        if high < known_end {
                            bail!("{}/{partition}: Iggy partition end regressed from {known_end} to {high}", tp.0);
                        }
                        known_end = high;
                        counts.lock().unwrap().insert(tp.0.clone(), info.partitions.len());
                    }
                    Err(e)
                        if e.downcast_ref::<IggyError>().is_some_and(transient)
                            || e.downcast_ref::<tokio::time::error::Elapsed>().is_some() => {}
                    Err(e) => return Err(e),
                }
                let n = errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if n.is_power_of_two() {
                    eprintln!(
                        "Iggy source {}/{partition}@{next}: broker is behind committed end {known_end}; retrying ({n})",
                        tp.0
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            if empty {
                if !empty_at_end(next, reply.current_offset, known_end) {
                    status.lock().unwrap().insert(tp.clone(), (known_end, Instant::now(), None));
                    let n = errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if n.is_power_of_two() {
                        eprintln!("Iggy source {}/{partition}@{next}: empty poll before broker end {known_end}; retrying ({n})", tp.0);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                status.lock().unwrap().insert(tp.clone(), (next as i64, polled_at.0, Some(polled_at.1)));
            } else {
                known_end = observed_end(reply.current_offset, after, known_end)?;
                status.lock().unwrap().insert(tp.clone(), (known_end, Instant::now(), None));
                for message in reply.messages.into_iter().filter(|m| m.header.offset >= next) {
                    let msg = Msg {
                        offset: i64::try_from(message.header.offset)?,
                        payload: message.payload.to_vec(),
                        ts: i64::try_from(message.header.origin_timestamp / 1000)?,
                    };
                    if tx.send(Polled::Msg(msg)).await.is_err() {
                        return Ok(());
                    }
                }
            }
            next = after;
            if refreshed.elapsed() >= Duration::from_secs(5) {
                match metadata(client, &tp.0).await {
                    Ok(info) => {
                        if let Some(p) = info.partitions.iter().find(|p| p.id == partition) {
                            let high = end(p)?;
                            if high < known_end {
                                bail!("{}/{partition}: Iggy partition end regressed from {known_end} to {high}", tp.0);
                            }
                            known_end = high;
                        } else {
                            bail!("{}/{partition}: Iggy source partition no longer exists", tp.0);
                        }
                        counts.lock().unwrap().insert(tp.0.clone(), info.partitions.len());
                    }
                    Err(e)
                        if e.downcast_ref::<IggyError>().is_some_and(transient)
                            || e.downcast_ref::<tokio::time::error::Elapsed>().is_some() => {}
                    Err(e) => return Err(e),
                }
                refreshed = Instant::now();
            }
            if empty {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
    #[cfg(test)]
    mod live_tests {
        #[test]
        fn partition_worker_failure_is_fatal_even_if_the_merge_is_not_polling_it() {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            for panics in [false, true] {
                rt.block_on(async {
                    let (stop, receive) = tokio::sync::oneshot::channel();
                    let error = Arc::new(Mutex::new(None));
                    let mut tasks = tokio::task::JoinSet::new();
                    tasks.spawn(async move {
                        assert!(!panics, "partition worker panic");
                    });
                    tokio::time::timeout(Duration::from_secs(1), supervise(tasks, receive, error.clone()))
                        .await
                        .expect("an unexpectedly stopped worker cannot leave a healthy pipeline waiting forever");
                    assert!(error.lock().unwrap().as_ref().unwrap().contains("partition worker"));
                    drop(stop);
                });
            }
        }

        #[test]
        fn requested_shutdown_cancels_workers_without_reporting_a_source_failure() {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let (stop, receive) = tokio::sync::oneshot::channel();
                let error = Arc::new(Mutex::new(None));
                let mut tasks = tokio::task::JoinSet::new();
                tasks.spawn(std::future::pending::<()>());
                stop.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(1), supervise(tasks, receive, error.clone())).await.unwrap();
                assert!(error.lock().unwrap().is_none());
            });
        }

        #[test]
        fn reconnects_transport_faults_but_rejects_credentials_and_missing_topics() {
            use super::transient;
            use iggy::prelude::IggyError;
            for e in
                [IggyError::Disconnected, IggyError::TcpError, IggyError::TransientNotAccepted, IggyError::TaskTimeout]
            {
                assert!(transient(&e));
            }
            for e in [
                IggyError::Unauthorized,
                IggyError::InvalidCredentials,
                IggyError::InvalidIdentifier,
                IggyError::InvalidCommand,
            ] {
                assert!(!transient(&e));
            }
        }

        #[test]
        fn partition_end_is_after_the_last_message_and_zero_for_empty_partitions() {
            use super::end;
            use iggy::prelude::Partition;
            let mut p = Partition {
                id: 0,
                created_at: 0.into(),
                segments_count: 0,
                current_offset: 0,
                size: 0.into(),
                messages_count: 0,
            };
            assert_eq!(end(&p).unwrap(), 0);
            p.messages_count = 1;
            assert_eq!(end(&p).unwrap(), 1);
            p.current_offset = 42;
            assert_eq!(end(&p).unwrap(), 43);
            p.messages_count = 0;
            assert_eq!(end(&p).unwrap(), 43, "expired records do not reset the partition's offset history");
            p.messages_count = 1;
            p.current_offset = i64::MAX as u64;
            assert!(end(&p).is_err());
        }

        use super::*;
        fn collect(reader: &Reader, count: usize) -> Vec<(i32, Msg)> {
            let mut rows = vec![];
            let deadline = Instant::now() + Duration::from_secs(30);
            while rows.len() < count {
                assert!(Instant::now() < deadline, "only {} of {count} records", rows.len());
                for (i, (_, p)) in reader.parts.iter().enumerate() {
                    if let Polled::Msg(m) = reader.poll(i).unwrap() {
                        rows.push((*p, m));
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            rows
        }

        #[test]
        #[ignore = "requires BRRRRR_IT_IGGY, a writable Iggy TCP broker"]
        fn nightly_raw_bytes_partitions_checkpoint_resume_latest_and_shutdown() {
            let connection = std::env::var("BRRRRR_IT_IGGY").expect("set BRRRRR_IT_IGGY");
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let client = IggyClient::from_connection_string(&connection).unwrap();
                client.connect().await.unwrap();
                let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
                let name = format!("brrrrr-source-test-{}-{nonce}", std::process::id());
                let stream = client.create_stream(&name).await.unwrap();
                let stream_id = Identifier::numeric(stream.id).unwrap();
                let topic = client
                    .create_topic(
                        &stream_id,
                        "trade.exchange",
                        &TopicCreateOptions {
                            partitions_count: Some(3),
                            segment_size: Some(8_388_608.into()),
                            durability: Durability::Persisted,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let topic_id = Identifier::numeric(topic.id).unwrap();
                for partition in 0..3 {
                    let mut messages: Vec<IggyMessage> = (0..1005)
                        .map(|i| {
                            // Raw protobuf contains zero bytes and bytes not UTF-8: no JSON/base64 transformation.
                            let mut m =
                                IggyMessage::from(vec![0x0a, 3, 0, 0xfe, 0xff, partition as u8, (i % 251) as u8]);
                            m.header.origin_timestamp = 1_800_000_000_000_500 + i * 1000;
                            m
                        })
                        .collect();
                    client
                        .send_messages(&stream_id, &topic_id, &Partitioning::partition_id(partition), &mut messages)
                        .await
                        .unwrap();
                }
                let topic_name = format!("{name}.trade.exchange");
                let reader = Reader::start(&connection, vec![(topic_name.clone(), false)], HashMap::new()).unwrap();
                assert_eq!(reader.parts.len(), 3);
                assert!(
                    reader.origins.iter().all(|(_, _, _, first)| *first == Some(1_800_000_000_000_500)),
                    "source bounds retain microseconds at window boundaries"
                );
                let rows = collect(&reader, 3015);
                for partition in 0..3 {
                    let found: Vec<_> = rows.iter().filter(|(p, _)| *p == partition).map(|(_, m)| m).collect();
                    assert_eq!(found.len(), 1005);
                    for (i, m) in found.iter().enumerate() {
                        assert_eq!(m.offset, i as i64);
                        assert_eq!(m.payload, vec![0x0a, 3, 0, 0xfe, 0xff, partition as u8, (i % 251) as u8]);
                        assert_eq!(m.ts, 1_800_000_000_000 + i as i64);
                    }
                }
                drop(reader);
                let blocked = Reader::start(&connection, vec![(topic_name.clone(), false)], HashMap::new()).unwrap();
                let deadline = Instant::now() + Duration::from_secs(3);
                while !blocked.queues.iter().all(|q| q.lock().unwrap().len() == 1000) {
                    assert!(Instant::now() < deadline, "partition queues did not reach bounded capacity");
                    std::thread::sleep(Duration::from_millis(1));
                }
                for partition in 0..3 {
                    assert!(
                        client
                            .get_consumer_offset(
                                &iggy::prelude::Consumer::new(Identifier::numeric(1).unwrap()),
                                &stream_id,
                                &topic_id,
                                Some(partition),
                            )
                            .await
                            .unwrap()
                            .is_none(),
                        "prefetched but unprocessed input must never commit a durable broker cursor"
                    );
                }
                let stopped = Instant::now();
                drop(blocked);
                assert!(stopped.elapsed() < Duration::from_secs(3), "shutdown cancels full partition queues");
                let positions = (0..3).map(|p| ((topic_name.clone(), p), 1000)).collect();
                let resumed = Reader::start(&connection, vec![(topic_name.clone(), true)], positions).unwrap();
                let rows = collect(&resumed, 15);
                assert!(rows.iter().all(|(_, m)| m.offset >= 1000 && m.offset < 1005));
                drop(resumed);
                let latest = Reader::start(&connection, vec![(topic_name.clone(), true)], HashMap::new()).unwrap();
                assert!(latest.starts.values().all(|o| *o == 1005));
                // Leave queues undrained: shutdown must cancel background I/O and backpressure.
                let started = Instant::now();
                drop(latest);
                assert!(started.elapsed() < Duration::from_secs(3));
                let beyond = (0..3).map(|p| ((topic_name.clone(), p), 1006)).collect();
                assert!(Reader::start(&connection, vec![(topic_name.clone(), false)], beyond).is_err());
                let disappeared = (0..4).map(|p| ((topic_name.clone(), p), 0)).collect();
                assert!(
                    Reader::start(&connection, vec![(topic_name.clone(), false)], disappeared).is_err(),
                    "a missing checkpointed partition must not silently lose its unfinished input"
                );
                assert!(Reader::start(&connection, vec![(format!("{name}.missing"), false)], HashMap::new()).is_err());
                client.delete_stream(&stream_id).await.unwrap();
                client.shutdown().await.unwrap();
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_topics_map_to_the_iggy_stream_without_changing_checkpoint_names() {
        assert_eq!(split_topic("market.trades.spot").unwrap(), ("market", "trades.spot"));
        assert_eq!(split_topic("market.l2.perps").unwrap(), ("market", "l2.perps"));
        for bad in ["market", ".trades", "market.", ""] {
            assert!(split_topic(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn checkpoint_positions_are_next_offsets_and_override_latest() {
        assert_eq!(start_offset(Some(42), true, 0, 90).unwrap(), 42);
        assert_eq!(start_offset(None, true, 0, 90).unwrap(), 90);
        assert_eq!(start_offset(None, false, 0, 90).unwrap(), 0);
        assert_eq!(start_offset(Some(0), false, 0, 90).unwrap(), 0);
        assert!(start_offset(Some(-1), false, 0, 90).is_err());
        assert!(start_offset(Some(42), false, 45, 90).is_err(), "retention must not silently lose checkpointed input");
        assert!(start_offset(Some(91), false, 0, 90).is_err(), "truncation must not silently reset recovery");
        assert_eq!(start_offset(None, false, 45, 90).unwrap(), 45, "fresh replay starts at retained input");
    }

    #[test]
    fn every_checkpointed_partition_must_still_exist_before_resuming_engine_state() {
        let assignment = vec![("raw.trades.a".into(), 0), ("raw.trades.b".into(), 1)];
        assert!(checkpoint_partitions(&Positions::new(), &assignment).is_ok());
        let positions = assignment.iter().cloned().map(|tp| (tp, 42)).collect();
        assert!(checkpoint_partitions(&positions, &assignment).is_ok());
        for missing in [("raw.trades.a".into(), 2), ("raw.trades.c".into(), 0)] {
            let mut positions = positions.clone();
            positions.insert(missing, 42);
            let error = checkpoint_partitions(&positions, &assignment).unwrap_err().to_string();
            assert!(error.contains("no longer exists"), "{error}");
        }
        assert!(checkpoint_partitions(&positions, &[]).is_err());
    }

    #[test]
    fn advertised_history_cannot_regress_or_exclude_a_delivered_record() {
        assert_eq!(observed_end(99, 100, 100).unwrap(), 100);
        assert_eq!(observed_end(109, 110, 100).unwrap(), 110);
        assert!(observed_end(19, 20, 100).is_err(), "a reset cannot erase the checkpoint's input history");
        assert!(observed_end(99, 101, 100).is_err(), "the advertised end must include every served record");
        assert!(observed_end(i64::MAX as u64, 0, 0).is_err());
        assert!(observed_end(u64::MAX, 0, 0).is_err());
    }

    #[test]
    fn broker_retries_must_never_skip_or_accept_gaps_in_a_partition() {
        assert_eq!(next_offset(42, &[40, 41, 42, 43]).unwrap(), 44);
        assert_eq!(next_offset(42, &[]).unwrap(), 42);
        assert!(next_offset(42, &[40, 41]).is_err(), "a replay-only response cannot loop forever without progress");
        assert!(next_offset(42, &[43]).is_err());
        assert!(next_offset(42, &[42, 44]).is_err());
        assert!(next_offset(42, &[42, 41]).is_err());
        assert!(next_offset(i64::MAX as u64, &[i64::MAX as u64]).is_err());
    }

    #[test]
    fn an_empty_reply_proves_end_only_after_every_advertised_record() {
        assert!(empty_at_end(0, 0, 0), "a truly empty partition uses offset zero as sentinel");
        assert!(empty_at_end(1, 0, 1));
        assert!(empty_at_end(42, 41, 42));
        assert!(!empty_at_end(0, 0, 1), "offset zero may be a real unread record");
        assert!(!empty_at_end(42, 50, 42), "broker end itself proves backlog");
        assert!(!empty_at_end(42, 50, 51), "an empty response before end is not EOF");
        assert!(!empty_at_end(42, 41, 50), "known committed backlog cannot be erased by an empty response");
        assert!(!empty_at_end(100, 19, 100), "a regressed log cannot prove that checkpointed history is complete");
    }

    #[test]
    fn an_empty_reply_at_an_unread_offset_cannot_close_windows_when_metadata_lags() {
        // The broker's inclusive last offset still names an unread record when
        // it equals the next requested offset. Accepting this as EOF releases
        // other partitions and closes windows before that record can contribute.
        for (next, current, known) in [(1, 1, 1), (42, 42, 42), (1, 1, 0), (0, 1, 0)] {
            assert!(
                !empty_at_end(next, current, known),
                "unread broker offset {current} cannot be EOF at next {next}, even with cached end {known}"
            );
        }
    }

    #[test]
    fn source_identity_separates_kafka_and_each_iggy_address_without_credentials() {
        assert_eq!(identity(None).unwrap(), "");
        for bad in ["iggy+tcp://", "iggy+tcp://user:pass@", "kafka://localhost:8090"] {
            assert!(identity(Some(bad)).is_err());
        }
        let a = identity(Some("iggy+tcp://alice:secret@localhost:8090")).unwrap();
        assert_eq!(a, identity(Some("iggy+tcp://bob:other@localhost:8090")).unwrap());
        assert_ne!(a, identity(Some("iggy+tcp://alice:secret@localhost:8091")).unwrap());
        assert!(!a.contains("secret"));
    }
}
