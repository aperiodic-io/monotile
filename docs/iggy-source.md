# Apache Iggy sources

Build with `cargo build --release -p brrrrr --features iggy` (an image: `FEATURES=iggy`). The
default build leaves Iggy out. `--features iggy,object-store` combines Iggy input with object
store checkpoints.

The official Rust SDK is pinned to 0.11.0 and requires Rust 1.95; the repository toolchain and
Docker builder use Rust 1.95.0. Source round trips were verified against the upstream October 4
nightly (`apache/iggy@sha256:0cbe04ad7d3becb96c231d3c8e91c8a06c4c157d83923156a28da966b04e4c67`).
CI pins that image in `.github/workflows/ci.yml`. Compatibility is established for it by tests,
rather than inferred from a version label. Verify message and offset durability
policies separately for each target image; protocol compatibility alone does not prove them.

## Run a pipeline on Iggy sources

Set `BRRRRR_IGGY` to an Iggy TCP connection string through the environment or a Kubernetes Secret:

```sh
export BRRRRR_IGGY='iggy+tcp://user:password@iggy:8090?reconnection_interval=100ms&reestablish_after=0s'
brrrrr run pipeline.sql --proto market.proto \
  --brokers redpanda:9092 --checkpoints /var/lib/brrrrr/checkpoints \
  --max-drift 0 --partition-wait-ms 50
```

`--iggy CONNECTION` is the equivalent CLI flag. `--brokers` and `--sink-brokers` still select
Kafka sinks. Source Kafka settings are ignored for Iggy data transport. SQL can retain its current
Kafka declarations: `market.trades.spot` reads stream `market`, topic `trades.spot`.
The split is at the first dot; stream and topic must both be nonempty. Every partition is read.
Iggy streams and topics must already exist. No consumer group or server auto commit is used.

The SDK uses one configured TCP seed and learns the cluster roster from server replies for reconnects.
Advertised addresses must be reachable from the consumer. TLS is available through the SDK connection
options (`tls=true`, `tls_domain=...`, `tls_ca_file=...`); certificate verification remains enabled.
Connection strings and credentials are not printed by the source.

## Recovery and source time

Messages remain protobuf bytes and go through the existing schema and engine. Alignment uses the
Iggy producer's `origin_timestamp` in microseconds, converted to milliseconds for the existing merge;
producers must set it to their capture time. Fresh replay's source bounds keep full microsecond precision.
The broker append timestamp is not the event timestamp. `--max-drift`, window closure, Kafka output
suppression and checkpoint leases use the existing runtime.

Checkpoint positions are the next offset in each partition. The source polls explicitly from them,
without committing on the broker. Prefetched messages never advance a checkpoint before the engine
processes them. Checkpoint positions override SQL `seek_to=latest`; without a checkpoint, `earliest`
starts at the first retained record and `latest` starts after the current end.

Iggy checkpoints have a separate identity from Kafka checkpoints and include the configured endpoint
without credentials. Changing the seed endpoint starts a separate checkpoint namespace. A checkpoint
outside retained history, a gap, unexpected partition, malformed protocol, missing topic or authorization
failure stops the source with an error. Transport failures retry with bounded individual poll deadlines,
count `brrrrr_consumer_errors_total` and log retries. A process can remain alive while its broker is down.

Each partition has a queue bounded to 1,000 messages and one outstanding fetch of up to 1,000 messages.
I/O runs on one thread. Shutdown cancels background polls and backpressure; SIGTERM takes the existing
final engine checkpoint and releases the lease. Empty polls prove EOF only after advertised backlog is
consumed. Partition merging and clock/idle window closure use the actual confirmed poll time; a retry
invalidates the EOF proof, and stale or missing proof cannot advance clock closure.

As with Kafka, partition alignment assumes capture-to-broker delay stays within the configured wait.
Inserting historical rows into a live partition after another partition reaches EOF can exceed that
bound; load historical data before starting the reader when testing replay.

## Durable cursors and brrrrr recovery

Iggy has broker-stored offsets for named or numeric consumers. A successful explicit offset store
under `consumer_offset_durability=persisted` provides a persistence completion guarantee. Message
`durability` and offset durability are independent, default to `replicated`, and must be chosen when
creating the topic; they cannot be changed in place. Persisting an offset does not make preceding
replicated messages persistent. Poll auto-commit is asynchronous: a successful poll does not prove
that its cursor committed or became durable. Await an explicit store when that completion matters.
See the [official durability contract](https://iggy.apache.org/docs/server/durability/).

For retained source messages that must survive a broker process crash, provision
`durability=persisted`. If another application also relies on broker-stored cursors, set both:

```sh
iggy topic create market trades.spot 3 none \
  --durability persisted --consumer-offset-durability persisted
```

The source does not change these policies or create topics. Check `GetTopic` before deploying;
existing topics need a migration when their creation policies are unsuitable.

**brrrrr's recovery authority is its checkpoint, not an Iggy cursor.** It polls explicit partition
offsets with auto-commit disabled and never stores its fetched position on the broker. A checkpoint
pairs the processed next source offsets with engine state and acknowledged Kafka output positions.
Prefetch is not processing. Restoring an older checkpoint replays from that checkpoint and suppresses
already written outputs; an unrelated broker cursor, even ahead of the checkpoint, cannot skip this
input. SQL `latest` only selects an initial position when there is no checkpoint. Keep checkpoints
on the existing durable volume or configured object store, and retain their original source history.

The source reads every partition directly. There is no Iggy consumer-group subscription, partition
rebalance, group takeover, or group offset commit. Multiple named broker consumers can maintain
independent cursors, but brrrrr instances coordinate through the existing checkpoint lease. A shared
checkpoint namespace permits one active pipeline owner; independent pipelines require independent
state and sink namespaces. Consumer-group membership is a separate transient session mechanism.

## Kafka comparison and limits

The Kafka comparison checks protobuf decoding, engine behavior, partition offset replay,
and output equality. It does not establish equivalent Kafka group coordination, transactions,
producer fencing, broker content deduplication, compacted-log behavior, or offset retention policies.
Iggy source recovery with Kafka sinks uses brrrrr's existing checkpoint/dedup protocol, rather than a
transaction spanning Iggy reads and Kafka writes. Iggy message IDs are not a content-dedup guarantee.
Producer acknowledgement requirements must be verified against the topic's chosen durability policy.

Checkpoints assume the original retained log under its logical stream/topic/partition names.
Detected expired positions, a disappearing checkpointed partition, and a falling broker end fail
loudly. Offset bounds cannot identify every new generation: deleting/recreating a topic, or purging
and refilling it past a saved cursor, can recreate valid-looking offsets. Do not do that under an
existing checkpoint. Stop the pipeline and explicitly migrate/reset its state and output suppression
contract. `--fresh` still reads back existing Kafka outputs and is not a general data-generation reset.

These source guards do not resolve upstream broker defects. The known metadata invariant failure
[Apache Iggy #4294](https://github.com/apache/iggy/issues/4294) and stale-prepare committed-log divergence issue
[Apache Iggy #4284](https://github.com/apache/iggy/issues/4284) remain limitations of the tested nightly.
A clean source checkpoint cannot recover input the broker itself has lost or refuses to reopen.
The durability tests verify explicit cursor completion, fresh-client reconnection, and SIGKILL/restart
of a disposable singleton broker without a graceful final flush. They do not simulate machine power
loss or storage-device failure. One earlier whole-process restart exceeded an eight-second shutdown
cap; an identical repeat exited in 0.33 seconds. This timing is not a guaranteed process shutdown bound.

## Broker tests

Set `BRRRRR_IT_IGGY` to a writable TCP broker and `BRRRRR_IT_BROKER` to a writable Kafka broker:

```sh
cargo test -p brrrrr --features iggy --bin brrrrr iggy:: -- --include-ignored
cargo test -p brrrrr --features iggy --test iggy -- --ignored
# Only an explicitly owned disposable broker: this test target SIGKILLs and restarts it.
export BRRRRR_IT_IGGY_CONTAINER=iggy
cargo test -p brrrrr --features iggy --test iggy_durability -- --ignored --test-threads=1
```

The broker tests create unique source streams and Kafka topics. They cover binary payloads, three
partitions, batches above 1,000 messages, exact offsets and timestamps, checkpoint resume, latest,
invalid history, missing topics, bounded shutdown, and subprocess engine/Kafka output after SIGKILL
and graceful restarts. The runtime test compares all output bytes to the pure engine and verifies
partition checkpoint positions. CI provisions its own brokers and runs these tests explicitly.

The durability target additionally checks explicitly persisted named cursors, independent consumers,
no commit on fetch, fresh-client resume, creation-policy independence, brrrrr's checkpoint precedence
over `latest` and an ahead broker cursor, missing checkpoint partitions, live purge/truncation, and
permission-denied reads, and byte-exact persisted message/cursor recovery after broker SIGKILL.
It creates and removes only its own streams and test users, and stops only the disposable Docker
broker named by `BRRRRR_IT_IGGY_CONTAINER`; it does not stop shared regression nodes. Run this
target serially on an isolated writable broker for destructive history and process-crash tests.
