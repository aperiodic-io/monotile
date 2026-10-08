#!/usr/bin/env python3
"""brrrrr against QuestDB 10, each held to one CPU core. See README.md.

    bench.py <data-dir> <n> [--reps 3] [--only brrrrr|questdb]

<data-dir> holds gen.py's output for <n> trades. The engine under test gets core $ENGINE_CORE
(default 1): brrrrr through `taskset`, QuestDB through `docker run --cpuset-cpus`. Redpanda gets
$BROKER_CORE (2); this script, the producer and the ILP sender run on $LOAD_CORE (3). Results are
printed and appended to <data-dir>/results.jsonl.
"""

import argparse
import glob
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ENGINE_CORE = os.environ.get("ENGINE_CORE", "1")
BROKER_CORE = os.environ.get("BROKER_CORE", "2")
LOAD_CORE = os.environ.get("LOAD_CORE", "3")
BROKER = "localhost:19092"
REDPANDA = "redpandadata/redpanda:v24.2.7"
QUESTDB = "questdb/questdb:10.0.1"
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
BRRRRR = os.environ.get("BRRRRR", os.path.join(ROOT, "target/release/brrrrr"))
BRRRRR_ARGS = os.environ.get("BRRRRR_ARGS", "").split()  # e.g. --source-kafka-config key=value

TABLE = """create table trades (symbol symbol capacity 2048, side symbol, exchange long, id varchar,
price double, quantity double, amount double, time long, timestamp timestamp)
timestamp(timestamp) partition by day wal"""

BARS_QUERY = """select timestamp, symbol, first(price) open, max(price) high, min(price) low,
last(price) close, sum(amount) volume, vwap(price, amount) vwap, count() trades
from trades sample by 1m"""

ROLLING_QUERY = """select timestamp, symbol, price,
avg(price) over (partition by symbol order by timestamp rows between 99 preceding and current row) ma100,
max(price) over (partition by symbol order by timestamp range between 1 minute preceding and current row) hi1m,
avg(price, 'alpha', 0.1) over d ema,
price - lag(price) over d dp,
sum(amount) over d cumvol
from trades
window d as (partition by symbol order by timestamp anchor expression timestamp_floor('1d', timestamp))"""

VIEWS = {
    "bars": (
        f"create materialized view bars as ({BARS_QUERY}) partition by day",
        "select sum(trades) from bars",
    ),
    "rolling": (
        f"create live view rolling flush every 1s start from beginning as {ROLLING_QUERY}",
        "select count() from rolling",
    ),
}


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def sh(*cmd, check=True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True).stdout


# ---------------------------------------------------------------- QuestDB


def q(sql, timeout=600):
    url = "http://localhost:9000/exec?" + urllib.parse.urlencode({"query": sql, "timings": "true"})
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            d = json.load(r)
    except urllib.error.HTTPError as e:
        d = json.load(e)
    if "error" in d:
        raise RuntimeError(f"{sql[:60]}...: {d['error']}")
    return d


def scalar(sql):
    try:
        ds = q(sql)["dataset"]
        return ds[0][0] if ds and ds[0][0] is not None else 0
    except (RuntimeError, OSError):
        return 0


def cgroup(name):
    cid = sh("docker", "inspect", "-f", "{{.Id}}", name).strip()
    paths = glob.glob(f"/sys/fs/cgroup/**/*{cid}*", recursive=True)
    return next(p for p in paths if os.path.exists(os.path.join(p, "cpu.stat")))


def cpu_s(cg):
    for line in open(os.path.join(cg, "cpu.stat")):
        if line.startswith("usage_usec"):
            return int(line.split()[1]) / 1e6


def mem_peak_mb(name):
    cid = sh("docker", "inspect", "-f", "{{.Id}}", name).strip()
    for pattern in (
        f"/sys/fs/cgroup/**/*{cid}*/memory.peak",
        f"/sys/fs/cgroup/memory/**/*{cid}*/memory.max_usage_in_bytes",
    ):
        for path in glob.glob(pattern, recursive=True):
            return int(open(path).read()) / 2**20
    return None


def questdb_up():
    sh("docker", "rm", "-f", "-v", "bench-qdb", check=False)
    sh(
        "docker",
        "run",
        "-d",
        "--name",
        "bench-qdb",
        "--network",
        "host",
        f"--cpuset-cpus={ENGINE_CORE}",
        "--memory=8g",
        QUESTDB,
    )
    for _ in range(600):
        try:
            q("select 1")
            break
        except (OSError, RuntimeError):
            time.sleep(0.1)
    q(TABLE)
    return cgroup("bench-qdb")


def send_ilp(path):
    with socket.create_connection(("localhost", 9009)) as s, open(path, "rb") as f:
        while chunk := f.read(1 << 22):
            s.sendall(chunk)


def wait_for(sql, want, since, poll=0.1, limit=3600):
    while (got := scalar(sql)) < want:
        if time.time() - since > limit:
            raise RuntimeError(f"{sql}: {got} of {want} after {limit}s")
        time.sleep(poll)
    return time.time() - since


def questdb_stream(data, n, views):
    """Views first, then ILP: rows arrive while the views keep up (the streaming case)."""
    cg = questdb_up()
    for v in views:
        q(VIEWS[v][0])
    c0, t0 = cpu_s(cg), time.time()
    send_ilp(os.path.join(data, "trades.ilp"))
    sent = time.time() - t0
    stored = wait_for("select count() from trades", n, t0)
    done = {v: wait_for(VIEWS[v][1], n, t0) for v in views}
    r = {
        "engine": "questdb",
        "mode": "stream",
        "views": views,
        "ilp_sent_s": sent,
        "stored_s": stored,
        **{f"{v}_s": s for v, s in done.items()},
        "wall_s": max([stored, *done.values()]),
        "cpu_s": cpu_s(cg) - c0,
        "mem_peak_mb": mem_peak_mb("bench-qdb"),
    }
    return r


def questdb_backfill(data, n):
    """ILP into a table with no views, then each view created over the stored rows, then the
    views' queries run as plain (batch) SQL."""
    cg = questdb_up()
    c0, t0 = cpu_s(cg), time.time()
    send_ilp(os.path.join(data, "trades.ilp"))
    stored = wait_for("select count() from trades", n, t0)
    r = {"engine": "questdb", "mode": "backfill", "stored_s": stored, "ingest_cpu_s": cpu_s(cg) - c0}
    for v in ("bars", "rolling"):
        c0, t0 = cpu_s(cg), time.time()
        q(VIEWS[v][0])
        r[f"{v}_s"] = wait_for(VIEWS[v][1], n, t0)
        r[f"{v}_cpu_s"] = cpu_s(cg) - c0
    # the same queries as plain SQL over the stored table, every output column consumed (batch SQL
    # has no ANCHOR; the data is one day, so a whole-table window is the same)
    batch = {
        "bars": f"select count(), sum(open), sum(high), sum(low), sum(close), sum(volume), sum(vwap) from ({BARS_QUERY})",
        "rolling": "select count(), sum(ma100), sum(hi1m), sum(ema), sum(dp), sum(cumvol) from ("
        + ROLLING_QUERY.replace(" anchor expression timestamp_floor('1d', timestamp)", "")
        + ")",
    }
    for v, wrapped in batch.items():
        runs = []
        for _ in range(3):
            t0 = time.time()
            q(wrapped)
            runs.append(time.time() - t0)
        r[f"{v}_query_s"] = min(runs)
    r["mem_peak_mb"] = mem_peak_mb("bench-qdb")
    return r


# ---------------------------------------------------------------- brrrrr


def redpanda_up():
    sh("docker", "rm", "-f", "-v", "bench-redpanda", check=False)
    sh(
        "docker",
        "run",
        "-d",
        "--name",
        "bench-redpanda",
        "--network",
        "host",
        f"--cpuset-cpus={BROKER_CORE}",
        REDPANDA,
        "redpanda",
        "start",
        "--smp",
        "1",
        "--memory",
        "2G",
        "--overprovisioned",
        "--node-id",
        "0",
        "--check=false",
        "--kafka-addr",
        "PLAINTEXT://0.0.0.0:19092",
        "--advertise-kafka-addr",
        f"PLAINTEXT://{BROKER}",
        "--pandaproxy-addr",
        "0.0.0.0:18082",
        "--schema-registry-addr",
        "0.0.0.0:18081",
        "--rpc-addr",
        "0.0.0.0:33145",
        "--advertise-rpc-addr",
        "localhost:33145",
    )
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": BROKER})
    for _ in range(600):
        try:
            admin.list_topics(timeout=1)
            break
        except Exception:
            time.sleep(0.1)
    admin.create_topics([NewTopic("bench.trades", 1, 1)])["bench.trades"].result()


def produce(path):
    from confluent_kafka import Producer

    p = Producer(
        {
            "bootstrap.servers": BROKER,
            "linger.ms": 50,
            "batch.size": 1 << 20,
            "queue.buffering.max.messages": 1_000_000,
            "enable.idempotence": False,
        }
    )
    buf, i, n = open(path, "rb").read(), 0, 0
    while i < len(buf):
        size = shift = 0
        while True:
            b = buf[i]
            i += 1
            size |= (b & 0x7F) << shift
            shift += 7
            if b < 0x80:
                break
        while True:
            try:
                p.produce("bench.trades", buf[i : i + size])
                break
            except BufferError:
                p.poll(0.05)
        i += size
        n += 1
    p.flush()
    return n


def metric(text, name):
    return sum(float(line.split()[-1]) for line in text.splitlines() if line.startswith(name))


def proc_cpu_s(pid):
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")


def proc_hwm_mb(pid):
    for line in open(f"/proc/{pid}/status"):
        if line.startswith("VmHWM"):
            return int(line.split()[1]) / 1024


def brrrrr(workload, n):
    """Replays the whole topic from the start: every row is in Kafka before brrrrr starts."""
    from confluent_kafka import Consumer, TopicPartition

    prefix = f"r{time.time_ns()}."
    sink = prefix + {"bars": "bench.bars", "rolling": "bench.rolling"}[workload]
    ck = tempfile.mkdtemp(prefix="bench-ck-")
    c = Consumer({"bootstrap.servers": BROKER, "group.id": "bench-watch"})
    t0 = time.time()
    proc = subprocess.Popen(
        [
            "taskset",
            "-c",
            ENGINE_CORE,
            BRRRRR,
            "run",
            os.path.join(HERE, f"{workload}.sql"),
            "--proto",
            os.path.join(ROOT, "fixtures/market.proto"),
            "--checkpoints",
            f"file://{ck}",
            "--brokers",
            BROKER,
            "--sink-topic-prefix",
            prefix,
            "--metrics",
            "127.0.0.1:19464",
            *BRRRRR_ARGS,
        ],
        stderr=subprocess.DEVNULL,
    )
    first = consumed = None
    last, last_change, cpu, hwm = 0, t0, 0.0, 0.0
    try:
        while True:
            time.sleep(0.05)
            now = time.time()
            if proc.poll() is not None:
                raise RuntimeError(f"brrrrr exited with {proc.returncode}")
            cpu, hwm = proc_cpu_s(proc.pid), proc_hwm_mb(proc.pid)
            try:
                text = urllib.request.urlopen("http://127.0.0.1:19464/metrics", timeout=1).read().decode()
            except OSError:
                continue
            got = metric(text, "brrrrr_received_events_total")
            if first is None and got > 0:
                first = now - t0
            if consumed is None and got >= n:
                consumed = now - t0
            try:
                high = c.get_watermark_offsets(TopicPartition(sink, 0), timeout=1)[1]
            except Exception:
                high = 0
            if high != last:
                last, last_change = high, now
            # every per-row output is in; bars: nothing new for 3 s after the last input row
            if consumed is not None and (high >= n if workload == "rolling" else now - last_change > 3):
                break
    finally:
        proc.terminate()
        proc.wait()
        c.close()
        shutil.rmtree(ck, ignore_errors=True)
    wall = last_change - t0
    return {
        "engine": "brrrrr",
        "workload": workload,
        "first_row_s": first,
        "consumed_s": consumed,
        "wall_s": wall,
        "outputs": last,
        "cpu_s": cpu,
        "rss_peak_mb": hwm,
    }


# ----------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data")
    ap.add_argument("n", type=int)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--only", choices=["brrrrr", "questdb"])
    a = ap.parse_args()
    os.sched_setaffinity(0, {int(LOAD_CORE)})
    out = open(os.path.join(a.data, "results.jsonl"), "a")
    runs = []

    def record(r):
        r["n"] = a.n
        log(json.dumps(r))
        out.write(json.dumps(r) + "\n")
        out.flush()
        runs.append(r)

    if a.only != "questdb":
        redpanda_up()
        t0 = time.time()
        log(f"produced {produce(os.path.join(a.data, 'trades.pb'))} in {time.time() - t0:.1f}s")
        for _ in range(a.reps):
            for w in ("bars", "rolling"):
                record(brrrrr(w, a.n))
        sh("docker", "rm", "-f", "-v", "bench-redpanda", check=False)
    if a.only != "brrrrr":
        for _ in range(a.reps):
            for views in ([], ["bars"], ["rolling"]):
                record(questdb_stream(a.data, a.n, views))
            record(questdb_backfill(a.data, a.n))
        sh("docker", "rm", "-f", "-v", "bench-qdb", check=False)

    # medians of each configuration
    groups = {}
    for r in runs:
        key = (r["engine"], r.get("workload") or r.get("mode"), tuple(r.get("views", ())))
        groups.setdefault(key, []).append(r)
    for key, rs in groups.items():
        med = {
            k: round(statistics.median(r[k] for r in rs), 3)
            for k in rs[0]
            if isinstance(rs[0][k], (int, float)) and all(r.get(k) is not None for r in rs)
        }
        print(json.dumps({"config": key, **med}))


if __name__ == "__main__":
    main()
