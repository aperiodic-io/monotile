#!/usr/bin/env python3
"""Synthetic input for an example pipeline: fixtures/pipeline-inputs/<pipeline>.json.gz.

Each file holds deterministic rows for every source of the pipeline's SQL (each stream no view
writes into), as chunks that the tests (crates/brrrrr-core/tests/common) feed one
`Engine::insert` each, then a row 4 days later on every source that closes every open window.
Three shapes of input:
- sparse (most pipelines): about 40 rounds of 0-6 rows per source over ~30 h, with rows right at
  minute boundaries, late rows older than windows already closed, and zeros for the null_if
  guards;
- trades (returns, state): 12 minutes of three symbols' trades, in bursts that share a time, in
  time order in chunks of 200 (the sequence aggregates' and window functions' state);
- book (sources with `is_snapshot`): per symbol a snapshot, then level changes in sequence order,
  a stale replayed message now and then, and a second snapshot midway.

Usage: scripts/record-pipeline-inputs.py fixtures/pipelines/*.sql
Needs only Python. Each input is seeded with the CRC-32 of its pipeline's name.
"""
import gzip, json, random, re, sys, zlib

BASE_US = 1_788_220_800_000_000  # 2026-09-01 00:00:00 UTC
END_US = BASE_US + 4 * 86_400_000_000  # closes every window
CHUNKS = 40
SCALE = {"BTC-USDT": 60_000.0, "ETH-USDT": 2_500.0, "spot-SOL-USDC": 150.0}


def columns(body):
    """`name type [MATERIALIZED ...]` entries of a column list, split on top-level commas."""
    out, depth, cur = [], 0, ""
    for ch in body:
        depth += ch in "(["
        depth -= ch in ")]"
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    return [c for c in out + [cur.strip()] if c]


def sources(script):
    """The pipeline's sources, the streams no view writes into: {name: [(col, type)]}."""
    script = "\n".join(l for l in script.splitlines() if not l.strip().startswith("--"))
    targets = set(re.findall(r"\bINTO\s+(\w+)", script))
    out = {}
    for s in (s.strip() for s in script.split(";")):
        m = re.match(r"CREATE (?:EXTERNAL )?STREAM IF NOT EXISTS (\w+) \(", s)
        if m and m.group(1) not in targets:
            depth, end = 1, m.end()
            while depth:
                depth += {"(": 1, ")": -1}.get(s[end], 0)
                end += 1
            out[m.group(1)] = [tuple(c.split(None, 1)) for c in columns(s[m.end():end - 1])]
    return out


def value(rng, col, ty, t_us, local_us, sym, n):
    if col == "time":
        return t_us
    if col == "local_timestamp":
        return local_us
    if col == "exchange":
        return 7
    if col == "symbol":
        return sym
    if col == "side":
        return rng.choice(["buy", "sell", "buy", "sell", "unknown"] if rng.random() < 0.05 else ["buy", "sell"])
    if col == "id":
        return str(n)
    assert ty == "float64", (col, ty)
    if rng.random() < 0.03:
        return 0.0  # exercises null_if(x, 0) and the division guards
    scale = SCALE[sym]
    if col == "funding_rate":
        return round(rng.uniform(-0.001, 0.001), 8)
    if "ratio" in col or col.endswith("_share"):
        return round(rng.uniform(0.2, 3.0), 4)
    if col in ("quantity", "amount", "ask_amount", "bid_amount", "open_interest"):
        return round(rng.lognormvariate(0, 1.5), rng.choice([0, 3, 6]))
    if col == "ask_price":
        return round(scale * rng.uniform(1.0, 1.002), 2)
    if col == "bid_price":
        return round(scale * rng.uniform(0.998, 1.0), 2)
    return round(scale * rng.uniform(0.99, 1.01), rng.choice([1, 2, 4]))


def closing(rng, sources):
    return [{"stream": name, "rows": [[value(rng, c, ty, END_US, END_US, "BTC-USDT", 0) for c, ty in cols]]}
            for name, cols in sorted(sources.items())]


def sparse(rng, sources):
    """Chunks over ~30 h: jitter across window edges, late rows, then a far flush."""
    n, out = 0, []
    step = 45 * 60_000_000
    for i in range(CHUNKS):
        for name in rng.sample(sorted(sources), len(sources)):
            rows = []
            for _ in range(rng.randint(0, 6)):
                t = BASE_US + i * step + rng.randrange(step)
                if rng.random() < 0.2:  # right at a minute boundary, around the 50 ms delay
                    t = t // 60_000_000 * 60_000_000 + rng.randint(-100_000, 100_000)
                local = t + rng.randint(0, 200_000)
                if rng.random() < 0.08:  # late: older than windows already closed
                    local -= rng.randint(1, 3) * step
                n += 1
                sym = rng.choice(sorted(SCALE))
                rows.append([value(rng, c, ty, t, local, sym, n) for c, ty in sources[name]])
            if rows:
                out.append({"stream": name, "rows": rows})
    return out + closing(rng, sources)


def trades(rng, sources):
    """12 minutes of three symbols' trades, bursts sharing a time, all in time order."""
    (name, cols), = sources.items()
    rows, next_id = [], {"A": 990, "B": 99_990, "C": 7}  # B's ids reach 6 digits: text order differs
    t0 = BASE_US + 40_000_000
    for sym in ("A", "B", "C"):
        t, price = t0, 100.0
        while t < t0 + 12 * 60_000_000:
            t += rng.choice([0, 0, 0, 1, 50, 900, 20_000, 400_000, 3_000_000])
            price = max(1.0, round(price + rng.choice([-0.5, 0, 0, 0.5, 1.0, -1.0]), 1))
            next_id[sym] += 1
            q = round(rng.uniform(0.001, 5), 3)
            tr = {"time": t - rng.randint(0, 3_000), "id": str(next_id[sym]), "exchange": 7, "symbol": sym,
                  "price": price, "local_timestamp": t, "side": rng.choice(["buy", "sell", "buy"]), "quantity": q,
                  "amount": round(price * q, 6)}
            rows.append([tr[c] for c, _ in cols])
    at = [c for c, _ in cols].index("local_timestamp")
    rows.sort(key=lambda r: r[at])  # stable: a symbol's trades keep their order
    out = [{"stream": name, "rows": rows[i:i + 200]} for i in range(0, len(rows), 200)]
    end = {"time": END_US, "id": "0", "exchange": 7, "symbol": "Z", "price": 1.0, "local_timestamp": END_US,
           "side": "buy", "quantity": 1.0, "amount": 1.0}
    return out + [{"stream": name, "rows": [[end[c] for c, _ in cols]]}]


def book(rng, sources):
    """Per source and symbol a 30-level snapshot, ~400 changes of 1-4 levels in sequence order (a
    stale replay now and then), a second snapshot midway, then a snapshot that closes every window;
    the sources' messages in time order, alternating in chunks of up to 100."""
    msgs = []
    for name in sorted(sources):
        for sym in ("BTC-USDT", "ETH-USDT"):
            scale, tick = SCALE[sym], SCALE[sym] / 10_000
            seq, t = rng.randint(1, 1_000_000), BASE_US + rng.randrange(1_000_000)

            def snapshot():
                mid = round(scale * rng.uniform(0.995, 1.005) / tick) * tick
                bids = {round(mid - (i + 1) * tick, 6): round(rng.lognormvariate(0, 1), 3) for i in range(30)}
                asks = {round(mid + (i + 1) * tick, 6): round(rng.lognormvariate(0, 1), 3) for i in range(30)}
                return bids, asks

            bids, asks = snapshot()
            msgs.append((name, t, sym, True, bids, asks, seq))
            sent = []
            for i in range(400):
                t += rng.choice([1_000, 50_000, 900_000, 4_000_000])
                if i == 200:
                    seq += 1
                    bids, asks = snapshot()
                    msgs.append((name, t, sym, True, bids, asks, seq))
                    continue
                if sent and rng.random() < 0.03:  # a replica's replay of an older message
                    old = rng.choice(sent)
                    msgs.append((name, t, sym, False, old[0], old[1], old[2]))
                    continue
                seq += rng.choice([1, 1, 1, 2])
                b, a = {}, {}
                for _ in range(rng.randint(1, 4)):
                    side, levels = (b, bids) if rng.random() < 0.5 else (a, asks)
                    if not levels:
                        continue
                    best = max(bids) if side is b else min(asks)
                    p = round(best + (-1 if side is b else 1) * rng.randint(-2, 35) * tick, 6)
                    amount = 0.0 if p in levels and rng.random() < 0.3 else round(rng.lognormvariate(0, 1), 3)
                    side[p] = amount
                    if amount:
                        levels[p] = amount
                    else:
                        levels.pop(p, None)
                sent.append((b, a, seq))
                msgs.append((name, t, sym, False, b, a, seq))
    msgs.sort(key=lambda m: m[1])
    out = []
    for name, t, sym, snap, bids, asks, seq in msgs:
        row = {"time": t, "exchange": 7, "symbol": sym, "is_snapshot": snap,
               "local_timestamp": t + rng.randint(0, 5_000), "bid_price": list(bids), "bid_amount": list(bids.values()),
               "ask_price": list(asks), "ask_amount": list(asks.values()), "venue_sequence": seq}
        if not out or out[-1]["stream"] != name or len(out[-1]["rows"]) == 100:
            out.append({"stream": name, "rows": []})
        out[-1]["rows"].append([row[c] for c, _ in sources[name]])
    for name in sorted(sources):
        row = {"time": END_US, "exchange": 7, "symbol": "BTC-USDT", "is_snapshot": True, "local_timestamp": END_US,
               "bid_price": [0.1], "bid_amount": [1.0], "ask_price": [0.2], "ask_amount": [1.0], "venue_sequence": 0}
        out.append({"stream": name, "rows": [[row[c] for c, _ in sources[name]]]})
    return out


def record(path):
    pipeline = path.rsplit("/", 1)[-1].removesuffix(".sql")
    src = sources(open(path).read())
    rng = random.Random(zlib.crc32(pipeline.encode()))
    shape = book if any(c == "is_snapshot" for cols in src.values() for c, _ in cols) else \
        trades if pipeline in ("returns", "state") else sparse
    input_chunks = shape(rng, src)
    with gzip.open(f"fixtures/pipeline-inputs/{pipeline}.json.gz", "wt") as f:
        json.dump({"pipeline": pipeline, "chunks": input_chunks}, f)
    print(f"{pipeline}: {len(input_chunks)} chunks, {sum(len(c['rows']) for c in input_chunks)} rows")


if __name__ == "__main__":
    for p in sys.argv[1:]:
        record(p)
