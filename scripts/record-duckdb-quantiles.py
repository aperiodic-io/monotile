#!/usr/bin/env python3
"""Record golden vectors for quantile_cont: what DuckDB (the Python package, 1.5.5) returns for
MEDIAN(x) and PERCENTILE_CONT(level) WITHIN GROUP (ORDER BY x) over windows of market-like values.

Each line holds the inputs in arrival order, each the bits of the f64 (hex) or "null", and per
level DuckDB's result: the bits of the f64, "null" or "nan". "median" is MEDIAN's, which
tests/quantile_cont.rs requires to be PERCENTILE_CONT(0.5)'s. Windows of every size from 1 to a few
past brrrrr's exact buffer (agg::CONT_EXACT), where quantile_cont must return these bits, and
larger ones, where its t-digest is held to them within its error: notionals (heavy tailed), prices
on a tick grid, slippage in basis points (both signs, a handful of distinct values), duplicates,
NULLs, NaNs, infinities, each in several arrival orders.
Usage: pip install duckdb==1.5.5 && scripts/record-duckdb-quantiles.py
"""
import gzip, io, json, math, random, struct

import duckdb

OUT = "fixtures/duckdb-vectors/quantiles.jsonl.gz"
LEVELS = [0.0, 0.05, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]
EXACT = 256  # agg::CONT_EXACT
rng = random.Random(122)
con = duckdb.connect()
con.execute("SET threads = 1")


def bits(v):
    if v is None:
        return "null"
    if v != v:
        return "nan"
    return format(struct.unpack("<Q", struct.pack("<d", v))[0], "016x")


def notionals(n):
    return [rng.lognormvariate(5, 2.5) for _ in range(n)]


def prices(n):
    base, tick = rng.choice([(60000.0, 0.1), (3000.0, 0.01), (0.5, 0.0001), (150.0, 0.01)])
    p, out = base, []
    for _ in range(n):
        p = round(max(tick, p + rng.choice([-2, -1, 0, 0, 0, 1, 2]) * tick), 10)
        out.append(p)
    return out


def bps(n):
    # (price - ask) / ask * 10000: a few ticks either side of a quote, mostly at it
    ask, tick = rng.choice([(60000.0, 0.1), (2500.0, 0.01), (0.5, 0.0001)])
    return [rng.choice([0, 0, 0, 0, 1, -1, 2, -2, -5, 7]) * tick / ask * 10000 for _ in range(n)]


def cases():
    yield []
    yield [None, None]
    yield [132.7346, 16.4054]  # two values: MEDIAN is their midpoint, 74.57
    yield [16.4054, 132.7346]
    sizes = list(range(1, 41)) + [63, 64, 65, 127, 128, 129, 199, 200, 201, 254, 255, 256, 257, 258, 300]
    for kind in (notionals, prices, bps):
        for n in sizes:
            xs = kind(n)
            yield xs
            yield sorted(xs)
            yield sorted(xs, reverse=True)
    for n in [1, 2, 3, 4, 5, EXACT - 1, EXACT, EXACT + 1]:  # duplicates: one value, and two
        yield [7.25] * n
        yield [rng.choice([-3.5, 1e-9]) for _ in range(n)]
    for n in [2, 3, 4, 5, 9, 50, EXACT]:  # NULLs and NaNs are skipped; the values left decide
        xs = notionals(n)
        yield [x for v in xs for x in (v, None)]
        yield [None] + xs + [float("nan")]
        yield [float("nan")] * 3 + bps(n)
    yield [float("nan")]
    yield [float("nan"), None]
    for xs in ([1.0, float("inf")], [float("inf")], [float("-inf"), float("inf")], [float("-inf"), 1.0, 2.0],
               [float("inf"), float("inf"), 1.0], [1.0, 2.0, 3.0, float("inf")], [1e308, 1e308], [-1e308, 1e308],
               [5e-324, 1e-323], [1.0, 1.0 + 2**-52]):
        yield xs
    for kind in (notionals, prices, bps):  # past the exact buffer: the digest's territory
        for n in [EXACT + 2, 400, 1000, 2047, 2048, 2049, 3000, 5000, 20000]:
            yield kind(n)
    # the digest's merges, every agg::BUFFER (512) values as they come (ADR-0014): either side
    # of the first two, in arrival order and sorted (last, so the windows above keep their values)
    for kind in (notionals, prices, bps):
        for n in [511, 512, 513, 1023, 1024, 1025]:
            xs = kind(n)
            yield xs
            yield sorted(xs)


with io.TextIOWrapper(gzip.GzipFile(OUT, "wb", mtime=0)) as out:  # reproducible bytes
    n = 0
    for xs in cases():
        row = {"xs": [bits(x) for x in xs]}
        src = "(SELECT unnest(?::DOUBLE[]) AS x)"
        (m,) = con.execute(f"SELECT MEDIAN(x) FROM {src}", [xs]).fetchone()
        row["median"] = bits(m)
        for q in LEVELS:
            (v,) = con.execute(f"SELECT PERCENTILE_CONT({q}) WITHIN GROUP (ORDER BY x) FROM {src}", [xs]).fetchone()
            row[str(q)] = bits(v)
        out.write(json.dumps(row) + "\n")
        n += 1
print(f"wrote {OUT}: {n} windows")
