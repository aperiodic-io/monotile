#!/usr/bin/env python3
"""Record golden vectors for skewness, kurtosis and kurtosis_pop: DuckDB's results (the Python
package, single-threaded so rows are accumulated in order, as in a window) and the exact values
of the same definitions, from rational arithmetic (square roots at 60 digits).

Each line holds the inputs and, per function, DuckDB's result and the exact one, as the bits of
the f64 (hex), "null", "nan", "error" where DuckDB raises an out-of-range error (brrrrr, which
cannot fail a running stream, returns NULL there), or "-" where no exact value exists (an
infinite input). DuckDB sums raw powers, which cancel on prices: where it strays from the exact
value, brrrrr is held to the exact one. Inputs are market-like prices and amounts, plus the edge
cases: too few rows, constant windows, two values, integers, large and tiny magnitudes.
Usage: pip install duckdb==1.5.5 && scripts/record-duckdb-vectors.py
"""
import decimal, gzip, io, json, math, random, struct
from fractions import Fraction

import duckdb

OUT = "fixtures/duckdb-vectors/moments.jsonl.gz"
rng = random.Random(11)
con = duckdb.connect()
con.execute("SET threads = 1")


def bits(v):
    return format(struct.unpack("<Q", struct.pack("<d", v))[0], "016x")


def result(sql, xs):
    try:
        (v,) = con.execute(sql, [xs]).fetchone()
    except duckdb.OutOfRangeException:
        return "error"
    if v is None:
        return "null"
    if v != v:
        return "nan"
    return bits(v)


def exact(xs):
    """skewness, kurtosis, kurtosis_pop by DuckDB's definitions, computed exactly."""
    if not all(math.isfinite(x) for x in xs):
        return {f: "-" for f in ["skewness", "kurtosis", "kurtosis_pop"]}
    n = len(xs)
    q = [Fraction(x) for x in xs]
    mean = sum(q) / n
    m2, m3, m4 = (sum((x - mean) ** k for x in q) / n for k in (2, 3, 4))
    decimal.getcontext().prec = 60
    dec = lambda r: decimal.Decimal(r.numerator) / decimal.Decimal(r.denominator)
    out = {}
    if n <= 2:
        out["skewness"] = "null"
    elif m2 == 0:
        out["skewness"] = "nan"
    else:
        g1 = decimal.Decimal(n * (n - 1)).sqrt() / (n - 2) * dec(m3) / (dec(m2) * dec(m2).sqrt())
        out["skewness"] = bits(float(g1))
    out["kurtosis"] = "null" if n <= 3 or m2 == 0 else bits(float((n - 1) * ((n + 1) * m4 / (m2 * m2) - 3 * (n - 1)) / ((n - 2) * (n - 3))))
    out["kurtosis_pop"] = "null" if n <= 1 or m2 == 0 else bits(float(m4 / (m2 * m2) - 3))
    return out


def cases():
    for n in range(1, 8):  # the NULL thresholds: skewness n <= 2, kurtosis n <= 3, kurtosis_pop n <= 1
        yield [rng.gauss(100.0, 3.0) for _ in range(n)]
    for _ in range(150):  # prices: a level, ticks of 0.1 or 0.01
        base, tick = rng.choice([(60000.0, 0.1), (3000.0, 0.01), (0.5, 0.0001), (150.0, 0.01)])
        yield [round(base + rng.gauss(0, 20) * tick, 6) for _ in range(rng.randint(2, 400))]
    for _ in range(100):  # trade amounts: heavy tailed
        yield [round(rng.lognormvariate(0, 2), 8) for _ in range(rng.randint(2, 300))]
    for _ in range(60):  # uniform and normal samples at several scales
        s = 10.0 ** rng.randint(-8, 9)
        yield [rng.uniform(-s, s) for _ in range(rng.randint(2, 200))]
    for v in [0.0, 1.0, 3.0, 0.1, 60000.1, 1e-8, 123456789.123, -7.25]:  # constant windows
        for n in [2, 3, 4, 5, 10, 37, 100]:
            yield [v] * n
    for _ in range(30):  # two distinct values
        a, b = rng.uniform(-100, 100), rng.uniform(-100, 100)
        yield [rng.choice([a, b]) for _ in range(rng.randint(2, 50))]
    for _ in range(30):  # integers
        yield [float(rng.randint(-1000, 1000)) for _ in range(rng.randint(2, 100))]
    for _ in range(20):  # a first row far from the rest (the worst case for a shift by the first row)
        base, spread = rng.choice([(60000.0, 5.0), (1.0, 0.1), (0.001, 0.0001)])
        first = base * rng.choice([1e3, 1e4, -1e3, 1e-3])
        yield [first] + [base + rng.gauss(0, spread) for _ in range(rng.randint(3, 500))]
    yield [1.0, 2.0, 3.0, 4.0, 10.0]  # the example of docs/compat/questdb-query-gap.md
    yield [1e300, -1e300, 1e300, -1e300, 2e300]  # overflows: DuckDB raises
    yield [0.0, float("inf"), 1.0, 2.0]


with io.TextIOWrapper(gzip.GzipFile(OUT, "wb", mtime=0)) as out:  # reproducible bytes
    for xs in cases():
        row = {"xs": [bits(x) for x in xs]}
        for f in ["skewness", "kurtosis", "kurtosis_pop"]:
            row[f] = result(f"SELECT {f}(x) FROM (SELECT unnest(?::DOUBLE[]) AS x)", xs)
        row["exact"] = exact(xs)
        out.write(json.dumps(row) + "\n")
print(f"wrote {OUT}")
