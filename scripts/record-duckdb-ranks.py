#!/usr/bin/env python3
"""Record DuckDB's answers to random ranking queries over random tables: the reference brrrrr's
cross-sectional `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist` and `ntile` are
held to (crates/brrrrr-core/tests/rank.rs).

Each line is a table `t` (rid, ts, g, x, y, s), a query in brrrrr's SQL, the same query in
DuckDB's, and DuckDB's rows. The tables are small cross-sections (one to forty rows a time,
a few times, sometimes out of time order) of ties, NULLs, NaN, -0.0, infinities and text, and
the queries rank them by every kind of key, ascending and descending, NULLs first and last,
in partitions of a time and of a time and a sector, after a WHERE or over a GROUP BY's buckets.

DuckDB's ORDER BY puts NULLs last whichever the direction; brrrrr's, as PostgreSQL's, sorts them
as the largest value: DuckDB runs with that order (`default_null_order`). `row_number` and
`ntile` number ties in an order of DuckDB's choosing; brrrrr's is the order the rows came, so
DuckDB's ORDER BY ends with `pos`, the row's place in the table (which a stable sort by time
keeps), for those two. Floats are the bits of the f64, in hex.

Usage: pip install duckdb==1.5.6 && python3 scripts/record-duckdb-ranks.py
"""
import gzip, json, random, struct

import duckdb

OUT = "fixtures/duckdb-vectors/ranks.jsonl.gz"
SEC = 1_000_000
rng = random.Random(7)
con = duckdb.connect()
con.execute("SET default_null_order = 'nulls_last_on_asc_first_on_desc'")


def bits(v):
    return format(struct.unpack("<Q", struct.pack("<d", v))[0], "016x")


def cell(v):
    if isinstance(v, float):
        return {"f64": bits(v)}
    return v


XS = [-0.0, 0.0, 1.5, -2.0, float("nan"), float("inf"), float("-inf"), 0.1, 3.0, None]
SS = ["a", "B", "ab", "", "b", "A", None]


def table():
    rows, rid, ts = [], 0, 1_704_067_200 * SEC
    continuous = rng.random() < 0.3
    for _ in range(rng.randint(1, 5)):
        ts += rng.choice([1, SEC, 7 * SEC])
        for _ in range(rng.choice([1, 1, 2, 3, rng.randint(1, 12), rng.randint(1, 40)])):
            x = rng.choice([None, round(rng.gauss(0, 1), 2)]) if continuous else rng.choice(XS)
            y = rng.choice([None, rng.randint(-3, 3)])
            rows.append([rid, ts, rng.choice(["a", "b", "c", None]), x, y, rng.choice(SS)])
            rid += 1
    if rng.random() < 0.25:
        rng.shuffle(rows)  # out of time order: brrrrr sorts it by ts, stably
    return rows


def order_key():
    e = rng.choice(["x", "y", "s", "-x", "coalesce(y, 0)", "g"])
    e += rng.choice(["", " ASC", " DESC"])
    return e + rng.choice(["", "", " NULLS FIRST", " NULLS LAST"])


def call():
    f = rng.choice(["row_number()", "rank()", "dense_rank()", "percent_rank()", "cume_dist()", "ntile"])
    if f == "ntile":
        f = f"ntile({rng.randint(1, 7)})"
    return f, [order_key() for _ in range(rng.choice([1, 1, 2, 3]))]


def over(partition, keys, numbered, tie):
    """OVER (...) in brrrrr's SQL, and in DuckDB's with `tie` last for row_number and ntile."""
    ours = f"PARTITION BY {', '.join(partition)} ORDER BY {', '.join(keys)}"
    theirs = ours + (f", {tie}" if numbered else "")
    return ours, theirs


def ranked(rows):
    """Ranks of the table's rows, after a WHERE or not."""
    items_b, items_d = ["rid"], ["rid"]
    for i in range(rng.randint(1, 7)):
        f, keys = call()
        partition = rng.choice([["ts"], ["ts"], ["ts", "g"], ["ts", "g", "y"]])
        ours, theirs = over(partition, keys, f.startswith(("row_number", "ntile")), "pos")
        items_b.append(f"{f} OVER ({ours}) AS c{i}")
        items_d.append(f"{f} OVER ({theirs}) AS c{i}")
    where = rng.choice(["", "", " WHERE y IS NULL OR y <> 0", " WHERE s > 'a'"])
    b = f"SELECT {', '.join(items_b)} FROM t{where} ORDER BY rid"
    d = f"SELECT {', '.join(items_d)} FROM t{where} ORDER BY rid"
    return b, d


def bucketed(rows):
    """Ranks of a GROUP BY's rows in each bucket: what a cross-section of bars is. Buckets of 5 s,
    which DuckDB's origin (2000-01-03) and brrrrr's (the epoch) agree on."""
    items_b = ["epoch_us(b) AS b", "g", "n", "hi"]
    items_d = list(items_b)
    for i in range(rng.randint(1, 5)):
        f, _ = call()
        keys = [rng.choice(["n", "hi", "g"]) + rng.choice(["", " DESC", " NULLS FIRST"]) for _ in range(rng.choice([1, 2]))]
        # a window's groups come in an order of brrrrr's choosing: ties numbered by the sector
        if f.startswith(("row_number", "ntile")):
            keys.append("g")
        ours = f"PARTITION BY b ORDER BY {', '.join(keys)}"
        items_b.append(f"{f} OVER ({ours}) AS c{i}")
        items_d.append(f"{f} OVER ({ours}) AS c{i}")
    sub_b = "SELECT time_bucket('5s', ts) AS b, g, count(*) AS n, max(y) AS hi FROM t GROUP BY b, g"
    sub_d = "SELECT time_bucket(INTERVAL '5 seconds', make_timestamp(ts)) AS b, g, count(*) AS n, max(y) AS hi FROM t GROUP BY b, g"
    tail = " ORDER BY b, g NULLS LAST"
    return (f"SELECT {', '.join(items_b)} FROM ({sub_b}){tail}", f"SELECT {', '.join(items_d)} FROM ({sub_d}){tail}")


def main():
    with gzip.open(OUT, "wt") as out:
        for case in range(1000):
            rows = table()
            con.execute("CREATE OR REPLACE TABLE t (rid BIGINT, ts BIGINT, g VARCHAR, x DOUBLE, y BIGINT, s VARCHAR, pos BIGINT)")
            con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)", [r + [i] for i, r in enumerate(rows)])
            ours, theirs = bucketed(rows) if case % 5 == 4 else ranked(rows)
            got = con.execute(theirs).fetchall()
            line = {"rows": [[cell(v) for v in r] for r in rows], "sql": ours, "duckdb": theirs, "expected": [[cell(v) for v in r] for r in got]}
            out.write(json.dumps(line) + "\n")


if __name__ == "__main__":
    main()
