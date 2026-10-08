#!/usr/bin/env python3
"""Record QuestDB's results for per-row window functions (OVER), to hold brrrrr's to them.

Runs against a QuestDB 10.0.1 container's HTTP endpoint:
    docker run -d --name qdb -p 9000:9000 questdb/questdb:10.0.1
    scripts/record-questdb-over.py [http://localhost:9000]

The input: three partitions of prices with NULLs, strictly increasing timestamps (no ties,
so QuestDB's timestamp order is brrrrr's arrival order). Each column is given in QuestDB's
spelling and brrrrr's (they differ only in RANGE intervals). Output:
fixtures/questdb-vectors/over.json.gz with the rows, the columns and QuestDB's results.
"""
import gzip, io, json, random, sys, time, urllib.parse, urllib.request

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:9000"
OUT = "fixtures/questdb-vectors/over.json.gz"
TABLE = "over_vectors"


def q(sql):
    with urllib.request.urlopen(f"{URL}/exec?" + urllib.parse.urlencode({"query": sql})) as r:
        d = json.load(r)
    if "error" in d:
        raise SystemExit(f"QuestDB: {d['error']}\n{sql[:300]}")
    return d


P = "partition by g order by ts"
COLUMNS = [  # (name, QuestDB, brrrrr)
    ("a5", f"avg(p) over ({P} rows between 5 preceding and current row)", None),
    ("s5", f"sum(p) over ({P} rows between 5 preceding and current row)", None),
    ("mn5", f"min(p) over ({P} rows between 5 preceding and current row)", None),
    ("mx5", f"max(p) over ({P} rows between 5 preceding and current row)", None),
    ("c5", f"count(p) over ({P} rows between 5 preceding and current row)", None),
    ("n5", f"count() over ({P} rows between 5 preceding and current row)", None),
    ("ar", f"avg(p) over ({P} range between '10' second preceding and current row)", f"avg(p) over ({P} range between interval '10' second preceding and current row)"),
    ("sr", f"sum(p) over ({P} range between '10' second preceding and current row)", f"sum(p) over ({P} range between interval '10' second preceding and current row)"),
    ("mnr", f"min(p) over ({P} range between '10' second preceding and current row)", f"min(p) over ({P} range between interval '10' second preceding and current row)"),
    ("mxr", f"max(p) over ({P} range between '10' second preceding and current row)", f"max(p) over ({P} range between interval '10' second preceding and current row)"),
    ("cr", f"count(p) over ({P} range between '10' second preceding and current row)", f"count(p) over ({P} range between interval '10' second preceding and current row)"),
    ("cs", f"sum(p) over ({P})", None),
    ("ca", f"avg(p) over ({P})", None),
    ("cmn", f"min(p) over ({P})", None),
    ("cmx", f"max(p) over ({P})", None),
    ("cc", f"count(p) over ({P})", None),
    ("l1", f"lag(p) over ({P})", None),
    ("l3", f"lag(p, 3, -1) over ({P})", None),
    ("fv", f"first_value(p) over ({P} rows between 3 preceding and current row)", None),
    ("lv", f"last_value(p) over ({P} rows between 3 preceding and current row)", None),
    ("e1", f"avg(p, 'alpha', 0.1) over ({P})", None),
    ("e2", f"avg(p, 'period', 20) over ({P})", None),
    ("rn", f"row_number() over ({P})", None),
]

rng = random.Random(5)
rows, ts, price = [], 1_767_225_600_000_000, {"a": 100.0, "b": 2500.0, "c": 0.5}
for _ in range(300):
    ts += rng.randint(1, 4_000_000)  # 1 us to 4 s apart: RANGE frames of 10 s hold 1 to ~20 rows
    g = rng.choice("abc")
    price[g] = round(price[g] * (1 + rng.gauss(0, 0.002)), 6)
    rows.append([ts, g, None if rng.random() < 0.08 else price[g]])

q(f"drop table if exists {TABLE}")
q(f"create table {TABLE} (ts timestamp, g symbol, p double) timestamp(ts) partition by day wal")
values = ",".join(f"({t}::timestamp, '{g}', {'null' if p is None else repr(p)})" for t, g, p in rows)
q(f"insert into {TABLE} values {values}")
for _ in range(60):  # WAL apply
    if q(f"select count() from {TABLE}")["dataset"][0][0] == len(rows):
        break
    time.sleep(0.5)
d = q(f"select {', '.join(f'{sql} {name}' for name, sql, _ in COLUMNS)} from {TABLE}")
results = d["dataset"]
assert len(results) == len(rows)

out = {
    "questdb": "10.0.1",
    "rows": rows,
    "columns": [{"name": n, "questdb": qs, "brrrrr": bs or qs} for n, qs, bs in COLUMNS],
    "results": results,
}
with io.TextIOWrapper(gzip.GzipFile(OUT, "wb", mtime=0)) as f:
    json.dump(out, f)
print(f"wrote {OUT}: {len(rows)} rows, {len(COLUMNS)} columns")
