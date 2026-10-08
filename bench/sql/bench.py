"""brrrrr against DuckDB, Polars and ClickHouse on the queries of queries.sql, each query a
process of its own (as a user runs it: `brrrrr sql "..."`, `python -c "duckdb.sql(...)"`, a
Polars script, `clickhouse local --query "..."`), timed whole, with its peak memory; and the
answers' row counts checked against brrrrr's.

    python3 bench.py BRRRRR_BIN DATA_DIR [--threads N] [--reps 3] [--only NAME]
                     [--engines brrrrr,duckdb,polars,clickhouse] [--json OUT]

DuckDB runs queries.sql with its spellings (`duck_sql`); Polars runs polars_queries.py, the
queries in its own API; ClickHouse runs clickhouse_queries.sql, the queries in its own SQL.
"""
import argparse
import json
import os
import re
import resource
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("brrrrr")
ap.add_argument("dir")
ap.add_argument("--threads", type=int, default=8)
ap.add_argument("--reps", type=int, default=3)
ap.add_argument("--only")
ap.add_argument("--engines", default="brrrrr,duckdb,polars,clickhouse")
ap.add_argument("--json", help="also write the results here (the website's benchmarks page)")
a = ap.parse_args()
engines = a.engines.split(",")


def named(path):
    """A file of `-- name: NAME` blocks: {name: SQL}, data paths filled in."""
    blocks = open(os.path.join(HERE, path)).read().split("-- name: ")[1:]
    return {n.strip(): s.strip().rstrip(";").replace("{dir}", a.dir) for n, s in (b.split("\n", 1) for b in blocks)}


queries = named("queries.sql")
clickhouse_sql = named("clickhouse_queries.sql")


def duck_sql(sql):
    # DuckDB's spellings of brrrrr's: time buckets take an interval, first/last are arg_min/max
    sql = re.sub(r"time_bucket\('(\d+)m'", r"time_bucket(INTERVAL '\1 minutes'", sql)
    sql = re.sub(r"time_bucket\('(\d+)h'", r"time_bucket(INTERVAL '\1 hours'", sql)
    sql = re.sub(r"\bfirst\(", "arg_min(", sql)
    return re.sub(r"\blast\(", "arg_max(", sql)


DUCK = "import duckdb,sys; c=duckdb.connect(); c.execute('SET threads={t}'); c.execute('SET enable_progress_bar=false'); r=c.sql(sys.argv[1]).fetchall(); print(len(r))"


def version(engine):
    """The engine's name and version, as the results name it."""
    run = lambda cmd: subprocess.run(cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL).stdout.strip().split("\n")[0]
    if engine == "brrrrr":
        return run([a.brrrrr, "--version"])
    if engine == "duckdb":
        return "DuckDB " + run([sys.executable, "-c", "import duckdb; print(duckdb.__version__)"])
    if engine == "polars":
        return "Polars " + run([sys.executable, "-c", "import polars; print(polars.__version__)"])
    return "ClickHouse " + run(["clickhouse", "local", "--query", "SELECT version()"])


def command(engine, name):
    """The process that runs query `name` on `engine`, and how to count its answer's rows."""
    t = str(a.threads)
    if engine == "brrrrr":
        return [a.brrrrr, "sql", "--threads", t, "--format", "csv", queries[name]], lambda out: out.count("\n") - 1
    if engine == "duckdb":
        return [sys.executable, "-c", DUCK.format(t=t), duck_sql(queries[name])], int
    if engine == "polars":
        return [sys.executable, os.path.join(HERE, "polars_queries.py"), name, a.dir], int
    q = clickhouse_sql[name]
    return ["clickhouse", "local", "--max_threads", t, "--output-format", "TSV", "--query", q], lambda out: out.count("\n")


# Polars reads its thread count from the environment, once
os.environ["POLARS_MAX_THREADS"] = str(a.threads)
# each measured in a fresh interpreter, so that the peak memory is its own
PEAK = "import resource,subprocess,sys; subprocess.run(sys.argv[1:], check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL); print(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)"


def timed(cmd):
    t = time.perf_counter()
    out = subprocess.run([sys.executable, "-c", PEAK, *cmd], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    took = time.perf_counter() - t
    if out.returncode != 0:
        raise SystemExit(f"{cmd[0]} failed: {out.stderr}")
    return took, int(out.stdout.strip())


def median(xs):
    return sorted(xs)[len(xs) // 2]


results = []
print(f"{'query':<20}" + "".join(f"{e + ' s':>13}" for e in engines) + "".join(f"{e + ' MiB':>15}" for e in engines))
for name in queries:
    if a.only and name != a.only:
        continue
    row = {"name": name, "sql": queries[name], "s": {}, "mib": {}}
    for e in engines:
        cmd, count = command(e, name)
        took, peak = [], 0
        for _ in range(a.reps):
            s, kib = timed(cmd)
            took.append(s)
            peak = max(peak, kib)
        rows = count(subprocess.run(cmd, capture_output=True, text=True, check=True, stdin=subprocess.DEVNULL).stdout)
        row.setdefault("rows", rows)
        if rows != row["rows"]:
            raise SystemExit(f"{name}: {e} gave {rows} rows, {engines[0]} {row['rows']}")
        row["s"][e] = round(median(took), 3)
        row["mib"][e] = round(peak / 1024)
    results.append(row)
    print(f"{name:<20}" + "".join(f"{row['s'][e]:13.3f}" for e in engines) + "".join(f"{row['mib'][e]:15}" for e in engines), flush=True)
if a.json:
    out = {"threads": a.threads, "reps": a.reps, "engines": [{"id": e, "name": version(e)} for e in engines], "queries": results}
    json.dump(out, open(a.json, "w"), indent=1)
