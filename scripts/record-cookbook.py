"""Records DuckDB's answer to every cookbook recipe (fixtures/cookbook/cookbook.sql) over the
cookbook's files, as `fixtures/cookbook/expected/<name>.csv`: the reference brrrrr's answers
are held to (crates/brrrrr-lake/tests/cookbook.rs). A recipe's `-- duckdb:` lines are DuckDB's
spelling of it; otherwise brrrrr's names are translated (time_bucket's width, first/last).

    docker build -t brrrrr-bench-py bench/sql
    docker run --rm -v $PWD:/w -w /w brrrrr-bench-py python3 scripts/record-cookbook.py
"""
import csv
import datetime
import os
import re
import sys

import duckdb

here = os.path.join(os.path.dirname(__file__), "..", "fixtures", "cookbook")


def recipes(text):
    for block in text.split("-- name: ")[1:]:
        lines = block.split("\n")
        name = lines[0].strip()
        sql = "\n".join(l for l in lines[1:] if not l.startswith("--")).strip().rstrip(";")
        duck = "\n".join(l[len("-- duckdb: "):] for l in lines if l.startswith("-- duckdb: "))
        yield name, sql, duck or None


UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


def translate(sql):
    sql = re.sub(r"time_bucket\('(\d+)([smhd])'", lambda m: f"time_bucket(INTERVAL '{m.group(1)} {UNITS[m.group(2)]}'", sql)
    sql = re.sub(r"\bfirst\(", "arg_min(", sql)
    return re.sub(r"\blast\(", "arg_max(", sql)


def cell(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ")
    return str(v)


def main():
    os.chdir(here)
    os.makedirs("expected", exist_ok=True)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    only = sys.argv[1:]
    for name, sql, duck in recipes(open("cookbook.sql").read()):
        if only and name not in only:
            continue
        q = duck or translate(sql)
        cur = con.execute(q)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        with open(f"expected/{name}.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows([[cell(v) for v in r] for r in rows])
        print(f"{name}: {len(rows)} rows")


main()
