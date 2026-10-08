import datetime, sys, traceback
DSN = "host=127.0.0.1 port=5433 dbname=brrrrr user=brrrrr" + (f" password={sys.argv[1]}" if len(sys.argv) > 1 else "")
results = []
def check(name, f):
    try:
        r = f()
        results.append(("ok", name, repr(r)[:200]))
    except Exception as e:
        results.append(("FAIL", name, f"{type(e).__name__}: {str(e).strip()[:300]}"))

import psycopg
def p3():
    c = psycopg.connect(DSN, autocommit=True)
    cur = c.cursor()
    check("psycopg3 simple", lambda: cur.execute("SELECT * FROM bars ORDER BY minute, symbol LIMIT 2").fetchall())
    check("psycopg3 types", lambda: [type(v).__name__ for v in cur.execute("SELECT minute, symbol, open, volume FROM bars LIMIT 1").fetchone()])
    check("psycopg3 param text", lambda: cur.execute("SELECT count(*) FROM trades WHERE symbol = %s", ["BTC"]).fetchone())
    check("psycopg3 param int", lambda: cur.execute("SELECT symbol, price FROM trades WHERE size > %s LIMIT 2", [0]).fetchall())
    check("psycopg3 param float", lambda: cur.execute("SELECT count(*) FROM trades WHERE price > %s", [101.5]).fetchone())
    check("psycopg3 param timestamp", lambda: cur.execute("SELECT count(*) FROM trades WHERE ts >= %s", [datetime.datetime(2024,1,2,9,35,tzinfo=datetime.timezone.utc)]).fetchone())
    check("psycopg3 param naive ts", lambda: cur.execute("SELECT count(*) FROM trades WHERE ts >= %s", [datetime.datetime(2024,1,2,9,35)]).fetchone())
    check("psycopg3 null result", lambda: cur.execute("SELECT NULL AS n, 1 AS one").fetchone())
    check("psycopg3 binary", lambda: c.cursor(binary=True).execute("SELECT minute, open FROM bars LIMIT 1").fetchone())
    check("psycopg3 error", lambda: cur.execute("SELECT nope FROM trades").fetchone())
    check("psycopg3 after error", lambda: cur.execute("SELECT 1").fetchone())
    check("psycopg3 version", lambda: cur.execute("SELECT version()").fetchone())
    check("psycopg3 server_version", lambda: c.info.server_version)
    check("psycopg3 txn", lambda: psycopg.connect(DSN).execute("SELECT count(*) FROM trades").fetchone())
    check("psycopg3 prepare", lambda: [cur.execute("SELECT count(*) FROM trades WHERE symbol = %s", [s], prepare=True).fetchone() for s in ["BTC","ETH","SOL"]])
p3()

import psycopg2
def p2():
    c = psycopg2.connect(DSN); c.autocommit = False
    cur = c.cursor()
    def q(sql, args=None):
        cur.execute(sql, args); return cur.fetchall()
    check("psycopg2 simple", lambda: q("SELECT * FROM bars ORDER BY minute, symbol LIMIT 2"))
    check("psycopg2 types", lambda: [type(v).__name__ for v in q("SELECT minute, symbol, open, volume FROM bars LIMIT 1")[0]])
    check("psycopg2 params", lambda: q("SELECT count(*) FROM trades WHERE symbol = %s AND price > %s AND size > %s AND ts >= %s", ("BTC", 101.5, 0, datetime.datetime(2024,1,2,9,35,tzinfo=datetime.timezone.utc))))
    check("psycopg2 null", lambda: q("SELECT NULL AS n"))
    check("psycopg2 error", lambda: q("SELECT nope FROM trades"))
    c.rollback()
    check("psycopg2 after error", lambda: q("SELECT 1"))
    check("psycopg2 server_version", lambda: c.server_version)
p2()
for r in results: print(*r, sep=" | ")
