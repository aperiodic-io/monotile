"""The cookbook's data: small, deterministic, readable. Three symbols trade and quote for half an
hour; a reference table; a sensor's readings; an app's events. Written once and committed.

    python3 fixtures/cookbook/gen.py fixtures/cookbook
"""
import csv
import math
import random
import sys

out = sys.argv[1]
r = random.Random(42)
t0 = 1704067200  # 2024-01-01 00:00:00 UTC


def iso(sec, us):
    import datetime as d

    return (d.datetime.fromtimestamp(sec, d.timezone.utc) + d.timedelta(microseconds=us)).strftime("%Y-%m-%dT%H:%M:%S.%f")


symbols = {"BTC": 42000.0, "ETH": 2300.0, "SOL": 101.0}
quotes, trades = [], []
mid = dict(symbols)
for i in range(1800 * 4):
    sec = t0 + i // 4
    us = (i % 4) * 250_000 + r.randrange(0, 1000)
    for s in symbols:
        if r.random() < 0.35:
            mid[s] *= math.exp(r.gauss(0, 0.0004))
            half = mid[s] * (0.00005 + r.random() * 0.0001)
            quotes.append((iso(sec, us), s, round(mid[s] - half, 4), round(mid[s] + half, 4), round(r.expovariate(0.2), 3), round(r.expovariate(0.2), 3)))
        if r.random() < 0.12:
            side = "buy" if r.random() < 0.5 else "sell"
            px = mid[s] * (1 + (0.0001 if side == "buy" else -0.0001))
            trades.append((iso(sec, us + 500), s, round(px, 4), round(r.expovariate(1.5), 4), side))
trades.sort(key=lambda t: t[0])
quotes.sort(key=lambda q: q[0])
with open(f"{out}/trades.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ts", "symbol", "price", "size", "side"])
    w.writerows(trades)
with open(f"{out}/quotes.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ts", "symbol", "bid", "ask", "bid_size", "ask_size"])
    w.writerows(quotes)
with open(f"{out}/instruments.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["symbol", "name", "tick_size", "sector"])
    w.writerows([("BTC", "Bitcoin", 0.1, "L1"), ("ETH", "Ether", 0.01, "L1"), ("SOL", "Solana", 0.001, "L1"), ("DOGE", "Dogecoin", 0.00001, "meme")])
with open(f"{out}/readings.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ts", "device", "counter", "latency_ms", "temperature"])
    counter = {"a": 0, "b": 1000}
    for i in range(600):
        for d in counter:
            if d == "b" and 200 <= i < 260:
                continue  # a gap
            counter[d] += r.randrange(5, 15)
            if d == "a" and i == 400:
                counter[d] = 3  # a reset
            w.writerow([iso(t0 + i * 3, 0), d, counter[d], round(r.lognormvariate(3, 0.6), 2), round(20 + 5 * math.sin(i / 50) + r.gauss(0, 0.3), 2)])
# an app's events over three days: users visit, some sign up, fewer buy (a generator of its own,
# so the files above stay as they were)
e = random.Random(7)
events = []
for user in range(1, 121):
    day = e.randrange(0, 3)
    sec = t0 + day * 86400 + e.randrange(6 * 3600, 22 * 3600)
    steps = ["visit"] + (["signup"] if e.random() < 0.4 else [])
    steps += ["purchase"] if len(steps) == 2 and e.random() < 0.5 else []
    for step in steps:
        for _ in range(e.randrange(1, 4) if step == "visit" else 1):
            events.append((iso(sec, 0), f"u{user:03d}", step, e.choice(["/", "/pricing", "/docs", "/blog"]), e.randrange(200, 9000)))
            sec += e.randrange(20, 900)
events.sort()
with open(f"{out}/events.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ts", "user_id", "event", "page", "duration_ms"])
    w.writerows(events)
print(len(trades), "trades", len(quotes), "quotes", len(events), "events")
