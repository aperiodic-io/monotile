"""Synthetic market data for the SQL benchmark: a day of trades and quotes for N symbols, in
time order, as Parquet (zstd), the way an archive keeps them.

    python3 gen.py OUT_DIR [TRADES] [SYMBOLS]
"""
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000_000
symbols = int(sys.argv[3]) if len(sys.argv) > 3 else 100
rng = np.random.default_rng(7)
day_us = 86_400_000_000
start = 1_704_067_200_000_000  # 2024-01-01

# skewed activity: a few symbols trade most
weights = 1.0 / np.arange(1, symbols + 1) ** 1.1
weights /= weights.sum()


def stream(rows, extra):
    ts = np.sort(rng.integers(start, start + day_us, rows))
    sym = rng.choice(symbols, rows, p=weights)
    base = 10.0 + sym * 7.3
    walk = np.cumsum(rng.normal(0, 0.0005, rows))
    price = np.round(base * np.exp(walk), 4)
    cols = {
        "ts": pa.array(ts, pa.timestamp("us", "UTC")),
        "symbol": pa.array([f"S{s:03d}" for s in sym]).dictionary_encode(),
    }
    cols.update(extra(rows, price))
    return pa.table(cols)


trades = stream(
    n,
    lambda rows, price: {
        "price": price,
        "size": np.round(rng.exponential(5.0, rows), 3),
        "side": pa.array(np.where(rng.random(rows) < 0.5, "buy", "sell")).dictionary_encode(),
    },
)
quotes = stream(
    n * 2,
    lambda rows, price: {
        "bid": np.round(price * (1 - 0.0002), 4),
        "ask": np.round(price * (1 + 0.0002), 4),
        "bid_size": np.round(rng.exponential(20.0, rows), 2),
        "ask_size": np.round(rng.exponential(20.0, rows), 2),
    },
)
for name, t in [("trades", trades), ("quotes", quotes)]:
    pq.write_table(t, f"{out}/{name}.parquet", compression="zstd", row_group_size=1_000_000)
    print(name, t.num_rows)
