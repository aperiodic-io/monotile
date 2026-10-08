#!/usr/bin/env python3
"""Deterministic synthetic trades, written twice: as length-delimited `Trade` protobuf
messages (for Kafka, then brrrrr) and as InfluxDB line protocol (for QuestDB's ILP port).

    gen.py <n> <out-dir> [--symbols 1000] [--seed 7]

Timestamps start at 2026-09-01T00:00:00Z and advance by 0-2 ms per trade (1 ms on average), in
order. Symbols are drawn with a skew (a few busy, a long quiet tail), each with its own random walk.
"""

import argparse
import math
import os
import random
import struct

START_US = 1_788_220_800_000_000  # 2026-09-01T00:00:00Z


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def string(tag, s):
    b = s.encode()
    return bytes([tag]) + varint(len(b)) + b


def trade_record(time, id_, exchange, symbol, price, local_ts, side, quantity, amount):
    # market.proto Trade: time=1 id=2 exchange=3 symbol=4 price=5 local_timestamp=6 side=7
    # quantity=8 amount=9
    return b"".join(
        (
            b"\x08" + varint(time),
            string(0x12, id_),
            b"\x18" + varint(exchange),
            string(0x22, symbol),
            b"\x29" + struct.pack("<d", price),
            b"\x30" + varint(local_ts),
            string(0x3A, side),
            b"\x41" + struct.pack("<d", quantity),
            b"\x49" + struct.pack("<d", amount),
        )
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("n", type=int)
    ap.add_argument("out")
    ap.add_argument("--symbols", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    names = [f"SYM{i:04d}USDT" for i in range(a.symbols)]
    # skewed popularity: weight 1/(rank+1)
    cum, total = [], 0.0
    for i in range(a.symbols):
        total += 1.0 / (i + 1)
        cum.append(total)
    prices = [10 ** rng.uniform(-2, 5) for _ in names]
    os.makedirs(a.out, exist_ok=True)
    ts = START_US
    with open(os.path.join(a.out, "trades.pb"), "wb") as pb, open(os.path.join(a.out, "trades.ilp"), "w") as ilp:
        for i in range(a.n):
            ts += rng.randint(0, 2000)
            u = rng.random() * total
            lo, hi = 0, a.symbols - 1
            while lo < hi:
                mid = (lo + hi) // 2
                if cum[mid] < u:
                    lo = mid + 1
                else:
                    hi = mid
            s = lo
            prices[s] *= math.exp(rng.gauss(0, 1e-4))
            price = float(f"{prices[s]:.6g}")
            quantity = float(f"{rng.expovariate(1.0):.4f}") + 0.0001
            amount = quantity
            side = "buy" if rng.random() < 0.5 else "sell"
            exchange = 1 + (s % 3)
            time = ts - rng.randint(1_000, 50_000)  # exchange time, before our local receive time
            msg = trade_record(time, str(i), exchange, names[s], price, ts, side, quantity, amount)
            pb.write(varint(len(msg)) + msg)
            ilp.write(
                f'trades,symbol={names[s]},side={side} exchange={exchange}i,id="{i}",price={price!r},'
                f"quantity={quantity!r},amount={amount!r},time={time}i {ts * 1000}\n"
            )


if __name__ == "__main__":
    main()
