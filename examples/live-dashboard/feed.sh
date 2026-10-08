#!/usr/bin/env bash
# Synthetic trades and quotes, written to the topics `trades` and `quotes` as JSON, one per
# message (`rpk topic produce -f '%t %v\n'`: the topic, then the message). Every 100 ms each
# symbol's mid takes a random step; a quote follows it, with a spread of one to three ticks;
# zero to four trades print at the bid or the ask.
set -euo pipefail
awk -v seed="$RANDOM" '
BEGIN {
  srand(seed)
  n = split("BTC ETH SOL", sym, " "); split("60000 3000 150", mid, " "); split("0.1 0.01 0.001", tick, " ")
  for (;;) {
    cmd = "date -u +%Y-%m-%dT%H:%M:%S.%3NZ"; cmd | getline ts; close(cmd)
    for (i = 1; i <= n; i++) {
      mid[i] *= 1 + (rand() - 0.5) * 0.0006
      half = tick[i] * int(1 + rand() * 3) / 2
      bid = mid[i] - half; ask = mid[i] + half
      printf "quotes {\"ts\":\"%s\",\"symbol\":\"%s\",\"bid\":%.4f,\"ask\":%.4f,\"bid_size\":%.3f,\"ask_size\":%.3f}\n", ts, sym[i], bid, ask, rand() * 5, rand() * 5
      k = int(rand() * 5)
      for (j = 0; j < k; j++) {
        buy = rand() < 0.5
        printf "trades {\"ts\":\"%s\",\"symbol\":\"%s\",\"side\":\"%s\",\"price\":%.4f,\"size\":%.4f}\n", ts, sym[i], buy ? "buy" : "sell", buy ? ask : bid, -log(rand()) * (i == 1 ? 0.05 : i == 2 ? 0.8 : 20)
      }
    }
    fflush()
    system("sleep 0.1")
  }
}' | rpk -X brokers=redpanda:9092 topic produce -f '%t %v\n' >/dev/null
