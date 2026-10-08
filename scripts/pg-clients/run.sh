#!/usr/bin/env bash
# `brrrrr serve`'s PostgreSQL protocol as real clients use it, each in Docker on the host network:
# psycopg 3 and psycopg2, the PostgreSQL JDBC driver, node-postgres, pgx, and Grafana (a
# provisioned data source, its health check and panel queries with its time macros). Each prints
# `ok | check | value` or `FAIL | check | why`; the `error` checks show brrrrr's message.
#   scripts/pg-clients/run.sh [path to brrrrr] [token]
# Uses ports 4242, 5433 and 3000; leaves the images it pulled (python, node, golang,
# eclipse-temurin, grafana/grafana) for the next run.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
bin="${1:-target/debug/brrrrr}"
token="${2:-}"
data="$(mktemp -d)"
args=(serve --data "$data" --http 127.0.0.1:4242 --pg 127.0.0.1:5433)
auth=()
if [ -n "$token" ]; then args+=(--token "$token"); auth=(-H "Authorization: Bearer $token"); fi
"$bin" "${args[@]}" > "$data/serve.log" 2>&1 &
serve=$!
trap 'kill $serve; docker rm -f brrrrr-pg-grafana >/dev/null 2>&1 || true; rm -rf "$data"' EXIT
until curl -s 127.0.0.1:4242/health > /dev/null; do sleep 0.2; done
# 600 trades of 3 symbols a second apart from 2024-01-02 09:30, every 7th without a note
awk 'BEGIN{for(i=0;i<600;i++){s=(i%3==0)?"BTC":((i%3==1)?"ETH":"SOL"); printf "{\"ts\":\"2024-01-02T09:%02d:%02dZ\",\"symbol\":\"%s\",\"price\":%.2f,\"size\":%.3f,\"note\":%s}\n", 30+int(i/60), i%60, s, 100+(i%17)/4, 0.5+(i%5)/10, (i%7==0)?"null":"\"x\""}}' > "$data/trades.jsonl"
curl -s "${auth[@]}" -X POST 127.0.0.1:4242/write/trades --data-binary @"$data/trades.jsonl"; echo
curl -s "${auth[@]}" -X POST 127.0.0.1:4242/query --data "CREATE LIVE VIEW bars AS SELECT time_bucket('1m', ts) AS minute, symbol, first(price, ts) AS open, max(price) AS high, min(price) AS low, last(price, ts) AS close, sum(size) AS volume FROM trades GROUP BY minute, symbol"; echo

echo "== psycopg 3, psycopg2"
docker run --rm --network host -v "$here:/t:ro" python:3.14-slim sh -c \
  "pip install -q --root-user-action=ignore 'psycopg[binary]' psycopg2-binary > /dev/null 2>&1; python /t/py_clients.py $token"
echo "== node-postgres"
docker run --rm --network host -v "$here/node:/t:ro" node:22-alpine sh -c "cp -r /t /n && cd /n && npm i -s pg > /dev/null 2>&1; node test.js $token"
echo "== pgx"
docker run --rm --network host -v "$here/gopgx:/t:ro" golang:1.23-alpine sh -c "cp -r /t /g && cd /g && go get github.com/jackc/pgx/v5@v5.7.1 > /dev/null 2>&1; go run . $token"
echo "== JDBC"
docker run --rm --network host -v "$here/jdbc:/t:ro" -w /t eclipse-temurin:21-jdk-alpine sh -c \
  "wget -q -O /tmp/pg.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && java -cp /tmp/pg.jar Test.java $token"
echo "== Grafana"
sed "s/password: change-me/password: '${token}'/" "$here/grafana/datasources/brrrrr.yaml" > "$data/brrrrr.yaml"
docker run -d --name brrrrr-pg-grafana --network host -v "$data/brrrrr.yaml:/etc/grafana/provisioning/datasources/brrrrr.yaml:ro" \
  grafana/grafana:13.2.3 > /dev/null
until curl -s localhost:3000/api/health | grep -q ok; do sleep 1; done
curl -s -u admin:admin localhost:3000/api/datasources/uid/brrrrr/health; echo
q="$here/grafana/query.sh"
$q 'SELECT minute AS time, symbol, close FROM bars WHERE $__timeFilter(minute) ORDER BY minute'
$q "SELECT \$__timeGroupAlias(ts, '5m'), symbol AS metric, avg(price) AS price FROM trades WHERE \$__timeFilter(ts) GROUP BY 1, 2 ORDER BY 1"
$q 'SELECT ts AS time, price FROM trades WHERE ts >= $__timeFrom() AND ts < $__timeTo() ORDER BY ts LIMIT 5' table
