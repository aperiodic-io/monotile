#!/usr/bin/env bash
# Every POC end to end, and the attack on each (README.md). Needs cargo, the wasm32-unknown-unknown
# target (rustup target add wasm32-unknown-unknown), age and gdb.
set -eu
cd "$(dirname "$0")"
export RUST_BACKTRACE=0 QUERY_FILE=$PWD/demo/alpha.sql
W=$(mktemp -d) R=$PWD/target/release T=$PWD/../../fixtures/cookbook/trades.csv
MOD="trades=$T=ts:time,symbol:str,price:f64,size:f64,side:str"
say() { printf '\n== %s\n' "$*"; }
seal() { age -r "$(age-keygen -y "$1")"; }
cargo build --release -q
cargo build --release -q -p guest --target wasm32-unknown-unknown
WASM=$PWD/target/wasm32-unknown-unknown/release/guest.wasm
age-keygen -o "$W/runner.key" 2>/dev/null
age-keygen -o "$W/client.key" 2>/dev/null
CLIENT=$(age-keygen -y "$W/client.key")

say "1. sealed SQL: the runner opens it in memory, seals the result to the client"
seal "$W/runner.key" < demo/alpha.sql > "$W/q.age"
$R/runner --key "$W/runner.key" --query "$W/q.age" --to "$CLIENT" -- trades="$T" | age -d -i "$W/client.key" | head -3
echo "-- attack: the operator holds the runner's key"
age -d -i "$W/runner.key" "$W/q.age" | head -2
echo "-- an opaque query reading what it may not: refused before it runs"
for q in "SELECT * FROM 'https://example.com/leak?k=1'" "SELECT * FROM '/etc/passwd'"; do
  echo "$q" | seal "$W/runner.key" > "$W/bad.age"
  $R/runner --key "$W/runner.key" --query "$W/bad.age" --to "$CLIENT" -- trades="$T" | age -d -i "$W/client.key"
done

say "2. obfuscated SQL: comments gone, names meaningless, the same rows"
$R/obfuscate --keep ts,symbol,price,size,side < demo/alpha.sql 2> "$W/map" | tee "$W/obf.sql"
seal "$W/runner.key" < "$W/obf.sql" > "$W/obf.age"
$R/runner --key "$W/runner.key" --query "$W/obf.age" --to "$CLIENT" -- trades="$T" | age -d -i "$W/client.key" | head -2

say "3. compiled upfront: the query encrypted inside a stripped binary"
$R/baked trades="$T" | head -2
echo "-- strings finds the query: $(strings $R/baked | grep -c 'WITH flow' || true) times"
echo "-- attack: stop it at its first write, dump its memory"
gdb -q -batch -ex 'catch syscall write' -ex run -ex "generate-core-file $W/core" -ex kill --args $R/baked trades="$T" > /dev/null 2>&1
strings "$W/core" | grep -m1 -A4 'WITH flow'
echo "-- attack without reading anything: the operator feeds the data, and reads the answers"
for s in 2.4 2.5 2.6 10; do
  printf 'ts,symbol,price,size,side\n2024-01-01T00:00:00,X,1,%s,buy\n' $s > "$W/probe.csv"
  echo "one buy of $s -> fade $($R/baked trades="$W/probe.csv" | tail -n +2 | cut -d, -f4)"
done

say "4. a WebAssembly module loaded at run time"
$R/wasm_host "$WASM" "$MOD" | head -2
echo "-- strings finds the query: $(strings "$WASM" | grep -c 'WITH flow') times"

say "5. a native module (.so) loaded at run time"
$R/so_host $R/libguest.so "$MOD" | head -2
echo "-- a module that reads a host file into its answer"
cargo build --release -q -p guest --features exfil
cargo build --release -q -p guest --features exfil --target wasm32-unknown-unknown
echo "native: $($R/so_host $R/libguest.so "$MOD" | tail -1)"
echo "wasm:   $($R/wasm_host "$WASM" "$MOD" 2> /dev/null | tail -1)"
cargo build --release -q -p guest
cargo build --release -q -p guest --target wasm32-unknown-unknown

say "6. a trusted execution environment (simulated): the key goes only to an attested image"
export NSM_DIR=$W
age-keygen -o "$W/query.key" 2>/dev/null
seal "$W/query.key" < demo/alpha.sql > "$W/q6.age"
# the client builds the published image itself (reproducibly) and allows its measurement
IMAGE=$(sha256sum $R/enclave | cut -d' ' -f1)
KMS="$R/kms --key $W/query.key --allow $IMAGE --vendor $($R/nsm vendor)"
cp $R/enclave "$W/enclave"
"$W/enclave" --nsm $R/nsm --kms "$KMS" --query "$W/q6.age" --to "$CLIENT" -- trades="$T" 2> /dev/null \
  | age -d -i "$W/client.key" | head -3
echo "-- attack: the operator builds an enclave that prints the query"
cargo build --release -q -p sealed --features leak --bin enclave
$R/enclave --nsm $R/nsm --kms "$KMS" --query "$W/q6.age" --to "$CLIENT" -- trades="$T" 2>&1 > /dev/null | grep -v attested || true
cargo build --release -q -p sealed --bin enclave
echo "-- attack: the operator signs an attestation for the allowed image itself"
mkdir -p "$W/own"
DOC=$(NSM_DIR=$W/own $R/nsm attest "$CLIENT" | sed "s/\"measurement\":\"[0-9a-f]*\"/\"measurement\":\"$IMAGE\"/")
echo "$DOC" | $KMS 2>&1 > /dev/null || true
echo "-- what the operator saw: the attestation, a sealed key, a sealed query and a sealed result"
rm -rf "$W"
