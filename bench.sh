#!/usr/bin/env bash
# Reproducible benchmark suite for rushort.
# Usage: ./bench.sh [bind_addr]   (default 127.0.0.1:8080)
# Requires: python3 or curl for seeding, ab (ApacheBench) optional for the round-trip test.
set -euo pipefail

BIND="${1:-127.0.0.1:8080}"
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo "== building release =="
cargo build --release

echo "== starting server on $BIND =="
./target/release/shortener --bind "$BIND" >/tmp/rushort-bench-server.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  if curl -fsS "http://$BIND/health" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -fsS "http://$BIND/health" >/dev/null

CODE=$(curl -fsS -X POST -H 'content-type: application/json' \
  -d '{"url":"https://example.com/bench"}' "http://$BIND/api/shorten" \
  | sed -E 's/.*"code":"([^"]+)".*/\1/')
echo "seed code: $CODE"

if command -v ab >/dev/null 2>&1; then
  echo
  echo "== round-trip, ApacheBench keep-alive (c=64, n=500000) =="
  ab -k -n 500000 -c 64 -q "http://$BIND/$CODE" | grep -E "Requests per second|Failed requests|50%|99%"
fi

echo
echo "== round-trip mixed 95% GET / 5% POST (saturate, 64 conns) =="
./target/release/loadgen --target "http://$BIND" --mode saturate --duration 8 \
  --connections 64 --seed 1000 --write-ratio 0.05 | grep -E "requests:|achieved:|latency all|RESULT"

echo
echo "== pipelined open-loop @ 500k req/s for 10s =="
./target/release/loadgen --target "http://$BIND" --rps 500000 --duration 10 \
  --connections 32 --pipeline 128 --seed 1000 | grep -E "requests:|achieved:|RESULT"

echo
echo "== pipelined open-loop @ 1M req/s for 10s =="
./target/release/loadgen --target "http://$BIND" --rps 1000000 --duration 10 \
  --connections 32 --pipeline 128 --seed 1000 | grep -E "requests:|achieved:|RESULT"

echo
echo "== pipelined open-loop @ 10M req/s for 10s =="
./target/release/loadgen --target "http://$BIND" --rps 10000000 --duration 10 \
  --connections 32 --pipeline 128 --seed 1000 | grep -E "requests:|achieved:|RESULT"

echo
echo "== pipelined saturate (peak) =="
./target/release/loadgen --target "http://$BIND" --mode saturate --duration 10 \
  --connections 32 --pipeline 128 --seed 1000 | grep -E "requests:|achieved:|RESULT"

echo
echo "== done =="
