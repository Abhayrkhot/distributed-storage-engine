#!/usr/bin/env bash
# Lock contention: 32 closed-loop gRPC clients (one request in flight each)
# against a standalone kvd, naive single-lock engine vs pipelined/sharded.
# Configurations are interleaved over ROUNDS rounds so drift in the shared
# disk's flush latency hits both equally. Run inside the dev container.
set -euo pipefail
BUILD=${BUILD:-build}
OUT=${OUT:-results/raw/contention.jsonl}
DATA=${DATA:-/data/contention}
PORT=${PORT:-7201}
CLIENTS=${CLIENTS:-32}
DURATION=${DURATION:-10}
ROUNDS=${ROUNDS:-5}
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

run() {  # label concurrency shards read_ratio
  local label=$1 conc=$2 shards=$3 rr=$4
  rm -rf "$DATA"
  "$BUILD/kvd" --mode=engine --listen=127.0.0.1:$PORT --dir="$DATA" \
    --concurrency="$conc" --shards="$shards" --log_level=warning &
  local pid=$!
  sleep 1
  "$BUILD/kvload" --endpoints=127.0.0.1:$PORT --clients="$CLIENTS" --depth=1 \
    --duration="$DURATION" --warmup=2 --read_ratio="$rr" --label="$label" > /tmp/kvload.out
  grep '^RESULT' /tmp/kvload.out | sed 's/^RESULT //' | python3 scripts/annotate.py >> "$OUT"
  grep -E '^(writes|reads)' /tmp/kvload.out | sed "s/^/[$label] /"
  kill -TERM $pid; wait $pid || true
}

for r in $(seq 1 "$ROUNDS"); do
  run global-lock global 1 0
  run pipelined pipelined 256 0
  run global-lock-mixed50 global 1 0.5
  run pipelined-mixed50 pipelined 256 0.5
done
