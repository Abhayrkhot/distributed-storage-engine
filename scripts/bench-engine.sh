#!/usr/bin/env bash
# In-process engine benchmarks (kvbench), interleaved over ROUNDS rounds.
set -euo pipefail
BUILD=${BUILD:-build}
OUT=${OUT:-results/raw/engine.jsonl}
DATA=${DATA:-/data/bench}
DURATION=${DURATION:-6}
ROUNDS=${ROUNDS:-3}
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

run() {  # label args...
  local label=$1; shift
  "$BUILD/kvbench" --dir="$DATA" --duration="$DURATION" --label="$label" "$@" > /tmp/kvbench.out 2>&1
  grep '^RESULT' /tmp/kvbench.out | sed 's/^RESULT //' | python3 scripts/annotate.py >> "$OUT"
  grep -E '^(writes|reads|wal|rocksdb)' /tmp/kvbench.out | sed "s/^/[$label] /"
}

for r in $(seq 1 "$ROUNDS"); do
  # Durable writes from 16 threads: blocking vs pipelined, and baselines.
  run native-t16-d1     --threads=16 --depth=1
  run native-t16-d64    --threads=16 --depth=64
  run native-t16-d256   --threads=16 --depth=256
  run global-t16-d1     --threads=16 --depth=1 --concurrency=global --shards=1
  run rocksdb-t16       --threads=16 --engine=rocksdb
  run rocksdb-t1024     --threads=1024 --engine=rocksdb
  # Ablations.
  run native-t16-d64-noprealloc --threads=16 --depth=64 --preallocate=false
  run native-t16-d64-nosync     --threads=16 --depth=64 --sync=none
  run native-t16-d64-1shard-mixed --threads=16 --depth=64 --shards=1 --read_ratio=0.5
  run native-t16-d64-256shard-mixed --threads=16 --depth=64 --shards=256 --read_ratio=0.5
done
