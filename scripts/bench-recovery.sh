#!/usr/bin/env bash
# Crash recovery: load N keys in a child, SIGKILL it, time Engine::Open.
# --cold evicts the data files from the page cache first (no root needed).
set -euo pipefail
BUILD=${BUILD:-build}
OUT=${OUT:-results/raw/recovery.jsonl}
DATA=${DATA:-/data/recovery}
ROUNDS=${ROUNDS:-3}
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

run() {  # label args...
  local label=$1; shift
  "$BUILD/kvbench" --workload=recover --dir="$DATA" --snapshot_trigger_mb=0 --label="$label" "$@" > /tmp/kvrec.out 2>&1
  grep '^RESULT' /tmp/kvrec.out | sed 's/^RESULT //' | python3 scripts/annotate.py >> "$OUT"
  grep -E '^recovered' /tmp/kvrec.out | sed "s/^/[$label] /"
}

for r in $(seq 1 "$ROUNDS"); do
  run 1m-wal-only-warm --records=1000000 --snapshot_before_crash=false
  run 1m-wal-only-cold --records=1000000 --snapshot_before_crash=false --cold
  run 1m-snapshot-tail-warm --records=1000000 --tail_records=100000
  run 1m-snapshot-tail-cold --records=1000000 --tail_records=100000 --cold
  run 1m-single-thread-cold --records=1000000 --tail_records=100000 --cold --recovery_threads=1
  run 10m-snapshot-tail-cold --records=10000000 --tail_records=500000 --cold
done
