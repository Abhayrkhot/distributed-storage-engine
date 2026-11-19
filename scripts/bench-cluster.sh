#!/usr/bin/env bash
# Replicated throughput and latency: a 3-node kvd cluster on this host and
# kvload against the leader. Every write is fsynced on a quorum (leader + 1
# follower) before it is acknowledged.
set -euo pipefail
BUILD=${BUILD:-build}
OUT=${OUT:-results/raw/cluster.jsonl}
DATA=${DATA:-/data/cluster}
BASE=${BASE:-7301}
DURATION=${DURATION:-10}
ROUNDS=${ROUNDS:-3}
mkdir -p "$(dirname "$OUT")"
: > "$OUT"
MEMBERS="1=127.0.0.1:$BASE,2=127.0.0.1:$((BASE+1)),3=127.0.0.1:$((BASE+2))"
EPS="127.0.0.1:$BASE,127.0.0.1:$((BASE+1)),127.0.0.1:$((BASE+2))"
PIDS=()

start() {
  rm -rf "$DATA"; mkdir -p "$DATA"
  for i in 1 2 3; do
    "$BUILD/kvd" --id=$i --members="$MEMBERS" --dir="$DATA/n$i" --log_level=warning \
      2>"$DATA/n$i.log" &
    PIDS+=($!)
  done
  for _ in $(seq 1 100); do
    "$BUILD/kvctl" --endpoints="$EPS" status 2>/dev/null | grep -q "role=leader" && return 0
    sleep 0.1
  done
  echo "no leader" >&2; return 1
}
stop() { kill -TERM "${PIDS[@]}" 2>/dev/null || true; wait "${PIDS[@]}" 2>/dev/null || true; PIDS=(); }
trap stop EXIT

run() {  # label kvload-args...
  local label=$1; shift
  "$BUILD/kvload" --endpoints="$EPS" --duration="$DURATION" --warmup=2 --label="$label" "$@" > /tmp/kvload.out
  grep '^RESULT' /tmp/kvload.out | sed 's/^RESULT //' | python3 scripts/annotate.py >> "$OUT"
  grep -E '^(writes|reads|errors)' /tmp/kvload.out | sed "s/^/[$label] /"
}

for r in $(seq 1 "$ROUNDS"); do
  start
  run raft3-c32-d1        --clients=32 --depth=1
  run raft3-c64-d16       --clients=64 --depth=16
  run raft3-c64-d16-sess  --clients=64 --depth=16 --sessions
  run raft3-c64-d16-batch16 --clients=64 --depth=16 --batch=16
  run raft3-mixed-linear  --clients=64 --depth=16 --read_ratio=0.5 --consistency=linearizable
  run raft3-mixed-lease   --clients=64 --depth=16 --read_ratio=0.5 --consistency=lease
  stop
done
