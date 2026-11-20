#!/usr/bin/env bash
# Failure testing: SIGKILL the leader of a 3-node cluster every 3 s under 16
# writers (every 4th cycle a SIGTERM graceful handover instead) and verify no
# acknowledged write is lost and no linearizable read is stale.
set -euo pipefail
BUILD=${BUILD:-build}
OUT=${OUT:-results/raw/chaos.jsonl}
DURATION=${DURATION:-300}
mkdir -p "$(dirname "$OUT")"
[ "${APPEND:-0}" = 1 ] || : > "$OUT"
"$BUILD/kvchaos" --kvd="$BUILD/kvd" --dir=/data/chaos --duration="$DURATION" \
  --kill_interval_ms=3000 --down_ms=1000 --graceful_every=4 --workers=16 \
  --label="leader-kill-${DURATION}s" "$@" | tee /tmp/kvchaos.out
grep '^RESULT' /tmp/kvchaos.out | sed 's/^RESULT //' | python3 scripts/annotate.py >> "$OUT"
