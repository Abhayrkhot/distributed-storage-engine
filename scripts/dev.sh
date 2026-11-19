#!/usr/bin/env bash
# Runs a command inside the kvdev toolchain container (Ubuntu 24.04 packages,
# same as CI) with the repo mounted at /src and a host data directory at /data.
# Benchmarks write under /data so fsync goes to the host filesystem, not the
# container's overlay.
#
#   scripts/dev.sh 'cmake -S . -B build -G Ninja && cmake --build build'
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
image="${KV_DEV_IMAGE:-kvdev:24.04}"
data="${KV_DATA:-$HOME/kvdata}"
mkdir -p "$data"
if ! docker image inspect "$image" >/dev/null 2>&1; then
  docker build -t "$image" -f "$repo/docker/dev.Dockerfile" "$repo/docker"
fi
tty=()
[ -t 0 ] && tty=(-it)
exec docker run --rm "${tty[@]}" --network host --ulimit nofile=65536:65536 \
  --security-opt seccomp=unconfined \
  -u "$(id -u):$(id -g)" -e HOME=/tmp/home -e TMPDIR=/data/tmp \
  -v "$repo:/src" -v "$data:/data" -w /src "$image" \
  bash -c "mkdir -p /tmp/home /data/tmp; $*"
