#!/bin/bash
# Runs the benchmark suite on one exclusive HPC node, inside the runtime image
# from docker/bench.Dockerfile (via Apptainer), with all data on node-local
# storage. Stage these two files in $STAGE first:
#   docker save kvbench-rt -o kvbench-rt.tar
#   tar --exclude='./build*' --exclude=./results --exclude=./.git -czf repo.tgz .
# then:  sbatch -p <partition> --constraint=<node type> scripts/slurm/kvbench.sbatch
#SBATCH -J kvbench
#SBATCH --exclusive
#SBATCH -N 1
#SBATCH --mem=0
#SBATCH --time=02:00:00
set -euo pipefail
STAGE=${STAGE:-$HOME/kvbench}
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY   # gRPC would honour them
export no_proxy=localhost,127.0.0.1
W=${TMPDIR:-/tmp}/kvbench-$SLURM_JOB_ID
mkdir -p "$W/data" "$W/repo" "$W/apptmp"
trap 'rm -rf "$W"' EXIT
export APPTAINER_TMPDIR=$W/apptmp APPTAINER_CACHEDIR=$W/apptmp
cd "$W"
echo "node $(hostname) start $(date -Is)"
apptainer build -F kvrt.sif "docker-archive://$STAGE/kvbench-rt.tar" > /dev/null
tar -xzf "$STAGE/repo.tgz" -C repo
cd repo
run() { apptainer exec --bind "$W/data:/data" --env BUILD=/opt/kv/build "$W/kvrt.sif" "$@"; }
run scripts/host-info.sh || echo "host-info failed (continuing)"
for s in ${SUITES:-engine contention recovery cluster chaos}; do
  echo "=== $s $(date -Is) load=$(cut -d' ' -f1 /proc/loadavg)"
  run "scripts/bench-$s.sh" 2>&1 | grep -vE '^[IW][0-9]' || true
done
mkdir -p "$STAGE/results-$SLURM_JOB_ID"
cp -r results "$STAGE/results-$SLURM_JOB_ID/"
echo "done $(date -Is)"
