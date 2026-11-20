# Benchmark runtime: the built binaries plus their Ubuntu 24.04 runtime
# libraries, for running scripts/bench-*.sh on hosts with a different distro
# (e.g. as an Apptainer image on an HPC node). The context is a directory with
# the binaries under build/ (the repo's .dockerignore excludes build dirs):
#   mkdir -p ctx/build && cp build/{kvd,kvctl,kvload,kvchaos,kvbench} ctx/build/
#   docker build -t kvbench-rt -f docker/bench.Dockerfile ctx
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 libabsl20220623t64 librocksdb8.9 python3 procps util-linux \
    && rm -rf /var/lib/apt/lists/*
COPY build/kvd build/kvctl build/kvload build/kvchaos build/kvbench /opt/kv/build/
