# Toolchain image for building, testing and benchmarking. Everything comes from
# Ubuntu 24.04 packages so CI (ubuntu-24.04 runners) and local builds match.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates git cmake ninja-build pkg-config g++ clang \
      protobuf-compiler protobuf-compiler-grpc libprotobuf-dev libgrpc++-dev \
      libabsl-dev librocksdb-dev libgtest-dev libgmock-dev \
      python3 procps iproute2 strace gdb \
    && rm -rf /var/lib/apt/lists/*
