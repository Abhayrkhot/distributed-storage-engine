# Runtime image for kvd/kvctl/kvload/kvchaos.
#   docker build -t kvd -f docker/Dockerfile .
FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates cmake ninja-build g++ protobuf-compiler protobuf-compiler-grpc \
      libprotobuf-dev libgrpc++-dev libabsl-dev libgtest-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY proto proto
COPY src src
COPY bench bench
COPY tests tests
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DKV_WITH_ROCKSDB=OFF -DKV_BUILD_TESTS=OFF \
    && cmake --build build --target kvd kvctl kvload kvchaos

FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 libabsl20220623t64 curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /var/lib/kvd kvd \
    && mkdir -p /var/lib/kvd && chown kvd /var/lib/kvd
COPY --from=build /src/build/kvd /src/build/kvctl /src/build/kvload /src/build/kvchaos /usr/local/bin/
USER kvd
VOLUME /var/lib/kvd
EXPOSE 7001 9100
ENTRYPOINT ["kvd", "--dir=/var/lib/kvd"]
