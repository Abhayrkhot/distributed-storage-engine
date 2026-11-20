# Distributed Storage Engine

A replicated key-value store in C++20. Each node runs a group-commit write-ahead
log, a sharded in-memory index with copy-on-write snapshots, parallel crash
recovery, and Raft replication over gRPC with quorum-acknowledged, exactly-once
writes and linearizable reads.

## Results

Measured with the scripts in `scripts/` on an i7-12700H with an SK hynix PC801
NVMe, shared with other services (raw flush latency during the runs was ~6×
the drive's idle figure, so flush-bound numbers are conservative). Details,
ranges and every configuration: [docs/results.md](docs/results.md).

| | Result |
|---|---|
| **Durable write throughput** (in-process, 16 threads, every write fdatasync'd) | **890K writes/s** median (up to 1.65M) with 256 writes in flight per thread; **8.8× RocksDB 8.9** (`sync=true`, 1,024 threads) on the same disk. Blocking writers: 2.2× RocksDB. |
| **Crash recovery** (SIGKILL, then `Open` until serving) | **1M records in 0.24 s** from a cold page cache (snapshot + WAL tail), 0.35 s replaying the WAL alone; **10.5M records in 1.18 s**. Every key verified after restart. |
| **Replication** (3-node Raft over gRPC) | **0 lost acknowledged writes and 0 stale linearizable reads** across 99 leader kills (75 SIGKILL, 24 graceful) under 16 writers. Failover p50 **191 ms**, max 315 ms; graceful handover 45 ms. 61.6K quorum-fsynced writes/s per RPC, 640K keys/s with 16-key batches. |
| **Lock contention** (32 gRPC clients, one request each) | Sharded rwlocks and the pipelined commit path cut **p99 write latency from 68 ms to 5.0 ms** and raise throughput from 976 to 23.8K writes/s vs the single-lock design. Mixed 50/50 reads: read p99 35 ms → 1.3 ms. |


## What's inside

| Layer | Highlights |
|---|---|
| **WAL** (`src/storage/wal.*`) | Group commit: one `pwrite` + one `fdatasync` per batch of concurrent writes. Pre-zeroed (`O_DIRECT`) and recycled 64 MiB segments, so flushes never journal a file-size change. CRC32C over segment seq + record. Fail-stop on fsync errors. |
| **MemTable** (`memtable.*`) | 256 hash shards, each an `absl::btree_map` behind its own reader/writer lock. Copy-on-write undo maps give point-in-time snapshots while writes continue. |
| **Applier** (`applier.*`) | Committed entries routed to N workers by shard: per-key order is preserved while applies and completions run in parallel. |
| **Snapshots + recovery** (`snapshot.*`, `wal_reader.*`, `engine.*`) | Multi-file snapshots written and loaded in parallel, CRC-checked. WAL replay with parallel decode/apply; torn tails dropped, index gaps fail loudly. |
| **Raft** (`src/raft/`) | PreVote, leader stickiness, CheckQuorum, quorum commit of fsynced entries, conflict-term log repair, ReadIndex and lease reads, InstallSnapshot, log compaction, leadership transfer, exactly-once client sessions. |
| **Server** (`src/server/`) | `kvd`: gRPC callback API (no thread blocks on a write), Admin service, Prometheus `/metrics`, `/healthz`, `/readyz`, graceful handover on SIGTERM. Also runs as a standalone engine, including the naive single-lock baseline. |
| **Tools** (`src/client/`, `bench/`) | Retrying client with leader redirects, `kvctl`, `kvbench` (in-process + crash/recover + RocksDB baseline), `kvload` (closed-loop gRPC load), `kvchaos` (kills leaders under load and verifies nothing acknowledged is lost). |

Design notes: [docs/architecture.md](docs/architecture.md). All measurements:
[docs/results.md](docs/results.md).

## Quick start

Build (Ubuntu 24.04 packages: gRPC 1.51, protobuf, Abseil, GoogleTest; RocksDB
only for the baseline benchmark):

```bash
docker build -t kvdev:24.04 -f docker/dev.Dockerfile docker
scripts/dev.sh 'cmake -S . -B build -G Ninja && cmake --build build && ./build/kv_tests'
```

Run a 3-node cluster (clients use the listed ports; Raft traffic uses port + 1000):

```bash
M=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
for i in 1 2 3; do ./build/kvd --id=$i --members=$M --dir=/tmp/kvd/n$i --metrics_port=910$i & done
E=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
./build/kvctl --endpoints=$E put hello world
./build/kvctl --endpoints=$E get hello            # linearizable (ReadIndex)
./build/kvctl --endpoints=$E status
./build/kvctl --endpoints=$E transfer             # graceful leadership handover
curl -s localhost:9101/metrics
```

Or with Docker Compose: `docker compose -f docker/docker-compose.yml up -d --build`.

Reproduce the benchmarks (inside the dev container; each script writes JSON
lines to `results/raw/`, and `scripts/report.py` renders `docs/results.md`):

```bash
scripts/dev.sh scripts/host-info.sh
scripts/dev.sh scripts/bench-engine.sh       # durable write throughput, RocksDB baseline, ablations
scripts/dev.sh scripts/bench-contention.sh   # 32 clients over gRPC: global lock vs sharded/pipelined
scripts/dev.sh scripts/bench-recovery.sh     # SIGKILL + restart with 1M / 10M keys
scripts/dev.sh scripts/bench-cluster.sh      # 3-node replicated throughput/latency
scripts/dev.sh scripts/bench-chaos.sh        # 5 min of leader kills under load
python3 scripts/report.py
```

## Guarantees

- A write is acknowledged only after it is durable on a majority (leader plus
  at least one follower in a 3-node cluster) and applied.
- `LINEARIZABLE` reads go through ReadIndex; `LEASE` reads use the leader
  lease, whose safety relies on bounded clock drift; `STALE` reads may lag.
- A write retried with the same `(client_id, seq)` is applied at most once.
- A node that cannot persist to disk stops instead of acknowledging.
- `Scan` reads shards one at a time, so it is not a point-in-time view across
  shards; single-key reads are linearizable.
- Membership is static, there is no authentication or TLS, and the data set
  must fit in memory (the index is an in-memory B-tree, recovered from
  snapshots and the WAL).

## Layout

```
proto/            kv.proto (client API), raft.proto (peer RPCs), snapshot.proto
src/common/       crc32c (SSE4.2/ARMv8), logging, histogram, fs helpers
src/storage/      wal, wal_reader, memtable, applier, snapshot, engine
src/raft/         raft_node, raft_state_machine, grpc_transport
src/server/       kvd main, KV/Admin services, metrics endpoint
src/client/       client library, kvctl
bench/            kvbench, kvload, kvchaos
tests/            GoogleTest suites (storage + in-process Raft clusters)
scripts/          dev container wrapper, benchmark scripts, report generator
docker/           dev image, runtime image, docker-compose cluster
```

## License

MIT
