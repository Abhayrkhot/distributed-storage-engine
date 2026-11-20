# Distributed Storage Engine

A replicated key-value store in C++20. Each node runs a group-commit write-ahead
log, a sharded in-memory index with copy-on-write snapshots, parallel crash
recovery, and Raft replication over gRPC with quorum-acknowledged, exactly-once
writes and linearizable reads.

## Results

Every number is produced by a script in `scripts/` (see
[results/README.md](results/README.md)). Each suite ran on two hosts. One is a
dedicated server with fast, uncontended flushes but 2.2 GHz cores. The other
is a desktop CPU that is about twice as fast per core, sharing its NVMe with
other services. All writes are fdatasync-durable before they are acknowledged.

| | Dedicated server ([details](docs/results.md)) | Desktop, shared disk ([details](docs/results-shared-box.md)) |
|---|---|---|
| **Durable writes, 16 threads, pipelined** (256 in flight per thread) | **608K/s**, 4.1× RocksDB 8.9 at 1,024 threads | **879K/s**, 8.6× RocksDB |
| **Durable writes, 16 blocking threads** | **35.1K/s**, 2.2× RocksDB (16.2K/s) | 7.3K/s (flush-bound on the shared disk), 2.7× RocksDB |
| **Crash recovery, 1M records** (SIGKILL → serving, every key verified) | **0.28 s** (0.59 s cold cache); 10.5M in 3.3 s | **0.15 s** (0.25 s cold); 10.5M in 1.14 s |
| **Leader failure** (99 kills under 16 writers, 3-node Raft) | **0 lost acknowledged writes, 0 stale reads**; failover p50 **162 ms**, max 205 ms; graceful handover 9 ms | **0 lost, 0 stale**; failover p50 172 ms, max 272 ms; handover 44 ms |
| **Lock contention, p99 write latency at 32 clients** | single lock **38 ms → 4.6 ms** sharded + pipelined (4.0K → 18.5K writes/s) | 63 ms → 5.8 ms (0.6K → 8.9K writes/s) |
| **Replicated writes** (3 nodes, quorum fsync) | 8.7K/s at 32 clients, p99 6.8 ms (1,024 in flight saturates gRPC on this CPU; see details) | 60K/s per RPC at 1,024 in flight, p99 29 ms; 649K keys/s with 16-key batches |

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
lines to `results/raw/`; `scripts/report.py --raw results/raw` renders them. The
published data and how each set was produced are in `results/README.md`, and
`scripts/slurm/kvbench.sbatch` runs the whole suite on an exclusive HPC node):

```bash
scripts/dev.sh scripts/host-info.sh
scripts/dev.sh scripts/bench-engine.sh       # durable write throughput, RocksDB baseline, ablations
scripts/dev.sh scripts/bench-contention.sh   # 32 clients over gRPC: global lock vs sharded/pipelined
scripts/dev.sh scripts/bench-recovery.sh     # SIGKILL + restart with 1M / 10M keys
scripts/dev.sh scripts/bench-cluster.sh      # 3-node replicated throughput/latency
scripts/dev.sh scripts/bench-chaos.sh        # 5 min of leader kills under load
python3 scripts/report.py --raw results/raw --out docs/results.md
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
