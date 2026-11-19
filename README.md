# Distributed Storage Engine

A replicated key-value store in C++20: a group-commit write-ahead log, a sharded
in-memory index with copy-on-write snapshots, parallel crash recovery, and Raft
replication over gRPC.

> Work in progress. The storage engine is complete and tested; replication,
> the server and the full benchmark write-up land in the next milestones.

## Storage engine

```
caller ──Append──▶ WAL queue ──writer: one pwrite + one fdatasync per group──▶
Applier (N workers, routed by shard) ──▶ memtable updated, caller acknowledged
```

- **Group commit.** Writers only take a short queue lock. One writer thread
  drains everything queued since its last pass into one `pwrite` and one
  `fdatasync`, so a single flush covers hundreds of writes under load.
- **Pre-zeroed, recycled WAL segments.** Appends overwrite blocks that already
  exist, so `fdatasync` does not have to journal a file-size change. Segments
  are zero-filled with `O_DIRECT` so ext4's ordered-mode journal never makes
  the WAL's flush wait on the zeroing. The segment number is folded into each
  record's CRC32C, so stale records in a recycled file never verify.
- **Sharded index.** 256 B-tree shards, each behind its own reader/writer lock.
- **Parallel apply.** Operations are routed to the worker that owns their shard,
  which keeps per-key order while completions run on several threads.
- **Point-in-time snapshots without stopping writes.** The first change to each
  key after a snapshot starts saves the old value in a per-shard undo map, so
  the serializer emits exactly the state at the snapshot index.
- **Crash recovery.** Load the newest valid snapshot (files checked in parallel
  against their CRC32C), then replay the WAL tail with decode and apply spread
  over all cores. A torn tail is dropped; a gap in log indexes anywhere else
  fails recovery instead of silently losing acknowledged writes.

## Building

Everything builds from Ubuntu 24.04 packages (gRPC 1.51, protobuf 3.21,
Abseil, RocksDB 8.9 for the baseline, GoogleTest):

```bash
docker build -t kvdev:24.04 -f docker/dev.Dockerfile docker
scripts/dev.sh 'cmake -S . -B build -G Ninja && cmake --build build && ./build/kv_tests'
```

## License

MIT
