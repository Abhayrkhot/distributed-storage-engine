# Architecture

```
            clients (kvctl, kvload, kv_client library)
                 │  gRPC: KV.Put/Get/Delete/Write/Scan, Admin.*
                 ▼
 ┌──────────────────────────── kvd ─────────────────────────────┐
 │ KvService (callback API: no thread waits on a write)         │
 │      │ Propose                         ▲ ReadBarrier         │
 │      ▼                                 │                     │
 │ RaftNode ── AppendEntries / RequestVote / InstallSnapshot ──▶ peers
 │   │  log (in memory, shared buffers)                         │
 │   │──Append──▶ WAL (group commit, one fdatasync per batch)   │
 │   │◀─durable─┘                                               │
 │   │ commit index                                             │
 │   ▼                                                          │
 │ Dispatcher ─▶ StateMachine: session dedup ─▶ Applier (N workers)
 │                                            │ routed by shard │
 │                                            ▼                 │
 │                     MemTable: 256 shards × (B-tree + rwlock) │
 │                                            │ COW snapshot    │
 │                                            ▼                 │
 │                                  snapshots/snap-<index>/     │
 └──────────────────────────────────────────────────────────────┘
```

The same storage pieces also run without Raft (`kvd --mode=engine`,
`storage/engine.h`), where a write is committed as soon as it is durable.

## Write-ahead log

`src/storage/wal.{h,cc}`, format in `wal_format.h`.

**Group commit.** `Wal::Append` takes a short queue lock and returns. One writer
thread drains everything queued since its previous pass, encodes it into one
buffer, and issues one `pwrite` and one `fdatasync`. Under load the batch grows
on its own: whatever arrived while the previous flush was in progress goes into
the next one. With 16 threads × 64 writes in flight, a group holds ~500 records,
so durability costs one device flush per 500 writes instead of one per write.

**Flush cost.** On ext4, `fdatasync` after an append that grows the file must
also commit a journal transaction for the new size, about twice the cost of a
flush that only overwrites existing blocks (measured 0.55 ms vs 0.25 ms on the
benchmark NVMe). Segments are therefore preallocated: a preparer thread keeps
the next 64 MiB segment ready, either a recycled old segment or a zero-filled
new one. Zero-filling goes through `O_DIRECT`. With buffered zero-filling, ext4's
ordered-mode journal flushed the preparer's dirty pages as part of the WAL's
next commit, and WAL flush latency rose to several milliseconds.

**Drive write caches.** Two later fixes came from a run on a server with a
SATA SSD (Dell BOSS) instead of NVMe:

- *Flushes drain everything.* Writing 64 MiB of zeros in one burst parks it in
  the drive's volatile write cache, and the next `FLUSH` from anyone (the WAL's
  own fdatasync) has to drain all of it. On SATA that was a ~300 ms stall. The
  fill now goes out in 2 MiB steps, each fdatasync'ed before the next, and is
  paced to ~256 MiB/s while the active segment is under half full. Snapshot
  files use the same rule: written in 2 MiB steps with fdatasync and a shared
  128 MiB/s token bucket. This followed the observation that 3-node p99 jumped
  from 7 ms to 350–500 ms as soon as the nodes started snapshotting
  concurrently.
- *Read-modify-write.* `O_DIRECT` leaves the zeroed pages out of the page cache,
  and a buffered append that starts mid-page on an uncached page makes the
  kernel read that page from the device inside `pwrite`. The writer spent
  ~0.5 ms per group on these reads. The preparer now reads each segment into
  the cache after zeroing or recycling it.

Even so, preallocation is a trade-off, not a free win. On the NVMe box it
doubles or triples durable throughput (an append flush there is 2× an
overwrite flush and shares the ext4 journal with other tenants). On the SATA
server, plain appends are faster, because zeroing costs bandwidth that the
cheaper overwrite flush does not pay back. `--preallocate` exists for that
reason, and the results report both.

**Recycling safely.** A recycled file still contains valid-looking records from
its previous life. Every record's CRC32C covers the segment's sequence number
followed by the record, so a stale record fails verification under the new
number. Each group write also ends with a zero length prefix (overwritten by
the next write), so the reader stops cleanly where valid data ends.

**Record format.** `crc32c | len | type | index | term | commit | payload`. The
commit field stamps the commit index known when the record was written, which
lets a restarted Raft node apply entries it knows are committed without
waiting for a leader.

**Failure handling.** If a write or `fdatasync` fails, the WAL stops for good.
Retrying a failed fsync can report success for data the kernel already dropped
("fsyncgate"). A Raft node that cannot persist exits; the standalone engine
rejects further writes.

## Recovery

`src/storage/wal_reader.cc`, `engine.cc`, `raft_node.cc`.

1. Load the newest snapshot whose manifest verifies. Data files are split by
   shard, read in parallel, checked against their CRC32C and inserted into the
   B-trees with an end hint, which is O(1) per key because they are sorted.
2. Replay WAL segments starting at the snapshot's `wal_start_segment`. Each
   segment is parsed up to its first invalid record. Across segments the reader
   enforces index continuity: every record's index is at most one past the
   previous one, and the first must connect to the snapshot. A torn tail in the
   last segment is normal after a crash and is dropped. A gap anywhere else
   means acknowledged data is missing, and recovery refuses to start rather
   than come up silently without it.
3. Decoding and applying are parallel: the replayed range is cut into
   contiguous chunks that are decoded concurrently into per-worker buckets (by
   shard), then each worker applies its buckets in chunk order, which keeps the
   log order within every shard.

After recovery, writes always go to a fresh segment. A torn record is never
overwritten in place, so stale bytes after it can never be read back as part
of a later record.

## MemTable and snapshots

`src/storage/memtable.cc`, `snapshot.cc`.

Keys are hash-partitioned (CRC32C + mix, stable across builds) into 256
shards. Each shard is an `absl::btree_map` behind a `std::shared_mutex`, so a
writer blocks only readers of the shard it modifies.

Snapshots are taken while writes continue. At a consistent cut (see the
applier below), `BeginSnapshot()` marks every shard. Until a shard has been
serialized, the first modification of each key saves its previous value (or
"absent") in that shard's undo map. The serializer reads the shard under a
shared lock and substitutes undo values, so it emits exactly the state at the
snapshot index. Readers are never blocked, and a writer pays one hash lookup
per key during the snapshot. A test drives a writer through a snapshot and
checks the result against the exact prefix of the log it claims to contain.

A snapshot is written to `snap-<index>.tmp/`, fsynced, and renamed into place.
The manifest (protobuf + CRC32C) is written last.

## Applying entries: the parallel applier

`src/storage/applier.cc`.

A single applier thread topped out at ~620K writes/s, mostly on completions
(waking the caller or finishing the gRPC call). The applier now routes every
operation to the worker that owns its shard (`shard % workers`):

- per-shard (and therefore per-key) order equals log order;
- a batch that spans several workers is split, and whichever worker applies
  the last piece completes it;
- `AppliedIndex()` is the minimum progress across workers;
- `RunQuiesced(fn)` waits until every submitted entry is applied and runs
  `fn` while nothing else is being applied. Snapshots start there.

With four workers, the pipelined write path reaches ~1.4M writes/s with fsync off (its CPU ceiling on the desktop host) and 0.6–0.9M fdatasync-durable writes/s, depending on the host ([results](results.md)).

## Raft

`src/raft/raft_node.{h,cc}`. Static membership.

**Elections.** Randomized timeouts (100–200 ms by default, heartbeat 20 ms).
Before incrementing its term, a node runs a PreVote round, so a node that was
partitioned cannot disrupt a healthy leader when it returns. Leader
stickiness: a node that has heard from a live leader within the minimum
election timeout refuses votes, unless the request is part of a leadership
transfer. Term and vote are persisted (temp file + fsync + rename + directory
fsync) before a vote is granted.

**Replication.** One replicator thread per follower keeps one AppendEntries
in flight and batches every entry that accumulated since the last one (up to
8 MiB). Entries are shared immutable buffers, so building a request does not
hold the node lock while serializing. A follower appends the entries to its
WAL and acknowledges only once they are durable; its `match_index` therefore
counts toward the quorum only for data that survives a crash. On a mismatch,
the follower returns a conflict term and index so the leader skips a whole
term per round trip. The leader writes its own WAL in parallel with
replication and counts its own durable index as one vote. An entry commits
when a majority has it durable and it belongs to the current term (Raft §5.4.2).

**Heartbeats travel separately.** Each follower also has a heartbeat thread
that sends an empty AppendEntries every 20 ms. Its `prev` is the follower's
known match index, so the follower can answer at once. As a result, leadership
confirmation, CheckQuorum, the lease and ReadIndex rounds never queue behind a
multi-megabyte batch or a slow follower fsync. Before this change, 3-node p99
under load was ~130 ms, with occasional leader churn. Peer traffic also has its
own gRPC server (client port + 1000, like etcd's separate peer port), so a
client flood cannot delay it either.

**CheckQuorum.** A leader that has not heard from a majority within two
election timeouts steps down. This only bounds how quickly a cut-off leader
gives up; lease safety does not depend on it. Writes pending on it fail as UNAVAILABLE. Their
outcome is unknown, which exactly-once sessions resolve (below).

**Reads.** `LINEARIZABLE` uses ReadIndex. The read index is
`max(commit, this term's no-op)`, and the leader confirms its leadership with
a heartbeat round sent after the read arrived. Rounds are numbered and shared
by all reads waiting at the time. Once a quorum acknowledges the round, the
read waits until that index is applied. `LEASE` skips the round while the
leader lease holds: the quorum-acknowledged send time plus 80% of the minimum
election timeout. Stickiness guarantees no other node can be elected inside
that window. `STALE` serves from any replica.

**Exactly-once writes.** Each write carries `(client_id, seq)`. The dispatcher
feeds committed entries to the state machine in log order and consults a
session table. A sequence number at or below the last one applied for that
client marks a retry of a write that already took effect, so it is
acknowledged without being applied again. Every replica makes the same
decision because they all see the same log. The table is part of each snapshot
and is bounded with LRU eviction by log index, which is also deterministic.

**Log compaction and InstallSnapshot.** After 256 MiB of applied log, a node
snapshots its state machine at the dispatched index, together with the session
table. It drops in-memory entries older than the snapshot minus a 100K-entry
cushion and releases (recycles) WAL segments the snapshot covers. A follower
that needs entries the leader no longer has receives the snapshot files
streamed in 1 MiB chunks. When installing:

- if the follower already holds the snapshot's last entry, it keeps the log
  after it (the Raft paper's rule);
- otherwise it rolls the WAL to a fresh segment and records that segment as
  the snapshot's `wal_start_segment` before renaming the snapshot into place.
  A crash before the old segments are deleted therefore cannot resurrect the
  discarded log.

**Leadership transfer.** On SIGTERM, a leader stops accepting writes, waits
until its most up-to-date follower has caught up, and sends `TimeoutNow`. The
target starts an election immediately, which bypasses stickiness. The handover
itself takes ~6 ms. Clients see about 40 ms including their redirect.

**Fault injection.** `Transport::SetBlocked` drops traffic to and from chosen
peers in both directions. The in-process tests use it for partitions, and
`kvchaos` exercises real process crashes.

## Threads

| Thread | Count | Role |
|---|---|---|
| WAL writer | 1 | group commit |
| WAL preparer | 1 | next segment ready (recycled or zero-filled) |
| applier workers | 4 | apply by shard, complete requests |
| replicators | N−1 | one in-flight AppendEntries per follower |
| heartbeat senders | N−1 | leadership confirmation, independent of appends |
| dispatcher | 1 | committed entries → session dedup → applier |
| reader | 1 | serves confirmed linearizable reads once applied |
| ticker | 1 | elections, CheckQuorum, transfer timeout |
| snapshot writer | 1 (+4 serializers) | background snapshots |
| gRPC | 2 servers | client: callback API for KV, sync Admin; peer: sync Raft |

## Bugs found by review (and the tests that pin them)

An independent review of the Raft and WAL code found, among smaller issues:

- **Stale suffix above a snapshot.** Suppose a follower's WAL held entries
  121..160 from an old term, followed by the new leader's overwrite of
  101..120, and a snapshot at 120 then covered the overwrite. Replay skipped
  the overwrite records because they were at or below the snapshot index, so
  it never truncated 121..160. A restarted node could then vote for a
  candidate missing committed entries. Records at or below the snapshot index
  now truncate everything replayed above it.
  `RaftTest.RecoveryDoesNotResurrectTruncatedSuffixAboveSnapshot` reproduces
  the bug: without the fix it recovers last index 160 instead of 120.
- **Concurrent InstallSnapshot streams** shared one staging directory. Each
  stream now gets a private directory, installs are serialized, and a sender
  stops once it is deposed.
- **Snapshot with nothing to write** left the snapshot thread waiting forever
  and copy-on-write tracking switched on.
  `RaftTest.RepeatedSnapshotWithNoNewEntriesCompletes` covers it.
- **Lease edge cases.** Lease reads are now disabled during a leadership
  transfer, the lease counts from the start of a snapshot stream, and a
  restarted node sits out one election timeout before voting.

## Testing

- **Unit tests** (`tests/`, GoogleTest): WAL (torn tails, mid-log corruption,
  recycled segments, Raft-style overwrites, concurrent group commit), memtable
  copy-on-write snapshots under a concurrent writer, snapshot corruption and
  fallback, engine crash via SIGKILL in a child process, snapshot consistency
  against the exact log prefix, and completion-implies-visibility for split
  batches.
- **Raft tests:** real gRPC over loopback covering elections, leader crash with
  restart, isolated leader (CheckQuorum plus discarded uncommitted writes),
  session dedup, lagging follower via InstallSnapshot followed by its own
  restart, full-cluster restart, and leadership transfer.
- **Sanitizers:** ASan+UBSan on everything, TSan on the storage layer (Ubuntu's
  gRPC build is not TSan-instrumented).
- **kvchaos:** real processes and real SIGKILLs. It checks every key against
  the last acknowledged and last attempted value and every linearizable read
  during the run. CI runs it for 30 s on every push.
