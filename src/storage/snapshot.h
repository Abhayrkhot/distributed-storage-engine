// Snapshot files: a point-in-time image of the memtable at a log index, split
// into several data files so both writing and loading run in parallel.
//
//   <root>/snap-<index>/MANIFEST    protobuf SnapshotManifest + trailing crc32c
//   <root>/snap-<index>/data-<i>    (shard:u32 count:u64 len:u64 entries)*
//
// A snapshot is built in snap-<index>.tmp/, fsynced, and renamed into place,
// so a crash at any point leaves either the old or the new snapshot.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/snapshot.pb.h"
#include "storage/memtable.h"

namespace kv {

struct SnapshotWriteOptions {
  uint64_t index = 0;
  uint64_t term = 0;
  uint64_t wal_start_segment = 0;
  std::string app_state;
  int files = 16;
  int threads = 4;
  int retain = 2;  // complete snapshots to keep (newest first)
  // Background I/O budget across all writer threads (0 = unlimited). Data is
  // also fdatasync'ed every few MiB: one big flush at the end would sit in the
  // drive's write cache and make the WAL's next fdatasync drain all of it.
  uint64_t rate_limit_bytes_per_s = 128ull << 20;
};

struct SnapshotLoadStats {
  uint64_t keys = 0;
  uint64_t bytes = 0;
  double seconds = 0;
};

std::string SnapshotDirName(uint64_t index);

// Serializes `mem` (BeginSnapshot() must already have been called at
// options.index) into a new snapshot under `root` and prunes old ones.
absl::StatusOr<snapshot::SnapshotManifest> WriteSnapshot(const std::string& root, MemTable& mem,
                                                         const SnapshotWriteOptions& options);

// Newest snapshot under `root` whose manifest parses and verifies. Data file
// checksums are verified when loading.
absl::StatusOr<std::optional<snapshot::SnapshotManifest>> FindLatestSnapshot(
    const std::string& root);

absl::StatusOr<snapshot::SnapshotManifest> ReadManifest(const std::string& snap_dir);
absl::Status WriteManifest(const std::string& snap_dir, const snapshot::SnapshotManifest& m);

// Loads a snapshot into an empty memtable using up to `threads` threads.
absl::Status LoadSnapshot(const std::string& root, const snapshot::SnapshotManifest& manifest,
                          MemTable& mem, int threads, SnapshotLoadStats* stats);

// Checks sizes and checksums of every data file in `snap_dir`.
absl::Status VerifySnapshotDir(const std::string& snap_dir,
                               const snapshot::SnapshotManifest& manifest);

// Removes snapshot directories older than the newest `retain` and stale .tmp dirs.
void PruneSnapshots(const std::string& root, int retain);

}  // namespace kv
