// Reading WAL segments back during recovery.
//
// Each segment is parsed up to its first invalid record (zero header, bad
// length, CRC mismatch or a record stamped with another segment's seq). Across
// segments the reader enforces index continuity: every record's index must be
// at most one past the previous record's. A torn tail in the last segment is
// the normal result of a crash mid-write and is dropped; a hole anywhere else
// means acknowledged data is missing and recovery fails loudly.
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "storage/wal.h"
#include "storage/wal_format.h"

namespace kv {

struct SegmentContents {
  uint64_t seq = 0;
  std::string data;                     // raw file bytes; entries point into it
  std::vector<wal::EntryView> entries;  // valid records in file order
  uint64_t valid_bytes = 0;             // end offset of the last valid record
  bool clean_end = true;                // false if parsing stopped at a damaged record
  std::string stop_reason;
};

absl::StatusOr<SegmentContents> ReadSegment(const std::string& dir, uint64_t seq);

// Segment seqs in `dir` that are >= start_seq, ascending.
absl::StatusOr<std::vector<uint64_t>> ListSegments(const std::string& dir, uint64_t start_seq);

struct WalReplayStats {
  uint64_t segments = 0;
  uint64_t records = 0;
  uint64_t bytes = 0;
  uint64_t torn_bytes = 0;  // bytes dropped after the last valid record
  uint64_t last_index = 0;
  uint64_t max_commit = 0;  // highest commit index stamped on any record
};

// Reads every segment >= start_seq in order, checks continuity starting from
// `after_index` (the snapshot index; 0 if none) and calls `on_segment` for each
// one. `segments` receives the descriptors Wal::Open needs.
absl::Status ReplayWal(const std::string& dir, uint64_t start_seq, uint64_t after_index,
                       const std::function<absl::Status(SegmentContents&)>& on_segment,
                       std::vector<WalSegment>* segments, WalReplayStats* stats);

}  // namespace kv
