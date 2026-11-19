// On-disk format of a write-ahead-log segment.
//
// A segment is a file named wal-<seq>.log holding back-to-back records:
//
//   offset  size  field
//   0       4     crc32c of (segment seq as 8 LE bytes) followed by bytes [4, 8+len)
//   4       4     len: bytes following this 8-byte prefix
//   8       1     record type
//   9       8     log index
//   17      8     term (0 outside Raft)
//   25      8     commit index known when the record was written (0 = none)
//   33      ...   payload (len - 25 bytes)
//
// Segments are pre-zeroed or recycled from old segments so appends overwrite
// already-allocated blocks and fdatasync does not have to journal a file size
// change. Folding the segment seq into the CRC makes stale records left in a
// recycled file fail verification, the same trick RocksDB uses for recycled
// logs. A zero length field marks the end of written data.
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/strings/str_format.h"

namespace kv::wal {

inline constexpr size_t kPrefixSize = 8;                    // crc + len
inline constexpr size_t kFixedBodySize = 1 + 8 + 8 + 8;     // type, index, term, commit
inline constexpr size_t kHeaderSize = kPrefixSize + kFixedBodySize;
inline constexpr uint32_t kMaxRecordBody = 256u << 20;      // sanity bound on len

enum class RecordType : uint8_t {
  kEntry = 1,  // a log entry (write batch in standalone mode, Raft entry otherwise)
};

struct EntryView {
  uint64_t index = 0;
  uint64_t term = 0;
  uint64_t commit = 0;
  std::string_view payload;
};

inline std::string SegmentName(uint64_t seq) { return absl::StrFormat("wal-%016x.log", seq); }

// Parses "wal-<hex>.log". Returns false for any other name.
inline bool ParseSegmentName(std::string_view name, uint64_t* seq) {
  constexpr std::string_view kPre = "wal-", kSuf = ".log";
  if (name.size() != kPre.size() + 16 + kSuf.size()) return false;
  if (name.substr(0, kPre.size()) != kPre || name.substr(name.size() - kSuf.size()) != kSuf)
    return false;
  uint64_t v = 0;
  for (char c : name.substr(kPre.size(), 16)) {
    v <<= 4;
    if (c >= '0' && c <= '9') v |= static_cast<uint64_t>(c - '0');
    else if (c >= 'a' && c <= 'f') v |= static_cast<uint64_t>(c - 'a' + 10);
    else return false;
  }
  *seq = v;
  return true;
}

}  // namespace kv::wal
