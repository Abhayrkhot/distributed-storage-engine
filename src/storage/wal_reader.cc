#include "storage/wal_reader.h"


#include "absl/strings/str_cat.h"
#include "common/coding.h"
#include "common/crc32c.h"
#include "common/fs.h"
#include "common/logging.h"

namespace kv {

absl::StatusOr<SegmentContents> ReadSegment(const std::string& dir, uint64_t seq) {
  SegmentContents out;
  out.seq = seq;
  auto data = ReadFile(dir + "/" + wal::SegmentName(seq));
  if (!data.ok()) return data.status();
  out.data = std::move(*data);

  char seq_le[8];
  EncodeFixed64(seq_le, seq);
  const uint32_t seed = crc32c::Value(seq_le, 8);

  const char* base = out.data.data();
  const size_t size = out.data.size();
  size_t off = 0;
  while (true) {
    // The writer only starts a record where the whole record fits, so a tail
    // shorter than the length prefix can never be part of one.
    if (size - off < wal::kPrefixSize) break;
    const uint32_t crc = DecodeFixed32(base + off);
    const uint32_t len = DecodeFixed32(base + off + 4);
    if (crc == 0 && len == 0) break;  // end marker / unwritten (pre-zeroed) space
    if (size - off < wal::kHeaderSize || len < wal::kFixedBodySize ||
        len > wal::kMaxRecordBody || len > size - off - wal::kPrefixSize) {
      out.clean_end = false;
      out.stop_reason = absl::StrCat("bad record length ", len, " at offset ", off);
      break;
    }
    const uint32_t actual = crc32c::Extend(seed, base + off + 4, 4 + len);
    if (actual != crc) {
      // Either a torn write or a stale record from the file's previous life as
      // another segment (recycling). Both end the valid prefix.
      out.clean_end = false;
      out.stop_reason = absl::StrCat("checksum mismatch at offset ", off);
      break;
    }
    const char* body = base + off + wal::kPrefixSize;
    if (static_cast<wal::RecordType>(body[0]) != wal::RecordType::kEntry) {
      out.clean_end = false;
      out.stop_reason = absl::StrCat("unknown record type at offset ", off);
      break;
    }
    wal::EntryView e;
    e.index = DecodeFixed64(body + 1);
    e.term = DecodeFixed64(body + 9);
    e.commit = DecodeFixed64(body + 17);
    e.payload = std::string_view(body + wal::kFixedBodySize, len - wal::kFixedBodySize);
    out.entries.push_back(e);
    off += wal::kPrefixSize + len;
  }
  out.valid_bytes = off;
  return out;
}

absl::StatusOr<std::vector<uint64_t>> ListSegments(const std::string& dir, uint64_t start_seq) {
  auto names = ListDir(dir);
  if (!names.ok()) {
    if (absl::IsNotFound(names.status())) return std::vector<uint64_t>{};
    return names.status();
  }
  std::vector<uint64_t> seqs;
  for (const auto& n : *names) {
    uint64_t seq;
    if (wal::ParseSegmentName(n, &seq) && seq >= start_seq) seqs.push_back(seq);
  }
  std::sort(seqs.begin(), seqs.end());
  return seqs;
}

absl::Status ReplayWal(const std::string& dir, uint64_t start_seq, uint64_t after_index,
                       const std::function<absl::Status(SegmentContents&)>& on_segment,
                       std::vector<WalSegment>* segments, WalReplayStats* stats) {
  auto seqs = ListSegments(dir, start_seq);
  if (!seqs.ok()) return seqs.status();
  uint64_t prev = after_index;
  for (size_t i = 0; i < seqs->size(); ++i) {
    const uint64_t seq = (*seqs)[i];
    auto seg = ReadSegment(dir, seq);
    if (!seg.ok()) return seg.status();
    WalSegment desc{seq, 0, 0, 0};
    for (const auto& e : seg->entries) {
      if (e.index > prev + 1) {
        return absl::DataLossError(absl::StrCat(
            "WAL gap: segment ", wal::SegmentName(seq), " jumps from index ", prev, " to ",
            e.index, "; acknowledged writes are missing (earlier segment damaged?)"));
      }
      if (desc.first_index == 0) desc.first_index = e.index;
      desc.last_index = e.index;
      desc.max_index = std::max(desc.max_index, e.index);
      prev = e.index;
      stats->max_commit = std::max(stats->max_commit, e.commit);
    }
    if (!seg->clean_end) {
      const uint64_t dropped = seg->data.size() - seg->valid_bytes;
      KV_LOG(Warning) << "WAL " << wal::SegmentName(seq) << ": " << seg->stop_reason
                      << "; treating the rest as unwritten (" << dropped << " bytes)";
      stats->torn_bytes += dropped;
    }
    ++stats->segments;
    stats->records += seg->entries.size();
    stats->bytes += seg->valid_bytes;
    if (!seg->entries.empty()) stats->last_index = seg->entries.back().index;
    segments->push_back(desc);
    if (auto st = on_segment(*seg); !st.ok()) return st;
  }
  if (stats->last_index == 0) stats->last_index = std::max(prev, after_index);
  return absl::OkStatus();
}

}  // namespace kv
