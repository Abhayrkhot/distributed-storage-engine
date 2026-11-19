// Group-commit write-ahead log.
//
// Producers call Append() from any thread; it only takes a short queue lock.
// A single writer thread drains everything queued since its last pass, encodes
// it into one buffer, issues one pwrite and one fdatasync, then hands the whole
// group to the owner's callback. Under load a single fdatasync covers hundreds
// of writes, which is what lets synchronous durability run at device-flush
// rate instead of one flush per write.
//
// A preparer thread keeps the next segment ready (pre-zeroed or recycled) so
// rolling to a new segment never blocks on file creation and appends never
// extend the file.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "storage/wal_format.h"

namespace kv {

enum class SyncMode {
  kAlways,  // fdatasync every group before acknowledging it (default)
  kNone,    // acknowledge after write(2); the OS flushes later (benchmarks only)
};

struct WalOptions {
  std::string dir;
  uint64_t segment_bytes = 64ull << 20;
  SyncMode sync_mode = SyncMode::kAlways;
  // Upper bound on bytes written per group. Bigger groups amortize the flush
  // over more writes; the cap keeps a burst from delaying the next group.
  size_t max_group_bytes = 16ull << 20;
  // Pre-zero new segments (and recycle released ones) so fdatasync only has to
  // flush data, not a journaled size change. Halves flush latency on ext4.
  bool preallocate = true;
  size_t max_recycled_segments = 8;
};

struct WalStats {
  uint64_t groups = 0;
  uint64_t records = 0;
  uint64_t bytes = 0;
  uint64_t syncs = 0;
  uint64_t sync_nanos = 0;
  uint64_t segments_rolled = 0;
  uint64_t segments_recycled = 0;
  uint64_t roll_stalls = 0;  // writer had to wait for the preparer
};

// Description of a segment found on disk during recovery.
struct WalSegment {
  uint64_t seq = 0;
  uint64_t first_index = 0;  // 0 if the segment holds no valid records
  uint64_t last_index = 0;
  uint64_t max_index = 0;    // highest index anywhere in the segment
};

class Wal {
 public:
  struct Record {
    uint64_t index = 0;  // 0 => assign the next index (standalone mode)
    uint64_t term = 0;
    uint64_t commit = 0;
    std::string payload;
    void* tag = nullptr;  // opaque to the WAL, returned to the callback
  };

  // Invoked on the writer thread after each group is durable (or failed). The
  // callee takes ownership of the records. Keep it cheap: it is on the commit
  // path of every write.
  using GroupCallback = std::function<void(const absl::Status&, std::vector<Record>&&)>;

  // `existing` lists segments recovered from disk (ascending seq); new writes
  // always start in a fresh segment after them. `last_index` is the highest
  // index already in the log.
  static absl::StatusOr<std::unique_ptr<Wal>> Open(const WalOptions& options,
                                                   std::vector<WalSegment> existing,
                                                   uint64_t last_index, GroupCallback cb);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  // Queues a record and returns its index. Thread-safe. Records are written in
  // the order Append is called. An explicit index may be at most last+1; a
  // smaller index overwrites the suffix of the log (Raft truncation), which
  // replay resolves by letting the later record win.
  uint64_t Append(Record&& record);

  // Blocks until every record appended before the call has been handed to the
  // callback. Used by tests and shutdown.
  void Flush();

  // Segments whose highest index is <= `index` are no longer needed (a
  // snapshot covers them). They are recycled for future segments or deleted.
  void ReleaseUpTo(uint64_t index);

  // Rolls to a fresh segment whose first record will be `last_index + 1` and
  // returns its seq. Older segments stay on disk until DropBefore().
  // Used when a Raft follower installs a snapshot that replaces its log.
  absl::StatusOr<uint64_t> Reset(uint64_t last_index);
  void DropBefore(uint64_t seq);

  // Seq of the segment the next record will go to.
  uint64_t ActiveSegment() const;
  // Lowest segment seq that may hold an index above `index`. Recovery from a
  // snapshot at `index` can skip every segment below it.
  uint64_t FirstSegmentAbove(uint64_t index) const;
  uint64_t LastIndex() const;
  absl::Status Health() const;
  WalStats Stats() const;
  const WalOptions& options() const { return opt_; }

 private:
  struct Segment {
    uint64_t seq = 0;
    int fd = -1;
    uint64_t offset = 0;
    uint64_t first_index = 0;
    uint64_t max_index = 0;
  };

  Wal(const WalOptions& options, GroupCallback cb);
  absl::Status Start(std::vector<WalSegment> existing, uint64_t last_index);
  void WriterLoop();
  void PreparerLoop();
  absl::Status WriteGroup(std::vector<Record>& group, uint64_t* bytes_written);
  absl::Status RollSegment();
  absl::Status FlushBuffer();
  absl::Status FlushWithEndMarker();
  void EncodeRecord(const Record& r);
  std::string PathFor(uint64_t seq) const;
  void RecycleOrDelete(uint64_t seq);

  const WalOptions opt_;
  GroupCallback cb_;

  // ---- queue (producers <-> writer) ----
  mutable std::mutex mu_;
  std::condition_variable writer_cv_;
  std::condition_variable flushed_cv_;
  std::vector<Record> queue_;
  size_t queue_bytes_ = 0;
  uint64_t last_index_ = 0;      // highest index appended
  uint64_t appended_count_ = 0;  // records appended (for Flush)
  uint64_t done_count_ = 0;      // records handed to the callback
  bool writer_idle_ = false;
  bool stopping_ = false;
  absl::Status error_;           // sticky: once a write or sync fails, the WAL is dead

  // ---- writer-thread state ----
  Segment active_;
  std::string buf_;  // encoded records not yet written to active_
  WalStats stats_;   // guarded by stats_mu_
  mutable std::mutex stats_mu_;

  // ---- segment bookkeeping (writer, preparer, ReleaseUpTo) ----
  mutable std::mutex seg_mu_;
  std::condition_variable seg_cv_;
  std::deque<WalSegment> sealed_;     // closed segments still needed, ascending seq
  std::vector<std::string> recycle_;  // released segment files available for reuse
  uint64_t active_seq_ = 0;
  int next_fd_ = -1;                  // prepared fd for active_seq_ + 1
  bool next_ready_ = false;
  bool preparer_stop_ = false;
  absl::Status prep_error_;  // last preparer failure; rolls fail while set

  std::thread writer_;
  std::thread preparer_;
};

}  // namespace kv
