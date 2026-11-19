#include "storage/wal.h"

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <unordered_set>

#include "common/coding.h"
#include "common/crc32c.h"
#include "common/fs.h"
#include "common/logging.h"

namespace kv {
namespace {

uint64_t NowNanos() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

// Creates `path` and fills it with `bytes` zeros. O_DIRECT keeps the zeros out
// of the page cache: with ext4's data=ordered journaling, buffered dirty pages
// of a freshly allocated file get flushed by whichever journal commit comes
// next, which would make the WAL's own fdatasync wait for them.
absl::Status CreateZeroedFile(const std::string& path, uint64_t bytes, int* fd_out) {
  constexpr size_t kChunk = 1 << 20;
  bytes = (bytes + 4095) & ~uint64_t{4095};
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC | O_DIRECT, 0644);
  bool direct = fd >= 0;
  if (!direct) fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) return ErrnoStatus("open", path, errno);
  void* buf = nullptr;
  if (::posix_memalign(&buf, 4096, kChunk) != 0) {
    ::close(fd);
    return absl::ResourceExhaustedError("posix_memalign");
  }
  std::memset(buf, 0, kChunk);
  absl::Status st;
  for (uint64_t off = 0; st.ok() && off < bytes; off += kChunk) {
    const size_t n = static_cast<size_t>(std::min<uint64_t>(kChunk, bytes - off));
    st = PWriteAll(fd, static_cast<const char*>(buf), n, off);
  }
  ::free(buf);
  if (st.ok() && ::fdatasync(fd) != 0) st = ErrnoStatus("fdatasync", path, errno);
  ::close(fd);
  if (!st.ok()) return st;
  // Reopen buffered for the WAL's small appends.
  fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) return ErrnoStatus("open", path, errno);
  *fd_out = fd;
  return absl::OkStatus();
}

// A recycled file still starts with a record from its previous life. Zero the
// first length prefix so the segment reads as empty until it is written,
// instead of looking like a torn record if we crash before using it.
absl::Status ClearRecycledHeader(int fd, const std::string& path) {
  const char zeros[wal::kPrefixSize] = {};
  if (auto st = PWriteAll(fd, zeros, sizeof(zeros), 0); !st.ok()) return st;
  if (::fdatasync(fd) != 0) return ErrnoStatus("fdatasync", path, errno);
  return absl::OkStatus();
}

}  // namespace

Wal::Wal(const WalOptions& options, GroupCallback cb) : opt_(options), cb_(std::move(cb)) {}

absl::StatusOr<std::unique_ptr<Wal>> Wal::Open(const WalOptions& options,
                                               std::vector<WalSegment> existing,
                                               uint64_t last_index, GroupCallback cb) {
  if (auto st = CreateDirs(options.dir); !st.ok()) return st;
  std::unique_ptr<Wal> wal(new Wal(options, std::move(cb)));
  if (auto st = wal->Start(std::move(existing), last_index); !st.ok()) return st;
  return wal;
}

std::string Wal::PathFor(uint64_t seq) const { return opt_.dir + "/" + wal::SegmentName(seq); }

absl::Status Wal::Start(std::vector<WalSegment> existing, uint64_t last_index) {
  last_index_ = last_index;
  std::unordered_set<uint64_t> keep;
  uint64_t max_seq = 0;
  for (const auto& s : existing) {
    keep.insert(s.seq);
    max_seq = std::max(max_seq, s.seq);
  }
  // Trailing segments without valid records (prepared but never written) are
  // reusable right away.
  while (!existing.empty() && existing.back().first_index == 0) {
    recycle_.push_back(PathFor(existing.back().seq));
    keep.erase(existing.back().seq);
    existing.pop_back();
  }
  for (const auto& s : existing) sealed_.push_back(s);

  auto names = ListDir(opt_.dir);
  if (!names.ok()) return names.status();
  for (const auto& name : *names) {
    uint64_t seq;
    if (wal::ParseSegmentName(name, &seq)) {
      max_seq = std::max(max_seq, seq);
      if (keep.count(seq) == 0 &&
          std::find(recycle_.begin(), recycle_.end(), PathFor(seq)) == recycle_.end()) {
        recycle_.push_back(PathFor(seq));  // obsolete: below the snapshot's start segment
      }
    } else if (name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
      ::unlink((opt_.dir + "/" + name).c_str());
    }
  }
  while (recycle_.size() > opt_.max_recycled_segments || (!opt_.preallocate && !recycle_.empty())) {
    ::unlink(recycle_.back().c_str());
    recycle_.pop_back();
  }

  // First active segment: reuse a recycled file if there is one, otherwise
  // zero-fill a new one before accepting writes so every fdatasync is an
  // overwrite. The preparer keeps the following segment ready.
  active_seq_ = max_seq + 1;
  int fd = -1;
  if (!recycle_.empty()) {
    std::string from = recycle_.back();
    recycle_.pop_back();
    if (auto st = RenameFile(from, PathFor(active_seq_)); !st.ok()) return st;
    fd = ::open(PathFor(active_seq_).c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) return ErrnoStatus("open", PathFor(active_seq_), errno);
    if (auto st = ClearRecycledHeader(fd, PathFor(active_seq_)); !st.ok()) {
      ::close(fd);
      return st;
    }
  } else if (opt_.preallocate) {
    if (auto st = CreateZeroedFile(PathFor(active_seq_), opt_.segment_bytes, &fd); !st.ok()) {
      return st;
    }
  } else {
    fd = ::open(PathFor(active_seq_).c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return ErrnoStatus("open", PathFor(active_seq_), errno);
  }
  if (auto st = SyncDir(opt_.dir); !st.ok()) {
    ::close(fd);
    return st;
  }
  active_ = Segment{active_seq_, fd, 0, 0, 0};

  writer_ = std::thread([this] { WriterLoop(); });
  preparer_ = std::thread([this] { PreparerLoop(); });
  return absl::OkStatus();
}

Wal::~Wal() {
  {
    std::lock_guard<std::mutex> l(mu_);
    stopping_ = true;
  }
  writer_cv_.notify_all();
  if (writer_.joinable()) writer_.join();
  {
    std::lock_guard<std::mutex> l(seg_mu_);
    preparer_stop_ = true;
  }
  seg_cv_.notify_all();
  if (preparer_.joinable()) preparer_.join();
  if (active_.fd >= 0) ::close(active_.fd);
  if (next_fd_ >= 0) ::close(next_fd_);
}

uint64_t Wal::Append(Record&& record) {
  std::unique_lock<std::mutex> l(mu_);
  if (record.index == 0) {
    record.index = ++last_index_;
  } else {
    KV_CHECK(record.index <= last_index_ + 1)
        << "WAL append would leave a gap: index " << record.index << " after " << last_index_;
    last_index_ = record.index;
  }
  const uint64_t index = record.index;
  queue_bytes_ += wal::kHeaderSize + record.payload.size();
  queue_.push_back(std::move(record));
  ++appended_count_;
  const bool wake = writer_idle_;
  l.unlock();
  if (wake) writer_cv_.notify_one();
  return index;
}

void Wal::Flush() {
  std::unique_lock<std::mutex> l(mu_);
  const uint64_t target = appended_count_;
  flushed_cv_.wait(l, [&] { return done_count_ >= target; });
}

void Wal::WriterLoop() {
  std::vector<Record> group;
  for (;;) {
    absl::Status err;
    {
      std::unique_lock<std::mutex> l(mu_);
      writer_idle_ = true;
      writer_cv_.wait(l, [&] { return !queue_.empty() || stopping_; });
      writer_idle_ = false;
      if (queue_.empty()) return;  // stopping and drained
      if (queue_bytes_ <= opt_.max_group_bytes) {
        group.swap(queue_);
        queue_bytes_ = 0;
      } else {
        size_t take = 0, bytes = 0;
        while (take < queue_.size() && (take == 0 || bytes < opt_.max_group_bytes)) {
          bytes += wal::kHeaderSize + queue_[take].payload.size();
          ++take;
        }
        group.assign(std::make_move_iterator(queue_.begin()),
                     std::make_move_iterator(queue_.begin() + static_cast<long>(take)));
        queue_.erase(queue_.begin(), queue_.begin() + static_cast<long>(take));
        queue_bytes_ -= bytes;
      }
      err = error_;
    }

    const size_t n = group.size();
    uint64_t bytes = 0;
    if (err.ok()) {
      err = WriteGroup(group, &bytes);
      if (!err.ok()) {
        KV_LOG(Error) << "WAL write failed, refusing further writes: " << err;
        std::lock_guard<std::mutex> l(mu_);
        error_ = err;
      }
    }
    {
      std::lock_guard<std::mutex> l(stats_mu_);
      ++stats_.groups;
      stats_.records += n;
      stats_.bytes += bytes;
    }
    cb_(err, std::move(group));
    group.clear();
    {
      std::lock_guard<std::mutex> l(mu_);
      done_count_ += n;
    }
    flushed_cv_.notify_all();
  }
}

void Wal::EncodeRecord(const Record& r) {
  const size_t start = buf_.size();
  const uint32_t len = static_cast<uint32_t>(wal::kFixedBodySize + r.payload.size());
  buf_.resize(start + wal::kHeaderSize);
  char* h = buf_.data() + start;
  EncodeFixed32(h + 4, len);
  h[8] = static_cast<char>(wal::RecordType::kEntry);
  EncodeFixed64(h + 9, r.index);
  EncodeFixed64(h + 17, r.term);
  EncodeFixed64(h + 25, r.commit);
  buf_.append(r.payload);
  char seq_le[8];
  EncodeFixed64(seq_le, active_.seq);
  uint32_t crc = crc32c::Value(seq_le, 8);
  crc = crc32c::Extend(crc, buf_.data() + start + 4, 4 + len);
  EncodeFixed32(buf_.data() + start, crc);
}

absl::Status Wal::FlushBuffer() {
  if (buf_.empty()) return absl::OkStatus();
  if (auto st = PWriteAll(active_.fd, buf_.data(), buf_.size(), active_.offset); !st.ok())
    return st;
  active_.offset += buf_.size();
  buf_.clear();
  return absl::OkStatus();
}

absl::Status Wal::WriteGroup(std::vector<Record>& group, uint64_t* bytes_written) {
  const bool sync = opt_.sync_mode == SyncMode::kAlways;
  buf_.clear();
  uint64_t bytes = 0;
  for (const Record& r : group) {
    const size_t rec_size = wal::kHeaderSize + r.payload.size();
    const uint64_t used = active_.offset + buf_.size();
    if (used > 0 && used + rec_size > opt_.segment_bytes) {
      bytes += buf_.size();
      if (auto st = FlushWithEndMarker(); !st.ok()) return st;
      if (sync && ::fdatasync(active_.fd) != 0)
        return ErrnoStatus("fdatasync", PathFor(active_.seq), errno);
      if (auto st = RollSegment(); !st.ok()) return st;
    }
    EncodeRecord(r);
    if (active_.first_index == 0) active_.first_index = r.index;
    active_.max_index = std::max(active_.max_index, r.index);
  }
  bytes += buf_.size();
  if (auto st = FlushWithEndMarker(); !st.ok()) return st;
  if (sync) {
    const uint64_t t0 = NowNanos();
    if (::fdatasync(active_.fd) != 0) return ErrnoStatus("fdatasync", PathFor(active_.seq), errno);
    const uint64_t dt = NowNanos() - t0;
    std::lock_guard<std::mutex> l(stats_mu_);
    ++stats_.syncs;
    stats_.sync_nanos += dt;
  }
  *bytes_written = bytes;
  return absl::OkStatus();
}

// Writes the buffered records followed by a zero length prefix (when it fits
// in the segment) in one pwrite. The marker terminates the valid prefix: in a
// recycled segment it hides the stale records that follow, so the reader only
// reports a checksum failure for a genuinely torn write. The next write starts
// at the marker and overwrites it.
absl::Status Wal::FlushWithEndMarker() {
  const size_t payload = buf_.size();
  const bool marker = active_.offset + payload + wal::kPrefixSize <= opt_.segment_bytes;
  if (marker) buf_.append(wal::kPrefixSize, '\0');
  if (auto st = FlushBuffer(); !st.ok()) return st;
  if (marker) active_.offset -= wal::kPrefixSize;
  return absl::OkStatus();
}

absl::Status Wal::RollSegment() {
  WalSegment sealed{active_.seq, active_.first_index, 0, active_.max_index};
  sealed.last_index = active_.max_index;
  std::unique_lock<std::mutex> l(seg_mu_);
  if (!next_ready_) {
    std::lock_guard<std::mutex> sl(stats_mu_);
    ++stats_.roll_stalls;
  }
  seg_cv_.wait(l, [&] { return next_ready_ || preparer_stop_ || !prep_error_.ok(); });
  if (!next_ready_) {
    return prep_error_.ok() ? absl::UnavailableError("WAL shutting down") : prep_error_;
  }
  const int old_fd = active_.fd;
  active_seq_ = active_.seq + 1;
  active_ = Segment{active_seq_, next_fd_, 0, 0, 0};
  next_fd_ = -1;
  next_ready_ = false;
  sealed_.push_back(sealed);
  l.unlock();
  seg_cv_.notify_all();
  ::close(old_fd);
  std::lock_guard<std::mutex> sl(stats_mu_);
  ++stats_.segments_rolled;
  return absl::OkStatus();
}

void Wal::PreparerLoop() {
  std::unique_lock<std::mutex> l(seg_mu_);
  for (;;) {
    seg_cv_.wait(l, [&] { return preparer_stop_ || !next_ready_; });
    if (preparer_stop_) return;
    const uint64_t seq = active_seq_ + 1;
    std::string recycled;
    if (!recycle_.empty()) {
      recycled = recycle_.back();
      recycle_.pop_back();
    }
    l.unlock();

    const std::string path = PathFor(seq);
    int fd = -1;
    absl::Status st;
    if (!recycled.empty()) {
      st = RenameFile(recycled, path);
      if (st.ok()) {
        fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
        if (fd < 0) st = ErrnoStatus("open", path, errno);
      }
      if (st.ok()) st = ClearRecycledHeader(fd, path);
      if (st.ok()) {
        std::lock_guard<std::mutex> sl(stats_mu_);
        ++stats_.segments_recycled;
      }
    } else if (opt_.preallocate) {
      st = CreateZeroedFile(path, opt_.segment_bytes, &fd);
    } else {
      fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
      if (fd < 0) st = ErrnoStatus("open", path, errno);
    }
    if (st.ok()) st = SyncDir(opt_.dir);

    l.lock();
    if (!st.ok()) {
      if (fd >= 0) ::close(fd);
      KV_LOG(Error) << "WAL segment preparation failed (will retry): " << st;
      prep_error_ = st;
      seg_cv_.notify_all();
      l.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      l.lock();
      continue;
    }
    if (seq != active_seq_ + 1 || next_ready_) {
      // A Reset() moved the active segment while we were preparing; this file
      // has the wrong name now. Put it back in the pool.
      ::close(fd);
      recycle_.push_back(path);
      continue;
    }
    next_fd_ = fd;
    next_ready_ = true;
    prep_error_ = absl::OkStatus();
    seg_cv_.notify_all();
  }
}

void Wal::RecycleOrDelete(uint64_t seq) {
  const std::string path = PathFor(seq);
  if (opt_.preallocate && recycle_.size() < opt_.max_recycled_segments) {
    recycle_.push_back(path);
  } else {
    ::unlink(path.c_str());
  }
}

void Wal::ReleaseUpTo(uint64_t index) {
  std::lock_guard<std::mutex> l(seg_mu_);
  while (!sealed_.empty() && sealed_.front().max_index <= index) {
    RecycleOrDelete(sealed_.front().seq);
    sealed_.pop_front();
  }
}

absl::StatusOr<uint64_t> Wal::Reset(uint64_t last_index) {
  std::unique_lock<std::mutex> l(mu_);
  flushed_cv_.wait(l, [&] { return done_count_ == appended_count_; });
  if (!error_.ok()) return error_;
  // The writer is parked on writer_cv_ (queue empty) and cannot run without
  // mu_, so its segment state is safe to touch here.
  if (active_.first_index != 0 || active_.offset > 0) {
    if (auto st = RollSegment(); !st.ok()) return st;
  }
  last_index_ = last_index;
  return active_.seq;
}

void Wal::DropBefore(uint64_t seq) {
  std::lock_guard<std::mutex> l(seg_mu_);
  while (!sealed_.empty() && sealed_.front().seq < seq) {
    RecycleOrDelete(sealed_.front().seq);
    sealed_.pop_front();
  }
}

uint64_t Wal::FirstSegmentAbove(uint64_t index) const {
  std::lock_guard<std::mutex> l(seg_mu_);
  for (const auto& s : sealed_) {
    if (s.max_index > index) return s.seq;
  }
  return active_seq_;
}

uint64_t Wal::ActiveSegment() const {
  std::lock_guard<std::mutex> l(seg_mu_);
  return active_seq_;
}

uint64_t Wal::LastIndex() const {
  std::lock_guard<std::mutex> l(mu_);
  return last_index_;
}

absl::Status Wal::Health() const {
  std::lock_guard<std::mutex> l(mu_);
  return error_;
}

WalStats Wal::Stats() const {
  std::lock_guard<std::mutex> l(stats_mu_);
  return stats_;
}

}  // namespace kv
