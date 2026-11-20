#include "storage/engine.h"

#include <algorithm>
#include <chrono>

#include "absl/strings/str_cat.h"
#include "common/thread_name.h"
#include "common/fs.h"
#include "common/logging.h"
#include "storage/snapshot.h"
#include "storage/wal_reader.h"

namespace kv {
namespace {

using Clock = std::chrono::steady_clock;

double SecondsSince(Clock::time_point t0) {
  return std::chrono::duration<double>(Clock::now() - t0).count();
}

// Blocks the calling thread until the write is durable and applied. The
// notify happens under the mutex, so the waiter cannot return (and destroy
// this object) until Done() has released it.
class SyncWaiter final : public WriteCompletion {
 public:
  void Done(const absl::Status& status, uint64_t) override {
    std::lock_guard<std::mutex> l(mu_);
    status_ = status;
    done_ = true;
    cv_.notify_one();
  }
  absl::Status Wait() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [&] { return done_; });
    return status_;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool done_ = false;
  absl::Status status_;
};

template <typename Fn>
void RunParallel(int n, Fn fn) {
  std::vector<std::thread> pool;
  pool.reserve(static_cast<size_t>(n));
  for (int i = 1; i < n; ++i) pool.emplace_back(fn, i);
  fn(0);
  for (auto& t : pool) t.join();
}

}  // namespace

Engine::Engine(const EngineOptions& options) : opt_(options), mem_(options.num_shards) {
  if (opt_.wal.dir.empty()) opt_.wal.dir = opt_.dir + "/wal";
  if (opt_.recovery_threads <= 0) {
    // Past ~16 threads recovery gets slower, not faster: the B-tree inserts
    // are memory-bound and a second socket only adds cross-node traffic.
    opt_.recovery_threads = static_cast<int>(std::clamp(std::thread::hardware_concurrency(), 1u, 16u));
  }
}

absl::StatusOr<std::unique_ptr<Engine>> Engine::Open(const EngineOptions& options) {
  if (options.dir.empty()) return absl::InvalidArgumentError("engine dir is required");
  std::unique_ptr<Engine> e(new Engine(options));
  if (auto st = e->Recover(); !st.ok()) return st;
  e->snapshotter_ = std::thread([p = e.get()] { p->SnapshotLoop(); });
  return e;
}

Engine::~Engine() {
  // Callers must have stopped writing. Drain the WAL (its last groups go to the
  // applier), drain the applier, let an in-flight snapshot finish (it still
  // uses the WAL), and only then close the WAL.
  if (wal_) wal_->Flush();
  applier_.reset();
  {
    std::lock_guard<std::mutex> l(snap_mu_);
    snap_stop_ = true;
  }
  snap_cv_.notify_all();
  if (snapshotter_.joinable()) snapshotter_.join();
  wal_.reset();
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

absl::Status Engine::Recover() {
  const auto t0 = Clock::now();
  const std::string snap_root = opt_.dir + "/snapshots";
  if (auto st = CreateDirs(snap_root); !st.ok()) return st;

  uint64_t snap_index = 0, wal_start = 0;
  auto latest = FindLatestSnapshot(snap_root);
  if (!latest.ok()) return latest.status();
  if (latest->has_value()) {
    const auto& m = **latest;
    SnapshotLoadStats ls;
    if (auto st = LoadSnapshot(snap_root, m, mem_, opt_.recovery_threads, &ls); !st.ok()) {
      return st;
    }
    snap_index = m.index();
    wal_start = m.wal_start_segment();
    recovery_.snapshot_seconds = ls.seconds;
    recovery_.snapshot_keys = ls.keys;
    recovery_.snapshot_index = snap_index;
  }

  const auto t1 = Clock::now();
  uint64_t applied = snap_index;
  std::vector<WalSegment> segments;
  WalReplayStats ws;
  auto st = ReplayWal(
      opt_.wal.dir, wal_start, snap_index,
      [&](SegmentContents& seg) -> absl::Status {
        std::vector<wal::EntryView> todo;
        todo.reserve(seg.entries.size());
        for (const auto& e : seg.entries) {
          if (e.index <= snap_index) continue;
          if (e.index != applied + 1) {
            return absl::DataLossError(absl::StrCat("WAL index ", e.index, " follows ", applied,
                                                    " in ", wal::SegmentName(seg.seq)));
          }
          todo.push_back(e);
          applied = e.index;
        }
        uint64_t ignored = 0;
        return ReplayParallel(todo, &ignored);
      },
      &segments, &ws);
  if (!st.ok()) return st;
  recovery_.wal_seconds = SecondsSince(t1);
  recovery_.wal_segments = ws.segments;
  recovery_.wal_records = ws.records;
  recovery_.wal_bytes = ws.bytes;
  recovery_.torn_bytes = ws.torn_bytes;
  recovery_.last_index = applied;

  if (opt_.concurrency == Concurrency::kPipelined) {
    applier_ = std::make_unique<Applier>(&mem_, opt_.apply_threads);
    applier_->Reset(applied);
  }
  auto wal = Wal::Open(opt_.wal, std::move(segments), applied,
                       [this](const absl::Status& s, std::vector<Wal::Record>&& g) {
                         OnWalGroup(s, std::move(g));
                       });
  if (!wal.ok()) return wal.status();
  wal_ = std::move(*wal);
  applied_index_.store(applied, std::memory_order_release);
  bytes_since_snapshot_ = ws.bytes;
  recovery_.keys = mem_.KeyCount();
  recovery_.total_seconds = SecondsSince(t0);
  KV_LOG(Info) << "engine recovered " << recovery_.keys << " keys (snapshot@" << snap_index
               << " + " << ws.records << " WAL records) in " << recovery_.total_seconds << "s";
  return absl::OkStatus();
}

absl::Status Engine::ReplayParallel(std::vector<wal::EntryView>& entries, uint64_t* applied) {
  if (entries.empty()) return absl::OkStatus();
  const int p = entries.size() < 4096 ? 1 : opt_.recovery_threads;
  if (p == 1) {
    for (const auto& e : entries) {
      if (auto st = mem_.ApplyBatch(e.payload); !st.ok()) return st;
    }
    *applied = entries.back().index;
    return absl::OkStatus();
  }
  // Phase 1: decode contiguous chunks of the log in parallel, bucketing each
  // op by the worker that owns its shard. Phase 2: each worker applies its
  // buckets chunk by chunk, so ops on a shard still apply in log order.
  const size_t n = entries.size();
  const size_t chunk = (n + static_cast<size_t>(p) - 1) / static_cast<size_t>(p);
  std::vector<std::vector<std::vector<BatchOp>>> buckets(
      static_cast<size_t>(p), std::vector<std::vector<BatchOp>>(static_cast<size_t>(p)));
  std::vector<absl::Status> errs(static_cast<size_t>(p));
  RunParallel(p, [&](int c) {
    const size_t lo = static_cast<size_t>(c) * chunk, hi = std::min(n, lo + chunk);
    auto& mine = buckets[static_cast<size_t>(c)];
    for (auto& b : mine) b.reserve((hi > lo ? hi - lo : 0) / static_cast<size_t>(p) + 16);
    for (size_t i = lo; i < hi; ++i) {
      BatchReader r(entries[i].payload);
      BatchOp op;
      while (r.Next(&op)) mine[mem_.ShardOf(op.key) % static_cast<uint32_t>(p)].push_back(op);
      if (!r.status().ok()) {
        errs[static_cast<size_t>(c)] = r.status();
        return;
      }
    }
  });
  for (auto& e : errs) {
    if (!e.ok()) return e;
  }
  RunParallel(p, [&](int w) {
    for (int c = 0; c < p; ++c) {
      for (const BatchOp& op : buckets[static_cast<size_t>(c)][static_cast<size_t>(w)]) {
        if (op.type == OpType::kPut) {
          mem_.Put(op.key, op.value);
        } else {
          mem_.Delete(op.key);
        }
      }
    }
  });
  *applied = entries.back().index;
  return absl::OkStatus();
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

absl::Status Engine::Put(std::string_view key, std::string_view value) {
  WriteBatch b;
  b.Put(key, value);
  return Write(std::move(b));
}

absl::Status Engine::Delete(std::string_view key) {
  WriteBatch b;
  b.Delete(key);
  return Write(std::move(b));
}

absl::Status Engine::Write(WriteBatch batch) {
  if (batch.Empty()) return absl::OkStatus();
  SyncWaiter waiter;
  if (opt_.concurrency == Concurrency::kGlobalLock) {
    // Naive baseline: the lock is held across append, fsync and apply.
    std::lock_guard<std::mutex> g(global_mu_);
    wal_->Append(Wal::Record{0, 0, 0, batch.ReleaseRep(), &waiter});
    return waiter.Wait();
  }
  wal_->Append(Wal::Record{0, 0, 0, batch.ReleaseRep(), &waiter});
  return waiter.Wait();
}

void Engine::WriteAsync(WriteBatch batch, WriteCompletion* done) {
  if (batch.Empty()) {
    done->Done(absl::OkStatus(), AppliedIndex());
    return;
  }
  if (opt_.concurrency == Concurrency::kGlobalLock) {
    done->Done(Write(std::move(batch)), AppliedIndex());
    return;
  }
  wal_->Append(Wal::Record{0, 0, 0, batch.ReleaseRep(), done});
}

void Engine::OnWalGroup(const absl::Status& st, std::vector<Wal::Record>&& group) {
  if (opt_.concurrency == Concurrency::kGlobalLock) {
    // The writer holds global_mu_ while it waits, so applying here is
    // serialized with readers exactly as in the naive design.
    for (auto& r : group) {
      if (st.ok()) {
        mem_.ApplyBatch(r.payload).IgnoreError();
        applied_index_.store(r.index, std::memory_order_release);
        bytes_since_snapshot_ += r.payload.size() + wal::kHeaderSize;
      }
      static_cast<WriteCompletion*>(r.tag)->Done(st, r.index);
    }
    MaybeStartSnapshot();
    return;
  }
  std::vector<Applier::Entry> entries;
  entries.reserve(group.size());
  uint64_t bytes = 0;
  for (auto& r : group) {
    bytes += r.payload.size() + wal::kHeaderSize;
    entries.push_back(Applier::Entry{r.index, std::move(r.payload),
                                     static_cast<WriteCompletion*>(r.tag), st, true});
  }
  std::lock_guard<std::mutex> l(dispatch_mu_);
  if (st.ok()) bytes_since_snapshot_ += bytes;
  applier_->Submit(std::move(entries));
  MaybeStartSnapshot();
}

uint64_t Engine::AppliedIndex() const {
  if (applier_) return applier_->AppliedIndex();
  return applied_index_.load(std::memory_order_acquire);
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

bool Engine::Get(std::string_view key, std::string* value) const {
  if (opt_.concurrency == Concurrency::kGlobalLock) {
    std::lock_guard<std::mutex> g(global_mu_);
    return mem_.Get(key, value);
  }
  return mem_.Get(key, value);
}

std::vector<std::pair<std::string, std::string>> Engine::Scan(std::string_view start,
                                                             std::string_view end,
                                                             size_t limit) const {
  if (opt_.concurrency == Concurrency::kGlobalLock) {
    std::lock_guard<std::mutex> g(global_mu_);
    return mem_.Scan(start, end, limit);
  }
  return mem_.Scan(start, end, limit);
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// Called with dispatch to the applier blocked (dispatch_mu_, or global_mu_ in
// baseline mode). RunQuiesced waits for the appliers to drain, so the memtable
// is exactly the state after `applied` when BeginSnapshot() marks it.
void Engine::MaybeStartSnapshot() {
  std::lock_guard<std::mutex> l(snap_mu_);
  if (snap_requested_) return;
  const bool due = opt_.snapshot_trigger_bytes > 0 && bytes_since_snapshot_ >= opt_.snapshot_trigger_bytes;
  if (!due && !snap_forced_) return;
  auto begin = [&](uint64_t applied) {
    mem_.BeginSnapshot();
    snap_index_ = applied;
  };
  if (applier_) {
    applier_->RunQuiesced(begin);
  } else {
    begin(applied_index_.load(std::memory_order_acquire));
  }
  snap_requested_ = true;
  snap_forced_ = false;
  bytes_since_snapshot_ = 0;
  snap_cv_.notify_all();
}

void Engine::SnapshotLoop() {
  SetThreadName("snapshot");
  for (;;) {
    uint64_t index;
    {
      std::unique_lock<std::mutex> l(snap_mu_);
      snap_cv_.wait(l, [&] { return snap_requested_ || snap_stop_; });
      if (snap_stop_ && !snap_requested_) return;
      index = snap_index_;
    }
    const auto t0 = Clock::now();
    SnapshotWriteOptions so;
    so.index = index;
    so.wal_start_segment = wal_ ? wal_->FirstSegmentAbove(index) : 0;
    so.files = opt_.snapshot_files;
    so.threads = opt_.snapshot_threads;
    so.rate_limit_bytes_per_s = opt_.snapshot_rate_bytes_per_s;
    auto m = WriteSnapshot(opt_.dir + "/snapshots", mem_, so);
    if (m.ok() && wal_) wal_->ReleaseUpTo(index);
    if (!m.ok()) KV_LOG(Error) << "snapshot at " << index << " failed: " << m.status();
    {
      std::lock_guard<std::mutex> l(snap_mu_);
      snap_requested_ = false;
      ++snapshots_done_;
      last_snapshot_status_ = m.status();
      if (m.ok()) {
        last_snapshot_index_ = index;
        last_snapshot_seconds_ = SecondsSince(t0);
      }
    }
    snap_cv_.notify_all();
  }
}

absl::Status Engine::Checkpoint() {
  uint64_t target;
  {
    std::unique_lock<std::mutex> l(snap_mu_);
    // A snapshot already running may predate writes acknowledged before this
    // call, so wait it out and require the next one.
    snap_cv_.wait(l, [&] { return !snap_requested_; });
    target = snapshots_done_ + 1;
    snap_forced_ = true;
  }
  if (opt_.concurrency == Concurrency::kGlobalLock) {
    std::lock_guard<std::mutex> g(global_mu_);
    MaybeStartSnapshot();
  } else {
    std::lock_guard<std::mutex> d(dispatch_mu_);
    MaybeStartSnapshot();
  }
  std::unique_lock<std::mutex> l(snap_mu_);
  snap_cv_.wait(l, [&] { return snapshots_done_ >= target; });
  return last_snapshot_status_;
}

EngineStats Engine::Stats() const {
  EngineStats s;
  s.applied_index = AppliedIndex();
  s.keys = mem_.KeyCount();
  s.bytes = mem_.ByteSize();
  {
    std::lock_guard<std::mutex> l(snap_mu_);
    s.snapshots = snapshots_done_;
    s.last_snapshot_index = last_snapshot_index_;
    s.last_snapshot_seconds = last_snapshot_seconds_;
  }
  if (wal_) s.wal = wal_->Stats();
  return s;
}

}  // namespace kv
