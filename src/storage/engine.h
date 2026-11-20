// Standalone storage engine: group-commit WAL + sharded memtable + background
// snapshots, with crash recovery on Open().
//
// Write path (kPipelined, the default):
//
//   caller ──Append──▶ WAL queue ──writer thread: pwrite + fdatasync──▶
//   Applier (N workers, routed by shard): memtable + complete ──▶ caller returns
//
// A write is acknowledged only after it is durable and visible. Each stage
// batches whatever accumulated while the previous batch was in flight, so
// throughput scales with load while a lone writer still sees one flush.
//
// kGlobalLock is the naive design kept for comparison: one mutex held across
// the WAL append, the fdatasync and the memtable update, and taken by readers.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "storage/applier.h"
#include "storage/memtable.h"
#include "storage/wal.h"
#include "storage/write_batch.h"

namespace kv {

enum class Concurrency { kPipelined, kGlobalLock };

struct EngineOptions {
  std::string dir;
  uint32_t num_shards = 256;
  Concurrency concurrency = Concurrency::kPipelined;
  int apply_threads = 4;  // parallel appliers (kPipelined)
  WalOptions wal;  // wal.dir defaults to <dir>/wal
  // Take a snapshot once this many WAL bytes accumulate (0 = never).
  uint64_t snapshot_trigger_bytes = 512ull << 20;
  int snapshot_files = 16;
  int snapshot_threads = 4;
  uint64_t snapshot_rate_bytes_per_s = 128ull << 20;  // 0 = unlimited
  int recovery_threads = 0;  // 0 = hardware concurrency
};

struct RecoveryStats {
  double total_seconds = 0;
  double snapshot_seconds = 0;
  double wal_seconds = 0;
  uint64_t snapshot_index = 0;
  uint64_t snapshot_keys = 0;
  uint64_t wal_segments = 0;
  uint64_t wal_records = 0;
  uint64_t wal_bytes = 0;
  uint64_t torn_bytes = 0;
  uint64_t last_index = 0;
  uint64_t keys = 0;
};

struct EngineStats {
  uint64_t applied_index = 0;
  uint64_t keys = 0;
  uint64_t bytes = 0;
  uint64_t snapshots = 0;
  uint64_t last_snapshot_index = 0;
  double last_snapshot_seconds = 0;
  WalStats wal;
};

// Completion hook for asynchronous writes.
class WriteCompletion {
 public:
  virtual ~WriteCompletion() = default;
  virtual void Done(const absl::Status& status, uint64_t index) = 0;
};

class Engine {
 public:
  static absl::StatusOr<std::unique_ptr<Engine>> Open(const EngineOptions& options);
  ~Engine();

  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  absl::Status Put(std::string_view key, std::string_view value);
  absl::Status Delete(std::string_view key);
  // Blocks until the batch is durable and visible.
  absl::Status Write(WriteBatch batch);
  // Returns immediately; `done` runs on an applier thread (keep it short).
  void WriteAsync(WriteBatch batch, WriteCompletion* done);

  bool Get(std::string_view key, std::string* value) const;
  std::vector<std::pair<std::string, std::string>> Scan(std::string_view start,
                                                        std::string_view end,
                                                        size_t limit) const;

  // Takes a snapshot now and waits for it (tests, admin RPC).
  absl::Status Checkpoint();

  uint64_t AppliedIndex() const;
  EngineStats Stats() const;
  const RecoveryStats& recovery() const { return recovery_; }
  const EngineOptions& options() const { return opt_; }
  absl::Status Health() const { return wal_->Health(); }

 private:
  explicit Engine(const EngineOptions& options);
  absl::Status Recover();
  absl::Status ReplayParallel(std::vector<wal::EntryView>& entries, uint64_t* applied);
  void OnWalGroup(const absl::Status& st, std::vector<Wal::Record>&& group);
  void SnapshotLoop();
  // Starts a snapshot if one is due or forced. Caller holds dispatch_mu_ (or
  // global_mu_ in baseline mode), so no entry is being dispatched.
  void MaybeStartSnapshot();

  EngineOptions opt_;
  MemTable mem_;
  std::unique_ptr<Applier> applier_;  // kPipelined only
  std::unique_ptr<Wal> wal_;
  RecoveryStats recovery_;

  // Serializes dispatch to the applier with snapshot starts.
  std::mutex dispatch_mu_;
  uint64_t bytes_since_snapshot_ = 0;  // guarded by dispatch_mu_ / global_mu_

  // Baseline mode only.
  mutable std::mutex global_mu_;
  std::atomic<uint64_t> applied_index_{0};

  // Snapshots.
  mutable std::mutex snap_mu_;
  std::condition_variable snap_cv_;
  bool snap_requested_ = false;  // a snapshot was started; the snapshot thread must finish it
  bool snap_forced_ = false;     // Checkpoint() wants one now
  uint64_t snap_index_ = 0;
  uint64_t snapshots_done_ = 0;
  uint64_t last_snapshot_index_ = 0;
  double last_snapshot_seconds_ = 0;
  absl::Status last_snapshot_status_;
  bool snap_stop_ = false;
  std::thread snapshotter_;
};

}  // namespace kv
