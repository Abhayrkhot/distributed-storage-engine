// Applies log entries to the memtable on several threads.
//
// A single producer submits entries in log order. Every operation is routed to
// the worker that owns its shard (shard % workers), so each shard still sees
// its operations in log order, which is all that per-key linearizability
// needs. A batch that spans several workers is split; whichever worker applies
// the last piece completes it. Completions (waking a blocked caller, finishing
// a gRPC reactor) therefore run in parallel too, which is where a single
// applier thread spent most of its time.
//
// RunQuiesced() gives the producer a consistent cut: it waits until every
// submitted entry is applied and runs a function while nothing else is applied.
// Snapshots start there.
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
#include "storage/memtable.h"

namespace kv {

class WriteCompletion;

class Applier {
 public:
  struct Entry {
    uint64_t index = 0;
    std::string payload;               // encoded WriteBatch
    WriteCompletion* done = nullptr;   // may be null (replicated entries from other nodes)
    absl::Status status;               // non-OK: skip the apply, report the error
    bool apply = true;                 // false: complete without applying (dedup, no-op)
  };

  Applier(MemTable* mem, int workers);
  ~Applier();

  Applier(const Applier&) = delete;
  Applier& operator=(const Applier&) = delete;

  // Single producer, log order.
  void Submit(std::vector<Entry>&& entries);
  // Every entry with index <= AppliedIndex() has been applied.
  uint64_t AppliedIndex() const;
  // Blocks until all submitted entries are applied, then runs fn(applied) with
  // the workers idle. Must be called from the producer (or under its lock).
  void RunQuiesced(const std::function<void(uint64_t)>& fn);
  // Sets the starting index (after recovery) without applying anything.
  void Reset(uint64_t applied);
  int workers() const { return static_cast<int>(workers_.size()); }

 private:
  struct Split {
    std::atomic<int> remaining;
  };
  struct Task {
    Entry* entry;
    Split* split;  // null: this worker applies the whole entry
  };
  struct Chunk {
    std::shared_ptr<std::vector<Entry>> block;  // keeps the entries alive
    std::vector<Task> tasks;
    std::shared_ptr<std::vector<std::unique_ptr<Split>>> splits;
    uint64_t last_index = 0;
  };
  struct Worker {
    std::mutex mu;
    std::condition_variable cv;
    std::vector<Chunk> queue;
    bool busy = false;
    bool stop = false;
    alignas(64) std::atomic<uint64_t> progress{0};
    std::thread thread;
  };

  void WorkerLoop(int w);
  void ApplyTask(int w, const Task& t);

  MemTable* mem_;
  std::vector<std::unique_ptr<Worker>> workers_;
  uint64_t dispatched_ = 0;  // producer only
  std::mutex idle_mu_;
  std::condition_variable idle_cv_;
};

}  // namespace kv
