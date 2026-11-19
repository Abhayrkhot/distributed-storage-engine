// The replicated state machine: memtable + parallel applier + client sessions.
//
// Raft entry payload:
//   kNoop  := 0x00                      (leader's first entry in a term)
//   kWrite := 0x01 client_id:u64 seq:u64 batch
//
// Committed entries are fed in log order by a single dispatcher. Exactly-once
// semantics come from the session table, which the dispatcher consults before
// handing an entry to the applier: a (client_id, seq) at or below the last one
// applied for that client is a retry of a write that already took effect, so
// it is acknowledged without being applied again. Because the dispatcher is
// the only reader/writer of the table and runs in log order, every replica
// makes the same decision.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "absl/status/status.h"
#include "storage/applier.h"
#include "storage/engine.h"
#include "storage/memtable.h"
#include "storage/snapshot.h"

namespace kv::raft {

enum class EntryType : uint8_t { kNoop = 0, kWrite = 1 };

std::string EncodeNoop();
std::string EncodeWrite(uint64_t client_id, uint64_t seq, std::string_view batch_rep);

struct DecodedEntry {
  EntryType type = EntryType::kNoop;
  uint64_t client_id = 0;
  uint64_t seq = 0;
  std::string_view batch;
};
absl::Status DecodeEntry(std::string_view payload, DecodedEntry* out);

struct CommittedEntry {
  uint64_t index = 0;
  std::shared_ptr<const std::string> data;
  WriteCompletion* done = nullptr;  // set on the leader that proposed it
};

class StateMachine {
 public:
  StateMachine(uint32_t num_shards, int apply_threads, size_t max_sessions = 1 << 16);

  // Dispatcher thread only, in log order.
  void Apply(std::vector<CommittedEntry>& entries);
  uint64_t AppliedIndex() const { return applier_.AppliedIndex(); }
  // Blocks until AppliedIndex() >= index.
  void WaitApplied(uint64_t index);
  bool WaitAppliedFor(uint64_t index, std::chrono::milliseconds timeout) {
    return applier_.WaitAppliedFor(index, timeout);
  }

  bool Get(std::string_view key, std::string* value) const { return mem_.Get(key, value); }
  std::vector<std::pair<std::string, std::string>> Scan(std::string_view s, std::string_view e,
                                                        size_t limit) const {
    return mem_.Scan(s, e, limit);
  }
  uint64_t KeyCount() const { return mem_.KeyCount(); }

  // Snapshot support (dispatcher thread): waits for in-flight applies, marks
  // the memtable copy-on-write and returns the session table as app state.
  void BeginSnapshot(uint64_t* index, std::string* app_state);
  MemTable& memtable() { return mem_; }

  // Replaces all state with a snapshot (dispatcher stopped).
  absl::Status Restore(const std::string& root, const snapshot::SnapshotManifest& m, int threads);

  uint64_t duplicates() const { return duplicates_.load(std::memory_order_relaxed); }

 private:
  struct Session {
    uint64_t seq = 0;
    uint64_t last_index = 0;  // for deterministic LRU eviction
  };
  std::string EncodeSessions() const;
  absl::Status DecodeSessions(std::string_view data);
  void Touch(uint64_t client_id, uint64_t seq, uint64_t index);

  MemTable mem_;
  Applier applier_;
  size_t max_sessions_;
  std::unordered_map<uint64_t, Session> sessions_;
  std::map<uint64_t, uint64_t> lru_;  // last_index -> client_id
  std::atomic<uint64_t> duplicates_{0};
};

}  // namespace kv::raft
