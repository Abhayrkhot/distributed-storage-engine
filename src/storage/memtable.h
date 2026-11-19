// The in-memory index: keys are hash-partitioned across N shards, each an
// ordered B-tree behind its own reader/writer lock. Readers on different
// shards never touch the same cache line, and a writer only blocks readers of
// the one shard it is modifying.
//
// Point-in-time snapshots without stopping writes: BeginSnapshot() (called by
// the single applier between two log entries) marks every shard. Until a
// shard has been serialized, the first modification of each key records the
// key's previous value in that shard's undo map, so the serializer can emit the
// shard exactly as it was at the snapshot index while new writes keep landing.
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"

namespace kv {

// Stable across processes and builds (snapshots store keys per shard).
uint64_t KeyHash(std::string_view key);

// Debian's Abseil build uses its own absl::string_view, so the containers get
// explicit transparent functors to allow std::string_view lookups.
struct StringLess {
  using is_transparent = void;
  bool operator()(std::string_view a, std::string_view b) const { return a < b; }
};
struct StringHash {
  using is_transparent = void;
  size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }
};
struct StringEq {
  using is_transparent = void;
  bool operator()(std::string_view a, std::string_view b) const { return a == b; }
};

class MemTable {
 public:
  explicit MemTable(uint32_t num_shards);

  uint32_t num_shards() const { return static_cast<uint32_t>(shards_.size()); }
  uint32_t ShardOf(std::string_view key) const {
    return static_cast<uint32_t>(KeyHash(key) % shards_.size());
  }

  // Mutations. Safe to call concurrently with readers and the snapshot
  // serializer; callers serialize mutations of a given key (the engine has a
  // single applier, or a global lock in the baseline mode).
  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);
  absl::Status ApplyBatch(std::string_view batch_rep);

  bool Get(std::string_view key, std::string* value) const;
  // Keys in [start, end) in order, at most `limit`. Empty `end` = unbounded.
  std::vector<std::pair<std::string, std::string>> Scan(std::string_view start,
                                                        std::string_view end,
                                                        size_t limit) const;

  uint64_t KeyCount() const;
  uint64_t ByteSize() const;

  // ---- snapshots ----
  void BeginSnapshot();
  // Appends shard `shard` as of BeginSnapshot() to `out` as
  // (varint klen, key, varint vlen, value)* in key order. Returns the entry
  // count. Clears the shard's undo log.
  uint64_t SerializeShard(uint32_t shard, std::string* out);
  // Inserts pre-sorted entries produced by SerializeShard (recovery path).
  absl::Status LoadShard(uint32_t shard, std::string_view data, uint64_t count);
  // Drops all data (used before installing a snapshot).
  void Clear();

 private:
  struct alignas(64) Shard {
    mutable std::shared_mutex mu;
    absl::btree_map<std::string, std::string, StringLess> map;
    uint64_t bytes = 0;
    bool snap_pending = false;
    absl::flat_hash_map<std::string, std::optional<std::string>, StringHash, StringEq> undo;
  };

  Shard& ShardFor(std::string_view key) { return *shards_[ShardOf(key)]; }
  const Shard& ShardFor(std::string_view key) const { return *shards_[ShardOf(key)]; }

  std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace kv
