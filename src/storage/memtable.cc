#include "storage/memtable.h"

#include <algorithm>
#include <mutex>

#include "common/coding.h"
#include "common/crc32c.h"
#include "storage/write_batch.h"

namespace kv {

uint64_t KeyHash(std::string_view key) {
  // CRC32C runs at memory speed with the hardware instruction; the splitmix
  // finalizer spreads its 32 bits over the full word.
  uint64_t x = crc32c::Value(key.data(), key.size()) ^ (static_cast<uint64_t>(key.size()) << 32);
  x += 0x9e3779b97f4a7c15ull;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
  return x ^ (x >> 31);
}

MemTable::MemTable(uint32_t num_shards) {
  if (num_shards == 0) num_shards = 1;
  shards_.reserve(num_shards);
  for (uint32_t i = 0; i < num_shards; ++i) shards_.push_back(std::make_unique<Shard>());
}

void MemTable::Put(std::string_view key, std::string_view value) {
  Shard& s = ShardFor(key);
  std::unique_lock<std::shared_mutex> l(s.mu);
  auto it = s.map.find(key);
  if (s.snap_pending && !s.undo.contains(key)) {
    s.undo.emplace(std::string(key), it == s.map.end() ? std::nullopt
                                                       : std::optional<std::string>(it->second));
  }
  if (it == s.map.end()) {
    s.map.emplace(std::string(key), std::string(value));
    s.bytes += key.size() + value.size();
  } else {
    s.bytes = s.bytes - it->second.size() + value.size();
    it->second.assign(value.data(), value.size());
  }
}

void MemTable::Delete(std::string_view key) {
  Shard& s = ShardFor(key);
  std::unique_lock<std::shared_mutex> l(s.mu);
  auto it = s.map.find(key);
  if (it == s.map.end()) return;
  if (s.snap_pending && !s.undo.contains(key)) {
    s.undo.emplace(std::string(key), std::optional<std::string>(it->second));
  }
  s.bytes -= it->first.size() + it->second.size();
  s.map.erase(it);
}

absl::Status MemTable::ApplyBatch(std::string_view batch_rep) {
  BatchReader r(batch_rep);
  BatchOp op;
  while (r.Next(&op)) {
    if (op.type == OpType::kPut) {
      Put(op.key, op.value);
    } else {
      Delete(op.key);
    }
  }
  return r.status();
}

bool MemTable::Get(std::string_view key, std::string* value) const {
  const Shard& s = ShardFor(key);
  std::shared_lock<std::shared_mutex> l(s.mu);
  auto it = s.map.find(key);
  if (it == s.map.end()) return false;
  value->assign(it->second);
  return true;
}

std::vector<std::pair<std::string, std::string>> MemTable::Scan(std::string_view start,
                                                               std::string_view end,
                                                               size_t limit) const {
  // Each shard is sorted, so collect up to `limit` candidates per shard and
  // merge. Good enough for bounded range reads; this is a point-lookup engine.
  std::vector<std::pair<std::string, std::string>> all;
  for (const auto& sp : shards_) {
    std::shared_lock<std::shared_mutex> l(sp->mu);
    size_t n = 0;
    for (auto it = sp->map.lower_bound(start); it != sp->map.end() && n < limit; ++it, ++n) {
      if (!end.empty() && std::string_view(it->first) >= end) break;
      all.emplace_back(it->first, it->second);
    }
  }
  std::sort(all.begin(), all.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  if (all.size() > limit) all.resize(limit);
  return all;
}

uint64_t MemTable::KeyCount() const {
  uint64_t n = 0;
  for (const auto& sp : shards_) {
    std::shared_lock<std::shared_mutex> l(sp->mu);
    n += sp->map.size();
  }
  return n;
}

uint64_t MemTable::ByteSize() const {
  uint64_t n = 0;
  for (const auto& sp : shards_) {
    std::shared_lock<std::shared_mutex> l(sp->mu);
    n += sp->bytes;
  }
  return n;
}

void MemTable::BeginSnapshot() {
  for (auto& sp : shards_) {
    std::unique_lock<std::shared_mutex> l(sp->mu);
    sp->snap_pending = true;
    sp->undo.clear();
  }
}

uint64_t MemTable::SerializeShard(uint32_t shard, std::string* out) {
  Shard& s = *shards_[shard];
  uint64_t count = 0;
  auto emit = [&](std::string_view k, std::string_view v) {
    PutLengthPrefixed(out, k);
    PutLengthPrefixed(out, v);
    ++count;
  };
  {
    // A shared lock is enough: the applier needs the exclusive lock to modify
    // this shard, so the map and undo log are frozen while we read them, and
    // readers are not blocked.
    std::shared_lock<std::shared_mutex> l(s.mu);
    // Keys deleted since the snapshot point exist only in the undo log.
    std::vector<std::pair<std::string_view, std::string_view>> resurrected;
    for (const auto& [k, old] : s.undo) {
      if (old.has_value() && !s.map.contains(k)) resurrected.emplace_back(k, *old);
    }
    std::sort(resurrected.begin(), resurrected.end());
    auto rit = resurrected.begin();
    for (const auto& [k, v] : s.map) {
      while (rit != resurrected.end() && rit->first < std::string_view(k)) {
        emit(rit->first, rit->second);
        ++rit;
      }
      if (s.snap_pending) {
        auto u = s.undo.find(k);
        if (u != s.undo.end()) {
          if (u->second.has_value()) emit(k, *u->second);  // modified after the snapshot
          continue;                                         // or created after it
        }
      }
      emit(k, v);
    }
    for (; rit != resurrected.end(); ++rit) emit(rit->first, rit->second);
  }
  std::unique_lock<std::shared_mutex> l(s.mu);
  s.snap_pending = false;
  s.undo.clear();
  return count;
}

absl::Status MemTable::LoadShard(uint32_t shard, std::string_view data, uint64_t count) {
  if (shard >= shards_.size()) return absl::InvalidArgumentError("shard out of range");
  Shard& s = *shards_[shard];
  std::unique_lock<std::shared_mutex> l(s.mu);
  for (uint64_t i = 0; i < count; ++i) {
    std::string_view k, v;
    if (!GetLengthPrefixed(&data, &k) || !GetLengthPrefixed(&data, &v)) {
      return absl::DataLossError("truncated snapshot shard");
    }
    // Entries arrive sorted, so hinting at end() makes each insert O(1).
    s.map.insert(s.map.end(), {std::string(k), std::string(v)});
    s.bytes += k.size() + v.size();
  }
  if (!data.empty()) return absl::DataLossError("trailing bytes in snapshot shard");
  return absl::OkStatus();
}

void MemTable::AbortSnapshot() {
  for (auto& sp : shards_) {
    std::unique_lock<std::shared_mutex> l(sp->mu);
    sp->snap_pending = false;
    sp->undo.clear();
  }
}

void MemTable::Clear() {
  for (auto& sp : shards_) {
    std::unique_lock<std::shared_mutex> l(sp->mu);
    sp->map.clear();
    sp->undo.clear();
    sp->snap_pending = false;
    sp->bytes = 0;
  }
}

}  // namespace kv
