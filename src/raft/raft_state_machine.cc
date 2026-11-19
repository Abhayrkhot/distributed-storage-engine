#include "raft/raft_state_machine.h"

#include "common/coding.h"
#include "common/logging.h"

namespace kv::raft {

std::string EncodeNoop() { return std::string(1, static_cast<char>(EntryType::kNoop)); }

std::string EncodeWrite(uint64_t client_id, uint64_t seq, std::string_view batch_rep) {
  std::string out;
  out.reserve(17 + batch_rep.size());
  out.push_back(static_cast<char>(EntryType::kWrite));
  PutFixed64(&out, client_id);
  PutFixed64(&out, seq);
  out.append(batch_rep);
  return out;
}

absl::Status DecodeEntry(std::string_view payload, DecodedEntry* out) {
  if (payload.empty()) return absl::DataLossError("empty raft entry");
  out->type = static_cast<EntryType>(payload[0]);
  if (out->type == EntryType::kNoop) return absl::OkStatus();
  if (out->type != EntryType::kWrite || payload.size() < 17) {
    return absl::DataLossError("malformed raft entry");
  }
  out->client_id = DecodeFixed64(payload.data() + 1);
  out->seq = DecodeFixed64(payload.data() + 9);
  out->batch = payload.substr(17);
  return absl::OkStatus();
}

StateMachine::StateMachine(uint32_t num_shards, int apply_threads, size_t max_sessions)
    : mem_(num_shards), applier_(&mem_, apply_threads), max_sessions_(max_sessions) {}

void StateMachine::Touch(uint64_t client_id, uint64_t seq, uint64_t index) {
  auto [it, inserted] = sessions_.try_emplace(client_id);
  if (!inserted) lru_.erase(it->second.last_index);
  it->second.seq = seq;
  it->second.last_index = index;
  lru_[index] = client_id;
  while (sessions_.size() > max_sessions_) {
    // Evict the session idle for the most log entries. Deterministic: every
    // replica sees the same log.
    auto oldest = lru_.begin();
    sessions_.erase(oldest->second);
    lru_.erase(oldest);
  }
}

void StateMachine::Apply(std::vector<CommittedEntry>& entries) {
  std::vector<Applier::Entry> out;
  out.reserve(entries.size());
  for (auto& c : entries) {
    Applier::Entry e;
    e.index = c.index;
    e.done = c.done;
    DecodedEntry d;
    if (auto st = DecodeEntry(*c.data, &d); !st.ok()) {
      KV_LOG(Error) << "committed entry " << c.index << " is malformed: " << st;
      e.apply = false;
      e.status = absl::InternalError("malformed committed entry");
    } else if (d.type == EntryType::kNoop) {
      e.apply = false;
    } else {
      if (d.client_id != 0) {
        auto it = sessions_.find(d.client_id);
        if (it != sessions_.end() && d.seq <= it->second.seq) {
          // A retry of a write that already took effect (e.g. the client timed
          // out while the old leader committed it). Acknowledge, don't redo.
          e.apply = false;
          duplicates_.fetch_add(1, std::memory_order_relaxed);
        } else {
          Touch(d.client_id, d.seq, c.index);
        }
      }
      if (e.apply) e.payload.assign(d.batch.data(), d.batch.size());
    }
    out.push_back(std::move(e));
  }
  applier_.Submit(std::move(out));
}

void StateMachine::WaitApplied(uint64_t index) { applier_.WaitApplied(index); }

void StateMachine::BeginSnapshot(uint64_t* index, std::string* app_state) {
  applier_.RunQuiesced([&](uint64_t applied) {
    mem_.BeginSnapshot();
    *index = applied;
  });
  *app_state = EncodeSessions();
}

std::string StateMachine::EncodeSessions() const {
  std::string out;
  PutVarint64(&out, sessions_.size());
  for (const auto& [index, client] : lru_) {
    PutFixed64(&out, client);
    PutVarint64(&out, sessions_.at(client).seq);
    PutVarint64(&out, index);
  }
  return out;
}

absl::Status StateMachine::DecodeSessions(std::string_view data) {
  sessions_.clear();
  lru_.clear();
  if (data.empty()) return absl::OkStatus();
  uint64_t n;
  if (!GetVarint64(&data, &n)) return absl::DataLossError("bad session table");
  for (uint64_t i = 0; i < n; ++i) {
    if (data.size() < 8) return absl::DataLossError("truncated session table");
    const uint64_t client = DecodeFixed64(data.data());
    data.remove_prefix(8);
    uint64_t seq, index;
    if (!GetVarint64(&data, &seq) || !GetVarint64(&data, &index)) {
      return absl::DataLossError("truncated session entry");
    }
    sessions_[client] = Session{seq, index};
    lru_[index] = client;
  }
  return absl::OkStatus();
}

absl::Status StateMachine::Restore(const std::string& root, const snapshot::SnapshotManifest& m,
                                   int threads) {
  applier_.RunQuiesced([](uint64_t) {});
  mem_.Clear();
  if (auto st = LoadSnapshot(root, m, mem_, threads, nullptr); !st.ok()) return st;
  if (auto st = DecodeSessions(m.app_state()); !st.ok()) return st;
  applier_.Reset(m.index());
  return absl::OkStatus();
}

}  // namespace kv::raft
