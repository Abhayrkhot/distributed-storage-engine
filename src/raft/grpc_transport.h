// gRPC channels to the other cluster members, plus fault injection used by
// tests and the chaos harness: a "blocked" peer is unreachable in both
// directions (outgoing calls fail fast; incoming RPCs from it are rejected).
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include "proto/raft.grpc.pb.h"

namespace kv::raft {

class Transport {
 public:
  Transport(uint32_t self, const std::map<uint32_t, std::string>& members);

  ::kv::raft::Raft::Stub* Stub(uint32_t peer);
  const std::map<uint32_t, std::string>& members() const { return members_; }
  uint32_t self() const { return self_; }

  void SetBlocked(const std::set<uint32_t>& peers);
  bool IsBlocked(uint32_t peer) const;

 private:
  const uint32_t self_;
  const std::map<uint32_t, std::string> members_;
  std::map<uint32_t, std::unique_ptr<::kv::raft::Raft::Stub>> stubs_;
  mutable std::mutex mu_;
  std::set<uint32_t> blocked_;
};

}  // namespace kv::raft
