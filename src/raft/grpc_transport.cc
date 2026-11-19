#include "raft/grpc_transport.h"

#include <grpcpp/grpcpp.h>

namespace kv::raft {

std::string OffsetPort(const std::string& addr, int offset) {
  if (offset == 0) return addr;
  const auto colon = addr.rfind(':');
  return addr.substr(0, colon + 1) + std::to_string(std::stoi(addr.substr(colon + 1)) + offset);
}

Transport::Transport(uint32_t self, const std::map<uint32_t, std::string>& members,
                     int peer_port_offset)
    : self_(self), members_(members) {
  for (const auto& [id, addr] : members_) {
    if (id == self_) continue;
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(256 << 20);
    args.SetMaxSendMessageSize(256 << 20);
    // Reconnect quickly after a peer restarts; the default backoff can reach
    // tens of seconds and would stretch failover.
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 50);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 500);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    // Each peer gets its own TCP connection rather than sharing a subchannel.
    args.SetInt("grpc.channel_id", static_cast<int>(id));
    auto channel = grpc::CreateCustomChannel(OffsetPort(addr, peer_port_offset),
                                             grpc::InsecureChannelCredentials(), args);
    stubs_[id] = ::kv::raft::Raft::NewStub(channel);
  }
}

::kv::raft::Raft::Stub* Transport::Stub(uint32_t peer) {
  auto it = stubs_.find(peer);
  return it == stubs_.end() ? nullptr : it->second.get();
}

void Transport::SetBlocked(const std::set<uint32_t>& peers) {
  std::lock_guard<std::mutex> l(mu_);
  blocked_ = peers;
}

bool Transport::IsBlocked(uint32_t peer) const {
  std::lock_guard<std::mutex> l(mu_);
  return blocked_.count(peer) > 0;
}

}  // namespace kv::raft
