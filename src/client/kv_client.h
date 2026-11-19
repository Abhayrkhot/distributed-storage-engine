// Synchronous client. Finds the leader from redirects ("not leader;
// leader=<id>@<addr>"), retries on UNAVAILABLE / DEADLINE_EXCEEDED with
// backoff, and tags every write with (client_id, seq) so a retry of a write
// whose outcome was unknown is applied at most once.
//
// One Client is one session: use it from one thread at a time (or give each
// thread its own Client).
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/kv.grpc.pb.h"

namespace kv::client {

struct ClientOptions {
  std::vector<std::string> endpoints;
  int timeout_ms = 10000;    // overall budget per operation, including retries
  int attempt_ms = 1500;     // per attempt
  bool sessions = true;      // exactly-once writes
  uint64_t client_id = 0;    // 0 = random
};

struct ClientStats {
  uint64_t retries = 0;
  uint64_t redirects = 0;
};

std::shared_ptr<grpc::Channel> MakeChannel(const std::string& addr);
// Parses "...leader=<id>@<host:port>" out of an error message.
std::string LeaderFromError(const std::string& message);

class Client {
 public:
  explicit Client(ClientOptions options);

  absl::Status Put(std::string_view key, std::string_view value, uint64_t* index = nullptr);
  absl::Status Delete(std::string_view key, uint64_t* index = nullptr);
  absl::Status Write(const std::vector<api::Op>& ops, uint64_t* index = nullptr);
  absl::StatusOr<std::optional<std::string>> Get(std::string_view key,
                                                 api::Consistency c = api::LINEARIZABLE);
  absl::StatusOr<std::vector<std::pair<std::string, std::string>>> Scan(
      std::string_view start, std::string_view end, uint32_t limit,
      api::Consistency c = api::LINEARIZABLE);

  absl::StatusOr<api::StatusResponse> Status(const std::string& endpoint);
  absl::StatusOr<uint32_t> TransferLeadership(uint32_t target);
  absl::StatusOr<uint64_t> Snapshot();

  std::string leader() const;
  uint64_t client_id() const { return client_id_; }
  ClientStats stats() const { return {retries_.load(), redirects_.load()}; }

 private:
  struct Endpoint {
    std::string addr;
    std::unique_ptr<api::KV::Stub> kv;
    std::unique_ptr<api::Admin::Stub> admin;
  };
  Endpoint* EndpointFor(const std::string& addr);
  // Runs fn(endpoint, ctx) against the presumed leader with redirects and retries.
  template <typename Fn>
  absl::Status Call(Fn fn);

  ClientOptions opt_;
  uint64_t client_id_;
  uint64_t seq_ = 0;
  mutable std::mutex mu_;
  std::vector<std::unique_ptr<Endpoint>> endpoints_;
  size_t leader_ = 0;
  std::atomic<uint64_t> retries_{0}, redirects_{0};
};

}  // namespace kv::client
