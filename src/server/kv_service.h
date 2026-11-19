// gRPC front end. KV uses the callback API so a write occupies no thread
// while it waits for replication and fsync; completion comes from the applier.
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "proto/kv.grpc.pb.h"
#include "raft/raft_node.h"
#include "storage/engine.h"

namespace kv::server {

// Where requests are served from: a Raft group, or a standalone engine.
class Backend {
 public:
  virtual ~Backend() = default;
  virtual void Write(uint64_t client_id, uint64_t seq, WriteBatch batch, WriteCompletion* done) = 0;
  // done(OK) when a read with this consistency may be served locally.
  virtual void ReadBarrier(api::Consistency c, std::function<void(const absl::Status&)> done) = 0;
  virtual bool Get(std::string_view key, std::string* value) = 0;
  virtual std::vector<std::pair<std::string, std::string>> Scan(std::string_view start,
                                                                std::string_view end,
                                                                size_t limit) = 0;
  virtual void FillStatus(api::StatusResponse* resp) = 0;
  virtual absl::Status TransferLeadership(uint32_t target, uint32_t* new_leader) = 0;
  virtual absl::Status Snapshot(uint64_t* index) = 0;
};

class RaftBackend final : public Backend {
 public:
  explicit RaftBackend(raft::RaftNode* node) : node_(node) {}
  void Write(uint64_t client_id, uint64_t seq, WriteBatch batch, WriteCompletion* done) override;
  void ReadBarrier(api::Consistency c, std::function<void(const absl::Status&)> done) override;
  bool Get(std::string_view key, std::string* value) override;
  std::vector<std::pair<std::string, std::string>> Scan(std::string_view start,
                                                        std::string_view end,
                                                        size_t limit) override;
  void FillStatus(api::StatusResponse* resp) override;
  absl::Status TransferLeadership(uint32_t target, uint32_t* new_leader) override;
  absl::Status Snapshot(uint64_t* index) override;

 private:
  raft::RaftNode* node_;
};

// Fixed pool for blocking work (the global-lock baseline's writes), so the
// gRPC callback threads never block.
class WorkPool {
 public:
  explicit WorkPool(int threads);
  ~WorkPool();
  void Run(std::function<void()> fn);

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> q_;
  bool stop_ = false;
  std::vector<std::thread> threads_;
};

class EngineBackend final : public Backend {
 public:
  explicit EngineBackend(Engine* engine);
  void Write(uint64_t client_id, uint64_t seq, WriteBatch batch, WriteCompletion* done) override;
  void ReadBarrier(api::Consistency c, std::function<void(const absl::Status&)> done) override;
  bool Get(std::string_view key, std::string* value) override;
  std::vector<std::pair<std::string, std::string>> Scan(std::string_view start,
                                                        std::string_view end,
                                                        size_t limit) override;
  void FillStatus(api::StatusResponse* resp) override;
  absl::Status TransferLeadership(uint32_t target, uint32_t* new_leader) override;
  absl::Status Snapshot(uint64_t* index) override;

 private:
  Engine* engine_;
  std::unique_ptr<WorkPool> pool_;  // global-lock mode only
};

struct ServiceCounters {
  std::atomic<uint64_t> writes{0};
  std::atomic<uint64_t> write_errors{0};
  std::atomic<uint64_t> reads{0};
  std::atomic<uint64_t> read_errors{0};
  std::atomic<uint64_t> not_leader{0};
};

class KvService final : public api::KV::CallbackService {
 public:
  explicit KvService(Backend* backend) : backend_(backend) {}
  grpc::ServerUnaryReactor* Put(grpc::CallbackServerContext* ctx, const api::PutRequest* req,
                                api::WriteResponse* resp) override;
  grpc::ServerUnaryReactor* Delete(grpc::CallbackServerContext* ctx,
                                   const api::DeleteRequest* req,
                                   api::WriteResponse* resp) override;
  grpc::ServerUnaryReactor* Write(grpc::CallbackServerContext* ctx, const api::WriteRequest* req,
                                  api::WriteResponse* resp) override;
  grpc::ServerUnaryReactor* Get(grpc::CallbackServerContext* ctx, const api::GetRequest* req,
                                api::GetResponse* resp) override;
  grpc::ServerUnaryReactor* Scan(grpc::CallbackServerContext* ctx, const api::ScanRequest* req,
                                 api::ScanResponse* resp) override;
  ServiceCounters& counters() { return counters_; }

 private:
  grpc::ServerUnaryReactor* SubmitWrite(grpc::CallbackServerContext* ctx,
                                        const api::Session& session, WriteBatch batch,
                                        api::WriteResponse* resp);
  Backend* backend_;
  ServiceCounters counters_;
};

class AdminService final : public api::Admin::Service {
 public:
  explicit AdminService(Backend* backend) : backend_(backend) {}
  grpc::Status Status(grpc::ServerContext*, const api::StatusRequest*,
                      api::StatusResponse* resp) override;
  grpc::Status TransferLeadership(grpc::ServerContext*, const api::TransferLeadershipRequest* req,
                                  api::TransferLeadershipResponse* resp) override;
  grpc::Status Snapshot(grpc::ServerContext*, const api::SnapshotRequest*,
                        api::SnapshotResponse* resp) override;

 private:
  Backend* backend_;
};

grpc::Status ToGrpc(const absl::Status& st);

}  // namespace kv::server
