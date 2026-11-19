#include "server/kv_service.h"

#include "common/logging.h"

namespace kv::server {

grpc::Status ToGrpc(const absl::Status& st) {
  if (st.ok()) return grpc::Status::OK;
  grpc::StatusCode code;
  switch (st.code()) {
    case absl::StatusCode::kUnavailable:
      code = grpc::StatusCode::UNAVAILABLE;
      break;
    case absl::StatusCode::kInvalidArgument:
      code = grpc::StatusCode::INVALID_ARGUMENT;
      break;
    case absl::StatusCode::kFailedPrecondition:
      code = grpc::StatusCode::FAILED_PRECONDITION;
      break;
    case absl::StatusCode::kDeadlineExceeded:
      code = grpc::StatusCode::DEADLINE_EXCEEDED;
      break;
    case absl::StatusCode::kNotFound:
      code = grpc::StatusCode::NOT_FOUND;
      break;
    case absl::StatusCode::kDataLoss:
      code = grpc::StatusCode::DATA_LOSS;
      break;
    default:
      code = grpc::StatusCode::INTERNAL;
  }
  return grpc::Status(code, std::string(st.message()));
}

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

void RaftBackend::Write(uint64_t client_id, uint64_t seq, WriteBatch batch, WriteCompletion* done) {
  node_->Propose(client_id, seq, batch.ReleaseRep(), done);
}

void RaftBackend::ReadBarrier(api::Consistency c, std::function<void(const absl::Status&)> done) {
  raft::ReadMode mode = raft::ReadMode::kLinearizable;
  if (c == api::LEASE) mode = raft::ReadMode::kLease;
  if (c == api::STALE) mode = raft::ReadMode::kStale;
  node_->ReadBarrier(mode, std::move(done));
}

bool RaftBackend::Get(std::string_view key, std::string* value) {
  return node_->state().Get(key, value);
}

std::vector<std::pair<std::string, std::string>> RaftBackend::Scan(std::string_view start,
                                                                   std::string_view end,
                                                                   size_t limit) {
  return node_->state().Scan(start, end, limit);
}

void RaftBackend::FillStatus(api::StatusResponse* resp) {
  auto s = node_->Status();
  resp->set_id(s.id);
  resp->set_role(raft::RoleName(s.role));
  resp->set_term(s.term);
  resp->set_leader_id(s.leader_id);
  resp->set_leader_address(s.leader_address);
  resp->set_commit_index(s.commit_index);
  resp->set_applied_index(s.applied_index);
  resp->set_last_log_index(s.last_index);
  resp->set_snapshot_index(s.snapshot_index);
  resp->set_keys(s.keys);
  for (const auto& p : s.peers) {
    auto* ps = resp->add_peers();
    ps->set_id(p.id);
    ps->set_address(p.address);
    ps->set_match_index(p.match);
    ps->set_next_index(p.next);
    ps->set_reachable(p.reachable);
  }
}

absl::Status RaftBackend::TransferLeadership(uint32_t target, uint32_t* new_leader) {
  return node_->TransferLeadership(target, new_leader);
}

absl::Status RaftBackend::Snapshot(uint64_t* index) { return node_->TakeSnapshot(index); }

WorkPool::WorkPool(int threads) {
  for (int i = 0; i < threads; ++i) {
    threads_.emplace_back([this] {
      for (;;) {
        std::function<void()> fn;
        {
          std::unique_lock<std::mutex> l(mu_);
          cv_.wait(l, [&] { return stop_ || !q_.empty(); });
          if (q_.empty()) return;
          fn = std::move(q_.front());
          q_.pop_front();
        }
        fn();
      }
    });
  }
}

WorkPool::~WorkPool() {
  {
    std::lock_guard<std::mutex> l(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto& t : threads_) t.join();
}

void WorkPool::Run(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> l(mu_);
    q_.push_back(std::move(fn));
  }
  cv_.notify_one();
}

EngineBackend::EngineBackend(Engine* engine) : engine_(engine) {
  if (engine_->options().concurrency == Concurrency::kGlobalLock) {
    pool_ = std::make_unique<WorkPool>(128);
  }
}

void EngineBackend::Write(uint64_t, uint64_t, WriteBatch batch, WriteCompletion* done) {
  if (pool_) {
    // The baseline blocks the calling thread on the global lock and the
    // flush; do that on the pool, as a thread-per-request server would.
    auto* b = new WriteBatch(std::move(batch));
    pool_->Run([this, b, done] {
      absl::Status st = engine_->Write(std::move(*b));
      delete b;
      done->Done(st, engine_->AppliedIndex());
    });
    return;
  }
  engine_->WriteAsync(std::move(batch), done);
}

void EngineBackend::ReadBarrier(api::Consistency, std::function<void(const absl::Status&)> done) {
  // A single node acknowledges a write only once it is applied, so every read
  // is already linearizable. In the baseline the read itself blocks on the
  // global lock, so run it (the continuation) on the pool.
  if (pool_) {
    pool_->Run([done = std::move(done)] { done(absl::OkStatus()); });
    return;
  }
  done(absl::OkStatus());
}

bool EngineBackend::Get(std::string_view key, std::string* value) { return engine_->Get(key, value); }

std::vector<std::pair<std::string, std::string>> EngineBackend::Scan(std::string_view start,
                                                                     std::string_view end,
                                                                     size_t limit) {
  return engine_->Scan(start, end, limit);
}

void EngineBackend::FillStatus(api::StatusResponse* resp) {
  auto s = engine_->Stats();
  resp->set_role("standalone");
  resp->set_commit_index(s.applied_index);
  resp->set_applied_index(s.applied_index);
  resp->set_last_log_index(s.applied_index);
  resp->set_snapshot_index(s.last_snapshot_index);
  resp->set_keys(s.keys);
}

absl::Status EngineBackend::TransferLeadership(uint32_t, uint32_t*) {
  return absl::FailedPreconditionError("standalone mode has no leader to transfer");
}

absl::Status EngineBackend::Snapshot(uint64_t* index) {
  auto st = engine_->Checkpoint();
  if (index) *index = engine_->Stats().last_snapshot_index;
  return st;
}

// ---------------------------------------------------------------------------
// KV service
// ---------------------------------------------------------------------------

namespace {

class WriteCall final : public WriteCompletion {
 public:
  WriteCall(grpc::ServerUnaryReactor* reactor, api::WriteResponse* resp, ServiceCounters* c)
      : reactor_(reactor), resp_(resp), counters_(c) {}
  void Done(const absl::Status& st, uint64_t index) override {
    if (st.ok()) {
      resp_->set_index(index);
      counters_->writes.fetch_add(1, std::memory_order_relaxed);
    } else {
      counters_->write_errors.fetch_add(1, std::memory_order_relaxed);
      if (absl::IsUnavailable(st)) counters_->not_leader.fetch_add(1, std::memory_order_relaxed);
    }
    reactor_->Finish(ToGrpc(st));
    delete this;
  }

 private:
  grpc::ServerUnaryReactor* reactor_;
  api::WriteResponse* resp_;
  ServiceCounters* counters_;
};

}  // namespace

grpc::ServerUnaryReactor* KvService::SubmitWrite(grpc::CallbackServerContext* ctx,
                                                 const api::Session& session, WriteBatch batch,
                                                 api::WriteResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  backend_->Write(session.client_id(), session.seq(), std::move(batch),
                  new WriteCall(reactor, resp, &counters_));
  return reactor;
}

grpc::ServerUnaryReactor* KvService::Put(grpc::CallbackServerContext* ctx,
                                         const api::PutRequest* req, api::WriteResponse* resp) {
  if (req->key().empty()) {
    auto* r = ctx->DefaultReactor();
    r->Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty key"));
    return r;
  }
  WriteBatch b;
  b.Put(req->key(), req->value());
  return SubmitWrite(ctx, req->session(), std::move(b), resp);
}

grpc::ServerUnaryReactor* KvService::Delete(grpc::CallbackServerContext* ctx,
                                            const api::DeleteRequest* req,
                                            api::WriteResponse* resp) {
  WriteBatch b;
  b.Delete(req->key());
  return SubmitWrite(ctx, req->session(), std::move(b), resp);
}

grpc::ServerUnaryReactor* KvService::Write(grpc::CallbackServerContext* ctx,
                                           const api::WriteRequest* req, api::WriteResponse* resp) {
  WriteBatch b;
  for (const auto& op : req->ops()) {
    if (op.type() == api::Op::DELETE) {
      b.Delete(op.key());
    } else {
      b.Put(op.key(), op.value());
    }
  }
  return SubmitWrite(ctx, req->session(), std::move(b), resp);
}

grpc::ServerUnaryReactor* KvService::Get(grpc::CallbackServerContext* ctx,
                                         const api::GetRequest* req, api::GetResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  backend_->ReadBarrier(req->consistency(), [this, reactor, req, resp](const absl::Status& st) {
    if (!st.ok()) {
      counters_.read_errors.fetch_add(1, std::memory_order_relaxed);
      reactor->Finish(ToGrpc(st));
      return;
    }
    std::string value;
    const bool found = backend_->Get(req->key(), &value);
    resp->set_found(found);
    if (found) resp->set_value(std::move(value));
    counters_.reads.fetch_add(1, std::memory_order_relaxed);
    reactor->Finish(grpc::Status::OK);
  });
  return reactor;
}

grpc::ServerUnaryReactor* KvService::Scan(grpc::CallbackServerContext* ctx,
                                          const api::ScanRequest* req, api::ScanResponse* resp) {
  auto* reactor = ctx->DefaultReactor();
  backend_->ReadBarrier(req->consistency(), [this, reactor, req, resp](const absl::Status& st) {
    if (!st.ok()) {
      reactor->Finish(ToGrpc(st));
      return;
    }
    const size_t limit = req->limit() == 0 ? 100 : std::min<size_t>(req->limit(), 10000);
    for (auto& [k, v] : backend_->Scan(req->start(), req->end(), limit)) {
      auto* item = resp->add_items();
      item->set_key(k);
      item->set_value(v);
    }
    reactor->Finish(grpc::Status::OK);
  });
  return reactor;
}

grpc::Status AdminService::Status(grpc::ServerContext*, const api::StatusRequest*,
                                  api::StatusResponse* resp) {
  backend_->FillStatus(resp);
  return grpc::Status::OK;
}

grpc::Status AdminService::TransferLeadership(grpc::ServerContext*,
                                              const api::TransferLeadershipRequest* req,
                                              api::TransferLeadershipResponse* resp) {
  uint32_t leader = 0;
  auto st = backend_->TransferLeadership(req->target_id(), &leader);
  resp->set_new_leader_id(leader);
  return ToGrpc(st);
}

grpc::Status AdminService::Snapshot(grpc::ServerContext*, const api::SnapshotRequest*,
                                    api::SnapshotResponse* resp) {
  uint64_t index = 0;
  auto st = backend_->Snapshot(&index);
  resp->set_index(index);
  return ToGrpc(st);
}

}  // namespace kv::server
