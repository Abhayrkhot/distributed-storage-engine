#include "client/kv_client.h"

#include <chrono>
#include <random>
#include <thread>

#include "absl/strings/str_cat.h"

namespace kv::client {
namespace {

using Clock = std::chrono::system_clock;

absl::Status FromGrpc(const grpc::Status& s) {
  if (s.ok()) return absl::OkStatus();
  switch (s.error_code()) {
    case grpc::StatusCode::UNAVAILABLE:
      return absl::UnavailableError(s.error_message());
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      return absl::DeadlineExceededError(s.error_message());
    case grpc::StatusCode::INVALID_ARGUMENT:
      return absl::InvalidArgumentError(s.error_message());
    case grpc::StatusCode::FAILED_PRECONDITION:
      return absl::FailedPreconditionError(s.error_message());
    default:
      return absl::InternalError(absl::StrCat(static_cast<int>(s.error_code()), ": ", s.error_message()));
  }
}

bool Retryable(grpc::StatusCode c) {
  return c == grpc::StatusCode::UNAVAILABLE || c == grpc::StatusCode::DEADLINE_EXCEEDED ||
         c == grpc::StatusCode::CANCELLED || c == grpc::StatusCode::UNKNOWN;
}

}  // namespace

std::shared_ptr<grpc::Channel> MakeChannel(const std::string& addr) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(256 << 20);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 50);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 500);
  return grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), args);
}

std::string LeaderFromError(const std::string& message) {
  const auto pos = message.find("leader=");
  if (pos == std::string::npos) return "";
  std::string hint = message.substr(pos + 7);
  const auto at = hint.find('@');
  if (at == std::string::npos) return "";
  return hint.substr(at + 1);
}

Client::Client(ClientOptions options) : opt_(std::move(options)) {
  client_id_ = opt_.client_id;
  if (client_id_ == 0) {
    std::random_device rd;
    client_id_ = (static_cast<uint64_t>(rd()) << 32 | rd()) | 1;
  }
  for (const auto& e : opt_.endpoints) EndpointFor(e);
}

Client::Endpoint* Client::EndpointFor(const std::string& addr) {
  for (auto& e : endpoints_) {
    if (e->addr == addr) return e.get();
  }
  auto e = std::make_unique<Endpoint>();
  e->addr = addr;
  auto ch = MakeChannel(addr);
  e->kv = api::KV::NewStub(ch);
  e->admin = api::Admin::NewStub(ch);
  endpoints_.push_back(std::move(e));
  return endpoints_.back().get();
}

std::string Client::leader() const {
  std::lock_guard<std::mutex> l(mu_);
  return endpoints_.empty() ? "" : endpoints_[leader_]->addr;
}

template <typename Fn>
absl::Status Client::Call(Fn fn) {
  const auto deadline = Clock::now() + std::chrono::milliseconds(opt_.timeout_ms);
  auto backoff = std::chrono::milliseconds(1);
  size_t failures = 0;
  grpc::Status last;
  for (;;) {
    Endpoint* ep;
    size_t n;
    {
      std::lock_guard<std::mutex> l(mu_);
      if (endpoints_.empty()) return absl::FailedPreconditionError("no endpoints");
      ep = endpoints_[leader_].get();
      n = endpoints_.size();
    }
    grpc::ClientContext ctx;
    ctx.set_deadline(std::min(deadline, Clock::now() + std::chrono::milliseconds(opt_.attempt_ms)));
    last = fn(ep, &ctx);
    if (last.ok()) return absl::OkStatus();
    if (!Retryable(last.error_code()) || Clock::now() >= deadline) return FromGrpc(last);
    retries_.fetch_add(1, std::memory_order_relaxed);
    const std::string hint = LeaderFromError(last.error_message());
    {
      std::lock_guard<std::mutex> l(mu_);
      // Follow a redirect at most once per round; during a failover followers
      // may still name the dead leader, so don't bounce on it forever.
      if (!hint.empty() && hint != ep->addr && failures % n == 0) {
        EndpointFor(hint);
        for (size_t i = 0; i < endpoints_.size(); ++i) {
          if (endpoints_[i]->addr == hint) leader_ = i;
        }
        redirects_.fetch_add(1, std::memory_order_relaxed);
        ++failures;
        continue;
      }
      leader_ = (leader_ + 1) % endpoints_.size();
    }
    // Sleep only after every endpoint has failed once in this round: the
    // election usually finishes within a few milliseconds of the timeout.
    if (++failures % n == 0) {
      std::this_thread::sleep_for(backoff);
      backoff = std::min(backoff * 2, std::chrono::milliseconds(16));
    }
  }
}

absl::Status Client::Put(std::string_view key, std::string_view value, uint64_t* index) {
  api::PutRequest req;
  if (opt_.sessions) {
    req.mutable_session()->set_client_id(client_id_);
    req.mutable_session()->set_seq(++seq_);  // same seq for every retry below
  }
  req.set_key(std::string(key));
  req.set_value(std::string(value));
  api::WriteResponse resp;
  auto st = Call([&](Endpoint* ep, grpc::ClientContext* ctx) { return ep->kv->Put(ctx, req, &resp); });
  if (st.ok() && index) *index = resp.index();
  return st;
}

absl::Status Client::Delete(std::string_view key, uint64_t* index) {
  api::DeleteRequest req;
  if (opt_.sessions) {
    req.mutable_session()->set_client_id(client_id_);
    req.mutable_session()->set_seq(++seq_);
  }
  req.set_key(std::string(key));
  api::WriteResponse resp;
  auto st =
      Call([&](Endpoint* ep, grpc::ClientContext* ctx) { return ep->kv->Delete(ctx, req, &resp); });
  if (st.ok() && index) *index = resp.index();
  return st;
}

absl::Status Client::Write(const std::vector<api::Op>& ops, uint64_t* index) {
  api::WriteRequest req;
  if (opt_.sessions) {
    req.mutable_session()->set_client_id(client_id_);
    req.mutable_session()->set_seq(++seq_);
  }
  for (const auto& op : ops) *req.add_ops() = op;
  api::WriteResponse resp;
  auto st =
      Call([&](Endpoint* ep, grpc::ClientContext* ctx) { return ep->kv->Write(ctx, req, &resp); });
  if (st.ok() && index) *index = resp.index();
  return st;
}

absl::StatusOr<std::optional<std::string>> Client::Get(std::string_view key, api::Consistency c) {
  api::GetRequest req;
  req.set_key(std::string(key));
  req.set_consistency(c);
  api::GetResponse resp;
  auto st = Call([&](Endpoint* ep, grpc::ClientContext* ctx) { return ep->kv->Get(ctx, req, &resp); });
  if (!st.ok()) return st;
  if (!resp.found()) return std::optional<std::string>();
  return std::optional<std::string>(resp.value());
}

absl::StatusOr<std::vector<std::pair<std::string, std::string>>> Client::Scan(
    std::string_view start, std::string_view end, uint32_t limit, api::Consistency c) {
  api::ScanRequest req;
  req.set_start(std::string(start));
  req.set_end(std::string(end));
  req.set_limit(limit);
  req.set_consistency(c);
  api::ScanResponse resp;
  auto st = Call([&](Endpoint* ep, grpc::ClientContext* ctx) { return ep->kv->Scan(ctx, req, &resp); });
  if (!st.ok()) return st;
  std::vector<std::pair<std::string, std::string>> out;
  for (const auto& kv : resp.items()) out.emplace_back(kv.key(), kv.value());
  return out;
}

absl::StatusOr<api::StatusResponse> Client::Status(const std::string& endpoint) {
  Endpoint* ep;
  {
    std::lock_guard<std::mutex> l(mu_);
    ep = EndpointFor(endpoint);
  }
  grpc::ClientContext ctx;
  ctx.set_deadline(Clock::now() + std::chrono::milliseconds(opt_.attempt_ms));
  api::StatusResponse resp;
  auto s = ep->admin->Status(&ctx, api::StatusRequest(), &resp);
  if (!s.ok()) return FromGrpc(s);
  return resp;
}

absl::StatusOr<uint32_t> Client::TransferLeadership(uint32_t target) {
  api::TransferLeadershipRequest req;
  req.set_target_id(target);
  api::TransferLeadershipResponse resp;
  // Only the leader can transfer; FAILED_PRECONDITION from a follower is not
  // retried by Call(), so find the leader from Status first.
  for (const auto& e : opt_.endpoints) {
    auto s = Status(e);
    if (s.ok() && s->role() == "leader") {
      std::lock_guard<std::mutex> l(mu_);
      for (size_t i = 0; i < endpoints_.size(); ++i) {
        if (endpoints_[i]->addr == e) leader_ = i;
      }
      break;
    }
  }
  auto st = Call([&](Endpoint* ep, grpc::ClientContext* ctx) {
    return ep->admin->TransferLeadership(ctx, req, &resp);
  });
  if (!st.ok()) return st;
  return resp.new_leader_id();
}

absl::StatusOr<uint64_t> Client::Snapshot() {
  api::SnapshotResponse resp;
  auto st = Call([&](Endpoint* ep, grpc::ClientContext* ctx) {
    ctx->set_deadline(Clock::now() + std::chrono::seconds(120));
    return ep->admin->Snapshot(ctx, api::SnapshotRequest(), &resp);
  });
  if (!st.ok()) return st;
  return resp.index();
}

}  // namespace kv::client
