// kvload: closed-loop gRPC load generator.
//
// --clients independent closed-loop clients, each keeping --depth requests in
// flight over the gRPC callback API, spread across --channels connections.
// Latency is measured per request from send to response and merged into one
// histogram after a --warmup period.
//
//   kvload --endpoints=127.0.0.1:7001,... --clients=32 --depth=1 --duration=20
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "client/kv_client.h"
#include "common/histogram.h"
#include "proto/kv.grpc.pb.h"

ABSL_FLAG(std::string, endpoints, "127.0.0.1:7001", "servers; requests go to the leader");
ABSL_FLAG(int, clients, 32, "closed-loop clients");
ABSL_FLAG(int, depth, 1, "requests in flight per client");
ABSL_FLAG(int, channels, 8, "gRPC channels (TCP connections)");
ABSL_FLAG(double, duration, 10, "measured seconds");
ABSL_FLAG(double, warmup, 2, "seconds before measuring");
ABSL_FLAG(double, read_ratio, 0, "fraction of requests that are Gets");
ABSL_FLAG(std::string, consistency, "linearizable", "linearizable | lease | stale");
ABSL_FLAG(int, value_size, 100, "value bytes");
ABSL_FLAG(uint64_t, key_space, 1000000, "distinct keys");
ABSL_FLAG(bool, sessions, false, "tag writes with exactly-once sessions");
ABSL_FLAG(int, batch, 1, "keys per write RPC (>1 uses the atomic Write RPC)");
ABSL_FLAG(std::string, label, "", "copied into the RESULT line");

namespace {

using Clock = std::chrono::steady_clock;

uint64_t NowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

struct Shared {
  std::vector<std::unique_ptr<kv::api::KV::Stub>> stubs;
  // Channels per address, created on demand when a redirect names a new leader.
  std::mutex redirect_mu;
  std::map<std::string, std::vector<std::unique_ptr<kv::api::KV::Stub>>> by_addr;
  int channels = 1;
  std::atomic<uint64_t> redirects{0};
  kv::api::KV::Stub* StubFor(const std::string& addr, int slot) {
    std::lock_guard<std::mutex> l(redirect_mu);
    auto& v = by_addr[addr];
    if (v.empty()) {
      for (int i = 0; i < channels; ++i) {
        grpc::ChannelArguments args;
        args.SetInt("kvload.channel", i);
        v.push_back(kv::api::KV::NewStub(
            grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), args)));
      }
    }
    return v[static_cast<size_t>(slot) % v.size()].get();
  }
  std::atomic<bool> stop{false};
  std::atomic<bool> measuring{false};
  std::atomic<int> outstanding{0};
  std::string value_pool;
  double read_ratio = 0;
  kv::api::Consistency consistency = kv::api::LINEARIZABLE;
  uint64_t key_space = 0;
  int value_size = 0;
  bool sessions = false;
  int batch = 1;
};

// One in-flight request slot. Completion re-issues the next request, so each
// slot is a closed loop.
class Slot {
 public:
  Slot(Shared* s, int id, kv::api::KV::Stub* stub)
      : s_(s), id_(id), stub_(stub), rng_(static_cast<uint64_t>(id) * 7919 + 1) {}

  void Issue() {
    if (s_->stop.load(std::memory_order_relaxed)) {
      s_->outstanding.fetch_sub(1);
      return;
    }
    ctx_ = std::make_unique<grpc::ClientContext>();
    ctx_->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    const uint64_t k = rng_() % s_->key_space;
    char key[32];
    std::snprintf(key, sizeof(key), "key%013lu", static_cast<unsigned long>(k));
    start_ = NowNs();
    std::uniform_real_distribution<double> coin(0, 1);
    if (s_->read_ratio > 0 && coin(rng_) < s_->read_ratio) {
      is_read_ = true;
      get_.set_key(key);
      get_.set_consistency(s_->consistency);
      stub_->async()->Get(ctx_.get(), &get_, &get_resp_, [this](grpc::Status st) { Done(st); });
    } else if (s_->batch > 1) {
      is_read_ = false;
      write_.Clear();
      for (int i = 0; i < s_->batch; ++i) {
        auto* op = write_.add_ops();
        char bkey[32];
        std::snprintf(bkey, sizeof(bkey), "key%013lu", static_cast<unsigned long>(rng_() % s_->key_space));
        op->set_key(bkey);
        const size_t off = rng_() % (s_->value_pool.size() - static_cast<size_t>(s_->value_size));
        op->set_value(s_->value_pool.substr(off, static_cast<size_t>(s_->value_size)));
      }
      if (s_->sessions) {
        write_.mutable_session()->set_client_id(static_cast<uint64_t>(id_) + 1);
        write_.mutable_session()->set_seq(++seq_);
      }
      stub_->async()->Write(ctx_.get(), &write_, &put_resp_, [this](grpc::Status st) { Done(st); });
    } else {
      is_read_ = false;
      put_.set_key(key);
      const size_t off = rng_() % (s_->value_pool.size() - static_cast<size_t>(s_->value_size));
      put_.set_value(s_->value_pool.substr(off, static_cast<size_t>(s_->value_size)));
      if (s_->sessions) {
        put_.mutable_session()->set_client_id(static_cast<uint64_t>(id_) + 1);
        put_.mutable_session()->set_seq(++seq_);
      }
      stub_->async()->Put(ctx_.get(), &put_, &put_resp_, [this](grpc::Status st) { Done(st); });
    }
  }

  void Done(const grpc::Status& st) {
    const uint64_t lat = NowNs() - start_;
    if (s_->measuring.load(std::memory_order_relaxed)) {
      if (st.ok()) {
        (is_read_ ? reads : writes).Record(lat);
      } else {
        ++errors;
        last_error = st.error_message();
      }
    }
    if (!st.ok()) {
      // Leader changed mid-run: follow the redirect, as a real client would.
      const std::string hint = kv::client::LeaderFromError(st.error_message());
      if (!hint.empty()) {
        stub_ = s_->StubFor(hint, id_);
        s_->redirects.fetch_add(1, std::memory_order_relaxed);
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
    }
    Issue();
  }

  kv::Histogram writes, reads;
  uint64_t errors = 0;
  std::string last_error;

 private:
  Shared* s_;
  int id_;
  kv::api::KV::Stub* stub_;
  std::mt19937_64 rng_;
  std::unique_ptr<grpc::ClientContext> ctx_;
  kv::api::PutRequest put_;
  kv::api::WriteRequest write_;
  kv::api::WriteResponse put_resp_;
  kv::api::GetRequest get_;
  kv::api::GetResponse get_resp_;
  bool is_read_ = false;
  uint64_t start_ = 0;
  uint64_t seq_ = 0;
};

std::string FindLeader(const std::vector<std::string>& endpoints) {
  kv::client::ClientOptions o;
  o.endpoints = endpoints;
  kv::client::Client c(o);
  for (int attempt = 0; attempt < 100; ++attempt) {
    for (const auto& e : endpoints) {
      auto s = c.Status(e);
      if (s.ok() && (s->role() == "leader" || s->role() == "standalone")) return e;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return "";
}

}  // namespace

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  std::vector<std::string> endpoints = absl::StrSplit(absl::GetFlag(FLAGS_endpoints), ',', absl::SkipEmpty());
  const std::string leader = FindLeader(endpoints);
  if (leader.empty()) {
    std::fprintf(stderr, "no leader found\n");
    return 1;
  }
  Shared s;
  s.read_ratio = absl::GetFlag(FLAGS_read_ratio);
  s.key_space = absl::GetFlag(FLAGS_key_space);
  s.value_size = absl::GetFlag(FLAGS_value_size);
  s.sessions = absl::GetFlag(FLAGS_sessions);
  s.batch = std::max(1, absl::GetFlag(FLAGS_batch));
  if (absl::GetFlag(FLAGS_consistency) == "lease") s.consistency = kv::api::LEASE;
  if (absl::GetFlag(FLAGS_consistency) == "stale") s.consistency = kv::api::STALE;
  s.value_pool.resize(1 << 20);
  std::mt19937_64 rng(1);
  for (auto& c : s.value_pool) c = static_cast<char>('a' + rng() % 26);
  s.channels = absl::GetFlag(FLAGS_channels);
  for (int i = 0; i < s.channels; ++i) {
    grpc::ChannelArguments args;
    args.SetInt("kvload.channel", i);  // distinct args => distinct connection
    s.stubs.push_back(kv::api::KV::NewStub(
        grpc::CreateCustomChannel(leader, grpc::InsecureChannelCredentials(), args)));
  }

  const int clients = absl::GetFlag(FLAGS_clients), depth = absl::GetFlag(FLAGS_depth);
  std::vector<std::unique_ptr<Slot>> slots;
  for (int c = 0; c < clients; ++c) {
    for (int d = 0; d < depth; ++d) {
      const int id = c * depth + d;
      slots.push_back(std::make_unique<Slot>(&s, id, s.stubs[static_cast<size_t>(id) % s.stubs.size()].get()));
    }
  }
  s.outstanding.store(static_cast<int>(slots.size()));
  for (auto& sl : slots) sl->Issue();

  std::this_thread::sleep_for(std::chrono::duration<double>(absl::GetFlag(FLAGS_warmup)));
  s.measuring.store(true);
  const auto t0 = Clock::now();
  std::this_thread::sleep_for(std::chrono::duration<double>(absl::GetFlag(FLAGS_duration)));
  s.measuring.store(false);
  const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  s.stop.store(true);
  while (s.outstanding.load() > 0) std::this_thread::sleep_for(std::chrono::milliseconds(5));

  kv::Histogram w, r;
  uint64_t errors = 0;
  std::string last_error;
  for (auto& sl : slots) {
    w.Merge(sl->writes);
    r.Merge(sl->reads);
    errors += sl->errors;
    if (!sl->last_error.empty()) last_error = sl->last_error;
  }
  std::printf("target %s, %d clients x depth %d over %zu channels, %.1fs measured\n", leader.c_str(),
              clients, depth, s.stubs.size(), secs);
  std::printf("writes: %.0f/s  %s\n", w.Count() / secs, w.Summary().c_str());
  if (s.batch > 1) std::printf("keys written: %.0f/s (%d per RPC)\n", w.Count() * s.batch / secs, s.batch);
  if (r.Count()) std::printf("reads:  %.0f/s  %s\n", r.Count() / secs, r.Summary().c_str());
  std::printf("errors: %lu, redirects: %lu%s%s\n", static_cast<unsigned long>(errors),
              static_cast<unsigned long>(s.redirects.load()), last_error.empty() ? "" : " last: ",
              last_error.c_str());
  std::printf(
      "RESULT {\"workload\":\"kvload\",\"label\":\"%s\",\"clients\":%d,\"depth\":%d,\"read_ratio\":%.2f,"
      "\"consistency\":\"%s\",\"value_size\":%d,\"batch\":%d,\"seconds\":%.2f,\"writes_per_s\":%.0f,\"keys_per_s\":%.0f,\"reads_per_s\":%.0f,"
      "\"w_p50_us\":%.1f,\"w_p90_us\":%.1f,\"w_p99_us\":%.1f,\"w_p999_us\":%.1f,\"w_max_us\":%.1f,"
      "\"r_p50_us\":%.1f,\"r_p99_us\":%.1f,\"errors\":%lu}\n",
      absl::GetFlag(FLAGS_label).c_str(), clients, depth, s.read_ratio,
      absl::GetFlag(FLAGS_consistency).c_str(), s.value_size, s.batch, secs, w.Count() / secs,
      w.Count() * s.batch / secs, r.Count() / secs,
      w.Percentile(50) / 1e3, w.Percentile(90) / 1e3, w.Percentile(99) / 1e3, w.Percentile(99.9) / 1e3,
      w.Max() / 1e3, r.Percentile(50) / 1e3, r.Percentile(99) / 1e3, static_cast<unsigned long>(errors));
  return 0;
}
