// kvd: the storage server.
//
//   Replicated (default):
//     kvd --id=1 --members=1=10.0.0.1:7001,2=10.0.0.2:7001,3=10.0.0.3:7001 --dir=/var/lib/kvd
//   Standalone engine (no replication; --concurrency=global is the naive
//   single-lock baseline used by the contention benchmark):
//     kvd --mode=engine --listen=0.0.0.0:7001 --dir=/var/lib/kvd
//
// SIGTERM/SIGINT: a leader hands leadership to the most up-to-date follower
// first, then the server drains and exits.
#include <signal.h>

#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "common/logging.h"
#include "raft/grpc_transport.h"
#include "raft/raft_node.h"
#include "server/kv_service.h"
#include "server/metrics_http.h"
#include "storage/engine.h"

ABSL_FLAG(std::string, mode, "raft", "raft | engine");
ABSL_FLAG(uint32_t, id, 1, "this node's id (raft mode)");
ABSL_FLAG(std::string, members, "", "id=host:port,... for every member, including this node");
ABSL_FLAG(std::string, listen, "", "client listen address (default: 0.0.0.0:<own port>)");
ABSL_FLAG(int, peer_port_offset, 1000,
          "Raft peer traffic uses its own gRPC server on <port>+offset, so client load never "
          "queues heartbeats (0 = share the client port)");
ABSL_FLAG(std::string, dir, "./kvd-data", "data directory");
ABSL_FLAG(std::string, concurrency, "pipelined", "engine mode: pipelined | global");
ABSL_FLAG(uint32_t, shards, 256, "memtable shards");
ABSL_FLAG(int, apply_threads, 4, "parallel appliers");
ABSL_FLAG(int, heartbeat_ms, 20, "raft heartbeat interval");
ABSL_FLAG(int, election_min_ms, 100, "raft election timeout lower bound");
ABSL_FLAG(int, election_max_ms, 200, "raft election timeout upper bound");
ABSL_FLAG(uint64_t, snapshot_trigger_mb, 256, "log MiB between snapshots (0 = never)");
ABSL_FLAG(std::string, sync, "always", "always | none (none is for benchmarks only)");
ABSL_FLAG(int, metrics_port, 0, "Prometheus /metrics port (0 = off)");
ABSL_FLAG(std::string, log_level, "info", "debug | info | warning | error");

namespace {

absl::StatusOr<std::map<uint32_t, std::string>> ParseMembers(const std::string& spec) {
  std::map<uint32_t, std::string> out;
  for (absl::string_view part : absl::StrSplit(spec, ',', absl::SkipEmpty())) {
    std::vector<std::string> kv = absl::StrSplit(part, absl::MaxSplits('=', 1));
    uint32_t id;
    if (kv.size() != 2 || !absl::SimpleAtoi(kv[0], &id) || id == 0) {
      return absl::InvalidArgumentError(absl::StrCat("bad member '", std::string(part), "'"));
    }
    out[id] = kv[1];
  }
  return out;
}

std::string PortOf(const std::string& addr) { return addr.substr(addr.rfind(':') + 1); }

}  // namespace

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  kv::log::SetMinLevel(absl::GetFlag(FLAGS_log_level));

  // Block termination signals in every thread; main waits for them below.
  sigset_t sigs;
  sigemptyset(&sigs);
  sigaddset(&sigs, SIGINT);
  sigaddset(&sigs, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &sigs, nullptr);
  signal(SIGPIPE, SIG_IGN);

  const std::string mode = absl::GetFlag(FLAGS_mode);
  kv::WalOptions wal;
  wal.sync_mode = absl::GetFlag(FLAGS_sync) == "none" ? kv::SyncMode::kNone : kv::SyncMode::kAlways;

  std::unique_ptr<kv::raft::Transport> transport;
  std::unique_ptr<kv::raft::RaftNode> node;
  std::unique_ptr<kv::Engine> engine;
  std::unique_ptr<kv::server::Backend> backend;
  std::string listen = absl::GetFlag(FLAGS_listen);

  if (mode == "raft") {
    auto members = ParseMembers(absl::GetFlag(FLAGS_members));
    if (!members.ok() || members->empty()) {
      std::fprintf(stderr, "--members is required in raft mode\n");
      return 2;
    }
    const uint32_t id = absl::GetFlag(FLAGS_id);
    if (!members->count(id)) {
      std::fprintf(stderr, "--id=%u is not in --members\n", id);
      return 2;
    }
    if (listen.empty()) listen = "0.0.0.0:" + PortOf(members->at(id));
    kv::log::SetPrefix(absl::StrCat("n", id));
    kv::raft::RaftOptions o;
    o.id = id;
    o.members = *members;
    o.dir = absl::GetFlag(FLAGS_dir);
    o.heartbeat_ms = absl::GetFlag(FLAGS_heartbeat_ms);
    o.election_min_ms = absl::GetFlag(FLAGS_election_min_ms);
    o.election_max_ms = absl::GetFlag(FLAGS_election_max_ms);
    o.snapshot_trigger_bytes = absl::GetFlag(FLAGS_snapshot_trigger_mb) << 20;
    o.num_shards = absl::GetFlag(FLAGS_shards);
    o.apply_threads = absl::GetFlag(FLAGS_apply_threads);
    o.wal = wal;
    transport = std::make_unique<kv::raft::Transport>(id, *members, absl::GetFlag(FLAGS_peer_port_offset));
    auto n = kv::raft::RaftNode::Open(o, transport.get());
    if (!n.ok()) {
      KV_LOG(Error) << "open: " << n.status();
      return 1;
    }
    node = std::move(*n);
    backend = std::make_unique<kv::server::RaftBackend>(node.get());
  } else if (mode == "engine") {
    if (listen.empty()) listen = "0.0.0.0:7001";
    kv::EngineOptions o;
    o.dir = absl::GetFlag(FLAGS_dir);
    o.num_shards = absl::GetFlag(FLAGS_shards);
    o.apply_threads = absl::GetFlag(FLAGS_apply_threads);
    o.concurrency = absl::GetFlag(FLAGS_concurrency) == "global" ? kv::Concurrency::kGlobalLock
                                                                  : kv::Concurrency::kPipelined;
    o.snapshot_trigger_bytes = absl::GetFlag(FLAGS_snapshot_trigger_mb) << 20;
    o.wal = wal;
    auto e = kv::Engine::Open(o);
    if (!e.ok()) {
      KV_LOG(Error) << "open: " << e.status();
      return 1;
    }
    engine = std::move(*e);
    backend = std::make_unique<kv::server::EngineBackend>(engine.get());
  } else {
    std::fprintf(stderr, "unknown --mode=%s\n", mode.c_str());
    return 2;
  }

  kv::server::KvService kv_service(backend.get());
  kv::server::AdminService admin_service(backend.get());
  std::unique_ptr<kv::raft::RaftService> raft_service;
  grpc::ServerBuilder builder;
  builder.SetMaxReceiveMessageSize(256 << 20);
  builder.SetMaxSendMessageSize(256 << 20);
  int bound_port = 0;
  builder.AddListeningPort(listen, grpc::InsecureServerCredentials(), &bound_port);
  builder.RegisterService(&kv_service);
  builder.RegisterService(&admin_service);
  std::unique_ptr<grpc::Server> peer_server;
  const int peer_offset = absl::GetFlag(FLAGS_peer_port_offset);
  if (node) {
    raft_service = std::make_unique<kv::raft::RaftService>(node.get());
    if (peer_offset == 0) {
      builder.RegisterService(raft_service.get());
    } else {
      const std::string peer_listen = kv::raft::OffsetPort(listen, peer_offset);
      grpc::ServerBuilder pb;
      pb.SetMaxReceiveMessageSize(256 << 20);
      pb.SetMaxSendMessageSize(256 << 20);
      int peer_port = 0;
      pb.AddListeningPort(peer_listen, grpc::InsecureServerCredentials(), &peer_port);
      pb.RegisterService(raft_service.get());
      peer_server = pb.BuildAndStart();
      if (!peer_server || peer_port == 0) {
        KV_LOG(Error) << "cannot listen on " << peer_listen << " (peer traffic)";
        return 1;
      }
    }
  }
  auto server = builder.BuildAndStart();
  if (!server || bound_port == 0) {
    KV_LOG(Error) << "cannot listen on " << listen;
    return 1;
  }
  if (node) node->Start();
  KV_LOG(Info) << "kvd (" << mode << ") serving on " << listen;

  std::unique_ptr<kv::server::MetricsServer> metrics;
  if (int port = absl::GetFlag(FLAGS_metrics_port); port > 0) {
    auto* svc = &kv_service;
    auto* be = backend.get();
    metrics = std::make_unique<kv::server::MetricsServer>(
        port,
        [svc, be] {
          kv::api::StatusResponse s;
          be->FillStatus(&s);
          auto& c = svc->counters();
          std::string out;
          auto line = [&](const char* name, const char* type, double v) {
            out += absl::StrCat("# TYPE ", name, " ", type, "\n", name, " ", v, "\n");
          };
          line("kv_writes_total", "counter", static_cast<double>(c.writes.load()));
          line("kv_write_errors_total", "counter", static_cast<double>(c.write_errors.load()));
          line("kv_reads_total", "counter", static_cast<double>(c.reads.load()));
          line("kv_read_errors_total", "counter", static_cast<double>(c.read_errors.load()));
          line("kv_not_leader_total", "counter", static_cast<double>(c.not_leader.load()));
          line("kv_keys", "gauge", static_cast<double>(s.keys()));
          line("raft_term", "gauge", static_cast<double>(s.term()));
          line("raft_is_leader", "gauge", s.role() == "leader" ? 1 : 0);
          line("raft_commit_index", "gauge", static_cast<double>(s.commit_index()));
          line("raft_applied_index", "gauge", static_cast<double>(s.applied_index()));
          line("raft_last_log_index", "gauge", static_cast<double>(s.last_log_index()));
          line("raft_snapshot_index", "gauge", static_cast<double>(s.snapshot_index()));
          return out;
        },
        [be] {
          kv::api::StatusResponse s;
          be->FillStatus(&s);
          return s.role() == "standalone" || s.leader_id() != 0;
        });
  }

  int sig = 0;
  sigwait(&sigs, &sig);
  KV_LOG(Info) << "signal " << sig << ": shutting down";
  if (node) {
    uint32_t new_leader = 0;
    if (node->Status().role == kv::raft::Role::kLeader) {
      auto t0 = std::chrono::steady_clock::now();
      auto st = node->TransferLeadership(0, &new_leader);
      KV_LOG(Info) << "leadership transfer: " << (st.ok() ? "ok" : std::string(st.message()))
                   << " -> node " << new_leader << " in "
                   << std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count()
                   << " ms";
    }
  }
  server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
  if (peer_server) peer_server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
  metrics.reset();
  if (node) node->Shutdown();
  node.reset();
  engine.reset();
  return 0;
}
