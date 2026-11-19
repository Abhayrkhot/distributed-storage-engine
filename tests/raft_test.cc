// In-process Raft clusters talking real gRPC over loopback. Partitions are
// simulated with the transport's fault injection; crashes by tearing a node
// down without a leadership transfer and reopening it from its directory.
#include "raft/raft_node.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <set>
#include <thread>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "test_util.h"

namespace kv::raft {
namespace {

using namespace std::chrono_literals;

int FreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
  socklen_t len = sizeof(a);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
  ::close(fd);
  return ntohs(a.sin_port);
}

class Waiter final : public WriteCompletion {
 public:
  void Done(const absl::Status& st, uint64_t index) override {
    std::lock_guard<std::mutex> l(mu_);
    status_ = st;
    index_ = index;
    done_ = true;
    cv_.notify_all();
  }
  absl::Status Wait(uint64_t* index = nullptr, std::chrono::milliseconds t = 5000ms) {
    std::unique_lock<std::mutex> l(mu_);
    if (!cv_.wait_for(l, t, [&] { return done_; })) return absl::DeadlineExceededError("timeout");
    if (index) *index = index_;
    return status_;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool done_ = false;
  absl::Status status_;
  uint64_t index_ = 0;
};

class Cluster {
 public:
  explicit Cluster(int n, uint64_t snapshot_bytes = 0, uint64_t retain = 100000) {
    for (int i = 1; i <= n; ++i) {
      members_[static_cast<uint32_t>(i)] = absl::StrCat("127.0.0.1:", FreePort());
    }
    snapshot_bytes_ = snapshot_bytes;
    retain_ = retain;
    for (auto& [id, addr] : members_) StartNode(id);
  }
  ~Cluster() {
    for (auto& [id, n] : nodes_) StopNode(id);
  }

  void StartNode(uint32_t id) {
    auto& n = nodes_[id];
    RaftOptions o;
    o.id = id;
    o.members = members_;
    o.dir = dir_.path() + "/n" + std::to_string(id);
    o.heartbeat_ms = 20;
    o.election_min_ms = 100;
    o.election_max_ms = 200;
    o.num_shards = 16;
    o.apply_threads = 2;
    o.recovery_threads = 2;
    o.snapshot_trigger_bytes = snapshot_bytes_;
    o.log_retain_entries = retain_;
    o.wal.segment_bytes = 1 << 20;
    n.transport = std::make_unique<Transport>(id, members_);
    auto node = RaftNode::Open(o, n.transport.get());
    ASSERT_TRUE(node.ok()) << node.status();
    n.node = std::move(*node);
    n.service = std::make_unique<RaftService>(n.node.get());
    grpc::ServerBuilder b;
    b.AddListeningPort(members_[id], grpc::InsecureServerCredentials());
    b.RegisterService(n.service.get());
    n.server = b.BuildAndStart();
    ASSERT_NE(n.server, nullptr);
    n.node->Start();
  }

  // Crash-like stop: no leadership transfer, in-flight work abandoned.
  void StopNode(uint32_t id) {
    auto& n = nodes_[id];
    if (!n.node) return;
    n.server->Shutdown(std::chrono::system_clock::now() + 200ms);
    n.node->Shutdown();
    n.node.reset();
    n.server.reset();
    n.service.reset();
    n.transport.reset();
  }

  RaftNode* node(uint32_t id) { return nodes_[id].node.get(); }
  bool running(uint32_t id) { return nodes_[id].node != nullptr; }

  // Cuts `id` off from everyone else (both directions).
  void Isolate(uint32_t id) {
    std::set<uint32_t> others;
    for (auto& [m, a] : members_) {
      if (m != id) others.insert(m);
    }
    if (running(id)) node(id)->transport()->SetBlocked(others);
    for (uint32_t m : others) {
      if (running(m)) node(m)->transport()->SetBlocked({id});
    }
  }
  void Heal() {
    for (auto& [m, a] : members_) {
      if (running(m)) node(m)->transport()->SetBlocked({});
    }
  }

  uint32_t WaitLeader(std::chrono::milliseconds timeout = 5000ms, uint32_t exclude = 0) {
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < end) {
      for (auto& [id, n] : nodes_) {
        if (id == exclude || !n.node) continue;
        auto s = n.node->Status();
        if (s.role == Role::kLeader && s.commit_index >= s.last_index) return id;
      }
      std::this_thread::sleep_for(10ms);
    }
    return 0;
  }

  absl::Status Put(uint32_t leader, const std::string& k, const std::string& v,
                   uint64_t client = 0, uint64_t seq = 0, uint64_t* index = nullptr) {
    WriteBatch b;
    b.Put(k, v);
    Waiter w;
    node(leader)->Propose(client, seq, b.ReleaseRep(), &w);
    return w.Wait(index);
  }

  bool WaitApplied(uint32_t id, uint64_t index, std::chrono::milliseconds t = 5000ms) {
    const auto end = std::chrono::steady_clock::now() + t;
    while (std::chrono::steady_clock::now() < end) {
      if (node(id)->state().AppliedIndex() >= index) return true;
      std::this_thread::sleep_for(5ms);
    }
    return false;
  }

  std::string Value(uint32_t id, const std::string& k) {
    std::string v;
    return node(id)->state().Get(k, &v) ? v : "<missing>";
  }

  const std::map<uint32_t, std::string>& members() const { return members_; }

 private:
  struct N {
    std::unique_ptr<Transport> transport;
    std::unique_ptr<RaftNode> node;
    std::unique_ptr<RaftService> service;
    std::unique_ptr<grpc::Server> server;
  };
  testing::TempDir dir_;
  std::map<uint32_t, std::string> members_;
  std::map<uint32_t, N> nodes_;
  uint64_t snapshot_bytes_ = 0;
  uint64_t retain_ = 100000;
};

TEST(RaftTest, SingleNodeCommitsAlone) {
  Cluster c(1);
  ASSERT_EQ(c.WaitLeader(), 1u);
  ASSERT_TRUE(c.Put(1, "k", "v").ok());
  EXPECT_EQ(c.Value(1, "k"), "v");
}

TEST(RaftTest, ElectsOneLeaderAndReplicates) {
  Cluster c(3);
  const uint32_t leader = c.WaitLeader();
  ASSERT_NE(leader, 0u);
  int leaders = 0;
  for (uint32_t id = 1; id <= 3; ++id) leaders += c.node(id)->Status().role == Role::kLeader;
  EXPECT_EQ(leaders, 1);
  uint64_t last = 0;
  for (int i = 0; i < 300; ++i) {
    ASSERT_TRUE(c.Put(leader, "k" + std::to_string(i), std::to_string(i), 0, 0, &last).ok());
  }
  for (uint32_t id = 1; id <= 3; ++id) {
    ASSERT_TRUE(c.WaitApplied(id, last)) << "node " << id;
    EXPECT_EQ(c.Value(id, "k299"), "299");
  }
}

TEST(RaftTest, FollowerRejectsWritesAndLinearizableReads) {
  Cluster c(3);
  const uint32_t leader = c.WaitLeader();
  ASSERT_NE(leader, 0u);
  const uint32_t follower = leader % 3 + 1;
  absl::Status st = c.Put(follower, "k", "v");
  EXPECT_TRUE(absl::IsUnavailable(st)) << st;
  EXPECT_NE(std::string(st.message()).find("leader="), std::string::npos);
  std::promise<absl::Status> p;
  c.node(follower)->ReadBarrier(ReadMode::kLinearizable, [&](const absl::Status& s) { p.set_value(s); });
  EXPECT_TRUE(absl::IsUnavailable(p.get_future().get()));
}

TEST(RaftTest, LinearizableReadSeesPriorWrite) {
  Cluster c(3);
  const uint32_t leader = c.WaitLeader();
  ASSERT_TRUE(c.Put(leader, "x", "1").ok());
  for (ReadMode m : {ReadMode::kLinearizable, ReadMode::kLease}) {
    std::promise<absl::Status> p;
    c.node(leader)->ReadBarrier(m, [&](const absl::Status& s) { p.set_value(s); });
    ASSERT_TRUE(p.get_future().get().ok());
    EXPECT_EQ(c.Value(leader, "x"), "1");
  }
}

TEST(RaftTest, LeaderCrashKeepsAcknowledgedWrites) {
  Cluster c(3);
  uint32_t leader = c.WaitLeader();
  ASSERT_NE(leader, 0u);
  for (int i = 0; i < 200; ++i) ASSERT_TRUE(c.Put(leader, "k" + std::to_string(i), "a").ok());
  c.StopNode(leader);
  const uint32_t next = c.WaitLeader(5000ms, leader);
  ASSERT_NE(next, 0u);
  // Every acknowledged write survives on the new leader.
  std::promise<absl::Status> p;
  c.node(next)->ReadBarrier(ReadMode::kLinearizable, [&](const absl::Status& s) { p.set_value(s); });
  ASSERT_TRUE(p.get_future().get().ok());
  for (int i = 0; i < 200; ++i) ASSERT_EQ(c.Value(next, "k" + std::to_string(i)), "a") << i;
  ASSERT_TRUE(c.Put(next, "after", "failover").ok());
  // The old leader restarts from disk and catches up as a follower.
  c.StartNode(leader);
  uint64_t idx = 0;
  ASSERT_TRUE(c.Put(next, "final", "x", 0, 0, &idx).ok());
  ASSERT_TRUE(c.WaitApplied(leader, idx));
  EXPECT_EQ(c.Value(leader, "after"), "failover");
  EXPECT_EQ(c.node(leader)->Status().role, Role::kFollower);
}

TEST(RaftTest, IsolatedLeaderStepsDownAndItsUncommittedWritesVanish) {
  Cluster c(3);
  const uint32_t old_leader = c.WaitLeader();
  ASSERT_TRUE(c.Put(old_leader, "base", "1").ok());
  c.Isolate(old_leader);
  // A write to the isolated leader cannot reach a quorum.
  WriteBatch b;
  b.Put("lost", "x");
  Waiter w;
  c.node(old_leader)->Propose(0, 0, b.ReleaseRep(), &w);
  const uint32_t new_leader = c.WaitLeader(5000ms, old_leader);
  ASSERT_NE(new_leader, 0u);
  // CheckQuorum makes the old leader give up and fail the pending write.
  EXPECT_TRUE(absl::IsUnavailable(w.Wait(nullptr, 3000ms)));
  ASSERT_TRUE(c.Put(new_leader, "majority", "y").ok());
  c.Heal();
  uint64_t idx = 0;
  ASSERT_TRUE(c.Put(new_leader, "healed", "z", 0, 0, &idx).ok());
  ASSERT_TRUE(c.WaitApplied(old_leader, idx));
  EXPECT_EQ(c.Value(old_leader, "lost"), "<missing>");
  EXPECT_EQ(c.Value(old_leader, "majority"), "y");
}

TEST(RaftTest, RetriedSessionWriteAppliesOnce) {
  Cluster c(3);
  const uint32_t leader = c.WaitLeader();
  ASSERT_TRUE(c.Put(leader, "k", "first", /*client=*/42, /*seq=*/7).ok());
  // Same (client, seq), different payload: a retry must not re-apply.
  ASSERT_TRUE(c.Put(leader, "k", "second", 42, 7).ok());
  EXPECT_EQ(c.Value(leader, "k"), "first");
  EXPECT_EQ(c.node(leader)->Status().duplicates, 1u);
  ASSERT_TRUE(c.Put(leader, "k", "third", 42, 8).ok());
  EXPECT_EQ(c.Value(leader, "k"), "third");
}

TEST(RaftTest, LaggingFollowerCatchesUpViaSnapshot) {
  // Tiny snapshot trigger and log retention force InstallSnapshot.
  Cluster c(3, /*snapshot_bytes=*/16 << 10, /*retain=*/10);
  const uint32_t leader = c.WaitLeader();
  const uint32_t lagger = leader % 3 + 1;
  c.StopNode(lagger);
  uint64_t idx = 0;
  const std::string v(200, 'v');
  for (int i = 0; i < 2000; ++i) {
    ASSERT_TRUE(c.Put(leader, "k" + std::to_string(i % 500), v + std::to_string(i), 0, 0, &idx).ok());
  }
  ASSERT_GT(c.node(leader)->Status().snapshot_index, 0u);
  c.StartNode(lagger);
  ASSERT_TRUE(c.WaitApplied(lagger, idx, 10000ms));
  for (int i = 1500; i < 2000; ++i) {
    ASSERT_EQ(c.Value(lagger, "k" + std::to_string(i % 500)), v + std::to_string(i));
  }
  EXPECT_GT(c.node(lagger)->Status().snapshot_index, 0u);
  // And it survives its own restart from the installed snapshot.
  c.StopNode(lagger);
  c.StartNode(lagger);
  ASSERT_TRUE(c.Put(leader, "post", "restart", 0, 0, &idx).ok());
  ASSERT_TRUE(c.WaitApplied(lagger, idx, 10000ms));
  EXPECT_EQ(c.Value(lagger, "k0"), v + "1500");
}

TEST(RaftTest, FullClusterRestartRecoversFromDisk) {
  auto c = std::make_unique<Cluster>(3, 32 << 10, 50);
  uint32_t leader = c->WaitLeader();
  for (int i = 0; i < 1000; ++i) ASSERT_TRUE(c->Put(leader, "k" + std::to_string(i), "v").ok());
  for (uint32_t id = 1; id <= 3; ++id) c->StopNode(id);
  for (uint32_t id = 1; id <= 3; ++id) c->StartNode(id);
  leader = c->WaitLeader();
  ASSERT_NE(leader, 0u);
  std::promise<absl::Status> p;
  c->node(leader)->ReadBarrier(ReadMode::kLinearizable, [&](const absl::Status& s) { p.set_value(s); });
  ASSERT_TRUE(p.get_future().get().ok());
  for (int i = 0; i < 1000; ++i) ASSERT_EQ(c->Value(leader, "k" + std::to_string(i)), "v");
}

TEST(RaftTest, LeadershipTransferIsFast) {
  Cluster c(3);
  const uint32_t leader = c.WaitLeader();
  ASSERT_TRUE(c.Put(leader, "k", "v").ok());
  const uint32_t target = leader % 3 + 1;
  uint32_t got = 0;
  const auto t0 = std::chrono::steady_clock::now();
  ASSERT_TRUE(c.node(leader)->TransferLeadership(target, &got).ok());
  const auto ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
  EXPECT_EQ(got, target);
  EXPECT_LT(ms, 100.0);
  ASSERT_EQ(c.WaitLeader(), target);
  ASSERT_TRUE(c.Put(target, "k2", "v2").ok());
}

}  // namespace
}  // namespace kv::raft
