#include "storage/engine.h"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <map>
#include <thread>

#include "common/fs.h"
#include "gtest/gtest.h"
#include "storage/snapshot.h"
#include "storage/wal_reader.h"
#include "test_util.h"

namespace kv {
namespace {

EngineOptions Opts(const std::string& dir, Concurrency c = Concurrency::kPipelined) {
  EngineOptions o;
  o.dir = dir;
  o.num_shards = 32;
  o.concurrency = c;
  o.wal.segment_bytes = 1 << 20;
  o.snapshot_trigger_bytes = 0;
  o.recovery_threads = 4;
  return o;
}

std::unique_ptr<Engine> MustOpen(const EngineOptions& o) {
  auto e = Engine::Open(o);
  EXPECT_TRUE(e.ok()) << e.status();
  return std::move(*e);
}

class EngineModeTest : public ::testing::TestWithParam<Concurrency> {};

TEST_P(EngineModeTest, BasicOps) {
  testing::TempDir dir;
  auto e = MustOpen(Opts(dir.path(), GetParam()));
  ASSERT_TRUE(e->Put("a", "1").ok());
  ASSERT_TRUE(e->Put("b", "2").ok());
  WriteBatch b;
  b.Put("c", "3");
  b.Delete("a");
  ASSERT_TRUE(e->Write(std::move(b)).ok());
  std::string v;
  EXPECT_FALSE(e->Get("a", &v));
  ASSERT_TRUE(e->Get("c", &v));
  EXPECT_EQ(v, "3");
  EXPECT_EQ(e->AppliedIndex(), 3u);
  EXPECT_EQ(e->Scan("", "", 10).size(), 2u);
}

TEST_P(EngineModeTest, ConcurrentWritersAllDurable) {
  testing::TempDir dir;
  {
    auto e = MustOpen(Opts(dir.path(), GetParam()));
    std::vector<std::thread> ts;
    for (int t = 0; t < 8; ++t) {
      ts.emplace_back([&, t] {
        for (int i = 0; i < 200; ++i) {
          ASSERT_TRUE(e->Put("t" + std::to_string(t) + "-" + std::to_string(i), std::to_string(i)).ok());
        }
      });
    }
    for (auto& t : ts) t.join();
    EXPECT_EQ(e->AppliedIndex(), 1600u);
  }
  auto e = MustOpen(Opts(dir.path(), GetParam()));
  EXPECT_EQ(e->recovery().keys, 1600u);
  std::string v;
  ASSERT_TRUE(e->Get("t7-199", &v));
  EXPECT_EQ(v, "199");
}

INSTANTIATE_TEST_SUITE_P(Modes, EngineModeTest,
                         ::testing::Values(Concurrency::kPipelined, Concurrency::kGlobalLock));

TEST(EngineTest, SurvivesSigkill) {
  testing::TempDir dir;
  // Child acknowledges writes through a pipe, then is killed mid-stream.
  int fds[2];
  ASSERT_EQ(::pipe(fds), 0);
  pid_t pid = ::fork();
  if (pid == 0) {
    ::close(fds[0]);
    auto e = Engine::Open(Opts(dir.path()));
    if (!e.ok()) _exit(2);
    for (uint32_t i = 0;; ++i) {
      if (!(*e)->Put("k" + std::to_string(i), "v" + std::to_string(i)).ok()) _exit(3);
      if (::write(fds[1], &i, sizeof(i)) != sizeof(i)) _exit(4);
    }
  }
  ::close(fds[1]);
  uint32_t last_acked = 0, x;
  size_t n = 0;
  while (n < 3000 && ::read(fds[0], &x, sizeof(x)) == sizeof(x)) {
    last_acked = x;
    ++n;
  }
  ::kill(pid, SIGKILL);
  ::waitpid(pid, nullptr, 0);
  while (::read(fds[0], &x, sizeof(x)) == sizeof(x)) last_acked = x;  // drain acks already sent
  ::close(fds[0]);

  auto e = MustOpen(Opts(dir.path()));
  std::string v;
  for (uint32_t i = 0; i <= last_acked; ++i) {
    ASSERT_TRUE(e->Get("k" + std::to_string(i), &v)) << "acknowledged write " << i << " lost";
    ASSERT_EQ(v, "v" + std::to_string(i));
  }
  // And it keeps accepting writes after recovery.
  ASSERT_TRUE(e->Put("after", "crash").ok());
}

TEST(EngineTest, SnapshotPlusWalTailRecovery) {
  testing::TempDir dir;
  uint64_t snap_index;
  {
    auto e = MustOpen(Opts(dir.path()));
    for (int i = 0; i < 5000; ++i) ASSERT_TRUE(e->Put("k" + std::to_string(i), "a").ok());
    ASSERT_TRUE(e->Checkpoint().ok());
    snap_index = e->Stats().last_snapshot_index;
    EXPECT_EQ(snap_index, 5000u);
    for (int i = 0; i < 5000; i += 2) ASSERT_TRUE(e->Put("k" + std::to_string(i), "b").ok());
    for (int i = 1; i < 5000; i += 10) ASSERT_TRUE(e->Delete("k" + std::to_string(i)).ok());
  }
  auto e = MustOpen(Opts(dir.path()));
  const auto& r = e->recovery();
  EXPECT_EQ(r.snapshot_index, snap_index);
  EXPECT_EQ(r.snapshot_keys, 5000u);
  EXPECT_EQ(e->AppliedIndex(), 5000u + 2500u + 500u);
  std::string v;
  ASSERT_TRUE(e->Get("k0", &v));
  EXPECT_EQ(v, "b");
  ASSERT_TRUE(e->Get("k3", &v));
  EXPECT_EQ(v, "a");
  EXPECT_FALSE(e->Get("k1", &v));
  EXPECT_EQ(e->recovery().keys, 4500u);
}

TEST(EngineTest, SnapshotReleasesOldWalSegments) {
  testing::TempDir dir;
  auto o = Opts(dir.path());
  o.wal.segment_bytes = 64 << 10;
  auto e = MustOpen(o);
  const std::string big(1000, 'x');
  for (int i = 0; i < 2000; ++i) ASSERT_TRUE(e->Put("k" + std::to_string(i % 100), big).ok());
  auto before = ListSegments(dir.path() + "/wal", 0);
  ASSERT_TRUE(before.ok());
  ASSERT_TRUE(e->Checkpoint().ok());
  // Segments are recycled into the pool or deleted, so the WAL directory no
  // longer grows with the history.
  for (int i = 0; i < 2000; ++i) ASSERT_TRUE(e->Put("k" + std::to_string(i % 100), big).ok());
  ASSERT_TRUE(e->Checkpoint().ok());
  auto after = ListSegments(dir.path() + "/wal", 0);
  ASSERT_TRUE(after.ok());
  EXPECT_LE(after->size(), before->size() + 2);
  EXPECT_GT(e->Stats().wal.segments_recycled, 0u);
}

// A single writer sets key (j % 50) = j for j = 1..N while a checkpoint runs.
// Whatever index S the snapshot captured, its contents must equal exactly the
// state after applying writes 1..S: no later write leaks in and none is lost.
TEST(EngineTest, SnapshotIsConsistentUnderConcurrentWrites) {
  testing::TempDir dir;
  auto o = Opts(dir.path());
  o.num_shards = 8;
  auto e = MustOpen(o);
  std::atomic<bool> stop{false};
  std::thread writer([&] {
    for (uint64_t j = 1; !stop.load(); ++j) {
      ASSERT_TRUE(e->Put("k" + std::to_string(j % 50), std::to_string(j)).ok());
    }
  });
  while (e->AppliedIndex() < 500) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_TRUE(e->Checkpoint().ok());
  stop.store(true);
  writer.join();

  auto latest = FindLatestSnapshot(dir.path() + "/snapshots");
  ASSERT_TRUE(latest.ok() && latest->has_value());
  const uint64_t s = (*latest)->index();
  MemTable loaded(8);
  ASSERT_TRUE(LoadSnapshot(dir.path() + "/snapshots", **latest, loaded, 2, nullptr).ok());
  for (uint64_t k = 0; k < 50; ++k) {
    // Largest j <= s with j % 50 == k.
    uint64_t j = s - ((s % 50 + 50 - k) % 50);
    std::string v;
    if (j == 0) {
      EXPECT_FALSE(loaded.Get("k" + std::to_string(k), &v));
    } else {
      ASSERT_TRUE(loaded.Get("k" + std::to_string(k), &v)) << k;
      EXPECT_EQ(v, std::to_string(j)) << "key k" << k << " at snapshot " << s;
    }
  }
}

// Completions run on parallel appliers, so they are not globally ordered. The
// contract is: each write completes exactly once, and only after every key it
// touched is visible (including batches split across appliers).
TEST(EngineTest, AsyncCompletionImpliesVisibility) {
  testing::TempDir dir;
  auto o = Opts(dir.path());
  o.apply_threads = 4;
  auto e = MustOpen(o);
  struct Rec : WriteCompletion {
    Engine* engine = nullptr;
    std::mutex mu;
    std::vector<int> seen = std::vector<int>(2001, 0);
    std::atomic<int> n{0};
    std::atomic<int> invisible{0};
    void Done(const absl::Status& st, uint64_t index) override {
      EXPECT_TRUE(st.ok());
      std::string v;
      // index i <= 1000 wrote key k(i-1); index > 1000 wrote a 16-key batch.
      if (index <= 1000) {
        if (!engine->Get("k" + std::to_string(index - 1), &v)) invisible.fetch_add(1);
      } else {
        for (int j = 0; j < 16; ++j) {
          if (!engine->Get("b" + std::to_string(index) + "-" + std::to_string(j), &v)) {
            invisible.fetch_add(1);
          }
        }
      }
      std::lock_guard<std::mutex> l(mu);
      ++seen[index];
      n.fetch_add(1);
    }
  } rec;
  rec.engine = e.get();
  for (int i = 0; i < 1000; ++i) {
    WriteBatch b;
    b.Put("k" + std::to_string(i), "v");
    e->WriteAsync(std::move(b), &rec);
  }
  for (int i = 1001; i <= 2000; ++i) {
    WriteBatch b;
    for (int j = 0; j < 16; ++j) b.Put("b" + std::to_string(i) + "-" + std::to_string(j), "v");
    e->WriteAsync(std::move(b), &rec);
  }
  while (rec.n.load() < 2000) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_EQ(rec.invisible.load(), 0);
  for (int i = 1; i <= 2000; ++i) EXPECT_EQ(rec.seen[static_cast<size_t>(i)], 1) << i;
  EXPECT_EQ(e->AppliedIndex(), 2000u);
}

}  // namespace
}  // namespace kv
