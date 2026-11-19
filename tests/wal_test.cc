#include "storage/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <mutex>

#include "common/fs.h"
#include "gtest/gtest.h"
#include "storage/wal_reader.h"
#include "test_util.h"

namespace kv {
namespace {

struct Collected {
  std::mutex mu;
  std::vector<uint64_t> indexes;
  absl::Status status;
};

std::unique_ptr<Wal> OpenWal(const std::string& dir, Collected* c, uint64_t seg_bytes,
                             std::vector<WalSegment> existing = {}, uint64_t last = 0) {
  WalOptions o;
  o.dir = dir;
  o.segment_bytes = seg_bytes;
  auto w = Wal::Open(o, std::move(existing), last,
                     [c](const absl::Status& st, std::vector<Wal::Record>&& g) {
                       std::lock_guard<std::mutex> l(c->mu);
                       if (!st.ok()) c->status = st;
                       for (auto& r : g) c->indexes.push_back(r.index);
                     });
  EXPECT_TRUE(w.ok()) << w.status();
  return std::move(*w);
}

std::string Payload(uint64_t i) { return "payload-" + std::to_string(i) + std::string(i % 50, 'p'); }

struct Replayed {
  std::vector<uint64_t> indexes;
  std::vector<std::string> payloads;
  std::vector<WalSegment> segments;
  WalReplayStats stats;
  absl::Status status;
};

Replayed Replay(const std::string& dir, uint64_t start_seq = 0, uint64_t after = 0) {
  Replayed r;
  r.status = ReplayWal(
      dir, start_seq, after,
      [&](SegmentContents& seg) {
        for (auto& e : seg.entries) {
          r.indexes.push_back(e.index);
          r.payloads.emplace_back(e.payload);
        }
        return absl::OkStatus();
      },
      &r.segments, &r.stats);
  return r;
}

void CorruptByte(const std::string& path, uint64_t offset) {
  int fd = ::open(path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  char c;
  ASSERT_EQ(::pread(fd, &c, 1, static_cast<off_t>(offset)), 1);
  c ^= 0x5a;
  ASSERT_EQ(::pwrite(fd, &c, 1, static_cast<off_t>(offset)), 1);
  ::close(fd);
}

TEST(WalTest, AppendAndReplayInOrder) {
  testing::TempDir dir;
  Collected c;
  {
    auto wal = OpenWal(dir.path(), &c, 1 << 20);
    for (uint64_t i = 1; i <= 1000; ++i) {
      EXPECT_EQ(wal->Append({0, 0, 0, Payload(i), nullptr}), i);
    }
    wal->Flush();
    EXPECT_TRUE(wal->Health().ok());
  }
  ASSERT_EQ(c.indexes.size(), 1000u);
  for (size_t i = 0; i < c.indexes.size(); ++i) EXPECT_EQ(c.indexes[i], i + 1);

  auto r = Replay(dir.path());
  ASSERT_TRUE(r.status.ok()) << r.status;
  ASSERT_EQ(r.indexes.size(), 1000u);
  for (uint64_t i = 1; i <= 1000; ++i) {
    EXPECT_EQ(r.indexes[i - 1], i);
    EXPECT_EQ(r.payloads[i - 1], Payload(i));
  }
  EXPECT_EQ(r.stats.torn_bytes, 0u);
}

TEST(WalTest, RollsSegmentsAndReplaysAcrossThem) {
  testing::TempDir dir;
  Collected c;
  {
    auto wal = OpenWal(dir.path(), &c, 8192);
    for (uint64_t i = 1; i <= 2000; ++i) wal->Append({0, 0, 0, Payload(i), nullptr});
    wal->Flush();
    EXPECT_GT(wal->Stats().segments_rolled, 10u);
  }
  auto r = Replay(dir.path());
  ASSERT_TRUE(r.status.ok()) << r.status;
  ASSERT_EQ(r.indexes.size(), 2000u);
  EXPECT_EQ(r.indexes.back(), 2000u);
  EXPECT_GT(r.segments.size(), 10u);
}

TEST(WalTest, TornTailIsDroppedNotFatal) {
  testing::TempDir dir;
  Collected c;
  {
    auto wal = OpenWal(dir.path(), &c, 1 << 20);
    for (uint64_t i = 1; i <= 100; ++i) wal->Append({0, 0, 0, Payload(i), nullptr});
    wal->Flush();
  }
  auto segs = ListSegments(dir.path(), 0);
  ASSERT_TRUE(segs.ok());
  // Damage the last record of the (only) written segment.
  auto before = Replay(dir.path());
  ASSERT_EQ(before.indexes.size(), 100u);
  auto seg = ReadSegment(dir.path(), before.segments.front().seq);
  ASSERT_TRUE(seg.ok());
  CorruptByte(dir.path() + "/" + wal::SegmentName(seg->seq), seg->valid_bytes - 3);

  auto r = Replay(dir.path());
  ASSERT_TRUE(r.status.ok()) << r.status;
  EXPECT_EQ(r.indexes.size(), 99u);
  EXPECT_GT(r.stats.torn_bytes, 0u);
}

TEST(WalTest, CorruptionBeforeLaterSegmentsIsDataLoss) {
  testing::TempDir dir;
  Collected c;
  {
    auto wal = OpenWal(dir.path(), &c, 4096);
    for (uint64_t i = 1; i <= 500; ++i) wal->Append({0, 0, 0, Payload(i), nullptr});
    wal->Flush();
  }
  auto before = Replay(dir.path());
  ASSERT_TRUE(before.status.ok());
  ASSERT_GT(before.segments.size(), 3u);
  // Flip a byte in the middle of the first segment.
  CorruptByte(dir.path() + "/" + wal::SegmentName(before.segments.front().seq), 100);
  auto r = Replay(dir.path());
  EXPECT_TRUE(absl::IsDataLoss(r.status)) << r.status;
}

TEST(WalTest, RecycledSegmentsDoNotResurrectOldRecords) {
  testing::TempDir dir;
  Collected c;
  uint64_t start = 0;
  {
    auto wal = OpenWal(dir.path(), &c, 4096);
    for (uint64_t i = 1; i <= 300; ++i) wal->Append({0, 0, 0, Payload(i), nullptr});
    wal->Flush();
    start = wal->FirstSegmentAbove(250);
    wal->ReleaseUpTo(250);
    for (uint64_t i = 301; i <= 900; ++i) wal->Append({0, 0, 0, Payload(i), nullptr});
    wal->Flush();
    EXPECT_GT(wal->Stats().segments_recycled, 0u);
  }
  // Recycled files now carry new segment numbers but still contain records
  // from their previous life; those must fail verification under the new seq.
  auto r = Replay(dir.path(), start, 250);
  ASSERT_TRUE(r.status.ok()) << r.status;
  ASSERT_FALSE(r.indexes.empty());
  EXPECT_EQ(r.indexes.back(), 900u);
  EXPECT_LE(r.indexes.front(), 251u);
  for (size_t i = 1; i < r.indexes.size(); ++i) EXPECT_EQ(r.indexes[i], r.indexes[i - 1] + 1);
  EXPECT_EQ(r.stats.torn_bytes, 0u);  // the end marker hides stale tails
}

TEST(WalTest, ReopenContinuesInFreshSegment) {
  testing::TempDir dir;
  Collected c;
  {
    auto wal = OpenWal(dir.path(), &c, 1 << 16);
    for (uint64_t i = 1; i <= 50; ++i) wal->Append({0, 0, 0, Payload(i), nullptr});
    wal->Flush();
  }
  auto r1 = Replay(dir.path());
  ASSERT_TRUE(r1.status.ok());
  {
    auto wal = OpenWal(dir.path(), &c, 1 << 16, r1.segments, r1.stats.last_index);
    for (uint64_t i = 51; i <= 80; ++i) EXPECT_EQ(wal->Append({0, 0, 0, Payload(i), nullptr}), i);
    wal->Flush();
  }
  auto r2 = Replay(dir.path());
  ASSERT_TRUE(r2.status.ok()) << r2.status;
  ASSERT_EQ(r2.indexes.size(), 80u);
  EXPECT_EQ(r2.indexes.back(), 80u);
}

TEST(WalTest, ExplicitIndexOverwriteSuffix) {
  testing::TempDir dir;
  Collected c;
  {
    auto wal = OpenWal(dir.path(), &c, 1 << 16);
    for (uint64_t i = 1; i <= 10; ++i) wal->Append({i, 1, 0, "t1", nullptr});
    for (uint64_t i = 6; i <= 8; ++i) wal->Append({i, 2, 0, "t2", nullptr});  // Raft truncation
    wal->Flush();
    EXPECT_EQ(wal->LastIndex(), 8u);
  }
  auto r = Replay(dir.path());
  ASSERT_TRUE(r.status.ok()) << r.status;
  ASSERT_EQ(r.indexes.size(), 13u);
  EXPECT_EQ(r.indexes[10], 6u);
  EXPECT_EQ(r.payloads[10], "t2");
}

TEST(WalTest, ConcurrentAppendersAreGroupCommitted) {
  testing::TempDir dir;
  Collected c;
  auto wal = OpenWal(dir.path(), &c, 1 << 20);
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 500; ++i) wal->Append({0, 0, 0, "x", nullptr});
    });
  }
  for (auto& t : threads) t.join();
  wal->Flush();
  auto st = wal->Stats();
  EXPECT_EQ(st.records, 4000u);
  EXPECT_LT(st.groups, 4000u);  // at least some batching happened
  std::lock_guard<std::mutex> l(c.mu);
  for (size_t i = 0; i < c.indexes.size(); ++i) ASSERT_EQ(c.indexes[i], i + 1);
}

}  // namespace
}  // namespace kv
