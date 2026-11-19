#include "storage/snapshot.h"

#include <fcntl.h>
#include <unistd.h>

#include "common/fs.h"
#include "gtest/gtest.h"
#include "test_util.h"

namespace kv {
namespace {

void Fill(MemTable& m, int n, const std::string& tag) {
  for (int i = 0; i < n; ++i) m.Put("key" + std::to_string(i), tag + std::to_string(i));
}

TEST(SnapshotTest, WriteFindLoadRoundTrip) {
  testing::TempDir dir;
  MemTable m(64);
  Fill(m, 20000, "v");
  m.BeginSnapshot();
  SnapshotWriteOptions o;
  o.index = 42;
  o.term = 3;
  o.wal_start_segment = 7;
  o.app_state = "sessions";
  o.files = 8;
  auto manifest = WriteSnapshot(dir.path(), m, o);
  ASSERT_TRUE(manifest.ok()) << manifest.status();
  EXPECT_EQ(manifest->key_count(), 20000u);

  auto latest = FindLatestSnapshot(dir.path());
  ASSERT_TRUE(latest.ok());
  ASSERT_TRUE(latest->has_value());
  EXPECT_EQ((*latest)->index(), 42u);
  EXPECT_EQ((*latest)->term(), 3u);
  EXPECT_EQ((*latest)->wal_start_segment(), 7u);
  EXPECT_EQ((*latest)->app_state(), "sessions");

  MemTable loaded(64);
  SnapshotLoadStats ls;
  ASSERT_TRUE(LoadSnapshot(dir.path(), **latest, loaded, 4, &ls).ok());
  EXPECT_EQ(ls.keys, 20000u);
  std::string v;
  ASSERT_TRUE(loaded.Get("key123", &v));
  EXPECT_EQ(v, "v123");
  EXPECT_EQ(loaded.KeyCount(), 20000u);
}

TEST(SnapshotTest, LoadsIntoDifferentShardCount) {
  testing::TempDir dir;
  MemTable m(16);
  Fill(m, 1000, "v");
  m.BeginSnapshot();
  SnapshotWriteOptions o;
  o.index = 1;
  ASSERT_TRUE(WriteSnapshot(dir.path(), m, o).ok());
  auto latest = FindLatestSnapshot(dir.path());
  MemTable loaded(256);
  ASSERT_TRUE(LoadSnapshot(dir.path(), **latest, loaded, 2, nullptr).ok());
  EXPECT_EQ(loaded.KeyCount(), 1000u);
  std::string v;
  ASSERT_TRUE(loaded.Get("key999", &v));
}

TEST(SnapshotTest, CorruptDataFileFailsLoad) {
  testing::TempDir dir;
  MemTable m(8);
  Fill(m, 5000, "v");
  m.BeginSnapshot();
  SnapshotWriteOptions o;
  o.index = 5;
  o.files = 2;
  auto manifest = WriteSnapshot(dir.path(), m, o);
  ASSERT_TRUE(manifest.ok());
  const std::string f = dir.path() + "/" + SnapshotDirName(5) + "/" + manifest->files(0).name();
  int fd = ::open(f.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::pwrite(fd, "X", 1, 100), 1);
  ::close(fd);
  MemTable loaded(8);
  auto st = LoadSnapshot(dir.path(), *manifest, loaded, 2, nullptr);
  EXPECT_TRUE(absl::IsDataLoss(st)) << st;
}

TEST(SnapshotTest, FallsBackPastDamagedManifest) {
  testing::TempDir dir;
  MemTable m(8);
  Fill(m, 100, "a");
  for (uint64_t idx : {10, 20}) {
    m.BeginSnapshot();
    SnapshotWriteOptions o;
    o.index = idx;
    ASSERT_TRUE(WriteSnapshot(dir.path(), m, o).ok());
  }
  const std::string mf = dir.path() + "/" + SnapshotDirName(20) + "/MANIFEST";
  int fd = ::open(mf.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::pwrite(fd, "\xff\xff", 2, 3), 2);
  ::close(fd);
  auto latest = FindLatestSnapshot(dir.path());
  ASSERT_TRUE(latest.ok());
  ASSERT_TRUE(latest->has_value());
  EXPECT_EQ((*latest)->index(), 10u);
}

TEST(SnapshotTest, PruneKeepsNewest) {
  testing::TempDir dir;
  MemTable m(4);
  Fill(m, 10, "a");
  for (uint64_t idx : {1, 2, 3, 4}) {
    m.BeginSnapshot();
    SnapshotWriteOptions o;
    o.index = idx;
    o.retain = 2;
    ASSERT_TRUE(WriteSnapshot(dir.path(), m, o).ok());
  }
  EXPECT_FALSE(FileExists(dir.path() + "/" + SnapshotDirName(1)));
  EXPECT_FALSE(FileExists(dir.path() + "/" + SnapshotDirName(2)));
  EXPECT_TRUE(FileExists(dir.path() + "/" + SnapshotDirName(3)));
  EXPECT_TRUE(FileExists(dir.path() + "/" + SnapshotDirName(4)));
}

}  // namespace
}  // namespace kv
