#include "storage/memtable.h"

#include <map>
#include <random>
#include <thread>

#include "gtest/gtest.h"

namespace kv {
namespace {

std::map<std::string, std::string> Dump(MemTable& m) {
  std::map<std::string, std::string> out;
  for (auto& [k, v] : m.Scan("", "", SIZE_MAX)) out[k] = v;
  return out;
}

// Serializes every shard and decodes the result.
std::map<std::string, std::string> SerializeAll(MemTable& m) {
  MemTable copy(m.num_shards());
  for (uint32_t s = 0; s < m.num_shards(); ++s) {
    std::string data;
    uint64_t n = m.SerializeShard(s, &data);
    EXPECT_TRUE(copy.LoadShard(s, data, n).ok());
  }
  return Dump(copy);
}

TEST(MemTableTest, PutGetDeleteScan) {
  MemTable m(16);
  m.Put("b", "2");
  m.Put("a", "1");
  m.Put("c", "3");
  m.Put("b", "22");
  std::string v;
  ASSERT_TRUE(m.Get("b", &v));
  EXPECT_EQ(v, "22");
  m.Delete("a");
  EXPECT_FALSE(m.Get("a", &v));
  auto items = m.Scan("", "", 10);
  ASSERT_EQ(items.size(), 2u);
  EXPECT_EQ(items[0].first, "b");
  EXPECT_EQ(items[1].first, "c");
  EXPECT_EQ(m.Scan("b", "c", 10).size(), 1u);
  EXPECT_EQ(m.KeyCount(), 2u);
  EXPECT_EQ(m.ByteSize(), 1u + 2u + 1u + 1u);
}

TEST(MemTableTest, SnapshotIsPointInTimeDespiteLaterWrites) {
  MemTable m(8);
  for (int i = 0; i < 1000; ++i) m.Put("k" + std::to_string(i), "v0-" + std::to_string(i));
  const auto expected = Dump(m);

  m.BeginSnapshot();
  // Mutations after the snapshot point: overwrite, delete, create, and
  // delete-then-recreate.
  for (int i = 0; i < 1000; i += 3) m.Put("k" + std::to_string(i), "v1");
  for (int i = 1; i < 1000; i += 7) m.Delete("k" + std::to_string(i));
  for (int i = 0; i < 100; ++i) m.Put("new" + std::to_string(i), "n");
  m.Delete("k2");
  m.Put("k2", "again");

  EXPECT_EQ(SerializeAll(m), expected);
  // A second serialization (no snapshot pending) sees the current state.
  EXPECT_EQ(SerializeAll(m), Dump(m));
}

TEST(MemTableTest, SnapshotUnderConcurrentWriter) {
  MemTable m(32);
  for (int i = 0; i < 5000; ++i) m.Put("k" + std::to_string(i), "base");
  const auto expected = Dump(m);
  m.BeginSnapshot();
  std::atomic<bool> stop{false};
  std::thread writer([&] {
    std::mt19937 rng(1);
    while (!stop.load()) {
      int k = static_cast<int>(rng() % 6000);
      if (rng() % 4 == 0) m.Delete("k" + std::to_string(k));
      else m.Put("k" + std::to_string(k), "w" + std::to_string(rng()));
    }
  });
  MemTable copy(m.num_shards());
  for (uint32_t s = 0; s < m.num_shards(); ++s) {
    std::string data;
    uint64_t n = m.SerializeShard(s, &data);
    ASSERT_TRUE(copy.LoadShard(s, data, n).ok());
  }
  stop.store(true);
  writer.join();
  EXPECT_EQ(Dump(copy), expected);
}

TEST(MemTableTest, KeyHashIsStable) {
  // Snapshots store keys per shard; the hash must never change.
  EXPECT_EQ(KeyHash("hello"), KeyHash(std::string("hello")));
  EXPECT_NE(KeyHash("hello"), KeyHash("hellp"));
  MemTable a(256), b(256);
  EXPECT_EQ(a.ShardOf("some-key"), b.ShardOf("some-key"));
}

}  // namespace
}  // namespace kv
