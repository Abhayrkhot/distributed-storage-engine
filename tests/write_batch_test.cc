#include "storage/write_batch.h"

#include "gtest/gtest.h"

namespace kv {
namespace {

TEST(WriteBatchTest, RoundTrip) {
  WriteBatch b;
  b.Put("alpha", "1");
  b.Delete("beta");
  b.Put(std::string("k\0ey", 4), std::string(1000, 'x'));
  EXPECT_EQ(b.Count(), 3u);

  BatchReader r(b.Rep());
  BatchOp op;
  ASSERT_TRUE(r.Next(&op));
  EXPECT_EQ(op.type, OpType::kPut);
  EXPECT_EQ(op.key, "alpha");
  EXPECT_EQ(op.value, "1");
  ASSERT_TRUE(r.Next(&op));
  EXPECT_EQ(op.type, OpType::kDelete);
  EXPECT_EQ(op.key, "beta");
  ASSERT_TRUE(r.Next(&op));
  EXPECT_EQ(op.key, std::string("k\0ey", 4));
  EXPECT_EQ(op.value.size(), 1000u);
  EXPECT_FALSE(r.Next(&op));
  EXPECT_TRUE(r.status().ok());
}

TEST(WriteBatchTest, DetectsTruncationAndGarbage) {
  WriteBatch b;
  b.Put("key", "value");
  std::string rep = b.Rep();
  EXPECT_FALSE(ValidateBatch(rep.substr(0, rep.size() - 2)).ok());
  EXPECT_FALSE(ValidateBatch(rep + "x").ok());
  std::string bad = rep;
  bad[4] = 9;  // unknown op type
  EXPECT_FALSE(ValidateBatch(bad).ok());
  EXPECT_FALSE(ValidateBatch("ab").ok());
  EXPECT_TRUE(ValidateBatch(rep).ok());
}

}  // namespace
}  // namespace kv
