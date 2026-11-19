#include "common/histogram.h"

#include <cmath>
#include <random>

#include "gtest/gtest.h"

namespace kv {
namespace {

TEST(HistogramTest, PercentilesWithinBucketError) {
  Histogram h;
  for (uint64_t v = 1; v <= 100000; ++v) h.Record(v);
  EXPECT_EQ(h.Count(), 100000u);
  EXPECT_EQ(h.Min(), 1u);
  EXPECT_EQ(h.Max(), 100000u);
  for (double p : {50.0, 90.0, 99.0, 99.9}) {
    const double expected = p / 100.0 * 100000;
    EXPECT_NEAR(static_cast<double>(h.Percentile(p)), expected, expected * 0.02) << p;
  }
}

TEST(HistogramTest, MergeMatchesSingle) {
  Histogram a, b, all;
  std::mt19937_64 rng(7);
  for (int i = 0; i < 10000; ++i) {
    uint64_t v = rng() % 1000000;
    (i % 2 ? a : b).Record(v);
    all.Record(v);
  }
  a.Merge(b);
  EXPECT_EQ(a.Count(), all.Count());
  EXPECT_EQ(a.Percentile(99), all.Percentile(99));
  EXPECT_EQ(a.Max(), all.Max());
}

TEST(HistogramTest, SmallValuesExact) {
  Histogram h;
  for (uint64_t v = 0; v < 64; ++v) h.Record(v);
  EXPECT_EQ(h.Percentile(100), 63u);
  EXPECT_EQ(h.Percentile(50), 31u);
}

}  // namespace
}  // namespace kv
