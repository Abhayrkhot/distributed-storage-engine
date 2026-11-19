// Log-linear latency histogram (HdrHistogram-style): 64 linear sub-buckets per
// power of two, so any recorded value is reported within ~1.6% of its true
// value. Recording is a couple of instructions; histograms are per-thread and
// merged after a run, so there is no shared state on the hot path.
#pragma once

#include <array>
#include <cstdint>
#include <string>

namespace kv {

class Histogram {
 public:
  void Record(uint64_t value);
  void Merge(const Histogram& other);
  void Clear();

  uint64_t Count() const { return count_; }
  uint64_t Min() const { return count_ ? min_ : 0; }
  uint64_t Max() const { return max_; }
  double Mean() const { return count_ ? static_cast<double>(sum_) / count_ : 0.0; }
  // p in [0, 100]. Returns the upper bound of the bucket holding the rank.
  uint64_t Percentile(double p) const;

  // "p50=.. p90=.. p99=.. p99.9=.. max=.." with values divided by `scale`.
  std::string Summary(double scale = 1e3, const char* unit = "us") const;

 private:
  static constexpr int kSubBits = 6;
  static constexpr int kSub = 1 << kSubBits;
  static constexpr int kBuckets = kSub + (64 - kSubBits) * kSub;

  static int BucketFor(uint64_t v);
  static uint64_t BucketUpper(int idx);

  std::array<uint64_t, kBuckets> buckets_{};
  uint64_t count_ = 0;
  uint64_t sum_ = 0;
  uint64_t min_ = UINT64_MAX;
  uint64_t max_ = 0;
};

}  // namespace kv
