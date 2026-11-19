#include "common/histogram.h"

#include <algorithm>
#include <bit>
#include <cmath>

#include "absl/strings/str_format.h"

namespace kv {

int Histogram::BucketFor(uint64_t v) {
  if (v < static_cast<uint64_t>(kSub)) return static_cast<int>(v);
  const int e = 63 - std::countl_zero(v);  // e >= kSubBits
  const int shift = e - kSubBits;
  const int sub = static_cast<int>((v >> shift) - kSub);
  return kSub + (e - kSubBits) * kSub + sub;
}

uint64_t Histogram::BucketUpper(int idx) {
  if (idx < kSub) return static_cast<uint64_t>(idx);
  const int i = idx - kSub;
  const int e = i / kSub + kSubBits;
  const int sub = i % kSub;
  const int shift = e - kSubBits;
  const uint64_t lower = static_cast<uint64_t>(kSub + sub) << shift;
  return lower + ((uint64_t{1} << shift) - 1);
}

void Histogram::Record(uint64_t value) {
  ++buckets_[BucketFor(value)];
  ++count_;
  sum_ += value;
  min_ = std::min(min_, value);
  max_ = std::max(max_, value);
}

void Histogram::Merge(const Histogram& other) {
  for (int i = 0; i < kBuckets; ++i) buckets_[i] += other.buckets_[i];
  count_ += other.count_;
  sum_ += other.sum_;
  min_ = std::min(min_, other.min_);
  max_ = std::max(max_, other.max_);
}

void Histogram::Clear() { *this = Histogram(); }

uint64_t Histogram::Percentile(double p) const {
  if (count_ == 0) return 0;
  const auto rank = static_cast<uint64_t>(std::ceil(p / 100.0 * static_cast<double>(count_)));
  const uint64_t target = std::max<uint64_t>(rank, 1);
  uint64_t seen = 0;
  for (int i = 0; i < kBuckets; ++i) {
    seen += buckets_[i];
    if (seen >= target) return std::min(BucketUpper(i), max_);
  }
  return max_;
}

std::string Histogram::Summary(double scale, const char* unit) const {
  return absl::StrFormat("n=%d mean=%.1f p50=%.1f p90=%.1f p99=%.1f p99.9=%.1f max=%.1f (%s)",
                         count_, Mean() / scale, Percentile(50) / scale, Percentile(90) / scale,
                         Percentile(99) / scale, Percentile(99.9) / scale, Max() / scale, unit);
}

}  // namespace kv
