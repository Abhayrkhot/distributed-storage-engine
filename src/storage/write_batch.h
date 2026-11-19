// An atomic group of puts and deletes. The encoded form is what goes into a
// WAL record payload, so it is also the unit of crash atomicity: either every
// operation in a batch is recovered or none is.
//
//   rep := count:fixed32 op*
//   op  := kPut key:lp value:lp | kDelete key:lp      (lp = varint length prefix)
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/status/status.h"

namespace kv {

enum class OpType : uint8_t { kPut = 1, kDelete = 2 };

struct BatchOp {
  OpType type;
  std::string_view key;
  std::string_view value;  // empty for deletes
};

class WriteBatch {
 public:
  WriteBatch();
  explicit WriteBatch(std::string rep) : rep_(std::move(rep)) {}

  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);
  void Clear();

  uint32_t Count() const;
  bool Empty() const { return Count() == 0; }
  const std::string& Rep() const { return rep_; }
  std::string&& ReleaseRep() { return std::move(rep_); }
  size_t ByteSize() const { return rep_.size(); }

 private:
  std::string rep_;
};

// Zero-copy reader over an encoded batch. Views returned by Next() point into
// the buffer passed to the constructor.
class BatchReader {
 public:
  explicit BatchReader(std::string_view rep);

  // Returns non-OK if the header is malformed.
  absl::Status status() const { return status_; }
  uint32_t count() const { return count_; }
  // Returns false at the end of the batch or on corruption (check status()).
  bool Next(BatchOp* op);

 private:
  std::string_view rest_;
  uint32_t count_ = 0;
  uint32_t read_ = 0;
  absl::Status status_;
};

// Validates every operation in an encoded batch.
absl::Status ValidateBatch(std::string_view rep);

}  // namespace kv
