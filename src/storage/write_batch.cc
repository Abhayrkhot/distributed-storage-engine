#include "storage/write_batch.h"

#include "common/coding.h"

namespace kv {

WriteBatch::WriteBatch() { rep_.assign(4, '\0'); }

void WriteBatch::Put(std::string_view key, std::string_view value) {
  EncodeFixed32(rep_.data(), Count() + 1);
  rep_.push_back(static_cast<char>(OpType::kPut));
  PutLengthPrefixed(&rep_, key);
  PutLengthPrefixed(&rep_, value);
}

void WriteBatch::Delete(std::string_view key) {
  EncodeFixed32(rep_.data(), Count() + 1);
  rep_.push_back(static_cast<char>(OpType::kDelete));
  PutLengthPrefixed(&rep_, key);
}

void WriteBatch::Clear() { rep_.assign(4, '\0'); }

uint32_t WriteBatch::Count() const { return rep_.size() >= 4 ? DecodeFixed32(rep_.data()) : 0; }

BatchReader::BatchReader(std::string_view rep) {
  if (rep.size() < 4) {
    status_ = absl::DataLossError("write batch shorter than header");
    return;
  }
  count_ = DecodeFixed32(rep.data());
  rest_ = rep.substr(4);
}

bool BatchReader::Next(BatchOp* op) {
  if (!status_.ok() || read_ == count_) {
    if (status_.ok() && !rest_.empty()) status_ = absl::DataLossError("trailing bytes in batch");
    return false;
  }
  if (rest_.empty()) {
    status_ = absl::DataLossError("write batch truncated");
    return false;
  }
  const auto type = static_cast<OpType>(rest_[0]);
  rest_.remove_prefix(1);
  if (!GetLengthPrefixed(&rest_, &op->key)) {
    status_ = absl::DataLossError("bad key in write batch");
    return false;
  }
  op->type = type;
  op->value = {};
  if (type == OpType::kPut) {
    if (!GetLengthPrefixed(&rest_, &op->value)) {
      status_ = absl::DataLossError("bad value in write batch");
      return false;
    }
  } else if (type != OpType::kDelete) {
    status_ = absl::DataLossError("unknown op type in write batch");
    return false;
  }
  ++read_;
  return true;
}

absl::Status ValidateBatch(std::string_view rep) {
  BatchReader r(rep);
  BatchOp op;
  while (r.Next(&op)) {
  }
  return r.status();
}

}  // namespace kv
