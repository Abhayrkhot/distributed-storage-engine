// Little-endian fixed-width and LEB128 varint encoding helpers shared by the
// WAL, write batches and snapshot files.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace kv {

inline void PutFixed32(std::string* dst, uint32_t v) {
  char buf[4];
  std::memcpy(buf, &v, 4);  // x86-64 and aarch64 are little-endian
  dst->append(buf, 4);
}

inline void PutFixed64(std::string* dst, uint64_t v) {
  char buf[8];
  std::memcpy(buf, &v, 8);
  dst->append(buf, 8);
}

inline void EncodeFixed32(char* dst, uint32_t v) { std::memcpy(dst, &v, 4); }
inline void EncodeFixed64(char* dst, uint64_t v) { std::memcpy(dst, &v, 8); }

inline uint32_t DecodeFixed32(const char* p) {
  uint32_t v;
  std::memcpy(&v, p, 4);
  return v;
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t v;
  std::memcpy(&v, p, 8);
  return v;
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  int n = 0;
  while (v >= 0x80) {
    buf[n++] = static_cast<char>(v | 0x80);
    v >>= 7;
  }
  buf[n++] = static_cast<char>(v);
  dst->append(buf, n);
}

inline size_t VarintLength(uint64_t v) {
  size_t len = 1;
  while (v >= 0x80) {
    v >>= 7;
    ++len;
  }
  return len;
}

// Parses a varint from the front of `in` and advances it. Returns false on
// truncated or over-long input.
inline bool GetVarint64(std::string_view* in, uint64_t* v) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && !in->empty(); shift += 7) {
    const auto byte = static_cast<uint8_t>((*in)[0]);
    in->remove_prefix(1);
    result |= static_cast<uint64_t>(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      *v = result;
      return true;
    }
  }
  return false;
}

inline void PutLengthPrefixed(std::string* dst, std::string_view s) {
  PutVarint64(dst, s.size());
  dst->append(s.data(), s.size());
}

inline bool GetLengthPrefixed(std::string_view* in, std::string_view* out) {
  uint64_t len;
  if (!GetVarint64(in, &len) || len > in->size()) return false;
  *out = in->substr(0, len);
  in->remove_prefix(len);
  return true;
}

}  // namespace kv
