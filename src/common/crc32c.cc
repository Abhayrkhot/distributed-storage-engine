#include "common/crc32c.h"

#include <array>
#include <cstring>

#if defined(__x86_64__)
#include <nmmintrin.h>
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace kv::crc32c {
namespace {

constexpr uint32_t kPoly = 0x82f63b78;  // reflected Castagnoli polynomial

constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ kPoly : c >> 1;
    t[i] = c;
  }
  return t;
}
constexpr auto kTable = MakeTable();

uint32_t ExtendSoftware(uint32_t crc, const char* data, size_t n) {
  uint32_t c = ~crc;
  const auto* p = reinterpret_cast<const uint8_t*>(data);
  for (size_t i = 0; i < n; ++i) c = kTable[(c ^ p[i]) & 0xff] ^ (c >> 8);
  return ~c;
}

#if defined(__x86_64__)
__attribute__((target("sse4.2"))) uint32_t ExtendHardware(uint32_t crc, const char* data,
                                                          size_t n) {
  uint64_t c = ~crc;
  while (n >= 8) {
    uint64_t v;
    std::memcpy(&v, data, 8);
    c = _mm_crc32_u64(c, v);
    data += 8;
    n -= 8;
  }
  auto c32 = static_cast<uint32_t>(c);
  while (n > 0) {
    c32 = _mm_crc32_u8(c32, static_cast<uint8_t>(*data++));
    --n;
  }
  return ~c32;
}
bool DetectHardware() { return __builtin_cpu_supports("sse4.2"); }
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
uint32_t ExtendHardware(uint32_t crc, const char* data, size_t n) {
  uint32_t c = ~crc;
  while (n >= 8) {
    uint64_t v;
    std::memcpy(&v, data, 8);
    c = __crc32cd(c, v);
    data += 8;
    n -= 8;
  }
  while (n > 0) {
    c = __crc32cb(c, static_cast<uint8_t>(*data++));
    --n;
  }
  return ~c;
}
bool DetectHardware() { return true; }
#else
uint32_t ExtendHardware(uint32_t crc, const char* data, size_t n) {
  return ExtendSoftware(crc, data, n);
}
bool DetectHardware() { return false; }
#endif

const bool kHardware = DetectHardware();

}  // namespace

uint32_t Extend(uint32_t crc, const char* data, size_t n) {
  return kHardware ? ExtendHardware(crc, data, n) : ExtendSoftware(crc, data, n);
}

bool HardwareAccelerated() { return kHardware; }

}  // namespace kv::crc32c
