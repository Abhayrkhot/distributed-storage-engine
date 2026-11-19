// CRC-32C (Castagnoli). Uses the SSE4.2 / ARMv8 CRC instructions when the CPU
// has them (~10 GB/s single stream) and a table fallback otherwise.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace kv::crc32c {

// Extends `crc` (the CRC of some prefix) with `n` more bytes.
uint32_t Extend(uint32_t crc, const char* data, size_t n);

inline uint32_t Value(const char* data, size_t n) { return Extend(0, data, n); }
inline uint32_t Value(std::string_view s) { return Extend(0, s.data(), s.size()); }

// True if the hardware path is in use (reported by kvbench).
bool HardwareAccelerated();

}  // namespace kv::crc32c
