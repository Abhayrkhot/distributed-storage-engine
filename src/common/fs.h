// Small POSIX file helpers. Everything that must survive a crash goes through
// WriteFileAtomic (write temp, fsync, rename, fsync directory).
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace kv {

absl::Status ErrnoStatus(std::string_view op, std::string_view path, int err);

absl::Status CreateDirs(const std::string& path);
absl::Status RemoveAll(const std::string& path);
absl::Status SyncDir(const std::string& path);
absl::Status RenameFile(const std::string& from, const std::string& to);
absl::Status WriteFileAtomic(const std::string& path, std::string_view data);
absl::StatusOr<std::string> ReadFile(const std::string& path);
absl::StatusOr<std::vector<std::string>> ListDir(const std::string& path);
bool FileExists(const std::string& path);

// Drops the file's clean pages from the page cache so the next read hits the
// device. Used by the recovery benchmark to measure cold starts without root.
void EvictFromPageCache(const std::string& path);

// Writes all of `data` at `offset`, retrying on short writes and EINTR.
absl::Status PWriteAll(int fd, const char* data, size_t len, uint64_t offset);

}  // namespace kv
