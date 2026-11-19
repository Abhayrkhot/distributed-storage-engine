#include "common/fs.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>

#include "absl/strings/str_cat.h"

namespace kv {

absl::Status ErrnoStatus(std::string_view op, std::string_view path, int err) {
  std::string msg = std::string(op) + " " + std::string(path) + ": " + std::strerror(err);
  switch (err) {
    case ENOENT:
      return absl::NotFoundError(msg);
    case EEXIST:
      return absl::AlreadyExistsError(msg);
    case ENOSPC:
      return absl::ResourceExhaustedError(msg);
    default:
      return absl::InternalError(msg);
  }
}

absl::Status CreateDirs(const std::string& path) {
  std::error_code ec;
  std::filesystem::create_directories(path, ec);
  if (ec) return absl::InternalError(absl::StrCat("mkdir ", path, ": ", ec.message()));
  return absl::OkStatus();
}

absl::Status RemoveAll(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) return absl::InternalError(absl::StrCat("rm -r ", path, ": ", ec.message()));
  return absl::OkStatus();
}

absl::Status SyncDir(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) return ErrnoStatus("open", path, errno);
  int rc = ::fsync(fd);
  int err = errno;
  ::close(fd);
  if (rc != 0) return ErrnoStatus("fsync", path, err);
  return absl::OkStatus();
}

absl::Status RenameFile(const std::string& from, const std::string& to) {
  if (::rename(from.c_str(), to.c_str()) != 0) return ErrnoStatus("rename", from, errno);
  return absl::OkStatus();
}

absl::Status PWriteAll(int fd, const char* data, size_t len, uint64_t offset) {
  while (len > 0) {
    ssize_t n = ::pwrite(fd, data, len, static_cast<off_t>(offset));
    if (n < 0) {
      if (errno == EINTR) continue;
      return ErrnoStatus("pwrite", "fd", errno);
    }
    data += n;
    len -= static_cast<size_t>(n);
    offset += static_cast<uint64_t>(n);
  }
  return absl::OkStatus();
}

absl::Status WriteFileAtomic(const std::string& path, std::string_view data) {
  const std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) return ErrnoStatus("open", tmp, errno);
  absl::Status st = PWriteAll(fd, data.data(), data.size(), 0);
  if (st.ok() && ::fdatasync(fd) != 0) st = ErrnoStatus("fdatasync", tmp, errno);
  ::close(fd);
  if (!st.ok()) return st;
  if (auto s = RenameFile(tmp, path); !s.ok()) return s;
  return SyncDir(std::filesystem::path(path).parent_path().string());
}

absl::StatusOr<std::string> ReadFile(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return ErrnoStatus("open", path, errno);
  struct stat sb;
  if (::fstat(fd, &sb) != 0) {
    int err = errno;
    ::close(fd);
    return ErrnoStatus("fstat", path, err);
  }
  std::string out;
  out.resize(static_cast<size_t>(sb.st_size));
  size_t done = 0;
  while (done < out.size()) {
    ssize_t n = ::pread(fd, out.data() + done, out.size() - done, static_cast<off_t>(done));
    if (n < 0) {
      if (errno == EINTR) continue;
      int err = errno;
      ::close(fd);
      return ErrnoStatus("pread", path, err);
    }
    if (n == 0) break;
    done += static_cast<size_t>(n);
  }
  out.resize(done);
  ::close(fd);
  return out;
}

absl::StatusOr<std::vector<std::string>> ListDir(const std::string& path) {
  DIR* d = ::opendir(path.c_str());
  if (d == nullptr) return ErrnoStatus("opendir", path, errno);
  std::vector<std::string> names;
  while (struct dirent* e = ::readdir(d)) {
    std::string_view name(e->d_name);
    if (name == "." || name == "..") continue;
    names.emplace_back(name);
  }
  ::closedir(d);
  return names;
}

bool FileExists(const std::string& path) {
  struct stat sb;
  return ::stat(path.c_str(), &sb) == 0;
}

void EvictFromPageCache(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return;
  ::fdatasync(fd);
  ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
  ::close(fd);
}

}  // namespace kv
