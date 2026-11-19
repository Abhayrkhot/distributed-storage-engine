#include "storage/snapshot.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "common/coding.h"
#include "common/crc32c.h"
#include "common/fs.h"
#include "common/logging.h"

namespace kv {
namespace {

constexpr uint32_t kFormatVersion = 1;
constexpr size_t kBlockHeader = 4 + 8 + 8;
constexpr size_t kFlushBytes = 8u << 20;

bool ParseSnapName(std::string_view name, uint64_t* index) {
  constexpr std::string_view kPre = "snap-";
  if (name.size() != kPre.size() + 16 || name.substr(0, kPre.size()) != kPre) return false;
  uint64_t v = 0;
  for (char c : name.substr(kPre.size())) {
    v <<= 4;
    if (c >= '0' && c <= '9') v |= static_cast<uint64_t>(c - '0');
    else if (c >= 'a' && c <= 'f') v |= static_cast<uint64_t>(c - 'a' + 10);
    else return false;
  }
  *index = v;
  return true;
}

// Runs fn(i) for i in [0, n) on up to `threads` threads; returns the first error.
template <typename Fn>
absl::Status ParallelFor(int n, int threads, Fn fn) {
  std::atomic<int> next{0};
  std::vector<absl::Status> errs(static_cast<size_t>(n));
  auto worker = [&] {
    for (int i; (i = next.fetch_add(1)) < n;) errs[static_cast<size_t>(i)] = fn(i);
  };
  const int t = std::max(1, std::min(threads, n));
  std::vector<std::thread> pool;
  for (int i = 1; i < t; ++i) pool.emplace_back(worker);
  worker();
  for (auto& th : pool) th.join();
  for (auto& e : errs) {
    if (!e.ok()) return e;
  }
  return absl::OkStatus();
}

}  // namespace

std::string SnapshotDirName(uint64_t index) { return absl::StrFormat("snap-%016x", index); }

absl::Status WriteManifest(const std::string& snap_dir, const snapshot::SnapshotManifest& m) {
  std::string body = m.SerializeAsString();
  PutFixed32(&body, crc32c::Value(body));
  return WriteFileAtomic(snap_dir + "/MANIFEST", body);
}

absl::StatusOr<snapshot::SnapshotManifest> ReadManifest(const std::string& snap_dir) {
  auto data = ReadFile(snap_dir + "/MANIFEST");
  if (!data.ok()) return data.status();
  if (data->size() < 4) return absl::DataLossError("manifest too short");
  const size_t n = data->size() - 4;
  if (crc32c::Value(data->data(), n) != DecodeFixed32(data->data() + n)) {
    return absl::DataLossError(absl::StrCat("manifest checksum mismatch in ", snap_dir));
  }
  snapshot::SnapshotManifest m;
  if (!m.ParseFromArray(data->data(), static_cast<int>(n))) {
    return absl::DataLossError(absl::StrCat("manifest does not parse in ", snap_dir));
  }
  if (m.format_version() != kFormatVersion) {
    return absl::FailedPreconditionError(
        absl::StrCat("unsupported snapshot format ", m.format_version()));
  }
  return m;
}

absl::StatusOr<snapshot::SnapshotManifest> WriteSnapshot(const std::string& root, MemTable& mem,
                                                         const SnapshotWriteOptions& opt) {
  const auto t0 = std::chrono::steady_clock::now();
  if (auto st = CreateDirs(root); !st.ok()) return st;
  const std::string final_dir = root + "/" + SnapshotDirName(opt.index);
  const std::string tmp_dir = final_dir + ".tmp";
  if (FileExists(final_dir)) {
    auto existing = ReadManifest(final_dir);
    if (existing.ok()) return *existing;  // already have this exact snapshot
    if (auto st = RemoveAll(final_dir); !st.ok()) return st;
  }
  if (auto st = RemoveAll(tmp_dir); !st.ok()) return st;
  if (auto st = CreateDirs(tmp_dir); !st.ok()) return st;

  const int nfiles = std::max(1, std::min<int>(opt.files, static_cast<int>(mem.num_shards())));
  std::vector<snapshot::SnapshotFile> files(static_cast<size_t>(nfiles));
  std::vector<uint64_t> keys(static_cast<size_t>(nfiles)), bytes(static_cast<size_t>(nfiles));

  absl::Status st = ParallelFor(nfiles, opt.threads, [&](int f) -> absl::Status {
    const std::string name = absl::StrFormat("data-%04d", f);
    const std::string path = tmp_dir + "/" + name;
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return ErrnoStatus("open", path, errno);
    std::string buf, shard_data;
    uint64_t off = 0, entries = 0;
    uint32_t crc = 0;
    absl::Status s;
    for (uint32_t shard = static_cast<uint32_t>(f); shard < mem.num_shards() && s.ok();
         shard += static_cast<uint32_t>(nfiles)) {
      shard_data.clear();
      const uint64_t count = mem.SerializeShard(shard, &shard_data);
      entries += count;
      PutFixed32(&buf, shard);
      PutFixed64(&buf, count);
      PutFixed64(&buf, shard_data.size());
      buf.append(shard_data);
      if (buf.size() >= kFlushBytes) {
        crc = crc32c::Extend(crc, buf.data(), buf.size());
        s = PWriteAll(fd, buf.data(), buf.size(), off);
        off += buf.size();
        buf.clear();
      }
    }
    if (s.ok() && !buf.empty()) {
      crc = crc32c::Extend(crc, buf.data(), buf.size());
      s = PWriteAll(fd, buf.data(), buf.size(), off);
      off += buf.size();
    }
    if (s.ok() && ::fdatasync(fd) != 0) s = ErrnoStatus("fdatasync", path, errno);
    ::close(fd);
    auto& fi = files[static_cast<size_t>(f)];
    fi.set_name(name);
    fi.set_size(off);
    fi.set_crc32c(crc);
    fi.set_entries(entries);
    return s;
  });
  if (!st.ok()) {
    RemoveAll(tmp_dir).IgnoreError();
    return st;
  }

  snapshot::SnapshotManifest m;
  m.set_format_version(kFormatVersion);
  m.set_index(opt.index);
  m.set_term(opt.term);
  m.set_num_shards(mem.num_shards());
  m.set_wal_start_segment(opt.wal_start_segment);
  uint64_t total = 0;
  for (auto& f : files) {
    total += f.entries();
    *m.add_files() = std::move(f);
  }
  m.set_key_count(total);
  m.set_byte_size(0);
  m.set_created_unix_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count());
  m.set_app_state(opt.app_state);
  if (auto s = WriteManifest(tmp_dir, m); !s.ok()) return s;
  if (auto s = SyncDir(tmp_dir); !s.ok()) return s;
  if (auto s = RenameFile(tmp_dir, final_dir); !s.ok()) return s;
  if (auto s = SyncDir(root); !s.ok()) return s;
  PruneSnapshots(root, opt.retain);
  const double secs =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  KV_LOG(Info) << "snapshot " << SnapshotDirName(opt.index) << ": " << total << " keys in "
               << nfiles << " files, " << absl::StrFormat("%.3f", secs) << "s";
  return m;
}

absl::StatusOr<std::optional<snapshot::SnapshotManifest>> FindLatestSnapshot(
    const std::string& root) {
  auto names = ListDir(root);
  if (!names.ok()) {
    if (absl::IsNotFound(names.status())) return std::optional<snapshot::SnapshotManifest>();
    return names.status();
  }
  std::vector<uint64_t> indexes;
  for (const auto& n : *names) {
    uint64_t idx;
    if (ParseSnapName(n, &idx)) indexes.push_back(idx);
  }
  std::sort(indexes.rbegin(), indexes.rend());
  for (uint64_t idx : indexes) {
    auto m = ReadManifest(root + "/" + SnapshotDirName(idx));
    if (m.ok()) return std::optional<snapshot::SnapshotManifest>(std::move(*m));
    KV_LOG(Warning) << "ignoring snapshot " << SnapshotDirName(idx) << ": " << m.status();
  }
  return std::optional<snapshot::SnapshotManifest>();
}

absl::Status VerifySnapshotDir(const std::string& snap_dir,
                               const snapshot::SnapshotManifest& manifest) {
  for (const auto& f : manifest.files()) {
    auto data = ReadFile(snap_dir + "/" + f.name());
    if (!data.ok()) return data.status();
    if (data->size() != f.size() || crc32c::Value(*data) != f.crc32c()) {
      return absl::DataLossError(absl::StrCat("snapshot file ", f.name(), " is corrupt"));
    }
  }
  return absl::OkStatus();
}

absl::Status LoadSnapshot(const std::string& root, const snapshot::SnapshotManifest& m,
                          MemTable& mem, int threads, SnapshotLoadStats* stats) {
  const auto t0 = std::chrono::steady_clock::now();
  const std::string dir = root + "/" + SnapshotDirName(m.index());
  const bool same_layout = m.num_shards() == mem.num_shards();
  std::atomic<uint64_t> keys{0};
  absl::Status st = ParallelFor(m.files_size(), threads, [&](int i) -> absl::Status {
    const auto& f = m.files(i);
    auto data = ReadFile(dir + "/" + f.name());
    if (!data.ok()) return data.status();
    if (data->size() != f.size() || crc32c::Value(*data) != f.crc32c()) {
      return absl::DataLossError(absl::StrCat("snapshot ", dir, "/", f.name(),
                                              " failed checksum verification"));
    }
    std::string_view in(*data);
    while (!in.empty()) {
      if (in.size() < kBlockHeader) return absl::DataLossError("truncated snapshot block");
      const uint32_t shard = DecodeFixed32(in.data());
      const uint64_t count = DecodeFixed64(in.data() + 4);
      const uint64_t len = DecodeFixed64(in.data() + 12);
      in.remove_prefix(kBlockHeader);
      if (len > in.size()) return absl::DataLossError("snapshot block overruns file");
      std::string_view block = in.substr(0, len);
      in.remove_prefix(len);
      if (same_layout) {
        if (auto s = mem.LoadShard(shard, block, count); !s.ok()) return s;
      } else {
        // Shard count changed since the snapshot: rehash every key.
        for (uint64_t k = 0; k < count; ++k) {
          std::string_view key, val;
          if (!GetLengthPrefixed(&block, &key) || !GetLengthPrefixed(&block, &val))
            return absl::DataLossError("truncated snapshot entry");
          mem.Put(key, val);
        }
      }
      keys.fetch_add(count, std::memory_order_relaxed);
    }
    return absl::OkStatus();
  });
  if (!st.ok()) return st;
  if (keys.load() != m.key_count()) {
    return absl::DataLossError(absl::StrCat("snapshot key count mismatch: manifest ",
                                            m.key_count(), ", loaded ", keys.load()));
  }
  if (stats) {
    stats->keys = keys.load();
    stats->seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  }
  return absl::OkStatus();
}

void PruneSnapshots(const std::string& root, int retain) {
  auto names = ListDir(root);
  if (!names.ok()) return;
  std::vector<uint64_t> indexes;
  for (const auto& n : *names) {
    uint64_t idx;
    if (ParseSnapName(n, &idx)) indexes.push_back(idx);
    else if (n.size() > 4 && n.substr(n.size() - 4) == ".tmp") RemoveAll(root + "/" + n).IgnoreError();
  }
  std::sort(indexes.rbegin(), indexes.rend());
  for (size_t i = static_cast<size_t>(std::max(retain, 1)); i < indexes.size(); ++i) {
    RemoveAll(root + "/" + SnapshotDirName(indexes[i])).IgnoreError();
  }
}

}  // namespace kv
