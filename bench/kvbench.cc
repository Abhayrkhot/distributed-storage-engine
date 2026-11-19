// kvbench: in-process benchmarks for the storage engine.
//
//   --workload=write     N threads issuing durable writes (optionally mixed
//                        with reads) for --duration seconds
//   --workload=recover   fill --records keys in a child process, SIGKILL it,
//                        then time Engine::Open in the parent
//
// --engine=rocksdb runs the same write workload against RocksDB with
// WriteOptions::sync, as an external baseline on the same disk.
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <random>
#include <semaphore>
#include <string>
#include <thread>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"
#include "common/crc32c.h"
#include "common/fs.h"
#include "common/histogram.h"
#include "common/logging.h"
#include "storage/engine.h"

#ifdef KV_HAVE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/version.h>
#endif

ABSL_FLAG(std::string, workload, "write", "write | recover");
ABSL_FLAG(std::string, engine, "native", "native | rocksdb");
ABSL_FLAG(std::string, dir, "/tmp/kvbench", "data directory (wiped first)");
ABSL_FLAG(std::string, concurrency, "pipelined", "pipelined | global (naive single-lock baseline)");
ABSL_FLAG(uint32_t, shards, 256, "memtable shards");
ABSL_FLAG(int, threads, 16, "client threads");
ABSL_FLAG(int, depth, 1, "outstanding writes per thread (1 = blocking Write)");
ABSL_FLAG(double, duration, 10, "seconds to run the write workload");
ABSL_FLAG(double, read_ratio, 0, "fraction of operations that are point reads");
ABSL_FLAG(int, value_size, 100, "value bytes");
ABSL_FLAG(uint64_t, key_space, 1000000, "distinct keys");
ABSL_FLAG(std::string, sync, "always", "always | none");
ABSL_FLAG(bool, preallocate, true, "pre-zero / recycle WAL segments");
ABSL_FLAG(uint64_t, records, 1000000, "recover: keys to load before the crash");
ABSL_FLAG(bool, snapshot_before_crash, true, "recover: checkpoint after loading");
ABSL_FLAG(uint64_t, tail_records, 0, "recover: extra writes after the checkpoint (WAL tail)");
ABSL_FLAG(bool, cold, false, "recover: evict data files from the page cache before reopening");
ABSL_FLAG(int, recovery_threads, 0, "recover: threads (0 = all cores)");
ABSL_FLAG(uint64_t, snapshot_trigger_mb, 512, "WAL MiB between automatic snapshots (0 = off)");
ABSL_FLAG(std::string, label, "", "free-form label copied into the RESULT line");

namespace kv {
namespace {

using Clock = std::chrono::steady_clock;

uint64_t NowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

std::string KeyFor(uint64_t k) { return absl::StrFormat("key%013d", k); }

// A store under test. Blocking writes only; async is native-only.
class Store {
 public:
  virtual ~Store() = default;
  virtual absl::Status Put(const std::string& k, std::string_view v) = 0;
  virtual bool Get(const std::string& k, std::string* v) = 0;
  virtual Engine* native() { return nullptr; }
  virtual std::string Describe() = 0;
};

class NativeStore : public Store {
 public:
  explicit NativeStore(std::unique_ptr<Engine> e) : e_(std::move(e)) {}
  absl::Status Put(const std::string& k, std::string_view v) override { return e_->Put(k, v); }
  bool Get(const std::string& k, std::string* v) override { return e_->Get(k, v); }
  Engine* native() override { return e_.get(); }
  std::string Describe() override {
    auto s = e_->Stats();
    double groups = static_cast<double>(std::max<uint64_t>(s.wal.groups, 1));
    return absl::StrFormat(
        "wal: %d groups, %.1f records/group, %d fdatasync, mean fdatasync %.1f us, %d segments "
        "rolled (%d recycled, %d stalls); snapshots %d",
        s.wal.groups, static_cast<double>(s.wal.records) / groups, s.wal.syncs,
        s.wal.syncs ? static_cast<double>(s.wal.sync_nanos) / s.wal.syncs / 1e3 : 0.0,
        s.wal.segments_rolled, s.wal.segments_recycled, s.wal.roll_stalls, s.snapshots);
  }

 private:
  std::unique_ptr<Engine> e_;
};

#ifdef KV_HAVE_ROCKSDB
class RocksStore : public Store {
 public:
  static absl::StatusOr<std::unique_ptr<Store>> Open(const std::string& dir, bool sync) {
    rocksdb::Options o;
    o.create_if_missing = true;
    o.IncreaseParallelism(static_cast<int>(std::thread::hardware_concurrency()));
    o.OptimizeLevelStyleCompaction();
    o.recycle_log_file_num = 4;  // same trick our WAL uses; makes fsync cheaper
    rocksdb::DB* db;
    auto st = rocksdb::DB::Open(o, dir, &db);
    if (!st.ok()) return absl::InternalError(st.ToString());
    auto s = std::make_unique<RocksStore>();
    s->db_.reset(db);
    s->wo_.sync = sync;
    return std::unique_ptr<Store>(std::move(s));
  }
  absl::Status Put(const std::string& k, std::string_view v) override {
    auto st = db_->Put(wo_, k, rocksdb::Slice(v.data(), v.size()));
    return st.ok() ? absl::OkStatus() : absl::InternalError(st.ToString());
  }
  bool Get(const std::string& k, std::string* v) override {
    return db_->Get(rocksdb::ReadOptions(), k, v).ok();
  }
  std::string Describe() override {
    return absl::StrFormat("rocksdb %d.%d.%d", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH);
  }

 private:
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::WriteOptions wo_;
};
#endif

EngineOptions MakeEngineOptions(const std::string& dir) {
  EngineOptions o;
  o.dir = dir;
  o.num_shards = absl::GetFlag(FLAGS_shards);
  o.concurrency = absl::GetFlag(FLAGS_concurrency) == "global" ? Concurrency::kGlobalLock
                                                                : Concurrency::kPipelined;
  o.wal.sync_mode = absl::GetFlag(FLAGS_sync) == "none" ? SyncMode::kNone : SyncMode::kAlways;
  o.wal.preallocate = absl::GetFlag(FLAGS_preallocate);
  o.snapshot_trigger_bytes = absl::GetFlag(FLAGS_snapshot_trigger_mb) << 20;
  o.recovery_threads = absl::GetFlag(FLAGS_recovery_threads);
  return o;
}

absl::StatusOr<std::unique_ptr<Store>> OpenStore(const std::string& dir) {
  if (absl::GetFlag(FLAGS_engine) == "rocksdb") {
#ifdef KV_HAVE_ROCKSDB
    return RocksStore::Open(dir, absl::GetFlag(FLAGS_sync) != "none");
#else
    return absl::UnimplementedError("built without RocksDB");
#endif
  }
  auto e = Engine::Open(MakeEngineOptions(dir));
  if (!e.ok()) return e.status();
  return std::unique_ptr<Store>(std::make_unique<NativeStore>(std::move(*e)));
}

// One client thread's async window: at most `depth` writes in flight.
struct AsyncWindow {
  explicit AsyncWindow(int depth) : slots(static_cast<size_t>(depth)), free_slots(depth) {
    for (auto& s : slots) s.owner = this;
    for (size_t i = 0; i < slots.size(); ++i) free_list.push_back(&slots[i]);
  }
  struct Slot final : WriteCompletion {
    AsyncWindow* owner = nullptr;
    uint64_t start = 0;
    void Done(const absl::Status& st, uint64_t) override { owner->Complete(this, st); }
  };
  void Complete(Slot* s, const absl::Status& st) {
    // Runs on the engine's applier thread, the only writer of `lat` and `errors`.
    if (!st.ok()) ++errors;
    lat.Record(NowNs() - s->start);
    {
      std::lock_guard<std::mutex> l(mu);
      free_list.push_back(s);
    }
    free_slots.release();
  }
  Slot* Acquire() {
    free_slots.acquire();
    std::lock_guard<std::mutex> l(mu);
    Slot* s = free_list.back();
    free_list.pop_back();
    return s;
  }
  std::vector<Slot> slots;
  std::counting_semaphore<1 << 20> free_slots;
  std::mutex mu;
  std::vector<Slot*> free_list;
  Histogram lat;
  uint64_t errors = 0;
};

int RunWrite() {
  const std::string dir = absl::GetFlag(FLAGS_dir);
  RemoveAll(dir).IgnoreError();
  auto store_or = OpenStore(dir);
  if (!store_or.ok()) {
    std::fprintf(stderr, "open: %s\n", store_or.status().ToString().c_str());
    return 1;
  }
  Store& store = **store_or;
  const int threads = absl::GetFlag(FLAGS_threads);
  const int depth = absl::GetFlag(FLAGS_depth);
  const double read_ratio = absl::GetFlag(FLAGS_read_ratio);
  const uint64_t key_space = absl::GetFlag(FLAGS_key_space);
  const int vsize = absl::GetFlag(FLAGS_value_size);
  const auto duration = std::chrono::duration<double>(absl::GetFlag(FLAGS_duration));
  if (depth > 1 && store.native() == nullptr) {
    std::fprintf(stderr, "--depth > 1 needs --engine=native\n");
    return 1;
  }

  std::string value_pool(1 << 20, '\0');
  std::mt19937_64 seed_rng(42);
  for (auto& c : value_pool) c = static_cast<char>('a' + seed_rng() % 26);

  std::atomic<bool> stop{false};
  std::vector<Histogram> wlat(static_cast<size_t>(threads)), rlat(static_cast<size_t>(threads));
  std::vector<uint64_t> writes(static_cast<size_t>(threads)), reads(static_cast<size_t>(threads)),
      errors(static_cast<size_t>(threads));
  std::vector<std::unique_ptr<AsyncWindow>> windows;
  for (int t = 0; t < threads; ++t) windows.push_back(std::make_unique<AsyncWindow>(std::max(depth, 1)));

  auto worker = [&](int t) {
    std::mt19937_64 rng(1000 + static_cast<uint64_t>(t));
    std::uniform_real_distribution<double> coin(0, 1);
    std::string val;
    const size_t ti = static_cast<size_t>(t);
    while (!stop.load(std::memory_order_relaxed)) {
      const std::string key = KeyFor(rng() % key_space);
      if (read_ratio > 0 && coin(rng) < read_ratio) {
        const uint64_t t0 = NowNs();
        store.Get(key, &val);
        rlat[ti].Record(NowNs() - t0);
        ++reads[ti];
        continue;
      }
      std::string_view v(value_pool.data() + rng() % (value_pool.size() - static_cast<size_t>(vsize)),
                         static_cast<size_t>(vsize));
      if (depth <= 1) {
        const uint64_t t0 = NowNs();
        if (!store.Put(key, v).ok()) ++errors[ti];
        wlat[ti].Record(NowNs() - t0);
      } else {
        AsyncWindow::Slot* s = windows[ti]->Acquire();
        WriteBatch b;
        b.Put(key, v);
        s->start = NowNs();
        store.native()->WriteAsync(std::move(b), s);
      }
      ++writes[ti];
    }
    if (depth > 1) {  // drain
      for (int i = 0; i < depth; ++i) windows[ti]->free_slots.acquire();
    }
  };

  const auto t0 = Clock::now();
  std::vector<std::thread> pool;
  for (int t = 0; t < threads; ++t) pool.emplace_back(worker, t);
  std::this_thread::sleep_for(duration);
  stop.store(true);
  for (auto& th : pool) th.join();
  const double secs = std::chrono::duration<double>(Clock::now() - t0).count();

  Histogram w, r;
  uint64_t nw = 0, nr = 0, nerr = 0;
  for (int t = 0; t < threads; ++t) {
    const size_t ti = static_cast<size_t>(t);
    w.Merge(depth > 1 ? windows[ti]->lat : wlat[ti]);
    r.Merge(rlat[ti]);
    nw += writes[ti];
    nr += reads[ti];
    nerr += errors[ti] + windows[ti]->errors;
  }
  const std::string desc = store.Describe();
  std::printf("engine=%s concurrency=%s shards=%u threads=%d depth=%d sync=%s prealloc=%d value=%d\n",
              absl::GetFlag(FLAGS_engine).c_str(), absl::GetFlag(FLAGS_concurrency).c_str(),
              absl::GetFlag(FLAGS_shards), threads, depth, absl::GetFlag(FLAGS_sync).c_str(),
              absl::GetFlag(FLAGS_preallocate) ? 1 : 0, vsize);
  std::printf("writes: %.0f/s  %s\n", static_cast<double>(nw) / secs, w.Summary().c_str());
  if (nr) std::printf("reads:  %.0f/s  %s\n", static_cast<double>(nr) / secs, r.Summary().c_str());
  std::printf("%s  errors=%lu  crc32c_hw=%d\n", desc.c_str(), static_cast<unsigned long>(nerr),
              crc32c::HardwareAccelerated() ? 1 : 0);
  std::printf(
      "RESULT {\"workload\":\"write\",\"label\":\"%s\",\"engine\":\"%s\",\"concurrency\":\"%s\","
      "\"shards\":%u,\"threads\":%d,\"depth\":%d,\"sync\":\"%s\",\"preallocate\":%s,"
      "\"value_size\":%d,\"read_ratio\":%.2f,\"seconds\":%.2f,\"writes_per_s\":%.0f,"
      "\"reads_per_s\":%.0f,\"w_p50_us\":%.1f,\"w_p99_us\":%.1f,\"w_p999_us\":%.1f,"
      "\"w_max_us\":%.1f,\"r_p50_us\":%.2f,\"r_p99_us\":%.2f,\"errors\":%lu}\n",
      absl::GetFlag(FLAGS_label).c_str(), absl::GetFlag(FLAGS_engine).c_str(),
      absl::GetFlag(FLAGS_concurrency).c_str(), absl::GetFlag(FLAGS_shards), threads, depth,
      absl::GetFlag(FLAGS_sync).c_str(), absl::GetFlag(FLAGS_preallocate) ? "true" : "false", vsize,
      read_ratio, secs, static_cast<double>(nw) / secs, static_cast<double>(nr) / secs,
      w.Percentile(50) / 1e3, w.Percentile(99) / 1e3, w.Percentile(99.9) / 1e3, w.Max() / 1e3,
      r.Percentile(50) / 1e3, r.Percentile(99) / 1e3, static_cast<unsigned long>(nerr));
  return nerr == 0 ? 0 : 2;
}

// Fills the engine in a child, kills it with SIGKILL (no clean shutdown), then
// times recovery in the parent.
int RunRecover() {
  const std::string dir = absl::GetFlag(FLAGS_dir);
  RemoveAll(dir).IgnoreError();
  const uint64_t records = absl::GetFlag(FLAGS_records);
  const uint64_t tail = absl::GetFlag(FLAGS_tail_records);
  const int vsize = absl::GetFlag(FLAGS_value_size);

  pid_t pid = fork();
  if (pid == 0) {
    auto e = Engine::Open(MakeEngineOptions(dir));
    if (!e.ok()) _exit(3);
    std::string value(static_cast<size_t>(vsize), 'v');
    auto load = [&](uint64_t from, uint64_t to) {
      // 16 writers with 64-deep async windows load quickly; each batch is one key.
      std::atomic<uint64_t> next{from};
      std::vector<std::thread> pool;
      for (int t = 0; t < 16; ++t) {
        pool.emplace_back([&, t] {
          AsyncWindow win(64);
          for (uint64_t k; (k = next.fetch_add(1)) < to;) {
            auto* s = win.Acquire();
            WriteBatch b;
            std::string v = value;
            std::memcpy(v.data(), &k, std::min<size_t>(8, v.size()));
            b.Put(KeyFor(k), v);
            (*e)->WriteAsync(std::move(b), s);
          }
          for (int i = 0; i < 64; ++i) win.free_slots.acquire();
        });
      }
      for (auto& th : pool) th.join();
    };
    load(0, records);
    if (absl::GetFlag(FLAGS_snapshot_before_crash)) {
      if (!(*e)->Checkpoint().ok()) _exit(4);
    }
    load(records, records + tail);
    raise(SIGKILL);
    _exit(5);
  }
  int status = 0;
  waitpid(pid, &status, 0);
  if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL) {
    std::fprintf(stderr, "loader child did not die by SIGKILL (status %d)\n", status);
    return 1;
  }
  if (absl::GetFlag(FLAGS_cold)) {
    for (const char* sub : {"/wal", "/snapshots"}) {
      std::vector<std::string> stack{dir + sub};
      while (!stack.empty()) {
        std::string d = stack.back();
        stack.pop_back();
        auto names = ListDir(d);
        if (!names.ok()) continue;
        for (const auto& n : *names) {
          const std::string p = d + "/" + n;
          if (ListDir(p).ok()) stack.push_back(p);
          else EvictFromPageCache(p);
        }
      }
    }
  }
  const auto t0 = Clock::now();
  auto e = Engine::Open(MakeEngineOptions(dir));
  const double open_s = std::chrono::duration<double>(Clock::now() - t0).count();
  if (!e.ok()) {
    std::fprintf(stderr, "recovery failed: %s\n", e.status().ToString().c_str());
    return 1;
  }
  const RecoveryStats& r = (*e)->recovery();
  // Verify every key came back with the right contents.
  uint64_t missing = 0;
  std::string v;
  for (uint64_t k = 0; k < records + tail; ++k) {
    if (!(*e)->Get(KeyFor(k), &v) || v.size() != static_cast<size_t>(vsize) ||
        std::memcmp(v.data(), &k, std::min<size_t>(8, v.size())) != 0) {
      ++missing;
    }
  }
  std::printf("recovered %lu keys in %.3fs (snapshot %.3fs: %lu keys @%lu; WAL %.3fs: %lu records, "
              "%.1f MiB, %lu segments, torn %lu B) cold=%d missing=%lu\n",
              static_cast<unsigned long>(r.keys), open_s, r.snapshot_seconds,
              static_cast<unsigned long>(r.snapshot_keys), static_cast<unsigned long>(r.snapshot_index),
              r.wal_seconds, static_cast<unsigned long>(r.wal_records), r.wal_bytes / 1048576.0,
              static_cast<unsigned long>(r.wal_segments), static_cast<unsigned long>(r.torn_bytes),
              absl::GetFlag(FLAGS_cold) ? 1 : 0, static_cast<unsigned long>(missing));
  std::printf(
      "RESULT {\"workload\":\"recover\",\"label\":\"%s\",\"records\":%lu,\"tail_records\":%lu,"
      "\"snapshot\":%s,\"cold\":%s,\"value_size\":%d,\"open_s\":%.4f,\"snapshot_s\":%.4f,"
      "\"wal_s\":%.4f,\"keys\":%lu,\"wal_records\":%lu,\"wal_mib\":%.1f,\"missing\":%lu,"
      "\"threads\":%d}\n",
      absl::GetFlag(FLAGS_label).c_str(), static_cast<unsigned long>(records),
      static_cast<unsigned long>(tail), absl::GetFlag(FLAGS_snapshot_before_crash) ? "true" : "false",
      absl::GetFlag(FLAGS_cold) ? "true" : "false", vsize, open_s, r.snapshot_seconds, r.wal_seconds,
      static_cast<unsigned long>(r.keys), static_cast<unsigned long>(r.wal_records),
      r.wal_bytes / 1048576.0, static_cast<unsigned long>(missing),
      (*e)->options().recovery_threads);
  return missing == 0 ? 0 : 2;
}

}  // namespace
}  // namespace kv

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  kv::log::SetMinLevel(kv::log::Level::kWarning);
  const std::string w = absl::GetFlag(FLAGS_workload);
  if (w == "write") return kv::RunWrite();
  if (w == "recover") return kv::RunRecover();
  std::fprintf(stderr, "unknown --workload=%s\n", w.c_str());
  return 1;
}
