// kvchaos: runs a real kvd cluster, kills nodes under load, and checks that no
// acknowledged write is lost and no linearizable read is stale.
//
// Each worker owns a set of keys and writes strictly increasing integers to
// them through the retrying client (exactly-once sessions). Per key it tracks
// the last value acknowledged and the last value attempted. Because each key
// has a single writer:
//   * a linearizable read of the key must return a value in [acked, attempted]
//     (lower = stale read or lost write, higher = impossible);
//   * after the run, with the cluster healed, the same must hold for every key.
// A chaos thread meanwhile SIGKILLs the leader (or SIGTERMs it for a graceful
// handover) every --kill_interval_ms and restarts it --down_ms later. Failover
// time is measured from the kill to the first write acknowledged by another node.
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "client/kv_client.h"
#include "common/fs.h"
#include "common/histogram.h"

ABSL_FLAG(std::string, kvd, "./build/kvd", "kvd binary");
ABSL_FLAG(std::string, kvd_args, "", "extra kvd flags (space separated)");
ABSL_FLAG(int, nodes, 3, "cluster size");
ABSL_FLAG(int, base_port, 7101, "first node's port");
ABSL_FLAG(std::string, dir, "/tmp/kvchaos", "data directory (wiped)");
ABSL_FLAG(double, duration, 60, "seconds of chaos");
ABSL_FLAG(int, kill_interval_ms, 3000, "time between kills");
ABSL_FLAG(int, down_ms, 1000, "how long a killed node stays down");
ABSL_FLAG(int, graceful_every, 0, "every Nth cycle uses SIGTERM (graceful handover); 0 = never");
ABSL_FLAG(std::string, target, "leader", "leader | random");
ABSL_FLAG(int, workers, 16, "writer threads");
ABSL_FLAG(int, keys_per_worker, 64, "keys owned by each worker");
ABSL_FLAG(std::string, label, "", "copied into the RESULT line");

namespace {

using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

uint64_t NowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

class Cluster {
 public:
  Cluster(int n, int base_port, std::string dir, std::string kvd, std::string extra)
      : dir_(std::move(dir)), kvd_(std::move(kvd)), extra_(std::move(extra)) {
    for (int i = 1; i <= n; ++i) addrs_[i] = absl::StrCat("127.0.0.1:", base_port + i - 1);
    std::string m;
    for (auto& [id, a] : addrs_) m += absl::StrCat(m.empty() ? "" : ",", id, "=", a);
    members_ = m;
  }
  ~Cluster() {
    for (auto& [id, pid] : pids_) {
      if (pid > 0) {
        ::kill(pid, SIGTERM);
        ::waitpid(pid, nullptr, 0);
      }
    }
  }
  void Start(int id) {
    pid_t pid = ::fork();
    if (pid == 0) {
      const std::string log = absl::StrCat(dir_, "/n", id, ".log");
      FILE* f = std::freopen(log.c_str(), "a", stderr);
      (void)f;
      std::vector<std::string> args = {kvd_, absl::StrCat("--id=", id), "--members=" + members_,
                                       absl::StrCat("--dir=", dir_, "/n", id)};
      for (absl::string_view a : absl::StrSplit(extra_, ' ', absl::SkipEmpty())) args.emplace_back(a);
      std::vector<char*> argv;
      for (auto& a : args) argv.push_back(a.data());
      argv.push_back(nullptr);
      ::execv(argv[0], argv.data());
      _exit(127);
    }
    pids_[id] = pid;
  }
  void Kill(int id, int sig) {
    if (pids_[id] <= 0) return;
    ::kill(pids_[id], sig);
    ::waitpid(pids_[id], nullptr, 0);
    pids_[id] = 0;
  }
  bool Running(int id) { return pids_[id] > 0; }
  const std::map<int, std::string>& addrs() const { return addrs_; }
  std::vector<std::string> endpoints() const {
    std::vector<std::string> e;
    for (auto& [id, a] : addrs_) e.push_back(a);
    return e;
  }

 private:
  std::string dir_, kvd_, extra_, members_;
  std::map<int, std::string> addrs_;
  std::map<int, pid_t> pids_;
};

int FindLeader(Cluster& c, kv::client::Client& probe, int timeout_ms) {
  const auto end = Clock::now() + milliseconds(timeout_ms);
  while (Clock::now() < end) {
    for (auto& [id, addr] : c.addrs()) {
      if (!c.Running(id)) continue;
      auto s = probe.Status(addr);
      if (s.ok() && s->role() == "leader") return id;
    }
    std::this_thread::sleep_for(milliseconds(10));
  }
  return 0;
}

struct Success {
  uint64_t t_ns;
  std::string endpoint;
};

struct WorkerState {
  std::vector<uint64_t> acked, attempted;
  std::vector<Success> successes;
  kv::Histogram write_lat;
  uint64_t writes = 0, write_errors = 0, reads = 0, read_errors = 0;
  uint64_t stale_reads = 0;
  std::string first_violation;
};

}  // namespace

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  const std::string dir = absl::GetFlag(FLAGS_dir);
  kv::RemoveAll(dir).IgnoreError();
  kv::CreateDirs(dir).IgnoreError();
  const int n = absl::GetFlag(FLAGS_nodes);
  Cluster cluster(n, absl::GetFlag(FLAGS_base_port), dir, absl::GetFlag(FLAGS_kvd),
                  absl::GetFlag(FLAGS_kvd_args));
  for (int i = 1; i <= n; ++i) cluster.Start(i);

  kv::client::ClientOptions po;
  po.endpoints = cluster.endpoints();
  po.attempt_ms = 300;
  kv::client::Client probe(po);
  if (FindLeader(cluster, probe, 10000) == 0) {
    std::fprintf(stderr, "cluster never elected a leader (see %s/n*.log)\n", dir.c_str());
    return 1;
  }

  const int workers = absl::GetFlag(FLAGS_workers), kpw = absl::GetFlag(FLAGS_keys_per_worker);
  std::vector<WorkerState> ws(static_cast<size_t>(workers));
  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;
  for (int w = 0; w < workers; ++w) {
    threads.emplace_back([&, w] {
      WorkerState& st = ws[static_cast<size_t>(w)];
      st.acked.assign(static_cast<size_t>(kpw), 0);
      st.attempted.assign(static_cast<size_t>(kpw), 0);
      kv::client::ClientOptions o;
      o.endpoints = cluster.endpoints();
      o.timeout_ms = 30000;
      o.attempt_ms = 1000;
      kv::client::Client c(o);
      std::mt19937_64 rng(static_cast<uint64_t>(w) + 99);
      uint64_t counter = 0;
      for (uint64_t op = 0; !stop.load(); ++op) {
        const size_t j = rng() % static_cast<size_t>(kpw);
        const std::string key = absl::StrCat("w", w, "-k", j);
        if (op % 4 == 3) {
          auto v = c.Get(key, kv::api::LINEARIZABLE);
          if (!v.ok()) {
            ++st.read_errors;
            continue;
          }
          ++st.reads;
          uint64_t got = 0;
          if (v->has_value()) absl::SimpleAtoi(**v, &got);
          if (got < st.acked[j] || got > st.attempted[j]) {
            ++st.stale_reads;
            if (st.first_violation.empty()) {
              st.first_violation = absl::StrCat(key, ": read ", got, ", acked ", st.acked[j],
                                                ", attempted ", st.attempted[j]);
            }
          }
          continue;
        }
        const uint64_t v = ++counter;
        st.attempted[j] = v;
        const uint64_t t0 = NowNs();
        auto s = c.Put(key, std::to_string(v));
        const uint64_t t1 = NowNs();
        if (s.ok()) {
          st.acked[j] = v;
          ++st.writes;
          st.write_lat.Record(t1 - t0);
          st.successes.push_back({t1, c.leader()});
        } else {
          ++st.write_errors;  // outcome unknown: attempted stays at v
        }
      }
    });
  }

  // Chaos.
  struct Kill {
    int id;
    uint64_t t_ns;
    bool graceful;
  };
  std::vector<Kill> kills;
  std::mt19937 rng(7);
  const auto end = Clock::now() + std::chrono::duration<double>(absl::GetFlag(FLAGS_duration));
  const int graceful_every = absl::GetFlag(FLAGS_graceful_every);
  int cycle = 0;
  std::this_thread::sleep_for(milliseconds(1000));
  while (Clock::now() < end) {
    int victim = FindLeader(cluster, probe, 5000);
    if (victim == 0) continue;
    if (absl::GetFlag(FLAGS_target) == "random") victim = static_cast<int>(rng() % n) + 1;
    ++cycle;
    const bool graceful = graceful_every > 0 && cycle % graceful_every == 0;
    const uint64_t t = NowNs();
    cluster.Kill(victim, graceful ? SIGTERM : SIGKILL);
    kills.push_back({victim, t, graceful});
    std::this_thread::sleep_for(milliseconds(absl::GetFlag(FLAGS_down_ms)));
    cluster.Start(victim);
    std::this_thread::sleep_for(
        milliseconds(std::max(0, absl::GetFlag(FLAGS_kill_interval_ms) - absl::GetFlag(FLAGS_down_ms))));
  }
  // Heal: everything up, a leader elected, then stop the workload.
  for (int i = 1; i <= n; ++i) {
    if (!cluster.Running(i)) cluster.Start(i);
  }
  FindLeader(cluster, probe, 10000);
  std::this_thread::sleep_for(milliseconds(1000));
  stop.store(true);
  for (auto& t : threads) t.join();

  // Final verification of every key through linearizable reads.
  kv::client::ClientOptions vo;
  vo.endpoints = cluster.endpoints();
  vo.timeout_ms = 30000;
  kv::client::Client verifier(vo);
  uint64_t lost = 0, phantom = 0, checked = 0;
  std::string first_lost;
  for (int w = 0; w < workers; ++w) {
    for (int j = 0; j < kpw; ++j) {
      const std::string key = absl::StrCat("w", w, "-k", j);
      auto v = verifier.Get(key, kv::api::LINEARIZABLE);
      if (!v.ok()) {
        std::fprintf(stderr, "verification read failed: %s\n", v.status().ToString().c_str());
        return 1;
      }
      uint64_t got = 0;
      if (v->has_value()) absl::SimpleAtoi(**v, &got);
      const auto& st = ws[static_cast<size_t>(w)];
      ++checked;
      if (got < st.acked[static_cast<size_t>(j)]) {
        ++lost;
        if (first_lost.empty()) first_lost = absl::StrCat(key, ": have ", got, ", acked ", st.acked[static_cast<size_t>(j)]);
      }
      if (got > st.attempted[static_cast<size_t>(j)]) ++phantom;
    }
  }

  // Failover: kill -> first write acknowledged by a node other than the victim.
  std::vector<Success> all;
  kv::Histogram lat;
  uint64_t writes = 0, write_errors = 0, reads = 0, read_errors = 0, stale = 0;
  std::string first_stale;
  for (auto& st : ws) {
    all.insert(all.end(), st.successes.begin(), st.successes.end());
    lat.Merge(st.write_lat);
    writes += st.writes;
    write_errors += st.write_errors;
    reads += st.reads;
    read_errors += st.read_errors;
    stale += st.stale_reads;
    if (first_stale.empty()) first_stale = st.first_violation;
  }
  std::sort(all.begin(), all.end(), [](const Success& a, const Success& b) { return a.t_ns < b.t_ns; });
  kv::Histogram failover_hard, failover_graceful;
  std::vector<double> hard_ms;
  for (const auto& k : kills) {
    const std::string victim = cluster.addrs().at(k.id);
    auto it = std::lower_bound(all.begin(), all.end(), k.t_ns,
                               [](const Success& s, uint64_t t) { return s.t_ns < t; });
    for (; it != all.end(); ++it) {
      if (it->endpoint != victim) {
        (k.graceful ? failover_graceful : failover_hard).Record(it->t_ns - k.t_ns);
        if (!k.graceful) hard_ms.push_back((it->t_ns - k.t_ns) / 1e6);
        break;
      }
    }
  }
  std::printf("kills: %zu (%lu graceful)\n", kills.size(), static_cast<unsigned long>(failover_graceful.Count()));
  std::printf("writes acknowledged: %lu, failed/unknown: %lu; linearizable reads: %lu (failed %lu)\n",
              static_cast<unsigned long>(writes), static_cast<unsigned long>(write_errors),
              static_cast<unsigned long>(reads), static_cast<unsigned long>(read_errors));
  std::printf("write latency: %s\n", lat.Summary(1e6, "ms").c_str());
  std::printf("failover after SIGKILL: %s\n", failover_hard.Summary(1e6, "ms").c_str());
  if (failover_graceful.Count()) {
    std::printf("handover after SIGTERM: %s\n", failover_graceful.Summary(1e6, "ms").c_str());
  }
  std::printf("verification: %lu keys checked, %lu lost acknowledged writes, %lu phantom values, "
              "%lu stale reads during the run\n",
              static_cast<unsigned long>(checked), static_cast<unsigned long>(lost),
              static_cast<unsigned long>(phantom), static_cast<unsigned long>(stale));
  if (!first_lost.empty()) std::printf("  first lost: %s\n", first_lost.c_str());
  if (!first_stale.empty()) std::printf("  first stale read: %s\n", first_stale.c_str());
  std::printf(
      "RESULT {\"workload\":\"chaos\",\"label\":\"%s\",\"kills\":%zu,\"graceful\":%lu,\"writes\":%lu,"
      "\"write_errors\":%lu,\"reads\":%lu,\"lost\":%lu,\"phantom\":%lu,\"stale_reads\":%lu,"
      "\"failover_p50_ms\":%.1f,\"failover_p90_ms\":%.1f,\"failover_p99_ms\":%.1f,\"failover_max_ms\":%.1f,"
      "\"handover_p50_ms\":%.1f,\"handover_max_ms\":%.1f,\"write_p99_ms\":%.2f}\n",
      absl::GetFlag(FLAGS_label).c_str(), kills.size(),
      static_cast<unsigned long>(failover_graceful.Count()), static_cast<unsigned long>(writes),
      static_cast<unsigned long>(write_errors), static_cast<unsigned long>(reads),
      static_cast<unsigned long>(lost), static_cast<unsigned long>(phantom),
      static_cast<unsigned long>(stale), failover_hard.Percentile(50) / 1e6,
      failover_hard.Percentile(90) / 1e6, failover_hard.Percentile(99) / 1e6, failover_hard.Max() / 1e6,
      failover_graceful.Percentile(50) / 1e6, failover_graceful.Max() / 1e6, lat.Percentile(99) / 1e6);
  return (lost == 0 && phantom == 0 && stale == 0) ? 0 : 3;
}
