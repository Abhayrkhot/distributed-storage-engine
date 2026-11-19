// kvctl: command-line client.
//
//   kvctl --endpoints=h1:7001,h2:7001,h3:7001 put <key> <value>
//   kvctl ... get <key> [--consistency=linearizable|lease|stale]
//   kvctl ... del <key>
//   kvctl ... scan <start> [end] [--limit=N]
//   kvctl ... status            (every endpoint)
//   kvctl ... transfer [id]     (graceful leadership transfer)
//   kvctl ... snapshot
#include <cstdio>
#include <string>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_split.h"
#include "client/kv_client.h"

ABSL_FLAG(std::string, endpoints, "127.0.0.1:7001", "comma-separated server addresses");
ABSL_FLAG(std::string, consistency, "linearizable", "linearizable | lease | stale");
ABSL_FLAG(uint32_t, limit, 100, "scan limit");
ABSL_FLAG(int, timeout_ms, 10000, "per-operation budget");

namespace {

int Fail(const absl::Status& st) {
  std::fprintf(stderr, "error: %s\n", st.ToString().c_str());
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<char*> args = absl::ParseCommandLine(argc, argv);
  if (args.size() < 2) {
    std::fprintf(stderr, "usage: kvctl [flags] put|get|del|scan|status|transfer|snapshot ...\n");
    return 2;
  }
  kv::client::ClientOptions o;
  o.endpoints = absl::StrSplit(absl::GetFlag(FLAGS_endpoints), ',', absl::SkipEmpty());
  o.timeout_ms = absl::GetFlag(FLAGS_timeout_ms);
  kv::client::Client c(o);
  const std::string cmd = args[1];
  auto arg = [&](size_t i) { return i < args.size() ? std::string(args[i]) : std::string(); };
  kv::api::Consistency cons = kv::api::LINEARIZABLE;
  if (absl::GetFlag(FLAGS_consistency) == "lease") cons = kv::api::LEASE;
  if (absl::GetFlag(FLAGS_consistency) == "stale") cons = kv::api::STALE;

  if (cmd == "put" && args.size() == 4) {
    uint64_t index = 0;
    if (auto st = c.Put(arg(2), arg(3), &index); !st.ok()) return Fail(st);
    std::printf("OK index=%lu\n", static_cast<unsigned long>(index));
  } else if (cmd == "get" && args.size() == 3) {
    auto v = c.Get(arg(2), cons);
    if (!v.ok()) return Fail(v.status());
    if (!v->has_value()) {
      std::printf("(not found)\n");
      return 3;
    }
    std::printf("%s\n", (*v)->c_str());
  } else if (cmd == "del" && args.size() == 3) {
    if (auto st = c.Delete(arg(2)); !st.ok()) return Fail(st);
    std::printf("OK\n");
  } else if (cmd == "scan" && args.size() >= 3) {
    auto items = c.Scan(arg(2), arg(3), absl::GetFlag(FLAGS_limit), cons);
    if (!items.ok()) return Fail(items.status());
    for (auto& [k, v] : *items) std::printf("%s\t%s\n", k.c_str(), v.c_str());
  } else if (cmd == "status") {
    for (const auto& e : o.endpoints) {
      auto s = c.Status(e);
      if (!s.ok()) {
        std::printf("%-22s unreachable (%s)\n", e.c_str(), std::string(s.status().message()).c_str());
        continue;
      }
      std::printf("%-22s id=%u role=%-9s term=%lu leader=%u commit=%lu applied=%lu last=%lu snap=%lu keys=%lu\n",
                  e.c_str(), s->id(), s->role().c_str(), static_cast<unsigned long>(s->term()),
                  s->leader_id(), static_cast<unsigned long>(s->commit_index()),
                  static_cast<unsigned long>(s->applied_index()),
                  static_cast<unsigned long>(s->last_log_index()),
                  static_cast<unsigned long>(s->snapshot_index()), static_cast<unsigned long>(s->keys()));
    }
  } else if (cmd == "transfer") {
    uint32_t target = args.size() > 2 ? static_cast<uint32_t>(std::stoul(arg(2))) : 0;
    auto r = c.TransferLeadership(target);
    if (!r.ok()) return Fail(r.status());
    std::printf("new leader: %u\n", *r);
  } else if (cmd == "snapshot") {
    auto r = c.Snapshot();
    if (!r.ok()) return Fail(r.status());
    std::printf("snapshot at index %lu\n", static_cast<unsigned long>(*r));
  } else {
    std::fprintf(stderr, "bad command\n");
    return 2;
  }
  return 0;
}
