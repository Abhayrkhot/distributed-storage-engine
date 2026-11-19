#include "common/logging.h"

#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <mutex>

namespace kv::log {
namespace {

std::atomic<int> g_min_level{static_cast<int>(Level::kInfo)};
std::mutex g_prefix_mu;
std::string g_prefix;

const char* LevelChar(Level l) {
  switch (l) {
    case Level::kDebug:
      return "D";
    case Level::kInfo:
      return "I";
    case Level::kWarning:
      return "W";
    case Level::kError:
      return "E";
    case Level::kFatal:
      return "F";
  }
  return "?";
}

const char* Basename(const char* path) {
  const char* slash = std::strrchr(path, '/');
  return slash ? slash + 1 : path;
}

}  // namespace

void SetMinLevel(Level level) { g_min_level.store(static_cast<int>(level)); }
Level MinLevel() { return static_cast<Level>(g_min_level.load(std::memory_order_relaxed)); }

void SetMinLevel(const std::string& name) {
  if (name == "debug") SetMinLevel(Level::kDebug);
  if (name == "info") SetMinLevel(Level::kInfo);
  if (name == "warning") SetMinLevel(Level::kWarning);
  if (name == "error") SetMinLevel(Level::kError);
}

void SetPrefix(const std::string& prefix) {
  std::lock_guard<std::mutex> l(g_prefix_mu);
  g_prefix = prefix;
}

Message::Message(Level level, const char* file, int line) : level_(level) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  gmtime_r(&ts.tv_sec, &tm);
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06ld", tm.tm_hour, tm.tm_min, tm.tm_sec,
                ts.tv_nsec / 1000);
  std::string prefix;
  {
    std::lock_guard<std::mutex> l(g_prefix_mu);
    prefix = g_prefix;
  }
  out_ << LevelChar(level) << buf << " " << static_cast<long>(::syscall(SYS_gettid)) << " ";
  if (!prefix.empty()) out_ << "[" << prefix << "] ";
  out_ << Basename(file) << ":" << line << "] ";
}

Message::~Message() {
  out_ << '\n';
  const std::string s = out_.str();
  ssize_t ignored = ::write(STDERR_FILENO, s.data(), s.size());
  (void)ignored;
  if (level_ == Level::kFatal) std::abort();
}

}  // namespace kv::log
