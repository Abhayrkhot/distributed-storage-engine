// Minimal leveled logger: one line per message to stderr with a timestamp,
// level, thread id and source location. Lines are formatted into a local
// buffer and written with a single write(2), so concurrent logs never
// interleave.
#pragma once

#include <sstream>
#include <string>

namespace kv::log {

enum class Level { kDebug = 0, kInfo = 1, kWarning = 2, kError = 3, kFatal = 4 };

void SetMinLevel(Level level);
Level MinLevel();
// Parses "debug", "info", "warning", "error". Unknown strings leave it as is.
void SetMinLevel(const std::string& name);
// Prefix added to every line (e.g. the node id) so multi-node logs can be merged.
void SetPrefix(const std::string& prefix);

class Message {
 public:
  Message(Level level, const char* file, int line);
  ~Message();
  std::ostream& stream() { return out_; }

 private:
  Level level_;
  std::ostringstream out_;
};

// Swallows the stream when a level is disabled.
struct Voidify {
  void operator&(std::ostream&) {}
};

}  // namespace kv::log

#define KV_LOG_ENABLED(lvl) (::kv::log::Level::lvl >= ::kv::log::MinLevel())
#define KV_LOG(lvl)                \
  !KV_LOG_ENABLED(k##lvl) ? (void)0 \
                          : ::kv::log::Voidify() & ::kv::log::Message(::kv::log::Level::k##lvl, __FILE__, __LINE__).stream()

#define KV_CHECK(cond)                                                                 \
  (cond) ? (void)0                                                                     \
         : ::kv::log::Voidify() &                                                      \
               ::kv::log::Message(::kv::log::Level::kFatal, __FILE__, __LINE__).stream() \
                   << "Check failed: " #cond " "
