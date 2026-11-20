// Names the calling thread (visible in top -H, perf, gdb). Linux limits names
// to 15 characters.
#pragma once

#include <pthread.h>

#include <string>

namespace kv {

inline void SetThreadName(const std::string& name) {
  pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
}

}  // namespace kv
