#pragma once

#include <unistd.h>

#include <cstdlib>
#include <string>

#include "common/fs.h"
#include "gtest/gtest.h"

namespace kv::testing {

// A fresh directory under $TMPDIR, removed when the test ends.
class TempDir {
 public:
  TempDir() {
    const char* base = std::getenv("TMPDIR");
    std::string tmpl = std::string(base ? base : "/tmp") + "/kvtest-XXXXXX";
    char* p = ::mkdtemp(tmpl.data());
    EXPECT_NE(p, nullptr);
    path_ = tmpl;
  }
  ~TempDir() { RemoveAll(path_).IgnoreError(); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

}  // namespace kv::testing
