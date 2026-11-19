// A deliberately small HTTP/1.0 endpoint for Prometheus scrapes and probes:
// GET /metrics, /healthz (process up) and /readyz (serving).
#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>

namespace kv::server {

class MetricsServer {
 public:
  MetricsServer(int port, std::function<std::string()> render, std::function<bool()> ready);
  ~MetricsServer();
  bool ok() const { return fd_ >= 0; }

 private:
  void Loop();
  int fd_ = -1;
  std::atomic<bool> stop_{false};
  std::function<std::string()> render_;
  std::function<bool()> ready_;
  std::thread thread_;
};

}  // namespace kv::server
