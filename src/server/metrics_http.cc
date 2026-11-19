#include "server/metrics_http.h"

#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "common/logging.h"

namespace kv::server {

MetricsServer::MetricsServer(int port, std::function<std::string()> render,
                             std::function<bool()> ready)
    : render_(std::move(render)), ready_(std::move(ready)) {
  fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd_ < 0) return;
  int one = 1;
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(fd_, 64) != 0) {
    KV_LOG(Error) << "metrics: cannot listen on :" << port << ": " << std::strerror(errno);
    ::close(fd_);
    fd_ = -1;
    return;
  }
  thread_ = std::thread([this] { Loop(); });
}

MetricsServer::~MetricsServer() {
  stop_.store(true);
  if (thread_.joinable()) thread_.join();
  if (fd_ >= 0) ::close(fd_);
}

void MetricsServer::Loop() {
  while (!stop_.load()) {
    pollfd p{fd_, POLLIN, 0};
    if (::poll(&p, 1, 200) <= 0) continue;
    int c = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC);
    if (c < 0) continue;
    timeval tv{1, 0};
    ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    char buf[1024];
    ssize_t n = ::recv(c, buf, sizeof(buf) - 1, 0);
    std::string req(buf, n > 0 ? static_cast<size_t>(n) : 0);
    std::string status = "200 OK", body, type = "text/plain; charset=utf-8";
    if (req.rfind("GET /metrics", 0) == 0) {
      body = render_();
      type = "text/plain; version=0.0.4";
    } else if (req.rfind("GET /healthz", 0) == 0) {
      body = "ok\n";
    } else if (req.rfind("GET /readyz", 0) == 0) {
      if (ready_()) {
        body = "ready\n";
      } else {
        status = "503 Service Unavailable";
        body = "not ready\n";
      }
    } else {
      status = "404 Not Found";
      body = "not found\n";
    }
    std::string resp = "HTTP/1.0 " + status + "\r\nContent-Type: " + type +
                       "\r\nContent-Length: " + std::to_string(body.size()) +
                       "\r\nConnection: close\r\n\r\n" + body;
    for (size_t off = 0; off < resp.size();) {
      ssize_t w = ::send(c, resp.data() + off, resp.size() - off, MSG_NOSIGNAL);
      if (w <= 0) break;
      off += static_cast<size_t>(w);
    }
    ::close(c);
  }
}

}  // namespace kv::server
