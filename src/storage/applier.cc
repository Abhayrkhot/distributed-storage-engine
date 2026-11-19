#include "storage/applier.h"

#include <bit>

#include "storage/engine.h"
#include "storage/write_batch.h"

namespace kv {

Applier::Applier(MemTable* mem, int workers) : mem_(mem) {
  workers = std::max(1, std::min(workers, 64));
  for (int i = 0; i < workers; ++i) workers_.push_back(std::make_unique<Worker>());
  for (int i = 0; i < workers; ++i) {
    workers_[static_cast<size_t>(i)]->thread = std::thread([this, i] { WorkerLoop(i); });
  }
}

Applier::~Applier() {
  for (auto& w : workers_) {
    {
      std::lock_guard<std::mutex> l(w->mu);
      w->stop = true;
    }
    w->cv.notify_one();
  }
  for (auto& w : workers_) w->thread.join();
}

void Applier::Reset(uint64_t applied) {
  dispatched_ = applied;
  for (auto& w : workers_) w->progress.store(applied, std::memory_order_release);
}

void Applier::Submit(std::vector<Entry>&& entries) {
  if (entries.empty()) return;
  auto block = std::make_shared<std::vector<Entry>>(std::move(entries));
  const auto n = static_cast<uint32_t>(workers_.size());
  std::vector<std::vector<Task>> per(n);
  std::shared_ptr<std::vector<std::unique_ptr<Split>>> splits;

  for (Entry& e : *block) {
    if (!e.status.ok() || !e.apply) {
      per[e.index % n].push_back({&e, nullptr});
      continue;
    }
    uint64_t mask = 0;
    BatchReader r(e.payload);
    BatchOp op;
    while (r.Next(&op)) mask |= uint64_t{1} << (mem_->ShardOf(op.key) % n);
    if (!r.status().ok()) {
      e.status = r.status();
      per[e.index % n].push_back({&e, nullptr});
      continue;
    }
    const int parts = std::popcount(mask);
    if (parts <= 1) {
      per[parts ? static_cast<uint32_t>(std::countr_zero(mask)) : 0].push_back({&e, nullptr});
      continue;
    }
    if (!splits) splits = std::make_shared<std::vector<std::unique_ptr<Split>>>();
    auto s = std::make_unique<Split>();
    s->remaining.store(parts, std::memory_order_relaxed);
    for (uint64_t m = mask; m; m &= m - 1) {
      per[static_cast<uint32_t>(std::countr_zero(m))].push_back({&e, s.get()});
    }
    splits->push_back(std::move(s));
  }

  const uint64_t last = block->back().index;
  dispatched_ = last;
  bool advanced_directly = false;
  for (uint32_t w = 0; w < n; ++w) {
    Worker& wk = *workers_[w];
    bool notify = false;
    {
      std::lock_guard<std::mutex> l(wk.mu);
      if (per[w].empty() && wk.queue.empty() && !wk.busy) {
        // Nothing for this worker and it is caught up: advance it directly.
        wk.progress.store(last, std::memory_order_release);
        advanced_directly = true;
        continue;
      }
      wk.queue.push_back(Chunk{block, std::move(per[w]), splits, last});
      notify = !wk.busy;
    }
    if (notify) wk.cv.notify_one();
  }
  if (advanced_directly) {
    // Workers only signal after processing; a direct advance may be what a
    // WaitApplied() caller is waiting for.
    { std::lock_guard<std::mutex> l(idle_mu_); }
    idle_cv_.notify_all();
  }
}

void Applier::ApplyTask(int w, const Task& t) {
  Entry& e = *t.entry;
  if (e.status.ok() && e.apply) {
    const auto n = static_cast<uint32_t>(workers_.size());
    BatchReader r(e.payload);
    BatchOp op;
    while (r.Next(&op)) {
      if (t.split && mem_->ShardOf(op.key) % n != static_cast<uint32_t>(w)) continue;
      if (op.type == OpType::kPut) {
        mem_->Put(op.key, op.value);
      } else {
        mem_->Delete(op.key);
      }
    }
  }
  if (t.split && t.split->remaining.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
  if (e.done) e.done->Done(e.status, e.index);
}

void Applier::WorkerLoop(int w) {
  Worker& wk = *workers_[static_cast<size_t>(w)];
  std::vector<Chunk> local;
  for (;;) {
    {
      std::unique_lock<std::mutex> l(wk.mu);
      wk.busy = false;
      wk.cv.wait(l, [&] { return !wk.queue.empty() || wk.stop; });
      if (wk.queue.empty()) return;  // stop requested and drained
      local.swap(wk.queue);
      wk.busy = true;
    }
    for (auto& c : local) {
      for (const Task& t : c.tasks) ApplyTask(w, t);
      wk.progress.store(c.last_index, std::memory_order_release);
    }
    local.clear();
    { std::lock_guard<std::mutex> l(idle_mu_); }
    idle_cv_.notify_all();
  }
}

uint64_t Applier::AppliedIndex() const {
  uint64_t m = UINT64_MAX;
  for (const auto& w : workers_) m = std::min(m, w->progress.load(std::memory_order_acquire));
  return m;
}

void Applier::WaitApplied(uint64_t index) {
  std::unique_lock<std::mutex> l(idle_mu_);
  idle_cv_.wait(l, [&] { return AppliedIndex() >= index; });
}

bool Applier::WaitAppliedFor(uint64_t index, std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> l(idle_mu_);
  return idle_cv_.wait_for(l, timeout, [&] { return AppliedIndex() >= index; });
}

void Applier::RunQuiesced(const std::function<void(uint64_t)>& fn) {
  std::unique_lock<std::mutex> l(idle_mu_);
  idle_cv_.wait(l, [&] { return AppliedIndex() >= dispatched_; });
  fn(dispatched_);
}

}  // namespace kv
