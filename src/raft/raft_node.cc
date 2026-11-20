#include "raft/raft_node.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <sstream>

#include "absl/strings/str_cat.h"
#include "common/thread_name.h"
#include "common/fs.h"
#include "common/logging.h"
#include "storage/snapshot.h"
#include "storage/wal_reader.h"

namespace kv::raft {
namespace {

using std::chrono::milliseconds;

constexpr size_t kSnapshotChunk = 1 << 20;
constexpr int kAppendDeadlineMs = 2000;

}  // namespace

const char* RoleName(Role r) {
  switch (r) {
    case Role::kFollower:
      return "follower";
    case Role::kCandidate:
      return "candidate";
    case Role::kLeader:
      return "leader";
  }
  return "?";
}

RaftNode::RaftNode(const RaftOptions& options, Transport* transport)
    : opt_(options), transport_(transport),
      rng_(std::random_device{}() ^ (static_cast<uint64_t>(options.id) << 32)) {}

absl::StatusOr<std::unique_ptr<RaftNode>> RaftNode::Open(const RaftOptions& options,
                                                         Transport* transport) {
  if (options.members.count(options.id) == 0) {
    return absl::InvalidArgumentError("this node's id is not in the member list");
  }
  std::unique_ptr<RaftNode> n(new RaftNode(options, transport));
  if (auto st = n->Recover(); !st.ok()) return st;
  return n;
}

RaftNode::~RaftNode() {
  Shutdown();
  wal_.reset();
  sm_.reset();
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

absl::Status RaftNode::PersistHardState() {
  return WriteFileAtomic(opt_.dir + "/raft-state",
                         absl::StrCat(current_term_, " ", voted_for_, "\n"));
}

absl::Status RaftNode::Recover() {
  const auto t0 = Clock::now();
  const std::string snap_root = opt_.dir + "/snapshots";
  for (const auto& d : {opt_.dir, snap_root, opt_.dir + "/wal"}) {
    if (auto st = CreateDirs(d); !st.ok()) return st;
  }
  // Partially received snapshots from before a crash are useless.
  if (auto names = ListDir(opt_.dir); names.ok()) {
    for (const auto& n : *names) {
      if (n.rfind("incoming", 0) == 0) RemoveAll(opt_.dir + "/" + n).IgnoreError();
    }
  }
  if (FileExists(opt_.dir + "/raft-state")) {
    auto hs = ReadFile(opt_.dir + "/raft-state");
    if (!hs.ok()) return hs.status();
    std::istringstream in(*hs);
    if (!(in >> current_term_ >> voted_for_)) return absl::DataLossError("bad raft-state file");
  }
  int threads = opt_.recovery_threads > 0 ? opt_.recovery_threads
                                          : static_cast<int>(std::clamp(std::thread::hardware_concurrency(), 1u, 16u));
  sm_ = std::make_unique<StateMachine>(opt_.num_shards, opt_.apply_threads);

  uint64_t wal_start = 0;
  auto latest = FindLatestSnapshot(snap_root);
  if (!latest.ok()) return latest.status();
  if (latest->has_value()) {
    const auto& m = **latest;
    if (auto st = sm_->Restore(snap_root, m, threads); !st.ok()) return st;
    base_index_ = snap_index_ = m.index();
    base_term_ = snap_term_ = m.term();
    wal_start = m.wal_start_segment();
  }
  commit_index_ = dispatched_index_ = base_index_;

  std::vector<WalSegment> segments;
  WalReplayStats ws;
  auto st = ReplayWal(
      opt_.dir + "/wal", wal_start, base_index_,
      [&](SegmentContents& seg) -> absl::Status {
        for (const auto& e : seg.entries) {
          if (e.index <= base_index_) {
            // An overwrite at or below the snapshot index was written after
            // anything already replayed above it, so it truncated those
            // entries: they are a stale suffix from an old term.
            log_.clear();
            continue;
          }
          // A record at or below the current end overwrites the suffix: the
          // log was truncated by a new leader after the original was written.
          if (e.index <= LastIndex()) TruncateFrom(e.index);
          if (e.index != LastIndex() + 1) {
            return absl::DataLossError(absl::StrCat("raft log gap at ", e.index));
          }
          log_.push_back(LogEntry{e.term, std::make_shared<const std::string>(e.payload)});
        }
        return absl::OkStatus();
      },
      &segments, &ws);
  if (!st.ok()) return st;
  durable_index_ = LastIndex();
  // Entries up to the highest commit index stamped on any record are known
  // committed and can be applied without waiting for a leader.
  commit_index_ = std::max(base_index_, std::min(ws.max_commit, LastIndex()));
  recovered_commit_ = commit_index_;

  WalOptions wo = opt_.wal;
  wo.dir = opt_.dir + "/wal";
  auto wal = Wal::Open(wo, std::move(segments), LastIndex(),
                       [this](const absl::Status& s, std::vector<Wal::Record>&& g) {
                         OnWalDurable(s, std::move(g));
                       });
  if (!wal.ok()) return wal.status();
  wal_ = std::move(*wal);

  for (const auto& [id, addr] : opt_.members) {
    if (id == opt_.id) continue;
    auto p = std::make_unique<Peer>();
    p->id = id;
    peers_.push_back(std::move(p));
  }
  recovery_seconds_ = std::chrono::duration<double>(Clock::now() - t0).count();
  KV_LOG(Info) << "raft node " << opt_.id << " recovered: term " << current_term_ << ", snapshot@"
               << base_index_ << ", log " << base_index_ + 1 << ".." << LastIndex()
               << ", commit " << commit_index_ << " (" << recovery_seconds_ << "s)";
  return absl::OkStatus();
}

void RaftNode::Start() {
  std::lock_guard<std::mutex> l(mu_);
  if (started_) return;
  started_ = true;
  no_votes_until_ = Clock::now() + milliseconds(opt_.election_min_ms);
  ResetElectionTimer();
  for (auto& p : peers_) {
    Peer* raw = p.get();
    p->thread = std::thread([this, raw] { ReplicatorLoop(raw); });
    p->hb_thread = std::thread([this, raw] { HeartbeatLoop(raw); });
  }
  ticker_ = std::thread([this] { TickerLoop(); });
  dispatcher_ = std::thread([this] { DispatcherLoop(); });
  reader_ = std::thread([this] { ReaderLoop(); });
  snapshotter_ = std::thread([this] { SnapshotLoop(); });
  commit_cv_.notify_all();  // apply anything recovered as committed
}

void RaftNode::Shutdown() {
  {
    std::lock_guard<std::mutex> l(mu_);
    if (stopping_) return;
    stopping_ = true;
    FailPending(absl::UnavailableError("node shutting down"));
    for (auto& r : pending_reads_) r.done(absl::UnavailableError("node shutting down"));
    pending_reads_.clear();
  }
  repl_cv_.notify_all();
  hb_cv_.notify_all();
  commit_cv_.notify_all();
  durable_cv_.notify_all();
  state_cv_.notify_all();
  reader_cv_.notify_all();
  {
    std::unique_lock<std::mutex> l(calls_mu_);
    for (auto* ctx : calls_) ctx->TryCancel();
    calls_cv_.wait(l, [&] { return calls_.empty(); });
  }
  for (auto& p : peers_) {
    if (p->thread.joinable()) p->thread.join();
    if (p->hb_thread.joinable()) p->hb_thread.join();
  }
  for (auto* t : {&ticker_, &dispatcher_, &reader_, &snapshotter_}) {
    if (t->joinable()) t->join();
  }
  if (wal_) wal_->Flush();
}

// ---------------------------------------------------------------------------
// Log helpers
// ---------------------------------------------------------------------------

uint64_t RaftNode::TermAt(uint64_t index) const {
  if (index == base_index_) return base_term_;
  if (index < base_index_ || index > LastIndex()) return 0;
  return EntryAt(index).term;
}

void RaftNode::TruncateFrom(uint64_t index) {
  KV_CHECK(index > base_index_) << "truncating below the snapshot";
  KV_CHECK(index > commit_index_) << "truncating committed entry " << index;
  log_.resize(index - base_index_ - 1);
  if (durable_index_ >= index) durable_index_ = index - 1;
}

void RaftNode::CompactUpTo(uint64_t index) {
  index = std::min(index, LastIndex());
  if (index <= base_index_) return;
  const uint64_t term = TermAt(index);
  log_.erase(log_.begin(), log_.begin() + static_cast<long>(index - base_index_));
  base_index_ = index;
  base_term_ = term;
}

void RaftNode::AppendLocal(uint64_t term, std::shared_ptr<const std::string> data) {
  const uint64_t index = LastIndex() + 1;
  wal_->Append(Wal::Record{index, term, commit_index_, *data, nullptr});
  log_.push_back(LogEntry{term, std::move(data)});
}

void RaftNode::ResetElectionTimer() {
  std::uniform_int_distribution<int> d(opt_.election_min_ms, opt_.election_max_ms);
  election_deadline_ = Clock::now() + milliseconds(d(rng_));
}

// ---------------------------------------------------------------------------
// Roles and elections
// ---------------------------------------------------------------------------

void RaftNode::FailPending(const absl::Status& st) {
  for (auto& [index, p] : pending_) p.done->Done(st, index);
  pending_.clear();
}

void RaftNode::BecomeFollower(uint64_t term, uint32_t leader) {
  const bool was_leader = role_ == Role::kLeader;
  if (term > current_term_) {
    current_term_ = term;
    voted_for_ = 0;
    if (auto st = PersistHardState(); !st.ok()) KV_LOG(Fatal) << "persist term: " << st;
  }
  role_ = Role::kFollower;
  leader_id_ = leader;
  transferring_ = false;
  if (was_leader) {
    KV_LOG(Info) << "stepping down in term " << current_term_;
    FailPending(absl::UnavailableError(
        "leadership lost; the write may or may not commit, retry with the same session seq"));
    for (auto& r : pending_reads_) r.done(absl::UnavailableError("leadership lost"));
    pending_reads_.clear();
  }
  ResetElectionTimer();
  state_cv_.notify_all();
}

void RaftNode::BecomeLeader() {
  role_ = Role::kLeader;
  leader_id_ = opt_.id;
  ++elections_won_;
  leader_since_ = Clock::now();
  transferring_ = false;
  lease_until_ = {};
  for (auto& p : peers_) {
    p->next = LastIndex() + 1;
    p->match = 0;
    p->last_ack = leader_since_;
    p->next_heartbeat = leader_since_;
    p->acked_round = 0;
    p->sent_round = 0;
    p->hb_round = 0;
    p->timeout_now_sent = false;
  }
  // A no-op in the new term lets the leader commit (and learn the commit
  // point of) everything before it; reads wait for it too.
  AppendLocal(current_term_, std::make_shared<const std::string>(EncodeNoop()));
  leader_first_index_ = LastIndex();
  KV_LOG(Info) << "node " << opt_.id << " is leader for term " << current_term_ << " (log end "
               << LastIndex() << ")";
  repl_cv_.notify_all();
  hb_cv_.notify_all();
  state_cv_.notify_all();
  if (peers_.empty()) AdvanceCommit();
}

void RaftNode::StartElection(bool pre_vote, bool transfer) {
  ++election_id_;
  votes_ = 1;
  if (!pre_vote) {
    ++current_term_;
    voted_for_ = opt_.id;
    role_ = Role::kCandidate;
    leader_id_ = 0;
    if (auto st = PersistHardState(); !st.ok()) KV_LOG(Fatal) << "persist vote: " << st;
  }
  ResetElectionTimer();
  if (votes_ >= Majority()) {
    if (pre_vote) {
      StartElection(false, transfer);
    } else {
      BecomeLeader();
    }
    return;
  }
  VoteRequest req;
  req.set_term(pre_vote ? current_term_ + 1 : current_term_);
  req.set_candidate_id(opt_.id);
  req.set_last_log_index(LastIndex());
  req.set_last_log_term(LastTerm());
  req.set_pre_vote(pre_vote);
  req.set_transfer(transfer);
  const uint64_t eid = election_id_;
  for (auto& p : peers_) {
    if (transport_->IsBlocked(p->id)) continue;
    auto* stub = transport_->Stub(p->id);
    struct Call {
      grpc::ClientContext ctx;
      VoteRequest req;
      VoteResponse resp;
    };
    auto* call = new Call;
    call->req = req;
    call->ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(opt_.election_min_ms));
    {
      std::lock_guard<std::mutex> cl(calls_mu_);
      calls_.insert(&call->ctx);
    }
    // Issue the RPC from a short-lived thread so the callback can never run
    // inline while mu_ is held.
    std::thread([this, stub, call, eid, pre_vote] {
      stub->async()->RequestVote(
          &call->ctx, &call->req, &call->resp, [this, call, eid, pre_vote](grpc::Status s) {
            if (s.ok()) {
              std::lock_guard<std::mutex> l(mu_);
              OnVoteReply(eid, pre_vote, call->resp.term(), call->resp.granted());
            }
            {
              std::lock_guard<std::mutex> cl(calls_mu_);
              calls_.erase(&call->ctx);
            }
            calls_cv_.notify_all();
            delete call;
          });
    }).detach();
  }
}

void RaftNode::OnVoteReply(uint64_t election, bool pre_vote, uint64_t term, bool granted) {
  if (stopping_) return;
  if (!granted && term > current_term_) {
    BecomeFollower(term, 0);
    return;
  }
  if (election != election_id_) return;
  if (pre_vote ? role_ == Role::kLeader : (role_ != Role::kCandidate || term != current_term_)) {
    return;
  }
  if (!granted) return;
  if (++votes_ >= Majority()) {
    if (pre_vote) {
      StartElection(false, false);
    } else {
      BecomeLeader();
    }
  }
}

grpc::Status RaftNode::HandleRequestVote(const VoteRequest& req, VoteResponse* resp) {
  if (transport_->IsBlocked(req.candidate_id())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "partitioned (fault injection)");
  }
  std::lock_guard<std::mutex> l(mu_);
  resp->set_term(current_term_);
  resp->set_granted(false);
  if (stopping_) return grpc::Status(grpc::StatusCode::UNAVAILABLE, "stopping");
  // Leader stickiness (Raft thesis 4.2.3): while we hear from a live leader,
  // refuse to help unseat it, unless the leader itself asked (transfer).
  const auto now = Clock::now();
  const bool leader_alive =
      role_ == Role::kLeader ||
      (leader_id_ != 0 && now - last_heard_ < milliseconds(opt_.election_min_ms));
  // After a restart we do not know whether we recently acknowledged a leader
  // whose lease is still running, so sit out one minimum election timeout.
  if ((leader_alive || now < no_votes_until_) && !req.transfer()) return grpc::Status::OK;
  const bool up_to_date = req.last_log_term() > LastTerm() ||
                          (req.last_log_term() == LastTerm() && req.last_log_index() >= LastIndex());
  if (req.pre_vote()) {
    resp->set_granted(req.term() > current_term_ && up_to_date);
    return grpc::Status::OK;
  }
  if (req.term() < current_term_) return grpc::Status::OK;
  if (req.term() > current_term_) BecomeFollower(req.term(), 0);
  if ((voted_for_ == 0 || voted_for_ == req.candidate_id()) && up_to_date) {
    voted_for_ = req.candidate_id();
    if (auto st = PersistHardState(); !st.ok()) KV_LOG(Fatal) << "persist vote: " << st;
    resp->set_granted(true);
    ResetElectionTimer();
  }
  resp->set_term(current_term_);
  return grpc::Status::OK;
}

grpc::Status RaftNode::HandleTimeoutNow(const TimeoutNowRequest& req, TimeoutNowResponse* resp) {
  if (transport_->IsBlocked(req.leader_id())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "partitioned (fault injection)");
  }
  std::lock_guard<std::mutex> l(mu_);
  resp->set_term(current_term_);
  if (stopping_ || req.term() != current_term_ || role_ == Role::kLeader ||
      req.leader_id() != leader_id_) {
    return grpc::Status::OK;
  }
  KV_LOG(Info) << "leadership transfer: starting election at the leader's request";
  StartElection(false, true);
  return grpc::Status::OK;
}

void RaftNode::TickerLoop() {
  SetThreadName("raft-ticker");
  std::unique_lock<std::mutex> l(mu_);
  while (!stopping_) {
    const auto now = Clock::now();
    if (role_ == Role::kLeader) {
      // CheckQuorum only decides how soon a cut-off leader gives up (lease
      // safety does not depend on it), so allow two election timeouts before
      // concluding a busy follower is gone.
      const auto window = milliseconds(2 * opt_.election_max_ms);
      if (opt_.check_quorum && now - leader_since_ > window) {
        size_t live = 1;
        for (auto& p : peers_) {
          if (now - p->last_ack <= window) ++live;
        }
        if (live < Majority()) {
          KV_LOG(Warning) << "CheckQuorum: heard from " << live << "/" << opt_.members.size()
                          << " recently; stepping down";
          BecomeFollower(current_term_, 0);
        }
      }
      if (transferring_ && now > transfer_deadline_) {
        transferring_ = false;
        state_cv_.notify_all();
      }
    } else if (started_ && now >= election_deadline_) {
      StartElection(/*pre_vote=*/true, /*transfer=*/false);
    }
    state_cv_.wait_for(l, milliseconds(5));
  }
}

// ---------------------------------------------------------------------------
// Replication (leader side)
// ---------------------------------------------------------------------------

void RaftNode::AdvanceCommit() {
  if (role_ != Role::kLeader) return;
  std::vector<uint64_t> m;
  m.reserve(peers_.size() + 1);
  m.push_back(durable_index_);
  for (auto& p : peers_) m.push_back(p->match);
  std::sort(m.begin(), m.end(), std::greater<>());
  const uint64_t n = m[Majority() - 1];
  // Only entries from the current term commit by counting (Raft 5.4.2).
  if (n > commit_index_ && TermAt(n) == current_term_) {
    commit_index_ = n;
    commit_cv_.notify_one();
  }
}

void RaftNode::UpdateLeaseAndReads() {
  if (role_ != Role::kLeader) return;
  const auto now = Clock::now();
  std::vector<Clock::time_point> acks{now};
  std::vector<uint64_t> rounds{read_round_};
  for (auto& p : peers_) {
    acks.push_back(p->last_ack);
    rounds.push_back(p->acked_round);
  }
  std::sort(acks.begin(), acks.end(), std::greater<>());
  std::sort(rounds.begin(), rounds.end(), std::greater<>());
  // A quorum answered requests sent at or after acks[k]; nobody else can win
  // an election until at least election_min after that (stickiness), so the
  // lease runs to that point minus a margin for clock drift.
  const auto lease = acks[Majority() - 1] + milliseconds(opt_.election_min_ms * 8 / 10);
  if (lease > lease_until_) lease_until_ = lease;
  const uint64_t quorum_round = rounds[Majority() - 1];
  bool any = false;
  while (!pending_reads_.empty() && pending_reads_.front().round <= quorum_round) {
    confirmed_reads_.push_back(std::move(pending_reads_.front()));
    pending_reads_.pop_front();
    any = true;
  }
  if (any) reader_cv_.notify_one();
}

void RaftNode::ReplicatorLoop(Peer* p) {
  SetThreadName("raft-repl-" + std::to_string(p->id));
  auto* stub = transport_->Stub(p->id);
  const auto hb = milliseconds(opt_.heartbeat_ms);
  Clock::time_point retry_at{};
  std::unique_lock<std::mutex> l(mu_);
  while (!stopping_) {
    if (role_ != Role::kLeader) {
      repl_cv_.wait(l);
      retry_at = {};
      continue;
    }
    const auto now = Clock::now();
    if (now < retry_at) {
      repl_cv_.wait_until(l, retry_at);
      continue;
    }
    const bool has_entries = p->next <= LastIndex();
    const bool transfer_ping = transferring_ && transfer_target_ == p->id &&
                               !p->timeout_now_sent && p->match == LastIndex();
    if (!has_entries && !transfer_ping) {
      // Heartbeats are the heartbeat thread's job; wait for entries.
      ++repl_idle_;
      repl_cv_.wait_until(l, now + hb);
      --repl_idle_;
      continue;
    }
    const uint64_t term = current_term_;
    if (transport_->IsBlocked(p->id)) {
      p->reachable = false;
      retry_at = now + hb;
      continue;
    }
    if (transfer_ping) {
      p->timeout_now_sent = true;
      l.unlock();
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(opt_.election_min_ms));
      TimeoutNowRequest req;
      req.set_term(term);
      req.set_leader_id(opt_.id);
      TimeoutNowResponse resp;
      stub->TimeoutNow(&ctx, req, &resp);
      l.lock();
      continue;
    }
    if (p->next <= base_index_) {
      l.unlock();
      const bool sent = SendSnapshot(p, term);
      l.lock();
      // Don't hammer an unreachable or failing follower with snapshots.
      if (!sent) retry_at = Clock::now() + milliseconds(500);
      continue;
    }

    AppendRequest req;
    req.set_term(term);
    req.set_leader_id(opt_.id);
    const uint64_t prev = p->next - 1;
    req.set_prev_log_index(prev);
    req.set_prev_log_term(TermAt(prev));
    std::vector<std::pair<uint64_t, LogEntry>> batch;
    size_t bytes = 0;
    for (uint64_t i = p->next; i <= LastIndex() && batch.size() < opt_.max_append_entries &&
                               bytes < opt_.max_append_bytes;
         ++i) {
      const LogEntry& e = EntryAt(i);
      bytes += e.data->size();
      batch.emplace_back(i, e);
    }
    req.set_leader_commit(commit_index_);
    req.set_round(read_round_);
    p->sent_round = read_round_;
    p->next_heartbeat = now + hb;
    l.unlock();

    // Serialize outside the lock: the entries are immutable shared buffers.
    for (const auto& [index, e] : batch) {
      Entry* pe = req.add_entries();
      pe->set_index(index);
      pe->set_term(e.term);
      pe->set_data(*e.data);
    }
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(kAppendDeadlineMs));
    AppendResponse resp;
    grpc::Status s = stub->AppendEntries(&ctx, req, &resp);

    l.lock();
    if (stopping_ || current_term_ != term || role_ != Role::kLeader) continue;
    if (!s.ok()) {
      if (p->reachable) KV_LOG(Debug) << "append to " << p->id << " failed: " << s.error_message();
      p->reachable = false;
      retry_at = Clock::now() + hb;
      continue;
    }
    p->reachable = true;
    if (resp.term() > current_term_) {
      BecomeFollower(resp.term(), 0);
      continue;
    }
    p->last_ack = std::max(p->last_ack, now);
    p->acked_round = std::max(p->acked_round, resp.round());
    if (resp.success()) {
      p->match = std::max(p->match, resp.match_index());
      p->next = std::max(p->next, p->match + 1);
      AdvanceCommit();
    } else {
      uint64_t next = resp.conflict_index();
      if (resp.conflict_term() != 0) {
        for (uint64_t i = std::min(prev, LastIndex()); i > base_index_; --i) {
          const uint64_t t = TermAt(i);
          if (t == resp.conflict_term()) {
            next = i + 1;
            break;
          }
          if (t < resp.conflict_term()) break;
        }
      }
      p->next = std::max(p->match + 1, std::min(next, LastIndex() + 1));
    }
    UpdateLeaseAndReads();
  }
}

// Heartbeats travel separately from log entries, so neither a multi-megabyte
// batch nor a slow fsync on the follower delays them. prev is the follower's
// known match index (durable there), so the follower answers at once and
// learns the commit index up to what it holds.
void RaftNode::HeartbeatLoop(Peer* p) {
  SetThreadName("raft-hb-" + std::to_string(p->id));
  auto* stub = transport_->Stub(p->id);
  const auto hb = milliseconds(opt_.heartbeat_ms);
  Clock::time_point next{};
  std::unique_lock<std::mutex> l(mu_);
  while (!stopping_) {
    if (role_ != Role::kLeader) {
      hb_cv_.wait(l);
      next = {};
      continue;
    }
    const auto now = Clock::now();
    if (now < next && read_round_ <= p->hb_round) {
      hb_cv_.wait_until(l, next);
      continue;
    }
    next = now + hb;
    if (transport_->IsBlocked(p->id)) continue;
    const uint64_t term = current_term_;
    AppendRequest req;
    req.set_term(term);
    req.set_leader_id(opt_.id);
    const uint64_t prev = p->match >= base_index_ ? p->match : 0;
    req.set_prev_log_index(prev);
    req.set_prev_log_term(TermAt(prev));
    req.set_leader_commit(commit_index_);
    req.set_round(read_round_);
    p->hb_round = read_round_;
    l.unlock();
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(opt_.election_min_ms));
    AppendResponse resp;
    grpc::Status s = stub->AppendEntries(&ctx, req, &resp);
    l.lock();
    if (stopping_ || current_term_ != term || role_ != Role::kLeader || !s.ok()) continue;
    if (resp.term() > current_term_) {
      BecomeFollower(resp.term(), 0);
      continue;
    }
    // Any reply in our term (even a log mismatch) means the follower accepts
    // this node as leader: that is what CheckQuorum, the lease and ReadIndex
    // need.
    p->reachable = true;
    p->last_ack = std::max(p->last_ack, now);
    p->acked_round = std::max(p->acked_round, resp.round());
    UpdateLeaseAndReads();
  }
}

bool RaftNode::SendSnapshot(Peer* p, uint64_t term) {
  uint64_t index, sterm;
  {
    std::lock_guard<std::mutex> l(mu_);
    index = snap_index_;
    sterm = snap_term_;
  }
  const std::string dir = opt_.dir + "/snapshots/" + SnapshotDirName(index);
  auto manifest_bytes = ReadFile(dir + "/MANIFEST");
  auto manifest = ReadManifest(dir);
  auto fail = [&](const std::string& why) {
    std::lock_guard<std::mutex> l(mu_);
    if (p->reachable) KV_LOG(Warning) << "snapshot " << index << " to " << p->id << " failed: " << why;
    p->reachable = false;
    return false;
  };
  auto still_leader = [&] {
    std::lock_guard<std::mutex> l(mu_);
    return !stopping_ && current_term_ == term && role_ == Role::kLeader;
  };
  // The lease may only count from when the follower last confirmed this
  // leader; the stream start is conservative.
  const auto started = Clock::now();
  if (index == 0 || !manifest_bytes.ok() || !manifest.ok()) return fail("no snapshot on disk");

  KV_LOG(Info) << "sending snapshot @" << index << " to node " << p->id;
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::minutes(10));
  InstallSnapshotResponse resp;
  auto stub = transport_->Stub(p->id);
  auto writer = stub->InstallSnapshot(&ctx, &resp);
  SnapshotChunk first;
  first.set_term(term);
  first.set_leader_id(opt_.id);
  first.set_last_index(index);
  first.set_last_term(sterm);
  first.set_manifest(*manifest_bytes);
  bool ok = writer->Write(first);
  std::string buf(kSnapshotChunk, '\0');
  for (int i = 0; ok && i < manifest->files_size(); ++i) {
    const auto& f = manifest->files(i);
    int fd = ::open((dir + "/" + f.name()).c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
      ok = false;
      break;
    }
    for (uint64_t off = 0; ok && off < f.size();) {
      ssize_t n = ::pread(fd, buf.data(), buf.size(), static_cast<off_t>(off));
      if (n <= 0) {
        ok = false;
        break;
      }
      SnapshotChunk c;
      c.set_file(f.name());
      c.set_offset(off);
      c.set_data(buf.data(), static_cast<size_t>(n));
      ok = writer->Write(c);
      off += static_cast<uint64_t>(n);
      if (ok && (off / kSnapshotChunk) % 16 == 0) ok = still_leader();  // deposed: stop streaming
    }
    ::close(fd);
  }
  writer->WritesDone();
  grpc::Status s = writer->Finish();
  if (!ok || !s.ok()) return fail(s.ok() ? "stream write failed" : s.error_message());

  std::lock_guard<std::mutex> l(mu_);
  p->reachable = true;
  if (current_term_ != term || role_ != Role::kLeader) return true;
  if (resp.term() > current_term_) {
    BecomeFollower(resp.term(), 0);
    return true;
  }
  if (resp.success()) {
    p->match = std::max(p->match, index);
    p->next = std::max(p->next, index + 1);
    p->last_ack = std::max(p->last_ack, started);
    AdvanceCommit();
  }
  KV_LOG(Info) << "snapshot @" << index << " delivered to node " << p->id;
  return resp.success();
}

// ---------------------------------------------------------------------------
// Replication (follower side)
// ---------------------------------------------------------------------------

grpc::Status RaftNode::HandleAppendEntries(const AppendRequest& req, AppendResponse* resp) {
  if (transport_->IsBlocked(req.leader_id())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "partitioned (fault injection)");
  }
  std::unique_lock<std::mutex> l(mu_);
  resp->set_term(current_term_);
  resp->set_success(false);
  resp->set_round(req.round());
  if (stopping_ || installing_) return grpc::Status(grpc::StatusCode::UNAVAILABLE, "busy");
  if (req.term() < current_term_) return grpc::Status::OK;
  if (req.term() > current_term_ || role_ != Role::kFollower) {
    BecomeFollower(req.term(), req.leader_id());
  }
  if (leader_id_ != req.leader_id()) {
    leader_id_ = req.leader_id();
    state_cv_.notify_all();  // e.g. a TransferLeadership() caller waiting to learn the new leader
  }
  last_heard_ = Clock::now();
  ++election_id_;  // late PreVote grants must not start an election now
  ResetElectionTimer();
  resp->set_term(current_term_);

  uint64_t prev = req.prev_log_index();
  if (prev > LastIndex()) {
    resp->set_conflict_index(LastIndex() + 1);
    resp->set_last_log_index(LastIndex());
    return grpc::Status::OK;
  }
  if (prev >= base_index_ && TermAt(prev) != req.prev_log_term()) {
    const uint64_t ct = TermAt(prev);
    uint64_t i = prev;
    while (i > base_index_ + 1 && TermAt(i - 1) == ct) --i;
    resp->set_conflict_term(ct);
    resp->set_conflict_index(i);
    resp->set_last_log_index(LastIndex());
    return grpc::Status::OK;
  }
  // prev < base_index_: everything up to our snapshot is committed and so
  // matches the leader; skip those entries below.
  const uint64_t last_new = prev + static_cast<uint64_t>(req.entries_size());
  const uint64_t stamp = std::max(commit_index_, std::min(req.leader_commit(), last_new));
  for (const Entry& e : req.entries()) {
    if (e.index() <= base_index_) continue;
    if (e.index() <= LastIndex()) {
      if (TermAt(e.index()) == e.term()) continue;
      TruncateFrom(e.index());
    }
    auto data = std::make_shared<const std::string>(e.data());
    wal_->Append(Wal::Record{e.index(), e.term(), stamp, *data, nullptr});
    log_.push_back(LogEntry{e.term(), std::move(data)});
  }
  const uint64_t new_commit = std::min(req.leader_commit(), last_new);
  if (new_commit > commit_index_) {
    commit_index_ = new_commit;
    commit_cv_.notify_one();
  }
  // Acknowledge only what is durable: the leader counts this reply toward
  // the commit quorum.
  const uint64_t target = std::max(last_new, base_index_);
  const uint64_t term = current_term_;
  durable_cv_.wait(l, [&] { return durable_index_ >= target || stopping_ || current_term_ != term; });
  if (stopping_ || current_term_ != term) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "term changed");
  }
  resp->set_success(true);
  resp->set_match_index(target);
  resp->set_last_log_index(LastIndex());
  return grpc::Status::OK;
}

void RaftNode::OnWalDurable(const absl::Status& st, std::vector<Wal::Record>&& group) {
  // A Raft node that cannot persist must stop: retrying fsync after a failure
  // can silently drop data (fsyncgate), and acknowledging would be a lie.
  if (!st.ok()) KV_LOG(Fatal) << "WAL write failed; stopping: " << st;
  if (group.empty()) return;
  std::lock_guard<std::mutex> l(mu_);
  const auto& last = group.back();
  // The WAL is FIFO, so everything appended before this record is durable
  // too. If the record was truncated away since, wait for a later group.
  if (last.index > durable_index_ && TermAt(last.index) == last.term) durable_index_ = last.index;
  AdvanceCommit();
  durable_cv_.notify_all();
}

grpc::Status RaftNode::HandleInstallSnapshot(grpc::ServerReader<SnapshotChunk>* reader,
                                             InstallSnapshotResponse* resp) {
  SnapshotChunk chunk;
  if (!reader->Read(&chunk)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty stream");
  if (transport_->IsBlocked(chunk.leader_id())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "partitioned (fault injection)");
  }
  const uint64_t index = chunk.last_index(), lterm = chunk.last_term();
  {
    std::lock_guard<std::mutex> l(mu_);
    resp->set_term(current_term_);
    resp->set_success(false);
    if (stopping_) return grpc::Status(grpc::StatusCode::UNAVAILABLE, "stopping");
    if (chunk.term() < current_term_) return grpc::Status::OK;
    if (chunk.term() > current_term_ || role_ != Role::kFollower) {
      BecomeFollower(chunk.term(), chunk.leader_id());
    }
    leader_id_ = chunk.leader_id();
    last_heard_ = Clock::now();
    ++election_id_;
    ResetElectionTimer();
    resp->set_term(current_term_);
  }
  KV_LOG(Info) << "receiving snapshot @" << index << " from node " << chunk.leader_id();
  const uint64_t term = chunk.term();
  // One install at a time; a stream from a deposed leader notices the newer
  // term below and gives up, so it cannot hold this for long.
  std::lock_guard<std::mutex> install(install_mu_);
  // A private directory per stream: nothing else can write into it, and a
  // leftover from a crash is removed at startup.
  const std::string incoming =
      absl::StrCat(opt_.dir, "/incoming-", index, "-", term, "-", ++install_seq_);
  struct Cleanup {
    std::string dir;
    ~Cleanup() {
      if (!dir.empty()) RemoveAll(dir).IgnoreError();
    }
  } cleanup{incoming};
  if (auto st = CreateDirs(incoming); !st.ok()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, std::string(st.message()));
  }
  if (auto st = WriteFileAtomic(incoming + "/MANIFEST", chunk.manifest()); !st.ok()) {
    return grpc::Status(grpc::StatusCode::INTERNAL, std::string(st.message()));
  }
  auto manifest = ReadManifest(incoming);
  if (!manifest.ok() || manifest->index() != index) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "bad snapshot manifest");
  }
  std::map<std::string, int> fds;
  for (const auto& f : manifest->files()) {
    if (f.name().empty() || f.name().find('/') != std::string::npos || f.name() == "MANIFEST") {
      for (auto& [n, fd] : fds) ::close(fd);
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "bad file name");
    }
    int fd = ::open((incoming + "/" + f.name()).c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
      for (auto& [n, fd2] : fds) ::close(fd2);
      return grpc::Status(grpc::StatusCode::INTERNAL, "open snapshot file");
    }
    fds[f.name()] = fd;
  }
  bool ok = true;
  auto last_touch = Clock::now();
  while (ok && reader->Read(&chunk)) {
    auto it = fds.find(chunk.file());
    if (it == fds.end()) {
      ok = false;
      break;
    }
    ok = PWriteAll(it->second, chunk.data().data(), chunk.data().size(), chunk.offset()).ok();
    if (Clock::now() - last_touch >= milliseconds(opt_.heartbeat_ms)) {
      // Still hearing from the leader: keep the election timer and leader
      // stickiness fresh, and abandon the stream if a newer term appeared.
      last_touch = Clock::now();
      std::lock_guard<std::mutex> l(mu_);
      if (current_term_ != term || stopping_) {
        ok = false;
        break;
      }
      last_heard_ = last_touch;
      ++election_id_;
      ResetElectionTimer();
    }
  }
  for (auto& [name, fd] : fds) {
    if (::fdatasync(fd) != 0) ok = false;
    ::close(fd);
  }
  if (!ok || !VerifySnapshotDir(incoming, *manifest).ok()) {
    std::lock_guard<std::mutex> l(mu_);
    resp->set_term(current_term_);
    return grpc::Status(grpc::StatusCode::DATA_LOSS, "snapshot transfer incomplete, stale or corrupt");
  }

  // Install. Lock order: dispatch_mu_ (pauses the dispatcher) before mu_.
  std::lock_guard<std::mutex> d(dispatch_mu_);
  bool keep_suffix;
  {
    std::unique_lock<std::mutex> l(mu_);
    state_cv_.wait(l, [&] { return !snap_in_progress_ || stopping_; });
    if (stopping_) return grpc::Status(grpc::StatusCode::UNAVAILABLE, "stopping");
    resp->set_term(current_term_);
    if (current_term_ != term) return grpc::Status::OK;  // a newer leader exists
    if (index <= dispatched_index_) {  // already have this state
      resp->set_success(true);
      return grpc::Status::OK;
    }
    installing_ = true;
    // Raft: if we hold the snapshot's last entry, keep the log after it.
    keep_suffix = index > base_index_ && index <= LastIndex() && TermAt(index) == lterm;
  }
  uint64_t start_seg;
  if (keep_suffix) {
    start_seg = wal_->FirstSegmentAbove(index);
  } else {
    // The log is discarded. New writes go to a fresh segment, and the
    // snapshot's manifest tells recovery to ignore everything older, so a
    // crash between here and DropBefore cannot resurrect the stale log.
    auto seg = wal_->Reset(index);
    if (!seg.ok()) KV_LOG(Fatal) << "WAL reset failed: " << seg.status();
    start_seg = *seg;
  }
  snapshot::SnapshotManifest m = *manifest;
  m.set_wal_start_segment(start_seg);
  const std::string root = opt_.dir + "/snapshots";
  const std::string final_dir = root + "/" + SnapshotDirName(index);
  absl::Status st = WriteManifest(incoming, m);
  if (st.ok()) st = SyncDir(incoming);
  if (st.ok()) st = RemoveAll(final_dir);
  if (st.ok()) st = RenameFile(incoming, final_dir);
  if (st.ok()) st = SyncDir(root);
  if (!st.ok()) KV_LOG(Fatal) << "installing snapshot failed: " << st;
  cleanup.dir.clear();  // renamed into place
  if (!keep_suffix) wal_->DropBefore(start_seg);
  const int threads = opt_.recovery_threads > 0 ? opt_.recovery_threads
                                                : static_cast<int>(std::clamp(std::thread::hardware_concurrency(), 1u, 16u));
  if (auto s = sm_->Restore(root, m, threads); !s.ok()) KV_LOG(Fatal) << "restore: " << s;
  PruneSnapshots(root, 2);
  {
    std::lock_guard<std::mutex> l(mu_);
    if (keep_suffix) {
      CompactUpTo(index);
    } else {
      log_.clear();
      base_index_ = index;
      base_term_ = lterm;
      durable_index_ = index;
    }
    commit_index_ = std::max(commit_index_, index);
    dispatched_index_ = index;
    snap_index_ = index;
    snap_term_ = lterm;
    bytes_since_snapshot_ = 0;
    installing_ = false;
    last_heard_ = Clock::now();
    ResetElectionTimer();
    resp->set_term(current_term_);
    commit_cv_.notify_one();
    state_cv_.notify_all();
  }
  KV_LOG(Info) << "installed snapshot @" << index << " (" << m.key_count() << " keys)";
  resp->set_success(true);
  return grpc::Status::OK;
}

// ---------------------------------------------------------------------------
// Client operations
// ---------------------------------------------------------------------------

std::string RaftNode::LeaderHint() const {
  std::lock_guard<std::mutex> l(mu_);
  if (leader_id_ == 0) return "";
  return absl::StrCat(leader_id_, "@", opt_.members.at(leader_id_));
}

void RaftNode::Propose(uint64_t client_id, uint64_t seq, std::string batch_rep,
                       WriteCompletion* done) {
  auto data = std::make_shared<const std::string>(EncodeWrite(client_id, seq, batch_rep));
  std::unique_lock<std::mutex> l(mu_);
  if (role_ != Role::kLeader || transferring_ || stopping_) {
    const std::string hint =
        leader_id_ != 0 && leader_id_ != opt_.id ? absl::StrCat(leader_id_, "@", opt_.members.at(leader_id_)) : "";
    l.unlock();
    done->Done(absl::UnavailableError(absl::StrCat("not leader; leader=", hint)), 0);
    return;
  }
  AppendLocal(current_term_, std::move(data));
  pending_[LastIndex()] = Pending{current_term_, done};
  // Busy replicators pick the entry up when their current RPC returns; only
  // parked ones need a wakeup.
  if (repl_idle_ > 0) repl_cv_.notify_all();
}

void RaftNode::ReadBarrier(ReadMode mode, std::function<void(const absl::Status&)> done) {
  if (mode == ReadMode::kStale) {
    done(absl::OkStatus());
    return;
  }
  std::unique_lock<std::mutex> l(mu_);
  if (role_ != Role::kLeader || stopping_) {
    const std::string hint =
        leader_id_ != 0 && leader_id_ != opt_.id ? absl::StrCat(leader_id_, "@", opt_.members.at(leader_id_)) : "";
    l.unlock();
    done(absl::UnavailableError(absl::StrCat("not leader; leader=", hint)));
    return;
  }
  // The read must observe every write committed before it arrived, including
  // ones committed by earlier leaders: wait for this term's no-op as well.
  const uint64_t read_index = std::max(commit_index_, leader_first_index_);
  const bool lease_ok = mode == ReadMode::kLease && opt_.check_quorum && !transferring_ &&
                        Clock::now() < lease_until_;
  if (lease_ok || peers_.empty()) {
    if (sm_->AppliedIndex() >= read_index) {
      l.unlock();
      done(absl::OkStatus());
      return;
    }
    confirmed_reads_.push_back(ReadWaiter{0, read_index, std::move(done)});
    reader_cv_.notify_one();
    return;
  }
  // ReadIndex: confirm leadership with a heartbeat round that a quorum
  // answers after this point. Concurrent reads share rounds.
  const bool idle = pending_reads_.empty();
  pending_reads_.push_back(ReadWaiter{++read_round_, read_index, std::move(done)});
  if (idle) hb_cv_.notify_all();
}

void RaftNode::ReaderLoop() {
  SetThreadName("raft-reader");
  std::deque<ReadWaiter> todo;
  std::unique_lock<std::mutex> l(mu_);
  for (;;) {
    reader_cv_.wait(l, [&] { return stopping_ || !confirmed_reads_.empty(); });
    todo.swap(confirmed_reads_);
    const bool stop = stopping_;
    l.unlock();
    for (auto& r : todo) {
      bool ready = false;
      while (!(ready = sm_->WaitAppliedFor(r.read_index, milliseconds(50)))) {
        std::lock_guard<std::mutex> g(mu_);
        if (stopping_) break;
      }
      r.done(ready ? absl::OkStatus() : absl::UnavailableError("node shutting down"));
    }
    todo.clear();
    l.lock();
    if (stop && confirmed_reads_.empty()) return;
  }
}

absl::Status RaftNode::TransferLeadership(uint32_t target, uint32_t* new_leader) {
  std::unique_lock<std::mutex> l(mu_);
  if (role_ != Role::kLeader) return absl::FailedPreconditionError("not leader");
  if (peers_.empty()) return absl::FailedPreconditionError("single-node cluster");
  Peer* tp = nullptr;
  for (auto& p : peers_) {
    if (target != 0 ? p->id == target : (!tp || (p->reachable && p->match > tp->match))) tp = p.get();
  }
  if (!tp) return absl::InvalidArgumentError("unknown target");
  transferring_ = true;
  lease_until_ = {};  // the target may win without our stickiness: no lease reads
  transfer_target_ = tp->id;
  tp->timeout_now_sent = false;
  transfer_deadline_ = Clock::now() + milliseconds(opt_.election_max_ms * 2);
  repl_cv_.notify_all();
  state_cv_.wait_until(l, transfer_deadline_, [&] { return role_ != Role::kLeader || !transferring_; });
  if (role_ == Role::kLeader) {
    transferring_ = false;
    return absl::DeadlineExceededError("leadership transfer timed out");
  }
  state_cv_.wait_for(l, milliseconds(opt_.election_max_ms), [&] { return leader_id_ != 0; });
  if (new_leader) *new_leader = leader_id_;
  return absl::OkStatus();
}

// ---------------------------------------------------------------------------
// Applying committed entries and snapshots
// ---------------------------------------------------------------------------

void RaftNode::DispatcherLoop() {
  SetThreadName("raft-dispatch");
  std::vector<CommittedEntry> batch;
  for (;;) {
    {
      std::unique_lock<std::mutex> l(mu_);
      commit_cv_.wait(l, [&] {
        return stopping_ || (!installing_ && (commit_index_ > dispatched_index_ ||
                                              (snap_forced_ && !snap_in_progress_)));
      });
      if (stopping_) return;
    }
    std::lock_guard<std::mutex> d(dispatch_mu_);
    bool snap_due = false;
    {
      std::lock_guard<std::mutex> l(mu_);
      if (installing_) continue;
      const uint64_t to = std::min(commit_index_, dispatched_index_ + 65536);
      for (uint64_t i = dispatched_index_ + 1; i <= to; ++i) {
        const LogEntry& e = EntryAt(i);
        CommittedEntry c{i, e.data, nullptr};
        auto it = pending_.find(i);
        if (it != pending_.end() && it->second.term == e.term) {
          c.done = it->second.done;
          pending_.erase(it);
        }
        bytes_since_snapshot_ += e.data->size();
        batch.push_back(std::move(c));
      }
      dispatched_index_ = to;
      snap_due = !snap_in_progress_ &&
                 (snap_forced_ || (opt_.snapshot_trigger_bytes > 0 &&
                                   bytes_since_snapshot_ >= opt_.snapshot_trigger_bytes));
    }
    sm_->Apply(batch);
    batch.clear();
    if (snap_due) StartLocalSnapshot();
  }
}

void RaftNode::StartLocalSnapshot() {
  uint64_t index;
  std::string app_state;
  sm_->BeginSnapshot(&index, &app_state);
  std::lock_guard<std::mutex> l(mu_);
  snap_forced_ = false;
  bytes_since_snapshot_ = 0;
  if (index == 0 || index <= snap_index_) {
    // Nothing applied since the last snapshot: nothing to write.
    sm_->memtable().AbortSnapshot();
    ++snapshots_done_;
    last_snapshot_status_ = absl::OkStatus();
    state_cv_.notify_all();
    return;
  }
  snap_in_progress_ = true;
  snap_requested_ = true;
  snap_pending_index_ = index;
  snap_pending_term_ = TermAt(index);
  snap_pending_state_ = std::move(app_state);
  state_cv_.notify_all();
}

void RaftNode::SnapshotLoop() {
  SetThreadName("raft-snapshot");
  std::unique_lock<std::mutex> l(mu_);
  for (;;) {
    state_cv_.wait(l, [&] { return stopping_ || snap_requested_; });
    if (!snap_requested_) return;  // stopping, nothing in flight
    snap_requested_ = false;
    SnapshotWriteOptions so;
    so.index = snap_pending_index_;
    so.term = snap_pending_term_;
    so.app_state = std::move(snap_pending_state_);
    so.files = 16;
    so.threads = opt_.snapshot_threads;
    so.rate_limit_bytes_per_s = opt_.snapshot_rate_bytes_per_s;
    l.unlock();
    so.wal_start_segment = wal_->FirstSegmentAbove(so.index);
    auto m = WriteSnapshot(opt_.dir + "/snapshots", sm_->memtable(), so);
    if (m.ok()) wal_->ReleaseUpTo(so.index);
    l.lock();
    snap_pending_index_ = 0;
    snap_in_progress_ = false;
    ++snapshots_done_;
    last_snapshot_status_ = m.status();
    if (m.ok()) {
      snap_index_ = so.index;
      snap_term_ = so.term;
      if (so.index > opt_.log_retain_entries) CompactUpTo(so.index - opt_.log_retain_entries);
    } else {
      KV_LOG(Error) << "snapshot failed: " << m.status();
    }
    state_cv_.notify_all();
  }
}

absl::Status RaftNode::TakeSnapshot(uint64_t* index) {
  std::unique_lock<std::mutex> l(mu_);
  state_cv_.wait(l, [&] { return !snap_in_progress_ || stopping_; });
  const uint64_t target = snapshots_done_ + 1;
  snap_forced_ = true;
  commit_cv_.notify_one();
  state_cv_.wait(l, [&] { return snapshots_done_ >= target || stopping_; });
  if (stopping_) return absl::UnavailableError("stopping");
  if (index) *index = snap_index_;
  return last_snapshot_status_;
}

RaftStatus RaftNode::Status() const {
  std::lock_guard<std::mutex> l(mu_);
  RaftStatus s;
  s.id = opt_.id;
  s.role = role_;
  s.term = current_term_;
  s.leader_id = leader_id_;
  if (leader_id_ != 0) s.leader_address = opt_.members.at(leader_id_);
  s.commit_index = commit_index_;
  s.applied_index = sm_->AppliedIndex();
  s.last_index = LastIndex();
  s.snapshot_index = snap_index_;
  s.keys = sm_->KeyCount();
  s.elections_won = elections_won_;
  s.duplicates = sm_->duplicates();
  for (const auto& p : peers_) {
    s.peers.push_back(PeerInfo{p->id, opt_.members.at(p->id), p->match, p->next, p->reachable});
  }
  return s;
}

}  // namespace kv::raft
