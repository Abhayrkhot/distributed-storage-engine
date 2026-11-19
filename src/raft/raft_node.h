// A Raft replica: leader election (with PreVote and leader stickiness),
// log replication with quorum commit, snapshot-based log compaction and
// InstallSnapshot, ReadIndex and lease reads, and leadership transfer.
//
// Persistence:
//   <dir>/raft-state   current term and vote (atomic rename; changes are rare)
//   <dir>/wal/         the Raft log, in the group-commit WAL (one fdatasync per
//                      batch of entries; each record stamped with the commit
//                      index known when it was written)
//   <dir>/snapshots/   state machine snapshots at an applied index
//
// Threads: a replicator per peer (one AppendEntries in flight each, batching
// whatever accumulated meanwhile), a heartbeat sender per peer (so a large
// batch or a slow follower fsync never delays leadership confirmation,
// CheckQuorum, leases or ReadIndex rounds), a ticker (elections, CheckQuorum), a commit
// dispatcher (feeds committed entries to the parallel applier), a reader
// (serves confirmed linearizable reads once applied), and a snapshot writer.
#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "proto/raft.grpc.pb.h"
#include "raft/grpc_transport.h"
#include "raft/raft_state_machine.h"
#include "storage/wal.h"

namespace kv::raft {

struct RaftOptions {
  uint32_t id = 0;
  std::map<uint32_t, std::string> members;  // id -> host:port, including this node
  std::string dir;
  int heartbeat_ms = 20;
  int election_min_ms = 100;
  int election_max_ms = 200;
  size_t max_append_bytes = 2u << 20;
  size_t max_append_entries = 16384;
  uint64_t snapshot_trigger_bytes = 256ull << 20;  // log bytes applied since the last snapshot
  uint64_t log_retain_entries = 100000;            // kept in memory behind a snapshot
  uint32_t num_shards = 256;
  int apply_threads = 4;
  int snapshot_threads = 4;
  int recovery_threads = 0;
  WalOptions wal;
  bool check_quorum = true;
};

enum class Role { kFollower, kCandidate, kLeader };
const char* RoleName(Role r);

enum class ReadMode { kLinearizable, kLease, kStale };

struct PeerInfo {
  uint32_t id = 0;
  std::string address;
  uint64_t match = 0;
  uint64_t next = 0;
  bool reachable = false;
};

struct RaftStatus {
  uint32_t id = 0;
  Role role = Role::kFollower;
  uint64_t term = 0;
  uint32_t leader_id = 0;
  std::string leader_address;
  uint64_t commit_index = 0;
  uint64_t applied_index = 0;
  uint64_t last_index = 0;
  uint64_t snapshot_index = 0;
  uint64_t keys = 0;
  uint64_t elections_won = 0;
  uint64_t duplicates = 0;
  std::vector<PeerInfo> peers;
};

class RaftNode {
 public:
  static absl::StatusOr<std::unique_ptr<RaftNode>> Open(const RaftOptions& options,
                                                        Transport* transport);
  ~RaftNode();

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  void Start();
  void Shutdown();

  // Proposes a write. `done` runs once the entry is committed and applied, or
  // with UNAVAILABLE if this node is not (or stops being) the leader; in the
  // latter case the write may still commit, so clients retry with the same
  // session seq and the state machine deduplicates.
  void Propose(uint64_t client_id, uint64_t seq, std::string batch_rep, WriteCompletion* done);

  // Calls done(OK) once the local state machine may serve a read with the
  // requested guarantee, or done(UNAVAILABLE) if this node cannot.
  void ReadBarrier(ReadMode mode, std::function<void(const absl::Status&)> done);

  absl::Status TransferLeadership(uint32_t target, uint32_t* new_leader);
  absl::Status TakeSnapshot(uint64_t* index);

  StateMachine& state() { return *sm_; }
  RaftStatus Status() const;
  uint32_t id() const { return opt_.id; }
  // Leader hint for redirects: "<id>@<address>", or empty if unknown.
  std::string LeaderHint() const;
  double recovery_seconds() const { return recovery_seconds_; }
  uint64_t recovered_commit() const { return recovered_commit_; }

  // RPC handlers.
  grpc::Status HandleRequestVote(const VoteRequest& req, VoteResponse* resp);
  grpc::Status HandleAppendEntries(const AppendRequest& req, AppendResponse* resp);
  grpc::Status HandleInstallSnapshot(grpc::ServerReader<SnapshotChunk>* reader,
                                     InstallSnapshotResponse* resp);
  grpc::Status HandleTimeoutNow(const TimeoutNowRequest& req, TimeoutNowResponse* resp);
  Transport* transport() { return transport_; }

 private:
  using Clock = std::chrono::steady_clock;

  struct LogEntry {
    uint64_t term;
    std::shared_ptr<const std::string> data;
  };
  struct Pending {
    uint64_t term;
    WriteCompletion* done;
  };
  struct Peer {
    uint32_t id = 0;
    uint64_t next = 1;
    uint64_t match = 0;
    Clock::time_point last_ack{};       // send time of the last answered request
    Clock::time_point next_heartbeat{};
    uint64_t acked_round = 0;
    uint64_t sent_round = 0;
    bool reachable = false;
    bool timeout_now_sent = false;
    uint64_t hb_round = 0;  // read round carried by the last heartbeat
    std::thread thread;     // replicator: log entries, snapshots, TimeoutNow
    std::thread hb_thread;  // heartbeats, independent of slow appends
  };
  struct ReadWaiter {
    uint64_t round;
    uint64_t read_index;
    std::function<void(const absl::Status&)> done;
  };

  explicit RaftNode(const RaftOptions& options, Transport* transport);
  absl::Status Recover();

  // Log helpers (mu_ held).
  uint64_t LastIndex() const { return base_index_ + log_.size(); }
  uint64_t LastTerm() const { return log_.empty() ? base_term_ : log_.back().term; }
  uint64_t TermAt(uint64_t index) const;  // 0 if unknown
  const LogEntry& EntryAt(uint64_t index) const { return log_[index - base_index_ - 1]; }
  void TruncateFrom(uint64_t index);
  void CompactUpTo(uint64_t index);
  void AppendLocal(uint64_t term, std::shared_ptr<const std::string> data);

  // Role transitions (mu_ held).
  void BecomeFollower(uint64_t term, uint32_t leader);
  void BecomeLeader();
  void StartElection(bool pre_vote, bool transfer);
  void OnVoteReply(uint64_t election, bool pre_vote, uint64_t term, bool granted);
  void AdvanceCommit();
  void UpdateLeaseAndReads();
  void FailPending(const absl::Status& st);
  void ResetElectionTimer();
  absl::Status PersistHardState();
  size_t Majority() const { return opt_.members.size() / 2 + 1; }

  void OnWalDurable(const absl::Status& st, std::vector<Wal::Record>&& group);
  void ReplicatorLoop(Peer* peer);
  void HeartbeatLoop(Peer* peer);
  bool SendSnapshot(Peer* peer, uint64_t term);
  void TickerLoop();
  void DispatcherLoop();
  void ReaderLoop();
  void SnapshotLoop();
  void StartLocalSnapshot();  // dispatch_mu_ held

  const RaftOptions opt_;
  Transport* const transport_;
  std::unique_ptr<StateMachine> sm_;
  std::unique_ptr<Wal> wal_;
  double recovery_seconds_ = 0;
  uint64_t recovered_commit_ = 0;

  mutable std::mutex mu_;
  std::condition_variable repl_cv_;     // replicators
  std::condition_variable hb_cv_;       // heartbeat senders
  int repl_idle_ = 0;                   // replicators parked with nothing to send
  std::condition_variable commit_cv_;   // dispatcher
  std::condition_variable durable_cv_;  // AppendEntries handlers waiting for fsync
  std::condition_variable state_cv_;    // role changes, snapshot/transfer progress
  std::condition_variable reader_cv_;

  // Persistent state.
  uint64_t current_term_ = 0;
  uint32_t voted_for_ = 0;
  // Log: log_[i] has index base_index_ + 1 + i; (base_index_, base_term_) is
  // the entry just before it (covered by a snapshot, or 0/0).
  std::deque<LogEntry> log_;
  uint64_t base_index_ = 0;
  uint64_t base_term_ = 0;

  // Volatile state.
  Role role_ = Role::kFollower;
  uint32_t leader_id_ = 0;
  uint64_t commit_index_ = 0;
  uint64_t durable_index_ = 0;
  uint64_t dispatched_index_ = 0;
  uint64_t leader_first_index_ = 0;  // index of this leader's no-op
  Clock::time_point last_heard_{};
  Clock::time_point election_deadline_{};
  Clock::time_point leader_since_{};
  uint64_t election_id_ = 0;
  size_t votes_ = 0;
  uint64_t elections_won_ = 0;
  std::mt19937_64 rng_;
  bool stopping_ = false;
  bool started_ = false;
  bool installing_ = false;

  // Leader state.
  std::vector<std::unique_ptr<Peer>> peers_;
  std::unordered_map<uint64_t, Pending> pending_;
  bool transferring_ = false;
  uint32_t transfer_target_ = 0;
  Clock::time_point transfer_deadline_{};
  Clock::time_point lease_until_{};
  uint64_t read_round_ = 0;
  std::deque<ReadWaiter> pending_reads_;
  std::deque<ReadWaiter> confirmed_reads_;

  // Snapshots.
  uint64_t snap_index_ = 0;  // newest snapshot on disk
  uint64_t snap_term_ = 0;
  uint64_t bytes_since_snapshot_ = 0;
  bool snap_in_progress_ = false;
  bool snap_forced_ = false;
  bool snap_requested_ = false;
  uint64_t snap_pending_index_ = 0;
  uint64_t snap_pending_term_ = 0;
  std::string snap_pending_state_;
  uint64_t snapshots_done_ = 0;
  absl::Status last_snapshot_status_;
  std::mutex dispatch_mu_;  // held while the dispatcher feeds the state machine
  std::mutex install_mu_;   // one InstallSnapshot at a time
  uint64_t install_seq_ = 0;
  Clock::time_point no_votes_until_{};  // vote embargo after restart

  // Outstanding async vote RPCs (must finish before destruction).
  std::mutex calls_mu_;
  std::condition_variable calls_cv_;
  std::set<grpc::ClientContext*> calls_;

  std::thread ticker_, dispatcher_, reader_, snapshotter_;
};

// gRPC adapter for the peer-facing Raft service.
class RaftService final : public ::kv::raft::Raft::Service {
 public:
  explicit RaftService(RaftNode* node) : node_(node) {}
  grpc::Status RequestVote(grpc::ServerContext*, const VoteRequest* req, VoteResponse* resp) override {
    return node_->HandleRequestVote(*req, resp);
  }
  grpc::Status AppendEntries(grpc::ServerContext*, const AppendRequest* req,
                             AppendResponse* resp) override {
    return node_->HandleAppendEntries(*req, resp);
  }
  grpc::Status InstallSnapshot(grpc::ServerContext*, grpc::ServerReader<SnapshotChunk>* reader,
                               InstallSnapshotResponse* resp) override {
    return node_->HandleInstallSnapshot(reader, resp);
  }
  grpc::Status TimeoutNow(grpc::ServerContext*, const TimeoutNowRequest* req,
                          TimeoutNowResponse* resp) override {
    return node_->HandleTimeoutNow(*req, resp);
  }

 private:
  RaftNode* node_;
};

}  // namespace kv::raft
