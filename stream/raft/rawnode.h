#pragma once
#include <spdlog/spdlog.h>
#include <string.h>

#include <seastar/core/future.hh>

#include "raft.h"
#include "raft_proto.h"

namespace snail {
namespace raft {

struct BasicStatus {
  uint64_t id;
  HardState hs;
  SoftState st;
  uint64_t applied;
  uint64_t lead_transferee;
};

struct Status : public BasicStatus {
  TrackerConfig cfg;
  ProgressMap prs;
};

enum class ProgressType {
  ProgressTypePeer = 0,
  ProgressTypeLearner = 1,
};

enum class SnapshotStatus {
  SnapshotFinish = 1,
  SnapshotFailure = 2,
};

class RawNode {
  RaftPtr raft_;
  SoftState prev_soft_state_;
  HardState prev_hard_state_;

 public:
  explicit RawNode(spdlog::logger* l) {
    raft_ = seastar::make_lw_shared<Raft>(l);
    memset(&prev_soft_state_, 0, sizeof(prev_soft_state_));
    memset(&prev_hard_state_, 0, sizeof(prev_hard_state_));
  }

  RaftPtr GetRaft() { return raft_; }

  seastar::future<int> Init(const Raft::Config cfg);

  seastar::future<> Tick();

  seastar::future<int> Campaign();

  seastar::future<int> Propose(seastar::temporary_buffer<char> data);

  seastar::future<int> ProposeConfChange(ConfChangeI cc);

  seastar::future<ConfState> ApplyConfChange(ConfChangeI cc);

  seastar::future<int> Step(MessagePtr m);

  seastar::future<ReadyPtr> GetReady(const SoftState st, const HardState hs);

  bool HasReady();

  seastar::future<> Advance(ReadyPtr rd);

  BasicStatus GetBasicStatus();

  Status GetStatus();

  void WithProgress(std::function<void(uint64_t id, ProgressType typ,
                                       ProgressPtr pr)> const& visitor);

  seastar::future<int> ReportUnreachable(uint64_t id);

  seastar::future<int> ReportSnapshot(uint64_t id, SnapshotStatus status);

  seastar::future<int> TransferLeader(uint64_t transferee);

  seastar::future<int> ReadIndex(seastar::temporary_buffer<char> rctx);

 private:
  ProgressMap GetProgressCopy();
};

using RawNodePtr = seastar::lw_shared_ptr<RawNode>;

}  // namespace raft
}  // namespace snail
