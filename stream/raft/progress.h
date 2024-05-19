#pragma once
#include "inflights.h"
#include "raft_proto.h"

namespace snail {
namespace raft {

class Progress {
  uint64_t match_;
  uint64_t next_;
  StateType state_;
  uint64_t pending_snapshot_;
  bool recent_active_;
  bool probe_sent_;
  Inflights inflights_;
  bool is_learner_;

 public:
  explicit Progress(size_t max_inflight);

  Progress(const Progress& x);

  Progress& operator=(const Progress& x);

  uint64_t match();
  uint64_t next();
  StateType state();
  uint64_t pending_snapshot();
  bool recent_active();
  bool probe_sent();
  Inflights& inflights();
  bool is_learner();

  void set_match(uint64_t match);
  void set_next(uint64_t next);
  void set_learner(bool learner);
  void set_recent_active(bool v);
  void set_probe_sent(bool v);
  void set_pending_snapshot(uint64_t v);

  void ResetState(StateType state);

  void ProbeAcked();

  void BecomeProbe();

  void BecomeReplicate();

  void BecomeSnapshot(uint64_t snapshot);

  bool MaybeUpdate(uint64_t n);

  void OptimisticUpdate(uint64_t n);

  bool MaybeDecrTo(uint64_t rejected, uint64_t match_hint);

  bool IsPaused();

  std::string String();
};

using ProgressPtr = seastar::lw_shared_ptr<Progress>;

using ProgressMap = std::unordered_map<uint64_t, ProgressPtr>;
}  // namespace raft
}  // namespace snail
