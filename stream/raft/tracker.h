#pragma once
#include <seastar/core/future.hh>

#include "common/macro.h"
#include "fmt/format.h"
#include "joint.h"
#include "progress.h"

namespace snail {
namespace raft {

class TrackerConfig {
 public:
  TrackerConfig() : auto_leave_(false) {}

  TrackerConfig(const TrackerConfig& cfg) {
    voters_ = cfg.voters_;
    auto_leave_ = cfg.auto_leave_;
    learners_ = cfg.learners_;
    learners_next_ = cfg.learners_next_;
  }

  TrackerConfig(TrackerConfig&& cfg) {
    voters_ = std::move(cfg.voters_);
    auto_leave_ = cfg.auto_leave_;
    learners_ = std::move(cfg.learners_);
    learners_next_ = std::move(cfg.learners_next_);
  }

  TrackerConfig& operator=(const TrackerConfig& cfg) {
    if (this != &cfg) {
      voters_ = cfg.voters_;
      auto_leave_ = cfg.auto_leave_;
      learners_ = cfg.learners_;
      learners_next_ = cfg.learners_next_;
    }
    return *this;
  }

  TrackerConfig& operator=(TrackerConfig&& cfg) {
    if (this != &cfg) {
      voters_ = std::move(cfg.voters_);
      auto_leave_ = cfg.auto_leave_;
      learners_ = std::move(cfg.learners_);
      learners_next_ = std::move(cfg.learners_next_);
    }
    return *this;
  }

  JointConfig& voters() { return voters_; }
  const JointConfig& voters() const { return voters_; }

  bool auto_leave() const { return auto_leave_; }

  void set_auto_leave(bool v) { auto_leave_ = v; }

  std::unordered_set<uint64_t>& learners() { return learners_; }
  const std::unordered_set<uint64_t>& learners() const { return learners_; }

  std::unordered_set<uint64_t>& learners_next() { return learners_next_; }
  const std::unordered_set<uint64_t>& learners_next() const {
    return learners_next_;
  }

  TrackerConfig Clone() {
    TrackerConfig cfg;
    cfg.voters_ = voters_;
    cfg.auto_leave_ = auto_leave_;
    cfg.learners_ = learners_;
    cfg.learners_next_ = learners_next_;
    return cfg;
  }

  std::string String() {
    return fmt::format(
        "voters={}[{}],[{}]{}, learners={}{}{}, learners_next={}{}{}, "
        "autoleave={}",
        "{", fmt::join(voters_[0], ","), fmt::join(voters_[1], ","), "}", "{",
        fmt::join(learners_, ","), "}", "{", fmt::join(learners_next_, ","),
        "}", auto_leave_);
  }

 protected:
  JointConfig voters_;
  bool auto_leave_;
  std::unordered_set<uint64_t> learners_;
  std::unordered_set<uint64_t> learners_next_;
};

class ProgressTracker;
using ProgressTrackerPtr = seastar::lw_shared_ptr<ProgressTracker>;

class ProgressTracker : public TrackerConfig {
 public:
  explicit ProgressTracker(size_t max_inflight)
      : TrackerConfig(), max_inflight_(max_inflight) {}

  ProgressTracker(const ProgressTracker& x) {
    voters_ = x.voters_;
    auto_leave_ = x.auto_leave_;
    learners_ = x.learners_;
    learners_next_ = x.learners_next_;
    progress_ = x.progress_;
    votes_ = x.votes_;
    max_inflight_ = x.max_inflight_;
  }

  ConfState GetConfState();

  bool IsSingleton();

  uint64_t Committed();

  seastar::future<> AsyncVisit(
      std::function<seastar::future<>(uint64_t, ProgressPtr pr)> const f);

  void Visit(std::function<void(uint64_t, Progress* pr)> const& f);

  bool QuorumActive();

  std::vector<uint64_t> VoterNodes();

  std::vector<uint64_t> LearnerNodes();

  void ResetVotes();

  void RecordVote(uint64_t id, bool v);

  std::tuple<int, int, VoteResult> TallyVotes();

  const ProgressMap& progress() const { return progress_; }
  ProgressMap& progress() { return progress_; }

  size_t max_inflight() const { return max_inflight_; }

  void set_config(const TrackerConfig& cfg);

  void set_progress(const ProgressMap& progress);

  SNAIL_PRIVATE
  ProgressMap progress_;
  std::unordered_map<uint64_t, bool> votes_;
  size_t max_inflight_;
};

}  // namespace raft
}  // namespace snail
