#pragma once
#include "tracker.h"
#include "util/status.h"

namespace snail {
namespace raft {

class Changer {
#ifdef RAFT_UT_TEST
   public:
#endif
    uint64_t group_;
    uint64_t id_;
    uint64_t last_index_;
    ProgressTrackerPtr tracker_;

   public:
    explicit Changer(uint64_t last_index, ProgressTrackerPtr tracker,
                     uint64_t group = 0, uint64_t id = 1)
        : last_index_(last_index),
          tracker_(seastar::make_lw_shared(*tracker)),
          group_(group),
          id_(id) {}

    Status<std::tuple<TrackerConfig, ProgressMap>> EnterJoint(
        bool auto_leave, const std::vector<ConfChangeSingle>& ccs);

    Status<std::tuple<TrackerConfig, ProgressMap>> LeaveJoint();

    Status<std::tuple<TrackerConfig, ProgressMap>> Simple(
        const std::vector<ConfChangeSingle>& ccs);

    Status<std::tuple<TrackerConfig, ProgressMap>> Restore(const ConfState& cs);

   private:
    Status<> Apply(TrackerConfig* cfg, ProgressMap& prs,
                   const std::vector<ConfChangeSingle>& ccs);

    void MakeVoter(TrackerConfig* cfg, ProgressMap& prs, uint64_t id);

    void MakeLearner(TrackerConfig* cfg, ProgressMap& prs, uint64_t id);

    void Remove(TrackerConfig* cfg, ProgressMap& prs, uint64_t id);

    void InitProgress(TrackerConfig* cfg, ProgressMap& prs, uint64_t id,
                      bool is_learner);

    Status<> CheckInvariants(const TrackerConfig& cfg, const ProgressMap& prs);

    Status<std::tuple<TrackerConfig, ProgressMap>> CheckAndCopy();

    Status<std::tuple<TrackerConfig, ProgressMap>> CheckAndReturn(
        TrackerConfig cfg, ProgressMap prs);
};

}  // namespace raft
}  // namespace snail
