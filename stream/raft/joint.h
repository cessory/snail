#pragma once
#include "majority.h"

namespace snail {
namespace raft {

using JointConfig = std::array<MajorityConfig, 2>;

std::unordered_set<uint64_t> IDs(const JointConfig& cfg);

uint64_t CommittedIndex(
    const JointConfig& cfg,
    std::function<std::tuple<uint64_t, bool>(uint64_t)> const& fn);

VoteResult GetVoteResult(const JointConfig& cfg,
                         const std::unordered_map<uint64_t, bool>& votes);

}  // namespace raft
}  // namespace snail
