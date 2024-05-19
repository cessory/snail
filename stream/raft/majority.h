#pragma once
#include <cstdint>
#include <functional>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace snail {
namespace raft {

enum class VoteResult {
  VotePending = 1,
  VoteLost = 2,
  VoteWon = 3,
};

using MajorityConfig = std::unordered_set<uint64_t>;
// return a sorted slice
std::vector<uint64_t> MajorityConfig2Slice(const MajorityConfig& cfg);

uint64_t CommittedIndex(
    const MajorityConfig& cfg,
    std::function<std::tuple<uint64_t, bool>(uint64_t)> const& fn);

VoteResult GetVoteResult(const MajorityConfig& cfg,
                         const std::unordered_map<uint64_t, bool>& votes);
}  // namespace raft
}  // namespace snail
