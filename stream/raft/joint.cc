#include "joint.h"

namespace snail {
namespace raft {

std::unordered_set<uint64_t> IDs(const JointConfig& cfg) {
    std::unordered_set<uint64_t> m;
    for (int i = 0; i < 2; i++) {
        for (const auto& key : cfg[i]) {
            m.insert(key);
        }
    }
    return m;
}

uint64_t CommittedIndex(
    const JointConfig& cfg,
    seastar::noncopyable_function<std::tuple<uint64_t, bool>(uint64_t)>&& fn) {
    auto idx0 = CommittedIndex(cfg[0], fn);
    auto idx1 = CommittedIndex(cfg[1], fn);

    return idx0 < idx1 ? idx0 : idx1;
}

VoteResult GetVoteResult(const JointConfig& cfg,
                         const std::unordered_map<uint64_t, bool>& votes) {
    auto r1 = GetVoteResult(cfg[0], votes);
    auto r2 = GetVoteResult(cfg[1], votes);

    if (r1 == r2) {
        return r1;
    }

    if (r1 == VoteResult::VoteLost || r2 == VoteResult::VoteLost) {
        return VoteResult::VoteLost;
    }

    return VoteResult::VotePending;
}

}  // namespace raft
}  // namespace snail
