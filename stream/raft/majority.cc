#include "majority.h"

#include <limits>

namespace snail {
namespace raft {

std::vector<uint64_t> MajorityConfig2Slice(const MajorityConfig& cfg) {
    std::vector<uint64_t> s;
    s.reserve(cfg.size());

    for (auto id : cfg) {
        s.push_back(id);
    }
    std::sort(s.begin(), s.end());
    return s;
}

static void insertionSort(uint64_t* s, size_t n) {
    for (size_t i = 1; i < n; i++) {
        for (size_t j = i; j > 0 && s[j] < s[j - 1]; j--) {
            uint64_t tmp = s[j];
            s[j] = s[j - 1];
            s[j - 1] = tmp;
        }
    }
}

uint64_t CommittedIndex(
    const MajorityConfig& cfg,
    seastar::noncopyable_function<std::tuple<uint64_t, bool>(uint64_t)>&& fn) {
    auto n = cfg.size();
    if (n == 0) {
        return std::numeric_limits<uint64_t>::max();
    }

    uint64_t stk[7] = {0};
    std::vector<uint64_t> tmp;
    uint64_t* srt;

    if (n <= 7) {
        srt = stk;
    } else {
        tmp.resize(n);
        srt = tmp.data();
    }

    auto i = n - 1;
    for (auto id : cfg) {
        auto res = fn(id);
        if (std::get<1>(res)) {
            srt[i] = std::get<0>(res);
            i--;
        }
    }

    insertionSort(srt, n);
    return srt[n - (n / 2 + 1)];
}

VoteResult GetVoteResult(const MajorityConfig& cfg,
                         const std::unordered_map<uint64_t, bool>& votes) {
    if (cfg.size() == 0) {
        return VoteResult::VoteWon;
    }

    int ny[2] = {0, 0};
    int missing = 0;
    for (auto id : cfg) {
        auto iter = votes.find(id);
        if (iter == votes.end()) {
            missing++;
            continue;
        }

        if (iter->second) {
            ny[1]++;
        } else {
            ny[0]++;
        }
    }

    int n = cfg.size() / 2 + 1;
    if (ny[1] >= n) {
        return VoteResult::VoteWon;
    }
    if (ny[1] + missing >= n) {
        return VoteResult::VotePending;
    }

    return VoteResult::VoteLost;
}

}  // namespace raft
}  // namespace snail
