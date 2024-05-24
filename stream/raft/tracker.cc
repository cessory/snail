#include "tracker.h"

#include <seastar/core/coroutine.hh>

namespace snail {
namespace raft {

ConfState ProgressTracker::GetConfState() {
    ConfState cs;
    cs.set_voters(MajorityConfig2Slice(voters_[0]));
    cs.set_voters_outgoing(MajorityConfig2Slice(voters_[1]));
    cs.set_learners(MajorityConfig2Slice(learners_));
    cs.set_learners_next(MajorityConfig2Slice(learners_next_));
    cs.set_auto_leave(auto_leave_);
    return cs;
}

bool ProgressTracker::IsSingleton() {
    return voters_[0].size() == 1 && voters_[1].size() == 0;
}

uint64_t ProgressTracker::Committed() {
    return CommittedIndex(voters_, [this](uint64_t id) {
        auto iter = progress_.find(id);
        if (iter == progress_.end()) {
            return std::make_tuple(0UL, false);
        }
        return std::make_tuple(iter->second->match(), true);
    });
}

seastar::future<Status<>> ProgressTracker::AsyncVisit(
    seastar::noncopyable_function<
        seastar::future<Status<>>(uint64_t, ProgressPtr pr)>&& f) {
    auto n = progress_.size();
    uint64_t s1[7] = {0};
    std::vector<uint64_t> tmp;
    uint64_t* ids;

    if (n <= 7) {
        ids = s1;
    } else {
        tmp.resize(n, 0);
        ids = tmp.data();
    }

    for (const auto& iter : progress_) {
        n--;
        ids[n] = iter.first;
    }

    n = progress_.size();

    auto fn = [](uint64_t* s, size_t count) {
        for (size_t i = 0; i < count; i++) {
            for (size_t j = i; j > 0 && s[j] < s[j - 1]; j--) {
                uint64_t t = s[j];
                s[j] = s[j - 1];
                s[j - 1] = t;
            }
        }
    };

    fn(ids, n);
    for (size_t i = 0; i < n; i++) {
        co_await f(ids[i], progress_[ids[i]]);
    }
    co_return Status<>();
}

void ProgressTracker::Visit(
    seastar::noncopyable_function<void(uint64_t, Progress* pr)>&& f) {
    auto n = progress_.size();
    uint64_t s1[7] = {0};
    std::vector<uint64_t> tmp;
    uint64_t* ids;

    if (n <= 7) {
        ids = s1;
    } else {
        tmp.resize(n, 0);
        ids = tmp.data();
    }

    for (const auto& iter : progress_) {
        n--;
        ids[n] = iter.first;
    }

    n = progress_.size();

    auto fn = [](uint64_t* s, size_t count) {
        for (size_t i = 0; i < count; i++) {
            for (size_t j = i; j > 0 && s[j] < s[j - 1]; j--) {
                uint64_t t = s[j];
                s[j] = s[j - 1];
                s[j - 1] = t;
            }
        }
    };

    fn(ids, n);
    for (size_t i = 0; i < n; i++) {
        f(ids[i], progress_[ids[i]].get());
    }
}

bool ProgressTracker::QuorumActive() {
    std::unordered_map<uint64_t, bool> votes;
    Visit([this, &votes](uint64_t id, Progress* pr) {
        if (pr->is_learner()) {
            return;
        }
        votes[id] = pr->recent_active();
    });
    return GetVoteResult(voters_, votes) == VoteResult::VoteWon;
}

std::vector<uint64_t> ProgressTracker::VoterNodes() {
    auto m = IDs(voters_);
    std::vector<uint64_t> nodes(m.size());
    int i = 0;
    for (auto& id : m) {
        nodes[i++] = id;
    }

    std::sort(nodes.begin(), nodes.end());
    return nodes;
}

std::vector<uint64_t> ProgressTracker::LearnerNodes() {
    std::vector<uint64_t> nodes;
    if (learners_.empty()) {
        return nodes;
    }

    nodes.resize(learners_.size());
    int i = 0;
    for (auto& id : learners_) {
        nodes[i++] = id;
    }
    std::sort(nodes.begin(), nodes.end());
    return nodes;
}

void ProgressTracker::ResetVotes() { votes_.clear(); }

void ProgressTracker::RecordVote(uint64_t id, bool v) {
    auto iter = votes_.find(id);
    if (iter == votes_.end()) {
        votes_[id] = v;
    }
}

std::tuple<int, int, VoteResult> ProgressTracker::TallyVotes() {
    int granted = 0;
    int rejected = 0;
    VoteResult res;

    for (auto& iter : progress_) {
        if (iter.second->is_learner()) {
            continue;
        }
        auto it = votes_.find(iter.first);
        if (it == votes_.end()) {
            continue;
        }
        if (it->second) {
            granted++;
        } else {
            rejected++;
        }
    }
    res = GetVoteResult(voters_, votes_);
    return std::make_tuple(granted, rejected, res);
}

void ProgressTracker::set_config(const TrackerConfig& cfg) {
    voters_ = cfg.voters();
    auto_leave_ = cfg.auto_leave();
    learners_ = cfg.learners();
    learners_next_ = cfg.learners_next();
}

void ProgressTracker::set_progress(const ProgressMap& progress) {
    progress_ = progress;
}

}  // namespace raft
}  // namespace snail
