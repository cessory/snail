
#include "raft/confchange.h"

#include <seastar/testing/seastar_test.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/testing/thread_test_case.hh>
#include <vector>

#include "raft/raft_proto.h"

namespace snail {
namespace raft {

static std::vector<uint64_t> conv(std::default_random_engine& e, int n) {
    std::vector<uint64_t> vec(n);
    for (int i = 0; i < n; i++) {
        vec[i] = (uint64_t)(i + 1);
    }

    for (int i = 1; i < n; i++) {
        std::uniform_int_distribution<int> u(i, n - 1);
        int idx = u(e);
        uint64_t tmp = vec[i - 1];
        vec[i - 1] = vec[idx];
        vec[idx] = tmp;
    }
    return vec;
}

static ConfState GenerateConfState() {
    ConfState cs;
    static std::default_random_engine e(time(NULL));
    std::uniform_int_distribution<int> u(1, 5);
    int nVoters = u(e);

    std::uniform_int_distribution<int> u2(0, 4);
    int nLearners = u2(e);

    std::uniform_int_distribution<int> u3(0, 2);
    int nRemovedVoters = u3(e);

    std::vector<uint64_t> ids =
        conv(e, 2 * (nVoters + nLearners + nRemovedVoters));
    for (int i = 0; i < nVoters; i++) {
        cs.voters().push_back(ids[i]);
    }

    for (int i = nVoters; i < nVoters + nLearners; i++) {
        cs.learners().push_back(ids[i]);
    }

    std::uniform_int_distribution<int> u4(0, nVoters);
    int nOutgoingRetainedVoters = u4(e);

    for (int i = 0; i < nOutgoingRetainedVoters; i++) {
        cs.voters_outgoing().push_back(cs.voters()[i]);
    }
    for (int i = nVoters + nLearners; i < nVoters + nLearners + nRemovedVoters;
         i++) {
        cs.voters_outgoing().push_back(ids[i]);
    }

    std::uniform_int_distribution<int> u5(0, nRemovedVoters);
    int nLearnersNext = u5(e);
    for (int i = nVoters + nLearners; i < nVoters + nLearners + nLearnersNext;
         i++) {
        cs.learners_next().push_back(ids[i]);
    }
    std::uniform_int_distribution<int> u6(0, 1);
    cs.set_auto_leave(cs.learners_next().size() > 0 && u6(e) == 1);
    return std::move(cs);
}

SEASTAR_THREAD_TEST_CASE(ConfChangeRestoreTest) {
    auto f = [](const ConfState& cs) {
        Changer chg(10, seastar::make_lw_shared<ProgressTracker>((uint64_t)20));

        TrackerConfig cfg;
        ProgressMap prs;
        auto s = chg.Restore(cs);
        BOOST_REQUIRE(s);
        std::tie(cfg, prs) = s.Value();
        chg.tracker_->set_config(cfg);
        chg.tracker_->set_progress(prs);

        auto cs2 = chg.tracker_->GetConfState();
        auto voters = cs.voters();
        std::sort(voters.begin(), voters.end());
        BOOST_REQUIRE(voters == cs2.voters());
        auto learners = cs.learners();
        std::sort(learners.begin(), learners.end());
        BOOST_REQUIRE(learners == cs2.learners());
        auto vo = cs.voters_outgoing();
        std::sort(vo.begin(), vo.end());
        BOOST_REQUIRE(vo == cs2.voters_outgoing());
        auto ln = cs.learners_next();
        std::sort(ln.begin(), ln.end());
        BOOST_REQUIRE(ln == cs2.learners_next());
    };

    std::array<ConfState, 4> css;
    css[1].set_voters(std::move(std::vector<uint64_t>{1, 2, 3}));
    css[2].set_voters(std::move(std::vector<uint64_t>{1, 2, 3}));
    css[2].set_learners(std::move(std::vector<uint64_t>{4, 5, 6}));
    css[3].set_voters(std::move(std::vector<uint64_t>{1, 2, 3}));
    css[3].set_learners(std::move(std::vector<uint64_t>{5}));
    css[3].set_voters_outgoing(std::move(std::vector<uint64_t>{1, 2, 4, 6}));
    css[3].set_learners_next(std::move(std::vector<uint64_t>{4}));

    for (auto& cs : css) {
        f(cs);
    }

    for (int i = 0; i < 4000; i++) {
        auto cs = GenerateConfState();
        f(cs);
    }
}

}  // namespace raft
}  // namespace snail
