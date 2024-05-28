#include "raft_test.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

namespace snail {
namespace raft {

SEASTAR_THREAD_TEST_CASE(ProgressLeaderTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    raft->prs_->progress()[2]->BecomeReplicate();

    for (int i = 0; i < 5; i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("foo", 3);
        ent->set_data(std::move(data));
        m->entries.push_back(ent);

        auto pr = raft->prs_->progress()[raft->id_];
        BOOST_REQUIRE_EQUAL(pr->state(),
                            snail::raft::StateType::StateReplicate);
        BOOST_REQUIRE_EQUAL(pr->match(), i + 1);
        BOOST_REQUIRE_EQUAL(pr->next(), pr->match() + 1);

        auto s = raft->Step(m).get0();
        BOOST_REQUIRE(s);
    }
}

SEASTAR_THREAD_TEST_CASE(ProgressResumeByHeartbeatRespTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    raft->prs_->progress()[2]->set_probe_sent(true);

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgBeat;
    raft->Step(m).get();
    BOOST_REQUIRE(raft->prs_->progress()[2]->probe_sent());

    raft->prs_->progress()[2]->BecomeReplicate();
    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHeartbeatResp;
    raft->Step(m).get();
    BOOST_REQUIRE(!raft->prs_->progress()[2]->probe_sent());
}

SEASTAR_THREAD_TEST_CASE(ProgressPausedTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    seastar::temporary_buffer<char> data("somedata", 8);
    for (int i = 0; i < 3; i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        ent->set_data(std::move(data.share()));
        m->entries.push_back(ent);
        raft->Step(m).get();
    }

    auto ms = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(ms.size(), 1);
}

SEASTAR_THREAD_TEST_CASE(ProgressFlowControlTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto cfg = newTestConfig(1, 5, 1, store);
    cfg.max_inflight_msgs = 3;
    cfg.max_size_per_msg = 2048;

    auto raft = seastar::make_lw_shared<snail::raft::Raft>();
    raft->Init(cfg).get();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    auto ms = std::move(raft->msgs_);
    raft->prs_->progress()[2]->BecomeProbe();
    std::string s(1000, 'a');
    seastar::temporary_buffer<char> blob(s.data(), s.size());
    for (int i = 0; i < 10; i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        ent->set_data(std::move(blob.share()));
        m->entries.push_back(ent);
        raft->Step(m).get();
    }
    ms = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(ms.size(), 1);
    BOOST_REQUIRE_EQUAL(ms[0]->type, snail::raft::MessageType::MsgApp);
    BOOST_REQUIRE_EQUAL(ms[0]->entries.size(), 2);
    BOOST_REQUIRE_EQUAL(ms[0]->entries[0]->data().size(), 0);
    BOOST_REQUIRE_EQUAL(ms[0]->entries[1]->data().size(), 1000);

    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgAppResp;
    m->index = ms[0]->entries[1]->index();
    raft->Step(m).get();
    ms = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(ms.size(), 3);
    for (int i = 0; i < ms.size(); i++) {
        BOOST_REQUIRE_EQUAL(ms[i]->type, snail::raft::MessageType::MsgApp);
        BOOST_REQUIRE_EQUAL(ms[i]->entries.size(), 2);
    }

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgAppResp;
    m->index = ms[2]->entries[1]->index();
    raft->Step(m).get();
    ms = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(ms.size(), 2);
    for (int i = 0; i < ms.size(); i++) {
        BOOST_REQUIRE_EQUAL(ms[i]->type, snail::raft::MessageType::MsgApp);
    }
    BOOST_REQUIRE_EQUAL(ms[0]->entries.size(), 2);
    BOOST_REQUIRE_EQUAL(ms[1]->entries.size(), 1);
}

SEASTAR_THREAD_TEST_CASE(UncommittedEntryLimitTest) {
    const int maxEntries = 1024;

    auto testEntry = snail::raft::make_entry();
    seastar::temporary_buffer<char> data("testdata", 8);
    testEntry->set_data(std::move(data.share()));
    uint64_t maxEntrySize = maxEntries * testEntry->data().size();

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto cfg = newTestConfig(1, 5, 1, store);
    cfg.max_inflight_msgs = 2 * 1024;
    cfg.max_uncommitted_entries_size = maxEntrySize;
    auto raft = seastar::make_lw_shared<snail::raft::Raft>();
    raft->Init(cfg).get();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    BOOST_REQUIRE_EQUAL(raft->uncommitted_size_, 0);

    const int numFollowers = 2;
    raft->prs_->progress()[2]->BecomeReplicate();
    raft->prs_->progress()[3]->BecomeReplicate();
    raft->uncommitted_size_ = 0;

    std::vector<snail::raft::EntryPtr> propEnts;
    for (int i = 0; i < maxEntries; i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        ent->set_data(std::move(data.share()));
        m->entries.push_back(ent);
        auto s = raft->Step(m).get0();
        BOOST_REQUIRE(s);
        ent = snail::raft::make_entry();
        ent->set_data(std::move(data.share()));
        propEnts.push_back(ent);
    }

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    auto ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    auto s = raft->Step(m).get0();
    BOOST_REQUIRE(s.Code() == ErrCode::ErrRaftProposalDropped);

    auto ms = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(ms.size(), maxEntries * numFollowers);
    raft->ReduceUncommittedSize(propEnts);
    BOOST_REQUIRE_EQUAL(raft->uncommitted_size_, 0);

    propEnts.clear();
    for (int i = 0; i < 2 * maxEntries; i++) {
        ent = snail::raft::make_entry();
        ent->set_data(std::move(data.share()));
        propEnts.push_back(ent);
    }
    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    m->entries = propEnts;
    s = raft->Step(m).get0();
    BOOST_REQUIRE(s);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    s = raft->Step(m).get0();
    BOOST_REQUIRE(s.Code() == ErrCode::ErrRaftProposalDropped);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    m->entries.push_back(snail::raft::make_entry());
    s = raft->Step(m).get0();
    BOOST_REQUIRE(s);

    ms = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(ms.size(), 2 * numFollowers);
    raft->ReduceUncommittedSize(propEnts);
    BOOST_REQUIRE_EQUAL(raft->uncommitted_size_, 0);
}

static void testLeaderElection(bool preVote) {
    std::function<void(snail::raft::Raft::Config*)> cfg;
    auto candState = snail::raft::RaftState::StateCandidate;
    uint64_t candTerm = 1;

    if (preVote) {
        cfg = preVoteConfig;
        candState = snail::raft::RaftState::StatePreCandidate;
        candTerm = 0;
    }

    struct testItem {
        NetworkPtr net;
        snail::raft::RaftState state;
        uint64_t expTerm;
    };

    testItem tests[6] = {
        {newNetworkWithConfig(cfg, {nullptr, nullptr, nullptr}),
         snail::raft::RaftState::StateLeader, 1},
        {newNetworkWithConfig(cfg, {nullptr, nullptr, nopStepper}),
         snail::raft::RaftState::StateLeader, 1},
        {newNetworkWithConfig(cfg, {nullptr, nopStepper, nopStepper}),
         candState, candTerm},
        {newNetworkWithConfig(cfg, {nullptr, nopStepper, nopStepper, nullptr}),
         candState, candTerm},
        {newNetworkWithConfig(
             cfg, {nullptr, nopStepper, nopStepper, nullptr, nullptr}),
         snail::raft::RaftState::StateLeader, 1},
        {newNetworkWithConfig(
             cfg, {nullptr, entsWithConfig(cfg, {1}), entsWithConfig(cfg, {1}),
                   entsWithConfig(cfg, {1, 1}), nullptr}),
         snail::raft::RaftState::StateFollower, 1},
    };

    for (int i = 0; i < 6; i++) {
        std::vector<snail::raft::MessagePtr> msgs;
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgHup;
        msgs.push_back(m);
        tests[i].net->Send(msgs).get();
        auto sm =
            seastar::dynamic_pointer_cast<RaftSM>(tests[i].net->peers_[1]);
        BOOST_REQUIRE_EQUAL(sm->raft_->state_, tests[i].state);
        BOOST_REQUIRE_EQUAL(sm->raft_->term_, tests[i].expTerm);
    }
}

SEASTAR_THREAD_TEST_CASE(LeaderElectionTest) { testLeaderElection(false); }

SEASTAR_THREAD_TEST_CASE(LeaderElectionPreVoteTest) {
    testLeaderElection(false);
}

SEASTAR_THREAD_TEST_CASE(LearnerElectionTimeoutTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto n1 = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto n2 = newTestRaft(2, 10, 1, store).get0();

    n1->BecomeFollower(1, 0);
    n2->BecomeFollower(1, 0);
    n2->randomized_election_timeout_ = n2->election_timeout_;
    for (int i = 0; i < n2->election_timeout_; i++) {
        n2->tick_().get();
    }
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(LearnerPromotionTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto n1 = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto n2 = newTestRaft(2, 10, 1, store).get0();

    n1->BecomeFollower(1, 0);
    n2->BecomeFollower(1, 0);

    auto nt = newNetwork(
        {RaftSM::MakeStateMachine(n1), RaftSM::MakeStateMachine(n2)});

    BOOST_REQUIRE(!(n1->state_ == snail::raft::RaftState::StateLeader));
    n1->randomized_election_timeout_ = n1->election_timeout_;
    for (int i = 0; i < n1->election_timeout_; i++) {
        n1->tick_().get();
    }
    BOOST_REQUIRE(n1->state_ == snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateFollower);

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgBeat;
    nt->Send({m}).get();

    snail::raft::ConfChange v1;
    v1.set_node_id(2);
    v1.set_type(snail::raft::ConfChangeType::ConfChangeAddNode);
    snail::raft::ConfChangeI cc(v1);
    n1->ApplyConfChange(cc.AsV2()).get();
    n2->ApplyConfChange(cc.AsV2()).get();
    BOOST_REQUIRE(!n2->is_learner_);

    n2->randomized_election_timeout_ = n2->election_timeout_;
    for (int i = 0; i < n2->election_timeout_; i++) {
        n2->tick_().get();
    }

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgBeat;
    nt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(LearnerCanVoteTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto n2 = newTestRaft(2, 10, 1, store).get0();

    n2->BecomeFollower(1, 0);
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 2;
    m->term = 2;
    m->type = snail::raft::MessageType::MsgVote;
    m->log_term = 11;
    m->index = 11;
    n2->Step(m).get();

    BOOST_REQUIRE_EQUAL(n2->msgs_.size(), 1);
    BOOST_REQUIRE(
        !(n2->msgs_[0]->type != snail::raft::MessageType::MsgVoteResp &&
          !n2->msgs_[0]->reject));
}

static void testLeaderCycle(bool preVote) {
    std::function<void(snail::raft::Raft::Config*)> cfg;
    if (preVote) {
        cfg = preVoteConfig;
    }

    auto n = newNetworkWithConfig(cfg, {nullptr, nullptr, nullptr});
    for (uint64_t campaignerID = 1; campaignerID <= 3; campaignerID++) {
        auto m = snail::raft::make_raft_message();
        m->from = campaignerID;
        m->to = campaignerID;
        m->type = snail::raft::MessageType::MsgHup;
        n->Send({m}).get();

        for (auto iter : n->peers_) {
            auto peer = iter.second;
            auto sm = seastar::dynamic_pointer_cast<RaftSM>(peer);
            BOOST_REQUIRE(
                !(sm->raft_->id_ == campaignerID &&
                  sm->raft_->state_ != snail::raft::RaftState::StateLeader));
            BOOST_REQUIRE(
                !(sm->raft_->id_ != campaignerID &&
                  sm->raft_->state_ != snail::raft::RaftState::StateFollower));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(LeaderCycleTest) { testLeaderCycle(false); }

SEASTAR_THREAD_TEST_CASE(LeaderCyclePreVoteTest) { testLeaderCycle(true); }

static void testLeaderElectionOverwriteNewerLogs(bool preVote) {
    std::function<void(snail::raft::Raft::Config*)> cfg;
    if (preVote) {
        cfg = preVoteConfig;
    }

    auto n = newNetworkWithConfig(
        cfg, {entsWithConfig(cfg, {1}), entsWithConfig(cfg, {1}),
              entsWithConfig(cfg, {2}), votedWithConfig(cfg, 3, 2),
              votedWithConfig(cfg, 3, 2)});
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    n->Send({m}).get();

    auto sm1 = seastar::dynamic_pointer_cast<RaftSM>(n->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm1->raft_->state_,
                        snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(sm1->raft_->term_, 2);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    n->Send({m}).get();
    BOOST_REQUIRE_EQUAL(sm1->raft_->state_,
                        snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(sm1->raft_->term_, 3);

    for (auto iter : n->peers_) {
        auto sm = seastar::dynamic_pointer_cast<RaftSM>(iter.second);
        auto entries = sm->raft_->raft_log_->AllEntries().get0();
        BOOST_REQUIRE_EQUAL(entries.size(), 2);
        BOOST_REQUIRE_EQUAL(entries[0]->term(), 1);
        BOOST_REQUIRE_EQUAL(entries[1]->term(), 3);
    }
}

SEASTAR_THREAD_TEST_CASE(LeaderElectionOverwriteNewerLogsTest) {
    testLeaderElectionOverwriteNewerLogs(false);
}

SEASTAR_THREAD_TEST_CASE(LeaderElectionOverwriteNewerLogsPreVoteTest) {
    testLeaderElectionOverwriteNewerLogs(true);
}

static void testVoteFromAnyState(snail::raft::MessageType vt) {
    for (int st = (int)snail::raft::RaftState::StateFollower;
         st <= (int)snail::raft::RaftState::StatePreCandidate; st++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
        auto raft = newTestRaft(1, 10, 1, store).get0();
        raft->term_ = 1;

        switch ((snail::raft::RaftState)st) {
            case snail::raft::RaftState::StateFollower:
                raft->BecomeFollower(raft->term_, 3);
                break;
            case snail::raft::RaftState::StatePreCandidate:
                raft->BecomePreCandidate();
                break;
            case snail::raft::RaftState::StateCandidate:
                raft->BecomeCandidate();
                break;
            case snail::raft::RaftState::StateLeader:
                raft->BecomeCandidate();
                raft->BecomeLeader().get();
                break;
        }

        uint64_t origTerm = raft->term_;
        uint64_t newTerm = raft->term_ + 1;

        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = vt;
        m->term = newTerm;
        m->log_term = newTerm;
        m->index = 42;
        auto s = raft->Step(m).get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(raft->msgs_.size(), 1);
        BOOST_REQUIRE_EQUAL(raft->msgs_[0]->type,
                            (vt == snail::raft::MessageType::MsgVote
                                 ? snail::raft::MessageType::MsgVoteResp
                                 : snail::raft::MessageType::MsgPreVoteResp));
        BOOST_REQUIRE(!raft->msgs_[0]->reject);
        if (vt == snail::raft::MessageType::MsgVote) {
            BOOST_REQUIRE_EQUAL(raft->state_,
                                snail::raft::RaftState::StateFollower);
            BOOST_REQUIRE_EQUAL(raft->term_, newTerm);
            BOOST_REQUIRE_EQUAL(raft->vote_, 2);
        } else {
            BOOST_REQUIRE_EQUAL(raft->state_, (snail::raft::RaftState)st);
            BOOST_REQUIRE_EQUAL(raft->term_, origTerm);
            BOOST_REQUIRE(!(raft->vote_ != 0 && raft->vote_ != 1));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(VoteFromAnyStateTest) {
    testVoteFromAnyState(snail::raft::MessageType::MsgVote);
}

SEASTAR_THREAD_TEST_CASE(PreVoteFromAnyStateTest) {
    testVoteFromAnyState(snail::raft::MessageType::MsgPreVote);
}

static std::vector<snail::raft::EntryPtr> nextEnts(
    snail::raft::RaftPtr r, seastar::shared_ptr<snail::raft::MemoryStorage> s) {
    s->Append(r->raft_log_->UnstableEntries());
    r->raft_log_->StableTo(r->raft_log_->LastIndex(),
                           r->raft_log_->LastTerm().get0());
    auto ents = r->raft_log_->NextEnts().get0();
    r->raft_log_->AppliedTo(r->raft_log_->committed());
    return ents;
}

SEASTAR_THREAD_TEST_CASE(LogReplicationTest) {
    struct testItem {
        NetworkPtr net;
        std::vector<snail::raft::MessagePtr> msgs;
        std::vector<snail::raft::MessagePtr> wmsgs;
        uint64_t wcommitted;
    };

    testItem tests[2] = {{newNetwork({nullptr, nullptr, nullptr}), {}, {}, 2},
                         {newNetwork({nullptr, nullptr, nullptr}), {}, {}, 4}};
    seastar::temporary_buffer<char> data("somedata", 8);
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    auto ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tests[0].msgs.push_back(m);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tests[0].wmsgs.push_back(m);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tests[1].msgs.push_back(m);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tests[1].wmsgs.push_back(m);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgHup;
    tests[1].msgs.push_back(m);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgHup;
    tests[1].wmsgs.push_back(m);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgProp;
    ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tests[1].msgs.push_back(m);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgProp;
    ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tests[1].wmsgs.push_back(m);

    for (int i = 0; i < 2; i++) {
        m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgHup;
        tests[i].net->Send({m}).get();
        for (auto msg : tests[i].msgs) {
            tests[i].net->Send({msg}).get();
        }

        for (auto iter : tests[i].net->peers_) {
            auto id = iter.first;
            auto sm = seastar::dynamic_pointer_cast<RaftSM>(iter.second);
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(),
                                tests[i].wcommitted);

            std::vector<snail::raft::EntryPtr> ents;
            auto entries = nextEnts(sm->raft_, tests[i].net->storage_[id]);
            for (auto e : entries) {
                if (!e->data().empty()) {
                    ents.push_back(e);
                }
            }
            std::vector<snail::raft::MessagePtr> props;
            for (auto msg : tests[i].wmsgs) {
                if (msg->type == snail::raft::MessageType::MsgProp) {
                    props.push_back(msg);
                }
            }
            BOOST_REQUIRE_EQUAL(ents.size(), props.size());

            for (int j = 0; j < props.size(); j++) {
                BOOST_REQUIRE(ents[j]->data() == props[j]->entries[0]->data());
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(LearnerLogReplicationTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto n1 = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto n2 = newTestRaft(2, 10, 1, store).get0();

    auto nt = newNetwork(
        {RaftSM::MakeStateMachine(n1), RaftSM::MakeStateMachine(n2)});
    n1->BecomeFollower(1, 0);
    n2->BecomeFollower(1, 0);

    n1->randomized_election_timeout_ = n1->election_timeout_;
    for (int i = 0; i < n1->election_timeout_; i++) {
        n1->tick_().get();
    }

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgBeat;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE(n2->is_learner_);

    uint64_t nextCommitted = n1->raft_log_->committed() + 1;
    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    auto ent = snail::raft::make_entry();
    seastar::temporary_buffer<char> data("somedata", 8);
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(n1->raft_log_->committed(), nextCommitted);
    BOOST_REQUIRE_EQUAL(n1->raft_log_->committed(), n2->raft_log_->committed());
    BOOST_REQUIRE_EQUAL(n2->raft_log_->committed(),
                        n1->prs_->progress()[2]->match());
}

SEASTAR_THREAD_TEST_CASE(SingleNodeCommitTest) {
    auto tt = newNetwork({nullptr});
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    for (int i = 0; i < 2; i++) {
        m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("some data", 9);
        ent->set_data(std::move(data.share()));
        m->entries.push_back(ent);
        tt->Send({m}).get();
    }

    auto sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(), 3);
}

SEASTAR_THREAD_TEST_CASE(CannotCommitWithoutNewTermEntryTest) {
    auto tt = newNetwork({nullptr, nullptr, nullptr, nullptr, nullptr});
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    tt->Cut(1, 3);
    tt->Cut(1, 4);
    tt->Cut(1, 5);

    for (int i = 0; i < 2; i++) {
        m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("some data", 9);
        ent->set_data(std::move(data.share()));
        m->entries.push_back(ent);
        tt->Send({m}).get();
    }

    auto sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(), 1);
    tt->Recover();
    tt->Ignore(snail::raft::MessageType::MsgApp);

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[2]);
    BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(), 1);
    tt->Recover();

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgBeat;
    tt->Send({m}).get();

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgProp;
    auto ent = snail::raft::make_entry();
    seastar::temporary_buffer<char> data("some data", 9);
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(), 5);
}

SEASTAR_THREAD_TEST_CASE(CommitWithoutNewTermEntryTest) {
    auto tt = newNetwork({nullptr, nullptr, nullptr, nullptr, nullptr});
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    tt->Cut(1, 3);
    tt->Cut(1, 4);
    tt->Cut(1, 5);

    for (int i = 0; i < 2; i++) {
        m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("some data", 9);
        ent->set_data(std::move(data.share()));
        m->entries.push_back(ent);
        tt->Send({m}).get();
    }

    auto sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(), 1);
    tt->Recover();

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(), 4);
}

SEASTAR_THREAD_TEST_CASE(DuelingCandidatesTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto b = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto c = newTestRaft(3, 10, 1, store).get0();

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});
    nt->Cut(1, 3);

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    auto sm = seastar::dynamic_pointer_cast<RaftSM>(nt->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm->raft_->state_, snail::raft::RaftState::StateLeader);

    sm = seastar::dynamic_pointer_cast<RaftSM>(nt->peers_[3]);
    BOOST_REQUIRE_EQUAL(sm->raft_->state_,
                        snail::raft::RaftState::StateCandidate);

    nt->Recover();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    auto ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ptr->ents_.push_back(ent);
    auto wlog = snail::raft::RaftLog::MakeRaftLog(store, 0);
    wlog->set_committed(1);
    wlog->unstable_.set_offset(2);

    struct testItem {
        snail::raft::RaftPtr sm;
        snail::raft::RaftState state;
        uint64_t term;
        snail::raft::RaftLogPtr raftLog;
    };

    testItem tests[3] = {
        {a, snail::raft::RaftState::StateFollower, 2, wlog},
        {b, snail::raft::RaftState::StateFollower, 2, wlog},
        {c, snail::raft::RaftState::StateFollower, 2,
         snail::raft::RaftLog::MakeRaftLog(
             newTestMemoryStorage(), std::numeric_limits<uint64_t>::max())}};

    for (int i = 0; i < 3; i++) {
        BOOST_REQUIRE_EQUAL(tests[i].sm->state_, tests[i].state);
        BOOST_REQUIRE_EQUAL(tests[i].sm->term_, tests[i].term);
        auto p = nt->peers_[i + 1];
        if (p && p->Type() == StateMachineType::RaftStateMachine) {
            sm = seastar::dynamic_pointer_cast<RaftSM>(p);
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(),
                                tests[i].raftLog->committed());
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->applied(),
                                tests[i].raftLog->applied());
            auto ents1 = sm->raft_->raft_log_->AllEntries().get0();
            auto ents2 = tests[i].raftLog->AllEntries().get0();
            BOOST_REQUIRE_EQUAL(ents1.size(), ents2.size());
            for (int j = 0; j < ents1.size(); j++) {
                BOOST_REQUIRE(*ents1[j] == *ents2[j]);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(DuelingPreCandidatesTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto cfgA = newTestConfig(1, 10, 1, store);

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto cfgB = newTestConfig(2, 10, 1, store);

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto cfgC = newTestConfig(3, 10, 1, store);

    cfgA.pre_vote = true;
    cfgB.pre_vote = true;
    cfgC.pre_vote = true;

    auto a = seastar::make_lw_shared<snail::raft::Raft>();
    a->Init(cfgA).get();

    auto b = seastar::make_lw_shared<snail::raft::Raft>();
    b->Init(cfgB).get();

    auto c = seastar::make_lw_shared<snail::raft::Raft>();
    c->Init(cfgC).get();

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});
    nt->Cut(1, 3);
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    auto sm = seastar::dynamic_pointer_cast<RaftSM>(nt->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm->raft_->state_, snail::raft::RaftState::StateLeader);

    sm = seastar::dynamic_pointer_cast<RaftSM>(nt->peers_[3]);
    BOOST_REQUIRE_EQUAL(sm->raft_->state_,
                        snail::raft::RaftState::StateFollower);

    nt->Recover();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    auto ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ptr->ents_.push_back(ent);
    auto wlog = snail::raft::RaftLog::MakeRaftLog(store, 0);
    wlog->set_committed(1);
    wlog->unstable_.set_offset(2);

    struct testItem {
        snail::raft::RaftPtr sm;
        snail::raft::RaftState state;
        uint64_t term;
        snail::raft::RaftLogPtr raftLog;
    };

    testItem tests[3] = {
        {a, snail::raft::RaftState::StateLeader, 1, wlog},
        {b, snail::raft::RaftState::StateFollower, 1, wlog},
        {c, snail::raft::RaftState::StateFollower, 1,
         snail::raft::RaftLog::MakeRaftLog(
             newTestMemoryStorage(), std::numeric_limits<uint64_t>::max())}};
    for (int i = 0; i < 3; i++) {
        BOOST_REQUIRE_EQUAL(tests[i].sm->state_, tests[i].state);
        BOOST_REQUIRE_EQUAL(tests[i].sm->term_, tests[i].term);
        auto p = nt->peers_[i + 1];
        if (p && p->Type() == StateMachineType::RaftStateMachine) {
            sm = seastar::dynamic_pointer_cast<RaftSM>(p);
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(),
                                tests[i].raftLog->committed());
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->applied(),
                                tests[i].raftLog->applied());
            auto ents1 = sm->raft_->raft_log_->AllEntries().get0();
            auto ents2 = tests[i].raftLog->AllEntries().get0();
            BOOST_REQUIRE_EQUAL(ents1.size(), ents2.size());
            for (int j = 0; j < ents1.size(); j++) {
                BOOST_REQUIRE(*ents1[j] == *ents2[j]);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(CandidateConcedeTest) {
    auto tt = newNetwork({nullptr, nullptr, nullptr});
    tt->Isolate(1);

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    tt->Recover();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgBeat;
    tt->Send({m}).get();

    seastar::temporary_buffer<char> data("force follower", 14);
    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgProp;
    auto ent = snail::raft::make_entry();
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    tt->Send({m}).get();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgBeat;
    tt->Send({m}).get();

    auto a = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
    BOOST_REQUIRE_EQUAL(a->raft_->state_,
                        snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(a->raft_->term_, 1);

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ptr->ents_.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(1);
    ent->set_data(std::move(data.share()));
    ptr->ents_.push_back(ent);

    auto wlog = snail::raft::RaftLog::MakeRaftLog(store, 0);
    wlog->set_committed(2);
    wlog->unstable_.set_offset(3);

    for (auto iter : tt->peers_) {
        auto p = iter.second;
        if (p && p->Type() == StateMachineType::RaftStateMachine) {
            auto sm = seastar::dynamic_pointer_cast<RaftSM>(p);
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(),
                                wlog->committed());
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->applied(),
                                wlog->applied());
            auto ents1 = sm->raft_->raft_log_->AllEntries().get0();
            auto ents2 = wlog->AllEntries().get0();
            BOOST_REQUIRE_EQUAL(ents1.size(), ents2.size());
            for (int j = 0; j < ents1.size(); j++) {
                BOOST_REQUIRE(*ents1[j] == *ents2[j]);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(SingleNodeCandidateTest) {
    auto tt = newNetwork({nullptr});

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    auto sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm->raft_->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(SingleNodePreCandidateTest) {
    auto tt = newNetworkWithConfig(preVoteConfig, {nullptr});
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    auto sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
    BOOST_REQUIRE_EQUAL(sm->raft_->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(OldMessagesTest) {
    auto tt = newNetwork({nullptr, nullptr, nullptr});

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 2;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    tt->Send({m}).get();

    auto ent = snail::raft::make_entry();
    m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgApp;
    m->term = 2;
    m->entries.push_back(ent);
    ent->set_index(3);
    ent->set_term(2);
    tt->Send({m}).get();

    seastar::temporary_buffer<char> data("somedata", 8);
    ent = snail::raft::make_entry();
    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    m->entries.push_back(ent);
    ent->set_data(std::move(data.share()));
    tt->Send({m}).get();

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ptr->ents_.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(2);
    ptr->ents_.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(3);
    ent->set_term(3);
    ptr->ents_.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(4);
    ent->set_term(3);
    ent->set_data(data.share());
    ptr->ents_.push_back(ent);

    auto wlog = snail::raft::RaftLog::MakeRaftLog(store, 0);
    wlog->set_committed(4);
    wlog->unstable_.set_offset(5);

    for (auto iter : tt->peers_) {
        auto p = iter.second;
        if (p && p->Type() == StateMachineType::RaftStateMachine) {
            auto sm = seastar::dynamic_pointer_cast<RaftSM>(p);
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(),
                                wlog->committed());
            BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->applied(),
                                wlog->applied());
            auto ents1 = sm->raft_->raft_log_->AllEntries().get0();
            auto ents2 = wlog->AllEntries().get0();
            BOOST_REQUIRE_EQUAL(ents1.size(), ents2.size());
            for (int j = 0; j < ents1.size(); j++) {
                BOOST_REQUIRE(*ents1[j] == *ents2[j]);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(ProposalTest) {
    struct testItem {
        NetworkPtr tt;
        bool success;
    };

    testItem tests[5] = {
        {newNetwork({nullptr, nullptr, nullptr}), true},
        {newNetwork({nullptr, nullptr, nopStepper}), true},
        {newNetwork({nullptr, nopStepper, nopStepper}), false},
        {newNetwork({nullptr, nopStepper, nopStepper, nullptr}), false},
        {newNetwork({nullptr, nopStepper, nopStepper, nullptr, nullptr}),
         true}};
    for (int i = 0; i < 5; i++) {
        auto tt = tests[i].tt;
        auto send = [tt,
                     success = tests[i].success](snail::raft::MessagePtr m) {
            try {
                tt->Send({m}).get();
            } catch (...) {
                BOOST_REQUIRE(!success);
            }
        };
        seastar::temporary_buffer<char> data("somedata", 8);

        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgHup;
        send(m);

        auto ent = snail::raft::make_entry();
        m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        m->entries.push_back(ent);
        ent->set_data(data.share());
        send(m);

        auto wlog = snail::raft::RaftLog::MakeRaftLog(
            newTestMemoryStorage(), std::numeric_limits<uint64_t>::max());
        if (tests[i].success) {
            auto store = newTestMemoryStorage();
            auto ptr =
                seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(
                    store);
            ent = snail::raft::make_entry();
            ent->set_index(1);
            ent->set_term(1);
            ptr->ents_.push_back(ent);
            ent = snail::raft::make_entry();
            ent->set_index(2);
            ent->set_term(1);
            ent->set_data(data.share());
            ptr->ents_.push_back(ent);

            wlog = snail::raft::RaftLog::MakeRaftLog(store, 0);
            wlog->set_committed(2);
            wlog->unstable_.set_offset(3);
        }

        for (auto iter : tt->peers_) {
            auto p = iter.second;
            if (p && p->Type() == StateMachineType::RaftStateMachine) {
                auto sm = seastar::dynamic_pointer_cast<RaftSM>(p);
                BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(),
                                    wlog->committed());
                BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->applied(),
                                    wlog->applied());
                auto ents1 = sm->raft_->raft_log_->AllEntries().get0();
                auto ents2 = wlog->AllEntries().get0();
                BOOST_REQUIRE_EQUAL(ents1.size(), ents2.size());
                for (int j = 0; j < ents1.size(); j++) {
                    BOOST_REQUIRE(*ents1[j] == *ents2[j]);
                }
            }
        }

        auto sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
        BOOST_REQUIRE_EQUAL(sm->raft_->term_, 1);
    }
}

SEASTAR_THREAD_TEST_CASE(ProposalByProxyTest) {
    seastar::temporary_buffer<char> data("somedata", 8);
    std::array<NetworkPtr, 2> tests = {
        newNetwork({nullptr, nullptr, nullptr}),
        newNetwork({nullptr, nullptr, nopStepper})};

    for (int i = 0; i < 2; i++) {
        auto tt = tests[i];

        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgHup;
        tt->Send({m}).get();

        auto ent = snail::raft::make_entry();
        m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 2;
        m->type = snail::raft::MessageType::MsgProp;
        m->entries.push_back(ent);
        ent->set_data(data.share());
        tt->Send({m}).get();

        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ent = snail::raft::make_entry();
        ent->set_index(1);
        ent->set_term(1);
        ptr->ents_.push_back(ent);
        ent = snail::raft::make_entry();
        ent->set_index(2);
        ent->set_term(1);
        ent->set_data(data.share());
        ptr->ents_.push_back(ent);

        auto wlog = snail::raft::RaftLog::MakeRaftLog(store, 0);
        wlog->set_committed(2);
        wlog->unstable_.set_offset(3);
        for (auto iter : tt->peers_) {
            auto p = iter.second;
            if (p && p->Type() == StateMachineType::RaftStateMachine) {
                auto sm = seastar::dynamic_pointer_cast<RaftSM>(p);
                BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->committed(),
                                    wlog->committed());
                BOOST_REQUIRE_EQUAL(sm->raft_->raft_log_->applied(),
                                    wlog->applied());
                auto ents1 = sm->raft_->raft_log_->AllEntries().get0();
                auto ents2 = wlog->AllEntries().get0();
                BOOST_REQUIRE_EQUAL(ents1.size(), ents2.size());
                for (int j = 0; j < ents1.size(); j++) {
                    BOOST_REQUIRE(*ents1[j] == *ents2[j]);
                }
            }
        }

        auto sm = seastar::dynamic_pointer_cast<RaftSM>(tt->peers_[1]);
        BOOST_REQUIRE_EQUAL(sm->raft_->term_, 1);
    }
}

SEASTAR_THREAD_TEST_CASE(CommitTest) {
    struct testItem {
        std::vector<uint64_t> matches;
        std::vector<snail::raft::EntryPtr> logs;
        uint64_t smTerm;
        uint64_t w;
    };

    std::vector<snail::raft::EntryPtr> ents;
    for (int i = 0; i < 3; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(1);
        ent->set_term(1);
        ents.push_back(ent);
    }
    auto ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(2);
    ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(2);
    ents.push_back(ent);

    for (int i = 0; i < 5; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(1);
        ent->set_term(1);
        ents.push_back(ent);
        ent = snail::raft::make_entry();
        ent->set_index(2);
        ent->set_term(2);
        ents.push_back(ent);
        ent = snail::raft::make_entry();
        ent->set_index(1);
        ent->set_term(1);
        ents.push_back(ent);
        ent = snail::raft::make_entry();
        ent->set_index(2);
        ent->set_term(1);
        ents.push_back(ent);
    }

    testItem tests[14] = {{{1}, {ents[0]}, 1, 1},
                          {{1}, {ents[1]}, 2, 0},
                          {{2}, {ents[2], ents[3]}, 2, 2},
                          {{1}, {ents[4]}, 2, 1},

                          {{2, 1, 1}, {ents[5], ents[6]}, 1, 1},
                          {{2, 1, 1}, {ents[7], ents[8]}, 2, 0},
                          {{2, 1, 2}, {ents[9], ents[10]}, 2, 2},
                          {{2, 1, 2}, {ents[11], ents[12]}, 2, 0},

                          {{2, 1, 1, 1}, {ents[13], ents[14]}, 1, 1},
                          {{2, 1, 1, 1}, {ents[15], ents[16]}, 2, 0},
                          {{2, 1, 1, 2}, {ents[17], ents[18]}, 1, 1},
                          {{2, 1, 1, 2}, {ents[19], ents[20]}, 2, 0},
                          {{2, 1, 2, 2}, {ents[21], ents[22]}, 2, 2},
                          {{2, 1, 2, 2}, {ents[23], ents[24]}, 2, 0}};

    for (int i = 0; i < 14; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1});
        ptr->Append(tests[i].logs);
        ptr->hs_.set_term(tests[i].smTerm);

        auto raft = newTestRaft(1, 10, 2, store).get0();
        for (int j = 0; j < tests[i].matches.size(); j++) {
            auto id = (uint64_t)(j + 1);
            if (id > 1) {
                snail::raft::ConfChange v1;
                v1.set_type(snail::raft::ConfChangeType::ConfChangeAddNode);
                v1.set_node_id(id);
                snail::raft::ConfChangeI cc(v1);
                raft->ApplyConfChange(cc.AsV2()).get();
            }
            auto pr = raft->prs_->progress()[id];
            pr->set_match(tests[i].matches[j]);
            pr->set_next(tests[i].matches[j] + 1);
        }
        raft->MaybeCommit().get();
        BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), tests[i].w);
    }
}

SEASTAR_THREAD_TEST_CASE(PastElectionTimeoutTest) {
    struct testItem {
        int elapse;
        float wprobability;
        bool round;
    };

    testItem tests[6] = {{5, 0, false},   {10, 0.1, true}, {13, 0.4, true},
                         {15, 0.6, true}, {18, 0.9, true}, {20, 1, false}};

    for (int i = 0; i < 6; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1});
        auto raft = newTestRaft(1, 10, 1, store).get0();
        raft->election_elapsed_ = tests[i].elapse;
        int c = 0;
        for (int j = 0; j < 10000; j++) {
            raft->ResetRandomizedElectionTimeout();
            if (raft->PastElectionTimeout()) {
                c++;
            }
        }

        auto got = (float)(c) / 10000.0;
        if (tests[i].round) {
            got = floor(got * 10 + 0.5) / 10.0;
        }
        BOOST_CHECK_CLOSE_FRACTION(got, tests[i].wprobability, 0.1);
    }
}

SEASTAR_THREAD_TEST_CASE(StepIgnoreOldTermMsgTest) {
    bool called = false;
    auto fakeStep =
        [&called](snail::raft::MessagePtr m) -> seastar::future<Status<>> {
        called = true;
        return seastar::make_ready_future<Status<>>();
    };

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    auto raft = newTestRaft(1, 10, 1, store).get0();
    raft->step_ = fakeStep;
    raft->term_ = 2;
    auto m = snail::raft::make_raft_message();
    m->type = snail::raft::MessageType::MsgApp;
    m->term = raft->term_ - 1;
    raft->Step(m).get();
    BOOST_REQUIRE(!called);
}

SEASTAR_THREAD_TEST_CASE(HandleMsgAppTest) {
    struct testItem {
        snail::raft::MessagePtr m;
        uint64_t windex;
        uint64_t wcommit;
        bool wreject;
    };

    testItem tests[11] = {{snail::raft::make_raft_message(), 2, 0, true},
                          {snail::raft::make_raft_message(), 2, 0, true},
                          {snail::raft::make_raft_message(), 2, 1, false},
                          {snail::raft::make_raft_message(), 1, 1, false},
                          {snail::raft::make_raft_message(), 4, 3, false},
                          {snail::raft::make_raft_message(), 3, 3, false},
                          {snail::raft::make_raft_message(), 2, 2, false},
                          {snail::raft::make_raft_message(), 2, 1, false},
                          {snail::raft::make_raft_message(), 2, 2, false},
                          {snail::raft::make_raft_message(), 2, 2, false},
                          {snail::raft::make_raft_message(), 2, 2, false}};
    snail::raft::EntryPtr ent;

    tests[0].m->type = snail::raft::MessageType::MsgApp;
    tests[0].m->term = 2;
    tests[0].m->log_term = 3;
    tests[0].m->index = 2;
    tests[0].m->commit = 3;
    tests[1].m->type = snail::raft::MessageType::MsgApp;
    tests[1].m->term = 2;
    tests[1].m->log_term = 3;
    tests[1].m->index = 3;
    tests[1].m->commit = 3;
    tests[2].m->type = snail::raft::MessageType::MsgApp;
    tests[2].m->term = 2;
    tests[2].m->log_term = 1;
    tests[2].m->index = 1;
    tests[2].m->commit = 1;
    tests[3].m->type = snail::raft::MessageType::MsgApp;
    tests[3].m->term = 2;
    tests[3].m->log_term = 0;
    tests[3].m->index = 0;
    tests[3].m->commit = 1;
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(2);
    tests[3].m->entries.push_back(ent);
    tests[4].m->type = snail::raft::MessageType::MsgApp;
    tests[4].m->term = 2;
    tests[4].m->log_term = 2;
    tests[4].m->index = 2;
    tests[4].m->commit = 3;
    ent = snail::raft::make_entry();
    ent->set_index(3);
    ent->set_term(2);
    tests[4].m->entries.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(4);
    ent->set_term(2);
    tests[4].m->entries.push_back(ent);
    tests[5].m->type = snail::raft::MessageType::MsgApp;
    tests[5].m->term = 2;
    tests[5].m->log_term = 2;
    tests[5].m->index = 2;
    tests[5].m->commit = 4;
    ent = snail::raft::make_entry();
    ent->set_index(3);
    ent->set_term(2);
    tests[5].m->entries.push_back(ent);
    tests[6].m->type = snail::raft::MessageType::MsgApp;
    tests[6].m->term = 2;
    tests[6].m->log_term = 1;
    tests[6].m->index = 1;
    tests[6].m->commit = 4;
    ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(2);
    tests[6].m->entries.push_back(ent);
    tests[7].m->type = snail::raft::MessageType::MsgApp;
    tests[7].m->term = 1;
    tests[7].m->log_term = 1;
    tests[7].m->index = 1;
    tests[7].m->commit = 3;
    tests[8].m->type = snail::raft::MessageType::MsgApp;
    tests[8].m->term = 1;
    tests[8].m->log_term = 1;
    tests[8].m->index = 1;
    tests[8].m->commit = 3;
    ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(2);
    tests[8].m->entries.push_back(ent);
    tests[9].m->type = snail::raft::MessageType::MsgApp;
    tests[9].m->term = 2;
    tests[9].m->log_term = 2;
    tests[9].m->index = 2;
    tests[9].m->commit = 3;
    tests[10].m->type = snail::raft::MessageType::MsgApp;
    tests[10].m->term = 2;
    tests[10].m->log_term = 2;
    tests[10].m->index = 2;
    tests[10].m->commit = 4;

    for (int i = 0; i < 11; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1});
        std::vector<snail::raft::EntryPtr> ents;
        for (int j = 0; j < 2; j++) {
            ent = snail::raft::make_entry();
            ent->set_index(j + 1);
            ent->set_term(j + 1);
            ents.push_back(ent);
        }
        ptr->Append(ents);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        raft->BecomeFollower(2, 0);
        raft->HandleAppendEntries(tests[i].m).get();
        BOOST_REQUIRE_EQUAL(raft->raft_log_->LastIndex(), tests[i].windex);
        BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), tests[i].wcommit);
        auto msgs = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(msgs.size(), 1);
        BOOST_REQUIRE_EQUAL(msgs[0]->reject, tests[i].wreject);
    }
}

SEASTAR_THREAD_TEST_CASE(HandleHeartbeatTest) {
    uint64_t commit = 2;
    struct testItem {
        snail::raft::MessagePtr m;
        uint64_t wcommit;
    };

    testItem tests[2] = {
        {snail::raft::make_raft_message(), commit + 1},
        {snail::raft::make_raft_message(), commit},
    };

    tests[0].m->from = 2;
    tests[0].m->to = 1;
    tests[0].m->type = snail::raft::MessageType::MsgHeartbeat;
    tests[0].m->term = 2;
    tests[0].m->commit = commit + 1;
    tests[1].m->from = 2;
    tests[1].m->to = 1;
    tests[1].m->type = snail::raft::MessageType::MsgHeartbeat;
    tests[1].m->term = 2;
    tests[1].m->commit = commit - 1;

    for (int i = 0; i < 2; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1, 2});
        std::vector<snail::raft::EntryPtr> ents;
        for (int j = 0; j < 3; j++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(j + 1);
            ent->set_term(j + 1);
            ents.push_back(ent);
        }
        ptr->Append(ents);
        auto raft = newTestRaft(1, 5, 1, store).get0();
        raft->BecomeFollower(2, 2);
        raft->raft_log_->CommitTo(commit);
        raft->HandleHeartbeat(tests[i].m);
        BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), tests[i].wcommit);
        auto msgs = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(msgs.size(), 1);
        BOOST_REQUIRE_EQUAL(msgs[0]->type,
                            snail::raft::MessageType::MsgHeartbeatResp);
    }
}

SEASTAR_THREAD_TEST_CASE(HandleHeartbeatRespTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2});
    std::vector<snail::raft::EntryPtr> ents;
    for (int j = 0; j < 3; j++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(j + 1);
        ent->set_term(j + 1);
        ents.push_back(ent);
    }
    ptr->Append(ents);
    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    raft->raft_log_->CommitTo(raft->raft_log_->LastIndex());

    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->type = snail::raft::MessageType::MsgHeartbeatResp;
    raft->Step(m).get();
    auto msgs = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(msgs.size(), 1);
    BOOST_REQUIRE_EQUAL(msgs[0]->type, snail::raft::MessageType::MsgApp);

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->type = snail::raft::MessageType::MsgHeartbeatResp;
    raft->Step(m).get();
    msgs = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(msgs.size(), 1);
    BOOST_REQUIRE_EQUAL(msgs[0]->type, snail::raft::MessageType::MsgApp);

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->type = snail::raft::MessageType::MsgAppResp;
    m->index = msgs[0]->index + msgs[0]->entries.size();
    raft->Step(m).get();
    msgs = std::move(raft->msgs_);

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->type = snail::raft::MessageType::MsgHeartbeatResp;
    raft->Step(m).get();
    msgs = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(msgs.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(RaftFreesReadOnlyMemTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2});
    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    raft->raft_log_->CommitTo(raft->raft_log_->LastIndex());

    seastar::temporary_buffer<char> ctx("ctx", 3);
    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->type = snail::raft::MessageType::MsgReadIndex;
    auto ent = snail::raft::make_entry();
    ent->set_data(ctx.share());
    m->entries.push_back(ent);
    raft->Step(m).get();

    auto msgs = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(msgs.size(), 1);
    BOOST_REQUIRE_EQUAL(msgs[0]->type, snail::raft::MessageType::MsgHeartbeat);
    BOOST_REQUIRE(msgs[0]->context == ctx);
    BOOST_REQUIRE_EQUAL(raft->read_only_->read_index_queue_.size(), 1);
    BOOST_REQUIRE_EQUAL(raft->read_only_->pending_read_index_.size(), 1);
    auto iter = raft->read_only_->pending_read_index_.find(
        seastar::sstring(ctx.get(), ctx.size()));
    BOOST_REQUIRE(iter != raft->read_only_->pending_read_index_.end());

    m = snail::raft::make_raft_message();
    m->from = 2;
    m->type = snail::raft::MessageType::MsgHeartbeatResp;
    m->context = ctx.share();
    raft->Step(m).get();

    BOOST_REQUIRE_EQUAL(raft->read_only_->read_index_queue_.size(), 0);
    BOOST_REQUIRE_EQUAL(raft->read_only_->pending_read_index_.size(), 0);
    iter = raft->read_only_->pending_read_index_.find(
        seastar::sstring(ctx.get(), ctx.size()));
    BOOST_REQUIRE(iter == raft->read_only_->pending_read_index_.end());
}

SEASTAR_THREAD_TEST_CASE(MsgAppRespWaitResetTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    raft->BcastAppend().get();
    auto msgs = std::move(raft->msgs_);

    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->type = snail::raft::MessageType::MsgAppResp;
    m->index = 1;
    raft->Step(m).get();
    BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), 1);
    msgs = std::move(raft->msgs_);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->type = snail::raft::MessageType::MsgProp;
    m->entries.push_back(snail::raft::make_entry());
    raft->Step(m).get();
    msgs = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(msgs.size(), 1);
    BOOST_REQUIRE(!(msgs[0]->type != snail::raft::MessageType::MsgApp ||
                    msgs[0]->to != 2));

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->type = snail::raft::MessageType::MsgAppResp;
    m->index = 1;
    raft->Step(m).get();

    msgs = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(msgs.size(), 1);
    BOOST_REQUIRE(!(msgs[0]->type != snail::raft::MessageType::MsgApp ||
                    msgs[0]->to != 3));
    BOOST_REQUIRE(
        !(msgs[0]->entries.size() != 1 || msgs[0]->entries[0]->index() != 2));
}

static void testRecvMsgVote(snail::raft::MessageType msgType) {
    struct testItem {
        snail::raft::RaftState state;
        uint64_t index;
        uint64_t logTerm;
        uint64_t voteFor;
        bool wreject;
    };

    testItem tests[21] = {
        {snail::raft::RaftState::StateFollower, 0, 0, 0, true},
        {snail::raft::RaftState::StateFollower, 0, 1, 0, true},
        {snail::raft::RaftState::StateFollower, 0, 2, 0, true},
        {snail::raft::RaftState::StateFollower, 0, 3, 0, false},

        {snail::raft::RaftState::StateFollower, 1, 0, 0, true},
        {snail::raft::RaftState::StateFollower, 1, 1, 0, true},
        {snail::raft::RaftState::StateFollower, 1, 2, 0, true},
        {snail::raft::RaftState::StateFollower, 1, 3, 0, false},

        {snail::raft::RaftState::StateFollower, 2, 0, 0, true},
        {snail::raft::RaftState::StateFollower, 2, 1, 0, true},
        {snail::raft::RaftState::StateFollower, 2, 2, 0, false},
        {snail::raft::RaftState::StateFollower, 2, 3, 0, false},

        {snail::raft::RaftState::StateFollower, 3, 0, 0, true},
        {snail::raft::RaftState::StateFollower, 3, 1, 0, true},
        {snail::raft::RaftState::StateFollower, 3, 2, 0, false},
        {snail::raft::RaftState::StateFollower, 3, 3, 0, false},

        {snail::raft::RaftState::StateFollower, 3, 2, 2, false},
        {snail::raft::RaftState::StateFollower, 3, 2, 1, true},

        {snail::raft::RaftState::StateLeader, 3, 3, 1, true},
        {snail::raft::RaftState::StatePreCandidate, 3, 3, 1, true},
        {snail::raft::RaftState::StateCandidate, 3, 3, 1, true},
    };

    for (int i = 0; i < 21; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1});
        auto raft = newTestRaft(1, 10, 1, store).get0();

        raft->state_ = tests[i].state;
        switch (tests[i].state) {
            case snail::raft::RaftState::StateFollower:
                raft->step_ = [raft](snail::raft::MessagePtr m)
                    -> seastar::future<Status<>> {
                    return raft->StepFollower(m);
                };
                break;
            case snail::raft::RaftState::StateCandidate:
            case snail::raft::RaftState::StatePreCandidate:
                raft->step_ = [raft](snail::raft::MessagePtr m)
                    -> seastar::future<Status<>> {
                    return raft->StepCandidate(m);
                };
                break;
            case snail::raft::RaftState::StateLeader:
                raft->step_ = [raft](snail::raft::MessagePtr m)
                    -> seastar::future<Status<>> {
                    return raft->StepLeader(m);
                };
                break;
        }
        raft->vote_ = tests[i].voteFor;
        store = newTestMemoryStorage();
        ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        for (int j = 0; j < 2; j++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(j + 1);
            ent->set_term(2);
            ptr->ents_.push_back(ent);
        }
        raft->raft_log_ = snail::raft::RaftLog::MakeRaftLog(
            store, 0, raft->group_, raft->id_);
        raft->raft_log_->unstable_.set_offset(3);
        auto term =
            std::max(raft->raft_log_->LastTerm().get0(), tests[i].logTerm);
        raft->term_ = term;

        auto m = snail::raft::make_raft_message();
        m->type = msgType;
        m->term = term;
        m->from = 2;
        m->index = tests[i].index;
        m->log_term = tests[i].logTerm;
        raft->Step(m).get();

        auto msgs = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(msgs.size(), 1);
        BOOST_REQUIRE_EQUAL(msgs[0]->type,
                            msgType == snail::raft::MessageType::MsgVote
                                ? snail::raft::MessageType::MsgVoteResp
                                : snail::raft::MessageType::MsgPreVoteResp);
        BOOST_REQUIRE_EQUAL(msgs[0]->reject, tests[i].wreject);
    }
}

SEASTAR_THREAD_TEST_CASE(RecvMsgVoteTest) {
    testRecvMsgVote(snail::raft::MessageType::MsgVote);
}

SEASTAR_THREAD_TEST_CASE(RecvMsgPreVoteTest) {
    testRecvMsgVote(snail::raft::MessageType::MsgPreVote);
}

SEASTAR_THREAD_TEST_CASE(StateTransitionTest) {
    struct testItem {
        snail::raft::RaftState from;
        snail::raft::RaftState to;
        bool wallow;
        uint64_t wterm;
        uint64_t wlead;
    };

    testItem tests[16] = {
        {snail::raft::RaftState::StateFollower,
         snail::raft::RaftState::StateFollower, true, 1, 0},
        {snail::raft::RaftState::StateFollower,
         snail::raft::RaftState::StatePreCandidate, true, 0, 0},
        {snail::raft::RaftState::StateFollower,
         snail::raft::RaftState::StateCandidate, true, 1, 0},
        {snail::raft::RaftState::StateFollower,
         snail::raft::RaftState::StateLeader, false, 0, 0},

        {snail::raft::RaftState::StatePreCandidate,
         snail::raft::RaftState::StateFollower, true, 0, 0},
        {snail::raft::RaftState::StatePreCandidate,
         snail::raft::RaftState::StatePreCandidate, true, 0, 0},
        {snail::raft::RaftState::StatePreCandidate,
         snail::raft::RaftState::StateCandidate, true, 1, 0},
        {snail::raft::RaftState::StatePreCandidate,
         snail::raft::RaftState::StateLeader, true, 0, 1},

        {snail::raft::RaftState::StateCandidate,
         snail::raft::RaftState::StateFollower, true, 0, 0},
        {snail::raft::RaftState::StateCandidate,
         snail::raft::RaftState::StatePreCandidate, true, 0, 0},
        {snail::raft::RaftState::StateCandidate,
         snail::raft::RaftState::StateCandidate, true, 1, 0},
        {snail::raft::RaftState::StateCandidate,
         snail::raft::RaftState::StateLeader, true, 0, 1},

        {snail::raft::RaftState::StateLeader,
         snail::raft::RaftState::StateFollower, true, 1, 0},
        {snail::raft::RaftState::StateLeader,
         snail::raft::RaftState::StatePreCandidate, false, 0, 0},
        {snail::raft::RaftState::StateLeader,
         snail::raft::RaftState::StateCandidate, false, 1, 0},
        {snail::raft::RaftState::StateLeader,
         snail::raft::RaftState::StateLeader, true, 0, 1},
    };

    for (int i = 0; i < 16; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1});
        auto raft = newTestRaft(1, 10, 1, store).get0();
        raft->state_ = tests[i].from;

        try {
            switch (tests[i].to) {
                case snail::raft::RaftState::StateFollower:
                    raft->BecomeFollower(tests[i].wterm, tests[i].wlead);
                    break;
                case snail::raft::RaftState::StatePreCandidate:
                    raft->BecomePreCandidate();
                    break;
                case snail::raft::RaftState::StateCandidate:
                    raft->BecomeCandidate();
                    break;
                case snail::raft::RaftState::StateLeader:
                    raft->BecomeLeader().get();
                    break;
            }
            BOOST_REQUIRE_EQUAL(raft->term_, tests[i].wterm);
            BOOST_REQUIRE_EQUAL(raft->lead_, tests[i].wlead);
        } catch (...) {
            BOOST_REQUIRE(!tests[i].wallow);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(AllServerStepdownTest) {
    struct testItem {
        snail::raft::RaftState state;
        snail::raft::RaftState wstate;
        uint64_t wterm;
        uint64_t windex;
    };

    testItem tests[4] = {
        {snail::raft::RaftState::StateFollower,
         snail::raft::RaftState::StateFollower, 3, 0},
        {snail::raft::RaftState::StatePreCandidate,
         snail::raft::RaftState::StateFollower, 3, 0},
        {snail::raft::RaftState::StateCandidate,
         snail::raft::RaftState::StateFollower, 3, 0},
        {snail::raft::RaftState::StateLeader,
         snail::raft::RaftState::StateFollower, 3, 1},
    };

    snail::raft::MessageType tmsgTypes[2] = {snail::raft::MessageType::MsgVote,
                                             snail::raft::MessageType::MsgApp};
    uint64_t tterm = 3;
    for (int i = 0; i < 4; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
        auto raft = newTestRaft(1, 10, 1, store).get0();

        switch (tests[i].state) {
            case snail::raft::RaftState::StateFollower:
                raft->BecomeFollower(1, 0);
                break;
            case snail::raft::RaftState::StatePreCandidate:
                raft->BecomePreCandidate();
                break;
            case snail::raft::RaftState::StateCandidate:
                raft->BecomeCandidate();
                break;
            case snail::raft::RaftState::StateLeader:
                raft->BecomeCandidate();
                raft->BecomeLeader().get();
                break;
        }

        for (int j = 0; j < 2; j++) {
            auto m = snail::raft::make_raft_message();
            m->from = 2;
            m->type = tmsgTypes[j];
            m->term = tterm;
            m->log_term = tterm;
            raft->Step(m).get();
            BOOST_REQUIRE_EQUAL(raft->state_, tests[i].wstate);
            BOOST_REQUIRE_EQUAL(raft->term_, tests[i].wterm);
            BOOST_REQUIRE_EQUAL(raft->raft_log_->LastIndex(), tests[i].windex);
            auto ents = raft->raft_log_->AllEntries().get0();
            BOOST_REQUIRE_EQUAL(ents.size(), tests[i].windex);
            uint64_t wlead = 2;
            if (tmsgTypes[j] == snail::raft::MessageType::MsgVote) {
                wlead = 0;
            }
            BOOST_REQUIRE_EQUAL(raft->lead_, wlead);
        }
    }
}

static void testCandidateResetTerm(snail::raft::MessageType mt) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto b = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto c = newTestRaft(3, 10, 1, store).get0();

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(b->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateFollower);

    nt->Isolate(3);
    for (int i = 0; i < 2; i++) {
        m = snail::raft::make_raft_message();
        m->from = 2 - i;
        m->to = 2 - i;
        m->type = snail::raft::MessageType::MsgHup;
        nt->Send({m}).get();
    }
    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(b->state_, snail::raft::RaftState::StateFollower);

    c->ResetRandomizedElectionTimeout();
    for (int i = 0; i < c->randomized_election_timeout_; i++) {
        c->tick_().get();
    }
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateCandidate);

    nt->Recover();
    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 3;
    m->type = mt;
    m->term = a->term_;
    nt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(a->term_, c->term_);
}

SEASTAR_THREAD_TEST_CASE(CandidateResetTermMsgHeartbeatTest) {
    testCandidateResetTerm(snail::raft::MessageType::MsgHeartbeat);
}

SEASTAR_THREAD_TEST_CASE(CandidateResetTermMsgAppTest) {
    testCandidateResetTerm(snail::raft::MessageType::MsgApp);
}

SEASTAR_THREAD_TEST_CASE(LeaderStepdownWhenQuorumActiveTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto raft = newTestRaft(1, 5, 1, store).get0();

    raft->check_quorum_ = true;
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    for (int i = 0; i < raft->election_timeout_ + 1; i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->term = raft->term_;
        m->type = snail::raft::MessageType::MsgHeartbeatResp;
        raft->Step(m).get();
        raft->tick_().get();
    }
    BOOST_REQUIRE_EQUAL(raft->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(LeaderStepdownWhenQuorumLostTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto raft = newTestRaft(1, 5, 1, store).get0();

    raft->check_quorum_ = true;
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    for (int i = 0; i < raft->election_timeout_ + 1; i++) {
        raft->tick_().get();
    }
    BOOST_REQUIRE_EQUAL(raft->state_, snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(LeaderSupersedingWithCheckQuorumTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto b = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto c = newTestRaft(3, 10, 1, store).get0();

    a->check_quorum_ = true;
    b->check_quorum_ = true;
    c->check_quorum_ = true;

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});
    b->randomized_election_timeout_ = b->election_timeout_ + 1;
    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateFollower);

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateCandidate);

    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }
    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(LeaderElectionWithCheckQuorumTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto b = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto c = newTestRaft(3, 10, 1, store).get0();

    a->check_quorum_ = true;
    b->check_quorum_ = true;
    c->check_quorum_ = true;

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});
    a->randomized_election_timeout_ = a->election_timeout_ + 1;
    b->randomized_election_timeout_ = b->election_timeout_ + 2;

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateFollower);

    a->randomized_election_timeout_ = a->election_timeout_ + 1;
    b->randomized_election_timeout_ = b->election_timeout_ + 2;
    for (int i = 0; i < a->election_timeout_; i++) {
        a->tick_().get();
    }

    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(FreeStuckCandidateWithCheckQuorumTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto b = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto c = newTestRaft(3, 10, 1, store).get0();

    a->check_quorum_ = true;
    b->check_quorum_ = true;
    c->check_quorum_ = true;

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});
    b->randomized_election_timeout_ = b->election_timeout_ + 1;

    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    nt->Isolate(1);

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(b->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateCandidate);
    BOOST_REQUIRE_EQUAL(c->term_, b->term_ + 1);

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(b->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateCandidate);
    BOOST_REQUIRE_EQUAL(c->term_, b->term_ + 2);
    nt->Recover();

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHeartbeat;
    m->term = a->term_;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(c->term_, a->term_);

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(c->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(NonPromotableVoterWithCheckQuorumTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    auto b = newTestRaft(2, 10, 1, store).get0();

    a->check_quorum_ = true;
    b->check_quorum_ = true;

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b)});

    b->randomized_election_timeout_ = b->election_timeout_ + 1;

    snail::raft::ConfChange v1;
    v1.set_type(snail::raft::ConfChangeType::ConfChangeRemoveNode);
    v1.set_node_id(2);

    snail::raft::ConfChangeI cc(v1);
    b->ApplyConfChange(cc.AsV2()).get();
    BOOST_REQUIRE(!b->Promotable());

    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(b->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(b->lead_, 1);
}

SEASTAR_THREAD_TEST_CASE(DisruptiveFollowerTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto n1 = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto n2 = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto n3 = newTestRaft(3, 10, 1, store).get0();

    n1->check_quorum_ = true;
    n2->check_quorum_ = true;
    n3->check_quorum_ = true;

    n1->BecomeFollower(1, 0);
    n2->BecomeFollower(1, 0);
    n3->BecomeFollower(1, 0);

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(n1), RaftSM::MakeStateMachine(n2),
                    RaftSM::MakeStateMachine(n3)});

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(n3->state_, snail::raft::RaftState::StateFollower);

    n3->randomized_election_timeout_ = n3->election_timeout_ + 2;
    for (int i = 0; i < n3->randomized_election_timeout_ - 1; i++) {
        n3->tick_().get();
    }
    n3->tick_().get();

    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(n3->state_, snail::raft::RaftState::StateCandidate);
    BOOST_REQUIRE_EQUAL(n1->term_, 2);
    BOOST_REQUIRE_EQUAL(n2->term_, 2);
    BOOST_REQUIRE_EQUAL(n3->term_, 3);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHeartbeat;
    m->term = n1->term_;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(n3->state_, snail::raft::RaftState::StateCandidate);
    BOOST_REQUIRE_EQUAL(n1->term_, 3);
    BOOST_REQUIRE_EQUAL(n2->term_, 2);
    BOOST_REQUIRE_EQUAL(n3->term_, 3);
}

SEASTAR_THREAD_TEST_CASE(DisruptiveFollowerPreVoteTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto n1 = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto n2 = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto n3 = newTestRaft(3, 10, 1, store).get0();

    n1->check_quorum_ = true;
    n2->check_quorum_ = true;
    n3->check_quorum_ = true;

    n1->BecomeFollower(1, 0);
    n2->BecomeFollower(1, 0);
    n3->BecomeFollower(1, 0);

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(n1), RaftSM::MakeStateMachine(n2),
                    RaftSM::MakeStateMachine(n3)});

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(n3->state_, snail::raft::RaftState::StateFollower);

    nt->Isolate(3);

    seastar::temporary_buffer<char> data("somedata", 8);
    snail::raft::EntryPtr ent;

    for (int i = 0; i < 3; i++) {
        m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        ent = snail::raft::make_entry();
        ent->set_data(data.share());
        m->entries.push_back(ent);
        nt->Send({m}).get();
    }
    n1->pre_vote_ = true;
    n2->pre_vote_ = true;
    n3->pre_vote_ = true;
    nt->Recover();

    m = snail::raft::make_raft_message();
    m->from = 3;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateLeader);
    BOOST_REQUIRE_EQUAL(n2->state_, snail::raft::RaftState::StateFollower);
    BOOST_REQUIRE_EQUAL(n3->state_, snail::raft::RaftState::StatePreCandidate);
    BOOST_REQUIRE_EQUAL(n1->term_, 2);
    BOOST_REQUIRE_EQUAL(n2->term_, 2);
    BOOST_REQUIRE_EQUAL(n3->term_, 2);

    m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 3;
    m->type = snail::raft::MessageType::MsgHeartbeat;
    m->term = n1->term_;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(n1->state_, snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(ReadOnlyOptionSafeTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto b = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto c = newTestRaft(3, 10, 1, store).get0();

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});

    b->randomized_election_timeout_ = b->election_timeout_ + 1;
    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);

    struct testItem {
        snail::raft::RaftPtr sm;
        int proposals;
        uint64_t wri;
        seastar::temporary_buffer<char> wctx;
    };

    testItem tests[6] = {
        {a, 10, 11, seastar::temporary_buffer<char>("ctx1", 4)},
        {b, 10, 21, seastar::temporary_buffer<char>("ctx2", 4)},
        {c, 10, 31, seastar::temporary_buffer<char>("ctx3", 4)},
        {a, 10, 41, seastar::temporary_buffer<char>("ctx4", 4)},
        {b, 10, 51, seastar::temporary_buffer<char>("ctx5", 4)},
        {c, 10, 61, seastar::temporary_buffer<char>("ctx6", 4)},
    };

    for (int i = 0; i < 6; i++) {
        for (int j = 0; j < tests[i].proposals; j++) {
            m = snail::raft::make_raft_message();
            m->from = 1;
            m->to = 1;
            m->type = snail::raft::MessageType::MsgProp;
            m->entries.push_back(snail::raft::make_entry());
            nt->Send({m}).get();
        }

        m = snail::raft::make_raft_message();
        m->from = tests[i].sm->id_;
        m->to = tests[i].sm->id_;
        m->type = snail::raft::MessageType::MsgReadIndex;
        auto ent = snail::raft::make_entry();
        ent->set_data(tests[i].wctx.share());
        m->entries.push_back(ent);
        nt->Send({m}).get();

        BOOST_REQUIRE(tests[i].sm->read_states_.size() > 0);
        BOOST_REQUIRE_EQUAL(tests[i].sm->read_states_[0].index, tests[i].wri);
        BOOST_REQUIRE(tests[i].sm->read_states_[0].request_ctx ==
                      tests[i].wctx);
        tests[i].sm->read_states_.clear();
    }
}

SEASTAR_THREAD_TEST_CASE(ReadOnlyWithLearnerTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1});
    ptr->snapshot_->metadata().conf_state().set_learners({2});
    auto b = newTestRaft(2, 10, 1, store).get0();

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b)});

    b->randomized_election_timeout_ = b->election_timeout_ + 1;
    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();
    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);

    struct testItem {
        snail::raft::RaftPtr sm;
        int proposals;
        uint64_t wri;
        seastar::temporary_buffer<char> wctx;
    };

    testItem tests[4] = {
        {a, 10, 11, seastar::temporary_buffer<char>("ctx1", 4)},
        {b, 10, 21, seastar::temporary_buffer<char>("ctx2", 4)},
        {a, 10, 31, seastar::temporary_buffer<char>("ctx4", 4)},
        {b, 10, 41, seastar::temporary_buffer<char>("ctx5", 4)},
    };

    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < tests[i].proposals; j++) {
            m = snail::raft::make_raft_message();
            m->from = 1;
            m->to = 1;
            m->type = snail::raft::MessageType::MsgProp;
            m->entries.push_back(snail::raft::make_entry());
            nt->Send({m}).get();
        }
        m = snail::raft::make_raft_message();
        m->from = tests[i].sm->id_;
        m->to = tests[i].sm->id_;
        m->type = snail::raft::MessageType::MsgReadIndex;
        auto ent = snail::raft::make_entry();
        ent->set_data(tests[i].wctx.share());
        m->entries.push_back(ent);
        nt->Send({m}).get();

        BOOST_REQUIRE(tests[i].sm->read_states_.size() > 0);
        BOOST_REQUIRE_EQUAL(tests[i].sm->read_states_[0].index, tests[i].wri);
        BOOST_REQUIRE(tests[i].sm->read_states_[0].request_ctx ==
                      tests[i].wctx);
        tests[i].sm->read_states_.clear();
    }
}

SEASTAR_THREAD_TEST_CASE(ReadOnlyOptionLeaseTest) {
    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto a = newTestRaft(1, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto b = newTestRaft(2, 10, 1, store).get0();

    store = newTestMemoryStorage();
    ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->snapshot_->metadata().conf_state().set_voters({1, 2, 3});
    auto c = newTestRaft(3, 10, 1, store).get0();

    a->read_only_->option_ = snail::raft::ReadOnlyOption::ReadOnlyLeaseBased;
    b->read_only_->option_ = snail::raft::ReadOnlyOption::ReadOnlyLeaseBased;
    c->read_only_->option_ = snail::raft::ReadOnlyOption::ReadOnlyLeaseBased;
    a->check_quorum_ = true;
    b->check_quorum_ = true;
    c->check_quorum_ = true;

    auto nt =
        newNetwork({RaftSM::MakeStateMachine(a), RaftSM::MakeStateMachine(b),
                    RaftSM::MakeStateMachine(c)});

    b->randomized_election_timeout_ = b->election_timeout_ + 1;
    for (int i = 0; i < b->election_timeout_; i++) {
        b->tick_().get();
    }

    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgHup;
    nt->Send({m}).get();

    BOOST_REQUIRE_EQUAL(a->state_, snail::raft::RaftState::StateLeader);

    struct testItem {
        snail::raft::RaftPtr sm;
        int proposals;
        uint64_t wri;
        seastar::temporary_buffer<char> wctx;
    };

    testItem tests[6] = {
        {a, 10, 11, seastar::temporary_buffer<char>("ctx1", 4)},
        {b, 10, 21, seastar::temporary_buffer<char>("ctx2", 4)},
        {c, 10, 31, seastar::temporary_buffer<char>("ctx3", 4)},
        {a, 10, 41, seastar::temporary_buffer<char>("ctx4", 4)},
        {b, 10, 51, seastar::temporary_buffer<char>("ctx5", 4)},
        {c, 10, 61, seastar::temporary_buffer<char>("ctx6", 4)},
    };

    for (int i = 0; i < 6; i++) {
        for (int j = 0; j < tests[i].proposals; j++) {
            m = snail::raft::make_raft_message();
            m->from = 1;
            m->to = 1;
            m->type = snail::raft::MessageType::MsgProp;
            m->entries.push_back(snail::raft::make_entry());
            nt->Send({m}).get();
        }

        m = snail::raft::make_raft_message();
        m->from = tests[i].sm->id_;
        m->to = tests[i].sm->id_;
        m->type = snail::raft::MessageType::MsgReadIndex;
        auto ent = snail::raft::make_entry();
        ent->set_data(tests[i].wctx.share());
        m->entries.push_back(ent);
        nt->Send({m}).get();

        BOOST_REQUIRE(tests[i].sm->read_states_.size() > 0);
        BOOST_REQUIRE_EQUAL(tests[i].sm->read_states_[0].index, tests[i].wri);
        BOOST_REQUIRE(tests[i].sm->read_states_[0].request_ctx ==
                      tests[i].wctx);
        tests[i].sm->read_states_.clear();
    }
}

}  // namespace raft
}  // namespace snail
