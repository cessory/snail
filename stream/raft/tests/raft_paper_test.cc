
#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "raft/raft.h"
#include "raft_test.h"
#include "util/logger.h"
namespace snail {
namespace raft {

static void testUpdateTermFromMessage(snail::raft::RaftState state) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, 10, 1, store).get0();
    switch (state) {
        case snail::raft::RaftState::StateFollower:
            raft->BecomeFollower(1, 2);
            break;
        case snail::raft::RaftState::StateCandidate:
            raft->BecomeCandidate();
            break;
        case snail::raft::RaftState::StateLeader:
            raft->BecomeCandidate();
            raft->BecomeLeader().get();
            break;
    }
    snail::raft::MessagePtr msg = snail::raft::make_raft_message();
    msg->type = snail::raft::MessageType::MsgApp;
    msg->term = 2;
    auto s = raft->Step(msg).get0();
    BOOST_REQUIRE(s);
    BOOST_REQUIRE_EQUAL(raft->term_, 2);
    BOOST_REQUIRE_EQUAL(raft->state_, snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(FollowerUpdateTermFromMessageTest) {
    LOG_INFO("FollowerUpdateTermFromMessageTest...");
    testUpdateTermFromMessage(snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(CandidateUpdateTermFromMessageTest) {
    LOG_INFO("CandidateUpdateTermFromMessageTest...");
    testUpdateTermFromMessage(snail::raft::RaftState::StateCandidate);
}

SEASTAR_THREAD_TEST_CASE(LeaderUpdateTermFromMessageTest) {
    LOG_INFO("LeaderUpdateTermFromMessageTest...");
    testUpdateTermFromMessage(snail::raft::RaftState::StateLeader);
}

SEASTAR_THREAD_TEST_CASE(RejectStaleTermMessageTest) {
    LOG_INFO("RejectStaleTermMessageTest...");
    bool called = false;
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, 10, 1, store).get0();
    raft->step_ =
        [&called](snail::raft::MessagePtr m) -> seastar::future<Status<>> {
        called = true;
        return seastar::make_ready_future<Status<>>();
    };
    snail::raft::HardState hs;
    hs.set_term(2);
    raft->LoadState(hs);

    snail::raft::MessagePtr msg = snail::raft::make_raft_message();
    msg->type = snail::raft::MessageType::MsgApp;
    msg->term = raft->term_ - 1;
    auto s = raft->Step(msg).get0();
    BOOST_REQUIRE(s);
    BOOST_REQUIRE(!called);
}

SEASTAR_THREAD_TEST_CASE(StartAsFollowerTest) {
    LOG_INFO("StartAsFollowerTest...");
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, 10, 1, store).get0();
    BOOST_REQUIRE_EQUAL(raft->state_, snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(LeaderBcastBeatTest) {
    LOG_INFO("LeaderBcastBeatTest...");
    int hi = 1;
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, 10, hi, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    for (int i = 0; i < 10; i++) {
        std::vector<snail::raft::EntryPtr> ents;
        snail::raft::EntryPtr ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ents.push_back(ent);
        auto res = raft->AppendEntry(std::move(ents)).get0();
        BOOST_REQUIRE(res);
    }

    for (int i = 0; i < hi; i++) {
        raft->tick_().get();
    }
    std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
    std::sort(
        msgs.begin(), msgs.end(),
        [](snail::raft::MessagePtr m1, snail::raft::MessagePtr m2) -> bool {
            return m1->to < m2->to;
        });
    BOOST_REQUIRE_EQUAL(msgs.size(), 2);
    for (int i = 0; i < 2; i++) {
        BOOST_REQUIRE_EQUAL(msgs[i]->from, 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->to, i + 2);
        BOOST_REQUIRE_EQUAL(msgs[i]->term, 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->type,
                            snail::raft::MessageType::MsgHeartbeat);
    }
}

static void testNonleaderStartElection(snail::raft::RaftState state) {
    int et = 10;
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);

    auto raft = newTestRaft(1, et, 1, store).get0();
    switch (state) {
        case snail::raft::RaftState::StateFollower:
            raft->BecomeFollower(1, 2);
            break;
        case snail::raft::RaftState::StateCandidate:
            raft->BecomeCandidate();
            break;
    }

    for (int i = 1; i < 2 * et; i++) {
        raft->tick_().get();
    }
    BOOST_REQUIRE_EQUAL(raft->term_, 2);
    BOOST_REQUIRE_EQUAL(raft->state_, snail::raft::RaftState::StateCandidate);
    BOOST_REQUIRE(raft->prs_->votes_[raft->id_]);

    std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
    std::sort(
        msgs.begin(), msgs.end(),
        [](snail::raft::MessagePtr m1, snail::raft::MessagePtr m2) -> bool {
            return m1->to < m2->to;
        });
    BOOST_REQUIRE_EQUAL(msgs.size(), 2);
    for (int i = 0; i < 2; i++) {
        BOOST_REQUIRE_EQUAL(msgs[i]->from, 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->to, i + 2);
        BOOST_REQUIRE_EQUAL(msgs[i]->term, 2);
        BOOST_REQUIRE_EQUAL(msgs[i]->type, snail::raft::MessageType::MsgVote);
    }
}

SEASTAR_THREAD_TEST_CASE(FollowerStartElectionTest) {
    LOG_INFO("FollowerStartElectionTest...");
    testNonleaderStartElection(snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(CandidateStartElectionTest) {
    LOG_INFO("CandidateStartElectionTest...");
    testNonleaderStartElection(snail::raft::RaftState::StateCandidate);
}

SEASTAR_THREAD_TEST_CASE(LeaderElectionInOneRoundRPCTest) {
    LOG_INFO("LeaderElectionInOneRoundRPCTest...");
    struct testItem {
        int size;
        std::unordered_map<uint64_t, bool> votes;
        snail::raft::RaftState state;
    };

    testItem tests[13] = {
        {1, {}, snail::raft::RaftState::StateLeader},
        {3, {{2, true}, {3, true}}, snail::raft::RaftState::StateLeader},
        {3, {{2, true}}, snail::raft::RaftState::StateLeader},
        {5,
         {{2, true}, {3, true}, {4, true}, {5, true}},
         snail::raft::RaftState::StateLeader},
        {5,
         {{2, true}, {3, true}, {4, true}},
         snail::raft::RaftState::StateLeader},
        {5, {{2, true}, {3, true}}, snail::raft::RaftState::StateLeader},
        {3, {{2, false}, {3, false}}, snail::raft::RaftState::StateFollower},
        {5,
         {{2, false}, {3, false}, {4, false}, {5, false}},
         snail::raft::RaftState::StateFollower},
        {5,
         {{2, true}, {3, false}, {4, false}, {5, false}},
         snail::raft::RaftState::StateFollower},
        {3, {}, snail::raft::RaftState::StateCandidate},
        {5, {{2, true}}, snail::raft::RaftState::StateCandidate},
        {5, {{2, false}, {3, false}}, snail::raft::RaftState::StateCandidate},
        {5, {}, snail::raft::RaftState::StateCandidate}};

    for (int i = 0; i < 13; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = idsBySize(tests[i].size);
        ptr->snapshot_->metadata().conf_state().set_voters(peers);

        auto raft = newTestRaft(1, 10, 1, store).get0();
        snail::raft::MessagePtr m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgHup;
        auto s = raft->Step(m).get0();
        BOOST_REQUIRE(s);
        for (auto& e : tests[i].votes) {
            m = snail::raft::make_raft_message();
            m->from = e.first;
            m->to = 1;
            m->term = raft->term_;
            m->type = snail::raft::MessageType::MsgVoteResp;
            m->reject = !e.second;
            s = raft->Step(m).get0();
            BOOST_REQUIRE(s);
        }
        BOOST_REQUIRE_EQUAL(raft->state_, tests[i].state);
        BOOST_REQUIRE_EQUAL(raft->term_, 1);
    }
}

SEASTAR_THREAD_TEST_CASE(FollowerVoteTest) {
    LOG_INFO("FollowerVoteTest...");
    struct testItem {
        uint64_t vote;
        uint64_t nvote;
        bool wreject;
    };

    testItem tests[6] = {{0, 1, false}, {0, 2, false}, {1, 1, false},
                         {2, 2, false}, {1, 2, true},  {2, 1, true}};
    for (int i = 0; i < 6; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2, 3};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);

        auto raft = newTestRaft(1, 10, 1, store).get0();
        snail::raft::HardState hs;
        hs.set_term(1);
        hs.set_vote(tests[i].vote);
        raft->LoadState(hs);

        auto m = snail::raft::make_raft_message();
        m->from = tests[i].nvote;
        m->to = 1;
        m->term = 1;
        m->type = snail::raft::MessageType::MsgVote;
        auto s = raft->Step(m).get0();
        BOOST_REQUIRE(s);
        std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(msgs.size(), 1);
        BOOST_REQUIRE_EQUAL(msgs[0]->from, 1);
        BOOST_REQUIRE_EQUAL(msgs[0]->to, tests[i].nvote);
        BOOST_REQUIRE_EQUAL(msgs[0]->term, 1);
        BOOST_REQUIRE_EQUAL(msgs[0]->type,
                            snail::raft::MessageType::MsgVoteResp);
        BOOST_REQUIRE_EQUAL(msgs[0]->reject, tests[i].wreject);
    }
}

SEASTAR_THREAD_TEST_CASE(CandidateFallbackTest) {
    LOG_INFO("CandidateFallbackTest...");
    std::vector<snail::raft::MessagePtr> tests;
    for (int i = 0; i < 2; i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->term = i + 1;
        m->type = snail::raft::MessageType::MsgApp;
        tests.push_back(m);
    }
    for (int i = 0; i < tests.size(); i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2, 3};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);

        auto raft = newTestRaft(1, 10, 1, store).get0();
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgHup;
        auto s = raft->Step(m).get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(raft->state_,
                            snail::raft::RaftState::StateCandidate);
        s = raft->Step(tests[i]).get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(raft->state_,
                            snail::raft::RaftState::StateFollower);
        BOOST_REQUIRE_EQUAL(raft->term_, tests[i]->term);
    }
}

static void testNonleaderElectionTimeoutRandomized(
    snail::raft::RaftState state) {
    int et = 10;
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, et, 1, store).get0();
    std::unordered_map<int, bool> timeouts;
    for (int round = 0; round < 50 * et; round++) {
        switch (state) {
            case snail::raft::RaftState::StateFollower:
                raft->BecomeFollower(raft->term_ + 1, 2);
                break;
            case snail::raft::RaftState::StateCandidate:
                raft->BecomeCandidate();
                break;
        }
        int time = 0;
        std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
        for (; msgs.empty(); time++) {
            raft->tick_().get();
            msgs = std::move(raft->msgs_);
        }
        timeouts[time] = true;
    }
    for (int d = et + 1; d < 2 * et; d++) {
        BOOST_REQUIRE(timeouts[d]);
    }
}

SEASTAR_THREAD_TEST_CASE(FollowerElectionTimeoutRandomizedTest) {
    LOG_INFO("FollowerElectionTimeoutRandomizedTest...");
    testNonleaderElectionTimeoutRandomized(
        snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(CandidateElectionTimeoutRandomizedTest) {
    LOG_INFO("CandidateElectionTimeoutRandomizedTest...");
    testNonleaderElectionTimeoutRandomized(
        snail::raft::RaftState::StateCandidate);
}

static void testNonleadersElectionTimeoutNonconflict(
    snail::raft::RaftState state) {
    int et = 10;
    int size = 5;
    std::vector<snail::raft::RaftPtr> rs;

    auto ids = idsBySize(size);
    for (int i = 0; i < size; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->snapshot_->metadata().conf_state().set_voters(ids);
        auto raft = newTestRaft(ids[i], et, 1, store).get0();
        rs.push_back(std::move(raft));
    }
    int conflicts = 0;
    for (int round = 0; round < 1000; round++) {
        for (int i = 0; i < size; i++) {
            switch (state) {
                case snail::raft::RaftState::StateFollower:
                    rs[i]->BecomeFollower(rs[i]->term_ + 1, 0);
                    break;
                case snail::raft::RaftState::StateCandidate:
                    rs[i]->BecomeCandidate();
                    break;
            }
        }

        int timeout_num = 0;
        while (timeout_num == 0) {
            for (int i = 0; i < size; i++) {
                rs[i]->tick_().get();
                std::vector<snail::raft::MessagePtr> msgs =
                    std::move(rs[i]->msgs_);
                if (msgs.size() > 0) {
                    timeout_num++;
                }
            }
        }
        if (timeout_num > 1) {
            conflicts++;
        }
    }

    BOOST_REQUIRE_LE(float(conflicts) / 1000, 0.3);
}

SEASTAR_THREAD_TEST_CASE(FollowersElectionTimeoutNonconflictTest) {
    LOG_INFO("FollowersElectionTimeoutNonconflictTest...");
    testNonleadersElectionTimeoutNonconflict(
        snail::raft::RaftState::StateFollower);
}

SEASTAR_THREAD_TEST_CASE(CandidatesElectionTimeoutNonconflictTest) {
    LOG_INFO("CandidatesElectionTimeoutNonconflictTest...");
    testNonleadersElectionTimeoutNonconflict(
        snail::raft::RaftState::StateCandidate);
}

static snail::raft::MessagePtr acceptAndReply(snail::raft::MessagePtr m) {
    if (m->type != snail::raft::MessageType::MsgApp) {
        std::runtime_error("type should be MsgApp");
    }
    snail::raft::MessagePtr msg = snail::raft::make_raft_message();
    msg->from = m->to;
    msg->to = m->from;
    msg->term = m->term;
    msg->type = snail::raft::MessageType::MsgAppResp;
    msg->index = m->index + m->entries.size();
    return msg;
}

static void commitNoopEntry(snail::raft::Raft* r,
                            seastar::shared_ptr<snail::raft::MemoryStorage> s) {
    BOOST_REQUIRE_EQUAL(r->state_, snail::raft::RaftState::StateLeader);
    r->BcastAppend().get();
    std::vector<snail::raft::MessagePtr> msgs = std::move(r->msgs_);
    for (auto m : msgs) {
        r->Step(acceptAndReply(m)).get();
    }
    msgs = std::move(r->msgs_);
    s->Append(r->raft_log_->UnstableEntries());
    r->raft_log_->AppliedTo(r->raft_log_->committed());
    r->raft_log_->StableTo(r->raft_log_->LastIndex(),
                           r->raft_log_->LastTerm().get0());
}

SEASTAR_THREAD_TEST_CASE(LeaderStartReplicationTest) {
    LOG_INFO("LeaderStartReplicationTest...");
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 10, 1, store).get0();

    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    commitNoopEntry(raft.get(), ptr);

    auto li = raft->raft_log_->LastIndex();
    auto m = snail::raft::make_raft_message();
    m->type = snail::raft::MessageType::MsgProp;
    m->from = 1;
    m->to = 1;
    auto ent = snail::raft::make_entry();
    seastar::temporary_buffer<char> data("some data", 9);
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    raft->Step(m).get();
    BOOST_REQUIRE_EQUAL(raft->raft_log_->LastIndex(), li + 1);
    BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), li);

    std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
    std::sort(
        msgs.begin(), msgs.end(),
        [](snail::raft::MessagePtr m1, snail::raft::MessagePtr m2) -> bool {
            return m1->to < m2->to;
        });
    BOOST_REQUIRE_EQUAL(msgs.size(), 2);
    for (int i = 0; i < msgs.size(); i++) {
        BOOST_REQUIRE_EQUAL(msgs[i]->from, 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->to, i + 2);
        BOOST_REQUIRE_EQUAL(msgs[i]->term, 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->type, snail::raft::MessageType::MsgApp);
        BOOST_REQUIRE_EQUAL(msgs[i]->index, li);
        BOOST_REQUIRE_EQUAL(msgs[i]->log_term, 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->commit, li);
        BOOST_REQUIRE_EQUAL(msgs[i]->entries.size(), 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->entries[0]->index(), li + 1);
        BOOST_REQUIRE_EQUAL(msgs[i]->entries[0]->term(), 1);
        BOOST_REQUIRE((msgs[i]->entries[0]->data() == data));
    }
    auto ents = raft->raft_log_->UnstableEntries();
    BOOST_REQUIRE_EQUAL(ents.size(), 1);
    BOOST_REQUIRE_EQUAL(ents[0]->index(), li + 1);
    BOOST_REQUIRE_EQUAL(ents[0]->term(), 1);
    BOOST_REQUIRE((ents[0]->data() == data));
}

SEASTAR_THREAD_TEST_CASE(LeaderCommitEntryTest) {
    LOG_INFO("LeaderCommitEntryTest...");
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2, 3};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 10, 1, store).get0();

    raft->BecomeCandidate();
    raft->BecomeLeader().get();
    commitNoopEntry(raft.get(), ptr);
    auto li = raft->raft_log_->LastIndex();
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    auto ent = snail::raft::make_entry();
    seastar::temporary_buffer<char> data("some data", 9);
    ent->set_data(std::move(data.share()));
    m->entries.push_back(ent);
    raft->Step(m).get();

    std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
    for (auto msg : msgs) {
        raft->Step(acceptAndReply(msg)).get();
    }
    BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), li + 1);

    auto went = snail::raft::make_entry();
    went->set_index(li + 1);
    went->set_term(1);
    went->set_data(std::move(data.share()));
    auto ents = raft->raft_log_->NextEnts().get0();
    BOOST_REQUIRE_EQUAL(ents.size(), 1);
    BOOST_REQUIRE((*ents[0] == *went));

    msgs = std::move(raft->msgs_);
    std::sort(
        msgs.begin(), msgs.end(),
        [](snail::raft::MessagePtr m1, snail::raft::MessagePtr m2) -> bool {
            return m1->to < m2->to;
        });
    for (int i = 0; i < msgs.size(); i++) {
        BOOST_REQUIRE_EQUAL(msgs[i]->to, i + 2);
        BOOST_REQUIRE_EQUAL(msgs[i]->type, snail::raft::MessageType::MsgApp);
        BOOST_REQUIRE_EQUAL(msgs[i]->commit, li + 1);
    }
}

SEASTAR_THREAD_TEST_CASE(LeaderAcknowledgeCommitTest) {
    LOG_INFO("LeaderAcknowledgeCommitTest...");
    struct testItem {
        int size;
        std::unordered_map<uint64_t, bool> acceptors;
        bool wack;
    };

    testItem tests[9] = {
        {1, {}, true},
        {3, {}, false},
        {3, {{2, true}}, true},
        {3, {{2, true}, {3, true}}, true},
        {5, {}, false},
        {5, {{2, true}}, false},
        {5, {{2, true}, {3, true}}, true},
        {5, {{2, true}, {3, true}, {4, true}}, true},
        {5, {{2, true}, {3, true}, {4, true}, {5, true}}, true}};

    for (int i = 0; i < 9; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = idsBySize(tests[i].size);
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        raft->BecomeCandidate();
        raft->BecomeLeader().get();
        commitNoopEntry(raft.get(), ptr);
        auto li = raft->raft_log_->LastIndex();
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("some data", 9);
        ent->set_data(std::move(data.share()));
        m->entries.push_back(ent);
        raft->Step(m).get();

        std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
        for (auto msg : msgs) {
            if (tests[i].acceptors[msg->to]) {
                raft->Step(acceptAndReply(msg)).get();
            }
        }
        auto ok = raft->raft_log_->committed() > li;
        BOOST_REQUIRE_EQUAL(ok, tests[i].wack);
    }
}

SEASTAR_THREAD_TEST_CASE(LeaderCommitPrecedingEntriesTest) {
    LOG_INFO("LeaderCommitPrecedingEntriesTest...");
    std::vector<std::vector<snail::raft::EntryPtr>> tests;
    std::vector<snail::raft::EntryPtr> ents;
    snail::raft::EntryPtr ent;

    tests.push_back(std::move(ents));
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(2);
    ents.push_back(ent);
    tests.push_back(std::move(ents));
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term(i + 1);
        ents.push_back(ent);
    }
    tests.push_back(std::move(ents));
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ents.push_back(ent);
    tests.push_back(std::move(ents));

    for (int i = 0; i < tests.size(); i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2, 3};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        ptr->Append(tests[i]);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        snail::raft::HardState hs(2, 0, 0);
        raft->LoadState(hs);
        raft->BecomeCandidate();
        raft->BecomeLeader().get();

        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("some data", 9);
        ent->set_data(std::move(data.clone()));
        m->entries.push_back(ent);
        raft->Step(m).get();

        std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
        for (auto msg : msgs) {
            raft->Step(acceptAndReply(msg)).get();
        }

        uint64_t li = tests[i].size();
        std::vector<snail::raft::EntryPtr> wents(tests[i].begin(),
                                                 tests[i].end());
        ent = snail::raft::make_entry();
        ent->set_term(3);
        ent->set_index(li + 1);
        wents.push_back(ent);
        ent = snail::raft::make_entry();
        ent->set_term(3);
        ent->set_index(li + 2);
        ent->set_data(std::move(data.clone()));
        wents.push_back(ent);

        auto g = raft->raft_log_->NextEnts().get0();
        BOOST_REQUIRE_EQUAL(g.size(), wents.size());
        for (int j = 0; j < g.size(); j++) {
            BOOST_REQUIRE((*g[j] == *wents[j]));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(FollowerCommitEntryTest) {
    LOG_INFO("FollowerCommitEntryTest...");
    struct testItem {
        std::vector<snail::raft::EntryPtr> ents;
        uint64_t commit;
    };

    testItem tests[4];
    snail::raft::EntryPtr ent;
    seastar::temporary_buffer<char> data("some data", 9);
    seastar::temporary_buffer<char> data1("some data2", 10);

    tests[0].commit = 1;
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ent->set_data(std::move(data.share()));
    tests[0].ents.push_back(ent);

    tests[1].commit = 2;
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ent->set_data(std::move(data.share()));
    tests[1].ents.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(1);
    ent->set_data(std::move(data1.share()));
    tests[1].ents.push_back(ent);

    tests[2].commit = 2;
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ent->set_data(std::move(data1.share()));
    tests[2].ents.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(1);
    ent->set_data(std::move(data.share()));
    tests[2].ents.push_back(ent);

    tests[3].commit = 1;
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(1);
    ent->set_data(std::move(data.share()));
    tests[3].ents.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(2);
    ent->set_term(1);
    ent->set_data(std::move(data1.share()));
    tests[3].ents.push_back(ent);

    for (int i = 0; i < 4; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2, 3};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        raft->BecomeFollower(1, 2);

        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgApp;
        m->term = 1;
        m->entries = tests[i].ents;
        m->commit = tests[i].commit;
        raft->Step(m).get();

        BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), tests[i].commit);
        auto g = raft->raft_log_->NextEnts().get0();
        BOOST_REQUIRE_EQUAL(g.size(), tests[i].commit);
        for (int j = 0; j < tests[i].commit; j++) {
            BOOST_REQUIRE((*g[j] == *tests[i].ents[j]));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(FollowerCheckMsgAppTest) {
    LOG_INFO("FollowerCheckMsgAppTest...");
    std::vector<snail::raft::EntryPtr> ents;
    for (int i = 0; i < 2; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        ents.push_back(ent);
    }
    struct testItem {
        uint64_t term;
        uint64_t index;
        uint64_t windex;
        bool wreject;
        uint64_t wrejectHint;
        uint64_t wlogterm;
    };

    testItem tests[5] = {
        {0, 0, 1, false, 0, 0},
        {ents[0]->term(), ents[0]->index(), 1, false, 0, 0},
        {ents[1]->term(), ents[1]->index(), 2, false, 0, 0},
        {ents[0]->term(), ents[1]->index(), ents[1]->index(), true, 1, 1},
        {ents[1]->term() + 1, ents[1]->index() + 1, ents[1]->index() + 1, true,
         2, 2}};
    for (int i = 0; i < 5; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2, 3};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        ptr->Append(ents);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        snail::raft::HardState hs(0, 0, 1);
        raft->LoadState(hs);
        raft->BecomeFollower(2, 2);

        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgApp;
        m->term = 2;
        m->log_term = tests[i].term;
        m->index = tests[i].index;
        raft->Step(m).get();

        auto msgs = std::move(raft->msgs_);
        auto wmsg = snail::raft::make_raft_message();
        wmsg->from = 1;
        wmsg->to = 2;
        wmsg->type = snail::raft::MessageType::MsgAppResp;
        wmsg->term = 2;
        wmsg->index = tests[i].windex;
        wmsg->reject = tests[i].wreject;
        wmsg->reject_hint = tests[i].wrejectHint;
        wmsg->log_term = tests[i].wlogterm;
        BOOST_REQUIRE_EQUAL(msgs.size(), 1);
        BOOST_REQUIRE((*(msgs[0]) == *wmsg));
    }
}

SEASTAR_THREAD_TEST_CASE(FollowerAppendEntriesTest) {
    LOG_INFO("FollowerAppendEntriesTest...");
    struct testItem {
        uint64_t index;
        uint64_t term;
        std::vector<snail::raft::EntryPtr> ents;
        std::vector<snail::raft::EntryPtr> wents;
        std::vector<snail::raft::EntryPtr> wunstable;
    };

    testItem tests[4];
    snail::raft::EntryPtr ent;

    tests[0].index = 2;
    tests[0].term = 2;
    ent = snail::raft::make_entry();
    ent->set_term(3);
    ent->set_index(3);
    tests[0].ents.push_back(ent);
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        tests[0].wents.push_back(ent);
    }
    ent = snail::raft::make_entry();
    ent->set_term(3);
    ent->set_index(3);
    tests[0].wunstable.push_back(ent);

    tests[1].index = 1;
    tests[1].term = 1;
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 3);
        ent->set_index(i + 2);
        tests[1].ents.push_back(ent);
    }
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        if (i == 0) {
            ent->set_term(i + 1);
        } else {
            ent->set_term(i + 2);
        }
        ent->set_index(i + 1);
        tests[1].wents.push_back(ent);
    }
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 3);
        ent->set_index(i + 2);
        tests[1].wunstable.push_back(ent);
    }

    tests[2].index = 0;
    tests[2].term = 0;
    ent = snail::raft::make_entry();
    ent->set_term(1);
    ent->set_index(1);
    tests[2].ents.push_back(ent);
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        tests[2].wents.push_back(ent);
    }

    tests[3].index = 0;
    tests[3].term = 0;
    ent = snail::raft::make_entry();
    ent->set_term(3);
    ent->set_index(1);
    tests[3].ents.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_term(3);
    ent->set_index(1);
    tests[3].wents.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_term(3);
    ent->set_index(1);
    tests[3].wunstable.push_back(ent);

    for (int i = 0; i < 4; i++) {
        std::vector<snail::raft::EntryPtr> ents;
        for (int j = 0; j < 2; j++) {
            auto ent = snail::raft::make_entry();
            ent->set_term(j + 1);
            ent->set_index(j + 1);
            ents.push_back(ent);
        }
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2, 3};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        ptr->Append(ents);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        raft->BecomeFollower(2, 2);

        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgApp;
        m->term = 2;
        m->log_term = tests[i].term;
        m->index = tests[i].index;
        m->entries = std::move(tests[i].ents);
        raft->Step(m).get();

        std::vector<snail::raft::EntryPtr> entries =
            raft->raft_log_->AllEntries().get0();
        std::cout << "i=" << i << std::endl;
        BOOST_REQUIRE_EQUAL(entries.size(), tests[i].wents.size());
        for (int j = 0; j < entries.size(); j++) {
            BOOST_REQUIRE((*entries[j] == *(tests[i].wents[j])));
        }

        entries = raft->raft_log_->UnstableEntries();
        BOOST_REQUIRE_EQUAL(entries.size(), tests[i].wunstable.size());
        for (int j = 0; j < entries.size(); j++) {
            BOOST_REQUIRE((*entries[j] == *(tests[i].wunstable[j])));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(LeaderSyncFollowerLogTest) {
    LOG_INFO("LeaderSyncFollowerLogTest...");
    std::vector<snail::raft::EntryPtr> ents;
    snail::raft::EntryPtr ent = snail::raft::make_entry();
    ents.push_back(ent);
    for (int i = 0; i < 10; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        if (i < 3) {
            ent->set_term(1);
        } else if (i < 5) {
            ent->set_term(4);
        } else if (i < 7) {
            ent->set_term(5);
        } else {
            ent->set_term(6);
        }
        ents.push_back(ent);
    }
    uint64_t term = 8;

    struct entryItem {
        uint64_t term;
        uint64_t index;
    };
    std::array<std::vector<entryItem>, 6> testEntries;
    testEntries[0] = {{1, 1}, {1, 2}, {1, 3}, {4, 4}, {4, 5},
                      {5, 6}, {5, 7}, {6, 8}, {6, 9}};
    testEntries[1] = {{1, 1}, {1, 2}, {1, 3}, {4, 4}};
    testEntries[2] = {{1, 1}, {1, 2}, {1, 3}, {4, 4},  {4, 5}, {5, 6},
                      {5, 7}, {6, 8}, {6, 9}, {6, 10}, {6, 11}};
    testEntries[3] = {{1, 1}, {1, 2}, {1, 3}, {4, 4},  {4, 5},  {5, 6},
                      {5, 7}, {6, 8}, {6, 9}, {6, 10}, {7, 11}, {7, 12}};
    testEntries[4] = {{1, 1}, {1, 2}, {1, 3}, {4, 4}, {4, 5}, {4, 6}, {4, 7}};
    testEntries[5] = {{1, 1}, {1, 2}, {1, 3}, {2, 4},  {2, 5}, {2, 6},
                      {3, 7}, {3, 8}, {3, 9}, {3, 10}, {3, 11}};
    std::array<std::vector<snail::raft::EntryPtr>, 6> tests;
    for (int i = 0; i < 6; i++) {
        ent = snail::raft::make_entry();
        tests[i].push_back(ent);
        for (int j = 0; j < testEntries[i].size(); j++) {
            ent = snail::raft::make_entry();
            ent->set_term(testEntries[i][j].term);
            ent->set_index(testEntries[i][j].index);
            tests[i].push_back(ent);
        }
    }

    for (int i = 0; i < 6; i++) {
        auto lead_store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> lead_ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(
                lead_store);
        std::vector<uint64_t> peers = {1, 2, 3};
        lead_ptr->snapshot_->metadata().conf_state().set_voters(peers);
        lead_ptr->Append(ents);
        auto lead_raft = newTestRaft(1, 10, 1, lead_store).get0();
        snail::raft::HardState hs;
        hs.set_term(term);
        hs.set_commit(lead_raft->raft_log_->LastIndex());
        lead_raft->LoadState(hs);

        auto follower_store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> follower_ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(
                follower_store);
        follower_ptr->snapshot_->metadata().conf_state().set_voters(peers);
        follower_ptr->Append(tests[i]);
        auto follower_raft = newTestRaft(2, 10, 1, follower_store).get0();
        hs.set_term(term - 1);
        hs.set_commit(0);
        follower_raft->LoadState(hs);

        std::vector<StateMachinePtr> sms;
        sms.push_back(RaftSM::MakeStateMachine(lead_raft));
        sms.push_back(RaftSM::MakeStateMachine(follower_raft));
        sms.push_back(nopStepper);
        auto net = newNetwork(sms);
        std::vector<snail::raft::MessagePtr> msgs;
        auto msg = snail::raft::make_raft_message();
        msg->from = 1;
        msg->to = 1;
        msg->type = snail::raft::MessageType::MsgHup;
        msgs.push_back(msg);
        net->Send(msgs).get();
        msgs.clear();

        msg = snail::raft::make_raft_message();
        msg->from = 3;
        msg->to = 1;
        msg->type = snail::raft::MessageType::MsgVoteResp;
        msg->term = term + 1;
        msgs.push_back(msg);
        net->Send(msgs).get();
        msgs.clear();

        msg = snail::raft::make_raft_message();
        msg->from = 1;
        msg->to = 1;
        msg->type = snail::raft::MessageType::MsgProp;
        msg->entries.push_back(snail::raft::make_entry());
        msgs.push_back(msg);
        net->Send(msgs).get();
        msgs.clear();
        BOOST_REQUIRE_EQUAL(lead_raft->raft_log_->committed(),
                            follower_raft->raft_log_->committed());
        BOOST_REQUIRE_EQUAL(lead_raft->raft_log_->applied(),
                            follower_raft->raft_log_->applied());
        auto lead_ents = lead_raft->raft_log_->AllEntries().get0();
        auto follower_ents = follower_raft->raft_log_->AllEntries().get0();
        BOOST_REQUIRE_EQUAL(lead_ents.size(), follower_ents.size());
        for (int i = 0; i < lead_ents.size(); i++) {
            BOOST_REQUIRE((*lead_ents[i] == *follower_ents[i]));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(VoteRequestTest) {
    LOG_INFO("VoteRequestTest...");
    struct testItem {
        std::vector<snail::raft::EntryPtr> ents;
        uint64_t wterm;
    };

    testItem tests[2];
    snail::raft::EntryPtr ent;
    ent = snail::raft::make_entry();
    ent->set_term(1);
    ent->set_index(1);
    tests[0].ents.push_back(ent);
    tests[0].wterm = 2;

    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        tests[1].ents.push_back(ent);
    }
    tests[1].wterm = 3;

    for (int i = 0; i < 2; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2, 3};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        auto raft = newTestRaft(1, 10, 1, store).get0();

        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgApp;
        m->term = tests[i].wterm - 1;
        m->log_term = 0;
        m->index = 0;
        m->entries = tests[i].ents;
        raft->Step(m).get();
        std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);

        for (int i = 1; i < raft->election_timeout_ * 2; i++) {
            raft->TickElection().get();
        }
        msgs = std::move(raft->msgs_);
        std::sort(
            msgs.begin(), msgs.end(),
            [](snail::raft::MessagePtr m1, snail::raft::MessagePtr m2) -> bool {
                return m1->to < m2->to;
            });
        BOOST_REQUIRE_EQUAL(msgs.size(), 2);
        for (int j = 0; j < msgs.size(); j++) {
            auto msg = msgs[j];
            BOOST_REQUIRE_EQUAL(msg->type, snail::raft::MessageType::MsgVote);
            BOOST_REQUIRE_EQUAL(msg->to, j + 2);
            BOOST_REQUIRE_EQUAL(msg->term, tests[i].wterm);
            BOOST_REQUIRE_EQUAL(msg->index, tests[i].ents.back()->index());
            BOOST_REQUIRE_EQUAL(msg->log_term, tests[i].ents.back()->term());
        }
    }
}

SEASTAR_THREAD_TEST_CASE(VoterTest) {
    LOG_INFO("VoterTest...");
    struct testItem {
        std::vector<snail::raft::EntryPtr> ents;
        uint64_t logterm;
        uint64_t index;
        bool wreject;
    };

    testItem tests[9];
    snail::raft::EntryPtr ent;

    ent = snail::raft::make_entry();
    ent->set_term(1);
    ent->set_index(1);
    tests[0] = {{ent}, 1, 1, false};
    ent = snail::raft::make_entry();
    ent->set_term(1);
    ent->set_index(1);
    tests[1] = {{ent}, 1, 2, false};
    tests[2] = {{}, 1, 1, true};
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(1);
        ent->set_index(i + 1);
        tests[2].ents.push_back(ent);
    }
    ent = snail::raft::make_entry();
    ent->set_term(1);
    ent->set_index(1);
    tests[3] = {{ent}, 2, 1, false};
    ent = snail::raft::make_entry();
    ent->set_term(1);
    ent->set_index(1);
    tests[4] = {{ent}, 2, 2, false};
    tests[5] = {{}, 2, 1, false};
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(1);
        ent->set_index(i + 1);
        tests[5].ents.push_back(ent);
    }
    ent = snail::raft::make_entry();
    ent->set_term(2);
    ent->set_index(1);
    tests[6] = {{ent}, 1, 1, true};
    ent = snail::raft::make_entry();
    ent->set_term(2);
    ent->set_index(1);
    tests[7] = {{ent}, 1, 2, true};
    tests[8] = {{}, 1, 1, true};
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(2 - i);
        ent->set_index(i + 1);
        tests[8].ents.push_back(ent);
    }

    for (int i = 0; i < 9; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        ptr->Append(tests[i].ents);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgVote;
        m->term = 3;
        m->log_term = tests[i].logterm;
        m->index = tests[i].index;
        raft->Step(m).get();

        auto msgs = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(msgs.size(), 1);
        BOOST_REQUIRE_EQUAL(msgs[0]->type,
                            snail::raft::MessageType::MsgVoteResp);
        BOOST_REQUIRE_EQUAL(msgs[0]->reject, tests[i].wreject);
    }
}

SEASTAR_THREAD_TEST_CASE(LeaderOnlyCommitsLogFromCurrentTermTest) {
    LOG_INFO("LeaderOnlyCommitsLogFromCurrentTermTest...");
    std::vector<snail::raft::EntryPtr> ents;
    for (int i = 0; i < 2; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        ents.push_back(ent);
    }
    struct testItem {
        uint64_t index;
        uint64_t wcommit;
    };
    testItem tests[3] = {{1, 0}, {2, 0}, {3, 3}};
    for (int i = 0; i < 3; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<uint64_t> peers = {1, 2};
        ptr->snapshot_->metadata().conf_state().set_voters(peers);
        ptr->Append(ents);
        auto raft = newTestRaft(1, 10, 1, store).get0();
        snail::raft::HardState hs(2, 0, 0);
        raft->LoadState(hs);
        raft->BecomeCandidate();
        raft->BecomeLeader().get();

        std::vector<snail::raft::MessagePtr> msgs = std::move(raft->msgs_);
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        m->entries.push_back(snail::raft::make_entry());
        raft->Step(m).get();

        m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgAppResp;
        m->term = raft->term_;
        m->index = tests[i].index;
        raft->Step(m).get();
        BOOST_REQUIRE_EQUAL(raft->raft_log_->committed(), tests[i].wcommit);
    }
}

}  // namespace raft
}  // namespace snail
