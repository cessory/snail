#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "raft/raft.h"
#include "raft_test.h"

namespace snail {
namespace raft {

SEASTAR_THREAD_TEST_CASE(MsgAppFlowControlFullTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    auto pr2 = raft->prs_->progress()[2];
    pr2->BecomeReplicate();

    for (int i = 0; i < raft->prs_->max_inflight(); i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("somedata", 8);
        ent->set_data(std::move(data));
        m->entries.push_back(ent);
        raft->Step(m).get();
        auto ms = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(ms.size(), 1);
    }
    BOOST_REQUIRE(pr2->inflights().Full());
    for (int i = 0; i < 10; i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("somedata", 8);
        ent->set_data(std::move(data));
        m->entries.push_back(ent);
        raft->Step(m).get();
        auto ms = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(ms.size(), 0);
    }
}

SEASTAR_THREAD_TEST_CASE(MsgAppFlowControlMoveForwardTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    auto pr2 = raft->prs_->progress()[2];
    pr2->BecomeReplicate();

    for (int i = 0; i < raft->prs_->max_inflight(); i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("somedata", 8);
        ent->set_data(std::move(data));
        m->entries.push_back(ent);
        raft->Step(m).get();
        auto ms = std::move(raft->msgs_);
    }

    for (int i = 2; i < raft->prs_->max_inflight(); i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgAppResp;
        m->index = i;
        raft->Step(m).get();
        auto ms = std::move(raft->msgs_);

        m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("somedata", 8);
        ent->set_data(std::move(data));
        m->entries.push_back(ent);
        raft->Step(m).get();
        ms = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(ms.size(), 1);
        BOOST_REQUIRE(pr2->inflights().Full());
        for (int j = 0; j < i; j++) {
            m = snail::raft::make_raft_message();
            m->from = 2;
            m->to = 1;
            m->type = snail::raft::MessageType::MsgAppResp;
            m->index = j;
            raft->Step(m).get();
            BOOST_REQUIRE(pr2->inflights().Full());
        }
    }
}

SEASTAR_THREAD_TEST_CASE(MsgAppFlowControlRecvHeartbeatTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 5, 1, store).get0();
    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    auto pr2 = raft->prs_->progress()[2];
    pr2->BecomeReplicate();

    for (int i = 0; i < raft->prs_->max_inflight(); i++) {
        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("somedata", 8);
        ent->set_data(std::move(data));
        m->entries.push_back(ent);
        raft->Step(m).get();
        auto ms = std::move(raft->msgs_);
    }

    for (int i = 1; i < 5; i++) {
        BOOST_REQUIRE(pr2->inflights().Full());
        for (int j = 0; j < i; j++) {
            auto m = snail::raft::make_raft_message();
            m->from = 2;
            m->to = 1;
            m->type = snail::raft::MessageType::MsgHeartbeatResp;
            raft->Step(m).get();
            auto ms = std::move(raft->msgs_);
            BOOST_REQUIRE(!pr2->inflights().Full());
        }

        auto m = snail::raft::make_raft_message();
        m->from = 1;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgProp;
        auto ent = snail::raft::make_entry();
        seastar::temporary_buffer<char> data("somedata", 8);
        ent->set_data(std::move(data.clone()));
        m->entries.push_back(ent);
        raft->Step(m).get();
        auto ms = std::move(raft->msgs_);
        BOOST_REQUIRE_EQUAL(ms.size(), 1);

        for (int j = 0; j < 10; j++) {
            m = snail::raft::make_raft_message();
            m->from = 1;
            m->to = 1;
            m->type = snail::raft::MessageType::MsgProp;
            ent->set_data(std::move(data.clone()));
            m->entries.push_back(ent);
            raft->Step(m).get();
            ms = std::move(raft->msgs_);
            BOOST_REQUIRE_EQUAL(ms.size(), 0);
        }

        // clear all pending messages.
        m = snail::raft::make_raft_message();
        m->from = 2;
        m->to = 1;
        m->type = snail::raft::MessageType::MsgHeartbeatResp;
        raft->Step(m).get();
        ms = std::move(raft->msgs_);
    }
}

}  // namespace raft
}  // namespace snail
