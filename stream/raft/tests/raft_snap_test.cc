#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "raft/raft.h"
#include "raft_test.h"

namespace snail {
namespace raft {

static snail::raft::SnapshotPtr getTestingSnap() {
    static snail::raft::SnapshotPtr testingSnap;
    if (testingSnap) {
        return testingSnap;
    }
    testingSnap = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_index(11);
    meta.set_term(11);
    snail::raft::ConfState cs;
    std::vector<uint64_t> voters = {1, 2};
    cs.set_voters(std::move(voters));
    meta.set_conf_state(std::move(cs));
    testingSnap->set_metadata(std::move(meta));
    return testingSnap;
}

SEASTAR_THREAD_TEST_CASE(SendingSnapshotSetPendingSnapshotTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 10, 1, store).get0();
    raft->Restore(getTestingSnap()).get();

    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    raft->prs_->progress()[2]->set_next(raft->raft_log_->FirstIndex());
    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgAppResp;
    m->index = raft->prs_->progress()[2]->next() - 1;
    m->reject = true;
    raft->Step(m).get();
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->pending_snapshot(), 11);
}

SEASTAR_THREAD_TEST_CASE(PendingSnapshotPauseReplicationTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 10, 1, store).get0();
    raft->Restore(getTestingSnap()).get();

    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    raft->prs_->progress()[2]->BecomeSnapshot(11);
    auto m = snail::raft::make_raft_message();
    m->from = 1;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgProp;
    seastar::temporary_buffer<char> data("somedata", 8);
    auto ent = snail::raft::make_entry();
    ent->set_data(std::move(data));
    m->entries.push_back(ent);
    raft->Step(m).get();

    auto msgs = std::move(raft->msgs_);
    BOOST_REQUIRE_EQUAL(msgs.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(SnapshotFailureTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 10, 1, store).get0();
    raft->Restore(getTestingSnap()).get();

    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    raft->prs_->progress()[2]->set_next(1);
    raft->prs_->progress()[2]->BecomeSnapshot(11);

    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgSnapStatus;
    m->reject = true;
    raft->Step(m).get();
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->pending_snapshot(), 0);
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->next(), 1);
    BOOST_REQUIRE(raft->prs_->progress()[2]->probe_sent());
}

SEASTAR_THREAD_TEST_CASE(SnapshotSucceedTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 10, 1, store).get0();
    raft->Restore(getTestingSnap()).get();

    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    raft->prs_->progress()[2]->set_next(1);
    raft->prs_->progress()[2]->BecomeSnapshot(11);

    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgSnapStatus;
    m->reject = false;
    raft->Step(m).get();
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->pending_snapshot(), 0);
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->next(), 12);
    BOOST_REQUIRE(raft->prs_->progress()[2]->probe_sent());
}

SEASTAR_THREAD_TEST_CASE(SnapshotAbortTest) {
    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    std::vector<uint64_t> peers = {1, 2};
    ptr->snapshot_->metadata().conf_state().set_voters(peers);
    auto raft = newTestRaft(1, 10, 1, store).get0();
    raft->Restore(getTestingSnap()).get();

    raft->BecomeCandidate();
    raft->BecomeLeader().get();

    raft->prs_->progress()[2]->set_next(1);
    raft->prs_->progress()[2]->BecomeSnapshot(11);

    auto m = snail::raft::make_raft_message();
    m->from = 2;
    m->to = 1;
    m->type = snail::raft::MessageType::MsgAppResp;
    m->index = 11;
    raft->Step(m).get();
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->pending_snapshot(), 0);
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->next(), 13);
    BOOST_REQUIRE_EQUAL(raft->prs_->progress()[2]->inflights().Count(), 1);
}

}  // namespace raft
}  // namespace snail
