
#include "raft/raft_proto.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "util/util.h"

namespace snail {
namespace raft {

SEASTAR_THREAD_TEST_CASE(EntryTest) {
    snail::raft::Entry ent;
    BOOST_REQUIRE_EQUAL(ent.ByteSize(), 0);
    ent.set_term(10000000000);
    ent.set_index(20000000000);

    auto size =
        2 + snail::VarintLength(10000000000) + snail::VarintLength(20000000000);
    BOOST_REQUIRE_EQUAL(ent.ByteSize(), size);

    seastar::temporary_buffer<char> buf(size);
    ent.MarshalTo(buf.get_write());
    ent.Reset();
    BOOST_REQUIRE_EQUAL(ent.index(), 0);
    BOOST_REQUIRE_EQUAL(ent.term(), 0);
    ent.Unmarshal(std::move(buf.share()));
    BOOST_REQUIRE_EQUAL(ent.index(), 20000000000);
    BOOST_REQUIRE_EQUAL(ent.term(), 10000000000);

    seastar::temporary_buffer<char> data(4096);
    memset(data.get_write(), 'a', data.size());
    ent.set_data(std::move(data.share()));

    size = ent.ByteSize();
    seastar::temporary_buffer<char> buf1(size);
    ent.MarshalTo(buf1.get_write());
    ent.Reset();
    BOOST_REQUIRE(!ent.Unmarshal(std::move(buf1.share(0, size - 1))));
    ent.Unmarshal(std::move(buf1.share()));
    BOOST_REQUIRE_EQUAL(ent.index(), 20000000000);
    BOOST_REQUIRE_EQUAL(ent.term(), 10000000000);
    BOOST_REQUIRE(ent.data() == data);
}

SEASTAR_THREAD_TEST_CASE(ConfStateTest) {
    snail::raft::ConfState cs;
    BOOST_REQUIRE_EQUAL(cs.ByteSize(), 0);
    cs.set_auto_leave(true);
    BOOST_REQUIRE_EQUAL(cs.ByteSize(), 1);

    seastar::temporary_buffer<char> b0(cs.ByteSize());
    cs.MarshalTo(b0.get_write());
    cs.Reset();
    BOOST_REQUIRE(!cs.auto_leave());

    cs.Unmarshal(std::move(b0.share()));
    BOOST_REQUIRE(cs.auto_leave());
    BOOST_REQUIRE(cs.voters().empty());
    BOOST_REQUIRE(cs.learners().empty());
    BOOST_REQUIRE(cs.voters_outgoing().empty());
    BOOST_REQUIRE(cs.learners_next().empty());

    std::vector<uint64_t> &voters = cs.voters();
    for (int i = 0; i < 5; i++) {
        voters.push_back(i + 1);
    }

    BOOST_REQUIRE_EQUAL(cs.voters().size(), 5);
    seastar::temporary_buffer<char> b1(cs.ByteSize());
    cs.MarshalTo(b1.get_write());
    cs.Reset();
    BOOST_REQUIRE(cs.Unmarshal(std::move(b1.share())));
    BOOST_REQUIRE(cs.auto_leave());
    BOOST_REQUIRE_EQUAL(cs.voters().size(), 5);
    for (int i = 0; i < 5; i++) {
        BOOST_REQUIRE_EQUAL(cs.voters()[i], i + 1);
    }
    BOOST_REQUIRE(cs.learners().empty());
    BOOST_REQUIRE(cs.voters_outgoing().empty());
    BOOST_REQUIRE(cs.learners_next().empty());

    std::vector<uint64_t> &learners = cs.learners();
    for (int i = 0; i < 5; i++) {
        learners.push_back(i + 1);
    }
    BOOST_REQUIRE_EQUAL(cs.learners().size(), 5);
    seastar::temporary_buffer<char> b2(cs.ByteSize());
    cs.MarshalTo(b2.get_write());
    cs.Reset();
    BOOST_REQUIRE(cs.Unmarshal(std::move(b2.share())));
    BOOST_REQUIRE(cs.auto_leave());
    BOOST_REQUIRE_EQUAL(cs.voters().size(), 5);
    for (int i = 0; i < 5; i++) {
        BOOST_REQUIRE_EQUAL(cs.voters()[i], i + 1);
    }
    BOOST_REQUIRE_EQUAL(cs.learners().size(), 5);
    for (int i = 0; i < 5; i++) {
        BOOST_REQUIRE_EQUAL(cs.learners()[i], i + 1);
    }
    BOOST_REQUIRE(cs.voters_outgoing().empty());
    BOOST_REQUIRE(cs.learners_next().empty());

    std::vector<uint64_t> &voters_outgoing = cs.voters_outgoing();
    std::vector<uint64_t> &learners_next = cs.learners_next();
    for (int i = 0; i < 5; i++) {
        voters_outgoing.push_back(i + 1);
        learners_next.push_back(i + 1);
    }
    seastar::temporary_buffer<char> b3(cs.ByteSize());
    cs.MarshalTo(b3.get_write());
    cs.Reset();
    BOOST_REQUIRE(cs.Unmarshal(std::move(b3.share())));
    BOOST_REQUIRE(cs.auto_leave());
    for (int i = 0; i < 5; i++) {
        BOOST_REQUIRE_EQUAL(cs.voters()[i], i + 1);
        BOOST_REQUIRE_EQUAL(cs.learners()[i], i + 1);
        BOOST_REQUIRE_EQUAL(cs.voters_outgoing()[i], i + 1);
        BOOST_REQUIRE_EQUAL(cs.learners_next()[i], i + 1);
    }
}

SEASTAR_THREAD_TEST_CASE(SnapshotMarshalTest) {
    snail::raft::Snapshot snap;
    snail::raft::SnapshotMetadata meta;
    snail::raft::ConfState cs;
    seastar::temporary_buffer<char> data("somedata", 8);

    BOOST_REQUIRE_EQUAL(snap.ByteSize(), 0);

    meta.set_index(std::numeric_limits<uint64_t>::max());
    meta.set_term(std::numeric_limits<uint64_t>::max() / 2);
    for (int i = 0; i < 5; i++) {
        cs.voters().push_back(i + 1);
        cs.learners().push_back(i + 1);
        cs.voters_outgoing().push_back(i + 1);
        cs.learners_next().push_back(i + 1);
    }
    meta.set_conf_state(std::move(cs));

    snap.set_data(std::move(data.share()));
    snap.set_metadata(std::move(meta));

    seastar::temporary_buffer<char> b(snap.ByteSize());
    snap.MarshalTo(b.get_write());
    snap.Reset();

    BOOST_REQUIRE(snap.Empty());
    BOOST_REQUIRE(snap.Unmarshal(std::move(b.share())));

    BOOST_REQUIRE(snap.data() == data);
    BOOST_REQUIRE_EQUAL(snap.metadata().index(),
                        std::numeric_limits<uint64_t>::max());
    BOOST_REQUIRE_EQUAL(snap.metadata().term(),
                        std::numeric_limits<uint64_t>::max() / 2);
    for (int i = 0; i < 5; i++) {
        BOOST_REQUIRE_EQUAL(snap.metadata().conf_state().voters()[i], i + 1);
        BOOST_REQUIRE_EQUAL(snap.metadata().conf_state().learners()[i], i + 1);
        BOOST_REQUIRE_EQUAL(snap.metadata().conf_state().voters_outgoing()[i],
                            i + 1);
        BOOST_REQUIRE_EQUAL(snap.metadata().conf_state().learners_next()[i],
                            i + 1);
    }
    BOOST_REQUIRE(!snap.metadata().conf_state().auto_leave());
}

SEASTAR_THREAD_TEST_CASE(MessageMarshalTest) {
    snail::raft::Message m;

    BOOST_REQUIRE_EQUAL(m.ByteSize(), 0);
    m.to = 100;
    m.from = 1000;
    m.entries.push_back(snail::raft::make_entry());

    seastar::temporary_buffer<char> b(m.ByteSize());
    m.MarshalTo(b.get_write());
    m.Reset();

    BOOST_REQUIRE(m.Empty());
    BOOST_REQUIRE(m.Unmarshal(std::move(b.share())));
    BOOST_REQUIRE_EQUAL(m.type, snail::raft::MessageType::MsgHup);
    BOOST_REQUIRE_EQUAL(m.to, 100);
    BOOST_REQUIRE_EQUAL(m.from, 1000);
    BOOST_REQUIRE_EQUAL(m.entries.size(), 1);
    BOOST_REQUIRE_EQUAL(m.entries[0]->ByteSize(), 0);

    m.entries.clear();
    seastar::temporary_buffer<char> data("somedata", 8);
    for (int i = 0; i < 10; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term(i + 1);
        ent->set_data(std::move(data.share()));
        m.entries.push_back(ent);
    }
    m.snapshot = snail::raft::make_snapshot();
    m.snapshot->metadata().set_index(100);
    m.snapshot->metadata().set_term(100);
    m.snapshot->set_data(std::move(data.share()));
    for (int i = 0; i < 5; i++) {
        m.snapshot->metadata().conf_state().voters().push_back(i + 1);
        m.snapshot->metadata().conf_state().learners().push_back(i + 1);
        m.snapshot->metadata().conf_state().voters_outgoing().push_back(i + 1);
        m.snapshot->metadata().conf_state().learners_next().push_back(i + 1);
    }
    m.context = std::move(data.share());

    seastar::temporary_buffer<char> b1(m.ByteSize());
    m.MarshalTo(b1.get_write());
    m.Reset();
    BOOST_REQUIRE(m.Empty());
    BOOST_REQUIRE(m.Unmarshal(std::move(b1.share())));
    BOOST_REQUIRE_EQUAL(m.type, snail::raft::MessageType::MsgHup);
    BOOST_REQUIRE_EQUAL(m.to, 100);
    BOOST_REQUIRE_EQUAL(m.from, 1000);
    BOOST_REQUIRE_EQUAL(m.entries.size(), 10);
    for (int i = 0; i < 10; i++) {
        auto ent = m.entries[i];
        BOOST_REQUIRE_EQUAL(ent->index(), i + 1);
        BOOST_REQUIRE_EQUAL(ent->term(), i + 1);
        BOOST_REQUIRE(ent->data() == data);
    }
    BOOST_REQUIRE_EQUAL(m.snapshot->metadata().index(), 100);
    BOOST_REQUIRE_EQUAL(m.snapshot->metadata().term(), 100);
    BOOST_REQUIRE(m.snapshot->data() == data);
    BOOST_REQUIRE_EQUAL(m.snapshot->metadata().conf_state().voters().size(), 5);
    BOOST_REQUIRE_EQUAL(m.snapshot->metadata().conf_state().learners().size(),
                        5);
    BOOST_REQUIRE_EQUAL(
        m.snapshot->metadata().conf_state().voters_outgoing().size(), 5);
    BOOST_REQUIRE_EQUAL(
        m.snapshot->metadata().conf_state().learners_next().size(), 5);

    for (int i = 0; i < 5; i++) {
        BOOST_REQUIRE_EQUAL(m.snapshot->metadata().conf_state().voters()[i],
                            i + 1);
        BOOST_REQUIRE_EQUAL(m.snapshot->metadata().conf_state().learners()[i],
                            i + 1);
        BOOST_REQUIRE_EQUAL(
            m.snapshot->metadata().conf_state().voters_outgoing()[i], i + 1);
        BOOST_REQUIRE_EQUAL(
            m.snapshot->metadata().conf_state().learners_next()[i], i + 1);
    }
    BOOST_REQUIRE(m.context == data);
}

SEASTAR_THREAD_TEST_CASE(HardStateMarshalTest) {
    snail::raft::HardState hs;
    BOOST_REQUIRE_EQUAL(hs.ByteSize(), 0);
    hs.set_term(9827387438);
    hs.set_vote(5);
    hs.set_commit(7439817938249089);

    seastar::temporary_buffer<char> b(hs.ByteSize());
    hs.MarshalTo(b.get_write());
    hs.Reset();
    BOOST_REQUIRE(hs.Empty());
    BOOST_REQUIRE(hs.Unmarshal(std::move(b.share())));
    BOOST_REQUIRE_EQUAL(hs.term(), 9827387438);
    BOOST_REQUIRE_EQUAL(hs.vote(), 5);
    BOOST_REQUIRE_EQUAL(hs.commit(), 7439817938249089);
}

SEASTAR_THREAD_TEST_CASE(ConfChangeMarshalTest) {
    snail::raft::ConfChange cc;
    seastar::temporary_buffer<char> data("somedata", 8);

    BOOST_REQUIRE_EQUAL(cc.ByteSize(), 0);
    cc.set_type(snail::raft::ConfChangeType::ConfChangeAddLearnerNode);
    cc.set_node_id(5);
    cc.set_context(std::move(data.share()));

    seastar::temporary_buffer<char> b(cc.ByteSize());
    cc.MarshalTo(b.get_write());
    cc.Reset();
    BOOST_REQUIRE(cc.Empty());
    BOOST_REQUIRE(cc.Unmarshal(std::move(b.share())));
    BOOST_REQUIRE(cc.type() ==
                  snail::raft::ConfChangeType::ConfChangeAddLearnerNode);
    BOOST_REQUIRE_EQUAL(cc.node_id(), 5);
    BOOST_REQUIRE(cc.context() == data);
}

SEASTAR_THREAD_TEST_CASE(ConfChangeSingleTest) {
    snail::raft::ConfChangeSingle ccs;

    BOOST_REQUIRE_EQUAL(ccs.ByteSize(), 0);
    ccs.set_type(snail::raft::ConfChangeType::ConfChangeAddLearnerNode);
    ccs.set_node_id(10);

    seastar::temporary_buffer<char> b(ccs.ByteSize());
    ccs.MarshalTo(b.get_write());
    ccs.Reset();
    BOOST_REQUIRE(ccs.Empty());
    BOOST_REQUIRE(ccs.Unmarshal(std::move(b.share())));
    BOOST_REQUIRE(ccs.type() ==
                  snail::raft::ConfChangeType::ConfChangeAddLearnerNode);
    BOOST_REQUIRE_EQUAL(ccs.node_id(), 10);

    snail::raft::ConfChangeV2 ccv2;
    BOOST_REQUIRE_EQUAL(ccv2.ByteSize(), 0);

    seastar::temporary_buffer<char> data("somedata", 8);
    ccv2.changes().push_back(ccs);
    BOOST_REQUIRE(ccv2.changes()[0].type() == ccs.type());
    ccv2.set_context(std::move(data.share()));
    BOOST_REQUIRE_EQUAL(ccv2.changes().size(), 1);

    seastar::temporary_buffer<char> b1(ccv2.ByteSize());
    ccv2.MarshalTo(b1.get_write());
    ccv2.Reset();
    BOOST_REQUIRE(ccv2.Empty());
    BOOST_REQUIRE_EQUAL(ccv2.changes().size(), 0);
    BOOST_REQUIRE(ccv2.Unmarshal(std::move(b1.share())));
    BOOST_REQUIRE(ccv2.transition() ==
                  snail::raft::ConfChangeTransition::ConfChangeTransitionAuto);
    BOOST_REQUIRE_EQUAL(ccv2.changes().size(), 1);
    BOOST_REQUIRE(ccv2.changes()[0].type() == ccs.type());
    BOOST_REQUIRE_EQUAL(ccv2.changes()[0].node_id(), ccs.node_id());
    BOOST_REQUIRE(ccv2.context() == data);
}

}  // namespace raft
}  // namespace snail
