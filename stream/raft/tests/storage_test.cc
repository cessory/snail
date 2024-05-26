#include "raft/storage.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <vector>

#include "raft/raft_proto.h"

namespace snail {
namespace raft {

SEASTAR_THREAD_TEST_CASE(StorageTermTest) {
    std::deque<EntryPtr> ents;

    for (uint64_t i = 3; i < 6; i++) {
        auto ent = make_entry();
        ent->set_index(i);
        ent->set_term(i);
        ents.push_back(ent);
    }

    struct result {
        uint64_t i;
        ErrCode code;
        uint64_t term;
    };

    result res[] = {
        {2, ErrCode::ErrRaftCompacted, 0},
        {3, ErrCode::OK, 3},
        {4, ErrCode::OK, 4},
        {5, ErrCode::OK, 5},
        {6, ErrCode::ErrRaftUnavailable, 0},
    };

    auto s = std::make_unique<MemoryStorage>();
    s->ents_ = ents;
    for (int i = 0; i < 5; i++) {
        auto st = s->Term(res[i].i).get0();
        BOOST_REQUIRE(st.Code() == res[i].code);
        if (st) {
            BOOST_REQUIRE_EQUAL(st.Value(), res[i].term);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(StorageEntriesTest) {
    std::deque<EntryPtr> ents;

    for (uint64_t i = 3; i < 7; i++) {
        auto ent = make_entry();
        ent->set_index(i);
        ent->set_term(i);
        ents.push_back(ent);
    }

    struct result {
        uint64_t lo;
        uint64_t hi;
        uint64_t maxSize;
        ErrCode code;
        std::vector<snail::raft::EntryPtr> ents;
    };

    result res[10];
    res[0] = {2, 6, std::numeric_limits<uint64_t>::max(),
              ErrCode::ErrRaftCompacted};
    res[1] = {3, 4, std::numeric_limits<uint64_t>::max(),
              ErrCode::ErrRaftCompacted};
    res[2] = {4, 5, std::numeric_limits<uint64_t>::max(), ErrCode::OK};
    {
        auto ent = make_entry();
        ent->set_index(4);
        ent->set_term(4);
        res[2].ents.push_back(ent);
    }
    res[3] = {4, 6, std::numeric_limits<uint64_t>::max(), ErrCode::OK};
    {
        for (uint64_t i = 4; i < 6; i++) {
            auto ent = make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[3].ents.push_back(ent);
        }
    }
    res[4] = {4, 7, std::numeric_limits<uint64_t>::max(), ErrCode::OK};
    {
        for (uint64_t i = 4; i < 7; i++) {
            auto ent = make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[4].ents.push_back(ent);
        }
    }
    res[5] = {4, 7, 0, ErrCode::OK};
    {
        for (uint64_t i = 4; i < 5; i++) {
            auto ent = make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[5].ents.push_back(ent);
        }
    }
    res[6] = {4, 7, ents[1]->ByteSize() + ents[2]->ByteSize(), ErrCode::OK};
    {
        for (uint64_t i = 4; i < 6; i++) {
            auto ent = make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[6].ents.push_back(ent);
        }
    }
    res[7] = {
        4, 7,
        ents[1]->ByteSize() + ents[2]->ByteSize() + ents[3]->ByteSize() / 2,
        ErrCode::OK};
    {
        for (uint64_t i = 4; i < 6; i++) {
            auto ent = make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[7].ents.push_back(ent);
        }
    }
    res[8] = {
        4, 7,
        ents[1]->ByteSize() + ents[2]->ByteSize() + ents[3]->ByteSize() - 1,
        ErrCode::OK};
    {
        for (uint64_t i = 4; i < 6; i++) {
            auto ent = make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[8].ents.push_back(ent);
        }
    }
    res[9] = {4, 7,
              ents[1]->ByteSize() + ents[2]->ByteSize() + ents[3]->ByteSize(),
              ErrCode::OK};
    {
        for (uint64_t i = 4; i < 7; i++) {
            auto ent = make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[9].ents.push_back(ent);
        }
    }

    auto s = std::make_unique<snail::raft::MemoryStorage>();
    s->ents_ = ents;
    for (int i = 0; i < 10; i++) {
        auto st = s->Entries(res[i].lo, res[i].hi, res[i].maxSize).get0();
        BOOST_REQUIRE(st.Code() == res[i].code);
        BOOST_REQUIRE_EQUAL(res[i].ents.size(), st.Value().size());
        for (int n = 0; n < st.Value().size(); n++) {
            BOOST_REQUIRE_EQUAL(res[i].ents[n]->type(), st.Value()[n]->type());
            BOOST_REQUIRE_EQUAL(res[i].ents[n]->term(), st.Value()[n]->term());
            BOOST_REQUIRE_EQUAL(res[i].ents[n]->index(),
                                st.Value()[n]->index());
        }
    }
}

SEASTAR_THREAD_TEST_CASE(StorageLastIndexTest) {
    std::deque<EntryPtr> ents;
    for (uint64_t i = 3; i < 6; i++) {
        auto ent = make_entry();
        ent->set_index(i);
        ent->set_term(i);
        ents.push_back(ent);
    }

    auto s = std::make_unique<MemoryStorage>();
    s->ents_ = ents;
    auto last = s->LastIndex();
    BOOST_REQUIRE_EQUAL(last, 5);
    auto ent = make_entry();
    ent->set_index(6);
    ent->set_term(5);
    s->Append({ent});

    last = s->LastIndex();
    BOOST_REQUIRE_EQUAL(last, 6);
}

SEASTAR_THREAD_TEST_CASE(StorageFirstIndexTest) {
    std::deque<EntryPtr> ents;
    for (uint64_t i = 3; i < 6; i++) {
        auto ent = make_entry();
        ent->set_index(i);
        ent->set_term(i);
        ents.push_back(ent);
    }

    auto s = std::make_unique<MemoryStorage>();
    s->ents_ = ents;
    auto first = s->FirstIndex();
    BOOST_REQUIRE_EQUAL(first, 4);
    s->Compact(4);

    first = s->FirstIndex();
    BOOST_REQUIRE_EQUAL(first, 5);
}

SEASTAR_THREAD_TEST_CASE(StorageCompactTest) {
    std::deque<EntryPtr> ents;
    for (uint64_t i = 3; i < 6; i++) {
        auto ent = make_entry();
        ent->set_index(i);
        ent->set_term(i);
        ents.push_back(ent);
    }

    struct result {
        uint64_t i;
        ErrCode code;
        uint64_t index;
        uint64_t term;
        int len;
    };
    result res[] = {
        {2, ErrCode::ErrRaftCompacted, 3, 3, 3},
        {3, ErrCode::ErrRaftCompacted, 3, 3, 3},
        {4, ErrCode::OK, 4, 4, 2},
        {5, ErrCode::OK, 5, 5, 1},
    };

    for (int i = 0; i < 4; i++) {
        auto s = std::make_unique<MemoryStorage>();
        s->ents_ = ents;
        auto st = s->Compact(res[i].i);
        BOOST_REQUIRE(res[i].code == st.Code());
        BOOST_REQUIRE_EQUAL(s->ents_[0]->index(), res[i].index);
        BOOST_REQUIRE_EQUAL(s->ents_[0]->term(), res[i].term);
    }
}

SEASTAR_THREAD_TEST_CASE(StorageCreateSnapshotTest) {
    std::deque<EntryPtr> ents;
    for (uint64_t i = 3; i < 6; i++) {
        auto ent = make_entry();
        ent->set_index(i);
        ent->set_term(i);
        ents.push_back(ent);
    }

    ConfState cs;
    std::vector<uint64_t> v = {1, 2, 3};
    cs.set_voters(std::move(v));
    seastar::temporary_buffer<char> data("data", 4);

    struct result {
        uint64_t i;
        ErrCode code;
        SnapshotPtr snap;
    };
    result res[] = {
        {4, ErrCode::OK, make_snapshot()},
        {5, ErrCode::OK, make_snapshot()},
    };

    for (uint64_t i = 4; i < 6; i++) {
        res[i - 4].snap->set_data(data.share());
        SnapshotMetadata m;
        m.set_index(i);
        m.set_term(i);
        m.set_conf_state(cs);
        res[i - 4].snap->set_metadata(std::move(m));
    }
    for (int i = 0; i < 2; i++) {
        auto s = std::make_unique<MemoryStorage>();
        s->ents_ = ents;
        auto st = s->CreateSnapshot(res[i].i, &cs, data.share());
        BOOST_REQUIRE(st.Code() == res[i].code);
        if (st) {
            auto snap = st.Value();
            BOOST_REQUIRE(*snap == *res[i].snap);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(StorageAppendTest) {
    std::deque<EntryPtr> ents;
    for (uint64_t i = 3; i < 6; i++) {
        auto ent = make_entry();
        ent->set_index(i);
        ent->set_term(i);
        ents.push_back(ent);
    }

    struct result {
        std::vector<snail::raft::EntryPtr> entries;
        ErrCode code;
        std::vector<snail::raft::EntryPtr> wentries;
    };

    result res[7];

    {
        for (uint64_t i = 1; i < 3; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[0].entries.push_back(ent);
        }
        res[0].code = ErrCode::OK;
        for (uint64_t i = 3; i < 6; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[0].wentries.push_back(ent);
        }
    }

    {
        for (uint64_t i = 3; i < 6; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[1].entries.push_back(ent);
        }
        res[1].code = ErrCode::OK;
        for (uint64_t i = 3; i < 6; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(i);
            res[1].wentries.push_back(ent);
        }
    }

    {
        uint64_t t[] = {3, 6, 6};
        for (uint64_t i = 3; i < 6; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(t[i - 3]);
            res[2].entries.push_back(ent);
        }
        res[2].code = ErrCode::OK;
        for (uint64_t i = 3; i < 6; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(t[i - 3]);
            res[2].wentries.push_back(ent);
        }
    }

    {
        uint64_t t[] = {3, 4, 5, 5};
        for (uint64_t i = 3; i < 7; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(t[i - 3]);
            res[3].entries.push_back(ent);
        }
        res[3].code = ErrCode::OK;
        for (uint64_t i = 3; i < 7; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(t[i - 3]);
            res[3].wentries.push_back(ent);
        }
    }

    {
        uint64_t t[] = {3, 3, 5};
        for (uint64_t i = 2; i < 5; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(t[i - 2]);
            res[4].entries.push_back(ent);
        }
        res[4].code = ErrCode::OK;
        uint64_t wt[] = {3, 5};
        for (uint64_t i = 3; i < 5; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(wt[i - 3]);
            res[4].wentries.push_back(ent);
        }
    }

    {
        auto ent = snail::raft::make_entry();
        ent->set_index(4);
        ent->set_term(5);
        res[5].entries.push_back(ent);
        res[5].code = ErrCode::OK;
        uint64_t t[] = {3, 5};
        for (uint64_t i = 3; i < 5; i++) {
            ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(t[i - 3]);
            res[5].wentries.push_back(ent);
        }
    }

    {
        auto ent = snail::raft::make_entry();
        ent->set_index(6);
        ent->set_term(5);
        res[6].entries.push_back(ent);
        res[6].code = ErrCode::OK;
        uint64_t t[] = {3, 4, 5, 5};
        for (uint64_t i = 3; i < 7; i++) {
            ent = snail::raft::make_entry();
            ent->set_index(i);
            ent->set_term(t[i - 3]);
            res[6].wentries.push_back(ent);
        }
    }

    for (int i = 0; i < 7; i++) {
        auto s = std::make_unique<snail::raft::MemoryStorage>();
        s->ents_ = ents;
        auto st = s->Append(res[i].entries);
        BOOST_REQUIRE(st.Code() == res[i].code);
        BOOST_REQUIRE_EQUAL(s->ents_.size(), res[i].wentries.size());
        for (int j = 0; j < res[i].wentries.size(); j++) {
            BOOST_REQUIRE((*(s->ents_[j]) == *(res[i].wentries[j])));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(StorageApplySnapshotTest) {
    snail::raft::ConfState cs;
    seastar::temporary_buffer<char> data("data", 4);
    std::vector<uint64_t> voters = {1, 2, 3};
    cs.set_voters(std::move(voters));

    std::vector<snail::raft::SnapshotPtr> tests;
    uint64_t v[] = {4, 3};
    for (int i = 0; i < 2; i++) {
        snail::raft::SnapshotMetadata m;
        auto snap = snail::raft::make_snapshot();
        tests.push_back(snap);
        snap->set_data(data.share());
        m.set_index(v[i]);
        m.set_term(v[i]);
        m.set_conf_state(cs);
        snap->set_metadata(std::move(m));
    }

    auto s = std::make_unique<snail::raft::MemoryStorage>();
    auto st = s->ApplySnapshot(tests[0]);
    BOOST_REQUIRE(st);
    st = s->ApplySnapshot(tests[1]);
    BOOST_REQUIRE(st.Code() == ErrCode::ErrRaftSnapOutOfData);
}

}  // namespace raft
}  // namespace snail
