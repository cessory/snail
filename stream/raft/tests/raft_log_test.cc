#include "raft/raft_log.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "raft_test.h"

namespace snail {
namespace raft {

SEASTAR_THREAD_TEST_CASE(FindConflictTest) {
    snail::raft::EntryPtr ent;
    std::vector<snail::raft::EntryPtr> previousEnts;
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        previousEnts.push_back(ent);
    }
    struct testItem {
        std::vector<snail::raft::EntryPtr> ents;
        uint64_t wconflict;
    };

    testItem tests[11] = {{{}, 0}, {{}, 0}, {{}, 0}, {{}, 0}, {{}, 4}, {{}, 4},
                          {{}, 4}, {{}, 4}, {{}, 1}, {{}, 2}, {{}, 3}};
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        tests[1].ents.push_back(ent);
    }

    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 2);
        ent->set_index(i + 2);
        tests[2].ents.push_back(ent);
    }

    ent = snail::raft::make_entry();
    ent->set_term(3);
    ent->set_index(3);
    tests[3].ents.push_back(ent);

    for (int i = 0; i < 5; i++) {
        ent = snail::raft::make_entry();
        if (i == 4) {
            ent->set_term(i);
        } else {
            ent->set_term(i + 1);
        }
        ent->set_index(i + 1);
        tests[4].ents.push_back(ent);
    }

    for (int i = 0; i < 4; i++) {
        ent = snail::raft::make_entry();
        if (i == 3) {
            ent->set_term(i + 1);
        } else {
            ent->set_term(i + 2);
        }
        ent->set_index(i + 2);
        tests[5].ents.push_back(ent);
    }

    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        if (i == 2) {
            ent->set_term(i + 2);
        } else {
            ent->set_term(i + 3);
        }
        ent->set_index(i + 3);
        tests[6].ents.push_back(ent);
    }

    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(4);
        ent->set_index(i + 4);
        tests[7].ents.push_back(ent);
    }

    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(4);
        ent->set_index(i + 1);
        tests[8].ents.push_back(ent);
    }

    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        if (i == 0) {
            ent->set_term(1);
        } else {
            ent->set_term(4);
        }
        ent->set_index(i + 2);
        tests[9].ents.push_back(ent);
    }

    for (int i = 0; i < 4; i++) {
        ent = snail::raft::make_entry();
        if (i < 2) {
            ent->set_term(i + 1);
        } else {
            ent->set_term(4);
        }
        ent->set_index(i + 3);
        tests[10].ents.push_back(ent);
    }

    for (int i = 0; i < 11; i++) {
        auto store = newTestMemoryStorage();
        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        raft_log->Append(previousEnts);
        auto gconflict =
            raft_log->FindConflict(tests[i].ents.data(), tests[i].ents.size())
                .get0();
        BOOST_REQUIRE_EQUAL(gconflict, tests[i].wconflict);
    }
}

SEASTAR_THREAD_TEST_CASE(IsUpToDateTest) {
    snail::raft::EntryPtr ent;
    std::vector<snail::raft::EntryPtr> previousEnts;
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        previousEnts.push_back(ent);
    }
    auto store = newTestMemoryStorage();
    auto raft_log = snail::raft::RaftLog::MakeRaftLog(
        store, std::numeric_limits<uint64_t>::max());
    raft_log->Append(previousEnts);

    struct testItem {
        uint64_t last_index;
        uint64_t term;
        bool wUpToDate;
    };
    testItem tests[9] = {{raft_log->LastIndex() - 1, 4, true},
                         {raft_log->LastIndex(), 4, true},
                         {raft_log->LastIndex() + 1, 4, true},

                         {raft_log->LastIndex() - 1, 2, false},
                         {raft_log->LastIndex(), 2, false},
                         {raft_log->LastIndex() + 1, 2, false},

                         {raft_log->LastIndex() - 1, 3, false},
                         {raft_log->LastIndex(), 3, true},
                         {raft_log->LastIndex() + 1, 3, true}};
    for (int i = 0; i < 9; i++) {
        auto gUpToDate =
            raft_log->IsUpToDate(tests[i].last_index, tests[i].term).get0();
        BOOST_REQUIRE_EQUAL(gUpToDate, tests[i].wUpToDate);
    }
}

SEASTAR_THREAD_TEST_CASE(AppendTest) {
    snail::raft::EntryPtr ent;
    std::vector<snail::raft::EntryPtr> previousEnts;
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        previousEnts.push_back(ent);
    }

    struct testItem {
        std::vector<snail::raft::EntryPtr> ents;
        uint64_t windex;
        std::vector<snail::raft::EntryPtr> wents;
        uint64_t wunstable;
    };
    testItem tests[4];
    tests[0].windex = 2;
    tests[0].wunstable = 3;
    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term(i + 1);
        tests[0].wents.push_back(ent);
    }

    ent = snail::raft::make_entry();
    ent->set_index(3);
    ent->set_term(2);
    tests[1].ents.push_back(ent);
    tests[1].windex = 3;
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term((i + 1 > 2 ? 2 : i + 1));
        tests[1].wents.push_back(ent);
    }
    tests[1].wunstable = 3;

    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(2);
    tests[2].ents.push_back(ent);
    tests[2].windex = 1;
    ent = snail::raft::make_entry();
    ent->set_index(1);
    ent->set_term(2);
    tests[2].wents.push_back(ent);
    tests[2].wunstable = 1;

    for (int i = 0; i < 2; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i + 2);
        ent->set_term(3);
        tests[3].ents.push_back(ent);
    }
    tests[3].windex = 3;
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term(i + 1 >= 2 ? 3 : i + 1);
        tests[3].wents.push_back(ent);
    }
    tests[3].wunstable = 2;

    for (int i = 0; i < 4; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->Append(previousEnts);
        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        auto index = raft_log->Append(tests[i].ents);

        BOOST_REQUIRE_EQUAL(index, tests[i].windex);
        auto s =
            raft_log->Entries(1, std::numeric_limits<uint64_t>::max()).get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(s.Value().size(), tests[i].wents.size());
        for (int j = 0; j < s.Value().size(); j++) {
            BOOST_REQUIRE(*(s.Value()[j]) == *(tests[i].wents[j]));
        }
        BOOST_REQUIRE_EQUAL(raft_log->unstable_.offset(), tests[i].wunstable);
    }
}

SEASTAR_THREAD_TEST_CASE(LogMaybeAppendTest) {
    snail::raft::EntryPtr ent;
    std::vector<snail::raft::EntryPtr> previousEnts;
    for (int i = 0; i < 3; i++) {
        ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        previousEnts.push_back(ent);
    }
    uint64_t lastindex = 3;
    uint64_t lastterm = 3;
    uint64_t commit = 1;

    struct testItem {
        uint64_t logTerm;
        uint64_t index;
        uint64_t committed;
        std::vector<snail::raft::EntryPtr> ents;
        uint64_t wlasti;
        bool wappend;
        uint64_t wcommit;
        bool wpanic;
    };

    testItem tests[15] = {
        {lastterm - 1, lastindex, lastindex, {}, 0, false, commit, false},
        {lastterm, lastindex + 1, lastindex, {}, 0, false, commit, false},
        {lastterm, lastindex, lastindex, {}, lastindex, true, lastindex, false},
        {lastterm,
         lastindex,
         lastindex + 1,
         {},
         lastindex,
         true,
         lastindex,
         false},
        {lastterm,
         lastindex,
         lastindex - 1,
         {},
         lastindex,
         true,
         lastindex - 1,
         false},
        {lastterm, lastindex, 0, {}, lastindex, true, commit, false},
        {0, 0, lastindex, {}, 0, true, commit, false},
        {lastterm,
         lastindex,
         lastindex,
         {},
         lastindex + 1,
         true,
         lastindex,
         false},
        {lastterm,
         lastindex,
         lastindex + 1,
         {},
         lastindex + 1,
         true,
         lastindex + 1,
         false},
        {lastterm,
         lastindex,
         lastindex + 2,
         {},
         lastindex + 1,
         true,
         lastindex + 1,
         false},
        {lastterm,
         lastindex,
         lastindex + 2,
         {},
         lastindex + 2,
         true,
         lastindex + 2,
         false},
        {lastterm - 1,
         lastindex - 1,
         lastindex,
         {},
         lastindex,
         true,
         lastindex,
         false},
        {lastterm - 2,
         lastindex - 2,
         lastindex,
         {},
         lastindex - 1,
         true,
         lastindex - 1,
         false},
        {lastterm - 3,
         lastindex - 3,
         lastindex,
         {},
         lastindex - 2,
         true,
         lastindex - 2,
         true},
        {lastterm - 2,
         lastindex - 2,
         lastindex,
         {},
         lastindex,
         true,
         lastindex,
         false}};

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex + 1);
    tests[0].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex + 2);
    tests[1].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex + 1);
    tests[7].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex + 1);
    tests[8].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex + 1);
    tests[9].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex + 1);
    tests[10].ents.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex + 2);
    tests[10].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex);
    tests[11].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex - 1);
    tests[12].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex - 2);
    tests[13].ents.push_back(ent);

    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex - 1);
    tests[14].ents.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_term(4);
    ent->set_index(lastindex);
    tests[14].ents.push_back(ent);

    for (int i = 0; i < 15; i++) {
        auto store = newTestMemoryStorage();
        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        raft_log->Append(previousEnts);
        raft_log->set_committed(commit);

        try {
            auto res = raft_log
                           ->MaybeAppend(tests[i].index, tests[i].logTerm,
                                         tests[i].committed, tests[i].ents)
                           .get0();
            uint64_t glasti;
            bool gappend;
            std::tie(glasti, gappend) = res;
            BOOST_REQUIRE_EQUAL(glasti, tests[i].wlasti);
            BOOST_REQUIRE_EQUAL(gappend, tests[i].wappend);
            BOOST_REQUIRE_EQUAL(raft_log->committed(), tests[i].wcommit);
            if (gappend && !tests[i].ents.empty()) {
                auto s = raft_log
                             ->Slice(raft_log->LastIndex() -
                                         tests[i].ents.size() + 1,
                                     raft_log->LastIndex() + 1,
                                     std::numeric_limits<uint64_t>::max())
                             .get0();
                BOOST_REQUIRE(s);
                BOOST_REQUIRE_EQUAL(s.Value().size(), tests[i].ents.size());
                for (int j = 0; j < s.Value().size(); j++) {
                    BOOST_REQUIRE(*(s.Value()[j]) == *tests[i].ents[j]);
                }
            }
        } catch (...) {
            BOOST_REQUIRE(tests[i].wpanic);
            return;
        }
    }
}

SEASTAR_THREAD_TEST_CASE(CompactionSideEffectsTest) {
    uint64_t lastIndex = 1000;
    uint64_t unstableIndex = 750;
    uint64_t lastTerm = lastIndex;

    auto store = newTestMemoryStorage();
    seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
        seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);

    std::vector<snail::raft::EntryPtr> ents;
    for (uint64_t i = 0; i < unstableIndex; i++) {
        snail::raft::EntryPtr ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term(i + 1);
        ents.push_back(ent);
    }
    ptr->Append(ents);
    auto raft_log = snail::raft::RaftLog::MakeRaftLog(
        store, std::numeric_limits<uint64_t>::max());
    ents.clear();
    for (uint64_t i = unstableIndex; i < lastIndex; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(i + 1);
        ent->set_term(i + 1);
        ents.push_back(ent);
    }
    raft_log->Append(ents);
    auto ok = raft_log->MaybeCommit(lastIndex, lastTerm).get0();
    BOOST_REQUIRE(ok);
    raft_log->AppliedTo(raft_log->committed());
    uint64_t offset = 500;
    ptr->Compact(offset);

    BOOST_REQUIRE_EQUAL(raft_log->LastIndex(), lastIndex);
    for (uint64_t i = offset; i <= raft_log->LastIndex(); i++) {
        auto s = raft_log->Term(i).get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(s.Value(), i);
    }

    for (uint64_t i = offset; i <= raft_log->LastIndex(); i++) {
        auto ok = raft_log->MatchTerm(i, i).get0();
        BOOST_REQUIRE(ok);
        auto unstableEnts = raft_log->UnstableEntries();
        BOOST_REQUIRE_EQUAL(unstableEnts.size(), 250);
        BOOST_REQUIRE_EQUAL(unstableEnts[0]->index(), 751);
    }

    auto prev = raft_log->LastIndex();
    ents.clear();
    auto ent = snail::raft::make_entry();
    ent->set_index(raft_log->LastIndex() + 1);
    ent->set_term(raft_log->LastIndex() + 1);
    ents.push_back(ent);
    raft_log->Append(ents);

    BOOST_REQUIRE_EQUAL(raft_log->LastIndex(), prev + 1);
    auto s = raft_log
                 ->Entries(raft_log->LastIndex(),
                           std::numeric_limits<uint64_t>::max())
                 .get0();
    BOOST_REQUIRE(s);
    BOOST_REQUIRE_EQUAL(s.Value().size(), 1);
}

SEASTAR_THREAD_TEST_CASE(HasNextEntsTest) {
    auto snap = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_term(1);
    meta.set_index(3);
    snap->set_metadata(std::move(meta));

    std::vector<snail::raft::EntryPtr> ents;
    for (int i = 0; i < 3; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_term(1);
        ent->set_index(i + 4);
        ents.push_back(ent);
    }

    struct testItem {
        uint64_t applied;
        bool hasNext;
    };

    testItem tests[4] = {{0, true}, {3, true}, {4, true}, {5, false}};
    for (int i = 0; i < 4; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->ApplySnapshot(snap);

        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        raft_log->Append(ents);
        raft_log->MaybeCommit(5, 1).get();
        raft_log->AppliedTo(tests[i].applied);
        BOOST_REQUIRE_EQUAL(raft_log->HasNextEnts(), tests[i].hasNext);
    }
}

SEASTAR_THREAD_TEST_CASE(NextEntsTest) {
    auto snap = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_term(1);
    meta.set_index(3);
    snap->set_metadata(std::move(meta));

    std::vector<snail::raft::EntryPtr> ents;
    for (int i = 0; i < 3; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_term(1);
        ent->set_index(i + 4);
        ents.push_back(ent);
    }

    struct testItem {
        uint64_t applied;
        std::vector<snail::raft::EntryPtr> wents;
    };

    testItem tests[4] = {{0, {ents[0], ents[1]}},
                         {3, {ents[0], ents[1]}},
                         {4, {ents[1]}},
                         {5, {}}};
    for (int i = 0; i < 4; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        ptr->ApplySnapshot(snap);

        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        raft_log->Append(ents);
        raft_log->MaybeCommit(5, 1).get();
        raft_log->AppliedTo(tests[i].applied);
        auto nents = raft_log->NextEnts().get0();
        BOOST_REQUIRE_EQUAL(nents.size(), tests[i].wents.size());
        for (int j = 0; j < nents.size(); j++) {
            BOOST_REQUIRE(*nents[j] == *tests[i].wents[j]);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(UnstableEntsTest) {
    std::vector<snail::raft::EntryPtr> previousEnts;
    for (int i = 0; i < 2; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        previousEnts.push_back(ent);
    }
    struct testItem {
        uint64_t unstable;
        std::vector<snail::raft::EntryPtr> wents;
    };

    testItem tests[2] = {{3, {}}, {1, previousEnts}};
    for (int i = 0; i < 2; i++) {
        auto store = newTestMemoryStorage();
        seastar::shared_ptr<snail::raft::MemoryStorage> ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<snail::raft::EntryPtr> vec;
        for (int j = 0; j < tests[i].unstable - 1; j++) {
            vec.push_back(previousEnts[j]);
        }
        ptr->Append(vec);

        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        vec.clear();
        for (int j = tests[i].unstable - 1; j < previousEnts.size(); j++) {
            vec.push_back(previousEnts[j]);
        }
        raft_log->Append(vec);

        auto ents = raft_log->UnstableEntries();
        if (ents.size() > 0) {
            raft_log->StableTo(ents.back()->index(), ents.back()->term());
        }
        BOOST_REQUIRE_EQUAL(ents.size(), tests[i].wents.size());
        for (int j = 0; j < ents.size(); j++) {
            BOOST_REQUIRE(*ents[j] == *tests[i].wents[j]);
        }
        BOOST_REQUIRE_EQUAL(raft_log->unstable_.offset(),
                            previousEnts.back()->index() + 1);
    }
}

SEASTAR_THREAD_TEST_CASE(CommitToTest) {
    std::vector<snail::raft::EntryPtr> previousEnts;
    for (int i = 0; i < 3; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_term(i + 1);
        ent->set_index(i + 1);
        previousEnts.push_back(ent);
    }
    uint64_t commit = 2;
    struct testItem {
        uint64_t commit;
        uint64_t wcommit;
        bool wpanic;
    };

    testItem tests[3] = {{3, 3, false}, {1, 2, false}, {4, 0, true}};
    for (int i = 0; i < 3; i++) {
        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            newTestMemoryStorage(), std::numeric_limits<uint64_t>::max());
        try {
            raft_log->Append(previousEnts);
            raft_log->set_committed(commit);
            raft_log->CommitTo(tests[i].commit);
            BOOST_REQUIRE_EQUAL(raft_log->committed(), tests[i].wcommit);
        } catch (...) {
            BOOST_REQUIRE(tests[i].wpanic);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(StableToTest) {
    struct testItem {
        uint64_t stablei;
        uint64_t stablet;
        uint64_t wunstable;
    };

    testItem tests[4] = {{1, 1, 2}, {2, 2, 3}, {2, 1, 1}, {3, 1, 1}};
    for (int i = 0; i < 4; i++) {
        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            newTestMemoryStorage(), std::numeric_limits<uint64_t>::max());
        std::vector<snail::raft::EntryPtr> ents;
        for (int i = 0; i < 2; i++) {
            auto ent = snail::raft::make_entry();
            ent->set_term(i + 1);
            ent->set_index(i + 1);
            ents.push_back(ent);
        }
        raft_log->Append(ents);
        raft_log->StableTo(tests[i].stablei, tests[i].stablet);
        BOOST_REQUIRE_EQUAL(raft_log->unstable_.offset(), tests[i].wunstable);
    }
}

SEASTAR_THREAD_TEST_CASE(StableToWithSnapTest) {
    uint64_t snapi = 5;
    uint64_t snapt = 2;

    struct testItem {
        uint64_t stablei;
        uint64_t stablet;
        std::vector<snail::raft::EntryPtr> newEnts;
        uint64_t wunstable;
    };

    testItem tests[12] = {{snapi + 1, snapt, {}, snapi + 1},
                          {snapi, snapt, {}, snapi + 1},
                          {snapi - 1, snapt, {}, snapi + 1},

                          {snapi + 1, snapt + 1, {}, snapi + 1},
                          {snapi, snapt + 1, {}, snapi + 1},
                          {snapi - 1, snapt + 1, {}, snapi + 1},

                          {snapi + 1, snapt, {}, snapi + 2},
                          {snapi, snapt, {}, snapi + 1},
                          {snapi - 1, snapt, {}, snapi + 1},

                          {snapi + 1, snapt + 1, {}, snapi + 1},
                          {snapi, snapt + 1, {}, snapi + 1},
                          {snapi - 1, snapt + 1, {}, snapi + 1}};
    for (int i = 6; i < 12; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(snapi + 1);
        ent->set_term(snapt);
        tests[i].newEnts.push_back(ent);
    }

    for (int i = 0; i < 12; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        auto snap = snail::raft::make_snapshot();
        snail::raft::SnapshotMetadata meta;
        meta.set_index(snapi);
        meta.set_term(snapt);
        snap->set_metadata(std::move(meta));
        ptr->ApplySnapshot(snap);

        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        raft_log->Append(tests[i].newEnts);
        raft_log->StableTo(tests[i].stablei, tests[i].stablet);
        BOOST_REQUIRE_EQUAL(raft_log->unstable_.offset(), tests[i].wunstable);
    }
}

SEASTAR_THREAD_TEST_CASE(CompactionTest) {
    struct testItem {
        uint64_t lastIndex;
        std::vector<uint64_t> compact;
        std::vector<int> wleft;
        bool wallow;
    };

    testItem tests[3] = {
        {1000, {1001}, {-1}, false},
        {1000, {300, 500, 800, 900}, {700, 500, 200, 100}, true},
        {1000, {300, 299}, {700, -1}, false}};
    for (int i = 0; i < 3; i++) {
        auto store = newTestMemoryStorage();
        auto ptr =
            seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
        std::vector<snail::raft::EntryPtr> ents;
        for (uint64_t j = 0; j < tests[i].lastIndex; j++) {
            auto ent = snail::raft::make_entry();
            ent->set_index(j + 1);
            ents.push_back(ent);
        }
        ptr->Append(ents);

        auto raft_log = snail::raft::RaftLog::MakeRaftLog(
            store, std::numeric_limits<uint64_t>::max());
        raft_log->MaybeCommit(tests[i].lastIndex, 0).get();
        raft_log->AppliedTo(raft_log->committed());
        for (uint64_t j = 0; j < tests[i].compact.size(); j++) {
            try {
                auto s = ptr->Compact(tests[i].compact[j]);
                if (!s) {
                    BOOST_REQUIRE(!tests[i].wallow);
                    continue;
                }

                auto ents = raft_log->AllEntries().get0();
                BOOST_REQUIRE_EQUAL(ents.size(), tests[i].wleft[j]);
            } catch (...) {
                BOOST_REQUIRE(!tests[i].wallow);
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(LogRestoreTest) {
    uint64_t index = 1000;
    uint64_t term = 1000;
    auto snap = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_index(index);
    meta.set_term(term);
    snap->set_metadata(std::move(meta));

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->ApplySnapshot(snap);

    auto raft_log = snail::raft::RaftLog::MakeRaftLog(
        store, std::numeric_limits<uint64_t>::max());
    auto ents = raft_log->AllEntries().get0();
    BOOST_REQUIRE_EQUAL(ents.size(), 0);
    BOOST_REQUIRE_EQUAL(raft_log->FirstIndex(), index + 1);
    BOOST_REQUIRE_EQUAL(raft_log->committed(), index);
    BOOST_REQUIRE_EQUAL(raft_log->unstable_.offset(), index + 1);
    auto s = raft_log->Term(index).get0();
    BOOST_REQUIRE(s);
    BOOST_REQUIRE_EQUAL(s.Value(), term);
}

SEASTAR_THREAD_TEST_CASE(TermTest) {
    uint64_t offset = 100;
    uint64_t num = 100;
    auto snap = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_index(offset);
    meta.set_term(1);
    snap->set_metadata(std::move(meta));

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->ApplySnapshot(snap);

    auto raft_log = snail::raft::RaftLog::MakeRaftLog(
        store, std::numeric_limits<uint64_t>::max());

    std::vector<snail::raft::EntryPtr> ents;
    for (int i = 1; i < num; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(offset + i);
        ent->set_term(i);
        ents.push_back(ent);
    }
    raft_log->Append(ents);

    struct testItem {
        uint64_t index;
        uint64_t w;
    };
    testItem tests[5] = {{offset - 1, 0},
                         {offset, 1},
                         {offset + num / 2, num / 2},
                         {offset + num - 1, num - 1},
                         {offset + num, 0}};

    for (int i = 0; i < 5; i++) {
        auto s = raft_log->Term(tests[i].index).get0();
        BOOST_REQUIRE_EQUAL(s.Value(), tests[i].w);
    }
}

SEASTAR_THREAD_TEST_CASE(TermWithUnstableSnapshotTest) {
    uint64_t storagesnapi = 100;
    uint64_t unstablesnapi = storagesnapi + 5;
    auto snap = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_index(storagesnapi);
    meta.set_term(1);
    snap->set_metadata(std::move(meta));

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->ApplySnapshot(snap);

    auto raft_log = snail::raft::RaftLog::MakeRaftLog(
        store, std::numeric_limits<uint64_t>::max());
    snap = snail::raft::make_snapshot();
    meta.set_index(unstablesnapi);
    meta.set_term(1);
    snap->set_metadata(std::move(meta));

    raft_log->Restore(snap);

    struct testItem {
        uint64_t index;
        uint64_t w;
    };
    testItem tests[4] = {{storagesnapi, 0},
                         {storagesnapi + 1, 0},
                         {unstablesnapi - 1, 0},
                         {unstablesnapi, 1}};
    for (int i = 0; i < 4; i++) {
        auto s = raft_log->Term(tests[i].index).get0();
        BOOST_REQUIRE_EQUAL(s.Value(), tests[i].w);
    }
}

SEASTAR_THREAD_TEST_CASE(SliceTest) {
    uint64_t offset = 100;
    uint64_t num = 100;
    uint64_t last = offset + num;
    uint64_t half = offset + num / 2;
    auto halfe = snail::raft::make_entry();
    halfe->set_index(half);
    halfe->set_term(half);

    auto snap = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_index(offset);
    snap->set_metadata(std::move(meta));

    auto store = newTestMemoryStorage();
    auto ptr = seastar::dynamic_pointer_cast<snail::raft::MemoryStorage>(store);
    ptr->ApplySnapshot(snap);

    std::vector<snail::raft::EntryPtr> ents;
    for (int i = 1; i < num / 2; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(offset + i);
        ent->set_term(offset + i);
        ents.push_back(ent);
    }
    ptr->Append(ents);

    auto raft_log = snail::raft::RaftLog::MakeRaftLog(
        store, std::numeric_limits<uint64_t>::max());
    ents.clear();
    for (int i = num / 2; i < num; i++) {
        auto ent = snail::raft::make_entry();
        ent->set_index(offset + i);
        ent->set_term(offset + i);
        ents.push_back(ent);
    }
    raft_log->Append(ents);

    struct testItem {
        uint64_t from;
        uint64_t to;
        uint64_t limit;
        std::vector<snail::raft::EntryPtr> w;
        bool wpanic;
    };

    ents.clear();
    for (int i = 0; i < 15; i++) {
        auto ent = snail::raft::make_entry();
        ents.push_back(ent);
    }
    ents[0]->set_index(half - 1);
    ents[0]->set_term(half - 1);
    ents[1]->set_index(half);
    ents[1]->set_term(half);
    ents[2]->set_index(half);
    ents[2]->set_term(half);
    ents[3]->set_index(last - 1);
    ents[3]->set_term(last - 1);
    ents[4]->set_index(half - 1);
    ents[4]->set_term(half - 1);
    ents[5]->set_index(half - 1);
    ents[5]->set_term(half - 1);
    ents[6]->set_index(half - 2);
    ents[6]->set_term(half - 2);
    ents[7]->set_index(half - 1);
    ents[7]->set_term(half - 1);
    ents[8]->set_index(half);
    ents[8]->set_term(half);
    ents[9]->set_index(half - 1);
    ents[9]->set_term(half - 1);
    ents[10]->set_index(half);
    ents[10]->set_term(half);
    ents[11]->set_index(half + 1);
    ents[11]->set_term(half + 1);
    ents[12]->set_index(half);
    ents[12]->set_term(half);
    ents[13]->set_index(half);
    ents[13]->set_term(half);
    ents[14]->set_index(half + 1);
    ents[14]->set_term(half + 1);

    testItem tests[13] = {
        {offset - 1,
         offset + 1,
         std::numeric_limits<uint64_t>::max(),
         {},
         false},
        {offset, offset + 1, std::numeric_limits<uint64_t>::max(), {}, false},
        {half - 1,
         half + 1,
         std::numeric_limits<uint64_t>::max(),
         {ents[0], ents[1]},
         false},
        {half,
         half + 1,
         std::numeric_limits<uint64_t>::max(),
         {ents[2]},
         false},
        {last - 1,
         last,
         std::numeric_limits<uint64_t>::max(),
         {ents[3]},
         false},
        {last, last + 1, std::numeric_limits<uint64_t>::max(), {}, true},
        {half - 1, half + 1, 0, {ents[4]}, false},
        {half - 1, half + 1, halfe->ByteSize() + 1, {ents[5]}, false},
        {half - 2, half + 1, halfe->ByteSize() + 1, {ents[6]}, false},
        {half - 1, half + 1, halfe->ByteSize() * 2, {ents[7], ents[8]}, false},
        {half - 1,
         half + 2,
         halfe->ByteSize() * 3,
         {ents[9], ents[10], ents[11]},
         false},
        {half, half + 2, halfe->ByteSize(), {ents[12]}, false},
        {half, half + 2, halfe->ByteSize() * 2, {ents[13], ents[14]}, false}};

    for (int i = 0; i < 13; i++) {
        try {
            auto s = raft_log->Slice(tests[i].from, tests[i].to, tests[i].limit)
                         .get0();
            BOOST_REQUIRE(!(tests[i].from <= offset &&
                            s.Code() != ErrCode::ErrRaftCompacted));
            BOOST_REQUIRE(!(tests[i].from > offset && !s));
            BOOST_REQUIRE_EQUAL(s.Value().size(), tests[i].w.size());
            for (int j = 0; j < s.Value().size(); j++) {
                BOOST_REQUIRE(*(s.Value()[j]) == *tests[i].w[j]);
            }
        } catch (...) {
            BOOST_REQUIRE(tests[i].wpanic);
        }
    }
}

}  // namespace raft
}  // namespace snail
