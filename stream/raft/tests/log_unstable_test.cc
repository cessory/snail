#include "raft/log_unstable.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

namespace snail {
namespace raft {

SEASTAR_THREAD_TEST_CASE(UnstableMaybeFirstIndexTest) {
    struct testItem {
        std::deque<snail::raft::EntryPtr> entries;
        snail::raft::SnapshotPtr snap;
        uint64_t offset;
        bool wok;
        uint64_t windex;
    };

    struct testItem tests[4];
    snail::raft::EntryPtr ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[0].entries.push_back(ent);
    tests[0].offset = 5;
    tests[0].wok = false;
    tests[0].windex = 0;

    tests[1].offset = 0;
    tests[1].wok = false;
    tests[1].windex = 0;

    snail::raft::SnapshotMetadata meta;
    meta.set_index(4);
    meta.set_term(1);
    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[2].entries.push_back(ent);
    tests[2].snap = snail::raft::make_snapshot();
    tests[2].snap->set_metadata(std::move(meta));
    tests[2].offset = 5;
    tests[2].wok = true;
    tests[2].windex = 5;

    meta.set_index(4);
    meta.set_term(1);
    tests[3].snap = snail::raft::make_snapshot();
    tests[3].snap->set_metadata(std::move(meta));
    tests[3].offset = 5;
    tests[3].wok = true;
    tests[3].windex = 5;

    for (int i = 0; i < 4; i++) {
        snail::raft::Unstable u;
        u.entries_ = std::move(tests[i].entries);
        u.snapshot_ = tests[i].snap;
        u.offset_ = tests[i].offset;

        uint64_t index;
        bool ok;
        auto r = u.MaybeFirstIndex();
        std::tie(index, ok) = r;
        BOOST_REQUIRE_EQUAL(index, tests[i].windex);
        BOOST_REQUIRE_EQUAL(ok, tests[i].wok);
    }
}

SEASTAR_THREAD_TEST_CASE(UnstableMaybeLastIndexTest) {
    struct testItem {
        std::deque<snail::raft::EntryPtr> entries;
        snail::raft::SnapshotPtr snap;
        uint64_t offset;
        bool wok;
        uint64_t windex;
    };

    struct testItem tests[4];
    snail::raft::EntryPtr ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[0].entries.push_back(ent);
    tests[0].offset = 5;
    tests[0].wok = true;
    tests[0].windex = 5;

    snail::raft::SnapshotMetadata meta;
    meta.set_index(4);
    meta.set_term(1);
    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[1].entries.push_back(ent);
    tests[1].snap = snail::raft::make_snapshot();
    tests[1].snap->set_metadata(std::move(meta));
    tests[1].offset = 5;
    tests[1].wok = true;
    tests[1].windex = 5;

    meta.set_index(4);
    meta.set_term(1);
    tests[2].snap = snail::raft::make_snapshot();
    tests[2].snap->set_metadata(std::move(meta));
    tests[2].offset = 5;
    tests[2].wok = true;
    tests[2].windex = 4;

    tests[3].offset = 0;
    tests[3].wok = false;
    tests[3].windex = 0;

    for (int i = 0; i < 4; i++) {
        snail::raft::Unstable u;
        u.entries_ = std::move(tests[i].entries);
        u.snapshot_ = tests[i].snap;
        u.offset_ = tests[i].offset;

        uint64_t index;
        bool ok;
        auto r = u.MaybeLastIndex();
        std::tie(index, ok) = r;
        BOOST_REQUIRE_EQUAL(index, tests[i].windex);
        BOOST_REQUIRE_EQUAL(ok, tests[i].wok);
    }
}

SEASTAR_THREAD_TEST_CASE(UnstableMaybeTermTest) {
    struct testItem {
        std::deque<snail::raft::EntryPtr> entries;
        snail::raft::SnapshotPtr snap;
        uint64_t offset;
        uint64_t index;

        bool wok;
        uint64_t wterm;
    };

    testItem tests[10];

    snail::raft::EntryPtr ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[0].entries.push_back(ent);
    tests[0].offset = 5;
    tests[0].index = 5;
    tests[0].wok = true;
    tests[0].wterm = 1;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[1].entries.push_back(ent);
    tests[1].offset = 5;
    tests[1].index = 6;
    tests[1].wok = false;
    tests[1].wterm = 0;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[2].entries.push_back(ent);
    tests[2].offset = 5;
    tests[2].index = 4;
    tests[2].wok = false;
    tests[2].wterm = 0;

    snail::raft::SnapshotMetadata meta;

    meta.set_index(4);
    meta.set_term(1);
    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[3].entries.push_back(ent);
    tests[3].snap = snail::raft::make_snapshot();
    tests[3].snap->set_metadata(std::move(meta));
    tests[3].offset = 5;
    tests[3].index = 5;
    tests[3].wok = true;
    tests[3].wterm = 1;

    meta.set_index(4);
    meta.set_term(1);
    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[4].entries.push_back(ent);
    tests[4].snap = snail::raft::make_snapshot();
    tests[4].snap->set_metadata(std::move(meta));
    tests[4].offset = 5;
    tests[4].index = 6;
    tests[4].wok = false;
    tests[4].wterm = 0;

    meta.set_index(4);
    meta.set_term(1);
    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[5].entries.push_back(ent);
    tests[5].snap = snail::raft::make_snapshot();
    tests[5].snap->set_metadata(std::move(meta));
    tests[5].offset = 5;
    tests[5].index = 4;
    tests[5].wok = true;
    tests[5].wterm = 1;

    meta.set_index(4);
    meta.set_term(1);
    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[6].entries.push_back(ent);
    tests[6].snap = snail::raft::make_snapshot();
    tests[6].snap->set_metadata(std::move(meta));
    tests[6].offset = 5;
    tests[6].index = 3;
    tests[6].wok = false;
    tests[6].wterm = 0;

    meta.set_index(4);
    meta.set_term(1);
    tests[7].snap = snail::raft::make_snapshot();
    tests[7].snap->set_metadata(std::move(meta));
    tests[7].offset = 5;
    tests[7].index = 5;
    tests[7].wok = false;
    tests[7].wterm = 0;

    meta.set_index(4);
    meta.set_term(1);
    tests[8].snap = snail::raft::make_snapshot();
    tests[8].snap->set_metadata(std::move(meta));
    tests[8].offset = 5;
    tests[8].index = 4;
    tests[8].wok = true;
    tests[8].wterm = 1;

    tests[9].offset = 0;
    tests[9].index = 5;
    tests[9].wok = false;
    tests[9].wterm = 0;

    for (int i = 0; i < 10; i++) {
        snail::raft::Unstable u;
        u.entries_ = std::move(tests[i].entries);
        u.snapshot_ = tests[i].snap;
        u.offset_ = tests[i].offset;

        uint64_t term;
        bool ok;
        auto r = u.MaybeTerm(tests[i].index);
        std::tie(term, ok) = r;
        BOOST_REQUIRE_EQUAL(term, tests[i].wterm);
        BOOST_REQUIRE_EQUAL(ok, tests[i].wok);
    }
}

SEASTAR_THREAD_TEST_CASE(UnstableRestoreTest) {
    snail::raft::Unstable u;
    snail::raft::EntryPtr ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    u.entries_.push_back(ent);
    u.offset_ = 5;
    u.snapshot_ = snail::raft::make_snapshot();
    snail::raft::SnapshotMetadata meta;
    meta.set_index(4);
    meta.set_term(1);
    u.snapshot_->set_metadata(std::move(meta));

    snail::raft::SnapshotPtr s = snail::raft::make_snapshot();
    meta.set_index(6);
    meta.set_term(2);
    s->set_metadata(std::move(meta));

    u.Restore(s);
    BOOST_REQUIRE_EQUAL(u.offset_, s->metadata().index() + 1);
    BOOST_REQUIRE(u.entries_.empty());
    bool ok = u.snapshot_ == s;
    BOOST_REQUIRE(ok);
}

SEASTAR_THREAD_TEST_CASE(UnstableStableToTest) {
    struct testItem {
        std::deque<snail::raft::EntryPtr> entries;
        uint64_t offset;
        snail::raft::SnapshotPtr snap;
        uint64_t index;
        uint64_t term;
        uint64_t woffset;
        int wlen;
    };

    testItem tests[11];

    tests[0].offset = 0;
    tests[0].index = 5;
    tests[0].term = 1;
    tests[0].woffset = 0;
    tests[0].wlen = 0;

    snail::raft::EntryPtr ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[1].entries.push_back(ent);
    tests[1].offset = 5;
    tests[1].index = 5;
    tests[1].term = 1;
    tests[1].woffset = 6;
    tests[1].wlen = 0;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[2].entries.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(6);
    ent->set_term(1);
    tests[2].entries.push_back(ent);
    tests[2].offset = 5;
    tests[2].index = 5;
    tests[2].term = 1;
    tests[2].woffset = 6;
    tests[2].wlen = 1;

    ent = snail::raft::make_entry();
    ent->set_index(6);
    ent->set_term(2);
    tests[3].entries.push_back(ent);
    tests[3].offset = 6;
    tests[3].index = 6;
    tests[3].term = 1;
    tests[3].woffset = 6;
    tests[3].wlen = 1;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[4].entries.push_back(ent);
    tests[4].offset = 5;
    tests[4].index = 4;
    tests[4].term = 1;
    tests[4].woffset = 5;
    tests[4].wlen = 1;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[5].entries.push_back(ent);
    tests[5].offset = 5;
    tests[5].index = 4;
    tests[5].term = 2;
    tests[5].woffset = 5;
    tests[5].wlen = 1;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    snail::raft::SnapshotMetadata meta;
    meta.set_index(4);
    meta.set_term(1);
    tests[6].entries.push_back(ent);
    tests[6].offset = 5;
    tests[6].snap = snail::raft::make_snapshot();
    tests[6].snap->set_metadata(std::move(meta));
    tests[6].index = 5;
    tests[6].term = 1;
    tests[6].woffset = 6;
    tests[6].wlen = 0;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[7].entries.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(6);
    ent->set_term(1);
    tests[7].entries.push_back(ent);
    meta.set_index(4);
    meta.set_term(1);
    tests[7].offset = 5;
    tests[7].snap = snail::raft::make_snapshot();
    tests[7].snap->set_metadata(std::move(meta));
    tests[7].index = 5;
    tests[7].term = 1;
    tests[7].woffset = 6;
    tests[7].wlen = 1;

    ent = snail::raft::make_entry();
    ent->set_index(6);
    ent->set_term(2);
    meta.set_index(5);
    meta.set_term(1);
    tests[8].entries.push_back(ent);
    tests[8].offset = 6;
    tests[8].snap = snail::raft::make_snapshot();
    tests[8].snap->set_metadata(std::move(meta));
    tests[8].index = 6;
    tests[8].term = 1;
    tests[8].woffset = 6;
    tests[8].wlen = 1;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    meta.set_index(4);
    meta.set_term(1);
    tests[9].entries.push_back(ent);
    tests[9].offset = 5;
    tests[9].snap = snail::raft::make_snapshot();
    tests[9].snap->set_metadata(std::move(meta));
    tests[9].index = 4;
    tests[9].term = 1;
    tests[9].woffset = 5;
    tests[9].wlen = 1;

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(2);
    meta.set_index(4);
    meta.set_term(2);
    tests[10].entries.push_back(ent);
    tests[10].offset = 5;
    tests[10].snap = snail::raft::make_snapshot();
    tests[10].snap->set_metadata(std::move(meta));
    tests[10].index = 4;
    tests[10].term = 1;
    tests[10].woffset = 5;
    tests[10].wlen = 1;

    for (int i = 0; i < 11; i++) {
        snail::raft::Unstable u;
        u.entries_ = std::move(tests[i].entries);
        u.offset_ = tests[i].offset;
        u.snapshot_ = tests[i].snap;

        u.StableTo(tests[i].index, tests[i].term);
        BOOST_REQUIRE_EQUAL(u.offset_, tests[i].woffset);
        BOOST_REQUIRE_EQUAL(u.entries_.size(), tests[i].wlen);
    }
}

SEASTAR_THREAD_TEST_CASE(UnstableTruncateAndAppendTest) {
    struct testItem {
        std::deque<snail::raft::EntryPtr> entries;
        uint64_t offset;
        snail::raft::SnapshotPtr snap;
        std::vector<snail::raft::EntryPtr> toappend;

        uint64_t woffset;
        std::vector<snail::raft::EntryPtr> wentries;
    };

    testItem tests[5];

    snail::raft::EntryPtr ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[0].entries.push_back(ent);
    tests[0].offset = 5;
    for (uint64_t i = 6; i < 8; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(1);
        tests[0].toappend.push_back(ent);
    }
    tests[0].woffset = 5;
    for (uint64_t i = 5; i < 8; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(1);
        tests[0].wentries.push_back(ent);
    }

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[1].entries.push_back(ent);
    tests[1].offset = 5;
    for (uint64_t i = 5; i < 7; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(2);
        tests[1].toappend.push_back(ent);
    }
    tests[1].woffset = 5;
    for (uint64_t i = 5; i < 7; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(2);
        tests[1].wentries.push_back(ent);
    }

    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[2].entries.push_back(ent);
    tests[2].offset = 5;
    for (uint64_t i = 4; i < 7; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(2);
        tests[2].toappend.push_back(ent);
    }
    tests[2].woffset = 4;
    for (uint64_t i = 4; i < 7; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(2);
        tests[2].wentries.push_back(ent);
    }

    for (uint64_t i = 5; i < 8; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(1);
        tests[3].entries.push_back(ent);
    }
    tests[3].offset = 5;
    ent = snail::raft::make_entry();
    ent->set_index(6);
    ent->set_term(2);
    tests[3].toappend.push_back(ent);
    tests[3].woffset = 5;
    ent = snail::raft::make_entry();
    ent->set_index(5);
    ent->set_term(1);
    tests[3].wentries.push_back(ent);
    ent = snail::raft::make_entry();
    ent->set_index(6);
    ent->set_term(2);
    tests[3].wentries.push_back(ent);

    for (uint64_t i = 5; i < 8; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(1);
        tests[4].entries.push_back(ent);
    }
    tests[4].offset = 5;
    for (uint64_t i = 7; i < 9; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        ent->set_term(2);
        tests[4].toappend.push_back(ent);
    }
    tests[4].woffset = 5;
    for (uint64_t i = 5; i < 9; i++) {
        ent = snail::raft::make_entry();
        ent->set_index(i);
        if (i < 7) {
            ent->set_term(1);
        } else {
            ent->set_term(2);
        }
        tests[4].wentries.push_back(ent);
    }

    for (int i = 0; i < 5; i++) {
        snail::raft::Unstable u;
        u.entries_ = std::move(tests[i].entries);
        u.offset_ = tests[i].offset;
        u.TruncateAndAppend(tests[i].toappend);
        BOOST_REQUIRE_EQUAL(u.offset_, tests[i].woffset);
        BOOST_REQUIRE_EQUAL(u.entries_.size(), tests[i].wentries.size());
        for (int j = 0; j < u.entries_.size(); j++) {
            BOOST_REQUIRE(*u.entries_[j] == *tests[i].wentries[j]);
        }
    }
}

}  // namespace raft
}  // namespace snail
