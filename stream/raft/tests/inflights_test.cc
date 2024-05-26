#include "raft/inflights.h"

#include "seastar/testing/test_case.hh"
#include "seastar/testing/thread_test_case.hh"

namespace snail {
namespace raft {

SEASTAR_THREAD_TEST_CASE(InflightsAddTest) {
    std::unique_ptr<snail::raft::Inflights> in =
        std::make_unique<snail::raft::Inflights>(10);
    for (int i = 0; i < 5; i++) {
        in->Add(uint64_t(i));
    }

    std::vector<uint64_t> buffer(10);
    for (uint64_t i = 0; i < 5; i++) {
        buffer[i] = i;
    }

    BOOST_REQUIRE(in->buffer_ == buffer);

    for (uint64_t i = 5; i < 10; i++) {
        in->Add(i);
    }

    for (uint64_t i = 0; i < 10; i++) {
        buffer[i] = i;
    }
    BOOST_REQUIRE(in->buffer_ == buffer);

    in->Reset();
    for (int i = 0; i < 10; i++) {
        in->buffer_[i] = 0;
        buffer[i] = 0;
    }
    in->head_ = 5;
    in->tail_ = 5;
    for (uint64_t i = 0; i < 5; i++) {
        in->Add(i);
        buffer[i + 5] = i;
    }
    BOOST_REQUIRE(in->buffer_ == buffer);

    for (uint64_t i = 5; i < 10; i++) {
        in->Add(i);
    }
    for (uint64_t i = 0; i < 5; i++) {
        buffer[i] = i + 5;
    }
    BOOST_REQUIRE(in->buffer_ == buffer);
}

SEASTAR_THREAD_TEST_CASE(InflightsFreeToTest) {
    std::unique_ptr<snail::raft::Inflights> in =
        std::make_unique<snail::raft::Inflights>(10);
    for (uint64_t i = 0; i < 10; i++) {
        in->Add(i);
    }
    in->FreeLE(4);

    std::vector<uint64_t> buffer = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    BOOST_REQUIRE(in->buffer_ == buffer);
    BOOST_REQUIRE_EQUAL(in->count_, 5);
    BOOST_REQUIRE_EQUAL(in->tail_, 5);

    in->FreeLE(8);
    BOOST_REQUIRE_EQUAL(in->count_, 1);
    BOOST_REQUIRE_EQUAL(in->tail_, 9);

    // rotating case
    for (uint64_t i = 10; i < 15; i++) {
        in->Add(i);
        buffer[i - 10] = i;  // {10, 11, 12, 13, 14, 5, 6, 7, 8, 9}
    }
    BOOST_REQUIRE(in->buffer_ == buffer);
    in->FreeLE(12);
    BOOST_REQUIRE_EQUAL(in->tail_, 3);
    BOOST_REQUIRE_EQUAL(in->head_, 5);
    BOOST_REQUIRE_EQUAL(in->count_, 2);
    in->FreeLE(14);
    BOOST_REQUIRE_EQUAL(in->tail_, 5);
    BOOST_REQUIRE_EQUAL(in->head_, 5);
    BOOST_REQUIRE_EQUAL(in->count_, 0);
}

SEASTAR_THREAD_TEST_CASE(InflightsFreeFirstOneTest) {
    std::unique_ptr<snail::raft::Inflights> in =
        std::make_unique<snail::raft::Inflights>(10);
    for (uint64_t i = 0; i < 10; i++) {
        in->Add(i);
    }

    in->FreeFirstOne();
    BOOST_REQUIRE_EQUAL(in->tail_, 1);
    BOOST_REQUIRE_EQUAL(in->head_, 0);
    BOOST_REQUIRE_EQUAL(in->count_, 9);
}

}  // namespace raft
}  // namespace snail
