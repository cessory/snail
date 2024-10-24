
#include "util/async_thread.h"

#include <iostream>
#include <random>
#include <seastar/core/memory.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "util/logger.h"

namespace snail {

SEASTAR_THREAD_TEST_CASE(async_thread_test) {
    AsyncThread th("test");
    for (int i = 0; i < 10000; i++) {
        int res = th.Submit<int>([i]() -> int { return i; }).get0();
        BOOST_REQUIRE_EQUAL(res, i);
    }
}

}  // namespace snail
