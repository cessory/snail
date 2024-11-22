#include "extentnode/journal.h"

#include <isa-l.h>

#include <seastar/core/align.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "extentnode/device.h"
#include "extentnode/store.h"
#include "util/logger.h"

namespace snail {
namespace stream {

SEASTAR_THREAD_TEST_CASE(journal_simple_test) {
    auto ok =
        Store::Format("/dev/sdb", 1, snail::stream::DevType::HDD, 1).get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);

    ExtentID extent_id(1, 1);
    auto s = store->CreateExtent(extent_id).get0();
    BOOST_REQUIRE(s);
    {
        ExtentPtr extent_ptr = store->GetExtent(extent_id);
        BOOST_REQUIRE(extent_ptr);
    }
    LOG_INFO("start close......");
    store->Close().get0();
    LOG_INFO("end close......");

    // load device again
    store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);
    {
        ExtentPtr extent_ptr = store->GetExtent(extent_id);
        BOOST_REQUIRE(extent_ptr);
        BOOST_REQUIRE_EQUAL(extent_ptr->chunk_idx, -1);
    }
    store->Close().get0();
}

SEASTAR_THREAD_TEST_CASE(background_flush_test) {}

}  // namespace stream
}  // namespace snail
