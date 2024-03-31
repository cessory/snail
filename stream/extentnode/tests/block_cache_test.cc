#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "extentnode/service.h"
#include "util/logger.h"

namespace snail {
namespace stream {

SEASTAR_THREAD_TEST_CASE(block_cache_test) {
    LRUCache<BlockCacheKey, TmpBuffer> block_cache(10);

    ExtentID extent_id(1, 1);

    for (int i = 0; i < 20; i++) {
        BlockCacheKey key(extent_id, i);
        TmpBuffer buf(4096);
        memset(buf.get_write(), i, 4096);
        block_cache.Insert(key, buf);
    }

    for (int i = 0; i < 10; i++) {
        BlockCacheKey key(extent_id, i);
        auto v = block_cache.Get(key);
        BOOST_REQUIRE(!v.has_value());
    }

    for (int i = 10; i < 20; i++) {
        BlockCacheKey key(extent_id, i);
        auto v = block_cache.Get(key);
        BOOST_REQUIRE(v.has_value());
        TmpBuffer buf(4096);
        memset(buf.get_write(), i, 4096);
        BOOST_REQUIRE(buf == v.value());
    }
}

}  // namespace stream
}  // namespace snail
