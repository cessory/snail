#include "store.h"

#include <isa-l.h>

#include <iostream>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "net/byteorder.h"
#include "spdlog/spdlog.h"

namespace snail {
namespace stream {

SEASTAR_THREAD_TEST_CASE(write_on_block) {
    auto store = Store::Load("/dev/sdb", DevType::HDD).get0();
    BOOST_REQUIRE(store);

    ExtentID extent_id(1, 1);
    Status<> s;
    s = store->CreateExtent(extent_id).get0();
    BOOST_REQUIRE(s.OK());

    auto extent_ptr = store->GetExtent(extent_id);
    BOOST_REQUIRE(extent_ptr);

    uint64_t off = 0;
    std::vector<iovec> iov;
    std::vector<TmpBuffer> buf_vec;
    TmpBuffer buf =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, 4096);
    net::BigEndian::PutUint32(
        buf.get_write() + 496,
        crc32_gzip_refl(0, reinterpret_cast<const unsigned char*>(buf.get()),
                        496));
    iov.push_back({buf.get_write(), 500});
    s = store->WriteBlocks(extent_ptr, off, std::move(iov)).get0();
    BOOST_REQUIRE(s.OK());

    BOOST_REQUIRE_EQUAL(extent_ptr->len, 496);

    off += 496;
    net::BigEndian::PutUint32(
        buf.get_write() + 16,
        crc32_gzip_refl(0, reinterpret_cast<const unsigned char*>(buf.get()),
                        16));
    iov.push_back({buf.get_write(), 20});
    buf_vec.emplace_back(std::move(buf));
    buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment, 32256);
    net::BigEndian::PutUint32(
        buf.get_write() + 32252,
        crc32_gzip_refl(0, reinterpret_cast<const unsigned char*>(buf.get()),
                        32252));
    iov.push_back({buf.get_write(), 32256});
    buf_vec.emplace_back(std::move(buf));
    for (int i = 0; i < 128; i++) {
        buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment, 32768);
        net::BigEndian::PutUint32(
            buf.get_write() + 32764,
            crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char*>(buf.get()), 32764));
        iov.push_back({buf.get_write(), 32768});
        buf_vec.emplace_back(std::move(buf));
    }
    s = store->WriteBlocks(extent_ptr, off, std::move(iov)).get0();
    BOOST_REQUIRE(s.OK());
    buf_vec.clear();
    SPDLOG_INFO("extent len={}", extent_ptr->len);

    // s = store->RemoveExtent(extent_id).get0();
    // BOOST_REQUIRE(s.OK());
    store->Close().get();
}

}  // namespace stream
}  // namespace snail
