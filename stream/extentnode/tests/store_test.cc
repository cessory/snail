#include "extentnode/store.h"

#include <isa-l.h>

#include <iostream>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "net/byteorder.h"
#include "util/logger.h"

namespace snail {
namespace stream {

SEASTAR_THREAD_TEST_CASE(sample_test) {
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
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
        buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment,
                                                       kBlockSize);
        uint32_t crc = crc32_gzip_refl(
            0, reinterpret_cast<const unsigned char*>(buf.get()),
            kBlockDataSize);
        net::BigEndian::PutUint32(buf.get_write() + kBlockDataSize, crc);
        iov.push_back({buf.get_write(), buf.size()});
        buf_vec.emplace_back(std::move(buf));
    }
    s = store->WriteBlocks(extent_ptr, off, std::move(iov)).get0();
    BOOST_REQUIRE(s.OK());
    buf_vec.clear();
    BOOST_REQUIRE_EQUAL(extent_ptr->len, 4226556);

    off = extent_ptr->len;
    buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment, 32565);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char*>(buf.get()), 32561);
    net::BigEndian::PutUint32(buf.get_write() + 32561, crc);
    iov.push_back({buf.get_write(), buf.size()});
    s = store->WriteBlocks(extent_ptr, off, std::move(iov)).get0();
    BOOST_REQUIRE(s.OK());
    buf_vec.clear();
    BOOST_REQUIRE_EQUAL(extent_ptr->len, 4259117);

    for (uint64_t off = 0; off < extent_ptr->len; off += kBlockDataSize) {
        auto st = store->ReadBlock(extent_ptr, off).get0();
        if (!st.OK()) {
            LOG_ERROR("read block off={} error: {}", off, st.String());
        }
        BOOST_REQUIRE(st.OK());
    }

    size_t read_bytes = 0;
    auto st = store->ReadBlocks(extent_ptr, 0, 4225251).get0();
    auto datas = std::move(st.Value());
    for (int i = 0; i < datas.size(); i++) {
        read_bytes += datas[i].size() - 4;
    }
    BOOST_REQUIRE_EQUAL(read_bytes, 4225251);

    store->Close().get();
}

}  // namespace stream
}  // namespace snail
