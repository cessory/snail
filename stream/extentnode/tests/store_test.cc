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
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(buf.get()), 496);
    net::BigEndian::PutUint32(buf.get_write() + 496, crc);
    iov.push_back({buf.get_write(), 500});
    s = store->WriteBlocks(extent_ptr, off, std::move(iov)).get0();
    BOOST_REQUIRE(s.OK());

    BOOST_REQUIRE_EQUAL(extent_ptr->len, 496);

    off += 496;
    crc = crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(buf.get()),
                          16);
    net::BigEndian::PutUint32(buf.get_write() + 16, crc);
    iov.push_back({buf.get_write(), 20});
    buf_vec.emplace_back(std::move(buf));
    buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment, 32256);
    crc = crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(buf.get()),
                          32252);
    net::BigEndian::PutUint32(buf.get_write() + 32252, crc);
    iov.push_back({buf.get_write(), 32256});
    buf_vec.emplace_back(std::move(buf));
    for (int i = 0; i < 128; i++) {
        buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment,
                                                       kBlockSize);
        uint32_t crc = crc32_gzip_refl(
            0, reinterpret_cast<const unsigned char *>(buf.get()),
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
    crc = crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(buf.get()),
                          32561);
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

SEASTAR_THREAD_TEST_CASE(delete_test) {
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);

    auto store = Store::Load("/dev/sdb", DevType::HDD).get0();
    BOOST_REQUIRE(store);

    for (int i = 0; i < 1000; i++) {
        ExtentID extent_id(1, i);
        Status<> s;
        s = store->CreateExtent(extent_id).get0();
        BOOST_REQUIRE(s.OK());
    }
    for (int i = 0; i < 1000; i++) {
        ExtentID extent_id(1, i);
        Status<> s;
        s = store->RemoveExtent(extent_id).get0();
        BOOST_REQUIRE(s.OK());
    }
    store->Close().get();
}

SEASTAR_THREAD_TEST_CASE(small_write_test) {
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
    // we will write 100M data, 200 bytes each time
    int n = 100 * 1024 * 1024 / 200;
    uint64_t offset = 0;
    for (int i = 0; i < n; i++) {
        uint64_t len = extent_ptr->len;
        uint64_t free = kBlockDataSize - len % kBlockDataSize;
        TmpBuffer b = seastar::temporary_buffer<char>::aligned(4096, 4096);
        std::vector<TmpBuffer> buffers;
        std::vector<iovec> iov;
        if (free < 200) {
            char *p = b.get_write();
            uint32_t crc = crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char *>(p), free);
            net::BigEndian::PutUint32(p + free, crc);
            iov.push_back({p, free + 4});
            p += free + 4;

            TmpBuffer buf =
                seastar::temporary_buffer<char>::aligned(4096, 4096);
            memcpy(buf.get_write(), p, 200 - free);
            crc = crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(p),
                                  200 - free);
            net::BigEndian::PutUint32(buf.get_write() + 200 - free, crc);
            iov.push_back({buf.get_write(), 200 - free + 4});
            buffers.emplace_back(std::move(buf));
        } else {
            uint64_t sector_free =
                kSectorSize - len % kBlockDataSize % kSectorSize;
            if (sector_free < 200) {
                char *p = b.get_write();
                uint32_t crc = crc32_gzip_refl(
                    0, reinterpret_cast<const unsigned char *>(p), sector_free);
                net::BigEndian::PutUint32(p + sector_free, crc);
                iov.push_back({p, sector_free + 4});
                p += sector_free + 4;

                TmpBuffer buf =
                    seastar::temporary_buffer<char>::aligned(4096, 4096);
                memcpy(buf.get_write(), p, 200 - sector_free);
                crc = crc32_gzip_refl(
                    0, reinterpret_cast<const unsigned char *>(p),
                    200 - sector_free);
                net::BigEndian::PutUint32(buf.get_write() + 200 - sector_free,
                                          crc);
                iov.push_back({buf.get_write(), 200 - sector_free + 4});
                buffers.emplace_back(std::move(buf));
            } else {
                char *p = b.get_write();
                uint32_t crc = crc32_gzip_refl(
                    0, reinterpret_cast<const unsigned char *>(p), 200);
                net::BigEndian::PutUint32(p + 200, crc);
                iov.push_back({p, 200 + 4});
            }
        }
        s = store->WriteBlocks(extent_ptr, offset, std::move(iov)).get0();
        BOOST_REQUIRE(s.OK());
        offset += 200;
        buffers.clear();
    }
    BOOST_REQUIRE_EQUAL(extent_ptr->len, n * 200);
}

SEASTAR_THREAD_TEST_CASE(large_write_test) {
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
    // we will write 100M data, 32764 bytes each time
    int n = 100 * 1024 * 1024 / kBlockDataSize;
    uint64_t offset = 0;

    for (int i = 0; i < n; i++) {
        TmpBuffer buf =
            seastar::temporary_buffer<char>::aligned(4096, kBlockSize);
        uint32_t crc = crc32_gzip_refl(
            0, reinterpret_cast<const unsigned char *>(buf.get()),
            kBlockDataSize);
        net::BigEndian::PutUint32(buf.get_write() + kBlockDataSize, crc);
        std::vector<iovec> iov;
        iov.push_back({buf.get_write(), buf.size()});
        s = store->WriteBlocks(extent_ptr, offset, std::move(iov)).get0();
        BOOST_REQUIRE(s.OK());
        offset += kBlockDataSize;
    }
    BOOST_REQUIRE_EQUAL(extent_ptr->len, n * kBlockDataSize);

    auto st = store->ReadBlock(extent_ptr, kBlockDataSize * 3).get0();
    BOOST_REQUIRE(st);
    BOOST_REQUIRE_EQUAL(st.Value().size(), kBlockSize);
    uint32_t origin_crc =
        net::BigEndian::Uint32(st.Value().get() + kBlockDataSize);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(st.Value().get()),
        kBlockDataSize);
    BOOST_REQUIRE_EQUAL(origin_crc, crc);
}

}  // namespace stream
}  // namespace snail
