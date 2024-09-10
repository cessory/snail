#include "extentnode/store.h"

#include <isa-l.h>

#include <iostream>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "net/byteorder.h"
#include "util/logger.h"
#include "util/util.h"

namespace snail {
namespace stream {

SEASTAR_THREAD_TEST_CASE(sample_test) {
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);

    ExtentID extent_id(1, 1);
    Status<> s;
    s = store->CreateExtent(extent_id).get0();
    BOOST_REQUIRE(s.OK());

    auto extent_ptr = store->GetExtent(extent_id);
    BOOST_REQUIRE(extent_ptr);

    uint64_t off = 0;
    std::vector<iovec> iov;
    std::vector<Buffer> buf_vec;
    Buffer buf =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, 4096);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(buf.get()), 496);
    net::BigEndian::PutUint32(buf.get_write() + 496, crc);
    iov.push_back({buf.get_write(), 500});
    s = store->Write(extent_ptr, off, std::move(iov)).get0();
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
    s = store->Write(extent_ptr, off, std::move(iov)).get0();
    BOOST_REQUIRE(s.OK());
    buf_vec.clear();
    BOOST_REQUIRE_EQUAL(extent_ptr->len, 4226556);

    off = extent_ptr->len;
    buf = seastar::temporary_buffer<char>::aligned(kMemoryAlignment, 32565);
    crc = crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(buf.get()),
                          32561);
    net::BigEndian::PutUint32(buf.get_write() + 32561, crc);
    iov.push_back({buf.get_write(), buf.size()});
    s = store->Write(extent_ptr, off, std::move(iov)).get0();
    BOOST_REQUIRE(s.OK());
    buf_vec.clear();
    BOOST_REQUIRE_EQUAL(extent_ptr->len, 4259117);

    for (uint64_t off = 0; off < extent_ptr->len; off += kBlockDataSize) {
        size_t n = kBlockDataSize;
        if (off + kBlockDataSize > extent_ptr->len) {
            n = extent_ptr->len - off;
        }
        auto st = store->Read(extent_ptr, off, n).get0();
        if (!st.OK()) {
            LOG_ERROR("read block off={} error: {}", off, st.String());
        }
        BOOST_REQUIRE(st.OK());
        std::vector<Buffer> res = std::move(st.Value());
        BOOST_REQUIRE_EQUAL(res.size(), 1);
        uint32_t crc = crc32_gzip_refl(
            0, reinterpret_cast<const unsigned char *>(res[0].get()), n);
        uint32_t origin_crc = net::BigEndian::Uint32(res[0].get() + n);
        BOOST_REQUIRE_EQUAL(crc, origin_crc);
    }

    size_t read_bytes = 0;
    size_t read_len = 4225251;
    auto st = store->Read(extent_ptr, 0, read_len).get0();
    auto datas = std::move(st.Value());
    for (int i = 0; i < datas.size(); i++) {
        size_t data_len = datas[i].size();
        const char *p = datas[i].get();
        while (data_len > 0) {
            size_t n = std::min(kBlockSize, data_len);
            uint32_t crc = crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char *>(p), n - 4);
            uint32_t origin_crc = net::BigEndian::Uint32(p + n - 4);
            BOOST_REQUIRE_EQUAL(crc, origin_crc);
            data_len -= n;
            p += n;
            read_bytes += n - 4;
        }
    }
    BOOST_REQUIRE_EQUAL(read_bytes, read_len);

    store->Close().get();
}

SEASTAR_THREAD_TEST_CASE(delete_test) {
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);

    auto store = Store::Load("/dev/sdb").get0();
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

    auto store = Store::Load("/dev/sdb").get0();
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
        Buffer b = seastar::temporary_buffer<char>::aligned(4096, 4096);
        std::vector<Buffer> buffers;
        std::vector<iovec> iov;
        if (free < 200) {
            char *p = b.get_write();
            uint32_t crc = crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char *>(p), free);
            net::BigEndian::PutUint32(p + free, crc);
            iov.push_back({p, free + 4});
            p += free + 4;

            Buffer buf = seastar::temporary_buffer<char>::aligned(4096, 4096);
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

                Buffer buf =
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
        s = store->Write(extent_ptr, offset, std::move(iov)).get0();
        BOOST_REQUIRE(s.OK());
        offset += 200;
        buffers.clear();
    }
    BOOST_REQUIRE_EQUAL(extent_ptr->len, n * 200);

    auto st = store->Read(extent_ptr, 0, n * 200).get0();
    BOOST_REQUIRE(st);
    store->Close().get();
}

SEASTAR_THREAD_TEST_CASE(large_write_test) {
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);

    auto store = Store::Load("/dev/sdb").get0();
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
        Buffer buf = seastar::temporary_buffer<char>::aligned(4096, kBlockSize);
        uint32_t crc = crc32_gzip_refl(
            0, reinterpret_cast<const unsigned char *>(buf.get()),
            kBlockDataSize);
        net::BigEndian::PutUint32(buf.get_write() + kBlockDataSize, crc);
        std::vector<iovec> iov;
        iov.push_back({buf.get_write(), buf.size()});
        s = store->Write(extent_ptr, offset, std::move(iov)).get0();
        BOOST_REQUIRE(s.OK());
        offset += kBlockDataSize;
    }
    BOOST_REQUIRE_EQUAL(extent_ptr->len, n * kBlockDataSize);

    auto st =
        store->Read(extent_ptr, kBlockDataSize * 3, kBlockDataSize).get0();
    BOOST_REQUIRE(st);
    auto result = std::move(st.Value());
    BOOST_REQUIRE_EQUAL(result.size(), 1);
    BOOST_REQUIRE_EQUAL(result[0].size(), kBlockSize);
    uint32_t origin_crc =
        net::BigEndian::Uint32(result[0].get() + kBlockDataSize);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(result[0].get()),
        kBlockDataSize);
    BOOST_REQUIRE_EQUAL(origin_crc, crc);
}

}  // namespace stream
}  // namespace snail
