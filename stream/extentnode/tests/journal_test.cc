#include "extentnode/journal.h"

#include <isa-l.h>

#include <seastar/core/align.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "extentnode/device.h"
#include "util/logger.h"

namespace snail {
namespace stream {

class MemDev : public Device {
    size_t capacity_;
    std::string name_;
    char* buffer_ = nullptr;

   public:
    MemDev(size_t cap) : capacity_(cap) {
        name_ = "memory device";
        buffer_ = (char*)malloc(cap);
        memset(buffer_, 0, cap);
    }

    virtual ~MemDev() { free(buffer_); }

    static std::pair<DevicePtr, SuperBlock> make_memory_device() {
        seastar::shared_ptr<MemDev> dev_ptr =
            seastar::make_shared<MemDev>(64 << 20);
        SuperBlock super_block;
        for (int i = 0; i < DATA_PT; i++) {
            super_block.pt[i].start = i * (16 << 20);
            super_block.pt[i].size = 16 << 20;
        }
        return std::make_pair<DevicePtr, SuperBlock>(
            seastar::dynamic_pointer_cast<Device, MemDev>(dev_ptr),
            std::move(super_block));
    }

    static std::pair<DevicePtr, SuperBlock> make_memory_device(
        size_t extent_cap, size_t chunk_cap, size_t journal_cap) {
        extent_cap = seastar::align_down(extent_cap, kChunkSize);
        chunk_cap = seastar::align_down(chunk_cap, kChunkSize);
        journal_cap = seastar::align_down(journal_cap, kChunkSize);
        seastar::shared_ptr<MemDev> dev_ptr = seastar::make_shared<MemDev>(
            extent_cap + chunk_cap + 2 * journal_cap);

        std::array<size_t, DATA_PT> pt_size_vec = {extent_cap, chunk_cap,
                                                   journal_cap, journal_cap};
        SuperBlock super_block;
        size_t off = 0;
        for (int i = 0; i < DATA_PT; i++) {
            super_block.pt[i].start = off;
            super_block.pt[i].size = pt_size_vec[i];
            off += pt_size_vec[i];
        }
        return std::make_pair<DevicePtr, SuperBlock>(
            seastar::dynamic_pointer_cast<Device, MemDev>(dev_ptr),
            std::move(super_block));
    }

    const std::string& Name() const { return name_; }

    size_t Capacity() const { return capacity_; }

    seastar::temporary_buffer<char> Get(size_t n) {
        size_t len = std::max(n, kMemoryAlignment);
        auto buf =
            seastar::temporary_buffer<char>::aligned(kMemoryAlignment, len);
        buf.trim(n);
        return std::move(buf);
    }

    seastar::future<Status<>> Write(uint64_t pos, const char* b, size_t len) {
        Status<> s;
        if ((pos & kSectorSizeMask) || (len & kSectorSizeMask) ||
            (reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
            s.Set(EINVAL);
            co_return s;
        }
        if (pos + len > capacity_) {
            s.Set(ERANGE);
            co_return s;
        }
        memcpy(buffer_ + pos, b, len);
        co_return s;
    }

    seastar::future<Status<>> Write(uint64_t pos, std::vector<iovec> iov) {
        Status<> s;
        if (pos & kSectorSizeMask) {
            s.Set(EINVAL);
            co_return s;
        }

        for (int i = 0; i < iov.size(); i++) {
            if (pos + iov[i].iov_len > capacity_) {
                s.Set(ERANGE);
                break;
            }
            if ((reinterpret_cast<uintptr_t>(iov[i].iov_base) &
                 kMemoryAlignmentMask) ||
                (iov[i].iov_len & kSectorSizeMask)) {
                s.Set(EINVAL);
                break;
            }
            memcpy(buffer_ + pos, iov[i].iov_base, iov[i].iov_len);
            pos += iov[i].iov_len;
        }
        co_return s;
    }

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len) {
        Status<size_t> s;
        if ((pos & kSectorSizeMask) || (len & kSectorSizeMask) ||
            (reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
            s.Set(EINVAL);
            co_return s;
        }

        if (pos >= capacity_) {
            s.SetValue(0);
            co_return s;
        } else if (pos + len > capacity_) {
            len = capacity_ - pos;
        }
        memcpy(b, buffer_ + pos, len);
        s.SetValue(len);
        co_return s;
    }

    seastar::future<> Close() { co_return; }
};

static seastar::future<Status<>> LoadMeta(
    DevicePtr dev_ptr, uint64_t off, size_t size,
    seastar::noncopyable_function<Status<>(const char*)>&& f) {
    Status<> s;
    auto buffer = dev_ptr->Get(kBlockSize);
    uint64_t end = off + size;
    for (; off < end; off += kBlockSize) {
        auto st =
            co_await dev_ptr->Read(off, buffer.get_write(), buffer.size());
        if (!st) {
            s.Set(s.Code(), s.Reason());
            co_return s;
        }
        for (const char* p = buffer.get(); p < buffer.get() + buffer.size();
             p += kSectorSize) {
            uint32_t crc = net::BigEndian::Uint32(p);
            uint16_t len = net::BigEndian::Uint16(p + 4);
            if (crc32_gzip_refl(0,
                                reinterpret_cast<const unsigned char*>(p + 4),
                                len + 2) != crc) {
                s.Set(ErrCode::ErrInvalidChecksum);
                co_return s;
            }
            s = f(p + 6);
            if (!s) {
                co_return s;
            }
        }
    }
    co_return s;
}

static seastar::future<Status<std::unordered_map<uint32_t, ExtentEntry>>>
LoadExtents(DevicePtr dev_ptr, uint64_t off, size_t size) {
    Status<std::unordered_map<uint32_t, ExtentEntry>> s;
    std::unordered_map<uint32_t, ExtentEntry> extent_ht;
    uint32_t index = 0;

    auto st = co_await LoadMeta(
        dev_ptr, off, size,
        [&extent_ht, &index, dev_ptr](const char* b) -> Status<> {
            Status<> s;
            ExtentEntry extent;
            extent.Unmarshal(b);
            if (extent.index != index) {
                LOG_ERROR(
                    "read device {} extent meta error: invalid index "
                    "index={} expect index={}",
                    dev_ptr->Name(), extent.index, index);
                s.Set(ErrCode::ErrUnExpect, "invalid index");
                return s;
            }
            extent_ht[index] = extent;
            index++;
            return s;
        });
    s.SetValue(std::move(extent_ht));
    co_return s;
}

static seastar::future<Status<std::unordered_map<uint32_t, ChunkEntry>>>
LoadChunks(DevicePtr dev_ptr, uint64_t off, size_t size) {
    Status<std::unordered_map<uint32_t, ChunkEntry>> s;
    std::unordered_map<uint32_t, ChunkEntry> chunk_ht;
    uint32_t index = 0;

    auto st = co_await LoadMeta(
        dev_ptr, off, size,
        [&chunk_ht, &index, dev_ptr](const char* b) -> Status<> {
            Status<> s;
            ChunkEntry chunk;
            chunk.Unmarshal(b);
            if (chunk.index != index) {
                LOG_ERROR(
                    "read device {} chunk meta error: invalid index "
                    "index={} expect index={}",
                    dev_ptr->Name(), chunk.index, index);
                s.Set(ErrCode::ErrUnExpect, "invalid index");
                return s;
            }
            chunk_ht[index] = chunk;
            index++;
            return s;
        });
    s.SetValue(std::move(chunk_ht));
    co_return s;
}

SEASTAR_THREAD_TEST_CASE(save_chunk_test) {
    auto pair_dev = MemDev::make_memory_device();
    auto dev_ptr = pair_dev.first;
    auto super_block = pair_dev.second;

    Status<> s;
    {
        Journal journal(dev_ptr, super_block);
        s = journal
                .Init([](const ExtentEntry&) -> Status<> { return Status<>(); },
                      [](const ChunkEntry&) -> Status<> { return Status<>(); })
                .get0();
        BOOST_REQUIRE(s);

        for (int i = 0; i < 10; i++) {
            ChunkEntry entry;
            entry.index = i;
            s = journal.SaveChunk(entry).get0();
            BOOST_REQUIRE(s);
        }
        journal.Close().get();
    }

    {
        Journal journal(dev_ptr, super_block);
        std::vector<ChunkEntry> chunks;
        s = journal
                .Init(
                    [&chunks](const ExtentEntry&) -> Status<> {
                        return Status<>();
                    },
                    [&chunks](const ChunkEntry& chunk) -> Status<> {
                        chunks.push_back(chunk);
                        return Status<>();
                    })
                .get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(chunks.size(), 10);
        for (int i = 0; i < 10; i++) {
            BOOST_REQUIRE_EQUAL(chunks[i].index, (uint32_t)i);
        }
        journal.Close().get();
    }
    dev_ptr->Close().get();
}

SEASTAR_THREAD_TEST_CASE(save_extent_test) {
    auto pair_dev = MemDev::make_memory_device();
    auto dev_ptr = pair_dev.first;
    auto super_block = pair_dev.second;

    Status<> s;
    {
        Journal journal(dev_ptr, super_block);
        s = journal
                .Init([](const ExtentEntry&) -> Status<> { return Status<>(); },
                      [](const ChunkEntry&) -> Status<> { return Status<>(); })
                .get0();
        BOOST_REQUIRE(s);

        for (int i = 0; i < 10; i++) {
            ExtentEntry entry;
            entry.index = i;
            s = journal.SaveExtent(entry).get0();
            BOOST_REQUIRE(s);
        }
        journal.Close().get();
    }

    {
        Journal journal(dev_ptr, super_block);
        std::vector<ExtentEntry> extents;
        s = journal
                .Init(
                    [&extents](const ExtentEntry& extent) -> Status<> {
                        extents.push_back(extent);
                        return Status<>();
                    },
                    [](const ChunkEntry& chunk) -> Status<> {
                        return Status<>();
                    })
                .get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(extents.size(), 10);
        for (int i = 0; i < 10; i++) {
            BOOST_REQUIRE_EQUAL(extents[i].index, (uint32_t)i);
        }
        journal.Close().get();
    }
    dev_ptr->Close().get();
}

SEASTAR_THREAD_TEST_CASE(background_flush_test) {
    auto pair_dev = MemDev::make_memory_device(16 << 20, 16 << 20, 4 << 20);
    auto dev_ptr = pair_dev.first;
    auto super_block = pair_dev.second;

    Status<> s;
    {
        Journal journal(dev_ptr, super_block);
        s = journal
                .Init([](const ExtentEntry&) -> Status<> { return Status<>(); },
                      [](const ChunkEntry&) -> Status<> { return Status<>(); })
                .get0();
        BOOST_REQUIRE(s);

        std::vector<seastar::future<Status<>>> fu_vec;
        for (int n = 0; n < 2; n++) {
            for (int i = 0; i < 32768; i++) {
                ChunkEntry chunk;
                ExtentEntry extent;
                chunk.index = i;
                chunk.len = 10 + n;

                extent.index = i;
                extent.id = ExtentID(i, i);
                extent.chunk_idx = chunk.index;

                auto f1 = journal.SaveChunk(chunk);
                auto f2 = journal.SaveExtent(extent);
                fu_vec.emplace_back(std::move(f1));
                fu_vec.emplace_back(std::move(f2));
                if (fu_vec.size() >= 1024) {
                    auto results =
                        seastar::when_all_succeed(fu_vec.begin(), fu_vec.end())
                            .get0();
                    for (int j = 0; j < results.size(); j++) {
                        BOOST_REQUIRE(results[j]);
                    }
                    fu_vec.clear();
                }
            }
        }

        if (fu_vec.size()) {
            auto results =
                seastar::when_all_succeed(fu_vec.begin(), fu_vec.end()).get0();
            for (int j = 0; j < results.size(); j++) {
                BOOST_REQUIRE(results[j]);
            }
            fu_vec.clear();
        }
        journal.Close().get();
    }

    {
        Journal journal(dev_ptr, super_block);
        std::unordered_map<uint32_t, ExtentEntry> extents;
        std::unordered_map<uint32_t, ChunkEntry> chunks;

        auto s1 = LoadExtents(dev_ptr, super_block.pt[EXTENT_PT].start,
                              super_block.pt[EXTENT_PT].size)
                      .get0();
        BOOST_REQUIRE(s1);
        extents = std::move(s1.Value());
        auto s2 = LoadChunks(dev_ptr, super_block.pt[CHUNK_PT].start,
                             super_block.pt[CHUNK_PT].size)
                      .get0();
        BOOST_REQUIRE(s2);
        chunks = std::move(s2.Value());

        s = journal
                .Init(
                    [&extents](const ExtentEntry& extent) -> Status<> {
                        extents[extent.index] = extent;
                        return Status<>();
                    },
                    [&chunks](const ChunkEntry& chunk) -> Status<> {
                        chunks[chunk.index] = chunk;
                        return Status<>();
                    })
                .get0();
        BOOST_REQUIRE(s);
        BOOST_REQUIRE_EQUAL(extents.size(), 32768);
        BOOST_REQUIRE_EQUAL(chunks.size(), 32768);
        for (int i = 0; i < 32768; i++) {
            BOOST_REQUIRE_EQUAL(extents[i].index, (uint32_t)i);
            BOOST_REQUIRE_EQUAL(extents[i].chunk_idx, (uint32_t)i);
            BOOST_REQUIRE_EQUAL(chunks[i].len, (uint32_t)11);
        }
        journal.Close().get();
    }
    dev_ptr->Close().get();
}

}  // namespace stream
}  // namespace snail
