#include "extentnode/journal.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "extentnode/device.h"

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

}  // namespace stream
}  // namespace snail
