#pragma once
#include <fmt/ostream.h>
#include <time.h>

#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <vector>

#include "net/byteorder.h"

namespace snail {
namespace stream {

constexpr int kChunkEntrySize = 20;
constexpr int kExtentEntrySize = 28;
constexpr size_t kMemoryAlignment = 4096;
constexpr size_t kMemoryAlignmentMask = 4095;
constexpr size_t kSectorSize = 512;
constexpr size_t kSectorSizeMask = 511;
constexpr size_t kChunkSize = 4 << 20;
constexpr size_t kChunkSizeMask = kChunkSize - 1;
constexpr size_t kChunkDataSize = kChunkSize - 512;
constexpr size_t kBlockSize = 32768;
constexpr size_t kBlockSizeMask = kBlockSize - 1;
constexpr size_t kBlockDataSize = 32764;
constexpr int kLastBlockIndex = 127;  // the last block index in the chunk
constexpr int kLastSectorIndex = 63;  // the last sector index in the block

enum class DevType {
    HDD,
    SSD,
    NVME,
    NVME_SPDK,
    PMEM,
};

enum class DevStatus {
    NORMAL,
    RDONLY,
    BROKEN,
    MAX,
};

struct ExtentID {
    uint64_t hi = 0;
    uint64_t lo = 0;

    ExtentID() noexcept = default;

    ExtentID(uint64_t h, uint64_t l) noexcept : hi(h), lo(l) {}

    ExtentID(const ExtentID& x) noexcept : hi(x.hi), lo(x.lo) {}

    ExtentID(ExtentID&& x) noexcept = default;

    ExtentID& operator=(const ExtentID& x) noexcept {
        if (this != &x) {
            hi = x.hi;
            lo = x.lo;
        }
        return *this;
    }

    ExtentID& operator=(ExtentID&& x) noexcept {
        if (this != &x) {
            hi = x.hi;
            lo = x.lo;
        }
        return *this;
    }

    inline bool operator==(const ExtentID& x) const {
        return (hi == x.hi && lo == x.lo);
    }

    friend std::ostream& operator<<(std::ostream& os,
                                    const ExtentID& extent_id) {
        os << extent_id.hi << "-" << extent_id.lo;
        return os;
    }

    bool Parse(const std::string& str) {
        if (str.size() != 16) {
            return false;
        }
        hi = net::BigEndian::Uint64(str.c_str());
        lo = net::BigEndian::Uint64(str.c_str() + 8);
        return true;
    }

    bool Empty() const { return (hi == 0 && lo == 0); }

    uint64_t Hash() const;
};

struct ChunkEntry {
    uint32_t index = -1;
    uint32_t next = -1;
    uint32_t len = 0;   // data len, exclude crc
    uint32_t crc = 0;   // the last block crc
    uint32_t scrc = 0;  // the last sector crc

    ChunkEntry() = default;

    ChunkEntry(uint32_t idx) {
        index = idx;
        next = -1;
        len = 0;
        crc = 0;
        scrc = 0;
    }

    ChunkEntry(const ChunkEntry& x) {
        index = x.index;
        next = x.next;
        len = x.len;
        crc = x.crc;
        scrc = x.scrc;
    }

    ChunkEntry& operator=(const ChunkEntry& x) {
        if (this != &x) {
            index = x.index;
            next = x.next;
            len = x.len;
            crc = x.crc;
            scrc = x.scrc;
        }
        return *this;
    }
    void MarshalTo(char* b) const;
    void Unmarshal(const char* b);
};

struct ExtentEntry {
    uint32_t index = -1;
    ExtentID id;
    uint32_t chunk_idx = -1;
    uint32_t ctime = time(0);

    ExtentEntry() = default;

    ExtentEntry(const ExtentEntry& x) {
        index = x.index;
        id = x.id;
        chunk_idx = x.chunk_idx;
        ctime = x.ctime;
    }

    ExtentEntry& operator=(const ExtentEntry& x) {
        if (this != &x) {
            index = x.index;
            id = x.id;
            chunk_idx = x.chunk_idx;
            ctime = x.ctime;
        }
        return *this;
    }

    void MarshalTo(char* b) const;
    void Unmarshal(const char* b);
};

class Store;

struct Extent : public ExtentEntry {
    size_t len;
    std::vector<ChunkEntry> chunks;
    seastar::shared_mutex mu;
    Store* store;

    Extent();
    ~Extent();
    ExtentEntry GetExtentEntry();
};

using ExtentPtr = seastar::lw_shared_ptr<Extent>;

}  // namespace stream
}  // namespace snail

#if FMT_VERSION >= 90000

template <>
struct fmt::formatter<snail::stream::ExtentID> : fmt::ostream_formatter {};

#endif

namespace std {
template <>
struct hash<snail::stream::ExtentID> {
    size_t operator()(const snail::stream::ExtentID& x) const {
        return x.Hash();
    }
};

}  // namespace std
