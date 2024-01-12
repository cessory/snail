#pragma once

namespace snail {
namespace stream {

constexpr int kChunkEntrySize = 16;
constexpr int kExtentEntrySize = 24;
constexpr size_t kMemoryAlignment = 4096;
constexpr size_t kMemoryAlignmentMask = 4095;
constexpr size_t kSectorSize = 512;
constexpr size_t kSectorSizeMask = 511;

using TmpBuffer = seastar::temporary_buffer<char>;

struct ExtentID {
    uint64_t hi = 0;
    uint64_t lo = 0;

    ExtentID() noexcept = default;

    ExtentID(uint64_t h, uint64_t l) : hi(h), lo(l) noexcept {}

    ExtentID(const ChunkID& x) : hi(x.hi), lo(x.lo) noexcept {}

    ExtentID(ChunkID&& x) noexcept = default;

    ExtentID& operator=(const ExtentID& x) noexcept {
        if (this != &x) {
            hi = x.hi;
            lo = x.lo
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

    inline bool Empty() const { return (hi == 0 && lo == 0); }

    inline uint64_t Hash() const { return hi & lo; }
};

}  // namespace stream
}  // namespace snail

namespace std {
template <>
struct hash<snail::stream::ExtentID> {
    size_t operator()(const snail::stream::ExtentID& x) const {
        return std::hash<uint64_t>()(x.Hash());
    }
};

}  // namespace std
namespace snail {
namespace stream {

struct ChunkEntry {
    uint32_t index;
    uint32_t next;
    uint32_t len;  // data len, exclude crc
    uint32_t crc;

    ChunkEntry() {
        index = 0;
        next = -1;
        len = 0;
        crc = 0;
    }

    ChunkEntry(uint32_t idx) {
        index = idx;
        next = -1;
        len = 0;
        crc = 0;
    }

    ChunkEntry(const ChunkEntry& x) {
        index = x.index;
        next = x.index;
        len = x.len;
        crc = x.crc;
    }

    ChunkEntry& operator=(const ChunkEntry& x) {
        if (this != &x) {
            index = x.index;
            next = x.index;
            len = x.len;
            crc = x.crc;
        }
        return *this;
    }
    int MarshalTo(char* b) const;
};

struct ExtentEntry {
    uint32_t index;
    ExtentID id;
    uint32_t chunk_idx;

    int MarshalTo(char* b) const;
};

struct Extent : public ExtentEntry {
    size_t len;
    std::vector<ChunkEntry> chunks;
    seastar::shared_mutex mu;
};

using ExtentPtr = seastar::lw_shared_ptr<Extent>;

}  // namespace stream
}  // namespace snail
