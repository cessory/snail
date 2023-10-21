#pragma once
#include <array>
#include <map>
#include <queue>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <set>
#include <unordered_set>

#include "status.h"

namespace snail {
namespace stream {

extern const size_t kBlockSize;
extern const size_t kSectorSize;
extern const size_t kMemoryAlignment;
extern const size_t kDataBlockSize;

enum class DiskType {
    HDD,
    SSD,
    NVME,
    PMEM,
};

struct SuperBlock {
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t cluster_id = 0;
    DiskType disk_type = DiskType::HDD;
    uint32_t disk_id = 0;
    uint64_t capacity = 0;
    uint64_t chunk_size = 0;

    seastar::temporary_buffer<char> Marshal();
    void Unmarshal(seastar::temporary_buffer<char>& b);
};

class ChunkID {
    char id_[16] = {};

   public:
    ChunkID() noexcept = default;

    ChunkID(uint64_t hi, uint64_t lo) noexcept {
        uint64_t* u64 = reinterpret_cast<uint64_t*>(id_);
        *u64++ = hi;
        *u64 = lo;
    }

    ChunkID(const ChunkID& x) noexcept { memcpy(id_, x.id_, 16); }

    ChunkID(ChunkID&& x) noexcept {
        memcpy(id_, x.id_, 16);
        uint64_t* u64 = reinterpret_cast<uint64_t*>(x.id_);
        *u64++ = 0;
        *u64 = 0;
    }

    ChunkID& operator=(const ChunkID& x) noexcept {
        if (this != &x) {
            memcpy(id_, x.id_, 16);
        }
        return *this;
    }

    ChunkID& operator=(ChunkID&& x) noexcept {
        if (this != &x) {
            memcpy(id_, x.id_, 16);
            uint64_t* u64 = reinterpret_cast<uint64_t*>(x.id_);
            *u64++ = 0;
            *u64 = 0;
        }
        return *this;
    }

    bool operator==(const ChunkID& x) const {
        const uint64_t* u64_1 = reinterpret_cast<const uint64_t*>(id_);
        const uint64_t* u64_2 = reinterpret_cast<const uint64_t*>(x.id_);
        return (*u64_1++ == *u64_2++ && *u64_1 == *u64_2);
    }

    bool Empty() const {
        const uint64_t* u64 = reinterpret_cast<const uint64_t*>(id_);
        return (*u64++ == 0 && *u64 == 0);
    }

    std::pair<uint64_t, uint64_t> GetID() const {
        std::pair<uint64_t, uint64_t> p;
        const uint64_t* u64 = reinterpret_cast<const uint64_t*>(id_);
        p.first = *u64++;
        p.second = *u64;
        return p;
    }
};

}  // namespace stream
}  // namespace snail

namespace std {
template <>
struct hash<snail::stream::ChunkID> {
    size_t operator()(const snail::stream::ChunkID& x) const {
        auto id_pair = x.GetID();
        return std::hash<uint64_t>()(id_pair.first & id_pair.second);
    }
};
}  // namespace std

namespace snail {
namespace stream {

//------------------block format---------------------
//|   64k data block|.......|       64k data block  |
// the last data block format
//|      64K - 512b DATA    |512 META|
struct Block {
    std::set<uint64_t>* free_set = nullptr;
    uint64_t offset = 0;
    ChunkID chunk_id;
    uint64_t next = 0;  // next block offset
    uint32_t len = 0;   // data len in block
    std::array<uint32_t, 64> crcs;

    Block(uint64_t off) : free_set(nullptr), offset(off) {
        next = 0;
        len = 0;
        memset(crcs.data(), 0, crcs.size() * sizeof(uint32_t));
    }

    Block(std::set<uint64_t>* set, uint64_t off) : free_set(set), offset(off) {
        next = 0;
        len = 0;
        memset(crcs.data(), 0, crcs.size() * sizeof(uint32_t));
    }

    ~Block() {
        if (free_set) {
            free_set->insert(offset);
        }
    }

    Block& operator=(const Block& x) {
        if (this != &x) {
            free_set = x.free_set;
            offset = x.offset;
            chunk_id = x.chunk_id;
            next = x.next;
            len = x.len;
            crcs = x.crcs;
        }
        return *this;
    }

    seastar::temporary_buffer<char> Marshal();

    void Unmarshal(seastar::temporary_buffer<char>& b);
};

using BlockPtr = seastar::lw_shared_ptr<Block>;

struct Chunk {
    bool deleted = false;
    std::queue<uint64_t>* free_q = nullptr;
    ChunkID chunk_id;
    uint64_t offset = 0;        // in memory
    uint64_t block_offset = 0;  // first block offset
    std::vector<BlockPtr> blocks;
    seastar::sstring last_sector = {};
    seastar::shared_mutex mutex;

    Chunk(uint64_t off) : free_q(nullptr), chunk_id(), offset(off) {}

    Chunk(const ChunkID& id, uint64_t off)
        : free_q(nullptr), chunk_id(id), offset(off) {}

    Chunk(std::queue<uint64_t>* q, const ChunkID& id, uint64_t off)
        : free_q(q), chunk_id(id), offset(off) {}

    ~Chunk() {
        if (free_q) {
            free_q->push(offset);
        }
    }

    bool HasDeleted() const { return deleted; }

    seastar::temporary_buffer<char> Marshal();

    void Unmarshal(seastar::temporary_buffer<char>& b);

    bool Empty() const;

    size_t Len() const;

    bool TryLock();
    void Unlock();
};

using ChunkPtr = seastar::lw_shared_ptr<Chunk>;

class Disk;

using DiskPtr = seastar::lw_shared_ptr<Disk>;

class Disk {
    seastar::file fp_;
    SuperBlock super_block_;
    uint64_t used_ = 0;
    std::set<uint64_t> free_blocks_;
    std::unordered_map<ChunkID, ChunkPtr> chunks_;
    std::unordered_set<ChunkID> allocas_;
    std::queue<uint64_t> free_chunks_;  // chunk's offset

    seastar::future<bool> LoadSuperBlock();
    seastar::future<bool> LoadChunk();

    seastar::future<Status<void>> Write(uint64_t pos, const char* b,
                                        size_t len);

    seastar::future<Status<void>> Write(uint64_t pos, std::vector<iovec> iov);

    seastar::future<Status<size_t>> Read(uint64_t pos, char* b, size_t len);

    seastar::future<Status<size_t>> Read(uint64_t pos, std::vector<iovec> iov);

    seastar::future<Status<BlockPtr>> AllocBlock(ChunkPtr ptr);

   public:
    static seastar::future<DiskPtr> Load(std::string_view name);

    static seastar::future<bool> Format(std::string_view name,
                                        uint32_t cluster_id, DiskType disk_type,
                                        uint32_t disk_id,
                                        uint64_t capacity = 0);
    Disk() = default;

    uint32_t DiskId() const;

    ChunkPtr GetChunk(const ChunkID& chunk_id);

    seastar::future<Status<ChunkPtr>> CreateChunk(ChunkID chunk_id);

    seastar::future<Status<void>> Write(ChunkPtr ptr, const char* b,
                                        size_t len);

    seastar::future<Status<size_t>> Read(ChunkPtr ptr, uint64_t pos,
                                         char* buffer, size_t len);

    seastar::future<Status<seastar::temporary_buffer<char>>> Read(ChunkPtr ptr,
                                                                  uint64_t pos,
                                                                  size_t len);

    seastar::future<Status<void>> RemoveChunk(ChunkPtr ptr);

    uint64_t Capacity() const;

    uint64_t Used() const;

    seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail

