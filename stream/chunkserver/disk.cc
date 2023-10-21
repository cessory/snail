#include "disk.h"

#include <isa-l.h>

#include <algorithm>
#include <iostream>
#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/defer.hh>
#include <system_error>

#include "spdlog/spdlog.h"
#include "status.h"

namespace snail {
namespace stream {

const size_t kMagic = 0x69616e73;
const size_t kVersion = 1;
const size_t kBlockSize = 4 << 20;
const size_t kSectorSize = 512;
const size_t kMemoryAlignment = 4096;
const size_t kDataBlockSize = 65536;

seastar::temporary_buffer<char> SuperBlock::Marshal() {
    seastar::temporary_buffer<char> b =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, kSectorSize);
    char *p = b.get_write();
    memset(p, 0, kSectorSize);
    uint32_t *u32 = reinterpret_cast<uint32_t *>(p);
    *u32++ = seastar::net::hton(magic);
    *u32++ = seastar::net::hton(version);
    *u32++ = seastar::net::hton(cluster_id);
    *u32++ = seastar::net::hton(static_cast<uint32_t>(disk_type));
    *u32++ = seastar::net::hton(disk_id);

    uint64_t *u64 = reinterpret_cast<uint64_t *>(u32);
    *u64++ = seastar::net::hton(capacity);
    *u64++ = seastar::net::hton(chunk_size);

    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(b.begin()), b.size() - 4);
    p = b.get_write() + kSectorSize - 4;
    u32 = reinterpret_cast<uint32_t *>(p);
    *u32 = seastar::net::hton(crc);

    return b;
}

void SuperBlock::Unmarshal(seastar::temporary_buffer<char> &b) {
    if (b.size() != kSectorSize) {
        throw std::runtime_error("invalid buffer");
    }

    const char *p = b.begin();
    const uint32_t *u32 = reinterpret_cast<const uint32_t *>(p);
    magic = seastar::net::ntoh(*u32++);
    version = seastar::net::ntoh(*u32++);
    cluster_id = seastar::net::ntoh(*u32++);
    disk_type = static_cast<DiskType>(seastar::net::ntoh(*u32++));
    disk_id = seastar::net::ntoh(*u32++);
    const uint64_t *u64 = reinterpret_cast<const uint64_t *>(u32);
    capacity = seastar::net::ntoh(*u64++);
    chunk_size = seastar::net::ntoh(*u64++);

    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(b.begin()), b.size() - 4);
    p = b.end() - 4;
    u32 = reinterpret_cast<const uint32_t *>(p);
    uint32_t origin_crc = seastar::net::ntoh(*u32);
    if (crc != origin_crc) {
        throw std::runtime_error("crc not match");
    }

    if ((capacity & (kBlockSize - 1)) != 0) {
        throw std::runtime_error("capacity is not aligned block size");
    }

    if ((chunk_size & (kSectorSize - 1)) != 0) {
        throw std::runtime_error("chunk size is not aligned sector size");
    }
}

seastar::temporary_buffer<char> Block::Marshal() {
    seastar::temporary_buffer<char> b =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, kSectorSize);
    char *p = b.get_write();
    memset(p, 0, kSectorSize);
    uint64_t *u64 = reinterpret_cast<uint64_t *>(p);
    auto id_pair = chunk_id.GetID();
    *u64++ = seastar::net::hton(id_pair.first);
    *u64++ = seastar::net::hton(id_pair.second);
    *u64++ = seastar::net::hton(next);

    uint32_t *u32 = reinterpret_cast<uint32_t *>(u64);
    *u32++ = seastar::net::hton(len);

    for (int i = 0; i < 64; i++) {
        *u32++ = seastar::net::hton(crcs[i]);
    }

    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(b.begin()), b.size() - 4);
    p = b.get_write() + kSectorSize - 4;
    u32 = reinterpret_cast<uint32_t *>(p);
    *u32 = seastar::net::hton(crc);
    return b;
}

void Block::Unmarshal(seastar::temporary_buffer<char> &b) {
    if (b.size() != kSectorSize) {
        throw std::runtime_error("invalid buffer");
    }

    const char *p = b.get();
    const uint64_t *u64 = reinterpret_cast<const uint64_t *>(p);
    uint64_t hi = seastar::net::ntoh(*u64++);
    uint64_t lo = seastar::net::ntoh(*u64++);
    chunk_id = ChunkID(hi, lo);
    next = seastar::net::ntoh(*u64++);
    const uint32_t *u32 = reinterpret_cast<const uint32_t *>(u64);
    len = seastar::net::ntoh(*u32++);

    for (int i = 0; i < 64; i++) {
        crcs[i] = seastar::net::ntoh(*u32++);
    }

    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(b.begin()), b.size() - 4);
    p = b.end() - 4;
    u32 = reinterpret_cast<const uint32_t *>(p);
    if (crc != seastar::net::ntoh(*u32)) {
        throw std::runtime_error("crc not match");
    }
}

seastar::temporary_buffer<char> Chunk::Marshal() {
    seastar::temporary_buffer<char> b =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, kSectorSize);
    char *p = b.get_write();
    memset(p, 0, kSectorSize);

    uint64_t *u64 = reinterpret_cast<uint64_t *>(p);
    auto id_pair = chunk_id.GetID();
    *u64++ = seastar::net::hton(id_pair.first);
    *u64++ = seastar::net::hton(id_pair.second);
    *u64++ = seastar::net::hton(block_offset);

    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(b.begin()), b.size() - 4);
    p = b.get_write() + kSectorSize - 4;
    uint32_t *u32 = reinterpret_cast<uint32_t *>(p);
    *u32 = seastar::net::hton(crc);
    return b;
}

void Chunk::Unmarshal(seastar::temporary_buffer<char> &b) {
    if (b.size() != kSectorSize) {
        throw std::runtime_error("invalid buffer");
    }
    const char *p = b.get();
    const uint64_t *u64 = reinterpret_cast<const uint64_t *>(p);
    uint64_t hi = seastar::net::ntoh(*u64++);
    uint64_t lo = seastar::net::ntoh(*u64++);
    chunk_id = ChunkID(hi, lo);
    block_offset = seastar::net::ntoh(*u64++);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(b.begin()), b.size() - 4);
    p = b.end() - 4;
    const uint32_t *u32 = reinterpret_cast<const uint32_t *>(p);
    if (seastar::net::hton(*u32) != crc) {
        throw std::runtime_error("crc not match");
    }
}

size_t Chunk::Len() const {
    return (blocks.size() - 1) * (kBlockSize - kSectorSize) +
           blocks.back()->len;
}

bool Chunk::Empty() const { return chunk_id.Empty() || block_offset == 0; }

bool Chunk::TryLock() { return mutex.try_lock(); }

void Chunk::Unlock() { mutex.unlock(); }

seastar::future<bool> Disk::Format(std::string_view name, uint32_t cluster_id,
                                   DiskType disk_type, uint32_t disk_id,
                                   uint64_t capacity) {
    seastar::file fp;
    try {
        fp = co_await seastar::open_file_dma(name, seastar::open_flags::rw);
    } catch (std::exception &e) {
        SPDLOG_ERROR("open disk {} error: {}", name, e.what());
        co_return false;
    }

    auto def = seastar::defer([&fp]() { (void)fp.close(); });
    if (capacity == 0) {
        try {
            uint64_t size = 0;
            co_await fp.ioctl(BLKGETSIZE64, &size);
            capacity = size;
        } catch (std::exception &e) {
            SPDLOG_ERROR("stat disk {} error: {}", name, e.what());
            co_return false;
        }
    }

    capacity = capacity / kBlockSize * kBlockSize;

    auto total_block_n = capacity / kBlockSize;

    uint32_t meta_block_n = (total_block_n * 512 + kBlockSize - 1) / kBlockSize;

    uint64_t meta_size = meta_block_n * kBlockSize;

    SPDLOG_INFO("capacity={} total_block_n={} meta_block_n={} meta_size={}",
                capacity, total_block_n, meta_block_n, meta_size);

    auto buf =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, kSectorSize);

    // 写设备的末尾 检测设备大小
    try {
        size_t n = co_await fp.dma_write(capacity - kSectorSize, buf.get(),
                                         kSectorSize);
        if (n != kSectorSize) {
            co_return false;
        }
    } catch (std::exception &e) {
        SPDLOG_ERROR("dma write disk {} error: {}", name, e.what());
        co_return false;
    }

    // format superblock
    SuperBlock sblock;
    sblock.magic = kMagic;
    sblock.version = kVersion;
    sblock.cluster_id = cluster_id;
    sblock.disk_type = disk_type;
    sblock.disk_id = disk_id;
    sblock.chunk_size = meta_size - kSectorSize;
    sblock.capacity = capacity;

    buf = sblock.Marshal();

    uint64_t offset = 0;
    try {
        size_t n = co_await fp.dma_write(offset, buf.get(), buf.size());
        if (n != buf.size()) {
            co_return false;
        }
    } catch (std::exception &e) {
        std::cerr << "format superblock to disk: " << name
                  << "error: " << e.what() << std::endl;
        co_return false;
    }

    offset += kSectorSize;
    auto buffer =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, kBlockSize);
    size_t buffer_offset = 0;
    int sector_n = sblock.chunk_size / kSectorSize;
    for (int i = 0; i < sector_n; i++) {
        Chunk chunk(0);
        auto b = chunk.Marshal();
        if (b.size() > buffer.size() - buffer_offset) {
            try {
                size_t n =
                    co_await fp.dma_write(offset, buffer.get(), buffer_offset);
                if (n != buffer_offset) {
                    co_return false;
                }
                offset += buffer_offset;
                buffer_offset = 0;
            } catch (std::exception &e) {
                std::cerr << "format chunk to disk: " << name
                          << "error: " << e.what() << std::endl;
                co_return false;
            }
        }
        memcpy(buffer.get_write() + buffer_offset, b.get(), b.size());
        buffer_offset += b.size();
    }
    if (buffer_offset > 0) {
        try {
            size_t n =
                co_await fp.dma_write(offset, buffer.get(), buffer_offset);
            if (n != buffer_offset) {
                co_return false;
            }
        } catch (std::exception &e) {
            std::cerr << "format chunk to disk: " << name
                      << "error: " << e.what() << std::endl;
            co_return false;
        }
    }
    co_return true;
}

seastar::future<DiskPtr> Disk::Load(std::string_view name) {
    // 1. load super block
    DiskPtr disk_ptr = seastar::make_lw_shared<Disk>();
    try {
        disk_ptr->fp_ =
            co_await seastar::open_file_dma(name, seastar::open_flags::rw);
        co_await disk_ptr->LoadSuperBlock();
        co_await disk_ptr->LoadChunk();
    } catch (std::exception &e) {
        SPDLOG_ERROR("load disk {} error: {}", name, e.what());
        co_await disk_ptr->fp_.close();
        co_return seastar::coroutine::return_exception(e);
    }
    SPDLOG_INFO("load disk {} success", name);
    co_return disk_ptr;
}

seastar::future<bool> Disk::LoadSuperBlock() {
    try {
        auto b = co_await fp_.dma_read<char>(static_cast<uint64_t>(0),
                                             static_cast<size_t>(kSectorSize));
        super_block_.Unmarshal(b);
        used_ += kSectorSize;
    } catch (...) {
        co_return seastar::coroutine::exception(std::current_exception());
    }
    co_return true;
}

seastar::future<bool> Disk::LoadChunk() {
    int sector_n = super_block_.chunk_size / kSectorSize;

    // add all blocks to free map
    uint64_t block_n =
        (super_block_.capacity - super_block_.chunk_size - kSectorSize) /
        kBlockSize;
    for (uint64_t off = kSectorSize + super_block_.chunk_size;
         off < super_block_.capacity; off += kBlockSize) {
        free_blocks_.insert(off);
    }

    used_ += super_block_.chunk_size;
    auto buffer =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, kBlockSize);
    size_t buffer_offset = 0;
    uint64_t offset = kSectorSize;

    for (int i = 0; i < sector_n;
         i++, offset += kSectorSize, buffer_offset += kSectorSize) {
        if ((buffer_offset & (kBlockSize - 1)) == 0) {
            buffer_offset = 0;
            auto n = co_await fp_.dma_read<char>(offset, buffer.get_write(),
                                                 buffer.size());
            if (n != buffer.size()) {
                co_return seastar::coroutine::exception(std::make_exception_ptr(
                    std::runtime_error("read chunk not return enough bytes")));
            }
        }
        auto b = buffer.share(buffer_offset, kSectorSize);

        ChunkPtr chunk = seastar::make_lw_shared<Chunk>(offset);
        chunk->Unmarshal(b);
        if (chunk->Empty()) {
            free_chunks_.push(offset);
            continue;
        }
        chunk->free_q = &free_chunks_;
        chunks_[chunk->chunk_id] = chunk;
        // load blocks
        uint64_t block_off = chunk->block_offset;
        while (block_off) {
            if ((block_off & (kBlockSize - 1)) != 0) {
                co_return seastar::coroutine::exception(
                    std::make_exception_ptr(std::runtime_error(
                        "invalid block: " + std::to_string(block_off))));
            }
            uint64_t off = block_off + kBlockSize - kSectorSize;
            auto iter = free_blocks_.find(block_off);
            if (iter == free_blocks_.end()) {
                co_return seastar::coroutine::exception(
                    std::make_exception_ptr(std::runtime_error(
                        "not found block: " + std::to_string(block_off))));
            }
            b = co_await fp_.dma_read<char>(off,
                                            static_cast<size_t>(kSectorSize));
            if (b.size() != kSectorSize) {
                co_return seastar::coroutine::exception(std::make_exception_ptr(
                    std::runtime_error("read meta error from block: " +
                                       std::to_string(block_off))));
            }
            auto block = seastar::make_lw_shared<Block>(block_off);
            block->Unmarshal(b);
            if (block->chunk_id != chunk->chunk_id) {
                co_return seastar::coroutine::exception(std::make_exception_ptr(
                    std::runtime_error("invalid chunkid from block: " +
                                       std::to_string(block_off))));
            }
            block_off = block->next;
            free_blocks_.erase(iter);
            chunk->blocks.push_back(block);
            block->free_set = &free_blocks_;
            used_ += kBlockSize;
        }
    }

    co_return true;
}

seastar::future<Status<ChunkPtr>> Disk::CreateChunk(ChunkID chunk_id) {
    if (chunks_.count(chunk_id) > 0 || allocas_.count(chunk_id) > 0) {
        co_return Status<ChunkPtr>(ErrCode::ErrExistChunk);
    }

    if (free_chunks_.empty()) {
        co_return Status<ChunkPtr>(ErrCode::ErrNoFreeChunks);
    }

    if (free_blocks_.empty()) {
        co_return Status<ChunkPtr>(ErrCode::ErrNoFreeBlocks);
    }

    allocas_.insert(chunk_id);

    auto def = seastar::defer([chunk_id, this]() { allocas_.erase(chunk_id); });

    auto chunk_offset = free_chunks_.front();
    free_chunks_.pop();
    auto iter = free_blocks_.begin();
    auto block_offset = *iter;
    free_blocks_.erase(iter);

    auto chunk =
        seastar::make_lw_shared<Chunk>(&free_chunks_, chunk_id, chunk_offset);
    auto block = seastar::make_lw_shared<Block>(&free_blocks_, block_offset);
    block->chunk_id = chunk_id;
    chunk->chunk_id = chunk_id;
    chunk->block_offset = block->offset;

    // save block meta to disk
    auto b = block->Marshal();
    uint64_t pos = block->offset + kBlockSize - kSectorSize;
    Status<void> s = co_await Write(pos, b.get(), b.size());
    if (!s.OK()) {
        co_return Status<ChunkPtr>(s.Code(), s.Reason());
    }

    // save chunk meta to disk
    b = chunk->Marshal();
    s = co_await Write(chunk->offset, b.get(), b.size());
    if (!s.OK()) {
        co_return Status<ChunkPtr>(s.Code(), s.Reason());
    }
    chunk->blocks.push_back(block);
    used_ += kBlockSize;
    chunks_[chunk_id] = chunk;
    Status<ChunkPtr> status(ErrCode::OK);
    status.SetValue(chunk);
    co_return status;
}

seastar::future<Status<void>> Disk::Write(uint64_t pos, const char *b,
                                          size_t len) {
    try {
        auto n = co_await fp_.dma_write(pos, b, len);
        if (n != len) {
            co_return Status<void>(ErrCode::ErrWriteDisk,
                                   "write bytes not expect");
        }
    } catch (std::system_error &e) {
        if (e.code().value() == EIO || e.code().value() == EBADF) {
            co_return Status<void>(ErrCode::ErrDisk, e.code().message());
        }
        co_return Status<void>(ErrCode::ErrSystem, e.code().message());
    } catch (std::exception &e) {
        co_return Status<void>(ErrCode::ErrWriteDisk, e.what());
    }
    co_return Status<void>(ErrCode::OK);
}

seastar::future<Status<void>> Disk::Write(uint64_t pos,
                                          std::vector<iovec> iov) {
    size_t expect = 0;
    for (int i = 0; i < iov.size(); i++) {
        expect += iov[i].iov_len;
    }
    try {
        auto n = co_await fp_.dma_write(pos, std::move(iov));
        if (n != expect) {
            co_return Status<void>(ErrCode::ErrWriteDisk,
                                   "write bytes not expect");
        }
    } catch (std::system_error &e) {
        if (e.code().value() == EIO || e.code().value() == EBADF) {
            co_return Status<void>(ErrCode::ErrDisk, e.code().message());
        }
        co_return Status<void>(ErrCode::ErrSystem, e.code().message());
    } catch (std::exception &e) {
        co_return Status<void>(ErrCode::ErrWriteDisk, e.what());
    }
    co_return Status<void>(ErrCode::OK);
}

seastar::future<Status<size_t>> Disk::Read(uint64_t pos, char *b, size_t len) {
    Status<size_t> s(ErrCode::OK);
    s.SetValue(0);
    try {
        size_t n = co_await fp_.dma_read<char>(pos, b, len);
        s.SetValue(n);
    } catch (std::system_error &e) {
        if (e.code().value() == EIO || e.code().value() == EBADF) {
            s.Set(ErrCode::ErrDisk, e.code().message());
        } else {
            s.Set(ErrCode::ErrSystem, e.code().message());
        }
    } catch (std::exception &e) {
        s.Set(ErrCode::ErrSystem, e.what());
    }
    co_return s;
}

seastar::future<Status<size_t>> Disk::Read(uint64_t pos,
                                           std::vector<iovec> iov) {
    Status<size_t> s(ErrCode::OK);
    try {
        size_t n = co_await fp_.dma_read(pos, iov);
        s.SetValue(n);
    } catch (std::system_error &e) {
        if (e.code().value() == EIO || e.code().value() == EBADF) {
            s.Set(ErrCode::ErrDisk, e.code().message());
        } else {
            s.Set(ErrCode::ErrSystem, e.code().message());
        }
    } catch (std::exception &e) {
        s.Set(ErrCode::ErrSystem, e.what());
    }
    co_return s;
}

uint32_t Disk::DiskId() const { return super_block_.disk_id; }

ChunkPtr Disk::GetChunk(const ChunkID &chunk_id) {
    auto iter = chunks_.find(chunk_id);
    if (iter == chunks_.end()) {
        return nullptr;
    }
    return iter->second;
}

seastar::future<Status<BlockPtr>> Disk::AllocBlock(ChunkPtr chunk_ptr) {
    if (free_blocks_.empty()) {
        co_return Status<BlockPtr>(ErrCode::ErrNoFreeBlocks);
    }

    // get a free block
    uint64_t block_off = 0;
    if (!chunk_ptr->blocks.empty()) {
        block_off = chunk_ptr->blocks.back()->offset;
    }
    auto iter = free_blocks_.upper_bound(block_off);
    if (iter == free_blocks_.end()) {
        iter = free_blocks_.begin();
    }

    // save free block meta
    auto block_offset = *iter;
    free_blocks_.erase(iter);
    auto block = seastar::make_lw_shared<Block>(&free_blocks_, block_offset);
    block->chunk_id = chunk_ptr->chunk_id;
    auto b = block->Marshal();
    uint64_t pos = block->offset + kBlockSize - kSectorSize;
    Status<void> s = co_await Write(pos, b.get(), b.size());
    if (!s.OK()) {
        co_return Status<BlockPtr>(s.Code(), s.Reason());
    }

    if (chunk_ptr->blocks.empty()) {  // need update chunk meta
        chunk_ptr->blocks.push_back(block);
        auto b = chunk_ptr->Marshal();
        auto s = co_await Write(chunk_ptr->offset, b.get(), b.size());
        if (!s.OK()) {
            chunk_ptr->blocks.pop_back();
            co_return Status<BlockPtr>(s.Code(), s.Reason());
        }
        Status<BlockPtr> st(ErrCode::OK);
        st.SetValue(block);
        co_return st;
    }

    // update the last block meta
    auto last = chunk_ptr->blocks.back();
    chunk_ptr->blocks.push_back(block);
    last->next = block->offset;
    b = last->Marshal();
    pos = last->offset + kBlockSize - kSectorSize;
    s = co_await Write(pos, b.get(), b.size());
    if (!s.OK()) {
        chunk_ptr->blocks.pop_back();
        last->next = 0;
        co_return Status<BlockPtr>(s.Code(), s.Reason());
    }

    Status<BlockPtr> st(ErrCode::OK);
    st.SetValue(block);
    co_return st;
}

seastar::future<Status<void>> Disk::Write(ChunkPtr chunk_ptr, const char *b,
                                          size_t len) {
    while (len > 0) {
        if (chunk_ptr->HasDeleted()) {
            co_return Status<void>(ErrCode::ErrDeletedChunk);
        }
        auto block = chunk_ptr->blocks.back();
        auto block_remaining = kBlockSize - kSectorSize - block->len;
        if (block_remaining == 0) {
            auto st = co_await AllocBlock(chunk_ptr);
            if (!st.OK()) {
                co_return Status<void>(st.Code(), st.Reason());
            }
            block = st.Value();
            block_remaining = kBlockSize - kSectorSize;
        }
        size_t n = std::min(static_cast<size_t>(block_remaining), len);
        seastar::temporary_buffer<char> buf((char *)b, n, seastar::deleter());
        if (!chunk_ptr->last_sector.empty() ||
            (reinterpret_cast<uintptr_t>(b) & (kMemoryAlignment - 1)) != 0 ||
            (n & (kSectorSize - 1)) != 0) {
            buf = seastar::temporary_buffer<char>::aligned(
                kMemoryAlignment,
                seastar::align_up<size_t>(chunk_ptr->last_sector.size() + n,
                                          kSectorSize));
            if (!chunk_ptr->last_sector.empty()) {
                memcpy(buf.get_write(), chunk_ptr->last_sector.data(),
                       chunk_ptr->last_sector.size());
            }
            memcpy(buf.get_write() + chunk_ptr->last_sector.size(), b, n);
        }

        auto s = co_await Write(block->offset + seastar::align_down<uint32_t>(
                                                    block->len, kSectorSize),
                                buf.get(), buf.size());
        if (!s.OK()) {
            co_return s;
        }
        // update meta
        Block tmp = *block;
        tmp.free_set = nullptr;  // prevent block release
        size_t ori_n = n;
        const char *p = b;
        while (n > 0) {  // update crc
            auto i = tmp.len / kDataBlockSize;
            size_t real_data_len = std::min(
                n, static_cast<size_t>(kDataBlockSize -
                                       (tmp.len & (kDataBlockSize - 1))));
            tmp.crcs[i] = crc32_gzip_refl(
                tmp.crcs[i], reinterpret_cast<const unsigned char *>(p),
                real_data_len);
            n -= real_data_len;
            p += real_data_len;
            tmp.len += real_data_len;
        }
        auto meta = tmp.Marshal();
        s = co_await Write(tmp.offset + kBlockSize - kSectorSize, meta.get(),
                           meta.size());
        if (!s.OK()) {
            co_return s;
        }
        block->len = tmp.len;
        block->crcs = tmp.crcs;
        uint32_t last_remaining =
            (chunk_ptr->last_sector.size() + ori_n) & (kSectorSize - 1);
        if (last_remaining > 0) {
            chunk_ptr->last_sector =
                seastar::sstring(b + ori_n - last_remaining, last_remaining);
        } else {
            chunk_ptr->last_sector = {};
        }
        len -= ori_n;
        b += ori_n;
    }
    co_return Status<void>(ErrCode::OK);
}

seastar::future<Status<size_t>> Disk::Read(ChunkPtr chunk_ptr, uint64_t pos,
                                           char *buffer, size_t len) {
    // 为了校验数据的crc, 需要64k对齐读
    Status<size_t> s(ErrCode::OK);
    if (pos >= chunk_ptr->Len()) {
        s.SetValue(0);
        co_return s;
    }

    int block_idx = pos / (kBlockSize - kSectorSize);
    size_t block_pos = pos % (kBlockSize - kSectorSize);
    size_t block_start = seastar::align_down<size_t>(block_pos, kDataBlockSize);
    int block_n = chunk_ptr->blocks.size();
    size_t total = 0;

    for (int i = block_idx; i < block_n && len > 0; i++) {
        auto block = chunk_ptr->blocks[i];
        std::array<uint32_t, 64> crcs = block->crcs;  // copy crc
        size_t block_end = block->len;

        // 在当前block需要读取的数据长度
        size_t n = std::min(block_end - block_pos, len);
        // 需要buffer长度, 对齐64K
        size_t buf_len = seastar::align_up<size_t>(n + block_pos - block_start,
                                                   kDataBlockSize);
        size_t tail_padding = 0;
        if (block_start + buf_len > block_end) {
            buf_len = seastar::align_up<size_t>(n + block_pos - block_start,
                                                kSectorSize);
            if (block_start + buf_len > block_end) {
                tail_padding = block_start + buf_len - block_end;
            }
        }
        auto buf =
            seastar::temporary_buffer<char>::aligned(kMemoryAlignment, buf_len);

        s = co_await Read(block->offset + block_start, buf.get_write(),
                          buf.size());
        if (!s.OK()) {
            co_return s;
        }
        if (s.Value() != buf.size()) {
            s.Set(ErrCode::ErrTooShort);
            co_return s;
        }

        // 尾部填充数据不计算CRC
        buf.trim(buf_len - tail_padding);
        // check crc
        const char *p = buf.get();
        int last_block_idx = (block_start + buf_len) / kDataBlockSize;
        for (int ii = block_start / kDataBlockSize; ii < last_block_idx; ii++) {
            uint32_t crc_len = buf.end() - p;
            if (crc_len > kDataBlockSize) {
                crc_len = kDataBlockSize;
            }
            uint32_t crc = crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char *>(p), crc_len);
            if (crc != crcs[ii]) {
                s.Set(ErrCode::ErrInvalidChecksum);
                co_return s;
            }
            p += crc_len;
        }
        buf.trim_front(block_pos - block_start);
        memcpy(buffer, buf.get(), n);
        total += n;
        len -= n;
        buffer += n;
        block_start = 0;
        block_pos = 0;
    }
    s.SetValue(total);
    co_return s;
}

seastar::future<Status<seastar::temporary_buffer<char>>> Disk::Read(
    ChunkPtr chunk_ptr, uint64_t pos, size_t len) {
    size_t clen = chunk_ptr->Len();
    if (pos >= clen) {
        co_return Status<seastar::temporary_buffer<char>>();
    }
    len = std::min(clen - pos, len);
    auto buffer =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, len);
    auto st = co_await Read(chunk_ptr, pos, buffer.get_write(), len);
    if (!st.OK()) {
        co_return Status<seastar::temporary_buffer<char>>(st.Code(),
                                                          st.Reason());
    }
    Status<seastar::temporary_buffer<char>> s(ErrCode::OK);
    buffer.trim(st.Value());
    s.SetValue(std::move(buffer));
    co_return s;
}

seastar::future<Status<void>> Disk::RemoveChunk(ChunkPtr chunk_ptr) {
    if (0 == chunks_.erase(chunk_ptr->chunk_id)) {
        co_return Status<void>(ErrCode::OK);
    }
    Chunk c(chunk_ptr->chunk_id, chunk_ptr->offset);
    auto b = c.Marshal();
    auto s = co_await Write(c.offset, b.get(), b.size());
    if (!s.OK()) {
        chunks_[chunk_ptr->chunk_id] = chunk_ptr;
        co_return Status<void>(s.Code(), s.Reason());
    }
    chunk_ptr->deleted = true;
    used_ -= chunk_ptr->blocks.size() * kBlockSize;
    co_return Status<void>(ErrCode::OK);
}

uint64_t Disk::Capacity() const { return super_block_.capacity; }

uint64_t Disk::Used() const { return used_; }

seastar::future<> Disk::Close() { return fp_.close(); }

}  // namespace stream
}  // namespace snail
