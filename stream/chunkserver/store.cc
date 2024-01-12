#include <isa-l.h>

#include <algorithm>
#include <iostream>
#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/defer.hh>
#include <system_error>

#include "disk.h"
#include "spdlog/spdlog.h"
#include "status.h"

namespace snail {
namespace stream {

const size_t kMagic = 0x69616e73;
const size_t kVersion = 1;
const size_t kChunkSize = 4 << 20;
const size_t kChunkDataSize = kChunkSize - 512;
const size_t kBlockSize = 32768;
const size_t kBlockDataSize = 32764;
const int kLastBlockIndex = 127;  // the last block index in the chunk
const int kLastSectorIndex = 63;  // the last sector index in the block

// include the last crc len
inline static uint32_t ChunkPhyLen(ChunkPtr chunk) {
    uint32_t len = chunk->len / kBlockDataSize * kBlockSize;
    uint32_t remain = chunk->len % kBlockDataSize;
    return len + (remain == 0 ? 0 : remain + 4);
}

ExtentPtr Store::GetExtent(const ExtentID& id) {
    auto it = extents_.find(id);
    if (it == extents_.end()) {
        return nullptr;
    }
    return it->second;
}

seastar::future<Status<>> Store::AllocChunk(ExtentPtr extent_ptr) {
    Status<> s;
    uint32_t chunk_idx = free_chunks_.front();
    free_chunks_.pop();
    if (extent_ptr->chunks.empty()) {
        ChunkEntry new_chunk(chunk_idx);
        ExtentEntry extent_entry;
        extent_entry.index = extent_ptr->index;
        extent_entry.id = extent_ptr->id;
        extent_entry.chunk_idx = extent_ptr->chunk_idx;
        auto f1 = log_ptr_->SaveChunk(new_chunk);
        auto f2 = log_ptr_->SaveExtent(extent_entry);
        Status<> s1 = std::get<0>(res);
        Status<> s2 = std::get<1>(res);
        if (!s1.OK() || !s2.OK()) {
            free_chunks_.push(chunk_idx);
            s = s1.OK() ? s2 : s1;
            co_return s;
        }
        extent_ptr->chunk_idx = new_chunk.index;
        extent_ptr->chunks.push_back(new_chunk);
    } else {
        ChunkEntry& chunk = extent_ptr->chunks.back();
        ChunkEntry new_chunk(chunk_idx);
        ChunkEntry old_chunk(chunk);
        old_chunk.next = new_chunk.index;
        auto f1 = log_ptr_->SaveChunk(new_chunk);
        auto f2 = log_ptr_->SaveChunk(old_chunk);
        auto res = co_await seastar::when_all(f1, f2);
        Status<> s1 = std::get<0>(res);
        Status<> s2 = std::get<1>(res);
        if (!s1.OK() || !s2.OK()) {
            free_chunks_.push(chunk_idx);
            s = s1.OK() ? s2 : s1;
            co_return s;
        }
        chunk.next = chunk_idx;
        extent_ptr->chunks.push_back(new_chunk);
    }
    co_return s;
}

// b include 4 bytes crc
seastar::future<Status<>> Store::WriteBlock(ExtentID id, uint64_t offset,
                                            char* b, size_t len) {
    Status<> s;
    if (len > kBlockSize || len <= 4 ||
        (reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask)) {
        s.Set(EINVAL);
        co_return s;
    }

    auto extent_ptr = GetExtent(id);
    if (!extent_ptr) {  // not found extent
        s.Set(ErrCode::ErrNoExtent);
        co_return s;
    }

    co_await extent_ptr->mu.lock();
    auto defer = seastar::defer([extent_ptr] { extent_ptr->mu.unlock(); });

    if (offset != extent_ptr->len) {
        s.Set(ErrCode::ErrOverWrite);
        co_return s;
    }

    ChunkEntry& chunk = extern_ptr->chunks.back();
    size_t chunk_free_size = kChunkSize - ChunkPhyLen(chunk);
    if (chunk_free_size == 0) {  // alloc a new chunk
        uint32_t chunk_idx = free_chunks_.front();
        free_chunks_.pop();
        ChunkEntry new_chunk(chunk_idx);
        ChunkEntry old_chunk(chunk);
        old_chunk.next = chunk_idx;
        auto f1 = log_ptr_->SaveChunk(new_chunk);
        auto f2 = log_ptr_->SaveChunk(old_chunk);
        auto res = co_await seastar::when_all(f1, f2);
        Status<> s1 = std::get<0>(res);
        Status<> s2 = std::get<1>(res);
        if (!s1.OK() || !s2.OK()) {
            free_chunks_.push(chunk_idx);
            s = s1.OK() ? s2 : s1;
            co_return s;
        }
        chunk.next = chunk_idx;
        extent_ptr->chunks.push_back(new_chunk);
        chunk_free_size = kChunkSize;
        chunk = extern_ptr->chunks.back();
    }

    if (chunk_free_size < len) {
        s.Set(ErrCode::ErrTooLarge);
        co_return s;
    }
    uint32_t last_block_crc = chunk.crc;
    int last_block_index = chunk.len / kBlockDataSize;
    uint64_t last_block_len = chunk.len % kBlockDataSize;
    if ((kBlockSize - last_block_len) < len) {  // can't over block size
        co_return Status<>(ErrCode::ErrTooLarge);
    }

    uint64_t phy_offset = super_block_.ChunkOffset() +
                          chunk.index * kChunkSize +
                          last_block_index * kBlockSize + last_block_len;

    ChunkEntry tmp_chunk(chunk);
    std::string last_sector;
    if (last_block_len == 0) {
        if ((len & kSectorSizeMask) == 0) {
            s = co_await dev_ptr_->Write(phy_offset, b, len);
        } else {
            auto tmp_buf = dev_ptr_->Get(seastar::align_up(len, kSectorSize));
            memcpy(tmp_buf.get_write(), b, len);
            s = co_await dev_ptr_->Write(phy_offset, tmp_buf.get(),
                                         tmp_buf.size());
        }
        tmp_chunk.crc = BigEndian::Uint32(b + len - 4, 4);
        tmp_chunk.len += len - 4;
        last_block_len = tmp_chunk.len % kBlockDataSize;
        if (last_block_len != 0) {
            last_sector =
                std::string_view(b + len - 4 - last_block_len, last_block_len);
        }
    } else {
        uint32_t* u32 = reinterpret_cast<uint32_t*>(b + len - 4);
        uint32_t crc = crc32_gzip_refl(last_block_crc, b, len - 4);
        *u32 = seastar : net::hton(crc);
        if ((len & kSectorSizeMask == 0) &&
            (phy_offset & kSectorSizeMask == 0)) {
            s = co_await dev_ptr_->Write(phy_offset, b, len);
        } else if (phy_offset & kSectorSizeMask == 0) {
            auto tmp_buf = dev_ptr_->Get(seastar::align_up(len, kSectorSize));
            memcpy(tmp_buf.get_write(), b, len);
            s = co_await dev_ptr_->Write(phy_offset, tmp_buf.get(),
                                         tmp_buf.size());
        } else {
            auto st = co_await GetLastSector(chunk.index);
            if (!st.OK()) {
                s.Set(st.Code(), st.Reason());
                co_return s;
            }
            std::string& last_sector = st.Value();
            if (last_sectore.size() != (phy_offset & kSectorSizeMask)) {
                s.Set(ErrCode::ErrUnExpect, "invalid last sector size");
                co_return s;
            }
            auto tmp_buf = dev_ptr_->Get(
                seastar::align_up(len + last_sector.size(), kSectorSize));
            memcpy(tmp_buf.get_write(), last_sector.get(), last_sector.size());
            memcpy(tmp_buf.get_write() + last_sector.size(), b, len);
            s = co_await dev_ptr_->Write(phy_offset - last_sector.size(),
                                         tmp_buf.get(), tmp_buf.size());
        }

        if (!s.OK()) {
            co_return s;
        }
        tmp_chunk.len += len - 4;
        tmp_chunk.crc = crc;
        s = co_await log_ptr_->SaveChunk(tmp_chunk);
        if (!s.OK()) {
            co_return s;
        }
        chunk.crc = tmp_chunk.crc;
        chunk.len = tmp_chunk.len;
    }

    co_return s;
}

static Status<> CheckIovec(const iovec& io, bool full) {
    Status<> s;
    if ((reinterpret_cast<uintptr_t>(io.iov_base) & kMemoryAlignmentMask) ||
        (io_vec[i].iov_len > kBlockSize) || (io_vec[i].iov_len <= 4)) {
        s.Set(EINVAL);
        return s;
    }

    if (full && io_vec[i].iov_len != kBlockSize) {
        s.Set(EINVAL);
    }
    return s;
}

seastar::future<Status<>> Store::Write(ExtentID id, uint64_t offset,
                                       std::vector<iovec> io_vec) {
    Status<> s;
    auto extent_ptr = GetExtent(id);
    if (!extent_ptr) {  // not found extent
        s.Set(ErrCode::ErrNoExtent);
        co_return s;
    }

    co_await extent_ptr->mu.lock();
    auto defer = seastar::defer([extent_ptr] { extent_ptr->mu.unlock(); });

    if (offset != extent_ptr->len) {
        s.Set(ErrCode::ErrOverWrite);
    }

    if (io_vec.empty()) {
        co_return s;
    }

    int n = io_vec.size();
    for (int i = 0; i < n; i++) {
        s = CheckIovec(io_vec[i], (i != 0 && i != n - 1));
        if (!s.OK()) {
            co_return s;
        }
    }

    if (extent_ptr->chunks.empty() || 0 == (kChunkSize - ChunkPhyLen(extent_ptr->chunks.back())) {
        s = co_await AllocChunk(extent_ptr);
        if (!s.OK()) {
            co_return s;
        }
    }

    ChunkEntry chunk = extent_ptr->chunks.back();
    size_t chunk_free_size = kChunkSize - ChunkPhyLen(chunk);

    uint32_t last_block_crc = chunk.crc;
    int last_block_index = chunk.len / kBlockDataSize;
    uint64_t last_block_len = chunk.len % kBlockDataSize;

    TmpBuffer sector_buf = dev_ptr->Get(kSectorSize);

    // 第一个io需要特殊处理
    if (last_block_len != 0) { // 最后一个block没有写满
        size_t last_sector_size = last_block_len & kSectorSize;
        int sector_index = last_block_len / kSectorSize;
        if (last_sector_size != 0) {
            // 最后一个sector需要减去crc的长度
            size_t crc_len = (sector_index == kLastSectorIndex ? 4 : 0);
            if (kSectorSize - last_sector_size - crc_len <
                io_vec[0].iov_len - 4) {
                s.Set(EINVAL);
                co_return s;
            }
            if (io_vec.size() > 1 && kSectorSize - last_sector_size - crc_len !=
                                         io_vec[0].iov_len - 4) {
                //
                s.Set(EINVAL);
                co_return s;
            }
            s = co_await GetLastSector(chunk.index);
            if (!s.OK()) {
                co_return s;
            }
            std::string& last_sector = s.Value();
            if (last_sector.size() != last_sector_size) {
                s.Set(ErrCode::ErrUnExpect,
                      "last sector cache size is invalid");
                co_return s;
            }
            memcpy(sector_buf.get_write(), last_sector.c_str(),
                   last_sector.size());
            memcpy(sector_buf.get_write() + last_sector.size(),
                   io_vec[0].iov_base, io_vec[0].iov_len - 4);
            last_block_crc = crc32_gzip_refl(last_block_crc, io_vec[0].iov_base,
                                             io_vec[0].iov_len - 4);
            chunk.crc = last_block_crc;
            chunk.len += io_vec[0].iov_len - 4;
            if (sector_index == kLastSectorIndex) {
                BigEndian::PutUint32(sector_buf.get_write() +
                                         last_sector.size() +
                                         io_vec[0].iov_len - 4,
                                     last_block_crc);
            }
            io_vec[0].iov_base = sector_buf.get();
            io_vec[0].iov_len = kSectorSize;
            offset -= last_sector.size();
        }
    }
}

}  // namespace stream
}  // namespace snail
