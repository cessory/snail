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
const size_t kBlockSize = 32 << 10;
const size_t kBlockDataSize = 32764;
const size_t kMemoryAlignment = 4096;
const int kBlockCount = kChunkSize / kBlockSize;

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

// b include data crc
seastar::future<Status<>> Store::WriteOneBlock(ExtentID id, uint64_t offset,
                                               char* b, size_t len) {
    auto extent_ptr = GetExtent(id);
    if (!extent_ptr) {  // not found extent
        co_return Status<>(ErrCode::ErrNotFoundExtent);
    }
    if (len > kBlockSize || len <= 4) {
        co_return Status<>(ErrCode::ErrInvalidArgs);
    }

    co_await extent_ptr->mu.lock();
    auto defer = seastar::defer([extent_ptr] { extent_ptr->mu.unlock(); });

    if (offset != extent_ptr->len) {
        co_return Status<>(ErrCode::ErrOverWrite);
    }
    uint32_t sector_size = super_block_.sector_size;

    auto chunk = extern_ptr->chunks.back();
    size_t chunk_free_size = kChunkSize - ChunkPhyLen(chunk);
    if (chunk_free_size == 0) {
        // TODO alloc block
    }
    uint32_t last_block_crc = block.crc;
    int last_block_index = chunk->len / kBlockDataSize;
    uint64_t last_block_len = chunk->len % kBlockDataSize;
    if ((kBlockSize - last_block_len) < len) {
        co_return Status<>(ErrCode::ErrTooLarge);
    }

    uint64_t phy_offset = super_block_.ChunkOffset() +
                          chunk->index * kChunkSize +
                          last_block_index * kBlockSize + last_block_len;
    Status<> st;
    std::string last_sector;
    if (last_block_len == 0) {
        if ((reinterpret_cast<uintptr_t>(b) % kMemoryAlignment == 0) &&
            (len % sector_size == 0)) {
            st = co_await dev_ptr_->Write(phy_offset, b, len);
        } else {
            auto tmp_buf = seastar::temporary_buffer<char>::aligned(
                kMemoryAlignment, seastar::align_up(len, sector_size));
            memcpy(tmp_buf.get_write(), b, len);
            st = co_await dev_ptr_->Write(phy_offset, tmp_buf.get(),
                                          tmp_buf.size());
            auto last_sector_len = len % sector_size;
            if (last_sector_len != 0) {
                if (last_sector_len < 4) {
                    last_sector_len = sector_size + last_sector_len - 4;
                } else if (last_sector_len == 4) {
                    last_sector_len = 0;
                } else {
                    last_sector_len = last_sector_len - 4;
                }
            }

            if (last_sector_len != 0) {
                last_sector =
                    std::string(b + len - 4 - last_sector_len, last_sector_len);
            }
        }
    } else {
        uint32_t* u32 = reinterpret_cast<uint32_t*>(b + len - 4);
        uint32_t crc = crc32_gzip_refl(last_block_crc, b, len - 4);
        *u32 = seastar : net::hton(crc);
        if ((reinterpret_cast<uintptr_t>(b) % kMemoryAlignment == 0) &&
            (len % sector_size == 0) && (phy_offset % sector_size == 0)) {
            st = co_await dev_ptr_->Write(phy_offset, b, len);
        } else if (phy_offset % sector_size == 0) {
            auto tmp_buf = seastar::temporary_buffer<char>::aligned(
                kMemoryAlignment, seastar::align_up(len, sector_size));
            memcpy(tmp_buf.get_write(), b, len);
            st = co_await dev_ptr_->Write(phy_offset, tmp_buf.get(),
                                          tmp_buf.size());
        } else {
            auto last_sector = co_await GetLastSector(chunk->index);
            auto tmp_buf = seastar::temporary_buffer<char>::aligned(
                kMemoryAlignment,
                seastar::align_up(len + last_sector.size(), sector_size));
            memcpy(tmp_buf.get_write(), last_sector.get(), last_sector.size());
            memcpy(tmp_buf.get_write() + last_sector.size(), b, len);
            st = co_await dev_ptr_->Write(phy_offset - last_sector.size(),
                                          tmp_buf.get(), tmp_buf.size());
        }
    }

    co_return st;
}

}  // namespace stream
}  // namespace snail
