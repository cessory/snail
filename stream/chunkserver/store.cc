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

Status<size_t> Store::HandleFirst(ChunkEntry& chunk, int io_n, char* b,
                                  size_t len,
                                  std::vector<TmpBuffer>& tmp_buf_vec,
                                  std::vector<iovec>& tmp_io_vec,
                                  uint64_t& offset,
                                  std::string& last_sector_data) {
    Status<size_t> s;
    uint32_t last_block_crc = chunk.crc;
    int last_block_index = chunk.len / kBlockDataSize;
    uint64_t last_block_len = chunk.len % kBlockDataSize;

    if (kBlockSize - last_block_len < len) {
        // io的数据长度不能跨block
        s.Set(EINVAL);
        return s;
    }

    size_t last_sector_size = last_block_len & kSectorSizeMask;
    if (last_sector_size != 0) {
        // 非sector对齐写, 需要先拷贝原来的数据
        bool block_full =
            (((chunk.len + len - 4) % kBlockDataSize) == 0 ? true : false);
        size_t crc_len = (block_full ? 4 : 0);
        if (kSectorSize - last_sector_size - crc_len < len - 4) {
            // 第一个io的长度不能超过sector大小
            s.Set(EINVAL);
            return s;
        }
        if (io_n > 1 && kSectorSize - last_sector_size - crc_len != len - 4) {
            // 如果不止一个io, 第一个io必须把sector填满
            s.Set(EINVAL);
            return s;
        }
        if (last_sector_data.size() != last_sector_size) {
            s.Set(ErrCode::ErrUnExpect, "last sector data size is invalid");
            return s;
        }
        auto tmp = dev_ptr_->Get(kSectorSize);
        memcpy(tmp.get_write(), last_sector_data.c_str(),
               last_sector_data.size());
        memcpy(tmp.get_write() + last_sector_data.size(), b, len - 4);
        last_block_crc = crc32_gzip_refl(last_block_crc, b, len - 4);
        chunk.crc = last_block_crc;
        chunk.len += len - 4;
        offset -= last_sector_data.size();

        if (block_full) {
            s.SetValue(len);  // block已写满
            BigEndian::PutUint32(
                tmp.get_write() + last_sector_data.size() + len - 4,
                last_block_crc);
            last_sector_data.clear();
        } else {
            s.SetValue(len - 4);
            size_t sector_remain =
                (last_sector_data.size() + len - 4) & kSectorSize;
            if (sector_remain > 0) {
                last_sector_data =
                    std::string_view(tmp.get() + last_sector_data.size() + len -
                                         4 - sector_remain,
                                     sector_remain);
            } else {
                last_sector_data.clear();
            }
        }

        tmp_io_vec.push_back({tmp.get_write(), tmp.size()});
        tmp_buf_vec.emplace_back(std::move(tmp));
        return s;
    }

    // 写入的位置是sector对齐的
    if (last_sector_data.size() != 0) {
        s.Set(ErrCode::ErrUnExpect, "last sector data is not empty");
        return s;
    }
    if (io_n > 1 && kBlockSize - last_block_len != len) {
        // 如果有多个io, 第一个io必须能够把block填满
        s.Set(EINVAL);
        return s;
    }
    last_block_crc = crc32_gzip_refl(last_block_crc, b, len - 4);
    // 修改crc
    BigEndian::PutUint32(b + len - 4, last_block_crc);
    if (len & kSectorSizeMask) {
        // 数据没有按512对齐, 这是最后一个io
        size_t aligned_size = len / kSectorSize * kSectorSize;
        size_t remain_size = len & kSectorSizeMask;
        if (aligned_size > 0) {
            tmp_io_vec.push_back({b, aligned_size});
        }
        auto tmp = dev_ptr_->Get(kSectorSize);
        memcpy(tmp.get_write(), b + aligned_size, remain_size);
        tmp_io_vec.push_back({tmp.get_write(), tmp.size()});
        tmp_buf_vec.emplace_back(tmp);
    } else {
        tmp_io_vec.push_back({b, len});
    }
    chunk.len += len - 4;
    chunk.crc = last_block_crc;
    if ((chunk.len % kBlockDataSize) == 0) {
        s.SetValue(len);  // block已写满
    } else {
        s.SetValue(len - 4);
        size_t sector_remain = (len - 4) & kSectorSizeMask;
        if (sector_remain > 0) {
            last_sector_data =
                std::string_view(b + len - 4 - sector_remain, sector_remain);
        }
    }
    return s;
}

Status<size_t> Store::HandleIO(ChunkEntry& chunk, int io_n, char* b, size_t len,
                               std::vector<TmpBuffer>& tmp_buf_vec,
                               std::vector<iovec>& tmp_io_vec, int i) {
    Status<size_t> s;
    uint32_t last_block_crc = chunk.crc;
    int last_block_index = chunk.len / kBlockDataSize;
    uint64_t last_block_len = chunk.len % kBlockDataSize;
    uint64_t last_block_free = kBlockSize - last_block_len;

    if (kBlockSize - last_block_len < len) {
        // io的数据长度不能跨block
        s.Set(EINVAL);
        return s;
    }

    size_t last_sector_size = last_block_len & kSectorSizeMask;
    if (last_sector_size != 0) {
        s.Set(ErrCode::ErrUnExpect, "chunk offset is not aligned");
        return s;
    }

    bool second = (i == 1);
    bool last = (i == io_n - 1);

    if (!last) {
        if (!second && last_block_len != 0) {
            // 不是第二个io 也不是最后一个io, block必须为空
            s.Set(ErrCode::ErrUnExpect, "the last block is not empty");
            return s;
        }
        if ((len & kSectorSizeMask) || last_block_free != len) {
            // 不是最后一个io, 数据长度必须对齐sector, 且必须填满block
            s.Set(EINVAL);
            return s;
        }
        if (last_block_len > 0) {
            last_block_crc = crc32_gzip_refl(last_block_crc, b, len - 4);
            BigEndian::PutUint32(b + len - 4, last_block_crc);
        } else {
            last_block_crc = BigEndian::Uint32(b + len - 4);
        }
        tmp_io_vec.push_back({b, len});
        chunk.len += len - 4;
        chunk.crc = last_block_crc;
        if ((chunk.len % kBlockDataSize) == 0) {
            s.SetValue(len);
        } else {
            s.SetValue(len - 4);
        }
        return s;
    }

    // 最后一个io 可能sector不对齐
    if (!second && last_block_len != 0) {
        // 如果最后一个io不是第二个io, block必须为空
        s.Set(ErrCode::ErrUnExpect, "the last block is not empty");
        return s;
    }
    if (last_block_len == 0) {
        last_block_crc = BigEndian::Uint32(b + len - 4);
    } else {
        last_block_crc = crc32_gzip_refl(last_block_crc, b, len - 4);
        BigEndian::PutUint32(b + len - 4, last_block_crc);
    }

    if (len == kBlockSize || !(len & kSectorSizeMask)) {
        tmp_io_vec.push_back({b, len});
    } else {
        size_t new_len = len / kSectorSize * kSectorSize;
        size_t remain = len & kSectorSizeMask;
        if (new_len) {
            tmp_io_vec.push_back({b, new_len});
        }
        auto tmp = dev_ptr_->Get(kSectorSize);
        memcpy(tmp.get_write(), b + new_len, remain);
        tmp_io_vec.push_back({tmp.get_write(), tmp.size()});
        tmp_buf_vec.emplace_back(std::move(tmp));
    }
    chunk.len += len - 4;
    chunk.crc = last_block_crc;
    if ((chunk.len % kBlockDataSize) == 0) {
        s.SetValue(len);
    } else {
        s.SetValue(len - 4);
    }
    return s;
}

// iovec include crc
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

    auto st = co_await GetLastSectorData(extent_ptr->index);
    if (!st.OK()) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    std::string last_sector_data = st.Value();

    ChunkEntry chunk = extent_ptr->chunks.back();
    std::vector<TmpBuffer> tmp_buf_vec;
    std::vector<iovec> tmp_io_vec;
    std::vector<ChunkEntry> chunks;

    chunks.push_back(chunk);
    for (int i=0; i<n; i++) {
        Status<size_t> st;
        if (i == 0) {
            st = HandleFirst(chunk, n, io_vec[i].iov_base, io_vec[i].iov_len,
                             tmp_buf_vec, tmp_io_vec, offset, last_sector_data);
        } else {
            st = HandleIO(chunk, n, io_vec[i].iov_base, io_vec[i].iov_len,
                          tmp_buf_vec, tmp_io_vec, i);
        }
        if (!st.OK()) {
            s.Set(st.Code(), st.Reason());
            co_return s;
        }

        if ((kChunkSize - ChunkPhyLen(chunk)) == 0) {
            s = co_await dev_ptr_->Write(offset, std::move(tmp_io_vec));
            if (!s.OK()) {
                co_return s;
            }
            if (i != n - 1) {
                if (free_chunks_.empty()) {
                    s.Set(ENOSPC);
                    co_return s;
                }

                new_chunk = ChunkEntry(free_chunks_.front());
                free_chunks_.pop();
                chunk.next = new_chunk.index;
                chunks.back() = chunk;
                chunks.push_back(new_chunk);
                chunk = new_chunk;
                offset = super_block_.DataOffset() * chunk.index;
            }
            tmp_buf_vec.clear();
            tmp_io_vec.clear();
        }
    }

    // 持久化元数据
    std::vector<seastar::future<Status<>>> fu_vec;
    for (int i = n-1; i>= 0 ; i--) {
        auto fu = log_ptr_->SaveChunk(chunks[i]);
        fu_vec.emplace_back(std::move(fu));
    }
    auto res = co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    for(auto& r : res) {
        if (!r.OK()) {
            s.Set(r.Code(), r.Reason());
            break;
        }
    }

    if (!s.OK()) {
        for (int i = 1; i < n; i++) {
            free_chunks_.push(chunks[i].index);
        }
    }else{
        extent_ptr->len += chunks[0].len - extent_ptr->chunks.back().len;
        extent_ptr->chunks.back() = chunks[0];
        for (int i = 1; i < n; i++) {
            extent_ptr->chunks.push_back(chunks[i]);
            extent_ptr->len += chunks[i].len;
        }
    }

    co_return s;
}

}  // namespace stream
}  // namespace snail
