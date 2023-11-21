#include "wal.h"

namespace snail {
namespace stream {

static const int kHeaderSize = 4 + 8;  // crc + ver
static const int kBlockHeaderSize =
    4 + 8 + 2 + 2;  // crc + ver + data len + padding len
static const int kRecordHeaderSize = 2 + 1;  // len + type
static const int kMaxBlockSize = 32768;

int BlockEntry::MarshalTo(char *b) const {
    uint32_t *u32 = reinterpret_cast<uint32_t *>(b);
    *u32++ = seastar::net::hton(index);
    *u32++ = seastar::net::hton(next);
    *u32++ = seastar::net::hton(len);
    *u32++ = seastar::net::hton(crc);
    return BLOCK_ENTRY_SIZE;
}

int ChunkEntry::MarshalTo(char *b) const {
    uint32_t *u32 = reinterpret_cast<uint32_t *>(b);
    *u32++ = seastar::net::hton(index);

    uint64_t *u64 = reinterpret_cast<uint64_t *>(u32);
    auto id_pair = id.GetID();
    *u64++ = seastar::net::hton(id_pair.first);
    *u64++ = seastar::net::hton(id_pair.second);

    u32 = reinterpret_cast<uint32_t *>(u64);
    *u32 = seastar::net::hton(block_idx);
    return CHUNK_ENTRY_SIZE;
}

size_t Wal::worker_item::Size() {
    size_t size = 0;
    int n = entries.size();
    for (int i = 0; i < n; i++) {
        size += kRecordHeaderSize;
        if (entries[i].index() == 0) {
            size += CHUNK_ENTRY_SIZE;
        } else {
            size += BLOCK_ENTRY_SIZE;
        }
    }
    return size;
}

Wal::Wal(DevicePtr dev_ptr) {}

seastar::future<Status<>> Wal::Recover() {}

void Wal::ResetMem(bool with_header) {
    char *p = mem_.get_write();
    with_header_ = with_header;
    if (with_header) {
        uint64_t ver = seastar::net::hton(version_);
        memcpy(p + 4, &ver, sizeof(ver));
        uint32_t crc = crc32_gzip_refl(0, p + 4, sizeof(ver));
        crc = seastar::net::hton(crc);
        memcpy(p, &crc, sizeof(crc));
        memcpy(p + kHeaderSize + 4, &ver, sizeof(ver));
        real_mem_size_ = kHeaderSize + kBlockHeaderSize;
    } else {
        uint64_t ver = seastar::net::hton(version_);
        memcpy(p + 4, &ver, sizeof(ver));
        real_mem_size_ = kBlockHeaderSize;
    }
}

seastar::future<Status<>> Wal::BackgroundFlush() {
    immu_bmem_ = std::move(bmem_);
    immu_cmem_ = std::move(cmem_);
    uint32_t sector_size = dev_ptr_->SectorSize();

    std::vector<iovec> iov;
    std::vector<seastar::temporary_buffer<char>> buf_vec;
    uint32_t prev_index = std::numeric_limits<uint32_t>::max();
    uint32_t first_index = std::numeric_limits<uint32_t>::max();

    for (auto iter : immu_bmem_) {
        auto buf = seastar::temporary_buffer<char>::aligin(kMemoryAlignment,
                                                           sector_size);
        iter->second.MarshalTo(buf.get_write());
        uint32_t crc = crc32_gzip_refl(0, buf.get(), BLOCK_ENTRY_SIZE);
        uint32_t *u32 =
            reinterpret_cast<uint32_t *>(buf.get_write() + BLOCK_ENTRY_SIZE);
        *u32 = seastar::net::htno(crc);

        if (buf_vec.size() == 0) {
            iov.push_back({buf.get(), buf.size()});
            buf_vec.push_back(std::move(buf));
            first_index = iter->first;
            prev_index = first_index;
        } else {
            if (iter->second.index == prev_index + 1) {
                iov.push_back({buf.get(), buf.size()});
            } else {
                uint64_t off = block_meta_offset_ + first_index * sector_size;
                co_await dev_ptr_->Write(off, std::move(iov));
                iov.push_back({buf.get(), buf.size()});
                first_index = iter->first;
            }
            buf_vec.push_back(std::move(buf));
            prev_index = iter->first;
        }
    }

    for (auto iter : immu_cmem_) {
    }
}

seastar::future<Status<>> Wal::Flush() {
    char *p = mem_.get_write();
    char *block_header = p;
    auto n = seastar::align_up(real_mem_size_, sector_size);
    size_t padding_size = n - real_mem_size_;
    size_t data_size = real_mem_size_ - kBlockHeaderSize;
    if (with_header_) {
        data_size -= kHeaderSize;
        block_header += kHeaderSize;
    }

    uint16_t *u16 = reinterpret_cast<uint16_t *>(block_header + 12);
    *u16++ = seastar::net::hton((uint16_t)data_size);
    *u16 = seastar::net::hton((uint16_t)(padding_size));
    uint32_t crc = crc32_gzip_refl(0, block_header + kBlockHeaderSize - 12,
                                   12 + data_size);

    uint32_t *u32 = reinterpret_cast<uint32_t *>(block_header);
    *u32 = seastar::net::hton(crc);

    auto s = co_await dev_ptr_->Write(offset_, mem_.get(), n);
    if (!s.OK()) {
        ResetMem(with_header_);
        co_return s;
    }
    offset_ += n;
    if (offset_ >= pt_[cur_pt_id_].start + pt_[cur_pt_id_].end) {
        cur_pt_id_ = (cur_pt_id_ + 1) % 2;
        offset_ = pt_[cur_pt_id_].start;
        ResetMem(true);
        co_await BackgroundFlush();
    } else {
        ResetMem(false);
    }
    co_return s;
}

void Wal::AppendToMem(Wal::worker_item *item) {
    char *p = mem_.get_write() + real_mem_size_;

    int n = item->entries.size();
    for (int i = 0; i < n; i++) {
        uint16_t *u16 = reinterpret_cast<uint16_t *>(p);
        if (item->entries[i].index() == 0) {
            *u16 = seastar::net::hton(CHUNK_ENTRY_SIZE + 1);
            *(p + 2) = EntryType::Chunk;
            ChunkEntry &ent = std::get<0>(item->entries[i]);
            ent.MarshalTo(p + kRecordHeaderSize);
            real_mem_size_ += kRecordHeaderSize + CHUNK_ENTRY_SIZE;
            cmem_[ent.index] = ent;
        } else {
            *u16 = seastar::net::hton(BLOCK_ENTRY_SIZE + 1);
            *(p + 2) = EntryType::Block;
            BlockEntry &ent = std::get<1>(item->entries[i]);
            ent.MarshalTo(p + kRecordHeaderSize);
            real_mem_size_ += kRecordHeaderSize + BLOCK_ENTRY_SIZE;
            bmem_[ent.index] = ent;
        }
    }
}

seastar::future<> Wal::LoopRun() {
    uint32_t sector_size = dev_ptr_->SectorSize();
    while (!stop_) {
        std::queue<worker_item *> tmp_queue;
        while (!queue_.empty()) {
            auto item = queue_.front();
            uint64_t free_size =
                pt[cur_pt_id_].offset + pt[cur_pt_id_].size - offset_;
            size_t item_size = item->Size();
            size_t n =
                seastar::align_up(item_size + real_mem_size_, sector_size);
            if (n > mem_.size() || n > free_size) {
                auto s = co_await Flush();
                while (!tmp_queue.empty()) {
                    auto tmp_item = tmp_queue.front();
                    tmp_queue.pop();
                    tmp_item->pr.set_value(s);
                }
            }
            AppendToMem(item);
            queue_.pop();
            tmp_queue.push(item);
        }
        auto s = co_await Flush();
        while (!tmp_queue.empty()) {
            auto tmp_item = tmp_queue.front();
            tmp_queue.pop();
            tmp_item->pr.set_value(s);
        }
        co_await cv_.wait();
    }
    co_return;
}

seastar::future<Status<>> Wal::SaveBlocks(
    const std::vector<BlockEntry> &blocks) {
    std::string str;
    int n = blocks.size();
    size_t size = n * (BLOCK_ENTRY_SIZE + kRecordHeaderSize);
    str.resize(size);
    char *p = str.data();
    for (int i = 0; i < n; i++) {
        uint16_t *u16 = reinterpret_cast<uint16_t *>(p);
        *u16 = seastar::net::hton(1 + BLOCK_ENTRY_SIZE);
        *(p + 2) = EntryType::Block;
        blocks[i].MarshalTo(p + 3);
        p += BLOCK_ENTRY_SIZE + kRecordHeaderSize;
    }

    worker_item *item = new worker_item;
    iterm->v.emplace_back(std::move(str));
    queue_.push(item);
    auto s = co_await item->pr.get_future();
    delete item;
    co_return s;
}

seastar::future<Status<>> Wal::SaveChunk(const ChunkEntry &chunk) {
    std::string str;
    str.resize(kRecordHeaderSize + CHUNK_ENTRY_SIZE);
    char *p = str.data();
    uint16_t *u16 = reinterpret_cast<uint16_t *>(p);
    *u16 = seastar::net::hton(1 + CHUNK_ENTRY_SIZE);
    *(p + 2) = EntryType::Chunk;
    chunk.MarshalTo(p + 3);

    worker_item *item = new worker_item;
    iterm->v.emplace_back(std::move(str));
    queue_.push(item);
    auto s = co_await item->pr.get_future();
    delete item;
    co_return s;
}

seastar::future<Status<>> Wal::SaveChunkAndBlock(const ChunkEntry &chunk,
                                                 const BlockEntry &block) {
    std::string str;
    str.resize(2 * kRecordHeaderSize + CHUNK_ENTRY_SIZE + BLOCK_ENTRY_SIZE);
    char *p = str.data();
    uint16_t *u16 = reinterpret_cast<uint16_t *>(p);
    *u16 = seastar::net::hton(1 + CHUNK_ENTRY_SIZE);
    *(p + 2) = EntryType::Chunk;
    chunk.MarshalTo(p + 3);
    p += kRecordHeaderSize + CHUNK_ENTRY_SIZE;
    u16 = reinterpret_cast<uint16_t *>(p);
    *u16 = seastar::net::hton(1 + BLOCK_ENTRY_SIZE);
    *(p + 2) = EntryType::Block;
    block.MarshalTo(p + 3);

    worker_item *item = new worker_item;
    iterm->v.emplace_back(std::move(str));
    queue_.push(item);
    auto s = co_await item->pr.get_future();
    delete item;
    co_return s;
}

}  // namespace stream
}  // namespace snail
