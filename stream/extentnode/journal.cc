#include "journal.h"

#include <isa-l.h>

#include <seastar/core/align.hh>
#include <seastar/util/defer.hh>

#include "net/byteorder.h"
#include "store.h"
#include "util/logger.h"
#include "util/util.h"

namespace snail {
namespace stream {

enum RecordType {
    Extent = 0,
    Chunk = 1,
};

static constexpr int kRecordHeadSize = 4 + 2 + 1;  // crc + len + type
static constexpr size_t kMinJournalSize = 16 << 20;

// journal format
// | crc(4B) |len(2B)|type(1B)|      data    |
// | crc(4B) |len(2B)|type(1B)|      data    |
// | crc(4B) |len(2B)|type(1B)|      data    |
//

Journal::Journal(DevicePtr dev_ptr, Store *store, size_t max_size)
    : dev_ptr_(dev_ptr),
      store_(store),
      chunk_sem_(seastar::align_up<size_t>(std::max(max_size, kMinJournalSize),
                                           kChunkSize) /
                 kChunkSize) {}

void Journal::worker_item::MarshalTo(char *b) {
    uint16_t len = kExtentEntrySize;
    RecordType type = RecordType::Extent;
    if (entry.index() == 1) {
        len = kChunkEntrySize;
        type = RecordType::Chunk;
    }

    net::BigEndian::PutUint16(b + 4, len);
    *(b + 6) = static_cast<char>(type);
    if (type == RecordType::Extent) {
        ExtentEntry &ent = std::get<0>(entry);
        ent.MarshalTo(b + kRecordHeadSize);
    } else {
        ChunkEntry &ent = std::get<1>(entry);
        ent.MarshalTo(b + kRecordHeadSize);
    }

    uint32_t crc =
        crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(b + 4),
                        kRecordHeadSize + len - 4);
    net::BigEndian::PutUint32(b, crc);
}

seastar::future<Status<>> Journal::Init(
    ExtentPtr extent,
    seastar::noncopyable_function<Status<>(const ExtentEntry &)> &&f1,
    seastar::noncopyable_function<Status<>(const ChunkEntry &)> &&f2) {
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    if (init_) {
        co_return s;
    }
    init_ = true;
    seastar::gate::holder holder(gate_);
    extent_ = extent->GetExtentEntry();
    std::vector<ChunkEntry> chunks = std::move(extent->chunks);

    auto buf = dev_ptr_->Get(kChunkSize);
    for (int i = 0; i < chunks.size(); ++i) {
        ChunkEntry chunk = chunks[i];
        archive_chunks_.push_back(chunk);
        size_t pos =
            store_->super_block_.DataOffset() + chunk.index * kChunkSize;
        auto st = co_await dev_ptr_->Read(pos, buf.get_write(), buf.size());
        if (!st) {
            LOG_ERROR("reload journal on device {} error: {}, pos={}",
                      dev_ptr_->Name(), st, pos);
            s.Set(st.Code(), st.Reason());
            co_return s;
        }
        std::map<uint32_t, ChunkEntry> immutable_chunks;
        std::map<uint32_t, ExtentEntry> immutable_extents;
        for (const char *p = buf.get(); p < buf.end();) {
            if (p + kRecordHeadSize > buf.end()) {
                break;
            }
            uint32_t crc = net::BigEndian::Uint32(p);
            uint16_t len = net::BigEndian::Uint16(p + 4);
            RecordType type = static_cast<RecordType>(*(p + 6));
            if (crc == 0 && len == 0 && type == RecordType::Extent) {
                // found eof
                break;
            }
            if (p + kRecordHeadSize + len > buf.end()) {
                LOG_ERROR("reload journal on device {} error: invalid record",
                          dev_ptr_->Name());
                s.Set(EINVAL);
                co_return s;
            }
            if (crc != crc32_gzip_refl(0, (const uint8_t *)(p + 4),
                                       kRecordHeadSize + len - 4)) {
                LOG_ERROR("reload journal on device {} error: invalid checksum",
                          dev_ptr_->Name());
                s.Set(EINVAL);
                co_return s;
            }
            if (type == RecordType::Extent) {
                ExtentEntry ent;
                ent.Unmarshal(p + kRecordHeadSize);
                immutable_extents[ent.index] = ent;
                f1(ent);
            } else if (type == RecordType::Chunk) {
                ChunkEntry ent;
                ent.Unmarshal(p + kRecordHeadSize);
                immutable_chunks[ent.index] = ent;
                f2(ent);
            } else {
                LOG_ERROR("reload journal on device {} error: invalid type",
                          dev_ptr_->Name());
                s.Set(EINVAL);
                co_return s;
            }
            p += kRecordHeadSize + len;
        }
        immutable_extents_.emplace(std::move(immutable_extents));
        immutable_chunks_.emplace(std::move(immutable_chunks));
    }
    s = co_await BackgroundFlush(true);
    if (!s) {
        co_return s;
    }
    chunk_sem_.consume(chunks.size());

    flush_fu_ = BackgroundFlush(false);
    loop_fu_ = LoopRun();
    co_return s;
}

seastar::future<Status<int>> Journal::SaveImmuChunks(
    std::queue<std::map<uint32_t, ChunkEntry>> immutable_chunks) {
    Status<int> s;
    int n = 0;

    while (!immutable_chunks.empty()) {
        std::map<uint32_t, ChunkEntry> &immutable_chunks_map =
            immutable_chunks.front();
        uint32_t prev_index = -1;
        uint32_t start_index = -1;
        auto buf = dev_ptr_->Get(kBlockSize);
        size_t buf_len = 0;
        for (auto iter : immutable_chunks_map) {
            if (start_index == -1) {  // this is the first chunk
                prev_index = start_index = iter.first;
                net::BigEndian::PutUint16(buf.get_write() + 4, kChunkEntrySize);
                iter.second.MarshalTo(buf.get_write() + 6);
                net::BigEndian::PutUint32(
                    buf.get_write(),
                    crc32_gzip_refl(
                        0,
                        reinterpret_cast<const unsigned char *>(buf.get() + 4),
                        kChunkEntrySize + 2));
                buf_len += kSectorSize;
                continue;
            }

            if (iter.first != prev_index + 1 ||
                buf_len + kSectorSize >= buf.size()) {
                uint64_t pos = store_->super_block_.ChunkMetaOffset() +
                               start_index * kSectorSize;
                auto st = co_await dev_ptr_->Write(pos, buf.get(), buf_len);
                if (!st) {
                    s.Set(st.Code(), st.Reason());
                    co_return s;
                }
                start_index = iter.first;
                buf_len = 0;
            }
            char *p = buf.get_write() + buf_len;
            net::BigEndian::PutUint16(p + 4, kChunkEntrySize);
            iter.second.MarshalTo(p + 6);
            net::BigEndian::PutUint32(
                p, crc32_gzip_refl(
                       0, reinterpret_cast<const unsigned char *>(p + 4),
                       kChunkEntrySize + 2));
            buf_len += kSectorSize;
            prev_index = iter.first;
        }
        if (buf_len) {
            uint64_t pos = store_->super_block_.ChunkMetaOffset() +
                           start_index * kSectorSize;
            auto st = co_await dev_ptr_->Write(pos, buf.get(), buf_len);
            if (!st) {
                s.Set(st.Code(), st.Reason());
                co_return s;
            }
        }
        immutable_chunks.pop();
        n++;
    }
    s.SetValue(n);
    co_return s;
}

seastar::future<Status<int>> Journal::SaveImmuExtents(
    std::queue<std::map<uint32_t, ExtentEntry>> immutable_extents) {
    Status<int> s;
    int n = 0;

    while (!immutable_extents.empty()) {
        std::map<uint32_t, ExtentEntry> &immutable_extents_map =
            immutable_extents.front();
        uint32_t prev_index = -1;
        uint32_t start_index = -1;
        auto buf = dev_ptr_->Get(kBlockSize);
        size_t buf_len = 0;
        for (auto iter : immutable_extents_map) {
            if (start_index == -1) {  // this is the first chunk
                prev_index = start_index = iter.first;
                net::BigEndian::PutUint16(buf.get_write() + 4,
                                          kExtentEntrySize);
                iter.second.MarshalTo(buf.get_write() + 6);
                net::BigEndian::PutUint32(
                    buf.get_write(),
                    crc32_gzip_refl(
                        0,
                        reinterpret_cast<const unsigned char *>(buf.get() + 4),
                        kExtentEntrySize + 2));
                buf_len += kSectorSize;
                continue;
            }

            if (iter.first != prev_index + 1 ||
                buf_len + kSectorSize >= buf.size()) {
                uint64_t pos = store_->super_block_.ExtentMetaOffset() +
                               start_index * kSectorSize;
                auto st = co_await dev_ptr_->Write(pos, buf.get(), buf_len);
                if (!st) {
                    s.Set(st.Code(), st.Reason());
                    co_return s;
                }
                start_index = iter.first;
                buf_len = 0;
            }
            char *p = buf.get_write() + buf_len;
            net::BigEndian::PutUint16(p + 4, kExtentEntrySize);
            iter.second.MarshalTo(p + 6);
            net::BigEndian::PutUint32(
                p, crc32_gzip_refl(
                       0, reinterpret_cast<const unsigned char *>(p + 4),
                       kExtentEntrySize + 2));
            buf_len += kSectorSize;
            prev_index = iter.first;
        }
        if (buf_len) {
            uint64_t pos = store_->super_block_.ExtentMetaOffset() +
                           start_index * kSectorSize;
            auto st = co_await dev_ptr_->Write(pos, buf.get(), buf_len);
            if (!st) {
                s.Set(st.Code(), st.Reason());
                co_return s;
            }
        }
        immutable_extents.pop();
        n++;
    }
    s.SetValue(n);
    co_return s;
}

seastar::future<Status<>> Journal::BackgroundFlush(bool once) {
    Status<> s;

    do {
        if (gate_.is_closed()) {
            break;
        }
        if (!once && immutable_chunks_.empty() && immutable_extents_.empty()) {
            co_await flush_cv_.wait();
            continue;
        }
        std::queue<std::map<uint32_t, ChunkEntry>> immutable_chunks;
        std::queue<std::map<uint32_t, ExtentEntry>> immutable_extents;
        immutable_chunks.swap(immutable_chunks_);
        immutable_extents.swap(immutable_extents_);

        auto startTime = std::chrono::system_clock::now();
        auto st1 = co_await SaveImmuChunks(std::move(immutable_chunks));
        if (!st1) {
            LOG_ERROR("save chunk meta on device {} error: {}",
                      dev_ptr_->Name(), st1);
            status_.Set(EIO);
            s.Set(st1.Code(), st1.Reason());
            break;
        }
        auto st2 = co_await SaveImmuExtents(std::move(immutable_extents));
        if (!st2) {
            LOG_ERROR("save extent meta on device {} error: {}",
                      dev_ptr_->Name(), st2);
            status_.Set(EIO);
            s.Set(st2.Code(), st2.Reason());
            break;
        }

        int n = std::max(st1.Value(), st2.Value());
        s = co_await ReleaseChunk(n);
        if (!s) {
            LOG_ERROR("release journal chunks on device {} error: {}",
                      dev_ptr_->Name(), s);
            status_.Set(EIO);
            break;
        }

        auto endTime = std::chrono::system_clock::now();
        auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(
            endTime - startTime);
        LOG_INFO(
            "background flush journal on device {} success "
            "cost={}",
            dev_ptr_->Name(), cost.count());
    } while (!once);

    co_return s;
}

void Journal::UpdateMem(const std::variant<ExtentEntry, ChunkEntry> &entry) {
    if (entry.index() == 0) {
        const ExtentEntry &ent = std::get<0>(entry);
        mutable_extents_[ent.index] = ent;
    } else {
        const ChunkEntry &ent = std::get<1>(entry);
        mutable_chunks_[ent.index] = ent;
    }
}

seastar::future<> Journal::LoopRun() {
    Buffer buf = dev_ptr_->Get(kBlockSize);
    size_t buf_len = 0;
    size_t chunk_len = 0;
    std::string last_sector;

    while (!gate_.is_closed()) {
        std::queue<worker_item *> tmp_queue;
        std::queue<worker_item *> ack_queue;
        if (queue_.empty()) {
            co_await cv_.wait();
            continue;
        }
        if (!last_sector.empty()) {
            assert(buf_len == 0);
            memcpy(buf.get_write(), last_sector.data(), last_sector.size());
            buf_len += last_sector.size();
        }

        tmp_queue.swap(queue_);
        while (!tmp_queue.empty()) {
            auto item = tmp_queue.front();
            if ((chunk_len + buf_len + item->Size() + 2 * kRecordHeadSize >
                 kChunkSize) ||
                (buf_len + item->Size() + 2 * kRecordHeadSize > buf.size())) {
                bool need_archive =
                    (chunk_len + buf_len + item->Size() + 2 * kRecordHeadSize >
                     kChunkSize);
                // add eof to buf
                memset(buf.get_write() + buf_len, 0, kRecordHeadSize);
                size_t n =
                    seastar::align_up(buf_len + kRecordHeadSize, kSectorSize);

                auto s = co_await Append(chunk_len, buf.get(), n);
                while (!ack_queue.empty()) {
                    auto ack_item = ack_queue.front();
                    ack_queue.pop();
                    Status<> st = s;
                    if (st) {
                        UpdateMem(item->entry);
                    }
                    ack_item->pr.set_value(std::move(st));
                }
                if (!s) {
                    LOG_ERROR("write journal to device {} error: {}",
                              dev_ptr_->Name(), s);
                    buf_len = last_sector.size();
                    continue;
                }
                if (need_archive) {
                    archive_chunks_.push_back(*chunk_);
                    chunk_.reset();
                    immutable_chunks_.push(std::move(mutable_chunks_));
                    immutable_extents_.push(std::move(mutable_extents_));
                    flush_cv_.signal();
                }
                last_sector.clear();
                size_t align_buf_len =
                    seastar::align_down(buf_len, kSectorSize);
                size_t remain_size = buf_len - align_buf_len;
                if (remain_size) {
                    last_sector =
                        std::string(buf.get() + align_buf_len, remain_size);
                }
                if (need_archive) {
                    chunk_len = 0;
                } else {
                    chunk_len += align_buf_len;
                }
                buf_len = 0;
            }
            tmp_queue.pop();
            ack_queue.push(item);
            item->MarshalTo(buf.get_write() + buf_len);
            buf_len += item->Size() + kRecordHeadSize;
            // add eof
            memset(buf.get_write() + buf_len, 0, kRecordHeadSize);
        }

        if (ack_queue.empty()) {
            buf_len = 0;
            continue;
        }

        size_t n = seastar::align_up(buf_len + kRecordHeadSize, kSectorSize);
        auto s = co_await Append(chunk_len, buf.get(), n);
        while (!ack_queue.empty()) {
            auto ack_item = ack_queue.front();
            ack_queue.pop();
            Status<> st = s;
            if (st) {
                UpdateMem(ack_item->entry);
            }
            ack_item->pr.set_value(std::move(st));
        }
        if (!s) {
            LOG_ERROR("write journal to device {} error: {}", dev_ptr_->Name(),
                      s);
            buf_len = 0;
            continue;
        }
        last_sector.clear();
        size_t align_buf_len = seastar::align_down(buf_len, kSectorSize);
        size_t remain_size = buf_len - align_buf_len;
        if (remain_size) {
            last_sector = std::string(buf.get() + align_buf_len, remain_size);
        }
        chunk_len += align_buf_len;
        buf_len = 0;
    }
    co_return;
}

seastar::future<Status<>> Journal::Append(size_t off, const char *p, size_t n) {
    Status<> s;
    if (!status_) {
        s = status_;
        co_return s;
    }
    if (!chunk_) {
        s = co_await AllocaChunk();
        if (!s) {
            LOG_ERROR("alloc journal chunk on device {} error: {}",
                      dev_ptr_->Name(), s);
            co_return s;
        }
    }
    size_t pos =
        store_->super_block_.DataOffset() + (*chunk_).index * kChunkSize + off;
    LOG_INFO("begin writing journal in chunk-{} off={} size={} device={}",
             (*chunk_).index, off, n, dev_ptr_->Name());
    s = co_await dev_ptr_->Write(pos, p, n);
    co_return s;
}

seastar::future<Status<>> Journal::AllocaChunk() {
    Status<> s;
    if (chunk_) {
        s.Set(EALREADY);
        co_return s;
    }
    try {
        co_await chunk_sem_.wait();
    } catch (std::exception &e) {
        s.Set(ErrCode::ErrInternal);
        co_return s;
    }

    if (store_->free_chunks_.empty()) {
        s.Set(ENOSPC);
        chunk_sem_.signal();
        co_return s;
    }

    uint32_t index = store_->free_chunks_.front();
    store_->free_chunks_.pop();
    ChunkEntry chunk(index);

    co_await extent_mu_.lock();
    auto defer = seastar::defer([this] { extent_mu_.unlock(); });
    if (archive_chunks_.empty()) {
        ExtentEntry ext = extent_;
        ext.chunk_idx = index;
        s = co_await store_->SaveExtentMeta(ext);
        if (!s) {
            chunk_sem_.signal();
            store_->free_chunks_.push(index);
            co_return s;
        }
        extent_.chunk_idx = index;
        chunk_ = chunk;
        store_->used_ += kChunkSize;
        co_return s;
    }

    ChunkEntry last_chunk = archive_chunks_.back();
    last_chunk.next = index;
    s = co_await store_->SaveChunkMeta(last_chunk);
    if (!s) {
        chunk_sem_.signal();
        store_->free_chunks_.push(index);
        co_return s;
    }
    archive_chunks_.back().next = index;
    chunk_ = chunk;
    store_->used_ += kChunkSize;
    co_return s;
}

seastar::future<Status<>> Journal::ReleaseChunk(uint32_t n) {
    Status<> s;
    if (n == 0) {
        co_return s;
    }
    co_await extent_mu_.lock();
    auto defer = seastar::defer([this] { extent_mu_.unlock(); });
    if (archive_chunks_.size() < n) {
        s.Set(ERANGE);
        co_return s;
    }

    ExtentEntry extent = extent_;
    extent.chunk_idx = archive_chunks_[n].index;
    s = co_await store_->SaveExtentMeta(extent);
    if (!s) {
        co_return s;
    }
    extent_.chunk_idx = extent.chunk_idx;

    std::vector<ChunkEntry> tmp_archive_chunks;
    for (uint32_t i = n; i < archive_chunks_.size(); i++) {
        tmp_archive_chunks.push_back(archive_chunks_[i]);
    }
    archive_chunks_ = std::move(tmp_archive_chunks);
    chunk_sem_.signal(n);
    store_->used_ -= n * kChunkSize;
    co_return s;
}

seastar::future<Status<>> Journal::SaveChunk(ChunkEntry chunk) {
    Status<> s;
    if (!init_) {
        s.Set(ENOTSUP);
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s = status_;
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    std::unique_ptr<worker_item> ptr(new worker_item);
    ptr->entry = std::move(std::variant<ExtentEntry, ChunkEntry>(chunk));
    queue_.push(ptr.get());
    cv_.signal();
    s = co_await ptr->pr.get_future();
    LOG_DEBUG(
        "save chunk (index={}, next={}, len={}, crc={}, scrc={}) success!",
        chunk.index, chunk.next, chunk.len, chunk.crc, chunk.scrc);
    co_return s;
}

seastar::future<Status<>> Journal::SaveExtent(ExtentEntry extent) {
    Status<> s;
    if (!init_) {
        s.Set(ENOTSUP);
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s = status_;
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    std::unique_ptr<worker_item> ptr(new worker_item);
    ptr->entry = std::move(std::variant<ExtentEntry, ChunkEntry>(extent));
    queue_.push(ptr.get());
    cv_.signal();
    s = co_await ptr->pr.get_future();
    LOG_DEBUG("save extent (index={}, id={}, chunk_idx={}) success!",
              extent.index, extent.id, extent.chunk_idx);
    co_return s;
}

seastar::future<> Journal::Close() {
    if (!gate_.is_closed()) {
        cv_.signal();
        flush_cv_.signal();
        co_await gate_.close();
        if (loop_fu_) {
            co_await std::move(loop_fu_.value());
            loop_fu_.reset();
        }
        if (flush_fu_) {
            co_await std::move(flush_fu_.value());
            flush_fu_.reset();
        }
    }
    co_return;
}

}  // namespace stream
}  // namespace snail
