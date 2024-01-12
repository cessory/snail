#include "log.h"

#include "net/byteorder.h"

using snail::net;

namespace snail {
namespace stream {

static constexpr int kHeaderSize = 4 + 8 + 1;  // crc + ver + flag
static constexpr int kRecordHeaderSize =
    4 + 8 + 2 + 1;  // crc + ver + len + type
static constexpr int kMaxMemSize = 8192;

// log format
// |   header    |             data                         |
// | header(512B) |    block(512B)  |   ....  |   block   |
//
// there are one or more records in the block
// record
// | crc(4B) |  version(8B)  |len(2B)|type(1B)|      data    |
//

static void LogHeaderMarshalTo(uint64_t ver, uint8_t flag, char *b) {
    BigEndian::PutUint64(b + 4, ver);
    *(b + 12) = flag;
    uint32_t crc = crc32_gzip_refl(0, b + 4, kHeaderSize - 4);
    BigEndian::PutUint32(b, crc);
}

Log::Log(DevicePtr dev_ptr, const SuperBlock &super_block)
    : dev_ptr_(dev_ptr), super_(super_block) {
    offset_ = super_.pt[LOGA_PT].start;
}

void Log::worker_item::MarshalTo(uint64_t ver, char *b) {
    uint16_t len = kExtentEntrySize;
    EntryType type = EntryType::Extent;
    if (entry.index() == 1) {
        len = kChunkEntrySize;
        type = EntryType::Chunk;
    }

    BigEndian::PutUint64(b + 4, ver);
    BigEndian::PutUint16(b + 12, len);
    *(b + 14) = static_cast<char>(type);
    if (type == EntryType::Extent) {
        ExtentEntry &ent = std::get<0>(entry);
        ent.MarshalTo(b + kRecordHeaderSize);
    } else {
        ChunkEntry &ent = std::get<1>(entry);
        ent.MarshalTo(b + kRecordHeaderSize);
    }

    uint32_t crc = crc32_gzip_refl(0, b + 4, kRecordHeaderSize + len - 4);
    BigEndian::PutUint32(b, crc);
}

seastar::future<Status<>> Log::Init(
    seastar::noncopyable_function<void(const ExtentEntry &)> &&f1,
    seastar::noncopyable_function<void(const ChunkEntry &)> &&f2) {
    Status<> s;
    if (init_) {
        s.Set(EALREADY);
        co_return s;
    }
    init_ = true;
    auto buf =
        seastar::temporary_buffer<char>::aligned(kMemoryAlignment, kMaxMemSize);
    uint32_t sector_size = super_.sector_size;

    uint64_t ver[2];
    int index[2] = {0, 1};
    uint8_t flags[2] = {0, 0};
    for (int i = 0; i < 2; i++) {
        s = dev_ptr_->Read(super_.pt[LOGA_PT + i].start, buf.get_write(),
                           sector_size);
        if (!s.OK()) {
            co_return s;
        }
        uint32_t crc = BigEndian::Uint32(buf.get());
        ver[i] = BigEndian::Uint64(buf.get() + 4);
        flags[i] = *(buf.get() + 12);
        if (crc == 0 && ver[i] == 0) {
            continue;
        }
        if (crc32_gzip_refl(0, buf.get() + 4, kHeaderSize - 4) != crc) {
            co_return Status<>(ErrCode::ErrInvalidChecksum);
        }
    }
    if (ver[0] > ver[1]) {
        index[0] = 1;
        index[1] = 0;
    }
    for (int i = 0; i < 2; i++) {
        int pt_n = LOGA_PT + index[i];
        uint64_t offset = super_.pt[pt_n].start;
        version_ = ver[index[i]];

        if (version_ == 0 || flags[index[i]] != 0) {
            continue;
        }

        while (offset < super_.pt[pt_n].start + super_.pt[pt_n].size) {
            auto s =
                co_await dev_ptr_->Read(offset, buf.get_write(), buf.size());
            if (!s.OK()) {
                co_return s;
            }
            const char *p = buf.get();
            if (offset == super_.pt[pt_n].start) {
                p += sector_size;
            }

            bool break_loop = false;
            for (; p < buf.get() + buf.size() && !break_loop;
                 p += sector_size) {
                const char *start = p;
                const char *end = p + sector_size;
                while (start < end && !break_loop) {
                    if (start + kRecordHeaderSize > end) {
                        break;
                    }
                    uint32_t crc = BigEndian::Uint32(start);
                    uint64_t version = BigEndian::Uint64(start + 4);
                    uint16_t len = BigEndian::Uint16(start + 12);
                    EntryType type = static_cast<EntryType>(*(start + 14));
                    if (crc == 0 && version == 0 && len == 0) {
                        if (start == p) {
                            // this sector is empty, so skip all
                            // back sector
                            break_loop = true;
                        }
                        break;
                    }
                    if (start + kRecordHeaderSize + len >
                        end) {  // invalid record
                        s.Set(EINVAL);
                        co_return s;
                    }
                    if (crc32_gzip_refl(0, p + 4,
                                        kRecordHeaderSize + len - 4) !=
                        crc) {  // invalid record
                        s.Set(ErrCode::ErrInvalidChecksum);
                        co_return s;
                    }
                    if (version != version_) {
                        break_loop = true;
                        break;
                    }

                    switch (type) {
                        case EntryType::Chunk: {
                            ChunkEntry ent;
                            ent.Unmarshal(start + kRecordHeaderSize);
                            immu_chunk_mem_[ent.index] = ent;
                            f2(ent);
                            break;
                        }
                        case EntryType::Extent: {
                            ExtentEntry ent;
                            ent.Unmarshal(start + kRecordHeaderSize);
                            immu_extent_mem_[ent.index] = ent;
                            f1(ent);
                            break;
                        }
                        default:
                            s.Set(EINVAL);
                            co_return s;
                    }
                }
            }
            if (break_loop) {
                break;
            }
            offset += buf.size();
        }
        s = co_await BackgroundFlush(version_, super_.pt[pt_n].start);
        if (!s.OK()) {
            co_return s;
        }
    }
    version_++;
    offset_ = super_[index[0] + LOGA_PT].start;
    loop_fu_ = LoopRun();
    co_return s;
}

seastar::future<Status<>> Log::SaveImmuChunks() {
    std::vector<iovec> iov;
    std::vector<seastar::temporary_buffer<char>> buf_vec;
    uint32_t prev_index = std::numeric_limits<uint32_t>::max();
    uint32_t first_index = std::numeric_limits<uint32_t>::max();
    uint32_t sector_size = super_.sector_size;
    Status<> s;
    static size_t parallel_submit = 16;
    seastar::semaphore sem(parallel_submit);

    for (auto iter : immu_chunk_mem_) {
        auto buf = seastar::temporary_buffer<char>::aligin(kMemoryAlignment,
                                                           sector_size);
        BigEndian::PutUint16(buf.get_write() + 4, kChunkEntrySize);
        iter->second.MarshalTo(buf.get_write() + 6);
        BigEndian::PutUint32(
            buf.get_write(),
            crc32_gzip_refl(0, buf.get() + 4, kChunkEntrySize + 2));

        if (buf_vec.size() == 0) {
            iov.emplace_back({buf.get(), buf.size()});
            buf_vec.emplace_back(std::move(buf));
            first_index = iter->first;
            prev_index = first_index;
        } else {
            if (iter->second.index == prev_index + 1) {
                iov.emplace_back({buf.get(), buf.size()});
            } else {
                uint64_t off =
                    super_.pt[CHUNK_PT].start + first_index * sector_size;
                co_await sem.wait();
                (void)dev_ptr_->Write(off, std::move(iov))
                    .then([&sem, &s](Status<> &st) {
                        if (!st.OK()) {
                            s = st;
                        }
                        sem.signal();
                    });
                iov.emplace_back({buf.get(), buf.size()});
                first_index = iter->first;
            }
            buf_vec.emplace_back(std::move(buf));
            prev_index = iter->first;
        }
    }
    co_await sem.wait(parallel_submit);
    if (!s.OK()) {
        co_return s;
    }

    if (!iov.empty()) {
        uint64_t off = super_.pt[CHUNK_PT].start + first_index * sector_size;
        s = co_await dev_ptr_->Write(off, std::move(iov));
    }
    co_return s;
}

seastar::future<Status<>> Log::SaveImmuExtents() {
    std::vector<iovec> iov;
    std::vector<seastar::temporary_buffer<char>> buf_vec;
    uint32_t prev_index = std::numeric_limits<uint32_t>::max();
    uint32_t first_index = std::numeric_limits<uint32_t>::max();
    uint32_t sector_size = super_.sector_size;
    Status<> s;
    static size_t parallel_submit = 16;
    seastar::semaphore sem(parallel_submit);

    for (auto iter : immu_extent_mem_) {
        auto buf = seastar::temporary_buffer<char>::aligin(kMemoryAlignment,
                                                           sector_size);
        BigEndian::PutUint16(buf.get_write() + 4, kExtentEntrySize);
        iter->second.MarshalTo(buf.get_write() + 6);
        BigEndian::PutUint32(
            buf.get_write(),
            crc32_gzip_refl(0, buf.get() + 4, kExtentEntrySize + 2));

        if (buf_vec.size() == 0) {
            iov.emplace_back({buf.get(), buf.size()});
            buf_vec.emplace_back(std::move(buf));
            first_index = iter->first;
            prev_index = first_index;
        } else {
            if (iter->second.index == prev_index + 1) {
                iov.emplace_back({buf.get(), buf.size()});
            } else {
                uint64_t off =
                    super_.pt[EXTENT_PT].start + first_index * sector_size;
                co_await sem.wait();
                (void)dev_ptr_->Write(off, std::move(iov))
                    .then([&sem, &s](Status<> &st) {
                        if (!st.OK()) {
                            s = st;
                        }
                        sem.signal();
                    });
                iov.emplace_back({buf.get(), buf.size()});
                first_index = iter->first;
            }
            buf_vec.emplace_back(std::move(buf));
            prev_index = iter->first;
        }
    }

    co_await sem.wait(parallel_submit);
    if (!s.OK()) {
        co_return s;
    }

    if (!iov.empty()) {
        uint64_t off = super_.pt[EXTENT_PT].start + first_index * sector_size;
        s = co_await dev_ptr_->Write(off, std::move(iov));
    }
    co_return s;
}

seastar::future<Status<>> Log::BackgroundFlush(uint64_t ver,
                                               uint64_t head_offset) {
    uint32_t sector_size = dev_ptr_->SectorSize();
    Status<> s;

    s = co_await SaveImmuChunks();
    if (!s.OK()) {
        status_ = s;
        co_return s;
    }
    s = co_await SaveImmuExtents();
    if (!s.OK()) {
        status_ = s;
        co_return s;
    }

    // change head flag
    auto buf = dev_ptr_->Get(kSectorSize);
    BigEndian::PutUint64(buf.get_write() + 4, ver);
    *(buf.get_write() + 12) = static_cast<char>(1);
    uint32_t crc = crc32_gzip_refl(0, buf.get() + 4, kHeaderSize - 4);
    BigEndian::PutUint32(buf.get_write(), crc);
    s = co_await dev_ptr_->Write(head_offset, buf.get(), sector_size);
    if (!s.OK()) {
        status_ = s;
        co_return s;
    }

    co_return s;
}

void Log::UpdateMem(const std::variant<ExtentEntry, ChunkEntry> &entry) {
    if (entry.index() == 0) {
        const ExtentEntry &ent = std::get<0>(entry);
        extent_mem_[ent.index] = ent;
    } else {
        const ChunkEntry &ent = std::get<1>(entry);
        chunk_mem_[ent.index] = ent;
    }
}

seastar::future<> Log::LoopRun() {
    int max_item_num = kMaxMemSize / kSectorSize;
    std::optional<seastar::future<Status<>>> background_fu;

    while (!closed_) {
        std::queue<worker_item *> tmp_queue;
        uint64_t free_size = super_.pt[current_log_pt_].offset +
                             super_.pt[current_log_pt_].size - offset_;
        TmpBuffer buffer = dev_ptr_->Get(std::min(kMaxMemSize, free_size));
        memset(buffer.get_write(), 0, buffer.size());
        size_t buffer_len = 0;

        while (buffer_len < buffer.size()) {
            if (queue_.empty()) {
                break;
            }
            if (offset_ == super_.pt[current_log_pt_].offset &&
                buffer_len == 0) {
                // add head
                LogHeaderMarshalTo(buffer.get_write());
                buffer_len += kSectorSize;
            }
            auto item = queue_.front();
            size_t n = item->Size() + kRecordHeaderSize;
            if (kSectorSize - (buffer_len & kSectorSizeMask) < n) {
                size_t len =
                    seastar::align_up(buffer_len, kSectorSize) + kSectorSize;
                if (len > buffer.size()) {
                    break;
                }
                // add padding
                buffer_len += kSectorSize - (buffer_len & kSectorSizeMask);
            }
            item->MarshalTo(version_, buffer.get_write() + buffer_len);
            buffer_len += n;
            queue_.pop();
            tmp_queue.push(item);
        }

        Status<> s = status_;
        if (!tmp_queue.empty() && s.OK()) {
            s = co_await dev_ptr_->Write(
                offset_, buffer.get(),
                seastar::align_up(buffer_len, kSectorSize));
        }
        if (s.OK()) {
            offset_ += seastar::align_up(buffer_len, kSectorSize);
        }
        while (!tmp_queue.empty()) {
            auto tmp_item = tmp_queue.front();
            tmp_queue.pop();
            if (s.OK()) {
                UpdateMem(tmp_item->entry);
            }
            auto st = s;
            tmp_item->pr.set_value(std::move(st));
        }

        if (s.OK() && (offset_ >= super_.pt[current_log_pt_].start +
                                      super_.pt[current_log_pt_].end)) {
            uint64_t old_header_offset = super_.pt[current_log_pt_].start;
            uint64_t old_version = version_;
            current_log_pt_ = (current_log_pt_ + 1) % 2 + LOGA_PT;
            offset_ = super_.pt[current_log_pt_].start;
            version_++;
            if (background_fu) {
                s = co_await background_fu.value();
                background_fu.reset();
                if (!s.OK()) {
                    break;
                }
            }
            if (closed_) {
                status_.Set(EPIPE);
                break;
            }
            immu_chunk_mem_ = std::move(chunk_mem_);
            immu_extent_mem_ = std::move(extent_mem_);
            background_fu = BackgroundFlush(old_version, old_header_offset);
        }
        if (queue_.empty() && !closed_) {
            co_await cv_.wait();
        }
    }

    while (!queue_.empty()) {
        auto item = queue_.front();
        queue_.pop();
        Status<> s = status_;
        item->pr.set_value(std::move(s));
    }
    if (background_fu) {
        co_await background_fu.value();
    }
    background_fu.reset();
    co_return;
}

seastar::future<Status<>> Log::SaveChunk(const ChunkEntry &chunk) {
    Status<> s;
    if (closed_ || !init_) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s = status_;
        co_return s;
    }
    std::unique_ptr<worker_item> ptr(new worker_item);
    ptr->entry = std::move(std::variant<ExtentEntry, ChunkEntry>(chunk));
    queue_.push(*ptr);
    auto s = co_await ptr->pr.get_future();
    co_return s;
}

seastar::future<Status<>> Log::SaveExtent(const ExtentEntry &extent) {
    Status<> s;
    if (closed_ || !init_) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s = status_;
        co_return s;
    }
    std::unique_ptr<worker_item> ptr(new worker_item);
    ptr->entry = std::move(std::variant<ExtentEntry, ChunkEntry>(extent));
    queue_.push(*ptr);
    auto s = co_await ptr->pr.get_future();
    co_return s;
}

seastar::future<> Log::Close() {
    if (!closed_) {
        closed_ = true;
        cv_.signal();
        if (loop_fu_) {
            co_await loop_fu_.value();
        }
    }
    co_return;
}

}  // namespace stream
}  // namespace snail
