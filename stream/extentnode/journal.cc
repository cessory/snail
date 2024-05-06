#include "journal.h"

#include <isa-l.h>

#include <seastar/core/align.hh>
#include <seastar/core/do_with.hh>

#include "net/byteorder.h"
#include "util/logger.h"

namespace snail {
namespace stream {

enum EntryType {
    Extent = 0,
    Chunk = 1,
};

static constexpr int kHeaderSize = 4 + 8 + 1;  // crc + ver + flag
static constexpr int kRecordHeaderSize =
    4 + 8 + 2 + 1;  // crc + ver + len + type
static constexpr size_t kMaxMemSize = 8192;

// journal format
// |   header    |             data                         |
// | header(512B) |    block(512B)  |   ....  |   block   |
//
// there are one or more records in the block
// record
// | crc(4B) |  version(8B)  |len(2B)|type(1B)|      data    |
//

static void JournalHeaderMarshalTo(uint64_t ver, uint8_t flag, char *b) {
    net::BigEndian::PutUint64(b + 4, ver);
    *(b + 12) = flag;
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(b + 4), kHeaderSize - 4);
    net::BigEndian::PutUint32(b, crc);
}

Journal::Journal(DevicePtr dev_ptr, const SuperBlock &super_block)
    : dev_ptr_(dev_ptr), super_(super_block) {
    offset_ = super_.pt[JOURNALA_PT].start;
}

void Journal::worker_item::MarshalTo(uint64_t ver, char *b) {
    uint16_t len = kExtentEntrySize;
    EntryType type = EntryType::Extent;
    if (entry.index() == 1) {
        len = kChunkEntrySize;
        type = EntryType::Chunk;
    }

    net::BigEndian::PutUint64(b + 4, ver);
    net::BigEndian::PutUint16(b + 12, len);
    *(b + 14) = static_cast<char>(type);
    if (type == EntryType::Extent) {
        ExtentEntry &ent = std::get<0>(entry);
        ent.MarshalTo(b + kRecordHeaderSize);
    } else {
        ChunkEntry &ent = std::get<1>(entry);
        ent.MarshalTo(b + kRecordHeaderSize);
    }

    uint32_t crc =
        crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(b + 4),
                        kRecordHeaderSize + len - 4);
    net::BigEndian::PutUint32(b, crc);
}

seastar::future<Status<>> Journal::Init(
    seastar::noncopyable_function<Status<>(const ExtentEntry &)> &&f1,
    seastar::noncopyable_function<Status<>(const ChunkEntry &)> &&f2) {
    Status<> s;
    if (init_) {
        s.Set(EALREADY);
        co_return s;
    }
    init_ = true;
    seastar::gate::holder(gate_);
    auto buf = dev_ptr_->Get(kBlockSize);

    uint64_t ver[2];
    int index[2] = {0, 1};
    uint8_t flags[2] = {0, 0};

    // read log head
    for (int i = 0; i < 2; i++) {
        auto st = co_await dev_ptr_->Read(super_.pt[JOURNALA_PT + i].start,
                                          buf.get_write(), kSectorSize);
        if (!st.OK()) {
            LOG_ERROR("read journal head from device {} log-{} error: {}",
                      dev_ptr_->Name(), i, st);
            s.Set(st.Code(), st.Reason());
            co_return s;
        }
        uint32_t crc = net::BigEndian::Uint32(buf.get());
        ver[i] = net::BigEndian::Uint64(buf.get() + 4);
        flags[i] = *(buf.get() + 12);
        if (crc == 0 && ver[i] == 0) {
            LOG_INFO("device-{} journal-{} is emtpy", dev_ptr_->Name(), i);
            continue;
        }
        if (crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char *>(buf.get() + 4),
                kHeaderSize - 4) != crc) {
            LOG_ERROR("device-{} journal-{} journal head has invalid checksum",
                      dev_ptr_->Name(), i);
            co_return Status<>(ErrCode::ErrInvalidChecksum);
        }
    }
    if (ver[0] > ver[1]) {
        index[0] = 1;
        index[1] = 0;
    }
    for (int i = 0; i < 2; i++) {
        int pt_n = JOURNALA_PT + index[i];
        uint64_t offset = super_.pt[pt_n].start;
        version_ = ver[index[i]];

        if (version_ == 0 || flags[index[i]] != 0) {
            LOG_INFO("device-{} journal-{} don't need to reload",
                     dev_ptr_->Name(), index[i]);
            continue;
        }
        LOG_INFO("device-{} journal-{} reload......", dev_ptr_->Name(),
                 index[i]);

        while (offset < super_.pt[pt_n].start + super_.pt[pt_n].size) {
            auto st =
                co_await dev_ptr_->Read(offset, buf.get_write(), buf.size());
            if (!st.OK()) {
                LOG_ERROR("device-{} journal-{} read error: {}, offset={}",
                          dev_ptr_->Name(), index[i], st, offset);
                s.Set(st.Code(), st.Reason());
                co_return s;
            }
            const char *p = buf.get();
            if (offset == super_.pt[pt_n].start) {
                p += kSectorSize;
            }

            bool break_loop = false;
            for (; p < buf.get() + buf.size() && !break_loop;
                 p += kSectorSize) {
                const char *start = p;
                const char *end = p + kSectorSize;
                while (start < end && !break_loop) {
                    if (start + kRecordHeaderSize > end) {
                        break;
                    }
                    uint32_t crc = net::BigEndian::Uint32(start);
                    uint64_t version = net::BigEndian::Uint64(start + 4);
                    uint16_t len = net::BigEndian::Uint16(start + 12);
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
                    if (crc32_gzip_refl(
                            0,
                            reinterpret_cast<const unsigned char *>(start + 4),
                            kRecordHeaderSize + len - 4) !=
                        crc) {  // invalid record
                        s.Set(ErrCode::ErrInvalidChecksum);
                        LOG_ERROR("device-{} journal-{} error: {}, offset={}",
                                  dev_ptr_->Name(), index[i], s, offset);
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
                            s = f2(ent);
                            if (!s) {
                                co_return s;
                            }
                            break;
                        }
                        case EntryType::Extent: {
                            ExtentEntry ent;
                            ent.Unmarshal(start + kRecordHeaderSize);
                            immu_extent_mem_[ent.index] = ent;
                            s = f1(ent);
                            if (!s) {
                                co_return s;
                            }
                            break;
                        }
                        default:
                            s.Set(EINVAL);
                            co_return s;
                    }
                    start += kRecordHeaderSize + len;
                }
            }
            if (break_loop) {
                break;
            }
            offset += buf.size();
        }
        s = co_await BackgroundFlush(version_, super_.pt[pt_n].start);
        if (!s) {
            co_return s;
        }
    }
    version_++;
    offset_ = super_.pt[index[0] + JOURNALA_PT].start;
    loop_fu_ = LoopRun();
    co_return s;
}

seastar::future<Status<>> Journal::SaveImmuChunks() {
    uint32_t prev_index = -1;
    uint32_t first_index = -1;
    Status<> s;
    static size_t parallel_submit = 16;
    seastar::semaphore sem(parallel_submit);
    auto buffer = dev_ptr_->Get(parallel_submit * kSectorSize);
    size_t buffer_len = 0;

    for (auto iter : immu_chunk_mem_) {
        auto buf = buffer.share(buffer_len, kSectorSize);
        net::BigEndian::PutUint16(buf.get_write() + 4, kChunkEntrySize);
        iter.second.MarshalTo(buf.get_write() + 6);
        net::BigEndian::PutUint32(
            buf.get_write(),
            crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char *>(buf.get() + 4),
                kChunkEntrySize + 2));
        buffer_len += kSectorSize;

        if (first_index == -1) {
            first_index = iter.first;
            prev_index = iter.first;
        } else if (iter.second.index != prev_index + 1 ||
                   buffer_len == buffer.size()) {
            uint64_t off =
                super_.pt[CHUNK_PT].start + first_index * kSectorSize;
            co_await sem.wait();
            (void)seastar::do_with(std::move(buffer.share(0, buffer_len)),
                                   [this, &sem, &s, off](auto &buf) {
                                       return dev_ptr_
                                           ->Write(off, buf.get(), buf.size())
                                           .then([&sem, &s](Status<> st) {
                                               if (!st) {
                                                   s = st;
                                               }
                                               sem.signal();
                                           });
                                   });
            first_index = -1;
            prev_index = -1;
            buffer_len = 0;
            buffer = dev_ptr_->Get(parallel_submit * kSectorSize);
        } else {
            prev_index = iter.first;
        }
    }
    co_await sem.wait(parallel_submit);
    if (!s) {
        LOG_ERROR("save chunk meta error: {}", s);
        co_return s;
    }

    if (buffer_len > 0) {
        uint64_t off = super_.pt[CHUNK_PT].start + first_index * kSectorSize;
        s = co_await dev_ptr_->Write(off, buffer.get(), buffer_len);
        if (!s) {
            LOG_ERROR(
                "save chunk meta error: {}, off={}, first_index={}, len={}", s,
                off, first_index, buffer_len);
        }
    }
    co_return s;
}

seastar::future<Status<>> Journal::SaveImmuExtents() {
    std::vector<iovec> iov;
    std::vector<seastar::temporary_buffer<char>> buf_vec;
    uint32_t prev_index = -1;
    uint32_t first_index = -1;
    Status<> s;
    static size_t parallel_submit = 16;
    seastar::semaphore sem(parallel_submit);

    for (auto iter : immu_extent_mem_) {
        auto buf = dev_ptr_->Get(kSectorSize);
        net::BigEndian::PutUint16(buf.get_write() + 4, kExtentEntrySize);
        iter.second.MarshalTo(buf.get_write() + 6);
        net::BigEndian::PutUint32(
            buf.get_write(),
            crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char *>(buf.get() + 4),
                kExtentEntrySize + 2));

        if (buf_vec.size() == 0) {
            iov.push_back({buf.get_write(), buf.size()});
            buf_vec.emplace_back(std::move(buf));
            first_index = iter.first;
            prev_index = first_index;
        } else {
            if (iter.second.index == prev_index + 1) {
                iov.push_back({buf.get_write(), buf.size()});
            } else {
                uint64_t off =
                    super_.pt[EXTENT_PT].start + first_index * kSectorSize;
                co_await sem.wait();
                (void)dev_ptr_->Write(off, std::move(iov))
                    .then(
                        [&sem, &s, buf_vec = std::move(buf_vec)](Status<> st) {
                            if (!st) {
                                s = st;
                            }
                            sem.signal();
                        });
                iov.push_back({buf.get_write(), buf.size()});
                first_index = iter.first;
            }
            buf_vec.emplace_back(std::move(buf));
            prev_index = iter.first;
        }
    }

    co_await sem.wait(parallel_submit);
    if (!s) {
        co_return s;
    }

    if (!iov.empty()) {
        uint64_t off = super_.pt[EXTENT_PT].start + first_index * kSectorSize;
        s = co_await dev_ptr_->Write(off, std::move(iov));
    }
    co_return s;
}

seastar::future<Status<>> Journal::BackgroundFlush(uint64_t ver,
                                                   uint64_t head_offset) {
    Status<> s;

    s = co_await SaveImmuChunks();
    if (!s) {
        LOG_ERROR("device-{} save chunk meta error: {}", dev_ptr_->Name(), s);
        status_.Set(EIO);
        co_return s;
    }
    s = co_await SaveImmuExtents();
    if (!s) {
        LOG_ERROR("device-{} save extent meta error: {}", dev_ptr_->Name(), s);
        status_.Set(EIO);
        co_return s;
    }

    // change head flag
    auto buf = dev_ptr_->Get(kSectorSize);
    net::BigEndian::PutUint64(buf.get_write() + 4, ver);
    *(buf.get_write() + 12) = static_cast<char>(1);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(buf.get() + 4),
        kHeaderSize - 4);
    net::BigEndian::PutUint32(buf.get_write(), crc);
    s = co_await dev_ptr_->Write(head_offset, buf.get(), kSectorSize);
    if (!s.OK()) {
        LOG_ERROR("device-{} save journal head error: {}", dev_ptr_->Name(), s);
        status_.Set(EIO);
        co_return s;
    }

    co_return s;
}

void Journal::UpdateMem(const std::variant<ExtentEntry, ChunkEntry> &entry) {
    if (entry.index() == 0) {
        const ExtentEntry &ent = std::get<0>(entry);
        extent_mem_[ent.index] = ent;
    } else {
        const ChunkEntry &ent = std::get<1>(entry);
        chunk_mem_[ent.index] = ent;
    }
}

seastar::future<> Journal::LoopRun() {
    std::optional<seastar::future<Status<>>> background_fu;

    while (!gate_.is_closed()) {
        std::queue<worker_item *> tmp_queue;
        uint64_t free_size = super_.pt[current_pt_].start +
                             super_.pt[current_pt_].size - offset_;
        TmpBuffer buffer = dev_ptr_->Get(std::min(kBlockSize, free_size));
        memset(buffer.get_write(), 0, buffer.size());
        size_t buffer_len = 0;

        while (buffer_len < buffer.size()) {
            if (queue_.empty()) {
                break;
            }
            if (offset_ == super_.pt[current_pt_].start && buffer_len == 0) {
                // add head
                JournalHeaderMarshalTo(version_, 0, buffer.get_write());
                buffer_len += kSectorSize;
            }
            auto item = queue_.front();
            size_t n = item->Size() + kRecordHeaderSize;
            if ((buffer_len & kSectorSizeMask) + n > kSectorSize) {
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
        if (!tmp_queue.empty() && s) {
            s = co_await dev_ptr_->Write(
                offset_, buffer.get(),
                seastar::align_up(buffer_len, kSectorSize));
        }
        if (s) {
            offset_ += seastar::align_up(buffer_len, kSectorSize);
        } else {
            LOG_ERROR("write journal error: {}, dev_name={}, offset={}", s,
                      dev_ptr_->Name(), offset_);
            if (status_) {
                status_.Set(EIO);
            }
        }
        while (!tmp_queue.empty()) {
            auto tmp_item = tmp_queue.front();
            tmp_queue.pop();
            if (s) {
                UpdateMem(tmp_item->entry);
            }
            auto st = s;
            tmp_item->pr.set_value(std::move(st));
        }

        if (s && (offset_ >=
                  super_.pt[current_pt_].start + super_.pt[current_pt_].size)) {
            uint64_t old_header_offset = super_.pt[current_pt_].start;
            uint64_t old_version = version_;
            current_pt_ = (current_pt_ + 1) % 2 + JOURNALA_PT;
            offset_ = super_.pt[current_pt_].start;
            version_++;
            if (background_fu) {
                s = co_await std::move(background_fu.value());
                background_fu.reset();
                if (!s.OK()) {
                    LOG_ERROR(
                        "Flush journal error: {}, whatever error is, we should "
                        "treat this error as EIO",
                        s);
                    break;
                }
            }
            immu_chunk_mem_ = std::move(chunk_mem_);
            immu_extent_mem_ = std::move(extent_mem_);
            background_fu = BackgroundFlush(old_version, old_header_offset);
        }
        if (queue_.empty() && !gate_.is_closed()) {
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
        co_await std::move(background_fu.value());
        background_fu.reset();
    }
    co_return;
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
    seastar::gate::holder(gate_);
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
    seastar::gate::holder(gate_);
    std::unique_ptr<worker_item> ptr(new worker_item);
    ptr->entry = std::move(std::variant<ExtentEntry, ChunkEntry>(extent));
    queue_.push(ptr.get());
    cv_.signal();
    s = co_await ptr->pr.get_future();
    LOG_DEBUG("save extent (index={}, id={}-{}, chunk_idx={}) success!",
              extent.index, extent.id.hi, extent.id.lo, extent.chunk_idx);
    co_return s;
}

seastar::future<> Journal::Close() {
    if (!gate_.is_closed()) {
        cv_.signal();
        co_await gate_.close();
        if (loop_fu_) {
            co_await std::move(loop_fu_.value());
            loop_fu_.reset();
        }
    }
    co_return;
}

}  // namespace stream
}  // namespace snail
