#include "store.h"

#include <isa-l.h>

#include <algorithm>
#include <iostream>
#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/defer.hh>
#include <system_error>

#include "net/byteorder.h"
#include "util/logger.h"

namespace snail {
namespace stream {

const uint32_t kMagic = 0x69616e73;
const uint32_t kVersion = 1;

Extent::Extent() : len(0), store(nullptr) {}

Extent::~Extent() {
    if (!store) {
        return;
    }
    for (auto& chunk : chunks) {
        store->free_chunks_.push(chunk.index);
        store->used_ -= kChunkSize;
    }
    if (index != -1) {
        store->last_sector_cache_.Erase(index);
        store->free_extents_.push(index);
    }
}

ExtentEntry Extent::GetExtentEntry() { return *((ExtentEntry*)this); }

inline static size_t ChunkPhyLen(const ChunkEntry& chunk) {
    return chunk.len / kBlockDataSize * kBlockSize + chunk.len % kBlockDataSize;
}

static bool InvalidSuperBlock(const std::string_view& name,
                              const SuperBlock& super_block) {
    if (super_block.magic != kMagic) {
        LOG_ERROR("device {} superblock has invalid magic({})", name,
                  super_block.magic);
        return false;
    }

    if (super_block.version != kVersion) {
        LOG_ERROR("device {} superblock has invalid version({})", name,
                  super_block.version);
        return false;
    }

    size_t off = kChunkSize;
    for (int i = 0; i < MAX_PT; i++) {
        if (super_block.pt[i].start != off ||
            (super_block.pt[i].size & kChunkSizeMask)) {
            LOG_ERROR(
                "device {} superblock has invalid pt({}) start={} size={}",
                name, i, super_block.pt[i].start, super_block.pt[i].size);
            return false;
        }
        off += super_block.pt[i].size;
    }
    return true;
}

static seastar::future<Status<>> LoadMeta(
    DevicePtr dev_ptr, uint64_t off, size_t size,
    seastar::noncopyable_function<Status<>(const char*)>&& f) {
    Status<> s;
    auto buffer = dev_ptr->Get(kBlockSize);
    uint64_t end = off + size;
    for (; off < end; off += kBlockSize) {
        auto st =
            co_await dev_ptr->Read(off, buffer.get_write(), buffer.size());
        if (!st) {
            s.Set(s.Code(), s.Reason());
            co_return s;
        }
        for (const char* p = buffer.get(); p < buffer.get() + buffer.size();
             p += kSectorSize) {
            uint32_t crc = net::BigEndian::Uint32(p);
            uint16_t len = net::BigEndian::Uint16(p + 4);
            if (crc32_gzip_refl(0,
                                reinterpret_cast<const unsigned char*>(p + 4),
                                len + 2) != crc) {
                s.Set(ErrCode::ErrInvalidChecksum);
                co_return s;
            }
            s = f(p + 6);
            if (!s) {
                co_return s;
            }
        }
    }
    co_return s;
}

static seastar::future<Status<std::map<uint32_t, ExtentPtr>>> LoadExtents(
    std::string_view name, DevicePtr dev_ptr, uint64_t off, size_t size) {
    Status<std::map<uint32_t, ExtentPtr>> s;
    std::map<uint32_t, ExtentPtr> extent_ht;
    uint32_t index = 0;

    auto st = co_await LoadMeta(
        dev_ptr, off, size,
        [&extent_ht, &index, dev_ptr](const char* b) -> Status<> {
            Status<> s;
            auto extent_ptr = seastar::make_lw_shared<Extent>();
            extent_ptr->Unmarshal(b);
            if (extent_ptr->index != index) {
                LOG_ERROR(
                    "read device {} extent meta error: invalid index "
                    "index={} expect index={}",
                    dev_ptr->Name(), extent_ptr->index, index);
                s.Set(ErrCode::ErrUnExpect, "invalid index");
                return s;
            }
            extent_ht[index] = extent_ptr;
            index++;
            return s;
        });
    if (!st) {
        LOG_ERROR("load extent meta error: {} device={}", st, dev_ptr->Name());
        s.Set(st.Code(), st.Reason());
        co_return s;
    }

    s.SetValue(std::move(extent_ht));
    co_return s;
}

static seastar::future<Status<std::map<uint32_t, ChunkEntry>>> LoadChunks(
    std::string_view name, DevicePtr dev_ptr, uint64_t off, size_t size) {
    Status<std::map<uint32_t, ChunkEntry>> s;
    std::map<uint32_t, ChunkEntry> chunk_ht;
    uint32_t index = 0;

    auto st = co_await LoadMeta(
        dev_ptr, off, size,
        [&chunk_ht, &index, dev_ptr](const char* b) -> Status<> {
            Status<> s;
            ChunkEntry chunk;
            chunk.Unmarshal(b);
            if (chunk.index != index) {
                LOG_ERROR(
                    "device {} chunk meta has invalid index, index={} expect "
                    "index={}",
                    dev_ptr->Name(), chunk.index, index);
                s.Set(ErrCode::ErrUnExpect, "invalid index");
                return s;
            }
            chunk_ht[index] = chunk;
            index++;
            return s;
        });
    if (!st) {
        LOG_ERROR("load chunk meta error: {} device={}", st, dev_ptr->Name());
        s.Set(st.Code(), st.Reason());
        co_return s;
    }

    s.SetValue(std::move(chunk_ht));
    co_return s;
}

seastar::future<StorePtr> Store::Load(std::string_view name, bool spdk_nvme,
                                      size_t cache_cap) {
    StorePtr store = seastar::make_lw_shared<Store>(cache_cap);
    DevicePtr dev_ptr;
    if (!spdk_nvme) {
        dev_ptr = co_await OpenKernelDevice(name);
    } else {
        dev_ptr = co_await OpenSpdkDevice(name);
    }
    if (!dev_ptr) {
        co_return nullptr;
    }

    auto tmp = dev_ptr->Get(kSectorSize);
    auto s = co_await dev_ptr->Read(0, tmp.get_write(), tmp.size());
    if (!s.OK()) {
        LOG_ERROR("read device {} superblock error: {}", name, s.String());
        co_await dev_ptr->Close();
        co_return nullptr;
    }

    uint32_t crc = net::BigEndian::Uint32(tmp.get());
    uint32_t len = net::BigEndian::Uint16(tmp.get() + 4);
    if (len + 6 > kSectorSize) {
        LOG_ERROR("device {} superblock has invalid len({})", name, len);
        co_await dev_ptr->Close();
        co_return nullptr;
    }
    if (crc != crc32_gzip_refl(
                   0, reinterpret_cast<const unsigned char*>(tmp.get() + 4),
                   len + 2)) {
        LOG_ERROR("device {} superblock has invaid checksum", name);
        co_await dev_ptr->Close();
        co_return nullptr;
    }

    SuperBlock super_block;
    super_block.Unmarshal(tmp.get() + 6);
    if (!InvalidSuperBlock(name, super_block)) {
        co_await dev_ptr->Close();
        co_return nullptr;
    }
    LOG_INFO(
        "device-{} get super block ver={} capacity={} cluster_id={} dev_id={}",
        dev_ptr->Name(), super_block.version, super_block.capacity,
        super_block.cluster_id, super_block.dev_id);
    for (int i = 0; i < MAX_PT; i++) {
        LOG_INFO("device-{} pt[{}] start={} size={}", dev_ptr->Name(), i,
                 super_block.pt[i].start, super_block.pt[i].size);
    }

    store->dev_ptr_ = dev_ptr;
    store->super_block_ = super_block;
    store->used_ = kChunkSize;
    for (int i = 0; i < DATA_PT; i++) {
        store->used_ += super_block.pt[i].size;
    }

    std::map<uint32_t, ExtentPtr> extent_ht;
    std::map<uint32_t, ChunkEntry> chunk_ht;

    auto s1 =
        co_await LoadExtents(name, dev_ptr, super_block.pt[EXTENT_PT].start,
                             super_block.pt[EXTENT_PT].size);
    if (!s1.OK()) {
        co_await dev_ptr->Close();
        co_return nullptr;
    }
    extent_ht = std::move(s1.Value());

    auto s2 = co_await LoadChunks(name, dev_ptr, super_block.pt[CHUNK_PT].start,
                                  super_block.pt[CHUNK_PT].size);
    if (!s1.OK()) {
        co_await dev_ptr->Close();
        co_return nullptr;
    }
    chunk_ht = std::move(s2.Value());

    JournalPtr journal_ptr =
        seastar::make_lw_shared<Journal>(dev_ptr, super_block);
    auto s3 = co_await journal_ptr->Init(
        [&extent_ht](const ExtentEntry& ext) -> Status<> {
            Status<> s;
            auto iter = extent_ht.find(ext.index);
            if (iter == extent_ht.end()) {
                s.Set(ErrCode::ErrUnExpect, "not found extent index");
                return s;
            }
            LOG_DEBUG("load a extent from journal extent-{} index={}", ext.id,
                      ext.index);
            auto extent_ptr = iter->second;
            extent_ptr->id = ext.id;
            extent_ptr->chunk_idx = ext.chunk_idx;
            return s;
        },
        [&chunk_ht](const ChunkEntry& chunk) -> Status<> {
            Status<> s;
            if (chunk_ht.count(chunk.index) != 1) {
                s.Set(ErrCode::ErrUnExpect, "not found chunk index");
                return s;
            }
            LOG_DEBUG("load a chunk from journal index={} next={} len={}",
                      chunk.index, (int)chunk.next, chunk.len);
            chunk_ht[chunk.index] = chunk;
            return s;
        });
    if (!s3.OK()) {
        LOG_ERROR("device {} init journal error: {}", name, s3.String());
        co_await dev_ptr->Close();
        co_return nullptr;
    }
    store->journal_ptr_ = journal_ptr;

    for (auto iter = extent_ht.begin(); iter != extent_ht.end(); ++iter) {
        auto extent_ptr = iter->second;
        if (extent_ptr->id.Empty()) {
            store->free_extents_.push(extent_ptr->index);
            continue;
        }
        uint32_t next = extent_ptr->chunk_idx;
        while (next != -1) {
            auto it = chunk_ht.find(next);
            if (it == chunk_ht.end()) {
                LOG_ERROR("invalid next({}) chunk index", next);
                co_await store->Close();
                co_return nullptr;
            }
            store->used_ += kChunkSize;
            extent_ptr->chunks.push_back(it->second);
            extent_ptr->len += it->second.len;
            next = it->second.next;
            chunk_ht.erase(it);
        }
        extent_ptr->store = store;
        store->extents_[extent_ptr->id] = extent_ptr;
    }

    for (auto iter = chunk_ht.begin(); iter != chunk_ht.end(); ++iter) {
        store->free_chunks_.push(iter->second.index);
    }

    co_return store;
}

seastar::future<bool> Store::Format(std::string_view name, uint32_t cluster_id,
                                    DevType dev_type, uint32_t dev_id,
                                    uint64_t journal_cap, uint64_t capacity) {
    DevicePtr dev_ptr;
    switch (dev_type) {
        case DevType::HDD:
        case DevType::SSD:
        case DevType::NVME:
            dev_ptr = co_await OpenKernelDevice(name);
            break;
        case DevType::NVME_SPDK:
            dev_ptr = co_await OpenSpdkDevice(name);
            break;
        default:
            LOG_ERROR("invalid dev type");
            break;
    }
    if (!dev_ptr) {
        co_return false;
    }

    uint64_t cap = dev_ptr->Capacity();
    if (capacity == 0) {
        capacity = cap;
    } else if (capacity > cap) {
        LOG_ERROR("capacity {} is larger than device capacity {}", capacity,
                  cap);
        co_await dev_ptr->Close();
        co_return false;
    }

    journal_cap = seastar::align_down(journal_cap, kChunkSize);
    capacity = seastar::align_down(capacity, kChunkSize);
    if (journal_cap == 0 || capacity == 0 ||
        (kChunkSize + 2 * journal_cap) >= capacity) {
        LOG_ERROR(
            "device {} capacity: {} journal_cap: {} capacity is too small",
            name, capacity, journal_cap);
        co_await dev_ptr->Close();
        co_return false;
    }

    size_t chunk_n =
        (capacity - kChunkSize - 2 * journal_cap) / (kChunkSize + 1024);
    if (chunk_n == 0) {
        LOG_ERROR("device {} capacity: {} journal_cap: {} not enough chunks",
                  name, capacity, journal_cap);
        co_await dev_ptr->Close();
        co_return false;
    }
    size_t extent_meta_size =
        seastar::align_up(chunk_n * kSectorSize, kChunkSize);
    size_t chunk_meta_size = extent_meta_size;
    if (capacity <= kChunkSize + 2 * chunk_meta_size + 2 * journal_cap) {
        LOG_ERROR(
            "device {} capacity: {} journal_cap: {} extent_meta_size: {} "
            "chunk_meta_size: {} capacity is too small",
            name, capacity, journal_cap, extent_meta_size, chunk_meta_size);
        co_await dev_ptr->Close();
        co_return false;
    }
    size_t data_size =
        capacity - kChunkSize - 2 * chunk_meta_size - 2 * journal_cap;

    SuperBlock super_block;
    super_block.magic = kMagic;
    super_block.version = kVersion;
    super_block.cluster_id = cluster_id;
    super_block.dev_type = dev_type;
    super_block.dev_id = dev_id;
    super_block.capacity = capacity;

    super_block.pt[EXTENT_PT].start = kChunkSize;
    super_block.pt[EXTENT_PT].size = extent_meta_size;
    super_block.pt[CHUNK_PT].start =
        super_block.pt[EXTENT_PT].start + super_block.pt[EXTENT_PT].size;
    super_block.pt[CHUNK_PT].size = chunk_meta_size;
    super_block.pt[JOURNALA_PT].start =
        super_block.pt[CHUNK_PT].start + super_block.pt[CHUNK_PT].size;
    super_block.pt[JOURNALA_PT].size = journal_cap;
    super_block.pt[JOURNALB_PT].start =
        super_block.pt[JOURNALA_PT].start + super_block.pt[JOURNALA_PT].size;
    super_block.pt[JOURNALB_PT].size = journal_cap;
    super_block.pt[DATA_PT].start =
        super_block.pt[JOURNALB_PT].start + super_block.pt[JOURNALB_PT].size;
    super_block.pt[DATA_PT].size = data_size;

    LOG_INFO(
        "Format device {} capacity: {} extent_meta: (start={} size={}) "
        "chunk_meta: (start={} size={}) journala: (start={} size={}) journalb: "
        "(start={} size={}) data: (start={}, size={})",
        name, capacity, super_block.pt[EXTENT_PT].start,
        super_block.pt[EXTENT_PT].size, super_block.pt[CHUNK_PT].start,
        super_block.pt[CHUNK_PT].size, super_block.pt[JOURNALA_PT].start,
        super_block.pt[JOURNALA_PT].size, super_block.pt[JOURNALB_PT].start,
        super_block.pt[JOURNALB_PT].size, super_block.pt[DATA_PT].start,
        super_block.pt[DATA_PT].size);
    // format extent meta
    auto buffer = dev_ptr->Get(kBlockSize);
    uint32_t index = 0;
    for (uint64_t off = super_block.pt[EXTENT_PT].start;
         off < super_block.pt[EXTENT_PT].start + super_block.pt[EXTENT_PT].size;
         off += kBlockSize) {
        int sector_n = kBlockSize / kSectorSize;
        for (int i = 0; i < sector_n; i++) {
            char* p = buffer.get_write() + i * kSectorSize;
            ExtentEntry ent;
            ent.index = index;
            net::BigEndian::PutUint16(p + 4, kExtentEntrySize);
            ent.MarshalTo(p + 6);
            net::BigEndian::PutUint32(
                p, crc32_gzip_refl(
                       0, reinterpret_cast<const unsigned char*>(p + 4),
                       kExtentEntrySize + 2));
            index++;
        }
        auto s = co_await dev_ptr->Write(off, buffer.get(), buffer.size());
        if (!s.OK()) {
            LOG_ERROR("device {} format extent meta error: {}", name,
                      s.String());
            co_await dev_ptr->Close();
            co_return false;
        }
    }
    LOG_INFO("device {} format extent meta success!", name);

    // format chunk meta
    index = 0;
    for (uint64_t off = super_block.pt[CHUNK_PT].start;
         off < super_block.pt[CHUNK_PT].start + super_block.pt[CHUNK_PT].size;
         off += kBlockSize) {
        int sector_n = kBlockSize / kSectorSize;
        for (int i = 0; i < sector_n; i++) {
            char* p = buffer.get_write() + i * kSectorSize;
            ChunkEntry ent(index);
            net::BigEndian::PutUint16(p + 4, kExtentEntrySize);
            ent.MarshalTo(p + 6);
            net::BigEndian::PutUint32(
                p, crc32_gzip_refl(
                       0, reinterpret_cast<const unsigned char*>(p + 4),
                       kExtentEntrySize + 2));
            index++;
        }
        auto s = co_await dev_ptr->Write(off, buffer.get(), buffer.size());
        if (!s.OK()) {
            LOG_ERROR("device {} format chunk meta error: {}", name,
                      s.String());
            co_await dev_ptr->Close();
            co_return false;
        }
    }
    LOG_INFO("device {} format chunk meta success!", name);

    // format loga and logb
    memset(buffer.get_write(), 0, buffer.size());
    for (uint64_t off = super_block.pt[JOURNALA_PT].start;
         off <
         super_block.pt[JOURNALB_PT].start + super_block.pt[JOURNALB_PT].size;
         off += kBlockSize) {
        auto s = co_await dev_ptr->Write(off, buffer.get(), buffer.size());
        if (!s.OK()) {
            LOG_ERROR("device {} format journal error: {}", name, s.String());
            co_await dev_ptr->Close();
            co_return false;
        }
    }
    LOG_INFO("device {} format journal success!", name);

    // format superblock
    net::BigEndian::PutUint16(buffer.get_write() + 4, kSuperBlockSize);
    super_block.MarshalTo(buffer.get_write() + 6);
    net::BigEndian::PutUint32(
        buffer.get_write(),
        crc32_gzip_refl(
            0, reinterpret_cast<const unsigned char*>(buffer.get() + 4),
            kSuperBlockSize + 2));
    auto s = co_await dev_ptr->Write(0, buffer.get(), kSectorSize);
    co_await dev_ptr->Close();
    if (!s.OK()) {
        LOG_ERROR("device {} save superblock error: {}", name, s.String());
        co_return false;
    }
    LOG_INFO("device {} save superblock success!", name);
    co_return true;
}

seastar::future<Status<>> Store::AllocChunk(ExtentPtr extent_ptr) {
    Status<> s;
    uint32_t chunk_idx = free_chunks_.front();
    free_chunks_.pop();
    if (extent_ptr->chunks.empty()) {
        ChunkEntry new_chunk(chunk_idx);
        ExtentEntry extent_entry = extent_ptr->GetExtentEntry();
        extent_entry.chunk_idx = chunk_idx;
        std::vector<seastar::future<Status<>>> fu_vec;
        auto f1 = journal_ptr_->SaveChunk(new_chunk);
        auto f2 = journal_ptr_->SaveExtent(extent_entry);
        fu_vec.emplace_back(std::move(f1));
        fu_vec.emplace_back(std::move(f2));
        auto res =
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        if (!res[0].OK() || !res[1].OK()) {
            free_chunks_.push(chunk_idx);
            s = res[0].OK() ? res[1] : res[0];
            co_return s;
        }
        extent_ptr->chunk_idx = new_chunk.index;
        extent_ptr->chunks.push_back(new_chunk);
    } else {
        ChunkEntry& chunk = extent_ptr->chunks.back();
        ChunkEntry new_chunk(chunk_idx);
        ChunkEntry old_chunk(chunk);
        old_chunk.next = new_chunk.index;
        std::vector<seastar::future<Status<>>> fu_vec;
        auto f1 = journal_ptr_->SaveChunk(new_chunk);
        auto f2 = journal_ptr_->SaveChunk(old_chunk);
        fu_vec.emplace_back(std::move(f1));
        fu_vec.emplace_back(std::move(f2));
        auto res =
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        if (!res[0].OK() || !res[1].OK()) {
            free_chunks_.push(chunk_idx);
            s = res[0].OK() ? res[1] : res[0];
            co_return s;
        }
        chunk.next = chunk_idx;
        extent_ptr->chunks.push_back(new_chunk);
    }
    co_return s;
}

seastar::future<Status<std::string>> Store::GetLastSectorData(
    ExtentPtr extent_ptr) {
    Status<std::string> s;
    size_t len = extent_ptr->len;

    size_t last_block_size = len % kBlockDataSize;
    if (last_block_size == 0) {
        co_return s;
    }

    size_t last_sector_size = last_block_size % kSectorSize;
    if (last_sector_size == 0) {
        co_return s;
    }

    auto opt = last_sector_cache_.Get(extent_ptr->index);
    if (opt.has_value()) {
        std::string last_sector_data = opt.value();
        if (last_sector_data.size() != last_sector_size) {
            s.Set(ErrCode::ErrUnExpect,
                  "the last sector data(in cache) size is invalid");
            co_return s;
        }
        s.SetValue(std::move(last_sector_data));
        co_return s;
    }

    // 从磁盘读取数据
    ChunkEntry& chunk = extent_ptr->chunks.back();
    size_t offset = super_block_.DataOffset() + chunk.index * kChunkSize +
                    ChunkPhyLen(chunk);
    size_t aligned_offset = seastar::align_down(offset, kSectorSize);
    if (offset - aligned_offset != last_sector_size) {
        s.Set(ErrCode::ErrUnExpect,
              "the last sector data(in disk) size is invalid");
        co_return s;
    }

    auto tmp = dev_ptr_->Get(kSectorSize);
    auto st =
        co_await dev_ptr_->Read(aligned_offset, tmp.get_write(), tmp.size());
    if (!st.OK()) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char*>(tmp.get()), last_sector_size);
    if (crc != chunk.scrc) {
        LOG_ERROR("invalid crc={} expect crc={} offset={} last_sector_size={}",
                  crc, chunk.scrc, offset, last_sector_size);
        s.Set(ErrCode::ErrInvalidChecksum);
        co_return s;
    }
    std::string last_sector_data = std::string(tmp.get(), last_sector_size);
    last_sector_cache_.Insert(extent_ptr->index, last_sector_data);
    s.SetValue(std::move(last_sector_data));
    co_return s;
}

seastar::future<Status<>> Store::CreateExtent(ExtentID id) {
    Status<> s;

    if (creating_extents_.count(id) == 1 || extents_.count(id) == 1) {
        s.Set(ErrCode::ErrExistExtent);
        LOG_ERROR("extent-{}-{} has already exist", id.hi, id.lo);
        co_return s;
    }

    creating_extents_.insert(id);
    auto defer = seastar::defer([this, &id] { creating_extents_.erase(id); });

    ExtentPtr extent_ptr = seastar::make_lw_shared<Extent>();
    extent_ptr->store = shared_from_this();
    if (free_extents_.empty() || free_chunks_.empty()) {
        s.Set(ENOSPC);
        LOG_ERROR("there is not enouth space to create extent-{}-{}", id.hi,
                  id.lo);
        co_return s;
    }

    extent_ptr->index = free_extents_.front();
    free_extents_.pop();
    extent_ptr->id = id;

    s = co_await AllocChunk(extent_ptr);
    if (!s.OK()) {
        LOG_ERROR("create extent-{}-{} error: {}", id.hi, id.lo, s.String());
        co_return s;
    }
    extents_[id] = extent_ptr;
    co_return s;
}

Status<> Store::HandleIO(ChunkEntry& chunk, char* b, size_t len, bool first,
                         std::vector<TmpBuffer>& tmp_buf_vec,
                         std::vector<iovec>& tmp_io_vec,
                         std::string& last_sector_cached_data) {
    Status<> s;
    uint64_t last_block_len = chunk.len % kBlockDataSize;
    uint32_t last_block_crc = (last_block_len == 0 ? 0 : chunk.crc);
    size_t last_sector_size = last_block_len & kSectorSizeMask;
    uint64_t phy_len = chunk.len / kBlockDataSize * kBlockSize + last_block_len;
    uint64_t last_block_free_size = kBlockSize - last_block_len;

    if (len <= 4 || (reinterpret_cast<uintptr_t>(b) & kMemoryAlignmentMask) ||
        phy_len + len > kChunkSize) {
        LOG_ERROR(
            "data len={} too small or too large or data address is not aligned",
            len);
        s.Set(EINVAL);
        return s;
    }
    size_t block_remain = (len - last_block_free_size) & kBlockSizeMask;
    if (len > last_block_free_size && block_remain > 0 && block_remain <= 4) {
        LOG_ERROR("data len={} invalid last_block_free_size={}", len,
                  last_block_free_size);
        s.Set(EINVAL);
        return s;
    }

    if (last_sector_size != last_sector_cached_data.size()) {
        LOG_ERROR(
            "last_sector_size={} is not match with cached data size={} chunk "
            "len={}",
            last_sector_size, last_sector_cached_data.size(), chunk.len);
        s.Set(ErrCode::ErrUnExpect,
              "last sector size is not match with cached data size");
        return s;
    }

    if (!first && last_sector_size != 0) {
        LOG_ERROR("last_sector_size={} is not zero, but this is not a first io",
                  last_sector_size);
        s.Set(ErrCode::ErrUnExpect,
              "last sector size is not match with cached data size");
        return s;
    }

    uint64_t data_len = 0;
    if (len <= last_block_free_size) {
        data_len = len - 4;
    } else {
        data_len = last_block_free_size - 4 +
                   (len - last_block_free_size) / kBlockSize * kBlockDataSize;
        size_t last_remain = (len - last_block_free_size) & kBlockSizeMask;
        if (last_remain) {
            data_len += last_remain - 4;
        }
    }

    // is_fill_block indicates whether the current data fills the
    // entire block
    bool is_fill_block = (((phy_len + len) & kBlockSizeMask) == 0);

    uint32_t crc = 0;
    if (last_block_crc) {
        if (len > last_block_free_size) {
            crc = net::BigEndian::Uint32(b + len - 4);
        } else {
            crc = crc32_gzip_refl(last_block_crc,
                                  reinterpret_cast<const unsigned char*>(b),
                                  len - 4);
        }
    } else {
        crc = net::BigEndian::Uint32(b + len - 4);
    }
    if (last_sector_size) {
        if (len >= last_block_free_size) {
            uint32_t first_block_crc = crc32_gzip_refl(
                last_block_crc, reinterpret_cast<const unsigned char*>(b),
                last_block_free_size - 4);
            net::BigEndian::PutUint32(b + last_block_free_size - 4,
                                      first_block_crc);
        }
        uint64_t valid_len = is_fill_block ? len : len - 4;
        auto tmp_buf = dev_ptr_->Get(
            seastar::align_up(last_sector_size + valid_len, kSectorSize));
        memcpy(tmp_buf.get_write(), last_sector_cached_data.data(),
               last_sector_size);
        memcpy(tmp_buf.get_write() + last_sector_size, b, valid_len);
        tmp_io_vec.push_back({tmp_buf.get_write(), tmp_buf.size()});
        tmp_buf_vec.emplace_back(std::move(tmp_buf));
    } else {
        if (last_block_crc && len >= last_block_free_size) {
            uint32_t first_block_crc = crc32_gzip_refl(
                last_block_crc, reinterpret_cast<const unsigned char*>(b),
                last_block_free_size - 4);
            net::BigEndian::PutUint32(b + last_block_free_size - 4,
                                      first_block_crc);
        }
        if (is_fill_block) {
            tmp_io_vec.push_back({b, len});
        } else if ((len - 4) & kSectorSizeMask) {
            uint64_t remain = (len - 4) & kSectorSizeMask;
            if (len - 4 - remain) {
                tmp_io_vec.push_back({b, len - 4 - remain});
            }
            auto tmp_buf =
                dev_ptr_->Get(seastar::align_up(remain, kSectorSize));
            memcpy(tmp_buf.get_write(), b + len - 4 - remain, remain);
            tmp_io_vec.push_back({tmp_buf.get_write(), tmp_buf.size()});
            tmp_buf_vec.emplace_back(std::move(tmp_buf));
        } else {
            tmp_io_vec.push_back({b, len - 4});
        }
    }

    chunk.len += data_len;
    chunk.crc = crc;
    chunk.scrc = 0;
    if (!is_fill_block) {
        uint64_t size = (last_sector_size + len - 4) & kSectorSizeMask;
        if (size == 0) {
            last_sector_cached_data = "";
        } else if (size < len - 4) {
            last_sector_cached_data = std::string(b + len - 4 - size, size);
        } else if (size == len - 4) {
            last_sector_cached_data = std::string(b, size);
        } else {
            uint64_t remain = size - (len - 4);
            last_sector_cached_data = last_sector_cached_data.substr(
                last_sector_size - remain, remain);
            last_sector_cached_data.append(b, len - 4);
        }
    } else {
        last_sector_cached_data = "";
    }

    if (last_sector_cached_data.size() > 0) {
        chunk.scrc = crc32_gzip_refl(0,
                                     reinterpret_cast<const unsigned char*>(
                                         last_sector_cached_data.c_str()),
                                     last_sector_cached_data.size());
    }

    return s;
}

// iovec include crc
seastar::future<Status<>> Store::Write(ExtentPtr extent_ptr, uint64_t offset,
                                       std::vector<iovec> iov) {
    Status<> s;
    if (offset != extent_ptr->len) {
        LOG_ERROR("extent({}-{}) is over write, offset={} len={}",
                  extent_ptr->id.hi, extent_ptr->id.lo, offset,
                  extent_ptr->len);
        s.Set(ErrCode::ErrOverWrite);
        co_return s;
    }

    if (iov.empty()) {
        co_return s;
    }

    if (extent_ptr->chunks.empty() ||
        kChunkDataSize == extent_ptr->chunks.back().len) {
        s = co_await AllocChunk(extent_ptr);
        if (!s.OK()) {
            LOG_ERROR("extent({}-{}) alloc chunk error: {}", extent_ptr->id.hi,
                      extent_ptr->id.lo, s.String());
            co_return s;
        }
    }

    auto st = co_await GetLastSectorData(extent_ptr);
    if (!st.OK()) {
        LOG_ERROR("extent({}-{}) get last sector data error: {}",
                  extent_ptr->id.hi, extent_ptr->id.lo, st.String());
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    std::string last_sector_data = std::move(st.Value());

    ChunkEntry chunk = extent_ptr->chunks.back();
    std::vector<TmpBuffer> tmp_buf_vec;
    std::vector<iovec> tmp_io_vec;
    std::vector<ChunkEntry> chunks;

    auto free_chunks = [this](const std::vector<ChunkEntry>& chunk_vec) {
        int n = chunk_vec.size();
        for (int i = 1; i < n; i++) {
            free_chunks_.push(chunk_vec[i].index);
        }
    };

    chunks.push_back(chunk);
    offset = super_block_.DataOffset() + chunk.index * kChunkSize +
             ChunkPhyLen(chunk) - last_sector_data.size();
    int n = iov.size();
    for (int i = 0; i < n; i++) {
        s = HandleIO(chunk, (char*)iov[i].iov_base, iov[i].iov_len, (i == 0),
                     tmp_buf_vec, tmp_io_vec, last_sector_data);
        if (!s.OK()) {
            LOG_ERROR("device-{} handle {}th io error: {}", dev_ptr_->Name(), i,
                      s.String());
            free_chunks(chunks);
            co_return s;
        }
        chunks.back() = chunk;  // update the last chunk
        if (kChunkDataSize == chunk.len) {
            s = co_await dev_ptr_->Write(offset, std::move(tmp_io_vec));
            if (!s.OK()) {
                LOG_ERROR("device-{} write data error: {}", dev_ptr_->Name(),
                          s.String());
                free_chunks(chunks);
                co_return s;
            }
            if (i != n - 1) {
                if (free_chunks_.empty()) {
                    s.Set(ENOSPC);
                    LOG_ERROR("device-{} error: {}", dev_ptr_->Name(),
                              s.String());
                    free_chunks(chunks);
                    co_return s;
                }

                ChunkEntry new_chunk(free_chunks_.front());
                free_chunks_.pop();
                chunks.back().next = new_chunk.index;
                chunks.push_back(new_chunk);
                chunk = new_chunk;
                offset = super_block_.DataOffset() + kChunkSize * chunk.index;
            }
            tmp_buf_vec.clear();
            tmp_io_vec.clear();
        }
    }

    if (!tmp_io_vec.empty()) {
        s = co_await dev_ptr_->Write(offset, std::move(tmp_io_vec));
        if (!s.OK()) {
            LOG_ERROR("device-{} write data error: {}", dev_ptr_->Name(),
                      s.String());
            free_chunks(chunks);
            co_return s;
        }
    }

    // save meta
    std::vector<seastar::future<Status<>>> fu_vec;
    for (int i = chunks.size() - 1; i >= 0; i--) {
        auto fu = journal_ptr_->SaveChunk(chunks[i]);
        fu_vec.emplace_back(std::move(fu));
    }
    auto res = co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    for (auto& r : res) {
        if (!r.OK()) {
            s.Set(r.Code(), r.Reason());
            LOG_ERROR("device-{} save chunk meta error: {}", dev_ptr_->Name(),
                      s.String());
            free_chunks(chunks);
            co_return s;
        }
    }

    if (last_sector_data.empty()) {
        last_sector_cache_.Erase(extent_ptr->index);
    } else {
        last_sector_cache_.Insert(extent_ptr->index, last_sector_data);
    }

    extent_ptr->len += chunks[0].len - extent_ptr->chunks.back().len;
    extent_ptr->chunks.back() = chunks[0];
    for (int i = 1; i < chunks.size(); i++) {
        used_ += kChunkSize;
        extent_ptr->chunks.push_back(chunks[i]);
        extent_ptr->len += chunks[i].len;
    }

    co_return s;
}

seastar::future<Status<>> Store::Write(ExtentPtr extent_ptr, uint64_t offset,
                                       std::vector<TmpBuffer> buffers) {
    std::vector<iovec> iov;
    int n = buffers.size();

    for (int i = 0; i < n; ++i) {
        iov.push_back({buffers[i].get_write(), buffers[i].size()});
    }

    auto s = co_await Write(extent_ptr, offset, std::move(iov));
    co_return s;
}

seastar::future<Status<std::vector<TmpBuffer>>> Store::Read(
    ExtentPtr extent_ptr, uint64_t off, size_t len) {
    Status<std::vector<TmpBuffer>> s;
    std::vector<TmpBuffer> result;

    if (off % kBlockDataSize) {
        LOG_ERROR("invalid arguments off={} is not align kBlockDataSize", off);
        s.Set(EINVAL);
        co_return s;
    }
    if (len == 0) {
        co_return s;
    }

    if (off + len > extent_ptr->len) {
        LOG_ERROR("len={} is out of range off={}, extent={}-{} extent len={}",
                  len, off, extent_ptr->id.hi, extent_ptr->id.lo,
                  extent_ptr->len);
        s.Set(ERANGE);
        co_return s;
    }

    uint64_t last_block_len = len % kBlockDataSize;
    auto chunks = extent_ptr->chunks;  // copy chunk meta
    size_t chunk_off = off % kChunkDataSize;

    for (int i = off / kChunkDataSize; i < chunks.size() && len > 0; ++i) {
        size_t chunk_len = chunks[i].len - chunk_off;
        uint64_t n = std::min(chunk_len, len);
        len -= n;

        // align up block data size
        n = (n + kBlockDataSize - 1) / kBlockDataSize * kBlockDataSize;
        if (n > chunk_len) {
            // read all chunk data
            n = chunk_len;
        }
        size_t phy_chunk_off = chunk_off / kBlockDataSize * kBlockSize +
                               chunk_off % kBlockDataSize;
        uint64_t phy_disk_off = super_block_.DataOffset() +
                                chunks[i].index * kChunkSize + phy_chunk_off;

        size_t buffer_len =
            n / kBlockDataSize * kBlockSize + n % kBlockDataSize;
        if (n % kBlockDataSize != 0) {
            buffer_len += 4;  // to save crc
        }
        size_t need_read_bytes = seastar::align_up(buffer_len, kSectorSize);
        auto buf = dev_ptr_->Get(need_read_bytes);

        auto st = co_await dev_ptr_->Read(phy_disk_off, buf.get_write(),
                                          need_read_bytes);
        if (!st) {
            LOG_ERROR(
                "read extent error: {}, extent={}-{} off={} chunk index={}",
                st.String(), extent_ptr->id.hi, extent_ptr->id.lo, off, i);
            s.Set(st.Code(), st.Reason());
            co_return s;
        }
        chunk_off = 0;
        if (st.Value() != need_read_bytes) {
            LOG_ERROR(
                "read extent error: unexpect bytes, extent={}-{} off={} chunk "
                "index={}",
                extent_ptr->id.hi, extent_ptr->id.lo, off, i);
            s.Set(ErrCode::ErrUnExpect, "read unexcept bytes");
            co_return s;
        }
        buf.trim(buffer_len);

        // check crc
        char* p = buf.get_write();
        for (char* p = buf.get_write(); p < buf.end(); p += kBlockSize) {
            size_t data_len = std::min(kBlockSize, (size_t)(buf.end() - p));
            uint32_t crc = crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char*>(p), data_len - 4);
            if (data_len != kBlockSize) {
                net::BigEndian::PutUint32(p + data_len - 4, chunks[i].crc);
            }
            uint32_t origin_crc = net::BigEndian::Uint32(p + data_len - 4);
            if (crc != origin_crc) {
                s.Set(ErrCode::ErrInvalidChecksum);
                co_return s;
            }
        }
        result.emplace_back(std::move(buf));
    }

    TmpBuffer& b = result.back();
    if (last_block_len) {  // modify the last block crc
        size_t last_block_n = b.size() & kBlockSizeMask;
        if (last_block_n == 0) {
            last_block_n = kBlockDataSize;
        } else {
            last_block_n -= 4;
        }
        size_t trim_len = last_block_n - last_block_len;
        b.trim(b.size() - trim_len);
        uint32_t crc = crc32_gzip_refl(0,
                                       reinterpret_cast<const unsigned char*>(
                                           b.end() - last_block_len - 4),
                                       last_block_len);
        net::BigEndian::PutUint32(b.get_write() + b.size() - 4, crc);
    }
    s.SetValue(std::move(result));

    co_return s;
}

seastar::future<Status<>> Store::RemoveExtent(ExtentID id) {
    Status<> s;
    auto iter = extents_.find(id);
    if (iter == extents_.end()) {
        s.Set(ErrCode::ErrExtentNotFound);
        co_return s;
    }
    auto extent_ptr = iter->second;
    extents_.erase(iter);

    if (!extent_ptr->mu.try_lock()) {
        s.Set(ErrCode::ErrExtentIsWriting);
        extents_[id] = extent_ptr;
        co_return s;
    }

    auto extent = extent_ptr->GetExtentEntry();
    extent.chunk_idx = -1;
    extent.id = ExtentID(0, 0);
    s = co_await journal_ptr_->SaveExtent(extent);
    if (!s.OK()) {
        extents_[id] = extent_ptr;
        extent_ptr->mu.unlock();
        co_return s;
    }
    last_sector_cache_.Erase(extent_ptr->index);
    extent_ptr->mu.unlock();
    co_return s;
}

ExtentPtr Store::GetExtent(const ExtentID& id) {
    auto it = extents_.find(id);
    if (it == extents_.end()) {
        return nullptr;
    }
    return it->second;
}

seastar::future<> Store::Close() {
    if (journal_ptr_) {
        co_await journal_ptr_->Close();
        journal_ptr_.release();
    }
    co_await dev_ptr_->Close();
    co_return;
}

}  // namespace stream
}  // namespace snail
