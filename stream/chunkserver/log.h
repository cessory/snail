#pragma once

#include "device.h"
#include "super_block.h"
#include "types.h"
#include "util/status.h"

namespace snail {
namespace stream {

enum EntryType {
    Extent = 0,
    Chunk = 1,
};

class Log {
    DevicePtr dev_ptr_;
    const SuperBlock &super_;

    bool init_ = false;
    uint64_t version_ = 0;
    int current_log_pt_ = LOGA_PT;
    uint64_t offset_;  // current offset

    std::map<uint32_t, ChunkEntry> chunk_mem_;
    std::map<uint32_t, ChunkEntry> immu_chunk_mem_;
    std::map<uint32_t, ExtentEntry> extent_mem_;
    std::map<uint32_t, ExtentEntry> immu_extent_mem_;

    struct worker_item {
        seastar::promise<Status<>> pr;
        std::variant<ExtentEntry, ChunkEntry> entry;
        size_t Size() const {
            return entry.index() == 0 ? kExtentEntrySize : kChunkEntrySize;
        }

        void MarshalTo(uint64_t ver, char *b);
    };

    std::queue<worker_item *> queue_;
    seastar::condition_variable cv_;
    Status<> status_;
    bool closed_ = false;
    std::optional<seastar::future<>> loop_fu_;

    seastar::future<> LoopRun();

    seastar::future<Status<>> SaveImmuChunks();
    seastar::future<Status<>> SaveImmuExtents();

    seastar::future<Status<>> BackgroundFlush(uint64_t ver,
                                              uint64_t header_offset);

    void UpdateMem(worker_item *item);

   public:
    explicit Log(DevicePtr dev_ptr, const SuperBlock &super_block);

    seastar::future<Status<>> Init(
        seastar::noncopyable_function<void(const ExtentEntry &)> &&f1,
        seastar::noncopyable_function<void(const ChunkEntry &)> &&f2);

    seastar::future<Status<>> SaveChunk(const ChunkEntry &chunk);

    seastar::future<Status<>> SaveExtent(const ExtentEntry &extent);

    seastar::future<> Close();
};

using LogPtr = seastar::lw_shared_ptr<Log>;

}  // namespace stream
}  // namespace snail
