#pragma once

#include <map>
#include <optional>
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <variant>

#include "device.h"
#include "super_block.h"
#include "types.h"
#include "util/status.h"

namespace snail {
namespace stream {

class Journal {
    DevicePtr dev_ptr_;
    SuperBlock super_;

    bool init_ = false;
    uint64_t version_ = 0;
    int current_pt_ = JOURNALA_PT;
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
    seastar::gate gate_;
    std::optional<seastar::future<>> loop_fu_;

    seastar::future<> LoopRun();

    seastar::future<Status<>> SaveImmuChunks();
    seastar::future<Status<>> SaveImmuExtents();

    seastar::future<Status<>> BackgroundFlush(uint64_t ver,
                                              uint64_t header_offset);

    void UpdateMem(const std::variant<ExtentEntry, ChunkEntry> &entry);

   public:
    explicit Journal(DevicePtr dev_ptr, const SuperBlock &super_block);

    seastar::future<Status<>> Init(
        seastar::noncopyable_function<Status<>(const ExtentEntry &)> &&f1,
        seastar::noncopyable_function<Status<>(const ChunkEntry &)> &&f2);

    seastar::future<Status<>> SaveChunk(ChunkEntry chunk);

    seastar::future<Status<>> SaveExtent(ExtentEntry extent);

    seastar::future<> Close();
};

using JournalPtr = seastar::lw_shared_ptr<Journal>;

}  // namespace stream
}  // namespace snail
