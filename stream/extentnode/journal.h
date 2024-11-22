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
class Store;

class Journal {
#ifdef EXTENTNODE_UT_TEST
   public:
#else
   private:
#endif
    DevicePtr dev_ptr_;
    Store *store_;
    ExtentEntry extent_;
    std::optional<ChunkEntry> chunk_;  // current chunk for write journal
    std::vector<ChunkEntry> archive_chunks_;
    seastar::shared_mutex extent_mu_;
    seastar::semaphore chunk_sem_;

    bool init_ = false;
    std::map<uint32_t, ChunkEntry> mutable_chunks_;
    std::map<uint32_t, ExtentEntry> mutable_extents_;
    std::queue<std::map<uint32_t, ChunkEntry>> immutable_chunks_;
    std::queue<std::map<uint32_t, ExtentEntry>> immutable_extents_;

    struct worker_item {
        seastar::promise<Status<>> pr;
        std::variant<ExtentEntry, ChunkEntry> entry;
        size_t Size() const {
            return entry.index() == 0 ? kExtentEntrySize : kChunkEntrySize;
        }

        void MarshalTo(char *b);
    };

    Status<> status_;
    seastar::gate gate_;
    std::optional<seastar::future<>> loop_fu_;
    std::optional<seastar::future<Status<>>> flush_fu_;
    std::queue<worker_item *> queue_;
    seastar::condition_variable cv_;
    seastar::condition_variable flush_cv_;

    seastar::future<> LoopRun();

    seastar::future<Status<int>> SaveImmuChunks(
        std::queue<std::map<uint32_t, ChunkEntry>> immutable_chunks);

    seastar::future<Status<int>> SaveImmuExtents(
        std::queue<std::map<uint32_t, ExtentEntry>> immutable_extents);

    seastar::future<Status<>> BackgroundFlush(bool once);

    void UpdateMem(const std::variant<ExtentEntry, ChunkEntry> &entry);

    seastar::future<Status<>> AllocaChunk();

    seastar::future<Status<>> ReleaseChunk(uint32_t n);

    seastar::future<Status<>> Append(size_t pos, const char *p, size_t n);

   public:
    explicit Journal(DevicePtr dev_ptr, Store *store, size_t max_size);

    seastar::future<Status<>> Init(
        ExtentPtr extent,
        seastar::noncopyable_function<Status<>(const ExtentEntry &)> &&f1,
        seastar::noncopyable_function<Status<>(const ChunkEntry &)> &&f2);

    seastar::future<Status<>> SaveChunk(ChunkEntry chunk);

    seastar::future<Status<>> SaveExtent(ExtentEntry extent);

    seastar::future<> Close();
};

using JournalPtr = seastar::lw_shared_ptr<Journal>;

}  // namespace stream
}  // namespace snail
