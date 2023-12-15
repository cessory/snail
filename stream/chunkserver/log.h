#pragma once

namespace snail {
namespace stream {

enum EntryType {
    Extent = 0,
    Chunk = 1,
};

class Log {
    DevicePtr dev_ptr_;
    const SuperBlock &super_;

    uint64_t version_ = 0;
    int current_log_pt_ = LOGA_PT;
    uint64_t offset_;  // current offset
    seastar::temporary_buffer<char> mem_;
    size_t mem_size_ = 0;
    std::map<uint32_t, ChunkEntry> chunk_mem_;
    std::map<uint32_t, ChunkEntry> immu_chunk_mem_;
    std::map<uint32_t, ExtentEntry> extent_mem_;
    std::map<uint32_t, ExtentEntry> immu_extent_mem_;

    struct worker_item {
        seastar::promise<Status<>> pr;
        std::variant<ExtentEntry, ChunkEntry> entry;
    };

    std::queue<worker_item *> queue_;
    seastar::condition_variable cv_;
    seastar::semaphore background_sem_;
    Status<> status_;

    seastar::future<> LoopRun();

    void ResetMem();
    seastar::future<Status<>> Flush();
    seastar::future<Status<>> BackgroundFlush();
    void AppendToMem(worker_item *item);
    void UpdateMem(worker_item *item);

   public:
    explicit Log(DevicePtr dev_ptr, const SuperBlock &super_block);

    seastar::future<Status<>> Recover(
        seastar::noncopyable_function<void(const ExtentEntry &)> &&f1,
        seastar::noncopyable_function<void(const ChunkEntry &)> &&f2);

    seastar::future<Status<>> SaveChunk(const ChunkEntry &chunk);

    seastar::future<Status<>> SaveExtent(const ExtentEntry &extent);

    seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail
