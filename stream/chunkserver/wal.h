#pragma once

namespace snail {
namespace stream {

#define BLOCK_ENTRY_SIZE 16
#define CHUNK_ENTRY_SIZE 24

enum EntryType {
    Chunk = 0,
    Block = 1,
};

struct BlockEntry {
    uint32_t index;
    uint32_t next;
    uint32_t len;
    uint32_t crc;

    int MarshalTo(char *b) const;
};

struct ChunkEntry {
    uint32_t index;
    ChunkID id;
    uint32_t block_idx;

    int MarshalTo(char *b) const;
};

class Wal {
    DevicePtr dev_ptr_;
    uint64_t block_meta_offset_;
    uint64_t chunk_meta_offset_;

    uint64_t version_;
    int cur_pt_id_ = 0;
    Partition pt_[2];
    uint64_t offset_;  // current offset
    seastar::temporary_buffer<char> mem_;
    size_t real_mem_size_;
    bool with_header_;
    std::map<uint32_t, BlockEntry> bmem_;
    std::map<uint32_t, BlockEntry> immu_bmem_;
    std::map<uint32_t, ChunkEntry> cmem_;
    std::map<uint32_t, ChunkEntry> immu_cmem_;

    struct worker_item {
        seastar::promise<Status<>> pr;
        std::vector<std::variant<ChunkEntry, BlockEntry>> entries;
        size_t Size();
    };
    std::queue<worker_item *> queue_;
    seastar::semaphore limit_;
    seastar::condition_variable cv_;

    seastar::future<> LoopRun();

    void ResetMem();
    seastar::future<Status<>> Flush();
    seastar::future<Status<>> BackgroundFlush();
    void AppendToMem(worker_item *item);

   public:
    explicit Wal(DevicePtr dev_ptr);

    seastar::future<Status<>> Recover();

    seastar::future<Status<>> SaveBlocks(std::vector<BlockEntry> block);

    seastar::future<Status<>> SaveChunk(ChunkEntry chunk);

    seastar::future<Status<>> SaveChunkAndBlock(ChunkEntry chunk,
                                                BlockEntry block);
};

}  // namespace stream
}  // namespace snail
