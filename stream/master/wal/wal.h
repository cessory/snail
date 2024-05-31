#pragma once
#include "raft/raft_proto.h"
#include "rocksdb/db.h"
#include "util/async_thread.h"

namespace snail {
namespace stream {

class RaftWal {
    AsyncThread worker_thread_;
    std::string path_;
    HardState hs_;
    uint64_t compacted_index_ = 0;
    uint64_t compacted_term_ = 0;
    uint64_t last_index_ = 0;

    rocksdb::DB* db_ = nullptr;

    explicit RaftWal(const std::string& path);

    bool poll();
    void work();
    seastar::future<Status<>> OpenDB();
    seastar::future<Status<>> Load();

   public:
    ~RaftWal();

    static seastar::future<Status<std::unique_ptr<RaftWal>>> OpenRaftWal(
        const std::string& path);

    HardState GetHardState();

    seastar::future<Status<>> Save(std::vector<EntryPtr> entries, HardState hs);

    uint64_t FirstIndex();

    uint64_t LastIndex();

    seastar::future<Status<uint64_t>> Term(uint64_t index);

    seastar::future<Status<>> Release(uint64_t index);

    seastar::future<Status<>>

        seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail
