#pragma once
#include "raft/raft_proto.h"
#include "rocksdb/db.h"
#include "util/async_thread.h"

namespace snail {
namespace stream {

class RaftWalFactory;

class RaftWal {
    struct Snapshot {
        uint64_t index = 0;
        uint64_t term = 0;
    };
    RaftWalFactory* factory_;
    uint64_t group_ = 0;
    raft::HardState hs_;
    RaftWal::Snapshot snapshot_;
    uint64_t last_index_ = 0;

    explicit RaftWal(RaftWalFactory* factory, uint64_t group);

    seastar::future<Status<>> LoadHardState();

    seastar::future<Status<>> LoadSnapshot();

    seastar::future<Status<>> LoadLastIndex();

    seastar::future<Status<>> Load();

    friend class RaftWalFactory;

   public:
    raft::HardState GetHardState();

    seastar::future<Status<>> Save(std::vector<raft::EntryPtr> entries,
                                   raft::HardState hs, bool sync = false);

    uint64_t FirstIndex();

    uint64_t LastIndex();

    seastar::future<Status<std::vector<raft::EntryPtr>>> Entries(
        uint64_t lo, uint64_t hi, size_t max_size);

    seastar::future<Status<uint64_t>> Term(uint64_t index);

    seastar::future<Status<>> Release(uint64_t index);

    seastar::future<Status<>> ApplySnapshot(uint64_t index, uint64_t term);
};

class RaftWalFactory {
    AsyncThread worker_thread_;
    std::string path_;
    rocksdb::DB* db_ = nullptr;

    explicit RaftWalFactory(const std::string& path);

    seastar::future<Status<>> OpenDB();

    friend class RaftWal;

   public:
    ~RaftWalFactory();

    static seastar::future<Status<std::unique_ptr<RaftWalFactory>>> Create(
        std::string path);

    seastar::future<Status<std::unique_ptr<RaftWal>>> OpenRaftWal(
        uint64_t group);
};

}  // namespace stream
}  // namespace snail
