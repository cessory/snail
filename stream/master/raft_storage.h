#pragma once

namespace snail {
namespace stream {

class RaftStorage : public raft::Storage {
    raft::ConfState cs_;
    RaftWal* wal_;
    LRUCache<uint64_t, raft::SmSnapshotPtr> snapshots_;

   public:
    explicit RaftStorage(const raft::ConfState& cs, RaftWal* wal);

    seastar::future<Status<std::tuple<raft::HardState, raft::ConfState>>>
    InitialState() override;

    seastar::future<Status<std::vector<raft::EntryPtr>>> Entries(
        uint64_t lo, uint64_t hi, size_t max_size) override;

    seastar::future<Status<uint64_t>> Term(uint64_t i) override;

    uint64_t LastIndex() override;

    uint64_t FirstIndex() override;

    seastar::future<Status<raft::SnapshotPtr>> Snapshot() override;

    void SetConfState(const raft::ConfState& cs) { cs_ = cs; }
};

}  // namespace stream
}  // namespace snail
