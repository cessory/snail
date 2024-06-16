#pragma once

namespace snail {
namespace stream {

using RaftNodePtr = seastar::lw_shared_ptr<RaftNode>;

class RaftStorage : public raft::Storage {
    raft::ConfState cs_;
    uint64_t applied_;
    std::unique_ptr<RaftWal> wal_;
    raft::Statemachine* sm_;
    std::unordered_map<uint64_t, RaftNodePtr> raft_node_map_;
    LRUCache<seastar::sstring, raft::SmSnapshotPtr> snapshot_cache_;

   public:
    explicit RaftStorage(const raft::ConfState& cs, uint64_t applied,
                         std::unique_ptr<RaftWal> wal, raft::Statemachine* sm);

    seastar::future<Status<std::tuple<raft::HardState, raft::ConfState>>>
    InitialState() override;

    seastar::future<Status<>> Save(std::vector<raft::EntryPtr> entries,
                                   raft::HardState hs);

    seastar::future<Status<std::vector<raft::EntryPtr>>> Entries(
        uint64_t lo, uint64_t hi, size_t max_size) override;

    seastar::future<Status<uint64_t>> Term(uint64_t i) override;

    uint64_t LastIndex() override;

    uint64_t FirstIndex() override;

    seastar::future<Status<>> Release(uint64_t index);

    seastar::future<Status<>> ApplySnapshot(uint64_t index, uint64_t term);

    seastar::future<Status<raft::SnapshotPtr>> Snapshot() override;

    void SetConfState(const raft::ConfState& cs) { cs_ = cs; }

    raft::SmSnapshotPtr GetSnapshot(const seastar::sstring& name);

    void ReleaseSnapshot(const seastar::sstring& name);

    void AddRaftNode(RaftNodePtr raft_node);

    void RemoveRaftNode(uint64_t node_id);

    void UpdateRaftNodes(const std::vector<RaftNode>& nodes);

    void SetApplied(uint64_t applied);

    uint64_t Applied() const { return applied_; }
};

}  // namespace stream
}  // namespace snail
