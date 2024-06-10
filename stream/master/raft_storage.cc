#include "raft_storage.h"

namespace snail {
namespace stream {

static constexpr size_t kDefaultSnapshotNum = 10;

RaftStorage::RaftStorage(const raft::ConfState& cs, RaftWal* wal)
    : cs_(cs), wal_(wal), snapshots_(kDefaultSnapshotNum) {}

seastar::future<Status<std::tuple<raft::HardState, raft::ConfState>>>
RaftStorage::InitialState() {
    Status<std::tuple<raft::HardState, raft::ConfState>> s;
    auto hs = wal_->GetHardState();
    auto cs = cs_;
    s.Set(std::make_tuple<raft::HardState, raft::ConfState>(hs, cs));
    co_return s;
}

seastar::future<Status<std::vector<raft::EntryPtr>>> RaftStorage::Entries(
    uint64_t lo, uint64_t hi, size_t max_size) {
    return wal_->Entries(lo, hi, max_size);
}

}  // namespace stream
}  // namespace snail
