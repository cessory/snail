#include "raft_storage.h"

#include "util/logger.h"

namespace snail {
namespace stream {

static constexpr size_t kDefaultSnapshotNum = 10;

RaftStorage::RaftStorage(const raft::ConfState& cs, uint64_t applied,
                         std::unique_ptr<RaftWal> wal, Statemachine* sm)
    : cs_(cs),
      applied_(applied),
      wal_(std::move(wal)),
      sm_(sm),
      snapshot_cache_(kDefaultSnapshotNum) {}

seastar::future<Status<std::tuple<raft::HardState, raft::ConfState>>>
RaftStorage::InitialState() {
    Status<std::tuple<raft::HardState, raft::ConfState>> s;
    auto hs = wal_->GetHardState();
    auto cs = cs_;
    s.SetValue(std::make_tuple<raft::HardState, raft::ConfState>(
        std::move(hs), std::move(cs)));
    co_return s;
}

seastar::future<Status<>> RaftStorage::Save(std::vector<raft::EntryPtr> entries,
                                            raft::HardState hs) {
    return wal_->Save(std::move(entries), std::move(hs));
}

seastar::future<Status<std::vector<raft::EntryPtr>>> RaftStorage::Entries(
    uint64_t lo, uint64_t hi, size_t max_size) {
    return wal_->Entries(lo, hi, max_size);
}

seastar::future<Status<uint64_t>> RaftStorage::Term(uint64_t i) {
    return wal_->Term(i);
}

uint64_t RaftStorage::LastIndex() { return wal_->LastIndex(); }

uint64_t RaftStorage::FirstIndex() { return wal_->FirstIndex(); }

seastar::future<Status<>> RaftStorage::Release(uint64_t index) {
    return wal_->Release(index);
}

seastar::future<Status<>> RaftStorage::ApplySnapshot(uint64_t index,
                                                     uint64_t term) {
    auto s = co_await wal_->ApplySnapshot(index, term);
    if (!s) {
        co_return s;
    }
    applied_ = index;
    co_return s;
}

seastar::future<Status<raft::SnapshotPtr>> RaftStorage::Snapshot() {
    Status<raft::SnapshotPtr> s;
    SnapshotMetaPayload payload;
    raft::ConfState cs = cs_;

    for (auto it : raft_node_map_) {
        RaftNodePtr ptr = it.second;
        auto raft_node = payload.add_nodes();
        raft_node->set_id(ptr->id());
        raft_node->set_raft_host(ptr->raft_host());
        raft_node->set_raft_port(ptr->raft_port());
        raft_node->set_host(ptr->host());
        raft_node->set_port(ptr->port());
    }

    auto st = co_await sm_->CreateSnapshot();
    if (!st) {
        LOG_ERROR("create snapshot error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    SmSnapshotPtr sm_snap = st.Value();
    auto index = sm_snap->Index();
    auto st1 = co_await Term(index);
    if (!st1) {
        LOG_ERROR("get term error: {}", st1);
        s.Set(st1.Code(), st1.Reason());
        co_return s;
    }
    uint64_t term = st1.Value();

    seastar::sstring snap_name = sm_snap->Name();
    payload.set_name(snap_name.data(), snap_name.size());
    // add snapshot into cache
    snapshot_cache_.Insert(snap_name, sm_snap);

    // construct a raft snapshot to return
    raft::SnapshotPtr snap = raft::make_snapshot();
    raft::SnapshotMetadata meta;
    meta.set_conf_state(std::move(cs));
    meta.set_index(index);
    meta.set_term(term);
    Buffer buffer(payload.ByteSizeLong());
    payload.SerializeToArray(buffer.get_write(), buffer.size());
    snap->set_data(std::move(buffer));
    snap->set_metadata(std::move(meta));
    s.SetValue(snap);
    co_return s;
}

SmSnapshotPtr RaftStorage::GetSnapshot(const seastar::sstring& name) {
    auto val = snapshot_cache_.Get(name);
    if (val) {
        return *val;
    }
    return nullptr;
}

void RaftStorage::ReleaseSnapshot(const seastar::sstring& name) {
    snapshot_cache_.Erase(name);
}

void RaftStorage::AddRaftNode(RaftNodePtr raft_node) {
    raft_node_map_[raft_node->id()] = raft_node;
}

void RaftStorage::RemoveRaftNode(uint64_t node_id) {
    raft_node_map_.erase(node_id);
}

RaftNode RaftStorage::GetRaftNode(uint64_t id) {
    RaftNode node;
    auto it = raft_node_map_.find(id);
    if (it == raft_node_map_.end()) {
        return node;
    }

    node = *(it->second);
    return node;
}

void RaftStorage::UpdateRaftNodes(const std::vector<RaftNode>& nodes) {
    std::unordered_map<uint64_t, RaftNodePtr> node_map;
    for (int i = 0; i < nodes.size(); ++i) {
        RaftNode node = nodes[i];
        RaftNodePtr node_ptr = seastar::make_lw_shared(std::move(node));
        node_map[node_ptr->id()] = node_ptr;
    }
    raft_node_map_ = std::move(node_map);
}

void RaftStorage::SetApplied(uint64_t applied) { applied_ = applied; }

}  // namespace stream
}  // namespace snail