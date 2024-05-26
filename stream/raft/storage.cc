#include "storage.h"

#include <seastar/core/coroutine.hh>

#include "util/logger.h"

namespace snail {
namespace raft {

uint64_t MemoryStorage::firstIndex() noexcept { return ents_[0]->index() + 1; }

uint64_t MemoryStorage::lastIndex() noexcept {
    return ents_[0]->index() + ents_.size() - 1;
}

seastar::future<Status<std::tuple<HardState, ConfState>>>
MemoryStorage::InitialState() {
    Status<std::tuple<HardState, ConfState>> s;
    auto cs = snapshot_->metadata().conf_state();
    auto hs = hs_;
    s.SetValue(std::make_tuple(hs, cs));
    return seastar::make_ready_future<Status<std::tuple<HardState, ConfState>>>(
        std::move(s));
}

seastar::future<Status<std::vector<EntryPtr>>> MemoryStorage::Entries(
    uint64_t lo, uint64_t hi, size_t max_size) {
    Status<std::vector<EntryPtr>> s;
    std::vector<EntryPtr> ents;
    uint64_t offset = ents_[0]->index();
    if (lo <= offset) {
        s.Set(ErrCode::ErrRaftCompacted);
        co_return s;
    }

    if (hi > lastIndex() + 1) {
        LOG_FATAL_THROW("entries' hi({}) is out of bound lastindex({})", hi,
                        lastIndex());
    }

    if (ents_.size() == 1) {
        s.Set(ErrCode::ErrRaftUnavailable);
        co_return s;
    }

    uint64_t sum = 0;
    for (size_t i = lo - offset; i < hi - offset; i++) {
        sum += ents_[i]->ByteSize();
        if (sum > max_size && ents.size() > 0) {
            break;
        }
        ents.push_back(ents_[i]);
    }
    s.SetValue(std::move(ents));
    co_return s;
}

seastar::future<Status<uint64_t>> MemoryStorage::Term(uint64_t i) {
    Status<uint64_t> s;
    uint64_t offset = ents_[0]->index();

    if (i < offset) {
        s.Set(ErrCode::ErrRaftCompacted);
        co_return s;
    }
    if (i - offset >= ents_.size()) {
        s.Set(ErrCode::ErrRaftUnavailable);
        co_return s;
    }
    s.SetValue(ents_[i - offset]->term());
    co_return s;
}

uint64_t MemoryStorage::LastIndex() { return lastIndex(); }
uint64_t MemoryStorage::FirstIndex() { return firstIndex(); }

seastar::future<Status<SnapshotPtr>> MemoryStorage::Snapshot() {
    Status<SnapshotPtr> s;
    s.SetValue(snapshot_);
    co_return s;
}

Status<> MemoryStorage::ApplySnapshot(SnapshotPtr s) {
    Status<> st;
    if (snapshot_->metadata().index() >= s->metadata().index()) {
        st.Set(ErrCode::ErrRaftSnapOutOfData);
        return st;
    }
    snapshot_ = s;
    ents_.clear();
    auto ent = make_entry();
    ent->set_term(s->metadata().term());
    ent->set_index(s->metadata().index());
    ents_.push_back(ent);
    return st;
}

Status<SnapshotPtr> MemoryStorage::CreateSnapshot(
    uint64_t i, const ConfState* cs, seastar::temporary_buffer<char> data) {
    Status<SnapshotPtr> s;
    if (i <= snapshot_->metadata().index()) {
        s.Set(ErrCode::ErrRaftSnapOutOfData);
        return s;
    }

    auto offset = ents_[0]->index();
    if (i > lastIndex()) {
        LOG_FATAL_THROW("snapshot {} is out of bound lastindex({})", i,
                        lastIndex());
    }
    snapshot_->metadata().set_index(i);
    snapshot_->metadata().set_term(ents_[i - offset]->term());
    if (cs) {
        snapshot_->metadata().set_conf_state(*cs);
    }
    snapshot_->set_data(std::move(data));
    s.SetValue(snapshot_);
    return s;
}

Status<> MemoryStorage::Compact(uint64_t compact_index) {
    Status<> s;
    auto offset = ents_[0]->index();
    if (compact_index <= offset) {
        s.Set(ErrCode::ErrRaftCompacted);
        return s;
    }

    if (compact_index > lastIndex()) {
        LOG_FATAL_THROW("compact {} is out of bound lastindex({})",
                        compact_index, lastIndex());
    }

    for (size_t i = 0; i < compact_index - offset; i++) {
        ents_.pop_front();
    }
    return s;
}

Status<> MemoryStorage::Append(const std::vector<EntryPtr>& entries) {
    Status<> s;
    if (entries.empty()) {
        return s;
    }

    auto first = firstIndex();
    auto last = entries[0]->index() + entries.size() - 1;
    if (last < first) {
        return s;
    }

    uint64_t starti = 0;
    if (first > entries[0]->index()) {
        starti = first - entries[0]->index();
    }

    auto offset = entries[starti]->index() - ents_[0]->index();
    auto n = ents_.size();
    if (n > offset) {
        for (uint64_t i = n - 1; i >= offset; i--) {
            ents_.pop_back();
        }
        for (uint64_t i = starti; i < entries.size(); i++) {
            ents_.push_back(entries[i]);
        }
    } else if (n == offset) {
        for (uint64_t i = starti; i < entries.size(); i++) {
            ents_.push_back(entries[i]);
        }
    } else {
        LOG_FATAL_THROW("missing log entry [last: {}, append at: {}]",
                        lastIndex(), entries[starti]->index());
    }
    return s;
}

}  // namespace raft
}  // namespace snail
