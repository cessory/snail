#pragma once
#include <spdlog/spdlog.h>

#include <deque>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <tuple>
#include <vector>

#include "raft_proto.h"
#include "util/status.h"
#include "util/util.h"

namespace snail {
namespace raft {

class Storage {
   public:
    // InitialState returns the saved HardState and ConfState information.
    virtual seastar::future<Status<std::tuple<HardState, ConfState>>>
    InitialState() = 0;

    // Entries returns a slice of log entries in the range [lo,hi).
    // MaxSize limits the total size of the log entries returned, but
    // Entries returns at least one entry if any.
    virtual seastar::future<Status<std::vector<EntryPtr>>> Entries(
        uint64_t lo, uint64_t hi, size_t max_size) = 0;

    // Term returns the term of entry i, which must be in the range
    // [FirstIndex()-1, LastIndex()]. The term of the entry before
    // FirstIndex is retained for matching purposes even though the
    // rest of that entry may not be available.
    virtual seastar::future<Status<uint64_t>> Term(uint64_t i) = 0;

    // LastIndex returns the index of the last entry in the log.
    virtual uint64_t LastIndex() = 0;

    // FirstIndex returns the index of the first log entry that is
    // possibly available via Entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    virtual uint64_t FirstIndex() = 0;

    // Snapshot returns the most recent snapshot.
    // If snapshot is temporarily unavailable, it should return
    // ErrSnapshotTemporarilyUnavailable, so raft state machine could know that
    // Storage needs some time to prepare snapshot and call Snapshot later.
    virtual seastar::future<Status<SnapshotPtr>> Snapshot() = 0;
};

using StoragePtr = seastar::shared_ptr<Storage>;

class MemoryStorage : public Storage {
#ifdef RAFT_UT_TEST
   public:
#endif
    HardState hs_;
    SnapshotPtr snapshot_;
    std::deque<EntryPtr> ents_;

    uint64_t firstIndex() noexcept;
    uint64_t lastIndex() noexcept;

   public:
    explicit MemoryStorage()
        : hs_(), snapshot_(make_snapshot()), ents_(1, make_entry()) {}
    seastar::future<Status<std::tuple<HardState, ConfState>>> InitialState()
        override;
    seastar::future<Status<std::vector<EntryPtr>>> Entries(
        uint64_t lo, uint64_t hi, size_t max_size) override;

    seastar::future<Status<uint64_t>> Term(uint64_t i) override;

    uint64_t LastIndex() override;
    uint64_t FirstIndex() override;
    seastar::future<Status<SnapshotPtr>> Snapshot() override;

    void SetHardState(const HardState &hs) { hs_ = hs; }
    Status<> ApplySnapshot(SnapshotPtr s);
    Status<SnapshotPtr> CreateSnapshot(uint64_t i, const ConfState *cs,
                                       seastar::temporary_buffer<char> data);

    Status<> Compact(uint64_t compact_index);

    Status<> Append(const std::vector<EntryPtr> &entries);
};

}  // namespace raft
}  // namespace snail
