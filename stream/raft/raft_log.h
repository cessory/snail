#pragma once
#include <seastar/core/shared_ptr.hh>

#include "log_unstable.h"
#include "storage.h"
#include "util/status.h"
#include "util/util.h"

namespace snail {
namespace raft {

class RaftLog;
using RaftLogPtr = seastar::lw_shared_ptr<RaftLog>;

class RaftLog {
    SNAIL_PRIVATE
    seastar::shared_ptr<Storage> storage_;
    Unstable unstable_;
    uint64_t committed_;
    uint64_t applied_;
    uint64_t max_next_ents_size_;
    uint64_t group_;
    uint64_t id_;

    explicit RaftLog(seastar::shared_ptr<Storage> storage,
                     uint64_t max_next_ents_size, uint64_t group = 0,
                     uint64_t id = 1) noexcept;

    seastar::future<uint64_t> FindConflict(const EntryPtr *ents, size_t n);

   public:
    static RaftLogPtr MakeRaftLog(seastar::shared_ptr<Storage> storage,
                                  uint64_t max_next_ents_size,
                                  uint64_t group = 0, uint64_t id = 1) noexcept;

    seastar::future<std::tuple<uint64_t, bool>> MaybeAppend(
        uint64_t index, uint64_t log_term, uint64_t committed,
        std::vector<EntryPtr> ents);

    uint64_t Append(const std::vector<EntryPtr> &ents);

    seastar::future<uint64_t> FindConflictByTerm(uint64_t index, uint64_t term);

    std::vector<EntryPtr> UnstableEntries();

    SnapshotPtr UnstableSnapshot() { return unstable_.snapshot(); }

    seastar::future<std::vector<EntryPtr>> NextEnts();

    bool HasNextEnts();

    bool HasPendingSnapshot();

    seastar::future<Status<SnapshotPtr>> Snapshot();

    uint64_t FirstIndex();
    uint64_t LastIndex();
    void CommitTo(uint64_t commit);

    void AppliedTo(uint64_t i);
    void StableTo(uint64_t i, uint64_t t);
    void StableSnapTo(uint64_t i);

    seastar::future<Status<uint64_t>> Term(uint64_t i);
    seastar::future<uint64_t> LastTerm();

    seastar::future<Status<std::vector<EntryPtr>>> Entries(uint64_t i,
                                                           uint64_t max_size);

    seastar::future<std::vector<EntryPtr>> AllEntries();

    seastar::future<bool> IsUpToDate(uint64_t lasti, uint64_t term);

    seastar::future<bool> MatchTerm(uint64_t i, uint64_t term);

    seastar::future<bool> MaybeCommit(uint64_t max_index, uint64_t term);

    void Restore(SnapshotPtr s);

    seastar::future<Status<std::vector<EntryPtr>>> Slice(uint64_t lo,
                                                         uint64_t hi,
                                                         uint64_t max_size);

    uint64_t ZeroTermOnErrCompacted(Status<uint64_t> &s);

    uint64_t committed() const { return committed_; }

    void set_committed(uint64_t v) { committed_ = v; }

    uint64_t applied() const { return applied_; }
};

}  // namespace raft
}  // namespace snail
