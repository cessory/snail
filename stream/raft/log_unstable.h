#pragma once
#include <deque>
#include <tuple>

#include "raft_proto.h"

namespace snail {
namespace raft {

class Unstable {
#ifdef RAFT_UT_TEST
   public:
#endif
    uint64_t group_;
    uint64_t id_;
    SnapshotPtr snapshot_;
    std::deque<EntryPtr> entries_;
    uint64_t offset_;

   public:
    explicit Unstable(uint64_t group = 0, uint64_t id = 1)
        : offset_(0), group_(group), id_(id) {}

    std::tuple<uint64_t, bool> MaybeFirstIndex();

    std::tuple<uint64_t, bool> MaybeLastIndex();

    std::tuple<uint64_t, bool> MaybeTerm(uint64_t i);

    void StableTo(uint64_t i, uint64_t t);

    void StableSnapTo(uint64_t i);

    void Restore(SnapshotPtr s);

    void TruncateAndAppend(const std::vector<EntryPtr>& ents);

    std::vector<EntryPtr> Slice(uint64_t lo, uint64_t hi);

    std::vector<EntryPtr> Entries();

    void set_offset(uint64_t v);

    uint64_t offset() { return offset_; }

    SnapshotPtr snapshot() { return snapshot_; }
};

}  // namespace raft
}  // namespace snail
