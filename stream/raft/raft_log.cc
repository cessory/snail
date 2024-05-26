#include "raft_log.h"

#include <seastar/core/coroutine.hh>

#include "util/logger.h"

namespace snail {
namespace raft {

RaftLog::RaftLog(seastar::shared_ptr<Storage> storage,
                 uint64_t max_next_ents_size, uint64_t group,
                 uint64_t id) noexcept
    : storage_(storage),
      unstable_(group, id),
      committed_(storage->FirstIndex() - 1),
      applied_(storage->FirstIndex() - 1),
      max_next_ents_size_(max_next_ents_size),
      group_(group),
      id_(id) {
    unstable_.set_offset(storage->LastIndex() + 1);
}
seastar::future<uint64_t> RaftLog::FindConflict(const EntryPtr *ents,
                                                size_t n) {
    for (size_t i = 0; i < n; i++) {
        auto index = ents[i]->index();
        auto term = ents[i]->term();
        auto r = co_await MatchTerm(index, term);
        if (r) {
            continue;
        }
        auto last_index = LastIndex();
        if (index <= last_index) {
            auto s = co_await Term(index);
            LOG_INFO(
                "[{}-{}] found conflict at index {} [existing term: {}, "
                "conflicting term: {}]",
                group_, id_, index, ZeroTermOnErrCompacted(s), term);
        }
        co_return index;
    }
    co_return 0;
}

RaftLogPtr RaftLog::MakeRaftLog(seastar::shared_ptr<Storage> storage,
                                uint64_t max_next_ents_size, uint64_t group,
                                uint64_t id) noexcept {
    return seastar::make_lw_shared<RaftLog>(
        RaftLog(storage, max_next_ents_size, group, id));
}

seastar::future<std::tuple<uint64_t, bool>> RaftLog::MaybeAppend(
    uint64_t index, uint64_t log_term, uint64_t committed,
    std::vector<EntryPtr> ents) {
    auto r = co_await MatchTerm(index, log_term);
    if (r) {
        auto lastnewi = index + ents.size();
        auto ci = co_await FindConflict(ents.data(), ents.size());
        if (ci == 0) {
        } else if (ci <= committed_) {
            LOG_FATAL_THROW(
                "[{}-{}] entry {} conflict with committed entry "
                "[committed({})]",
                group_, id_, ci, committed_);
        } else {
            std::vector<EntryPtr> tmp;
            for (size_t i = ci - index - 1; i < ents.size(); i++) {
                tmp.push_back(ents[i]);
            }
            Append(tmp);
        }
        CommitTo(std::min(committed, lastnewi));
        co_return std::make_tuple(lastnewi, true);
    }
    co_return std::make_tuple(0, false);
}

uint64_t RaftLog::Append(const std::vector<EntryPtr> &ents) {
    if (ents.empty()) {
        return LastIndex();
    }

    if (ents[0]->index() - 1 < committed_) {
        LOG_FATAL_THROW("[{}-{}] after({}) is out of range [committed({})]",
                        group_, id_, ents[0]->index() - 1, committed_);
    }

    unstable_.TruncateAndAppend(ents);
    return LastIndex();
}

seastar::future<uint64_t> RaftLog::FindConflictByTerm(uint64_t index,
                                                      uint64_t term) {
    auto li = LastIndex();
    if (index > li) {
        LOG_WARN(
            "[{}-{}] index({}) is out of range [0, lastIndex({})] in "
            "findConflictByTerm",
            group_, id_, index, li);
        co_return index;
    }
    for (;;) {
        auto s = co_await Term(index);
        if (!s || s.Value() <= term) {
            break;
        }
        index--;
    }
    co_return index;
}

std::vector<EntryPtr> RaftLog::UnstableEntries() { return unstable_.Entries(); }

seastar::future<std::vector<EntryPtr>> RaftLog::NextEnts() {
    auto first_index = FirstIndex();
    auto off = std::max(applied_ + 1, first_index);
    if (committed_ + 1 > off) {
        auto s = co_await Slice(off, committed_ + 1, max_next_ents_size_);
        if (!s) {
            LOG_FATAL_THROW(
                "[{}-{}] unexpected error when getting unapplied entries ({})",
                group_, id_, s);
        }
        co_return std::move(s.Value());
    }
    co_return std::vector<EntryPtr>();
}

bool RaftLog::HasNextEnts() {
    auto first_index = FirstIndex();
    return committed_ + 1 > std::max(applied_ + 1, first_index);
}

bool RaftLog::HasPendingSnapshot() {
    return unstable_.snapshot() &&
           unstable_.snapshot()->metadata().index() != 0;
}

seastar::future<Status<SnapshotPtr>> RaftLog::Snapshot() {
    Status<SnapshotPtr> s;
    auto snapshot = unstable_.snapshot();
    if (snapshot) {
        s.SetValue(snapshot);
        co_return s;
    }
    s = co_await storage_->Snapshot();
    co_return s;
}

seastar::future<Status<std::vector<EntryPtr>>> RaftLog::Entries(
    uint64_t i, uint64_t max_size) {
    Status<std::vector<EntryPtr>> s;

    if (i > LastIndex()) {
        co_return s;
    }
    s = co_await Slice(i, LastIndex() + 1, max_size);
    co_return s;
}

seastar::future<std::vector<EntryPtr>> RaftLog::AllEntries() {
    auto s =
        co_await Entries(FirstIndex(), std::numeric_limits<uint64_t>::max());
    if (s) {
        co_return std::move(s.Value());
    } else if (s.Code() == ErrCode::ErrRaftCompacted) {
        auto ents = co_await AllEntries();
        co_return std::move(ents);
    }
    LOG_FATAL_THROW("[{}-{}] all entries error: {}", group_, id_, s);
    co_return std::vector<EntryPtr>();
}

seastar::future<bool> RaftLog::IsUpToDate(uint64_t lasti, uint64_t term) {
    auto t = co_await LastTerm();
    co_return term > t || (term == t && lasti >= LastIndex());
}

seastar::future<bool> RaftLog::MaybeCommit(uint64_t max_index, uint64_t term) {
    auto s = co_await Term(max_index);
    if (max_index > committed_ && ZeroTermOnErrCompacted(s) == term) {
        CommitTo(max_index);
        co_return true;
    }
    co_return false;
}

void RaftLog::Restore(SnapshotPtr s) {
    LOG_INFO(
        "[{}-{}] log [committed={}, applied={}, unstable.offset={}, "
        "len(unstable.Entries)={}] starts to restore snapshot [index: "
        "{}, term: {}]",
        group_, id_, committed_, applied_, unstable_.offset(),
        unstable_.Entries().size(), s->metadata().index(),
        s->metadata().term());
    committed_ = s->metadata().index();
    unstable_.Restore(s);
}

uint64_t RaftLog::FirstIndex() {
    uint64_t index;
    bool ok;

    auto r = unstable_.MaybeFirstIndex();
    std::tie(index, ok) = r;

    if (ok) {
        return index;
    }

    return storage_->FirstIndex();
}

uint64_t RaftLog::LastIndex() {
    uint64_t index;
    bool ok;

    auto r = unstable_.MaybeLastIndex();
    std::tie(index, ok) = r;
    if (ok) {
        return index;
    }

    return storage_->LastIndex();
}

void RaftLog::CommitTo(uint64_t commit) {
    if (committed_ < commit) {
        auto last_index = LastIndex();
        if (last_index < commit) {
            LOG_FATAL_THROW(
                "[{}-{}] tocommit({}) is out of range [lastIndex({})]. Was the "
                "raft log corrupted, truncated, or lost?",
                group_, id_, commit, last_index);
        }
        committed_ = commit;
    }
}

void RaftLog::AppliedTo(uint64_t i) {
    if (i == 0) {
        return;
    }
    if (committed_ < i || i < applied_) {
        LOG_FATAL_THROW(
            "[{}-{}] applied({}) is out of range [prevApplied({}), "
            "committed({})]",
            group_, id_, i, applied_, committed_);
    }
    applied_ = i;
}

void RaftLog::StableTo(uint64_t i, uint64_t t) { unstable_.StableTo(i, t); }

void RaftLog::StableSnapTo(uint64_t i) { unstable_.StableSnapTo(i); }

seastar::future<Status<uint64_t>> RaftLog::Term(uint64_t i) {
    Status<uint64_t> s;

    auto first_index = FirstIndex();
    auto last_index = LastIndex();
    uint64_t dummy_index = first_index - 1;

    if (i < dummy_index || i > last_index) {
        s.SetValue(0);
        co_return s;
    }

    auto r = unstable_.MaybeTerm(i);
    if (std::get<1>(r)) {
        s.SetValue(std::get<0>(r));
        co_return s;
    }

    s = co_await storage_->Term(i);
    if (s) {
        co_return s;
    } else if (s.Code() != ErrCode::ErrRaftCompacted &&
               s.Code() != ErrCode::ErrRaftUnavailable) {
        LOG_FATAL_THROW("[{}-{}] storage::Term error: {}", group_, id_, s);
    }
    co_return s;
}

seastar::future<uint64_t> RaftLog::LastTerm() {
    auto last_index = LastIndex();
    auto s = co_await Term(last_index);
    if (!s) {
        LOG_FATAL_THROW(
            "[{}-{}] unexpected error when getting the last term ({})", group_,
            id_, s);
    }
    co_return s.Value();
}

seastar::future<bool> RaftLog::MatchTerm(uint64_t i, uint64_t term) {
    auto s = co_await Term(i);
    if (!s) {
        co_return false;
    }
    co_return s.Value() == term;
}

seastar::future<Status<std::vector<EntryPtr>>> RaftLog::Slice(
    uint64_t lo, uint64_t hi, uint64_t max_size) {
    Status<std::vector<EntryPtr>> s;

    if (lo > hi) {
        LOG_FATAL_THROW("[{}-{}] invalid slice {} > {}", group_, id_, lo, hi);
    }
    auto fi = FirstIndex();
    if (lo < fi) {
        s.Set(ErrCode::ErrRaftCompacted);
        co_return s;
    }

    auto last_index = LastIndex();
    if (hi > last_index + 1) {
        LOG_FATAL_THROW("[{}-{}] slice[{},{}) out of bound [{},{}]", group_,
                        id_, lo, hi, fi, last_index);
    }

    if (lo == hi) {
        co_return s;
    }

    std::vector<EntryPtr> ents;
    if (lo < unstable_.offset()) {
        s = co_await storage_->Entries(lo, std::min(hi, unstable_.offset()),
                                       max_size);
        if (s.Code() == ErrCode::ErrRaftCompacted) {
            co_return s;
        } else if (s.Code() == ErrCode::ErrRaftUnavailable) {
            LOG_FATAL_THROW(
                "[{}-{}] entries[{}:{}) is unavailable from storage", group_,
                id_, lo, std::min(hi, unstable_.offset()));
        } else if (!s) {
            LOG_FATAL_THROW("[{}-{}] get entries from storage error: {}",
                            group_, id_, s);
        }

        if (s.Value().size() < std::min(hi, unstable_.offset()) - lo) {
            co_return std::move(s);
        }

        ents = std::move(s.Value());
    }

    if (hi > unstable_.offset()) {
        auto unstable_ents =
            unstable_.Slice(std::max(lo, unstable_.offset()), hi);
        for (auto ent : unstable_ents) {
            ents.push_back(ent);
        }
    }

    if (ents.empty()) {
        co_return std::move(s);
    }
    uint64_t size = ents[0]->ByteSize();
    for (size_t i = 1; i < ents.size(); i++) {
        size += ents[i]->ByteSize();
        if (size > max_size) {
            ents.resize(i);
            break;
        }
    }
    s.SetValue(std::move(ents));
    co_return std::move(s);
}

uint64_t RaftLog::ZeroTermOnErrCompacted(Status<uint64_t> &s) {
    if (s) {
        return s.Value();
    } else if (s.Code() == ErrCode::ErrRaftCompacted) {
        return 0;
    }
    LOG_FATAL_THROW("[{}-{}] unexpected error ({})", group_, id_, s);
    return 0;
}

}  // namespace raft
}  // namespace snail
