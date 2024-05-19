#include "raft_log.h"

#include "logger.h"
#include "raft_errno.h"
#include "seastar/core/coroutine.hh"

namespace snail {
namespace raft {

RaftLog::RaftLog(seastar::shared_ptr<Storage> storage,
                 uint64_t max_next_ents_size, spdlog::logger *logger,
                 uint64_t group, uint64_t id) noexcept
    : storage_(storage),
      unstable_(logger, group, id),
      committed_(storage->FirstIndex() - 1),
      applied_(storage->FirstIndex() - 1),
      max_next_ents_size_(max_next_ents_size),
      logger_(logger),
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
      auto r1 = co_await Term(index);
      RAFT_LOG_INFO(logger_,
                    "[{}-{}] found conflict at index {} [existing term: {}, "
                    "conflicting term: {}]",
                    group_, id_, index, ZeroTermOnErrCompacted(r1.val, r1.err),
                    term);
    }
    co_return index;
  }
  co_return 0;
}

RaftLogPtr RaftLog::MakeRaftLog(seastar::shared_ptr<Storage> storage,
                                uint64_t max_next_ents_size,
                                spdlog::logger *logger, uint64_t group,
                                uint64_t id) noexcept {
  return seastar::make_lw_shared<RaftLog>(
      RaftLog(storage, max_next_ents_size, logger, group, id));
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
      RAFT_LOG_FATAL(
          logger_,
          "[{}-{}] entry {} conflict with committed entry [committed({})]",
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
    RAFT_LOG_FATAL(logger_, "[{}-{}] after({}) is out of range [committed({})]",
                   group_, id_, ents[0]->index() - 1, committed_);
  }

  unstable_.TruncateAndAppend(ents);
  return LastIndex();
}

seastar::future<uint64_t> RaftLog::FindConflictByTerm(uint64_t index,
                                                      uint64_t term) {
  auto li = LastIndex();
  if (index > li) {
    RAFT_LOG_WARN(logger_,
                  "[{}-{}] index({}) is out of range [0, lastIndex({})] in "
                  "findConflictByTerm",
                  group_, id_, index, li);
    co_return index;
  }
  for (;;) {
    auto r = co_await Term(index);
    if (r.err != RAFT_OK || r.val <= term) {
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
    auto r = co_await Slice(off, committed_ + 1, max_next_ents_size_);
    if (r.err != RAFT_OK) {
      RAFT_LOG_FATAL(
          logger_,
          "[{}-{}] unexpected error when getting unapplied entries ({})",
          group_, id_, raft_error(r.err));
    }
    co_return std::move(r.val);
  }
  co_return std::vector<EntryPtr>();
}

bool RaftLog::HasNextEnts() {
  auto first_index = FirstIndex();
  return committed_ + 1 > std::max(applied_ + 1, first_index);
}

bool RaftLog::HasPendingSnapshot() {
  return unstable_.snapshot() && unstable_.snapshot()->metadata().index() != 0;
}

seastar::future<Result<SnapshotPtr>> RaftLog::Snapshot() {
  Result<SnapshotPtr> res;
  auto snapshot = unstable_.snapshot();
  if (snapshot) {
    res.val = snapshot;
    res.err = RAFT_OK;
    return seastar::make_ready_future<Result<SnapshotPtr>>(res);
  }
  return storage_->Snapshot();
}

seastar::future<Result<std::vector<EntryPtr>>> RaftLog::Entries(
    uint64_t i, uint64_t max_size) {
  Result<std::vector<EntryPtr>> res;

  if (i > LastIndex()) {
    res.err = RAFT_OK;
    return seastar::make_ready_future<Result<std::vector<EntryPtr>>>(res);
  }
  return Slice(i, LastIndex() + 1, max_size);
}

seastar::future<std::vector<EntryPtr>> RaftLog::AllEntries() {
  auto res =
      co_await Entries(FirstIndex(), std::numeric_limits<uint64_t>::max());
  if (res.err == RAFT_OK) {
    co_return std::move(res.val);
  } else if (res.err == RAFT_ERR_COMPACTED) {
    auto ents = co_await AllEntries();
    co_return std::move(ents);
  }
  RAFT_LOG_FATAL(logger_, "[{}-{}] all entries error: {}", group_, id_,
                 raft_error(res.err));
  co_return std::vector<EntryPtr>();
}

seastar::future<bool> RaftLog::IsUpToDate(uint64_t lasti, uint64_t term) {
  auto t = co_await LastTerm();
  co_return term > t || (term == t && lasti >= LastIndex());
}

seastar::future<bool> RaftLog::MaybeCommit(uint64_t max_index, uint64_t term) {
  auto res = co_await Term(max_index);
  if (max_index > committed_ &&
      ZeroTermOnErrCompacted(res.val, res.err) == term) {
    CommitTo(max_index);
    co_return true;
  }
  co_return false;
}

void RaftLog::Restore(SnapshotPtr s) {
  RAFT_LOG_INFO(logger_,
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
      RAFT_LOG_FATAL(
          logger_,
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
    RAFT_LOG_FATAL(
        logger_,
        "[{}-{}] applied({}) is out of range [prevApplied({}), committed({})]",
        group_, id_, i, applied_, committed_);
  }
  applied_ = i;
}

void RaftLog::StableTo(uint64_t i, uint64_t t) { unstable_.StableTo(i, t); }

void RaftLog::StableSnapTo(uint64_t i) { unstable_.StableSnapTo(i); }

seastar::future<Result<uint64_t>> RaftLog::Term(uint64_t i) {
  Result<uint64_t> res;

  res.err = RAFT_OK;
  auto first_index = FirstIndex();
  auto last_index = LastIndex();
  uint64_t dummy_index = first_index - 1;

  if (i < dummy_index || i > last_index) {
    res.val = 0;
    co_return res;
  }

  auto r = unstable_.MaybeTerm(i);
  if (std::get<1>(r)) {
    res.val = std::get<0>(r);
    co_return res;
  }

  auto r1 = co_await storage_->Term(i);
  if (r1.err == RAFT_OK) {
    res = r1;
    co_return res;
  } else if (r1.err != RAFT_ERR_COMPACTED && r1.err != RAFT_ERR_UNAVAILABLE) {
    RAFT_LOG_FATAL(logger_, "[{}-{}] storage::Term error: {}", group_, id_,
                   raft_error(r1.err));
  }
  res.err = r1.err;
  res.val = 0;
  co_return res;
}

seastar::future<uint64_t> RaftLog::LastTerm() {
  auto last_index = LastIndex();
  auto r = co_await Term(last_index);
  if (r.err != RAFT_OK) {
    RAFT_LOG_FATAL(logger_,
                   "[{}-{}] unexpected error when getting the last term ({})",
                   group_, id_, raft_error(r.err));
  }
  co_return r.val;
}

seastar::future<bool> RaftLog::MatchTerm(uint64_t i, uint64_t term) {
  auto r = co_await Term(i);
  if (r.err != RAFT_OK) {
    co_return false;
  }
  co_return r.val == term;
}

seastar::future<Result<std::vector<EntryPtr>>> RaftLog::Slice(
    uint64_t lo, uint64_t hi, uint64_t max_size) {
  Result<std::vector<EntryPtr>> res;

  if (lo > hi) {
    RAFT_LOG_FATAL(logger_, "[{}-{}] invalid slice {} > {}", group_, id_, lo,
                   hi);
  }
  auto fi = FirstIndex();
  if (lo < fi) {
    res.err = RAFT_ERR_COMPACTED;
    co_return res;
  }

  auto last_index = LastIndex();
  if (hi > last_index + 1) {
    RAFT_LOG_FATAL(logger_, "[{}-{}] slice[{},{}) out of bound [{},{}]", group_,
                   id_, lo, hi, fi, last_index);
  }

  if (lo == hi) {
    res.err = RAFT_OK;
    co_return res;
  }

  std::vector<EntryPtr> ents;
  if (lo < unstable_.offset()) {
    auto r = co_await storage_->Entries(lo, std::min(hi, unstable_.offset()),
                                        max_size);
    if (r.err == RAFT_ERR_COMPACTED) {
      res.err = r.err;
      co_return res;
    } else if (r.err == RAFT_ERR_UNAVAILABLE) {
      RAFT_LOG_FATAL(logger_,
                     "[{}-{}] entries[{}:{}) is unavailable from storage",
                     group_, id_, lo, std::min(hi, unstable_.offset()));
    } else if (r.err != RAFT_OK) {
      RAFT_LOG_FATAL(logger_, "[{}-{}] get entries from storage error: {}",
                     group_, id_, raft_error(r.err));
    }

    if (r.val.size() < std::min(hi, unstable_.offset()) - lo) {
      res.val = std::move(r.val);
      res.err = RAFT_OK;
      co_return std::move(res);
    }

    ents = std::move(r.val);
  }

  if (hi > unstable_.offset()) {
    auto unstable_ents = unstable_.Slice(std::max(lo, unstable_.offset()), hi);
    for (auto ent : unstable_ents) {
      ents.push_back(ent);
    }
  }

  if (ents.empty()) {
    res.err = RAFT_OK;
    co_return res;
  }
  uint64_t size = ents[0]->ByteSize();
  for (size_t i = 1; i < ents.size(); i++) {
    size += ents[i]->ByteSize();
    if (size > max_size) {
      ents.resize(i);
      break;
    }
  }
  res.val = std::move(ents);
  res.err = RAFT_OK;
  co_return std::move(res);
}

uint64_t RaftLog::ZeroTermOnErrCompacted(uint64_t t, int err) {
  if (err == RAFT_OK) {
    return t;
  } else if (err == RAFT_ERR_COMPACTED) {
    return 0;
  }
  RAFT_LOG_FATAL(logger_, "[{}-{}] unexpected error ({})", group_, id_,
                 raft_error(err));
  return 0;
}

}  // namespace raft
}  // namespace snail
