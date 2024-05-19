#include "log_unstable.h"

#include "logger.h"

namespace snail {
namespace raft {

std::tuple<uint64_t, bool> Unstable::MaybeFirstIndex() {
  if (snapshot_) {
    return std::make_tuple(snapshot_->metadata().index() + 1, true);
  }
  return std::make_tuple(0, false);
}

std::tuple<uint64_t, bool> Unstable::MaybeLastIndex() {
  if (!entries_.empty()) {
    return std::make_tuple(offset_ + entries_.size() - 1, true);
  }

  if (snapshot_) {
    return std::make_tuple(snapshot_->metadata().index(), true);
  }
  return std::make_tuple(0, false);
}

std::tuple<uint64_t, bool> Unstable::MaybeTerm(uint64_t i) {
  if (i < offset_) {
    if (snapshot_ && snapshot_->metadata().index() == i) {
      return std::make_tuple(snapshot_->metadata().term(), true);
    }
    return std::make_tuple(0, false);
  }

  uint64_t last;
  bool ok;
  auto r = MaybeLastIndex();
  std::tie(last, ok) = r;
  if (!ok) {
    return std::make_tuple(0, false);
  }
  if (i > last) {
    return std::make_tuple(0, false);
  }

  return std::make_tuple(entries_[i - offset_]->term(), true);
}

void Unstable::StableTo(uint64_t i, uint64_t t) {
  uint64_t gt;
  bool ok;
  auto r = MaybeTerm(i);
  std::tie(gt, ok) = r;
  if (!ok) {
    return;
  }
  if (gt == t && i >= offset_) {
    for (uint64_t j = 0; j < i + 1 - offset_; j++) {
      entries_.pop_front();
    }
    offset_ = i + 1;
  }
}

void Unstable::StableSnapTo(uint64_t i) {
  if (snapshot_ && snapshot_->metadata().index() == i) {
    snapshot_.release();
  }
}

void Unstable::Restore(SnapshotPtr s) {
  offset_ = s->metadata().index() + 1;
  entries_.clear();
  snapshot_ = s;
}

void Unstable::TruncateAndAppend(const std::vector<EntryPtr>& ents) {
  uint64_t after = ents[0]->index();

  if (after == offset_ + entries_.size()) {
  } else if (after <= offset_) {
    RAFT_LOG_INFO(logger_, "[{}-{}] replace the unstable entries from index {}",
                  group_, id_, after);
    offset_ = after;
    entries_.clear();
  } else {
    RAFT_LOG_INFO(logger_,
                  "[{}-{}] truncate the unstable entries before index {}",
                  group_, id_, after);
    auto entries = Slice(offset_, after);
    entries_.clear();
    for (auto e : entries) {
      entries_.push_back(e);
    }
  }
  for (auto e : ents) {
    entries_.push_back(e);
  }
}

std::vector<EntryPtr> Unstable::Slice(uint64_t lo, uint64_t hi) {
  std::vector<EntryPtr> ents;

  if (lo > hi) {
    RAFT_LOG_FATAL(logger_, "[{}-{}] invalid unstable.slice {} > {}", group_,
                   id_, lo, hi);
  }

  auto upper = offset_ + entries_.size();
  if (lo < offset_ || hi > upper) {
    RAFT_LOG_FATAL(logger_,
                   "[{}-{}] unstable.slice[{},{}) out of bound [{},{}]", group_,
                   id_, lo, hi, offset_, upper);
  }
  for (int i = lo - offset_; i < hi - offset_; i++) {
    ents.push_back(entries_[i]);
  }
  return ents;
}

std::vector<EntryPtr> Unstable::Entries() {
  return std::vector<EntryPtr>(entries_.begin(), entries_.end());
}

void Unstable::set_offset(uint64_t v) { offset_ = v; }

}  // namespace raft
}  // namespace snail
