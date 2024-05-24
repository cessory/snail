#include "progress.h"

#include <algorithm>

#include "fmt/format.h"

namespace snail {
namespace raft {

Progress::Progress(size_t max_inflight)
    : match_(0),
      next_(0),
      state_(StateType::StateProbe),
      pending_snapshot_(0),
      recent_active_(false),
      probe_sent_(false),
      inflights_(max_inflight),
      is_learner_(false) {}

Progress::Progress(const Progress& x)
    : match_(x.match_),
      next_(x.next_),
      state_(x.state_),
      pending_snapshot_(x.pending_snapshot_),
      recent_active_(x.recent_active_),
      probe_sent_(x.probe_sent_),
      inflights_(x.inflights_),
      is_learner_(x.is_learner_) {}

Progress& Progress::operator=(const Progress& x) {
    if (this != &x) {
        match_ = x.match_;
        next_ = x.next_;
        state_ = x.state_;
        pending_snapshot_ = x.pending_snapshot_;
        recent_active_ = x.recent_active_;
        probe_sent_ = x.probe_sent_;
        inflights_ = x.inflights_;
        is_learner_ = x.is_learner_;
    }
    return *this;
}

uint64_t Progress::match() { return match_; }

uint64_t Progress::next() { return next_; }

StateType Progress::state() { return state_; }

uint64_t Progress::pending_snapshot() { return pending_snapshot_; }

bool Progress::recent_active() { return recent_active_; }

bool Progress::probe_sent() { return probe_sent_; }

Inflights& Progress::inflights() { return inflights_; }

bool Progress::is_learner() { return is_learner_; }

void Progress::set_match(uint64_t match) { match_ = match; }

void Progress::set_next(uint64_t next) { next_ = next; }

void Progress::set_learner(bool learner) { is_learner_ = learner; }

void Progress::set_recent_active(bool v) { recent_active_ = v; }

void Progress::set_probe_sent(bool v) { probe_sent_ = v; }

void Progress::set_pending_snapshot(uint64_t v) { pending_snapshot_ = v; }

void Progress::ResetState(StateType state) {
    probe_sent_ = false;
    pending_snapshot_ = 0;
    state_ = state;
    inflights_.Reset();
}

void Progress::ProbeAcked() { probe_sent_ = false; }

void Progress::BecomeProbe() {
    if (state_ == StateType::StateSnapshot) {
        uint64_t pending_snapshot = pending_snapshot_;
        ResetState(StateType::StateProbe);
        next_ = std::max(match_ + 1, pending_snapshot + 1);
    } else {
        ResetState(StateType::StateProbe);
        next_ = match_ + 1;
    }
}

void Progress::BecomeReplicate() {
    ResetState(StateType::StateReplicate);
    next_ = match_ + 1;
}

void Progress::BecomeSnapshot(uint64_t snapshot) {
    ResetState(StateType::StateSnapshot);
    pending_snapshot_ = snapshot;
}

bool Progress::MaybeUpdate(uint64_t n) {
    bool updated = false;
    if (match_ < n) {
        match_ = n;
        updated = true;
        ProbeAcked();
    }
    next_ = std::max(next_, n + 1);
    return updated;
}

void Progress::OptimisticUpdate(uint64_t n) { next_ = n + 1; }

bool Progress::MaybeDecrTo(uint64_t rejected, uint64_t match_hint) {
    if (state_ == StateType::StateReplicate) {
        if (rejected <= match_) {
            return false;
        }
        next_ = match_ + 1;
        return true;
    }

    if (next_ - 1 != rejected) {
        return false;
    }

    next_ = std::max(std::min(rejected, match_hint + 1), (uint64_t)1);
    probe_sent_ = false;
    return true;
}

bool Progress::IsPaused() {
    switch (state_) {
        case StateType::StateProbe:
            return probe_sent_;
        case StateType::StateReplicate:
            return inflights_.Full();
        case StateType::StateSnapshot:
            return true;
        default:
            throw std::runtime_error("unexpected state");
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const Progress& pro) {
    fmt::memory_buffer out;
    fmt::format_to(std::back_inserter(out), "{} match={} next={}",
                   static_cast<int>(state_), match_, next_);
    if (is_learner_) {
        fmt::format_to(std::back_inserter(out), " learner");
    }
    if (IsPaused()) {
        fmt::format_to(std::back_inserter(out), " paused");
    }
    if (pending_snapshot_ > 0) {
        fmt::format_to(std::back_inserter(out), " pendingSnap={}",
                       pending_snapshot_);
    }
    if (!recent_active_) {
        fmt::format_to(std::back_inserter(out), " inactive");
    }
    if (inflights_.Count() > 0) {
        fmt::format_to(std::back_inserter(out), " inflight={}",
                       inflights_.Count());
        if (inflights_.Full()) {
            fmt::format_to(std::back_inserter(out), "[full]");
        }
    }
    os << fmt::to_string(out);
    return os;
}

}  // namespace raft
}  // namespace snail
