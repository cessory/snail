#include "inflights.h"

#include <cassert>

namespace snail {
namespace raft {

Inflights::Inflights(size_t size)
    : head_(0), tail_(0), count_(0), buffer_(size) {}

Inflights::Inflights(const Inflights& x)
    : head_(x.head_), tail_(x.tail_), count_(x.count_), buffer_(x.buffer_) {}

Inflights& Inflights::operator=(const Inflights& x) {
  if (this != &x) {
    head_ = x.head_;
    tail_ = x.tail_;
    count_ = x.count_;
    buffer_ = x.buffer_;
  }
  return *this;
}

void Inflights::Add(uint64_t inflight) {
  if (count_ == buffer_.size()) {
    assert("cannot add into a Full inflights");
  }
  buffer_[head_] = inflight;
  head_ = (head_ + 1) % buffer_.size();
  count_++;
}

bool Inflights::Full() { return count_ == buffer_.size(); }

size_t Inflights::Count() { return count_; }

void Inflights::Reset() {
  head_ = 0;
  tail_ = 0;
  count_ = 0;
}

void Inflights::FreeFirstOne() { FreeLE(buffer_[tail_]); }

void Inflights::FreeLE(uint64_t to) {
  if (count_ == 0 || to < buffer_[tail_]) {
    return;
  }

  while (count_ > 0) {
    if (to < buffer_[tail_]) {
      break;
    }
    tail_ = (tail_ + 1) % buffer_.size();
    count_--;
  }
}

}  // namespace raft
}  // namespace snail
