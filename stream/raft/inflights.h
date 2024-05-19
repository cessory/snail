#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/macro.h"

namespace snail {
namespace raft {

class Inflights {
  SNAIL_PRIVATE

  size_t head_;
  size_t tail_;
  size_t count_;

  // buffer contains the index of the last entry
  // inside one message.
  std::vector<uint64_t> buffer_;

 public:
  explicit Inflights(size_t size);

  Inflights(const Inflights& x);

  Inflights& operator=(const Inflights& x);

  void Add(uint64_t inflight);

  bool Full();

  size_t Count();

  void Reset();

  void FreeFirstOne();

  void FreeLE(uint64_t to);
};

}  // namespace raft
}  // namespace snail
