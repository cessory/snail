#pragma once

namespace snail {
namespace stream {

class ExtentID {
  char id_[16] = {};

 public:
  ExtentID() noexcept = default;

  ExtentID(uint64_t hi, uint64_t lo) noexcept {
    uint64_t* u64 = reinterpret_cast<uint64_t*>(id_);
    *u64++ = hi;
    *u64 = lo;
  }

  ExtentID(const ChunkID& x) noexcept { memcpy(id_, x.id_, 16); }

  ExtentID(ChunkID&& x) noexcept {
    memcpy(id_, x.id_, 16);
    uint64_t* u64 = reinterpret_cast<uint64_t*>(x.id_);
    *u64++ = 0;
    *u64 = 0;
  }

  ExtentID& operator=(const ExtentID& x) noexcept {
    if (this != &x) {
      memcpy(id_, x.id_, 16);
    }
    return *this;
  }

  ExtentID& operator=(ExtentID&& x) noexcept {
    if (this != &x) {
      memcpy(id_, x.id_, 16);
      uint64_t* u64 = reinterpret_cast<uint64_t*>(x.id_);
      *u64++ = 0;
      *u64 = 0;
    }
    return *this;
  }

  inline bool operator==(const ExtentID& x) const {
    const uint64_t* u64_1 = reinterpret_cast<const uint64_t*>(id_);
    const uint64_t* u64_2 = reinterpret_cast<const uint64_t*>(x.id_);
    return (*u64_1++ == *u64_2++ && *u64_1 == *u64_2);
  }

  inline bool Empty() const {
    const uint64_t* u64 = reinterpret_cast<const uint64_t*>(id_);
    return (*u64++ == 0 && *u64 == 0);
  }

  inline std::pair<uint64_t, uint64_t> GetID() const {
    std::pair<uint64_t, uint64_t> p;
    const uint64_t* u64 = reinterpret_cast<const uint64_t*>(id_);
    p.first = *u64++;
    p.second = *u64;
    return p;
  }

  inline uint64_t Hash() const {
    const uint64_t* u64 = reinterpret_cast<const uint64_t*>(id_);
    return *u64++ & *u64;
  }
};

}  // namespace stream
}  // namespace snail

namespace std {
template <>
struct hash<snail::stream::ExtentID> {
  size_t operator()(const snail::stream::ExtentID& x) const {
    return std::hash<uint64_t>()(x.Hash());
  }
};
}  // namespace std

namespace snail {
namespace stream {

struct Extent {
  DevicePtr dev_ptr;
  ExtentID id;
  uint32_t index;  // in memory
  size_t len;
  std::vector<ChunkPtr> chunks;
  seastar::shared_mutex mu;
};

using ExtentPtr = seastar::lw_shared_ptr<Extent>;

}  // namespace stream
}  // namespace snail
