#pragma once

namespace snail {
namespace stream {

struct Partition {
  uint64_t start = 0;
  uint64_t size = 0;
};

struct SuperBlock {
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t cluster_id = 0;
  DevType dev_type = DevType::HDD;
  uint32_t dev_id = 0;
  uint32_t sector_size = 512;
  uint64_t capacity = 0;
  Partition pt[5];  // extent meta pt[0] chunk meta pt[1]
                    // log pt[2] pt[3] data pt[4]

  void MarshalTo(char *b);

  void Unmarshal(const char *b, size_t len);

  inline uint64_t ExtentMetaOffset(uint32_t i) const {
    return pt[0].start + (i * sector_size);
  }

  inline uint64_t ChunkMetaOffset(uint32_t i) const {
    return pt[1].start + (i * sector_size);
  }

  inline uint64_t ChunkOffset() const { return pt[4].start; }
};

}  // namespace stream
}  // namespace snail
