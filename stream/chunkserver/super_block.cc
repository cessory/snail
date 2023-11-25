#include "super_block.h"

namespace snail {
namespace stream {

static uint32_t super_block_size = 24 + 8 + 80;

void SuperBlock::MarshalTo(char *b) {
  uint32_t *u32 = reinterpret_cast<uint32_t *>(b);
  *u32++ = seastar::net::hton(magic);
  *u32++ = seastar::net::hton(version);
  *u32++ = seastar::net::hton(cluster_id);
  *u32++ = seastar::net::hton(static_cast<uint32_t>(dev_type));
  *u32++ = seastar::net::hton(dev_id);
  *u32++ = seastar::net::hton(sector_size);

  uint64_t *u64 = reinterpret_cast<uint64_t *>(u32);
  *u64++ = seastar::net::hton(capacity);

  for (int i = 0; i < 5; i++) {
    *u64++ = seastar::net::hton(pt[i].start);
    *u64++ = seastar::net::hton(pt[i].size);
  }

  uint32_t crc = crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(b),
                                 super_block_size);
  u32 = reinterpret_cast<uint32_t *>(u64);
  *u32 = seastar::net::hton(crc);
}

void SuperBlock::Unmarshal(const char *b, size_t len) {
  const uint32_t *u32 = reinterpret_cast<const uint32_t *>(b);
  if (len < super_block_size + 4) {
    throw std::runtime_error("the buffer is too short");
  }
  magic = seastar::net::ntoh(*u32++);
  cluster_id = seastar::net::ntoh(*u32++);
  dev_type = static_cast<DiskType>(seastar::net::ntoh(*u32++));
  dev_id = seastar::net::ntoh(*u32++);
  sector_size = seastar::net::ntoh(*u32++);

  const uint64_t *u64 = reinterpret_cast<const uint64_t *>(u32);
  capacity = seastar::net::ntoh(*u64++);
  for (int i = 0; i < 5; i++) {
    pt[i].start = seastar::net::ntoh(*u64++);
    pt[i].size = seastar::net::ntoh(*u64++);
  }
  u32 = reinterpret_cast<const uint32_t *>(u64);
  uint32_t crc = crc32_gzip_refl(0, reinterpret_cast<const unsigned char *>(b),
                                 super_block_size);
  if (seastar::net::ntoh(*u32) != crc) {
    throw std::runtime_error("invalid checksum");
  }
}

}  // namespace stream
}  // namespace snail
