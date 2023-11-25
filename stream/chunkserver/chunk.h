#pragma once

namespace snail {
namespace stream {

struct Chunk {
  uint32_t index;
  uint32_t next;
  uint32_t len;  // data len, exclude crc
  uint32_t crc;
};

using ChunkPtr = seastar::lw_shared_ptr<Chunk>;

}  // namespace stream
}  // namespace snail
