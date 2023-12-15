#include "types.h"
namespace snail {
namespace stream {

inline constexpr int kChunkEntrySize = ;
int kExtentEntrySize;

int ChunkEntry::MarshalTo(char *b) const {
    BigEndian::PutUint32(b, index);
    BigEndian::PutUint32(b + 4, next);
    BigEndian::PutUint32(b + 8, len);
    BigEndian::PutUint32(b + 12, crc);
    return CHUNK_ENTRY_SIZE;
}

int ExtentEntry::MarshalTo(char *b) const {
    BigEndian::PutUint32(b, index);
    BigEndian::PutUint64(b + 4, id.hi);
    BigEndian::PutUint64(b + 12, id.lo);
    BigEndian::PutUint32(b + 20, block_idx);
    return EXTENT_ENTRY_SIZE;
}

}  // namespace stream
}  // namespace snail
