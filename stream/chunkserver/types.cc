#include "types.h"

#include "net/byteorder.h"
namespace snail {
namespace stream {

void ChunkEntry::MarshalTo(char *b) const {
    net::BigEndian::PutUint32(b, index);
    net::BigEndian::PutUint32(b + 4, next);
    net::BigEndian::PutUint32(b + 8, len);
    net::BigEndian::PutUint32(b + 12, crc);
    net::BigEndian::PutUint32(b + 16, scrc);
}

void ChunkEntry::Unmarshal(const char *b) {
    index = net::BigEndian::Uint32(b);
    next = net::BigEndian::Uint32(b + 4);
    len = net::BigEndian::Uint32(b + 8);
    crc = net::BigEndian::Uint32(b + 12);
    scrc = net::BigEndian::Uint32(b + 16);
}

void ExtentEntry::MarshalTo(char *b) const {
    net::BigEndian::PutUint32(b, index);
    net::BigEndian::PutUint64(b + 4, id.hi);
    net::BigEndian::PutUint64(b + 12, id.lo);
    net::BigEndian::PutUint32(b + 20, chunk_idx);
    net::BigEndian::PutUint32(b + 24, ctime);
}

void ExtentEntry::Unmarshal(const char *b) {
    index = net::BigEndian::Uint32(b);
    id.hi = net::BigEndian::Uint64(b + 4);
    id.lo = net::BigEndian::Uint64(b + 12);
    chunk_idx = net::BigEndian::Uint32(b + 20);
    ctime = net::BigEndian::Uint32(b + 24);
}

}  // namespace stream
}  // namespace snail
