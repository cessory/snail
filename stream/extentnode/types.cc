#include "types.h"

#include "net/byteorder.h"
namespace snail {
namespace stream {

uint64_t ExtentID::Hash() const {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = 0xb0f57ee3 ^ (16 * m);
    uint64_t blocks[2] = {hi, lo};
    for (int i = 0; i < 2; i++) {
        uint64_t k = blocks[i];
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }
    h ^= blocks[1] << 8;
    h ^= blocks[0];
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

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
