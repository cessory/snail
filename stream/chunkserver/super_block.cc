#include "super_block.h"

#include <seastar/net/byteorder.hh>

namespace snail {
namespace stream {

void SuperBlock::MarshalTo(char *b) {
    uint32_t *u32 = reinterpret_cast<uint32_t *>(b);
    *u32++ = seastar::net::hton(magic);
    *u32++ = seastar::net::hton(version);
    *u32++ = seastar::net::hton(cluster_id);
    *u32++ = seastar::net::hton(static_cast<uint32_t>(dev_type));
    *u32++ = seastar::net::hton(dev_id);

    uint64_t *u64 = reinterpret_cast<uint64_t *>(u32);
    *u64++ = seastar::net::hton(capacity);

    for (int i = 0; i < MAX_PT; i++) {
        *u64++ = seastar::net::hton(pt[i].start);
        *u64++ = seastar::net::hton(pt[i].size);
    }
}

void SuperBlock::Unmarshal(const char *b) {
    const uint32_t *u32 = reinterpret_cast<const uint32_t *>(b);
    magic = seastar::net::ntoh(*u32++);
    version = seastar::net::ntoh(*u32++);
    cluster_id = seastar::net::ntoh(*u32++);
    dev_type = static_cast<DevType>(seastar::net::ntoh(*u32++));
    dev_id = seastar::net::ntoh(*u32++);

    const uint64_t *u64 = reinterpret_cast<const uint64_t *>(u32);
    capacity = seastar::net::ntoh(*u64++);
    for (int i = 0; i < 5; i++) {
        pt[i].start = seastar::net::ntoh(*u64++);
        pt[i].size = seastar::net::ntoh(*u64++);
    }
}

}  // namespace stream
}  // namespace snail
