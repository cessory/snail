#include "util.h"

#include <uuid/uuid.h>

#include <string_view>
namespace snail {

seastar::temporary_buffer<char> foreign_buffer_copy(
    seastar::foreign_ptr<std::unique_ptr<seastar::temporary_buffer<char>>>
        org) {
    if (org.get_owner_shard() == seastar::this_shard_id()) {
        return std::move(*org);
    }
    seastar::temporary_buffer<char>* one = org.get();
    return seastar::temporary_buffer<char>(one->get_write(), one->size(),
                                           make_object_deleter(std::move(org)));
}

int VarintLength(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

char* PutVarint32(char* dst, uint32_t v) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    static const int B = 128;
    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = v >> 21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = (v >> 21) | B;
        *(ptr++) = v >> 28;
    }
    return reinterpret_cast<char*>(ptr);
}

char* PutVarint64(char* dst, uint64_t v) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    while (v >= 128) {
        *(ptr++) = v | 128;
        v >>= 7;
    }
    *(ptr++) = static_cast<uint8_t>(v);
    return reinterpret_cast<char*>(ptr);
}

const char* GetVarint32(const char* s, size_t n, uint32_t* v) {
    uint32_t result = 0;
    const char* limit = s + n;
    for (uint32_t shift = 0; shift <= 28 && s < limit; shift += 7) {
        uint32_t b = *(reinterpret_cast<const uint8_t*>(s));
        s++;
        if (b & 128) {
            // More bytes are present
            result |= ((b & 127) << shift);
        } else {
            result |= (b << shift);
            *v = result;
            return reinterpret_cast<const char*>(s);
        }
    }
    return nullptr;
}

const char* GetVarint64(const char* s, size_t n, uint64_t* v) {
    uint64_t result = 0;
    const char* limit = s + n;
    for (uint32_t shift = 0; shift <= 63 && s < limit; shift += 7) {
        uint64_t b = *(reinterpret_cast<const uint8_t*>(s));
        s++;
        if (b & 128) {
            // More bytes are present
            result |= ((b & 127) << shift);
        } else {
            result |= (b << shift);
            *v = result;
            return reinterpret_cast<const char*>(s);
        }
    }
    return nullptr;
}

std::string GenerateReqid() {
    uuid_t uuid;
    std::string out;
    out.resize(UUID_STR_LEN + 1);
    uuid_generate(uuid);
    uuid_unparse_lower(uuid, out.data());
    return out;
}

}  // namespace snail

