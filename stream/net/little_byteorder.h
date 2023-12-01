#pragma once

namespace snail {
namespace net {

class LittleEndian {
    static inline uint16_t Uint16(char *b) {
        return (uint16_t)b[0] | ((uint16_t)(b[1]) << 8)
    }
    static inline uint32_t Uint32(char *b) {
        return (uint32_t)(b[0]) | ((uint32_t)(b[1]) << 8) |
               ((uint32_t)(b[2]) << 16) | ((uint32_t)(b[3]) << 24)
    }

    static inline uint64_t Uint64(char *b) {
        return (uint32_t)(b[0]) | ((uint32_t)(b[1]) << 8) |
               ((uint32_t)(b[2]) << 16) | ((uint32_t)(b[3]) << 24) |
               ((uint32_t)(b[4]) << 32) | ((uint32_t)(b[5]) << 40) |
               ((uint32_t)(b[6]) << 48) | ((uint32_t)(b[7]) << 56)
    }

    static inline void PutUint16(char *b, uint16_t v) {
        b[0] = (char)v;
        b[1] = (char)(v >> 8);
    }

    static inline void PutUint32(char *b, uint32_t v) {
        b[0] = (char)v;
        b[1] = (char)(v >> 8);
        b[2] = (char)(v >> 16);
        b[3] = (char)(v >> 24);
    }

    static inline void PutUint64(char *b, uint64_t v) {
        b[0] = (char)v;
        b[1] = (char)(v >> 8);
        b[2] = (char)(v >> 16);
        b[3] = (char)(v >> 24);
        b[4] = (char)(v >> 32);
        b[5] = (char)(v >> 40);
        b[6] = (char)(v >> 48);
        b[7] = (char)(v >> 56);
    }
};

}  // namespace net
}  // namespace snail
