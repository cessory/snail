#include "id_generator.h"

#include <chrono>
#include <cstdint>

namespace snail {
namespace stream {

#define LOWBIT(x, n) ((x) & (UINT64_MAX >> (64 - n)))

IDGenerator::IDGenerator(uint64_t node_id) : prefix_(node_id << 48) {
    uint64_t unix_milli =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    suffix_ = LOWBIT(unix_milli, 40) << 8;
}

uint64_t IDGenerator::Next() { return prefix_ | LOWBIT(++suffix_, 48); }

}  // namespace stream
}  // namespace snail
