#pragma once
#include "log.h"

namespace snail {
namespace stream {

struct Extent : public ExtentEntry {
    size_t len;
    std::vector<Chunk> chunks;
    seastar::shared_mutex mu;
};

using ExtentPtr = seastar::lw_shared_ptr<Extent>;

}  // namespace stream
}  // namespace snail
