#pragma once
#include <atomic>
#include <seastar/core/shared_ptr.hh>

namespace snail {
namespace stream {

class IDGenerator {
    uint64_t prefix_;
    std::atomic_uint64_t suffix_;

   public:
    explicit IDGenerator(uint64_t node_id);

    uint64_t Next();
};

using IDGeneratorPtr = seastar::shared_ptr<IDGenerator>;

}  // namespace stream
}  // namespace snail
