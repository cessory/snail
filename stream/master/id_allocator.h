#pragma once
#include <chrono>
#include <seastar/core/future.hh>

#include "id_generator.h"
#include "storage.h"
#include "util/util.h"

namespace snail {
namespace stream {

class IdAllocator : public ApplyHandler {
    unsigned shard_;
    uint64_t next_id_ = 1;
    uint64_t max_id_ = 1;

    Storage* store_;
    IDGenerator* id_gen_;
    ApplyType type_;
    Buffer key_;

    struct alloc_item {
        seastar::promise<Status<uint64_t>> pr;
    };
    std::unordered_map<uint64_t, alloc_item*> pendings_;

    seastar::future<Status<>> Init();

   public:
    explicit IdAllocator(Storage* store, IDGenerator* id_gen, ApplyType type,
                         Buffer key);
    virtual ~IdAllocator();

    ApplyType Type() const override { return type_; }

    seastar::future<Status<>> Apply(Buffer reqid, uint64_t id, Buffer ctx,
                                    Buffer data) override;
    seastar::future<Status<>> Reset() override;

    seastar::future<Status<>> Restore(Buffer key, Buffer val) override;

    seastar::future<Status<uint64_t>> Alloc(Buffer reqid, size_t count,
                                            std::chrono::milliseconds timeout);

    static seastar::future<Status<seastar::shared_ptr<IdAllocator>>>
    CreateDiskIdAllocator(Storage* store, IDGenerator* id_gen);

    static seastar::future<Status<seastar::shared_ptr<IdAllocator>>>
    CreateNodeIdAllocator(Storage* store, IDGenerator* id_gen);
};

using IdAllocatorPtr = seastar::shared_ptr<IdAllocator>;

}  // namespace stream
}  // namespace snail
