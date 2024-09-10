#pragma once
#include <chrono>
#include <seastar/core/future.hh>

#include "id_generator.h"
#include "storage.h"
#include "util/util.h"

namespace snail {
namespace stream {

class IdAllocator : public ApplyHandler {
    uint64_t next_id_ = 1;
    uint64_t max_id_ = 1;

    StoragePtr store_;
    IDGeneratorPtr id_gen_;
    ApplyType type_;
    Buffer key_;

    struct alloc_item {
        seastar::promise<Status<uint64_t>> pr;
    };
    std::unordered_map<uint64_t, alloc_item*> pendings_;

   public:
    explicit IdAllocator(StoragePtr store, IDGeneratorPtr id_gen,
                         ApplyType type, Buffer key);
    virtual ~IdAllocator();

    seastar::future<Status<>> Init();

    seastar::future<Status<>> Apply(Buffer reqid, uint64_t id, Buffer ctx,
                                    Buffer data) override;
    void Reset() override;

    void Restore(Buffer key, Buffer val) override;

    seastar::future<Status<uint64_t>> Alloc(RaftServerPtr raft, Buffer reqid,
                                            size_t count,
                                            std::chrono::milliseconds timeout);

    static seastar::shared_ptr<IdAllocator> CreateDiskIdAllocator(
        StoragePtr store, IDGeneratorPtr id_gen);

    static seastar::shared_ptr<IdAllocator> CreateNodeIdAllocator(
        StoragePtr store, IDGeneratorPtr id_gen);
};

using IdAllocatorPtr = seastar::shared_ptr<IdAllocator>;

}  // namespace stream
}  // namespace snail
