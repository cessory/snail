#pragma once

#include "net/tcp_session.h"
#include "proto/extentnode.pb.h"
#include "store.h"

namespace snail {
namespace stream {

struct BlockCacheKey {
    ExtentID extent_id;
    uint64_t offset = 0;

    BlockCacheKey() = default;
    BlockCacheKey(ExtentID id, uint64_t off) : extent_id(id), offset(off) {}

    inline bool operator==(const BlockCacheKey &x) const {
        return extent_id == x.extent_id && offset == x.offset;
    }

    inline uint64_t Hash() const { return (extent_id.Hash() << 32) & offset; }
};

}  // namespace stream
}  // namespace snail

namespace std {
template <>
struct hash<snail::stream::BlockCacheKey> {
    size_t operator()(const snail::stream::BlockCacheKey &x) const {
        return std::hash<uint64_t>()(x.Hash());
    }
};
}  // namespace std

namespace snail {
namespace stream {

class Service {
    StorePtr store_;
    LRUCache<BlockCacheKey, TmpBuffer> block_cache_;

   public:
    explicit Service(StorePtr store, size_t capacity = 8192)
        : store_(store), block_cache_(capacity) {}

    seastar::future<Status<>> HandleWriteExtent(const WriteExtentReq *req,
                                                net::StreamPtr stream);

    seastar::future<Status<>> HandleReadExtent(const ReadExtentReq *req,
                                               net::StreamPtr stream);

    seastar::future<Status<>> HandleCreateExtent(const CreateExtentReq *req,
                                                 net::StreamPtr stream);

    seastar::future<Status<>> HandleDeleteExtent(const DeleteExtentReq *req,
                                                 net::StreamPtr stream);

    seastar::future<Status<>> HandleGetExtent(const GetExtentReq *req,
                                              net::StreamPtr s);

    seastar::future<Status<>> HandleUpdateDiskStatus(
        const UpdateDiskStatusReq *req, net::StreamPtr s);
};

using ServicePtr = seastar::lw_shared_ptr<Service>;

}  // namespace stream
}  // namespace snail
