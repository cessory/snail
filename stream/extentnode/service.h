#pragma once

#include <fmt/ostream.h>

#include <seastar/core/sharded.hh>

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

    friend std::ostream &operator<<(std::ostream &os,
                                    const BlockCacheKey &key) {
        os << key.extent_id << "-" << key.offset;
        return os;
    }

    inline uint64_t Hash() const { return (extent_id.Hash() << 32) | offset; }
};

#if FMT_VERSION >= 90000

template <>
struct fmt::formatter<BlockCacheKey> : fmt::ostream_formatter {};

#endif

}  // namespace stream
}  // namespace snail

namespace std {
template <>
struct hash<snail::stream::BlockCacheKey> {
    size_t operator()(const snail::stream::BlockCacheKey &x) const {
        return x.Hash();
    }
};
}  // namespace std

namespace snail {
namespace stream {

class Service {
    StorePtr store_;
    LRUCache<BlockCacheKey, TmpBuffer> block_cache_;

   public:
    explicit Service(StorePtr store, size_t cache_capacity = 8192)
        : store_(store), block_cache_(cache_capacity) {}

    seastar::future<Status<>> HandleWriteExtent(
        const WriteExtentReq *req, seastar::foreign_ptr<net::StreamPtr> stream);

    seastar::future<Status<>> HandleReadExtent(
        const ReadExtentReq *req, seastar::foreign_ptr<net::StreamPtr> stream);

    seastar::future<Status<>> HandleCreateExtent(
        const CreateExtentReq *req,
        seastar::foreign_ptr<net::StreamPtr> stream);

    seastar::future<Status<>> HandleDeleteExtent(
        const DeleteExtentReq *req,
        seastar::foreign_ptr<net::StreamPtr> stream);

    seastar::future<Status<>> HandleGetExtent(
        const GetExtentReq *req, seastar::foreign_ptr<net::StreamPtr> s);

    seastar::future<Status<>> HandleUpdateDiskStatus(
        const UpdateDiskStatusReq *req,
        seastar::foreign_ptr<net::StreamPtr> stream);
};

using ServicePtr = seastar::lw_shared_ptr<Service>;

}  // namespace stream
}  // namespace snail
