#pragma once

#include <fmt/ostream.h>

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include "net/tcp_session.h"
#include "proto/extentnode.pb.h"
#include "store.h"
#include "util/util.h"

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

}  // namespace stream
}  // namespace snail

#if FMT_VERSION >= 90000

template <>
struct fmt::formatter<snail::stream::BlockCacheKey> : fmt::ostream_formatter {};

#endif

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
    LRUCache<BlockCacheKey, Buffer> block_cache_;
    seastar::gate gate_;

   public:
    explicit Service(StorePtr store, size_t cache_capacity = 8192)
        : store_(store), block_cache_(cache_capacity) {}

    uint32_t DeviceID() const { return store_->DeviceId(); }

    seastar::future<> Close();

    seastar::future<Status<>> HandleWriteExtent(const WriteExtentReq *req,
                                                net::Stream *stream);

    seastar::future<Status<>> HandleReadExtent(const ReadExtentReq *req,
                                               net::Stream *stream);

    seastar::future<Status<>> HandleCreateExtent(const CreateExtentReq *req,
                                                 net::Stream *stream);

    seastar::future<Status<>> HandleDeleteExtent(const DeleteExtentReq *req,
                                                 net::Stream *stream);

    seastar::future<Status<>> HandleGetExtent(const GetExtentReq *req,
                                              net::Stream *stream);

    seastar::future<Status<>> HandleUpdateDiskStatus(
        const UpdateDiskStatusReq *req, net::Stream *stream);
};

using ServicePtr = seastar::lw_shared_ptr<Service>;

}  // namespace stream
}  // namespace snail
