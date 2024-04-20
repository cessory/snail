#pragma once
#include <array>
#include <map>
#include <queue>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <set>
#include <unordered_set>

#include "device.h"
#include "log.h"
#include "super_block.h"
#include "types.h"
#include "util/lru_cache.h"
#include "util/status.h"

namespace snail {
namespace stream {

class Store;
using StorePtr = seastar::lw_shared_ptr<Store>;

struct Extent : public ExtentEntry {
    size_t len;
    std::vector<ChunkEntry> chunks;
    seastar::shared_mutex mu;
    StorePtr store;

    Extent();
    ~Extent();
    ExtentEntry GetExtentEntry();
};

using ExtentPtr = seastar::lw_shared_ptr<Extent>;
class Store : public seastar::enable_lw_shared_from_this<Store> {
    DevicePtr dev_ptr_;
    SuperBlock super_block_;
    uint64_t used_ = 0;
    DevStatus dev_status_ = DevStatus::NORMAL;
    std::unordered_map<ExtentID, ExtentPtr> extents_;
    std::unordered_set<ExtentID> creating_extents_;
    std::queue<uint32_t> free_extents_;
    std::queue<uint32_t> free_chunks_;

    LogPtr log_ptr_;

    // key = extent index
    LRUCache<uint32_t, std::string> last_sector_cache_;

    seastar::future<Status<>> AllocChunk(ExtentPtr extent_ptr);

    Status<> HandleIO(ChunkEntry& chunk, char* b, size_t len, bool first,
                      std::vector<TmpBuffer>& tmp_buf_vec,
                      std::vector<iovec>& tmp_io_vec,
                      std::string& last_sector_data);

    seastar::future<Status<std::string>> GetLastSectorData(ExtentPtr ptr);

    friend struct Extent;

   public:
    static seastar::future<StorePtr> Load(std::string_view name,
                                          bool spdk_nvme = false,
                                          size_t cache_cap = 1024);

    static seastar::future<bool> Format(std::string_view name,
                                        uint32_t cluster_id, DevType dev_type,
                                        uint32_t dev_id,
                                        uint64_t log_cap = 16 << 20,
                                        uint64_t capacity = 0);

    Store(size_t cache_capacity = 1024) : last_sector_cache_(cache_capacity) {}

    ~Store() {}

    uint32_t DeviceId() const { return super_block_.dev_id; }

    DevStatus GetStatus() const { return dev_status_; }

    void SetStatus(DevStatus status) { dev_status_ = status; }

    seastar::future<Status<>> CreateExtent(ExtentID id);

    // 同时写入多个block数据, 包含crc
    // 如果有多个io, 第一个io的数据必须填充到sector对齐
    // 中间的io必须是对齐sector
    // 该函数不负责crc的正确性检查
    seastar::future<Status<>> Write(ExtentPtr extent_ptr, uint64_t offset,
                                    std::vector<iovec> iov);

    seastar::future<Status<>> Write(ExtentPtr extent_ptr, uint64_t offset,
                                    std::vector<TmpBuffer> buffers);

    // 读取多个block的数据
    // off - 必须block对齐(32k -4)
    // len   - 数据长度, off+len不能超过extent的数据长度,
    // 需要上层根据extent的实际长度计算好.
    // 如果len大于chunk的大小, 会返回多个数据块
    seastar::future<Status<std::vector<TmpBuffer>>> Read(ExtentPtr extent_ptr,
                                                         uint64_t off,
                                                         size_t len);

    seastar::future<Status<>> RemoveExtent(ExtentID id);

    ExtentPtr GetExtent(const ExtentID& id);

    uint64_t Capacity() const { return super_block_.capacity; }

    uint64_t Used() const { return used_; }

    seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail

