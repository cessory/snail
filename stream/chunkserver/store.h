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
    std::unordered_map<ExtentID, ExtentPtr> extents_;
    std::unordered_set<ExtentID> creating_extents_;
    std::queue<uint32_t> free_extents_;
    std::queue<uint32_t> free_chunks_;

    LogPtr log_ptr_;

    // key = extent index
    LRUCache<uint32_t, std::string> last_sector_cache_;

    seastar::future<Status<>> AllocChunk(ExtentPtr extent_ptr);

    Status<> HandleFirst(ChunkEntry& chunk, int io_n, char* b, size_t len,
                         std::vector<TmpBuffer>& tmp_buf_vec,
                         std::vector<iovec>& tmp_io_vec, uint64_t& offset,
                         std::string& last_sector_data);

    Status<> HandleIO(ChunkEntry& chunk, int io_n, char* b, size_t len,
                      std::vector<TmpBuffer>& tmp_buf_vec,
                      std::vector<iovec>& tmp_io_vec,
                      std::string& last_sector_data, int i);

    seastar::future<Status<std::string>> GetLastSectorData(ExtentPtr ptr);

    friend struct Extent;

   public:
    static seastar::future<StorePtr> Load(std::string_view name,
                                          DevType dev_type,
                                          size_t cache_cap = 1024);

    static seastar::future<bool> Format(std::string_view name,
                                        uint32_t cluster_id, DevType dev_type,
                                        uint32_t dev_id,
                                        uint64_t log_cap = 64 << 20,
                                        uint64_t capacity = 0);

    Store(size_t cache_capacity = 1024) : last_sector_cache_(cache_capacity) {}

    ~Store() {}

    uint32_t DeviceId() const { return super_block_.dev_id; }

    seastar::future<Status<>> CreateExtent(ExtentID id);

    // 同时写入多个block数据, 包含crc
    // 中间的block必须是全部填满block大小
    // 该函数不负责crc的正确性检查
    seastar::future<Status<>> WriteBlocks(ExtentPtr extent_ptr, uint64_t offset,
                                          std::vector<iovec> iov);

    // 读取一个block的数据
    // off - 必须block对齐(32k -4)
    // 返回一个block的物理数据, 包含crc
    seastar::future<Status<seastar::temporary_buffer<char>>> ReadBlock(
        ExtentPtr extent_ptr, uint64_t off);

    // 读取多个block的数据, 每读完一个chunk, 会调用一次callback
    // off - 必须block对齐(32k -4)
    // len   - 数据长度, off+len不能超过extent的数据长度,
    // 需要上层根据extent的实际长度计算好.
    // callback - 读取数据的回调函数
    // 返回的最后一个块的数据可能不足32k
    seastar::future<Status<>> ReadBlocks(
        ExtentPtr extent_ptr, uint64_t off, size_t len,
        seastar::noncopyable_function<
            Status<>(std::vector<seastar::temporary_buffer<char>>)>
            callback);

    seastar::future<Status<>> RemoveExtent(ExtentID id);

    ExtentPtr GetExtent(const ExtentID& id);

    uint64_t Capacity() const;

    uint64_t Used() const;

    seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail

