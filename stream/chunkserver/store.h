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

#include "status.h"

namespace snail {
namespace stream {

enum class DevType {
  HDD,
  SSD,
  NVME_KERNEL,
  NVME_SPDK,
  PMEM,
};

class Store {
  DevicePtr dev_ptr_;
  SuperBlock super_block_;
  uint64_t used_ = 0;
  std::unordered_map<ExtentID, ExtentPtr> extents_;
  std::queue<uint32_t> free_extents_;
  std::queue<uint32_t> free_chunks_;

  boost::compute::detail::lru_cache<uint32_t, std::string> last_sector_cache_;

  seastar::future<bool> Recover();

  seastar::future<Status<BlockPtr>> AllocBlock(ChunkPtr ptr);

  ExtentPtr GetExtent(const ExtentID& id);

  seastar::future<Status<std::string>> GetLastSector(uint32_t index);

 public:
  static seastar::future<DiskPtr> Load(std::string_view name);

  static seastar::future<bool> Format(std::string_view name,
                                      uint32_t cluster_id, DevType dev_type,
                                      uint32_t dev_id, uint64_t capacity = 0);
  Store() = default;

  uint32_t DeviceId() const;

  uint32_t SectorSize() const;

  seastar::future<Status<>> CreateExtent(ExtentID id);

  seastar::future<Status<>> Write(ExtentID id, uint64_t offset, const char* b,
                                  size_t len);

  seastar::future<Status<>> Write(ExtentID id, uint64_t offset,
                                  std::vector<iovec> iov);

  seastar::future<Status<size_t>> Read(ExtentID id, uint64_t pos, char* buffer,
                                       size_t len);

  seastar::future<Status<seastar::temporary_buffer<char>>> Read(ExtentID id,
                                                                uint64_t pos,
                                                                size_t len);

  seastar::future<Status<void>> RemoveExtent(ExtentID id);

  uint64_t Capacity() const;

  uint64_t Used() const;

  seastar::future<> Close();
};

using StorePtr = seastar::lw_shared_ptr<Store>;

}  // namespace stream
}  // namespace snail

