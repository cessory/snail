#pragma once

namespace snail {
namespace stream {

class LeaseMgr {
    std::unordered_map<ChunkID, uint64_t> leases_;

   public:
    LeaseMgr() = default;

    uint64_t GetLease(ChunkID id) { return leases_[id]; }

    void SetLease(ChunkID id, uint64_t lease) { leases_[id] = lease; }

    void RemoveLease(ChunkID id) { leases_.erase(id); }
};

using LeaseMgrPtr = seastar::lw_shared_ptr<LeaseMgr>;

}  // namespace stream
}  // namespace snail
