#include "status.h"

#include <unordered_map>

namespace snail {

static std::unordered_map<ErrCode, const char*> codeMaps = {
    {ErrCode::OK, "OK"},
    {ErrCode::ErrEOF, "end of file"},
    {ErrCode::ErrExistExtent, "extent has already exist"},
    {ErrCode::ErrExtentNotFound, "not found extent"},
    {ErrCode::ErrExtentIsWriting, "extent is writing"},
    {ErrCode::ErrOverWrite, "write disk error"},
    {ErrCode::ErrParallelWrite, "parallel write extent"},
    {ErrCode::ErrTooShort, "data too short"},
    {ErrCode::ErrTooLarge, "data too larger"},
    {ErrCode::ErrInvalidChecksum, "invalid checksum"},
    {ErrCode::ErrCluster, "invalid cluster id"},

    ////////raft error/////////////
    {ErrCode::ErrRaftCompacted,
     "requested index is unavailable due to compaction"},
    {ErrCode::ErrRaftUnavailable, "requested entry at index is unavailable"},
    {ErrCode::ErrRaftSnapOutOfData,
     "requested index is older than the existing snapshot"},
    {ErrCode::ErrRaftUnvalidata, "raft config is unvalidate"},
    {ErrCode::ErrRaftConfChange, "invalid confchange"},
    {ErrCode::ErrRaftConfStates, "ConfStates not equivalent"},
    {ErrCode::ErrRaftSnapshotTemporarilyUnavailable,
     "snapshot is temporarily unavailable"},
    {ErrCode::ErrRaftProposalDropped, "raft proposal dropped"},
    {ErrCode::ErrRaftTransfering, "raft is transfering leadership"},
    {ErrCode::ErrRaftIslearner, "node is learner"},
    {ErrCode::ErrRaftLeadtransferProgressing,
     "transfer leadership is in progress"},
    {ErrCode::ErrRaftLeadtransferSelf, "transfer leadership to self"},
    {ErrCode::ErrRaftNoLeader, "no leader"},
    {ErrCode::ErrRaftStepLocalMsg, "step local msg"},
    {ErrCode::ErrRaftStepPeerNotFound, "raft peer not found"},
    {ErrCode::ErrRaftAbort, "raft abort"},
    {ErrCode::ErrUnExpect, "unexpect error"},
};

const char* GetReason(ErrCode code) {
    if (static_cast<int>(code) < 10000 && static_cast<int>(code) != 0) {
        return std::strerror(static_cast<int>(code));
    }
    auto iter = codeMaps.find(code);
    if (iter == codeMaps.end()) {
        return "unknown error";
    }
    return iter->second;
}

}  // namespace snail
