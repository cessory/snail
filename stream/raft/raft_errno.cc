#include "raft_errno.h"

#include <unordered_map>

namespace snail {
namespace raft {

static std::unordered_map<int, const char*> raft_err_map = {
    {RAFT_OK, "OK"},
    {RAFT_ERR_COMPACTED, "requested index is unavailable due to compaction"},
    {RAFT_ERR_UNAVAILABLE, "requested entry at index is unavailable"},
    {RAFT_ERR_SNAPOUTOFDATA,
     "requested index is older than the existing snapshot"},
    {RAFT_ERR_UNVALIDATA, "raft config is unvalidate"},
    {RAFT_ERR_CONFCHANGE, "invalid confchange"},
    {RAFT_ERR_SNAPSHOTTEMPORARILYUNAVAILABLE,
     "snapshot is temporarily unavailable"},
    {RAFT_ERR_CONFSTATES, "ConfStates not equivalent"},
    {RAFT_ERR_NOLEADER, "no leader"},
    {RAFT_ERR_PROPOSALDROPPED, "raft proposal dropped"},
    {RAFT_ERR_TRANSFERING, "raft is transfering leadership"},
    {RAFT_ERR_INVALID_CONFCHANGEDATA, "invalid confchange data"},
    {RAFT_ERR_ISLEARNER, "node is learner"},
    {RAFT_ERR_LEADTRANSFER_PROGRESSING, "transfer leadership is in progress"},
    {RAFT_ERR_LEADTRANSFER_SELF, "transfer leadership to self"},
    {RAFT_ERR_STEP_LOCALMSG, "step local msg"},
    {RAFT_ERR_STEP_PEER_NOTFOUND, "peer not found"},
    {RAFT_ERR_NETWORK, "network error"},
    {RAFT_ERR_OPENDIR, "open wal dir error"},
};

const char* raft_error(int err) {
  auto iter = raft_err_map.find(err);
  if (iter == raft_err_map.end()) {
    return "unknown error";
  }
  return iter->second;
}
}  // namespace raft
}  // namespace snail
