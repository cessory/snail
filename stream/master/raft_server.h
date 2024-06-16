#pragma once
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>

#include "raft/raft_proto.h"
#include "raft/rawnode.h"
#include "util/util.h"

namespace snail {
namespace stream {

struct RaftServerOption {
    uint64_t node_id = 0;
    uint16_t raft_port = 0;
    uint32_t tick_interval = 1;  // unit(s)
    uint32_t heartbeat_tick = 2;
    uint32_t election_tick = 5;
    std::string wal_dir;
};

class RaftServer;

using RaftServerPtr = seastar::lw_shared_ptr<RaftServer>;

class RaftServer {
    struct ProposeEntry {
        Buffer b;
        seastar::promise<Status<>> pr;
    };
    uint64_t node_id_;
    RaftServerOption opt_;
    std::unique_ptr<RaftWalFactory> wal_factory_;
    raft::RawNodePtr raft_node_;
    RaftReceiverPtr receiver_;
    RaftSenderPtr sender_;
    RaftStoragePtr store_;
    raft::StatemachinePtr sm_;
    uint64_t lead_ = 0;
    seastar::gate gate_;

    seastar::condition_variable cv_;
    seastar::condition_variable apply_cv_;
    seastar::semaphore apply_sem_ = {128};

    std::queue<ProposeEntry> propose_pending_;
    std::queue<raft::ReadyPtr> apply_pending_;
    std::queue<raft::ReadyPtr> apply_completed_;
    std::queue<raft::MessagePtr> recv_pending_;
    std::queue<raft::EntryPtr> conf_change_;
    std::optional<uint64_t> pending_release_index_;
    std::optional<bool> tick_;

    std::optional<seastar::future<Status<>>> conf_change_fu_;

    RaftServer() = default;

    seastar::future<> HandleReady(raft::ReadyPtr rd);
    seastar::future<> Run();

    void HandleRecvMsg(raft::MessagePtr msg);
    seastar::future<Status<>> HandleRecvSnapshot(Buffer b);

    void ReportSnapshot(raft::SnapshotStatus status);

   public:
    static seastar::future<Status<RaftServerPtr>> Create(
        const RaftServerOption opt, uint64_t applied,
        std::vector<RaftNode> nodes, raft::StatemachinePtr sm);

    seastar::future<Status<>> Propose(Buffer b);

    seastar::future<Status<>> AddRaftNode(uint64_t node_id,
                                          std::string raft_host,
                                          uint16_t raft_port, bool learner,
                                          std::string host, uint16_t port);

    seastar::future<Status<>> RemoveRaftNode(uint64_t node_id);

    seastar::future<Status<>> TransferLeader(uint64_t transferee);

    seastar::future<Status<>> ReadIndex();

    // release the wal that is smaller than index
    void ReleaseIndex(uint64_t index);

    bool HasLeader() { return lead_ != 0; }

    seastar::future<Status<>> Close();
};

}  // namespace stream
}  // namespace snail
