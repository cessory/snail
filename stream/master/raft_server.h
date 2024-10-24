#pragma once
#include <chrono>
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include "proto/master.pb.h"
#include "raft/raft_proto.h"
#include "raft/rawnode.h"
#include "raft/storage.h"
#include "raft_statemachine.h"
#include "raft_storage.h"
#include "raft_transport.h"
#include "util/util.h"
#include "wal/wal.h"

namespace snail {
namespace stream {

struct RaftServerOption {
    uint64_t node_id = 0;
    uint32_t tick_interval = 1;  // unit(s)
    uint32_t heartbeat_tick = 1;
    uint32_t election_tick = 3;
    std::string wal_dir;
};

class RaftServer;

using RaftServerPtr = seastar::lw_shared_ptr<RaftServer>;

class RaftServer {
    struct ProposeEntry {
        raft::EntryPtr ent;
        seastar::promise<Status<>> pr;
    };

    struct ReadIndexItem {
        uint64_t id;
        seastar::timer<std::chrono::steady_clock> timer;
        seastar::shared_promise<Status<uint64_t>> pr;
    };

    using ReadIndexItemPtr = seastar::lw_shared_ptr<ReadIndexItem>;

    uint64_t node_id_;
    RaftServerOption opt_;
    std::unique_ptr<RaftWalFactory> wal_factory_;
    raft::RawNodePtr raft_node_;
    RaftReceiverPtr receiver_;
    RaftSenderPtr sender_;
    seastar::shared_ptr<RaftStorage> store_;
    StatemachinePtr sm_;
    uint64_t lead_ = 0;
    uint64_t read_index_seq_ = 0;
    seastar::gate gate_;

    seastar::condition_variable cv_;
    seastar::condition_variable apply_cv_;
    seastar::condition_variable apply_waiter_;
    seastar::semaphore apply_sem_ = {128};

    std::queue<ProposeEntry*> propose_pending_;
    std::queue<raft::ReadyPtr> apply_pending_;
    std::queue<raft::MessagePtr> recv_pending_;
    std::queue<raft::EntryPtr> conf_change_;
    std::optional<uint64_t> pending_release_wal_;
    std::optional<bool> tick_;
    std::optional<ReadIndexItemPtr> read_index_;

    RaftServer() = default;

    seastar::future<> HandlePropose();
    seastar::future<Status<>> HandleConfChange(raft::EntryPtr ent);
    seastar::future<> SendSnapshotMsg(raft::MessagePtr msg);
    seastar::future<> HandleReady(raft::ReadyPtr rd);
    void HandleRecvMsg(raft::MessagePtr msg);
    seastar::future<Status<>> ApplySnapshot(raft::SnapshotPtr meta,
                                            SmSnapshotPtr body);
    void ReportSnapshot(raft::SnapshotStatus status);
    void ApplySnapshotCompleted(raft::MessagePtr msg);

    seastar::future<> Run();
    seastar::future<> ApplyLoop();

   public:
    static seastar::future<Status<RaftServerPtr>> Create(
        const RaftServerOption opt, uint64_t applied,
        std::vector<RaftNode> nodes, StatemachinePtr sm);

    seastar::future<> Start();

    seastar::future<Status<>> Propose(Buffer b);

    seastar::future<Status<>> AddRaftNode(uint64_t node_id,
                                          std::string raft_host,
                                          uint16_t raft_port, bool learner,
                                          std::string host, uint16_t port);

    seastar::future<Status<>> RemoveRaftNode(uint64_t node_id);

    RaftNode GetRaftNode(uint64_t id);

    void TransferLeader(uint64_t transferee);

    // return the committed index
    seastar::future<Status<uint64_t>> ReadIndex();

    // release the wal that is smaller than index
    void ReleaseWal(uint64_t index);

    bool HasLeader() { return lead_ != 0; }

    seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail
