#pragma once
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include "net/session.h"
#include "proto/master.pb.h"
#include "raft/storage.h"
#include "raft_statemachine.h"

namespace snail {
namespace stream {

class RaftSender {
    struct Client {
        uint64_t node_id;
        std::string host;
        uint16_t port;
        bool last_error_log;
        seastar::shared_mutex sess_mutex;
        net::SessionPtr sess;
        std::optional<net::StreamPtr> stream;
        seastar::gate gate;

        Client(uint64_t id, const std::string& raft_host, uint16_t raft_port);
        seastar::future<Status<>> Connect();
        seastar::future<> Send(std::vector<Buffer> buffers);

        seastar::future<Status<>> SendSnapshot(raft::MessagePtr msg,
                                               SmSnapshotPtr body);

        seastar::future<> Close();
    };
    using ClientPtr = seastar::lw_shared_ptr<Client>;

    std::unordered_map<uint64_t, ClientPtr> senders_;
    seastar::gate gate_;

   public:
    void AddRaftNode(uint64_t node_id, const std::string& raft_host,
                     uint16_t raft_port);

    seastar::future<> RemoveRaftNode(uint64_t node_id);

    seastar::future<> UpdateRaftNodes(std::vector<RaftNode> nodes);

    seastar::future<> Send(std::vector<raft::MessagePtr> msgs);

    seastar::future<Status<>> SendSnapshot(raft::MessagePtr msg,
                                           SmSnapshotPtr body);

    seastar::future<> Close();
};

class RaftReceiver {
    std::string host_;
    uint16_t port_;
    seastar::noncopyable_function<void(raft::MessagePtr)> msg_handle_func_;
    seastar::noncopyable_function<seastar::future<Status<>>(
        raft::SnapshotPtr meta, SmSnapshotPtr body)>
        apply_snapshot_func_;
    seastar::pollable_fd listen_fd_;
    std::unordered_map<uint64_t, net::SessionPtr> sess_map_;
    seastar::gate gate_;

    seastar::future<> HandleSession(net::SessionPtr sess);
    seastar::future<> HandleStream(net::StreamPtr stream);

   public:
    explicit RaftReceiver(
        const std::string& host, uint16_t port,
        seastar::noncopyable_function<void(raft::MessagePtr)> msg_func,
        seastar::noncopyable_function<seastar::future<Status<>>(
            raft::SnapshotPtr meta, SmSnapshotPtr body)>
            apply_snapshot_func);

    seastar::future<> Start();

    seastar::future<> Close();
};

using RaftSenderPtr = seastar::lw_shared_ptr<RaftSender>;
using RaftReceiverPtr = seastar::lw_shared_ptr<RaftReceiver>;

}  // namespace stream
}  // namespace snail
