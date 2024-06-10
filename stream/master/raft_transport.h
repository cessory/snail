#pragma once

namespace snail {
namespace stream {

class RaftSender {
    struct Client {
        uint64_t node_id;
        std::string host;
        uint16_t port;
        seastar::shared_mutex sess_mutex;
        net::SessionPtr sess;
        std::optional<net::StreamPtr> stream;

        Client(uint64_t id, const std::string& raft_host, uint16_t raft_port);
        seastar::future<Status<>> Connect();
        seastar::future<> Send(std::vector<Buffer> buffers);

        seastar::future<Status<>> SendSnapshot(SnapshotPtr snap,
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

    seastar::future<> Send(std::vector<MessagePtr> msgs);

    seastar::future<Status<>> SendSnapshot(uint64_t to, SnapshotPtr snap,
                                           SmSnapshotPtr body);

    seastar::future<> Close();
};

class RaftReceiver {
    std::string host_;
    uint16_t port_;
    seastar::nocopyable_function<seastar::future<Status<>>(Buffer)>
        msg_handle_func_;
    seastar::nocopyable_function<seastar::future<Status<>>(Buffer)>
        snap_handle_func_;
    seastar::pollable_fd listen_fd_;
    std::unordered_map<uint64_t, net::SessionPtr> sess_map_;
    seastar::gate gate_;

    seastar::future<> HandleSession(net::SessionPtr sess);

   public:
    explicit RaftReceiver(
        const std::string& host, uint16_t port,
        seastar::nocopyable_function<seastar::future<Status<>>(Buffer b)>
            msg_func,
        seastar::nocopyable_function<seastar::future<Status<>>(Buffer b)>
            snap_func);

    seastar::future<Status<>> Start();

    seastar::future<> Close();
};

using RaftSenderPtr = seastar::lw_shared_ptr<RaftSender>;
using RaftReceiverPtr = seastar::lw_shared_ptr<RaftReceiver>;

}  // namespace stream
}  // namespace snail
