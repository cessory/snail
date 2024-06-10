#pragma once

namespace snail {
namespace stream {

class RaftServer {
    struct ProposeEntry {
        Buffer b;
        seastar::promise<Status<>> pr;
    };
    RawNodePtr raft_node_;
    RaftReceiverPtr receiver_;
    RaftSenderPtr sender_;
    RaftStoragePtr store_;
    StatemachinePtr sm_;
    uint64_t lead_ = 0;
    seastar::condition_variable cv_;
    seastar::gate gate_;

    std::queue<ProposeEntry> propose_pending_;

    seastar::condition_variable ready_cv_;
    seastar::semaphore ready_sem_ = {128};
    std::queue<ReadyPtr> ready_pending_;
    std::queue<ReadyPtr> ready_completed_;
    std::queue<MessagePtr> recv_pending_;
    std::queue<EntryPtr> conf_change_;

    std::optional<seastar::future<Status<>>> conf_change_fu_;

    RaftServer();

    seastar::future<> Run();

   public:
    static seastar::future<Status<RaftServerPtr>> Create();

    seastar::future<Status<>> Propose(Buffer b);

    seastar::future<Status<>> AddRaftNode(uint64_t node_id,
                                          std::string raft_host,
                                          uint16_t raft_port, bool learner,
                                          std::string host, uint16_t port);

    seastar::future<Status<>> RemoveRaftNode(uint64_t node_id);

    seastar::future<Status<>> TransferLeader(uint64_t transferee);

    seastar::future<Status<>> ReadIndex(seastar::temporary_buffer<char> rctx);

    bool HasLeader() { return lead_ != 0; }

    seastar::future<Status<>> Close();
};

}  // namespace stream
}  // namespace snail
