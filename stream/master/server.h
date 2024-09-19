#pragma once

#include "raft_server.h"
#include "service.h"
#include "storage.h"

namespace snail {
namespace stream {

class Server {
    std::string host_;
    uint16_t port_;
    ServicePtr service_;

    seastar::gate gate_;
    seastar::pollable_fd fd_;
    std::unordered_map<uint64_t, net::SessionPtr> sess_mgr_;

    seastar::future<> HandleStream(net::StreamPtr stream);
    seastar::future<> HandleSession(net::SessionPtr sess);

   public:
    explicit Server(const std::string& host, uint16_t port, StoragePtr store,
                    RaftServerPtr raft, ServicePtr service);

    seastar::future<> Start();

    seastar::future<> Close();
};

using ServerPtr = seastar::lw_shared_ptr<Server>;

}  // namespace stream
}  // namespace snail
