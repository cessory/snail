#pragma once
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/socket_defs.hh>
#include <string>

#include "net/tcp_session.h"
#include "net/tcp_stream.h"
#include "service.h"
namespace snail {
namespace stream {

class TcpServer {
    seastar::socket_address sa_;
    unsigned shard_index_;
    std::vector<unsigned> shards_;
    seastar::pollable_fd fd_;
    // each disk has a service
    std::unordered_map<uint32_t, seastar::foreign_ptr<ServicePtr>> service_map_;
    std::optional<seastar::promise<>> start_pr_;
    std::vector<std::unordered_map<uint64_t, net::SessionPtr>> sess_mgr_;

    seastar::future<> HandleSession(net::SessionPtr sess);
    seastar::future<> HandleStream(net::StreamPtr stream);
    seastar::future<Status<>> HandleMessage(seastar::temporary_buffer<char> b,
                                            net::StreamPtr stream);

   public:
    TcpServer(const std::string& host, uint16_t port,
              const std::set<unsigned>& shards);

    seastar::future<> RegisterService(seastar::foreign_ptr<ServicePtr> service);

    seastar::future<> Start();

    seastar::future<> Close();

    static seastar::future<Status<>> SendResp(
        const ::google::protobuf::Message* resp, ExtentnodeMsgType msgType,
        net::Stream* stream);
};

}  // namespace stream
}  // namespace snail
