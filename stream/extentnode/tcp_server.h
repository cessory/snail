#pragma once
#include <seastar/core/future.hh>
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
    ServicePtr service_;

    seastar::future<> HandleSession(net::SessionPtr sess);
    seastar::future<> HandleStream(net::StreamPtr stream);
    seastar::future<Status<>> HandleMessage(seastar::temporary_buffer<char> b,
                                            net::StreamPtr stream);

   public:
    TcpServer(const std::string& host, uint16_t port, ServicePtr s);

    seastar::future<> Start();
};

}  // namespace stream
}  // namespace snail