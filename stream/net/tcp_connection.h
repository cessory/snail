#pragma once
#include "connection.h"

namespace snail {
namespace net {

class TcpConnection : public Connection {
    seastar::connected_socket socket_;
    seastar::output_stream out_;
    seastar::input_stream in_;

   public:
    explicit TcpConnection(seastar::connected_socket&& socket);
    virtual ~TcpConnection() {}
    virtual seastar::future<Status<>> Write(seastar::net::packet p);
    virtual seastar::future<Status<>> Flush();
    virtual seastar::future<Status<seastar::temporary_buffer<char>>> Read();
    virtual seastar::future<Status<seastar::temporary_buffer<char>>>
    ReadExactly(size_t n);
    virtual seastar::future<Status<>> Close();
    virtual std::string LocalAddress();
    virtual std::string RemoteAddress();

    static seastar::shared_ptr<Connection> make_connection(
        seastar::connected_socket&& socket);
};

using TcpConnectionPtr = seastar::shared_ptr<TcpConnection>;

}  // namespace net
}  // namespace snail
