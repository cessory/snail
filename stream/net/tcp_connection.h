#pragma once
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>

#include "connection.h"

namespace snail {
namespace net {

class TcpConnection : public Connection {
    seastar::connected_socket socket_;
    seastar::socket_address remote_address_;
    seastar::output_stream<char> out_;
    seastar::input_stream<char> in_;
    bool closed_;

   public:
    explicit TcpConnection(seastar::connected_socket socket,
                           seastar::socket_address remote);
    virtual ~TcpConnection() {}
    virtual seastar::future<Status<>> Write(seastar::net::packet&& p);
    virtual seastar::future<Status<>> Flush();
    virtual seastar::future<Status<seastar::temporary_buffer<char>>> Read();
    virtual seastar::future<Status<seastar::temporary_buffer<char>>>
    ReadExactly(size_t n);
    virtual seastar::future<Status<>> Close();
    virtual std::string LocalAddress();
    virtual std::string RemoteAddress();

    static seastar::shared_ptr<Connection> make_connection(
        seastar::connected_socket socket, seastar::socket_address remote);
};

using TcpConnectionPtr = seastar::shared_ptr<TcpConnection>;

}  // namespace net
}  // namespace snail
