#pragma once
#include <seastar/core/fstream.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>

#include "util/status.h"

namespace snail {
namespace net {

class TcpConnection {
    seastar::pollable_fd fd_;
    seastar::socket_address remote_address_;
    bool closed_;

   public:
    explicit TcpConnection(seastar::pollable_fd fd,
                           seastar::socket_address remote);
    ~TcpConnection() {}
    seastar::future<Status<>> Write(seastar::net::packet&& p);
    seastar::future<Status<size_t>> Read(char* buffer, size_t size);
    seastar::future<Status<size_t>> ReadExactly(char* buffer, size_t size);
    void Close();
    std::string LocalAddress();
    std::string RemoteAddress();

    static seastar::shared_ptr<TcpConnection> make_connection(
        seastar::pollable_fd fd, seastar::socket_address remote);
};

using TcpConnectionPtr = seastar::shared_ptr<TcpConnection>;

}  // namespace net
}  // namespace snail
