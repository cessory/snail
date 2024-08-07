#include "tcp_connection.h"

#include <netinet/tcp.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/posix.hh>
namespace snail {
namespace net {

TcpConnection::TcpConnection(seastar::pollable_fd fd,
                             seastar::socket_address remote)
    : fd_(std::move(fd)), remote_address_(remote), closed_(false) {
    int val = 1;
    fd_.get_file_desc().setsockopt(IPPROTO_TCP, TCP_NODELAY & val, sizeof(val));
}

seastar::future<Status<>> TcpConnection::Write(seastar::net::packet&& p) {
    Status<> s;
    try {
        co_await fd_.write_all(p);
    } catch (std::system_error& e) {
        s.Set(e.code().value());
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrUnExpect, e.what());
    }
    co_return s;
}

seastar::future<Status<size_t>> TcpConnection::Read(char* buffer, size_t len) {
    Status<size_t> s;
    try {
        int val = 1;
        auto n = co_await fd_.read_some(buffer, len);
        fd_.get_file_desc().setsockopt(IPPROTO_TCP, TCP_QUICKACK, &val,
                                       sizeof(int));
        s.SetValue(n);
    } catch (std::system_error& e) {
        s.Set(e.code().value());
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrUnExpect, e.what());
    }
    co_return s;
}

seastar::future<Status<size_t>> TcpConnection::ReadExactly(char* buffer,
                                                           size_t len) {
    Status<size_t> s;
    size_t n = 0;
    while (n < len) {
        try {
            auto bytes = co_await fd_.read_some(buffer + n, len - n);
            n += bytes;
            if (bytes == 0) {  // the conn has been closed
                s.SetValue(0);
                break;
            }
            int val = 1;
            fd_.get_file_desc().setsockopt(IPPROTO_TCP, TCP_QUICKACK, &val,
                                           sizeof(int));
        } catch (std::system_error& e) {
            s.Set(e.code().value());
            co_return s;
        } catch (std::exception& e) {
            s.Set(ErrCode::ErrUnExpect, e.what());
            co_return s;
        }
    }
    s.SetValue(n);
    co_return s;
}

void TcpConnection::Close() {
    if (!closed_) {
        closed_ = true;
        fd_.shutdown(SHUT_RDWR);
    }
}

std::string TcpConnection::LocalAddress() {
    seastar::socket_address sa = fd_.get_file_desc().get_address();
    std::ostringstream ss;
    ss << sa;
    return ss.str();
}

std::string TcpConnection::RemoteAddress() {
    std::ostringstream ss;
    ss << remote_address_;
    return ss.str();
}

seastar::shared_ptr<TcpConnection> TcpConnection::make_connection(
    seastar::pollable_fd fd, seastar::socket_address remote) {
    return seastar::make_shared<TcpConnection>(std::move(fd), remote);
}

}  // namespace net
}  // namespace snail
