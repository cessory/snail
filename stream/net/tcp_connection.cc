#include "tcp_connection.h"

#include <seastar/core/coroutine.hh>
namespace snail {
namespace net {

TcpConnection::TcpConnection(seastar::connected_socket socket,
                             seastar::socket_address remote)
    : socket_(std::move(socket)), remote_address_(remote) {
    socket_.set_nodelay(true);
    out_ = socket_.output(65536);
    in_ = socket_.input();
    closed_ = false;
}

seastar::future<Status<>> TcpConnection::Write(seastar::net::packet&& p) {
    Status<> s(ErrCode::OK);
    try {
        co_await out_.write(std::move(p));
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrSystem, e.what());
    }
    co_return s;
}

seastar::future<Status<>> TcpConnection::Flush() {
    Status<> s(ErrCode::OK);
    try {
        co_await out_.flush();
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrSystem, e.what());
    }
    co_return s;
}

seastar::future<Status<seastar::temporary_buffer<char>>> TcpConnection::Read() {
    Status<seastar::temporary_buffer<char>> s(ErrCode::OK);
    try {
        auto b = co_await in_.read();
        s.SetValue(std::move(b));
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrSystem, e.what());
    }
    co_return s;
}

seastar::future<Status<seastar::temporary_buffer<char>>>
TcpConnection::ReadExactly(size_t n) {
    Status<seastar::temporary_buffer<char>> s(ErrCode::OK);
    try {
        auto b = co_await in_.read_exactly(n);
        s.SetValue(std::move(b));
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrSystem, e.what());
    }
    co_return s;
}

seastar::future<Status<>> TcpConnection::Close() {
    Status<> s;
    if (!closed_) {
        closed_ = true;
        try {
            co_await out_.close();
            co_await in_.close();
            socket_.shutdown_output();
            socket_.shutdown_input();
        } catch (std::exception& e) {
            s.Set(ErrCode::ErrSystem, e.what());
        }
    }
    co_return s;
}

std::string TcpConnection::LocalAddress() {
    seastar::socket_address sa = socket_.local_address();
    std::ostringstream ss;
    ss << sa;
    return ss.str();
}

std::string TcpConnection::RemoteAddress() {
    std::ostringstream ss;
    ss << remote_address_;
    return ss.str();
}

seastar::shared_ptr<Connection> TcpConnection::make_connection(
    seastar::connected_socket socket, seastar::socket_address remote) {
    TcpConnectionPtr ptr =
        seastar::make_shared<TcpConnection>(std::move(socket), remote);
    return seastar::dynamic_pointer_cast<Connection, TcpConnection>(ptr);
}

}  // namespace net
}  // namespace snail
