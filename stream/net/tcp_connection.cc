#include "tcp_connection.h"

namespace snail {
namespace net {

TcpConnection::TcpConnection(seastar::connected_socket&& socket)
    socket_(socket) {
    out_ = socket_.output();
    in_ = socket_.input();
}

seastar::future<Status<>> TcpConnection::Write(seastar::net::packet p) {
    Status<> s(ErrCode::OK);
    try {
        co_await out_.write(p);
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
    Status<> s(ErrCode::OK);
    try {
        co_await out_.close();
        co_await in_.close();
        socket_.shutdown_output();
        socket_.shutdown_input();
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrSystem, e.what());
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
    seastar::socket_address sa = socket_.remote_address();
    std::ostringstream ss;
    ss << sa;
    return ss.str();
}

seastar::shared_ptr<Connection> TcpConnection::make_connection(
    seastar::connected_socket&& socket) {
    TcpConnectionPtr ptr = seastar::make_shared<TcpConnection>(socket);
    return seastar::dynamic_pointer_cast<Connection, TcpConnection>(ptr);
}

}  // namespace net
}  // namespace snail
