#include "server.h"

#include <seastar/core/when_all.hh>

#include "net/tcp_connection.h"
#include "net/tcp_session.h"
#include "util/logger.h"

namespace snail {
namespace stream {

Server::Server(const std::string& host, uint16_t port, ServicePtr service)
    : host_(host), port_(port), service_(service) {}

seastar::future<> Server::HandleSession(net::SessionPtr sess) {
    if (!gate_.is_closed()) {
        sess_mgr_.erase(sess->ID());
        co_await sess->Close();
        co_return;
    }

    seastar::gate::holder holder(gate_);
    while (!gate_.is_closed()) {
        auto st = co_await sess->AcceptStream();
        if (!st) {
            break;
        }
        (void)HandleStream(st.Value());
    }
    sess_mgr_.erase(sess->ID());
    co_await sess->Close();
    co_return;
}

seastar::future<> Server::HandleStream(net::StreamPtr stream) {
    if (!gate_.is_closed()) {
        co_await stream->Close();
        co_return;
    }
    seastar::gate::holder holder(gate_);
    while (!gate_.is_closed()) {
        auto st = co_await stream->ReadFrame();
        if (!st) {
            break;
        }

        auto s = co_await service_->HandleMessage(stream.get(),
                                                  std::move(st.Value()));
        if (!s) {
            break;
        }
    }
    co_await stream->Close();
    co_return;
}

seastar::future<> Server::Start() {
    seastar::socket_address sa(seastar::ipv4_addr(host_, port_));

    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    try {
        fd_ = seastar::engine().posix_listen(sa);
    } catch (std::exception& e) {
        LOG_ERROR("listen error: {}", e.what());
        co_return;
    }

    while (!gate_.is_closed()) {
        try {
            auto ar = co_await fd_.accept();
            auto conn = net::TcpConnection::make_connection(
                std::move(std::get<0>(ar)), std::get<1>(ar));
            net::Option opt;
            auto sess = net::TcpSession::make_session(opt, conn, false);
            sess_mgr_[sess->ID()] = sess;
            (void)HandleSession(sess);
        } catch (std::exception& e) {
            LOG_ERROR("accept error: {}", e.what());
            break;
        }
    }
    co_return;
}

seastar::future<> Server::Close() {
    if (gate_.is_closed()) {
        co_return;
    }
    fd_.close();
    co_await gate_.close();

    std::vector<seastar::future<>> fu_vec;
    std::vector<net::SessionPtr> sess_vec;
    for (;;) {
        auto it = sess_mgr_.begin();
        for (int i = 0; i < 1024 && it != sess_mgr_.end(); i++) {
            auto fu = it->second->Close();
            fu_vec.emplace_back(std::move(fu));
            sess_vec.emplace_back(it->second);
            sess_mgr_.erase(it++);
        }
        if (fu_vec.empty()) {
            break;
        }
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        fu_vec.clear();
        sess_vec.clear();
    }
    // TODO close another source
    co_return;
}

}  // namespace stream
}  // namespace snail
