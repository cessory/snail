#include "client.h"

#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>

#include "tcp_connection.h"
#include "tcp_session.h"

namespace snail {
namespace net {

Client::Client(const std::string& host, uint16_t port, const Option opt,
               uint32_t connect_timeout, net::BufferAllocator* allocator)
    : host_(host),
      port_(port),
      opt_(opt),
      connect_timeout_(connect_timeout),
      allocator_(allocator) {
    if (connect_timeout_ == 0) connect_timeout_ = 100;
    (void)RecyleLoop();
}

seastar::future<> Client::RecyleLoop() {
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::timer<seastar::steady_clock_type> timer;
    timer.set_callback([this] { recyle_cv_.signal(); });

    seastar::gate::holder holder(gate_);
    while (!gate_.is_closed()) {
        if (recyle_list_.empty()) {
            co_await recyle_cv_.wait();
            continue;
        }
        for (auto iter = recyle_list_.begin(); iter != recyle_list_.end();) {
            SessionImplPtr impl = *iter;
            std::vector<seastar::future<>> fu_vec;
            while (!impl->streams_.empty()) {
                auto stream = impl->streams_.front();
                impl->streams_.pop();
                auto fu = stream->Close();
                fu_vec.emplace_back(std::move(fu));
            }
            if (!fu_vec.empty()) {
                co_await seastar::when_all_succeed(fu_vec.begin(),
                                                   fu_vec.end());
            }
            if (impl->sess_->Streams() == 0 || !impl->sess_->Valid()) {
                co_await impl->sess_->Close();
                recyle_list_.erase(iter++);
            } else {
                iter++;
            }
        }

        if (gate_.is_closed()) {
            break;
        }
        timer.arm(std::chrono::milliseconds(connect_timeout_));
        co_await recyle_cv_.wait();
        timer.cancel();
    }

    co_return;
}

seastar::future<Status<StreamPtr>> Client::Get() {
    Status<StreamPtr> s;
    net::StreamPtr stream;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    uint64_t old_sess_id = 0;
    if (sess_impl_) {
        old_sess_id = sess_impl_->sess_->ID();
    }
    if (sess_impl_ && sess_impl_->sess_->Valid()) {
        if (!sess_impl_->streams_.empty()) {
            stream = sess_impl_->streams_.front();
            sess_impl_->streams_.pop();
            s.SetValue(stream);
            co_return s;
        }
        s = co_await sess_impl_->sess_->OpenStream();
        if (s) {
            co_return s;
        }
    }

    co_await mu_.lock();
    if (sess_impl_ && old_sess_id != sess_impl_->sess_->ID()) {
        recyle_list_.push_back(sess_impl_);
        sess_impl_ = nullptr;
        recyle_cv_.signal();
    } else if (sess_impl_) {
        mu_.unlock();
        s = co_await sess_impl_->sess_->OpenStream();
        co_return s;
    }

    // create connection
    seastar::pollable_fd fd;
    seastar::timer<seastar::steady_clock_type> timer;
    try {
        auto addr = co_await seastar::net::dns::resolve_name(host_);
        seastar::socket_address sa(addr, port_);
        fd = seastar::engine().make_pollable_fd(sa, 0);
        timer.set_callback([&fd] { fd.close(); });
        timer.arm(std::chrono::milliseconds(connect_timeout_));
        co_await seastar::engine().posix_connect(fd, sa,
                                                 seastar::socket_address());
        timer.cancel();
        auto conn = TcpConnection::make_connection(std::move(fd), sa);
        auto sess = TcpSession::make_session(opt_, conn, true);
        sess_impl_ = seastar::make_lw_shared<SessionImpl>();
        sess_impl_->sess_ = sess;
    } catch (std::system_error& e) {
        timer.cancel();
        s.Set(e.code().value(), e.what());
        if (fd) fd.close();
        mu_.unlock();
        co_return s;
    } catch (std::exception& e) {
        timer.cancel();
        s.Set(ErrCode::ErrInternal, e.what());
        if (fd) fd.close();
        mu_.unlock();
        co_return s;
    }

    s = co_await sess_impl_->sess_->OpenStream();
    mu_.unlock();
    co_return s;
}

seastar::future<> Client::Put(net::StreamPtr s) {
    if (gate_.is_closed()) {
        co_await s->Close();
        co_return;
    }
    seastar::gate::holder holder(gate_);
    if (!s->Valid() || !sess_impl_ || sess_impl_->sess_->ID() != s->SessID()) {
        co_await s->Close();
        co_return;
    }

    sess_impl_->streams_.push(s);
    co_return;
}

seastar::future<> Client::Close() {
    if (gate_.is_closed()) {
        co_return;
    }
    auto fu = gate_.close();
    recyle_cv_.signal();
    co_await std::move(fu);
    if (sess_impl_) {
        co_await sess_impl_->sess_->Close();
    }

    for (auto iter = recyle_list_.begin(); iter != recyle_list_.end();) {
        SessionImplPtr impl = *iter;
        co_await impl->sess_->Close();
        recyle_list_.erase(iter++);
    }
    co_return;
}

}  // namespace net
}  // namespace snail
