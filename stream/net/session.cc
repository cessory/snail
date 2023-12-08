#include "session.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include "byteorder.h"

namespace snail {
namespace net {

static const size_t default_accept_backlog = 1024;

Session::Session(const Option &opt, ConnectionPtr conn, bool client)
    : opt_(opt),
      conn_(std::move(conn)),
      next_id_(0),
      sequence_(0),
      bucket_sem_(opt.max_receive_buffer),
      die_(false),
      accept_sem_(default_accept_backlog) {}

SessionPtr Session::make_session(const Option &opt, ConnectionPtr conn,
                                 bool client) {
    SessionPtr sess_ptr = seastar::make_lw_shared<Session>(opt, conn, client);
    sess_ptr->recv_fu_ = sess_ptr->RecvLoop();
    sess_ptr->send_fu_ = sess_ptr->SendLoop();
    if (!opt.keep_alive_disabled) {
        sess_ptr->StartKeepalive();
    }
    return sess_ptr;
}

seastar::future<> Session::RecvLoop() {
    auto ptr = shared_from_this();
    auto defer = seastar::defer([this] {
        std::cout << "exit recv loop****************" << std::endl;
        accept_cv_.signal();
        w_cv_.signal();
    });
    while (!die_) {
        auto s = co_await conn_->ReadExactly(
            static_cast<size_t>(STREAM_HEADER_SIZE));
        if (!s.OK()) {
            std::cout << "connection read error: " << s.Reason() << std::endl;
            status_.Set(s.Code(), s.Reason());
            co_await conn_->Close();
            CloseAllStreams();
            break;
        }
        auto hdr = std::move(s.Value());
        if (hdr.empty()) {
            status_.Set(ECONNABORTED);
            std::cout << "connection abort" << std::endl;
            co_await conn_->Close();
            CloseAllStreams();
            break;
        }
        uint8_t ver = hdr[0];
        CmdType cmd = static_cast<CmdType>(hdr[1]);
        uint16_t len = LittleEndian::Uint16(hdr.get() + 2);
        uint32_t sid = LittleEndian::Uint32(hdr.get() + 4);
        if (ver != 1) {
            status_.Set(ECONNABORTED);
            co_await conn_->Close();
            CloseAllStreams();
            break;
        }
        switch (cmd) {
            case CmdType::NOP:
                break;
            case CmdType::SYN: {
                auto it = streams_.find(sid);
                if (it == streams_.end()) {
                    StreamPtr stream = seastar::make_lw_shared<Stream>(
                        sid, (uint32_t)opt_.max_frame_size, shared_from_this());
                    streams_[sid] = stream;
                    try {
                        std::cout << "accept stream=" << sid << std::endl;
                        co_await accept_sem_.wait();
                    } catch (std::exception &e) {
                        std::cout << "accept stream=" << sid << " exception"
                                  << std::endl;
                        co_return;
                    }
                    std::cout << "accept stream=" << sid << " in queue"
                              << std::endl;
                    accept_q_.push(stream);
                    accept_cv_.signal();
                }
                break;
            }
            case CmdType::FIN: {
                auto it = streams_.find(sid);
                if (it != streams_.end()) {
                    auto stream = it->second;
                    stream->Fin();
                }
                break;
            }
            case CmdType::PSH:
                if (len > 0) {
                    auto s =
                        co_await conn_->ReadExactly(static_cast<size_t>(len));
                    if (!s.OK()) {
                        std::cout << "connection read error: " << s.Reason()
                                  << std::endl;
                        status_.Set(s.Code(), s.Reason());
                        co_await conn_->Close();
                        CloseAllStreams();
                        co_return;
                    }
                    if (s.Value().empty()) {
                        std::cout << "connection abort" << std::endl;
                        status_.Set(ECONNABORTED);
                        co_await conn_->Close();
                        CloseAllStreams();
                        co_return;
                    }
                    auto it = streams_.find(sid);
                    if (it != streams_.end()) {
                        auto stream = it->second;
                        try {
                            std::cout << "begin bucket wait len=" << len
                                      << " sid=" << sid << std::endl;
                            co_await bucket_sem_.wait(len);
                        } catch (std::exception &e) {
                            std::cout << "end bucket wait exception len=" << len
                                      << " sid=" << sid << std::endl;
                            co_return;
                        }
                        std::cout << "end bucket wait len=" << len
                                  << " sid=" << sid << std::endl;
                        stream->PushData(std::move(s.Value()));
                    }
                }
                break;
            case CmdType::UPD:
            default:
                status_.Set(EINVAL);
                co_await conn_->Close();
                CloseAllStreams();
                co_return;
        }
    }
    co_return;
}

seastar::future<> Session::SendLoop() {
    auto ptr = shared_from_this();
    while (!die_ && status_.OK()) {
        Status<> s;
        std::queue<Session::write_request *> sent_q;
        seastar::net::packet packet;
        for (int i = 0; i < write_q_.size(); i++) {
            while (!write_q_[i].empty()) {
                auto req = write_q_[i].front();
                write_q_[i].pop();
                sent_q.push(req);
                seastar::temporary_buffer<char> hdr(STREAM_HEADER_SIZE);
                *hdr.get_write() = req->frame.ver;
                *(hdr.get_write() + 1) = req->frame.cmd;
                LittleEndian::PutUint16(hdr.get_write() + 2,
                                        (uint16_t)(req->frame.data.size()));
                LittleEndian::PutUint32(hdr.get_write() + 4, req->frame.sid);
                packet =
                    seastar::net::packet(std::move(packet), std::move(hdr));
                if (req->frame.data.size() > 0) {
                    packet = seastar::net::packet(std::move(packet),
                                                  std::move(req->frame.data));
                }
            }
        }
        if (packet.len() > 0) {
            s = co_await conn_->Write(std::move(packet));
            if (s.OK()) {
                s = co_await conn_->Flush();
            }

            while (!sent_q.empty()) {
                auto req = sent_q.front();
                sent_q.pop();
                if (req->pr.has_value()) {
                    req->pr.value().set_value(s);
                } else {
                    delete req;
                }
            }

            if (!s.OK()) {
                status_ = s;
                co_await conn_->Close();
                CloseAllStreams();
                break;
            }
        }

        co_await w_cv_.wait();
    }

    co_return;
}

void Session::StartKeepalive() {
    if (keepalive_timer_ == nullptr) {
        keepalive_timer_ = new seastar::timer<seastar::steady_clock_type>(
            [this]() { WritePing(); });
    }
}

seastar::future<Status<>> Session::WriteFrameInternal(Frame f,
                                                      ClassID classid) {
    Status<> s;
    if (die_) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s = status_;
        co_return s;
    }

    write_request *req = new write_request();

    req->classid = classid;
    req->frame = std::move(f);
    req->seq = ++sequence_;
    req->pr = seastar::promise<Status<>>();

    write_q_[static_cast<int>(classid)].push(req);
    w_cv_.signal();
    s = co_await req->pr.value().get_future();
    delete req;
    co_return s;
}

void Session::WritePing() {
    if (die_) {
        return;
    }
    if (!status_.OK()) {
        return;
    }

    write_request *req = new write_request();
    req->classid = ClassID::CTRL;
    req->frame.ver = opt_.version;
    req->frame.cmd = CmdType::NOP;
    req->frame.sid = 0;
    req->seq = ++sequence_;

    write_q_[static_cast<int>(ClassID::CTRL)].push(req);
    w_cv_.signal();
    return;
}

seastar::future<Status<StreamPtr>> Session::OpenStream() {
    Status<StreamPtr> s;
    if (die_) {
        s.Set(EPIPE);
        co_return s;
    }
    uint32_t id = next_id_ + 2;
    if (id < next_id_) {
        s.Set(ENOSR);
        co_return s;
    }
    next_id_ = id;

    auto stream = Stream::make_stream(id, (uint32_t)opt_.max_frame_size,
                                      shared_from_this());
    Frame frame;
    frame.ver = opt_.version;
    frame.cmd = CmdType::SYN;
    frame.sid = id;
    auto st = co_await WriteFrameInternal(std::move(frame), ClassID::CTRL);
    if (!st.OK()) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    if (die_) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s.Set(status_.Code(), status_.Reason());
        co_return s;
    }
    streams_[id] = stream;
    s.SetValue(stream);
    co_return s;
}

seastar::future<Status<StreamPtr>> Session::AcceptStream() {
    Status<StreamPtr> s;
    for (;;) {
        if (die_) {
            s.Set(EPIPE);
            break;
        }
        if (!status_.OK()) {
            s.Set(status_.Code(), status_.Reason());
            break;
        }
        std::cout << "begin accept stream" << std::endl;
        co_await accept_cv_.wait();
        std::cout << "end accept stream" << std::endl;

        if (die_) {
            s.Set(EPIPE);
            break;
        }
        if (!status_.OK()) {
            s.Set(status_.Code(), status_.Reason());
            break;
        }

        if (!accept_q_.empty()) {
            auto stream = accept_q_.front();
            accept_q_.pop();
            accept_sem_.signal();
            s.SetValue(stream);
            break;
        }
    }
    co_return s;
}

seastar::future<> Session::Close() {
    if (!die_) {
        die_ = true;
        w_cv_.signal();
        accept_sem_.broken();
        bucket_sem_.broken();
        accept_cv_.signal();
        if (keepalive_timer_) {
            keepalive_timer_->cancel();
            delete keepalive_timer_;
            keepalive_timer_ = nullptr;
        }
        co_await conn_->Close();
        co_await send_fu_.value().then([] {});
        co_await recv_fu_.value().then([] {});
        CloseAllStreams();
    }
    co_return;
}

void Session::CloseAllStreams() {
    for (auto &iter : streams_) {
        iter.second->SessionClose();
    }
    streams_.clear();
    std::queue<StreamPtr> tmp = std::move(accept_q_);
}

}  // namespace net
}  // namespace snail
