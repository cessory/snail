#include "session.h"

#include <limits.h>

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include "byteorder.h"

namespace snail {
namespace net {

static const size_t default_accept_backlog = 1024;

Session::Session(const Option &opt, ConnectionPtr conn, bool client)
    : opt_(opt),
      conn_(std::move(conn)),
      next_id_((client ? 1 : 0)),
      die_(false),
      accept_sem_(default_accept_backlog),
      tokens_(opt.max_receive_buffer) {}

SessionPtr Session::make_session(const Option &opt, ConnectionPtr conn,
                                 bool client) {
    SessionPtr sess_ptr = seastar::make_lw_shared<Session>(opt, conn, client);
    sess_ptr->recv_fu_ = sess_ptr->RecvLoop();
    sess_ptr->send_fu_ = sess_ptr->SendLoop();
    sess_ptr->StartKeepalive();
    return sess_ptr;
}

seastar::future<> Session::RecvLoop() {
    auto ptr = shared_from_this();
    auto defer = seastar::defer([this] {
        accept_cv_.signal();
        w_cv_.signal();
    });
    while (!die_) {
        if (tokens_ < 0) {
            co_await token_cv_.wait();
            if (die_ || !status_.OK()) {
                break;
            }
        }
        auto s = co_await conn_->ReadExactly(
            static_cast<size_t>(STREAM_HEADER_SIZE));
        if (!s.OK()) {
            SetStatus(s.Code(), s.Reason());
            co_await conn_->Close();
            CloseAllStreams();
            break;
        }
        auto hdr = std::move(s.Value());
        if (hdr.empty()) {
            SetStatus(static_cast<ErrCode>(ECONNABORTED));
            co_await conn_->Close();
            CloseAllStreams();
            break;
        }
        uint8_t ver = hdr[0];
        CmdType cmd = static_cast<CmdType>(hdr[1]);
        uint16_t len = LittleEndian::Uint16(hdr.get() + 2);
        uint32_t sid = LittleEndian::Uint32(hdr.get() + 4);
        if (ver != 1 && ver != 2) {
            SetStatus(static_cast<ErrCode>(ECONNABORTED));
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
                    StreamPtr stream = Stream::make_stream(
                        sid, ver, (uint32_t)opt_.max_frame_size,
                        shared_from_this());
                    streams_[sid] = stream;
                    try {
                        co_await accept_sem_.wait();
                    } catch (std::exception &e) {
                        co_return;
                    }
                    accept_q_.emplace(stream);
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
                        SetStatus(s.Code(), s.Reason());
                        co_await conn_->Close();
                        CloseAllStreams();
                        co_return;
                    }
                    if (s.Value().empty()) {
                        SetStatus(static_cast<ErrCode>(ECONNABORTED));
                        co_await conn_->Close();
                        CloseAllStreams();
                        co_return;
                    }
                    auto it = streams_.find(sid);
                    if (it != streams_.end()) {
                        auto stream = it->second;
                        stream->PushData(std::move(s.Value()));
                        tokens_ -= len;
                    }
                }
                break;
            case CmdType::UPD:
                if (len != 8) {
                    SetStatus(static_cast<ErrCode>(EINVAL));
                    co_await conn_->Close();
                    CloseAllStreams();
                    co_return;
                } else {
                    auto s =
                        co_await conn_->ReadExactly(static_cast<size_t>(len));
                    if (!s.OK()) {
                        SetStatus(s.Code(), s.Reason());
                        co_await conn_->Close();
                        CloseAllStreams();
                        co_return;
                    }
                    if (s.Value().empty()) {
                        SetStatus(static_cast<ErrCode>(ECONNABORTED));
                        co_await conn_->Close();
                        CloseAllStreams();
                        co_return;
                    }
                    auto it = streams_.find(sid);
                    if (it != streams_.end()) {
                        auto stream = it->second;
                        uint32_t consumed =
                            LittleEndian::Uint32(s.Value().get());
                        uint32_t window =
                            LittleEndian::Uint32(s.Value().get() + 4);
                        stream->Update(consumed, window);
                    }
                }
                break;
            default:
                SetStatus(static_cast<ErrCode>(EINVAL));
                co_await conn_->Close();
                CloseAllStreams();
                co_return;
        }
    }
    co_return;
}

seastar::future<> Session::SendLoop() {
    static uint32_t max_packet_num = IOV_MAX;
    auto ptr = shared_from_this();
    auto defer = seastar::defer([this] {
        accept_cv_.signal();
        accept_sem_.broken();
        token_cv_.signal();
        SetStatus(static_cast<ErrCode>(EPIPE));
        for (int i = 0; i < 2; i++) {
            while (!write_q_[i].empty()) {
                auto req = write_q_[i].front();
                write_q_[i].pop();
                if (req->pr.has_value()) {
                    req->pr.value().set_value(status_);
                } else {
                    delete req;
                }
            }
        }
    });
    while (!die_ && status_.OK()) {
        Status<> s;
        std::queue<Session::write_request *> sent_q;
        seastar::net::packet packet;
        uint32_t packet_n = 0;
        for (int i = 0; i < write_q_.size(); i++) {
            while (!write_q_[i].empty() && packet_n < max_packet_num) {
                auto req = write_q_[i].front();
                if (req->frame.data.size() > 0) {
                    packet_n += 2;
                } else {
                    packet_n += 1;
                }
                if (packet_n > max_packet_num) {
                    break;
                }
                write_q_[i].pop();
                sent_q.emplace(req);
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
                SetStatus(s);
                co_await conn_->Close();
                CloseAllStreams();
                break;
            }
        }

        if (write_q_[0].empty() && write_q_[1].empty()) {
            co_await w_cv_.wait();
        }
    }

    co_return;
}

void Session::StartKeepalive() {
    if (opt_.keep_alive_enable) {
        keepalive_timer_.set_callback([this]() { WritePing(); });
        keepalive_timer_.rearm_periodic(
            std::chrono::seconds(opt_.keep_alive_interval));
    }
}

void Session::ReturnTokens(uint32_t n) {
    tokens_ += n;
    if (tokens_ > 0) {
        token_cv_.signal();
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
    req->pr = seastar::promise<Status<>>();

    write_q_[static_cast<int>(classid)].emplace(req);
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

    write_q_[static_cast<int>(ClassID::CTRL)].emplace(req);
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

    auto stream = Stream::make_stream(
        id, opt_.version, (uint32_t)opt_.max_frame_size, shared_from_this());
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
        if (accept_q_.empty()) {
            co_await accept_cv_.wait();
        }

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
        token_cv_.signal();
        accept_cv_.signal();
        keepalive_timer_.cancel();
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
