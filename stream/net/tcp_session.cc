#include "tcp_session.h"

#include <limits.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

#include "byteorder.h"

namespace snail {
namespace net {

static const size_t default_accept_backlog = 1024;

static constexpr uint32_t kMinStreamBufferSize = 1 << 20;
static constexpr uint32_t kMinReceiveBufferSize = 4 << 20;

std::atomic<uint64_t> TcpSession::session_id_ = 1;

class DefaultBufferAllocator : public BufferAllocator {
   public:
    virtual ~DefaultBufferAllocator() {}
    seastar::temporary_buffer<char> Allocate(size_t len) override {
        size_t origin_len = len;
        len = std::max(len, static_cast<size_t>(4096));
        auto buf = seastar::temporary_buffer<char>::aligned(4096, len);
        buf.trim(origin_len);
        return buf;
    }
};

TcpSession::TcpSession(const Option &opt, TcpConnectionPtr conn, bool client,
                       std::unique_ptr<BufferAllocator> allocator)
    : opt_(opt),
      sess_id_(TcpSession::session_id_++),
      max_receive_buffer_(
          std::max(32 * opt.max_frame_size, kMinReceiveBufferSize)),
      max_stream_buffer_(
          std::max(8 * opt.max_frame_size, kMinStreamBufferSize)),
      conn_(std::move(conn)),
      client_(client),
      allocator_(allocator ? allocator.release()
                           : dynamic_cast<BufferAllocator *>(
                                 new DefaultBufferAllocator())),
      next_id_((client ? 1 : 0)),
      accept_sem_(default_accept_backlog),
      tokens_(max_receive_buffer_) {
    if (opt.max_frame_size > kMaxFrameSize) {
        throw std::runtime_error("invalid max frame size");
    }
}

TcpSession::~TcpSession() { ClearWriteq(); }

SessionPtr TcpSession::make_session(
    const Option &opt, TcpConnectionPtr conn, bool client,
    std::unique_ptr<BufferAllocator> allocator) {
    auto sess_ptr = seastar::make_shared<TcpSession>(opt, conn, client,
                                                     std::move(allocator));
    sess_ptr->recv_fu_ = sess_ptr->RecvLoop();
    sess_ptr->send_fu_ = sess_ptr->SendLoop();
    sess_ptr->StartKeepalive();
    return seastar::dynamic_pointer_cast<Session, TcpSession>(sess_ptr);
}

seastar::future<> TcpSession::RecvLoop() {
    auto ptr = shared_from_this();
    char hdr[STREAM_HEADER_SIZE];
    char buffer[4096];
    bool break_loop = false;
    seastar::timer<seastar::steady_clock_type> ping_timer;
    ping_timer.set_callback([this] {
        SetStatus(static_cast<ErrCode>(ETIMEDOUT));
        conn_->Close();
    });
    bool wait_ping =
        opt_.keep_alive_enable && opt_.keep_alive_interval > 0 && client_;
    uint32_t hdr_len = 0;
    uint32_t buffer_len = 0;
    uint32_t buffer_pos = 0;

    while (!gate_.is_closed() && !break_loop) {
        if (tokens_ < 0) {
            co_await token_cv_.wait();
            if (gate_.is_closed() || !status_.OK()) {
                break;
            }
        }
        if (buffer_len) {
            uint32_t n = std::min(buffer_len, STREAM_HEADER_SIZE - hdr_len);
            if (n) {
                memcpy(&hdr[hdr_len], &buffer[buffer_pos], n);
                buffer_len -= n;
                buffer_pos += n;
                hdr_len += n;
                if (buffer_len == 0) {
                    buffer_pos = 0;
                }
            }
        } else {
            buffer_pos = 0;
        }

        if (hdr_len < STREAM_HEADER_SIZE) {
            if (wait_ping) {
                ping_timer.arm(
                    std::chrono::seconds(3 * opt_.keep_alive_interval));
            }
            auto s = co_await conn_->Read(&buffer[buffer_pos],
                                          sizeof(buffer) - buffer_pos);
            ping_timer.cancel();
            if (!s) {
                SetStatus(s.Code(), s.Reason());
                break;
            }

            if (s.Value() == 0) {
                SetStatus(static_cast<ErrCode>(ECONNABORTED));
                break;
            }
            buffer_len += s.Value();
            continue;
        }
        Frame f;
        uint32_t len = f.Unmarshal(hdr);
        if (f.ver != 2) {
            SetStatus(static_cast<ErrCode>(ECONNABORTED));
            break;
        }
        hdr_len = 0;
        switch (f.cmd) {
            case CmdType::PING:
                WritePingPong(false);
                break;
            case CmdType::PONG:
                break;
            case CmdType::SYN: {
                auto it = streams_.find(f.sid);
                if (it == streams_.end()) {
                    StreamPtr stream = TcpStream::make_stream(
                        f.sid, f.ver, opt_.max_frame_size, shared_from_this());
                    streams_[f.sid] =
                        seastar::dynamic_pointer_cast<TcpStream, Stream>(
                            stream);
                    try {
                        co_await accept_sem_.wait();
                        accept_q_.emplace(stream);
                        accept_cv_.signal();
                    } catch (std::exception &e) {
                        break_loop = true;
                    }
                }
                break;
            }
            case CmdType::FIN: {
                auto it = streams_.find(f.sid);
                if (it != streams_.end()) {
                    auto stream = it->second;
                    stream->Fin();
                }
                break;
            }
            case CmdType::PSH:
                if (len > 0) {
                    auto buf = allocator_->Allocate(static_cast<size_t>(len));
                    uint32_t data_len = 0;
                    if (buffer_len) {
                        uint32_t n = std::min(buffer_len, len);
                        memcpy(buf.get_write(), &buffer[buffer_pos], n);
                        buffer_len -= n;
                        buffer_pos += n;
                        data_len += n;
                    }
                    if (len > data_len) {
                        if (wait_ping) {
                            ping_timer.arm(std::chrono::seconds(
                                3 * opt_.keep_alive_interval));
                        }
                        auto s = co_await conn_->ReadExactly(
                            buf.get_write() + data_len, len - data_len);
                        ping_timer.cancel();
                        if (!s || s.Value() == 0) {
                            if (!s) {
                                SetStatus(s.Code(), s.Reason());
                            } else {
                                SetStatus(static_cast<ErrCode>(ECONNRESET));
                            }
                            break_loop = true;
                        }
                    }
                    if (!break_loop) {
                        auto it = streams_.find(f.sid);
                        if (it != streams_.end()) {
                            auto stream = it->second;
                            stream->PushData(std::move(buf));
                            tokens_ -= len;
                        }
                    }
                }
                break;
            case CmdType::UPD:
                if (len != 8) {
                    SetStatus(static_cast<ErrCode>(EINVAL));
                    break_loop = true;
                } else {
                    char buf[8];
                    uint32_t data_len = 0;
                    if (buffer_len) {
                        uint32_t n = std::min(buffer_len, len);
                        memcpy(buf, &buffer[buffer_pos], n);
                        buffer_len -= n;
                        buffer_pos += n;
                        data_len += n;
                    }
                    if (len > data_len) {
                        if (wait_ping) {
                            ping_timer.arm(std::chrono::seconds(
                                3 * opt_.keep_alive_interval));
                        }
                        auto s = co_await conn_->ReadExactly(&buf[data_len],
                                                             len - data_len);
                        ping_timer.cancel();
                        if (!s || s.Value() == 0) {
                            if (!s) {
                                SetStatus(s.Code(), s.Reason());
                            } else {
                                SetStatus(static_cast<ErrCode>(ECONNRESET));
                            }
                            break_loop = true;
                        }
                    }
                    if (!break_loop) {
                        auto it = streams_.find(f.sid);
                        if (it != streams_.end()) {
                            auto stream = it->second;
                            uint32_t consumed = LittleEndian::Uint32(buf);
                            uint32_t window = LittleEndian::Uint32(&buf[4]);
                            stream->Update(consumed, window);
                        }
                    }
                }
                break;
            default:
                SetStatus(static_cast<ErrCode>(EINVAL));
                break_loop = true;
                break;
        }
    }
    conn_->Close();
    accept_cv_.signal();
    w_cv_.signal();
    co_await CloseAllStreams();
    co_return;
}

seastar::future<> TcpSession::SendLoop() {
    static uint32_t max_packet_num = IOV_MAX;
    auto ptr = shared_from_this();
    seastar::timer<seastar::steady_clock_type> write_timer;
    write_timer.set_callback([this] {
        SetStatus(static_cast<ErrCode>(ETIMEDOUT));
        conn_->Close();
    });

    while (!gate_.is_closed() && status_.OK()) {
        Status<> s;
        std::queue<TcpSession::write_request *> sent_q;
        seastar::net::packet packet;
        uint32_t packet_n = 0;
        for (int i = 0; i < write_q_.size(); i++) {
            while (!write_q_[i].empty() && packet_n < max_packet_num) {
                auto req = write_q_[i].front();
                if (req->frame.packet.nr_frags() > 0) {
                    packet_n += 1 + req->frame.packet.nr_frags();
                } else {
                    packet_n += 1;
                }
                if (packet_n > max_packet_num) {
                    break;
                }
                write_q_[i].pop();
                sent_q.emplace(req);
                seastar::temporary_buffer<char> hdr(STREAM_HEADER_SIZE);
                req->frame.MarshalTo(hdr.get_write());
                packet.append(std::move(hdr));
                if (req->frame.packet.nr_frags() > 0) {
                    packet.append(std::move(req->frame.packet));
                }
            }
        }
        if (packet.len() > 0) {
            if (opt_.write_timeout_s > 0) {
                write_timer.arm(std::chrono::seconds(opt_.write_timeout_s));
            }
            s = co_await conn_->Write(std::move(packet));
            write_timer.cancel();
            while (!sent_q.empty()) {
                auto req = sent_q.front();
                sent_q.pop();
                if (req->pr.has_value()) {
                    Status<> st = s;
                    req->pr.value().set_value(std::move(st));
                } else {
                    delete req;
                }
            }

            if (!s) {
                SetStatus(s);
                break;
            }
        }

        if (write_q_[0].empty() && write_q_[1].empty()) {
            co_await w_cv_.wait();
        }
    }

    conn_->Close();
    auto fu = CloseAllStreams();
    accept_cv_.signal();
    accept_sem_.broken();
    token_cv_.signal();
    SetStatus(static_cast<ErrCode>(EPIPE));
    ClearWriteq();
    co_await std::move(fu);
    co_return;
}

void TcpSession::ClearWriteq() {
    for (int i = 0; i < 2; i++) {
        while (!write_q_[i].empty()) {
            auto req = write_q_[i].front();
            write_q_[i].pop();
            if (req->pr.has_value()) {
                Status<> s = status_;
                req->pr.value().set_value(std::move(s));
            } else {
                delete req;
            }
        }
    }
}

void TcpSession::StartKeepalive() {
    if (opt_.keep_alive_enable && client_ && opt_.keep_alive_interval > 0) {
        keepalive_timer_.set_callback([this]() { WritePingPong(true); });
        keepalive_timer_.rearm_periodic(
            std::chrono::seconds(opt_.keep_alive_interval));
    }
}

void TcpSession::ReturnTokens(uint32_t n) {
    tokens_ += n;
    if (tokens_ > 0) {
        token_cv_.signal();
    }
}

seastar::future<Status<>> TcpSession::WriteFrameInternal(Frame f,
                                                         ClassID classid) {
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s = status_;
        co_return s;
    }

    std::unique_ptr<write_request> req(new write_request());

    req->frame = std::move(f);
    req->pr = seastar::promise<Status<>>();

    write_q_[static_cast<int>(classid)].emplace(req.get());
    w_cv_.signal();
    s = co_await req->pr.value().get_future();
    co_return s;
}

void TcpSession::WritePingPong(bool ping) {
    if (gate_.is_closed()) {
        return;
    }
    if (!status_) {
        return;
    }

    write_request *req = new write_request();
    req->frame.ver = 2;
    req->frame.cmd = ping ? CmdType::PING : CmdType::PONG;
    req->frame.sid = 0;

    write_q_[static_cast<int>(ClassID::DATA)].emplace(req);
    w_cv_.signal();
    return;
}

seastar::future<Status<StreamPtr>> TcpSession::OpenStream() {
    Status<StreamPtr> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    uint32_t id = next_id_ + 2;
    if (id < next_id_) {
        s.Set(ENOSR);
        co_return s;
    }
    next_id_ = id;

    auto stream =
        TcpStream::make_stream(id, 2, opt_.max_frame_size, shared_from_this());
    Frame frame;
    frame.ver = 2;
    frame.cmd = CmdType::SYN;
    frame.sid = id;
    auto st = co_await WriteFrameInternal(std::move(frame), ClassID::CTRL);
    if (!st.OK()) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    if (!status_.OK()) {
        s.Set(status_.Code(), status_.Reason());
        co_return s;
    }
    streams_[id] = seastar::dynamic_pointer_cast<TcpStream, Stream>(stream);
    s.SetValue(stream);
    co_return s;
}

seastar::future<Status<StreamPtr>> TcpSession::AcceptStream() {
    Status<StreamPtr> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    for (;;) {
        if (gate_.is_closed()) {
            s.Set(EPIPE);
            break;
        }
        if (!status_.OK()) {
            s.Set(status_.Code(), status_.Reason());
            break;
        }
        if (accept_q_.empty()) {
            co_await accept_cv_.wait();
            continue;
        }
        break;
    }
    if (!accept_q_.empty() && s) {
        auto stream = accept_q_.front();
        accept_q_.pop();
        accept_sem_.signal();
        s.SetValue(stream);
    }
    co_return s;
}

seastar::future<> TcpSession::Close() {
    if (!gate_.is_closed()) {
        auto fu = gate_.close();
        w_cv_.signal();
        accept_sem_.broken();
        token_cv_.signal();
        accept_cv_.signal();
        keepalive_timer_.cancel();
        conn_->Close();
        co_await std::move(send_fu_.value());
        co_await std::move(recv_fu_.value());
        co_await CloseAllStreams();
        co_await std::move(fu);
    }
    co_return;
}

seastar::future<> TcpSession::CloseAllStreams() {
    std::vector<seastar::future<>> fu_vec;
    for (auto &iter : streams_) {
        auto fu = iter.second->SessionClose();
        fu_vec.emplace_back(std::move(fu));
    }
    streams_.clear();
    std::queue<StreamPtr> tmp = std::move(accept_q_);
    co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    co_return;
}

}  // namespace net
}  // namespace snail
