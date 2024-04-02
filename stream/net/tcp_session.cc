#include "tcp_session.h"

#include <limits.h>

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include "byteorder.h"

namespace snail {
namespace net {

static const size_t default_accept_backlog = 1024;

static constexpr uint32_t kMinStreamBufferSize = 1 << 20;
static constexpr uint32_t kMinReceiveBufferSize = 4 << 20;

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

Session::Session(const Option &opt, TcpConnectionPtr conn, bool client,
                 std::unique_ptr<BufferAllocator> allocator)
    : opt_(opt),
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
      die_(false),
      accept_sem_(default_accept_backlog),
      tokens_(max_receive_buffer_) {
    if (opt.max_frame_size > kMaxFrameSize) {
        throw std::runtime_error("invalid max frame size");
    }
}

SessionPtr Session::make_session(const Option &opt, TcpConnectionPtr conn,
                                 bool client,
                                 std::unique_ptr<BufferAllocator> allocator) {
    SessionPtr sess_ptr = seastar::make_lw_shared<Session>(
        opt, conn, client, std::move(allocator));
    sess_ptr->recv_fu_ = sess_ptr->RecvLoop();
    sess_ptr->send_fu_ = sess_ptr->SendLoop();
    sess_ptr->StartKeepalive();
    return sess_ptr;
}

seastar::future<> Session::RecvLoop() {
    auto ptr = shared_from_this();
    auto defer = seastar::defer([this] {
        conn_->Close();
        CloseAllStreams();
        accept_cv_.signal();
        w_cv_.signal();
    });

    char hdr[STREAM_HEADER_SIZE];
    while (!die_) {
        if (tokens_ < 0) {
            co_await token_cv_.wait();
            if (die_ || !status_.OK()) {
                break;
            }
        }
        auto s = co_await conn_->ReadExactly(hdr, STREAM_HEADER_SIZE);
        if (!s.OK()) {
            SetStatus(s.Code(), s.Reason());
            break;
        }

        if (s.Value() == 0) {
            SetStatus(static_cast<ErrCode>(ECONNABORTED));
            break;
        }
        Frame f;
        uint32_t len = f.Unmarshal(hdr);
        if (f.ver != 2) {
            SetStatus(static_cast<ErrCode>(ECONNABORTED));
            break;
        }
        switch (f.cmd) {
            case CmdType::NOP:
                break;
            case CmdType::SYN: {
                auto it = streams_.find(f.sid);
                if (it == streams_.end()) {
                    StreamPtr stream = Stream::make_stream(
                        f.sid, f.ver, opt_.max_frame_size, shared_from_this());
                    streams_[f.sid] = stream;
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
                    auto s = co_await conn_->ReadExactly(buf.get_write(),
                                                         buf.size());
                    if (!s.OK()) {
                        SetStatus(s.Code(), s.Reason());
                        co_return;
                    }
                    if (s.Value() == 0) {
                        SetStatus(static_cast<ErrCode>(ECONNABORTED));
                        co_return;
                    }
                    auto it = streams_.find(f.sid);
                    if (it != streams_.end()) {
                        auto stream = it->second;
                        stream->PushData(std::move(buf));
                        tokens_ -= len;
                    }
                }
                break;
            case CmdType::UPD:
                if (len != 8) {
                    SetStatus(static_cast<ErrCode>(EINVAL));
                    co_return;
                } else {
                    char buf[8];
                    auto s = co_await conn_->ReadExactly(buf, 8);
                    if (!s.OK()) {
                        SetStatus(s.Code(), s.Reason());
                        co_return;
                    }
                    if (s.Value() == 0) {
                        SetStatus(static_cast<ErrCode>(ECONNABORTED));
                        co_return;
                    }
                    auto it = streams_.find(f.sid);
                    if (it != streams_.end()) {
                        auto stream = it->second;
                        uint32_t consumed = LittleEndian::Uint32(buf);
                        uint32_t window = LittleEndian::Uint32(&buf[4]);
                        stream->Update(consumed, window);
                    }
                }
                break;
            default:
                SetStatus(static_cast<ErrCode>(EINVAL));
                co_return;
        }
    }
    co_return;
}

seastar::future<> Session::SendLoop() {
    static uint32_t max_packet_num = IOV_MAX;
    auto ptr = shared_from_this();
    seastar::timer<seastar::steady_clock_type> write_timer;
    write_timer.set_callback([this] { conn_->Close(); });

    auto defer = seastar::defer([this] {
        conn_->Close();
        CloseAllStreams();
        accept_cv_.signal();
        accept_sem_.broken();
        token_cv_.signal();
        SetStatus(static_cast<ErrCode>(EPIPE));
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
    });

    while (!die_ && status_.OK()) {
        Status<> s;
        std::queue<Session::write_request *> sent_q;
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

            if (!s.OK()) {
                SetStatus(s);
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
    if (opt_.keep_alive_enable && client_) {
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

    std::unique_ptr<write_request> req(new write_request());

    req->classid = classid;
    req->frame = std::move(f);
    req->pr = seastar::promise<Status<>>();

    write_q_[static_cast<int>(classid)].emplace(req.get());
    w_cv_.signal();
    s = co_await req->pr.value().get_future();
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
    req->frame.ver = 2;
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

    auto stream =
        Stream::make_stream(id, 2, opt_.max_frame_size, shared_from_this());
    Frame frame;
    frame.ver = 2;
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

seastar::future<> Session::Close() {
    if (!die_) {
        die_ = true;
        w_cv_.signal();
        accept_sem_.broken();
        token_cv_.signal();
        accept_cv_.signal();
        keepalive_timer_.cancel();
        conn_->Close();
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
