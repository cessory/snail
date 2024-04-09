#include "tcp_stream.h"

#include <seastar/core/coroutine.hh>

#include "byteorder.h"
#include "tcp_session.h"

namespace snail {
namespace net {

static constexpr uint32_t kInitialWnd = 262144;

TcpStream::TcpStream(uint32_t id, uint8_t ver, uint32_t frame_size,
                     TcpSessionPtr sess)
    : id_(id), ver_(ver), frame_size_(frame_size), sess_(sess) {
    remote_wnd_ = std::max(kInitialWnd, frame_size_ * 8);
}

StreamPtr TcpStream::make_stream(uint32_t id, uint8_t ver, uint32_t frame_size,
                                 TcpSessionPtr sess) {
    seastar::shared_ptr<TcpStream> stream =
        seastar::make_shared<TcpStream>(id, ver, frame_size, sess);
    return seastar::dynamic_pointer_cast<Stream, TcpStream>(stream);
}

seastar::future<Status<seastar::temporary_buffer<char>>> TcpStream::ReadFrame(
    int timeout) {
    Status<seastar::temporary_buffer<char>> s;
    for (;;) {
        if (!buffers_.empty()) {
            auto &b = buffers_.front();
            uint32_t n = static_cast<uint32_t>(b.size());
            buffer_size_ -= n;
            sess_->ReturnTokens(n);
            s.SetValue(std::move(b));
            buffers_.pop();
            recv_bytes_ += n;
            incr_ += n;
            if (incr_ >= sess_->max_stream_buffer_ / 2 || recv_bytes_ == n) {
                incr_ = 0;
                auto st = co_await SendWindowUpdate(recv_bytes_);
                if (!st.OK()) {
                    s.Set(st.Code(), st.Reason());
                    co_return s;
                }
            }
            break;
        }
        auto st = co_await WaitRead(timeout);
        if (!st.OK()) {
            s.Set(st.Code(), st.Reason());
            break;
        }
    }
    co_return s;
}

seastar::future<Status<>> TcpStream::WaitRead(int timeout) {
    Status<> s;
    if (die_) {
        s.Set(EPIPE);
        co_return s;
    }
    if (has_fin_) {
        if (buffers_.empty()) {
            s.Set(ErrCode::ErrEOF);
        }
        co_return s;
    }
    if (!sess_->status_.OK()) {
        s = sess_->status_;
        co_return s;
    }
    if (timeout < 0) {
        co_await r_cv_.wait();
    } else if (timeout > 0) {
        try {
            co_await r_cv_.wait(std::chrono::milliseconds(timeout));
        } catch (seastar::condition_variable_timed_out &e) {
            s.Set(ETIME);
            co_return s;
        }
    }
    if (die_) {
        s.Set(EPIPE);
        co_return s;
    }
    if (has_fin_) {
        if (buffers_.empty()) {
            s.Set(ErrCode::ErrEOF);
        }
        co_return s;
    }
    if (!sess_->status_.OK()) {
        s = sess_->status_;
    }
    co_return s;
}

void TcpStream::PushData(seastar::temporary_buffer<char> data) {
    buffer_size_ += data.size();
    buffers_.emplace(std::move(data));
    r_cv_.signal();
}

void TcpStream::Fin() {
    has_fin_ = true;
    r_cv_.signal();
    wnd_cv_.broadcast();
}

seastar::future<Status<>> TcpStream::SendWindowUpdate(uint32_t consumed) {
    Frame f;
    f.ver = ver_;
    f.cmd = CmdType::UPD;
    f.sid = id_;
    auto data = seastar::temporary_buffer<char>(8);
    LittleEndian::PutUint32(data.get_write(), consumed);
    LittleEndian::PutUint32(data.get_write() + 4, sess_->max_stream_buffer_);
    f.packet = std::move(seastar::net::packet(std::move(data)));
    return sess_->WriteFrameInternal(std::move(f), TcpSession::ClassID::DATA);
}

seastar::future<Status<>> TcpStream::WriteFrame(std::vector<iovec> iov) {
    Status<> s;

    if (die_ || has_fin_) {
        s.Set(EPIPE);
        co_return s;
    }

    if (iov.size() >= IOV_MAX) {
        s.Set(EMSGSIZE);
        co_return s;
    }

    seastar::net::packet packet;
    for (int i = 0; i < iov.size(); ++i) {
        seastar::net::packet p = seastar::net::packet::from_static_data(
            reinterpret_cast<const char *>(iov[i].iov_base), iov[i].iov_len);
        packet.append(std::move(p));
    }
    if (packet.len() > frame_size_) {
        s.Set(EMSGSIZE);
        co_return s;
    } else if (packet.len() == 0) {
        co_return s;
    }

    int32_t inflight = static_cast<int32_t>(sent_bytes_ - remote_consumed_);
    int32_t win = static_cast<int32_t>(remote_wnd_) - inflight;
    while (inflight < 0 || win <= 0) {
        co_await wnd_cv_.wait();
        if (die_ || has_fin_) {
            s.Set(EPIPE);
            co_return s;
        }
        inflight = static_cast<int32_t>(sent_bytes_ - remote_consumed_);
        win = static_cast<int32_t>(remote_wnd_) - inflight;
    }
    sent_bytes_ += packet.len();
    Frame frame;
    frame.ver = ver_;
    frame.cmd = CmdType::PSH;
    frame.sid = id_;
    frame.packet = std::move(packet);
    s = co_await sess_->WriteFrameInternal(std::move(frame),
                                           TcpSession::ClassID::DATA);
    co_return s;
}

seastar::future<Status<>> TcpStream::WriteFrame(const char *b, size_t n) {
    iovec iov = {(void *)b, n};
    return WriteFrame({iov});
}

seastar::future<Status<>> TcpStream::WriteFrame(
    seastar::temporary_buffer<char> b) {
    iovec iov = {(void *)b.get_write(), b.size()};
    auto s = co_await WriteFrame({iov});
    co_return s;
}

seastar::future<Status<>> TcpStream::WriteFrame(
    std::vector<seastar::temporary_buffer<char>> buffers) {
    std::vector<iovec> iov;
    int n = buffers.size();
    for (int i = 0; i < n; i++) {
        iov.push_back({(void *)buffers[i].get_write(), buffers[i].size()});
    }
    auto s = co_await WriteFrame(std::move(iov));
    co_return s;
}

void TcpStream::Update(uint32_t consumed, uint32_t window) {
    remote_consumed_ = consumed;
    remote_wnd_ = window;
    wnd_cv_.broadcast();
}

void TcpStream::SessionClose() {
    if (!die_) {
        die_ = true;
        r_cv_.signal();
        wnd_cv_.broadcast();
    }
}

std::string TcpStream::LocalAddress() const { return sess_->LocalAddress(); }

std::string TcpStream::RemoteAddress() const { return sess_->RemoteAddress(); }

seastar::future<> TcpStream::Close() {
    if (!die_) {
        die_ = true;
        r_cv_.signal();
        wnd_cv_.broadcast();
        Frame frame;
        frame.ver = ver_;
        frame.cmd = CmdType::FIN;
        frame.sid = id_;
        co_await sess_->WriteFrameInternal(std::move(frame),
                                           TcpSession::ClassID::CTRL);
        if (buffer_size_ > 0) {
            sess_->ReturnTokens(buffer_size_);
            buffer_size_ = 0;
        }
        sess_->streams_.erase(id_);
    }
    co_return;
}

}  // namespace net
}  // namespace snail
