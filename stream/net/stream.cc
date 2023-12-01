#include "stream.h"

namespace snail {
namespace net {
Stream::Stream(uint32_t id, uint32_t frame_size, SessionPtr sess)
    : id_(id), frame_size_(frame_size), sess_(sess) {
    has_fin_ = false;
    die_ = false;
}

StreamPtr Stream::make_stream(uint32_t id, uint32_t frame_size,
                              SessionPtr sess) {
    StreamPtr stream = seastar::make_lw_shared<Stream>(id, frame_size, sess);
    stream->WaitSess();
    return stream;
}

seastar::future<> Stream::WaitSess() {
    auto ptr = shared_from_this();
    co_await sess->Wait();
    r_cv_.signal();
    co_return;
}

seastar::future<Status<seastar::temporary_buffer<char>>> Stream::ReadFrame() {
    Status<seastar::temporary_buffer<char>> s(ErrCode::OK);
    for (;;) {
        if (!buffers_.empty()) {
            auto b = buffers_.front();
            buffers_.pop();
            sess_->ReturnTokens(b.size());
            s.SetValue(b);
            break;
        }
        auto st = co_await WaitRead(timeout);
        if (st.OK()) {
            s.Set(s.Code());
            break;
        }
    }
    co_return s;
}

seastar::future<Status<>> Stream::WaitRead(std::chrono::milliseconds timeout) {
    Status<> s(ErrCode::OK);
    if (die_) {
        s.Set(ErrCode::ErrEOF);
        co_return s;
    }
    if (has_fin_) {
        if (buffers_.empty()) {
            s.Set(ErrCode::ErrEOF);
        }
        co_return s;
    }
    if (sess_->StatusCode() != ErrCode::OK) {
        s.Set(sess_->StatusCode());
        co_return s;
    }
    try {
        co_await r_cv_.wait(timeout);
    } catch (seastar::condition_variable_timed_out &e) {
        s.Set(ErrCode::ErrTimeout);
        co_return s;
    }
    if (has_fin_) {
        if (buffers_.empty()) {
            s.Set(ErrCode::ErrEOF);
        }
        co_return s;
    }
    if (sess_->StatusCode() != ErrCode::OK) {
        s.Set(sess_->StatusCode());
    }
    co_return s;
}

void Stream::PushData(seastar::temporary_buffer<char> data) {
    buffers_.push(std::move(data));
    NotifyReadEvent();
}

void Stream::Fin() {
    has_fin_ = true;
    NotifyReadEvent();
}

void Stream::NotifyReadEvent() { r_cv_.signal(); }

seastar::future<Status<>> Stream::WriteFrame(char *b, int n) {
    Status<> s(ErrCode::OK);

    if (n > frame_size_) {
        s.Set(ErrCode::ErrTooLarge);
        co_return s;
    }

    if (die_) {
        s.Set(ErrCode::ErrClosedPipe);
        co_return s;
    }

    Frame frame;
    frame.ver = sess_->GetVersion();
    frame.cmd = CmdType::PSH;
    frame.sid = id_;
    frame.data = seastar::temporary_buffer<char>(b, n, seastar::deleter());
    s = co_await sess_->WriteFrameInternal(frame, ClassID::DATA);
    co_return s;
}

seastar::future<> Stream::Close() {
    if (!die_) {
        die_ = true;
        r_cv_.signal();
        Frame frame;
        frame.ver = sess_->GetVersion();
        frame.cmd = CmdType::PSH;
        frame.sid = id_;
        co_await sess_->WriteFrameInternal(frame, ClassID::CTRL);
        sess_->StreamClosed(id_);
    }
    co_return;
}

}  // namespace net
}  // namespace snail
