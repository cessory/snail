#include "stream.h"

#include <seastar/core/coroutine.hh>

#include "session.h"

namespace snail {
namespace net {

Stream::Stream(uint32_t id, uint16_t frame_size, SessionPtr sess)
    : id_(id), frame_size_(frame_size), sess_(sess) {
    buffer_size_ = 0;
    has_fin_ = false;
    die_ = false;
}

StreamPtr Stream::make_stream(uint32_t id, uint16_t frame_size,
                              SessionPtr sess) {
    StreamPtr stream = seastar::make_lw_shared<Stream>(id, frame_size, sess);
    return stream;
}

seastar::future<Status<seastar::temporary_buffer<char>>> Stream::ReadFrame(
    int timeout) {
    Status<seastar::temporary_buffer<char>> s;
    for (;;) {
        if (!buffers_.empty()) {
            auto &b = buffers_.front();
            buffer_size_ -= b.size();
            sess_->ReturnTokens(b.size());
            s.SetValue(std::move(b));
            buffers_.pop();
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

seastar::future<Status<>> Stream::WaitRead(int timeout) {
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

void Stream::PushData(seastar::temporary_buffer<char> data) {
    buffer_size_ += data.size();
    buffers_.emplace(std::move(data));
    NotifyReadEvent();
}

void Stream::Fin() {
    has_fin_ = true;
    NotifyReadEvent();
}

void Stream::NotifyReadEvent() { r_cv_.signal(); }

seastar::future<Status<>> Stream::WriteFrame(char *b, int n) {
    Status<> s;

    if (n > frame_size_) {
        s.Set(EMSGSIZE);
        co_return s;
    }

    if (die_) {
        s.Set(EPIPE);
        co_return s;
    }

    Frame frame;
    frame.ver = sess_->opt_.version;
    frame.cmd = CmdType::PSH;
    frame.sid = id_;
    frame.data = seastar::temporary_buffer<char>(b, n, seastar::deleter());
    s = co_await sess_->WriteFrameInternal(std::move(frame),
                                           Session::ClassID::DATA);
    co_return s;
}

void Stream::SessionClose() {
    if (!die_) {
        die_ = true;
        r_cv_.signal();
    }
}

seastar::future<> Stream::Close() {
    if (!die_) {
        die_ = true;
        r_cv_.signal();
        Frame frame;
        frame.ver = sess_->opt_.version;
        frame.cmd = CmdType::FIN;
        frame.sid = id_;
        co_await sess_->WriteFrameInternal(std::move(frame),
                                           Session::ClassID::CTRL);
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
