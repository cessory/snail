#include "net/session.h"

namespace snail {
namespace stream {

class MemStream : public net::Stream {
    std::queue<seastar::temporary_buffer<char>> recv_q_;
    seastar::condition_variable read_cv_;
    std::queue<seastar::temporary_buffer<char>> *send_q_ = nullptr;
    seastar::condition_variable *send_cv_ = nullptr;
    bool stop_ = false;

   public:
    MemStream() {}
    virtual ~MemStream() {}

    static std::pair<net::StreamPtr, net::StreamPtr> make_stream_pair() {
        seastar::shared_ptr<MemStream> client_stream =
            seastar::make_shared<MemStream>();
        seastar::shared_ptr<MemStream> server_stream =
            seastar::make_shared<MemStream>();
        client_stream->send_q_ = &server_stream->recv_q_;
        client_stream->send_cv_ = &server_stream->read_cv_;
        server_stream->send_q_ = &client_stream->recv_q_;
        server_stream->send_cv_ = &client_stream->read_cv_;
        return std::make_pair(
            seastar::dynamic_pointer_cast<net::Stream, MemStream>(
                client_stream),
            seastar::dynamic_pointer_cast<net::Stream, MemStream>(
                server_stream));
    }
    uint32_t ID() const { return 1; }
    uint32_t MaxFrameSize() const { return 131072; }
    seastar::future<Status<seastar::temporary_buffer<char>>> ReadFrame(
        int timeout = -1) {
        Status<seastar::temporary_buffer<char>> s;
        seastar::temporary_buffer<char> b;
        while (!stop_) {
            if (!recv_q_.empty()) {
                b = std::move(recv_q_.front());
                recv_q_.pop();
                if (b.empty()) {
                    s.Set(ErrCode::ErrEOF);
                }
                s.SetValue(std::move(b));
                break;
            }
            if (timeout < 0) {
                co_await read_cv_.wait();
            } else if (timeout > 0) {
                try {
                    co_await read_cv_.wait(std::chrono::milliseconds(timeout));
                } catch (seastar::condition_variable_timed_out &e) {
                    s.Set(ETIME);
                    break;
                }
            }
        }
        if (stop_) {
            s.Set(EPIPE);
        }
        co_return s;
    }
    seastar::future<Status<>> WriteFrame(const char *b, size_t n) {
        seastar::temporary_buffer<char> buf(b, n);
        send_q_->push(std::move(buf));
        send_cv_->signal();
        co_return Status<>();
    }

    seastar::future<Status<>> WriteFrame(std::vector<iovec> iov) {
        Status<> s;
        if (stop_) {
            s.Set(EPIPE);
            co_return s;
        }
        if (iov.size() > IOV_MAX) {
            s.Set(EMSGSIZE);
            co_return s;
        }
        size_t n = 0;
        for (int i = 0; i < iov.size(); ++i) {
            n += iov[i].iov_len;
        }
        if (n > MaxFrameSize()) {
            s.Set(EMSGSIZE);
            co_return s;
        }
        seastar::temporary_buffer<char> buffer(n);
        char *p = buffer.get_write();
        for (int i = 0; i < iov.size(); ++i) {
            memcpy(p, iov[i].iov_base, iov[i].iov_len);
            p += iov[i].iov_len;
        }
        send_q_->push(std::move(buffer));
        send_cv_->signal();
        co_return s;
    }

    seastar::future<Status<>> WriteFrame(seastar::temporary_buffer<char> b) {
        Status<> s;
        if (stop_) {
            s.Set(EPIPE);
            co_return s;
        }
        if (b.size() > MaxFrameSize()) {
            s.Set(EMSGSIZE);
            co_return s;
        }
        send_q_->push(std::move(b));
        send_cv_->signal();
        co_return s;
    }
    seastar::future<Status<>> WriteFrame(
        std::vector<seastar::temporary_buffer<char>> buffers) {
        Status<> s;
        if (stop_) {
            s.Set(EPIPE);
            co_return s;
        }
        if (buffers.size() > IOV_MAX) {
            s.Set(EMSGSIZE);
            co_return s;
        }
        size_t n = 0;
        for (int i = 0; i < buffers.size(); ++i) {
            n += buffers[i].size();
        }
        if (n > MaxFrameSize()) {
            s.Set(EMSGSIZE);
            co_return s;
        }
        seastar::temporary_buffer<char> buffer(n);
        char *p = buffer.get_write();

        for (int i = 0; i < buffers.size(); ++i) {
            memcpy(p, buffers[i].begin(), buffers[i].size());
            p += buffers[i].size();
        }
        send_q_->push(std::move(buffer));
        send_cv_->signal();
        co_return s;
    }

    std::string LocalAddress() const { return "127.0.0.1"; }

    std::string RemoteAddress() const { return "127.0.0.1"; }

    seastar::future<> Close() {
        stop_ = true;
        seastar::temporary_buffer<char> b;
        send_q_->push(std::move(b));
        send_cv_->signal();
        co_return;
    }
};

}  // namespace stream
}  // namespace snail
