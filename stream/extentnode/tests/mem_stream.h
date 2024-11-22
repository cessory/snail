#include "net/session.h"
#include "util/logger.h"

namespace snail {
namespace stream {

class MemStream : public net::Stream {
    std::queue<seastar::temporary_buffer<char>> recv_q_;
    seastar::condition_variable read_cv_;
    std::queue<seastar::temporary_buffer<char>> *send_q_ = nullptr;
    seastar::condition_variable *send_cv_ = nullptr;
    bool stop_ = false;
    unsigned shard_;

    seastar::future<Status<Buffer>> read_frame(int timeout = -1) {
        Status<seastar::temporary_buffer<char>> s;
        seastar::temporary_buffer<char> b;
        while (!stop_) {
            if (!recv_q_.empty()) {
                b = std::move(recv_q_.front());
                recv_q_.pop();
                if (b.empty()) {
                    s.Set(ErrCode::ErrEOF);
                } else {
                    s.SetValue(std::move(b));
                }
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

   public:
    MemStream() : shard_(seastar::this_shard_id()) {}
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
    uint64_t SessID() const { return 1; }
    bool Valid() const { return true; }
    uint32_t MaxFrameSize() const { return 131072; }

    seastar::future<Status<Buffer>> ReadFrame(int timeout = -1) {
        if (shard_ == seastar::this_shard_id()) {
            auto s = co_await read_frame(timeout);
            co_return s;
        }

        Status<Buffer> s;
        Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>> st;
        st = co_await seastar::smp::submit_to(
            shard_,
            seastar::coroutine::lambda(
                [this, timeout]()
                    -> seastar::future<
                        Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>>> {
                    Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>> s;
                    auto st = co_await read_frame(timeout);
                    if (!st) {
                        s.Set(st.Code(), st.Reason());
                        co_return s;
                    }
                    std::unique_ptr<Buffer> b =
                        std::make_unique<Buffer>(std::move(st.Value()));
                    s.SetValue(seastar::make_foreign<std::unique_ptr<Buffer>>(
                        std::move(b)));
                    co_return s;
                }));
        if (!st) {
            s.Set(st.Code(), st.Reason());
            co_return s;
        }
        s.SetValue(foreign_buffer_copy(std::move(st.Value())));
        co_return s;
    }

    seastar::future<Status<>> WriteFrame(const char *b, size_t n) {
        Status<> s;
        std::vector<iovec> iov;
        iovec o;
        o.iov_base = (void *)b;
        o.iov_len = n;
        iov.push_back(o);
        s = co_await WriteFrame(std::move(iov));
        co_return s;
    }

    seastar::future<Status<>> WriteFrame(std::vector<iovec> iov) {
        Status<> s;
        s = co_await seastar::smp::submit_to(
            shard_, seastar::coroutine::lambda(
                        [this, iov]() -> seastar::future<Status<>> {
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
                            try {
                                seastar::temporary_buffer<char> buffer =
                                    seastar::temporary_buffer<char>::aligned(
                                        kMemoryAlignment,
                                        std::max(kMemoryAlignment, n));
                                buffer.trim(n);
                                char *p = buffer.get_write();
                                for (int i = 0; i < iov.size(); ++i) {
                                    memcpy(p, iov[i].iov_base, iov[i].iov_len);
                                    p += iov[i].iov_len;
                                }
                                send_q_->push(std::move(buffer));
                                send_cv_->signal();
                            } catch (std::system_error &e) {
                                s.Set(e.code().value(), e.code().message());
                            } catch (std::exception &e) {
                                s.Set(ErrCode::ErrInternal, e.what());
                            }
                            co_return s;
                        }));
        co_return s;
    }

    seastar::future<Status<>> WriteFrame(seastar::temporary_buffer<char> b) {
        Status<> s;
        std::vector<iovec> iov;
        iovec o;
        o.iov_base = (void *)b.get_write();
        o.iov_len = b.size();
        iov.push_back(o);
        s = co_await WriteFrame(std::move(iov));
        co_return s;
    }
    seastar::future<Status<>> WriteFrame(
        std::vector<seastar::temporary_buffer<char>> buffers) {
        Status<> s;
        std::vector<iovec> iov;
        for (int i = 0; i < buffers.size(); ++i) {
            iov.push_back({buffers[i].get_write(), buffers[i].size()});
        }
        s = co_await WriteFrame(std::move(iov));
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
