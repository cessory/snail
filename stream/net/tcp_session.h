#pragma once
#include <array>
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <unordered_map>

#include "frame.h"
#include "tcp_connection.h"
#include "tcp_stream.h"
#include "util/status.h"

namespace snail {
namespace net {

struct Option {
    bool keep_alive_enable = true;
    int keep_alive_interval = 10;      // unit: s
    int write_timeout_s = 10;          // unit: s
    uint32_t max_frame_size = 131072;  // 128K
};

class BufferAllocator {
   public:
    virtual ~BufferAllocator() = default;
    virtual seastar::temporary_buffer<char> Allocate(size_t len) = 0;
};

class Session : public seastar::enable_lw_shared_from_this<Session> {
    Option opt_;
    uint32_t max_receive_buffer_;
    uint32_t max_stream_buffer_;
    TcpConnectionPtr conn_;
    bool client_;
    std::unique_ptr<BufferAllocator> allocator_;
    uint32_t next_id_;

    std::unordered_map<uint32_t, StreamPtr> streams_;
    bool die_;
    std::optional<seastar::future<>> recv_fu_;
    std::optional<seastar::future<>> send_fu_;

    Status<> status_;

    std::queue<StreamPtr> accept_q_;
    seastar::semaphore accept_sem_;
    ssize_t tokens_;
    seastar::condition_variable accept_cv_;
    seastar::condition_variable token_cv_;

    enum ClassID {
        CTRL = 0,
        DATA = 1,
    };

    struct write_request {
        ClassID classid;
        Frame frame;
        std::optional<seastar::promise<Status<>>> pr;
    };

    std::array<std::queue<write_request *>, 2> write_q_;
    seastar::condition_variable w_cv_;
    seastar::timer<seastar::steady_clock_type> keepalive_timer_;

   private:
    seastar::future<> RecvLoop();
    seastar::future<> SendLoop();

    void SetStatus(const Status<> &s) {
        if (status_.OK()) {
            status_ = s;
        }
    }

    void SetStatus(ErrCode code) {
        if (status_.OK()) {
            status_.Set(code);
        }
    }

    void SetStatus(ErrCode code, const char *reason) {
        if (status_.OK()) {
            status_.Set(code, reason);
        }
    }
    void StartKeepalive();

    void ReturnTokens(uint32_t n);

    seastar::future<Status<>> WriteFrameInternal(Frame f, ClassID classid);

    void WritePing();

    void CloseAllStreams();

    friend class Stream;

   public:
    explicit Session(const Option &opt, TcpConnectionPtr conn, bool client,
                     std::unique_ptr<BufferAllocator> allocator = nullptr);

    static SessionPtr make_session(
        const Option &opt, TcpConnectionPtr conn, bool client,
        std::unique_ptr<BufferAllocator> allocator = nullptr);

    ~Session() { std::cout << "session deconstructer" << std::endl; }

    seastar::future<Status<StreamPtr>> OpenStream();

    seastar::future<Status<StreamPtr>> AcceptStream();

    size_t Streams() const { return streams_.size(); }

    std::string LocalAddress() { return conn_->LocalAddress(); }

    std::string RemoteAddress() { return conn_->RemoteAddress(); }

    seastar::future<> Close();
};

}  // namespace net
}  // namespace snail
