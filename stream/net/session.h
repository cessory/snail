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

#include "connection.h"
#include "frame.h"
#include "stream.h"
#include "util/status.h"

namespace snail {
namespace net {

struct Option {
    int version = 1;
    bool keep_alive_disabled = false;
    int keep_alive_interval = 10;  // unit: s
    int keep_alive_timeout = 30;
    uint16_t max_frame_size = 65535;
    int max_receive_buffer = 67108864;  // 64M
    int max_stream_buffer = 65536;
};

class Session : public seastar::enable_lw_shared_from_this<Session> {
    Option opt_;
    ConnectionPtr conn_;
    uint32_t next_id_;
    uint32_t sequence_;

    seastar::semaphore bucket_sem_;
    std::unordered_map<uint32_t, StreamPtr> streams_;
    bool die_;
    std::optional<seastar::future<>> recv_fu_;
    std::optional<seastar::future<>> send_fu_;

    Status<> status_;

    std::queue<StreamPtr> accept_q_;
    seastar::semaphore accept_sem_;
    seastar::condition_variable accept_cv_;

    enum ClassID {
        CTRL = 0,
        DATA = 1,
    };

    struct write_request {
        ClassID classid;
        Frame frame;
        uint32_t seq;
        std::optional<seastar::promise<Status<>>> pr;
    };

    std::array<std::queue<write_request *>, 2> write_q_;
    seastar::condition_variable w_cv_;
    seastar::timer<seastar::steady_clock_type> *keepalive_timer_ = nullptr;

   private:
    seastar::future<> RecvLoop();
    seastar::future<> SendLoop();

    void StartKeepalive();

    void ReturnTokens(uint32_t n) { bucket_sem_.signal(n); }

    seastar::future<Status<>> WriteFrameInternal(Frame f, ClassID classid);

    void WritePing();

    void CloseAllStreams();

    friend class Stream;

   public:
    explicit Session(const Option &opt, ConnectionPtr conn, bool client);

    static SessionPtr make_session(const Option &opt, ConnectionPtr conn,
                                   bool client);

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
