#pragma once
#include <sys/uio.h>

#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timer.hh>

#include "util/status.h"

namespace snail {
namespace net {

class Stream;
class Session;

using SessionPtr = seastar::lw_shared_ptr<Session>;
using StreamPtr = seastar::lw_shared_ptr<Stream>;

class Stream {
    uint32_t id_;
    uint8_t ver_;
    uint32_t frame_size_;
    SessionPtr sess_;
    std::queue<seastar::temporary_buffer<char>> buffers_;
    size_t buffer_size_ = 0;
    seastar::condition_variable r_cv_;
    bool has_fin_ = false;
    bool die_ = false;

    uint32_t recv_bytes_ = 0;
    uint32_t sent_bytes_ = 0;
    uint32_t incr_ = 0;
    uint32_t remote_consumed_ = 0;
    uint32_t remote_wnd_;
    seastar::condition_variable wnd_cv_;

    friend class Session;

   private:
    seastar::future<> WaitSess();

    seastar::future<Status<>> WaitRead(int timeout);

    void PushData(seastar::temporary_buffer<char> data);

    void Fin();

    seastar::future<Status<>> SendWindowUpdate(uint32_t consumed);

    void SessionClose();

    void Update(uint32_t consumed, uint32_t window);

   public:
    explicit Stream(uint32_t id, uint8_t ver, uint32_t frame_size,
                    SessionPtr sess);

    ~Stream() { sess_.release(); }

    static StreamPtr make_stream(uint32_t id, uint8_t ver, uint32_t frame_size,
                                 SessionPtr sess);

    uint32_t ID() const { return id_; }

    seastar::future<Status<seastar::temporary_buffer<char>>> ReadFrame(
        int timeout = -1);

    seastar::future<Status<>> WriteFrame(const char *b, size_t n);

    seastar::future<Status<>> WriteFrame(std::vector<iovec> iov);

    seastar::future<> Close();
};

}  // namespace net
}  // namespace snail
