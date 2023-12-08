#pragma once
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
    uint32_t frame_size_;
    SessionPtr sess_;
    std::queue<seastar::temporary_buffer<char>> buffers_;
    size_t buffer_size_;
    seastar::condition_variable r_cv_;
    bool has_fin_;
    bool die_;

    friend class Session;

   private:
    seastar::future<> WaitSess();

    seastar::future<Status<>> WaitRead(int timeout);

    void PushData(seastar::temporary_buffer<char> data);

    void Fin();

    void NotifyReadEvent();

    void SessionClose();

   public:
    explicit Stream(uint32_t id, uint32_t frame_size, SessionPtr sess);

    ~Stream() {
        sess_.release();
        std::cout << "stream id=" << id_ << " deconstructer" << std::endl;
    }

    static StreamPtr make_stream(uint32_t id, uint32_t frame_size,
                                 SessionPtr sess);

    uint32_t ID() const { return id_; }

    seastar::future<Status<seastar::temporary_buffer<char>>> ReadFrame(
        int timeout = -1);

    seastar::future<Status<>> WriteFrame(char *b, int n);

    seastar::future<> Close();
};

}  // namespace net
}  // namespace snail
