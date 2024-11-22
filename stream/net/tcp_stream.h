#pragma once
#include <sys/uio.h>

#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timer.hh>

#include "session.h"
#include "util/status.h"

namespace snail {
namespace net {

class TcpStream;
class TcpSession;

using TcpStreamPtr = seastar::shared_ptr<TcpStream>;

class TcpStream : public Stream {
    unsigned shard_;
    uint32_t id_;
    uint8_t ver_;
    uint32_t frame_size_;
    TcpSession* sess_;
    std::queue<seastar::temporary_buffer<char>> buffers_;
    size_t buffer_size_ = 0;
    seastar::condition_variable r_cv_;
    bool has_fin_ = false;
    seastar::gate gate_;

    uint32_t recv_bytes_ = 0;
    uint32_t sent_bytes_ = 0;
    uint32_t incr_ = 0;
    uint32_t remote_consumed_ = 0;
    uint32_t remote_wnd_;
    seastar::condition_variable wnd_cv_;

    friend class TcpSession;

   private:
    seastar::future<> WaitSess();

    seastar::future<Status<>> WaitRead(int timeout);

    void PushData(seastar::temporary_buffer<char> data);

    void Fin();

    seastar::future<Status<>> SendWindowUpdate(uint32_t consumed);

    seastar::future<> SessionClose();

    void Update(uint32_t consumed, uint32_t window);

    seastar::future<Status<Buffer>> read_frame(int timeout);

   public:
    explicit TcpStream(uint32_t id, uint8_t ver, uint32_t frame_size,
                       TcpSession* sess);

    virtual ~TcpStream() { sess_ = nullptr; }

    static StreamPtr make_stream(uint32_t id, uint8_t ver, uint32_t frame_size,
                                 TcpSession* sess);

    uint32_t ID() const { return id_; }

    uint64_t SessID() const;

    bool Valid() const;

    uint32_t MaxFrameSize() const { return frame_size_; }

    seastar::future<Status<Buffer>> ReadFrame(int timeout = -1);

    seastar::future<Status<>> WriteFrame(const char* b, size_t n);

    seastar::future<Status<>> WriteFrame(std::vector<iovec> iov);

    seastar::future<Status<>> WriteFrame(Buffer b);

    seastar::future<Status<>> WriteFrame(std::vector<Buffer> buffers);

    std::string LocalAddress() const;

    std::string RemoteAddress() const;

    seastar::future<> Close();
};

}  // namespace net
}  // namespace snail
