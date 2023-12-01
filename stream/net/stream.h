#pragma once

namespace snail {
namespace net {

class Stream {
    uint32_t id_;
    uint32_t frame_size_;
    SessionPtr sess_;
    std::deque<seastar::temporary_buffer<char>> buffers_;
    seastar::condition_variable r_cv_;
    bool has_fin_;
    bool die_;

    friend class Session;

   private:
    seastar::future<> WaitSess();

    seastar::future<Status<>> WaitRead(std::chrono::milliseconds timeout);

    void PushData(seastar::temporary_buffer<char> data);

    void Fin();

    void NotifyReadEvent();

   public:
    explicit Stream(uint32_t id, uint32_t frame_size, SessionPtr sess);

    static StreamPtr make_stream(uint32_t id, uint32_t frame_size,
                                 SessionPtr sess);

    uint32_t ID() const { return id_; }

    seastar::future<Status<seastar::temporary_buffer<char>>> ReadFrame(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

    seastar::future<Status<>> WriteFrame(char *b, int n);

    seastar::future<> Close();
};

using StreamPtr = seastar::lw_shared_ptr<Stream>;

}  // namespace net
}  // namespace snail
