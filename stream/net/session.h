#pragma once
#include <sys/uio.h>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <string>
#include <vector>

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

class Stream {
   public:
    virtual ~Stream() {}
    virtual uint32_t ID() const = 0;
    virtual uint32_t MaxFrameSize() const = 0;
    virtual seastar::future<Status<seastar::temporary_buffer<char>>> ReadFrame(
        int timeout = -1) = 0;
    virtual seastar::future<Status<>> WriteFrame(const char *b, size_t n) = 0;

    virtual seastar::future<Status<>> WriteFrame(std::vector<iovec> iov) = 0;

    virtual seastar::future<Status<>> WriteFrame(
        seastar::temporary_buffer<char> b) = 0;
    virtual seastar::future<Status<>> WriteFrame(
        std::vector<seastar::temporary_buffer<char>> buffers) = 0;

    virtual std::string LocalAddress() const = 0;

    virtual std::string RemoteAddress() const = 0;

    virtual seastar::future<> Close() = 0;
};

using StreamPtr = seastar::shared_ptr<Stream>;

class Session {
   public:
    virtual ~Session() {}
    virtual uint64_t ID() const = 0;
    virtual Status<> GetStatus() const = 0;
    virtual seastar::future<Status<StreamPtr>> OpenStream() = 0;
    virtual seastar::future<Status<StreamPtr>> AcceptStream() = 0;
    virtual size_t Streams() const = 0;
    virtual std::string LocalAddress() const = 0;
    virtual std::string RemoteAddress() const = 0;
    virtual seastar::future<> Close() = 0;
};

using SessionPtr = seastar::shared_ptr<Session>;

}  // namespace net
}  // namespace snail
