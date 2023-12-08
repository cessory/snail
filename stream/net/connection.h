#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>

#include "util/status.h"

namespace snail {
namespace net {

// note: All methods must be called sequentially. That is, no method may be
// invoked before the previous method's returned future is resolved.
class Connection {
   public:
    virtual ~Connection() {}
    virtual seastar::future<Status<>> Write(seastar::net::packet&& p) = 0;
    virtual seastar::future<Status<>> Flush() = 0;
    virtual seastar::future<Status<seastar::temporary_buffer<char>>> Read() = 0;
    virtual seastar::future<Status<seastar::temporary_buffer<char>>>
    ReadExactly(size_t n) = 0;
    virtual seastar::future<Status<>> Close() = 0;
    virtual std::string LocalAddress() = 0;
    virtual std::string RemoteAddress() = 0;
};

using ConnectionPtr = seastar::shared_ptr<Connection>;

}  // namespace net
}  // namespace snail
