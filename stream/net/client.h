#pragma once

#include <list>
#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_mutex.hh>

#include "session.h"
namespace snail {
namespace net {

class Client {
    std::string host_;
    uint16_t port_;
    Option opt_;
    uint32_t connect_timeout_;  // ms
    net::BufferAllocator* allocator_;
    seastar::shared_mutex mu_;
    struct SessionImpl {
        SessionPtr sess_;
        std::queue<StreamPtr> streams_;
    };
    using SessionImplPtr = seastar::lw_shared_ptr<SessionImpl>;

    SessionImplPtr sess_impl_;
    std::queue<SessionImplPtr> recyle_q_;
    seastar::condition_variable recyle_cv_;
    seastar::gate gate_;

    seastar::future<> RecyleLoop();

   public:
    explicit Client(const std::string& host, uint16_t port,
                    const Option opt = Option(), uint32_t connect_timeout = 100,
                    net::BufferAllocator* allocator = nullptr);

    seastar::future<Status<StreamPtr>> Get();

    seastar::future<> Put(StreamPtr s);

    seastar::future<> Close();
};

}  // namespace net
}  // namespace snail
