#include "util/async_thread.h"

namespace snail {

AsyncThread::WorkQueue::WorkQueue()
    : pending_(), completed_(), start_eventfd_(0) {}

AsyncThread::AsyncThread()
    : r_(seastar::engine()),
      shard_id_(seastar::this_shard_id()),
      worker_queue_(),
      worker_thread_([this] { work(); }),
      poller_(seastar::reactor::poller::simple(
          [this]() -> bool { return poll(); })) {}

AsyncThread::~AsyncThread() {
    stopped_.store(true, std::memory_order_relaxed);
    worker_queue_.start_eventfd_.signal(1);
    worker_thread_.join();
}

void AsyncThread::work() {
    std::array<AsyncThread::WorkQueue::WorkItem*,
               AsyncThread::WorkQueue::queue_length>
        buf;
    for (;;) {
        uint64_t count;
        auto r = ::read(worker_queue_.start_eventfd_.get_read_fd(), &count,
                        sizeof(count));
        assert(r == sizeof(count));
        if (stopped_.load(std::memory_order_relaxed)) {
            break;
        }
        auto end = buf.data();
        worker_queue_.pending_.consume_all(
            [&](WorkQueue::WorkItem* wi) { *end++ = wi; });
        for (auto p = buf.data(); p != end; ++p) {
            auto wi = *p;
            wi->Process();
            worker_queue_.completed_.push(wi);
        }
        r_.wakeup();
    }
}

bool AsyncThread::poll() {
    std::array<AsyncThread::WorkQueue::WorkItem*,
               AsyncThread::WorkQueue::queue_length>
        tmp_buf;
    auto end = tmp_buf.data();
    auto nr = worker_queue_.completed_.consume_all(
        [&](WorkQueue::WorkItem* wi) { *end++ = wi; });
    for (auto p = tmp_buf.data(); p != end; ++p) {
        auto wi = *p;
        wi->Complete();
    }
    worker_queue_.queue_has_room_.signal(nr);
    return true;
}

}  // namespace snail
