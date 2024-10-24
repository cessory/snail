#include "util/async_thread.h"

#include <seastar/core/posix.hh>

namespace snail {

AsyncThread::WorkQueue::WorkQueue()
    : pending_(), completed_(), start_eventfd_(0) {}

AsyncThread::Poller::Poller(AsyncThread& th) : async_thread_(th) {}

bool AsyncThread::Poller::poll() { return async_thread_.poll(); }

bool AsyncThread::Poller::pure_poll() { return poll(); }

bool AsyncThread::Poller::try_enter_interrupt_mode() {
    async_thread_.enter_interrupt_mode();
    if (poll()) {
        async_thread_.exit_interrupt_mode();
        return false;
    }
    return true;
}

void AsyncThread::Poller::exit_interrupt_mode() {
    async_thread_.exit_interrupt_mode();
}

AsyncThread::AsyncThread(std::string name)
    : r_(seastar::engine()),
      shard_id_(seastar::this_shard_id()),
      worker_queue_(),
      worker_thread_([this, name] { work(name); }),
      poller_(std::make_unique<AsyncThread::Poller>(*this)) {}

AsyncThread::~AsyncThread() {
    stopped_.store(true, std::memory_order_relaxed);
    worker_queue_.start_eventfd_.signal(1);
    worker_thread_.join();
}

void AsyncThread::work(std::string name) {
    pthread_setname_np(pthread_self(), name.c_str());
    sigset_t mask;
    sigfillset(&mask);
    auto r = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
    seastar::throw_pthread_error(r);
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
        if (main_thread_idle_.load(std::memory_order_seq_cst)) {
            r_.wakeup();
        }
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
    return nr > 0 ? true : false;
}

void AsyncThread::enter_interrupt_mode() {
    main_thread_idle_.store(true, std::memory_order_seq_cst);
}

void AsyncThread::exit_interrupt_mode() {
    main_thread_idle_.store(false, std::memory_order_seq_cst);
}

}  // namespace snail
