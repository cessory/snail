#pragma once
#include <boost/lockfree/spsc_queue.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/std-compat.hh>

#include "util/status.h"

namespace snail {
class AsyncThread {
    class WorkQueue {
        struct WorkItem;
        static constexpr size_t queue_length = 128;
        using lf_queue = boost::lockfree::spsc_queue<
            WorkItem *, boost::lockfree::capacity<queue_length>>;

        struct WorkItem {
            virtual ~WorkItem() {}
            virtual void Process() = 0;
            virtual void Complete() = 0;
        };

        template <typename T>
        struct WorkItemImpl : WorkItem {
            seastar::noncopyable_function<T()> func;
            seastar::promise<T> pr;
            std::optional<T> result;

            WorkItemImpl(seastar::noncopyable_function<T()> f)
                : func(std::move(f)) {}
            virtual void Process() override { this->result = this->func(); }
            virtual void Complete() override {
                this->pr.set_value(std::move(*this->result));
            }
            seastar::future<T> GetFuture() { return this->pr.get_future(); }
        };

        WorkQueue();

        template <typename T>
        seastar::future<T> Submit(
            seastar::noncopyable_function<T()> func) noexcept {
            auto wi = std::make_unique<WorkItemImpl<T>>(std::move(func));

            co_await queue_has_room_.wait();
            pending_.push(wi.get());
            start_eventfd_.signal(1);
            auto result = co_await wi->GetFuture();
            co_return std::move(result);
        }

        lf_queue pending_;
        lf_queue completed_;
        seastar::writeable_eventfd start_eventfd_;
        seastar::semaphore queue_has_room_ = {queue_length};
        friend class AsyncThread;
    };

    seastar::reactor &r_;
    unsigned shard_id_;
    AsyncThread::WorkQueue worker_queue_;
    seastar::posix_thread worker_thread_;
    seastar::reactor::poller poller_;
    std::atomic<bool> stopped_ = {false};

    void work();
    bool poll();

   public:
    AsyncThread();
    ~AsyncThread();

    template <typename T>
    seastar::future<T> Submit(
        seastar::noncopyable_function<T()> func) noexcept {
        if (seastar::this_shard_id() == shard_id_) {
            auto res = co_await worker_queue_.Submit(std::move(func));
            co_return std::move(res);
        }
        auto res = co_await seastar::smp::submit_to(
            shard_id_,
            [this, f = std::move(func)]() mutable -> seastar::future<T> {
                return worker_queue_.Submit(
                    std::forward<seastar::noncopyable_function<T()>>(f));
            });
        co_return std::move(res);
    }
};
}  // namespace snail
