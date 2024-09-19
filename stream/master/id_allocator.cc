#include "id_allocator.h"

#include <seastar/core/lowres_clock.hh>

#include "net/byteorder.h"
#include "util/logger.h"

namespace snail {
namespace stream {

static const uint64_t kDefaultStep = 100;

IdAllocator::IdAllocator(Storage* store, IDGenerator* id_gen, ApplyType type,
                         Buffer key)
    : shard_(seastar::this_shard_id()),
      store_(store),
      id_gen_(id_gen),
      type_(type),
      key_(std::move(key)) {}

IdAllocator::~IdAllocator() {}

seastar::future<Status<IdAllocatorPtr>> IdAllocator::CreateDiskIdAllocator(
    Storage* store, IDGenerator* id_gen) {
    Status<IdAllocatorPtr> s;
    Buffer key("0diskid", 7);
    char* p = key.get_write();
    *p = static_cast<char>(ApplyType::DiskID);

    auto ptr = seastar::make_shared<IdAllocator>(
        store, id_gen, ApplyType::DiskID, std::move(key));
    auto st = co_await ptr->Init();
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    s.SetValue(ptr);
    co_return s;
}

seastar::future<Status<IdAllocatorPtr>> IdAllocator::CreateNodeIdAllocator(
    Storage* store, IDGenerator* id_gen) {
    Status<IdAllocatorPtr> s;
    Buffer key("0nodeid", 7);
    char* p = key.get_write();
    *p = static_cast<char>(ApplyType::NodeID);

    auto ptr = seastar::make_shared<IdAllocator>(
        store, id_gen, ApplyType::NodeID, std::move(key));
    auto st = co_await ptr->Init();
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    s.SetValue(ptr);
    co_return s;
}

seastar::future<Status<>> IdAllocator::Init() {
    Status<> s;
    auto st = co_await store_->Get(key_.share());
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    Buffer b = std::move(st.Value());
    if (b.empty()) {
        co_return s;
    }
    next_id_ = max_id_ = net::BigEndian::Uint64(b.get());
    co_return s;
}

seastar::future<Status<>> IdAllocator::Apply(Buffer reqid, uint64_t id,
                                             Buffer ctx, Buffer data) {
    auto foreign_reqid =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(reqid)));
    auto foreign_data =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(data)));
    return seastar::smp::submit_to(
        shard_,
        seastar::coroutine::lambda(
            [this, foreign_reqid = std::move(foreign_reqid), id,
             foreign_data = std::move(
                 foreign_data)]() mutable -> seastar::future<Status<>> {
                Status<> s;
                Status<uint64_t> st;

                Buffer local_reqid =
                    foreign_buffer_copy(std::move(foreign_reqid));
                Buffer local_data =
                    foreign_buffer_copy(std::move(foreign_data));

                uint64_t count = net::BigEndian::Uint64(local_data.get());
                uint64_t val = next_id_;
                auto iter = pendings_.find(id);
                if (max_id_ - next_id_ >= count) {
                    next_id_ += count;
                    if (iter != pendings_.end()) {
                        st.SetValue(val);
                        iter->second->pr.set_value(st);
                        pendings_.erase(iter);
                    }
                    co_return s;
                }

                uint64_t remain = count + next_id_ - max_id_;
                uint64_t max_id = next_id_ + std::max(remain, kDefaultStep);
                char value[sizeof(max_id)];
                net::BigEndian::PutUint64(value, max_id);
                s = co_await store_->Put(key_.share(),
                                         Buffer(value, sizeof(value)));
                if (!s) {
                    LOG_ERROR(
                        "reqid={} put key={}, value={} error: {}",
                        std::string_view(local_reqid.get(), local_reqid.size()),
                        std::string_view(key_.get(), key_.size()), max_id, s);
                    co_return s;
                }
                next_id_ += count;
                max_id_ = max_id;
                co_return s;
            }));
}

seastar::future<Status<>> IdAllocator::Reset() {
    return seastar::smp::submit_to(shard_, [this]() -> Status<> {
        next_id_ = 1;
        max_id_ = 1;
        pendings_.clear();
        return Status<>();
    });
}

seastar::future<Status<>> IdAllocator::Restore(Buffer key, Buffer val) {
    auto foreign_key =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(key)));
    auto foreign_val =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(val)));
    return seastar::smp::submit_to(
        shard_,
        [this, foreign_key = std::move(foreign_key),
         foreign_val = std::move(foreign_val)]() mutable -> Status<> {
            Buffer local_key = foreign_buffer_copy(std::move(foreign_key));
            Buffer local_val = foreign_buffer_copy(std::move(foreign_val));
            if (local_key != key_) {
                LOG_FATAL("illegal key={}, expect key={}",
                          std::string_view(local_key.get(), local_key.size()),
                          std::string_view(key_.get(), key_.size()));
            }
            max_id_ = next_id_ = net::BigEndian::Uint64(local_val.get());
            return Status<>();
        });
}

seastar::future<Status<uint64_t>> IdAllocator::Alloc(
    Buffer reqid, size_t count, std::chrono::milliseconds timeout) {
    auto foreign_reqid =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(reqid)));
    return seastar::smp::submit_to(
        shard_, seastar::coroutine::lambda(
                    [this, foreign_reqid = std::move(foreign_reqid), count,
                     timeout]() mutable -> seastar::future<Status<uint64_t>> {
                        Status<uint64_t> s;
                        char val[sizeof(count)];
                        std::unique_ptr<alloc_item> item(new alloc_item);
                        auto reqid =
                            foreign_buffer_copy(std::move(foreign_reqid));

                        net::BigEndian::PutUint64(val, count);
                        uint64_t id = id_gen_->Next();
                        Buffer body(val, sizeof(val), seastar::deleter());
                        pendings_[id] = item.get();

                        seastar::timer<seastar::lowres_clock> timer(
                            [this, id, item = item.get()]() {
                                Status<uint64_t> s;
                                s.Set(ETIME);
                                item->pr.set_value(s);
                                pendings_.erase(id);
                            });
                        timer.arm(timeout);
                        auto st = co_await store_->Propose(
                            reqid.share(), id, type_, Buffer(), body.share());
                        if (!st) {
                            pendings_.erase(id);
                            s.Set(st.Code(), st.Reason());
                            timer.cancel();
                            co_return s;
                        }
                        s = co_await item->pr.get_future();
                        pendings_.erase(id);
                        timer.cancel();
                        co_return s;
                    }));
}

}  // namespace stream
}  // namespace snail
