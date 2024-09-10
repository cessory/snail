#include "id_allocator.h"

#include <seastar/core/lowres_clock.hh>

#include "net/byteorder.h"
#include "util/logger.h"

namespace snail {
namespace stream {

static const uint64_t kDefaultStep = 100;

IdAllocator::IdAllocator(StoragePtr store, IDGeneratorPtr id_gen,
                         ApplyType type, Buffer key)
    : store_(store), id_gen_(id_gen), type_(type), key_(std::move(key)) {}

IdAllocator::~IdAllocator() {}

seastar::shared_ptr<IdAllocator> IdAllocator::CreateDiskIdAllocator(
    StoragePtr store, IDGeneratorPtr id_gen) {
    Buffer key("0diskid", 7);
    char *p = key.get_write();
    *p = static_cast<char>(ApplyType::DiskID);

    return seastar::make_shared<IdAllocator>(store, id_gen, ApplyType::DiskID,
                                             std::move(key));
}

seastar::shared_ptr<IdAllocator> IdAllocator::CreateNodeIdAllocator(
    StoragePtr store, IDGeneratorPtr id_gen) {
    Buffer key("0nodeid", 7);
    char *p = key.get_write();
    *p = static_cast<char>(ApplyType::NodeID);

    return seastar::make_shared<IdAllocator>(store, id_gen, ApplyType::NodeID,
                                             std::move(key));
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
    Status<> s;
    Status<uint64_t> st;
    uint64_t count = net::BigEndian::Uint64(data.get());
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
    s = co_await store_->Put(key_.share(), Buffer(value, sizeof(value)));
    if (!s) {
        LOG_ERROR("reqid={} put key={}, value={} error: {}",
                  std::string_view(reqid.get(), reqid.size()),
                  std::string_view(key_.get(), key_.size()), max_id, s);
        co_return s;
    }
    next_id_ += count;
    max_id_ = max_id;
    co_return s;
}
void IdAllocator::Reset() {
    next_id_ = 1;
    max_id_ = 1;
    pendings_.clear();
}

void IdAllocator::Restore(Buffer key, Buffer val) {
    if (key != key_) {
        LOG_FATAL("illegal key={}, expect key={}",
                  std::string_view(key.get(), key.size()),
                  std::string_view(key_.get(), key_.size()));
    }
    max_id_ = next_id_ = net::BigEndian::Uint64(val.get());
}

seastar::future<Status<uint64_t>> IdAllocator::Alloc(
    RaftServerPtr raft, Buffer reqid, size_t count,
    std::chrono::milliseconds timeout) {
    Status<uint64_t> s;
    char val[sizeof(count)];
    std::unique_ptr<alloc_item> item(new alloc_item);

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
    auto st = co_await store_->Propose(raft, reqid.share(), id, type_, Buffer(),
                                       body.share());
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
}

}  // namespace stream
}  // namespace snail
