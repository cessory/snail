#include "storage.h"

#include <limits.h>

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include "net/byteorder.h"
#include "util/logger.h"

namespace snail {
namespace stream {

struct ApplyEntry {
    ApplyType type;
    uint64_t id;
    Buffer reqid;
    Buffer ctx;
    Buffer body;

    Buffer Marshal() {
        size_t size = 1 + 1 + VarintLength(id);
        if (reqid.size()) {
            size += 1 + VarintLength(reqid.size()) + reqid.size();
        }
        if (ctx.size()) {
            size += 1 + VarintLength(ctx.size()) + ctx.size();
        }
        if (body.size()) {
            size += 1 + VarintLength(body.size()) + body.size();
        }

        Buffer b(size);
        char* p = b.get_write();
        *p++ = static_cast<char>(type);
        p = PutVarint32(p, 1);
        p = PutVarint64(p, id);
        if (reqid.size()) {
            p = PutVarint32(p, 2);
            p = PutVarint32(p, reqid.size());
            memcpy(p, reqid.get(), reqid.size());
            p += reqid.size();
        }
        if (ctx.size()) {
            p = PutVarint32(p, 3);
            p = PutVarint32(p, ctx.size());
            memcpy(p, ctx.get(), ctx.size());
            p += ctx.size();
        }
        if (body.size()) {
            p = PutVarint32(p, 4);
            p = PutVarint32(p, body.size());
            memcpy(p, body.get(), body.size());
            p += body.size();
        }
        return b;
    }
    bool Unmarshal(Buffer& b) {
        const char* p = b.get();
        const char* pend = b.end();
        size_t n = b.size();

        if (b.empty()) {
            return false;
        }
        type = static_cast<ApplyType>(*p++);

        uint32_t old_tag = 0;
        while (old_tag < 4 && p < pend) {
            uint32_t tag = 0;
            // get tag
            p = GetVarint32(p, n, &tag);
            if (p == nullptr) {
                return false;
            }
            n = pend - p;
            if (tag == 0 || tag > 4 || tag <= old_tag) {
                return false;
            }
            old_tag = tag;
            uint32_t len = 0;
            switch (tag) {
                case 1:
                    p = GetVarint64(p, n, &id);
                    if (p == nullptr) {
                        return false;
                    }
                    n = pend - p;
                    break;
                case 2:
                    p = GetVarint32(p, n, &len);
                    if (p == nullptr) {
                        return false;
                    }
                    n = pend - p;
                    if (n < len) {
                        return false;
                    }
                    reqid = b.share(p - b.get(), len);
                    n -= len;
                    p += len;
                    break;
                case 3:
                    p = GetVarint32(p, n, &len);
                    if (p == nullptr) {
                        return false;
                    }
                    n = pend - p;
                    if (n < len) {
                        return false;
                    }
                    ctx = b.share(p - b.get(), len);
                    n -= len;
                    p += len;
                    break;
                case 4:
                    p = GetVarint32(p, n, &len);
                    if (p == nullptr) {
                        return false;
                    }
                    n = pend - p;
                    if (n < len) {
                        return false;
                    }
                    body = b.share(p - b.get(), len);
                    n -= len;
                    p += len;
                    break;
            }
        }
        return true;
    }
};

void WriteBatch::Put(const std::string_view& key,
                     const std::string_view& value) {
    batch_.Put(rocksdb::Slice(key.data(), key.size()),
               rocksdb::Slice(value.data(), value.size()));
}

void WriteBatch::Delete(const std::string_view& key) {
    batch_.Delete(rocksdb::Slice(key.data(), key.size()));
}

void WriteBatch::DeleteRange(const std::string_view& start,
                             const std::string_view& end) {
    batch_.DeleteRange(rocksdb::Slice(start.data(), start.size()),
                       rocksdb::Slice(end.data(), end.size()));
}

class SmSnapshotImpl : public SmSnapshot {
    AsyncThread* worker_thread_ = nullptr;
    rocksdb::DB* db_ = nullptr;
    const rocksdb::Snapshot* snapshot_ = nullptr;
    rocksdb::Iterator* iter_ = nullptr;
    seastar::sstring name_;
    uint64_t index_;
    seastar::gate* external_gate_;
    seastar::gate gate_;
    uint32_t& pending_snapshot_;

    friend class Storage;

   public:
    SmSnapshotImpl(const seastar::sstring& name, uint64_t index,
                   seastar::gate* gate, uint32_t& pending_snapshot)
        : name_(name),
          index_(index),
          external_gate_(gate),
          pending_snapshot_(pending_snapshot) {
        gate->enter();
    }

    virtual ~SmSnapshotImpl() { (void)Close(); }

    const seastar::sstring& Name() override { return name_; }

    uint64_t Index() const override { return index_; }

    seastar::future<Status<Buffer>> Read() override {
        static size_t max_buffer_size = 4 << 20;
        Status<Buffer> s;
        if (!db_ || !snapshot_ || !iter_) {
            s.Set(EINVAL);
            co_return s;
        }
        if (gate_.is_closed()) {
            s.Set(EPIPE);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        Buffer b(max_buffer_size);
        size_t len = 0;
        auto st = co_await worker_thread_->Submit<Status<>>(
            [this, &b, &len]() -> Status<> {
                Status<> s;
                char* p = b.get_write() + len;
                for (; iter_->Valid(); iter_->Next()) {
                    auto key = iter_->key();
                    auto value = iter_->value();
                    uint32_t key_len = VarintLength(key.size());
                    uint32_t val_len = VarintLength(value.size());
                    if (len + key.size() + value.size() + key_len + val_len >
                        max_buffer_size) {
                        if (len == 0) {
                            s.Set(EOVERFLOW);
                            return s;
                        }
                        break;
                    }
                    p = PutVarint32(p, key_len);
                    memcpy(p, key.data(), key.size());
                    p += key.size();
                    p = PutVarint32(p, val_len);
                    memcpy(p, value.data(), value.size());
                    p += value.size();
                    len = p - b.get();
                }
                if (!iter_->status().ok()) {
                    s.Set(ErrCode::ErrUnExpect, iter_->status().ToString());
                }
                return s;
            });
        if (!st) {
            s.Set(st.Code(), st.Reason());
            co_return s;
        }
        if (len > 0) {
            s.SetValue(std::move(b));
        }
        co_return s;
    }

    seastar::future<> Close() override {
        if (gate_.is_closed()) {
            co_return;
        }
        auto fu = gate_.close();

        if (iter_) {
            delete iter_;
            iter_ = nullptr;
        }
        if (pending_snapshot_ > 0) {
            pending_snapshot_--;
        }

        if (snapshot_) {
            co_await worker_thread_->Submit<Status<>>([this]() -> Status<> {
                Status<> s;
                db_->ReleaseSnapshot(snapshot_);
                return s;
            });
            snapshot_ = nullptr;
        }
        db_ = nullptr;
        external_gate_->leave();
        co_return;
    }
};

using SmSnapshotImplPtr = seastar::shared_ptr<SmSnapshotImpl>;

Storage::StatemachineImpl::StatemachineImpl(std::string_view db_path,
                                            uint64_t retain_wal_entries)
    : db_path_(db_path),
      worker_thread_("storage"),
      prev_applied_(0),
      applied_(0),
      retain_wal_entries_(retain_wal_entries),
      snapshot_seq_(time(0)),
      lead_(0),
      pending_snapshot_(0),
      applied_key_(8, 0) {
    applied_key_[0] = static_cast<char>(ApplyType::Raft);
    memcpy(applied_key_.data() + 1, "applied", 7);
}

Storage::Storage(std::string_view db_path, uint64_t retain_wal_entries)
    : shard_(seastar::this_shard_id()) {
    impl_ = seastar::make_shared<StatemachineImpl>(std::move(db_path),
                                                   retain_wal_entries);
}

seastar::future<Status<StoragePtr>> Storage::Create(
    std::string_view db_path, RaftServerOption opt, uint64_t retain_wal_entries,
    std::vector<RaftNode> cfg_nodes) {
    Status<StoragePtr> s;

    std::string path(db_path);
    seastar::shared_ptr<Storage> store_ptr =
        seastar::make_shared<Storage>(std::move(db_path), retain_wal_entries);
    auto st =
        co_await store_ptr->impl_->worker_thread_.Submit<Status<rocksdb::DB*>>(
            [path]() -> Status<rocksdb::DB*> {
                Status<rocksdb::DB*> s;
                rocksdb::DB* db = nullptr;
                rocksdb::Options opt;
                opt.create_if_missing = true;
                opt.max_log_file_size = 1 << 30;
                opt.keep_log_file_num = 10;
                auto st = rocksdb::DB::Open(opt, path, &db);
                if (!st.ok()) {
                    s.Set(ErrCode::ErrUnExpect, st.ToString());
                    return s;
                }
                s.SetValue(db);
                return s;
            });
    if (!st) {
        s.Set(st.Code(), st.Reason());
        LOG_ERROR("open db {} error: {}", path, s);
        co_return s;
    }
    std::unique_ptr<rocksdb::DB> db_ptr(st.Value());
    store_ptr->impl_->db_ = std::move(db_ptr);
    auto st2 = co_await store_ptr->impl_->LoadRaftConfig();
    if (!st2) {
        LOG_ERROR("load data from db error: {}", st2);
        s.Set(st2.Code(), st2.Reason());
        co_return s;
    }
    auto raft_nodes = store_ptr->impl_->GetRaftNodes();
    if (raft_nodes.empty()) {
        raft_nodes = cfg_nodes;
    }
    if (raft_nodes.empty()) {
        s.Set(ErrCode::ErrUnExpect, "raft node is empty");
        co_return s;
    }
    auto st3 = co_await snail::stream::RaftServer::Create(
        opt, store_ptr->impl_->applied_, std::move(raft_nodes),
        seastar::dynamic_pointer_cast<Statemachine, StatemachineImpl>(
            store_ptr->impl_));
    if (!st3) {
        s.Set(st3.Code(), st3.Reason());
        co_return s;
    }
    store_ptr->raft_ = st3.Value();
    store_ptr->impl_->raft_ = store_ptr->raft_;
    s.SetValue(store_ptr);
    co_return s;
}

bool Storage::RegisterApplyHandler(ApplyHandler* handler) {
    auto type = handler->Type();
    if (impl_->apply_handler_vec_[(int)type]) {
        return false;
    }

    impl_->apply_handler_vec_[(int)type] = handler;
    return true;
}

seastar::future<> Storage::Start() {
    co_await seastar::smp::submit_to(
        shard_, seastar::coroutine::lambda([this]() -> seastar::future<> {
            co_await raft_->Start();
            co_return;
        }));
    co_return;
}

seastar::future<> Storage::WaitLeaderChange() {
    return seastar::smp::submit_to(shard_, [this]() -> seastar::future<> {
        return impl_->WaitLeaderChange();
    });
}

seastar::future<> Storage::Reload() {
    for (;;) {
        auto s = co_await ReadIndex();
        if (!s) {
            if (s.Code() == ErrCode(EPIPE) ||
                s.Code() == ErrCode::ErrRaftAbort) {
                break;
            }
            continue;
        }
        break;
    }
    co_return;
}

seastar::future<Status<>> Storage::StatemachineImpl::LoadRaftConfig() {
    Status<> s;

    s = co_await worker_thread_.Submit<Status<>>([this]() -> Status<> {
        Status<> s;
        rocksdb::ReadOptions ro;
        char start_key[1];
        char end_key[1];

        start_key[0] = static_cast<char>(ApplyType::Raft);
        end_key[0] = static_cast<char>((char)ApplyType::Raft + 1);
        rocksdb::Slice lower_bound(start_key, 1);
        rocksdb::Slice upper_bound(end_key, 1);

        ro.fill_cache = false;
        ro.iterate_lower_bound = &lower_bound;
        ro.iterate_upper_bound = &upper_bound;

        auto iter = db_->NewIterator(ro);
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            auto key = iter->key();
            auto value = iter->value();

            if (0 == key.compare(applied_key_)) {
                applied_ = net::BigEndian::Uint64(value.data());
                prev_applied_ = applied_;
                continue;
            }

            RaftNode node;
            if (!node.ParseFromArray(value.data(), value.size())) {
                s.Set(ErrCode::ErrUnExpect, "invalid raft node");
                break;
            }
            if (node.id() == 0) {
                continue;
            }

            raft_nodes_[node.id()] = node;
        }
        auto st = iter->status();
        if (!st.ok()) {
            s.Set(ErrCode::ErrUnExpect, st.ToString());
        }
        delete iter;
        return s;
    });
    if (!s) {
        LOG_ERROR("load raft config from db error: {}", s);
    }
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::SaveApplied(uint64_t index,
                                                                 bool sync) {
    Status<> s;

    Buffer key(applied_key_.data(), applied_key_.size(), seastar::deleter());
    char value[sizeof(index)];
    net::BigEndian::PutUint64(value, index);
    Buffer val(value, sizeof(value), seastar::deleter());
    s = co_await Put(std::move(key), std::move(val), sync);
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::Apply(
    std::vector<Buffer> datas, uint64_t index) {
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    std::vector<seastar::future<Status<>>> fu_vec;
    for (int i = 0; i < datas.size(); i++) {
        if (datas[i].empty()) {
            continue;
        }
        ApplyEntry entry;
        if (!entry.Unmarshal(datas[i])) {
            s.Set(EINVAL);
            co_return s;
        }
        int type = (int)entry.type;

        if (apply_handler_vec_[type]) {
            auto fu = apply_handler_vec_[type]->Apply(
                std::move(entry.reqid), entry.id, std::move(entry.ctx),
                std::move(entry.body));
            fu_vec.emplace_back(std::move(fu));
        }
    }
    auto results =
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    for (int i = 0; i < results.size(); i++) {
        if (!results[i]) {
            s = results[i];
            co_return s;
        }
    }

    s = co_await SaveApplied(index, false);
    if (!s) {
        co_return s;
    }
    applied_ = index;
    if (applied_ - prev_applied_ >= retain_wal_entries_ &&
        pending_snapshot_ == 0) {
        s = co_await Flush();
        if (s && prev_applied_ > 0) {
            if (raft_) {
                raft_->ReleaseWal(prev_applied_);
            }
            prev_applied_ = applied_;
        }
    }
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::ApplyConfChange(
    raft::ConfChangeType type, uint64_t node_id, std::string raft_host,
    uint16_t raft_port, std::string host, uint16_t port, uint64_t index) {
    Status<> s;
    RaftNode raft_node;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    WriteBatch batch;
    char applied_val[sizeof(index)];
    char key[sizeof(node_id) + 1];

    net::BigEndian::PutUint64(applied_val, index);

    batch.Put(std::string_view(applied_key_),
              std::string_view(applied_val, sizeof(index)));
    switch (type) {
        case raft::ConfChangeType::ConfChangeAddNode:
        case raft::ConfChangeType::ConfChangeAddLearnerNode: {
            raft_node.set_id(node_id);
            raft_node.set_raft_host(raft_host);
            raft_node.set_raft_port(raft_port);
            raft_node.set_host(host);
            raft_node.set_port(port);
            raft_node.set_learner(
                type == raft::ConfChangeType::ConfChangeAddLearnerNode);
            Buffer b(raft_node.ByteSizeLong());
            raft_node.SerializeToArray(b.get_write(), b.size());

            key[0] = static_cast<char>(ApplyType::Raft);
            net::BigEndian::PutUint64(&key[1], node_id);
            batch.Put(std::string_view(key, sizeof(key)),
                      std::string_view(b.get(), b.size()));
            raft_nodes_[raft_node.id()] = raft_node;
            break;
        }
        case raft::ConfChangeType::ConfChangeRemoveNode:
            key[0] = static_cast<char>(ApplyType::Raft);
            net::BigEndian::PutUint64(&key[1], node_id);
            batch.Delete(std::string_view(key, sizeof(key)));
            raft_nodes_.erase(node_id);
            break;
        default:
            break;
    }
    s = co_await Write(std::move(batch), true);
    applied_ = index;
    co_return s;
}

seastar::future<Status<SmSnapshotPtr>>
Storage::StatemachineImpl::CreateSnapshot() {
    Status<SmSnapshotPtr> s;

    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    seastar::sstring name = seastar::internal::to_sstring<seastar::sstring>(
        (unsigned long)snapshot_seq_++);
    pending_snapshot_++;
    SmSnapshotImplPtr snapshot = seastar::make_shared<SmSnapshotImpl>(
        name, applied_, &gate_, pending_snapshot_);
    snapshot->db_ = db_.get();
    auto st = co_await worker_thread_.Submit<
        Status<std::tuple<const rocksdb::Snapshot*, rocksdb::Iterator*>>>(
        [this]() -> Status<
                     std::tuple<const rocksdb::Snapshot*, rocksdb::Iterator*>> {
            Status<std::tuple<const rocksdb::Snapshot*, rocksdb::Iterator*>> s;
            auto snap = db_->GetSnapshot();
            rocksdb::ReadOptions ro;
            ro.snapshot = snap;
            ro.fill_cache = false;

            char start_prefix[1];
            start_prefix[0] = static_cast<char>((char)ApplyType::Raft + 1);
            rocksdb::Slice start(start_prefix, 1);
            ro.iterate_lower_bound = &start;
            auto iter = db_->NewIterator(ro);
            iter->SeekToFirst();
            s.SetValue(std::make_tuple(snap, iter));
            return s;
        });
    if (!st) {
        LOG_ERROR("create snapshot error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    snapshot->snapshot_ = std::get<0>(st.Value());
    snapshot->iter_ = std::get<1>(st.Value());
    s.SetValue(snapshot);
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::ApplySnapshot(
    raft::SnapshotPtr meta, SmSnapshotPtr body) {
    SnapshotMetaPayload meta_payload;
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_await body->Close();
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    if (!meta_payload.ParseFromArray(meta->data().get(), meta->data().size())) {
        s.Set(ErrCode::ErrUnExpect, "failed to parse meta");
        co_await body->Close();
        co_return s;
    }

    char start_key[1] = {static_cast<char>(ApplyType::Raft)};
    char end_key[1] = {static_cast<char>(ApplyType::Max)};

    s = co_await DeleteRange(Buffer(start_key, 1, seastar::deleter()),
                             Buffer(end_key, 1, seastar::deleter()));

    if (!s) {
        LOG_ERROR("delete data for snapshot={} error: {}", body->Name(), s);
        co_await body->Close();
        co_return s;
    }
    for (int i = 0; i < apply_handler_vec_.size(); i++) {
        if (apply_handler_vec_[i]) {
            co_await apply_handler_vec_[i]->Reset();
        }
    }

    for (;;) {
        auto st = co_await body->Read();
        if (!st) {
            LOG_ERROR("read data for snapshot={} error: {}", body->Name(), st);
            s.Set(st.Code(), st.Reason());
            co_await body->Close();
            co_return s;
        }
        if (st.Value().empty()) {
            break;
        }

        WriteBatch batch;
        std::vector<Buffer> key_vec;
        std::vector<Buffer> value_vec;
        Buffer b = std::move(st.Value());

        while (!b.empty()) {
            Buffer key, value;
            for (int i = 0; i < 2; i++) {
                uint32_t len = 0;
                const char* p = GetVarint32(b.get(), b.size(), &len);
                if (p == nullptr) {
                    s.Set(EINVAL);
                    LOG_ERROR(
                        "snapshot={} body is invalid, it cann't unmarshal",
                        body->Name());
                    co_await body->Close();
                    co_return s;
                }
                b.trim_front(b.get() - p);
                if (b.size() < len) {
                    s.Set(EINVAL);
                    LOG_ERROR(
                        "snapshot={} body is invalid, it cann't unmarshal",
                        body->Name());
                    co_await body->Close();
                    co_return s;
                }
                if (i == 0) {
                    key = std::move(b.share(0, len));
                } else {
                    value = std::move(b.share(0, len));
                }
                b.trim_front(len);
            }
            batch.Put(std::string_view(key.get(), key.size()),
                      std::string_view(value.get(), value.size()));
            key_vec.emplace_back(std::move(key));
            value_vec.emplace_back(std::move(value));
        }

        s = co_await Write(std::move(batch));
        if (!s) {
            LOG_ERROR("save snapshot={} body error: {}", body->Name(), st);
            s.Set(st.Code(), st.Reason());
            co_await body->Close();
            co_return s;
        }

        for (int i = 0; i < key_vec.size(); i++) {
            ApplyType type = static_cast<ApplyType>(key_vec[i][0]);
            if (!apply_handler_vec_[(int)type]) {
                continue;
            }
            co_await apply_handler_vec_[(int)type]->Restore(
                std::move(key_vec[i]), std::move(value_vec[i]));
        }
    }

    WriteBatch batch;
    applied_ = meta->metadata().index();
    char applied_val[sizeof(applied_)];
    net::BigEndian::PutUint64(applied_val, applied_);
    batch.Put(std::string_view(applied_key_),
              std::string_view(applied_val, sizeof(applied_val)));

    int n = meta_payload.nodes_size();
    for (int i = 0; i < n; ++i) {
        const auto& raft_node = meta_payload.nodes(i);
        Buffer b(raft_node.ByteSizeLong());
        raft_node.SerializeToArray(b.get_write(), b.size());
        char key[sizeof(uint64_t) + 1];
        key[0] = static_cast<char>(ApplyType::Raft);
        net::BigEndian::PutUint64(&key[1], raft_node.id());
        batch.Put(std::string_view(key, sizeof(key)),
                  std::string_view(b.get(), b.size()));
    }

    s = co_await Write(std::move(batch), true);
    co_await body->Close();
    co_return s;
}

void Storage::StatemachineImpl::LeadChange(uint64_t node_id) {
    lead_ = node_id;
    lead_change_cv_.broadcast();
}

seastar::future<> Storage::StatemachineImpl::WaitLeaderChange() {
    return lead_change_cv_.wait();
}

seastar::future<Status<>> Storage::StatemachineImpl::Put(Buffer key, Buffer val,
                                                         bool sync) {
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    s = co_await worker_thread_.Submit<Status<>>(
        [this, &key, &val, sync]() -> Status<> {
            Status<> s;
            rocksdb::WriteOptions wo;
            wo.sync = sync;
            wo.disableWAL = true;

            auto st = db_->Put(wo, rocksdb::Slice(key.get(), key.size()),
                               rocksdb::Slice(val.get(), val.size()));
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnExpect, st.ToString());
            }
            return s;
        });
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::Write(WriteBatch batch,
                                                           bool sync) {
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    s = co_await worker_thread_.Submit<Status<>>(
        [this, &batch, sync]() -> Status<> {
            Status<> s;
            rocksdb::WriteOptions wo;
            wo.sync = sync;
            wo.disableWAL = true;

            auto st = db_->Write(wo, &batch.batch_);
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnExpect, st.ToString());
            }
            return s;
        });

    co_return s;
}

seastar::future<Status<Buffer>> Storage::StatemachineImpl::Get(Buffer key) {
    Status<Buffer> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    auto st = co_await worker_thread_.Submit<Status<std::string>>(
        [this, &key]() -> Status<std::string> {
            rocksdb::ReadOptions ro;
            Status<std::string> s;
            std::string value;
            auto st =
                db_->Get(ro, rocksdb::Slice(key.get(), key.size()), &value);
            if (!st.ok()) {
                if (st.IsNotFound()) {
                    return std::move(s);
                }
                s.Set(ErrCode::ErrUnExpect, st.ToString());
                return s;
            }
            s.SetValue(std::move(value));
            return std::move(s);
        });
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    if (!st.Value().size()) {
        co_return s;  // return empty value
    }

    Buffer b = Buffer::copy_of(st.Value());
    s.SetValue(std::move(b));
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::Delete(Buffer key,
                                                            bool sync) {
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    auto st = co_await worker_thread_.Submit<Status<>>(
        [this, &key, sync]() -> Status<> {
            rocksdb::WriteOptions wo;
            Status<> s;
            wo.sync = sync;
            wo.disableWAL = true;

            auto st = db_->Delete(wo, rocksdb::Slice(key.get(), key.size()));
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnExpect, st.ToString());
                return s;
            }
            return s;
        });
    if (!st) {
        s.Set(st.Code(), st.Reason());
    }
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::DeleteRange(Buffer start,
                                                                 Buffer end,
                                                                 bool sync) {
    Status<> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    WriteBatch batch;
    batch.DeleteRange(std::string_view(start.get(), start.size()),
                      std::string_view(end.get(), end.size()));
    s = co_await Write(std::move(batch), sync);
    co_return s;
}

seastar::future<Status<rocksdb::Iterator*>>
Storage::StatemachineImpl::NewIterator(Buffer start, Buffer end,
                                       bool fill_cache) {
    Status<rocksdb::Iterator*> s;

    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);
    auto iter = co_await worker_thread_.Submit<rocksdb::Iterator*>(
        [this, &start, &end, fill_cache]() -> rocksdb::Iterator* {
            rocksdb::ReadOptions ro;
            Status<rocksdb::Iterator*> s;
            rocksdb::Slice lower_bound(start.get(), start.size());
            rocksdb::Slice upper_bound(end.get(), end.size());
            ro.fill_cache = fill_cache;
            ro.iterate_lower_bound = &lower_bound;
            ro.iterate_upper_bound = &upper_bound;
            auto iter = db_->NewIterator(ro);
            iter->SeekToFirst();
            return iter;
        });
    s.SetValue(iter);
    co_return s;
}

seastar::future<Status<>> Storage::StatemachineImpl::Flush() {
    Status<> s;
    if (gate_.is_closed()) {
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    s = co_await worker_thread_.Submit<Status<>>([this]() -> Status<> {
        Status<> s;
        rocksdb::FlushOptions opts;
        opts.wait = true;
        auto st = db_->Flush(opts);
        if (!st.ok()) {
            s.Set(ErrCode::ErrUnExpect, st.ToString());
        }
        return s;
    });
    co_return s;
}

seastar::future<> Storage::StatemachineImpl::ReleaseIterator(
    rocksdb::Iterator* iter) {
    if (gate_.is_closed()) {
        delete iter;
        co_return;
    }
    seastar::gate::holder holder(gate_);
    co_await worker_thread_.Submit<Status<>>([this, iter]() -> Status<> {
        delete iter;
        return Status<>();
    });
    co_return;
}

seastar::future<Status<std::vector<std::pair<std::string, std::string>>>>
Storage::StatemachineImpl::Range(rocksdb::Iterator* iter, size_t n) {
    Status<std::vector<std::pair<std::string, std::string>>> s;
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    s = co_await worker_thread_
            .Submit<Status<std::vector<std::pair<std::string, std::string>>>>(
                [this, iter, n]() mutable
                -> Status<std::vector<std::pair<std::string, std::string>>> {
                    Status<std::vector<std::pair<std::string, std::string>>> st;
                    std::vector<std::pair<std::string, std::string>> res;
                    for (size_t i = 0; iter->Valid() && i < n; i++) {
                        auto key = iter->key().ToString();
                        auto val = iter->value().ToString();
                        std::pair<std::string, std::string> p(std::move(key),
                                                              std::move(val));
                        res.emplace_back(std::move(p));
                        iter->Next();
                    }
                    auto st1 = iter->status();
                    if (!st1.ok()) {
                        st.Set(ErrCode::ErrUnExpect, st1.ToString());
                    } else {
                        st.SetValue(std::move(res));
                    }
                    return st;
                });
    co_return s;
}

std::vector<RaftNode> Storage::StatemachineImpl::GetRaftNodes() const {
    std::vector<RaftNode> nodes;
    for (auto iter : raft_nodes_) {
        nodes.push_back(iter.second);
    }
    return nodes;
}

seastar::future<> Storage::StatemachineImpl::Close() {
    if (gate_.is_closed()) {
        co_return;
    }

    lead_change_cv_.broken();
    co_await gate_.close();
    co_return;
}

seastar::future<Status<>> Storage::Propose(Buffer reqid, uint64_t id,
                                           ApplyType type, Buffer ctx,
                                           Buffer data) {
    ApplyEntry entry;
    entry.reqid = reqid.share();
    entry.id = id;
    entry.type = type;
    entry.ctx = ctx.share();
    entry.body = data.share();

    auto b = entry.Marshal();
    auto foreign_body =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(b)));
    return seastar::smp::submit_to(
        shard_,
        [this, foreign_body = std::move(
                   foreign_body)]() mutable -> seastar::future<Status<>> {
            auto body = foreign_buffer_copy(std::move(foreign_body));
            return raft_->Propose(std::move(body));
        });
}

seastar::future<Status<uint64_t>> Storage::ReadIndex() {
    return seastar::smp::submit_to(
        shard_, [this]() -> seastar::future<Status<uint64_t>> {
            return raft_->ReadIndex();
        });
}

seastar::future<Status<>> Storage::Put(Buffer key, Buffer val, bool sync) {
    auto foreign_key =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(key)));
    auto foreign_val =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(val)));
    return seastar::smp::submit_to(
        shard_,
        [this, foreign_key = std::move(foreign_key),
         foreign_val = std::move(foreign_val),
         sync]() mutable -> seastar::future<Status<>> {
            auto local_key = foreign_buffer_copy(std::move(foreign_key));
            auto local_val = foreign_buffer_copy(std::move(foreign_val));
            return impl_->Put(std::move(local_key), std::move(local_val), sync);
        });
}

seastar::future<Status<>> Storage::Write(WriteBatch batch, bool sync) {
    return seastar::smp::submit_to(
        shard_,
        [this, batch = std::move(batch),
         sync]() mutable -> seastar::future<Status<>> {
            return impl_->Write(std::move(batch), sync);
        });
}

seastar::future<Status<Buffer>> Storage::Get(Buffer key) {
    auto foreign_key =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(key)));
    Status<Buffer> s;
    auto st = co_await seastar::smp::submit_to(
        shard_, seastar::coroutine::lambda(
                    [this, foreign_key = std::move(foreign_key)]() mutable
                    -> seastar::future<
                        Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>>> {
                        Status<seastar::foreign_ptr<std::unique_ptr<Buffer>>> s;
                        auto local_key =
                            foreign_buffer_copy(std::move(foreign_key));
                        auto st = co_await impl_->Get(std::move(local_key));
                        if (!st) {
                            s.Set(st.Code(), st.Reason());
                            co_return s;
                        }
                        s.SetValue(seastar::make_foreign(
                            std::make_unique<Buffer>(std::move(st.Value()))));
                        co_return s;
                    }));
    if (!st) {
        s.Set(st.Code(), st.Reason());
        LOG_ERROR("get error: {}", st);
        co_return s;
    }
    s.SetValue(foreign_buffer_copy(std::move(st.Value())));
    co_return s;
}

seastar::future<Status<>> Storage::Delete(Buffer key, bool sync) {
    auto foreign_key =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(key)));
    return seastar::smp::submit_to(
        shard_,
        [this, foreign_key = std::move(foreign_key),
         sync]() mutable -> seastar::future<Status<>> {
            auto local_key = foreign_buffer_copy(std::move(foreign_key));
            return impl_->Delete(std::move(local_key), sync);
        });
}

seastar::future<Status<>> Storage::DeleteRange(Buffer start, Buffer end,
                                               bool sync) {
    auto foreign_start =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(start)));
    auto foreign_end =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(end)));
    return seastar::smp::submit_to(
        shard_,
        [this, foreign_start = std::move(foreign_start),
         foreign_end = std::move(foreign_end),
         sync]() mutable -> seastar::future<Status<>> {
            auto local_start = foreign_buffer_copy(std::move(foreign_start));
            auto local_end = foreign_buffer_copy(std::move(foreign_end));
            return impl_->DeleteRange(std::move(local_start),
                                      std::move(local_end), sync);
        });
}

seastar::future<Status<>> Storage::Range(
    Buffer start, Buffer end,
    seastar::noncopyable_function<void(const std::string&, const std::string&)>
        fn) {
    Status<> s;
    auto foreign_start =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(start)));
    auto foreign_end =
        seastar::make_foreign(std::make_unique<Buffer>(std::move(end)));

    auto st1 = co_await seastar::smp::submit_to(
        shard_,
        [this, foreign_start = std::move(foreign_start),
         foreign_end = std::move(foreign_end)]() mutable
        -> seastar::future<Status<rocksdb::Iterator*>> {
            auto local_start = foreign_buffer_copy(std::move(foreign_start));
            auto local_end = foreign_buffer_copy(std::move(foreign_end));
            return impl_->NewIterator(std::move(local_start),
                                      std::move(local_end));
        });
    if (!st1) {
        s.Set(st1.Code(), st1.Reason());
        co_return s;
    }

    rocksdb::Iterator* iter = st1.Value();
    const size_t batch = 1024;
    for (;;) {
        auto st2 = co_await seastar::smp::submit_to(
            shard_,
            [this, iter, batch]()
                -> seastar::future<
                    Status<std::vector<std::pair<std::string, std::string>>>> {
                return impl_->Range(iter, batch);
            });
        if (!st2) {
            s.Set(st2.Code(), st2.Reason());
            break;
        }

        std::vector<std::pair<std::string, std::string>> results =
            std::move(st2.Value());
        for (int i = 0; i < results.size(); i++) {
            fn(results[i].first, results[i].second);
        }
        if (results.size() < batch) {
            break;
        }
    }

    co_await seastar::smp::submit_to(shard_,
                                     [this, iter]() -> seastar::future<> {
                                         return impl_->ReleaseIterator(iter);
                                     });
    co_return s;
}

seastar::future<> Storage::Close() {
    co_await seastar::smp::submit_to(shard_, [this]() -> seastar::future<> {
        if (raft_) {
            co_await raft_->Close();
            raft_ = nullptr;
        }
        co_await impl_->Close();
        impl_ = nullptr;
        co_return;
    });
}

}  // namespace stream
}  // namespace snail
