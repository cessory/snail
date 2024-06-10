#include "wal.h"

namespace snail {
namespace stream {

static constexpr size_t kRecordKeySize = 18;

enum class RecordType {
    kMeta = 1,
    kLogEntry = 2,
};

static Buffer genDbKey(uint64_t group, uint16_t prefix, uint64_t key) {
    Buffer b(kRecordKeySize);
    net::BigEndian::PutUint64(b.get_write(), group);
    net::BigEndian::PutUint16(b.get_write() + 8, prefix);
    net::BigEndian::PutUint64(b.get_write() + 10, key);
    return b;
}

#define genHardStateKey(group) \
    genDbKey(group, static_cast<uint16_t>(RecordType::kMeta), 0)

#define genSnapshotKey(group) \
    genDbKey(group, static_cast<uint16_t>(RecordType::kMeta), 1)

#define genLogEntryKey(group, index) \
    genDbKey(group, static_cast<uint16_t>(RecordType::kLogEntry), index)

RaftWal::RaftWal(RaftWalFactory* factory, uint64_t group)
    : factory_(factory), group_(group) {}

seastar::future<Status<>> RaftWal::LoadHardState() {
    Status<> s;

    auto st = co_await factory_->worker_thread_.Submit(
        [this, db = factory_->db_]() -> Status<std::string> {
            Status<std::string> s;
            rocksdb::ReadOptions ro;
            auto key = genHardStateKey(group_);
            rocksdb::Slice kSlice(key.get(), key.size());
            std::string value;
            auto st = db->Get(ro, kSlice, &value);
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }
            s.SetValue(std::move(value));
            return std::move(s);
        });
    if (!st) {
        s.Set(st.Code(), st.Reason());
        LOG_ERROR("get hard state from wal error: {}", s);
        co_return s;
    }

    if (st.Value().empty()) {
        co_return s;
    }

    Buffer tmp(st.Value().get(), st.Value().size(), seastar::deleter());
    if (!hs_.Unmarshal(std::move(tmp))) {
        LOG_ERROR("invalid hard state");
        s.Set(EINVAL);
    }
    co_return s;
}

seastar::future<Status<>> RaftWal::LoadSnapshot() {
    Status<> s;
    auto st = co_await factory_->worker_thread_.Submit(
        [this, db = factory_->db_]() -> Status<std::string> {
            Status<std::string> s;
            rocksdb::ReadOptions ro;

            auto key = genSnapshotKey(group_);
            rocksdb::Slice kSlice(key.get(), key.size());
            std::string value;
            auto st = db->Get(ro, kSlice, &value);
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }

            s.SetValue(std::move(value));
            return std::move(s);
        });

    if (!st) {
        s.Set(st.Code(), st.Reason());
        LOG_ERROR("get snapshot from wal error: {}", s);
        co_return s;
    }
    if (st.Value().empty()) {  // snapshot is empty
        co_return s;
    }

    if (st.Value().size() != sizeof(RaftWal::Snapshot)) {
        LOG_ERROR("invalid snapshot size: {}", st.Value().size());
        s.Set(EINVAL);
        co_return s;
    }

    snapshot_.index = net::BigEndian::Uint64(st.Value().data());
    snapshot_.term =
        net::BigEndian::Uint64(st.Value().data() + sizeof(uint64_t));
    co_return s;
}

seastar::future<Status<>> RaftWal::LoadLastIndex() {
    Status<> s;

    auto st = co_await factory_->worker_thread_.Submit(
        [this, db = factory_->db_]() -> Status<std::string> {
            Status<std::string> s;

            rocksdb::ReadOptions ro;
            auto lower_key = genLogEntryKey(group_, 0);
            auto upper_key =
                genLogEntryKey(group_, std::numeric_limits<uint64_t>::max());
            rocksdb::Slice lower_bound(lower_key.get(), lower_key.size());
            rocksdb::Slice upper_bound(upper_key.get(), upper_key.size());
            ro.iterate_lower_bound = &lower_key;
            ro.iterate_upper_bound = &upper_key;

            std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(ro));
            iter->SeekToLast();
            auto st = iter->status();
            if (!st.ok()) {
                LOG_ERROR("get last index from wal error: {}", st.ToString());
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }
            std::string last_index = iter->key().ToString();
            s.SetValue(std::move(last_index));
            return std::move(s);
        });
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }

    if (st.Value().empty()) {
        co_return s;
    }

    if (st.Value().size() != kRecordKeySize) {
        s.Set(EINVAL);
        LOG_ERROR("invalid last index, key size={}", st.Value().size());
        co_return s;
    }
    uint64_t group = net::BigEndian::Uint64(st.Value().data());
    if (group != group_) {
        LOG_ERROR("get invalid group={}, except group={}", group, group_);
        s.Set(EINVAL);
        co_return s;
    }
    uint16_t type =
        net::BigEndian::Uint16(st.Value().data() + sizeof(uint64_t));
    if (type != (uint16_t)RecordType::kLogEntry) {
        LOG_ERROR("get invalid type={}, except type={}", type,
                  (uint16_t)RecordType::kLogEntry);
        s.Set(EINVAL);
        co_return s;
    }
    last_index_ = net::BigEndian::Uint64(st.Value().data() + 10);
    co_return s;
}

seastar::future<Status<>> RaftWal::Load() {
    Status<> s;

    s = co_await LoadHardState();
    if (!s) {
        co_return s;
    }

    s = co_await LoadSnapshot();
    if (!s) {
        co_return s;
    }

    s = co_await LoadLastIndex();
    if (last_index_ < snapshot_.index) {
        last_index_ = snapshot_.index;
    }
    co_return s;
}

HardState RaftWal::GetHardState() { return hs_; }

seastar::future<Status<>> RaftWal::Save(std::vector<EntryPtr> entries,
                                        HardState hs, bool sync) {
    Status<> s;

    std::vector<Buffer> buffers;
    rocksdb::WriteBatch batch;

    int n = entries.size();
    if (n) {
        if (entries[0]->index() == last_index_ + 1) {
            // nothing to do
        } else if (entries[0]->index() > last_index_ + 1) {
            LOG_ERROR("log index({}) is larger than last index({}) group={}",
                      entries[0]->index(), last_index_, group_);
            s.Set(EINVAL);
            co_return s;
        } else if (entries[0]->index() < last_index_) {
            auto begin = genLogEntryKey(group_, entries[0]->index());
            auto end = genLogEntryKey(group_, last_index_);
            batch.DeleteRange(rocksdb::Slice(begin.get(), begin.size()),
                              rocksdb::Slice(end.get(), end.size()));
            buffers.emplace_back(std::move(begin));
            buffers.emplace_back(std::move(end));
        } else if (entries[0]->index() == last_index_) {
            auto key = genLogEntryKey(group_, last_index_);
            batch.Delete(rocksdb::Slice(key.get(), key.size()));
            buffers.emplace_back(std::move(key));
        }

        for (int i = 0; i < n; ++i) {
            Buffer b(entries[i]->ByteSize());
            entries[i]->MarshalTo(b.get_write());
            Buffer hsKey = genLogEntryKey(group_, entries[i]->index());

            rocksdb::Slice key(hsKey.get(), hsKey.size());
            rocksdb::Slice value(b.get(), b.size());
            batch.Put(key, value);

            buffers.emplace_back(std::move(b));
            buffers.emplace_back(std::move(hsKey));
        }
    }

    if (!hs.Empty()) {
        Buffer b(hs.ByteSize());
        Buffer hsKey = genHardStateKey(group_);
        hs.MarshalTo(b.get_write());

        rocksdb::Slice key(hsKey.get(), hsKey.size());
        rocksdb::Slice value(b.get(), b.size());
        batch.Put(key, value);

        buffers.emplace_back(std::move(b));
        buffers.emplace_back(std::move(hsKey));
    }
    if (buffers.empty()) {
        co_return s;
    }
    s = co_await factory_->worker_thread_.Submit(
        [this, db = factory_->db_, sync, &batch]() -> Status<> {
            Status<> s;
            rocksdb::WriteOptions wo;
            wo.sync = sync;

            auto st = db->Write(wo, &batch);
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnexpect, st.ToString());
            }
            return s;
        });
    if (!s) {
        LOG_ERROR("save wal error: {}, group={}", s, group_);
        co_return s;
    }

    if (n) {
        last_index_ = entries.back()->index();
    }
    if (!hs.Empty()) {
        hs_ = hs;
    }
    co_return s;
}

uint64_t RaftWal::FirstIndex() { return snapshot_.index + 1; }

uint64_t RaftWal::LastIndex() { return last_index_; }

seastar::future<Status<std::vector<raft::EntryPtr>>> RaftWal::Entries(
    uint64_t lo, uint64_t hi, size_t max_size) {
    Status<std::vector<raft::EntryPtr>> s;
    std::vector<raft::EntryPtr> ents;

    if (lo >= hi) {
        s.Set(ErrCode::ErrRaftUnavailable);
        co_return s;
    } else if (lo <= snapshot_.index) {
        s.Set(ErrCode::ErrRaftCompacted);
        co_return s;
    }

    if (hi > last_index_ + 1) {
        s.Set(ErrCode::ErrRaftUnavailable, "hi is out of bound last index");
        co_return s;
    }

    s = co_await factory_->worker_thread_.Submit(
        [this, db = factory_->db_, lo, hi,
         max_size]() -> Status<std::vector<raft::EntryPtr>> {
            Status<std::vector<raft::EntryPtr>> s;
            std::vector<raft::EntryPtr> ents;
            rocksdb::ReadOptions ro;

            auto lower_key = genLogEntryKey(group_, lo);
            auto upper_key = genLogEntryKey(group_, hi);
            rocksdb::Slice lower_bound(lower_key.get(), lower_key.size());
            rocksdb::Slice upper_bound(upper_key.get(), upper_key.size());
            ro.iterate_lower_bound = &lower_key;
            ro.iterate_upper_bound = &upper_key;

            std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(ro));
            iter->SeekToFirst();
            auto st = iter->status();
            if (!st.ok()) {
                LOG_ERROR("get entries from wal error: {}", st.ToString());
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }
            size_t n = 0;
            for (; iter->Valid(); iter->Next()) {
                raft::EntryPtr ent = raft::make_entry();
                auto v = iter->value();
                Buffer b(v.data(), v.size(), seastar::deleter());
                ent->Unmarshal(std::move(b));
                if (ent.data().size() + n > max_size && !ents.empty()) {
                    break;
                }
                ents.push_back(ent);
            }
            s.SetValue(std::move(ents));
            return s;
        });
    co_return s;
}

seastar::future<Status<uint64_t>> RaftWal::Term(uint64_t index) {
    Status<uint64_t> s;
    if (index < snapshot_.index) {
        s.Set(ErrCode::ErrRaftCompacted);
        co_return s;
    } else if (index == snapshot_.index) {
        s.SetValue(snapshot_.term);
        co_return s;
    }

    if (index > last_index_) {
        LOG_ERROR("index={} is larger than last index({}), group={}", index,
                  last_index_, group_);
        s.Set(ErrCode::ErrRaftUnavailable);
        co_return s;
    }

    auto st = co_await factory_->worker_thread_.Submit(
        [this, db = factory_->db_, index]() -> Status<std::string> {
            Status<std::string> s;
            rocksdb::ReadOptions ro;

            auto key = genLogEntryKey(group_, index);
            rocksdb::Slice kSlice(key.get(), key.size());
            std::string value;
            auto st = db->Get(ro, kSlice, &value);
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }

            s.SetValue(std::move(value));
            return std::move(s);
        });
    if (!st) {
        s.Set(st.Code(), st.Reason());
        LOG_ERROR("get log entry from wal error: {}", s);
        co_return s;
    }

    if (st.Value().empty()) {
        LOG_ERROR("not found index={} in wal, group={}", index, group_);
        s.Set(ErrCode::ErrRaftUnavailable);
        co_return s;
    }

    Entry entry;
    if (!entry.Unmarshal(
            Buffer(st.Value().data(), st.Value().size(), seastar::deleter()))) {
        LOG_ERROR("unmarshal log entry error, group={}", group_);
        s.Set(ErrCode::ErrUnexpect, "unmarshal log entry error");
        co_return s;
    }
    co_return entry.term();
}

seastar::future<Status<>> RaftWal::Release(uint64_t index) {
    Status<> s;
    if (index <= snapshot_.index) {
        s.Set(ErrCode::ErrRaftCompacted);
        co_return s;
    }

    auto st = co_await Term(index);
    if (!st) {
        LOG_ERROR("get term error: {} index={} group={}", st, index, group_);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }

    s = co_await factory_->worker_thread_.Submit(
        [this, db = factory_->db_, index, term]() -> Status<> {
            Status<> s;
            rocksdb::WriteOptions wo;
            wo.sync = true;

            rocksdb::WriteBatch batch;
            auto key1 = genSnapshotKey(group_);
            char value[sizeof(RaftWal::Snapshot)];
            net::BigEndian::PutUint64(&value[0], index);
            net::BigEndian::PutUint64(&value[8], term);
            batch.Put(rocksdb::Slice(key1.get(), key1.size()),
                      rocksdb::Slice(value, sizeof(RaftWal::Snapshot)));

            auto begin = genLogEntryKey(group_, 0);
            auto end = genLogEntryKey(group_, index);
            batch.DeleteRange(rocksdb::Slice(begin.get(), begin.size()),
                              rocksdb::Slice(end.begin(), end.size()));
            auto st = db->Write(wo, &batch);
            if (!st.ok()) {
                s.Set(ErrCode::ErrUnExpect, st.ToString());
            }
            return s;
        });
    if (!s) {
        LOG_ERROR("release index={} error: {}, group={}", index, s, group_);
        co_return s;
    }
    snapshot_.index = index;
    snapshot_.term = term;
    co_return s;
}

seastar::future<Status<>> RaftWal::ApplySnapshot(uint64_t index,
                                                 uint64_t term) {
    Status<> s;

    s = co_await factory_->worker_thread_.Submit([this, db = factory_->db_,
                                                  index, term]() -> Status<> {
        Status<> s;
        HardState hs;
        hs.set_commit(index);
        hs.set_term(term);

        auto key1 = genHardStateKey(group_);
        auto key2 = genSnapshotKey(group_);

        Buffer val1(hs.ByteSize());
        Buffer val2(sizeof(RaftWal::Snapshot));
        hs.MarshalTo(val1.get_write());
        net::BigEndian::PutUint64(val2.get_write(), index);
        net::BigEndian::PutUint64(val2.get_write() + 8, term);

        rocksdb::WriteOptions wo;
        wo.sync = true;

        rocksdb::WriteBatch batch;
        batch.Put(rocksdb::Slice(key1.get(), key1.size()),
                  rocksdb::Slice(val1.get(), val1.size()));
        batch.Put(rocksdb::Slice(key2.get(), key2.size()),
                  rocksdb::Slice(val2.get(), val2.size()));

        auto begin = genLogEntryKey(group_, 0);
        auto end = genLogEntryKey(group_, std::numeric_limits<uint64_t>::max());
        batch.DeleteRange(rocksdb::Slice(begin.get(), begin.size()),
                          rocksdb::Slice(end.get(), end.size()));
        auto st = db->Write(wo, &batch);
        if (!st.ok()) {
            s.Set(ErrCode::ErrUnExpect, st.ToString());
        }
        return s;
    });
    if (!s) {
        LOG_ERROR("apply snapshot error: {}, group={}", s, group_);
        co_return s;
    }
    hs_.set_commit(index);
    hs_.set_term(term);
    hs_.set_vote(0);

    snapshot_.index = index;
    snapshot_.term = term;
    last_index_ = index;
    co_return s;
}

RaftWalFactory::RaftWalFactory(const std::string& path)
    : worker_thread_(), path_(path) {}

RaftWalFactory::~RaftWalFactory() {
    if (db_) {
        delete db_;
    }
}

seastar::future<Status<>> RaftWalFactory::OpenDB() {
    Status<> s;
    s = co_await worker_thread_.Submit([this] -> Status<> {
        Status<> s;
        rocksdb::Options options;
        options.create_if_missing = true;
        options.max_log_file_size = 100 << 20;
        options.keep_log_file_num = 10;

        auto st = rocksdb::DB::Open(options, path_, &db_);
        if (!st.ok()) {
            s.Set(ErrCode::ErrUnexpect, st.ToString());
        }
        return s;
    });
    co_return s;
}

seastar::future<Status<std::unique_ptr<RaftWalFactory>>> RaftWalFactory::Create(
    const std::string& path) {
    Status<std::unique_ptr<RaftWalFactory>> s;

    std::unique_ptr<RaftWalFactory> factory =
        std::make_unique<RaftWalFactory>(path);
    auto st = co_await factory->OpenDB();
    if (!st) {
        LOG_ERROR("open wal error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }

    s.SetValue(std::move(factory));
    co_return s;
}

seastar::future<Status<std::unique_ptr<RaftWal>>> RaftWalFactory::OpenRaftWal(
    uint64_t group) {
    Status<std::unique_ptr<RaftWal>> s;

    std::unique_ptr<RaftWal> raft_wal_ptr =
        std::make_unique<RaftWal>(this, group);

    auto st = co_await raft_wal_ptr->Load();
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    s.SetValue(std::move(raft_wal_ptr));
    co_return s;
}

}  // namespace stream
}  // namespace snail
