#include "wal.h"

namespace snail {
namespace stream {

static constexpr size_t kRecordKeySize = 10;

enum class RecordType {
    kHardState = 1,
    kCompacted = 2,
    kLogEntry = 3,
};

static Buffer genDbKey(uint16_t prefix, uint64_t val) {
    Buffer key(kRecordSizeLen);
    net::BigEndian::PutUint16(key.get_write(), prefix);
    net::BigEndian::PutUint16(key.get_write() + 2, val);
    return key;
}

#define genHardStateKey() \
    genDbKey(static_cast<uint16_t>(RecordType::kHardState), 0)

#define genCompactedIndexKey() \
    genDbKey(static_cast<uint16_t>(RecordType::kCompacted), 0)

#define genCompactedTermKey() \
    genDbKey(static_cast<uint16_t>(RecordType::kCompacted), 1)

#define genLogEntryKey(index) \
    genDbKey(static_cast<uint16_t>(RecordType::kLogEntry), index)

RaftWal::RaftWal(const std::string& path) : worker_thread_(), path_(path) {}

RaftWal::~RaftWal() {
    if (db_) {
        delete db_;
    }
}

seastar::future<Status<std::unique_ptr<RaftWal>>> RaftWal::OpenRaftWal(
    const std::string& path) {
    Status<std::unique_ptr<RaftWal>> s;
    try {
        std::unique_ptr<RaftWal> raft_wal_ptr = std::make_unique<RaftWal>(path);
        auto st = co_await raft_wal_ptr->Load();
        if (!st) {
            s.Set(st.Code(), st.Reason());
            co_return s;
        }
        s.SetValue(std::move(raft_wal_ptr));
    } catch (std::exception& e) {
        s.Set(ErrCode::ErrUnexpect, e.what());
    }
    co_return s;
}

bool RaftWal::poll() {
    if (_stopped.load(std::memory_order_relaxed)) {
        return false;
    }
    work_queue_.Complete();
    return true;
}

void RaftWal::work() {
    for (;;) {
        work_queue_.WaitEvent();
        if (stopped_.load(std::memory_order_relaxed)) {
            break;
        }
        work_queue_.Consume();
        reactor_->wakeup();
    }
}

seastar::future<Status<>> RaftWal::OpenDB() {
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

seastar::future<Status<>> RaftWal::Load() {
    Status<> s;
    auto st = co_await worker_thread_.Submit(
        [this] -> Status<std::tuple<std::string, std::string, std::string>> {
            Status<std::tuple<std::string, std::string, std::string>> s;
            rocksdb::ReadOptions ro;
            auto key = genHardStateKey();
            rocksdb::Slice kSlice(key.get(), key.size());
            std::string value;
            auto st = db_->Get(ro, kSlice, &value);
            if (!st.ok()) {
                LOG_ERROR("get hard state from wal error: {}", st.ToString());
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }

            auto lower_key = genLogEntryKey(0);
            auto upper_key =
                genLogEntryKey(std::numeric_limits<uint64_t>::max());
            rocksdb::Slice lower_bound(lower_key.get(), lower_key.size());
            rocksdb::Slice upper_bound(upper_key.get(), upper_key.size());
            ro.iterate_lower_bound = &lower_bound;
            ro.iterate_upper_bound = &upper_key;

            std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(ro));

            iter->SeekToFirst();
            st = iter->status();
            if (!st.ok()) {
                LOG_ERROR("get first index from wal error: {}", st.ToString());
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }
            std::string first_index = iter->key().ToString();

            iter->SeekToLast();
            st = iter->status();
            if (!st.ok()) {
                LOG_ERROR("get first index from wal error: {}", st.ToString());
                s.Set(ErrCode::ErrUnexpect, st.ToString());
                return s;
            }
            std::string last_index = iter->key().ToString();
            s.SetValue(std::make_tuple(std::move(value), std::move(first_index),
                                       std::move(last_index)));
            return std::move(s);
        });
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    std::string value;
    std::string first_index;
    std::string last_index;
    std::tie(value, first_index, last_index) = st.Value();

    if (value.size() != 0 && !hs_.Unmarshal(Buffer(value.data(), value.size(),
                                                   seastar::deleter()))) {
        s.Set(ErrCode::ErrUnexpect, "unmarshal HardState failure");
        co_return s;
    }

    if (first_index.size() != 0 && first_index.size() != kRecordKeySize) {
        LOG_ERROR("invalid first index, key size is illeg");
        s.Set(ErrCode::ErrUnexpect, "invalid first index");
        co_return s;
    } else if (first_index.size() == kRecordKeySize) {
        first_index_ = net::BigEndian::Uint64(first_index.data() + 2);
    }

    if (last_index.size() != 0 && last_index.size() != kRecordKeySize) {
        LOG_ERROR("invalid last index, key size is illeg");
        s.Set(ErrCode::ErrUnexpect, "invalid last index");
        co_return s;
    } else if (last_index.size() == kRecordKeySize) {
        last_index_ = net::BigEndian::Uint64(last_index.data() + 2);
    }

    if (first_index_ > last_index_) {
        s.Set(ErrCode::ErrUnexpect, "first index is greater than last index");
    }
    co_return s;
}

HardState RaftWal::GetHardState() { return hs_; }

}  // namespace stream
}  // namespace snail
