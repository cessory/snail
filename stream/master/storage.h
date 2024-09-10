#pragma once
#include <chrono>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <string_view>

#include "raft_server.h"
#include "raft_statemachine.h"
#include "rocksdb/write_batch.h"
#include "util/async_thread.h"

namespace snail {
namespace stream {

class Storage;
using StoragePtr = seastar::shared_ptr<Storage>;

enum class ApplyType {
    Raft = 1,
    Stream = 2,
    Extent = 3,
    Disk = 4,
    Node = 5,
    Task = 6,
    DiskID = 7,
    NodeID = 8,
    Max = 255,
};

class WriteBatch {
    rocksdb::WriteBatch batch_;

    friend class Storage;

   public:
    WriteBatch() = default;
    WriteBatch(WriteBatch&&) = default;

    void Put(const std::string_view& key, const std::string_view& value);
    void Delete(const std::string_view& key);
    void DeleteRange(const std::string_view& start,
                     const std::string_view& end);
};

class ApplyHandler {
   public:
    virtual seastar::future<Status<>> Apply(Buffer reqid, uint64_t id,
                                            Buffer ctx, Buffer data) = 0;
    virtual void Reset() = 0;

    virtual void Restore(Buffer key, Buffer val) = 0;
};

using ApplyHandlerPtr = seastar::shared_ptr<ApplyHandler>;

class Storage : public Statemachine {
    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;
    AsyncThread worker_thread_;
    uint64_t applied_;
    uint64_t snapshot_seq_;
    uint64_t lead_;
    std::string applied_key_;
    seastar::gate gate_;

    std::unordered_map<uint64_t, RaftNode> raft_nodes_;

    std::array<seastar::foreign_ptr<ApplyHandlerPtr>, (int)ApplyType::Max>
        apply_handler_vec_;

    seastar::future<Status<>> LoadRaft();

    seastar::future<Status<>> SaveApplied(uint64_t index, bool sync);

   public:
    explicit Storage(const std::string& db_path);
    static seastar::future<Status<StoragePtr>> Create(std::string_view db_path);

    bool RegisterApplyHandler(ApplyType type,
                              seastar::foreign_ptr<ApplyHandlerPtr> handler);

    seastar::future<Status<>> Apply(std::vector<Buffer> datas,
                                    uint64_t index) override;

    seastar::future<Status<>> ApplyConfChange(raft::ConfChangeType type,
                                              uint64_t node_id,
                                              std::string raft_host,
                                              uint16_t raft_port,
                                              std::string host, uint16_t port,
                                              uint64_t index) override;

    seastar::future<Status<SmSnapshotPtr>> CreateSnapshot() override;

    seastar::future<Status<>> ApplySnapshot(raft::SnapshotPtr meta,
                                            SmSnapshotPtr body) override;

    void LeadChange(uint64_t node_id) override;

    seastar::future<Status<>> Propose(RaftServerPtr raft, Buffer reqid,
                                      uint64_t id, ApplyType type, Buffer ctx,
                                      Buffer data);

    seastar::future<Status<>> Put(Buffer key, Buffer val, bool sync = false);

    seastar::future<Status<>> Write(WriteBatch batch, bool sync = false);

    seastar::future<Status<Buffer>> Get(Buffer key);

    seastar::future<Status<>> Delete(Buffer key, bool sync = false);

    seastar::future<Status<>> DeleteRange(Buffer start, Buffer end,
                                          bool sync = false);

    seastar::future<Status<>> Range(Buffer start, Buffer end,
                                    seastar::noncopyable_function<void(
                                        const std::string&, const std::string&)>
                                        fn);

    uint64_t Applied() const { return applied_; }

    std::vector<RaftNode> GetRaftNodes() const;

    seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail
