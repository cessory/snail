#pragma once
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "raft/raft_proto.h"
#include "util/status.h"
#include "util/util.h"

namespace snail {
namespace stream {

class SmSnapshot {
   public:
    virtual const seastar::sstring& Name() = 0;

    virtual uint64_t Index() const = 0;

    virtual seastar::future<Status<Buffer>> Read() = 0;

    virtual seastar::future<> Close() = 0;
};

using SmSnapshotPtr = seastar::shared_ptr<SmSnapshot>;

class Statemachine {
   public:
    virtual seastar::future<Status<>> Apply(std::vector<Buffer> datas,
                                            uint64_t index) = 0;

    virtual seastar::future<Status<>> ApplyConfChange(
        raft::ConfChangeType type, uint64_t node_id, std::string raft_host,
        uint16_t raft_port, std::string host, uint16_t port,
        uint64_t index) = 0;

    virtual seastar::future<Status<SmSnapshotPtr>> CreateSnapshot() = 0;

    virtual seastar::future<Status<>> ApplySnapshot(raft::SnapshotPtr meta,
                                                    SmSnapshotPtr s) = 0;

    virtual void LeadChange(uint64_t node_id) = 0;
};

using StatemachinePtr = seastar::shared_ptr<Statemachine>;

}  // namespace stream
}  // namespace snail
