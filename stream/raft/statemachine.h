#pragma once
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "raft_proto.h"
#include "util/status.h"
#include "util/util.h"

namespace snail {
namespace raft {

class SmSnapshot {
   public:
    virtual const seastar::sstring Name() = 0;

    virtual uint64_t Index() const = 0;

    virtual seastar::future<Status<Buffer>> Read() = 0;
};

using SmSnapshotPtr = seastar::shared_ptr<SmSnapshot>;

class Statemachine {
   public:
    virtual seastar::future<Status<>> Apply(Buffer data, uint64_t index) = 0;

    virtual seastar::future<Status<>> ApplyConfChange(
        ConfChangeType type, uint64_t node_id, std::string raft_host,
        uint16_t raft_port, std::string host, uint16_t port,
        uint64_t index) = 0;

    virtual seastar::future<Status<SmSnapshotPtr>> CreateSnapshot() = 0;

    virtual seastar::future<Status<>> ApplySnapshot(SnapshotMetadata meta,
                                                    SmSnapshotPtr s) = 0;
};

using StatemachinePtr = seastar::shared_ptr<Statemachine>;

}  // namespace raft
}  // namespace snail
