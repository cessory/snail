#pragma once
#include "extentnode_mgr.h"
#include "id_allocator.h"

namespace snail {
namespace stream {

class Service {
    uint32_t timeout_;  // request timeout(ms)
    uint32_t cluster_id_;
    RaftServerPtr raft_;
    IdAllocatorPtr diskid_alloctor_;
    ExtentnodeMgrPtr extentnode_mgr_;

   public:
    seastar::future<Status<>> HandleMessage(net::Stream* stream, Buffer b);
    // alloca an disk id
    seastar::future<Status<>> HandleAllocaDiskID(const AllocDiskIdReq* req,
                                                 net::Stream* stream);

    // and an extentnode into cluster
    seastar::future<Status<>> HandleAddNode(const AddNodeReq* req,
                                            net::Stream* stream);

    // add disk into node
    seastar::future<Status<>> HandleAddDisk();

    // update disk status
    seastar::future<Status<>> HandleUpdateDiskStatus();

    // get all disk ids in a node
    seastar::future<Status<>> HandleGetDisksByNodeID();

    seastar::future<Status<>> HandleGetNodeHostPortByDiskID();

    // update an extentnode info
    seastar::future<Status<>> HandleUpdateNode();

    // handle node's heartbeat
    seastar::future<Status<>> HandleNodeHeartbeat();

    seastar::future<Status<>> HandleGetNodeIDByHost();

    seastar::future<Status<>> HandleGetHostByNodeID();

    seastar::future<Status<>> HandleCreateNamespace();

    /////////////////stream manager////////////////
    // create a new stream for write
    seastar::future<Status<>> HandleCreateStream();

    // lease a batch of streams for write
    seastar::future<Status<>> HandleLeaseStreams();

    // open a stream for read
    seastar::future<Status<>> HandleOpenStream();

    // delete a stream, only stream that didn't have a lease can be deleted
    seastar::future<Status<>> HandleDeleteStream();

    seastar::future<Status<>> HandleRenameStream();

    seastar::future<Status<>> HandleListStreams();

    seastar::future<Status<>> HandleSealExtent();

    seastar::future<Status<>> HandleSealAndCreateExtent();
};

using ServicePtr = seastar::lw_shared_ptr<Service>;

}  // namespace stream
}  // namespace snail
