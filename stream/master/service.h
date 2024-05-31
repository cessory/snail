#pragma once

namespace snail {
namespace stream {

class Service {
   public:
    // alloca an disk id
    seastar::future<Status<>> HandleAllocaDiskID();

    // add disk into node
    seastar::future<Status<>> HandleAddDisk();

    // update disk status
    seastar::future<Status<>> HandleUpdateDiskStatus();

    // get all disk ids in a node
    seastar::future<Status<>> HandleGetDisksByNodeID();

    seastar::future<Status<>> HandleGetNodeHostPortByDiskID();

    // and an extentnode into cluster
    seastar::future<Status<>> HandleAddNode();

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

}  // namespace stream
}  // namespace snail
