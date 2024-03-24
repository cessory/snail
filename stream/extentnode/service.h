#pragma once

#include "net/tcp_session.h"
#include "proto/extentnode.pb.h"
#include "store.h"

namespace snail {
namespace stream {

class Service {
    StorePtr store_;

   public:
    explicit Service(StorePtr store) : store_(store) {}

    seastar::future<Status<>> HandleWriteExtent(const WriteExtentReq *req,
                                                net::StreamPtr stream);

    seastar::future<Status<>> HandleReadExtent(const ReadExtentReq *req,
                                               net::StreamPtr stream);

    seastar::future<Status<>> HandleCreateExtent(const CreateExtentReq *req,
                                                 net::StreamPtr stream);

    seastar::future<Status<>> HandleDeleteExtent(const DeleteExtentReq *req,
                                                 net::StreamPtr stream);

    seastar::future<Status<>> HandleGetExtent(const GetExtentReq *req,
                                              net::StreamPtr s);

    seastar::future<Status<>> HandleUpdateDiskStatus(
        const UpdateDiskStatusReq *req, net::StreamPtr s);
};

using ServicePtr = seastar::lw_shared_ptr<Service>;

}  // namespace stream
}  // namespace snail
