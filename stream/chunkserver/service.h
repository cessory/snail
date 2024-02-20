#pragma once

#include "store.h"

namespace snail {
namespace stream {

class Service {
    StorePtr store_;

   public:
    explicit Service(StorePtr store) : store_(store) {}

    seastar::future<Status<>> WriteExtent(const WriteExtentReq *req,
                                          net::StreamPtr s);

    seastar::future<Status<>> ReadExtent(const ReadExtentReq *req,
                                         net::StreamPtr s);

    seastar::future<Status<>> DeleteExtent(const DeleteExtentReq *req,
                                           net::StreamPtr s);

    seastar::future<Status<>> GetExtent(const GetExtentReq *req,
                                        net::StreamPtr s);

    seastar::future<Status<>> UpdateDiskStatus(const UpdateDiskStatusReq *req,
                                               net::StreamPtr s);
};

}  // namespace stream
}  // namespace snail
