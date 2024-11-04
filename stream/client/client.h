#pragma once
#include <google/protobuf/message.h>

#include "net/client.h"

namespace snail {
namespace stream {

class ClientWrapp {
    uint32_t timeout_;  // ms
    seastar::lw_shared_ptr<net::Client> client_;

   protected:
    bool init_ = false;
    seastar::gate gate_;

    seastar::future<Status<::google::protobuf::Message*>> Call(
        const ::google::protobuf::Message* req, uint16_t req_type,
        ::google::protobuf::Message* resp, uint16_t resp_type);

   public:
    explicit ClientWrapp(const std::string& host, uint16_t port,
                         uint32_t timeout, uint32_t connect_timeout = 100);

    virtual ~ClientWrapp() {}

    virtual seastar::future<Status<>> Init();

    seastar::future<> Close();
};

}  // namespace stream
}  // namespace snail
