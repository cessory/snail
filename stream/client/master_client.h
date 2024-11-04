#pragma once

#include "client.h"

namespace snail {
namespace stream {

class MasterClient : public ClientWrapp {
    uint32_t cluster_id_ = 0;

   public:
    MasterClient(const std::string& host, uint16_t port, uint32_t timeout,
                 uint32_t connect_timeout = 100);

    virtual seastar::future<Status<>> Init() override;

    seastar::future<Status<std::vector<uint32_t>>> AllocaDiskID(uint32_t n);
};

}  // namespace stream
}  // namespace snail
