#include "master_client.h"

#include "proto/master.pb.h"
#include "util/logger.h"
#include "util/util.h"

namespace snail {
namespace stream {

MasterClient::MasterClient(const std::string& host, uint16_t port,
                           uint32_t timeout, uint32_t connect_timeout)
    : ClientWrapp(host, port, timeout, connect_timeout) {}

seastar::future<Status<>> MasterClient::Init() {
    CommonReq req;
    CommonResp resp;
    Status<> s;

    if (init_) {
        co_return s;
    }

    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }

    seastar::gate::holder holder(gate_);

    req.set_reqid(GenerateReqid());
    auto st =
        co_await Call(&req, GET_CLUSTER_INFO_REQ, &resp, GET_CLUSTER_INFO_RESP);
    if (!st) {
        s.Set(st.Code(), st.Reason());
        LOG_ERROR("get cluster info error: {}", s);
        co_return s;
    }
    cluster_id_ = resp.cluster_id();
    if (cluster_id_ == 0) {
        s.Set(EINVAL);
        LOG_ERROR("get cluster info error: invalid cluster id");
        co_return s;
    }
    init_ = true;
    co_return s;
}

seastar::future<Status<std::vector<uint32_t>>> MasterClient::AllocaDiskID(
    uint32_t n) {
    Status<std::vector<uint32_t>> s;
    std::vector<uint32_t> res;

    AllocDiskIdReq req;
    AllocDiskIdResp resp;

    req.mutable_header()->set_reqid(GenerateReqid());
    req.mutable_header()->set_cluster_id(cluster_id_);
    req.set_count(n);

    auto st = co_await Call(&req, ALLOC_DISKID_REQ, &resp, ALLOC_DISKID_RESP);
    if (!st) {
        LOG_ERROR("alloca disk id error: {}, reqid={}", st,
                  req.header().reqid());
        s.Set(st.Code(), st.Reason());
        co_return s;
    }

    for (int i = 0; i < resp.count(); i++) {
        res.push_back(resp.start() + i);
    }
    s.SetValue(std::move(res));
    co_return s;
}

}  // namespace stream
}  // namespace snail
