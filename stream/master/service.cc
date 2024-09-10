#include "service.h"

#include "proto/master.pb.h"
#include "proto/rpc.h"
#include "util/logger.h"

namespace snail {
namespace stream {

static seastar::future<Status<>> SendResp(
    const ::google::protobuf::Message* resp, MasterMsgType msgType,
    net::Stream* stream) {
    Buffer buf = MarshalRpcMessage(resp, (uint16_t)msgType);
    Status<> s;
    s = co_await stream->WriteFrame(buf.get(), buf.size());
    co_return s;
}

seastar::future<Status<>> Service::HandleMessage(net::Stream* stream,
                                                 Buffer b) {
    Status<> s;
    auto st = UnmarshalRpcMessage(b.share());
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    MasterMsgType type = static_cast<MasterMsgType>(std::get<0>(st.Value()));
    b = std::move(std::get<1>(st.Value()));

    switch (type) {
        case ALLOC_DISKID_REQ: {
            std::unique_ptr<AllocDiskIdReq> req(new AllocDiskIdReq);
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await HandleAllocaDiskID(req.get(), stream);
            break;
        }
        case ADD_NODE_REQ: {
            std::unique_ptr<AddNodeReq> req(new AddNodeReq);
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await HandleAddNode(req.get(), stream);
            break;
        }
        default:
            LOG_ERROR("invalid msg type={}", static_cast<uint16_t>(type));
            s.Set(EBADMSG);
            break;
    }
    co_return s;
}

seastar::future<Status<>> Service::HandleAllocaDiskID(const AllocDiskIdReq* req,
                                                      net::Stream* stream) {
    Status<> s;
    Buffer reqid = Buffer::copy_of(req->header().reqid());
    AllocDiskIdResp resp;
    resp.mutable_header()->set_cluster_id(cluster_id_);
    if (req->header().cluster_id() != cluster_id_) {
        resp.mutable_header()->set_code(
            static_cast<uint32_t>(ErrCode::ErrCluster));
        s = co_await SendResp(&resp, ALLOC_DISKID_RESP, stream);
        co_return s;
    }
    resp.mutable_header()->set_reqid(req->header().reqid());
    std::chrono::milliseconds timeout{timeout_};
    auto st = co_await diskid_alloctor_->Alloc(raft_, reqid.share(),
                                               req->count(), timeout);
    resp.mutable_header()->set_code(static_cast<int32_t>(st.Code()));
    if (!st) {
        resp.mutable_header()->set_reason(st.Reason());
        resp.mutable_header()->set_reqid(req->header().reqid());
    } else {
        if (st.Value() > (uint64_t)UINT32_MAX) {
            resp.mutable_header()->set_code(ERANGE);
        } else {
            resp.set_start(st.Value());
            resp.set_count(req->count());
        }
    }
    s = co_await SendResp(&resp, ALLOC_DISKID_RESP, stream);
    co_return s;
}

seastar::future<Status<>> Service::HandleAddNode(const AddNodeReq* req,
                                                 net::Stream* stream) {
    Status<> s;
    Buffer reqid = Buffer::copy_of(req->header().reqid());
    AddNodeResp resp;
    resp.mutable_header()->set_reqid(req->header().reqid());
    if (req->header().cluster_id() != cluster_id_) {
        resp.mutable_header()->set_code(
            static_cast<uint32_t>(ErrCode::ErrCluster));
        s = co_await SendResp(&resp, ADD_NODE_RESP, stream);
        co_return s;
    }
    resp.mutable_header()->set_cluster_id(cluster_id_);
    if (req->host().empty() || req->port() == 0 || req->rack().empty() ||
        req->az().empty()) {
        LOG_ERROR("reqid={} invalid AddNodeReq",
                  std::string_view(reqid.get(), reqid.size()));
        resp.mutable_header()->set_code(EINVAL);
        s = co_await SendResp(&resp, ADD_NODE_RESP, stream);
        co_return s;
    }
    std::chrono::milliseconds timeout{timeout_};
    auto st = co_await extentnode_mgr_->AddNode(
        raft_, reqid.share(), req->host(), req->port(), req->rack(), req->az(),
        timeout);
    resp.mutable_header()->set_code(static_cast<uint32_t>(st.Code()));
    if (!st) {
        resp.mutable_header()->set_reason(st.Reason());
        LOG_ERROR("reqid={} add node host={} port={} rack={} az={} error: {}",
                  std::string_view(reqid.get(), reqid.size()), req->host(),
                  req->port(), req->rack(), req->az(), st);
    } else {
        resp.set_node_id(st.Value());
        LOG_ERROR(
            "reqid={} add node host={} port={} rack={} az={} succeed, return "
            "node_id={}",
            std::string_view(reqid.get(), reqid.size()), req->host(),
            req->port(), req->rack(), req->az(), st.Value());
    }
    s = co_await SendResp(&resp, ADD_NODE_RESP, stream);
    co_return s;
}

}  // namespace stream
}  // namespace snail
