#include "tcp_server.h"

#include <isa-l.h>

#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>

#include "net/tcp_connection.h"
#include "proto/extentnode.pb.h"
#include "proto/rpc.h"
#include "util/logger.h"
#include "util/util.h"

namespace snail {
namespace stream {

seastar::future<Status<>> TcpServer::SendResp(
    const ::google::protobuf::Message* resp, ExtentnodeMsgType msgType,
    net::Stream* stream, unsigned shard_id) {
    Buffer buf = MarshalRpcMessage(resp, (uint16_t)msgType);
    Status<> s;
    if (shard_id == seastar::this_shard_id()) {
        s = co_await stream->WriteFrame(buf.get(), buf.size());
    } else {
        s = co_await seastar::smp::submit_to(
            shard_id,
            [stream, b = buf.get(),
             len = buf.size()]() -> seastar::future<Status<>> {
                auto s = co_await stream->WriteFrame(b, len);
                co_return s;
            });
    }
    co_return s;
}

TcpServer::TcpServer(const std::string& host, uint16_t port,
                     const std::set<unsigned>& shards)
    : sa_(seastar::ipv4_addr(host, port)), shard_index_(0) {
    for (auto shard : shards) {
        shards_.push_back(shard);
    }
    if (shards.empty()) {
        shards_.push_back(0);
    }
}

seastar::future<> TcpServer::RegisterService(
    seastar::foreign_ptr<ServicePtr> service) {
    auto it = service_map_.find(service->DeviceID());
    if (it != service_map_.end()) {
        LOG_ERROR("service-{} is already exist", service->DeviceID());
        if (service.get_owner_shard() == seastar::this_shard_id()) {
            co_await service->Close();
        } else {
            co_await seastar::smp::submit_to(
                service.get_owner_shard(),
                seastar::coroutine::lambda(
                    [&service]() { return service->Close(); }));
        }
    } else {
        service_map_[service->DeviceID()] = std::move(service);
    }
    co_return;
}

seastar::future<> TcpServer::Start() {
    try {
        fd_ = seastar::engine().posix_listen(sa_);
    } catch (std::exception& e) {
        LOG_ERROR("listen error: {}", e.what());
        co_return;
    }
    start_pr_ = seastar::promise<>();
    sess_mgr_.resize(seastar::smp::count);
    for (;;) {
        try {
            auto ar = co_await fd_.accept();
            unsigned shard = shards_[shard_index_++ % shards_.size()];
            if (shard == seastar::this_shard_id()) {
                auto conn = net::TcpConnection::make_connection(
                    std::move(std::get<0>(ar)), std::get<1>(ar));
                net::Option opt;
                auto sess = net::TcpSession::make_session(opt, conn, false);
                sess_mgr_[shard][sess->ID()] = sess;
                (void)HandleSession(sess);
            } else {
                (void)seastar::smp::submit_to(
                    shard, seastar::coroutine::lambda([this, shard,
                                                       ar = std::move(ar)]() {
                        auto conn = net::TcpConnection::make_connection(
                            std::move(std::get<0>(ar)), std::get<1>(ar));
                        net::Option opt;
                        auto sess =
                            net::TcpSession::make_session(opt, conn, false);
                        sess_mgr_[shard][sess->ID()] = sess;
                        (void)HandleSession(sess);
                    }));
            }
        } catch (std::exception& e) {
            LOG_ERROR("accept error: {}", e.what());
            break;
        }
    }
    start_pr_.value().set_value();
}

seastar::future<> TcpServer::HandleSession(net::SessionPtr sess) {
    for (;;) {
        auto s = co_await sess->AcceptStream();
        if (!s) {
            break;
        }

        (void)HandleStream(s.Value());
    }
    co_await sess->Close();
    sess_mgr_[seastar::this_shard_id()].erase(sess->ID());
    co_return;
}

seastar::future<> TcpServer::HandleStream(net::StreamPtr stream) {
    for (;;) {
        auto s = co_await stream->ReadFrame();
        if (!s) {
            break;
        }
        auto buf = std::move(s.Value());

        auto st = co_await HandleMessage(std::move(buf), stream);
        if (!st) {
            break;
        }
    }
    co_await stream->Close();
    co_return;
}

seastar::future<Status<>> TcpServer::HandleMessage(
    seastar::temporary_buffer<char> b, net::StreamPtr stream) {
    Status<> s;
    auto st = UnmarshalRpcMessage(b.share());
    if (!st) {
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    b = std::move(std::get<1>(st.Value()));
    ExtentnodeMsgType type =
        static_cast<ExtentnodeMsgType>(std::get<0>(st.Value()));

    std::unique_ptr<google::protobuf::Message> req;
    std::unordered_map<uint32_t, seastar::foreign_ptr<ServicePtr>>::iterator it;
    unsigned shard = seastar::this_shard_id();
    Service* service = nullptr;
    switch (type) {
        case WRITE_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new WriteExtentReq()));
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            it = service_map_.find(((WriteExtentReq*)req.get())->diskid());
            if (it == service_map_.end()) {
                LOG_ERROR("reqid={} not found disk={} for write",
                          ((WriteExtentReq*)req.get())->header().reqid(),
                          ((WriteExtentReq*)req.get())->diskid());
                s.Set(ENODEV);
                CommonResp resp;
                resp.set_reqid(((WriteExtentReq*)req.get())->header().reqid());
                resp.set_code(static_cast<int>(s.Code()));
                resp.set_reason(s.Reason());
                co_await SendResp(&resp, WRITE_EXTENT_RESP, stream.get(),
                                  shard);
                co_return s;
            }
            service = it->second.get();
            if (it->second.get_owner_shard() == shard) {
                s = co_await service->HandleWriteExtent(
                    (const WriteExtentReq*)req.get(), stream.get(), shard);
            } else {
                s = co_await seastar::smp::submit_to(
                    it->second.get_owner_shard(),
                    seastar::coroutine::lambda(
                        [service, r = (const WriteExtentReq*)req.get(),
                         sm = stream.get(),
                         shard]() -> seastar::future<Status<>> {
                            auto s = co_await service->HandleWriteExtent(r, sm,
                                                                         shard);
                            co_return s;
                        }));
            }
            break;
        case READ_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new ReadExtentReq()));
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            it = service_map_.find(((ReadExtentReq*)req.get())->diskid());
            if (it == service_map_.end()) {
                LOG_ERROR("reqid={} not found disk={} for read",
                          ((ReadExtentReq*)req.get())->header().reqid(),
                          ((ReadExtentReq*)req.get())->diskid());
                s.Set(ENODEV);
                ReadExtentResp resp;
                resp.mutable_header()->set_reqid(
                    ((ReadExtentReq*)req.get())->header().reqid());
                resp.mutable_header()->set_code(static_cast<int>(s.Code()));
                resp.mutable_header()->set_reason(s.Reason());
                co_await SendResp(&resp, READ_EXTENT_RESP, stream.get(), shard);
                co_return s;
            }
            service = it->second.get();

            service = it->second.get();
            if (it->second.get_owner_shard() == shard) {
                s = co_await service->HandleReadExtent(
                    (const ReadExtentReq*)req.get(), stream.get(), shard);
            } else {
                s = co_await seastar::smp::submit_to(
                    it->second.get_owner_shard(),
                    seastar::coroutine::lambda(
                        [service, r = (const ReadExtentReq*)req.get(),
                         sm = stream.get(),
                         shard]() -> seastar::future<Status<>> {
                            auto s = co_await service->HandleReadExtent(r, sm,
                                                                        shard);
                            co_return s;
                        }));
            }
            break;
        case CREATE_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new CreateExtentReq()));
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            it = service_map_.find(((CreateExtentReq*)req.get())->diskid());
            if (it == service_map_.end()) {
                LOG_ERROR("reqid={} not found disk={} for create extent",
                          ((CreateExtentReq*)req.get())->header().reqid(),
                          ((CreateExtentReq*)req.get())->diskid());
                s.Set(ENODEV);
                CommonResp resp;
                resp.set_reqid(((CreateExtentReq*)req.get())->header().reqid());
                resp.set_code(static_cast<int>(s.Code()));
                resp.set_reason(s.Reason());
                co_await SendResp(&resp, CREATE_EXTENT_RESP, stream.get(),
                                  shard);
                co_return s;
            }

            service = it->second.get();
            if (it->second.get_owner_shard() == shard) {
                s = co_await service->HandleCreateExtent(
                    (const CreateExtentReq*)req.get(), stream.get(), shard);
            } else {
                s = co_await seastar::smp::submit_to(
                    it->second.get_owner_shard(),
                    seastar::coroutine::lambda(
                        [service, r = (const CreateExtentReq*)req.get(),
                         sm = stream.get(),
                         shard]() -> seastar::future<Status<>> {
                            auto s = co_await service->HandleCreateExtent(
                                r, sm, shard);
                            co_return s;
                        }));
            }
            break;
        case DELETE_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new DeleteExtentReq()));
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            it = service_map_.find(((DeleteExtentReq*)req.get())->diskid());
            if (it == service_map_.end()) {
                LOG_ERROR("reqid={} not found disk={} for delete extent",
                          ((DeleteExtentReq*)req.get())->header().reqid(),
                          ((DeleteExtentReq*)req.get())->diskid());
                s.Set(ENODEV);
                CommonResp resp;
                resp.set_reqid(((DeleteExtentReq*)req.get())->header().reqid());
                resp.set_code(static_cast<int>(s.Code()));
                resp.set_reason(s.Reason());
                co_await SendResp(&resp, DELETE_EXTENT_RESP, stream.get(),
                                  shard);
                co_return s;
            }

            service = it->second.get();
            if (it->second.get_owner_shard() == shard) {
                s = co_await service->HandleDeleteExtent(
                    (const DeleteExtentReq*)req.get(), stream.get(), shard);
            } else {
                s = co_await seastar::smp::submit_to(
                    it->second.get_owner_shard(),
                    seastar::coroutine::lambda(
                        [service, r = (const DeleteExtentReq*)req.get(),
                         sm = stream.get(),
                         shard]() -> seastar::future<Status<>> {
                            auto s = co_await service->HandleDeleteExtent(
                                r, sm, shard);
                            co_return s;
                        }));
            }
            break;
        case GET_EXTENT_REQ:
            req = std::move(
                std::unique_ptr<google::protobuf::Message>(new GetExtentReq()));
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            it = service_map_.find(((GetExtentReq*)req.get())->diskid());
            if (it == service_map_.end()) {
                LOG_ERROR("reqid={} not found disk={} for get extent",
                          ((GetExtentReq*)req.get())->header().reqid(),
                          ((GetExtentReq*)req.get())->diskid());
                s.Set(ENODEV);
                GetExtentResp resp;
                resp.mutable_header()->set_reqid(
                    ((GetExtentReq*)req.get())->header().reqid());
                resp.mutable_header()->set_code(static_cast<int>(s.Code()));
                resp.mutable_header()->set_reason(s.Reason());
                co_await SendResp(&resp, GET_EXTENT_RESP, stream.get(), shard);
                co_return s;
            }

            service = it->second.get();
            if (it->second.get_owner_shard() == seastar::this_shard_id()) {
                s = co_await service->HandleGetExtent(
                    (const GetExtentReq*)req.get(), stream.get(), shard);
            } else {
                s = co_await seastar::smp::submit_to(
                    it->second.get_owner_shard(),
                    seastar::coroutine::lambda(
                        [service, r = (const GetExtentReq*)req.get(),
                         sm = stream.get(),
                         shard]() -> seastar::future<Status<>> {
                            auto s =
                                co_await service->HandleGetExtent(r, sm, shard);
                            co_return s;
                        }));
            }
            break;
        case UPDATE_DISK_STATUS_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new UpdateDiskStatusReq()));
            if (!req->ParseFromArray(b.get(), b.size())) {
                s.Set(EBADMSG);
                co_return s;
            }
            it = service_map_.find(((UpdateDiskStatusReq*)req.get())->diskid());
            if (it == service_map_.end()) {
                LOG_ERROR("reqid={} not found disk={} for update disk status",
                          ((UpdateDiskStatusReq*)req.get())->header().reqid(),
                          ((UpdateDiskStatusReq*)req.get())->diskid());
                s.Set(ENODEV);
                CommonResp resp;
                resp.set_reqid(
                    ((UpdateDiskStatusReq*)req.get())->header().reqid());
                resp.set_code(static_cast<int>(s.Code()));
                resp.set_reason(s.Reason());
                co_await SendResp(&resp, UPDATE_DISK_STATUS_RESP, stream.get(),
                                  shard);
                co_return s;
            }

            service = it->second.get();
            if (it->second.get_owner_shard() == shard) {
                s = co_await service->HandleUpdateDiskStatus(
                    (const UpdateDiskStatusReq*)req.get(), stream.get(), shard);
            } else {
                s = co_await seastar::smp::submit_to(
                    it->second.get_owner_shard(),
                    seastar::coroutine::lambda(
                        [service, r = (const UpdateDiskStatusReq*)req.get(),
                         sm = stream.get(),
                         shard]() -> seastar::future<Status<>> {
                            auto s = co_await service->HandleUpdateDiskStatus(
                                r, sm, shard);
                            co_return s;
                        }));
            }
            break;
        default:
            s.Set(EBADMSG);
            break;
    }
    co_return s;
}

static seastar::future<> CloseNetSessions(
    std::unordered_map<uint64_t, net::SessionPtr>* sessions) {
    std::vector<seastar::future<>> fu_vec;
    for (;;) {
        auto it = sessions->begin();
        for (int i = 0; i < 1024 && it != sessions->end(); i++) {
            auto fu = it->second->Close();
            fu_vec.emplace_back(std::move(fu));
            sessions->erase(it++);
        }
        if (fu_vec.empty()) {
            break;
        }
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        fu_vec.clear();
    }
    co_return;
}

seastar::future<> TcpServer::Close() {
    if (start_pr_) {
        fd_.close();
        co_await start_pr_.value().get_future();
        start_pr_.reset();
    }
    // close all session
    std::vector<seastar::future<>> fu_vec;
    for (int i = 0; i < sess_mgr_.size(); ++i) {
        std::unordered_map<uint64_t, net::SessionPtr> mgr =
            std::move(sess_mgr_[i]);
        std::unordered_map<uint64_t, net::SessionPtr>* ptr = &mgr;
        if (i == seastar::this_shard_id()) {
            auto fu = CloseNetSessions(ptr);
            fu_vec.emplace_back(std::move(fu));
        } else {
            auto fu = seastar::smp::submit_to(
                i, seastar::coroutine::lambda([ptr]() -> seastar::future<> {
                    return CloseNetSessions(ptr);
                }));
            fu_vec.emplace_back(std::move(fu));
        }
    }
    co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());

    // close service
    for (auto it = service_map_.begin(); it != service_map_.end(); it++) {
        seastar::foreign_ptr<ServicePtr> service = std::move(it->second);
        if (service.get_owner_shard() == seastar::this_shard_id()) {
            co_await service->Close();
        } else {
            co_await seastar::smp::submit_to(
                service.get_owner_shard(),
                seastar::coroutine::lambda([&service]() -> seastar::future<> {
                    co_await service->Close();
                }));
        }
    }
    co_return;
}

}  // namespace stream
}  // namespace snail
