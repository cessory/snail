#include "client.h"

#include <seastar/core/when_all.hh>

#include "proto/rpc.h"
#include "util/logger.h"

namespace snail {
namespace stream {

seastar::future<Status<::google::protobuf::Message*>> ClientWrapp::Call(
    const ::google::protobuf::Message* req, uint16_t req_type,
    ::google::protobuf::Message* resp, uint16_t resp_type) {
    Status<::google::protobuf::Message*> s;
    if (!init_) {
        s.Set(ENOTSUP);
        co_return s;
    }

    if (!gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }

    seastar::gate::holder holder(gate_);

    auto b = MarshalRpcMessage(req, req_type);
    auto st = co_await client_->Get();
    if (!st) {
        LOG_ERROR("connect master error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    net::StreamPtr stream = std::move(st.Value());
    auto st1 = co_await stream->WriteFrame(std::move(b));
    if (!st1) {
        LOG_ERROR("send message to master error: {}", st1);
        s.Set(st1.Code(), st1.Reason());
        co_await stream->Close();
        co_return s;
    }

    auto st2 = co_await stream->ReadFrame(timeout_);
    if (!st2) {
        LOG_ERROR("read message from master error: {}", st2);
        co_await stream->Close();
        s.Set(st2.Code(), st2.Reason());
        co_return s;
    }
    auto st3 = UnmarshalRpcMessage(std::move(st2.Value()));
    if (!st3) {
        LOG_ERROR("unmarshal response message error: {}", st3);
        s.Set(st3.Code(), st3.Reason());
        co_await stream->Close();
        co_return s;
    }
    std::tuple<uint16_t, Buffer> res = std::move(st3.Value());
    uint16_t type = std::get<0>(res);
    if (type != resp_type) {
        LOG_ERROR("recv invalid message type");
        co_await client_->Put(stream);
        s.Set(EBADMSG);
        co_return s;
    }
    Buffer body = std::move(std::get<1>(res));
    if (!resp->ParseFromArray(body.get(), body.size())) {
        LOG_ERROR("parse response message error");
        co_await client_->Put(stream);
        s.Set(EBADMSG);
        co_return s;
    }
    co_await client_->Put(stream);
    s.SetValue(resp);
    co_return s;
}

ClientWrapp::ClientWrapp(const std::string& host, uint16_t port,
                         uint32_t timeout, uint32_t connect_timeout) {
    net::Option opt;
    timeout_ = timeout;
    opt.write_timeout_ms = timeout_;
    client_ =
        seastar::make_lw_shared<net::Client>(host, port, opt, connect_timeout);
}

seastar::future<Status<>> ClientWrapp::Init() {
    init_ = true;
    co_return Status<>();
}

seastar::future<> ClientWrapp::Close() {
    if (gate_.is_closed()) {
        co_return;
    }

    auto fu1 = gate_.close();
    auto fu2 = client_->Close();
    co_await seastar::when_all_succeed(std::move(fu1), std::move(fu2));
    co_return;
}

}  // namespace stream
}  // namespace snail
