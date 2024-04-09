#include "tcp_server.h"

#include <isa-l.h>

#include <seastar/core/reactor.hh>

#include "net/tcp_connection.h"
#include "proto/extentnode.pb.h"

namespace snail {
namespace stream {

TcpServer::TcpServer(const std::string& host, uint16_t port, ServicePtr s)
    : sa_(seastar::ipv4_addr(host, port)), service_(s) {}

seastar::future<> TcpServer::Start() {
    auto fd = seastar::engine().posix_listen(sa_);
    for (;;) {
        auto ar = co_await fd.accept();
        auto conn = net::TcpConnection::make_connection(
            std::move(std::get<0>(ar)), std::get<1>(ar));
        net::Option opt;
        auto sess = net::TcpSession::make_session(opt, conn, false);
        (void)HandleSession(sess);
    }
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
    co_return;
}

seastar::future<> TcpServer::HandleStream(net::StreamPtr stream) {
    for (;;) {
        auto s = co_await stream->ReadFrame();
        if (!s) {
            break;
        }
        auto buf = std::move(s.Value());
        if (buf.size() <= kMetaMsgHeaderLen) {
            break;
        }

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
    uint32_t origin_crc = net::BigEndian::Uint32(b.get());
    ExtentnodeMsgType type =
        static_cast<ExtentnodeMsgType>(net::BigEndian::Uint16(b.get() + 4));
    uint16_t len = net::BigEndian::Uint16(b.get() + 6);

    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char*>(b.get() + 4), b.size() - 4);
    if (crc != origin_crc) {
        s.Set(ErrCode::ErrInvalidChecksum);
        co_return s;
    }

    if (len != b.size() - kMetaMsgHeaderLen) {
        s.Set(EBADMSG);
        co_return s;
    }

    std::unique_ptr<google::protobuf::Message> req;
    switch (type) {
        case WRITE_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new WriteExtentReq()));
            if (!req->ParseFromArray(b.get() + kMetaMsgHeaderLen, len)) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await service_->HandleWriteExtent(
                (const WriteExtentReq*)req.get(), stream);
            break;
        case READ_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new ReadExtentReq()));
            if (!req->ParseFromArray(b.get() + kMetaMsgHeaderLen, len)) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await service_->HandleReadExtent(
                (const ReadExtentReq*)req.get(), stream);
            break;
        case CREATE_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new CreateExtentReq()));
            if (!req->ParseFromArray(b.get() + kMetaMsgHeaderLen, len)) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await service_->HandleCreateExtent(
                (const CreateExtentReq*)req.get(), stream);
            break;
        case DELETE_EXTENT_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new DeleteExtentReq()));
            if (!req->ParseFromArray(b.get() + kMetaMsgHeaderLen, len)) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await service_->HandleDeleteExtent(
                (const DeleteExtentReq*)req.get(), stream);
            break;
        case GET_EXTENT_REQ:
            req = std::move(
                std::unique_ptr<google::protobuf::Message>(new GetExtentReq()));
            if (!req->ParseFromArray(b.get() + kMetaMsgHeaderLen, len)) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await service_->HandleGetExtent(
                (const GetExtentReq*)req.get(), stream);
            break;
        case UPDATE_DISK_STATUS_REQ:
            req = std::move(std::unique_ptr<google::protobuf::Message>(
                new UpdateDiskStatusReq()));
            if (!req->ParseFromArray(b.get() + kMetaMsgHeaderLen, len)) {
                s.Set(EBADMSG);
                co_return s;
            }
            s = co_await service_->HandleUpdateDiskStatus(
                (const UpdateDiskStatusReq*)req.get(), stream);
            break;
        default:
            s.Set(EBADMSG);
            break;
    }
    co_return s;
}

}  // namespace stream
}  // namespace snail
