#include "raft_transport.h"

#include <isa-l/crc.h>

#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/defer.hh>

#include "net/frame.h"
#include "net/tcp_connection.h"
#include "net/tcp_session.h"
#include "raft/raft_proto.h"
#include "util/logger.h"

namespace snail {
namespace stream {

enum class RpcMessageType {
    Normal = 0,
    Snapshot = 1,
};

//| 4 bytes | 4 bytes |1 bytes|
//|   crc   |  len    |type|
static constexpr uint32_t kRpcHeaderSize = 9;

class ReceivedSnapshot : public SmSnapshot {
    seastar::sstring name_;
    uint64_t index_;
    bool eof_;
    std::queue<Buffer> pending_;
    seastar::semaphore pending_sem_ = {128};
    seastar::condition_variable cv_;
    seastar::gate gate_;

   public:
    explicit ReceivedSnapshot(const seastar::sstring& name, uint64_t index)
        : name_(name), index_(index), eof_(false) {}

    virtual ~ReceivedSnapshot() {}

    seastar::future<Status<>> Write(Buffer b) {
        Status<> s;
        if (gate_.is_closed()) {
            s.Set(EPIPE);
            co_return s;
        }
        seastar::gate::holder holder(gate_);
        try {
            co_await pending_sem_.wait();
        } catch (...) {
            s.Set(EPIPE);
            co_return s;
        }
        pending_.push(std::move(b));
        cv_.signal();
        co_return s;
    }

    const seastar::sstring& Name() override { return name_; }

    uint64_t Index() const override { return index_; }

    seastar::future<Status<Buffer>> Read() override {
        Status<Buffer> s;

        if (gate_.is_closed()) {
            s.Set(EPIPE);
            co_return s;
        }
        seastar::gate::holder holder(gate_);

        while (!gate_.is_closed()) {
            if (eof_) {
                break;
            }
            if (!pending_.empty()) {
                Buffer& b = pending_.front();
                if (b.empty()) {
                    eof_ = true;
                } else {
                    s.SetValue(std::move(b));
                }
                pending_.pop();
                pending_sem_.signal();
                break;
            }
            co_await cv_.wait();
        }

        if (gate_.is_closed()) {
            s.Set(EPIPE);
        }
        co_return s;
    };

    seastar::future<> Close() override {
        if (!gate_.is_closed()) {
            cv_.signal();
            pending_sem_.broken();
            co_await gate_.close();
        }
        co_return;
    }
};

RaftSender::Client::Client(uint64_t id, const std::string& raft_host,
                           uint16_t raft_port)
    : node_id(id), host(raft_host), port(raft_port), last_error_log(false) {
    client = seastar::make_lw_shared<net::Client>(raft_host, raft_port);
}

seastar::future<> RaftSender::Client::Send(std::vector<Buffer> buffers) {
    if (gate.is_closed()) {
        co_return;
    }

    seastar::gate::holder holder(gate);

    if (!stream || !((*stream)->Valid())) {
        if (stream) {
            co_await stream.value()->Close();
            stream.reset();
        }
        auto st = co_await client->Get();
        if (!st) {
            if (!last_error_log) {
                last_error_log = true;
                LOG_ERROR("connect raft node(id={} host={} port={}) error: {}",
                          node_id, host, port, st);
            }
            co_return;
        }
        stream = st.Value();
        LOG_INFO("connect raft node(id={} host={} port={}) succeed", node_id,
                 host, port);
        last_error_log = false;
    }
    uint32_t max_frame_size = (*stream)->MaxFrameSize();
    std::vector<seastar::future<Status<>>> fu_vec;
    for (int i = 0; i < buffers.size(); ++i) {
        uint32_t n = 0;
        for (const char* p = buffers[i].get(); p < buffers[i].end(); p += n) {
            n = std::min(buffers[i].end() - p,
                         static_cast<long int>(max_frame_size));
            auto fu = (*stream)->WriteFrame(p, n);
            fu_vec.emplace_back(std::move(fu));
        }
    }
    auto results =
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    for (int i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG_ERROR(
                "send raft message to raft node(id={} host={} port={}) error: "
                "{}",
                node_id, host, port, results[i]);
            co_await stream.value()->Close();
            stream.reset();
            break;
        }
    }
    co_return;
}

seastar::future<Status<>> RaftSender::Client::SendSnapshot(raft::MessagePtr msg,
                                                           SmSnapshotPtr body) {
    Status<> s;
    if (gate.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate);
    net::StreamPtr stream_ptr;

    auto st = co_await client->Get();
    if (!st) {
        LOG_ERROR("open stream error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    stream_ptr = st.Value();

    uint32_t n = msg->ByteSize();
    Buffer b(n + kRpcHeaderSize);
    msg->MarshalTo(b.get_write() + kRpcHeaderSize);
    net::BigEndian::PutUint32(b.get_write() + 4, n);
    *(b.get_write() + kRpcHeaderSize - 1) =
        static_cast<char>(RpcMessageType::Snapshot);
    uint32_t crc =
        crc32_gzip_refl(0, (const uint8_t*)(b.get() + 4), b.size() - 4);
    net::BigEndian::PutUint32(b.get_write(), crc);

    std::optional<seastar::future<Status<>>> fu;
    fu = stream_ptr->WriteFrame(std::move(b));

    std::vector<Buffer> buffers;
    while (!gate.is_closed()) {
        auto st = co_await body->Read();
        if (!st) {
            LOG_ERROR("read raft snapshot {} error: {}", body->Name(), st);
            s.Set(st.Code(), st.Reason());
            break;
        }
        b = std::move(st.Value());
        if (b.empty()) {
            break;
        }

        Buffer head(kRpcHeaderSize);
        net::BigEndian::PutUint32(head.get_write() + 4, b.size());
        *(head.get_write() + kRpcHeaderSize - 1) =
            static_cast<char>(RpcMessageType::Snapshot);
        uint32_t crc = crc32_gzip_refl(0, (const uint8_t*)(head.get() + 4),
                                       kRpcHeaderSize - 4);
        crc = crc32_gzip_refl(crc, (const uint8_t*)(b.get()), b.size());
        net::BigEndian::PutUint32(head.get_write(), crc);
        size_t n = head.size();
        buffers.emplace_back(std::move(head));
        while (n < stream_ptr->MaxFrameSize() && !b.empty()) {
            size_t body_size = stream_ptr->MaxFrameSize() - n;
            body_size = std::min(body_size, b.size());
            buffers.emplace_back(std::move(b.share(0, body_size)));
            s = co_await stream_ptr->WriteFrame(std::move(buffers));
            if (!s) {
                LOG_ERROR(
                    "send raft snapshot {} to raft node(id={}, raft_host={} "
                    "raft_port={}) error: {}",
                    body->Name(), node_id, host, port, s);
                break;
            }
            b.trim_front(body_size);
            n = 0;
        }
        if (!s) {
            break;
        }
    }

    if (s && !gate.is_closed()) {
        // sending empty data means that the snapshot has ended
        Buffer head(kRpcHeaderSize);
        net::BigEndian::PutUint32(head.get_write() + 4, 0);
        *(head.get_write() + kRpcHeaderSize - 1) =
            static_cast<char>(RpcMessageType::Snapshot);
        uint32_t crc = crc32_gzip_refl(0, (const uint8_t*)(head.get() + 4),
                                       kRpcHeaderSize - 4);
        net::BigEndian::PutUint32(head.get_write(), crc);
        s = co_await stream_ptr->WriteFrame(std::move(head));
        if (!s) {
            LOG_ERROR(
                "send raft snapshot {} to raft node(id={}, raft_host={} "
                "raft_port={}) error: {}",
                body->Name(), node_id, host, port, s);
        } else {
            // recv snapshot response
            auto st = co_await stream_ptr->ReadFrame();
            if (!st) {
                LOG_ERROR(
                    "recv raft snapshot {}  response from raft node(id={}, "
                    "raft_host={} "
                    "raft_port={}) error: {}",
                    body->Name(), node_id, host, port, st);
                co_await stream_ptr->Close();
                s.Set(st.Code(), st.Reason());
                co_return s;
            }
            Buffer buf = std::move(st.Value());
            if (buf.size() != kRpcHeaderSize) {
                s.Set(EBADMSG);
                co_await stream_ptr->Close();
                co_return s;
            }

            // check response
            uint32_t crc = net::BigEndian::Uint32(buf.get());
            uint32_t len = net::BigEndian::Uint32(buf.get() + 4);
            RpcMessageType type = static_cast<RpcMessageType>(buf[8]);
            uint32_t origin_crc = crc32_gzip_refl(
                0, (const uint8_t*)(buf.get() + 4), kRpcHeaderSize - 4);
            if (crc != origin_crc) {
                s.Set(ErrCode::ErrInvalidChecksum);
                co_await stream_ptr->Close();
                co_return s;
            }
            if (len != 0 || type != RpcMessageType::Snapshot) {
                s.Set(EBADMSG);
                co_await stream_ptr->Close();
                co_return s;
            }
        }
    }

    co_await stream_ptr->Close();
    co_return s;
}

seastar::future<> RaftSender::Client::Close() {
    if (!gate.is_closed()) {
        co_await gate.close();
        co_await client->Close();
    }
    co_return;
}

void RaftSender::AddRaftNode(uint64_t node_id, const std::string& raft_host,
                             uint16_t raft_port) {
    if (senders_.count(node_id)) {
        LOG_WARN("raft node(id={}, host={}, port={}) has already exist",
                 node_id, raft_host, raft_port);
        return;
    }

    RaftSender::ClientPtr client = seastar::make_lw_shared<RaftSender::Client>(
        node_id, raft_host, raft_port);
    senders_[node_id] = client;
    return;
}

seastar::future<> RaftSender::RemoveRaftNode(uint64_t node_id) {
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    auto iter = senders_.find(node_id);
    if (iter == senders_.end()) {
        co_return;
    }
    RaftSender::ClientPtr client = iter->second;
    senders_.erase(iter);
    co_await client->Close();
}

seastar::future<> RaftSender::UpdateRaftNodes(std::vector<RaftNode> nodes) {
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    std::unordered_map<uint64_t, ClientPtr> senders;
    for (int i = 0; i < nodes.size(); ++i) {
        auto iter = senders_.find(nodes[i].id());
        if (iter == senders_.end()) {
            RaftSender::ClientPtr client =
                seastar::make_lw_shared<RaftSender::Client>(
                    nodes[i].id(), nodes[i].raft_host(), nodes[i].raft_port());
            senders[nodes[i].id()] = client;
        } else {
            senders[nodes[i].id()] = iter->second;
        }
    }
    senders_.swap(senders);
    std::vector<seastar::future<>> fu_vec;
    for (auto it : senders) {
        if (!senders_.count(it.first)) {
            auto fu = it.second->Close();
            fu_vec.emplace_back(std::move(fu));
        }
    }
    if (!fu_vec.empty()) {
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    }
    co_return;
}

seastar::future<> RaftSender::Send(std::vector<raft::MessagePtr> msgs) {
    std::unordered_map<uint64_t, std::vector<Buffer>> node_msgs;
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    for (int i = 0; i < msgs.size(); i++) {
        if (!msgs[i] || msgs[i]->to == 0) {
            continue;
        }
        uint32_t len = msgs[i]->ByteSize();
        Buffer buf(len + kRpcHeaderSize);
        msgs[i]->MarshalTo(buf.get_write() + kRpcHeaderSize);
        net::BigEndian::PutUint32(buf.get_write() + 4, len);
        *(buf.get_write() + kRpcHeaderSize - 1) =
            static_cast<char>(RpcMessageType::Normal);
        uint32_t crc = crc32_gzip_refl(0, (const unsigned char*)(buf.get() + 4),
                                       buf.size() - 4);
        net::BigEndian::PutUint32(buf.get_write(), crc);

        auto iter = node_msgs.find(msgs[i]->to);
        if (iter == node_msgs.end()) {
            std::vector<Buffer> buffers;
            buffers.emplace_back(std::move(buf));
            node_msgs[msgs[i]->to] = std::move(buffers);
        } else {
            iter->second.emplace_back(std::move(buf));
        }
        co_await seastar::coroutine::maybe_yield();
    }

    std::vector<seastar::future<>> fu_vec;
    for (auto& it : node_msgs) {
        auto iter = senders_.find(it.first);
        if (iter == senders_.end()) {
            continue;
        }
        auto fu = iter->second->Send(std::move(it.second));
        fu_vec.emplace_back(std::move(fu));
    }
    if (fu_vec.size()) {
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    }
    co_return;
}

seastar::future<Status<>> RaftSender::SendSnapshot(raft::MessagePtr msg,
                                                   SmSnapshotPtr body) {
    Status<> s;
    if (msg->type != raft::MessageType::MsgSnap || !msg->snapshot) {
        s.Set(EINVAL);
        co_return s;
    }
    if (gate_.is_closed()) {
        s.Set(EPIPE);
        co_return s;
    }
    seastar::gate::holder holder(gate_);

    auto iter = senders_.find(msg->to);
    if (iter == senders_.end()) {
        s.Set(EEXIST);
        co_return s;
    }
    s = co_await iter->second->SendSnapshot(msg, body);
    co_return s;
}

seastar::future<> RaftSender::Close() {
    if (!gate_.is_closed()) {
        std::vector<seastar::future<>> fu_vec;
        auto fu = gate_.close();
        fu_vec.emplace_back(std::move(fu));
        auto senders = std::move(senders_);
        for (auto& iter : senders) {
            auto ft = iter.second->Close();
            fu_vec.emplace_back(std::move(ft));
        }
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    }
}

RaftReceiver::RaftReceiver(
    const std::string& host, uint16_t port,
    seastar::noncopyable_function<void(raft::MessagePtr msg)> msg_func,
    seastar::noncopyable_function<
        seastar::future<Status<>>(raft::SnapshotPtr meta, SmSnapshotPtr body)>
        apply_snapshot_func)
    : host_(host),
      port_(port),
      msg_handle_func_(std::move(msg_func)),
      apply_snapshot_func_(std::move(apply_snapshot_func)) {}

seastar::future<> RaftReceiver::HandleStream(net::StreamPtr stream) {
    std::optional<RpcMessageType> prev_type;
    raft::MessagePtr snapshot_msg = nullptr;
    seastar::shared_ptr<ReceivedSnapshot> snapshot_body = nullptr;
    std::optional<seastar::future<Status<>>> apply_snapshot_fu;

    seastar::gate::holder holder(gate_);

    for (;;) {
        auto s = co_await stream->ReadFrame();
        if (!s) {
            break;
        }
        Buffer b = std::move(s.Value());
        if (b.size() < kRpcHeaderSize) {
            LOG_ERROR("recv invalid raft message from {}, msg is too short",
                      stream->RemoteAddress());
            break;
        }
        uint32_t crc = net::BigEndian::Uint32(b.get());
        uint32_t len = net::BigEndian::Uint32(b.get() + 4);
        RpcMessageType type =
            static_cast<RpcMessageType>(*(b.get() + kRpcHeaderSize - 1));

        uint32_t origin_crc =
            crc32_gzip_refl(0, (const uint8_t*)(b.get() + 4), b.size() - 4);

        if (b.size() - kRpcHeaderSize < len) {
            // invalid msg
            LOG_ERROR("recv invalid raft message from {}, len is too short",
                      stream->RemoteAddress());
            break;
        }
        uint32_t n = len - b.size() + kRpcHeaderSize;
        std::vector<Buffer> buffers;
        b.trim_front(kRpcHeaderSize);
        buffers.emplace_back(std::move(b));
        while (n) {
            s = co_await stream->ReadFrame();
            if (!s) {
                LOG_ERROR("recv raft message from {} error: {}",
                          stream->RemoteAddress(), s);
                break;
            }
            b = std::move(s.Value());
            if (b.size() > len) {
                LOG_ERROR(
                    "recv invalid raft message from {}, msg is too larger",
                    stream->RemoteAddress());
                s.Set(EINVAL);
                break;
            }
            origin_crc = crc32_gzip_refl(origin_crc, (const uint8_t*)(b.get()),
                                         b.size());
            n -= b.size();
            buffers.emplace_back(std::move(b));
        }
        if (!s) {
            break;
        }

        if (origin_crc != crc) {
            LOG_ERROR("recv invalid raft message from {}, invalid crc",
                      stream->RemoteAddress());
            break;
        }
        if (!prev_type) {
            prev_type = type;
        }
        if (type != *prev_type) {
            LOG_ERROR("recv invalid raft message from {}, invalid type",
                      stream->RemoteAddress());
            break;
        }

        if (buffers.size() > 1) {
            Buffer tmp(len);
            char* p = tmp.get_write();
            for (int i = 0; i < buffers.size(); ++i) {
                memcpy(p, buffers[i].get(), buffers[i].size());
                p += buffers[i].size();
            }
            b = std::move(tmp);
        } else {
            b = std::move(buffers[0]);
        }

        if (type == RpcMessageType::Snapshot) {
            if (snapshot_body) {
                if (apply_snapshot_fu.value().available()) {
                    auto st = co_await std::move(apply_snapshot_fu.value());
                    if (!st) {
                        LOG_ERROR("apply raft snapshot error: {}", st);
                        break;
                    }
                }
                auto st = co_await snapshot_body->Write(b.share());
                if (!st) {
                    LOG_ERROR("apply raft snapshot error: {}", st);
                    break;
                }
                if (b.empty()) {
                    st = co_await std::move(apply_snapshot_fu.value());
                    if (!st) {
                        LOG_ERROR("apply raft snapshot error: {}", st);
                        break;
                    }
                    msg_handle_func_(snapshot_msg);

                    // send response
                    Buffer head(kRpcHeaderSize);
                    net::BigEndian::PutUint32(head.get_write() + 4, b.size());
                    *(head.get_write() + kRpcHeaderSize - 1) =
                        static_cast<char>(RpcMessageType::Snapshot);
                    uint32_t crc =
                        crc32_gzip_refl(0, (const uint8_t*)(head.get() + 4),
                                        kRpcHeaderSize - 4);
                    crc = crc32_gzip_refl(crc, (const uint8_t*)(b.get()),
                                          b.size());
                    net::BigEndian::PutUint32(head.get_write(), crc);
                    co_await stream->WriteFrame(std::move(head));
                    break;
                }
            } else {
                snapshot_msg = raft::make_raft_message();
                if (!snapshot_msg->Unmarshal(b.share()) ||
                    !snapshot_msg->snapshot) {
                    LOG_ERROR(
                        "recv invalid raft snapshot msg from {}, unmarshal "
                        "error",
                        stream->RemoteAddress());
                    break;
                }
                SnapshotMetaPayload payload;
                auto& data = snapshot_msg->snapshot->data();
                if (payload.ParseFromArray(data.get(), data.size())) {
                    LOG_ERROR(
                        "recv invalid raft snapshot msg from {}, parse "
                        "snapshot "
                        "payload error",
                        stream->RemoteAddress());
                    break;
                }
                seastar::sstring name(payload.name().data(),
                                      payload.name().size());
                uint64_t index = snapshot_msg->snapshot->metadata().index();
                snapshot_body =
                    seastar::make_shared<ReceivedSnapshot>(name, index);
                apply_snapshot_fu = apply_snapshot_func_(
                    snapshot_msg->snapshot,
                    seastar::dynamic_pointer_cast<SmSnapshot, ReceivedSnapshot>(
                        snapshot_body));
            }
        } else {
            raft::MessagePtr msg = raft::make_raft_message();
            if (!msg->Unmarshal(b.share())) {
                break;
            }
            msg_handle_func_(msg);
        }
    }
    co_await stream->Close();
    if (snapshot_body) {
        co_await snapshot_body->Close();
    }
    co_return;
}

seastar::future<> RaftReceiver::HandleSession(net::SessionPtr sess) {
    if (!gate_.is_closed()) {
        seastar::gate::holder holder(gate_);
        for (;;) {
            auto s = co_await sess->AcceptStream();
            if (!s) {
                LOG_ERROR("accept stream error: {}", s);
                break;
            }
            (void)HandleStream(s.Value());
        }
    }
    co_await sess->Close();
    sess_map_.erase(sess->ID());
    co_return;
}

seastar::future<> RaftReceiver::Start() {
    if (gate_.is_closed()) {
        co_return;
    }
    seastar::gate::holder holder(gate_);
    try {
        seastar::socket_address sa(seastar::ipv4_addr(host_, port_));
        seastar::listen_options opts;
        opts.reuse_address = true;
        opts.set_fixed_cpu(seastar::this_shard_id());
        listen_fd_ = seastar::engine().posix_listen(sa, opts);
    } catch (std::exception& e) {
        LOG_ERROR("start raft receiver ({}:{}) error: {}", host_, port_,
                  e.what());
        co_return;
    }

    for (;;) {
        std::tuple<seastar::pollable_fd, seastar::socket_address> ar;
        try {
            ar = co_await listen_fd_.accept();
        } catch (std::exception& e) {
            LOG_ERROR("raft receiver ({}:{}) accept error: {}", host_, port_,
                      e.what());
            break;
        }
        auto conn = snail::net::TcpConnection::make_connection(
            std::move(std::get<0>(ar)), std::get<1>(ar));
        snail::net::Option opt;
        auto sess = snail::net::TcpSession::make_session(opt, conn, false);
        sess_map_[sess->ID()] = sess;
        (void)HandleSession(sess);
    }
    co_return;
}

seastar::future<> RaftReceiver::Close() {
    if (!gate_.is_closed()) {
        std::vector<seastar::future<>> fu_vec;
        listen_fd_.close();
        auto ft = gate_.close();
        fu_vec.emplace_back(std::move(ft));
        for (auto& iter : sess_map_) {
            auto fu = iter.second->Close();
            fu_vec.emplace_back(std::move(fu));
        }
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    }
    co_return;
}

}  // namespace stream
}  // namespace snail
