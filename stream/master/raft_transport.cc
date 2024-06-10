#include "raft_transport.h"

namespace snail {
namespace stream {

enum class RpcMessageType {
    Normal = 0,
    Snapshot = 1,
};

//| 4 bytes | 4 bytes |1 bytes|
//|   crc   |  len    |type|
static constexpr uint32_t kRpcHeaderSize = 9;

RaftSender::Client::Client(uint64_t id, const std::string& raft_host,
                           uint16_t raft_port)
    : node_id(id), host(raft_host), port(raft_port) {}

seastar::future<Status<>> RaftSender::Client::Connect() {
    Status<> s;
    co_await sess_mutex.lock();
    seastar::defer defer([this] { sess_mutex.unlock(); });

    if (sess && sess->GetStatus()) {
        co_return s;
    }
    if (sess) {
        co_await sess->Close();
    }
    seastar::timer<seastar::steady_clock_type> timer;
    try {
        net::Option opt;
        opt.max_frame_size = net::kMaxFrameSize;
        seastar::socket_address sa(seastar::ipv4_addr(host, port));
        auto fd = seastar::engine().make_pollable_fd(sa, 0);
        timer.set_callback([&fd] { fd.close(); });
        timer.arm(std::chrono::seconds(opt.write_timeout_s));
        co_await seastar::engine().posix_connect(fd, sa,
                                                 seastar::socket_address());
        arm.cancel();
        auto conn = net::TcpConnection::make_connection(std::move(fd), sa);
        sess = net::TcpSession::make_session(opt, conn, true);
    } catch (std::system_error& e) {
        arm.cancel();
        s.Set(e.code().value(), e.what());
        LOG_ERROR("connect raft node(id={} host={} port={}) error: {}", node_id,
                  host, port, s);
        stream.reset();
        co_return s;
    } catch (std::exception& e) {
        arm.cancel();
        s.Set(ErrCode::ErrUnExpect, e.what());
        LOG_ERROR("connect raft node(id={} host={} port={}) error: {}", node_id,
                  host, port, s);
        stream.reset();
        co_return s;
    }
    co_return s;
}

seastar::future<> RaftSender::Client::Send(std::vector<Buffer> buffers) {
    auto s = co_await Connect();
    if (!s) {
        co_return;
    }

    if (!stream || !stream->Valid()) {
        if (stream) {
            co_await stream->Close();
        }
        auto st = co_await sess->OpenStream();
        if (!st) {
            co_return;
        }
        stream = st.Value();
    }
    uint32_t max_frame_size = stream->MaxFrameSize();
    std::vector<seastar::future<Status<>>> fu_vec;
    for (int i = 0; i < buffers.size(); ++i) {
        uint32_t n = 0;
        for (const char* p = buffer[i].get(); p < buffer[i].end(); p += n) {
            n = std::min(buffer[i].end() - p, max_frame_size);
            auto fu = stream->WriteFrame(p, n);
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
            co_await stream->Close();
            break;
        }
    }
    co_return;
}

seastar::future<Status<>> RaftSender::Client::SendSnapshot(SnapshotPtr snap,
                                                           SmSnapshotPtr body) {
    Status<> s;
    net::StreamPtr stream_ptr;
    s = co_await Connect();
    if (!s) {
        co_return s;
    }

    auto st = co_await sess_ptr->OpenStream();
    if (!st) {
        LOG_ERROR("open stream error: {}", st);
        s.Set(st.Code(), st.Reason());
        co_return s;
    }
    stream_ptr = st.Value();

    uint32_t n = snap->ByteSize();
    Buffer b(n + kRpcHeaderSize);
    snap->MarshalTo(b.get_write() + kRpcHeaderSize);
    net::BigEndian::PutUint32(b.get_write() + 4, n);
    *(b.get_write() + kRpcHeaderSize - 1) = RpcMessageType::Snapshot;
    uint32_t crc =
        crc32_gzip_refl(0, (const uint8_t*)(b.get() + 4), b.size() - 4);
    net::BigEndian::PutUint32(b.get_write(), crc);

    std::optional<seastar::future<Status<>>> fu;
    fu = stream_ptr->WriteFrame(std::move(b));

    std::vector<Buffer> buffers;
    for (;;) {
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
        *(head.get_write() + kRpcHeaderSize - 1) = RpcMessageType::Snapshot;
        uint32_t crc = crc32_gzip_refl(0, (const uint8_t*)(head.get() + 4),
                                       kRpcHeaderSize - 4);
        crc = crc32_gzip_refl(crc, (const uint8_t*)(b.get()), b.size());
        net::BigEndian::PutUint32(head.get_write(), crc);
        buffers.emplace_back(std::move(head));
        buffers.emplace_back(std::move(b));
        if (fu) {
            s = co_await std::move(fu.value());
            if (!s) {
                LOG_ERROR(
                    "send raft snapshot {} to raft node(id={}, raft_host={} "
                    "raft_port={}) error: {}",
                    body->Name(), node_id, host, port, s);
                fu.reset();
                break;
            }
            fu.reset();
        }
        fu = stream_ptr->WriteFrame(std::move(buffers));
    }

    if (fu) {
        s = co_await std::move(fu.value());
        if (!s) {
            LOG_ERROR(
                "send raft snapshot {} to raft node(id={}, raft_host={} "
                "raft_port={}) error: {}",
                body->Name(), node_id, host, port, s);
        }
    }

    if (s) {
        // sending empty data means that the snapshot has ended
        Buffer head(kRpcHeaderSize);
        net::BigEndian::PutUint32(head.get_write() + 4, 0);
        *(head.get_write() + kRpcHeaderSize - 1) = RpcMessageType::Snapshot;
        uint32_t crc = crc32_gzip_refl(0, (const uint8_t*)(head.get() + 4),
                                       kRpcHeaderSize - 4);
        net::BigEndian::PutUint32(head.get_write(), crc);
        s = co_await stream_ptr->WriteFrame(std::move(head));
        if (!s) {
            LOG_ERROR(
                "send raft snapshot {} to raft node(id={}, raft_host={} "
                "raft_port={}) error: {}",
                body->Name(), node_id, host, port, s);
        }
    }

    co_await stream_ptr->Close();
    co_return s;
}

seastar::future<> RaftSender::Client::Close() {
    if (sess) {
        co_await sess->Close();
        sess.reset();
    }
    co_return;
}

RaftSender::AddRaftNode(uint64_t node_id, const std::string& raft_host,
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
    seastar::holder holder(gate_);
    auto iter = senders_.find(node_id);
    if (iter == senders_.end()) {
        return;
    }
    RaftSender::ClientPtr client = iter->second;
    senders_.erase(iter);
    co_await client->Close();
}

seastar::future<> RaftSender::Send(std::vector<MessagePtr> msgs) {
    std::unordered_map<uint64_t, std::vector<Buffer>> node_msgs;
    seastar::holder holder(gate_);
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

    std::vector<seatar::future<>> fu_vec;
    for (auto& it : node_msgs) {
        auto iter = sender_.find(it.first);
        if (iter == sender_.end()) {
            continue;
        }
        auto fu = iter->second.Send(std::move(it.second));
        fu_vec.emplace_back(std::move(fu));
    }
    if (fu_vec.size()) {
        co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    }
    co_return;
}

seastar::future<Status<>> RaftSender::SendSnapshot(uint64_t to,
                                                   SnapshotPtr snap,
                                                   SmSnapshotPtr body) {
    Status<> s;
    seastar::holder holder(gate_);

    auto iter = sender_.find(to);
    if (iter == sender_.end()) {
        s.Set(EEXIST);
        co_return s;
    }
    s = co_await iter->second->SendSnapshot(snap, body);
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
    seastar::nocopyable_function<seastar::future<Status<>>(Buffer b)> msg_func,
    seastar::nocopyable_function<seastar::future<Status<>>(Buffer b)> snap_func)
    : host_(host),
      port_(port),
      msg_handle_func_(std::move(msg_func)),
      snap_handle_func_(std::move(snap_func)) {}

seastar::future<> RaftReceiver::HandleStream(net::StreamPtr stream) {
    seastar::holder holder(gate_);
    std::optional<bool> handle_snap;
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
        if (!handle_snap) {
            handle_snap = (type == RpcMessageType::Snapshot);
        } else if (*handle_snap != (type == RpcMessageType::Snapshot)) {
            break;
        }

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

        Status<> st;
        if (*handle_snap) {
            st = co_await snap_handle_func_(std::move(b));
        } else {
            st = co_await msg_handle_func_(std::move(b));
        }
        if (!st) {
            break;
        }
    }
    co_await stream->Close();
    co_return;
}

seastar::future<> RaftReceiver::HandleSession(net::SessionPtr sess) {
    seastar::holder holder(gate_);
    for (;;) {
        auto s = co_await sess->AcceptStream();
        if (!s) {
            LOG_ERROR("accept stream error: {}", s);
            break;
        }
        (void)HandleStream(s.Value());
    }
    co_await sess->Close();
    sess_map_.erase(sess->ID());
    co_return;
}

seastar::future<> RaftReceiver::Start() {
    seastar::holder holder(gate_);
    try {
        seastar::socket_address sa(seastar::ipv4_addr(host_, port_));
        listen_fd_ = seastar::engine().posix_listen(sa);
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
        opt.max_fram_size = net::kMaxFrameSize;
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
