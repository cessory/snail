#include "session.h"

namespace snail {
namespace net {

static const size_t default_accept_backlog = 1024;

Session::Session(const Option &opt, seastar::connected_socket conn, bool client)
    : opt_(opt),
      conn_(std::move(conn)),
      next_id_(0),
      sequence_(0),
      bucket_sem_(opt.max_receive_buffer),
      die_(false),
      accept_sem_(default_accept_backlog),
      keepalive_timer_(nullptr) {}

seastar::future<> Session::RecvLoop() {
  auto ptr = shared_from_this();
  while (!die_) {
    auto s = co_await conn_->ReadExactly(HEADER_SIZE);
    if (!s.OK()) {
      read_error_.Set(s.Code(), s.Reason());
      break;
    }
    auto hdr = s.Value();
    uint8_t ver = hdr[0];
    CmdType cmd = static_cast<CmdType>(hdr[1]);
    uint32_t sid = LittleEndian::Uint32(hdr.get() + 4);
    uint16_t len = LittleEndian::Uint16(hdr.get() + 2);

    switch (cmd) {
      case CmdType::NOP:
        break;
      case CmdType::SYN: {
        auto it = streams_.find(sid);
        if (it == streams_.end()) {
          StreamPtr stream =
              seastar::make_lw_shared(sid, opt_.frame_size, shared_from_this());
          streams_[sid] = stream;
          try {
            co_await accept_sem_.wait();
          } catch (std::exception &e) {
            co_return;
          }
          accept_q_.push(stream);
        }
        break;
      }
      case CmdType::FIN: {
        auto it = streams_.find(sid);
        if (it != streams_.end()) {
          auto stream = it->second;
          stream->Fin();
        }
        break;
      }
      case CmdType::PSH:
        if (len > 0) {
          auto s = co_await conn_->ReadExactly(len);
          if (!s.OK()) {
            read_error_.Set(s.Code(), s.Reason());
            NotifyError();
            co_return;
          }
          auto it = streams_.find(sid);
          if (it != streams_.end()) {
            auto stream = it->second;
            try {
              co_await bucket_sem_.wait(len);
            } catch (std::exception &e) {
              co_return;
            }
            auto data = s.Value();
            stream->PushData(std::move(data));
          }
        }
        break;
      case CmdType::UPD:
      default:
        proto_error_.Set(ErrCode::ErrInvalidProtocol);
        NotifyError();
        co_return;
    }
  }
  co_return;
}

seastar::future<> Session::SendLoop() {
  auto ptr = shared_from_this();
  while (!die_) {
    Status<> s(ErrCode::OK);
    std::queue<Session::write_request *> sent_q;
    seastar::net::packet packet;

    for (int i = 0; i < write_q_.size(); i++) {
      while (!write_q_[i].empty()) {
        auto req = write_q_[i].front();
        write_q_[i].pop();
        sent_q.push(req);
        char header[HEADER_SIZE];
        header[0] = req->frame.ver;
        header[1] = req->frame.cmd;
        LittleEndian::PutUint16(&header[2], (uint16_t)(req->frame.data.size()));
        LittleEndian::PutUint32(&header[4], req->frame.sid);
        seastar::net::packet header(header, HEADER_SIZE);
        seastar::net::packet body(std::move(req->frame.data));
        packet.append(std::move(header));
        packet.append(std::move(body));
      }
    }
    if (packet.len() > 0) {
      size_t nr = 0;
      s = co_await conn_->Write(std::move(packet));
      if (s.OK()) {
        s = co_await conn_->Flush();
      }

      while (!sent_q.empty()) {
        auto req = sent_q.front();
        sent_q.pop();
        if (req->pr.has_value()) {
          req->pr.value().set_value(s);
        } else {
          delete req;
        }
        nr++;
      }

      if (!s.OK()) {
        write_error_ = s;
        break;
      }
    }

    co_await w_cv_.wait();
  }
  co_return;
}

void Session::StartKeepalive() {
  if (keepalive_timer_ == nullptr) {
    keepalive_timer_ = new seastar::timer<seastar::steady_clock_type>(
        [this]() { WritePing(); });
  }
}

ErrCode Session::StatusCode() {
  if (!read_error_.OK()) {
    return read_error_.Code();
  }

  if (!write_error_.OK()) {
    return write_error_.Code();
  }

  if (!proto_error_.OK()) {
    return proto_error_.Code();
  }

  if (die_) {
    return ErrCode::ErrClosedPipe;
  }

  return ErrCode::OK;
}

void Session::NotifyError() {
  if (!shared_pr_.available()) {
    shared_pr_.set_value();
  }
}

seastar::future<Status<>> Session::WriteFrameInternal(Frame f,
                                                      ClassID classid) {
  if (die_) {
    co_return Status<>(ErrCode::ErrClosedPipe);
  }
  if (!error_.OK()) {
    co_return Status<>(error_);
  }

  write_request *req = new write_request();

  req->classid = classid;
  req->frame = std::move(f);
  req->seq = ++sequence_;
  req->pr = seastar::promise<Status<>>();

  write_q_[static_cast<int>(classid)].push(req);
  w_cv_.signal();
  auto s = co_await req->pr.value().get_future();
  co_return s;
}

void Session::WritePing() {
  if (die_) {
    co_return Status<>(ErrCode::ErrClosedPipe);
  }
  if (!error_.OK()) {
    co_return Status<>(error_);
  }

  write_request *req = new write_request();
  req->classid = classid;
  req->frame = std::move(f);
  req->seq = ++sequence_;

  write_q_[static_cast<int>(classid)].push(req);
  w_cv_.signal();
  return;
}

}  // namespace net
}  // namespace snail
