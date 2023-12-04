#pragma once
#include "util/status.h"

namespace snail {
namespace net {

struct Option struct {
  int version = 1;
  bool keep_alive_disabled = false;
  int keep_alive_interval = 10;  // unit: s
  int keep_alive_timeout = 30;
  int max_frame_size = 65536;
  int max_receive_buffer = 4194304;
  int max_stream_buffer = 65536;
};

class Stream;

class Session : public enable_shared_from_this<Session> {
  Option opt_;
  ConnectionPtr conn_;
  uint32_t next_id_;
  uint32_t sequence_;

  seastar::semaphore bucket_sem_;
  std::unordered_map<uint32_t, StreamPtr> streams_;
  bool die_;
  seastar::shared_promise<> shared_pr_;

  Status<> error_;

  std::queue<StreamPtr> accept_q_;
  seastar::semaphore accept_sem_;

  enum ClassID {
    CTRL = 0,
    DATA = 1,
  };

  struct write_request {
    ClassID classid;
    Frame frame;
    uint32_t seq;
    std::optional<seastar::promise<Status<>>> pr;
  };

  std::array<std::queue<write_request *>, 2> write_q_;
  seastar::condition_variable w_cv_;
  seastar::timer<seastar::steady_clock_type> *keepalive_timer_;

 private:
  seastar::future<> RecvLoop();
  seastar::future<> SendLoop();

  void StartKeepalive();

  void ReturnTokens(uint32_t n) { bucket_sem_.signal(n); }

  seastar::future<> Wait() { return shared_pr_.get_shared_future(); }

  ErrCode StatusCode();

  void NotifyError();

  seastar::future<Status<>> WriteFrameInternal(Frame f, ClassID classid);

  void WritePing();

  friend class Stream;

 public:
  explicit Session(const Option &opt, ConnectionPtr conn, bool client);

  ~Session();

  seastar::future<Status<StreamPtr>> OpenStream();

  seastar::future<Status<StreamPtr>> AcceptStream();

  seastar::future<> Close();
};

}  // namespace net
}  // namespace snail
