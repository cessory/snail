#include "service.h"

namespace snail {
namespace stream {

static constexpr size_t kMsgHeaderLen = 4 + 2 + 2;  // crc + type + body_len

static seastar::future<Status<>> SendResp(net::StreamPtr s, uint64_t reqid,
                                          DatanodeMsgType type,
                                          google::protobuf::Message *msg) {
    Status<> st;
    size_t len = msg->ByteSizeLong();
    if (len + kMsgHeaderLen > kBlockSize) {
        LOG_ERROR("[{}] response msg len is too long, len={}", reqid, len);
        st.Set(EMSGSIZE);
        co_return st;
    }
    seastar::temporary_buffer<char> buf(kMsgHeaderLen + len);
    net::BigEndian::PutUint16(buf.get_write() + 4, static_cast<uint16_t>(type));
    net::BigEndian::PutUint16(buf.get_write() + 6, static_cast<uint16_t>(len));
    if (!msg->SerializeToArray(buf.get_write() + kMsgHeaderLen, len)) {
        LOG_ERROR("[{}] serialize msg error, type={}", reqid,
                  static_cast<int>(type));
        st.Set(EBADMSG);
        co_return st;
    }
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(buf.get() + 4),
        kMsgHeaderLen + len - 4);
    net::BigEndian::PutUint32(buf.get_write(), crc);
    st = co_await s->WriteFrame(buf.get(), buf.size());
    if (!st) {
        LOG_ERROR("[{}] send msg error: {}", reqid, st.String());
    }
    co_return st;
}

seastar::future<Status<>> Service::Write(const WriteExtentReq *req,
                                         net::StreamPtr s) {
    Status<> st;
    std::unique_ptr<CommonResp> resp(new CommonResp());
    uint64_t reqid = req->base().reqid();
    resp->set_reqid(reqid);
    if (req->diskid() != store_->DeviceId()) {
        LOG_ERROR("{} diskid={} is not matched, our diskid is {}", reqid,
                  req->diskid(), store_->DeviceId());
        st.Set(ErrCode::ErrDiskNotMatch);
        resp->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                          resp.get());
        co_return st;
    }
    const std::string &extent_id = req->extent_id();
    if (extent_id.size() != sizeof(ExtentID)) {
        LOG_ERROR("{} invalid extent_id", reqid, extent_id);
        st.Set(EINVAL);
        resp->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                          resp.get());
        co_return st;
    }
    ExtentID eid(net::BigEndian::Uint64(extent_id.c_str()),
                 net::BigEndian::Uint64(extent_id.c_str() + 8));
    auto extent_ptr = store_->GetExtent(eid);
    if (!extent_ptr) {
        LOG_ERROR("{} not found extent, extent_id={}-{}", reqid, eid.hi,
                  eid.lo);
        st.Set(ErrCode::ErrNoExtent);
        resp->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                          resp.get());
        co_return st;
    }

    if (!extent_ptr->mu.try_lock()) {
        LOG_ERROR("{} lock extent failed, extent_id={}-{} has been writing",
                  reqid, eid.hi, eid.lo);
        st.Set(ErrCode::ErrParallelWrite);
        resp->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                          resp.get());
        co_return st;
    }

    seastar::defer defer([extent_ptr] { extent_ptr->mu.unlock(); });

    uint64_t off = req->off();
    uint64_t next_off = off;
    uint64_t len = req->len();
    static int max_frame_num = 8;
    std::vector<TmpBuffer> buffer_vec;
    std::vector<iovec> iov;
    std::vector<seastar::future<Status<>>> fu_vec;
    while (len > 0) {
        for (int i = 0; i < max_frame_num && len > 0; i++) {
            auto st1 = co_await s->ReadFrame();
            if (!st1) {
                LOG_ERROR("{} extent_id={}-{} recv frame error: {}", reqid,
                          eid.hi, eid.lo, s.String());
                st.Set(st1.Code(), st1.Reason());
                resp->set_code(static_cast<int32_t>(st.Code()));
                co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                                  resp.get());
                co_return st;
            }

            auto &buf = s.Value();
            if (buf.size() <= 4 || buf.size() > len) {
                LOG_ERROR(
                    "{} extent_id={}-{} recv invalid frame error: illegal data "
                    "len",
                    reqid, eid.hi, eid.lo);
                st.Set(EINVAL);
                resp->set_code(static_cast<int32_t>(s.Code()));
                co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                                  resp.get());
                co_return st;
            }

            uint32_t crc = net::BigEndian::Uint32(buf.get() + buf.size() - 4);
            if (crc != crc32_gzip_refl(
                           0,
                           reinterpret_cast<const unsigned char *>(buf.get()),
                           buf.size() - 4)) {
                LOG_ERROR(
                    "{} extent_id={}-{} recv invalid frame error: checksum "
                    "error",
                    reqid, eid.hi, eid.lo);
                st.Set(ErrCode::ErrInvalidChecksum);
                resp->set_code(static_cast<int32_t>(s.Code()));
                co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                                  resp.get());
                co_return st;
            }
            len -= buf.size();
            next_off += buf.size() - 4;
            buffer_vec.emplace_back(std::move(buf));
            iov.push_back({buf.get_write(), buf.size()});
        }
        st = co_await store_->WriteBlocks(extent_ptr, off, std::move(iov));
        if (!st) {
            LOG_ERROR("{} extent_id={}-{} write error: {}", reqid, eid.hi,
                      eid.lo, st.String());
            resp->set_code(static_cast<int32_t>(st.Code()));
            co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                              resp.get());
            co_return st;
        }
        off = next_off;
        buffer_vec.clear();
    }
    resp->set_code(static_cast<int32_t>(ErrCode::OK));
    st = co_await SendResp(s, reqid, DatanodeMsgType::WRITE_EXTENT_RESP,
                           resp.get());
    co_return st;
}

seastar::future<Status<>> Service::ReadExtent(const ReadExtentReq *req,
                                              net::StreamPtr s) {
    Status<> st;
    std::unique_ptr<ReadExtentResp> resp(new ReadExtentResp());
    uint64_t reqid = req->base().reqid();
    resp->mutable_base()->set_reqid(reqid);
    if (req->diskid() != store_->DeviceId()) {
        LOG_ERROR("{} diskid={} is not matched, our diskid is {}", reqid,
                  req->diskid(), store_->DeviceId());
        st.Set(ErrCode::ErrDiskNotMatch);
        resp->mutable_base()->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::READ_EXTENT_RESP,
                          resp.get());
        co_return st;
    }
    const std::string &extent_id = req->extent_id();
    if (extent_id.size() != sizeof(ExtentID)) {
        LOG_ERROR("{} invalid extent_id", reqid, extent_id);
        st.Set(EINVAL);
        resp->mutable_base()->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::READ_EXTENT_RESP,
                          resp.get());
        co_return st;
    }
    ExtentID eid(net::BigEndian::Uint64(extent_id.c_str()),
                 net::BigEndian::Uint64(extent_id.c_str() + 8));
    auto extent_ptr = store_->GetExtent(eid);
    if (!extent_ptr) {
        LOG_ERROR("{} not found extent, extent_id={}-{}", reqid, eid.hi,
                  eid.lo);
        st.Set(ErrCode::ErrNoExtent);
        resp->mutable_base()->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::READ_EXTENT_RESP,
                          resp.get());
        co_return st;
    }

    uint64_t off = req->off();
    uint64_t len = req->len();
    if (off >= extent_ptr->len) {
        resp->mutable_base()->set_code(static_cast<int32_t>(ErrCode::OK));
        resp->set_len(0);
        st = co_await SendResp(s, reqid, DatanodeMsgType::READ_EXTENT_RESP,
                               resp.get());
        co_return st;
    }
    if (off + len > extent_ptr->len) {
        len = extent_ptr->len - off;
    }
    resp->mutable_base()->set_code(static_cast<int32_t>(ErrCode::OK));
    resp->set_len(len);
    st = co_await SendResp(s, reqid, DatanodeMsgType::READ_EXTENT_RESP,
                           resp.get());
    if (!st) {
        co_return st;
    }
    uint64_t aligned_off = off / kBlockDataSize * kBlockDataSize;
    len += off - aligned_off;
    int block_n = (len + kBlockDataSize - 1) / kBlockDataSize;
    for (int i = 0; i < block_n; i++) {
        std::vector<seastar::future<Status<>>> fu_vec;
        std::vector<TmpBuffer> buffer_vec;
        uint64_t bytes =
            std::min(kChunkDataSize - aligned_off % kChunkDataSize, len);
        st = co_await store_->ReadBlocks(
            extent_ptr, aligned_off, bytes,
            [s,
             &buffer_vec](std::vector<seastar::temporary_buffer<char>> buf_vec)
                -> Status<> {
                buffer_vec = std::move(buf_vec);
                return Status<>();
            });
        if (!st) {
            LOG_ERROR("{} read extent_id={}-{} error: {}", reqid, eid.hi,
                      eid.lo, st.String());
            co_return st;
        }
        std::vector<TmpBuffer> tmp_buffer_vec;
        for (int i = 0; i < buffer_vec.size(); i++) {
            std::vector<iovec> iov;
            if (i == 0 && aligned_off != off) {
                auto buf = buffer_ver[i].share(
                    off - aligned_off,
                    buffer_vec[i].size() + aligned_off - off);
                iov.push_back({buf.get_write(), buf.size()});
                uint32_t crc = crc32_gzip_refl(
                    0, reinterpret_cast<const unsigned char *>(buf.get()),
                    buf.size());
                auto tmp_buf = seastar::temporary_buffer<char>(4);
                net::BigEndian::PutUint32(tmp_buf.get_write(), crc);
                iov.push_back({tmp_buf.get_write(), tmp_buf.size()});
                tmp_buffer_vec.emplace_back(std::move(tmp_buf));
            } else {
                iov.push_back(
                    {buffer_vec[i].get_write(), buffer_vec[i].size()});
            }
            auto ft = s->WriteFrame(std::move(iov));
            fu_vec.emplace_back(std::move(ft));
        }
        auto res =
            co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
        for (int i = 0; i < res.size(); i++) {
            if (!res[i]) {
                st = res[i];
                co_return st;
            }
        }
        len -= bytes;
        aligned_off += bytes;
    }
    co_return st;
}

seastar::future<Status<>> Service::DeleteExtent(const DeleteExtentReq *req,
                                                net::StreamPtr s) {
    Status<> st;
    std::unique_ptr<CommonResp> resp(new CommonResp());
    uint64_t reqid = req->base().reqid();
    resp->set_reqid(reqid);
    if (req->diskid() != store_->DeviceId()) {
        LOG_ERROR("{} diskid={} is not matched, our diskid is {}", reqid,
                  req->diskid(), store_->DeviceId());
        st.Set(ErrCode::ErrDiskNotMatch);
        resp->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::DELETE_EXTENT_RESP,
                          resp.get());
        co_return st;
    }
    const std::string &extent_id = req->extent_id();
    if (extent_id.size() != sizeof(ExtentID)) {
        LOG_ERROR("{} invalid extent_id={}", reqid, extent_id);
        st.Set(EINVAL);
        resp->set_code(static_cast<int32_t>(st.Code()));
        co_await SendResp(s, reqid, DatanodeMsgType::DELETE_EXTENT_RESP,
                          resp.get());
        co_return st;
    }
    ExtentID eid(net::BigEndian::Uint64(extent_id.c_str()),
                 net::BigEndian::Uint64(extent_id.c_str() + 8));
    st = co_await store_->RemoveExtent(eid);
    if (!st) {
        LOG_ERROR("{} delete extent={}-{} error: {}", reqid, eid.hi, eid.lo,
                  st.String());
    }
    resp->set_code(static_cast<int32_t>(st.Code()));
    co_await SendResp(s, reqid, DatanodeMsgType::DELETE_EXTENT_RESP,
                      resp.get());
    co_return st;
}

seastar::future<Status<>> Service::GetExtent(const GetExtentReq *req,
                                             net::StreamPtr s) {}

seastar::future<Status<>> Service::UpdateDiskStatus(
    const UpdateDiskStatusReq *req, net::StreamPtr s) {}

}  // namespace stream
}  // namespace snail
