#include "service.h"

#include <isa-l.h>

#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>
#include <string_view>

#include "proto/common.pb.h"
#include "proto/extentnode.pb.h"
#include "util/logger.h"

namespace snail {
namespace stream {

static seastar::future<Status<>> SendResp(
    const ::google::protobuf::Message *resp, ExtentnodeMsgType msgType,
    net::StreamPtr stream) {
    size_t n = resp->ByteSizeLong();
    TmpBuffer buf(n + kMetaMsgHeaderLen);
    resp->SerializeToArray(buf.get_write() + kMetaMsgHeaderLen, n);
    net::BigEndian::PutUint16(buf.get_write() + 4, (uint16_t)msgType);
    net::BigEndian::PutUint16(buf.get_write() + 6, (uint16_t)n);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char *>(buf.get() + 4),
        n + kMetaMsgHeaderLen - 4);
    net::BigEndian::PutUint32(buf.get_write(), crc);
    auto s = co_await stream->WriteFrame(buf.get(), buf.size());
    co_return s;
}

static seastar::future<Status<>> SendCommonResp(
    net::StreamPtr stream, std::string_view reqid, ExtentnodeMsgType msgType,
    ErrCode err, std::string reason,
    std::map<std::string, std::string> headers) {
    std::unique_ptr<::google::protobuf::Message> resp;
    Status<> s;

    switch (msgType) {
        case WRITE_EXTENT_RESP:
        case UPDATE_DISK_STATUS_RESP:
        case CREATE_EXTENT_RESP:
        case DELETE_EXTENT_RESP: {
            std::unique_ptr<CommonResp> ptr(new CommonResp());
            ptr->set_reqid(std::string(reqid.data(), reqid.size()));
            ptr->set_code((int)err);
            ptr->set_reason(reason);
            ptr->mutable_headers()->insert(headers.begin(), headers.end());
            resp = std::move(ptr);
            break;
        }
        case READ_EXTENT_RESP: {
            std::unique_ptr<ReadExtentResp> ptr(new ReadExtentResp());
            ptr->mutable_base()->set_reqid(
                std::string(reqid.data(), reqid.size()));
            ptr->mutable_base()->set_code((int)err);
            ptr->mutable_base()->set_reason(reason);
            ptr->mutable_base()->mutable_headers()->insert(headers.begin(),
                                                           headers.end());
            resp = std::move(ptr);
            break;
        }
        case GET_EXTENT_RESP: {
            std::unique_ptr<GetExtentResp> ptr(new GetExtentResp());
            ptr->mutable_base()->set_reqid(
                std::string(reqid.data(), reqid.size()));
            ptr->mutable_base()->set_code((int)err);
            ptr->mutable_base()->set_reason(reason);
            ptr->mutable_base()->mutable_headers()->insert(headers.begin(),
                                                           headers.end());
            resp = std::move(ptr);
            break;
        }
        default:
            s.Set(EINVAL);
            break;
    }

    if (resp) {
        s = co_await SendResp(resp.get(), msgType, stream);
    }
    co_return s;
}

static seastar::future<Status<>> SendCommonResp(net::StreamPtr stream,
                                                std::string_view reqid,
                                                ExtentnodeMsgType msgType,
                                                ErrCode err) {
    return SendCommonResp(stream, reqid, msgType, err, "",
                          std::map<std::string, std::string>());
}

static seastar::future<Status<>> SendCommonResp(net::StreamPtr stream,
                                                std::string_view reqid,
                                                ExtentnodeMsgType msgType,
                                                ErrCode err,
                                                std::string reason) {
    auto s =
        co_await SendCommonResp(stream, reqid, msgType, err, std::move(reason),
                                std::map<std::string, std::string>());
    co_return s;
}

static seastar::future<Status<>> SendCommonResp(
    net::StreamPtr stream, std::string_view reqid, ExtentnodeMsgType msgType,
    ErrCode err, std::map<std::string, std::string> headers) {
    auto s = co_await SendCommonResp(stream, reqid, msgType, err, "",
                                     std::move(headers));
    co_return s;
}

static seastar::future<Status<>> SendReadExtentResp(net::StreamPtr stream,
                                                    std::string_view reqid,
                                                    uint64_t len) {
    std::unique_ptr<ReadExtentResp> resp(new ReadExtentResp);
    resp->mutable_base()->set_reqid(std::string(reqid.data(), reqid.size()));
    resp->mutable_base()->set_code(static_cast<int>(ErrCode::OK));
    resp->set_len(len);
    auto s = co_await SendResp(resp.get(), READ_EXTENT_RESP, stream);
    co_return s;
}

static seastar::future<Status<>> SendGetExtentResp(net::StreamPtr stream,
                                                   std::string_view reqid,
                                                   uint64_t len,
                                                   uint32_t ctime) {
    std::unique_ptr<GetExtentResp> resp(new GetExtentResp);
    resp->mutable_base()->set_reqid(std::string(reqid.data(), reqid.size()));
    resp->mutable_base()->set_code(static_cast<int>(ErrCode::OK));
    resp->set_len(len);
    resp->set_ctime(ctime);
    auto s = co_await SendResp(resp.get(), READ_EXTENT_RESP, stream);
    co_return s;
}

seastar::future<Status<>> Service::HandleWriteExtent(const WriteExtentReq *req,
                                                     net::StreamPtr stream) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    auto diskid = req->diskid();
    uint64_t off = req->off();
    uint64_t len = req->len();
    const std::string &eid_str = req->extent_id();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        s = co_await SendCommonResp(stream, reqid, WRITE_EXTENT_RESP,
                                    (ErrCode)EINVAL);
        co_return s;
    }

    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotMatch);
        s = co_await SendCommonResp(stream, reqid, WRITE_EXTENT_RESP, s.Code());
        co_return s;
    }

    auto extent_ptr = store_->GetExtent(extent_id);
    if (!extent_ptr) {
        s.Set(ErrCode::ErrNoExtent);
        s = co_await SendCommonResp(stream, reqid, WRITE_EXTENT_RESP, s.Code());
        co_return s;
    }

    if (!extent_ptr->mu.try_lock()) {
        s.Set(ErrCode::ErrParallelWrite);
        s = co_await SendCommonResp(stream, reqid, WRITE_EXTENT_RESP, s.Code());
        co_return s;
    }

    auto defer = seastar::defer([extent_ptr] { extent_ptr->mu.unlock(); });

    std::vector<TmpBuffer> buffers;
    std::optional<seastar::future<Status<>>> fu;
    uint64_t sent = 0;
    uint64_t ready = 0;

    while (len > 0) {
        auto st = co_await stream->ReadFrame();
        if (!st) {
            s.Set(st.Code(), st.Reason());
            co_return s;
        }
        auto b = std::move(st.Value());
        if (b.size() <= 4) {
            s.Set(EBADMSG);
            co_return s;
        }
        size_t data_len = 0;
        for (const char *p = b.get(); p < b.end(); p += kBlockSize) {
            size_t n = std::min(kBlockSize, (size_t)(b.end() - p));
            if (n <= 4) {
                s.Set(EBADMSG);
                co_return s;
            }
            data_len += n - 4;
            uint32_t crc = crc32_gzip_refl(0, (const unsigned char *)p, n - 4);
            uint32_t origin_crc = net::BigEndian::Uint32(p + n - 4);
            if (crc != origin_crc) {
                s.Set(ErrCode::ErrInvalidChecksum);
                co_return s;
            }
        }
        if (len < data_len) {
            s.Set(EBADMSG);
            co_return s;
        }
        len -= data_len;
        if (buffers.size() == 4) {
            if (fu) {
                s = co_await std::move(fu.value());
                if (!s) {
                    s = co_await SendCommonResp(stream, reqid,
                                                WRITE_EXTENT_RESP, s.Code());
                    co_return s;
                }
                off += sent;
                sent = 0;
                fu.reset();
            }
            fu = store_->Write(extent_ptr, off, std::move(buffers));
            sent = ready;
            ready = 0;
        }
        ready += data_len;
        buffers.emplace_back(std::move(b));
    }

    if (fu) {
        s = co_await std::move(fu.value());
        if (!s) {
            s = co_await SendCommonResp(stream, reqid, WRITE_EXTENT_RESP,
                                        s.Code());
            co_return s;
        }
        fu.reset();
        off += sent;
    }

    if (buffers.size() > 0) {
        s = co_await store_->Write(extent_ptr, off, std::move(buffers));
    }
    s = co_await SendCommonResp(stream, reqid, WRITE_EXTENT_RESP, s.Code());
    co_return s;
}

static seastar::future<Status<>> WriteFrames(net::StreamPtr stream,
                                             std::vector<TmpBuffer> buffers,
                                             bool first, bool last,
                                             size_t trim_front_len,
                                             size_t trim_len) {
    Status<> s;
    int n = buffers.size();
    std::vector<seastar::future<Status<>>> fu_vec;
    std::vector<TmpBuffer> first_frame;
    std::vector<TmpBuffer> last_frame;
    for (int i = 0; i < n; ++i) {
        TmpBuffer buf = std::move(buffers[i]);
        if (i == 0 && first && trim_front_len) {
            TmpBuffer first_block =
                buf.share(trim_front_len,
                          std::min(kBlockSize, buf.size()) - trim_front_len);
            buf.trim_front(std::min(kBlockSize, buf.size()));

            if (i == n - 1 && last && trim_len && buf.empty()) {
                first_block.trim(first_block.size() - trim_len);
            }

            TmpBuffer first_block_crc(4);
            net::BigEndian::PutUint32(
                first_block_crc.get_write(),
                crc32_gzip_refl(0, (const unsigned char *)(first_block.get()),
                                first_block.size() - 4));
            first_frame.emplace_back(
                std::move(first_block.share(0, first_block.size() - 4)));
            first_frame.emplace_back(std::move(first_block_crc));
        }
        if (i == n - 1 && last && trim_len && buf.size()) {
            int blocks = buf.size() / kBlockSize;
            TmpBuffer last_block =
                buf.share(blocks * kBlockSize, buf.size() % kBlockSize);
            buf.trim(last_block.size());
            last_block.trim(trim_len);
            TmpBuffer last_block_crc(4);
            net::BigEndian::PutUint32(
                last_block_crc.get_write(),
                crc32_gzip_refl(0, (const unsigned char *)(last_block.get()),
                                last_block.size() - 4));
            last_frame.emplace_back(
                std::move(last_block.share(0, last_block.size() - 4)));
            last_frame.emplace_back(std::move(last_block_crc));
        }

        if (first_frame.size()) {
            auto f = stream->WriteFrame(std::move(first_frame));
            fu_vec.emplace_back(std::move(f));
        }

        if (buf.size()) {
            auto f = stream->WriteFrame(std::move(buf));
            fu_vec.emplace_back(std::move(f));
        }

        if (last_frame.size()) {
            auto f = stream->WriteFrame(std::move(last_frame));
            fu_vec.emplace_back(std::move(f));
        }
    }

    auto res = co_await seastar::when_all_succeed(fu_vec.begin(), fu_vec.end());
    for (int i = 0; i < n; ++i) {
        if (!res[i]) {
            s = std::move(res[i]);
            break;
        }
    }
    co_return s;
}

seastar::future<Status<>> Service::HandleReadExtent(const ReadExtentReq *req,
                                                    net::StreamPtr stream) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    uint64_t off = req->off();
    uint64_t len = req->len();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        s.Set(EINVAL);
        s = co_await SendCommonResp(stream, reqid, READ_EXTENT_RESP, s.Code());
        co_return s;
    }

    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotMatch);
        s = co_await SendCommonResp(stream, reqid, READ_EXTENT_RESP, s.Code());
        co_return s;
    }
    auto extent_ptr = store_->GetExtent(extent_id);
    if (!extent_ptr) {
        s.Set(ErrCode::ErrNoExtent);
        s = co_await SendCommonResp(stream, reqid, READ_EXTENT_RESP, s.Code());
        co_return s;
    }

    if (off > extent_ptr->len) {
        s.Set(EINVAL);
        s = co_await SendCommonResp(stream, reqid, READ_EXTENT_RESP, s.Code());
        co_return s;
    }

    if (extent_ptr->len <= off + len) {
        len = extent_ptr->len - off;
    }

    s = co_await SendReadExtentResp(stream, reqid, len);
    if (!s) {
        co_return s;
    }
    if (len == 0) {
        co_return s;
    }

    uint64_t origin_len = len;
    uint64_t origin_off = off;

    off = off / kBlockDataSize * kBlockDataSize;
    uint64_t trim_front_len = origin_off - off;
    len = origin_len + trim_front_len;
    uint64_t aligned_len =
        (len + kBlockDataSize - 1) / kBlockDataSize * kBlockDataSize;
    if (aligned_len + off <= extent_ptr->len) {
        len = aligned_len;  // prefetch
    }
    uint64_t trim_len = len - origin_len - trim_front_len;
    size_t max_data_size = stream->MaxFrameSize() / kBlockSize * kBlockDataSize;

    bool first = false;
    std::optional<seastar::future<Status<>>> fu;
    std::vector<TmpBuffer> buffers;
    while (len > 0) {
        size_t bytes = std::min(max_data_size, len);
        len -= bytes;
        size_t need_to_read = 0;
        size_t read_off = off;
        while (bytes > 0) {
            size_t n = std::min(bytes, kBlockDataSize);
            bytes -= n;
            BlockCacheKey key(extent_id, off);
            auto v = block_cache_.Get(key);
            if (!v.has_value() || v.value().size() < n + 4) {
                need_to_read += n;
                if (bytes != 0) continue;
            }

            if (v.has_value() && v.value().size() - 4 > n) {
                // only happened when this is the last block data
                trim_len += v.value().size() - 4 - n;
            }

            if (fu) {
                s = co_await std::move(fu.value());
                if (!s) {
                    LOG_ERROR("reply data to {} error: {}",
                              stream->RemoteAddress(), s.String());
                    co_return s;
                }
                fu.reset();
            }

            std::vector<TmpBuffer> buffers;
            if (need_to_read > 0) {
                auto st =
                    co_await store_->Read(extent_ptr, read_off, need_to_read);
                if (!st) {
                    s.Set(st.Code(), st.Reason());
                    co_return s;  // return error, then close stream
                }
                buffers = std::move(st.Value());
                for (int i = 0; i < buffers.size(); ++i) {
                    for (size_t pos = 0; pos < buffers[i].size();) {
                        size_t share_len =
                            std::min(kBlockSize, buffers[i].size() - pos);
                        auto tmp_block_buffer =
                            buffers[i].share(pos, share_len);
                        BlockCacheKey bkey(extent_ptr->id, off);
                        block_cache_.Insert(bkey, tmp_block_buffer);
                        pos += share_len;
                        off += share_len - 4;
                    }
                }
                if (v.has_value()) {
                    off += v.value().size() - 4;
                    buffers.emplace_back(std::move(v.value()));
                }
                need_to_read = 0;
            } else {
                off += v.value().size() - 4;
                buffers.emplace_back(std::move(v.value()));
            }

            fu = WriteFrames(stream, std::move(buffers), first, len == 0,
                             trim_front_len, trim_len);
            first = false;
            read_off = off;
        }
    }

    if (fu) {
        s = co_await std::move(fu.value());
    }

    co_return s;
}

seastar::future<Status<>> Service::HandleCreateExtent(
    const CreateExtentReq *req, net::StreamPtr stream) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        LOG_ERROR("{} parse extent id error", reqid);
        s.Set(EINVAL);
        s = co_await SendCommonResp(stream, reqid, CREATE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotMatch);
        s = co_await SendCommonResp(stream, reqid, CREATE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    s = co_await store_->CreateExtent(extent_id);
    s = co_await SendCommonResp(stream, reqid, CREATE_EXTENT_RESP, s.Code());
    co_return s;
}

seastar::future<Status<>> Service::HandleDeleteExtent(
    const DeleteExtentReq *req, net::StreamPtr stream) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        LOG_ERROR("{} parse extent id error", reqid);
        s.Set(EINVAL);
        s = co_await SendCommonResp(stream, reqid, DELETE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotMatch);
        s = co_await SendCommonResp(stream, reqid, DELETE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    s = co_await store_->RemoveExtent(extent_id);
    s = co_await SendCommonResp(stream, reqid, DELETE_EXTENT_RESP, s.Code());
    co_return s;
}

seastar::future<Status<>> Service::HandleGetExtent(const GetExtentReq *req,
                                                   net::StreamPtr stream) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        LOG_ERROR("{} parse extent id error", reqid);
        s.Set(EINVAL);
        s = co_await SendCommonResp(stream, reqid, GET_EXTENT_RESP, s.Code());
        co_return s;
    }
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotMatch);
        s = co_await SendCommonResp(stream, reqid, GET_EXTENT_RESP, s.Code());
        co_return s;
    }

    auto extent_ptr = store_->GetExtent(extent_id);
    if (!extent_ptr) {
        s.Set(ErrCode::ErrNoExtent);
        s = co_await SendCommonResp(stream, reqid, GET_EXTENT_RESP, s.Code());
        co_return s;
    }

    s = co_await SendGetExtentResp(stream, reqid, extent_ptr->len,
                                   extent_ptr->ctime);
    co_return s;
}

seastar::future<Status<>> Service::HandleUpdateDiskStatus(
    const UpdateDiskStatusReq *req, net::StreamPtr stream) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    uint32_t status = req->status();
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotMatch);
        s = co_await SendCommonResp(stream, reqid, UPDATE_DISK_STATUS_RESP,
                                    s.Code());
        co_return s;
    }
    if (status >= static_cast<uint32_t>(DevStatus::MAX)) {
        s.Set(EINVAL);
        s = co_await SendCommonResp(stream, reqid, UPDATE_DISK_STATUS_RESP,
                                    s.Code());
        co_return s;
    }

    store_->SetStatus(static_cast<DevStatus>(status));
    s = co_await SendCommonResp(stream, reqid, UPDATE_DISK_STATUS_RESP,
                                s.Code());
    co_return s;
}

}  // namespace stream
}  // namespace snail
