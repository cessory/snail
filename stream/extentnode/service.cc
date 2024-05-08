#include "service.h"

#include <isa-l.h>

#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>
#include <string_view>

#include "proto/common.pb.h"
#include "proto/extentnode.pb.h"
#include "tcp_server.h"
#include "util/logger.h"
#include "util/util.h"

namespace snail {
namespace stream {

static seastar::future<Status<>> SendCommonResp(
    unsigned shard_id, net::Stream *stream, std::string_view reqid,
    ExtentnodeMsgType msgType, ErrCode err, std::string reason,
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
        s = co_await TcpServer::SendResp(resp.get(), msgType, stream, shard_id);
    }
    co_return s;
}

static seastar::future<Status<>> SendCommonResp(unsigned shard_id,
                                                net::Stream *stream,
                                                std::string_view reqid,
                                                ExtentnodeMsgType msgType,
                                                ErrCode err) {
    return SendCommonResp(shard_id, stream, reqid, msgType, err, "",
                          std::map<std::string, std::string>());
}

static seastar::future<Status<>> SendCommonResp(
    unsigned shard_id, net::Stream *stream, std::string_view reqid,
    ExtentnodeMsgType msgType, ErrCode err, std::string reason) {
    auto s = co_await SendCommonResp(shard_id, stream, reqid, msgType, err,
                                     std::move(reason),
                                     std::map<std::string, std::string>());
    co_return s;
}

static seastar::future<Status<>> SendCommonResp(
    unsigned shard_id, net::Stream *stream, std::string_view reqid,
    ExtentnodeMsgType msgType, ErrCode err,
    std::map<std::string, std::string> headers) {
    auto s = co_await SendCommonResp(shard_id, stream, reqid, msgType, err, "",
                                     std::move(headers));
    co_return s;
}

static seastar::future<Status<>> SendReadExtentResp(unsigned shard_id,
                                                    net::Stream *stream,
                                                    std::string_view reqid,
                                                    uint64_t len) {
    std::unique_ptr<ReadExtentResp> resp(new ReadExtentResp);
    resp->mutable_base()->set_reqid(std::string(reqid.data(), reqid.size()));
    resp->mutable_base()->set_code(static_cast<int>(ErrCode::OK));
    resp->set_len(len);
    auto s = co_await TcpServer::SendResp(resp.get(), READ_EXTENT_RESP, stream,
                                          shard_id);
    co_return s;
}

static seastar::future<Status<>> SendGetExtentResp(unsigned shard_id,
                                                   net::Stream *stream,
                                                   std::string_view reqid,
                                                   uint64_t len,
                                                   uint32_t ctime) {
    std::unique_ptr<GetExtentResp> resp(new GetExtentResp);
    resp->mutable_base()->set_reqid(std::string(reqid.data(), reqid.size()));
    resp->mutable_base()->set_code(static_cast<int>(ErrCode::OK));
    resp->set_len(len);
    resp->set_ctime(ctime);
    auto s = co_await TcpServer::SendResp(resp.get(), READ_EXTENT_RESP, stream,
                                          shard_id);
    co_return s;
}

seastar::future<Status<>> Service::HandleWriteExtent(const WriteExtentReq *req,
                                                     net::Stream *stream,
                                                     unsigned shard) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    auto diskid = req->diskid();
    uint64_t off = req->off();
    uint64_t len = req->len();
    const std::string &eid_str = req->extent_id();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        LOG_ERROR("reqid={} parse extent_id={} error", reqid, eid_str);
        s.Set(EINVAL);
        co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                s.Code());
        co_return s;
    }

    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotFound);
        LOG_ERROR("reqid={} extent_id={}-{} diskid={} devid={} error: {}",
                  reqid, extent_id.hi, extent_id.lo, diskid, store_->DeviceId(),
                  s);
        co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                s.Code());
        co_return s;
    }

    auto extent_ptr = store_->GetExtent(extent_id);
    if (!extent_ptr) {
        s.Set(ErrCode::ErrExtentNotFound);
        LOG_ERROR("reqid={} extent_id={}-{} diskid={} error: {}", reqid,
                  extent_id.hi, extent_id.lo, diskid, s.String());
        co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                s.Code());
        co_return s;
    }

    if (!extent_ptr->mu.try_lock()) {
        s.Set(ErrCode::ErrParallelWrite);
        LOG_ERROR("reqid={} extent_id={}-{} diskid={} error: {}", reqid,
                  extent_id.hi, extent_id.lo, diskid, s.String());
        co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                s.Code());
        co_return s;
    }
    auto defer = seastar::defer([extent_ptr] { extent_ptr->mu.unlock(); });

    std::vector<TmpBuffer> buffers;
    std::optional<seastar::future<Status<>>> fu;
    uint64_t sent = 0;
    uint64_t ready = 0;
    bool first_block = true;
    size_t first_block_len = kBlockDataSize - off % kBlockDataSize;

    while (len > 0) {
        Status<seastar::foreign_ptr<std::unique_ptr<TmpBuffer>>> st;
        if (seastar::this_shard_id() == shard) {
            auto st1 = co_await stream->ReadFrame();
            st.Set(st1.Code(), st1.Reason());
            if (st1) {
                st.SetValue(seastar::make_foreign(std::unique_ptr<TmpBuffer>(
                    new TmpBuffer(std::move(st1.Value())))));
            }
        } else {
            st = co_await seastar::smp::submit_to(
                shard,
                [stream]()
                    -> seastar::future<Status<
                        seastar::foreign_ptr<std::unique_ptr<TmpBuffer>>>> {
                    Status<seastar::foreign_ptr<std::unique_ptr<TmpBuffer>>> st;
                    auto st1 = co_await stream->ReadFrame();
                    st.Set(st1.Code(), st1.Reason());
                    if (st1) {
                        st.SetValue(
                            seastar::make_foreign(std::unique_ptr<TmpBuffer>(
                                new TmpBuffer(std::move(st1.Value())))));
                    }
                    co_return std::move(st);
                });
        }
        if (!st) {
            s.Set(st.Code(), st.Reason());
            LOG_ERROR(
                "reqid={} extent_id={}-{} diskid={} read data frame error: {}",
                reqid, extent_id.hi, extent_id.lo, diskid, s);
            co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                    s.Code());
            co_return s;
        }
        auto b = foreign_buffer_copy(std::move(st.Value()));
        if (b.size() <= 4) {
            LOG_ERROR("reqid={} extent_id={}-{} diskid={} data too short",
                      reqid, extent_id.hi, extent_id.lo, diskid);
            s.Set(EBADMSG);
            co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                    s.Code());
            co_return s;
        }
        size_t data_len = 0;
        for (const char *p = b.get(); p < b.end();) {
            size_t n =
                first_block
                    ? std::min(first_block_len + 4, (size_t)(b.end() - p))
                    : std::min(kBlockSize, (size_t)(b.end() - p));
            if (n <= 4) {
                LOG_ERROR("reqid={} extent_id={}-{} diskid={} data too short",
                          reqid, extent_id.hi, extent_id.lo, diskid);
                s.Set(EBADMSG);
                co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                        s.Code());
                co_return s;
            }
            data_len += n - 4;
            uint32_t crc = crc32_gzip_refl(0, (const unsigned char *)p, n - 4);
            uint32_t origin_crc = net::BigEndian::Uint32(p + n - 4);
            if (crc != origin_crc) {
                LOG_ERROR(
                    "reqid={} extent_id={}-{} diskid={} data has invalid "
                    "checksum n={}",
                    reqid, extent_id.hi, extent_id.lo, diskid, n);
                s.Set(ErrCode::ErrInvalidChecksum);
                co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                        s.Code());
                co_return s;
            }
            p += n;
            first_block = false;
        }
        if (len < data_len) {
            LOG_ERROR("reqid={} extent_id={}-{} diskid={} invalid len", reqid,
                      extent_id.hi, extent_id.lo, diskid);
            s.Set(EBADMSG);
            co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                    s.Code());
            co_return s;
        }
        len -= data_len;
        if (buffers.size() == 4) {
            if (fu) {
                s = co_await std::move(fu.value());
                if (!s) {
                    co_await SendCommonResp(shard, stream, reqid,
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
            co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                    s.Code());
            co_return s;
        }
        fu.reset();
        off += sent;
    }

    if (buffers.size() > 0) {
        s = co_await store_->Write(extent_ptr, off, std::move(buffers));
    }
    if (s) {
        s = co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                    s.Code());
    } else {
        co_await SendCommonResp(shard, stream, reqid, WRITE_EXTENT_RESP,
                                s.Code());
    }
    co_return s;
}

static seastar::future<Status<>> WriteFrames(
    unsigned shard, net::Stream *stream, std::vector<TmpBuffer> buffers,
    bool first, bool last, size_t trim_front_len, size_t trim_len) {
    Status<> s;
    int n = buffers.size();
    std::vector<TmpBuffer> send_frame;
    std::vector<TmpBuffer> last_frame;
    std::vector<iovec> iov;

    for (int i = 0; i < n; ++i) {
        TmpBuffer &buf = buffers[i];
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
            iov.push_back({first_block.get_write(), first_block.size() - 4});
            iov.push_back(
                {first_block_crc.get_write(), first_block_crc.size()});
            send_frame.emplace_back(
                std::move(first_block.share(0, first_block.size() - 4)));
            send_frame.emplace_back(std::move(first_block_crc));
        }
        if (i == n - 1 && last && trim_len && buf.size()) {
            size_t last_block_pos = seastar::align_down(buf.size(), kBlockSize);
            size_t last_block_remain = buf.size() & kBlockSizeMask;
            if (last_block_remain == 0 && last_block_pos) {
                last_block_pos -= kBlockSize;
                last_block_remain = kBlockSize;
            }
            TmpBuffer last_block = buf.share(last_block_pos, last_block_remain);
            buf.trim(buf.size() - last_block.size());
            last_block.trim(last_block_remain - trim_len);
            TmpBuffer last_block_crc(4);
            net::BigEndian::PutUint32(
                last_block_crc.get_write(),
                crc32_gzip_refl(0, (const unsigned char *)(last_block.get()),
                                last_block.size() - 4));
            last_frame.emplace_back(
                std::move(last_block.share(0, last_block.size() - 4)));
            last_frame.emplace_back(std::move(last_block_crc));
        }

        if (buf.size()) {
            iov.push_back({buf.get_write(), buf.size()});
            send_frame.emplace_back(std::move(buf));
        }

        for (int j = 0; j < last_frame.size(); j++) {
            iov.push_back({last_frame[j].get_write(), last_frame[j].size()});
            send_frame.emplace_back(std::move(last_frame[j]));
        }
    }

    if (shard == seastar::this_shard_id()) {
        s = co_await stream->WriteFrame(std::move(iov));
    } else {
        s = co_await seastar::smp::submit_to(
            shard, [&iov, stream] { return stream->WriteFrame(iov); });
    }

    co_return s;
}

seastar::future<> Service::Close() { return store_->Close(); }

seastar::future<Status<>> Service::HandleReadExtent(const ReadExtentReq *req,
                                                    net::Stream *stream,
                                                    unsigned shard) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    uint64_t off = req->off();
    uint64_t len = req->len();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        s.Set(EINVAL);
        s = co_await SendCommonResp(shard, stream, reqid, READ_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotFound);
        s = co_await SendCommonResp(shard, stream, reqid, READ_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }
    auto extent_ptr = store_->GetExtent(extent_id);
    if (!extent_ptr) {
        s.Set(ErrCode::ErrExtentNotFound);
        s = co_await SendCommonResp(shard, stream, reqid, READ_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    if (off > extent_ptr->len) {
        s.Set(EINVAL);
        s = co_await SendCommonResp(shard, stream, reqid, READ_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    if (extent_ptr->len <= off + len) {
        len = extent_ptr->len - off;
    }

    s = co_await SendReadExtentResp(shard, stream, reqid, len);
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

    bool first = true;
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
                v.reset();
                if (bytes != 0) continue;
            }

            if (v.has_value() && v.value().size() > n + 4 && len == 0 &&
                bytes == 0) {
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

            fu =
                WriteFrames(shard, stream, std::move(buffers), first,
                            (len == 0 && bytes == 0), trim_front_len, trim_len);
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
    const CreateExtentReq *req, net::Stream *stream, unsigned shard) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        LOG_ERROR("{} parse extent id error", reqid);
        s.Set(EINVAL);
        s = co_await SendCommonResp(shard, stream, reqid, CREATE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotFound);
        s = co_await SendCommonResp(shard, stream, reqid, CREATE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    s = co_await store_->CreateExtent(extent_id);
    s = co_await SendCommonResp(shard, stream, reqid, CREATE_EXTENT_RESP,
                                s.Code());
    co_return s;
}

seastar::future<Status<>> Service::HandleDeleteExtent(
    const DeleteExtentReq *req, net::Stream *stream, unsigned shard) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        LOG_ERROR("{} parse extent id error", reqid);
        s.Set(EINVAL);
        s = co_await SendCommonResp(shard, stream, reqid, DELETE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotFound);
        s = co_await SendCommonResp(shard, stream, reqid, DELETE_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    s = co_await store_->RemoveExtent(extent_id);
    s = co_await SendCommonResp(shard, stream, reqid, DELETE_EXTENT_RESP,
                                s.Code());
    co_return s;
}

seastar::future<Status<>> Service::HandleGetExtent(const GetExtentReq *req,
                                                   net::Stream *stream,
                                                   unsigned shard) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    ExtentID extent_id;
    if (!extent_id.Parse(req->extent_id())) {
        LOG_ERROR("{} parse extent id error", reqid);
        s.Set(EINVAL);
        s = co_await SendCommonResp(shard, stream, reqid, GET_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotFound);
        s = co_await SendCommonResp(shard, stream, reqid, GET_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    auto extent_ptr = store_->GetExtent(extent_id);
    if (!extent_ptr) {
        s.Set(ErrCode::ErrExtentNotFound);
        s = co_await SendCommonResp(shard, stream, reqid, GET_EXTENT_RESP,
                                    s.Code());
        co_return s;
    }

    s = co_await SendGetExtentResp(shard, stream, reqid, extent_ptr->len,
                                   extent_ptr->ctime);
    co_return s;
}

seastar::future<Status<>> Service::HandleUpdateDiskStatus(
    const UpdateDiskStatusReq *req, net::Stream *stream, unsigned shard) {
    Status<> s;

    const std::string &reqid = req->base().reqid();
    uint32_t diskid = req->diskid();
    uint32_t status = req->status();
    if (diskid != store_->DeviceId()) {
        s.Set(ErrCode::ErrDiskNotFound);
        s = co_await SendCommonResp(shard, stream, reqid,
                                    UPDATE_DISK_STATUS_RESP, s.Code());
        co_return s;
    }
    if (status >= static_cast<uint32_t>(DevStatus::MAX)) {
        s.Set(EINVAL);
        s = co_await SendCommonResp(shard, stream, reqid,
                                    UPDATE_DISK_STATUS_RESP, s.Code());
        co_return s;
    }

    store_->SetStatus(static_cast<DevStatus>(status));
    s = co_await SendCommonResp(shard, stream, reqid, UPDATE_DISK_STATUS_RESP,
                                s.Code());
    co_return s;
}

}  // namespace stream
}  // namespace snail
