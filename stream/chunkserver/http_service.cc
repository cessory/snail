#include "http_service.h"

#include <isa-l.h>

#include <seastar/core/coroutine.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/defer.hh>

namespace snail {
namespace stream {

const size_t kCrcBlockSize = kBlockSize - kSectorSize;

static void ReplyError(std::unique_ptr<seastar::http::reply>& rep, ErrCode code,
                       const std::string& reason = "") {
    rep->set_status(seastar::http::reply::status_type::expectation_failed);
    rep->write_body("json", ToJsonString(code, reason));
}

Status<std::pair<uint64_t, uint64_t>> parseChunkID(const seastar::sstring& v) {
    if (v.size() != 32) {
        return Status<std::pair<uint64_t, uint64_t>>(
            ErrCode::ErrInvalidParameter);
    }
    char* endptr = nullptr;
    auto hv = v.substr(0, 16);
    auto lv = v.substr(16, 16);
    uint64_t hi = std::strtoul(hv.c_str(), &endptr, 16);
    if (endptr - hv.c_str() != 16) {
        return Status<std::pair<uint64_t, uint64_t>>(
            ErrCode::ErrInvalidParameter);
    }
    endptr = nullptr;
    uint64_t lo = std::strtoul(lv.c_str(), &endptr, 16);
    if (endptr - lv.c_str() != 16) {
        return Status<std::pair<uint64_t, uint64_t>>(
            ErrCode::ErrInvalidParameter);
    }
    Status<std::pair<uint64_t, uint64_t>> s;
    s.SetValue(std::pair<uint64_t, uint64_t>(hi, lo));
    return s;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpWriteHandler::handle(
    const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
    std::unique_ptr<seastar::http::reply> rep) {
    auto value = req->get_query_param("disk_id");
    auto disk_id = std::strtoul(value.c_str(), NULL, 10);
    value = req->get_query_param("chunk_id");
    auto st = parseChunkID(value);
    if (!st.OK()) {
        ReplyError(rep, st.Code());
        co_return rep;
    }
    ChunkID chunk_id(st.Value().first, st.Value().second);
    value = req->get_query_param("lease");
    auto lease = std::strtoul(value.c_str(), NULL, 10);

    value = req->get_header("Content-Length");
    auto len = std::strtoul(value.c_str(), NULL, 10);
    if (len == 0) {
        ReplyError(rep, ErrCode::ErrMissingLength);
        co_return rep;
    }

    ChunkPtr chunk_ptr;
    if (disk_id != disk_ptr_->DiskId()) {
        ReplyError(rep, ErrCode::ErrDiskIDNotMatch);
        co_return rep;
    }
    chunk_ptr = disk_ptr_->GetChunk(chunk_id);
    if (!chunk_ptr) {
        ReplyError(rep, ErrCode::ErrNotFoundChunk);
        co_return rep;
    }
    if (lease == 0 || lease_mgr_ptr_->GetLease(chunk_id) != lease) {
        ReplyError(rep, ErrCode::ErrInvalidLease);
        co_return rep;
    }
    if (!chunk_ptr->TryLock()) {
        ReplyError(rep, ErrCode::ErrChunkConflict);
        co_return rep;
    }

    auto def = seastar::defer([chunk_ptr]() { chunk_ptr->Unlock(); });

    seastar::temporary_buffer<char> buffer;
    size_t start_off = 0;
    size_t offset = chunk_ptr->Len();
    while (len > 0) {
        auto n = std::min(kCrcBlockSize + 4, len);
        if (n < 4) {
            ReplyError(rep, ErrCode::ErrTooShort);
            co_return rep;
        }
        try {
            buffer = co_await req->content_stream->read_exactly(n);
        } catch (std::exception& e) {
            ReplyError(rep, ErrCode::ErrSystem, e.what());
            co_return rep;
        }

        if (buffer.size() != n) {
            ReplyError(rep, ErrCode::ErrTooShort);
            co_return rep;
        }
        const uint32_t* u32 =
            reinterpret_cast<const uint32_t*>(buffer.end() - 4);
        uint32_t origin_crc = seastar::net::ntoh(*u32);
        buffer.trim(buffer.size() - 4);
        // check crc
        if (origin_crc !=
            crc32_gzip_refl(
                0, reinterpret_cast<const unsigned char*>(buffer.get()),
                buffer.size())) {
            ReplyError(rep, ErrCode::ErrInvalidChecksum);
            co_return rep;
        }

        auto s =
            co_await disk_ptr_->Write(chunk_ptr, buffer.get(), buffer.size());
        if (!s.OK()) {
            ReplyError(rep, s.Code(), s.Reason());
            co_return rep;
        }
        len -= n;
    }
    Reply(rep, offset);
    co_return rep;
}

void HttpWriteHandler::Reply(std::unique_ptr<seastar::http::reply>& rep,
                             size_t offset) {
    std::ostringstream oss;

    oss << "{\"code\": " << static_cast<int>(ErrCode::OK)
        << ", \"message\": \"OK\", "
        << "\"offset\": " << offset << "}";
    rep->write_body("json", seastar::sstring(oss.str()));
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpReadHandler::handle(
    const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
    std::unique_ptr<seastar::http::reply> rep) {
    auto value = req->get_query_param("disk_id");
    auto disk_id = std::strtoul(value.c_str(), NULL, 10);
    value = req->get_query_param("chunk_id");
    auto s = parseChunkID(value);
    if (!s.OK()) {
        ReplyError(rep, s.Code());
        co_return rep;
    }
    ChunkID chunk_id(s.Value().first, s.Value().second);
    value = req->get_query_param("offset");
    auto offset = std::strtoul(value.c_str(), NULL, 10);
    value = req->get_query_param("len");
    auto len = std::strtoul(value.c_str(), NULL, 10);

    ChunkPtr chunk_ptr;
    if (disk_id != disk_ptr_->DiskId()) {
        ReplyError(rep, ErrCode::ErrDiskIDNotMatch);
        co_return rep;
    }
    chunk_ptr = disk_ptr_->GetChunk(chunk_id);
    if (!chunk_ptr) {
        ReplyError(rep, ErrCode::ErrNotFoundChunk);
        co_return rep;
    }

    rep->write_body(
        "bin",
        [=, this](
            seastar::output_stream<char>&& out) mutable -> seastar::future<> {
            while (len > 0) {
                size_t n = std::min(kCrcBlockSize, len);
                auto buffer = seastar::temporary_buffer<char>::aligned(
                    kMemoryAlignment, n + 4);
                auto st = co_await disk_ptr_->Read(chunk_ptr, offset,
                                                   buffer.get_write(), n);
                if (!st.OK()) {
                    co_await out.close();
                    throw std::runtime_error(st.Reason());
                }

                size_t rn = st.Value();
                uint32_t crc = crc32_gzip_refl(
                    0, reinterpret_cast<const unsigned char*>(buffer.get()),
                    rn);
                uint32_t* u32 =
                    reinterpret_cast<uint32_t*>(buffer.get_write() + rn);
                *u32 = seastar::net::hton(crc);
                buffer.trim(rn + 4);
                co_await out.write(std::move(buffer));
                if (rn != n) {
                    co_await out.flush();
                    break;
                }

                len -= n;
                offset += n;
            }
            co_await out.close();
            co_return;
        });
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>>
HttpCreateHandler::handle(const seastar::sstring& path,
                          std::unique_ptr<seastar::http::request> req,
                          std::unique_ptr<seastar::http::reply> rep) {
    auto value = req->get_query_param("disk_id");
    auto disk_id = std::strtoul(value.c_str(), NULL, 10);
    value = req->get_query_param("chunk_id");
    auto s = parseChunkID(value);
    if (!s.OK()) {
        ReplyError(rep, s.Code());
        co_return rep;
    }
    ChunkID chunk_id(s.Value().first, s.Value().second);
    value = req->get_query_param("lease");
    auto lease = std::strtoul(value.c_str(), NULL, 10);

    if (disk_id != disk_ptr_->DiskId()) {
        ReplyError(rep, ErrCode::ErrDiskIDNotMatch);
        co_return rep;
    }

    auto st = co_await disk_ptr_->CreateChunk(chunk_id);
    if (!st.OK()) {
        ReplyError(rep, st.Code());
    }
    lease_mgr_ptr_->SetLease(chunk_id, lease);
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>>
HttpRemoveHandler::handle(const seastar::sstring& path,
                          std::unique_ptr<seastar::http::request> req,
                          std::unique_ptr<seastar::http::reply> rep) {
    auto value = req->get_query_param("disk_id");
    auto disk_id = std::strtoul(value.c_str(), NULL, 10);
    value = req->get_query_param("chunk_id");
    auto s = parseChunkID(value);
    if (!s.OK()) {
        ReplyError(rep, s.Code());
        co_return rep;
    }
    ChunkID chunk_id(s.Value().first, s.Value().second);

    if (disk_id != disk_ptr_->DiskId()) {
        ReplyError(rep, ErrCode::ErrDiskIDNotMatch);
        co_return rep;
    }

    auto chunk_ptr = disk_ptr_->GetChunk(chunk_id);
    if (!chunk_ptr) {
        ReplyError(rep, ErrCode::ErrNotFoundChunk);
        co_return rep;
    }

    if (!chunk_ptr->TryLock()) {
        ReplyError(rep, ErrCode::ErrChunkConflict);
        co_return rep;
    }

    auto def = seastar::defer([chunk_ptr]() { chunk_ptr->Unlock(); });

    auto st = co_await disk_ptr_->RemoveChunk(chunk_ptr);
    if (!st.OK()) {
        ReplyError(rep, st.Code());
    }
    lease_mgr_ptr_->RemoveLease(chunk_id);
    co_return rep;
}

}  // namespace stream
}  // namespace snail
