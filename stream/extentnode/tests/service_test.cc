
#include "extentnode/service.h"

#include <isa-l.h>

#include <iostream>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "mem_stream.h"
#include "net/byteorder.h"
#include "proto/extentnode.pb.h"
#include "util/logger.h"

namespace snail {
namespace stream {

static bool ParseResponse(const char* p, size_t len,
                          ::google::protobuf::Message* resp,
                          ExtentnodeMsgType& type) {
    if (len <= kMetaMsgHeaderLen) {
        LOG_ERROR("invalid len");
        return false;
    }
    uint32_t origin_crc = net::BigEndian::Uint32(p);
    uint32_t crc = crc32_gzip_refl(0, (const unsigned char*)(p + 4), len - 4);
    if (origin_crc != crc) {
        LOG_ERROR("invalid crc");
        return false;
    }
    type = static_cast<ExtentnodeMsgType>(net::BigEndian::Uint16(p + 4));
    uint16_t body_len = net::BigEndian::Uint16(p + 6);
    if (body_len + kMetaMsgHeaderLen != len) {
        LOG_ERROR("invalid body_len={} len={}", body_len, len);
        return false;
    }
    return resp->ParseFromArray(p + kMetaMsgHeaderLen, body_len);
}

SEASTAR_THREAD_TEST_CASE(handle_write_test) {
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb", DevType::HDD).get0();
    BOOST_REQUIRE(store);

    Status<> s;
    ExtentID extent_id(1, 1);
    s = store->CreateExtent(extent_id).get0();
    BOOST_REQUIRE(s);
    char str_extent_id[16];
    net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
    net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);
    auto pair_streams = MemStream::make_stream_pair();

    net::StreamPtr client_stream = pair_streams.first;
    net::StreamPtr server_stream = pair_streams.second;

    Service service(store);

    auto extent_ptr = store->GetExtent(extent_id);
    uint64_t off = 0;
    size_t body_len = 1 << 20;
    TmpBuffer content(16 << 20);
    // write 16M data
    for (int i = 0; i < 16; ++i, off += body_len) {
        std::unique_ptr<WriteExtentReq> req =
            std::make_unique<WriteExtentReq>();
        req->mutable_base()->set_reqid("123456");
        req->set_diskid(1);
        req->set_extent_id(std::string(str_extent_id, 16));
        req->set_off(off);
        req->set_len(body_len);

        TmpBuffer body =
            content.share(off, std::min(body_len, content.size() - off));
        std::vector<TmpBuffer> packets;
        for (size_t pos = 0; pos < body.size();) {
            size_t n = std::min(kBlockDataSize, (size_t)(body.size() - pos));
            uint32_t crc =
                crc32_gzip_refl(0, (const unsigned char*)(body.get() + pos), n);
            TmpBuffer crc_buf(4);
            net::BigEndian::PutUint32(crc_buf.get_write(), crc);
            packets.emplace_back(std::move(body.share(pos, n)));
            packets.emplace_back(std::move(crc_buf));
            pos += n;
        }

        LOG_INFO("write off={} len={}", off, body_len);
        s = client_stream->WriteFrame(std::move(packets)).get0();
        s = service.HandleWriteExtent(req.get(), server_stream).get0();
        BOOST_REQUIRE(s);
        LOG_INFO("write off={} len={} success, read response", off, body_len);
        auto st = client_stream->ReadFrame().get0();
        BOOST_REQUIRE(st);
        TmpBuffer resp_buf = std::move(st.Value());
        std::unique_ptr<CommonResp> resp(new CommonResp());
        ExtentnodeMsgType type;
        BOOST_REQUIRE(
            ParseResponse(resp_buf.get(), resp_buf.size(), resp.get(), type));
        BOOST_REQUIRE_EQUAL(resp->code(), static_cast<int>(ErrCode::OK));
        BOOST_REQUIRE_EQUAL(extent_ptr->len, body_len);
    }

    // read data by service interface
    std::unique_ptr<ReadExtentReq> read_req(new ReadExtentReq);
    read_req->mutable_base()->set_reqid("1234567");
    read_req->set_diskid(1);
    read_req->set_extent_id(std::string(str_extent_id, 16));
    read_req->set_off(1233);
    read_req->set_len(4 << 20);

    LOG_INFO("begin read off={} len={}", read_req->off(), read_req->len());
    auto fu =
        service.HandleReadExtent(read_req.get(), server_stream)
            .then([server_stream](auto f) { return server_stream->Close(); });

    auto st2 = client_stream->ReadFrame().get0();
    BOOST_REQUIRE(st2);
    TmpBuffer read_buf = std::move(st2.Value());
    std::unique_ptr<ReadExtentResp> read_resp(new ReadExtentResp);
    ExtentnodeMsgType type;
    BOOST_REQUIRE(
        ParseResponse(read_buf.get(), read_buf.size(), read_resp.get(), type));
    BOOST_REQUIRE(type == ExtentnodeMsgType::READ_EXTENT_RESP);
    BOOST_REQUIRE(read_resp->base().code() == static_cast<int>(ErrCode::OK));
    BOOST_REQUIRE(read_resp->len() == (4 << 20));
    // read data
    size_t read_len = 0;
    bool first = true;
    size_t first_block_len = kBlockDataSize - 1233;
    while (read_len < read_resp->len()) {
        auto st3 = client_stream->ReadFrame().get0();
        if (st3.Code() == ErrCode::ErrEOF) {
            break;
        }
        BOOST_REQUIRE(st3);
        TmpBuffer b = std::move(st3.Value());
        size_t data_len = b.size();
        const char* p = b.get();
        while (p < b.end()) {
            size_t n = first ? first_block_len + 4
                             : std::min(kBlockSize, (size_t)(b.end() - p));
            read_len += n - 4;
            uint32_t origin_crc = net::BigEndian::Uint32(p + n - 4);
            uint32_t crc = crc32_gzip_refl(0, (const unsigned char*)p, n - 4);
            BOOST_REQUIRE_EQUAL(origin_crc, crc);
            p += n;
        }
    }
    BOOST_REQUIRE_EQUAL(read_len, read_resp->len());

    store->Close().get();
}

}  // namespace stream
}  // namespace snail
