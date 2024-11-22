
#include "extentnode/service.h"

#include <isa-l.h>

#include <iostream>
#include <random>
#include <seastar/core/memory.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "mem_stream.h"
#include "net/byteorder.h"
#include "proto/extentnode.pb.h"
#include "proto/rpc.h"
#include "util/logger.h"
#include "util/util.h"

namespace snail {
namespace stream {

static bool ParseResponse(Buffer b, ::google::protobuf::Message* resp,
                          ExtentnodeMsgType& type) {
    auto s = UnmarshalRpcMessage(b.share());
    if (!s) {
        LOG_ERROR("parse response error: {}", s);
        return false;
    }
    b = std::move(std::get<1>(s.Value()));
    type = static_cast<ExtentnodeMsgType>(std::get<0>(s.Value()));
    return resp->ParseFromArray(b.get(), b.size());
}

static seastar::future<Status<std::unique_ptr<GetExtentResp>>> GetExtent(
    Service& service, const ExtentID extent_id, net::Stream* client_stream,
    net::Stream* server_stream) {
    Status<std::unique_ptr<GetExtentResp>> s;
    char str_extent_id[16];
    net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
    net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);

    std::unique_ptr<GetExtentReq> req(new GetExtentReq);
    req->mutable_header()->set_reqid("123456");
    req->set_diskid(1);
    req->set_extent_id(std::string(str_extent_id, 16));

    auto st1 = co_await service.HandleGetExtent(req.get(), server_stream);
    if (!st1) {
        LOG_ERROR("get extent={} error: {}", extent_id, st1);
        s.Set(st1.Code(), st1.Reason());
        co_return s;
    }

    Buffer resp_buf;
    auto st2 = co_await client_stream->ReadFrame();
    if (!st2) {
        s.Set(st2.Code(), st2.Reason());
        co_return s;
    }
    resp_buf = std::move(st2.Value());

    std::unique_ptr<GetExtentResp> resp(new GetExtentResp);
    ExtentnodeMsgType type;
    if (!ParseResponse(resp_buf.share(), resp.get(), type)) {
        s.Set(EINVAL);
        co_return s;
    }
    if (type != ExtentnodeMsgType::GET_EXTENT_RESP) {
        s.Set(EBADMSG);
    } else {
        s.SetValue(std::move(resp));
    }
    co_return s;
}

SEASTAR_THREAD_TEST_CASE(handle_write_read_test) {
    LOG_INFO("test write read extent...");
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);

    Status<> s;
    ExtentID extent_id(1, 1);
    s = store->CreateExtent(extent_id).get0();
    BOOST_REQUIRE(s);
    char str_extent_id[16];
    net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
    net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);

    Service service(store);

    auto extent_ptr = store->GetExtent(extent_id);
    {
        uint64_t off = 0;
        size_t body_len = 1 << 20;
        Buffer content(16 << 20);
        memset(content.get_write(), 'a', content.size());

        auto pair_streams = MemStream::make_stream_pair();
        net::StreamPtr client_stream = pair_streams.first;
        net::StreamPtr server_stream = pair_streams.second;
        // write 16M data
        for (int i = 0; i < 16; ++i, off += body_len) {
            std::unique_ptr<WriteExtentReq> req =
                std::make_unique<WriteExtentReq>();
            req->mutable_header()->set_reqid("123456");
            req->set_diskid(1);
            req->set_extent_id(std::string(str_extent_id, 16));
            req->set_off(off);
            req->set_len(body_len);

            Buffer body =
                content.share(off, std::min(body_len, content.size() - off));
            std::vector<Buffer> packets;
            size_t packets_len = 0;
            bool first_block = true;
            size_t first_block_len = kBlockDataSize - off % kBlockDataSize;
            for (size_t pos = 0; pos < body.size();) {
                size_t n =
                    std::min(kBlockDataSize, (size_t)(body.size() - pos));
                if (first_block) {
                    first_block = false;
                    n = std::min(first_block_len, n);
                }
                if (packets_len + n + 4 > client_stream->MaxFrameSize()) {
                    s = client_stream->WriteFrame(std::move(packets)).get0();
                    BOOST_REQUIRE(s);
                    packets_len = 0;
                }
                uint32_t crc = crc32_gzip_refl(
                    0, (const unsigned char*)(body.get() + pos), n);
                Buffer crc_buf(4);
                net::BigEndian::PutUint32(crc_buf.get_write(), crc);
                packets.emplace_back(std::move(body.share(pos, n)));
                packets.emplace_back(std::move(crc_buf));
                pos += n;
                packets_len += n + 4;
            }

            if (packets_len) {
                s = client_stream->WriteFrame(std::move(packets)).get0();
                BOOST_REQUIRE(s);
            }
            s = service.HandleWriteExtent(req.get(), server_stream.get())
                    .get0();
            BOOST_REQUIRE(s);
            auto st = client_stream->ReadFrame().get0();
            BOOST_REQUIRE(st);
            Buffer resp_buf = std::move(st.Value());
            std::unique_ptr<CommonResp> resp(new CommonResp());
            ExtentnodeMsgType type;
            BOOST_REQUIRE(ParseResponse(resp_buf.share(), resp.get(), type));
            BOOST_REQUIRE_EQUAL(resp->code(), static_cast<int>(ErrCode::OK));
            BOOST_REQUIRE_EQUAL(extent_ptr->len, body_len * (i + 1));
        }
    }

    uint64_t lens[4] = {4 << 20, 4 << 20, 4 << 20, (4 << 20) - 10};
    uint64_t offs[4] = {1233, 1233, (16 << 20) - (4 << 20),
                        (16 << 20) - (4 << 20)};
    for (int i = 0; i < 4; i++) {
        // the second read is hit cache
        auto read_streams = MemStream::make_stream_pair();
        net::StreamPtr read_client_stream = read_streams.first;
        net::StreamPtr read_server_stream = read_streams.second;
        // read data by service interface
        std::unique_ptr<ReadExtentReq> read_req(new ReadExtentReq);
        read_req->mutable_header()->set_reqid("1234567");
        read_req->set_diskid(1);
        read_req->set_extent_id(std::string(str_extent_id, 16));
        read_req->set_off(offs[i]);
        read_req->set_len(lens[i]);

        LOG_INFO("begin read i={} off={} len={}", i, read_req->off(),
                 read_req->len());
        auto fu =
            service.HandleReadExtent(read_req.get(), read_server_stream.get());

        auto st2 = read_client_stream->ReadFrame().get0();
        BOOST_REQUIRE(st2);
        Buffer read_buf = std::move(st2.Value());
        std::unique_ptr<ReadExtentResp> read_resp(new ReadExtentResp);
        ExtentnodeMsgType type;
        BOOST_REQUIRE(ParseResponse(read_buf.share(), read_resp.get(), type));
        BOOST_REQUIRE(type == ExtentnodeMsgType::READ_EXTENT_RESP);
        BOOST_REQUIRE(read_resp->header().code() ==
                      static_cast<int>(ErrCode::OK));
        BOOST_REQUIRE(read_resp->len() == lens[i]);
        // read data
        size_t read_len = 0;
        bool first = true;
        size_t first_block_len = kBlockDataSize - offs[i] % kBlockDataSize;
        while (read_len < read_resp->len()) {
            auto st3 = read_client_stream->ReadFrame().get0();
            if (st3.Code() == ErrCode::ErrEOF) {
                break;
            }
            BOOST_REQUIRE(st3);
            Buffer b = std::move(st3.Value());
            size_t data_len = b.size();
            const char* p = b.get();
            while (p < b.end()) {
                size_t n = std::min(kBlockSize, (size_t)(b.end() - p));
                if (first) {
                    n = first_block_len + 4;
                    first = false;
                }
                read_len += n - 4;
                uint32_t origin_crc = net::BigEndian::Uint32(p + n - 4);
                uint32_t crc =
                    crc32_gzip_refl(0, (const unsigned char*)p, n - 4);
                BOOST_REQUIRE_EQUAL(origin_crc, crc);
                p += n;
            }
        }
        BOOST_REQUIRE_EQUAL(read_len, read_resp->len());
        fu.get0();
    }
    service.Close().get();
}

SEASTAR_THREAD_TEST_CASE(handle_multi_thread_write_read_test) {
    LOG_INFO("test multi thread write read...");
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);

    if (seastar::smp::count <= 1) {
        LOG_WARN("no multi thread env");
        return;
    }

    seastar::memory::enable_abort_on_allocation_failure();
    Status<> s;
    ExtentID extent_id(1, 1);
    s = store->CreateExtent(extent_id).get0();
    BOOST_REQUIRE(s);
    char str_extent_id[16];
    net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
    net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);

    Service service(store);

    auto extent_ptr = store->GetExtent(extent_id);
    uint64_t off = 0;
    size_t body_len = 1 << 20;

    {
        auto tp =
            seastar::smp::submit_to(1, []() {
                auto pair_streams = MemStream::make_stream_pair();
                return std::make_tuple(
                    seastar::make_foreign(std::move(pair_streams.first)),
                    seastar::make_foreign(std::move(pair_streams.second)));
            }).get0();

        seastar::foreign_ptr<net::StreamPtr> client_stream =
            std::move(std::get<0>(tp));
        seastar::foreign_ptr<net::StreamPtr> server_stream =
            std::move(std::get<1>(tp));

        Buffer content(16 << 20);
        memset(content.get_write(), 'a', content.size());
        // write 16M data
        for (int i = 0; i < 16; ++i, off += body_len) {
            std::unique_ptr<WriteExtentReq> req =
                std::make_unique<WriteExtentReq>();
            req->mutable_header()->set_reqid("123456");
            req->set_diskid(1);
            req->set_extent_id(std::string(str_extent_id, 16));
            req->set_off(off);
            req->set_len(body_len);

            Buffer body =
                content.share(off, std::min(body_len, content.size() - off));
            std::vector<Buffer> packets;
            std::vector<iovec> iov;
            size_t packets_len = 0;
            bool first_block = true;
            size_t first_block_len = kBlockDataSize - off % kBlockDataSize;
            for (size_t pos = 0; pos < body.size();) {
                size_t n =
                    std::min(kBlockDataSize, (size_t)(body.size() - pos));
                if (first_block) {
                    first_block = false;
                    n = std::min(first_block_len, n);
                }
                if (packets_len + n + 4 > client_stream->MaxFrameSize()) {
                    s = seastar::smp::submit_to(1, [stream =
                                                        client_stream.get(),
                                                    &iov]() {
                            return stream->WriteFrame(std::move(iov));
                        }).get0();
                    BOOST_REQUIRE(s);
                    packets_len = 0;
                    packets.clear();
                }
                uint32_t crc = crc32_gzip_refl(
                    0, (const unsigned char*)(body.get() + pos), n);
                Buffer crc_buf(4);
                net::BigEndian::PutUint32(crc_buf.get_write(), crc);
                iov.push_back({body.get_write() + pos, n});
                iov.push_back({crc_buf.get_write(), crc_buf.size()});
                packets.emplace_back(std::move(crc_buf));
                pos += n;
                packets_len += n + 4;
            }

            if (packets_len) {
                s = seastar::smp::submit_to(1, [stream = client_stream.get(),
                                                &iov]() {
                        return stream->WriteFrame(std::move(iov));
                    }).get0();
                BOOST_REQUIRE(s);
            }
            s = service.HandleWriteExtent(req.get(), server_stream.get())
                    .get0();
            BOOST_REQUIRE(s);
            auto st = client_stream->ReadFrame().get0();
            BOOST_REQUIRE(st);
            Buffer resp_buf = std::move(st.Value());
            std::unique_ptr<CommonResp> resp(new CommonResp());
            ExtentnodeMsgType type;
            BOOST_REQUIRE(ParseResponse(resp_buf.share(), resp.get(), type));
            BOOST_REQUIRE_EQUAL(resp->code(), static_cast<int>(ErrCode::OK));
            BOOST_REQUIRE_EQUAL(extent_ptr->len, body_len * (i + 1));
        }

        auto st = GetExtent(service, extent_id, client_stream.get(),
                            server_stream.get())
                      .get0();
        BOOST_REQUIRE(st);
        BOOST_REQUIRE_EQUAL(st.Value()->len(), 16 << 20);
    }

    uint64_t lens[4] = {4 << 20, 4 << 20, 4 << 20, (4 << 20) - 10};
    uint64_t offs[4] = {1233, 1233, (16 << 20) - (4 << 20),
                        (16 << 20) - (4 << 20)};
    for (int i = 0; i < 4; i++) {
        // the second read is hit cache
        auto tp = seastar::smp::submit_to(1, []() {
                      auto pair_streams = MemStream::make_stream_pair();
                      return std::make_tuple(
                          seastar::make_foreign(pair_streams.first),
                          seastar::make_foreign(pair_streams.second));
                  }).get0();

        seastar::foreign_ptr<net::StreamPtr> read_client_stream =
            std::move(std::get<0>(tp));
        seastar::foreign_ptr<net::StreamPtr> read_server_stream =
            std::move(std::get<1>(tp));
        // read data by service interface
        std::unique_ptr<ReadExtentReq> read_req(new ReadExtentReq);
        read_req->mutable_header()->set_reqid("1234567");
        read_req->set_diskid(1);
        read_req->set_extent_id(std::string(str_extent_id, 16));
        read_req->set_off(offs[i]);
        read_req->set_len(lens[i]);

        LOG_INFO("begin read i={} off={} len={}", i, read_req->off(),
                 read_req->len());
        auto stat = seastar::memory::stats();
        LOG_INFO(
            "total_memory={}, free_memory={}, cross_cpu_frees={}, "
            "foreign_mallocs={}, "
            "foreign_frees={}, foreign_cross_frees={}",
            stat.total_memory(), stat.free_memory(), stat.cross_cpu_frees(),
            stat.foreign_mallocs(), stat.foreign_frees(),
            stat.foreign_cross_frees());
        auto fu =
            service.HandleReadExtent(read_req.get(), read_server_stream.get());

        auto st2 = read_client_stream->ReadFrame().get0();
        BOOST_REQUIRE(st2.OK());
        Buffer read_buf = std::move(st2.Value());
        std::unique_ptr<ReadExtentResp> read_resp(new ReadExtentResp);
        ExtentnodeMsgType type;
        BOOST_REQUIRE(ParseResponse(read_buf.share(), read_resp.get(), type));
        BOOST_REQUIRE(type == ExtentnodeMsgType::READ_EXTENT_RESP);
        BOOST_REQUIRE(read_resp->header().code() ==
                      static_cast<int>(ErrCode::OK));
        BOOST_REQUIRE(read_resp->len() == lens[i]);
        // read data
        size_t read_len = 0;
        bool first = true;
        size_t first_block_len = kBlockDataSize - offs[i] % kBlockDataSize;
        while (read_len < read_resp->len()) {
            auto st3 = read_client_stream->ReadFrame().get0();
            if (st3.Code() == ErrCode::ErrEOF) {
                break;
            }
            BOOST_REQUIRE(st3.OK());
            Buffer b = std::move(st3.Value());
            size_t data_len = b.size();
            const char* p = b.get();
            while (p < b.end()) {
                size_t n = std::min(kBlockSize, (size_t)(b.end() - p));
                if (first) {
                    n = first_block_len + 4;
                    first = false;
                }
                read_len += n - 4;
                uint32_t origin_crc = net::BigEndian::Uint32(p + n - 4);
                uint32_t crc =
                    crc32_gzip_refl(0, (const unsigned char*)p, n - 4);
                BOOST_REQUIRE_EQUAL(origin_crc, crc);
                p += n;
            }
        }
        BOOST_REQUIRE_EQUAL(read_len, read_resp->len());
        fu.get0();
    }

    service.Close().get();
}

SEASTAR_THREAD_TEST_CASE(handle_create_test) {
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);

    Status<> s;
    Service service(store);
    auto pair_streams = MemStream::make_stream_pair();
    net::StreamPtr client_stream = pair_streams.first;
    net::StreamPtr server_stream = pair_streams.second;

    ExtentID extent_id(1, 1);
    char str_extent_id[16];
    net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
    net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);

    std::unique_ptr<CreateExtentReq> req(new CreateExtentReq);
    req->mutable_header()->set_reqid("123456");
    req->set_diskid(1);
    req->set_extent_id(std::string(str_extent_id, 16));

    s = service.HandleCreateExtent(req.get(), server_stream.get()).get0();
    BOOST_REQUIRE(s);
    auto st = client_stream->ReadFrame().get();
    BOOST_REQUIRE(st);

    Buffer resp_buf = std::move(st.Value());
    std::unique_ptr<CommonResp> resp(new CommonResp);
    ExtentnodeMsgType type;
    BOOST_REQUIRE(ParseResponse(resp_buf.share(), resp.get(), type));
    BOOST_REQUIRE(type == ExtentnodeMsgType::CREATE_EXTENT_RESP);
    BOOST_REQUIRE(resp->code() == static_cast<int>(ErrCode::OK));
    service.Close().get();
}

SEASTAR_THREAD_TEST_CASE(handle_delete_extent_test) {
    LOG_INFO("test delete extent...");
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);

    Status<> s;
    Service service(store);
    auto pair_streams = MemStream::make_stream_pair();
    net::StreamPtr client_stream = pair_streams.first;
    net::StreamPtr server_stream = pair_streams.second;

    for (int i = 0; i < 100; i++) {
        ExtentID extent_id(i + 1, i + 1);
        char str_extent_id[16];
        net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
        net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);

        std::unique_ptr<CreateExtentReq> req(new CreateExtentReq);
        req->mutable_header()->set_reqid("123456");
        req->set_diskid(1);
        req->set_extent_id(std::string(str_extent_id, 16));

        s = service.HandleCreateExtent(req.get(), server_stream.get()).get0();
        BOOST_REQUIRE(s);
        auto st = client_stream->ReadFrame().get();
        BOOST_REQUIRE(st);

        Buffer resp_buf = std::move(st.Value());
        std::unique_ptr<CommonResp> resp(new CommonResp);
        ExtentnodeMsgType type;
        BOOST_REQUIRE(ParseResponse(resp_buf.share(), resp.get(), type));
        BOOST_REQUIRE(type == ExtentnodeMsgType::CREATE_EXTENT_RESP);
        BOOST_REQUIRE(resp->code() == static_cast<int>(ErrCode::OK));
    }

    // delete an exist extent
    {
        ExtentID extent_id(50, 50);
        char str_extent_id[16];
        net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
        net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);
        std::unique_ptr<DeleteExtentReq> req(new DeleteExtentReq);
        req->mutable_header()->set_reqid("123456");
        req->set_diskid(1);
        req->set_extent_id(std::string(str_extent_id, 16));

        s = service.HandleDeleteExtent(req.get(), server_stream.get()).get0();
        BOOST_REQUIRE(s);
        auto st = client_stream->ReadFrame().get();
        BOOST_REQUIRE(st);

        Buffer resp_buf = std::move(st.Value());
        std::unique_ptr<CommonResp> resp(new CommonResp);
        ExtentnodeMsgType type;
        BOOST_REQUIRE(ParseResponse(resp_buf.share(), resp.get(), type));
        BOOST_REQUIRE(type == ExtentnodeMsgType::DELETE_EXTENT_RESP);
        BOOST_REQUIRE(resp->code() == static_cast<int>(ErrCode::OK));

        auto extent_ptr = store->GetExtent(extent_id);
        BOOST_REQUIRE(!extent_ptr);
    }
    service.Close().get0();
}

SEASTAR_THREAD_TEST_CASE(handle_get_extent_test) {
    LOG_INFO("test get extent...");
    auto ok = snail::stream::Store::Format("/dev/sdb", 1,
                                           snail::stream::DevType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto store = Store::Load("/dev/sdb").get0();
    BOOST_REQUIRE(store);

    Status<> s;
    Service service(store);
    auto pair_streams = MemStream::make_stream_pair();
    net::StreamPtr client_stream = pair_streams.first;
    net::StreamPtr server_stream = pair_streams.second;

    // create extent
    for (int i = 0; i < 100; i++) {
        ExtentID extent_id(i + 1, i + 1);
        char str_extent_id[16];
        net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
        net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);

        std::unique_ptr<CreateExtentReq> req(new CreateExtentReq);
        req->mutable_header()->set_reqid("123456");
        req->set_diskid(1);
        req->set_extent_id(std::string(str_extent_id, 16));

        s = service.HandleCreateExtent(req.get(), server_stream.get()).get0();
        BOOST_REQUIRE(s);
        auto st = client_stream->ReadFrame().get();
        BOOST_REQUIRE(st);

        Buffer resp_buf = std::move(st.Value());
        std::unique_ptr<CommonResp> resp(new CommonResp);
        ExtentnodeMsgType type;
        BOOST_REQUIRE(ParseResponse(resp_buf.share(), resp.get(), type));
        BOOST_REQUIRE(type == ExtentnodeMsgType::CREATE_EXTENT_RESP);
        BOOST_REQUIRE(resp->code() == static_cast<int>(ErrCode::OK));
    }

    // random get a extent
    {
        std::random_device rd;
        std::default_random_engine el(rd());
        std::uniform_int_distribution<int> uniform_dist(1, 100);
        int i = uniform_dist(el);

        ExtentID extent_id(i, i);
        char str_extent_id[16];
        net::BigEndian::PutUint64(&str_extent_id[0], extent_id.hi);
        net::BigEndian::PutUint64(&str_extent_id[8], extent_id.lo);

        std::unique_ptr<GetExtentReq> req(new GetExtentReq);
        req->mutable_header()->set_reqid("123456");
        req->set_diskid(1);
        req->set_extent_id(std::string(str_extent_id, 16));

        auto st = GetExtent(service, extent_id, client_stream.get(),
                            server_stream.get())
                      .get0();
        BOOST_REQUIRE(st);

        BOOST_REQUIRE(st.Value()->header().code() ==
                      static_cast<int>(ErrCode::OK));
        BOOST_REQUIRE(st.Value()->len() == 0);
    }
}

}  // namespace stream
}  // namespace snail
