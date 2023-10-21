#include "disk.h"

#include <iostream>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

SEASTAR_THREAD_TEST_CASE(simple_write_read) {
    auto ok = snail::stream::Disk::Format("/dev/sdb", 1,
                                          snail::stream::DiskType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto disk = snail::stream::Disk::Load("/dev/sdb").get();
    snail::stream::ChunkPtr chunk_ptr;
    auto st = disk->CreateChunk(snail::stream::ChunkID(1, 1)).get();
    BOOST_REQUIRE(st.OK());
    chunk_ptr = disk->GetChunk(snail::stream::ChunkID(1, 1));
    BOOST_REQUIRE(chunk_ptr);
    BOOST_REQUIRE_EQUAL(chunk_ptr->Len(), 0);
    auto s1 = disk->Write(chunk_ptr, "1234567890", 10).get();
    BOOST_REQUIRE(s1.OK());
    BOOST_REQUIRE_EQUAL(chunk_ptr->Len(), 10);
    auto s2 = disk->Read(chunk_ptr, 0, 20).get0();
    BOOST_REQUIRE_MESSAGE(s2.OK(), s2.Reason());
    BOOST_REQUIRE_EQUAL(std::string_view(s2.Value().get(), s2.Value().size()),
                        std::string_view("1234567890", 10));
    auto s3 = disk->RemoveChunk(chunk_ptr).get();
    BOOST_REQUIRE(s3.OK());
    chunk_ptr = disk->GetChunk(snail::stream::ChunkID(1, 1));
    BOOST_REQUIRE((!chunk_ptr));
    disk->Close().get();
}

static std::string gen_string(size_t size) {
    static const char alphanum[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    std::string str(size, 0);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, sizeof(alphanum));
    for (size_t i = 0; i < size; i++) {
        str[i] = alphanum[distrib(gen)];
    }
    return std::move(str);
}

SEASTAR_THREAD_TEST_CASE(random_size_write_read) {
    auto ok = snail::stream::Disk::Format("/dev/sdb", 1,
                                          snail::stream::DiskType::HDD, 1)
                  .get0();
    BOOST_REQUIRE(ok);
    auto disk = snail::stream::Disk::Load("/dev/sdb").get();
    std::array<snail::stream::ChunkPtr, 10> chunks;
    for (int i = 0; i < chunks.size(); i++) {
        auto st = disk->CreateChunk(snail::stream::ChunkID(1, i + 1)).get();
        BOOST_REQUIRE(st.OK());
        chunks[i] = st.Value();
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1048576, 4194304);
    size_t total = 0;
    for (int i = 0; i < 10; i++) {
        auto str = gen_string(distrib(gen));
        total += str.size();
        for (int ii = 0; ii < chunks.size(); ii++) {
            auto st = disk->Write(chunks[ii], str.data(), str.size()).get();
            BOOST_REQUIRE(st.OK());
        }
    }

    for (int i = 0; i < chunks.size(); i++) {
        BOOST_REQUIRE_EQUAL(chunks[i]->Len(), total);
    }

    std::cout << "============write total " << total
              << " bytes==================" << std::endl;

    std::uniform_int_distribution<> distrib1(4096, 65536);
    for (size_t off = 0; off < total;) {
        std::vector<seastar::temporary_buffer<char>> contents;
        auto len = distrib1(gen);
        if (off + len > total) {
            len = total - off;
        }
        for (int i = 0; i < chunks.size(); i++) {
            auto st = disk->Read(chunks[i], off, len).get();
            BOOST_REQUIRE_MESSAGE(st.OK(), st.Reason());
            contents.push_back(std::move(st.Value()));
        }

        for (int i = 1; i < contents.size(); i++) {
            BOOST_REQUIRE_EQUAL(
                std::string_view(contents[i - 1].get(), contents[i - 1].size()),
                std::string_view(contents[i].get(), contents[i].size()));
        }
        off += len;
    }
}
