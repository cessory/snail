#pragma once
#include <fmt/format.h>

#include <memory>
#include <ostream>
#include <seastar/core/sharded.hh>
#include <seastar/core/temporary_buffer.hh>

namespace snail {

using Buffer = seastar::temporary_buffer<char>;

seastar::temporary_buffer<char> foreign_buffer_copy(
    seastar::foreign_ptr<std::unique_ptr<seastar::temporary_buffer<char>>> org);

int VarintLength(uint64_t v);

char* PutVarint32(char* dst, uint32_t v);
char* PutVarint64(char* dst, uint64_t v);

const char* GetVarint32(const char* s, size_t n, uint32_t* v);
const char* GetVarint64(const char* s, size_t n, uint64_t* v);
std::string GenerateReqid();

}  // namespace snail

