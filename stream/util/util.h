#pragma once
#include <memory>
#include <seastar/core/sharded.hh>
#include <seastar/core/temporary_buffer.hh>

namespace snail {

seastar::temporary_buffer<char> foreign_buffer_copy(
    seastar::foreign_ptr<std::unique_ptr<seastar::temporary_buffer<char>>> org);

}
