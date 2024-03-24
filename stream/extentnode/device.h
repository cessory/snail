#pragma once
#include <sys/uio.h>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <string_view>
#include <vector>

#include "util/status.h"

namespace snail {
namespace stream {

class Device {
   public:
    virtual const std::string& Name() const = 0;

    virtual size_t Capacity() const = 0;

    virtual seastar::temporary_buffer<char> Get(size_t n) = 0;

    virtual seastar::future<Status<>> Write(uint64_t pos, const char* b,
                                            size_t len) = 0;

    virtual seastar::future<Status<>> Write(uint64_t pos,
                                            std::vector<iovec> iov) = 0;

    virtual seastar::future<Status<size_t>> Read(uint64_t pos, char* b,
                                                 size_t len) = 0;

    virtual seastar::future<Status<size_t>> Read(uint64_t pos,
                                                 std::vector<iovec> iov) = 0;

    virtual seastar::future<> Close() = 0;
};

using DevicePtr = seastar::shared_ptr<Device>;

seastar::future<DevicePtr> OpenKernelDevice(const std::string_view name);

seastar::future<DevicePtr> OpenSpdkDevice(const std::string_view name);

}  // namespace stream
}  // namespace snail
