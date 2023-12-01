#pragma once

namespace snail {
namespace net {

enum CmdType {
    SYN = 0,
    FIN = 1,
    PSH = 2,
    NOP = 3,
    UPD = 4,
};

// |-1-|-1-|--2--|----4---|
// |ver|cmd| len |   sid  |
#define HEADER_SIZE 8

struct Frame {
    uint8_t ver;
    uint8_t cmd;
    uint32_t sid;
    seastar::temporary_buffer<char> data;
};

}  // namespace net
}  // namespace snail
