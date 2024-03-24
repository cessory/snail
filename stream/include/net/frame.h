#pragma once
#include <seastar/net/packet.hh>
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
#define STREAM_HEADER_SIZE 8

struct Frame {
    uint8_t ver = 0;
    uint8_t cmd = 0;
    uint32_t sid = 0;
    seastar::net::packet packet;

    Frame() = default;
    Frame(const Frame&) = delete;
    Frame& operator=(const Frame& x) = delete;

    Frame(Frame&& x) {
        ver = x.ver;
        cmd = x.cmd;
        sid = x.sid;
        packet = std::move(x.packet);
    }

    Frame& operator=(Frame&& x) {
        if (this != &x) {
            ver = x.ver;
            cmd = x.cmd;
            sid = x.sid;
            packet = std::move(x.packet);
        }
        return *this;
    }
};

}  // namespace net
}  // namespace snail
