#pragma once
#include <seastar/net/packet.hh>

#include "byteorder.h"
namespace snail {
namespace net {

enum CmdType {
    SYN = 0,
    FIN = 1,
    PSH = 2,
    NOP = 3,
    UPD = 4,
};

// |---1---|----3----|----4---|
// |ver|cmd|   len   |   sid  |
#define STREAM_HEADER_SIZE 8
constexpr uint32_t kMaxFrameSize = ((16 << 20) - 1);

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

    void MarshalTo(char* b) {
        uint32_t v = ((uint32_t)ver << 28) |
                     (((uint32_t)cmd << 24) & 0x0F000000) |
                     (packet.len() & 0x00FFFFFF);
        LittleEndian::PutUint32(b, v);
        LittleEndian::PutUint32(b + 4, sid);
    }

    uint32_t Unmarshal(const char* b) {
        uint32_t v = LittleEndian::Uint32(b);
        ver = (uint8_t)(v >> 28);
        cmd = (uint8_t)((v >> 24) & 0x0F);
        sid = LittleEndian::Uint32(b + 4);
        return v & 0x00FFFFFF;
    }
};

}  // namespace net
}  // namespace snail
