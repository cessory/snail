#include "rpc.h"

#include <isa-l.h>

#include "net/byteorder.h"

namespace snail {

Status<std::tuple<uint16_t, Buffer>> UnmarshalRpcMessage(Buffer b) {
    Status<std::tuple<uint16_t, Buffer>> s;
    if (b.size() < kRpcMsgHeaderLen) {
        s.Set(EBADMSG);
        return s;
    }
    uint32_t origin_crc = net::BigEndian::Uint32(b.get());
    uint16_t type = net::BigEndian::Uint16(b.get() + 4);
    uint16_t len = net::BigEndian::Uint16(b.get() + 6);

    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char*>(b.get() + 4), b.size() - 4);
    if (crc != origin_crc) {
        s.Set(ErrCode::ErrInvalidChecksum);
        return s;
    }

    if (len != b.size() - kRpcMsgHeaderLen) {
        s.Set(EBADMSG);
        return s;
    }
    b.trim_front(kRpcMsgHeaderLen);
    s.SetValue(std::make_tuple(type, std::move(b)));
    return s;
}

Buffer MarshalRpcMessage(const ::google::protobuf::Message* msg,
                         uint16_t type) {
    size_t n = msg->ByteSizeLong();
    Buffer buf(n + kRpcMsgHeaderLen);
    msg->SerializeToArray(buf.get_write() + kRpcMsgHeaderLen, n);
    net::BigEndian::PutUint16(buf.get_write() + 4, type);
    net::BigEndian::PutUint16(buf.get_write() + 6, (uint16_t)n);
    uint32_t crc = crc32_gzip_refl(
        0, reinterpret_cast<const unsigned char*>(buf.get() + 4),
        n + kRpcMsgHeaderLen - 4);
    net::BigEndian::PutUint32(buf.get_write(), crc);
    return buf;
}

}  // namespace snail
