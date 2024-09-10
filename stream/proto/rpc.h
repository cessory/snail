#pragma once
#include <google/protobuf/message.h>

#include <tuple>

#include "util/status.h"
#include "util/util.h"

namespace snail {
constexpr size_t kRpcMsgHeaderLen = 4 + 2 + 2;  // crc + type + body len

// return msg body
Status<std::tuple<uint16_t, Buffer>> UnmarshalRpcMessage(Buffer b);

Buffer MarshalRpcMessage(const ::google::protobuf::Message* msg, uint16_t type);

}  // namespace snail
