#include "status.h"

#include <unordered_map>

namespace snail {

static std::unordered_map<ErrCode, std::string> codeMaps = {
    {ErrCode::OK, "OK"},
    {ErrCode::ErrEOF, "end of file"},
    {ErrCode::ErrExistExtent, "extent has already exist"},
    {ErrCode::ErrOverWrite, "write disk error"},
    {ErrCode::ErrTooShort, "data too short"},
    {ErrCode::ErrTooLarge, "data too larger"},
    {ErrCode::ErrInvalidChecksum, "invalid checksum"},
    {ErrCode::ErrSystem, "system error"},
};

std::string GetReason(ErrCode code) {
    if (static_cast<int>(code) < 10000 && static_cast<int>(code) != 0) {
        return std::strerror(static_cast<int>(code));
    }
    auto iter = codeMaps.find(code);
    if (iter == codeMaps.end()) {
        return "unknown error";
    }
    return iter->second;
}

seastar::sstring ToJsonString(ErrCode code, const std::string& reason) {
    std::ostringstream oss;
    oss << "{\"code\": " << static_cast<int>(code) << ", \"message\": \""
        << (reason == "" ? GetReason(code) : reason) << "\"}";
    return oss.str();
}

}  // namespace snail
