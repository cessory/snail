#include "status.h"

#include <unordered_map>

namespace snail {
namespace stream {

static std::unordered_map<ErrCode, std::string> codeMaps = {
    {ErrCode::OK, "OK"},
    {ErrCode::ErrExistChunk, "chunk has already exist"},
    {ErrCode::ErrNoFreeChunks, "no free chunks"},
    {ErrCode::ErrNoFreeBlocks, "no free blocks"},
    {ErrCode::ErrDisk, "disk error"},
    {ErrCode::ErrWriteDisk, "write disk error"},
    {ErrCode::ErrInvalidPos, "invalid position"},
    {ErrCode::ErrInvalidParameter, "invalid parameter"},
    {ErrCode::ErrTooShort, "data too short"},
    {ErrCode::ErrNotFoundChunk, "not found chunk"},
    {ErrCode::ErrInvalidChecksum, "invalid checksum"},
    {ErrCode::ErrDiskIDNotMatch, "diskid not match"},
    {ErrCode::ErrDeletedChunk, "chunk has been deleted"},
    {ErrCode::ErrInvalidLease, "invalid lease"},
    {ErrCode::ErrMsgTooLarge, "payload too large"},
    {ErrCode::ErrMissingLength, "missing length"},
    {ErrCode::ErrChunkConflict, "chunk conflict write"},
    {ErrCode::ErrSystem, "system error"},
};

std::string GetReason(ErrCode code) {
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

}  // namespace stream
}  // namespace snail
