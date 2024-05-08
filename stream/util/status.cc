#include "status.h"

#include <unordered_map>

namespace snail {

static std::unordered_map<ErrCode, const char*> codeMaps = {
    {ErrCode::OK, "OK"},
    {ErrCode::ErrEOF, "end of file"},
    {ErrCode::ErrExistExtent, "extent has already exist"},
    {ErrCode::ErrExtentNotFound, "not found extent"},
    {ErrCode::ErrExtentIsWriting, "extent is writing"},
    {ErrCode::ErrDiskNotFound, "disk not found"},
    {ErrCode::ErrOverWrite, "write disk error"},
    {ErrCode::ErrParallelWrite, "parallel write extent"},
    {ErrCode::ErrTooShort, "data too short"},
    {ErrCode::ErrTooLarge, "data too larger"},
    {ErrCode::ErrInvalidChecksum, "invalid checksum"},
    {ErrCode::ErrUnExpect, "unexpect error"},
};

const char* GetReason(ErrCode code) {
    if (static_cast<int>(code) < 10000 && static_cast<int>(code) != 0) {
        return std::strerror(static_cast<int>(code));
    }
    auto iter = codeMaps.find(code);
    if (iter == codeMaps.end()) {
        return "unknown error";
    }
    return iter->second;
}

}  // namespace snail
