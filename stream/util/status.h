#pragma once
#include <fmt/ostream.h>

#include <seastar/core/sstring.hh>
#include <string>
#include <tuple>

namespace snail {

enum class ErrCode {
    OK = 0,
    //////////common error ////////
    ErrEOF = 10000,
    ErrExistExtent = 20000,
    ErrNoExtent = 20001,
    ErrExtentIsWriting = 20002,
    ErrDiskNotMatch = 20003,
    ErrOverWrite = 20004,
    ErrParallelWrite = 20005,
    ErrTooShort = 20007,
    ErrTooLarge = 20008,
    ErrInvalidChecksum = 20010,
    ErrUnExpect = 29999
};

const char* GetReason(ErrCode code);

template <typename... T>
class Status;

template <typename T>
class Status<T> {
    ErrCode code_;
    seastar::sstring reason_;
    T val_;

   public:
    Status() noexcept : code_(ErrCode::OK) {}

    Status(ErrCode code) noexcept {
        code_ = code;
        reason_ = GetReason(code);
    }

    Status(ErrCode code, const std::string& reason) noexcept {
        code_ = code;
        reason_ = reason;
    }

    Status(ErrCode code, const char* reason) noexcept {
        code_ = code;
        reason_ = reason;
    }

    Status(const Status& s) noexcept {
        val_ = s.val_;
        code_ = s.code_;
        reason_ = s.reason_;
    }

    Status(Status&& s) noexcept {
        val_ = std::move(s.val_);
        code_ = s.code_;
        reason_ = std::move(s.reason_);
    }

    Status& operator=(const Status& s) {
        if (this != &s) {
            val_ = s.val_;
            code_ = s.code_;
            reason_ = s.reason_;
        }
        return *this;
    }

    Status& operator=(Status&& s) {
        if (this != &s) {
            val_ = std::move(s.val_);
            code_ = s.code_;
            reason_ = std::move(s.reason_);
        }
        return *this;
    }

    explicit operator bool() const noexcept { return code_ == ErrCode::OK; }

    friend std::ostream& operator<<(std::ostream& os, const Status<T>& s) {
        os << "{\"code\": " << static_cast<int>(s.code_) << ", \"reason\": \""
           << (s.reason_.empty() ? GetReason(s.code_) : s.reason_) << "\"}";
        return os;
    }

    bool OK() const { return code_ == ErrCode::OK; }

    void Set(ErrCode code) {
        code_ = code;
        reason_ = GetReason(code_);
    }

    void Set(ErrCode code, const char* reason) {
        code_ = code;
        reason_ = reason;
    }

    void Set(ErrCode code, const std::string& reason) {
        code_ = code;
        reason_ = reason;
    }

    void Set(int code) {
        code_ = static_cast<ErrCode>(code);
        reason_ = GetReason(code_);
    }

    void Set(int code, const std::string& reason) {
        code_ = static_cast<ErrCode>(code);
        reason_ = reason;
    }

    ErrCode Code() const { return code_; }

    const char* Reason() const { return reason_.c_str(); }

    T& Value() { return val_; }

    void SetValue(T val) { val_ = std::move(val); }

    std::string String() const {
        std::ostringstream oss;
        oss << "{\"code\": " << static_cast<int>(code_) << ", \"reason\": \""
            << (reason_.empty() ? GetReason(code_) : reason_) << "\"}";
        return oss.str();
    }
};

#if FMT_VERSION >= 90000

template <T>
struct fmt::formatter<Status<T>> : fmt::ostream_formatter {};

#endif

template <>
class Status<> {
    ErrCode code_;
    seastar::sstring reason_;

   public:
    Status() noexcept : code_(ErrCode::OK) {}

    Status(ErrCode code) noexcept {
        code_ = code;
        reason_ = GetReason(code);
    }

    Status(ErrCode code, const std::string& reason) noexcept {
        code_ = code;
        reason_ = reason;
    }

    Status(ErrCode code, const char* reason) noexcept {
        code_ = code;
        reason_ = reason;
    }

    Status(const Status& s) noexcept {
        code_ = s.code_;
        reason_ = s.reason_;
    }

    Status(Status&& s) noexcept {
        code_ = s.code_;
        reason_ = std::move(s.reason_);
    }

    Status& operator=(const Status& s) {
        if (this != &s) {
            code_ = s.code_;
            reason_ = s.reason_;
        }
        return *this;
    }

    Status& operator=(Status&& s) {
        if (this != &s) {
            code_ = s.code_;
            reason_ = std::move(s.reason_);
        }
        return *this;
    }

    explicit operator bool() const noexcept { return code_ == ErrCode::OK; }

    friend std::ostream& operator<<(std::ostream& os, const Status<>& s) {
        os << "{\"code\": " << static_cast<int>(s.code_) << ", \"reason\": \""
           << (s.reason_.empty() ? GetReason(s.code_) : s.reason_) << "\"}";
        return os;
    }

    bool OK() const { return code_ == ErrCode::OK; }

    void Set(ErrCode code) {
        code_ = code;
        reason_ = GetReason(code_);
    }

    void Set(ErrCode code, const char* reason) {
        code_ = code;
        reason_ = reason;
    }

    void Set(ErrCode code, const std::string& reason) {
        code_ = code;
        reason_ = reason;
    }

    void Set(int code) {
        code_ = static_cast<ErrCode>(code);
        reason_ = GetReason(code_);
    }

    void Set(int code, const std::string& reason) {
        code_ = static_cast<ErrCode>(code);
        reason_ = reason;
    }

    ErrCode Code() const { return code_; }

    const char* Reason() const { return reason_.c_str(); }

    std::string String() const {
        std::ostringstream oss;
        oss << "{\"code\": " << static_cast<int>(code_) << ", \"reason\": \""
            << (reason_.empty() ? GetReason(code_) : reason_) << "\"}";
        return oss.str();
    }
};

#if FMT_VERSION >= 90000

template <>
struct fmt::formatter<Status<>> : fmt::ostream_formatter {};

#endif

}  // namespace snail
