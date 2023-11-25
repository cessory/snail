#pragma once
#include <seastar/core/sstring.hh>
#include <string>
#include <tuple>

namespace snail {
namespace stream {

enum class ErrCode {
  OK = 0,
  ErrExistExtent = 10000,
  ErrNoFreeChunks = 10001,
  ErrNoFreeBlocks = 10002,
  ErrIOErr = 10003,
  ErrOverWrite = 10004,
  ErrInvalidArgs = 10006,
  ErrTooShort = 10007,
  ErrTooLarge = 10008,
  ErrNotFoundExtent = 10009,
  ErrInvalidChecksum = 10010,
  ErrDiskIDNotMatch = 10011,
  ErrDeletedChunk = 10012,
  ErrInvalidLease = 10013,
  ErrMissingLength = 10014,
  ErrChunkConflict = 10015,
  ErrSystem = 19999,
};

std::string GetReason(ErrCode code);

template <typename... T>
class Status;

template <typename T>
class Status<T> {
  ErrCode code_;
  std::string reason_;
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

  bool OK() const { return code_ == ErrCode::OK; }

  void Set(ErrCode code) {
    code_ = code;
    reason_ = GetReason(code_);
  }

  void Set(ErrCode code, const std::string& reason) {
    code_ = code;
    reason_ = reason;
  }

  ErrCode Code() const { return code_; }

  const std::string& Reason() const { return reason_; }

  T& Value() { return val_; }

  void SetValue(T val) { val_ = std::move(val); }
};

template <>
class Status<> {
  ErrCode code_;
  std::string reason_;

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

  bool OK() const { return code_ == ErrCode::OK; }

  void Set(ErrCode code) {
    code_ = code;
    reason_ = GetReason(code_);
  }

  void Set(ErrCode code, const std::string& reason) {
    code_ = code;
    reason_ = reason;
  }

  ErrCode Code() const { return code_; }

  const std::string& Reason() const { return reason_; }
};

seastar::sstring ToJsonString(ErrCode code, const std::string& reason = "");

}  // namespace stream
}  // namespace snail
