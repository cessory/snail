#pragma once
#include <stdlib.h>

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

#ifndef SPDLOG_HEADER_ONLY
#define SPDLOG_HEADER_ONLY 1
#endif

#ifdef LOG_INIT
#undef LOG_INIT
#endif

#ifdef LOG_SET_LEVEL
#undef LOG_SET_LEVEL
#endif

#ifdef LOG_TRACE
#undef LOG_TRACE
#endif

#ifdef LOG_DEBUG
#undef LOG_DEBUG
#endif

#ifdef LOG_INFO
#undef LOG_INFO
#endif

#ifdef LOG_WARN
#undef LOG_WARN
#endif

#ifdef LOG_ERROR
#undef LOG_ERROR
#endif

#ifdef LOG_FATAL
#undef LOG_FATAL
#endif

#define LOG_INIT(filename, max_file_size, max_files)       \
    spdlog::set_default_logger(spdlog::rotating_logger_mt( \
        "rotating log", filename, max_file_size, max_files))

// str_level value: trace, debug, info, warning, error, critical
#define LOG_SET_LEVEL(str_level) \
    spdlog::set_level(spdlog::level::from_str(str_level))

#define LOG_TRACE(...)                                                   \
    do {                                                                 \
        if (spdlog::should_log(spdlog::level::trace)) {                  \
            spdlog::default_logger_raw()->log(                           \
                spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                spdlog::level::trace, __VA_ARGS__);                      \
        }                                                                \
    } while (0)

#define LOG_DEBUG(...)                                                   \
    do {                                                                 \
        if (spdlog::should_log(spdlog::level::debug)) {                  \
            spdlog::default_logger_raw()->log(                           \
                spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                spdlog::level::debug, __VA_ARGS__);                      \
        }                                                                \
    } while (0)

#define LOG_INFO(...)                                                    \
    do {                                                                 \
        if (spdlog::should_log(spdlog::level::info)) {                   \
            spdlog::default_logger_raw()->log(                           \
                spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                spdlog::level::info, __VA_ARGS__);                       \
        }                                                                \
    } while (0)

#define LOG_WARN(...)                                                    \
    do {                                                                 \
        if (spdlog::should_log(spdlog::level::warn)) {                   \
            spdlog::default_logger_raw()->log(                           \
                spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                spdlog::level::warn, __VA_ARGS__);                       \
        }                                                                \
    } while (0)

#define LOG_ERROR(...)                                                   \
    do {                                                                 \
        if (spdlog::should_log(spdlog::level::err)) {                    \
            spdlog::default_logger_raw()->log(                           \
                spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                spdlog::level::err, __VA_ARGS__);                        \
        }                                                                \
    } while (0)

#define LOG_FATAL(...)                                               \
    do {                                                             \
        spdlog::default_logger_raw()->log(                           \
            spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
            spdlog::level::critical, __VA_ARGS__);                   \
        abort();                                                     \
    } while (0)

#define LOG_FATAL_THROW(...)                                         \
    do {                                                             \
        spdlog::default_logger_raw()->log(                           \
            spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
            spdlog::level::critical, __VA_ARGS__);                   \
        throw std::runtime_error(fmt::format(__VA_ARGS__));          \
    } while (0)

