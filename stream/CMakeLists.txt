cmake_minimum_required (VERSION 3.5)
project(stream)

option(COVERAGE "enable coverage" OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
IF (COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -fprofile-arcs -ftest-coverage")
ELSE()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -O2")
ENDIF()
find_package(Seastar REQUIRED)
find_package(Protobuf 3 REQUIRED)
find_package(PkgConfig)
pkg_check_modules(ISAL REQUIRED libisal)
pkg_check_modules(SPDK spdk_nvme spdk_env_dpdk)
if (SPDK_FOUND)
    pkg_check_modules(OPENSSL REQUIRED openssl)
endif()
message(STATUS "SPDK libs:" SPDK_LIBRARIES)

FILE(GLOB util_srcs ${PROJECT_SOURCE_DIR}/util/*.cc)
FILE(GLOB net_srcs ${PROJECT_SOURCE_DIR}/net/*.cc)
FILE(GLOB raft_srcs ${PROJECT_SOURCE_DIR}/raft/*.cc)

FILE(GLOB proto_files ${PROJECT_SOURCE_DIR}/proto/*.proto)
FOREACH(proto_file ${proto_files})
    STRING(REGEX REPLACE "[^/]proto" "" proto_file_name ${proto_file})
    LIST(APPEND proto_srcs "${proto_file_name}.pb.cc")
    ADD_CUSTOM_COMMAND(
        OUTPUT "${proto_file_name}.pb.h" "${proto_file_name}.pb.cc"
        COMMAND protoc --proto_path=${PROJECT_SOURCE_DIR}/proto --cpp_out=${PROJECT_SOURCE_DIR}/proto/ ${proto_file}
		DEPENDS ${proto_file}
        )
ENDFOREACH(proto_file)

include_directories(${PROJECT_SOURCE_DIR})
add_definitions(-DSPDLOG_FMT_EXTERNAL)
if (${SPDK_FOUND})
    add_definitions(-DHAS_SPDK)
endif()

add_library(UTIL_OBJS OBJECT ${util_srcs})
target_link_libraries(UTIL_OBJS Seastar::seastar)

add_library(PROTO_OBJS OBJECT ${proto_srcs})
target_link_libraries(PROTO_OBJS Seastar::seastar)

add_library(NET_OBJS OBJECT ${net_srcs})
target_link_libraries(NET_OBJS Seastar::seastar)

add_library(RAFT_OBJS OBJECT ${raft_srcs})
target_link_libraries(RAFT_OBJS Seastar::seastar)

add_subdirectory(extentnode)
add_subdirectory(demos)
add_subdirectory(raft)


