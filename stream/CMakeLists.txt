cmake_minimum_required (VERSION 3.5)
project(stream)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -O2")
find_package(Seastar REQUIRED)
find_package(Protobuf 3 REQUIRED)

FILE(GLOB util_srcs ${PROJECT_SOURCE_DIR}/util/*.cc)
FILE(GLOB net_srcs ${PROJECT_SOURCE_DIR}/net/*.cc)

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

add_library(UTIL_OBJS OBJECT ${util_srcs})
target_link_libraries(UTIL_OBJS Seastar::seastar)

add_library(PROTO_OBJS OBJECT ${proto_srcs})
target_link_libraries(PROTO_OBJS Seastar::seastar)

add_library(NET_OBJS OBJECT ${net_srcs})
target_link_libraries(NET_OBJS Seastar::seastar)

add_subdirectory(extentnode)
add_subdirectory(demos)
#add_subdirectory(tests)


