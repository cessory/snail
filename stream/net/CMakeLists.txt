cmake_minimum_required (VERSION 3.5)
project(net)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -O2")
find_package(Seastar REQUIRED)

include_directories(../)

add_definitions(-DSPDLOG_ACTIVE_LEVEL=0 -DSPDLOG_FMT_EXTERNAL)
add_library(obj OBJECT tcp_session.cc tcp_stream.cc tcp_connection.cc ../util/status.cc)
target_link_libraries(obj isal Seastar::seastar)

add_executable (client client.cc)
target_link_libraries(client $<TARGET_OBJECTS:obj> isal Seastar::seastar)

add_executable (server server.cc)
target_link_libraries(server $<TARGET_OBJECTS:obj> isal Seastar::seastar)

