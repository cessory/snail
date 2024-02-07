cmake_minimum_required (VERSION 3.5)
project(chunk_server)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -O2")
find_package(Seastar REQUIRED)

include_directories(../)
add_definitions(-DSPDLOG_FMT_EXTERNAL)
add_library(obj OBJECT store.cc ../util/status.cc device.cc log.cc super_block.cc types.cc)
target_link_libraries(obj isal Seastar::seastar)

add_executable (chunk_server main.cc)
target_link_libraries(chunk_server $<TARGET_OBJECTS:obj> isal Seastar::seastar)

add_executable (store_test store_test.cc)
target_link_libraries(store_test $<TARGET_OBJECTS:obj> isal Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(store_test PRIVATE SEASTAR_TESTING_MAIN)

