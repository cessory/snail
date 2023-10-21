cmake_minimum_required (VERSION 3.5)
project(chunk_server)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -O2")
find_package(Seastar REQUIRED)

add_definitions(-DSPDLOG_ACTIVE_LEVEL=0 -DSPDLOG_FMT_EXTERNAL)
add_library(obj OBJECT disk.cc status.cc http_service.cc)
target_link_libraries(obj isal Seastar::seastar)

add_executable (chunk_server main.cc)
target_link_libraries(chunk_server $<TARGET_OBJECTS:obj> isal Seastar::seastar)

add_executable (disk_test disk_test.cc)
target_link_libraries(disk_test $<TARGET_OBJECTS:obj> isal Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(disk_test PRIVATE SEASTAR_TESTING_MAIN)

