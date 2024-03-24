cmake_minimum_required (VERSION 3.5)
project(stream)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -g -O2")
find_package(Seastar REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/include)
add_definitions(-DSPDLOG_FMT_EXTERNAL)

aux_source_directory(${PROJECT_SOURCE_DIR}/util UTIL_SRC)
aux_source_directory(${PROJECT_SOURCE_DIR}/proto PROTO_SRC)
aux_source_directory(${PROJECT_SOURCE_DIR}/net NET_SRC)

add_library(UTIL_OBJS OBJECT ${UTIL_SRC})
target_link_libraries(UTIL_OBJS Seastar::seastar)

add_library(PROTO_OBJS OBJECT ${PROTO_SRC})
target_link_libraries(UTIL_OBJS Seastar::seastar)

add_library(NET_OBJS OBJECT ${NET_SRC})
target_link_libraries(NET_OBJS Seastar::seastar)

add_subdirectory(extentnode)
add_subdirectory(demos)
#add_subdirectory(tests)


