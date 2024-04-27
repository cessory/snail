set(src 
    "device.cc"
    "log.cc"
    "service.cc"
    "store.cc"
    "super_block.cc"
    "tcp_server.cc"
    "types.cc"
    )

add_library(OBJS OBJECT ${src})
target_link_libraries(OBJS Seastar::seastar)

add_executable(extentnode main.cc)
target_link_libraries(extentnode $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> $<TARGET_OBJECTS:OBJS> protobuf::libprotobuf spdk_nvme spdk_sock spdk_sock_posix spdk_trace spdk_rpc spdk_jsonrpc spdk_json spdk_util spdk_vfio_user spdk_log isal Seastar::seastar)

add_subdirectory(tests)

