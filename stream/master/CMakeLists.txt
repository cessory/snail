set(src 
    "id_allocator.cc"
    "id_generator.cc"
    "extentnode_mgr.cc"
    "raft_server.cc"
    "raft_storage.cc"
    "raft_transport.cc"
    "storage.cc"
    "server.cc"
    "service.cc"
    "wal/wal.cc"
    )

add_library(MASTER_OBJS OBJECT ${src})
target_link_libraries(MASTER_OBJS Seastar::seastar)

add_executable(master main.cc)
target_link_libraries(master $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:MASTER_OBJS> protobuf::libprotobuf ${SPDK_LIBRARIES} ${ISAL_LIBRARIES} ${OPENSSL_LIBRARIES} rocksdb z bz2 lz4 uuid Seastar::seastar)

#add_subdirectory(tests)

