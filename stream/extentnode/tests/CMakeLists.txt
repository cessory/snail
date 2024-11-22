add_definitions(-DSEASTAR_TESTING_MAIN -DEXTENTNODE_UT_TEST)

add_executable (store_test store_test.cc)
target_link_libraries(store_test $<TARGET_OBJECTS:OBJS> $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> protobuf::libprotobuf ${SPDK_LIBRARIES} ${ISAL_LIBRARIES} ${OPENSSL_LIBRARIES} uuid Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(store_test PRIVATE SEASTAR_TESTING_MAIN)

add_executable (block_cache_test block_cache_test.cc)
target_link_libraries(block_cache_test $<TARGET_OBJECTS:OBJS> $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> protobuf::libprotobuf ${SPDK_LIBRARIES} ${ISAL_LIBRARIES} ${OPENSSL_LIBRARIES} uuid Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(block_cache_test PRIVATE SEASTAR_TESTING_MAIN)

add_executable (service_test service_test.cc)
target_link_libraries(service_test $<TARGET_OBJECTS:OBJS> $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> protobuf::libprotobuf ${SPDK_LIBRARIES} ${ISAL_LIBRARIES} ${OPENSSL_LIBRARIES} uuid Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(service_test PRIVATE SEASTAR_TESTING_MAIN)

add_executable (journal_test journal_test.cc)
target_link_libraries(journal_test $<TARGET_OBJECTS:OBJS> $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> protobuf::libprotobuf ${SPDK_LIBRARIES} ${ISAL_LIBRARIES} ${OPENSSL_LIBRARIES} uuid Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(journal_test PRIVATE SEASTAR_TESTING_MAIN)
