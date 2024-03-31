add_executable (store_test store_test.cc)
target_link_libraries(store_test $<TARGET_OBJECTS:OBJS> $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> protobuf isal Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(store_test PRIVATE SEASTAR_TESTING_MAIN)

add_executable (block_cache_test block_cache_test.cc)
target_link_libraries(block_cache_test $<TARGET_OBJECTS:OBJS> $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> protobuf isal Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(block_cache_test PRIVATE SEASTAR_TESTING_MAIN)
