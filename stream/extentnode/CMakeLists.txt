aux_source_directory(. SRC)
add_executable (chunk_server ${SRC})
target_link_libraries(chunk_server $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> $<TARGET_OBJECTS:PROTO_OBJS> protobuf isal Seastar::seastar)

#add_executable (store_test store_test.cc)
#target_link_libraries(store_test $<TARGET_OBJECTS:obj> isal Seastar::seastar Seastar::seastar_testing)
#target_compile_definitions(store_test PRIVATE SEASTAR_TESTING_MAIN)

