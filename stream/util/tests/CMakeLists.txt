add_executable (async_thread_test async_thread_test.cc)
target_link_libraries(async_thread_test $<TARGET_OBJECTS:UTIL_OBJS> Seastar::seastar Seastar::seastar_testing)
target_compile_definitions(async_thread_test PRIVATE SEASTAR_TESTING_MAIN)

