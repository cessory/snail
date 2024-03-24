
add_executable (client client.cc)
target_link_libraries(client $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> isal Seastar::seastar)

add_executable (server server.cc)
target_link_libraries(server $<TARGET_OBJECTS:NET_OBJS> $<TARGET_OBJECTS:UTIL_OBJS> isal Seastar::seastar)

