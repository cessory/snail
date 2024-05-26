add_definitions(-DSEASTAR_TESTING_MAIN -DRAFT_UT_TEST)

add_executable(confchange_test confchange_test.cc $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(confchange_test Seastar::seastar Seastar::seastar_testing)

add_executable(storage_test storage_test.cc $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(storage_test Seastar::seastar Seastar::seastar_testing)

add_executable(inflights_test inflights_test.cc $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(inflights_test Seastar::seastar Seastar::seastar_testing)

add_executable(log_unstable_test log_unstable_test.cc $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(log_unstable_test Seastar::seastar Seastar::seastar_testing)

add_library(raft_test_obj OBJECT raft_common_test.cc)
target_link_libraries(raft_test_obj Seastar::seastar Seastar::seastar_testing)

add_executable(raft_paper_test raft_paper_test.cc $<TARGET_OBJECTS:raft_test_obj> $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(raft_paper_test Seastar::seastar Seastar::seastar_testing)

add_executable(raft_flow_control_test raft_flow_control_test.cc $<TARGET_OBJECTS:raft_test_obj> $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(raft_flow_control_test Seastar::seastar Seastar::seastar_testing)

add_executable(raft_snap_test raft_snap_test.cc $<TARGET_OBJECTS:raft_test_obj> $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(raft_snap_test Seastar::seastar Seastar::seastar_testing)

add_executable(raft_log_test raft_log_test.cc $<TARGET_OBJECTS:raft_test_obj> $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(raft_log_test Seastar::seastar Seastar::seastar_testing)

#add_executable(raft_test raft_test.cc $<TARGET_OBJECTS:raft_test_obj> $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
#target_link_libraries(raft_test common gtest Seastar::seastar Seastar::seastar_testing)
#
add_executable(raft_proto_test raft_proto_test.cc $<TARGET_OBJECTS:raft_test_obj> $<TARGET_OBJECTS:RAFT_OBJS> $<TARGET_OBJECTS:UTIL_OBJS>)
target_link_libraries(raft_proto_test Seastar::seastar Seastar::seastar_testing)
