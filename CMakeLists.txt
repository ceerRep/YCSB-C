project(YCSB-C)
cmake_minimum_required(VERSION 2.8)

set (CMAKE_CXX_STANDARD 17)

find_package (Seastar REQUIRED)
find_package (Threads REQUIRED)

include_directories(
    ${CMAKE_SOURCE_DIR}
)

set (SOURCE
    core/core_workload.cc
    ycsbc.cc)


add_library(ycsb STATIC ${SOURCE})
target_link_libraries(ycsb Seastar::seastar ${CMAKE_THREAD_LIBS_INIT})

if (YCSB_TEST)
    add_executable(basic_test ycsbc_test.cc)
    target_link_libraries(basic_test ycsb Seastar::seastar ${CMAKE_THREAD_LIBS_INIT})
endif (YCSB_TEST)
