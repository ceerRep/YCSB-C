project(YCSB-C)
cmake_minimum_required(VERSION 2.8)

find_package(Threads REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

include_directories(
    ${CMAKE_SOURCE_DIR}
)

set (SOURCE
    core/core_workload.cc
    ycsbc.cc)

add_library(ycsb ${SOURCE})
target_link_libraries(ycsb ${CMAKE_THREAD_LIBS_INIT})

if (YCSB_TEST)
    add_executable(basic_test ycsbc_test.cc)
    target_link_libraries(basic_test ycsb ${CMAKE_THREAD_LIBS_INIT})
endif (YCSB_TEST)
