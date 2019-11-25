project(YCSB-C)
cmake_minimum_required(VERSION 2.8)

find_package(Threads REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/megakv/include
)

add_subdirectory(megakv)
add_subdirectory(core)
add_subdirectory(db)

set(EXTRA_LIBS ${EXTRA_LIBS} LibCore)
set(EXTRA_LIBS ${EXTRA_LIBS} LibDB)
set(EXTRA_LIBS ${EXTRA_LIBS} ${CMAKE_THREAD_LIBS_INIT})
set(EXTRA_LIBS ${EXTRA_LIBS} libmegakv)

add_executable(ycsbc ycsbc.cc)
target_link_libraries(ycsbc ${EXTRA_LIBS})
