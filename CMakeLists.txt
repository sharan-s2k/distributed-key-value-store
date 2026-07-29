cmake_minimum_required(VERSION 3.16)
project(raft_kv_simulator LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

add_library(raft_core
    src/raft.cpp
    src/simulator.cpp
    src/history.cpp
    src/storage.cpp
)
target_include_directories(raft_core PUBLIC src)

add_executable(raft-simulator src/main.cpp)
target_link_libraries(raft-simulator PRIVATE raft_core)

add_executable(raft-tests tests/test_main.cpp)
target_link_libraries(raft-tests PRIVATE raft_core)

enable_testing()
add_test(NAME raft-tests COMMAND raft-tests)