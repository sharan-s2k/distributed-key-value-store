cmake_minimum_required(VERSION 3.16)
project(distributed_key_value_store LANGUAGES CXX)

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

add_executable(raft-simulator apps/raft_simulator_main.cpp)
target_link_libraries(raft-simulator PRIVATE raft_core)

add_executable(raft-tests tests/test_main.cpp)
target_link_libraries(raft-tests PRIVATE raft_core)

enable_testing()
add_test(NAME raft-tests COMMAND raft-tests)

option(RAFTKV_ENABLE_GRPC "Build the real multi-process gRPC runtime" ON)

if(RAFTKV_ENABLE_GRPC)
    find_package(Protobuf CONFIG QUIET)
    find_package(gRPC CONFIG QUIET)

    if(Protobuf_FOUND AND gRPC_FOUND)
        set(PROTO_FILES
            ${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto
            ${CMAKE_CURRENT_SOURCE_DIR}/proto/kv.proto
        )
        set(PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
        file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

        set(PROTO_SRCS)
        set(PROTO_HDRS)
        set(GRPC_SRCS)
        set(GRPC_HDRS)

        foreach(PROTO_FILE ${PROTO_FILES})
            get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
            list(APPEND PROTO_SRCS ${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.cc)
            list(APPEND PROTO_HDRS ${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.h)
            list(APPEND GRPC_SRCS ${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc)
            list(APPEND GRPC_HDRS ${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h)

            add_custom_command(
                OUTPUT
                    ${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.cc
                    ${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.h
                    ${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc
                    ${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h
                COMMAND protobuf::protoc
                ARGS
                    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
                    --cpp_out=${PROTO_GENERATED_DIR}
                    --grpc_out=${PROTO_GENERATED_DIR}
                    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
                    ${PROTO_FILE}
                DEPENDS ${PROTO_FILE}
                VERBATIM
            )
        endforeach()

        add_library(raft_proto ${PROTO_SRCS} ${GRPC_SRCS})
        target_include_directories(raft_proto PUBLIC ${PROTO_GENERATED_DIR})
        target_link_libraries(raft_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

        add_library(raft_grpc_runtime
            src/grpc/event_loop.cpp
            src/grpc/proto_converter.cpp
            src/grpc/grpc_transport.cpp
            src/grpc/node_runtime.cpp
            src/grpc/raft_rpc_service.cpp
            src/grpc/kv_rpc_service.cpp
        )
        target_include_directories(raft_grpc_runtime PUBLIC src ${PROTO_GENERATED_DIR})
        target_link_libraries(raft_grpc_runtime PUBLIC raft_core raft_proto gRPC::grpc++)

        add_executable(kv-server apps/kv_server_main.cpp)
        target_link_libraries(kv-server PRIVATE raft_grpc_runtime)

        add_executable(kv-client apps/kv_client_main.cpp)
        target_link_libraries(kv-client PRIVATE raft_proto gRPC::grpc++)

        message(STATUS "gRPC runtime enabled: building kv-server and kv-client")
    else()
        message(WARNING "gRPC/Protobuf not found. Building simulator only. Install both packages or configure with -DRAFTKV_ENABLE_GRPC=OFF.")
    endif()
endif()
