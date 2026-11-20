cmake_minimum_required(VERSION 3.25)
project(distributed_storage_engine VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

set(KV_SANITIZE "" CACHE STRING "Sanitizer: address, thread, or empty")
option(KV_WERROR "Treat warnings as errors" OFF)
option(KV_WITH_ROCKSDB "Build the RocksDB baseline engine for benchmarks" ON)
option(KV_BUILD_TESTS "Build unit and integration tests" ON)

add_compile_options(-Wall -Wextra -Wno-unused-parameter)
if(KV_WERROR)
  add_compile_options(-Werror)
endif()
if(KV_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(KV_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()
if(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
  # The default -O2 leaves measurable throughput on the table for the hot paths.
  string(REPLACE "-O2" "-O3" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
endif()

find_package(Threads REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()
find_package(gRPC CONFIG REQUIRED)

# ---------------------------------------------------------------------------
# Generated protobuf / gRPC code
# ---------------------------------------------------------------------------
# protoc is invoked directly rather than through protobuf_generate(), whose
# output layout differs between CMake versions (Ubuntu's protobuf ships no
# CMake package, so the FindProtobuf module's copy is used).
set(KV_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(KV_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)
set(KV_PROTO_SRCS)
foreach(name kv raft snapshot)
  set(out ${KV_GEN_DIR}/proto/${name})
  add_custom_command(
    OUTPUT ${out}.pb.cc ${out}.pb.h ${out}.grpc.pb.cc ${out}.grpc.pb.h
    COMMAND ${CMAKE_COMMAND} -E make_directory ${KV_GEN_DIR}/proto
    COMMAND $<TARGET_FILE:protobuf::protoc> --proto_path=${KV_PROTO_DIR}
            --cpp_out=${KV_GEN_DIR}/proto --grpc_out=${KV_GEN_DIR}/proto
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            ${KV_PROTO_DIR}/${name}.proto
    DEPENDS ${KV_PROTO_DIR}/${name}.proto protobuf::protoc gRPC::grpc_cpp_plugin
    COMMENT "protoc ${name}.proto")
  list(APPEND KV_PROTO_SRCS ${out}.pb.cc ${out}.grpc.pb.cc)
endforeach()
add_library(kv_proto STATIC ${KV_PROTO_SRCS})
target_link_libraries(kv_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
# Our code includes "proto/kv.pb.h"; generated code includes "kv.pb.h".
target_include_directories(kv_proto PUBLIC ${KV_GEN_DIR} ${KV_GEN_DIR}/proto)
target_compile_options(kv_proto PRIVATE -w)

# ---------------------------------------------------------------------------
# Core libraries
# ---------------------------------------------------------------------------
# Ubuntu 24.04 ships Abseil 20220623 (no absl/crc or absl/log), so CRC32C and
# logging live in src/common.
set(ABSL_LIBS absl::status absl::statusor absl::strings absl::str_format absl::synchronization
              absl::time absl::btree absl::flat_hash_map absl::random_random
              absl::flags absl::flags_parse)

add_library(kv_storage STATIC
  src/common/crc32c.cc
  src/common/logging.cc
  src/common/histogram.cc
  src/common/fs.cc
  src/storage/write_batch.cc
  src/storage/wal.cc
  src/storage/wal_reader.cc
  src/storage/memtable.cc
  src/storage/applier.cc
  src/storage/snapshot.cc
  src/storage/engine.cc)
target_include_directories(kv_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kv_storage PUBLIC kv_proto ${ABSL_LIBS} Threads::Threads)
# Objects include generated headers; make sure protoc has run first (Ninja
# does not order compilation after a linked library's custom commands).
add_dependencies(kv_storage kv_proto)

add_library(kv_raft STATIC
  src/raft/raft_node.cc
  src/raft/raft_state_machine.cc
  src/raft/grpc_transport.cc)
target_link_libraries(kv_raft PUBLIC kv_storage)

add_library(kv_server STATIC
  src/server/kv_service.cc
  src/server/metrics_http.cc)
target_link_libraries(kv_server PUBLIC kv_raft)

add_library(kv_client STATIC src/client/kv_client.cc)
target_include_directories(kv_client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kv_client PUBLIC kv_proto ${ABSL_LIBS})
add_dependencies(kv_client kv_proto)

add_executable(kvd src/server/main.cc)
target_link_libraries(kvd PRIVATE kv_server)

add_executable(kvctl src/client/kvctl.cc)
target_link_libraries(kvctl PRIVATE kv_client)

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
set(KV_BENCH_SOURCES bench/kvbench.cc)
if(KV_WITH_ROCKSDB)
  find_package(RocksDB CONFIG QUIET)
  if(RocksDB_FOUND)
    set(KV_ROCKSDB_TARGET RocksDB::rocksdb)
  else()
    find_library(KV_ROCKSDB_LIB rocksdb)
    if(KV_ROCKSDB_LIB)
      set(KV_ROCKSDB_TARGET ${KV_ROCKSDB_LIB})
    endif()
  endif()
endif()
add_executable(kvbench ${KV_BENCH_SOURCES})
target_link_libraries(kvbench PRIVATE kv_storage)
if(KV_ROCKSDB_TARGET)
  target_compile_definitions(kvbench PRIVATE KV_HAVE_ROCKSDB=1)
  target_link_libraries(kvbench PRIVATE ${KV_ROCKSDB_TARGET})
  message(STATUS "kvbench: RocksDB baseline enabled (${KV_ROCKSDB_TARGET})")
endif()

add_executable(kvload bench/kvload.cc)
target_link_libraries(kvload PRIVATE kv_client kv_storage)

add_executable(kvchaos bench/kvchaos.cc)
target_link_libraries(kvchaos PRIVATE kv_client kv_storage)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(KV_BUILD_TESTS)
  enable_testing()
  find_package(GTest REQUIRED)
  add_executable(kv_tests
    tests/write_batch_test.cc
    tests/wal_test.cc
    tests/memtable_test.cc
    tests/snapshot_test.cc
    tests/engine_test.cc
    tests/histogram_test.cc
    tests/raft_test.cc)
  target_link_libraries(kv_tests PRIVATE kv_server kv_client GTest::gtest GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(kv_tests DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 300)
endif()
