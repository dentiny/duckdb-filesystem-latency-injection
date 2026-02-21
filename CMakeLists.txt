cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME latency_injection_fs)

find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(src/utils/include)

set(EXTENSION_SOURCES
    src/latency_injection_file_handle.cpp
    src/latency_injection_file_system.cpp
    src/latency_model.cpp
    src/latency_injection_fs_extension.cpp
    src/latency_injection_fs_instance_state.cpp
    src/latency_injection_fs_query_functions.cpp
    src/utils/fake_filesystem.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

if(${BUILD_UNITTESTS})
  add_subdirectory(test/unittest)
endif()
