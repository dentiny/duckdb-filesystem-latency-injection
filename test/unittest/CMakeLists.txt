include_directories(${CMAKE_CURRENT_SOURCE_DIR})
get_filename_component(EXTENSION_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../.."
                       ABSOLUTE)
include_directories(${EXTENSION_SOURCE_DIR}/src/include)
include_directories(${EXTENSION_SOURCE_DIR}/src/utils/include)
include_directories(${DuckDB_SOURCE_DIR}/test/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)

file(GLOB_RECURSE LATENCY_INJECTION_FS_UNITTEST_OBJECTS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
     "${EXTENSION_SOURCE_DIR}/src/utils/mock_filesystem.cpp")

add_executable(unittest_latency_injection
               main.cpp ${LATENCY_INJECTION_FS_UNITTEST_OBJECTS})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest_latency_injection test_helpers duckdb
                        ${EXTENSION_NAME})
else()
  target_link_libraries(unittest_latency_injection test_helpers duckdb_static
                        ${EXTENSION_NAME})
endif()

if(MINGW OR WIN32)
  target_link_libraries(unittest_latency_injection crypt32)
endif()
