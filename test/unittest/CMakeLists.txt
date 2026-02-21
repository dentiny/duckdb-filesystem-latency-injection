include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/test/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)

file(GLOB_RECURSE QUACK_UNITTEST_OBJECTS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(unittest_latency_injection main.cpp ${QUACK_UNITTEST_OBJECTS})

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
