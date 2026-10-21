PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=latency_injection_fs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Autoloading and installation are runtime settings in DuckDB v2.0.
BUILD_FLAGS := $(filter-out -DENABLE_EXTENSION_AUTOLOADING=% -DENABLE_EXTENSION_AUTOINSTALL=%,$(BUILD_FLAGS))

format-all: format-fix
	find test/unittest -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
	cmake-format -i test/unittest/CMakeLists.txt

test_reldebug_unit:
	./build/reldebug/test/unittest_latency_injection

.PHONY: format-all test_reldebug_unit
