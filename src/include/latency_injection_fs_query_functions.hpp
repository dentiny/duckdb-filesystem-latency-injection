#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Get the table function to query wrapped latency injection filesystems.
TableFunction GetWrappedLatencyFsFunc();

} // namespace duckdb
