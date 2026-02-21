#pragma once

namespace duckdb {

// List operation defaults
constexpr double LATENCY_INJECT_FS_DEFAULT_LIST_MEAN_MS = 2.0;
constexpr double LATENCY_INJECT_FS_DEFAULT_LIST_STDDEV = 0.5;

// Stat operation defaults
constexpr double LATENCY_INJECT_FS_DEFAULT_STAT_MEAN_MS = 1.0;
constexpr double LATENCY_INJECT_FS_DEFAULT_STAT_STDDEV = 0.3;

// Metadata write operation defaults
constexpr double LATENCY_INJECT_FS_DEFAULT_METADATA_WRITE_MEAN_MS = 3.0;
constexpr double LATENCY_INJECT_FS_DEFAULT_METADATA_WRITE_STDDEV = 0.5;

// Read operation defaults
constexpr double LATENCY_INJECT_FS_DEFAULT_READ_BASE_MEAN_MS = 5.0;
constexpr double LATENCY_INJECT_FS_DEFAULT_READ_BASE_STDDEV = 1.0;
constexpr double LATENCY_INJECT_FS_DEFAULT_READ_BYTES_PER_MS = 1000000.0; // bytes per millisecond

// Write operation defaults
constexpr double LATENCY_INJECT_FS_DEFAULT_WRITE_BASE_MEAN_MS = 10.0;
constexpr double LATENCY_INJECT_FS_DEFAULT_WRITE_BASE_STDDEV = 2.0;
constexpr double LATENCY_INJECT_FS_DEFAULT_WRITE_BYTES_PER_MS = 500000.0; // bytes per millisecond

// Enable/disable default
constexpr bool LATENCY_INJECT_FS_DEFAULT_ENABLED = true;

} // namespace duckdb
