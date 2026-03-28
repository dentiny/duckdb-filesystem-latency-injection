#pragma once

#include "duckdb/common/common.hpp"
#include "latency_injection_fs_constants.hpp"

#include <random>

namespace duckdb {

enum class OperationType { LIST, STAT, READ, WRITE, METADATA_WRITE };

struct LatencyConfig {
	// Log-normal distribution parameters per operation
	double list_mean_ms = LATENCY_INJECT_FS_DEFAULT_LIST_MEAN_MS;
	double list_stddev = LATENCY_INJECT_FS_DEFAULT_LIST_STDDEV;
	double stat_mean_ms = LATENCY_INJECT_FS_DEFAULT_STAT_MEAN_MS;
	double stat_stddev = LATENCY_INJECT_FS_DEFAULT_STAT_STDDEV;
	double metadata_write_mean_ms = LATENCY_INJECT_FS_DEFAULT_METADATA_WRITE_MEAN_MS;
	double metadata_write_stddev = LATENCY_INJECT_FS_DEFAULT_METADATA_WRITE_STDDEV;
	double read_base_mean_ms = LATENCY_INJECT_FS_DEFAULT_READ_BASE_MEAN_MS;
	double read_base_stddev = LATENCY_INJECT_FS_DEFAULT_READ_BASE_STDDEV;
	double write_base_mean_ms = LATENCY_INJECT_FS_DEFAULT_WRITE_BASE_MEAN_MS;
	double write_base_stddev = LATENCY_INJECT_FS_DEFAULT_WRITE_BASE_STDDEV;
	// Size-dependent latency (latency per byte)
	double read_bytes_per_ms = LATENCY_INJECT_FS_DEFAULT_READ_BYTES_PER_MS;   // bytes per millisecond
	double write_bytes_per_ms = LATENCY_INJECT_FS_DEFAULT_WRITE_BYTES_PER_MS; // bytes per millisecond
	// Enable/disable latency injection
	bool enabled = LATENCY_INJECT_FS_DEFAULT_ENABLED;
};

class LatencyModel {
public:
	explicit LatencyModel(const LatencyConfig &config);

	// Sample latency from log-normal distribution for a given operation
	double GetOperationLatency(OperationType op_type, size_t bytes = 0);

	// Check if latency injection is enabled
	bool IsEnabled() const {
		return config.enabled;
	}

	// Get configuration
	const LatencyConfig &GetConfig() const {
		return config;
	}

private:
	LatencyConfig config;

	struct PrecomputedParams {
		double log_mean = 0.0;
		double log_stddev = 0.0;
		bool valid = false;
	};

	PrecomputedParams list_params;
	PrecomputedParams stat_params;
	PrecomputedParams metadata_write_params;
	PrecomputedParams read_params;
	PrecomputedParams write_params;

	// Convert user-facing (mean, stddev) into log-normal distribution parameters.
	static PrecomputedParams ComputeParams(double mean, double stddev);

	// Sample from precomputed parameters using a thread-local generator.
	static double SampleFrom(const PrecomputedParams &params);

	// Thread-local RNG.
	static std::mt19937 &GetThreadLocalGenerator();
};

} // namespace duckdb
