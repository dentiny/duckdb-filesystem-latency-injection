#pragma once

#include "duckdb/common/common.hpp"
#include <random>
#include <mutex>

namespace duckdb {

enum class OperationType {
	LIST,
	STAT,
	READ,
	WRITE
};

struct LatencyConfig {
	// Log-normal distribution parameters per operation
	double list_mean_ms = 2.0;
	double list_stddev = 0.5;
	double stat_mean_ms = 1.0;
	double stat_stddev = 0.3;
	double read_base_mean_ms = 5.0;
	double read_base_stddev = 1.0;
	double write_base_mean_ms = 10.0;
	double write_base_stddev = 2.0;
	// Size-dependent latency (latency per byte)
	double read_bytes_per_ms = 1000000.0;  // bytes per millisecond
	double write_bytes_per_ms = 500000.0;  // bytes per millisecond
	// Bandwidth limits
	double read_bandwidth_bps = 100000000.0;   // bytes per second
	double write_bandwidth_bps = 50000000.0;   // bytes per second
	size_t read_burst_bytes = 10000000;        // 10MB burst
	size_t write_burst_bytes = 5000000;        // 5MB burst
	// Enable/disable latency injection
	bool enabled = true;
};

class LatencyModel {
public:
	explicit LatencyModel(const LatencyConfig &config);
	
	// Sample latency from log-normal distribution for a given operation
	double GetOperationLatency(OperationType op_type, size_t bytes = 0);
	
	// Check if latency injection is enabled
	bool IsEnabled() const { return config.enabled; }
	
	// Get configuration
	const LatencyConfig &GetConfig() const { return config; }

private:
	LatencyConfig config;
	std::mt19937 generator;
	std::mutex generator_mutex;
	
	// Sample from log-normal distribution
	double SampleLogNormal(double mean, double stddev);
};

} // namespace duckdb
