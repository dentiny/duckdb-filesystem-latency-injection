#include "latency_model.hpp"
#include <cmath>
#include <random>

namespace duckdb {

LatencyModel::LatencyModel(const LatencyConfig &config) : config(config) {
	// Precompute log-normal parameters once so that GetOperationLatency
	// never has to call std::log / std::sqrt on the hot path.
	list_params = ComputeParams(config.list_mean_ms, config.list_stddev);
	stat_params = ComputeParams(config.stat_mean_ms, config.stat_stddev);
	metadata_write_params = ComputeParams(config.metadata_write_mean_ms, config.metadata_write_stddev);
	read_params = ComputeParams(config.read_base_mean_ms, config.read_base_stddev);
	write_params = ComputeParams(config.write_base_mean_ms, config.write_base_stddev);
}

LatencyModel::PrecomputedParams LatencyModel::ComputeParams(double mean, double stddev) {
	PrecomputedParams params;
	if (mean <= 0.0 || stddev < 0.0) {
		return params; // valid stays false
	}

	double variance = stddev * stddev;
	double mean_squared = mean * mean;

	params.log_mean = std::log(mean_squared / std::sqrt(variance + mean_squared));
	params.log_stddev = std::sqrt(std::log(1.0 + variance / mean_squared));
	params.valid = true;
	return params;
}

std::mt19937 &LatencyModel::GetThreadLocalGenerator() {
	thread_local std::mt19937 generator(std::random_device {}());
	return generator;
}

double LatencyModel::SampleFrom(const PrecomputedParams &params) {
	if (!params.valid) {
		return 0.0;
	}
	std::lognormal_distribution<double> dist(params.log_mean, params.log_stddev);
	return dist(GetThreadLocalGenerator());
}

double LatencyModel::GetOperationLatency(OperationType op_type, size_t bytes) {
	if (!config.enabled) {
		return 0.0;
	}

	double base_latency = 0.0;

	switch (op_type) {
	case OperationType::LIST:
		base_latency = SampleFrom(list_params);
		break;
	case OperationType::STAT:
		base_latency = SampleFrom(stat_params);
		break;
	case OperationType::METADATA_WRITE:
		base_latency = SampleFrom(metadata_write_params);
		break;
	case OperationType::READ:
		base_latency = SampleFrom(read_params);
		if (bytes > 0 && config.read_bytes_per_ms > 0) {
			base_latency += static_cast<double>(bytes) / config.read_bytes_per_ms;
		}
		break;
	case OperationType::WRITE:
		base_latency = SampleFrom(write_params);
		if (bytes > 0 && config.write_bytes_per_ms > 0) {
			base_latency += static_cast<double>(bytes) / config.write_bytes_per_ms;
		}
		break;
	}

	return base_latency;
}

} // namespace duckdb
