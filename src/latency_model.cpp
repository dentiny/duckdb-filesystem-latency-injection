#include "latency_model.hpp"
#include <chrono>
#include <random>

namespace duckdb {

LatencyModel::LatencyModel(const LatencyConfig &config) : config(config) {
	// Seed generator with current time
	auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
	generator.seed(static_cast<unsigned int>(seed));
}

double LatencyModel::SampleLogNormal(double mean, double stddev) {
	std::lock_guard<std::mutex> lock(generator_mutex);
	std::lognormal_distribution<double> dist(mean, stddev);
	return dist(generator);
}

double LatencyModel::GetOperationLatency(OperationType op_type, size_t bytes) {
	if (!config.enabled) {
		return 0.0;
	}
	
	double base_latency = 0.0;
	
	switch (op_type) {
	case OperationType::LIST:
		base_latency = SampleLogNormal(config.list_mean_ms, config.list_stddev);
		break;
	case OperationType::STAT:
		base_latency = SampleLogNormal(config.stat_mean_ms, config.stat_stddev);
		break;
	case OperationType::METADATA_WRITE:
		base_latency = SampleLogNormal(config.metadata_write_mean_ms, config.metadata_write_stddev);
		break;
	case OperationType::READ:
		base_latency = SampleLogNormal(config.read_base_mean_ms, config.read_base_stddev);
		if (bytes > 0 && config.read_bytes_per_ms > 0) {
			base_latency += static_cast<double>(bytes) / config.read_bytes_per_ms;
		}
		break;
	case OperationType::WRITE:
		base_latency = SampleLogNormal(config.write_base_mean_ms, config.write_base_stddev);
		if (bytes > 0 && config.write_bytes_per_ms > 0) {
			base_latency += static_cast<double>(bytes) / config.write_bytes_per_ms;
		}
		break;
	}
	
	return base_latency;
}

} // namespace duckdb
