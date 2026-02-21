#pragma once

#include "duckdb/common/common.hpp"
#include <chrono>
#include <mutex>

namespace duckdb {

class TokenBucket {
public:
	// bandwidth_bps: bytes per second
	// burst_bytes: maximum tokens that can accumulate
	TokenBucket(double bandwidth_bps, size_t burst_bytes);
	
	// Wait until enough tokens are available, then consume them
	void WaitForTokens(size_t bytes);
	
	// Consume tokens (assumes tokens are already available)
	void ConsumeTokens(size_t bytes);
	
	// Get current available tokens
	size_t GetAvailableTokens();
	
	// Reset the token bucket
	void Reset();

private:
	double bandwidth_bps;      // bytes per second
	size_t burst_bytes;         // maximum tokens
	size_t tokens;              // current tokens
	std::chrono::steady_clock::time_point last_update;
	std::mutex mutex;
	
	// Refill tokens based on elapsed time
	void RefillTokens();
};

} // namespace duckdb
