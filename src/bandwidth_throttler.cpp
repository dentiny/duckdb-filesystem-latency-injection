#include "bandwidth_throttler.hpp"
#include <thread>
#include <chrono>
#include <algorithm>

namespace duckdb {

TokenBucket::TokenBucket(double bandwidth_bps, size_t burst_bytes)
    : bandwidth_bps(bandwidth_bps), burst_bytes(burst_bytes), tokens(burst_bytes),
      last_update(std::chrono::steady_clock::now()) {
}

void TokenBucket::RefillTokens() {
	auto now = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - last_update);
	
	// Calculate tokens to add based on elapsed time
	// bandwidth_bps is bytes per second, so tokens per microsecond = bandwidth_bps / 1e6
	double tokens_to_add = (bandwidth_bps / 1000000.0) * elapsed.count();
	
	tokens = std::min(burst_bytes, tokens + static_cast<size_t>(tokens_to_add));
	last_update = now;
}

void TokenBucket::WaitForTokens(size_t bytes) {
	if (bytes == 0) {
		return;
	}
	
	std::unique_lock<std::mutex> lock(mutex);
	
	// Refill tokens based on elapsed time
	RefillTokens();
	
	// If we don't have enough tokens, wait
	while (tokens < bytes) {
		size_t tokens_needed = bytes - tokens;
		
		// Calculate how long to wait: tokens_needed / bandwidth_bps seconds, convert to milliseconds
		double wait_time_ms = (static_cast<double>(tokens_needed) / bandwidth_bps) * 1000.0;
		
		// Ensure minimum wait time to avoid busy waiting, but cap at reasonable maximum
		if (wait_time_ms < 0.1) {
			wait_time_ms = 0.1;
		}
		if (wait_time_ms > 1000.0) {
			wait_time_ms = 1000.0; // Cap at 1 second to prevent extremely long waits
		}
		
		lock.unlock();
		std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(wait_time_ms));
		lock.lock();
		
		// Refill again after waiting
		RefillTokens();
	}
	
	// Consume the tokens
	tokens -= bytes;
}

void TokenBucket::ConsumeTokens(size_t bytes) {
	std::lock_guard<std::mutex> lock(mutex);
	RefillTokens();
	if (tokens >= bytes) {
		tokens -= bytes;
	} else {
		// This shouldn't happen if WaitForTokens was called first
		// But handle it gracefully
		tokens = 0;
	}
}

size_t TokenBucket::GetAvailableTokens() {
	std::lock_guard<std::mutex> lock(mutex);
	RefillTokens();
	return tokens;
}

void TokenBucket::Reset() {
	std::lock_guard<std::mutex> lock(mutex);
	tokens = burst_bytes;
	last_update = std::chrono::steady_clock::now();
}

} // namespace duckdb
