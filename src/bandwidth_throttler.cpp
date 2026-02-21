#include "bandwidth_throttler.hpp"
#include <chrono>
#include <algorithm>

namespace duckdb {

TokenBucket::TokenBucket(double bandwidth_bps_p, size_t burst_bytes_p)
    : bandwidth_bps(bandwidth_bps_p), burst_bytes(burst_bytes_p), tokens(burst_bytes_p),
      last_update(std::chrono::steady_clock::now()) {
}

void TokenBucket::RefillTokens() {
	auto now = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - last_update);
	
	// Calculate tokens to add based on elapsed time
	// bandwidth_bps is bytes per second, so tokens per microsecond = bandwidth_bps / 1e6
	double tokens_to_add = (bandwidth_bps / 1000000.0) * elapsed.count();
	
	size_t old_tokens = tokens;
	tokens = std::min(burst_bytes, tokens + static_cast<size_t>(tokens_to_add));
	last_update = now;
	
	// Notify waiting threads if tokens increased
	if (tokens > old_tokens) {
		cv.notify_all();
	}
}

void TokenBucket::WaitForTokens(size_t bytes) {
	if (bytes == 0) {
		return;
	}
	
	if (bandwidth_bps <= 0) {
		return; // No bandwidth limit, don't wait
	}
	
	std::unique_lock<std::mutex> lock(mutex);
	
	// Refill tokens based on elapsed time
	RefillTokens();
	
	// If we don't have enough tokens, wait
	while (tokens < bytes) {
		size_t tokens_needed = bytes - tokens;
		
		// Calculate when enough tokens will be available
		// tokens_needed / bandwidth_bps gives seconds needed
		double seconds_needed = static_cast<double>(tokens_needed) / bandwidth_bps;
		auto wait_until_time = last_update + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
			std::chrono::duration<double>(seconds_needed));
		
		// Wait until tokens are available or timeout
		cv.wait_until(lock, wait_until_time, [this, bytes] {
			RefillTokens();
			return tokens >= bytes;
		});
		
		// Refill again after waiting (in case we were woken up)
		RefillTokens();
	}
	
	// Consume the tokens
	tokens -= bytes;
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
	cv.notify_all();
}

} // namespace duckdb
