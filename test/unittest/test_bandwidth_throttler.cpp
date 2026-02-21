#include "catch/catch.hpp"

#include "bandwidth_throttler.hpp"
#include <chrono>
#include <thread>

using namespace duckdb; // NOLINT

TEST_CASE("Test TokenBucket construction", "[bandwidth throttler test]") {
	TokenBucket bucket(1000.0, 100);
	// Should be able to construct without issues
	REQUIRE(true);
}

TEST_CASE("Test TokenBucket WaitForTokens with zero bytes", "[bandwidth throttler test]") {
	TokenBucket bucket(1000.0, 100);
	
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(0);
	auto end_time = std::chrono::high_resolution_clock::now();
	
	// Should return immediately
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 10);
}

TEST_CASE("Test TokenBucket WaitForTokens with zero bandwidth", "[bandwidth throttler test]") {
	TokenBucket bucket(0.0, 100);
	
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(1000);
	auto end_time = std::chrono::high_resolution_clock::now();
	
	// Should return immediately when bandwidth is zero
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 10);
}

TEST_CASE("Test TokenBucket WaitForTokens with sufficient initial tokens", "[bandwidth throttler test]") {
	// 1000 bytes/second, 1000 byte burst - should have 1000 tokens initially
	TokenBucket bucket(1000.0, 1000);
	
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(500); // Request less than initial burst
	auto end_time = std::chrono::high_resolution_clock::now();
	
	// Should return immediately since we have enough tokens
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 10);
}

TEST_CASE("Test TokenBucket WaitForTokens waits when tokens insufficient", "[bandwidth throttler test]") {
	// 1000 bytes/second, 100 byte burst
	TokenBucket bucket(1000.0, 100);
	
	// Consume all initial tokens
	bucket.WaitForTokens(100);
	
	// Now request more tokens - should wait
	// 200 bytes at 1000 bytes/second = 0.2 seconds = 200ms
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(200);
	auto end_time = std::chrono::high_resolution_clock::now();
	
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	// Should wait approximately 200ms (allow some tolerance for system scheduling)
	REQUIRE(duration_ms >= 180);
	REQUIRE(duration_ms < 500); // Should not take too long
}

TEST_CASE("Test TokenBucket Reset", "[bandwidth throttler test]") {
	TokenBucket bucket(1000.0, 1000);
	
	// Consume all tokens
	bucket.WaitForTokens(1000);
	
	// Reset should restore tokens
	bucket.Reset();
	
	// Should be able to consume tokens immediately after reset
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(500);
	auto end_time = std::chrono::high_resolution_clock::now();
	
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 10);
}

TEST_CASE("Test TokenBucket token refilling over time", "[bandwidth throttler test]") {
	// 1000 bytes/second, 100 byte burst
	TokenBucket bucket(1000.0, 100);
	
	// Consume all initial tokens
	bucket.WaitForTokens(100);
	
	// Wait for tokens to refill (100ms should give us 100 bytes at 1000 bytes/second)
	std::this_thread::sleep_for(std::chrono::milliseconds(110));
	
	// Should be able to consume tokens without waiting
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(100);
	auto end_time = std::chrono::high_resolution_clock::now();
	
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 50); // Should be fast since tokens were refilled
}

TEST_CASE("Test TokenBucket burst capacity limit", "[bandwidth throttler test]") {
	// 1000 bytes/second, 100 byte burst
	TokenBucket bucket(1000.0, 100);
	
	// Consume all initial tokens
	bucket.WaitForTokens(100);
	
	// Wait longer than needed to refill (200ms should give 200 bytes, but burst is only 100)
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	
	// Should only be able to consume up to burst size immediately
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(100);
	auto end_time = std::chrono::high_resolution_clock::now();
	
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 50); // Should be fast since we have burst capacity
	
	// Requesting more than burst should require waiting
	start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(100);
	end_time = std::chrono::high_resolution_clock::now();
	
	duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 80); // Should wait for more tokens
}

TEST_CASE("Test TokenBucket multiple sequential requests", "[bandwidth throttler test]") {
	// 1000 bytes/second, 1000 byte burst
	TokenBucket bucket(1000.0, 1000);
	
	// Make multiple requests
	bucket.WaitForTokens(100);
	bucket.WaitForTokens(200);
	bucket.WaitForTokens(300);
	
	// Should still have 400 tokens left
	// Requesting 400 should be immediate
	auto start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(400);
	auto end_time = std::chrono::high_resolution_clock::now();
	
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 10);
	
	// Next request should wait
	start_time = std::chrono::high_resolution_clock::now();
	bucket.WaitForTokens(100);
	end_time = std::chrono::high_resolution_clock::now();
	
	duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 80); // Should wait approximately 100ms
}
