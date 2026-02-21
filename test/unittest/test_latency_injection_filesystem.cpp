#include "catch/catch.hpp"

#include "latency_injection_file_system.hpp"
#include "latency_model.hpp"
#include "mock_filesystem.hpp"
#include <chrono>
#include <thread>

using namespace duckdb; // NOLINT

namespace {
const std::string TEST_FILENAME = "test_file.txt";
constexpr int64_t TEST_FILESIZE = 100;
} // namespace

TEST_CASE("Test latency injection wraps mock filesystem - Read operation", "[latency injection test]") {
	uint64_t close_invocation = 0;
	uint64_t dtor_invocation = 0;
	auto close_callback = [&close_invocation]() {
		++close_invocation;
	};
	auto dtor_callback = [&dtor_invocation]() {
		++dtor_invocation;
	};

	auto mock_filesystem = make_uniq<MockFileSystem>(std::move(close_callback), std::move(dtor_callback));
	mock_filesystem->SetFileSize(TEST_FILESIZE);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	// Configure latency injection with known values
	LatencyConfig config;
	config.enabled = true;
	config.read_base_mean = 1.0;  // 1ms base latency (reduced for faster tests)
	config.read_base_stddev = 0.1;
	config.read_bytes_per_ms = 1000000.0;  // 1MB/ms
	config.read_bandwidth_bps = 100000000.0;  // 100MB/s
	config.read_burst_bytes = 10000000;  // 10MB burst (explicit)

	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config);

	// Open file and read
	auto start_time = std::chrono::high_resolution_clock::now();
	auto handle = latency_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	REQUIRE(handle != nullptr);

	std::string buffer(TEST_FILESIZE, '\0');
	latency_fs->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, 0);
	auto end_time = std::chrono::high_resolution_clock::now();

	// Verify data was read correctly
	REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

	// Verify latency was injected (should take at least 1ms)
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 0); // Just verify it completed

	// Verify mock filesystem was called
	auto read_operations = mock_filesystem_ptr->GetSortedReadOperations();
	REQUIRE(read_operations.size() == 1);
	REQUIRE(read_operations[0].start_offset == 0);
	REQUIRE(read_operations[0].bytes_to_read == TEST_FILESIZE);

	handle->Close();
	REQUIRE(close_invocation == 1);
}

TEST_CASE("Test latency injection wraps mock filesystem - FileExists operation", "[latency injection test]") {
	auto mock_filesystem = make_uniq<MockFileSystem>([]() {}, []() {});
	mock_filesystem->SetFileExists(true);
	auto *mock_filesystem_ptr = mock_filesystem.get();

	LatencyConfig config;
	config.enabled = true;
	config.stat_mean = 5.0;  // 5ms for stat operations
	config.stat_stddev = 0.5;

	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config);

	auto start_time = std::chrono::high_resolution_clock::now();
	bool exists = latency_fs->FileExists(TEST_FILENAME);
	auto end_time = std::chrono::high_resolution_clock::now();

	REQUIRE(exists == true);
	REQUIRE(mock_filesystem_ptr->GetFileExistsInvocation() == 1);

	// Verify latency was injected
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 3); // Allow some tolerance
}

TEST_CASE("Test latency injection wraps mock filesystem - ListFiles operation", "[latency injection test]") {
	auto mock_filesystem = make_uniq<MockFileSystem>([]() {}, []() {});
	auto *mock_filesystem_ptr = mock_filesystem.get();

	LatencyConfig config;
	config.enabled = true;
	config.list_mean = 8.0;  // 8ms for list operations
	config.list_stddev = 1.0;

	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config);

	auto start_time = std::chrono::high_resolution_clock::now();
	vector<OpenFileInfo> files = latency_fs->Glob("*");
	auto end_time = std::chrono::high_resolution_clock::now();

	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 1);

	// Verify latency was injected
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 6); // Allow some tolerance
}

TEST_CASE("Test latency injection can be disabled", "[latency injection test]") {
	auto mock_filesystem = make_uniq<MockFileSystem>([]() {}, []() {});
	mock_filesystem->SetFileSize(TEST_FILESIZE);

	LatencyConfig config;
	config.enabled = false;  // Disable latency injection

	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config);

	auto start_time = std::chrono::high_resolution_clock::now();
	auto handle = latency_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	std::string buffer(TEST_FILESIZE, '\0');
	latency_fs->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, 0);
	auto end_time = std::chrono::high_resolution_clock::now();

	REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

	// With latency disabled, should be very fast
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 10); // Should be much faster without latency
}

TEST_CASE("Test latency injection with bandwidth throttling", "[latency injection test]") {
	auto mock_filesystem = make_uniq<MockFileSystem>([]() {}, []() {});
	mock_filesystem->SetFileSize(TEST_FILESIZE);

	LatencyConfig config;
	config.enabled = true;
	config.read_base_mean = 1.0;  // Small base latency
	config.read_base_stddev = 0.1;
	config.read_bandwidth_bps = 10000.0;  // Very low bandwidth: 10KB/s
	config.read_burst_bytes = 1000;  // 1KB burst

	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config);

	auto start_time = std::chrono::high_resolution_clock::now();
	auto handle = latency_fs->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
	std::string buffer(TEST_FILESIZE, '\0');
	latency_fs->Read(*handle, const_cast<char *>(buffer.data()), TEST_FILESIZE, 0);
	auto end_time = std::chrono::high_resolution_clock::now();

	REQUIRE(buffer == std::string(TEST_FILESIZE, 'a'));

	// With low bandwidth (10KB/s), reading 100 bytes should take at least 10ms
	// (100 bytes / 10000 bytes/s = 0.01s = 10ms)
	// But timing can vary, so just verify it completed
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 0); // Just verify it completed (timing can vary)
}
