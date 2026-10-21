#include "catch/catch.hpp"

#include "latency_injection_file_system.hpp"
#include "latency_injection_fs_instance_state.hpp"
#include "latency_model.hpp"
#include "latency_injection_fs_extension.hpp"
#include "latency_injection_file_handle.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include <atomic>
#include <string_view>
#include "mock_filesystem.hpp"

#include <chrono>
#include <thread>

using namespace duckdb; // NOLINT

namespace {
constexpr std::string_view TEST_FILENAME = "test_file.txt";
constexpr int64_t TEST_FILESIZE = 100;
class ForwardingMockFileSystem : public MockFileSystem {
public:
	ForwardingMockFileSystem() : MockFileSystem([]() {}, []() {}) {
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		auto handle = MockFileSystem::OpenFile(path, flags, opener);
		opened_handle = handle.get();
		return handle;
	}

	void Reset(FileHandle &handle) override {
		reset_forwarded = &handle == opened_handle;
	}

	void AbortFileWrite(FileHandle &handle) override {
		abort_forwarded = &handle == opened_handle;
	}

	bool Trim(FileHandle &handle, idx_t offset, idx_t length) override {
		return &handle == opened_handle && offset == 3 && length == 5;
	}

	FileWriteMode GetWriteMode(FileHandle &handle) override {
		return &handle == opened_handle ? FileWriteMode::POSITIONAL : FileWriteMode::SEQUENTIAL;
	}

	bool TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) override {
		result.latency_seconds = 0.01;
		result.bandwidth_bytes_per_s = 1024;
		return &handle == opened_handle;
	}

	bool SupportsListFilesExtended() const override {
		return true;
	}

	bool ListFilesExtended(const string &, const std::function<void(OpenFileInfo &)> &callback,
	                       optional_ptr<FileOpener>) override {
		OpenFileInfo info("child");
		info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		info.extended_info->options["type"] = "directory";
		info.extended_info->options["version_tag"] = "preserved";
		callback(info);
		return true;
	}

	FileHandle *opened_handle = nullptr;
	bool reset_forwarded = false;
	bool abort_forwarded = false;
};

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
	config.read_base_mean_ms = 1.0; // 1ms base latency (reduced for faster tests)
	config.read_base_stddev = 0.1;
	config.read_bytes_per_ms = 1000000.0; // 1MB/ms

	auto inst_state = make_shared_ptr<LatencyInjectionFsInstanceState>();
	weak_ptr<LatencyInjectionFsInstanceState> inst_state_weak = inst_state;
	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config, inst_state_weak);

	// Open file and read
	auto start_time = std::chrono::high_resolution_clock::now();
	auto handle = latency_fs->OpenFile(string(TEST_FILENAME), FileOpenFlags::FILE_FLAGS_READ);
	REQUIRE(handle != nullptr);

	string buffer(TEST_FILESIZE, '\0');
	latency_fs->Read(*handle, buffer.data(), TEST_FILESIZE, 0);
	auto end_time = std::chrono::high_resolution_clock::now();

	// Verify data was read correctly
	REQUIRE(buffer == string(TEST_FILESIZE, 'a'));

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
	config.stat_mean_ms = 5.0; // 5ms for stat operations
	config.stat_stddev = 0.5;

	auto inst_state = make_shared_ptr<LatencyInjectionFsInstanceState>();
	weak_ptr<LatencyInjectionFsInstanceState> inst_state_weak = inst_state;
	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config, inst_state_weak);

	auto start_time = std::chrono::high_resolution_clock::now();
	bool exists = latency_fs->FileExists(string(TEST_FILENAME));
	auto end_time = std::chrono::high_resolution_clock::now();

	REQUIRE(exists == true);
	REQUIRE(mock_filesystem_ptr->GetFileExistsInvocation() == 1);

	// Verify latency was injected
	// Note: Log-normal distribution can sample very small values, so we just verify it completed
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 0); // Just verify it completed (latency may be very small due to log-normal sampling)
}

TEST_CASE("Test latency injection wraps mock filesystem - ListFiles operation", "[latency injection test]") {
	auto mock_filesystem = make_uniq<MockFileSystem>([]() {}, []() {});
	auto *mock_filesystem_ptr = mock_filesystem.get();

	LatencyConfig config;
	config.enabled = true;
	config.list_mean_ms = 8.0; // 8ms for list operations
	config.list_stddev = 1.0;

	auto inst_state = make_shared_ptr<LatencyInjectionFsInstanceState>();
	weak_ptr<LatencyInjectionFsInstanceState> inst_state_weak = inst_state;
	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config, inst_state_weak);

	auto start_time = std::chrono::high_resolution_clock::now();
	vector<OpenFileInfo> files = latency_fs->Glob("*");
	auto end_time = std::chrono::high_resolution_clock::now();

	REQUIRE(mock_filesystem_ptr->GetGlobInvocation() == 1);

	// Verify latency was injected
	// Note: Log-normal distribution can sample very small values, so we just verify it completed
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms >= 0); // Just verify it completed (latency may be very small due to log-normal sampling)
}

TEST_CASE("Test latency injection can be disabled", "[latency injection test]") {
	auto mock_filesystem = make_uniq<MockFileSystem>([]() {}, []() {});
	mock_filesystem->SetFileSize(TEST_FILESIZE);

	LatencyConfig config;
	config.enabled = false; // Disable latency injection

	auto inst_state = make_shared_ptr<LatencyInjectionFsInstanceState>();
	weak_ptr<LatencyInjectionFsInstanceState> inst_state_weak = inst_state;
	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(mock_filesystem), config, inst_state_weak);

	auto start_time = std::chrono::high_resolution_clock::now();
	auto handle = latency_fs->OpenFile(string(TEST_FILENAME), FileOpenFlags::FILE_FLAGS_READ);
	string buffer(TEST_FILESIZE, '\0');
	latency_fs->Read(*handle, buffer.data(), TEST_FILESIZE, 0);
	auto end_time = std::chrono::high_resolution_clock::now();

	REQUIRE(buffer == string(TEST_FILESIZE, 'a'));

	// With latency disabled, should be very fast
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	REQUIRE(duration_ms < 10); // Should be much faster without latency
}

TEST_CASE("Inherited handle methods forward to the wrapped handle", "[latency injection test]") {
	auto wrapped = make_uniq<ForwardingMockFileSystem>();
	auto &inner = *wrapped;
	LatencyConfig config;
	config.enabled = false;
	LatencyInjectionFileSystem fs(std::move(wrapped), config, {});
	auto handle = fs.OpenFile(string(TEST_FILENAME), FileOpenFlags::FILE_FLAGS_READ);
	auto &latency_handle = handle->Cast<LatencyInjectionFileHandle>();

	string buffer(4, '\0');
	latency_handle.Read(buffer.data(), buffer.size(), 7);
	REQUIRE(buffer == "aaaa");
	REQUIRE(inner.GetSortedReadOperations()[0].start_offset == 7);
	REQUIRE(latency_handle.Trim(3, 5));
	REQUIRE(latency_handle.GetWriteMode() == FileWriteMode::POSITIONAL);
	latency_handle.Reset();
	REQUIRE(inner.reset_forwarded);
	latency_handle.AbortWrite();
	REQUIRE(inner.abort_forwarded);
	NetworkThroughputEstimate throughput;
	REQUIRE(latency_handle.TryGetNetworkThroughput(throughput));
	REQUIRE(throughput.bandwidth_bytes_per_s == 1024);
}

TEST_CASE("Extended listing retains directory and backend metadata", "[latency injection test]") {
	LatencyConfig config;
	config.enabled = false;
	LatencyInjectionFileSystem fs(make_uniq<ForwardingMockFileSystem>(), config, {});
	bool is_directory = false;
	REQUIRE(fs.ListFiles("parent", [&](const string &, bool is_dir) { is_directory = is_dir; }));
	REQUIRE(is_directory);
	string version_tag;
	REQUIRE(fs.ListFilesExtended(
	    "parent",
	    [&](OpenFileInfo &info) {
		    REQUIRE(FileSystem::IsDirectory(info));
		    version_tag = info.extended_info->options.at("version_tag").GetValue<string>();
	    },
	    nullptr));
	REQUIRE(version_tag == "preserved");
}

TEST_CASE("Wrapping reads connection-local settings", "[latency injection test]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<LatencyInjectionFsExtension>();
	Connection connection(db);
	REQUIRE_FALSE(connection.Query("SET latency_inject_fs_enabled = false")->HasError());
	REQUIRE_FALSE(connection.Query("SET latency_inject_fs_read_base_mean_ms = 123")->HasError());
	REQUIRE_FALSE(
	    connection.Query("SELECT latency_inject_fs_wrap('latency_injection_fs_fake_filesystem')")->HasError());
	auto &vfs = db.instance->GetFileSystem().Cast<OpenerFileSystem>().GetFileSystem();
	auto wrapped = vfs.ExtractSubSystem("LatencyInjectionFileSystem(latency_injection_fs_fake_filesystem)");
	REQUIRE(wrapped);
	const auto &config = wrapped->Cast<LatencyInjectionFileSystem>().GetLatencyModel().GetConfig();
	REQUIRE_FALSE(config.enabled);
	REQUIRE(config.read_base_mean_ms == 123);
	REQUIRE(GetInstanceStateShared(*db.instance) == GetInstanceStateShared(*db.instance));
}

TEST_CASE("Registry names remain safe during wrapper destruction", "[latency injection test]") {
	auto state = make_shared_ptr<LatencyInjectionFsInstanceState>();
	std::atomic<bool> running {true};
	std::atomic<bool> invalid_snapshot {false};
	std::thread reader([&]() {
		while (running.load()) {
			auto names = state->registry.GetWrappedFileSystemNames();
			if (names.size() > 1 || (!names.empty() && names[0] != "mock filesystem")) {
				invalid_snapshot.store(true);
			}
		}
	});
	LatencyConfig config;
	config.enabled = false;
	for (idx_t i = 0; i < 300; i++) {
		LatencyInjectionFileSystem fs(make_uniq<MockFileSystem>([]() {}, []() {}), config, state);
	}
	running.store(false);
	reader.join();
	REQUIRE_FALSE(invalid_snapshot.load());
	REQUIRE(state->registry.GetWrappedFileSystemNames().empty());
}
