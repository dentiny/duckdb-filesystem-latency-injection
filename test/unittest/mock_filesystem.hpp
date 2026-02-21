// Simplified mock filesystem for testing latency injection
#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include <mutex>
#include <functional>
#include <vector>
#include <deque>
#include <cstring>

namespace duckdb {

class MockFileHandle : public FileHandle {
public:
	MockFileHandle(FileSystem &file_system, string path, FileOpenFlags flags, std::function<void()> close_callback_p,
	               std::function<void()> dtor_callback_p);
	~MockFileHandle() override {
		if (dtor_callback) {
			dtor_callback();
		}
	}
	void Close() override {
		if (close_callback) {
			close_callback();
		}
	}

private:
	std::function<void()> close_callback;
	std::function<void()> dtor_callback;
};

class MockFileSystem : public FileSystem {
public:
	struct ReadOper {
		uint64_t start_offset = 0;
		int64_t bytes_to_read = 0;
	};

	MockFileSystem(std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
	    : close_callback(std::move(close_callback_p)), dtor_callback(std::move(dtor_callback_p)) {
	}
	~MockFileSystem() override = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	int64_t GetFileSize(FileHandle &handle) override {
		std::lock_guard<std::mutex> lck(mtx);
		++get_file_size_invocation;
		return file_size;
	}
	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		std::lock_guard<std::mutex> lck(mtx);
		++get_last_mod_time_invocation;
		return last_modification_time;
	}
	string GetVersionTag(FileHandle &handle) override {
		std::lock_guard<std::mutex> lck(mtx);
		++get_version_tag_invocation;
		return version_tag;
	}
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		std::lock_guard<std::mutex> lck(mtx);
		++file_exists_invocation;
		return file_exists;
	}
	string GetName() const override {
		return "mock filesystem";
	}

	// Test helpers
	void SetFileSize(int64_t file_size_p) {
		std::lock_guard<std::mutex> lck(mtx);
		file_size = file_size_p;
	}
	void SetLastModificationTime(timestamp_t last_modification_time_p) {
		std::lock_guard<std::mutex> lck(mtx);
		last_modification_time = last_modification_time_p;
	}
	void SetVersionTag(string version_tag_p) {
		std::lock_guard<std::mutex> lck(mtx);
		version_tag = std::move(version_tag_p);
	}
	void SetFileExists(bool exists) {
		std::lock_guard<std::mutex> lck(mtx);
		file_exists = exists;
	}
	void SetGlobResults(vector<OpenFileInfo> file_open_infos) {
		std::lock_guard<std::mutex> lck(mtx);
		glob_returns = std::deque<OpenFileInfo> {std::make_move_iterator(file_open_infos.begin()),
		                                         std::make_move_iterator(file_open_infos.end())};
	}
	vector<ReadOper> GetSortedReadOperations() {
		std::lock_guard<std::mutex> lck(mtx);
		std::sort(read_operations.begin(), read_operations.end(),
		          [](const ReadOper &lhs, const ReadOper &rhs) {
			          return std::tie(lhs.start_offset, lhs.bytes_to_read) <
			                 std::tie(rhs.start_offset, rhs.bytes_to_read);
		          });
		return read_operations;
	}
	uint64_t GetFileOpenInvocation() const {
		std::lock_guard<std::mutex> lck(mtx);
		return file_open_invocation;
	}
	uint64_t GetGlobInvocation() const {
		std::lock_guard<std::mutex> lck(mtx);
		return glob_invocation;
	}
	uint64_t GetSizeInvocation() const {
		std::lock_guard<std::mutex> lck(mtx);
		return get_file_size_invocation;
	}
	uint64_t GetLastModTimeInvocation() const {
		std::lock_guard<std::mutex> lck(mtx);
		return get_last_mod_time_invocation;
	}
	uint64_t GetVersionTagInvocation() const {
		std::lock_guard<std::mutex> lck(mtx);
		return get_version_tag_invocation;
	}
	uint64_t GetFileExistsInvocation() const {
		std::lock_guard<std::mutex> lck(mtx);
		return file_exists_invocation;
	}
	void ClearReadOperations() {
		std::lock_guard<std::mutex> lck(mtx);
		read_operations.clear();
	}

private:
	int64_t file_size = 0;
	timestamp_t last_modification_time = timestamp_t::infinity();
	string version_tag;
	bool file_exists = true;
	std::deque<OpenFileInfo> glob_returns;
	std::function<void()> close_callback;
	std::function<void()> dtor_callback;

	mutable std::mutex mtx;
	uint64_t file_open_invocation = 0;
	uint64_t glob_invocation = 0;
	uint64_t get_file_size_invocation = 0;
	uint64_t get_last_mod_time_invocation = 0;
	uint64_t get_version_tag_invocation = 0;
	uint64_t file_exists_invocation = 0;
	vector<ReadOper> read_operations;
};

} // namespace duckdb
