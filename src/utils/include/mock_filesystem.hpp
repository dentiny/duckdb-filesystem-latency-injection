// Simplified mock filesystem for testing latency injection
#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/deque.hpp"
#include <mutex>
#include <functional>

namespace duckdb {

class MockFileHandle : public FileHandle {
public:
	MockFileHandle(FileSystem &file_system, string path, FileOpenFlags flags, std::function<void()> close_callback_p,
	               std::function<void()> dtor_callback_p);
	~MockFileHandle() override;
	void Close() override;

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

	MockFileSystem(std::function<void()> close_callback_p, std::function<void()> dtor_callback_p);
	~MockFileSystem() override = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	string GetName() const override;

	// Test helpers
	void SetFileSize(int64_t file_size_p);
	void SetLastModificationTime(timestamp_t last_modification_time_p);
	void SetVersionTag(string version_tag_p);
	void SetFileExists(bool exists);
	void SetGlobResults(vector<OpenFileInfo> file_open_infos);
	vector<ReadOper> GetSortedReadOperations();
	uint64_t GetFileOpenInvocation() const;
	uint64_t GetGlobInvocation() const;
	uint64_t GetSizeInvocation() const;
	uint64_t GetLastModTimeInvocation() const;
	uint64_t GetVersionTagInvocation() const;
	uint64_t GetFileExistsInvocation() const;
	void ClearReadOperations();

private:
	int64_t file_size = 0;
	timestamp_t last_modification_time = timestamp_t::infinity();
	string version_tag;
	bool file_exists = true;
	deque<OpenFileInfo> glob_returns;
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
