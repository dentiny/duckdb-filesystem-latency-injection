// Simplified mock filesystem for testing latency injection
#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/mutex.hpp"
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
	mutable annotated_mutex mtx;
	int64_t file_size DUCKDB_GUARDED_BY(mtx) = 0;
	timestamp_t last_modification_time DUCKDB_GUARDED_BY(mtx) = timestamp_t::infinity();
	string version_tag DUCKDB_GUARDED_BY(mtx);
	bool file_exists DUCKDB_GUARDED_BY(mtx) = true;
	deque<OpenFileInfo> glob_returns DUCKDB_GUARDED_BY(mtx);
	std::function<void()> close_callback;
	std::function<void()> dtor_callback;

	uint64_t file_open_invocation DUCKDB_GUARDED_BY(mtx) = 0;
	uint64_t glob_invocation DUCKDB_GUARDED_BY(mtx) = 0;
	uint64_t get_file_size_invocation DUCKDB_GUARDED_BY(mtx) = 0;
	uint64_t get_last_mod_time_invocation DUCKDB_GUARDED_BY(mtx) = 0;
	uint64_t get_version_tag_invocation DUCKDB_GUARDED_BY(mtx) = 0;
	uint64_t file_exists_invocation DUCKDB_GUARDED_BY(mtx) = 0;
	vector<ReadOper> read_operations DUCKDB_GUARDED_BY(mtx);
};

} // namespace duckdb
