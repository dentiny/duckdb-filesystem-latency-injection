#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "latency_model.hpp"
#include "bandwidth_throttler.hpp"
#include "latency_injection_file_handle.hpp"
#include <thread>
#include <chrono>

namespace duckdb {

class LatencyInjectionFileSystem : public FileSystem {
public:
	LatencyInjectionFileSystem(unique_ptr<FileSystem> wrapped_fs_p, const LatencyConfig &config_p);
	
	~LatencyInjectionFileSystem() override = default;
	
	// Accessors for latency model and throttlers
	LatencyModel &GetLatencyModel() { return latency_model; }
	TokenBucket &GetReadThrottler() { return read_throttler; }
	TokenBucket &GetWriteThrottler() { return write_throttler; }
	
	// Override FileSystem methods to inject latency
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                       optional_ptr<FileOpener> opener) override;
	
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void FileSync(FileHandle &handle) override;
	
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	
	bool ListFiles(const string &directory,
	              const std::function<void(const string &, bool)> &callback,
	              FileOpener *opener = nullptr) override;
	bool ListFilesExtended(const string &directory,
	                      const std::function<void(OpenFileInfo &info)> &callback,
	                      optional_ptr<FileOpener> opener) override;
	
	void MoveFile(const string &source, const string &target,
	             optional_ptr<FileOpener> opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	
	string PathSeparator(const string &path) override;
	string GetHomeDirectory() override;
	string ExpandPath(const string &path) override;
	
	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override;
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;
	void UnregisterSubSystem(const string &name) override;
	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override;
	vector<string> ListSubSystems() override;
	void SetDisabledFileSystems(const vector<string> &names) override;
	bool SubSystemIsDisabled(const string &name) override;
	
	bool CanHandleFile(const string &fpath) override;
	bool IsManuallySet() override;
	bool CanSeek() override;
	bool OnDiskFile(FileHandle &handle) override;
	
	unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
	                                         bool write) override;
	
	std::string GetName() const override;
	
	bool SupportsOpenFileExtended() const override {
		// Call the public OpenFile method to check if extended is supported
		// We can't call the protected method directly
		return true; // Assume true if we're overriding OpenFileExtended
	}
	
	bool SupportsListFilesExtended() const override {
		// Call the public ListFiles method to check if extended is supported
		// We can't call the protected method directly
		return true; // Assume true if we're overriding ListFilesExtended
	}

private:
	unique_ptr<FileSystem> wrapped_fs;
	LatencyModel latency_model;
	TokenBucket read_throttler;
	TokenBucket write_throttler;
	
	// Helper to inject latency for stat-like operations
	void ApplyStatLatency();
	
	// Helper to inject latency for list operations
	void ApplyListLatency();
	
	// Helper to sleep for a duration in milliseconds
	void SleepForDuration(double milliseconds);
	
	// Helper to check if handle is our wrapper
	bool IsLatencyInjectionHandle(FileHandle &handle);
};

} // namespace duckdb
