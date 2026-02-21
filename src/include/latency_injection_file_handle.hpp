#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

// Forward declaration
class LatencyInjectionFileSystem;

// Simple pass-through wrapper - all latency logic is in LatencyInjectionFileSystem
class LatencyInjectionFileHandle : public FileHandle {
public:
	LatencyInjectionFileHandle(LatencyInjectionFileSystem &fs_p, unique_ptr<FileHandle> child_handle_p,
	                           const string &path_p, FileOpenFlags flags_p);
	
	~LatencyInjectionFileHandle() override = default;
	
	// Simple pass-through - latency is handled by FileSystem
	// Note: These methods are NOT virtual in FileHandle, so we override them by calling through file_system
	int64_t Read(void *buffer, idx_t nr_bytes);
	int64_t Read(QueryContext context, void *buffer, idx_t nr_bytes);
	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Read(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location);
	
	int64_t Write(void *buffer, idx_t nr_bytes);
	int64_t Write(QueryContext context, void *buffer, idx_t nr_bytes);
	void Write(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location);
	
	void Close() override;
	
	// Delegate other methods to child handle
	// Only GetProgress, GetFileCompressionType, and Close are virtual
	idx_t GetProgress() override {
		return child_handle->GetProgress();
	}
	
	FileCompressionType GetFileCompressionType() override {
		return child_handle->GetFileCompressionType();
	}
	
	// Non-virtual methods - just delegate
	bool CanSeek() {
		return child_handle->CanSeek();
	}
	
	bool IsPipe() {
		return child_handle->IsPipe();
	}
	
	bool OnDiskFile() {
		return child_handle->OnDiskFile();
	}
	
	idx_t GetFileSize() {
		return child_handle->GetFileSize();
	}
	
	FileType GetType() {
		return child_handle->GetType();
	}
	
	void Seek(idx_t location) {
		child_handle->Seek(location);
	}
	
	void Reset() {
		child_handle->Reset();
	}
	
	idx_t SeekPosition() {
		return child_handle->SeekPosition();
	}
	
	void Sync() {
		child_handle->Sync();
	}
	
	void Truncate(int64_t new_size) {
		child_handle->Truncate(new_size);
	}
	
	string ReadLine() {
		return child_handle->ReadLine();
	}
	
	string ReadLine(QueryContext context) {
		return child_handle->ReadLine(context);
	}
	
	bool Trim(idx_t offset_bytes, idx_t length_bytes) {
		return child_handle->Trim(offset_bytes, length_bytes);
	}

	// Expose child_handle for LatencyInjectionFileSystem to access
	unique_ptr<FileHandle> child_handle;
};

} // namespace duckdb
