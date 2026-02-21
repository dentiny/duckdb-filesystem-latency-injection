#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

// Forward declaration
class LatencyInjectionFileSystem;

// Simple pass-through wrapper - all latency logic is in LatencyInjectionFileSystem
class LatencyInjectionFileHandle : public FileHandle {
public:
	LatencyInjectionFileHandle(LatencyInjectionFileSystem &fs_p, unique_ptr<FileHandle> internal_handle_p,
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
	
	// Delegate other methods to internal handle
	// Only GetProgress, GetFileCompressionType, and Close are virtual
	idx_t GetProgress() override {
		return internal_handle->GetProgress();
	}
	
	FileCompressionType GetFileCompressionType() override {
		return internal_handle->GetFileCompressionType();
	}
	
	// Non-virtual methods - just delegate
	bool CanSeek() {
		return internal_handle->CanSeek();
	}
	
	bool IsPipe() {
		return internal_handle->IsPipe();
	}
	
	bool OnDiskFile() {
		return internal_handle->OnDiskFile();
	}
	
	idx_t GetFileSize() {
		return internal_handle->GetFileSize();
	}
	
	FileType GetType() {
		return internal_handle->GetType();
	}
	
	void Seek(idx_t location) {
		internal_handle->Seek(location);
	}
	
	void Reset() {
		internal_handle->Reset();
	}
	
	idx_t SeekPosition() {
		return internal_handle->SeekPosition();
	}
	
	void Sync() {
		internal_handle->Sync();
	}
	
	void Truncate(int64_t new_size) {
		internal_handle->Truncate(new_size);
	}
	
	string ReadLine() {
		return internal_handle->ReadLine();
	}
	
	string ReadLine(QueryContext context) {
		return internal_handle->ReadLine(context);
	}
	
	bool Trim(idx_t offset_bytes, idx_t length_bytes) {
		return internal_handle->Trim(offset_bytes, length_bytes);
	}

	// Expose internal_handle for LatencyInjectionFileSystem to access
	unique_ptr<FileHandle> internal_handle;
};

} // namespace duckdb
