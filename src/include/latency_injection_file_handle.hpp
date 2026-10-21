#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class LatencyInjectionFileSystem;

class LatencyInjectionFileHandle : public FileHandle {
public:
	LatencyInjectionFileHandle(LatencyInjectionFileSystem &fs_p, unique_ptr<FileHandle> internal_handle_p,
	                           const string &path_p, FileOpenFlags flags_p);

	void Close() override;

	idx_t GetProgress() override {
		return internal_handle->GetProgress();
	}

	FileCompressionType GetFileCompressionType() override {
		return internal_handle->GetFileCompressionType();
	}

	bool CanSeek() override {
		return internal_handle->CanSeek();
	}

	unique_ptr<FileHandle> internal_handle;
};

} // namespace duckdb
