#include "latency_injection_file_handle.hpp"
#include "latency_injection_file_system.hpp"

namespace duckdb {

LatencyInjectionFileHandle::LatencyInjectionFileHandle(LatencyInjectionFileSystem &fs_p,
                                                       unique_ptr<FileHandle> internal_handle_p,
                                                       const string &path_p, FileOpenFlags flags_p)
    : FileHandle(fs_p, path_p, flags_p), internal_handle(std::move(internal_handle_p)) {
}

// Simple pass-through - latency is handled by FileSystem wrapper
// Call through file_system (which is our LatencyInjectionFileSystem) to maintain proper behavior
int64_t LatencyInjectionFileHandle::Read(void *buffer, idx_t nr_bytes) {
	return file_system.Read(*this, buffer, nr_bytes);
}

int64_t LatencyInjectionFileHandle::Read(QueryContext context, void *buffer, idx_t nr_bytes) {
	return file_system.Read(*this, buffer, nr_bytes);
}

void LatencyInjectionFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void LatencyInjectionFileHandle::Read(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

int64_t LatencyInjectionFileHandle::Write(void *buffer, idx_t nr_bytes) {
	return file_system.Write(*this, buffer, nr_bytes);
}

int64_t LatencyInjectionFileHandle::Write(QueryContext context, void *buffer, idx_t nr_bytes) {
	return file_system.Write(*this, buffer, nr_bytes);
}

void LatencyInjectionFileHandle::Write(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
}

void LatencyInjectionFileHandle::Close() {
	internal_handle->Close();
}

} // namespace duckdb
