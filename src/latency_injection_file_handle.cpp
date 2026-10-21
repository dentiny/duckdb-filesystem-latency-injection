#include "latency_injection_file_handle.hpp"
#include "latency_injection_file_system.hpp"

namespace duckdb {

LatencyInjectionFileHandle::LatencyInjectionFileHandle(LatencyInjectionFileSystem &fs_p,
                                                       unique_ptr<FileHandle> internal_handle_p, const string &path_p,
                                                       FileOpenFlags flags_p)
    : FileHandle(fs_p, path_p, std::move(flags_p)), internal_handle(std::move(internal_handle_p)) {
}

void LatencyInjectionFileHandle::Close() {
	internal_handle->Close();
}

} // namespace duckdb
