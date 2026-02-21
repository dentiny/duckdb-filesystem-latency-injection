#include "fake_filesystem.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

LatencyInjectionFsFakeFileSystem::LatencyInjectionFsFakeFileSystem() {
}

bool LatencyInjectionFsFakeFileSystem::CanHandleFile(const string &path) {
	return StringUtil::StartsWith(path, "/tmp/latency_injection_fs_fake_filesystem/");
}

} // namespace duckdb
