#include "fake_filesystem.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

LatencyInjectionFsFakeFileSystem::LatencyInjectionFsFakeFileSystem() {
}

bool LatencyInjectionFsFakeFileSystem::CanHandleFile(const string &path) {
	return StringUtil::StartsWith(path, "/tmp/latency_injection_fs_fake_filesystem/");
}

unique_ptr<FileHandle> LatencyInjectionFsFakeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                                  optional_ptr<FileOpener> opener) {
	OpenFileInfo file_info(path);
	return OpenFileExtended(file_info, flags, opener);
}

unique_ptr<FileHandle> LatencyInjectionFsFakeFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                                          optional_ptr<FileOpener> opener) {
	if (flags.OpenForWriting()) {
		auto sep = PathSeparator(file.path);
		auto pos = file.path.find_last_of(sep);
		if (pos != string::npos) {
			auto parent_dir = file.path.substr(0, pos);
			if (!parent_dir.empty() && !LocalFileSystem::DirectoryExists(parent_dir)) {
				CreateDirectoriesRecursive(parent_dir);
			}
		}
	}
	return LocalFileSystem::OpenFile(file.path, flags, opener);
}

} // namespace duckdb
