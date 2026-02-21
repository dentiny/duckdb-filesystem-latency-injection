#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {

class LatencyInjectionFsFakeFileSystem : public LocalFileSystem {
public:
	LatencyInjectionFsFakeFileSystem();

	string GetName() const override {
		return "latency_injection_fs_fake_filesystem";
	}

	bool CanHandleFile(const string &path) override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
};

} // namespace duckdb
