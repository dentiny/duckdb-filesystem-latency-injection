#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

class LatencyInjectionFsFakeFileSystem : public LocalFileSystem {
public:
	LatencyInjectionFsFakeFileSystem();

	string GetName() const override {
		return "latency_injection_fs_fake_filesystem";
	}

	bool CanHandleFile(const string &path) override;
};

} // namespace duckdb
