#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

class DatabaseInstance;
class ExtensionLoader;

void RegisterLatencyInjectionFsFunctions(ExtensionLoader &loader);

// Wraps the registered filesystem `filesystem_name` with latency injection, using the global latency_inject_fs_*
// settings. Throws if no such filesystem is registered.
void WrapFileSystem(DatabaseInstance &db, const string &filesystem_name);

} // namespace duckdb
