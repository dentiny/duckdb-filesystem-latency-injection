#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterLatencyInjectionFsFunctions(ExtensionLoader &loader);

} // namespace duckdb
