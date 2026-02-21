#define DUCKDB_EXTENSION_MAIN

#include "latency_injection_fs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "latency_injection_file_system.hpp"
#include "latency_model.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void QuackScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Quack " + name.GetString() + " 🐥");
	});
}

inline void QuackOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Quack " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto quack_scalar_function = ScalarFunction("quack", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun);
	loader.RegisterFunction(quack_scalar_function);

	// Register another scalar function
	auto quack_openssl_version_scalar_function = ScalarFunction("quack_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, QuackOpenSSLVersionScalarFun);
	loader.RegisterFunction(quack_openssl_version_scalar_function);

	// Register latency injection filesystem wrapper
	// This will wrap the default filesystem or can be configured to wrap specific subsystems
	// For now, we create a default configuration that can be customized
	LatencyConfig default_config;
	// Default config values are already set in the struct definition
	// Users can customize these via environment variables or settings if needed

	// Get the database instance to access its filesystem
	auto &db = loader.GetDatabaseInstance();
	auto &vfs = db.GetFileSystem();

	// Extract and wrap the default filesystem
	// Note: This is a simple approach - in practice, you might want to wrap specific subsystems
	// For S3/httpfs, you would extract that subsystem and wrap it
	auto default_fs = vfs.ExtractSubSystem("LocalFileSystem");
	if (!default_fs) {
		// If we can't extract, create a local filesystem to wrap
		default_fs = FileSystem::CreateLocal();
	}

	// Create the latency injection filesystem wrapper
	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(default_fs), default_config);

	// Register it as a subsystem
	// Note: This replaces the default, which might not be desired
	// A better approach would be to wrap specific subsystems like httpfs/s3fs
	// For now, this demonstrates the integration
	// vfs.RegisterSubSystem(std::move(latency_fs));
}

void LatencyInjectionFsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string LatencyInjectionFsExtension::Name() {
	return "latency_injection_fs";
}

std::string LatencyInjectionFsExtension::Version() const {
#ifdef EXT_VERSION_LATENCY_INJECTION_FS
	return EXT_VERSION_LATENCY_INJECTION_FS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(latency_injection_fs, loader) {
	duckdb::LoadInternal(loader);
}
}
