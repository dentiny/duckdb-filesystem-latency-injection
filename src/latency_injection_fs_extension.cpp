#define DUCKDB_EXTENSION_MAIN

#include "latency_injection_fs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "fake_filesystem.hpp"
#include "latency_injection_fs_constants.hpp"
#include "latency_injection_fs_functions.hpp"
#include "latency_injection_fs_instance_state.hpp"

namespace duckdb {

namespace {

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	// Create per-instance state for this extension
	auto state = make_shared_ptr<LatencyInjectionFsInstanceState>();
	SetInstanceState(db, state);

	// Register a fake filesystem at extension load for testing purpose.
	auto &opener_filesystem = db.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	vfs.RegisterSubSystem(make_uniq<LatencyInjectionFsFakeFileSystem>());

	// Register extension settings for latency and stddev parameters
	// List operation settings
	config.AddExtensionOption("latency_inject_fs_list_mean_ms",
	                          "Mean latency in milliseconds for LIST operations (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_LIST_MEAN_MS));
	config.AddExtensionOption("latency_inject_fs_list_stddev",
	                          "Standard deviation for LIST operations latency (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_LIST_STDDEV));

	// Stat operation settings
	config.AddExtensionOption("latency_inject_fs_stat_mean_ms",
	                          "Mean latency in milliseconds for STAT operations (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_STAT_MEAN_MS));
	config.AddExtensionOption("latency_inject_fs_stat_stddev",
	                          "Standard deviation for STAT operations latency (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_STAT_STDDEV));

	// Metadata write operation settings
	config.AddExtensionOption("latency_inject_fs_metadata_write_mean_ms",
	                          "Mean latency in milliseconds for METADATA_WRITE operations (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE},
	                          Value(LATENCY_INJECT_FS_DEFAULT_METADATA_WRITE_MEAN_MS));
	config.AddExtensionOption("latency_inject_fs_metadata_write_stddev",
	                          "Standard deviation for METADATA_WRITE operations latency (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE},
	                          Value(LATENCY_INJECT_FS_DEFAULT_METADATA_WRITE_STDDEV));

	// Read operation settings
	config.AddExtensionOption("latency_inject_fs_read_base_mean_ms",
	                          "Base mean latency in milliseconds for READ operations (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_READ_BASE_MEAN_MS));
	config.AddExtensionOption("latency_inject_fs_read_base_stddev",
	                          "Base standard deviation for READ operations latency (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_READ_BASE_STDDEV));
	config.AddExtensionOption("latency_inject_fs_read_bytes_per_ms",
	                          "Read throughput in bytes per millisecond (size-dependent latency)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_READ_BYTES_PER_MS));

	// Write operation settings
	config.AddExtensionOption("latency_inject_fs_write_base_mean_ms",
	                          "Base mean latency in milliseconds for WRITE operations (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_WRITE_BASE_MEAN_MS));
	config.AddExtensionOption("latency_inject_fs_write_base_stddev",
	                          "Base standard deviation for WRITE operations latency (log-normal distribution)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_WRITE_BASE_STDDEV));
	config.AddExtensionOption("latency_inject_fs_write_bytes_per_ms",
	                          "Write throughput in bytes per millisecond (size-dependent latency)",
	                          LogicalType {LogicalTypeId::DOUBLE}, Value(LATENCY_INJECT_FS_DEFAULT_WRITE_BYTES_PER_MS));

	// Enable/disable setting
	config.AddExtensionOption("latency_inject_fs_enabled", "Enable or disable latency injection",
	                          LogicalType {LogicalTypeId::BOOLEAN}, Value(LATENCY_INJECT_FS_DEFAULT_ENABLED));

	RegisterLatencyInjectionFsFunctions(loader);
}

} // namespace

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
