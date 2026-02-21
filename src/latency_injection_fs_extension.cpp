#define DUCKDB_EXTENSION_MAIN

#include "latency_injection_fs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "fake_filesystem.hpp"
#include "latency_injection_file_system.hpp"
#include "latency_injection_fs_instance_state.hpp"
#include "latency_injection_fs_query_functions.hpp"
#include "latency_injection_fs_constants.hpp"
#include "latency_model.hpp"

namespace duckdb {

namespace {

// Get database instance from expression state.
// Returned instance ownership lies in the given [`state`].
DatabaseInstance &GetDatabaseInstance(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return *client_context.db.get();
}

// Read current extension settings and populate LatencyConfig
LatencyConfig ReadLatencyConfigFromSettings(DatabaseInstance &db) {
	LatencyConfig config;
	Value setting_value;

	// Read list operation settings
	if (db.TryGetCurrentSetting("latency_inject_fs_list_mean_ms", setting_value)) {
		config.list_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_list_stddev", setting_value)) {
		config.list_stddev = setting_value.GetValue<double>();
	}

	// Read stat operation settings
	if (db.TryGetCurrentSetting("latency_inject_fs_stat_mean_ms", setting_value)) {
		config.stat_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_stat_stddev", setting_value)) {
		config.stat_stddev = setting_value.GetValue<double>();
	}

	// Read metadata_write operation settings
	if (db.TryGetCurrentSetting("latency_inject_fs_metadata_write_mean_ms", setting_value)) {
		config.metadata_write_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_metadata_write_stddev", setting_value)) {
		config.metadata_write_stddev = setting_value.GetValue<double>();
	}

	// Read read operation settings
	if (db.TryGetCurrentSetting("latency_inject_fs_read_base_mean_ms", setting_value)) {
		config.read_base_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_read_base_stddev", setting_value)) {
		config.read_base_stddev = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_read_bytes_per_ms", setting_value)) {
		config.read_bytes_per_ms = setting_value.GetValue<double>();
	}

	// Read write operation settings
	if (db.TryGetCurrentSetting("latency_inject_fs_write_base_mean_ms", setting_value)) {
		config.write_base_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_write_base_stddev", setting_value)) {
		config.write_base_stddev = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_write_bytes_per_ms", setting_value)) {
		config.write_bytes_per_ms = setting_value.GetValue<double>();
	}

	// Read enabled setting
	if (db.TryGetCurrentSetting("latency_inject_fs_enabled", setting_value)) {
		config.enabled = setting_value.GetValue<bool>();
	}

	return config;
}

// Wrap the filesystem with latency injection filesystem.
// Throw exception if the requested filesystem hasn't been registered into duckdb instance.
void WrapLatencyFileSystem(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	const string filesystem_name = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();

	// duckdb instance has a opener filesystem, which is a wrapper around virtual filesystem.
	auto &duckdb_instance = GetDatabaseInstance(state);
	auto &opener_filesystem = duckdb_instance.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	auto internal_filesystem = vfs.ExtractSubSystem(filesystem_name);
	if (internal_filesystem == nullptr) {
		throw InvalidInputException("Filesystem %s hasn't been registered yet! Use "
		                            "latency_inject_fs_list_filesystems() to see available filesystems.",
		                            filesystem_name);
	}

	// Get or create instance state
	auto inst_state = GetInstanceStateShared(duckdb_instance);
	if (!inst_state) {
		inst_state = make_shared_ptr<LatencyInjectionFsInstanceState>();
		SetInstanceState(duckdb_instance, inst_state);
	}

	LatencyConfig config = ReadLatencyConfigFromSettings(duckdb_instance);

	// Create the latency injection filesystem wrapper
	// Convert shared_ptr to weak_ptr for the constructor
	weak_ptr<LatencyInjectionFsInstanceState> inst_state_weak = inst_state;
	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(internal_filesystem), config, inst_state_weak);
	vfs.RegisterSubSystem(std::move(latency_fs));
	DUCKDB_LOG_DEBUG(duckdb_instance,
	                 StringUtil::Format("Wrap filesystem %s with latency injection filesystem.", filesystem_name));

	result.Reference(Value(true));
}

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

	// Register a function to wrap duckdb-vfs-compatible filesystems.
	// Example usage:
	// D. LOAD httpfs;
	// -- Wrap filesystem with its name.
	// D. SELECT latency_inject_fs_wrap('HTTPFileSystem');
	ScalarFunction wrap_latency_filesystem_function("latency_inject_fs_wrap",
	                                                /*arguments=*/ {LogicalTypeId::VARCHAR},
	                                                /*return_type=*/LogicalTypeId::BOOLEAN, WrapLatencyFileSystem);
	loader.RegisterFunction(wrap_latency_filesystem_function);

	// Register wrapped latency injection filesystems info.
	loader.RegisterFunction(GetWrappedLatencyFsFunc());
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
