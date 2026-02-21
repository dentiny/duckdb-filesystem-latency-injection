#define DUCKDB_EXTENSION_MAIN

#include "latency_injection_fs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/main/client_context.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "latency_injection_file_system.hpp"
#include "latency_injection_fs_instance_state.hpp"
#include "latency_injection_fs_query_functions.hpp"
#include "latency_model.hpp"
#include "fake_filesystem.hpp"

namespace duckdb {

namespace {

// Get database instance from expression state.
// Returned instance ownership lies in the given [`state`].
DatabaseInstance &GetDatabaseInstance(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return *client_context.db.get();
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

	// Create default config
	LatencyConfig default_config;

	// Create the latency injection filesystem wrapper
	auto latency_fs =
	    make_uniq<LatencyInjectionFileSystem>(std::move(internal_filesystem), default_config, inst_state.get());
	vfs.RegisterSubSystem(std::move(latency_fs));
	DUCKDB_LOG_DEBUG(duckdb_instance,
	                 StringUtil::Format("Wrap filesystem %s with latency injection filesystem.", filesystem_name));

	result.Reference(Value(true));
}

} // namespace

static void LoadInternal(ExtensionLoader &loader) {

	// Get the database instance
	auto &db = loader.GetDatabaseInstance();

	// Create per-instance state for this extension
	auto state = make_shared_ptr<LatencyInjectionFsInstanceState>();
	SetInstanceState(db, state);

	// Register a fake filesystem at extension load for testing purpose.
	auto &opener_filesystem = db.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	vfs.RegisterSubSystem(make_uniq<LatencyInjectionFsFakeFileSystem>());

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
