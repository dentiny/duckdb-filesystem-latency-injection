#include "latency_injection_fs_functions.hpp"

#include <algorithm>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "latency_injection_file_system.hpp"
#include "latency_injection_fs_instance_state.hpp"
#include "latency_injection_fs_query_functions.hpp"
#include "latency_model.hpp"

namespace duckdb {

namespace {

LatencyConfig ReadLatencyConfigFromSettings(ClientContext &context) {
	LatencyConfig config;
	context.TryGetCurrentSetting("latency_inject_fs_list_mean_ms", config.list_mean_ms);
	context.TryGetCurrentSetting("latency_inject_fs_list_stddev", config.list_stddev);
	context.TryGetCurrentSetting("latency_inject_fs_stat_mean_ms", config.stat_mean_ms);
	context.TryGetCurrentSetting("latency_inject_fs_stat_stddev", config.stat_stddev);
	context.TryGetCurrentSetting("latency_inject_fs_metadata_write_mean_ms", config.metadata_write_mean_ms);
	context.TryGetCurrentSetting("latency_inject_fs_metadata_write_stddev", config.metadata_write_stddev);
	context.TryGetCurrentSetting("latency_inject_fs_read_base_mean_ms", config.read_base_mean_ms);
	context.TryGetCurrentSetting("latency_inject_fs_read_base_stddev", config.read_base_stddev);
	context.TryGetCurrentSetting("latency_inject_fs_read_bytes_per_ms", config.read_bytes_per_ms);
	context.TryGetCurrentSetting("latency_inject_fs_write_base_mean_ms", config.write_base_mean_ms);
	context.TryGetCurrentSetting("latency_inject_fs_write_base_stddev", config.write_base_stddev);
	context.TryGetCurrentSetting("latency_inject_fs_write_bytes_per_ms", config.write_bytes_per_ms);
	context.TryGetCurrentSetting("latency_inject_fs_enabled", config.enabled);
	return config;
}

void WrapLatencyFileSystem(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &db = *context.db;
	auto &vfs = db.GetFileSystem().Cast<OpenerFileSystem>().GetFileSystem();
	auto inst_state = GetInstanceStateShared(db);
	auto config = ReadLatencyConfigFromSettings(context);
	weak_ptr<LatencyInjectionFsInstanceState> inst_state_weak = inst_state;

	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t name) {
		auto filesystem_name = name.GetString();
		auto internal_filesystem = vfs.ExtractSubSystem(filesystem_name);
		if (!internal_filesystem) {
			// Repeating a wrap preserves the existing wrapper and its latency configuration.
			const auto wrapped_name = StringUtil::Format("LatencyInjectionFileSystem(%s)", filesystem_name);
			const auto registered = vfs.ListSubSystems();
			if (std::find(registered.begin(), registered.end(), wrapped_name) != registered.end()) {
				return true;
			}
			throw InvalidInputException("Filesystem %s hasn't been registered yet! Use "
			                            "latency_inject_fs_list_filesystems() to see available filesystems.",
			                            filesystem_name);
		}
		vfs.RegisterSubSystem(
		    make_uniq<LatencyInjectionFileSystem>(std::move(internal_filesystem), config, inst_state_weak));
		DUCKDB_LOG_DEBUG(db, "Wrap filesystem %s with latency injection filesystem.", filesystem_name);
		return true;
	});
}

template <class CREATE_INFO, class FUNCTION>
void RegisterFunction(ExtensionLoader &loader, FUNCTION function, vector<string> parameter_names, string description,
                      vector<string> examples, vector<string> categories) {
	CREATE_INFO info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

	FunctionDescription function_description;
	function_description.parameter_names = std::move(parameter_names);
	function_description.description = std::move(description);
	function_description.examples = std::move(examples);
	function_description.categories = std::move(categories);
	info.descriptions.push_back(std::move(function_description));

	loader.RegisterFunction(std::move(info));
}

} // namespace

void RegisterLatencyInjectionFsFunctions(ExtensionLoader &loader) {
	ScalarFunction wrap_function("latency_inject_fs_wrap", {LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                             WrapLatencyFileSystem);
	wrap_function.SetVolatile();
	wrap_function.SetFallible();
	RegisterFunction<CreateScalarFunctionInfo>(
	    loader, std::move(wrap_function),
	    /*parameter_names=*/ {"filesystem_name"},
	    /*description=*/
	    "Wraps a registered DuckDB filesystem with latency injection using the current "
	    "latency_inject_fs_* settings.",
	    /*examples=*/ {"SELECT latency_inject_fs_wrap('HTTPFileSystem');"},
	    /*categories=*/ {"filesystem", "latency"});
	RegisterFunction<CreateTableFunctionInfo>(
	    loader, GetWrappedLatencyFsFunc(),
	    /*parameter_names=*/ {},
	    /*description=*/"Returns the names of filesystems currently wrapped by the latency injection filesystem.",
	    /*examples=*/ {"SELECT * FROM latency_inject_fs_list_filesystems();"},
	    /*categories=*/ {"filesystem", "latency"});
}

} // namespace duckdb
