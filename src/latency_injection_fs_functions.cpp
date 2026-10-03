#include "latency_injection_fs_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "latency_injection_file_system.hpp"
#include "latency_injection_fs_instance_state.hpp"
#include "latency_injection_fs_query_functions.hpp"
#include "latency_model.hpp"

namespace duckdb {

namespace {

DatabaseInstance &GetDatabaseInstance(ExpressionState &state) {
	auto *executor = state.root.executor;
	auto &client_context = executor->GetContext();
	return *client_context.db.get();
}

LatencyConfig ReadLatencyConfigFromSettings(DatabaseInstance &db) {
	LatencyConfig config;
	Value setting_value;

	if (db.TryGetCurrentSetting("latency_inject_fs_list_mean_ms", setting_value)) {
		config.list_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_list_stddev", setting_value)) {
		config.list_stddev = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_stat_mean_ms", setting_value)) {
		config.stat_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_stat_stddev", setting_value)) {
		config.stat_stddev = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_metadata_write_mean_ms", setting_value)) {
		config.metadata_write_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_metadata_write_stddev", setting_value)) {
		config.metadata_write_stddev = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_read_base_mean_ms", setting_value)) {
		config.read_base_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_read_base_stddev", setting_value)) {
		config.read_base_stddev = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_read_bytes_per_ms", setting_value)) {
		config.read_bytes_per_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_write_base_mean_ms", setting_value)) {
		config.write_base_mean_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_write_base_stddev", setting_value)) {
		config.write_base_stddev = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_write_bytes_per_ms", setting_value)) {
		config.write_bytes_per_ms = setting_value.GetValue<double>();
	}
	if (db.TryGetCurrentSetting("latency_inject_fs_enabled", setting_value)) {
		config.enabled = setting_value.GetValue<bool>();
	}

	return config;
}

void WrapLatencyFileSystem(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	const string filesystem_name = args.GetValue(/*col_idx=*/0, /*index=*/0).ToString();
	WrapFileSystem(GetDatabaseInstance(state), filesystem_name);
	result.Reference(Value(true));
}

void RegisterScalarFunction(ExtensionLoader &loader, ScalarFunction function, vector<string> parameter_names,
                            string description, vector<string> examples, vector<string> categories) {
	CreateScalarFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

	FunctionDescription function_description;
	function_description.parameter_names = std::move(parameter_names);
	function_description.description = std::move(description);
	function_description.examples = std::move(examples);
	function_description.categories = std::move(categories);
	info.descriptions.push_back(std::move(function_description));

	loader.RegisterFunction(std::move(info));
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunction function, vector<string> parameter_names,
                           string description, vector<string> examples, vector<string> categories) {
	CreateTableFunctionInfo info(std::move(function));
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

void WrapFileSystem(DatabaseInstance &db, const string &filesystem_name) {
	auto &opener_filesystem = db.GetFileSystem().Cast<OpenerFileSystem>();
	auto &vfs = opener_filesystem.GetFileSystem();
	auto internal_filesystem = vfs.ExtractSubSystem(filesystem_name);
	if (internal_filesystem == nullptr) {
		throw InvalidInputException("Filesystem %s hasn't been registered yet! Use "
		                            "latency_inject_fs_list_filesystems() to see available filesystems.",
		                            filesystem_name);
	}

	auto inst_state = GetLatencyInjectionFsStateShared(db);
	if (!inst_state) {
		inst_state = make_shared_ptr<LatencyInjectionFsInstanceState>();
		SetInstanceState(db, inst_state);
	}

	LatencyConfig config = ReadLatencyConfigFromSettings(db);
	weak_ptr<LatencyInjectionFsInstanceState> inst_state_weak = inst_state;
	auto latency_fs = make_uniq<LatencyInjectionFileSystem>(std::move(internal_filesystem), config, inst_state_weak);
	vfs.RegisterSubSystem(std::move(latency_fs));
	DUCKDB_LOG_DEBUG(db, StringUtil::Format("Wrap filesystem %s with latency injection filesystem.", filesystem_name));
}

void RegisterLatencyInjectionFsFunctions(ExtensionLoader &loader) {
	RegisterScalarFunction(loader,
	                       ScalarFunction("latency_inject_fs_wrap", /*arguments=*/ {LogicalTypeId::VARCHAR},
	                                      /*return_type=*/LogicalTypeId::BOOLEAN, WrapLatencyFileSystem),
	                       /*parameter_names=*/ {"filesystem_name"},
	                       /*description=*/
	                       "Wraps a registered DuckDB filesystem with latency injection using the current "
	                       "latency_inject_fs_* settings.",
	                       /*examples=*/ {"SELECT latency_inject_fs_wrap('HTTPFileSystem');"},
	                       /*categories=*/ {"filesystem", "latency"});
	RegisterTableFunction(
	    loader, GetWrappedLatencyFsFunc(),
	    /*parameter_names=*/ {},
	    /*description=*/"Returns the names of filesystems currently wrapped by the latency injection filesystem.",
	    /*examples=*/ {"SELECT * FROM latency_inject_fs_list_filesystems();"},
	    /*categories=*/ {"filesystem", "latency"});
}

} // namespace duckdb
