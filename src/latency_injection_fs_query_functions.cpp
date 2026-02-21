#include "latency_injection_fs_query_functions.hpp"

#include "latency_injection_fs_instance_state.hpp"
#include "latency_injection_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Wrapped latency injection filesystem query function
//===--------------------------------------------------------------------===//
struct WrappedFilesystemsData : public GlobalTableFunctionState {
	vector<string> wrapped_filesystems;

	// Used to record the progress of emission.
	uint64_t offset = 0;
};

unique_ptr<FunctionData> WrappedLatencyFsFuncBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(1);
	names.reserve(1);

	// Wrapped latency injection filesystem name.
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});
	names.emplace_back("wrapped_filesystems");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> WrappedLatencyFsFuncInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<WrappedFilesystemsData>();
	auto &wrapped_filesystems = result->wrapped_filesystems;

	// Get latency injection filesystems from per-instance registry
	auto &inst_state = GetInstanceStateOrThrow(*context.db);
	auto latency_filesystem_instances = inst_state.registry.GetAllLatencyFs();
	wrapped_filesystems.reserve(latency_filesystem_instances.size());

	for (auto *cur_latency_fs : latency_filesystem_instances) {
		auto *wrapped_fs = cur_latency_fs->GetWrappedFileSystem();
		if (wrapped_fs) {
			wrapped_filesystems.emplace_back(wrapped_fs->GetName());
		}
	}

	// Sort the results to ensure determinism and testability.
	std::sort(wrapped_filesystems.begin(), wrapped_filesystems.end());

	return std::move(result);
}

void WrappedLatencyFsTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<WrappedFilesystemsData>();

	// All entries have been emitted.
	if (data.offset >= data.wrapped_filesystems.size()) {
		return;
	}

	// Start filling in the result buffer.
	idx_t count = 0;
	while (data.offset < data.wrapped_filesystems.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.wrapped_filesystems[data.offset++];
		idx_t col = 0;

		// Wrapped latency injection filesystem name.
		output.SetValue(col, count, entry);

		count++;
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction GetWrappedLatencyFsFunc() {
	TableFunction wrapped_latency_fs_query_func {/*name=*/"latency_inject_fs_list_filesystems",
	                                             /*arguments=*/ {},
	                                             /*function=*/WrappedLatencyFsTableFunc,
	                                             /*bind=*/WrappedLatencyFsFuncBind,
	                                             /*init_global=*/WrappedLatencyFsFuncInit};
	return wrapped_latency_fs_query_func;
}

} // namespace duckdb
