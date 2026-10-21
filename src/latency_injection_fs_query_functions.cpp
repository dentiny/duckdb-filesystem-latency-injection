#include "latency_injection_fs_query_functions.hpp"

#include "latency_injection_fs_instance_state.hpp"
#include "latency_injection_file_system.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
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
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	D_ASSERT(return_types.empty());
	D_ASSERT(names.empty());

	return_types.reserve(1);
	names.reserve(1);

	// Wrapped latency injection filesystem name.
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("wrapped_filesystems");

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> WrappedLatencyFsFuncInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<WrappedFilesystemsData>();
	auto &wrapped_filesystems = result->wrapped_filesystems;
	wrapped_filesystems = GetInstanceStateOrThrow(context).registry.GetWrappedFileSystemNames();

	// Sort the results to ensure determinism and testability.
	std::sort(wrapped_filesystems.begin(), wrapped_filesystems.end());

	return result;
}

void WrappedLatencyFsTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<WrappedFilesystemsData>();

	auto count = MinValue<idx_t>(data.wrapped_filesystems.size() - data.offset, STANDARD_VECTOR_SIZE);
	auto writer = FlatVector::Writer<string_t>(output.data[0], count);
	for (idx_t row = 0; row < count; row++) {
		writer.WriteValue(data.wrapped_filesystems[data.offset++]);
	}
	output.CheckCardinality(count);
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
