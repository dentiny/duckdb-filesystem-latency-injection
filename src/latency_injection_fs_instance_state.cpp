#include "latency_injection_fs_instance_state.hpp"

#include "latency_injection_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// InstanceLatencyFsRegistry
//===--------------------------------------------------------------------===//

void InstanceLatencyFsRegistry::Register(LatencyInjectionFileSystem *fs) {
	annotated_lock_guard<annotated_mutex> lock(registry_mutex);
	latency_filesystems.insert(fs);
}

void InstanceLatencyFsRegistry::Unregister(LatencyInjectionFileSystem *fs) {
	annotated_lock_guard<annotated_mutex> lock(registry_mutex);
	latency_filesystems.erase(fs);
}

vector<string> InstanceLatencyFsRegistry::GetWrappedFileSystemNames() const {
	annotated_lock_guard<annotated_mutex> lock(registry_mutex);
	vector<string> names;
	names.reserve(latency_filesystems.size());
	for (auto *fs : latency_filesystems) {
		names.push_back(fs->GetWrappedFileSystem()->GetName());
	}
	return names;
}

void InstanceLatencyFsRegistry::Reset() {
	annotated_lock_guard<annotated_mutex> lock(registry_mutex);
	latency_filesystems.clear();
}

//===--------------------------------------------------------------------===//
// LatencyInjectionFsInstanceState
//===--------------------------------------------------------------------===//

LatencyInjectionFsInstanceState::LatencyInjectionFsInstanceState() = default;

//===--------------------------------------------------------------------===//
// Helper functions to access instance state
//===--------------------------------------------------------------------===//

shared_ptr<LatencyInjectionFsInstanceState> GetInstanceStateShared(DatabaseInstance &instance) {
	return instance.GetObjectCache().GetOrCreate<LatencyInjectionFsInstanceState>(
	    string(LatencyInjectionFsInstanceState::CACHE_KEY));
}

LatencyInjectionFsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance) {
	auto state = GetInstanceStateShared(instance);
	if (!state) {
		throw InternalException("LatencyInjectionFsInstanceState not found");
	}
	return *state;
}

LatencyInjectionFsInstanceState &GetInstanceStateOrThrow(ClientContext &context) {
	return GetInstanceStateOrThrow(*context.db);
}

} // namespace duckdb
