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
	lock_guard<mutex> lock(registry_mutex);
	latency_filesystems.insert(fs);
}

void InstanceLatencyFsRegistry::Unregister(LatencyInjectionFileSystem *fs) {
	lock_guard<mutex> lock(registry_mutex);
	latency_filesystems.erase(fs);
}

unordered_set<LatencyInjectionFileSystem *> InstanceLatencyFsRegistry::GetAllLatencyFs() const {
	lock_guard<mutex> lock(registry_mutex);
	return latency_filesystems;
}

void InstanceLatencyFsRegistry::Reset() {
	lock_guard<mutex> lock(registry_mutex);
	latency_filesystems.clear();
}

//===--------------------------------------------------------------------===//
// LatencyInjectionFsInstanceState
//===--------------------------------------------------------------------===//

LatencyInjectionFsInstanceState::LatencyInjectionFsInstanceState() {
}

optional_idx LatencyInjectionFsInstanceState::GetEstimatedCacheMemory() const {
	return optional_idx {};
}

//===--------------------------------------------------------------------===//
// Helper functions to access instance state
//===--------------------------------------------------------------------===//

void SetInstanceState(DatabaseInstance &instance, shared_ptr<LatencyInjectionFsInstanceState> state) {
	auto &object_cache = instance.GetObjectCache();
	object_cache.Put(LatencyInjectionFsInstanceState::CACHE_KEY, std::move(state));
}

shared_ptr<LatencyInjectionFsInstanceState> GetInstanceStateShared(DatabaseInstance &instance) {
	auto &object_cache = instance.GetObjectCache();
	auto entry = object_cache.Get<LatencyInjectionFsInstanceState>(LatencyInjectionFsInstanceState::CACHE_KEY);
	if (!entry) {
		return nullptr;
	}
	return entry;
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
