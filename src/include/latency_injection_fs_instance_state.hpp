#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

// Forward declarations
class LatencyInjectionFileSystem;
class DatabaseInstance;
class ClientContext;

//===--------------------------------------------------------------------===//
// Per-instance filesystem registry
//===--------------------------------------------------------------------===//
class InstanceLatencyFsRegistry {
public:
	// Register `fs` in the registry. Registering the same LatencyInjectionFileSystem for multiple time is harmless but
	// useless.
	void Register(LatencyInjectionFileSystem *fs);
	// Unregister `fs` from the registry. Unregistering a LatencyInjectionFileSystem that is not registered doesn't have
	// any effect.
	void Unregister(LatencyInjectionFileSystem *fs);
	unordered_set<LatencyInjectionFileSystem *> GetAllLatencyFs() const;
	void Reset();

private:
	mutable mutex registry_mutex;
	unordered_set<LatencyInjectionFileSystem *> latency_filesystems;
};

//===--------------------------------------------------------------------===//
// Main per-instance state container
// Inherits from ObjectCacheEntry for automatic cleanup when DatabaseInstance is destroyed
//===--------------------------------------------------------------------===//
struct LatencyInjectionFsInstanceState : public ObjectCacheEntry {
	static constexpr const char *OBJECT_TYPE = "LatencyInjectionFsInstanceState";
	static constexpr const char *CACHE_KEY = "latency_injection_fs_instance_state";

	// Latency injection filesystem registry.
	InstanceLatencyFsRegistry registry;

	LatencyInjectionFsInstanceState();

	// ObjectCacheEntry interface
	string GetObjectType() override {
		return OBJECT_TYPE;
	}

	static string ObjectType() {
		return OBJECT_TYPE;
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}
};

//===--------------------------------------------------------------------===//
// Helper functions to access instance state
//===--------------------------------------------------------------------===//

// Store instance state in DatabaseInstance
void SetInstanceState(DatabaseInstance &instance, shared_ptr<LatencyInjectionFsInstanceState> state);

// Get instance state as shared_ptr from DatabaseInstance (returns nullptr if not set)
shared_ptr<LatencyInjectionFsInstanceState> GetInstanceStateShared(DatabaseInstance &instance);

// Get instance state, throwing if not found
LatencyInjectionFsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance);

// Get instance state from ClientContext, throwing if not found
LatencyInjectionFsInstanceState &GetInstanceStateOrThrow(ClientContext &context);

} // namespace duckdb
