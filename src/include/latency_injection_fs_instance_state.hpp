#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/mutex.hpp"
#include <string_view>

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
	vector<string> GetWrappedFileSystemNames() const;
	void Reset();

private:
	mutable annotated_mutex registry_mutex;
	unordered_set<LatencyInjectionFileSystem *> latency_filesystems DUCKDB_GUARDED_BY(registry_mutex);
};

//===--------------------------------------------------------------------===//
// Main per-instance state container
// Inherits from ObjectCacheEntry for automatic cleanup when DatabaseInstance is destroyed
//===--------------------------------------------------------------------===//
struct LatencyInjectionFsInstanceState : public ObjectCacheEntry {
	static constexpr std::string_view OBJECT_TYPE = "LatencyInjectionFsInstanceState";
	static constexpr std::string_view CACHE_KEY = "latency_injection_fs_instance_state";

	// Latency injection filesystem registry.
	InstanceLatencyFsRegistry registry;

	LatencyInjectionFsInstanceState();

	// ObjectCacheEntry interface
	string GetObjectType() override {
		return string(OBJECT_TYPE);
	}

	static string ObjectType() {
		return string(OBJECT_TYPE);
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}
};

//===--------------------------------------------------------------------===//
// Helper functions to access instance state
//===--------------------------------------------------------------------===//

// Atomically get or create instance state in the database object cache
shared_ptr<LatencyInjectionFsInstanceState> GetInstanceStateShared(DatabaseInstance &instance);

// Get instance state, throwing if not found
LatencyInjectionFsInstanceState &GetInstanceStateOrThrow(DatabaseInstance &instance);

// Get instance state from ClientContext, throwing if not found
LatencyInjectionFsInstanceState &GetInstanceStateOrThrow(ClientContext &context);

} // namespace duckdb
