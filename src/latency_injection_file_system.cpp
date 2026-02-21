#include "latency_injection_file_system.hpp"
#include <thread>
#include <chrono>

namespace duckdb {

LatencyInjectionFileSystem::LatencyInjectionFileSystem(unique_ptr<FileSystem> wrapped_fs_p,
                                                       const LatencyConfig &config_p)
    : wrapped_fs(std::move(wrapped_fs_p)),
      latency_model(config_p),
      read_throttler(config_p.read_bandwidth_bps, config_p.read_burst_bytes),
      write_throttler(config_p.write_bandwidth_bps, config_p.write_burst_bytes) {
}

void LatencyInjectionFileSystem::SleepForDuration(double milliseconds) {
	if (milliseconds > 0) {
		std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(milliseconds));
	}
}

void LatencyInjectionFileSystem::ApplyStatLatency() {
	if (!latency_model.IsEnabled()) {
		return;
	}
	double latency_ms = latency_model.GetOperationLatency(OperationType::STAT);
	SleepForDuration(latency_ms);
}

void LatencyInjectionFileSystem::ApplyListLatency() {
	if (!latency_model.IsEnabled()) {
		return;
	}
	double latency_ms = latency_model.GetOperationLatency(OperationType::LIST);
	SleepForDuration(latency_ms);
}

bool LatencyInjectionFileSystem::IsLatencyInjectionHandle(FileHandle &handle) {
	// Check if the handle is our wrapper by checking if it's a LatencyInjectionFileHandle
	// We can do this by checking if the file_system reference matches
	return &handle.file_system == this;
}

unique_ptr<FileHandle> LatencyInjectionFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                             optional_ptr<FileOpener> opener) {
	auto child_handle = wrapped_fs->OpenFile(path, flags, opener);
	if (!child_handle) {
		return nullptr;
	}
	return make_uniq<LatencyInjectionFileHandle>(*this, std::move(child_handle), path, flags);
}

unique_ptr<FileHandle> LatencyInjectionFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                                    optional_ptr<FileOpener> opener) {
	// Use the public OpenFile API which will internally call OpenFileExtended if supported
	// This avoids calling the protected method directly
	auto child_handle = wrapped_fs->OpenFile(file.path, flags, opener);
	if (!child_handle) {
		return nullptr;
	}
	return make_uniq<LatencyInjectionFileHandle>(*this, std::move(child_handle), file.path, flags);
}

void LatencyInjectionFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// Apply latency before the operation
	if (latency_model.IsEnabled() && nr_bytes > 0) {
		double latency_ms = latency_model.GetOperationLatency(OperationType::READ, static_cast<size_t>(nr_bytes));
		read_throttler.WaitForTokens(static_cast<size_t>(nr_bytes));
		SleepForDuration(latency_ms);
	}
	
	// Extract child handle if it's our wrapper, otherwise use handle directly
	if (IsLatencyInjectionHandle(handle)) {
		wrapped_fs->Read(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get(), buffer, nr_bytes,
		                 location);
	} else {
		wrapped_fs->Read(handle, buffer, nr_bytes, location);
	}
}

void LatencyInjectionFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// Apply latency before the operation
	if (latency_model.IsEnabled() && nr_bytes > 0) {
		double latency_ms = latency_model.GetOperationLatency(OperationType::WRITE, static_cast<size_t>(nr_bytes));
		write_throttler.WaitForTokens(static_cast<size_t>(nr_bytes));
		SleepForDuration(latency_ms);
	}
	
	// Extract child handle if it's our wrapper, otherwise use handle directly
	if (IsLatencyInjectionHandle(handle)) {
		wrapped_fs->Write(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get(), buffer, nr_bytes,
		                 location);
	} else {
		wrapped_fs->Write(handle, buffer, nr_bytes, location);
	}
}

int64_t LatencyInjectionFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	// Apply latency before the operation
	if (latency_model.IsEnabled() && nr_bytes > 0) {
		double latency_ms = latency_model.GetOperationLatency(OperationType::READ, static_cast<size_t>(nr_bytes));
		read_throttler.WaitForTokens(static_cast<size_t>(nr_bytes));
		SleepForDuration(latency_ms);
	}
	
	// Extract child handle if it's our wrapper, otherwise use handle directly
	if (IsLatencyInjectionHandle(handle)) {
		return wrapped_fs->Read(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get(), buffer,
		                        nr_bytes);
	} else {
		return wrapped_fs->Read(handle, buffer, nr_bytes);
	}
}

int64_t LatencyInjectionFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	// Apply latency before the operation
	if (latency_model.IsEnabled() && nr_bytes > 0) {
		double latency_ms = latency_model.GetOperationLatency(OperationType::WRITE, static_cast<size_t>(nr_bytes));
		write_throttler.WaitForTokens(static_cast<size_t>(nr_bytes));
		SleepForDuration(latency_ms);
	}
	
	// Extract child handle if it's our wrapper, otherwise use handle directly
	if (IsLatencyInjectionHandle(handle)) {
		return wrapped_fs->Write(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get(), buffer,
		                         nr_bytes);
	} else {
		return wrapped_fs->Write(handle, buffer, nr_bytes);
	}
}

int64_t LatencyInjectionFileSystem::GetFileSize(FileHandle &handle) {
	ApplyStatLatency();
	if (IsLatencyInjectionHandle(handle)) {
		return wrapped_fs->GetFileSize(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get());
	}
	return wrapped_fs->GetFileSize(handle);
}

timestamp_t LatencyInjectionFileSystem::GetLastModifiedTime(FileHandle &handle) {
	ApplyStatLatency();
	if (IsLatencyInjectionHandle(handle)) {
		return wrapped_fs->GetLastModifiedTime(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get());
	}
	return wrapped_fs->GetLastModifiedTime(handle);
}

string LatencyInjectionFileSystem::GetVersionTag(FileHandle &handle) {
	if (IsLatencyInjectionHandle(handle)) {
		return wrapped_fs->GetVersionTag(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get());
	}
	return wrapped_fs->GetVersionTag(handle);
}

FileType LatencyInjectionFileSystem::GetFileType(FileHandle &handle) {
	if (IsLatencyInjectionHandle(handle)) {
		return wrapped_fs->GetFileType(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get());
	}
	return wrapped_fs->GetFileType(handle);
}

void LatencyInjectionFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	if (IsLatencyInjectionHandle(handle)) {
		wrapped_fs->Truncate(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get(), new_size);
	} else {
		wrapped_fs->Truncate(handle, new_size);
	}
}

void LatencyInjectionFileSystem::FileSync(FileHandle &handle) {
	if (IsLatencyInjectionHandle(handle)) {
		wrapped_fs->FileSync(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get());
	} else {
		wrapped_fs->FileSync(handle);
	}
}

bool LatencyInjectionFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return wrapped_fs->DirectoryExists(directory, opener);
}

void LatencyInjectionFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	wrapped_fs->CreateDirectory(directory, opener);
}

void LatencyInjectionFileSystem::CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener) {
	wrapped_fs->CreateDirectoriesRecursive(path, opener);
}

void LatencyInjectionFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	wrapped_fs->RemoveDirectory(directory, opener);
}

bool LatencyInjectionFileSystem::ListFiles(const string &directory,
                                          const std::function<void(const string &, bool)> &callback,
                                          FileOpener *opener) {
	ApplyListLatency();
	return wrapped_fs->ListFiles(directory, callback, opener);
}

bool LatencyInjectionFileSystem::ListFilesExtended(const string &directory,
                                                   const std::function<void(OpenFileInfo &info)> &callback,
                                                   optional_ptr<FileOpener> opener) {
	ApplyListLatency();
	// Convert to the non-extended callback format
	return wrapped_fs->ListFiles(directory, [&callback](const string &name, bool is_dir) {
		OpenFileInfo info;
		info.path = name;
		// We don't have full info, but this should work for basic cases
		callback(info);
	}, opener.get());
}

void LatencyInjectionFileSystem::MoveFile(const string &source, const string &target,
                                         optional_ptr<FileOpener> opener) {
	wrapped_fs->MoveFile(source, target, opener);
}

bool LatencyInjectionFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	ApplyStatLatency();
	return wrapped_fs->FileExists(filename, opener);
}

bool LatencyInjectionFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return wrapped_fs->IsPipe(filename, opener);
}

void LatencyInjectionFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	wrapped_fs->RemoveFile(filename, opener);
}

bool LatencyInjectionFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return wrapped_fs->TryRemoveFile(filename, opener);
}

vector<OpenFileInfo> LatencyInjectionFileSystem::Glob(const string &path, FileOpener *opener) {
	ApplyListLatency();
	return wrapped_fs->Glob(path, opener);
}

string LatencyInjectionFileSystem::PathSeparator(const string &path) {
	return wrapped_fs->PathSeparator(path);
}

string LatencyInjectionFileSystem::GetHomeDirectory() {
	return wrapped_fs->GetHomeDirectory();
}

string LatencyInjectionFileSystem::ExpandPath(const string &path) {
	return wrapped_fs->ExpandPath(path);
}

void LatencyInjectionFileSystem::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	wrapped_fs->RegisterSubSystem(std::move(sub_fs));
}

void LatencyInjectionFileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	wrapped_fs->RegisterSubSystem(compression_type, std::move(fs));
}

void LatencyInjectionFileSystem::UnregisterSubSystem(const string &name) {
	wrapped_fs->UnregisterSubSystem(name);
}

unique_ptr<FileSystem> LatencyInjectionFileSystem::ExtractSubSystem(const string &name) {
	return wrapped_fs->ExtractSubSystem(name);
}

vector<string> LatencyInjectionFileSystem::ListSubSystems() {
	return wrapped_fs->ListSubSystems();
}

void LatencyInjectionFileSystem::SetDisabledFileSystems(const vector<string> &names) {
	wrapped_fs->SetDisabledFileSystems(names);
}

bool LatencyInjectionFileSystem::SubSystemIsDisabled(const string &name) {
	return wrapped_fs->SubSystemIsDisabled(name);
}

bool LatencyInjectionFileSystem::CanHandleFile(const string &fpath) {
	return wrapped_fs->CanHandleFile(fpath);
}

bool LatencyInjectionFileSystem::IsManuallySet() {
	return wrapped_fs->IsManuallySet();
}

bool LatencyInjectionFileSystem::CanSeek() {
	return wrapped_fs->CanSeek();
}

bool LatencyInjectionFileSystem::OnDiskFile(FileHandle &handle) {
	if (IsLatencyInjectionHandle(handle)) {
		return wrapped_fs->OnDiskFile(*static_cast<LatencyInjectionFileHandle &>(handle).child_handle.get());
	}
	return wrapped_fs->OnDiskFile(handle);
}

unique_ptr<FileHandle> LatencyInjectionFileSystem::OpenCompressedFile(QueryContext context,
                                                                     unique_ptr<FileHandle> handle, bool write) {
	return wrapped_fs->OpenCompressedFile(context, std::move(handle), write);
}

std::string LatencyInjectionFileSystem::GetName() const {
	return "LatencyInjectionFileSystem(" + wrapped_fs->GetName() + ")";
}

} // namespace duckdb
