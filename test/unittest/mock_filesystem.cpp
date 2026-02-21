#include "mock_filesystem.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <tuple>
#include <cstring>

namespace duckdb {

MockFileHandle::MockFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
                               std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
    : FileHandle(file_system, path, flags), close_callback(std::move(close_callback_p)),
      dtor_callback(std::move(dtor_callback_p)) {
}

MockFileHandle::~MockFileHandle() {
	if (dtor_callback) {
		dtor_callback();
	}
}

void MockFileHandle::Close() {
	if (close_callback) {
		close_callback();
	}
}

MockFileSystem::MockFileSystem(std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
    : close_callback(std::move(close_callback_p)), dtor_callback(std::move(dtor_callback_p)) {
}

unique_ptr<FileHandle> MockFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	std::lock_guard<std::mutex> lck(mtx);
	++file_open_invocation;
	return make_uniq<MockFileHandle>(*this, path, flags, close_callback, dtor_callback);
}

void MockFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	std::lock_guard<std::mutex> lck(mtx);
	std::memset(buffer, 'a', nr_bytes);
	ReadOper oper;
	oper.start_offset = location;
	oper.bytes_to_read = nr_bytes;
	read_operations.emplace_back(oper);
}

int64_t MockFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	Read(handle, buffer, nr_bytes, handle.SeekPosition());
	return nr_bytes;
}

vector<OpenFileInfo> MockFileSystem::Glob(const string &path, FileOpener *opener) {
	std::lock_guard<std::mutex> lck(mtx);
	++glob_invocation;
	if (!glob_returns.empty()) {
		vector<OpenFileInfo> cur_glob_ret;
		cur_glob_ret.emplace_back(std::move(glob_returns.front()));
		glob_returns.pop_front();
		return cur_glob_ret;
	}
	return {};
}

int64_t MockFileSystem::GetFileSize(FileHandle &handle) {
	std::lock_guard<std::mutex> lck(mtx);
	++get_file_size_invocation;
	return file_size;
}

timestamp_t MockFileSystem::GetLastModifiedTime(FileHandle &handle) {
	std::lock_guard<std::mutex> lck(mtx);
	++get_last_mod_time_invocation;
	return last_modification_time;
}

string MockFileSystem::GetVersionTag(FileHandle &handle) {
	std::lock_guard<std::mutex> lck(mtx);
	++get_version_tag_invocation;
	return version_tag;
}

bool MockFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	std::lock_guard<std::mutex> lck(mtx);
	++file_exists_invocation;
	return file_exists;
}

string MockFileSystem::GetName() const {
	return "mock filesystem";
}

void MockFileSystem::SetFileSize(int64_t file_size_p) {
	std::lock_guard<std::mutex> lck(mtx);
	file_size = file_size_p;
}

void MockFileSystem::SetLastModificationTime(timestamp_t last_modification_time_p) {
	std::lock_guard<std::mutex> lck(mtx);
	last_modification_time = last_modification_time_p;
}

void MockFileSystem::SetVersionTag(string version_tag_p) {
	std::lock_guard<std::mutex> lck(mtx);
	version_tag = std::move(version_tag_p);
}

void MockFileSystem::SetFileExists(bool exists) {
	std::lock_guard<std::mutex> lck(mtx);
	file_exists = exists;
}

void MockFileSystem::SetGlobResults(vector<OpenFileInfo> file_open_infos) {
	std::lock_guard<std::mutex> lck(mtx);
	glob_returns = std::deque<OpenFileInfo> {std::make_move_iterator(file_open_infos.begin()),
	                                         std::make_move_iterator(file_open_infos.end())};
}

vector<MockFileSystem::ReadOper> MockFileSystem::GetSortedReadOperations() {
	std::lock_guard<std::mutex> lck(mtx);
	std::sort(read_operations.begin(), read_operations.end(),
	          [](const ReadOper &lhs, const ReadOper &rhs) {
		          return std::tie(lhs.start_offset, lhs.bytes_to_read) <
		                 std::tie(rhs.start_offset, rhs.bytes_to_read);
	          });
	return read_operations;
}

uint64_t MockFileSystem::GetFileOpenInvocation() const {
	std::lock_guard<std::mutex> lck(mtx);
	return file_open_invocation;
}

uint64_t MockFileSystem::GetGlobInvocation() const {
	std::lock_guard<std::mutex> lck(mtx);
	return glob_invocation;
}

uint64_t MockFileSystem::GetSizeInvocation() const {
	std::lock_guard<std::mutex> lck(mtx);
	return get_file_size_invocation;
}

uint64_t MockFileSystem::GetLastModTimeInvocation() const {
	std::lock_guard<std::mutex> lck(mtx);
	return get_last_mod_time_invocation;
}

uint64_t MockFileSystem::GetVersionTagInvocation() const {
	std::lock_guard<std::mutex> lck(mtx);
	return get_version_tag_invocation;
}

uint64_t MockFileSystem::GetFileExistsInvocation() const {
	std::lock_guard<std::mutex> lck(mtx);
	return file_exists_invocation;
}

void MockFileSystem::ClearReadOperations() {
	std::lock_guard<std::mutex> lck(mtx);
	read_operations.clear();
}

} // namespace duckdb
