#include "mock_filesystem.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {

MockFileHandle::MockFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
                               std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
    : FileHandle(file_system, path, flags), close_callback(std::move(close_callback_p)),
      dtor_callback(std::move(dtor_callback_p)) {
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

} // namespace duckdb
