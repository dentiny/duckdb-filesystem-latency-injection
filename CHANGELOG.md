## 0.2.0

### Changed

- Upgrade DuckDB to `v2.0.0` and pin extension-ci-tools to `fe955ed3a046b0324cac2c691e73d8d776a7d5cc`.
- Require standard C++17, use `constexpr std::string_view` constants, and adopt DuckDB v2.0 identifiers, vector writers, and annotated mutexes.
- Use atomic object-cache state creation and typed connection settings, and remove redundant file-handle forwarding.

### Fixed

- Preserve connection-local latency settings and extended directory listing metadata.
- Execute filesystem wrapping per non-NULL input row and defer its side effects until execution.
- Snapshot registry names under the registry lock to avoid dereferencing destroyed wrappers.
- Forward write modes, aborted writes, trim, reset, and throughput estimates to the wrapped filesystem.

## 0.1.4

### Changed

- Update DuckDB and extension-ci-tools to `v1.5.6`.

### Added

- Add descriptions, examples, parameter names, and categories for all extension functions in `duckdb_functions()`.

## 0.1.3

### Changed

- Upgrade DuckDB core and extension-ci-tools to v1.5.5

## 0.1.2

### Changed

- Upgrade DuckDB core and extension-ci-tools to v1.5.4

## 0.1.1

### Changed

- Upgrade DuckDB core and extension-ci-tools to v1.5.3

## 0.1.0

### Changed

- Upgrade DuckDB core and extension-ci-tools to v1.5.2

### Fixed

- Add missing filesystem APIs ([#17])

[#17]: https://github.com/dentiny/duckdb-filesystem-latency-injection/pull/17

## 0.0.4

### Changed

- Upgrade DuckDB core and extension-ci-tools to v1.5.1 ([#10])

[#10]: https://github.com/dentiny/duckdb-filesystem-latency-injection/pull/10

### Improved

- Improve latency generation ([#15])

[#15]: https://github.com/dentiny/duckdb-filesystem-latency-injection/pull/15

### Fixed

- Fix compression file open ([#14])

[#14]: https://github.com/dentiny/duckdb-filesystem-latency-injection/pull/14

- Fix extended open file implementation ([#11])

[#11]: https://github.com/dentiny/duckdb-filesystem-latency-injection/pull/11

- Fix glob implementation when extended version supported ([#16])

[#16]: https://github.com/dentiny/duckdb-filesystem-latency-injection/pull/16

## 0.0.3

### Changed

- Upgrade DuckDB and extension-ci-tools to v1.5.0
