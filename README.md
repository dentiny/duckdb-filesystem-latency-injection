# DuckDB Filesystem Latency Injection Extension

A DuckDB extension that wraps filesystem operations to inject configurable latency, making it useful for local benchmarking and testing how DuckDB performs under various filesystem latency conditions.

## Loading the extension
Since DuckDB v1.0.0, cache httpfs can be loaded as a community extension without requiring the `unsigned` flag. From any DuckDB instance, the following two commands will allow you to install and load the extension:
```sql
FORCE INSTALL latency_inject_fs from community;
LOAD latency_inject_fs;
```

## Why This Extension?

This extension is **useful for local benchmarking** scenarios where you want to simulate real-world filesystem latency conditions without needing to deploy to remote storage systems or cloud environments. By injecting configurable latency into filesystem operations, you can:

- **Test performance characteristics** of DuckDB queries under different latency conditions
- **Benchmark query performance** with realistic filesystem delays (e.g., simulating S3, HDFS, or network storage)
- **Reproduce performance issues** that occur in production environments with high-latency storage

This is particularly valuable for developers who want to understand how DuckDB behaves with slow filesystems without the complexity and cost of setting up actual remote storage infrastructure.

## Design Principles

- Use log-normal distribution to simulate latency
- Don't throttle any IO operations, which is based on the assumption that b/w and QPS is not bottleneck
  + [rate_limit_fs](https://duckdb.org/community_extensions/extensions/rate_limit_fs) could be used to set throttle

## How to Use

### Wrapping a Filesystem

To inject latency into a filesystem, you need to wrap it using the `latency_inject_fs_wrap()` function. First, load the filesystem you want to wrap (e.g., `httpfs`), then wrap it:

```sql
LOAD httpfs;
SELECT latency_inject_fs_wrap('HTTPFileSystem');
```

### Listing Wrapped Filesystems

To see which filesystems are currently wrapped with latency injection:

```sql
SELECT * FROM latency_inject_fs_list_filesystems();
```

### Configuring Latency Parameters

The extension provides configurable settings for different types of filesystem operations. All latencies use a log-normal distribution.

#### List Operations
```sql
SET latency_inject_fs_list_mean_ms = 2.0;        -- Mean latency in milliseconds (default: 2.0)
SET latency_inject_fs_list_stddev = 0.5;          -- Standard deviation (default: 0.5)
```

#### Stat Operations (file existence checks, metadata reads)
```sql
SET latency_inject_fs_stat_mean_ms = 1.0;         -- Mean latency in milliseconds (default: 1.0)
SET latency_inject_fs_stat_stddev = 0.3;           -- Standard deviation (default: 0.3)
```

#### Metadata Write Operations
```sql
SET latency_inject_fs_metadata_write_mean_ms = 3.0;  -- Mean latency in milliseconds (default: 3.0)
SET latency_inject_fs_metadata_write_stddev = 0.5;   -- Standard deviation (default: 0.5)
```

#### Read Operations
Read latency is size-dependent: `base_latency + (bytes / bytes_per_ms)`

```sql
SET latency_inject_fs_read_base_mean_ms = 5.0;    -- Base mean latency in milliseconds (default: 5.0)
SET latency_inject_fs_read_base_stddev = 1.0;       -- Base standard deviation (default: 1.0)
SET latency_inject_fs_read_bytes_per_ms = 1000000.0;  -- Throughput in bytes per millisecond (default: 1000000.0)
```

#### Write Operations
Write latency is size-dependent: `base_latency + (bytes / bytes_per_ms)`

```sql
SET latency_inject_fs_write_base_mean_ms = 10.0;   -- Base mean latency in milliseconds (default: 10.0)
SET latency_inject_fs_write_base_stddev = 2.0;      -- Base standard deviation (default: 2.0)
SET latency_inject_fs_write_bytes_per_ms = 500000.0;  -- Throughput in bytes per millisecond (default: 500000.0)
```

## License

See [LICENSE](LICENSE) file for details.
