# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```
For DuckDB v2.0, use Python 3.10 or newer. After a reldebug build, run the SQL and C++ suites with:

```sh
make test_reldebug
make test_reldebug_unit
```

The C++ test targets use the matching build mode. Build that mode first:

```sh
make release && make test_release_unit
make debug && make test_debug_unit
make reldebug && make test_reldebug_unit
make relassert && make test_relassert_unit
```

`make test_unit` is an alias for `make test_release_unit`, matching the default SQL test mode.
