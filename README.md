# AST Project - Comparing SQL Dialects Testsuites

Full Repo URL: https://github.com/fishiu/SqlCompare

## Structure
This folder only contains basically Xiaoyuan's work
- the extraction of DuckDB and PostgreSQL
- running the other 3 on DuckDB or PostgreSQL
- the reproduce environment (docker-compose)
- ./duckdb and ./postgres are the source code of these two DBs

## Data artifacts
- duckdb extracted testcases artifacts
  - extract/duckdb/duck_testcase_data
- PostgreSQL extracted testcases artifacts
  - extract/postgresql/pg_testcase_data
- Results of running other 3 testcases on duckdb
  - duck_run_xxx.py (our implementation)
  - duck_run_xxx.log (compare result)
- Results of running other 3 testcases on PostgreSQL
  - pg_run_xxx.py (our implementation)
  - pg_run_xxx.log (compare result)

