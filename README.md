# AST Project - Comparing SQL Dialects Testsuites

## Code structure

For each sql, we have two directories, one for extraction and execution on itself (to get ground truth), one for executing other testcases (to compare and analysis). Take duckdb as an example:
- duckdb/extract.py extracts testcases
- duckdb/run_test.py generates groundtruth
- duck_run_others/duck_run_click.py run clickhouse testcases on duckdb and compare the results

## Reproduce instructions

First clone postgresql source and duckdb source into `source_code`

```bash
git clone https://github.com/duckdb/duckdb.git
git clone https://github.com/postgres/postgres.git
```

Then run docker-compose

```bash
docker-compose up -d

# enter the main docker
docker exec -it xy-python-container /bin/bash
```

After that, enter the main docker and run the four `*/extract.py` and `*/run_test.py` mentioned above to extract and execute. Then run the 12 `*_run_*.py` to compare and analysis.
