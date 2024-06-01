# run duckdb testcase in psql
import pathlib
import collections

import clickhouse_driver
import duckdb

conn_params = {
    'host': 'localhost',
    'port': '19000',
    'user': 'default',
    'password': '',
    'database': 'test'
}


def remove_comments(line):
    comment_start_index = line.find('--')

    if comment_start_index != -1:
        return line[:comment_start_index].strip()
    else:
        return line.strip()


def split_sql(filepath):
    lines = []
    with open(filepath, 'r') as file:
        for line in file.readlines():
            line = line.strip()
            line = remove_comments(line)
            if line:
                lines.append(line)
    statements = '\n'.join(lines).split(';')
    statements = [statement.strip() + ';' for statement in statements if statement.strip()]
    return statements


class DuckTestCase:
    def __init__(self, test_case_dir):
        self.c = duckdb.connect()
        self.cur = self.c.cursor()

        self.db_sql = []
        self.query_sql = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        self.db_sql = split_sql(self.test_case_dir / "db.sql")

        # read query.sql
        self.query_sql = split_sql(self.test_case_dir / "test.sql")

    def execute_db(self):
        for db_sql in self.db_sql:
            self.cur.execute(db_sql).fetchall()

    def execute_query(self):
        return self.cur.execute(self.query_sql[0]).fetchall()

    # destructor
    def __del__(self):
        self.cur.close()
        self.c.close()


class ClickTestCase:
    def __init__(self, test_case_dir):
        # self.client = clickhouse_driver.Client(**conn_params)

        self.db_sql = []
        self.query_sql = []
        self.result = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        self.db_sql = split_sql(self.test_case_dir / "db.sql")

        # read query.sql
        self.query_sql = split_sql(self.test_case_dir / "test.sql")

        with open(self.test_case_dir / "result.txt", 'r') as f:
            self.result = f.read().strip()

    # def init_db(self):
    #     tables = self.client.execute("SHOW TABLES;")
    #     for table in tables:
    #         self.client.execute(f"DROP TABLE IF EXISTS {table[0]};")
    #     # print(tables)
    #
    # def execute_db(self):
    #     # first init
    #     self.init_db()
    #     for db_sql in self.db_sql:
    #         self.client.execute(db_sql)
    #
    # def execute_query(self):
    #     result = []
    #     for query_sql in self.query_sql:
    #         res = self.client.execute(query_sql)
    #         result.append(res)
    #     return result


def main():
    res_counter = collections.Counter()

    click_testcase_base_dir = pathlib.Path('../clickhouse/clickhouse_testcase/clickhouse')

    for testcase_dir in click_testcase_base_dir.glob("testcase*"):
        print(f"\nProcessing {testcase_dir}")

        duck_test_case = DuckTestCase(testcase_dir)
        duck_res = None
        duck_error = None
        try:
            duck_test_case.execute_db()
            duck_res = duck_test_case.execute_query()
        except Exception as e:
            duck_error = e
            print(f"Duck error: {e}")

        click_test_case = ClickTestCase(testcase_dir)
        click_res = click_test_case.result
        click_error = None
        # try:
        #     click_test_case.execute_db()
        #     click_res = click_test_case.execute_query()
        # except Exception as e:
        #     click_error = e
        #     print(f"Click error: {e}")

        if duck_error is None and click_error is None:
            if str(duck_res).strip() == click_res:
                res_counter['same'] += 1
                print(f"[RESULT] same")
            else:
                res_counter['different'] += 1
                print(f"[RESULT] different")
                print(f"duck: {duck_res}")
                print(f"click: {click_res}")
        else:
            res_counter['error'] += 1
            print(f"[RESULT] error, duck {duck_error is None}, click {click_error is None}")

        # break

    with open('../stats/duck_run_click.stat', 'w') as f:
        f.write(str(res_counter['error']) + '\n')
        f.write(str(res_counter['same']) + '\n')
        f.write(str(res_counter['different']) + '\n')
    print(res_counter)


if __name__ == '__main__':
    main()
