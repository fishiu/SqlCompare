# run duckdb testcase in psql
import pathlib
import collections

import clickhouse_driver
import psycopg2

click_conn_params = {
    'host': 'localhost',
    'port': '19000',
}

pg_conn_params = {
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432',
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


class PgTestCase:
    def __init__(self, test_case_dir):
        self.conn = psycopg2.connect(**pg_conn_params)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        self.db_sql = []
        self.query_sql = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        self.db_sql = split_sql(self.test_case_dir / "db.sql")

        # read query.sql
        self.query_sql = split_sql(self.test_case_dir / "test.sql")
        # assert len(self.query_sql) == 1
        if len(self.query_sql) > 1:
            print(f"Multiple queries found in {self.test_case_dir}")
        self.query_sql = self.query_sql[0]

    def init_pg(self):
        self.cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public';")
        tables = self.cur.fetchall()
        for table in tables:
            self.cur.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE;")

        # Query to get all user views
        self.cur.execute("""
            DROP SCHEMA IF EXISTS public CASCADE;
            CREATE SCHEMA public;
            GRANT ALL ON SCHEMA public TO public;
        """)

    def execute_db(self):
        self.init_pg()
        for db_sql in self.db_sql:
            self.cur.execute(db_sql)

    def execute_query(self):
        self.cur.execute(self.query_sql)
        return self.cur.fetchall()

    # destructor
    def __del__(self):
        self.cur.close()
        self.conn.close()


class ClickTestCase:
    def __init__(self, test_case_dir):
        # self.client = clickhouse_driver.Client(**click_conn_params)

        self.db_sql = []
        self.query_sql = []
        self.result = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        self.db_sql = split_sql(self.test_case_dir / "db.sql")

        # read query.sql
        self.query_sql = split_sql(self.test_case_dir / "test.sql")

        with open(self.test_case_dir / "result.txt", 'r') as file:
            self.result = file.read().strip()

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

        pg_test_case = PgTestCase(testcase_dir)
        pg_res = None
        pg_error = None
        try:
            pg_test_case.execute_db()
            pg_res = pg_test_case.execute_query()
        except Exception as e:
            pg_error = e
            print(f"Pg error: {e}")
        # pg_test_case.execute_db()
        # pg_res_list = pg_test_case.execute_query()

        click_test_case = ClickTestCase(testcase_dir)
        click_res = click_test_case.result
        click_error = None
        # try:
        #     click_test_case.execute_db()
        #     click_res = click_test_case.execute_query()
        # except Exception as e:
        #     click_error = e
        #     print(f"Click error: {e}")

        if pg_error is None and click_error is None:
            if str(pg_res).strip() == click_res:
                res_counter['same'] += 1
                print(f"[RESULT] same")
            else:
                res_counter['different'] += 1
                print(f"[RESULT] different")
                print(f"pg: {pg_res}")
                print(f"click: {click_res}")
        else:
            res_counter['error'] += 1
            print(f"[RESULT] error, pg {pg_error is None}, click {click_error is None}")

        # break

    with open('../stats/pg_run_click.stat', 'w') as f:
        f.write(str(res_counter['error']) + '\n')
        f.write(str(res_counter['same']) + '\n')
        f.write(str(res_counter['different']) + '\n')
    print(res_counter)


if __name__ == '__main__':
    main()
