# run duckdb testcase in psql
import pathlib
import collections

import psycopg2
import duckdb

conn_params = {
    'user': 'postgres',
    'password': '123456',
    'host': 'postgres',
    'port': '5432',
}


def init_pg(cur):
    # Query to get all user views
    cur.execute("""
        DROP SCHEMA IF EXISTS public CASCADE;
        CREATE SCHEMA public;
        GRANT ALL ON SCHEMA public TO public;

        DROP SCHEMA IF EXISTS information_schema CASCADE;
        DROP SCHEMA IF EXISTS testxmlschema CASCADE;
    """)


class DuckTestCase:
    def __init__(self, test_case_dir):
        # self.c = duckdb.connect()
        # self.cur = self.c.cursor()

        self.db_sql = []
        self.query_sql = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        with open(self.test_case_dir / "db.sql", 'r') as f:
            self.db_sql = "".join(f.readlines())

        # read query.sql
        with open(self.test_case_dir / "test.sql", 'r') as f:
            self.query_sql = "".join(f.readlines())

        with open(self.test_case_dir / "result.txt", 'r') as f:
            self.result = f.read().strip()

    # def execute_db(self):
    #     if self.db_sql:
    #         self.cur.execute(self.db_sql)
    #
    # def execute_query(self):
    #     return self.cur.execute(self.query_sql).fetchall()
    #
    # # destructor
    # def __del__(self):
    #     self.cur.close()
    #     self.c.close()


class PgTestCase:
    def __init__(self, test_case_dir):
        self.conn = psycopg2.connect(**conn_params)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        init_pg(self.cur)

        self.db_sql = []
        self.query_sql = []
        self.result = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        with open(self.test_case_dir / "db.sql", 'r') as f:
            self.db_sql = "".join(f.readlines())

        # read query.sql
        with open(self.test_case_dir / "test.sql", 'r') as f:
            self.query_sql = "".join(f.readlines())

    def execute_db(self):
        if self.db_sql:
            self.cur.execute(self.db_sql)

    def execute_query(self):
        self.cur.execute(self.query_sql)
        return self.cur.fetchall()

    # destructor
    def __del__(self):
        self.cur.close()
        self.conn.close()


def main():
    res_counter = collections.Counter()

    duck_testcase_base_dir = pathlib.Path('../duckdb/testcase_data')
    duck_testcase_txt = pathlib.Path('../duckdb/pass.txt')
    with open(duck_testcase_txt, 'r') as f:
        duck_testcase_list = [line.strip() for line in f.readlines()]

    # duck_testcase_list = ['']

    for testcase in duck_testcase_list:
        testcase_dir = duck_testcase_base_dir / testcase
        print(f"Processing {testcase_dir.relative_to(duck_testcase_base_dir)}")

        duck_test_case = DuckTestCase(testcase_dir)
        duck_res = duck_test_case.result
        duck_error = None
        # try:
        #     duck_test_case.execute_db()
        #     duck_res = duck_test_case.execute_query()
        # except Exception as e:
        #     duck_error = e
        #     print(e)

        pg_test_case = PgTestCase(testcase_dir)
        pg_res = None
        pg_error = None
        try:
            pg_test_case.execute_db()
            pg_res = pg_test_case.execute_query()
        except Exception as e:
            pg_error = e
            print(e)

        if duck_error is None and pg_error is None:
            if duck_res == str(pg_res).strip():
                res_counter['same'] += 1
                print(f"[RESULT] same")
            else:
                res_counter['different'] += 1
                print(f"[RESULT] different")
                print(f"duck: {duck_res}")
                print(f"pg: {pg_res}")
        else:
            res_counter['error'] += 1
            print(f"[RESULT] error, duck {duck_error is None}, pg {pg_error is None}")

    with open('../stats/pg_run_duck.stat', 'w') as f:
        f.write(str(res_counter['error']) + '\n')
        f.write(str(res_counter['same']) + '\n')
        f.write(str(res_counter['different']) + '\n')
    print(res_counter)


if __name__ == '__main__':
    main()
