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


def remove_comments(line):
    comment_start_index = line.find('--')

    if comment_start_index != -1:
        return line[:comment_start_index].strip()
    else:
        return line.strip()


def exe_sql(cur, filepath):
    commands = []
    with open(filepath, 'r') as file:
        for command in file.readlines():
            command = command.strip()
            command = remove_comments(command)
            commands.append(command)
    commands = '\n'.join(commands)
    for command in commands.split(';'):
        command = command.strip()
        if command:
            cur.execute(command)


def init_pg(cur):
    # Query to get all user views
    cur.execute("""
        DROP SCHEMA IF EXISTS public CASCADE;
        CREATE SCHEMA public;
        GRANT ALL ON SCHEMA public TO public;

        DROP SCHEMA IF EXISTS information_schema CASCADE;
        DROP SCHEMA IF EXISTS testxmlschema CASCADE;
    """)

    exe_sql(cur, '../postgresql/test_setup_for_run.sql')


class DuckTestCase:
    def __init__(self, test_case_dir):
        self.c = duckdb.connect()
        self.cur = self.c.cursor()

        self.db_sql = []
        self.query_sql = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        with open(self.test_case_dir / "db.sql", 'r') as f:
            self.db_sql = "".join(f.readlines())

        # read query.sql
        with open(self.test_case_dir / "test.sql", 'r') as f:
            self.query_sql = "".join(f.readlines())

    def execute_db(self):
        exe_sql(self.cur, "../postgresql/test_setup_for_duckdb.sql")

        if self.db_sql:
            self.cur.execute(self.db_sql)

    def execute_query(self):
        return self.cur.execute(self.query_sql).fetchall()

    # destructor
    def __del__(self):
        self.cur.close()
        self.c.close()


class PgTestCase:
    def __init__(self, test_case_dir):
        self.conn = psycopg2.connect(**conn_params)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

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

        with open(self.test_case_dir / "result.txt", 'r') as f:
            self.result = f.read().strip()

    # def execute_db(self):
    #     if self.db_sql:
    #         self.cur.execute(self.db_sql)
    #
    # def execute_query(self):
    #     self.cur.execute(self.query_sql)
    #     return self.cur.fetchall()
    #
    # # destructor
    # def __del__(self):
    #     self.cur.close()
    #     self.conn.close()


def main():
    res_counter = collections.Counter()

    pg_testcase_base_dir = pathlib.Path('../postgresql/pg_testcase_data')
    pg_testcase_txt = pathlib.Path('../postgresql/pass.txt')
    with open(pg_testcase_txt, 'r') as f:
        pg_testcase_list = [line.strip() for line in f.readlines()]

    # pg_testcase_list = ['create_misc.sql/0']

    for testcase in pg_testcase_list:
        testcase_dir = pg_testcase_base_dir / testcase
        print(f"\nProcessing {testcase_dir.relative_to(pg_testcase_base_dir)}")

        duck_test_case = DuckTestCase(testcase_dir)
        duck_res = None
        duck_error = None
        try:
            duck_test_case.execute_db()
            duck_res = duck_test_case.execute_query()
        except Exception as e:
            duck_error = e
            print(f"duckdb: {e}")

        pg_test_case = PgTestCase(testcase_dir)
        pg_res = pg_test_case.result
        pg_error = None
        # try:
        #     pg_test_case.execute_db()
        #     pg_res = pg_test_case.execute_query()
        # except Exception as e:
        #     pg_error = e
        #     print(e)

        if duck_error is None and pg_error is None:
            if str(duck_res).strip() == pg_res:
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

    with open('../stats/duck_run_pg.stat', 'w') as f:
        f.write(str(res_counter['error']) + '\n')
        f.write(str(res_counter['same']) + '\n')
        f.write(str(res_counter['different']) + '\n')
    print(res_counter)


if __name__ == '__main__':
    main()
