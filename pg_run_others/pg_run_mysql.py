# run duckdb testcase in psql
import pathlib
import collections

import mysql.connector
import psycopg2

mysql_conn_params = {
    'user': 'root',
    'password': "123456",
    'host': 'mysql',
    'port': '3306',
    'database': 'test'
}

pg_conn_params = {
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
            # lower_sql = db_sql.lower()
            # if "sql_mode" in lower_sql or "sql_warnings" in lower_sql:
            #     continue
            self.cur.execute(db_sql)

    def execute_query(self):
        self.cur.execute(self.query_sql[0])
        return self.cur.fetchall()

    # destructor
    def __del__(self):
        self.cur.close()
        self.conn.close()


class MysqlTestCase:
    def __init__(self, test_case_dir):
        # self.conn = mysql.connector.connect(**mysql_conn_params)
        # self.conn.autocommit = True
        # self.cur = self.conn.cursor()

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

    # def init_mysql(self):
    #     self.cur.execute("SHOW TABLES")
    #     tables = self.cur.fetchall()
    #     for table in tables:
    #         self.cur.execute(f"DROP TABLE IF EXISTS {table[0]}")
    #
    # def execute_db(self):
    #     self.init_mysql()
    #     for db_sql in self.db_sql:
    #         self.cur.execute(db_sql)
    #         self.cur.fetchall()
    #
    # def execute_query(self):
    #     result = []
    #     for query_sql in self.query_sql:
    #         self.cur.execute(query_sql)
    #         result.append(self.cur.fetchall())
    #     return result
    #
    # # destructor
    # def __del__(self):
    #     self.cur.close()
    #     self.conn.close()


def main():
    res_counter = collections.Counter()

    mysql_testcase_base_dir = pathlib.Path('../mysql/mysql_testcase')

    for testcase_dir in mysql_testcase_base_dir.glob("testcase*"):
        print(f"\nProcessing {testcase_dir}")

        pg_test_case = PgTestCase(testcase_dir)
        pg_res = None
        pg_error = None
        try:
            pg_test_case.execute_db()
            pg_res = pg_test_case.execute_query()
        except Exception as e:
            pg_error = e
            print(f"pg error: {e}")

        mysql_test_case = MysqlTestCase(testcase_dir)
        mysql_res = mysql_test_case.result
        mysql_error = None
        # try:
        #     mysql_test_case.execute_db()
        #     mysql_res = mysql_test_case.execute_query()
        # except Exception as e:
        #     mysql_error = e
        #     print(f"mysql error: {e}")

        if pg_error is None and mysql_error is None:
            if str(pg_res).strip() == mysql_res:
                res_counter['same'] += 1
                print(f"[RESULT] same")
            else:
                res_counter['different'] += 1
                print(f"[RESULT] different")
                print(f"pg: {pg_res}")
                print(f"mysql: {mysql_res}")
        else:
            res_counter['error'] += 1
            print(f"[RESULT] error, pg {pg_error is None}, mysql {mysql_error is None}")

        # break
    with open('../stats/pg_run_mysql.stat', 'w') as f:
        f.write(str(res_counter['error']) + '\n')
        f.write(str(res_counter['same']) + '\n')
        f.write(str(res_counter['different']) + '\n')
    print(res_counter)


if __name__ == '__main__':
    main()
