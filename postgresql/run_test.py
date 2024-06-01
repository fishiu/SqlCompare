import pathlib
import shutil

import psycopg2

ommit_set = {
    'macaddr8.sql',
    'date.sql',  # too many errors in db.sql
}

testcase_ommit_set = {
    '',
}

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


# Function to drop all user-created objects
def init_pg(cur):
    # Query to get all user views
    cur.execute("""
        DROP SCHEMA IF EXISTS public CASCADE;
        CREATE SCHEMA public;
        GRANT ALL ON SCHEMA public TO public;

        DROP SCHEMA IF EXISTS testxmlschema CASCADE;
    """)

    exe_sql(cur, './pg_init.sql')


def init_pg_without_cur():
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()
    init_pg(cur)
    cur.close()
    conn.close()


class TestCase:
    def __init__(self, test_case_dir):
        self.db_sql = []
        self.query_sql = []
        self.result = []

        self.test_case_dir = pathlib.Path(test_case_dir)

        # read db.sql
        with open(self.test_case_dir / "db.sql", 'r') as f:
            # for line in f:
            #     self.db_sql.append(line.strip())
            self.db_sql = "".join(f.readlines())

        # read query.sql
        with open(self.test_case_dir / "test.sql", 'r') as f:
            # for line in f:
            #     self.query_sql.append(line.strip())
            self.query_sql = "".join(f.readlines())

        # read result.txt
        with open(self.test_case_dir / "result.txt", 'r') as f:
            # for line in f:
            #     self.result.append(line.strip())
            self.result = "".join(f.readlines())

    def execute(self, cursor):
        print("----db.sql----")
        # for line in self.db_sql:
        #     print(cursor.execute(line).fetchall())
        if self.db_sql:
            print(cursor.execute(self.db_sql))

        print("----query.sql----")
        # for line in self.query_sql:
        #     print(cursor.execute(line).fetchall())
        cursor.execute(self.query_sql)
        res = cursor.fetchall()
        print(res)

        # save result
        with open(self.test_case_dir / "result.txt", 'w') as f:
            f.write(str(res) + '\n')


def main():
    with open('pass.txt', 'r') as f:
        testcase_ommit_set.update([line.strip() for line in f.readlines()])
    testcase_base_dir = pathlib.Path('./testcase_data')
    with open('pass.txt', 'a') as f:
        for testcase_group_dir in testcase_base_dir.iterdir():
            relative = testcase_group_dir.relative_to(testcase_base_dir)
            if str(relative) in ommit_set:
                continue
            if not testcase_group_dir.is_dir():
                continue
            print(f"Processing {testcase_group_dir.absolute()}")
            for testcase_dir in testcase_group_dir.iterdir():
                testcase_relative = str(testcase_dir.relative_to(testcase_base_dir))
                print(testcase_relative)
                if str(testcase_relative) in testcase_ommit_set:
                    continue
                test_case = TestCase(testcase_dir)

                conn = psycopg2.connect(**conn_params)
                conn.autocommit = True
                cur = conn.cursor()

                # init
                init_pg(cur)

                try:
                    test_case.execute(cur)
                except Exception as e:
                    print(e)
                    continue
                cur.close()
                conn.close()
                f.write(testcase_relative + '\n')


def single_test():
    testcase_group_dir = pathlib.Path('./testcase_data/amutils.sql')
    for testcase_dir in testcase_group_dir.iterdir():
        print(testcase_dir)
        test_case = TestCase(testcase_dir)
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cur = conn.cursor()

        init_pg(cur)
        test_case.execute(cur)

        cur.close()
        conn.close()


def clean_res():
    suc_set = set()
    with open('pass.txt', 'r') as f:
        suc_set.update([line.strip() for line in f.readlines()])

    target_dir = pathlib.Path('./pg_testcase_data')
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir()

    cnt = 0
    for suc in suc_set:
        src = pathlib.Path(f'./testcase_data/{suc}')
        target = pathlib.Path(f'./pg_testcase_data/{suc}')
        if not target.parent.exists():
            target.parent.mkdir(parents=True)
        shutil.copytree(src, target)
        cnt += 1
    print(f"Copy {cnt} dirs")


if __name__ == '__main__':
    # init_pg_without_cur()
    main()
    # single_test()
    clean_res()
