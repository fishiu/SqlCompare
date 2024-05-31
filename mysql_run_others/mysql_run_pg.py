import os
import mysql.connector
import re
import pathlib
import shutil

ommit_set = {
    'macaddr8.sql',
    'date.sql',  # too many errors in db.sql
}

testcase_ommit_set = {
    '',
}

def create_client():
    print("Connecting to MySQL database...")
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="test"
    )
    return connection


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
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    print(tables)
    for table in tables:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {table[0]}")
        except Exception as e:
            print(11)
    # Query to get all user views
    # cur.execute("""
    #     DROP SCHEMA IF EXISTS public CASCADE;
    #     CREATE SCHEMA public;
    #     GRANT ALL ON SCHEMA public TO public;

    #     DROP SCHEMA IF EXISTS information_schema CASCADE;
    #     DROP SCHEMA IF EXISTS testxmlschema CASCADE;
    # """)

    exe_sql(cur, './test_setup_for_run.sql')


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
        result_path = os.path.join(self.test_case_dir, 'result_mysql.txt')
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(tables)
        for table in tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
            except Exception as e:
                print((f"ERROR in {self.test_case_dir}: {str(e)}",))
            
        print("----db.sql----")
        if self.db_sql:
            try:
                cursor.execute(self.db_sql)
                # print(cursor.fetchall())
            except Exception as e:
                print((f"ERROR in {self.test_case_dir}: {str(e)}",))

        print("----query.sql----")
        # print(client.execute(self.query_sql))
        try:
            cursor.execute(self.query_sql)
            result = cursor.fetchall()
            # results.append(str(result) + '\n')
        except Exception as e:
            result = (f"ERROR in {self.test_case_dir}: {str(e)}",)

        print("----result----")
        # print(self.result)
        # for line in self.db_sql:
        #     print(cursor.execute(line).fetchall())
        # print(self.result)

        with open(result_path, 'w', encoding='utf-8') as result_file:
            result_file.write(str(result) + '\n')

        if 'error' in str(result).lower():
            target_folder = 'error'
        elif str(result).lower() == self.result.lower().strip():
            target_folder = 'same'
        else:
            target_folder = 'different'
        
        destination_path = os.path.join('./', target_folder, f'{self.test_case_dir}')
        shutil.copytree(self.test_case_dir, destination_path)

        # print("----result----")
        # for line in self.result:
        #     print(line)
        # print(self.result)


def main():

    target_folders = ['same', 'error', 'different']

    for folder in target_folders:
        os.makedirs(os.path.join('./', folder), exist_ok=True)


    with open('pass.txt', 'r') as f:
        testcase_ommit_set.update([line.strip() for line in f.readlines()])
    testcase_base_dir = pathlib.Path('./pg_testcase_data')
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

                c = create_client()
                # init
                init_pg(c.cursor())
                test_case.execute(c.cursor())
                c.close()
                # f.write(testcase_relative + '\n')


if __name__ == '__main__':
    # init_pg_without_cur()
    main()
    # single_test()