import pathlib
import shutil
from enum import Enum


src_base_dir = pathlib.Path('../../postgres/src/test/regress/sql')
testcase_base_dir = pathlib.Path('./testcase_data')

omit_set = {
    'collate.windows.win1252.sql',
}


class StType(Enum):
    EMPTY = 0
    COMMENT = 1
    QUERY = 2
    OTHER = 3
    NONE = 4
    COMMAND = 5
    BEGIN = 6
    DOLLAR = 7


class Statement:
    def __init__(self):
        self.type = StType.NONE
        self.statement_list = []


class TestFile:
    def __init__(self, test_file: pathlib.Path):
        self.test_file = test_file
        self.test_name = self.test_file.relative_to(src_base_dir)
        print(f"Processing {self.test_name}")

    def generate_testcases(self):
        test_case_cnt = 0
        test_case_list = []
        with self.test_file.open('r') as f:
            all_db = []

            while stmt := self.next_stmt(f):
                if stmt.type == StType.NONE:
                    break
                if stmt.type in [StType.COMMAND, StType.BEGIN, StType.DOLLAR]:
                    return []
                # comment
                if stmt.type == StType.COMMENT:
                    continue
                # empty line
                if stmt.type == StType.EMPTY:
                    continue
                # query
                if stmt.type == StType.QUERY:
                    test_case = TestCase(test_case_cnt, self.test_name)
                    test_case_cnt += 1
                    test_case.query_sql.extend(stmt.statement_list)
                    test_case.db_sql = all_db.copy()
                    test_case_list.append(test_case)
                # other (db)
                if stmt.type == StType.OTHER:
                    all_db.extend(stmt.statement_list)

        return test_case_list

    @staticmethod
    def next_stmt(f):
        statement = Statement()
        while line := f.readline():
            line = line.strip()
            if '\\' in line:
                statement.type = StType.COMMAND
                return statement
            if 'BEGIN' in line or 'begin' in line:
                statement.type = StType.BEGIN
                return statement
            if '$$' in line:
                statement.type = StType.DOLLAR
                return statement

            if line == '':
                statement.type = StType.EMPTY
                return statement
            elif line.startswith('--'):
                statement.type = StType.COMMENT
                statement.statement_list.append(line)
                return statement
            else:
                if line.startswith('SELECT') or line.startswith('select'):
                    statement.type = StType.QUERY
                else:
                    statement.type = StType.OTHER
                statement.statement_list.append(line)
                if line.endswith(';'):
                    return statement
                else:
                    while line := f.readline():
                        line = line.strip()

                        if '\\' in line:
                            statement.type = StType.COMMAND
                            return statement
                        if 'BEGIN' in line or 'begin' in line:
                            statement.type = StType.BEGIN
                            return statement
                        if '$$' in line:
                            statement.type = StType.DOLLAR
                            return statement

                        statement.statement_list.append(line)
                        if line.endswith(';'):
                            return statement
        return statement


class TestCase:
    def __init__(self, test_case_id: int, test_case_name):
        self.tid = test_case_id
        self.db_sql = []
        self.query_sql = []
        self.result = []

        self.test_case_directory = testcase_base_dir / test_case_name / str(test_case_id)

    def save_to_3files(self):
        if not self.test_case_directory.exists():
            self.test_case_directory.mkdir(parents=True, exist_ok=True)

        # db.sql
        with open(self.test_case_directory / "db.sql", 'w') as f:
            for line in self.db_sql:
                f.write(line + "\n")
        # query.sql
        with open(self.test_case_directory / "query.sql", 'w') as f:
            for line in self.query_sql:
                f.write(line + "\n")
        # result.txt
        with open(self.test_case_directory / "result.txt", 'w') as f:
            for line in self.result:
                f.write(line + "\n")


def single():
    test_file = TestFile(src_base_dir / "xmlmap.sql")
    for test_case in test_file.generate_testcases():
        test_case.save_to_3files()


def main():
    if testcase_base_dir.exists():
        shutil.rmtree(testcase_base_dir)
    testcase_base_dir.mkdir(parents=True, exist_ok=True)

    cnt = 0

    for test_file in src_base_dir.rglob("*.sql"):
        relative_name = test_file.relative_to(src_base_dir)
        if str(relative_name) in omit_set:
            continue
        print(f"Processing {relative_name}")
        test_file = TestFile(test_file)
        for test_case in test_file.generate_testcases():
            cnt += 1
            test_case.save_to_3files()
    print(f"Total {cnt} test cases")


if __name__ == '__main__':
    # single()
    main()
