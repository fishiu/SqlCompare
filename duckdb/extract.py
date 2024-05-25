import pathlib
import collections
import shutil

testcase_data_dir = "./testcase_data"
problematic_test_cases = (
    "insert/test_insert_invalid.test",
    # "types/enum/test_enum_constraints.test",
    # "types/enum/test_alter_enum.test",
    # "settings/reset/reset_threads.test",
    # "prepared/prepare_summarize.test",
    # "join/iejoin/predicate_expressions.test",
)


pragma_counter = collections.Counter()

class DuckTestFile:
    def __init__(self, src_base_dir: pathlib.Path, test_file: pathlib.Path):
        self.test_file = test_file
        self.test_name = self.test_file.relative_to(src_base_dir)

    def generate_test_cases(self):
        test_case_cnt = 0
        duck_test_case_list = []
        lineno = 0
        with self.test_file.open('r') as f:
            duck_test_case = None
            all_statement = []

            while line := f.readline():
                lineno += 1
                line = line.strip()

                if line.startswith("#"):
                    # comments
                    if isinstance(duck_test_case, DuckTestCase):
                        duck_test_case.push_comment(line)
                elif line == "":
                    # empty line, terminate current test case
                    if duck_test_case is not None:
                        print("Error: Should never happen")
                        duck_test_case_list.append(duck_test_case)
                        duck_test_case = None
                elif line.startswith("statement"):
                    # statement
                    statement_split = line.split(" ")
                    statement_type = statement_split[1]
                    if len(statement_split) >= 3:
                        if statement_split[2].startswith("con"):
                            # multi connection (transaction)
                            return []
                    if statement_type == "error":
                        # ignore error statement
                        while True:
                            line = f.readline().strip()
                            lineno += 1
                            if line == "":
                                break
                    else:
                        while True:
                            line = f.readline().strip()
                            lineno += 1
                            if line == "":
                                if len(all_statement) > 0 and not all_statement[-1].endswith(";"):
                                    all_statement[-1] += ";"
                                break
                            elif line.startswith("PRAGMA"):
                                pragma_counter[line] += 1
                                if line.startswith("PRAGMA enable_verification"):
                                    continue
                                else:
                                    return []
                            elif line.startswith("#"):
                                continue
                            elif 'read_csv' in line or '.csv' in line or '__TEST_DIR__/' in line:
                                return []
                            all_statement.append(line)
                elif line.startswith("query"):
                    query_split = line.split(" ")
                    for s in query_split:
                        if s.startswith("con"):
                            # multi connection (transaction)
                            return []

                    duck_test_case = DuckTestCase(test_case_cnt, self.test_name)
                    test_case_cnt += 1
                    duck_test_case.query_type = DuckQueryType(line)
                    duck_test_case.db_sql = all_statement.copy()

                    skip_result = True
                    # read query
                    while True:
                        line = f.readline().strip()
                        lineno += 1
                        # assume that the query is always ended by "----"
                        if line == "----":
                            if len(duck_test_case.query_sql) > 0 and not duck_test_case.query_sql[-1].endswith(";"):
                                duck_test_case.query_sql[-1] += ";"  # add missing semicolon
                            break
                        elif line == "":
                            if len(duck_test_case.query_sql) > 0 and not duck_test_case.query_sql[-1].endswith(";"):
                                duck_test_case.query_sql[-1] += ";"  # add missing semicolon
                            skip_result = False
                            break
                        elif 'read_csv' in line or '.csv' in line or '__TEST_DIR__/' in line:
                            return []
                        else:
                            duck_test_case.query_sql.append(line)

                    # not "----" terminating query, skip result
                    if not skip_result:
                        continue

                    # read result
                    while True:
                        line = f.readline().strip()
                        lineno += 1
                        if line == "":
                            duck_test_case_list.append(duck_test_case)
                            duck_test_case = None
                            break
                        else:
                            duck_test_case.result.append(line)
                elif line.startswith("loop") or line.startswith("foreach") or line.startswith("concurrentloop"):
                    # print(f"Abort: loop/foreach not supported yet")
                    return []
                    # while True:
                    #     line = f.readline().strip()
                    #     lineno += 1
                    #     if line.startswith("endloop"):
                    #         break
                    #     elif line.startswith("statement"):
                    #         print("Warn: statement in loop detected")
                elif line.startswith("require"):
                    required = line.split(" ")[1]
                    if required == "skip_reload":
                        ...
                    else:
                        print(f"Error: Unknown require: {required}")
                    return []
                elif line.startswith("mode"):
                    mode = line.split(" ")[1]
                    if mode == "skip":
                        while line := f.readline():
                            line = line.strip()
                            lineno += 1
                            if line.startswith("mode unskip"):
                                break
                    elif mode == "unskip":
                        print(f"Error: No matching mode skip for this unskip")
                    else:
                        print(f"Error: Unknown mode: {mode}")
                elif line.startswith("load"):
                    return []
                else:
                    print(f"Error: Unknown line[{lineno}]: {line}")
                    return []
        # print(f"Info: saved {test_case_cnt} test cases")
        return duck_test_case_list


class DuckQueryType:
    def __init__(self, query_type: str):
        # query <type-string> <sort-mode> <label>
        self.query_type = query_type
        self.type_string = None
        self.sort_mode = None
        self.label = None

        query_split = self.query_type.split(" ")
        if len(query_split) >= 2:
            self.type_string = query_split[1]
        if len(query_split) >= 3:
            self.sort_mode = query_split[2]
        if len(query_split) >= 4:
            self.label = query_split[3]


class DuckTestCase:
    def __init__(self, test_case_id: int, test_case_name):
        self.tid = test_case_id
        self.query_type = None
        self.db_sql = []
        self.query_sql = []
        self.result = []

        self.test_case_directory = pathlib.Path(testcase_data_dir) / str(test_case_name) / str(test_case_id)

    def push_comment(self, comment: str):
        pass

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


def main():
    test_src_base_dir = pathlib.Path("../../duckdb/test/sql")

    # recreate the directory
    td = pathlib.Path(testcase_data_dir)
    if td.exists():
        shutil.rmtree(td)
    td.mkdir(parents=True, exist_ok=True)

    counter = 0
    for sub_base_dir in test_src_base_dir.iterdir():
        if sub_base_dir.is_dir():
            print(f"------------- Processing {sub_base_dir} -------------")
            for test_file in sub_base_dir.rglob("*.test"):
                relative = test_file.relative_to(test_src_base_dir)
                if str(relative) in problematic_test_cases:
                    continue
                print(f"Processing {relative}")
                duck_test_file = DuckTestFile(test_src_base_dir, test_file)
                for test_case in duck_test_file.generate_test_cases():
                    test_case.save_to_3files()
                    counter += 1
    print(pragma_counter)
    print(f"Total {counter} test cases saved")


def single_case():
    test_src_base_dir = pathlib.Path("../../duckdb/test/sql")
    test_file = test_src_base_dir / "order/test_limit.test"
    duck_test_file = DuckTestFile(test_src_base_dir, test_file)
    for test_case in duck_test_file.generate_test_cases():
        test_case.save_to_3files()


if __name__ == '__main__':
    main()
    # single_case()
