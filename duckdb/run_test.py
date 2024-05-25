import pathlib
import duckdb

ommit_set = {
    'setops/union_all_projection_pushdown.test',
    'types/map/map_null.test',
    'types/nested/map/map_error.test',
    'prepared/prepare_copy.test',

    'insert/unaligned_interleaved_appends.test',  # multi connection not supported
    'update/test_update_many_updaters_nulls.test',  # multi connection not supported
    'update/test_update_delete_same_tuple.test',  # multi connection not supported
    'update/test_update_many_updaters.test',  # multi connection not supported
    'catalog/test_schema.test',  # multi connection not supported
    'catalog/dependencies/test_schema_dependency.test',  # multi connection not supported
    'alter/alter_col/test_not_null_multi_tran.test',  # multi connection not supported
    'alter/rename_table/test_rename_table_many_transactions.test',  # multi connection not supported

    'join/iejoin/test_iejoin_overlaps.test',  # extern csv data
    'topn/tpcds_q14_topn.test',  # extern csv data
    'topn/tpcds_q59_topn.test',  # extern csv data
    'subquery/lateral/pg_lateral.test',  # extern file
    'subquery/lateral/lateral_binding_views.test',  # extern file
    'cast/string_to_list_cast.test',  # extern file
    'copy/tmp_file.test',  # extern file
    'copy/csv/test_imdb.test',

    'function/string/null_byte.test',  # create in query

    'index/art/issues/test_art_issue_7349.test',  # constraint exception
}

testcase_ommit_set = {
    'types/interval/test_interval.test/32',
    'types/interval/test_interval.test/42',
    'types/interval/test_interval.test/43',
}


class DuckTestCase:
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
        with open(self.test_case_dir / "query.sql", 'r') as f:
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
            print(cursor.execute(self.db_sql).fetchall())

        print("----query.sql----")
        # for line in self.query_sql:
        #     print(cursor.execute(line).fetchall())
        print(cursor.execute(self.query_sql).fetchall())

        print("----result----")
        # for line in self.result:
        #     print(line)
        print(self.result)


def main():
    with open('pass.txt', 'r') as f:
        testcase_ommit_set.update([line.strip() for line in f.readlines()])
    testcase_base_dir = pathlib.Path('./testcase_data')
    with open('pass.txt', 'a') as f:
        for testcase_group_dir in testcase_base_dir.rglob("*.test"):
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
                test_case = DuckTestCase(testcase_dir)
                c = duckdb.connect()
                test_case.execute(c.cursor())
                c.close()
                f.write(testcase_relative + '\n')


def single_test():
    testcase_group_dir = pathlib.Path('./testcase_data/test_pivot.test')
    for testcase_dir in testcase_group_dir.iterdir():
        test_case = DuckTestCase(testcase_dir)
        c = duckdb.connect()
        test_case.execute(c.cursor())
        c.close()


if __name__ == '__main__':
    main()
    # single_test()
