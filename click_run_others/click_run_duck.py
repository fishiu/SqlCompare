import os
from clickhouse_driver import Client
import re
import pathlib
import shutil

def create_client():
    print("connect...")
    host = '127.0.0.1' 
    user = 'default'
    password = ''
    database = 'test'  
    return Client(host=host, user=user, password=password, database=database)



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
        with open(self.test_case_dir / "test.sql", 'r') as f:
            # for line in f:
            #     self.query_sql.append(line.strip())
            self.query_sql = "".join(f.readlines())

        # read result.txt
        with open(self.test_case_dir / "result.txt", 'r') as f:
            # for line in f:
            #     self.result.append(line.strip())
            self.result = "".join(f.readlines())

    def execute(self, client):
        result_path = os.path.join(self.test_case_dir, 'result_clickhouse.txt')
        print("----db.sql----")
        if self.db_sql:
            try:
                client.execute(self.db_sql)
                # if result:
                #     print(result)
                # result_file.write(str(result) + '\n')
            except Exception as e:
                print((f"ERROR in {self.test_case_dir}: {str(e)}",))

        print("----query.sql----")
        # print(client.execute(self.query_sql))
        try:
            result = client.execute(self.query_sql)
            # results.append(str(result) + '\n')
        except Exception as e:
            result = (f"ERROR in {self.test_case_dir}: {str(e)}",)

        print("----result----")
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


def main():

    target_folders = ['same', 'error', 'different']

    for folder in target_folders:
        os.makedirs(os.path.join('./', folder), exist_ok=True)



    with open('pass.txt', 'r') as f:
        testcase_ommit_set.update([line.strip() for line in f.readlines()])
    testcase_base_dir = pathlib.Path('./duckdb_testcase_data')
    with open('pass.txt', 'a') as f:
        for testcase_group_dir in testcase_base_dir.rglob("*.test"):
            relative = testcase_group_dir.relative_to(testcase_base_dir)
            if str(relative) in ommit_set:
                continue
            if not testcase_group_dir.is_dir():
                continue
            # print(f"Processing {testcase_group_dir.absolute()}")
            for testcase_dir in testcase_group_dir.iterdir():
                testcase_relative = str(testcase_dir.relative_to(testcase_base_dir))
                
                if str(testcase_relative) in testcase_ommit_set:
                    continue
                print(testcase_relative)
                test_case = DuckTestCase(testcase_dir)
                client = create_client()
                test_case.execute(client)
                client.disconnect()
                # f.write(testcase_relative + '\n')


if __name__ == '__main__':
    main()


    # single_test()
