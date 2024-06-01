import os
import mysql.connector

def create_client():
    print("Connecting to MySQL database...")
    connection = mysql.connector.connect(
        host="mysql",
        user="root",
        password="123456",
        database="default"
    )
    return connection



def execute_sql_script(connection, file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()
    
    sql_statements = sql_script.split(';')

    cursor = connection.cursor()
    results = []
    for statement in sql_statements:
        statement = statement.strip()
        if statement:  # 检查语句是否为空
            cursor.execute(statement)
            # print(statement)
            try:
                result = cursor.fetchall()
                if result:
                    results.append(result)
            except mysql.connector.Error as e:
                print(f"SQL Warning/Exception: {e}")
    cursor.close()
    return results


def drop_selected_tables(connection):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
    tables = cursor.fetchall()
    print("SHOW TABLES:", tables)
    cursor.execute(f"DROP DATABASE IF EXISTS mysqltest;")
    cursor.execute(f"DROP DATABASE IF EXISTS s;")
    cursor.execute(f"DROP EVENT IF EXISTS event2;")
    cursor.close()


def process_folder(connection, folder_name):
    db_sql_path = os.path.join(folder_name, 'db.sql')
    test_sql_path = os.path.join(folder_name, 'test.sql')
    result_path = os.path.join(folder_name, 'result.txt')
    
    execute_sql_script(connection, db_sql_path)
    test_results = execute_sql_script(connection, test_sql_path)
    
    with open(result_path, 'w', encoding='utf-8') as result_file:
        for result in test_results:
            result_file.write(str(result) + '\n')

connection = create_client()

drop_selected_tables(connection)

folders = [f for f in os.listdir('.') if os.path.isdir(f) and f.startswith('testcase')]
folders.sort(key=lambda x: int(x.replace('testcase', '')))

folder_count = 0
for folder in folders:
    process_folder(connection, folder)
    drop_selected_tables(connection)
    print(f'Processed {folder}')
    folder_count += 1
    connection.close()
    connection = create_client()

connection.close()