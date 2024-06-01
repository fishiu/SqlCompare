import os
import mysql.connector
import shutil

def create_client():
    print("Connecting to MySQL database...")
    connection = mysql.connector.connect(
        host="mysql",
        user="root",
        password="123456",
        database="test"
    )
    return connection

def execute_sql_script(connection, file_path):
    """Read SQL file and execute, returning results"""
    with open(file_path, 'r') as file:
        sql_script = file.read()
    
    sql_statements = sql_script.split(';')
    cursor = connection.cursor()
    results = []
    for statement in sql_statements:
        if statement.strip():
            try:
                cursor.execute(statement)
                result = cursor.fetchall()
                if result:
                    results.append(result)
            except Exception as e:
                results.append((f"ERROR in {file_path}: {str(e)}",))  # Append error as tuple
    cursor.close()
    return results

def drop_selected_tables(connection):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(tables)
    for table in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
        except Exception as e:
            print((f"ERROR in dropping: {str(e)}",))
    cursor.close()

def process_folder(connection, folder_name):
    """Process a single folder, execute SQL scripts and save results"""
    # print(folder_name)
    db_sql_path = os.path.join(folder_name, 'db.sql')
    test_sql_path = os.path.join(folder_name, 'test.sql')
    result_path = os.path.join(folder_name, 'result_mysql.txt')
    txt_path = os.path.join(folder_name, 'result.txt')
    
    execute_sql_script(connection, db_sql_path)
    test_results = execute_sql_script(connection, test_sql_path)
    
    with open(result_path, 'w', encoding='utf-8') as result_file:
        for result in test_results:
            result_file.write(str(result) + '\n')

    with open(result_path, 'r', encoding='utf-8') as file_mysql, \
        open(txt_path, 'r', encoding='utf-8') as file_result:
        result = file_mysql.read().lower().strip()
        content_result = file_result.read().lower().strip()

    if 'error' in str(result).lower():
        target_folder = 'error'
    elif str(result).lower() == content_result:
        target_folder = 'same'
    else:
        target_folder = 'different'

    
    destination_path = os.path.join(target_folder, folder_name.split('/')[-1])
    shutil.copytree(folder_name, destination_path)

def main():

    target_folders = ['same', 'error', 'different']

    for folder in target_folders:
        os.makedirs(os.path.join('./', folder), exist_ok=True)

    # Initialize database connection
    connection = create_client()
    # Drop all tables
    drop_selected_tables(connection)

    test_cases_dir = os.path.join('./', 'click_testcase_data')

    # Get all folders starting with 'testcase' and sort them by the numerical suffix
    folders = [os.path.join(test_cases_dir, f) for f in os.listdir(test_cases_dir)
        if os.path.isdir(os.path.join(test_cases_dir, f)) and f.startswith('testcase')]
    folders.sort(key=lambda x: int(x.split('testcase')[-1]))

    # Process each folder
    for folder in folders:
        # print(folder)
        process_folder(connection, folder)
        drop_selected_tables(connection)
        print(f'Processed {folder}')
        connection.close()
        connection = create_client()

    # Cleanup and close database connection
    connection.close()

if __name__ == "__main__":
    main()
