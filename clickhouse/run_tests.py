import os
from clickhouse_driver import Client
import re

def create_client():
    print("connect...")
    host = 'clickhouse'  
    user = 'default' 
    password = '' 
    database = 'test'  
    return Client(host=host, user=user, password=password, database=database)

def execute_sql_script(client, file_path):
    with open(file_path, 'r') as file:
        sql_statements = file.read().split(';')

    results = []
    for statement in sql_statements:
        if statement.strip():  # Check if statement is not empty
            result = client.execute(statement)
            if result:
                results.append(result)
    return results

def drop_selected_tables(client):
    tables = client.execute("SHOW TABLES")
    for table in tables:
        client.execute(f"DROP TABLE IF EXISTS {table[0]}")
    print(tables)

def process_folder(client, folder_name):
    db_sql_path = os.path.join(folder_name, 'db.sql')
    test_sql_path = os.path.join(folder_name, 'test.sql')
    result_path = os.path.join(folder_name, 'result.txt')
    
    execute_sql_script(client, db_sql_path)
    test_results = execute_sql_script(client, test_sql_path)
    
    with open(result_path, 'w', encoding='utf-8') as result_file:
        for result in test_results:
            result_file.write(str(result) + '\n')


client = create_client()

drop_selected_tables(client)

folders = [f for f in os.listdir('.') if os.path.isdir(f) and f.startswith('testcase')]
folders.sort(key=lambda x: int(x.replace('testcase', '')))

folder_count = 0
for folder in folders:
    process_folder(client, folder)
    print(f'Processed {folder}')
    drop_selected_tables(client)
    folder_count += 1
    if folder_count % 5 == 0:
        client.disconnect()
        client = create_client()

client.disconnect()
