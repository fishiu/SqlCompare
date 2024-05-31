import os
import re


def remove_comments(sql_script):
    cleaned_script = re.sub(r'(?m)^--error.*?(?=\n\s*\n)', '', sql_script, flags=re.DOTALL)
    cleaned_script = re.sub(r'(?m)^\s*--(?!9223372036854775808).*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)^let.*\n?', '', cleaned_script, flags=re.MULTILINE)
    cleaned_script = re.sub(r'(?i)^check.*\n?', '', cleaned_script, flags=re.MULTILINE)
    cleaned_script = re.sub(r'^while\s*\(.*?\)\s*\{.*?\}', '', cleaned_script, flags=re.MULTILINE | re.DOTALL)
    cleaned_script = re.sub(r'--.*?$', '', cleaned_script, flags=re.MULTILINE)
    cleaned_script = re.sub(r'(?m)^\s*(#).*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?m)(?i)^\s*connect\s.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?m)^connection\s.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?m)^disconnect\s.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?m)(?i)^show\s.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*DEBUG.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*RESET.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*reap.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*CALL.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*copy_file.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*INSTALL PLUGIN.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*query_log.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?i)(?m)^.*send.*\n?', '', cleaned_script)
    cleaned_script = re.sub(r'(?m)^\s*\n', '', cleaned_script)
    return cleaned_script


def ensure_table_recreation(sql_script, table_name):
    table_name_escaped = re.escape(table_name)
    pattern = re.compile(f'CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+\\b{table_name_escaped}\\b', re.IGNORECASE)
    def replace_func(match):
        return f"DROP TABLE IF EXISTS {table_name};\n{match.group()}"

    # Apply the replacement
    updated_script = pattern.sub(replace_func, sql_script)
    return updated_script


def ensure_view_recreation(sql_script, view_name):
    view_name_escaped = re.escape(view_name)
    pattern = re.compile(f'CREATE VIEW {view_name_escaped}', re.IGNORECASE)
    def replace_func(match):
        return f"DROP VIEW IF EXISTS {view_name};\n{match.group()}"

    # Apply the replacement
    updated_script = pattern.sub(replace_func, sql_script)

    pattern = re.compile(f'CREATE  VIEW {view_name_escaped}', re.IGNORECASE)
    updated_script = pattern.sub(replace_func, updated_script)
    return updated_script


def ensure_event_recreation(sql_script, event_name):
    event_name_escaped = re.escape(event_name)
    pattern = re.compile(f'CREATE[\\s]+EVENT[\\s]+{event_name_escaped}', re.IGNORECASE)
    def replace_func(match):
        return f"DROP EVENT IF EXISTS {event_name};\n{match.group()}"

    # Apply the replacement
    updated_script = pattern.sub(replace_func, sql_script)
    return updated_script


def ensure_database_recreation(sql_script, database_name):
    view_name_escaped = re.escape(database_name)
    pattern = re.compile(f'CREATE DATABASE {view_name_escaped}', re.IGNORECASE)
    def replace_func(match):
        return f"DROP DATABASE IF EXISTS {database_name};\n{match.group()}"

    # Apply the replacement
    updated_script = pattern.sub(replace_func, sql_script)
    return updated_script


def ensure_schema_recreation(sql_script, database_name):
    view_name_escaped = re.escape(database_name)
    pattern = re.compile(f'CREATE SCHEMA {view_name_escaped}', re.IGNORECASE)
    def replace_func(match):
        return f"DROP SCHEMA IF EXISTS {database_name};\n{match.group()}"

    # Apply the replacement
    updated_script = pattern.sub(replace_func, sql_script)
    return updated_script


def ensure_function_recreation(sql_script, function_name):
    function_name_escaped = re.escape(function_name)
    pattern = re.compile(f'CREATE[\\s]+FUNCTION[\\s]+{function_name_escaped}', re.IGNORECASE)
    def replace_func(match):
        return f"DROP FUNCTION IF EXISTS {function_name};\n{match.group()}"

    # Apply the replacement
    updated_script = pattern.sub(replace_func, sql_script)
    return updated_script


def remove_eval_multiline(sql_script):
    pattern = re.compile(r'\beval (?=\S)', re.IGNORECASE | re.MULTILINE)
    updated_script = pattern.sub('', sql_script)
    return updated_script


def cut_off_after_use_test(sql_script):
    # 查找 "USE test" 并从该位置切断字符串
    match = re.search(r'\bUSE test\b', sql_script, re.IGNORECASE)
    if match:
        return sql_script[:match.start()]
    return sql_script


def convert_let_syntax(sql_script):
    pattern = re.compile(r'let\s+\$(\w+)\s*=\s*(select[^;]+);', re.IGNORECASE)

    # 替换为 "SELECT @variable := (...);" 的形式
    def replacement(match):
        variable = match.group(1)
        query = match.group(2).strip()
        return f'SELECT @{variable} := ({query});'

    converted_script = pattern.sub(replacement, sql_script)
    return converted_script


def insert_before_select(sql_script):
    target_pattern = re.compile(r'select -\(-9223372036854775808\), -\(-\(-9223372036854775808\)\);', re.IGNORECASE)
    insert_line = " --9223372036854775808, ---9223372036854775808, ----9223372036854775808;\n"
    def replace_func(match):
        return insert_line + match.group()

    # Apply the replacement
    updated_script = target_pattern.sub(replace_func, sql_script)
    return updated_script


def convert_all_quotes(sql_script):
    pattern = re.compile(r'"((?:[^"\\]|\\.)*)"')
    updated_script = pattern.sub(lambda m: f"'{m.group(1).replace('\'', '\\\'')}'", sql_script)
    pattern = re.compile(r'dec \$1;\s*}', re.IGNORECASE)

    # Replace the found pattern with just '}'
    updated_script = pattern.sub('', updated_script)
    return updated_script


def convert_quotes_in_braces(sql_script):
    pattern = re.compile(r'\{[^{}]*\}')

    def replace_quotes(match):
        return match.group().replace("'", '"')

    # Apply the transformation to each matched section
    updated_script = pattern.sub(replace_quotes, sql_script)
    return updated_script


def replace_double_single_quotes(sql_statement):
    updated_statement = re.sub(r"''(\d+)''", r"'\1'", sql_statement)
    return updated_statement


def split_sql_statements(sql_script):
    keywords = r"\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|SYSTEM)\b"
    pattern = re.compile(keywords, re.IGNORECASE)

    # 初始化变量
    statements = []
    current_statement = []
    last_pos = 0
    open_brackets = 0

    # 遍历SQL脚本逐字符处理
    for i, char in enumerate(sql_script):
        if char == '(':
            open_brackets += 1
        elif char == ')':
            open_brackets -= 1
        elif char == ';' and open_brackets == 0:
            current_statement.append(sql_script[last_pos:i + 1].strip())
            last_pos = i + 1
            statements.append("".join(current_statement).strip())
            current_statement = []
        elif pattern.match(sql_script[i:]):
            if open_brackets == 0:
                if last_pos < i:
                    current_statement.append(sql_script[last_pos:i].strip())
                    last_pos = i

    # 添加最后一条语句
    if last_pos < len(sql_script):
        current_statement.append(sql_script[last_pos:].strip())
        statements.append("".join(current_statement).strip())

    # 检查并添加分号
    for i, statement in enumerate(statements):
        if not statement.endswith(';'):
            statements[i] = statement + ';'

    return statements


# 定义要跳过的条件列表
skip_conditions = [
    "serverError",
    "allow_deprecated_syntax_for_merge_tree",
    "cast_ipv4_ipv6_default_on_conversion_error",
    "$global_tmp_storage_engine",
    "INSTALL COMPONENT",
    "CREATE USER",
    "LOAD DATA",
    "$query",
    "info =",
    "delimiter",
    "DELIMITER",
    "LIKE_RANGE_MIN",
    "mysql.",
    "JSON DEFAULT",
    "QN.a",
    "$statement_digest_a",
    "$nines",
    "load data",
    "infile",
    "con1_id",
    "load_file",
    "max_allowed_packet"
]


def process_sql_files(directory):
    sql_files = sorted([f for f in os.listdir(directory) if f.endswith('.test') and 'debug' not in f.lower() and 'error' not in f.lower() and 'bug' not in f.lower() and 'unix' not in f.lower()])
    print(sql_files)
    index = 0

    # Regex to find SQL statements assuming they start with keywords
    # sql_statement_regex = re.compile(r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|SET|SYSTEM|ENGINE)\b', re.IGNORECASE)

    for sql_file in sql_files:
        with open(os.path.join(directory, sql_file), 'r') as file:
            statements = file.read()

        if any(condition in statements for condition in skip_conditions):
            continue 

        print(sql_file)

        if 'let' in statements:
            statements = convert_let_syntax(statements)

        # Find all statements based on the regex
        statements = remove_comments(statements)
        # 检查并切断 "USE test" 之后的内容
        statements = cut_off_after_use_test(statements)
        # Insert specific SELECT statement modifications
        statements = insert_before_select(statements)
        # Ensure each "CREATE TABLE t1" is preceded by a "DROP TABLE IF EXISTS t1;"
        statements = ensure_table_recreation(statements, "t1")
        statements = ensure_table_recreation(statements, "t")
        statements = ensure_table_recreation(statements, "t2")
        statements = ensure_table_recreation(statements, "tb")
        statements = ensure_table_recreation(statements, "tmp_t1")
        statements = ensure_table_recreation(statements, "bug20691")
        statements = ensure_table_recreation(statements, "child")
        statements = ensure_table_recreation(statements, "ti")
        statements = ensure_table_recreation(statements, "v1")
        statements = ensure_table_recreation(statements, "v2")
        statements = ensure_view_recreation(statements, "v")
        statements = ensure_view_recreation(statements, "v1")
        statements = ensure_view_recreation(statements, "v2")
        statements = ensure_view_recreation(statements, "t")
        statements = ensure_database_recreation(statements, "db1")
        statements = ensure_database_recreation(statements, "test1")
        statements = ensure_database_recreation(statements, "MYDB")
        statements = ensure_schema_recreation(statements, "s")
        statements = ensure_event_recreation(statements, "event1")
        statements = ensure_event_recreation(statements, "event2")
        statements = ensure_function_recreation(statements, "hello")
        statements = ensure_function_recreation(statements, "r_instr")
        statements = ensure_function_recreation(statements, "r_like")
        statements = ensure_function_recreation(statements, "r_replace")
        statements = ensure_function_recreation(statements, "f")
        statements = ensure_function_recreation(statements, "r_substr")
        statements = ensure_table_recreation(statements, "v5")
        statements = ensure_view_recreation(statements, "v5")
        statements = ensure_view_recreation(statements, "v6")
        statements = ensure_view_recreation(statements, "v2")
        statements = ensure_view_recreation(statements, "v3")
        statements = ensure_view_recreation(statements, "v4")
        statements = remove_eval_multiline(statements)
        # Convert the quotes
        statements = convert_all_quotes(statements)
        # Convert single quotes to double quotes within curly braces
        statements = convert_quotes_in_braces(statements)
        # print(statements)
        statements = replace_double_single_quotes(statements)
        statements = split_sql_statements(statements)
        # print(statements)
        in_select_block = False
        select_buffer = []
        non_select_buffer = []

        for statement in statements:
            if statement.strip().lower().startswith('select'):
                # if not in_select_block and non_select_buffer:
                #     write_to_files(non_select_buffer, select_buffer, index)
                #     non_select_buffer = []
                #     # index += 1

                select_buffer.append(statement)
                write_to_files(non_select_buffer, select_buffer, index)
                if non_select_buffer and (non_select_buffer[0].startswith('drop table if exists t1, t2, t3;') or non_select_buffer[0].startswith('CREATE USER wl12475@localhost;')):
                    non_select_buffer = []
                if non_select_buffer and (non_select_buffer[-1] == "insert into mysqltest.t1 (name) values (\"mysqltest\");"):
                    non_select_buffer.append("DROP TABLE IF EXISTS t1;")
                select_buffer = []
                index += 1
                
            else:
                # if in_select_block and select_buffer:
                #     write_to_files(non_select_buffer, select_buffer, index)
                #     select_buffer = []
                #     index += 1
                # if in_select_block:
                    # write_to_files(non_select_buffer, select_buffer, index)
                    # index += 1
                    # select_buffer = []
                    # if non_select_buffer and (non_select_buffer[0].startswith('drop table if exists t1, t2, t3;') or non_select_buffer[0].startswith('CREATE USER wl12475@localhost;')):
                    #     non_select_buffer = []
                    # if non_select_buffer and (non_select_buffer[-1] == "insert into mysqltest.t1 (name) values (\"mysqltest\");"):
                    #     non_select_buffer.append("DROP TABLE IF EXISTS t1;")
                    # in_select_block = False
                non_select_buffer.append(statement)
                

        # Flush remaining buffers
        if select_buffer:
            write_to_files(non_select_buffer, select_buffer, index)
            index += 1

        # input()

def adjust_select_from_spaces(sql_statements):
    adjusted_statements = []
    for statement in sql_statements:
        statement = re.sub(r'(?i)([^ ])(SELECT)', r'\1 \2', statement)
        statement = re.sub(r'(?i)([^ ])(CREATE)', r'\1 \2', statement)
        statement = re.sub(r'(?i)([^ ])(UPDATE)', r'\1 \2', statement)
        statement = re.sub(r'(?i)([^ ])(DROP)', r'\1 \2', statement)
        statement = re.sub(r'(?i)([^ ])(ALTER)', r'\1 \2', statement)
        statement = re.sub(r'do(?=insert)', 'do ', statement, flags=re.IGNORECASE)
        statement = re.sub(r'tree(?=insert)', 'tree; ', statement, flags=re.IGNORECASE)
        statement = re.sub(r'tree(?=delete)', 'tree; ', statement, flags=re.IGNORECASE)
        statement = re.sub(r'explain(?=insert)', 'explain ', statement, flags=re.IGNORECASE)
        re.sub(r'(?i)(FROM)(?! Text)(?=\S)', r'\1 ', statement)
        
        adjusted_statements.append(statement)
    return adjusted_statements


def write_to_files(non_select_buffer, select_buffer, index):
    non_select_buffer = adjust_select_from_spaces(non_select_buffer)
    select_buffer = adjust_select_from_spaces(select_buffer)
    os.makedirs(f'testcase{index}', exist_ok=True)
    with open(f'testcase{index}/db.sql', 'w') as db_file:
        db_file.write('\n'.join(non_select_buffer)) 
    with open(f'testcase{index}/test.sql', 'w') as test_file:
        test_file.write('\n'.join(select_buffer))  

if __name__ == "__main__":
    process_sql_files('t')
