import os
import re


def remove_comments(sql_script):
    cleaned_script = re.sub(r'--\s*\{?\s*serverError\s*\d+\s*\}?\s*$', '', sql_script, flags=re.MULTILINE | re.IGNORECASE)
    cleaned_script = re.sub(r'(?m)^\s*--.*$', '', sql_script)
    cleaned_script = re.sub(r'--.*?$', '', cleaned_script, flags=re.MULTILINE)
    cleaned_script = re.sub(r'(?m)--[^\n]*', '', cleaned_script)
    cleaned_script = re.sub(r'(?m)^\s*\n', '', cleaned_script)
    return cleaned_script


def fix_unterminated_strings(sql_script):
    pattern = re.compile(r"(\s*(SELECT|DROP|INSERT|UPDATE|DELETE|CREATE|ALTER)\s+'[^';]*)(?<!')\s*;", flags=re.IGNORECASE)

    def correct_strings(match):
        string = match.group(0)
        quotes_count = string.count("'")
        if quotes_count % 2 != 0:
            return string[:-1] + "'" + string[-1]
        return string

    fixed_script = pattern.sub(correct_strings, sql_script)
    return fixed_script


def split_sql_statements(sql_script):
    keywords = r"\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|SYSTEM)\b"
    pattern = re.compile(keywords, re.IGNORECASE)

    statements = []
    current_statement = []
    last_pos = 0
    open_brackets = 0

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

    if last_pos < len(sql_script):
        current_statement.append(sql_script[last_pos:].strip())
        statements.append("".join(current_statement).strip())

    for i, statement in enumerate(statements):
        if not statement.endswith(';'):
            statements[i] = statement + ';'

    return statements


def process_sql_files(directory):
    sql_files = sorted([f for f in os.listdir(directory) if f.endswith('.sql')])
    index = 0

    # Regex to find SQL statements assuming they start with keywords
    # sql_statement_regex = re.compile(r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|SET|SYSTEM|ENGINE)\b', re.IGNORECASE)

    skip_conditions = [
        "serverError",
        "allow_deprecated_syntax_for_merge_tree",
        "cast_ipv4_ipv6_default_on_conversion_error",
        "CLICKHOUSE_DATABASE:Identifier",
        "allow_experimental_analyzer",
        "allow_deprecated_functions",
        "quantilesDD",
        "quantileExactWeighted"
    ]

    for sql_file in sql_files:
        with open(os.path.join(directory, sql_file), 'r') as file:
            statements = file.read()
        
        if any(condition in statements for condition in skip_conditions):
            continue 

        # Find all statements based on the regex
        statements = remove_comments(statements)
        statements = fix_unterminated_strings(statements)
        statements = split_sql_statements(statements)
        in_select_block = False
        select_buffer = []
        non_select_buffer = []

        for statement in statements:
            if statement.strip().lower().startswith('select'):
                # if not in_select_block and non_select_buffer:
                #     write_to_files(non_select_buffer, select_buffer, index)
                #     non_select_buffer = []
                #     # index += 1
                in_select_block = True
                select_buffer.append(statement)
                write_to_files(non_select_buffer, select_buffer, index)
                # if non_select_buffer and (non_select_buffer[0].startswith('RENAME')):
                #     non_select_buffer = []
                select_buffer = []
                index += 1
            else:
                # if in_select_block and select_buffer:
                #     write_to_files(non_select_buffer, select_buffer, index)
                #     select_buffer = []
                #     index += 1
                # if in_select_block:
                #     if non_select_buffer and (non_select_buffer[0].startswith('RENAME') or non_select_buffer[0].startswith('SET max_execution_speed = 4000000')):
                #         non_select_buffer = []
                #     in_select_block = False
                non_select_buffer.append(statement)
                

        # Flush remaining buffers
        if select_buffer:
            write_to_files(non_select_buffer, select_buffer, index)
            index += 1

        # input()


def adjust_select_from_spaces(sql_statements):
    adjusted_statements = []
    for statement in sql_statements:
        statement = re.sub(r'(?i)([^ _])(SELECT)', r'\1 \2', statement)
        statement = re.sub(r'(?i)(FROM)([^ ])', r'\1 \2', statement)
        statement = re.sub(r'(SYSTEM)([^ ])', r'\1 \2', statement)
        statement = re.sub(r'(AS)([^ ])', r'\1 \2', statement)
        statement = re.sub(r'(EXISTS)([^ ])', r'\1 \2', statement)
        statement = re.sub(r'(CAS)(\s+)(T)', r'\1\3', statement)
        statement = re.sub(r'(CAS)(\s+)(E)', r'\1\3', statement)
        statement = re.sub(r'(LAS)(\s+)(T)', r'\1\3', statement)
        statement = re.sub(r'(AS)(\s+)(C\b)', r'\1\3', statement)
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
    process_sql_files('0_stateless')
