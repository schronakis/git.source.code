import pandas as pd
import re
import sys
import hashlib


def csv_to_pd(path, filename, **kwargs):
    try:
        df = pd.read_csv(f'{path}{filename}', **kwargs)
        return df
    
    except Exception as e:
        print(f'An Error Loading the Source File: {e}')
        sys.exit(1)


def extract_columnnames(create_script):
    try:
        # Extract column definitions
        column_definitions = re.findall(r"\b(\w+)\s+\w+(?:\([^)]+\))?", create_script)

        column_names = [
            col for col in column_definitions 
            if col.lower() not in {"create", "table"}]

        _columns = f"({', '.join(column_names)})"
        _values = f"({', '.join(['?'] * len(column_names))})"
        return _columns, _values
    
    except Exception as e:
        print(f'An Error Extracting the Column Names: {e}')
        sys.exit(1)


def generate_hash_key(*args):
    try:
        formatted_hash = '0x' + (hashlib.sha256("".join(map(str, args)).encode('utf-8')).hexdigest().upper())
        return formatted_hash

    except Exception as e:
        print(f'An Error Generating the HashKey: {e}')
        sys.exit(1)
    


def dataframe_to_database(class_obj, conn, create_stmt, _schema, db_table, df):
    try:
        conn.fast_executemany = True

        generate_schema = class_obj.generate_db_schema(_schema)
        conn.execute(generate_schema)
        conn.commit()

        create_table = class_obj.generate_db_table(create_stmt, _schema, db_table)
        conn.execute(create_table)
        conn.commit()
        print(f'Table {_schema}.{db_table} was created')

        truncate_table = class_obj.truncate_db_table(f'{_schema}.{db_table}')
        conn.execute(truncate_table)
        conn.commit()
        print(f'Table {_schema}.{db_table} was truncated')

        columnnames, columnvalues = (extract_columnnames(create_stmt))

        _query, _batch = class_obj.insert_data(f'{_schema}.{db_table}', df, columnnames, columnvalues)
        conn.executemany(_query, _batch)
        conn.commit()
        print(f'{len(df)} rows were Inserted to {_schema}.{db_table}')

    except Exception as e:
        print(f'An Error Occurred while Loading in Database: {e}')
        sys.exit(1)