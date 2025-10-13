import pandas as pd
import sqlite3
from backend import close_connection, connect_to_sql, select_all_from_table
import backend.connect_local as cl

tables_allow_empty = ['project_forecasts_nonpc', 'capex_forecasts']
columns_to_check = ['PO', 'Department', 'Project Name']
column_name_to_table = {
    'PO': 'POs',
    'Department': 'departments',
    'Project Name': 'projects'
}

def check_input_integrity(df_upload, table_name):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    colums = df_upload.columns

    db_columns = []
    cursor.execute(f"PRAGMA table_info({table_name});")
    rows = cursor.fetchall()
    for row in rows:
            db_columns.append(row[1])
    col_count = len(colums)
    if len(df_upload.columns != col_count):
        return False
    else:
        return True



def check_if_all_tables_empty(type):
    if type == 'local':
        conn = cl.connect_local()
        cursor, cnxn = conn.connect_to_db()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        table_names = [row[0] for row in cursor.fetchall()]

        for table_name in table_names:
            cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1;")
            if cursor.fetchone():
                close_connection(cursor, cnxn)
                return False  # Found a non-empty table

        close_connection(cursor, cnxn)
        return True  # All tables are empty

    elif type == 'server':
        cursor, cnxn = connect_to_sql()
        return True
    else:
        return "incorrect connection type"

def get_columns_types(type, table_name):
    if type == 'local':
        conn = cl.connect_local()
        cursor, cnxn = conn.connect_to_db()
        res = {}
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        column_types = {}
        for col in columns_info:
            column_name = col[1]  # Column name is at index 1
            data_type = col[2]   # Data type is at index 2
            column_types[column_name] = data_type
        return column_types

    elif type == 'server':
        cursor, cnxn = connect_to_sql()
        return True
    else:
        return "incorrect connection type"
    
def clear_table(cursor, cnxn, table_name, conn_type):
    if conn_type == 'server':
        drop_query = f"TRUNCATE TABLE {table_name};"
        cursor.execute(drop_query)
        cnxn.commit()
    else:
        drop_query = f"DELETE FROM {table_name};"
        cursor.executescript(drop_query)
        cnxn.commit()

def check_missing_attribute(df_upload, table_name, type):
    if type == 'server':
        engine, cursor, cnxn = connect_to_sql(engine=True)
    else:
        conn = cl.connect_local()
        engine, cursor, cnxn = conn.connect_to_db(engine=True)
    if table_name in tables_allow_empty:
        return False, None
    columns_check = []
    for column in df_upload.columns:
        if column in columns_to_check:
            columns_check.append(column)
    for column in columns_check:
        table = column_name_to_table[column]
        df = select_all_from_table(cursor, cnxn, table)
        db_values = df['name'].values
        df_values = df_upload[column].values
        for value in df_values:
            value = str(value)
            if value not in db_values:
                print('test2')
                return True, column
        print('test3')
    return False, None

