import pandas as pd
import sqlite3
from backend import close_connection, connect_to_sql, select_all_from_table
import backend.connect_local as cl

tables_allow_empty = ['project_forecasts_nonpc', 'capex_forecasts']
# Columns in incoming CSVs we validate against reference tables
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
    """Validate that key text columns in the upload exist in reference tables.

    We check presence for columns that actually exist in the incoming DataFrame among
    ['PO', 'Department', 'Project Name'] and compare against the corresponding
    reference tables' name columns. Comparison is case-insensitive and ignores
    leading/trailing whitespace to be robust to minor formatting differences.
    Returns (True, column) on first missing value, else (False, None).
    """
    # Pick connection
    if type == 'server':
        engine, cursor, cnxn = connect_to_sql(engine=True)
    else:
        conn = cl.connect_local()
        engine, cursor, cnxn = conn.connect_to_db(engine=True)

    # Tables that may be uploaded before reference data
    if table_name in tables_allow_empty:
        return False, None

    # Determine which columns to validate (only those present)
    columns_check = [c for c in columns_to_check if c in list(df_upload.columns)]
    if not columns_check:
        return False, None

    # Normalizer: string, strip, lower
    def norm(x):
        try:
            s = str(x)
        except Exception:
            s = ''
        return s.strip().lower()

    for column in columns_check:
        table = column_name_to_table.get(column)
        if not table:
            continue
        try:
            ref_df = select_all_from_table(cursor, cnxn, table)
        except Exception:
            ref_df = None
        if ref_df is None or ref_df.empty:
            # If reference table is empty, treat as missing
            return True, column

        # Determine the reference name column (prefer 'name')
        ref_name_col = 'name' if 'name' in ref_df.columns else (ref_df.columns[0] if len(ref_df.columns) > 0 else None)
        if ref_name_col is None:
            return True, column

        # Build normalized set of known names
        try:
            ref_values = set(ref_df[ref_name_col].dropna().map(norm).tolist())
        except Exception:
            ref_values = set(norm(v) for v in ref_df[ref_name_col].tolist())

        # Validate every non-null value in the upload column
        try:
            series = df_upload[column]
        except Exception:
            series = []
        for raw in series:
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                # Skip nulls; they will be handled by downstream uploaders
                continue
            if norm(raw) not in ref_values:
                return True, column

    return False, None

