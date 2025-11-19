import os
import pandas as pd
from backend.connect_pyodbc import connect_to_sql, clear_table, close_connection, select_all_from_table
from backend.merge_insert import join_tables
import backend.connect_local as cl

def upload_capex_expense(path: str, clear=True):
    df_upload = pd.read_csv(path)
    engine, cursor, cnxn = connect_to_sql(engine=True)
    upload_capex_expense_df(df_upload, engine, cursor, cnxn, 'server', clear)


def upload_capex_expense(path: pd.DataFrame, clear=True):
    engine, cursor, cnxn = connect_to_sql(engine=True)
    upload_capex_expense_df(path, engine, cursor, cnxn, 'server', clear)


def upload_capex_expense_local(df_upload, clear=True):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    upload_capex_expense_df(df_upload, engine, cursor, cnxn, 'local', clear)


def upload_capex_expense_df(df_upload, engine, cursor, cnxn, type, clear=True):
    # Optionally clear table
    if clear:
        if type == 'server':
            clear_table(cursor, cnxn, 'capex_expenses')
        else:
            cl.clear_table(cursor, cnxn, 'capex_expenses')

    # Normalize inputs
    try:
        df_upload['PO'] = df_upload['PO'].astype(str)
    except Exception:
        pass
    # Prefer provided expense_date; otherwise derive from fiscal_year
    try:
        if 'expense_date' not in df_upload.columns or df_upload['expense_date'].isna().all():
            df_upload['expense_date'] = df_upload.get('fiscal_year')
    except Exception:
        df_upload['expense_date'] = df_upload.get('fiscal_year')

    # Reference tables
    po = select_all_from_table(cursor, cnxn, "POs")
    projects = select_all_from_table(cursor, cnxn, "projects")
    departments = select_all_from_table(cursor, cnxn, "departments")

    # Map PO name -> po_id (first)
    df_upload = join_tables(
        df_upload,
        po,
        'PO',
        'name',
        ['PO', 'name'],
        {'id': 'po_id'}
    )

    # Resolve project_id by project name only; do not depend on project fiscal_year/PO
    df_upload = join_tables(
        df_upload,
        projects,
        'Project Name',
        'name',
        ['Project Name', 'name', 'category_id', 'department_id', 'department_id_x', 'department_id_y', 'category_id_x', 'category_id_y'],
        {'id': 'project_id', 'Expense': 'expense', 'Project number': 'project_number'}
    )

    # Map Department name -> department_id (after project join to avoid suffix duplication)
    df_upload = join_tables(
        df_upload,
        departments,
        'Department',
        'name',
        ['Department', 'name', 'po_id_y', 'department_id_x', 'department_id_y'],
        {'id': 'department_id'}
    )

    # Normalize potential suffixed columns from merges
    # Ensure we end with plain 'po_id' and 'department_id' columns
    try:
        # If merge created suffixed po_id, prefer left side (po_id_x)
        if 'po_id' not in df_upload.columns and 'po_id_x' in df_upload.columns:
            df_upload = df_upload.rename(columns={'po_id_x': 'po_id'})
        # Drop lingering right-side suffixed po_id
        if 'po_id_y' in df_upload.columns:
            df_upload.drop(columns=['po_id_y'], inplace=True)
    except Exception:
        pass
    try:
        if 'department_id' not in df_upload.columns and 'department_id_x' in df_upload.columns:
            df_upload = df_upload.rename(columns={'department_id_x': 'department_id'})
        for c in ['department_id_x','department_id_y']:
            if c in df_upload.columns:
                df_upload.drop(columns=[c], inplace=True)
    except Exception:
        pass

    # Set cap_year from fiscal_year then remove fiscal_year
    try:
        df_upload['cap_year'] = df_upload['fiscal_year']
    except Exception:
        pass
    try:
        if 'fiscal_year' in df_upload.columns:
            df_upload.drop(columns=['fiscal_year'], inplace=True)
    except Exception:
        pass

    # Insert
    df_upload.to_sql('capex_expenses', con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    capex_expense_path = 'processed_data\\expenses\\capex_expenses_.csv'
    upload_capex_expense(capex_expense_path)