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
    # engine, cursor, cnxn = connect_to_sql(engine=True)
    if clear:
        if type == 'server':
            clear_table(cursor, cnxn, 'capex_expenses')
        else:
            cl.clear_table(cursor, cnxn, 'capex_expenses')
    
    df_upload['PO'] = df_upload['PO'].astype(str)
    # df_upload['fiscal_year'] = pd.to_datetime(df_upload['fiscal_year'], format='%Y')
    df_upload['expense_date'] = df_upload['fiscal_year']

    departments = select_all_from_table(cursor, cnxn, "departments")
    po = select_all_from_table(cursor, cnxn, "POs")
    projects = select_all_from_table(cursor, cnxn, "projects")


    # df_upload = join_tables(
    #     df_upload,
    #     departments,
    #     'Department',
    #     'name',
    #     ['Department', 'name', 'po_id'],
    #     {'id': 'department_id'}
    # )

    df_upload = join_tables(
        df_upload,
        po,
        'PO',
        'name',
        ['PO', 'name'],
        {'id': 'po_id'}
    )

    df_upload = join_tables(
        df_upload,
        projects,
        'Project Name',
        'name',
        ['Project Name', 'name', 'category_id', 'Department','fiscal_year_y'],
        {'id': 'project_id', 'fiscal_year_x': 'cap_year', 'Expense': 'expense', 'Project number': 'project_number'}
    )

    df_upload.to_sql('capex_expenses', con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    capex_expense_path = 'processed_data\\expenses\\capex_expenses_.csv'
    upload_capex_expense(capex_expense_path)