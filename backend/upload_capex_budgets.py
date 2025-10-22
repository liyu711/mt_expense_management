import os
import pandas as pd
from backend.connect_pyodbc import connect_to_sql, close_connection, select_all_from_table, clear_table
from backend.merge_insert import *
import backend.connect_local as cl
from backend.connect_local import select_columns_from_table
from backend.table_values import table_column_dict

def upload_capex_budget(path, clear=True):
    df_upload = pd.read_csv(path)
    engine, cursor, cnxn = connect_to_sql(engine=True)
    upload_capex_budget_df(df_upload, engine, cursor, cnxn, 'server', clear)

def upload_capex_budget_df(df_upload, engine, cursor, cnxn, type, clear=True):
    # if clear:
    #     if type == 'server':
    #         clear_table(cursor, cnxn, 'capex_budgets')
    #     else:
    #         cl.clear_table(cursor, cnxn, 'capex_budgets')
    df_upload['PO'] = df_upload['PO'].astype(str)
    print(df_upload.columns)
    df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)

    # remove exact duplicate upload rows early to avoid duplicates
    try:
        dedupe_cols = [c for c in ['PO', 'for_project', 'Department', 'fiscal_year'] if c in df_upload.columns]
        if dedupe_cols:
            df_upload = df_upload.drop_duplicates(subset=dedupe_cols, keep='first')
    except Exception:
        pass

    departments = select_all_from_table(cursor, cnxn, "departments")
    po = select_all_from_table(cursor, cnxn, "POs")
    projects = select_all_from_table(cursor, cnxn, "projects")

    # ensure reference tables have unique join keys to avoid multiplicative merges
    try:
        if departments is not None and not departments.empty and 'name' in departments.columns:
            departments = departments.drop_duplicates(subset=['name'])
    except Exception:
        pass
    try:
        if po is not None and not po.empty and 'name' in po.columns:
            po = po.drop_duplicates(subset=['name'])
    except Exception:
        pass
    try:
        if projects is not None and not projects.empty and 'name' in projects.columns:
            projects = projects.drop_duplicates(subset=['name'])
    except Exception:
        pass

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
        'for_project',
        'name',
        ['for_project', 'name', 'category_id', 'Department'],
        {'id': 'project_id', 'fiscal_year': 'cap_year'}
    )
    # deduplicate final merged DataFrame on id keys when present
    try:
        dedupe_ids = [c for c in ['po_id', 'project_id', 'department_id', 'cap_year'] if c in df_upload.columns]
        if dedupe_ids:
            df_upload = df_upload.drop_duplicates(subset=dedupe_ids, keep='first')
    except Exception:
        pass
    # df_upload['department_id'] = df_upload['department_id'].astype(int)
    # df_upload.to_sql('capex_budgets', con=engine, if_exists='append', index=False)

    # close_connection(cursor, cnxn)
    return df_upload

def upload_capex_budget_local(df_upload, clear=True):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    years = df_upload['fiscal_year'].unique().tolist()
    for year in years:
        cl.clear_table_by_year(cursor, cnxn, "capex_budgets", year)
    df_upload = upload_capex_budget_df(df_upload, engine, cursor, cnxn, 'local')
    df_upload.to_sql('capex_budgets', con=engine, if_exists='append', index=False)

def upload_capex_budgets_local_m(df_upload):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    existing_values = select_columns_from_table(cursor, "capex_budgets", table_column_dict["capex_budgets"])
    df_fin = upload_capex_budget_df(df_upload, engine, cursor, cnxn, 'local')
    
    shared_columns = [col for col in df_fin.columns if col in existing_values.columns and col != 'budget']
    merged = df_fin.merge(existing_values[shared_columns], on=shared_columns, how='left', indicator=True)
    df_filtered = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    # print(df_filtered)
    df_filtered.to_sql("capex_budgets", con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    capex_budget_path = 'processed_data\\budgets\\capex_budgets_.csv'
    upload_capex_budget(capex_budget_path)