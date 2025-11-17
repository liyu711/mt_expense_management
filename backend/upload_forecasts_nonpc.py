import os
import datetime
import pandas as pd
from backend.connect_pyodbc import connect_to_sql, close_connection, select_all_from_table, clear_table
from backend.merge_insert import *
import backend.connect_local as cl
from backend.table_values import table_column_dict, tables_to_consider
from backend.connect_local import select_columns_from_table

def upload_nonpc_forecasts(file_path: str):
    df_upload = pd.read_csv(file_path)
    engine, cursor, cnxn = connect_to_sql(True)
    clear_table(cursor, cnxn, "project_forecasts_nonpc")
    upload_nonpc_forecasts_df(df_upload, engine, cursor, cnxn, 'server')

def upload_nonpc_forecasts(file_path: pd.DataFrame):
    engine, cursor, cnxn = connect_to_sql(True)
    upload_nonpc_forecasts_df(file_path, engine, cursor, cnxn, 'server')

def upload_nonpc_forecasts_local(df_upload):
    """SQLite version clearing per year then uploading (IO removed)."""
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    years = df_upload['fiscal_year'].unique().tolist()
    for year in years:
        cl.clear_table_by_year(cursor, cnxn, "project_forecasts_nonpc", year)
    df_fin = upload_nonpc_forecasts_df2(df_upload, engine, cursor, cnxn, 'local')
    return df_fin.to_sql("project_forecasts_nonpc", con=engine, if_exists='append', index=False)
    
def upload_nonpc_forecasts_local_m(df_upload):
    """Merge-friendly upload without IO key; de-duplicate against existing values excluding non_personnel_expense."""
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    df_fin = upload_nonpc_forecasts_df2(df_upload, engine, cursor, cnxn, 'local')
    existing_values = select_columns_from_table(cursor, "project_forecasts_nonpc", table_column_dict["project_forecasts_nonpc"])
    shared_columns = [col for col in df_fin.columns if col in existing_values.columns and col != 'non_personnel_expense']
    merged = df_fin.merge(existing_values[shared_columns], on=shared_columns, how='left', indicator=True)
    df_filtered = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    return df_filtered.to_sql("project_forecasts_nonpc", con=engine, if_exists='append', index=False)
    
def upload_nonpc_forecasts_df(df_upload, engine, cursor, cnxn, type):
    """Prepare non-personnel forecast rows (IO removed)."""
    df_upload['PO'] = df_upload['PO'].astype(str)
    df_upload['Project Category'] = df_upload['Project Category'].astype(str)
    df_upload['Project Name'] = df_upload['Project Name'].astype(str)
    df_upload['Department'] = df_upload['Department'].astype(str)
    df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)
    try:
        dedupe_subset = [c for c in ['PO', 'Project Name', 'Department', 'fiscal_year'] if c in df_upload.columns]
        if dedupe_subset:
            df_upload = df_upload.drop_duplicates(subset=dedupe_subset, keep='first')
    except Exception:
        pass
    # departments
    cur_departments = select_all_from_table(cursor, cnxn, "departments")
    department_local = df_upload[['Department']]
    department_upload = merge_departments(cur_departments, department_local)
    department_upload.to_sql("departments", con=engine, if_exists='append', index=False)
    merged_departments = select_all_from_table(cursor, cnxn, "departments")
    # project categories
    cur_project_categories = select_all_from_table(cursor, cnxn, "project_categories")
    project_categories_local = df_upload[['Project Category']]
    project_categories_upload = merge_dataframes(cur_project_categories, project_categories_local, ['category'], 'category')
    project_categories_upload.to_sql("project_categories", con=engine, if_exists='append', index=False)
    merged_project_category = select_all_from_table(cursor, cnxn, "project_categories")
    # projects
    cur_projects = select_all_from_table(cursor, cnxn, "projects")
    projects_local = df_upload[['Project Category', 'Project Name']]
    projects_local = pd.merge(projects_local, merged_project_category, left_on='Project Category', right_on='category', how='left')
    projects_local = projects_local[['Project Name', 'id']]
    projects_upload = merge_dataframes(cur_projects, projects_local, ['name', 'category_id'], 'name')
    projects_upload.to_sql("projects", con=engine, if_exists='append', index=False)
    projects_merged = select_all_from_table(cursor, cnxn, "projects")
    # PO
    cur_pos = select_all_from_table(cursor, cnxn, "pos")
    pos_local = df_upload[['PO']]
    pos_upload = merge_dataframes(cur_pos, pos_local, ['name'], 'name')
    pos_upload.to_sql("pos", con=engine, if_exists='append', index=False)
    pos_merged = select_all_from_table(cursor, cnxn, "pos")
    # uniqueness on keys
    for df_ref, col in [(merged_project_category, 'category'), (projects_merged, 'name'), (pos_merged, 'name')]:
        try:
            if df_ref is not None and not df_ref.empty and col in df_ref.columns:
                df_ref.drop_duplicates(subset=[col], inplace=True)
        except Exception:
            pass
    # final merge (no IO)
    df_upload_fin = pd.merge(df_upload, merged_departments, left_on='Department', right_on='name', how='left')
    df_upload_fin.drop(['name', 'Department'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'department_id'}, inplace=True)
    df_upload_fin = pd.merge(df_upload_fin, projects_merged, left_on='Project Name', right_on='name', how='left')
    df_upload_fin.drop(['Project Category', 'Project Name', 'name'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'project_id', 'category_id': 'project_category_id'}, inplace=True)
    df_upload_fin = pd.merge(df_upload_fin, pos_merged, left_on='PO', right_on='name', how='left')
    df_upload_fin.drop(['PO', 'name'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'PO_id', 'Fiscal Year': 'fiscal_year', 'Non-personnel cost': 'non_personnel_expense'}, inplace=True)
    # reconcile possible duplicate fiscal_year columns
    try:
        df_upload_fin.drop(['fiscal_year_x'], axis=1, inplace=True)
    except Exception:
        pass
    try:
        df_upload_fin.rename(columns={'fiscal_year_y': 'fiscal_year'}, inplace=True)
    except Exception:
        pass
    return df_upload_fin


def upload_nonpc_forecasts_df2(df_upload, engine, cursor, cnxn, type):
    """Lightweight upload path (no IO)."""
    df_upload['PO'] = df_upload['PO'].astype(str)
    df_upload['Project Category'] = df_upload['Project Category'].astype(str)
    df_upload['Project Name'] = df_upload['Project Name'].astype(str)
    df_upload['Department'] = df_upload['Department'].astype(str)
    df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)
    try:
        dedupe_subset = [c for c in ['PO', 'Project Name', 'Department', 'fiscal_year'] if c in df_upload.columns]
        if dedupe_subset:
            df_upload = df_upload.drop_duplicates(subset=dedupe_subset, keep='first')
    except Exception:
        pass
    merged_departments = select_all_from_table(cursor, cnxn, "departments")
    projects_merged = select_all_from_table(cursor, cnxn, "projects")
    pos_merged = select_all_from_table(cursor, cnxn, "pos")
    for df_ref, col in [(merged_departments, 'name'), (projects_merged, 'name'), (pos_merged, 'name')]:
        try:
            if df_ref is not None and not df_ref.empty and col in df_ref.columns:
                df_ref.drop_duplicates(subset=[col], inplace=True)
        except Exception:
            pass
    df_upload_fin = pd.merge(df_upload, merged_departments, left_on='Department', right_on='name', how='left')
    df_upload_fin.drop(['name', 'Department', 'po_id'], axis=1, inplace=True, errors='ignore')
    df_upload_fin.rename(columns={'id': 'department_id'}, inplace=True)
    df_upload_fin = pd.merge(df_upload_fin, projects_merged, left_on='Project Name', right_on='name', how='left')
    df_upload_fin.drop(['Project Category', 'Project Name', 'name'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'project_id', 'category_id': 'project_category_id'}, inplace=True)
    df_upload_fin = pd.merge(df_upload_fin, pos_merged, left_on='PO', right_on='name', how='left')
    df_upload_fin.drop(['PO', 'name'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'PO_id', 'Fiscal Year': 'fiscal_year', 'Non-personnel cost': 'non_personnel_expense'}, inplace=True)
    df_upload_fin.rename(columns={'department_id_x': 'department_id'}, inplace=True, errors='ignore')
    df_upload_fin.drop(['department_id_y'], axis=1, inplace=True)
    return df_upload_fin




if __name__ == "__main__":
    filepath_nonpc = 'processed_data\\forecasts\\forecasts_nonpc_.csv'
    upload_nonpc_forecasts(filepath_nonpc)