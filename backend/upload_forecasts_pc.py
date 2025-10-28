import os
import pandas as pd
from backend.connect_pyodbc import connect_to_sql, close_connection, select_all_from_table, clear_table
from backend.merge_insert import *
import backend.connect_local as cl
from backend.connect_local import select_columns_from_table
from backend.table_values import table_column_dict

# not done
def upload_pc_forecasts(file_path: str):
    df_upload = pd.read_csv(file_path)
    clear_table(cursor, cnxn, "project_forecasts_pc")

    engine, cursor, cnxn = connect_to_sql(True)
    upload_pc_forecasts_df(df_upload, engine, cursor, cnxn, 'server')


def upload_pc_forecasts_local(df_upload):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    # cl.clear_table(cursor, cnxn, "project_forecasts_pc")
    years = df_upload['fiscal_year'].unique().tolist()
    for year in years:
        cl.clear_table_by_year(cursor, cnxn, "project_forecasts_pc", year)
    
    df_fin = upload_pc_forecasts_df(df_upload, engine, cursor, cnxn, 'local')
    return df_fin.to_sql("project_forecasts_pc", con=engine, if_exists='append', index=False)


def upload_pc_forecasts_local_m(df_upload):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    df_fin = upload_pc_forecasts_df(df_upload, engine, cursor, cnxn, 'local')
    existing_values = select_columns_from_table(cursor, "project_forecasts_pc", table_column_dict["project_forecasts_pc"])
    
    shared_columns = [col for col in df_fin.columns if col in existing_values.columns and col != 'personnel_expense']

    # Remove rows from df_fin that exist in existing_values based on shared columns
    merged = df_fin.merge(existing_values[shared_columns], on=shared_columns, how='left', indicator=True)
    df_filtered = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    # print(df_filtered)
    return df_filtered.to_sql("project_forecasts_pc", con=engine, if_exists='append', index=False)
    # print(existing_values)


def upload_pc_forecasts_df(df_upload, engine, cursor, cnxn, type):
    df_upload['PO'] = df_upload['PO'].astype(str)
    # df_upload['PO'] = df_upload['PO'].astype(str)
    df_upload['Project Category'] = df_upload['Project Category'].astype(str)
    df_upload['Project Name'] = df_upload['Project Name'].astype(str)
    df_upload['Department'] = df_upload['Department'].astype(str)
    df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)
    # remove exact duplicate upload rows to avoid inserting duplicates
    try:
        dedupe_subset = [c for c in ['PO', 'Project Name', 'Department', 'IO', 'fiscal_year', 'Human resource category'] if c in df_upload.columns]
        if dedupe_subset:
            df_upload = df_upload.drop_duplicates(subset=dedupe_subset, keep='first')
    except Exception:
        pass
    # engine, cursor, cnxn = connect_to_sql(True)

    # upload human resource categories
    # cur_hr_categories = select_all_from_table(cursor, cnxn, 'human_resource_categories')
    # hr_categories_local = df_upload[['Human resource category']]
    # hr_categories_columns = ['name']
    # hr_categories_uplod = merge_dataframes(cur_hr_categories, hr_categories_local, hr_categories_columns, 'name')

    # hr_categories_uplod.to_sql("human_resource_categories", con=engine, if_exists='append', index=False)
    hr_categories_merged = select_all_from_table(cursor, cnxn, 'human_resource_categories')

    merged_departments = select_all_from_table(cursor, cnxn, "departments")

    projects_merged = select_all_from_table(cursor, cnxn, "projects")

    io_merged = select_all_from_table(cursor, cnxn, "ios")

    pos_merged = select_all_from_table(cursor, cnxn, "pos")
    # ensure uniqueness on join keys in reference tables to avoid multiplicative joins
    try:
        if hr_categories_merged is not None and not hr_categories_merged.empty and 'name' in hr_categories_merged.columns:
            hr_categories_merged = hr_categories_merged.drop_duplicates(subset=['name'])
    except Exception:
        pass
    try:
        if merged_departments is not None and not merged_departments.empty and 'name' in merged_departments.columns:
            merged_departments = merged_departments.drop_duplicates(subset=['name'])
    except Exception:
        pass
    try:
        if projects_merged is not None and not projects_merged.empty and 'name' in projects_merged.columns:
            projects_merged = projects_merged.drop_duplicates(subset=['name'])
    except Exception:
        pass
    try:
        if io_merged is not None and not io_merged.empty and 'IO_num' in io_merged.columns:
            io_merged = io_merged.drop_duplicates(subset=['IO_num'])
    except Exception:
        pass
    try:
        if pos_merged is not None and not pos_merged.empty and 'name' in pos_merged.columns:
            pos_merged = pos_merged.drop_duplicates(subset=['name'])
    except Exception:
        pass
    # upload expenses
    # df_upload_fin = pd.merge(df_upload, merged_departments, left_on='Department', right_on='name', how='left')
    # df_upload_fin.drop(['name', 'Department', 'po_id'], axis=1, inplace=True)
    # df_upload_fin.rename(columns={'id': 'department_id'}, inplace=True)

    df_upload_fin = pd.merge(df_upload, hr_categories_merged, left_on = 'Human resource category', right_on='name', how='left')
    df_upload_fin.rename(columns={'id': 'human_resource_category_id'}, inplace=True)
    df_upload_fin.drop(['Human resource category', 'name', 'Department', 'po_id'], axis=1, inplace=True)
    df_upload_fin = pd.merge(df_upload_fin, projects_merged, left_on='Project Name', right_on='name', how='left')
    df_upload_fin.drop(['Project Category', 'Project Name', 'name'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'project_id', 'category_id': 'project_category_id'}, inplace=True)
    df_upload_fin = pd.merge(df_upload_fin, io_merged, left_on='IO', right_on='IO_num', how='left')
    df_upload_fin.drop(['IO', 'IO_num', 'project_id_y'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'io_id', 'project_id_x': 'project_id'}, inplace=True)
    df_upload_fin = pd.merge(df_upload_fin, pos_merged, left_on='PO', right_on='name', how='left')
    # df_upload_fin.drop(['PO', 'name', 'value'], axis=1, inplace=True)
    df_upload_fin.drop(['PO', 'name'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'id': 'po_id', 'Fiscal Year': 'fiscal_year'}, inplace=True)
    
    df_upload_fin.rename(columns={'Human resource FTE': 'human_resource_fte', 'Personnel cost': 'personnel_expense'}, inplace=True)
    # df_upload_fin['fiscal_year'] =pd.to_datetime(df_upload_fin['fiscal_year'], format='%Y')
    # Deduplicate final merged DataFrame on id columns where possible to avoid duplicate inserts
    try:
        dedupe_ids = [c for c in ['po_id', 'project_id', 'department_id', 'io_id', 'fiscal_year', 'human_resource_category_id'] if c in df_upload_fin.columns]
        if dedupe_ids:
            df_upload_fin = df_upload_fin.drop_duplicates(subset=dedupe_ids, keep='first')
        else:
            # fallback: dedupe on names/IO/fiscal_year
            fallback = [c for c in ['PO', 'Project Name', 'Department', 'IO', 'fiscal_year'] if c in df_upload_fin.columns]
            if fallback:
                df_upload_fin = df_upload_fin.drop_duplicates(subset=fallback, keep='first')
    except Exception:
        pass
    df_upload_fin.drop(['fiscal_year_x'], axis=1, inplace=True)
    df_upload_fin.rename(columns={'fiscal_year_y': 'fiscal_year'}, inplace=True)

    # close_connection(cursor, cnxn)
    return df_upload_fin


    

if __name__ == "__main__":
    filepath_pc = 'processed_data\\forecasts\\forecasts_pc_.csv'
    upload_pc_forecasts(filepath_pc)