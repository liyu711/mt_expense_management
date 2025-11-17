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
    # Cast & normalize raw upload columns
    df_upload['PO'] = df_upload['PO'].astype(str)
    df_upload['Project Category'] = df_upload['Project Category'].astype(str)
    df_upload['Project Name'] = df_upload['Project Name'].astype(str)
    df_upload['Department'] = df_upload['Department'].astype(str)
    df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)
    try:
        dedupe_subset = [c for c in ['PO', 'Project Name', 'Department', 'fiscal_year', 'Human resource category'] if c in df_upload.columns]
        if dedupe_subset:
            df_upload = df_upload.drop_duplicates(subset=dedupe_subset, keep='first')
    except Exception:
        pass

    # Reference tables
    hr_categories_merged = select_all_from_table(cursor, cnxn, 'human_resource_categories')
    merged_departments = select_all_from_table(cursor, cnxn, 'departments')
    projects_merged = select_all_from_table(cursor, cnxn, 'projects')
    pos_merged = select_all_from_table(cursor, cnxn, 'pos')

    # Ensure uniqueness on key columns to avoid multiplicative joins
    for df_ref, col in [(hr_categories_merged, 'name'), (merged_departments, 'name'), (projects_merged, 'name'), (pos_merged, 'name')]:
        try:
            if df_ref is not None and not df_ref.empty and col in df_ref.columns:
                df_ref.drop_duplicates(subset=[col], inplace=True)
        except Exception:
            pass

    # Merge HR category
    df_upload_fin = pd.merge(df_upload, hr_categories_merged, left_on='Human resource category', right_on='name', how='left')
    df_upload_fin.rename(columns={'id': 'human_resource_category_id'}, inplace=True)
    df_upload_fin.drop(['Human resource category', 'name'], axis=1, inplace=True, errors='ignore')

    # Merge Department to get department_id
    df_upload_fin = pd.merge(df_upload_fin, merged_departments, left_on='Department', right_on='name', how='left')
    df_upload_fin.rename(columns={'id': 'department_id'}, inplace=True)
    df_upload_fin.drop(['Department', 'name', 'po_id'], axis=1, inplace=True, errors='ignore')

    # Merge Project & Category
    df_upload_fin = pd.merge(df_upload_fin, projects_merged, left_on='Project Name', right_on='name', how='left')
    df_upload_fin.rename(columns={'id': 'project_id', 'category_id': 'project_category_id'}, inplace=True)
    df_upload_fin.drop(['Project Category', 'Project Name', 'name'], axis=1, inplace=True, errors='ignore')

    # Merge PO
    df_upload_fin = pd.merge(df_upload_fin, pos_merged, left_on='PO', right_on='name', how='left')
    df_upload_fin.rename(columns={'id': 'PO_id', 'Fiscal Year': 'fiscal_year'}, inplace=True)
    df_upload_fin.drop(['PO', 'name', 'department_id_y'], axis=1, inplace=True, errors='ignore')
    df_upload_fin.rename(columns={'department_id_x': 'department_id'}, inplace=True)

    # Final column naming adjustments
    df_upload_fin.rename(columns={'Human resource FTE': 'human_resource_fte', 'Personnel cost': 'personnel_expense'}, inplace=True)

    # Deduplicate final rows on stable id columns
    
    # Clean any duplicate fiscal_year columns if produced by merges
    for col in ['fiscal_year_x', 'fiscal_year_y']:
        if col in df_upload_fin.columns:
            try:
                if col == 'fiscal_year_x':
                    df_upload_fin.drop(['fiscal_year_x'], axis=1, inplace=True)
                elif col == 'fiscal_year_y' and 'fiscal_year' not in df_upload_fin.columns:
                    df_upload_fin.rename(columns={'fiscal_year_y': 'fiscal_year'}, inplace=True)
                elif col == 'fiscal_year_y':
                    df_upload_fin.drop(['fiscal_year_y'], axis=1, inplace=True)
            except Exception:
                pass

    return df_upload_fin


    

if __name__ == "__main__":
    filepath_pc = 'processed_data\\forecasts\\forecasts_pc_.csv'
    upload_pc_forecasts(filepath_pc)