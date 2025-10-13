
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from backend.connect_local import connect_local, close_connection
from backend.connect_pyodbc import select_all_from_table
from backend import merge_dataframes, merge_departments, join_tables


# Generalized function to add/merge entries into a table, based on the logic from upload_forecasts_nonpc.py
def add_entry(df_upload, table_name, merge_columns, merge_on):
    """
    Adds or merges entries into a table using the provided DataFrame and column mapping.
    Parameters:
        df_upload: DataFrame containing the new data to upload
        table_name: str, name of the table to update
        local_col_names: list of str, columns from df_upload to use
        merge_columns: list of str, columns for the upload table (target table columns)
        merge_on: str, column to merge on
    Returns:
        DataFrame of merged/uploaded entries
    """
    conn_obj = connect_local()
    engine, cursor, cnxn = conn_obj.connect_to_db(engine=True)

    # 1. Get current table contents
    cur_table = select_all_from_table(cursor, cnxn, table_name)
    print(table_name)
    # 2. Prepare local data for upload
    local_data = df_upload
    if table_name == 'projects':
        merged_project_category = select_all_from_table(cursor, cnxn, "project_categories")
        try:
            local_data = pd.merge(local_data, merged_project_category, left_on='category', right_on='category', how='left')
        except:
            local_data.columns = ['name', 'category']
            local_data = pd.merge(local_data, merged_project_category, left_on='category', right_on='category', how='left')
        local_data = local_data[['name', 'id']]

    if table_name == "ios":
        projects_merged = select_all_from_table(cursor, cnxn, "projects")
        try:
            local_data = pd.merge(local_data, projects_merged, left_on='project_name', right_on='name',how='left')
        except:
            local_data.columns = ['IO', 'project_name']
            local_data = pd.merge(local_data, projects_merged, left_on='project_name', right_on='name',how='left')
        print(local_data)
        local_data = local_data[['IO', 'id']]
    
    if table_name == 'capex_forecasts':
        local_data['po'] = local_data['po'].astype(str)
        local_data['cap_year'] = local_data['cap_year'].astype(int)
        
        departments = select_all_from_table(cursor, cnxn, "departments")
        po = select_all_from_table(cursor, cnxn, "POs")
        projects = select_all_from_table(cursor, cnxn, "projects")

        local_data = join_tables(
            local_data,
            departments,
            'department',
            'name',
            ['department', 'name'],
            {'id': 'department_id'}
        )

        local_data = join_tables(
            local_data,
            po,
            'po',
            'name',
            ['po', 'name'],
            {'id': 'po_id'}
        )

        local_data = join_tables(
            local_data,
            projects,
            'project_name',
            'name',
            ['project_name', 'name', 'category_id'],
            {'id': 'project_id', 'Forecast': 'capex_forecast'}
        )
        print(local_data.columns)
        local_data = local_data[['po_id', 'department_id', 'cap_year', 'project_id', 'capex_description', 'capex_forecast', 'cost_center']]


    if table_name == 'capex_budgets':
        # local_data['po'] = local_data['po'].astype(str)
        # print(local_data.columns)
        local_data['cap_year'] = local_data['cap_year'].astype(int)

        departments = select_all_from_table(cursor, cnxn, "departments")
        po = select_all_from_table(cursor, cnxn, "POs")
        projects = select_all_from_table(cursor, cnxn, "projects")

        local_data = join_tables(
            local_data,
            departments,
            'department',
            'name',
            ['department', 'name'],
            {'id': 'department_id'}
        )

        local_data = join_tables(
            local_data,
            po,
            'po',
            'name',
            ['po', 'name'],
            {'id': 'po_id'}
        )

        local_data = join_tables(
            local_data,
            projects,
            'project_name',
            'name',
            ['project_name', 'name', 'category_id'],
            {'id': 'project_id', 'fiscal_year': 'cap_year'}
        )
        local_data = local_data[['po_id', 'department_id',  'project_id', 'cap_year','capex_description', 'approved_budget']]

    # 3. Merge new data with current table
    if table_name == 'departments':
        # Special case for departments (uses merge_departments)
        upload_df = merge_departments(cur_table, local_data)
    else:
        upload_df = merge_dataframes(cur_table, local_data, merge_columns, merge_on)
    
    print(local_data)
    print(upload_df)
    # 4. Upload new/merged data to the table
    upload_df.to_sql(table_name, con=engine, if_exists='append', index=False)

    # 5. Return the merged table for confirmation
    return select_all_from_table(cursor, cnxn, table_name)


def add_entry_to_department(df_upload):
    conn_obj = connect_local()
    engine, cursor, cnxn = conn_obj.connect_to_db(engine=True)
    cur_departments = select_all_from_table(cursor, cnxn, "departments")
    department_local = df_upload[['Department']]
    department_upload = merge_departments(cur_departments, department_local)

    return department_upload.to_sql("departments", con=engine, if_exists='append', index=False)
    
