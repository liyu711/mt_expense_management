import pandas as pd
from backend.connect_pyodbc import connect_to_sql, close_connection, clear_table, select_all_from_table
from backend.merge_insert import *
import backend.connect_local as cl
from backend.connect_local import select_columns_from_table
from backend.table_values import table_column_dict

def upload_budgets(file_path, replace=True):
    df_upload = pd.read_csv(file_path)
    engine, cursor, cnxn = connect_to_sql(engine=True)
    upload_budgets_df(df_upload, engine, cursor, cnxn, 'server')

def upload_budgets_local(df_upload):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    years = df_upload['fiscal_year'].unique().tolist()
    # for year in years:
    #     cl.clear_table_by_year(cursor, cnxn, "budgets", year)
    df_upload = upload_budgets_df(df_upload, engine, cursor, cnxn, 'local')
    # upload_budgets_df(df_upload, engine, cursor, cnxn, 'local')
    df_upload.to_sql("budgets", con=engine, if_exists='append', index=False)

def upload_budgets_local_m(df_upload):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    existing_values = select_columns_from_table(cursor, "budgets", table_column_dict["budgets"])
    df_fin = upload_budgets_df(df_upload, engine, cursor, cnxn, 'local')
    
    shared_columns = [col for col in df_fin.columns if col in existing_values.columns and col != 'non_personnel_expense' and col != 'human_resource_expense']
    merged = df_fin.merge(existing_values[shared_columns], on=shared_columns, how='left', indicator=True)
    df_filtered = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    # print(df_filtered)
    df_filtered.to_sql("budgets", con=engine, if_exists='append', index=False)

def upload_budgets_df(df_upload, engine, cursor, cnxn, type, replace=True):
    df_upload['PO'] = df_upload['PO'].astype(str)
    df_upload['fiscal_year'] = df_upload['fiscal_year'].astype(int)

    departments = select_all_from_table(cursor, cnxn, "departments")
    df_upload = pd.merge(df_upload, departments, left_on="Department", right_on="name", how='left')
    df_upload.drop(['Department', 'name'],axis=1, inplace=True)
    df_upload.rename(columns={
        "id": "department_id",
        "Human Resources Budget": "human_resource_expense",
        "Non-Human Resources Budget": "non_personnel_expense"
        }, inplace=True)
    
    pos_merged = select_all_from_table(cursor, cnxn, "pos")
    df_upload = pd.merge(df_upload, pos_merged, left_on='PO', right_on='name', how='left')
    df_upload.drop(['PO', 'name'], axis=1, inplace=True)
    df_upload.rename(columns={'id': 'po_id'}, inplace=True)
    
    return df_upload

    
def upload_fundings(funding_path, replace=True):
    engine, cursor, cnxn = connect_to_sql(engine=True)
    df_upload = pd.read_csv(funding_path)
    upload_fundings_df(df_upload, engine, cursor, cnxn, 'server', replace)

def upload_fundings_local(df_upload, replace=True):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    upload_fundings_df(df_upload, engine, cursor, cnxn, 'local', replace)

def upload_fundings_df(df_upload, engine, cursor, cnxn, type, replace=True):
    print(df_upload)
    df_upload['PO'] = df_upload['PO'].astype(str)
    department = select_all_from_table(cursor, cnxn, 'departments')
    po = select_all_from_table(cursor, cnxn, 'POs')
    projects = select_all_from_table(cursor, cnxn, 'projects')

    df_upload = join_tables(
        df_upload,
        department,
        'Department',
        'name',
        ['Department', 'name'],
        {'id': 'department_id'}
    )

    df_upload = join_tables(
        df_upload,
        po,
        'PO',
        'name',
        ['PO', 'name'],
        {'id': 'po_id'}
    )

    df_upload.to_sql("fundings", con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    budget_path = 'processed_data\\budgets\\budgets_.csv'
    funding_path = 'processed_data\\budgets\\fundings_.csv'
    upload_fundings(funding_path)