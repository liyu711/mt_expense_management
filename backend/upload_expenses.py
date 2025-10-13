import pandas as pd
from backend.connect_pyodbc import connect_to_sql, close_connection, clear_table, select_all_from_table
from backend.merge_insert import *
import backend.connect_local as cl


def clear_expenses_table():
    cursor, cnxn = connect_to_sql()
        
    drop_query = \
    """
    TRUNCATE TABLE expenses;
    """
    cursor.execute(drop_query)
    cnxn.commit()
    close_connection(cursor, cnxn)

def select_expense_from_database():
    select_expense_query = \
    """
    select *
    from expenses
    """
    cursor, cnxn = connect_to_sql()
    cursor.execute(select_expense_query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columns)
    close_connection(cursor, cnxn)
    return df

# not done
def upload_expenses(file_path):
    df_upload = pd.read_csv(file_path)
    engine, cursor, cnxn = connect_to_sql(engine=True)
    upload_expenses_df(df_upload, engine, cursor, cnxn)


def upload_expenses_local(df_upload):
    conn = cl.connect_local()
    engine, cursor, cnxn = conn.connect_to_db(engine=True)
    df_upload = upload_expenses_df(df_upload, engine, cursor, cnxn)
    print(df_upload)
    df_upload.to_sql("expenses", con=engine, if_exists='append', index=False)

def upload_expenses_df(file, engine, cursor, cnxn):
    df_upload = file
    df_upload['Cost element'] = df_upload['Cost element'].astype(int)
    df_upload['Order'] = df_upload['Order'].astype(int)
    
    cur_co_object = select_all_from_table(cursor, cnxn, "co_object_names")
    co_object_names_local = df_upload[['CO object name']]
    co_object_names_columns = ['name']
    co_object_names_upload = merge_dataframes(cur_co_object, co_object_names_local, co_object_names_columns, 'name')
    co_object_names_upload.to_sql("co_object_names", con=engine, if_exists='append', index=False)
    co_object_names_merged = select_all_from_table(cursor, cnxn, "co_object_names")

    cur_cost_elements = select_all_from_table(cursor, cnxn, "cost_elements")
    cost_elements_local = df_upload[['Cost element', 'Cost element name']]
    cost_elements_columns = ['co_id', 'name']
    cost_elements_upload = merge_cost_elements(cur_cost_elements, cost_elements_local, cost_elements_columns, 'co_id')
    cost_elements_upload.to_sql("cost_elements", con=engine, if_exists='append', index=False)
    cost_elements_merged = select_all_from_table(cursor, cnxn, "cost_elements")

    departments = select_all_from_table(cursor, cnxn, "departments")
    io = select_all_from_table(cursor, cnxn, "IOs")

    df_upload = pd.merge(df_upload, departments, left_on="Department", right_on="name", how='left')
    df_upload.drop(['Department', 'name'],axis=1, inplace=True)
    df_upload.rename(columns={
        "id": "department_id",
        "Val.in rep.cur": "expense_value",
        "Cost element name": "co_element_name"
        }, inplace=True)
    
    df_upload = pd.merge(df_upload, cost_elements_merged, left_on='Cost element', right_on='co_id', how='left')
    df_upload.drop(['Cost element', 'co_id', "name"], axis=1, inplace=True)
    df_upload.rename(columns={"id": "cost_element_id"}, inplace=True)
    
    df_upload = pd.merge(df_upload, co_object_names_merged, left_on='CO object name', right_on='name', how='left')
    df_upload.drop(['CO object name', 'name'], axis=1, inplace=True)
    df_upload.rename(columns={'id':'co_object_id'}, inplace=True)

    df_upload = pd.merge(df_upload, io, left_on='Order', right_on='IO_num', how='left')
    df_upload.drop(['Order', 'IO_num', 'project_id'], axis=1, inplace=True)
    df_upload.rename(columns={"id": "io_id"}, inplace=True)
    df_upload.rename(columns={'Name':'name'}, inplace=True)
    
    # df_upload.to_sql("expenses", con=engine, if_exists='append', index=False)
    return df_upload


if __name__ == "__main__":
    file_path = "processed_data/expenses/expenses_.csv"
    upload_expenses(file_path)

