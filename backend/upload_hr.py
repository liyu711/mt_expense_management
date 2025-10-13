import pandas as pd
from backend.connect_local import connect_local, select_all_from_table
from backend import merge_dataframes


def upload_human_resource(df_upload):
    conn = connect_local()
    engine, cursor, cnxn = conn.connect_to_db()
    cur_hr_categories = select_all_from_table(cursor, cnxn, 'human_resource_categories')
    hr_categories_columns = ['name', 'value']
    hr_categories_uplod = merge_dataframes(cur_hr_categories, df_upload, hr_categories_columns, 'name')
    hr_categories_uplod.to_sql("human_resource_categories", con=engine, if_exists='append', index=False)
    
    