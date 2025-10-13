import pandas as pd
from backend.modify_table_local import add_entry_to_department, add_entry


df_upload = pd.read_csv("upload_data/departments.csv")
add_entry(df_upload, "departments", 'name', "departments", "name")
