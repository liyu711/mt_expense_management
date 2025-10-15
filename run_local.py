# from backend.generate_data import generate_tables
from backend.connect_local import connect_local, drop_all_tables, initialize_database, clear_table, select_columns_from_table
from backend.upload_forecasts_nonpc import upload_nonpc_forecasts_local
from backend.upload_forecasts_pc import upload_pc_forecasts_local
from backend.upload_expenses import upload_expenses_local
from backend.upload_budgets import upload_budgets_local, upload_fundings_local
from backend.upload_capex_forecast import upload_capex_forecasts_local
from backend.upload_capex_budgets import upload_capex_budget_local
from backend.upload_capex_expenses import upload_capex_expense_local
from backend.modify_table_local import add_entry
from backend import \
    upload_nonpc_forecasts_local_m, upload_pc_forecasts_local_m, \
    upload_budgets_local_m, upload_capex_forecast_m, upload_capex_budgets_local_m, upload_human_resource
import pandas as pd

funding_path = 'processed_data/budgets/fundings_.csv'
filepath_nonpc = 'processed_data/forecasts/forecasts_nonpc_.csv'
filepath_pc = 'processed_data/forecasts/forecasts_pc_.csv'
file_path_expenses = "processed_data/expenses/expenses_.csv"
budget_path = 'processed_data/budgets/budgets_.csv'
funding_path = 'processed_data/budgets/fundings_.csv'

capex_forecast_path = "processed_data/forecasts/capex_forecasts_.csv"
capex_budgets_path = "processed_data/budgets/capex_budgets_.csv"
capex_expense_path = "processed_data/expenses/capex_expenses_.csv"
hr_path = "processed_data/info/personnel_categories.csv"

df_nonpc = pd.read_csv(filepath_nonpc)
df_pc = pd.read_csv(filepath_pc)
df_expense = pd.read_csv(file_path_expenses)
df_budgets = pd.read_csv(budget_path)
df_funding = pd.read_csv(funding_path)

df_capex_forecast = pd.read_csv(capex_forecast_path)
df_capex_budget = pd.read_csv(capex_budgets_path)
df_capex_expense = pd.read_csv(capex_expense_path)
df_hr = pd.read_csv(hr_path)

connect_obj = connect_local()

engine, cursor, cnxn = connect_obj.connect_to_db(engine=True)
# drop_all_tables(cursor, cnxn)
initialize_database(cursor, cnxn, initial_values=False)

# df_test = pd.read_csv("df_test.csv")

# res = select_columns_from_table(cursor, "project_forecasts_nonpc", ['fiscal_year', 'department_id', 'project_id','project_category_id', 'io_id', 'po_id'])
# print(res)

df_po = df_nonpc[['PO']]
df_io = df_nonpc[['IO', 'Project Name']]
df_department = df_nonpc[['Department']]
df_project_category = df_nonpc[['Project Category']]
df_projects = df_nonpc[['Project Name', 'Project Category']]
print(df_nonpc.columns)


department_row = add_entry(df_department, 'departments',['category'], 'name')
po_row = add_entry(df_po, "pos", ['name'], 'name')
category_row = add_entry(df_project_category, "project_categories", ['category'], 'category')
projects_row = add_entry(df_projects, "projects", ['name', 'category_id'], 'name')
add_entry(df_hr, "human_resource_categories", ['name'], 'name')
add_entry(df_io, "ios", ['IO_num', 'project_id'], 'IO_num')

# io_row = add_entry(df_io, 'ios', )

upload_nonpc_forecasts_local(df_nonpc)
upload_pc_forecasts_local(df_pc)

cnxn.commit()

upload_expenses_local(df_expense)
upload_budgets_local(df_budgets)

upload_fundings_local(df_funding)
upload_capex_forecasts_local(df_capex_forecast)

upload_capex_budget_local(df_capex_budget)
upload_capex_expense_local(df_capex_expense)


# upload_pc_forecasts_local_m(df_pc)
# upload_budgets_local_m(df_budgets)
# upload_capex_forecast_m(df_capex_forecast)
# upload_capex_budgets_local_m(df_capex_budget)


def temp_get_all_table_columns():
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"\nTable: {table_name}")

        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        for col_info in columns:
            # col_info structure: (cid, name, type, notnull, dflt_value, pk)
            print(f"  Column Name: {col_info[1]}, Type: {col_info[2]}")
