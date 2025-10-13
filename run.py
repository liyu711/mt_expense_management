from backend.generate_data import generate_tables
from backend.connect_pyodbc import \
    connect_to_sql, close_connection, initialize_database
from backend.upload_forecasts_nonpc import upload_nonpc_forecasts, upload_nonpc_forecasts_df
from backend.upload_forecasts_pc import upload_pc_forecasts, upload_pc_forecasts_df
from backend.upload_expenses import upload_expenses, upload_expenses_df
from backend.upload_budgets import upload_budgets, upload_fundings, upload_budgets_df, upload_fundings_df
from backend.upload_capex_forecast import upload_capex_forecasts, upload_capex_forecasts_df
from backend.upload_capex_budgets import upload_capex_budget
from backend.upload_capex_expenses import upload_capex_expense
import pandas as pd

funding_path = 'processed_data\\budgets\\fundings_.csv'
filepath_nonpc = 'processed_data\\forecasts\\forecasts_nonpc_.csv'
filepath_pc = 'processed_data\\forecasts\\forecasts_pc_.csv'
file_path_expenses = "processed_data/expenses/expenses_.csv"
budget_path = 'processed_data\\budgets\\budgets_.csv'
capex_forecast_path = "processed_data/forecasts/capex_forecasts_.csv"
capex_budget_path = "processed_data\\budgets\\capex_budgets_.csv"
capex_expense_path = "processed_data\\expenses\\capex_expenses_.csv"

df_capex_forecast = pd.read_csv(capex_forecast_path)

generate_tables()
engine, cursor, cnxn = connect_to_sql(engine=True)
# engine, cursor, cnxn = connect_to_db(engine=True)
initialize_database(cursor, cnxn)
df_nonpc = pd.read_csv(filepath_nonpc)

upload_nonpc_forecasts(df_nonpc)
upload_pc_forecasts(filepath_pc)
upload_expenses(file_path_expenses)
upload_budgets(budget_path)
upload_fundings(funding_path)
upload_capex_forecasts(capex_forecast_path)
upload_capex_budget(capex_budget_path)
upload_capex_expense(capex_expense_path)


close_connection(cursor, cnxn)